/**
 * ZcodeAdapterV2 unit tests: wire-payload helpers (pure), the
 * ensureThread/startTurn mapping onto the shared app-server client, the
 * ProviderEvent → ProviderAdapterV2Event projection (driven through the REAL
 * ZcodeSessionRuntime against a scripted ZcodeProtocolClient), and the
 * deliberately-unsupported operations.
 */
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  EventId,
  type ModelSelection,
  NodeId,
  type OrchestrationV2AppThread,
  type OrchestrationV2ProviderThread,
  ProjectId,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  RunAttemptId,
  RunId,
  type ProviderEvent,
  ThreadId,
  ZCodeSettings,
} from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import { describe, expect, it } from "vite-plus/test";

import * as ServerConfig from "../../config.ts";
import type {
  ZcodeProtocolClientShape,
  ZcodeProtocolNotification,
} from "../../provider/zcode/ZcodeProtocolClient.ts";
import * as IdAllocator from "../IdAllocator.ts";
import {
  ProviderAdapterV2RuntimePolicy,
  type ProviderAdapterV2Event,
  type ProviderAdapterV2SessionRuntime,
} from "../ProviderAdapter.ts";
import {
  makeZcodeAdapterV2,
  projectZcodeToolNode,
  readZcodeErrorMessage,
  ZCODE_PROVIDER,
  zcodeResultTypeToTerminalStatus,
} from "./ZcodeAdapterV2.ts";

const INSTANCE_ID = ProviderInstanceId.make("zcode");
const THREAD_ID = ThreadId.make("thread:zcode-v2-test");
const NOW = DateTime.makeUnsafe("2026-10-07T00:00:00.000Z");
const ZCODE_TEST_SETTINGS = Schema.decodeSync(ZCodeSettings)({ enabled: true });

const layerTest = Layer.mergeAll(
  NodeServices.layer,
  NodeCrypto.layer,
  IdAllocator.layer,
  ServerConfig.layerTest(process.cwd(), { prefix: "zcode-v2-adapter-" }).pipe(
    Layer.provide(NodeServices.layer),
  ),
);

const runtimePolicy = ProviderAdapterV2RuntimePolicy.make({
  runtimeMode: "full-access",
  interactionMode: "default",
  cwd: "/workspace/zcode",
});

const modelSelection = (model = "zcode/test-model"): ModelSelection => ({
  instanceId: INSTANCE_ID,
  model,
});

const makeAppThread = (): OrchestrationV2AppThread =>
  ({
    createdBy: "user",
    creationSource: "web",
    id: THREAD_ID,
    projectId: ProjectId.make("project:fixture:zcode"),
    title: "Zcode test thread",
    providerInstanceId: INSTANCE_ID,
    modelSelection: modelSelection(),
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    activeProviderThreadId: null,
    lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: THREAD_ID },
    forkedFrom: null,
    createdAt: NOW,
    updatedAt: NOW,
    archivedAt: null,
    settledOverride: null,
    settledAt: null,
    lastVisitedAt: null,
    deletedAt: null,
  }) as OrchestrationV2AppThread;

const makeProviderThread = (nativeId: string): OrchestrationV2ProviderThread => ({
  id: ProviderThreadId.make("provider-thread:provider:zcode:native-thread:test"),
  driver: ZCODE_PROVIDER,
  providerInstanceId: INSTANCE_ID,
  providerSessionId: null,
  appThreadId: THREAD_ID,
  ownerNodeId: null,
  nativeThreadRef: { driver: ZCODE_PROVIDER, nativeId, strength: "strong" },
  nativeConversationHeadRef: null,
  status: "idle",
  firstRunOrdinal: null,
  lastRunOrdinal: null,
  handoffIds: [],
  forkedFrom: null,
  createdAt: NOW,
  updatedAt: NOW,
});

const zcodeEvent = (input: {
  readonly method: string;
  readonly payload?: unknown;
}): ProviderEvent =>
  ({
    id: EventId.make("evt_test"),
    kind: "notification",
    provider: ZCODE_PROVIDER,
    threadId: THREAD_ID,
    createdAt: "2026-10-07T00:00:00.000Z",
    method: input.method,
    ...(input.payload !== undefined ? { payload: input.payload } : {}),
  }) as ProviderEvent;

class ZcodeTestTimeoutError extends Schema.TaggedError<ZcodeTestTimeoutError>()(
  "ZcodeTestTimeoutError",
  {},
) {
  override get message(): string {
    return "timed out waiting for zcode adapter events";
  }
}

// ── Pure helpers ───────────────────────────────────────────────────────

describe("zcodeResultTypeToTerminalStatus", () => {
  it("maps success and every error_* resultType", () => {
    expect(zcodeResultTypeToTerminalStatus("success", false)).toBe("completed");
    expect(zcodeResultTypeToTerminalStatus(undefined, false)).toBe("completed");
    for (const resultType of [
      "error_max_turns",
      "error_max_budget",
      "error_during_execution",
      "error_max_tool_calls",
    ]) {
      expect(zcodeResultTypeToTerminalStatus(resultType, false)).toBe("failed");
      expect(zcodeResultTypeToTerminalStatus(resultType, true)).toBe("failed");
    }
  });

  it("splits cancelled by whether T3 requested the interrupt", () => {
    expect(zcodeResultTypeToTerminalStatus("cancelled", false)).toBe("cancelled");
    expect(zcodeResultTypeToTerminalStatus("cancelled", true)).toBe("interrupted");
  });
});

describe("readZcodeErrorMessage", () => {
  it("reads object errors, bare string errors, and errorMessage fields", () => {
    expect(readZcodeErrorMessage({ error: { message: "model overloaded" } })).toBe(
      "model overloaded",
    );
    expect(readZcodeErrorMessage({ error: "connection reset" })).toBe("connection reset");
    expect(readZcodeErrorMessage({ errorMessage: "budget spent" })).toBe("budget spent");
    expect(readZcodeErrorMessage({ error: { message: "  " } })).toBeUndefined();
    expect(readZcodeErrorMessage(undefined)).toBeUndefined();
  });
});

describe("projectZcodeToolNode", () => {
  const context = {
    idAllocator: {
      nodeFromProviderItem: (input: { readonly nativeItemId: string }) =>
        NodeId.make(`node:provider:zcode:native-item:${input.nativeItemId}`),
    },
    threadId: THREAD_ID,
    runId: RunId.make("run:test"),
    rootNodeId: NodeId.make("node:run:test:root"),
    providerThreadId: ProviderThreadId.make("provider-thread:test"),
    providerTurnId: "..." as never,
    toolCallNames: new Map<string, string>(),
  };

  it("maps the scheduled → started → result lifecycle onto node statuses", () => {
    const scheduled = projectZcodeToolNode(
      zcodeEvent({ method: "tool.updated" }),
      { kind: "scheduled", toolCallId: "call_1", toolName: "Bash" },
      context,
      NOW,
    );
    expect(scheduled?.kind).toBe("tool_call");
    expect(scheduled?.status).toBe("pending");
    expect(scheduled?.nativeItemRef?.nativeId).toBe("call_1");
    expect(scheduled?.providerTurnId).toBe(context.providerTurnId);

    const started = projectZcodeToolNode(
      zcodeEvent({ method: "tool.updated" }),
      { kind: "started", toolCallId: "call_1" },
      context,
      NOW,
    );
    expect(started?.status).toBe("running");

    // Wire `result` carries no toolName; the name resolves from the cache
    // the scheduled event populated, and the entry is dropped after.
    const result = projectZcodeToolNode(
      zcodeEvent({ method: "tool.updated" }),
      { kind: "result", toolCallId: "call_1", result: { success: true, content: "/tmp\n" } },
      context,
      NOW,
    );
    expect(result?.status).toBe("completed");
    expect(result?.completedAt).toEqual(NOW);
    expect(context.toolCallNames.has("call_1")).toBe(false);

    const failed = projectZcodeToolNode(
      zcodeEvent({ method: "tool.updated" }),
      { kind: "result", toolCallId: "call_2", result: { success: false } },
      context,
      NOW,
    );
    expect(failed?.status).toBe("failed");
  });

  it("drops batch rollups and malformed payloads", () => {
    expect(
      projectZcodeToolNode(zcodeEvent({ method: "tool.updated" }), { kind: "batch" }, context, NOW),
    ).toBeUndefined();
    expect(
      projectZcodeToolNode(
        zcodeEvent({ method: "tool.updated" }),
        { kind: "scheduled" },
        context,
        NOW,
      ),
    ).toBeUndefined();
    expect(
      projectZcodeToolNode(zcodeEvent({ method: "tool.updated" }), undefined, context, NOW),
    ).toBeUndefined();
  });
});

// ── Adapter against a scripted protocol client ─────────────────────────

interface FakeZcodeClient {
  readonly client: ZcodeProtocolClientShape;
  readonly requests: Array<{ readonly method: string; readonly params?: unknown }>;
  readonly terminate: Effect.Effect<void>;
  readonly pushNotification: (params: unknown) => Effect.Effect<void>;
}

const makeFakeClient = Effect.fnUntraced(function* () {
  const terminated = yield* Deferred.make<void>();
  const notifications = yield* Queue.unbounded<ZcodeProtocolNotification>();
  const requests: Array<{ readonly method: string; readonly params?: unknown }> = [];
  let sessionCount = 0;
  const client: ZcodeProtocolClientShape = {
    request: (method, params) =>
      Effect.sync(() => {
        requests.push({ method, ...(params !== undefined ? { params } : {}) });
        if (method === "session/create") {
          sessionCount += 1;
          return { session: { sessionId: `sess_${sessionCount}` } };
        }
        if (method === "session/resume") {
          const record = params as { readonly sessionId?: unknown } | undefined;
          return { session: { sessionId: record?.sessionId ?? "sess_resumed" } };
        }
        return {};
      }),
    notifications: Stream.fromQueue(notifications),
    handleServerRequest: () => Effect.void,
    whenTerminated: Deferred.await(terminated),
    close: Effect.void,
    command: "fake-zcode",
  };
  return {
    client,
    requests,
    terminate: Deferred.succeed(terminated, undefined),
    pushNotification: (params: unknown) =>
      Queue.offer(notifications, { method: "session/event", params }).pipe(Effect.asVoid),
  } satisfies FakeZcodeClient;
});

const makeHarness = Effect.fnUntraced(function* () {
  const idAllocator = yield* IdAllocator.IdAllocatorV2;
  const serverConfig = yield* ServerConfig.ServerConfig;
  const crypto = yield* Crypto.Crypto;
  const adapterScope = yield* Effect.acquireRelease(Scope.make(), (scope) =>
    Scope.close(scope, Exit.void).pipe(Effect.ignore),
  );
  const spawned: Array<FakeZcodeClient> = [];
  const adapter = makeZcodeAdapterV2({
    instanceId: INSTANCE_ID,
    settings: ZCODE_TEST_SETTINGS,
    environment: {},
    idAllocator,
    serverConfig,
    crypto,
    // The makeClient seam replaces spawning, so the spawner must never run.
    spawner: "unused" as never,
    adapterScope,
    makeClient: () =>
      Effect.map(makeFakeClient(), (fake) => {
        spawned.push(fake);
        return fake.client;
      }),
  });
  const openSession = (threadId: ThreadId = THREAD_ID) =>
    adapter.openSession({
      threadId,
      providerSessionId: ProviderSessionId.make(`provider-session:${threadId}`),
      modelSelection: modelSelection(),
      runtimePolicy,
    });
  return { adapter, spawned, openSession };
});

/** Collects the runtime's adapter events; `waitUntil` polls with a running buffer. */
const collectRuntimeEvents = Effect.fnUntraced(function* (
  runtime: ProviderAdapterV2SessionRuntime,
) {
  const queue = yield* Queue.unbounded<ProviderAdapterV2Event>();
  yield* Stream.runForEach(runtime.events, (event) =>
    Queue.offer(queue, event).pipe(Effect.asVoid),
  ).pipe(Effect.forkScoped);
  const seen: Array<ProviderAdapterV2Event> = [];
  const waitUntil = (predicate: (events: ReadonlyArray<ProviderAdapterV2Event>) => boolean) =>
    Effect.gen(function* () {
      for (let attempt = 0; attempt < 400; attempt += 1) {
        const batch = Array.from(yield* Queue.takeAll(queue));
        seen.push(...batch);
        if (predicate(seen)) {
          return seen;
        }
        yield* Effect.sleep("25 millis");
      }
      return yield* new ZcodeTestTimeoutError();
    });
  return { waitUntil };
});

const ensureThreadInput = () => ({
  threadId: THREAD_ID,
  modelSelection: modelSelection(),
  runtimePolicy,
});

const startTurnInput = (providerThread: OrchestrationV2ProviderThread) => ({
  appThread: makeAppThread(),
  threadId: THREAD_ID,
  runId: RunId.make("run:zcode-v2-test"),
  runOrdinal: 1,
  providerTurnOrdinal: 1,
  attemptId: RunAttemptId.make("run-attempt:zcode-v2-test:1"),
  rootNodeId: NodeId.make("node:run:zcode-v2-test:root"),
  providerThread,
  message: {
    messageId: "..." as never,
    text: "hello zcode",
    attachments: [],
    createdBy: "user" as const,
    creationSource: "web" as const,
  },
  modelSelection: modelSelection(),
  runtimePolicy,
});

const turnEventOf = (events: ReadonlyArray<ProviderAdapterV2Event>, status: string) =>
  events.find(
    (event): event is Extract<ProviderAdapterV2Event, { type: "provider_turn.updated" }> =>
      event.type === "provider_turn.updated" && event.providerTurn.status === status,
  );

describe("ZcodeAdapterV2", () => {
  it("spawns the shared client lazily and binds the native session on ensureThread", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness();
      const runtime = yield* harness.openSession();
      expect(harness.spawned).toHaveLength(0);

      const providerThread = yield* runtime.ensureThread(ensureThreadInput());
      expect(harness.spawned).toHaveLength(1);
      expect(providerThread.nativeThreadRef?.nativeId).toBe("sess_1");
      expect(providerThread.appThreadId).toBe(THREAD_ID);
      expect(providerThread.driver).toBe(ZCODE_PROVIDER);
      const methods = harness.spawned[0]!.requests.map((request) => request.method);
      expect(methods).toContain("session/create");
      expect(methods).toContain("session/subscribe");

      // Idempotent: a repeat ensure returns the same row without respawning.
      const again = yield* runtime.ensureThread(ensureThreadInput());
      expect(again.id).toBe(providerThread.id);
      expect(harness.spawned).toHaveLength(1);
    }).pipe(Effect.scoped, Effect.provide(layerTest), Effect.runPromise));

  it("resumes an existing native thread through session/resume", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness();
      const runtime = yield* harness.openSession();
      const existing = makeProviderThread("sess_persisted");
      const resumed = yield* runtime.ensureThread({
        ...ensureThreadInput(),
        existingProviderThread: existing,
      });
      expect(resumed.id).toBe(existing.id);
      const resume = harness.spawned[0]!.requests.find(
        (request) => request.method === "session/resume",
      );
      expect(resume?.params).toMatchObject({ sessionId: "sess_persisted" });
      expect(resumed.nativeThreadRef?.nativeId).toBe("sess_persisted");
    }).pipe(Effect.scoped, Effect.provide(layerTest), Effect.runPromise));

  it("projects startTurn, tool updates and turn completion onto adapter events", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness();
      const runtime = yield* harness.openSession();
      const { waitUntil } = yield* collectRuntimeEvents(runtime);
      const providerThread = yield* runtime.ensureThread(ensureThreadInput());
      yield* runtime.startTurn(startTurnInput(providerThread));

      const fake = harness.spawned[0]!;
      const send = fake.requests.find((request) => request.method === "session/send");
      expect(send?.params).toMatchObject({ content: "hello zcode" });

      yield* fake.pushNotification({
        type: "tool.updated",
        sessionId: "sess_1",
        payload: { toolCallId: "call_1", toolName: "Bash", kind: "scheduled" },
      });
      yield* fake.pushNotification({
        type: "tool.updated",
        sessionId: "sess_1",
        payload: {
          toolCallId: "call_1",
          result: { success: true, content: "/tmp\n" },
          kind: "result",
        },
      });
      yield* fake.pushNotification({
        type: "turn.completed",
        sessionId: "sess_1",
        payload: {
          resultType: "success",
          usage: { totalTokens: 100, inputTokens: 80, outputTokens: 20 },
        },
      });

      const events = yield* waitUntil((collected) =>
        collected.some((event) => event.type === "turn.terminal"),
      );

      const turnEvents = events.filter((event) => event.type === "provider_turn.updated");
      expect(
        turnEvents.map(
          (event) =>
            (event as Extract<ProviderAdapterV2Event, { type: "provider_turn.updated" }>)
              .providerTurn.status,
        ),
      ).toEqual(["running", "completed"]);
      const completed = turnEventOf(events, "completed");
      expect(completed?.providerTurn.tokenUsage?.usedTokens).toBe(100);

      const nodeEvents = events.filter((event) => event.type === "node.updated");
      expect(
        nodeEvents.map(
          (event) =>
            (event as Extract<ProviderAdapterV2Event, { type: "node.updated" }>).node.status,
        ),
      ).toEqual(["pending", "completed"]);
      expect(
        (nodeEvents[0] as Extract<ProviderAdapterV2Event, { type: "node.updated" }>).node.kind,
      ).toBe("tool_call");

      const terminal = events.find((event) => event.type === "turn.terminal") as {
        status: string;
        failure: unknown;
      };
      expect(terminal.status).toBe("completed");
      expect(terminal.failure).toBeNull();
    }).pipe(Effect.scoped, Effect.provide(layerTest), Effect.runPromise));

  it("maps a provider turn failure onto a failed terminal event", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness();
      const runtime = yield* harness.openSession();
      const { waitUntil } = yield* collectRuntimeEvents(runtime);
      const providerThread = yield* runtime.ensureThread(ensureThreadInput());
      yield* runtime.startTurn(startTurnInput(providerThread));
      yield* harness.spawned[0]!.pushNotification({
        type: "turn.completed",
        sessionId: "sess_1",
        payload: {
          resultType: "error_max_budget",
          error: { message: "budget exhausted" },
        },
      });
      const events = yield* waitUntil((collected) =>
        collected.some((event) => event.type === "turn.terminal" && event.status === "failed"),
      );
      const terminal = events.find(
        (event) => event.type === "turn.terminal" && event.status === "failed",
      ) as { failure: { message: string } };
      expect(terminal.failure.message).toBe("budget exhausted");
    }).pipe(Effect.scoped, Effect.provide(layerTest), Effect.runPromise));

  it("steers by sending again and maps an interrupted cancel onto interrupted", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness();
      const runtime = yield* harness.openSession();
      const { waitUntil } = yield* collectRuntimeEvents(runtime);
      const providerThread = yield* runtime.ensureThread(ensureThreadInput());
      yield* runtime.startTurn(startTurnInput(providerThread));
      const running = yield* waitUntil(
        (collected) => turnEventOf(collected, "running") !== undefined,
      );
      const runningTurnId = turnEventOf(running, "running")!.providerTurn.id;

      yield* runtime.steerTurn({
        threadId: THREAD_ID,
        runId: RunId.make("run:zcode-v2-test"),
        providerThread,
        providerTurnId: runningTurnId,
        message: {
          messageId: "..." as never,
          text: "actually, do it differently",
          attachments: [],
          createdBy: "user",
          creationSource: "web",
        },
      });
      const sends = harness.spawned[0]!.requests.filter(
        (request) => request.method === "session/send",
      ).map((request) => (request.params as { readonly content?: unknown }).content);
      expect(sends).toEqual(["hello zcode", "actually, do it differently"]);

      yield* runtime.interruptTurn({
        providerThread,
        providerTurnId: runningTurnId,
      });
      expect(
        harness.spawned[0]!.requests.some((request) => request.method === "session/stop"),
      ).toBe(true);
      yield* harness.spawned[0]!.pushNotification({
        type: "turn.completed",
        sessionId: "sess_1",
        payload: { resultType: "cancelled" },
      });
      const events = yield* waitUntil((collected) =>
        collected.some((event) => event.type === "turn.terminal"),
      );
      const terminal = events.find((event) => event.type === "turn.terminal") as {
        status: string;
      };
      expect(terminal.status).toBe("interrupted");
    }).pipe(Effect.scoped, Effect.provide(layerTest), Effect.runPromise));

  it("fails unsupported operations (rollback, fork, runtime requests)", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness();
      const runtime = yield* harness.openSession();
      const rollback = yield* Effect.flip(
        runtime.rollbackThread({
          providerThread: makeProviderThread("sess_1"),
          target: "..." as never,
          providerThreadTurns: [],
        }),
      );
      expect(rollback._tag).toBe("ProviderAdapterRollbackThreadError");

      const fork = yield* Effect.flip(
        runtime.forkThread({
          sourceProviderThread: makeProviderThread("sess_1"),
          targetThreadId: ThreadId.make("thread:target"),
        }),
      );
      expect(fork._tag).toBe("ProviderAdapterForkThreadError");

      const respond = yield* Effect.flip(
        runtime.respondToRuntimeRequest({ requestId: "..." as never }),
      );
      expect(respond._tag).toBe("ProviderAdapterRuntimeRequestResponseError");
    }).pipe(Effect.scoped, Effect.provide(layerTest), Effect.runPromise));

  it("respawns the shared app-server after the client terminates", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness();
      const firstThreadId = ThreadId.make("thread:zcode-respawn-1");
      const first = yield* harness.openSession(firstThreadId);
      yield* first.ensureThread({
        ...ensureThreadInput(),
        threadId: firstThreadId,
      });
      expect(harness.spawned).toHaveLength(1);

      // Kill the shared process; the adapter must drop the dead client.
      yield* harness.spawned[0]!.terminate;
      yield* Effect.sleep(50);

      const secondThreadId = ThreadId.make("thread:zcode-respawn-2");
      const second = yield* harness.openSession(secondThreadId);
      const thread = yield* second.ensureThread({
        ...ensureThreadInput(),
        threadId: secondThreadId,
      });
      expect(harness.spawned).toHaveLength(2);
      expect(thread.nativeThreadRef?.nativeId).toBe("sess_1");
    }).pipe(Effect.scoped, Effect.provide(layerTest), Effect.runPromise));

  it("streams assistant text from model.streaming deltas and finalizes on turn completion", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness();
      const runtime = yield* harness.openSession();
      const { waitUntil } = yield* collectRuntimeEvents(runtime);
      const providerThread = yield* runtime.ensureThread(ensureThreadInput());
      yield* runtime.startTurn(startTurnInput(providerThread));
      const fake = harness.spawned[0]!;

      yield* fake.pushNotification({
        type: "model.streaming",
        sessionId: "sess_1",
        payload: { assistantMessageId: "msg_a1", delta: "你好", done: false, kind: "text_delta" },
      });
      yield* fake.pushNotification({
        type: "model.streaming",
        sessionId: "sess_1",
        payload: { assistantMessageId: "msg_a1", delta: "，世界", done: false, kind: "text_delta" },
      });
      yield* fake.pushNotification({
        type: "model.streaming",
        sessionId: "sess_1",
        payload: {
          assistantMessageId: "msg_a1",
          delta: "思考",
          done: false,
          kind: "reasoning_delta",
        },
      });
      yield* fake.pushNotification({
        type: "turn.completed",
        sessionId: "sess_1",
        payload: { resultType: "success", usage: { totalTokens: 10 } },
      });

      const events = yield* waitUntil((collected) =>
        collected.some((event) => event.type === "turn.terminal"),
      );
      const messageEvents = events.filter(
        (event): event is Extract<ProviderAdapterV2Event, { type: "message.updated" }> =>
          event.type === "message.updated",
      );
      // reasoning_delta is skipped; both deltas stream, turn.completed
      // finalizes the accumulated segment (done never arrived).
      expect(messageEvents.map((event) => event.message.text)).toEqual([
        "你好",
        "你好，世界",
        "你好，世界",
      ]);
      expect(messageEvents.map((event) => event.message.streaming)).toEqual([true, true, false]);
      expect(messageEvents.every((event) => event.message.role === "assistant")).toBe(true);

      const itemEvents = events.filter(
        (event): event is Extract<ProviderAdapterV2Event, { type: "turn_item.updated" }> =>
          event.type === "turn_item.updated",
      );
      expect(itemEvents.map((event) => event.turnItem.type)).toEqual([
        "assistant_message",
        "assistant_message",
        "assistant_message",
      ]);
      const lastItem = itemEvents[2]!.turnItem;
      expect(lastItem.type === "assistant_message" && lastItem.streaming === false).toBe(true);

      const nodeKinds = events
        .filter(
          (event): event is Extract<ProviderAdapterV2Event, { type: "node.updated" }> =>
            event.type === "node.updated",
        )
        .map((event) => event.node.kind);
      expect(nodeKinds).toContain("assistant_message");
    }).pipe(Effect.scoped, Effect.provide(layerTest), Effect.runPromise));

  it("treats message.upserted as the authoritative overwrite and skips user echo", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness();
      const runtime = yield* harness.openSession();
      const { waitUntil } = yield* collectRuntimeEvents(runtime);
      const providerThread = yield* runtime.ensureThread(ensureThreadInput());
      yield* runtime.startTurn(startTurnInput(providerThread));
      const fake = harness.spawned[0]!;

      yield* fake.pushNotification({
        type: "model.streaming",
        sessionId: "sess_1",
        payload: { assistantMessageId: "msg_a2", delta: "草稿", done: false, kind: "text_delta" },
      });
      // User echo (attachments, no toolCalls) must NOT project a second copy.
      yield* fake.pushNotification({
        type: "message.upserted",
        sessionId: "sess_1",
        payload: { content: "hello zcode", attachments: [] },
      });
      yield* fake.pushNotification({
        type: "message.upserted",
        sessionId: "sess_1",
        payload: { messageId: "msg_a2", content: "定稿文本", toolCalls: [] },
      });
      yield* fake.pushNotification({
        type: "turn.completed",
        sessionId: "sess_1",
        payload: { resultType: "success", usage: { totalTokens: 10 } },
      });

      const events = yield* waitUntil((collected) =>
        collected.some((event) => event.type === "turn.terminal"),
      );
      const messageEvents = events
        .filter(
          (event): event is Extract<ProviderAdapterV2Event, { type: "message.updated" }> =>
            event.type === "message.updated",
        )
        .map((event) => event.message);
      expect(messageEvents.map((message) => message.text)).toEqual(["草稿", "定稿文本"]);
      // One stable id across streaming + overwrite + finalize re-emit.
      expect(new Set(messageEvents.map((message) => message.id)).size).toBe(1);
      expect(messageEvents.at(-1)?.streaming).toBe(false);
    }).pipe(Effect.scoped, Effect.provide(layerTest), Effect.runPromise));

  it("backstops a turn with no streamed segments using turn.completed response", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness();
      const runtime = yield* harness.openSession();
      const { waitUntil } = yield* collectRuntimeEvents(runtime);
      const providerThread = yield* runtime.ensureThread(ensureThreadInput());
      yield* runtime.startTurn(startTurnInput(providerThread));
      const fake = harness.spawned[0]!;

      yield* fake.pushNotification({
        type: "turn.completed",
        sessionId: "sess_1",
        payload: { resultType: "success", response: "成功", usage: { totalTokens: 10 } },
      });

      const events = yield* waitUntil((collected) =>
        collected.some((event) => event.type === "turn.terminal"),
      );
      const messageEvents = events.filter(
        (event): event is Extract<ProviderAdapterV2Event, { type: "message.updated" }> =>
          event.type === "message.updated",
      );
      expect(messageEvents.map((event) => event.message.text)).toEqual(["成功"]);
      expect(messageEvents[0]?.message.streaming).toBe(false);
    }).pipe(Effect.scoped, Effect.provide(layerTest), Effect.runPromise));
});
