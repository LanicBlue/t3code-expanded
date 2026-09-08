import { describe, expect, it } from "@effect/vitest";
import {
  EventId,
  type ProviderEvent,
  ProviderDriverKind,
  ThreadId,
  TurnId,
  ZCodeSettings,
} from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import { ChildProcessSpawner } from "effect/unstable/process";

import { makeZcodeAdapter, mapZcodeEventToRuntimeEvents } from "./ZCodeAdapter.ts";
import type { ZcodeProtocolClientShape } from "../zcode/ZcodeProtocolClient.ts";

const PROVIDER = ProviderDriverKind.make("zcode");
const THREAD_ID = ThreadId.make("thr_1");

function zcodeEvent(input: {
  readonly kind?: ProviderEvent["kind"];
  readonly method: string;
  readonly payload?: unknown;
  readonly turnId?: TurnId | undefined;
}): ProviderEvent {
  return {
    id: EventId.make("evt_1"),
    kind: input.kind ?? "notification",
    provider: PROVIDER,
    threadId: THREAD_ID,
    createdAt: "2026-09-07T00:00:00.000Z",
    method: input.method,
    ...(input.turnId ? { turnId: input.turnId } : {}),
    ...(input.payload !== undefined ? { payload: input.payload } : {}),
  } as ProviderEvent;
}

describe("mapZcodeEventToRuntimeEvents", () => {
  it("maps session lifecycle events", () => {
    const connecting = mapZcodeEventToRuntimeEvents(
      zcodeEvent({ kind: "session", method: "session/connecting", payload: undefined }),
      THREAD_ID,
    );
    expect(connecting).toHaveLength(1);
    expect(connecting[0]?.type).toBe("session.state.changed");
    expect(connecting[0]?.payload).toMatchObject({ state: "starting" });

    const started = mapZcodeEventToRuntimeEvents(
      zcodeEvent({
        kind: "session",
        method: "session/started",
        payload: { sessionId: "sess_1", resumed: false },
      }),
      THREAD_ID,
    );
    expect(started.map((event) => event.type)).toEqual(["session.started", "thread.started"]);
    expect(started[1]?.payload).toMatchObject({ providerThreadId: "sess_1" });
    expect(started[0]?.raw?.source).toBe("zcode.app-server.notification");

    const closed = mapZcodeEventToRuntimeEvents(
      zcodeEvent({ kind: "session", method: "session/closed" }),
      THREAD_ID,
    );
    expect(closed[0]?.type).toBe("session.exited");
    expect(closed[0]?.payload).toMatchObject({ exitKind: "graceful" });
  });

  it("maps model.streaming deltas by kind", () => {
    const text = mapZcodeEventToRuntimeEvents(
      zcodeEvent({
        method: "model.streaming",
        turnId: TurnId.make("turn_1"),
        payload: {
          type: "model.streaming",
          sessionId: "sess_1",
          payload: { assistantMessageId: "m1", delta: "hello", kind: "text_delta", done: false },
        },
      }),
      THREAD_ID,
    );
    expect(text).toHaveLength(1);
    expect(text[0]?.type).toBe("content.delta");
    expect(text[0]?.payload).toMatchObject({ streamKind: "assistant_text", delta: "hello" });

    const reasoning = mapZcodeEventToRuntimeEvents(
      zcodeEvent({
        method: "model.streaming",
        payload: {
          type: "model.streaming",
          sessionId: "sess_1",
          payload: { delta: "thinking", kind: "reasoning_delta" },
        },
      }),
      THREAD_ID,
    );
    expect(reasoning[0]?.payload).toMatchObject({ streamKind: "reasoning_text" });
  });

  it("maps turn.completed with usage and resultType", () => {
    const events = mapZcodeEventToRuntimeEvents(
      zcodeEvent({
        method: "turn.completed",
        turnId: TurnId.make("turn_1"),
        payload: {
          type: "turn.completed",
          sessionId: "sess_1",
          payload: {
            response: "done",
            resultType: "success",
            usage: { totalTokens: 100, inputTokens: 80, outputTokens: 20, cacheReadTokens: 4 },
          },
        },
      }),
      THREAD_ID,
    );
    expect(events.map((event) => event.type)).toEqual([
      "turn.completed",
      "thread.token-usage.updated",
    ]);
    expect(events[0]?.payload).toMatchObject({ state: "completed" });
    expect(events[1]?.payload).toMatchObject({
      usage: { usedTokens: 100, inputTokens: 80, outputTokens: 20, cachedInputTokens: 4 },
    });

    const cancelled = mapZcodeEventToRuntimeEvents(
      zcodeEvent({
        method: "turn.completed",
        payload: {
          type: "turn.completed",
          sessionId: "sess_1",
          payload: { response: "", resultType: "cancelled" },
        },
      }),
      THREAD_ID,
    );
    expect(cancelled[0]?.payload).toMatchObject({ state: "cancelled" });
  });

  it("maps every error_* resultType onto a failed turn with the error message (B2)", () => {
    // The wire enum is success | cancelled | error_max_turns |
    // error_max_budget | error_during_execution | error_max_tool_calls —
    // all error_* variants are failures.
    const resultTypes = [
      "error_max_turns",
      "error_max_budget",
      "error_during_execution",
      "error_max_tool_calls",
    ];
    for (const resultType of resultTypes) {
      const events = mapZcodeEventToRuntimeEvents(
        zcodeEvent({
          method: "turn.completed",
          turnId: TurnId.make("turn_1"),
          payload: {
            type: "turn.completed",
            sessionId: "sess_1",
            payload: {
              resultType,
              error: { message: `boom: ${resultType}` },
              usage: { totalTokens: 5 },
            },
          },
        }),
        THREAD_ID,
      );
      expect(events[0]?.type).toBe("turn.completed");
      expect(events[0]?.payload).toMatchObject({
        state: "failed",
        errorMessage: `boom: ${resultType}`,
      });
      expect(events[1]?.type).toBe("thread.token-usage.updated");
    }
  });

  it("maps turn.failed onto a failed turn completion with the error message (B1)", () => {
    const events = mapZcodeEventToRuntimeEvents(
      zcodeEvent({
        method: "turn.failed",
        turnId: TurnId.make("turn_1"),
        payload: {
          type: "turn.failed",
          sessionId: "sess_1",
          turnId: "turn_wire_1",
          payload: { error: { message: "model overloaded" }, turnPhase: "execution" },
        },
      }),
      THREAD_ID,
    );
    expect(events).toHaveLength(1);
    expect(events[0]?.type).toBe("turn.completed");
    expect(events[0]?.payload).toMatchObject({
      state: "failed",
      errorMessage: "model overloaded",
    });
  });

  it("accepts a bare string error on turn.failed", () => {
    const events = mapZcodeEventToRuntimeEvents(
      zcodeEvent({
        method: "turn.failed",
        payload: {
          type: "turn.failed",
          sessionId: "sess_1",
          payload: { error: "connection reset" },
        },
      }),
      THREAD_ID,
    );
    expect(events[0]?.payload).toMatchObject({ state: "failed", errorMessage: "connection reset" });
  });

  it("maps state.updated status patches", () => {
    const running = mapZcodeEventToRuntimeEvents(
      zcodeEvent({
        method: "state.updated",
        payload: { patch: { status: "running" }, reason: "prompt_started", sessionId: "sess_1" },
      }),
      THREAD_ID,
    );
    expect(running[0]?.payload).toMatchObject({ state: "running" });

    const idle = mapZcodeEventToRuntimeEvents(
      zcodeEvent({
        method: "state.updated",
        payload: { patch: { status: "idle" }, reason: "prompt_failed", sessionId: "sess_1" },
      }),
      THREAD_ID,
    );
    expect(idle[0]?.payload).toMatchObject({ state: "ready" });

    // The wire enum also has completed | waiting | paused | error: `completed`
    // behaves like idle; the interactive and error states are deliberately
    // not emitted as session-state changes (see the mapper comment).
    const completed = mapZcodeEventToRuntimeEvents(
      zcodeEvent({
        method: "state.updated",
        payload: { patch: { status: "completed" }, sessionId: "sess_1" },
      }),
      THREAD_ID,
    );
    expect(completed[0]?.payload).toMatchObject({ state: "ready" });

    for (const status of ["waiting", "paused", "error"]) {
      expect(
        mapZcodeEventToRuntimeEvents(
          zcodeEvent({
            method: "state.updated",
            payload: { patch: { status }, sessionId: "sess_1" },
          }),
          THREAD_ID,
        ),
      ).toHaveLength(0);
    }

    const noStatus = mapZcodeEventToRuntimeEvents(
      zcodeEvent({
        method: "state.updated",
        payload: { patch: { model: { current: {} } }, sessionId: "sess_1" },
      }),
      THREAD_ID,
    );
    expect(noStatus).toHaveLength(0);
  });

  it("maps session.titleUpdated onto thread metadata", () => {
    const events = mapZcodeEventToRuntimeEvents(
      zcodeEvent({
        method: "session.titleUpdated",
        payload: {
          type: "session.titleUpdated",
          sessionId: "sess_1",
          payload: { title: "Fix the bug", source: "first_input", previousTitle: "" },
        },
      }),
      THREAD_ID,
    );
    expect(events[0]?.type).toBe("thread.metadata.updated");
    expect(events[0]?.payload).toMatchObject({ name: "Fix the bug" });
  });

  it("drops unknown session/event types without crashing", () => {
    expect(
      mapZcodeEventToRuntimeEvents(
        zcodeEvent({
          method: "tool.callStarted",
          payload: { type: "tool.callStarted", sessionId: "sess_1", payload: {} },
        }),
        THREAD_ID,
      ),
    ).toHaveLength(0);
    expect(
      mapZcodeEventToRuntimeEvents(zcodeEvent({ method: "session.updated" }), THREAD_ID),
    ).toHaveLength(0);
  });
});

// ── Shared-client respawn after process death (S2) ────────────────────────

const ZCODE_TEST_SETTINGS = Schema.decodeSync(ZCodeSettings)({});

describe("makeZcodeAdapter shared client", () => {
  it.live("respawns the app-server after the shared client terminates", () =>
    Effect.gen(function* () {
      const scope = yield* Effect.scope;
      let spawnCount = 0;
      const crashes: Array<Effect.Effect<void>> = [];

      const makeClient = () =>
        Effect.gen(function* () {
          spawnCount += 1;
          const terminated = yield* Deferred.make<void>();
          let sessionCount = 0;
          const client: ZcodeProtocolClientShape = {
            request: (method) =>
              Effect.succeed(
                method === "session/create"
                  ? { session: { sessionId: `sess_${(sessionCount += 1)}` } }
                  : {},
              ),
            notifications: Stream.empty,
            handleServerRequest: () => Effect.void,
            whenTerminated: Deferred.await(terminated),
            close: Effect.void,
            command: "fake-zcode",
          };
          crashes.push(Deferred.succeed(terminated, undefined));
          return client;
        });

      // The makeClient seam replaces spawning, so this spawner must never run.
      const unusedSpawner = ChildProcessSpawner.make(() =>
        Effect.die("the makeClient seam replaces spawning"),
      );
      const fakeCrypto = {
        randomUUIDv4: Effect.succeed("adapter-test-uuid"),
      } as unknown as Crypto.Crypto;

      const adapter = yield* makeZcodeAdapter(ZCODE_TEST_SETTINGS, {
        makeClient,
      }).pipe(
        Effect.provideService(Scope.Scope, scope),
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, unusedSpawner),
        Effect.provideService(Crypto.Crypto, fakeCrypto),
      );

      yield* adapter.startSession({ threadId: ThreadId.make("thr_1"), runtimeMode: "full-access" });
      expect(spawnCount).toBe(1);

      // Kill the shared process; the adapter must drop the dead client.
      yield* crashes[0]!;
      yield* Effect.sleep(50);

      const second = yield* adapter.startSession({
        threadId: ThreadId.make("thr_2"),
        runtimeMode: "full-access",
      });
      expect(spawnCount).toBe(2);
      expect((second.resumeCursor as { sessionId: string }).sessionId).toBe("sess_1");
    }),
  );
});
