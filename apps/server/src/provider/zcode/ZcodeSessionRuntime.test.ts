import { describe, expect, it } from "@effect/vitest";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";

import { RuntimeMode, type ProviderEvent, ThreadId } from "@t3tools/contracts";

import {
  makeZcodeSessionRuntime,
  runtimeModeToZcodeMode,
  type ZcodeSessionRuntimeShape,
} from "./ZcodeSessionRuntime.ts";
import {
  ZcodeProtocolRequestError,
  type ZcodeProtocolClientShape,
  type ZcodeProtocolNotification,
} from "./ZcodeProtocolClient.ts";

describe("runtimeModeToZcodeMode", () => {
  it("maps approval-carrying T3 modes onto zcode edit until approvals are bridged", () => {
    // TEMPORARY COMPROMISE: zcode `build` gates every file edit behind the
    // `interaction/requestPermission` reverse request, which the protocol
    // client answers with -32601 — so `build` would leave agents unable to
    // edit files. See the mapping table's comment for the full rationale.
    expect(runtimeModeToZcodeMode("approval-required")).toBe("edit");
    expect(runtimeModeToZcodeMode("auto-accept-edits")).toBe("edit");
    expect(runtimeModeToZcodeMode("auto")).toBe("edit");
    expect(runtimeModeToZcodeMode("full-access")).toBe("yolo");
  });

  it("covers every RuntimeMode literal", () => {
    const modes: ReadonlyArray<RuntimeMode> = [
      "approval-required",
      "auto-accept-edits",
      "auto",
      "full-access",
    ];
    for (const mode of modes) {
      expect(["plan", "build", "edit", "yolo", "auto"]).toContain(runtimeModeToZcodeMode(mode));
    }
  });
});

// ── Runtime behavior against a scripted client (no real process) ──────────

type RequestHandler = (params: unknown) => Effect.Effect<unknown, ZcodeProtocolRequestError>;

/** Client double: records requests, answers from per-method scripts. */
const makeScriptedClient = (handlers: Record<string, RequestHandler>) => {
  const calls: Array<{ readonly method: string; readonly params: unknown }> = [];
  const client: ZcodeProtocolClientShape = {
    request: (method, params) =>
      Effect.gen(function* () {
        calls.push({ method, params });
        const handler = handlers[method];
        if (handler === undefined) {
          return yield* new ZcodeProtocolRequestError({
            method,
            code: -32601,
            protocolMessage: `No script for ${method}`,
          });
        }
        return yield* handler(params);
      }),
    notifications: Stream.empty,
    handleServerRequest: () => Effect.void,
    whenTerminated: Effect.void,
    close: Effect.void,
    command: "scripted",
  };
  return { client, calls };
};

const ok =
  (result: unknown): RequestHandler =>
  () =>
    Effect.succeed(result);
const failWith =
  (code: number, message: string): RequestHandler =>
  () =>
    Effect.fail(
      new ZcodeProtocolRequestError({
        method: "session/setModel",
        code,
        protocolMessage: message,
      }),
    );

const createSnapshot = (
  sessionId: string,
  extra?: { readonly status?: string; readonly settingsMode?: string },
) => ({
  session: {
    sessionId,
    ...(extra?.status ? { status: extra.status } : {}),
  },
  ...(extra?.settingsMode ? { settings: { mode: { current: extra.settingsMode } } } : {}),
});

const scriptedCrypto = (() => {
  let counter = 0;
  return { randomUUIDv4: Effect.sync(() => `uuid-${(counter += 1)}`) } as unknown as Crypto.Crypto;
})();

const makeRuntime = (
  client: ZcodeProtocolClientShape,
  options?: Partial<Parameters<typeof makeZcodeSessionRuntime>[0]>,
) =>
  makeZcodeSessionRuntime(
    {
      threadId: ThreadId.make("thr_scripted"),
      cwd: "/tmp",
      runtimeMode: "full-access",
      ...options,
    },
    { client },
  ).pipe(Effect.provideService(Crypto.Crypto, scriptedCrypto));

const sessionEventNotification = (params: unknown): ZcodeProtocolNotification => ({
  method: "session/event",
  params,
});

const collectEvents = (
  runtime: ZcodeSessionRuntimeShape,
  count: number,
): Effect.Effect<ReadonlyArray<ProviderEvent>> =>
  Effect.map(Stream.runCollect(Stream.take(runtime.events, count)), (chunk) => Array.from(chunk));

/** start() emits connecting + ready + started; later notifications follow. */
const START_EVENT_COUNT = 3;

describe("makeZcodeSessionRuntime (scripted client)", () => {
  it.effect("resets the session on turn.failed so the turn cannot stick (B1)", () =>
    Effect.gen(function* () {
      const { client } = makeScriptedClient({
        "session/create": ok(createSnapshot("sess_1")),
        "session/subscribe": ok({}),
        "session/send": ok({ accepted: true, sessionId: "sess_1", stateRevision: 2 }),
      });
      const runtime = yield* makeRuntime(client);

      yield* runtime.start();
      const turn = yield* runtime.sendTurn({ input: "go" });
      let session = yield* runtime.getSession;
      expect(session.status).toBe("running");
      expect(session.activeTurnId).toBe(turn.turnId);

      // Failure paths emit turn.failed INSTEAD of turn.completed.
      yield* runtime.handleNotification(
        sessionEventNotification({
          type: "turn.failed",
          sessionId: "sess_1",
          turnId: "turn_wire_1",
          payload: { error: { message: "model overloaded" }, turnPhase: "execution" },
        }),
      );

      session = yield* runtime.getSession;
      expect(session.status).toBe("ready");
      expect(session.activeTurnId).toBeUndefined();

      const events = yield* collectEvents(runtime, START_EVENT_COUNT + 1);
      expect(events[START_EVENT_COUNT]?.method).toBe("turn.failed");
      expect(events[START_EVENT_COUNT]?.turnId).toBe(turn.turnId);
    }),
  );

  it.effect("keeps the canonical turn id on terminal events (B1-regression)", () =>
    Effect.gen(function* () {
      const { client } = makeScriptedClient({
        "session/create": ok(createSnapshot("sess_1")),
        "session/subscribe": ok({}),
        "session/send": ok({ accepted: true, sessionId: "sess_1", stateRevision: 2 }),
      });
      const runtime = yield* makeRuntime(client);

      yield* runtime.start();
      const turn = yield* runtime.sendTurn({ input: "go" });

      // The state mutation clears activeTurnId BEFORE the event is emitted;
      // the emitted turn.completed must still carry the stamped turn id.
      yield* runtime.handleNotification(
        sessionEventNotification({
          type: "turn.completed",
          sessionId: "sess_1",
          turnId: "turn_wire_1",
          payload: { response: "ok", resultType: "success" },
        }),
      );

      const session = yield* runtime.getSession;
      expect(session.status).toBe("ready");
      expect(session.activeTurnId).toBeUndefined();

      const events = yield* collectEvents(runtime, START_EVENT_COUNT + 1);
      const terminal = events[START_EVENT_COUNT];
      expect(terminal?.method).toBe("turn.completed");
      expect(terminal?.turnId).toBe(turn.turnId);
    }),
  );

  it.effect("unsticks a running session on state.updated status error (M4)", () =>
    Effect.gen(function* () {
      const { client } = makeScriptedClient({
        "session/create": ok(createSnapshot("sess_1")),
        "session/subscribe": ok({}),
        "session/send": ok({ accepted: true, sessionId: "sess_1", stateRevision: 2 }),
      });
      const runtime = yield* makeRuntime(client);

      yield* runtime.start();
      yield* runtime.sendTurn({ input: "go" });

      yield* runtime.handleNotification({
        method: "state.updated",
        params: { patch: { status: "error" }, reason: "prompt_failed", sessionId: "sess_1" },
      });

      const session = yield* runtime.getSession;
      expect(session.status).toBe("ready");
      expect(session.activeTurnId).toBeUndefined();
    }),
  );

  it.effect("keeps mid-turn waiting/paused states running (M4)", () =>
    Effect.gen(function* () {
      const { client } = makeScriptedClient({
        "session/create": ok(createSnapshot("sess_1")),
        "session/subscribe": ok({}),
        "session/send": ok({ accepted: true, sessionId: "sess_1", stateRevision: 2 }),
      });
      const runtime = yield* makeRuntime(client);

      yield* runtime.start();
      const turn = yield* runtime.sendTurn({ input: "go" });
      yield* runtime.handleNotification({
        method: "state.updated",
        params: { patch: { status: "waiting" }, sessionId: "sess_1" },
      });

      const session = yield* runtime.getSession;
      expect(session.status).toBe("running");
      expect(session.activeTurnId).toBe(turn.turnId);
    }),
  );

  it.effect("reconciles the mode after resume and preserves a running snapshot (S3/M5)", () =>
    Effect.gen(function* () {
      const { client, calls } = makeScriptedClient({
        "session/resume": ok(
          createSnapshot("sess_resumed", { status: "running", settingsMode: "yolo" }),
        ),
        "session/setMode": ok({}),
        "session/subscribe": ok({}),
      });
      const runtime = yield* makeRuntime(client, {
        runtimeMode: "approval-required",
        resumeCursor: { sessionId: "sess_resumed" },
      });

      const started = yield* runtime.start();

      // The persisted session was created in yolo; T3 asked for
      // approval-required → edit, so setMode must have been issued.
      const setMode = calls.find((call) => call.method === "session/setMode");
      expect(setMode?.params).toEqual({ sessionId: "sess_resumed", mode: "edit" });
      // A resumed mid-turn session must not be forced ready.
      expect(started.status).toBe("running");
    }),
  );

  it.effect("does not setMode after resume when the mode already matches (S3)", () =>
    Effect.gen(function* () {
      const { client, calls } = makeScriptedClient({
        "session/resume": ok(createSnapshot("sess_resumed", { settingsMode: "yolo" })),
        "session/subscribe": ok({}),
      });
      const runtime = yield* makeRuntime(client, {
        runtimeMode: "full-access",
        resumeCursor: { sessionId: "sess_resumed" },
      });

      yield* runtime.start();
      expect(calls.some((call) => call.method === "session/setMode")).toBe(false);
    }),
  );

  it.effect("falls back to fresh create when the resumed session is gone", () =>
    Effect.gen(function* () {
      const { client, calls } = makeScriptedClient({
        "session/resume": failWith(-32004, "Session not found: sess_gone"),
        "session/create": ok(createSnapshot("sess_fresh")),
        "session/subscribe": ok({}),
      });
      const runtime = yield* makeRuntime(client, {
        resumeCursor: { sessionId: "sess_gone" },
      });

      const started = yield* runtime.start();
      expect(started.status).toBe("ready");
      expect((started.resumeCursor as { sessionId: string }).sessionId).toBe("sess_fresh");
      expect(calls.some((call) => call.method === "session/create")).toBe(true);
    }),
  );

  it.effect("sendTurn survives a rejected setModel and does not stamp it (S4)", () =>
    Effect.gen(function* () {
      const { client } = makeScriptedClient({
        "session/create": ok(createSnapshot("sess_1")),
        "session/subscribe": ok({}),
        "session/setModel": failWith(-32603, "Unsupported model"),
        "session/send": ok({ accepted: true, sessionId: "sess_1", stateRevision: 2 }),
      });
      const runtime = yield* makeRuntime(client);
      yield* runtime.start();

      const turn = yield* runtime.sendTurn({
        input: "go",
        model: "builtin:bigmodel-coding-plan/GLM-Does-Not-Exist",
      });

      expect(turn.resumeCursor).toEqual({ sessionId: "sess_1" });
      const session = yield* runtime.getSession;
      expect(session.status).toBe("running");
      // The rejected slug must not be recorded as the session's model.
      expect(session.model).toBeUndefined();
    }),
  );

  it.effect("reverts the running stamp when session/send itself fails (M1)", () =>
    Effect.gen(function* () {
      const { client } = makeScriptedClient({
        "session/create": ok(createSnapshot("sess_1")),
        "session/subscribe": ok({}),
        "session/send": failWith(-32000, "Send rejected"),
      });
      const runtime = yield* makeRuntime(client);
      yield* runtime.start();

      const failure = yield* Effect.flip(runtime.sendTurn({ input: "go" }));
      expect(failure._tag).toBe("ZcodeProtocolRequestError");

      const session = yield* runtime.getSession;
      expect(session.status).toBe("ready");
      expect(session.activeTurnId).toBeUndefined();
    }),
  );

  it.effect(
    "attributes racing notifications to the canonical turn id stamped before the send (M1)",
    () =>
      Effect.gen(function* () {
        const { client } = makeScriptedClient({
          "session/create": ok(createSnapshot("sess_1")),
          "session/subscribe": ok({}),
          "session/send": ok({ accepted: true, sessionId: "sess_1", stateRevision: 2 }),
        });
        const runtime = yield* makeRuntime(client);
        yield* runtime.start();

        const turn = yield* runtime.sendTurn({ input: "go" });

        // Notifications that race ahead of the send response still carry the
        // canonical id because sendTurn stamps activeTurnId before sending.
        yield* runtime.handleNotification(
          sessionEventNotification({
            type: "model.streaming",
            sessionId: "sess_1",
            turnId: "turn_wire_9",
            payload: { kind: "text_delta", delta: "hi", done: false },
          }),
        );

        const collected = yield* collectEvents(runtime, START_EVENT_COUNT + 1);
        const streaming = collected[START_EVENT_COUNT];
        expect(streaming?.method).toBe("model.streaming");
        expect(streaming?.turnId).toBe(turn.turnId);
        expect(streaming?.turnId).not.toBe("turn_wire_9");
      }),
  );
});
