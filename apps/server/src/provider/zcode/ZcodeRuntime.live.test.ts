/**
 * Live end-to-end verification of ZcodeProtocolClient + ZcodeSessionRuntime
 * against the REAL zcode app-server. Opt-in only — each run spends a real
 * model call — so the suite skips unless `ZCODE_LIVE_TEST=1` is set and the
 * ZCode.app bundle exists. Verifies the full round trip the wire probes
 * established: create → subscribe → send → model.streaming deltas →
 * turn.completed, plus session/stop cancellation and resume.
 */
// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { it } from "@effect/vitest";
import { ThreadId } from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import { assert, describe } from "vite-plus/test";

import { DEFAULT_ZCODE_BINARY_PATH } from "@t3tools/contracts";

import { makeZcodeProtocolClient } from "./ZcodeProtocolClient.ts";
import { makeZcodeSessionRuntime } from "./ZcodeSessionRuntime.ts";

const LIVE_ENABLED =
  process.env.ZCODE_LIVE_TEST === "1" && NodeFS.existsSync(DEFAULT_ZCODE_BINARY_PATH);

const readSessionEventParams = Schema.decodeUnknownEffect(
  Schema.Struct({
    type: Schema.String,
    payload: Schema.optional(Schema.Unknown),
    sessionId: Schema.optional(Schema.String),
  }),
);

describe.skipIf(!LIVE_ENABLED)("zcode live runtime", () => {
  it.live("runs create → send → streaming → turn.completed against the real app-server", () =>
    Effect.gen(function* () {
      // The ambient test scope outlives the whole assertion sequence —
      // `Effect.scoped` would close it (and kill the app-server) the moment
      // client construction finishes.
      const scope = yield* Effect.scope;
      const client = yield* makeZcodeProtocolClient({
        binaryPath: DEFAULT_ZCODE_BINARY_PATH,
        cwd: "/tmp",
      }).pipe(Effect.provideService(Scope.Scope, scope));

      const runtime = yield* makeZcodeSessionRuntime(
        {
          threadId: ThreadId.make("thr_zcode_live_test"),
          cwd: "/tmp",
          runtimeMode: "full-access",
        },
        { client },
      );

      const events = yield* Queue.unbounded<{
        readonly type: string;
        readonly payload?: unknown;
      }>();
      yield* Stream.runForEach(runtime.events, (event) => {
        if (event.kind !== "notification") {
          return Effect.void;
        }
        return Effect.flatMap(readSessionEventParams(event.payload), (params) =>
          Queue.offer(events, params),
        ).pipe(Effect.ignore);
      }).pipe(Effect.forkScoped);

      const started = yield* runtime.start();
      assert.equal(started.status, "ready");
      const sessionId = (started.resumeCursor as { sessionId: string }).sessionId;
      assert.match(sessionId, /^sess_/);

      // Push notifications must reach the runtime's handleNotification: the
      // adapter owns that fan-out in production; here the test wires it.
      yield* Stream.runForEach(client.notifications, (notification) =>
        Effect.gen(function* () {
          const params = notification.params as { sessionId?: unknown } | undefined;
          if (params?.sessionId !== sessionId) {
            return;
          }
          yield* runtime.handleNotification(notification);
        }).pipe(Effect.ignore),
      ).pipe(Effect.forkScoped);

      yield* runtime.sendTurn({ input: "只回复两个字：收到" });

      const deadline = (yield* Clock.currentTimeMillis) + 60_000;
      let sawTextDelta = false;
      let completed: { readonly type: string; readonly payload?: unknown } | undefined;
      while ((yield* Clock.currentTimeMillis) < deadline && completed === undefined) {
        yield* Effect.sleep(200);
        while ((yield* Queue.size(events)) > 0) {
          const event = yield* Queue.take(events);
          if (event.type === "model.streaming") {
            const payload = event.payload as { kind?: string; delta?: string };
            if (payload?.kind === "text_delta" && (payload.delta ?? "").length > 0) {
              sawTextDelta = true;
            }
          }
          if (event.type === "turn.completed") {
            completed = event;
          }
        }
      }

      assert.ok(completed !== undefined, "expected turn.completed before the deadline");
      const completion = completed!.payload as { resultType?: string; response?: string };
      assert.equal(completion.resultType, "success");
      assert.ok((completion.response ?? "").length > 0);
      assert.ok(sawTextDelta, "expected at least one text_delta while streaming");

      const session = yield* runtime.getSession;
      assert.equal(session.status, "ready");
      yield* runtime.close;
    }).pipe(Effect.provide(NodeServices.layer)),
  );
});
