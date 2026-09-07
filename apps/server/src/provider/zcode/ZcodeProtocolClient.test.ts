import { describe, expect, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import { ChildProcessSpawner } from "effect/unstable/process";

import {
  isZcodeSessionNotFoundError,
  makeZcodeProtocolClient,
  readZcodeSessionSnapshot,
  type ZcodeProtocolClientShape,
  type ZcodeProtocolError,
  zcodeModelRefToSlug,
  zcodeSlugToModelRef,
  ZcodeProtocolRequestError,
} from "./ZcodeProtocolClient.ts";

/** In-memory app-server: `stdin` captures writes, `stdout` plays pushed lines. */
interface FakeZcodeServer {
  readonly written: Queue.Queue<string>;
  readonly pushLine: (line: string) => Effect.Effect<void>;
}

const makeFakeZcodeServer = Effect.gen(function* () {
  const written = yield* Effect.acquireRelease(Queue.unbounded<string>(), (queue) =>
    Queue.shutdown(queue),
  );
  const output = yield* Effect.acquireRelease(Queue.unbounded<string>(), (queue) =>
    Queue.shutdown(queue),
  );
  const decoder = new TextDecoder();
  const spawner = ChildProcessSpawner.make(() =>
    Effect.succeed(
      ChildProcessSpawner.makeHandle({
        pid: ChildProcessSpawner.ProcessId(4242),
        // Never resolves: a real child's exit code only completes when the
        // process exits, and the client treats early completion as a crash.
        exitCode: Effect.never,
        isRunning: Effect.succeed(true),
        kill: () => Effect.void,
        unref: Effect.succeed(Effect.void),
        stdin: Sink.forEach((chunk: Uint8Array) =>
          Queue.offer(written, decoder.decode(chunk, { stream: true })),
        ),
        stdout: Stream.encodeText(Stream.fromQueue(output)),
        stderr: Stream.empty,
        all: Stream.empty,
        getInputFd: () => Sink.drain,
        getOutputFd: () => Stream.empty,
      }),
    ),
  );
  const server: FakeZcodeServer = {
    written,
    pushLine: (line) => Queue.offer(output, `${line}\n`).pipe(Effect.asVoid),
  };
  return { server, spawner };
});

const withClient = <A, ER>(
  run: (input: {
    readonly server: FakeZcodeServer;
    readonly client: ZcodeProtocolClientShape;
  }) => Effect.Effect<A, ZcodeProtocolError, ER>,
) =>
  Effect.gen(function* () {
    const { server, spawner } = yield* makeFakeZcodeServer;
    const client = yield* makeZcodeProtocolClient({
      binaryPath: "/fake/zcode.cjs",
      cwd: "/tmp",
    }).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner));
    return yield* run({ server, client });
  });

/** Reads the next line the client wrote to the (fake) process stdin. */
const nextWrittenLine = (server: FakeZcodeServer) =>
  Effect.map(
    Queue.take(server.written),
    (chunk) =>
      chunk
        .split("\n")
        .map((line) => line.trim())
        .find((line) => line.length > 0) ?? "",
  );

const isZcodeProtocolRequestError = Schema.is(ZcodeProtocolRequestError);

const readJson = Schema.decodeUnknownSync(Schema.fromJsonString(Schema.Unknown));
const writeJson = Schema.encodeUnknownSync(Schema.fromJsonString(Schema.Unknown));

describe("makeZcodeProtocolClient", () => {
  it.effect("correlates responses by request id", () =>
    withClient(({ server, client }) =>
      Effect.gen(function* () {
        const responseFiber = yield* Effect.forkScoped(client.request("session/list", {}));
        const outgoing = readJson(yield* nextWrittenLine(server)) as {
          id: number;
          method: string;
          params: unknown;
        };
        expect(outgoing.method).toBe("session/list");
        expect(outgoing.params).toEqual({});
        expect(typeof outgoing.id).toBe("number");
        yield* server.pushLine(writeJson({ id: outgoing.id, result: { sessions: [] } }));
        const result = (yield* Fiber.join(responseFiber)) as { sessions: unknown[] };
        expect(result.sessions).toEqual([]);
      }),
    ),
  );

  it.effect("surfaces protocol errors and classifies session-not-found", () =>
    withClient(({ server, client }) =>
      Effect.gen(function* () {
        const responseFiber = yield* Effect.forkScoped(
          client.request("session/resume", { sessionId: "sess_missing" }),
        );
        const outgoing = readJson(yield* nextWrittenLine(server)) as { id: number };
        yield* server.pushLine(
          writeJson({
            id: outgoing.id,
            error: { code: -32004, message: "Session not found: sess_missing" },
          }),
        );
        const exit = yield* Fiber.await(responseFiber);
        const error = Exit.match(exit, {
          onSuccess: () => null,
          onFailure: (cause) =>
            cause.reasons.find((reason) => Cause.isFailReason(reason))?.error ?? null,
        });
        if (error === null) {
          throw new Error("expected the request to fail");
        }
        expect(isZcodeProtocolRequestError(error)).toBe(true);
        expect(isZcodeSessionNotFoundError(error)).toBe(true);
        expect(
          isZcodeSessionNotFoundError(
            new ZcodeProtocolRequestError({
              method: "session/resume",
              code: -32602,
              protocolMessage: "Invalid params",
            }),
          ),
        ).toBe(false);
      }),
    ),
  );

  it.effect("answers known server requests and rejects unknown ones with -32601", () =>
    withClient(({ server }) =>
      Effect.gen(function* () {
        yield* server.pushLine(
          writeJson({
            id: "server-1",
            method: "session/requestRuntimePreferences",
            params: { sessionId: "sess_1", scope: "runtime-materialization" },
          }),
        );
        const preferenceReply = readJson(yield* nextWrittenLine(server)) as {
          id: string;
          result: { nativeSearchEnhancementsEnabled: boolean };
        };
        expect(preferenceReply.id).toBe("server-1");
        expect(preferenceReply.result.nativeSearchEnhancementsEnabled).toBe(false);

        yield* server.pushLine(
          writeJson({
            id: "server-2",
            method: "interaction/requestOfficialMcpAuthHeaders",
            params: { mcpKey: "x" },
          }),
        );
        const unavailableReply = readJson(yield* nextWrittenLine(server)) as {
          id: string;
          error: { code: number };
        };
        expect(unavailableReply.id).toBe("server-2");
        expect(unavailableReply.error.code).toBe(-32601);
      }),
    ),
  );

  it.effect("exposes notifications on the shared stream", () =>
    withClient(({ server, client }) =>
      Effect.gen(function* () {
        const received = yield* Queue.unbounded<unknown>();
        yield* Stream.runForEach(client.notifications, (notification) =>
          Queue.offer(received, notification),
        ).pipe(Effect.forkScoped);
        yield* server.pushLine(
          writeJson({
            method: "session/event",
            params: { type: "turn.started", sessionId: "sess_1" },
          }),
        );
        const notification = (yield* Queue.take(received)) as {
          method: string;
          params: { type: string };
        };
        expect(notification.method).toBe("session/event");
        expect(notification.params.type).toBe("turn.started");
      }),
    ),
  );

  it.effect("skips non-JSON stdout lines instead of terminating the client (S2)", () =>
    withClient(({ server, client }) =>
      Effect.gen(function* () {
        // Log/banner noise on stdout must not take the client down — the very
        // next line is a correlated response that still has to arrive.
        const responseFiber = yield* Effect.forkScoped(client.request("session/list", {}));
        const outgoing = readJson(yield* nextWrittenLine(server)) as { id: number };
        yield* server.pushLine("ZCode v0.16.5 — this line is not JSON {");
        yield* server.pushLine("neither is this one");
        yield* server.pushLine(writeJson({ id: outgoing.id, result: { sessions: [] } }));

        const result = (yield* Fiber.join(responseFiber)) as { sessions: unknown[] };
        expect(result.sessions).toEqual([]);

        // The client is still alive: it answers another request.
        const secondFiber = yield* Effect.forkScoped(client.request("session/list", {}));
        const second = readJson(yield* nextWrittenLine(server)) as { id: number };
        yield* server.pushLine(writeJson({ id: second.id, result: { sessions: [1] } }));
        expect(((yield* Fiber.join(secondFiber)) as { sessions: unknown[] }).sessions).toEqual([1]);
      }),
    ),
  );
});

describe("zcode model slugs", () => {
  it("round-trips composite providerId/modelId slugs", () => {
    const ref = { providerId: "builtin:bigmodel-coding-plan", modelId: "GLM-5.3" };
    const slug = zcodeModelRefToSlug(ref);
    expect(slug).toBe("builtin:bigmodel-coding-plan/GLM-5.3");
    expect(zcodeSlugToModelRef(slug)).toEqual(ref);
    expect(zcodeSlugToModelRef("no-slash")).toBeUndefined();
    expect(zcodeSlugToModelRef("a/")).toBeUndefined();
  });
});

describe("readZcodeSessionSnapshot", () => {
  it("accepts a probe-shaped create result and rejects foreign payloads", () => {
    const snapshot = readZcodeSessionSnapshot({
      session: {
        sessionId: "sess_1",
        mode: "build",
        model: { providerId: "p", modelId: "m" },
      },
      settings: {
        model: {
          available: [{ ref: { providerId: "p", modelId: "m" }, label: "M" }],
          current: { providerId: "p", modelId: "m" },
        },
        mode: { current: "build" },
      },
    });
    expect(snapshot?.session.sessionId).toBe("sess_1");
    expect(snapshot?.settings?.model?.available?.[0]?.label).toBe("M");
    expect(readZcodeSessionSnapshot({ unexpected: true })).toBeUndefined();
    expect(readZcodeSessionSnapshot(null)).toBeUndefined();
  });
});
