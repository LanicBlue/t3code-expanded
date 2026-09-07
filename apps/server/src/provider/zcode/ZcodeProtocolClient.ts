/**
 * ZcodeProtocolClient — wire-level client for the ZCode Protocol v1 app-server.
 *
 * ZCode's `app-server` subcommand is a resident stdio service speaking
 * line-delimited JSON that is JSON-RPC-shaped WITHOUT the `jsonrpc` key:
 *   - client → server request:  `{"id":1,"method":"session/list","params":{}}`
 *   - response:                 `{"id":1,"result":…}` or `{"id":1,"error":{…}}`
 *   - server → client request:  `{"id":"server-1","method":…,"params":…}`
 *     (must be answered within 15s or the server times out with -32022)
 *   - notification:             `{"method":"session/event","params":{…}}`
 *
 * There is no handshake; any method may be called immediately after spawn.
 *
 * This module deliberately mirrors the structure of
 * `packages/effect-codex-app-server/src/protocol.ts` (the one other stdio
 * app-server this codebase speaks to) but is hand-rolled: the zcode protocol
 * is only probe-verified (see the protocol notes in `ZcodeSessionRuntime`),
 * so the framing/correlation core stays small and the payload schemas are
 * permissive optionalKey structs rather than a generated contract.
 *
 * The pure pieces (message classification, payload schemas) are plain
 * functions/values; Effect wraps only the process, queue and fiber plumbing.
 *
 * NOTE on process sharing: one client is intended to back ALL sessions of a
 * provider instance (the protocol multiplexes by `sessionId` and the desktop
 * client uses a single app-server process). If session interference ever
 * shows up on the wire, the fallback is one process per session — nothing in
 * this module prevents that, it just changes who owns the client.
 *
 * @module provider/zcode/ZcodeProtocolClient
 */
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";

import { resolveSpawnCommand } from "@t3tools/shared/shell";

// ── Errors ─────────────────────────────────────────────────────────────

export class ZcodeProtocolSpawnError extends Schema.TaggedErrorClass<ZcodeProtocolSpawnError>()(
  "ZcodeProtocolSpawnError",
  {
    command: Schema.String,
    cause: Schema.Defect(),
  },
) {
  override get message(): string {
    return `Failed to spawn zcode app-server: ${this.command}`;
  }
}

export class ZcodeProtocolTransportError extends Schema.TaggedErrorClass<ZcodeProtocolTransportError>()(
  "ZcodeProtocolTransportError",
  {
    operation: Schema.String,
    cause: Schema.Defect(),
  },
) {
  override get message(): string {
    return `zcode app-server transport failed during ${this.operation}.`;
  }
}

export class ZcodeProtocolRequestError extends Schema.TaggedErrorClass<ZcodeProtocolRequestError>()(
  "ZcodeProtocolRequestError",
  {
    method: Schema.String,
    code: Schema.Number,
    protocolMessage: Schema.String,
  },
) {
  override get message(): string {
    return `zcode app-server request '${this.method}' failed (${this.code}): ${this.protocolMessage}`;
  }
}

export class ZcodeProtocolProcessExitedError extends Schema.TaggedErrorClass<ZcodeProtocolProcessExitedError>()(
  "ZcodeProtocolProcessExitedError",
  {
    code: Schema.Number,
    pid: Schema.optional(Schema.Number),
  },
) {
  override get message(): string {
    return `zcode app-server process exited with code ${this.code}.`;
  }
}

export class ZcodeProtocolInputStreamEndedError extends Schema.TaggedErrorClass<ZcodeProtocolInputStreamEndedError>()(
  "ZcodeProtocolInputStreamEndedError",
  {},
) {
  override get message(): string {
    return "zcode app-server closed its output stream.";
  }
}

export type ZcodeProtocolError =
  | ZcodeProtocolSpawnError
  | ZcodeProtocolTransportError
  | ZcodeProtocolRequestError
  | ZcodeProtocolProcessExitedError
  | ZcodeProtocolInputStreamEndedError;

/** `session/resume` (and friends) report a missing session as -32004. */
export const ZCODE_SESSION_NOT_FOUND_ERROR_CODE = -32004;

export function isZcodeSessionNotFoundError(error: unknown): boolean {
  return (
    typeof error === "object" &&
    error !== null &&
    "_tag" in error &&
    (error as { readonly _tag?: unknown })._tag === "ZcodeProtocolRequestError" &&
    "code" in error &&
    (error as { readonly code?: unknown }).code === ZCODE_SESSION_NOT_FOUND_ERROR_CODE
  );
}

// ── Wire message classification (pure) ─────────────────────────────────

export interface ZcodeProtocolNotification {
  readonly method: string;
  readonly params?: unknown;
}

export interface ZcodeProtocolServerRequest {
  readonly id: string | number;
  readonly method: string;
  readonly params?: unknown;
}

type JsonRpcId = string | number;

interface ZcodeProtocolResponse {
  readonly id: JsonRpcId;
  readonly result?: unknown;
  readonly error?: {
    readonly code: number;
    readonly message: string;
    readonly data?: unknown;
  };
}

const isObject = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null;

const isJsonRpcId = (value: unknown): value is JsonRpcId =>
  typeof value === "string" || typeof value === "number";

function isIncomingRequest(value: unknown): value is ZcodeProtocolServerRequest {
  return isObject(value) && typeof value.method === "string" && isJsonRpcId(value.id);
}

function isIncomingNotification(value: unknown): value is ZcodeProtocolNotification {
  return isObject(value) && typeof value.method === "string" && !("id" in value);
}

function isIncomingResponse(value: unknown): value is ZcodeProtocolResponse {
  if (!isObject(value) || !isJsonRpcId(value.id)) {
    return false;
  }
  return "result" in value || "error" in value;
}

const encodeJsonString = Schema.encodeUnknownEffect(Schema.fromJsonString(Schema.Unknown));
const decodeJsonString = Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Unknown));

// ── Payload schemas (permissive, probe-verified) ────────────────────────

export const ZcodeModelRefSchema = Schema.Struct({
  providerId: Schema.String,
  modelId: Schema.String,
});
export type ZcodeModelRef = typeof ZcodeModelRefSchema.Type;

/** `providerId/modelId` composite — the slug format T3 uses for zcode models. */
export function zcodeModelRefToSlug(ref: ZcodeModelRef): string {
  return `${ref.providerId}/${ref.modelId}`;
}

export function zcodeSlugToModelRef(slug: string): ZcodeModelRef | undefined {
  const separator = slug.indexOf("/");
  if (separator <= 0 || separator === slug.length - 1) {
    return undefined;
  }
  return { providerId: slug.slice(0, separator), modelId: slug.slice(separator + 1) };
}

export const ZcodeModelDescriptorSchema = Schema.Struct({
  ref: ZcodeModelRefSchema,
  label: Schema.String,
  contextWindow: Schema.optional(Schema.Number),
  maxOutputTokens: Schema.optional(Schema.Number),
  providerLabel: Schema.optional(Schema.String),
});
export type ZcodeModelDescriptor = typeof ZcodeModelDescriptorSchema.Type;

const ZcodeModelSettingsSchema = Schema.Struct({
  available: Schema.optional(Schema.Array(ZcodeModelDescriptorSchema)),
  current: Schema.optional(ZcodeModelRefSchema),
  lastUsed: Schema.optional(ZcodeModelRefSchema),
});
export type ZcodeModelSettings = typeof ZcodeModelSettingsSchema.Type;

export const ZcodeWorkspaceSchema = Schema.Struct({
  workspacePath: Schema.String,
  workspaceKey: Schema.String,
});
export type ZcodeWorkspace = typeof ZcodeWorkspaceSchema.Type;

const ZcodeSettingsBlockSchema = Schema.Struct({
  model: Schema.optional(ZcodeModelSettingsSchema),
  mode: Schema.optional(Schema.Struct({ current: Schema.String })),
});
export type ZcodeSettingsBlock = typeof ZcodeSettingsBlockSchema.Type;

export const ZcodeSessionSummarySchema = Schema.Struct({
  sessionId: Schema.String,
  title: Schema.optional(Schema.String),
  status: Schema.optional(Schema.String),
  mode: Schema.optional(Schema.String),
  workspace: Schema.optional(ZcodeWorkspaceSchema),
  createdAt: Schema.optional(Schema.Number),
  updatedAt: Schema.optional(Schema.Number),
  sessionKind: Schema.optional(Schema.String),
});
export type ZcodeSessionSummary = typeof ZcodeSessionSummarySchema.Type;

/**
 * Full-snapshot response of `session/create`, `session/resume`,
 * `session/setMode`, `session/setModel` and `session/read`. All fields beyond
 * `session` are optional because the probe coverage varies per method.
 */
export const ZcodeSessionSnapshotSchema = Schema.Struct({
  session: Schema.Struct({
    sessionId: Schema.String,
    mode: Schema.optional(Schema.String),
    model: Schema.optional(ZcodeModelRefSchema),
    status: Schema.optional(Schema.String),
    workspace: Schema.optional(ZcodeWorkspaceSchema),
  }),
  settings: Schema.optional(ZcodeSettingsBlockSchema),
});
export type ZcodeSessionSnapshot = typeof ZcodeSessionSnapshotSchema.Type;

export const ZcodeSessionListResultSchema = Schema.Struct({
  sessions: Schema.Array(ZcodeSessionSummarySchema),
});
export type ZcodeSessionListResult = typeof ZcodeSessionListResultSchema.Type;

const isZcodeSessionSnapshot = Schema.is(ZcodeSessionSnapshotSchema);

/** Decode a full-session snapshot, or `undefined` when the shape is foreign. */
export function readZcodeSessionSnapshot(value: unknown): ZcodeSessionSnapshot | undefined {
  return isZcodeSessionSnapshot(value) ? value : undefined;
}

// ── Client ─────────────────────────────────────────────────────────────

export interface ZcodeProtocolClientOptions {
  readonly binaryPath: string;
  /** Pre-tokenized extra args inserted after `app-server`. */
  readonly launchArgs?: ReadonlyArray<string>;
  readonly cwd: string;
  readonly environment?: NodeJS.ProcessEnv;
  /**
   * Reply sent to the server's `session/requestRuntimePreferences` reverse
   * request. `session/create` emits that request immediately; not answering
   * within 15s fails the create with -32022.
   */
  readonly nativeSearchEnhancementsEnabled?: boolean;
}

export interface ZcodeProtocolClientShape {
  /** Send a request and await its correlated response. */
  readonly request: (
    method: string,
    params?: unknown,
  ) => Effect.Effect<unknown, ZcodeProtocolError>;
  /** Every notification the server pushes (all sessions — filter by `params.sessionId`). */
  readonly notifications: Stream.Stream<ZcodeProtocolNotification>;
  /**
   * Register (or replace) the handler for a server→client request method.
   * Registrations must be in place before the triggering client request is
   * sent; unknown methods are answered with -32601, which the server treats
   * as an acceptable "client does not support this" skip. Handlers must not
   * fail — catch internally; failures would surface to the user as a
   * protocol reply error.
   */
  readonly handleServerRequest: (
    method: string,
    handler: (params: unknown) => Effect.Effect<unknown>,
  ) => Effect.Effect<void>;
  /**
   * Resolves once the process/transport has terminated (crash, external kill
   * or `close`). Owners of a shared client watch this to drop the reference so
   * the next request spawns a fresh app-server.
   */
  readonly whenTerminated: Effect.Effect<void>;
  readonly close: Effect.Effect<void>;
  /** The spawn command, for error messages. */
  readonly command: string;
}

const ZCODE_APP_SERVER_FORCE_KILL_AFTER = "2 seconds" as const;

interface PendingRequest {
  readonly deferred: Deferred.Deferred<unknown, ZcodeProtocolError>;
  readonly method: string;
}

export const makeZcodeProtocolClient = Effect.fn("makeZcodeProtocolClient")(function* (
  options: ZcodeProtocolClientOptions,
): Effect.fn.Return<
  ZcodeProtocolClientShape,
  ZcodeProtocolSpawnError,
  ChildProcessSpawner.ChildProcessSpawner | Scope.Scope
> {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const scope = yield* Scope.Scope;

  const extendEnv = options.environment === undefined;
  const spawnCommand = yield* resolveSpawnCommand(
    process.execPath,
    [options.binaryPath, "app-server", ...(options.launchArgs ?? [])],
    { ...(options.environment ? { env: options.environment } : {}), extendEnv },
  ).pipe(
    Effect.mapError((cause) => new ZcodeProtocolSpawnError({ command: options.binaryPath, cause })),
  );
  const command = `${options.binaryPath} app-server`;

  const child = yield* spawner
    .spawn(
      ChildProcess.make(spawnCommand.command, spawnCommand.args, {
        cwd: options.cwd,
        ...(options.environment ? { env: options.environment } : {}),
        extendEnv,
        forceKillAfter: ZCODE_APP_SERVER_FORCE_KILL_AFTER,
        shell: spawnCommand.shell,
      }),
    )
    .pipe(
      Effect.provideService(Scope.Scope, scope),
      Effect.mapError((cause) => new ZcodeProtocolSpawnError({ command, cause })),
    );

  const outgoing = yield* Effect.acquireRelease(
    Queue.unbounded<string, Cause.Done<void>>(),
    (queue) => Queue.shutdown(queue),
  );
  const notificationQueue = yield* Effect.acquireRelease(
    Queue.unbounded<ZcodeProtocolNotification>(),
    (queue) => Queue.shutdown(queue),
  );
  const pending = yield* Ref.make(new Map<string, PendingRequest>());
  const nextRequestId = yield* Ref.make(1);
  const serverRequestHandlers = new Map<string, (params: unknown) => Effect.Effect<unknown>>();
  const terminationHandled = yield* Ref.make(false);
  const terminationFailure = yield* Ref.make(Option.none<ZcodeProtocolError>());
  const terminationSignal = yield* Deferred.make<void>();

  // Defaults registered before any client request can trigger them.
  serverRequestHandlers.set("session/requestRuntimePreferences", () =>
    Effect.succeed({
      nativeSearchEnhancementsEnabled: options.nativeSearchEnhancementsEnabled ?? false,
    }),
  );

  const failAllPending = (error: ZcodeProtocolError) =>
    Ref.get(pending).pipe(
      Effect.flatMap((current) =>
        Effect.forEach([...current.values()], ({ deferred }) => Deferred.fail(deferred, error), {
          discard: true,
        }),
      ),
      Effect.andThen(Ref.set(pending, new Map())),
    );

  const handleTermination = (classify: () => Effect.Effect<ZcodeProtocolError>) =>
    Ref.modify(terminationHandled, (handled) => {
      if (handled) {
        return [Effect.void, true] as const;
      }
      return [
        Effect.gen(function* () {
          const error = yield* classify();
          yield* Ref.set(terminationFailure, Option.some(error));
          yield* failAllPending(error);
          yield* Queue.end(outgoing);
          yield* Deferred.succeed(terminationSignal, undefined);
        }),
        true,
      ] as const;
    }).pipe(Effect.flatten);

  const write = (message: Record<string, unknown>) =>
    Effect.gen(function* () {
      const failure = yield* Ref.get(terminationFailure);
      if (Option.isSome(failure)) return yield* failure.value;
      const encoded = yield* encodeJsonString(message).pipe(
        Effect.mapError(
          (cause) =>
            new ZcodeProtocolTransportError({ operation: "encode-outgoing-message", cause }),
        ),
      );
      const accepted = yield* Queue.offer(outgoing, `${encoded}\n`);
      if (!accepted) {
        return yield* new ZcodeProtocolInputStreamEndedError();
      }
    });

  const respond = (requestId: JsonRpcId, result: unknown) => write({ id: requestId, result });

  const respondError = (requestId: JsonRpcId, code: number, message: string) =>
    write({ id: requestId, error: { code, message } });

  const resolvePending = (
    requestId: string,
    handler: (pendingRequest: PendingRequest) => Effect.Effect<void>,
  ) =>
    Ref.modify(pending, (current) => {
      const pendingRequest = current.get(requestId);
      if (!pendingRequest) {
        return [Effect.void, current] as const;
      }
      const next = new Map(current);
      next.delete(requestId);
      return [handler(pendingRequest), next] as const;
    }).pipe(Effect.flatten);

  const handleResponse = (response: ZcodeProtocolResponse) => {
    const requestId = String(response.id);
    const protocolError = response.error;
    if (protocolError !== undefined) {
      return resolvePending(requestId, ({ deferred, method }) =>
        Deferred.fail(
          deferred,
          new ZcodeProtocolRequestError({
            method,
            code: protocolError.code,
            protocolMessage: protocolError.message,
          }),
        ),
      );
    }
    return resolvePending(requestId, ({ deferred }) => Deferred.succeed(deferred, response.result));
  };

  const handleServerRequest = (request: ZcodeProtocolServerRequest) =>
    Effect.gen(function* () {
      const handler = serverRequestHandlers.get(request.method);
      if (!handler) {
        // Graceful "unsupported": the server skips the feature (observed with
        // interaction/requestOfficialMcpAuthHeaders → -32601).
        yield* respondError(request.id, -32601, `Method not found: ${request.method}`);
        return;
      }
      yield* respond(request.id, yield* handler(request.params));
    }).pipe(
      Effect.catchCause((cause) =>
        Effect.logWarning("zcode server-request handler failed", {
          method: request.method,
          cause,
        }),
      ),
    );

  const handleLine = (line: string): Effect.Effect<void> => {
    const trimmed = line.trim();
    if (trimmed.length === 0) {
      return Effect.void;
    }
    return decodeJsonString(line).pipe(
      Effect.matchEffect({
        onFailure: () =>
          // Non-JSON stdout lines (log/banner noise) are skipped, not fatal —
          // the same policy as unroutable messages below: one bad line must
          // not take the client (and every multiplexed session) down.
          Effect.logWarning("zcode app-server sent a non-JSON line", {
            line: trimmed.slice(0, 200),
          }),
        onSuccess: (message) => {
          if (isIncomingRequest(message)) {
            return handleServerRequest(message);
          }
          if (isIncomingNotification(message)) {
            return Queue.offer(notificationQueue, message).pipe(Effect.asVoid);
          }
          if (isIncomingResponse(message)) {
            return handleResponse(message);
          }
          // Unroutable traffic is logged, not fatal: the protocol is young and
          // unknown message kinds must not take sessions down.
          return Effect.logWarning("zcode app-server sent an unroutable message", {
            keys: isObject(message) ? Object.keys(message) : typeof message,
          });
        },
      }),
    );
  };

  yield* child.stdout.pipe(
    Stream.interruptWhen(Deferred.await(terminationSignal)),
    Stream.decodeText(),
    Stream.splitLines,
    Stream.map((line) => line.replace(/\r$/, "")),
    Stream.runForEach(handleLine),
    Effect.matchEffect({
      onFailure: (error) =>
        handleTermination(() =>
          Effect.succeed(
            new ZcodeProtocolTransportError({ operation: "read-process-output", cause: error }),
          ),
        ),
      onSuccess: () =>
        handleTermination(() => Effect.succeed(new ZcodeProtocolInputStreamEndedError())),
    }),
    Effect.forkIn(scope),
  );

  yield* child.exitCode.pipe(
    Effect.flatMap((code) =>
      handleTermination(() =>
        Effect.succeed(
          new ZcodeProtocolProcessExitedError({ code, ...(child.pid ? { pid: child.pid } : {}) }),
        ),
      ),
    ),
    Effect.catchCause(() => Effect.void),
    Effect.forkIn(scope),
  );

  // Keep stderr visible but non-fatal; zcode logs diagnostics there.
  yield* child.stderr.pipe(
    Stream.decodeText(),
    Stream.splitLines,
    Stream.runForEach((line) => {
      const trimmed = line.trim();
      return trimmed.length > 0
        ? Effect.logDebug("zcode app-server stderr", { line: trimmed })
        : Effect.void;
    }),
    Effect.ignore,
    Effect.forkIn(scope),
  );

  yield* Stream.fromQueue(outgoing).pipe(
    Stream.encodeText,
    Stream.run(child.stdin),
    Effect.ignore,
    Effect.forkIn(scope),
  );

  const request = (method: string, params?: unknown) =>
    Effect.gen(function* () {
      const failure = yield* Ref.get(terminationFailure);
      if (Option.isSome(failure)) return yield* failure.value;

      const requestId = yield* Ref.modify(
        nextRequestId,
        (current) => [current, current + 1] as const,
      );
      const deferred = yield* Deferred.make<unknown, ZcodeProtocolError>();
      yield* Ref.update(pending, (current) =>
        new Map(current).set(String(requestId), { deferred, method }),
      );
      yield* write({ id: requestId, method, ...(params !== undefined ? { params } : {}) });
      return yield* Deferred.await(deferred);
    });

  return {
    request,
    notifications: Stream.fromQueue(notificationQueue),
    handleServerRequest: (method, handler) =>
      Effect.sync(() => {
        serverRequestHandlers.set(method, handler);
      }),
    whenTerminated: Deferred.await(terminationSignal),
    close: Effect.gen(function* () {
      yield* handleTermination(() => Effect.succeed(new ZcodeProtocolInputStreamEndedError()));
      yield* Effect.ignore(Scope.close(scope, Exit.void));
    }),
    command,
  } satisfies ZcodeProtocolClientShape;
});
