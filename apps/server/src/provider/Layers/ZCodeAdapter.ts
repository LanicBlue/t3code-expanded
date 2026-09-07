/**
 * ZCodeAdapter — scoped live implementation for the ZCode provider adapter.
 *
 * Owns ONE shared zcode app-server process (via `ZcodeProtocolClient`) for
 * every session of the provider instance — the zcode protocol multiplexes
 * sessions by `sessionId`, which is how the ZCode desktop client uses it.
 * Session notifications are fanned out to per-thread `ZcodeSessionRuntime`
 * instances, whose `ProviderEvent`s are mapped onto the shared
 * `ProviderRuntimeEvent` algebra here.
 *
 * Known protocol gaps (see the module notes in `../zcode/` for details):
 *   - approval/user-input requests are not bridged: zcode `build` mode gates
 *     every file edit behind the `interaction/requestPermission` reverse
 *     request, which this driver answers with -32601 — so until permission
 *     requests are bridged onto T3's approval flow, approval-carrying T3 modes
 *     map to zcode `edit` (see `runtimeModeToZcodeMode`) and
 *     `respondToRequest`/`respondToUserInput` fail;
 *   - attachments cannot be forwarded (`session/send` takes plain text);
 *   - tool-call event payloads have not been observed yet and are TODO.
 *
 * @module ZCodeAdapter
 */
import {
  type ProviderEvent,
  ProviderDriverKind,
  ProviderInstanceId,
  type ProviderRuntimeEvent,
  type ZCodeSettings,
  RuntimeItemId,
  type ThreadTokenUsageSnapshot,
  ThreadId,
  TurnId,
} from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Semaphore from "effect/Semaphore";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import { ChildProcessSpawner } from "effect/unstable/process";

import {
  ProviderAdapterProcessError,
  ProviderAdapterRequestError,
  ProviderAdapterSessionClosedError,
  ProviderAdapterSessionNotFoundError,
  ProviderAdapterValidationError,
  type ProviderAdapterError,
} from "../Errors.ts";
import type { ZCodeAdapterShape } from "../Services/ZCodeAdapter.ts";
import {
  makeZcodeProtocolClient,
  type ZcodeProtocolClientShape,
  type ZcodeProtocolError,
} from "../zcode/ZcodeProtocolClient.ts";
import {
  isZcodeRoutableNotificationMethod,
  makeZcodeSessionRuntime,
  readZcodeResumeCursor,
  type ZcodeSessionRuntimeError,
  type ZcodeSessionRuntimeOptions,
  type ZcodeSessionRuntimeShape,
} from "../zcode/ZcodeSessionRuntime.ts";
import { resolveZcodeLaunchArgs, zcodeLaunchArgv } from "./zcodeLaunchArgs.ts";

const PROVIDER = ProviderDriverKind.make("zcode");

export interface ZCodeAdapterLiveOptions {
  readonly instanceId?: ProviderInstanceId;
  readonly environment?: NodeJS.ProcessEnv;
  /** Test seam replacing the shared protocol client. */
  readonly makeClient?: (
    options: Parameters<typeof makeZcodeProtocolClient>[0],
  ) => Effect.Effect<
    ZcodeProtocolClientShape,
    ZcodeProtocolError,
    ChildProcessSpawner.ChildProcessSpawner | Scope.Scope
  >;
  readonly makeRuntime?: (
    options: ZcodeSessionRuntimeOptions,
    services: { readonly client: ZcodeProtocolClientShape },
  ) => Effect.Effect<ZcodeSessionRuntimeShape, never, Crypto.Crypto>;
}

interface ZCodeAdapterSessionContext {
  readonly threadId: ThreadId;
  readonly runtime: ZcodeSessionRuntimeShape;
  readonly eventFiber: Fiber.Fiber<void, never>;
  stopped: boolean;
}

function mapZcodeRuntimeError(
  threadId: ThreadId,
  method: string,
  error: ZcodeSessionRuntimeError,
): ProviderAdapterError {
  if (
    error._tag === "ZcodeProtocolProcessExitedError" ||
    error._tag === "ZcodeProtocolInputStreamEndedError" ||
    error._tag === "ZcodeProtocolTransportError"
  ) {
    return new ProviderAdapterSessionClosedError({
      provider: PROVIDER,
      threadId,
      cause: error,
    });
  }
  return new ProviderAdapterRequestError({
    provider: PROVIDER,
    method,
    detail: error.message,
    cause: error,
  });
}

function asRecord(value: unknown): Record<string, unknown> | undefined {
  return typeof value === "object" && value !== null
    ? (value as Record<string, unknown>)
    : undefined;
}

function trimText(value: unknown): string | undefined {
  if (typeof value !== "string") return undefined;
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

function readEventStringPayload(event: ProviderEvent): Record<string, unknown> | undefined {
  return asRecord(event.payload);
}

/**
 * `turn.failed`/`turn.completed` error details → an error message. `turn.failed`
 * carries `{error, turnPhase, inputId?}` where `error` is `{message, …}` (or a
 * bare string); `turn.completed` failure payloads may carry the same `error`.
 */
function readZcodeErrorMessage(payload: Record<string, unknown> | undefined): string | undefined {
  if (payload === undefined) return undefined;
  const error = payload.error;
  if (typeof error === "string") return trimText(error);
  const message =
    (typeof error === "object" && error !== null
      ? (error as Record<string, unknown>).message
      : undefined) ?? payload.errorMessage;
  return trimText(message);
}

/**
 * `turn.completed.resultType` → the shared turn-state algebra. The wire enum
 * is `success | cancelled | error_max_turns | error_max_budget |
 * error_during_execution | error_max_tool_calls` (bundle-verified — there is
 * no bare "error"/"failed"/"interrupted"); every `error_*` variant is a
 * failure.
 */
function toTurnState(resultType: string | undefined): "completed" | "failed" | "cancelled" {
  if (resultType !== undefined && resultType.startsWith("error_")) {
    return "failed";
  }
  return resultType === "cancelled" ? "cancelled" : "completed";
}

function normalizeZcodeTokenUsage(
  usage: Record<string, unknown> | undefined,
): ThreadTokenUsageSnapshot | undefined {
  if (usage === undefined) return undefined;
  const count = (value: unknown): number | undefined =>
    typeof value === "number" && Number.isFinite(value) && value >= 0
      ? Math.round(value)
      : undefined;
  const usedTokens = count(usage.totalTokens) ?? count(usage.tokenCount);
  if (usedTokens === undefined || usedTokens <= 0) {
    return undefined;
  }
  return {
    usedTokens,
    ...(count(usage.inputTokens) !== undefined ? { inputTokens: count(usage.inputTokens) } : {}),
    ...(count(usage.outputTokens) !== undefined ? { outputTokens: count(usage.outputTokens) } : {}),
    ...(count(usage.cacheReadTokens) !== undefined
      ? { cachedInputTokens: count(usage.cacheReadTokens) }
      : {}),
    ...(count(usage.reasoningTokens) !== undefined
      ? { reasoningOutputTokens: count(usage.reasoningTokens) }
      : {}),
  };
}

function runtimeEventBase(
  event: ProviderEvent,
  canonicalThreadId: ThreadId,
): Omit<ProviderRuntimeEvent, "type" | "payload"> {
  return {
    eventId: event.id,
    provider: event.provider,
    threadId: canonicalThreadId,
    createdAt: event.createdAt,
    ...(event.turnId ? { turnId: event.turnId } : {}),
    ...(event.itemId ? { itemId: RuntimeItemId.make(event.itemId) } : {}),
    raw: {
      source: "zcode.app-server.notification",
      method: event.method,
      payload: event.payload ?? {},
    },
  };
}

function contentStreamKind(kind: unknown): "assistant_text" | "reasoning_text" | "unknown" {
  switch (kind) {
    case "text_delta":
      return "assistant_text";
    case "reasoning_delta":
    case "thought_delta":
      return "reasoning_text";
    default:
      return "unknown";
  }
}

/**
 * Map one zcode session-runtime `ProviderEvent` onto shared runtime events.
 * Pure so it can be asserted against captured wire traces.
 */
export function mapZcodeEventToRuntimeEvents(
  event: ProviderEvent,
  canonicalThreadId: ThreadId,
): ReadonlyArray<ProviderRuntimeEvent> {
  if (event.kind === "session") {
    switch (event.method) {
      case "session/connecting":
        return [
          {
            ...runtimeEventBase(event, canonicalThreadId),
            type: "session.state.changed",
            payload: {
              state: "starting",
              ...(event.message ? { reason: event.message } : {}),
            },
          },
        ];
      case "session/ready":
        return [
          {
            ...runtimeEventBase(event, canonicalThreadId),
            type: "session.state.changed",
            payload: {
              state: "ready",
              ...(event.message ? { reason: event.message } : {}),
            },
          },
        ];
      case "session/started": {
        const payload = readEventStringPayload(event);
        const sessionId = trimText(payload?.sessionId);
        return [
          {
            ...runtimeEventBase(event, canonicalThreadId),
            type: "session.started",
            payload: {
              ...(event.message ? { message: event.message } : {}),
              ...(payload !== undefined ? { resume: payload } : {}),
            },
          },
          ...(sessionId
            ? [
                {
                  ...runtimeEventBase(event, canonicalThreadId),
                  type: "thread.started" as const,
                  payload: { providerThreadId: sessionId },
                },
              ]
            : []),
        ];
      }
      case "session/closed":
        return [
          {
            ...runtimeEventBase(event, canonicalThreadId),
            type: "session.exited",
            payload: {
              ...(event.message ? { reason: event.message } : {}),
              exitKind: "graceful",
            },
          },
        ];
      default:
        return [];
    }
  }

  const payload = readEventStringPayload(event);
  const wirePayload = asRecord(payload?.payload);

  switch (event.method) {
    case "turn.started":
      return [
        {
          ...runtimeEventBase(event, canonicalThreadId),
          type: "turn.started",
          payload: {},
        },
      ];
    case "model.streaming": {
      const delta = trimText(wirePayload?.delta);
      if (!delta) {
        return [];
      }
      return [
        {
          ...runtimeEventBase(event, canonicalThreadId),
          type: "content.delta",
          payload: {
            streamKind: contentStreamKind(wirePayload?.kind),
            delta,
          },
        },
      ];
    }
    case "turn.completed": {
      const usage = normalizeZcodeTokenUsage(asRecord(wirePayload?.usage));
      const state = toTurnState(trimText(wirePayload?.resultType));
      const errorMessage = state === "failed" ? readZcodeErrorMessage(wirePayload) : undefined;
      return [
        {
          ...runtimeEventBase(event, canonicalThreadId),
          type: "turn.completed",
          payload: { state, ...(errorMessage ? { errorMessage } : {}) },
        },
        ...(usage
          ? [
              {
                ...runtimeEventBase(event, canonicalThreadId),
                type: "thread.token-usage.updated" as const,
                payload: { usage },
              },
            ]
          : []),
      ];
    }
    case "turn.failed": {
      // Failure paths emit `turn.failed` INSTEAD of `turn.completed` — map it
      // onto the same turn-completion event so T3's turn always finishes.
      const errorMessage = readZcodeErrorMessage(wirePayload);
      return [
        {
          ...runtimeEventBase(event, canonicalThreadId),
          type: "turn.completed",
          payload: { state: "failed", ...(errorMessage ? { errorMessage } : {}) },
        },
      ];
    }
    case "session.titleUpdated": {
      const title = trimText(wirePayload?.title);
      if (!title) {
        return [];
      }
      return [
        {
          ...runtimeEventBase(event, canonicalThreadId),
          type: "thread.metadata.updated",
          payload: { name: title },
        },
      ];
    }
    case "state.updated": {
      const patch = asRecord(payload?.patch);
      const status = trimText(patch?.status);
      // Wire status enum: idle | running | waiting | paused | completed |
      // error. "waiting"/"paused" are interactive mid-turn states with no T3
      // bridge (the turn stays running); "error" is already surfaced at the
      // turn level via `turn.failed`/`error_*` resultTypes — emitting a
      // session-level error here would mislabel user-cancelled prompts
      // (reason "prompt_failed"). Both are deliberately ignored.
      let state: "running" | "ready" | undefined;
      if (status === "running") {
        state = "running";
      } else if (status === "idle" || status === "completed") {
        state = "ready";
      }
      if (state === undefined) {
        return [];
      }
      return [
        {
          ...runtimeEventBase(event, canonicalThreadId),
          type: "session.state.changed",
          payload: { state },
        },
      ];
    }
    case "session.updated":
      // Multi-purpose payload (model request details, message counts, …).
      // Nothing maps onto the shared algebra yet; raw is preserved above.
      return [];
    default:
      // Unknown session/event types (tool calls among them — shape not yet
      // observed on the wire) intentionally pass through as nothing but the
      // raw payload; the native event logger still captures them.
      return [];
  }
}

/**
 * Build a ZCode provider adapter bound to a specific `ZCodeSettings` payload.
 * All closures capture that payload — two instances with different
 * `binaryPath`s drive independent app-server processes.
 */
export const makeZcodeAdapter = Effect.fn("makeZcodeAdapter")(function* (
  zcodeConfig: ZCodeSettings,
  options?: ZCodeAdapterLiveOptions,
) {
  const boundInstanceId = options?.instanceId ?? ProviderInstanceId.make("zcode");
  const childProcessSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const crypto = yield* Crypto.Crypto;
  const adapterScope = yield* Scope.Scope;
  const runtimeEventQueue = yield* Queue.unbounded<ProviderRuntimeEvent>();
  const sessions = new Map<ThreadId, ZCodeAdapterSessionContext>();

  const createClient = options?.makeClient ?? makeZcodeProtocolClient;
  // Dedicated scope for the shared app-server process: closing it releases
  // the child (and is idempotent with the client's own `close`). Acquired at
  // construction so the per-operation closures below stay R-free.
  const clientScope = yield* Effect.acquireRelease(Scope.make(), (scope) =>
    Scope.close(scope, Exit.void).pipe(Effect.ignore),
  );
  // The app-server spawns lazily on the first session: constructing the
  // adapter at server boot must not pay for (or fail on) a process no thread
  // has asked for yet. Single-flight so concurrent starts share one process.
  const clientRef = yield* Ref.make<ZcodeProtocolClientShape | null>(null);
  const clientSemaphore = yield* Semaphore.make(1);

  const routeNotifications = Effect.fn("ZCodeAdapter.routeNotifications")(function* (
    client: ZcodeProtocolClientShape,
  ) {
    // One fan-out fiber for the whole process: notifications carry the zcode
    // session id at `params.sessionId`; route to the owning runtime.
    yield* Stream.runForEach(client.notifications, (notification) =>
      Effect.gen(function* () {
        if (!isZcodeRoutableNotificationMethod(notification.method)) {
          return;
        }
        const params = asRecord(notification.params);
        const sessionId = trimText(params?.sessionId);
        if (!sessionId) {
          return;
        }
        for (const session of sessions.values()) {
          if (session.stopped) {
            continue;
          }
          const current = yield* session.runtime.getSession;
          const ownedSessionId = readZcodeResumeCursor(current.resumeCursor)?.sessionId;
          if (ownedSessionId === sessionId) {
            yield* session.runtime.handleNotification(notification);
            return;
          }
        }
      }).pipe(
        Effect.catchCause((cause) =>
          Effect.logWarning("failed to route zcode notification", { cause }),
        ),
      ),
    ).pipe(Effect.forkIn(adapterScope), Effect.asVoid);
  });

  const ensureClient = Effect.fn("ZCodeAdapter.ensureClient")(function* () {
    const existing = yield* Ref.get(clientRef);
    if (existing !== null) {
      return existing;
    }
    return yield* clientSemaphore.withPermit(
      Effect.gen(function* () {
        const raced = yield* Ref.get(clientRef);
        if (raced !== null) {
          return raced;
        }
        const client = yield* createClient({
          binaryPath: zcodeConfig.binaryPath,
          launchArgs: zcodeLaunchArgv(
            resolveZcodeLaunchArgs(zcodeConfig.launchArgs, options?.environment),
          ),
          cwd: process.cwd(),
          ...(options?.environment ? { environment: options.environment } : {}),
        }).pipe(
          Effect.provideService(Scope.Scope, clientScope),
          Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, childProcessSpawner),
        );
        yield* Ref.set(clientRef, client);
        // A dead process must not strand the provider instance for good: once
        // the client terminates (crash, external kill), drop the cached
        // reference so the next ensureClient respawns a fresh app-server.
        // Forked immediately after the Ref.set (and before the interruption
        // window of routeNotifications) so a stored client can never end up
        // without its watcher; if termination already happened, the already-
        // resolved deferred clears the ref as soon as the fiber starts.
        // Mid-flight sessions have already failed their in-flight requests
        // with ProcessExited errors of their own; `closeClient` also resolves
        // `whenTerminated`, and the redundant reset is harmless.
        yield* client.whenTerminated.pipe(
          Effect.andThen(Ref.set(clientRef, null)),
          Effect.forkIn(adapterScope),
        );
        yield* routeNotifications(client);
        return client;
      }),
    );
  });

  const closeClient = Effect.gen(function* () {
    const client = yield* Ref.getAndSet(clientRef, null);
    if (client !== null) {
      yield* client.close.pipe(Effect.ignore);
    }
  });

  const requireSession = Effect.fn("ZCodeAdapter.requireSession")(function* (threadId: ThreadId) {
    const session = sessions.get(threadId);
    if (!session || session.stopped) {
      return yield* new ProviderAdapterSessionNotFoundError({
        provider: PROVIDER,
        threadId,
      });
    }
    return session;
  });

  const startSession: ZCodeAdapterShape["startSession"] = (input) =>
    Effect.gen(function* () {
      if (input.provider !== undefined && input.provider !== PROVIDER) {
        return yield* new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation: "startSession",
          issue: `Expected provider '${PROVIDER}' but received '${input.provider}'.`,
        });
      }

      const existing = sessions.get(input.threadId);
      if (existing && !existing.stopped) {
        yield* stopSessionInternal(existing);
      }

      const createRuntime =
        options?.makeRuntime ??
        ((
          runtimeOptions: ZcodeSessionRuntimeOptions,
          services: { readonly client: ZcodeProtocolClientShape },
        ) => makeZcodeSessionRuntime(runtimeOptions, services));
      const client = yield* ensureClient().pipe(
        Effect.mapError(
          (cause) =>
            new ProviderAdapterProcessError({
              provider: PROVIDER,
              threadId: input.threadId,
              detail: cause.message,
              cause,
            }),
        ),
      );
      const runtime = yield* createRuntime(
        {
          threadId: input.threadId,
          providerInstanceId: boundInstanceId,
          cwd: input.cwd ?? process.cwd(),
          runtimeMode: input.runtimeMode,
          ...(input.modelSelection?.instanceId === boundInstanceId
            ? { model: input.modelSelection.model }
            : {}),
          ...(readZcodeResumeCursor(input.resumeCursor)
            ? { resumeCursor: readZcodeResumeCursor(input.resumeCursor) }
            : {}),
        },
        { client },
      ).pipe(Effect.provideService(Crypto.Crypto, crypto));

      const eventFiber = yield* Stream.runForEach(runtime.events, (event) =>
        Effect.gen(function* () {
          const runtimeEvents = mapZcodeEventToRuntimeEvents(event, event.threadId);
          if (runtimeEvents.length === 0) {
            yield* Effect.logDebug("ignoring unhandled ZCode provider event", {
              method: event.method,
              threadId: event.threadId,
            });
            return;
          }
          yield* Queue.offerAll(runtimeEventQueue, runtimeEvents);
        }),
      ).pipe(Effect.forkIn(adapterScope));

      const started = yield* runtime.start().pipe(
        Effect.mapError((cause) => mapZcodeRuntimeError(input.threadId, "session/start", cause)),
        Effect.onError(() =>
          Effect.all(
            [runtime.close.pipe(Effect.ignore), Fiber.interrupt(eventFiber).pipe(Effect.ignore)],
            { discard: true },
          ),
        ),
      );

      sessions.set(input.threadId, {
        threadId: input.threadId,
        runtime,
        eventFiber,
        stopped: false,
      });

      return started;
    });

  const sendTurn: ZCodeAdapterShape["sendTurn"] = Effect.fn("ZCodeAdapter.sendTurn")(
    function* (input) {
      // `session/send` takes plain text only; attachments cannot be forwarded.
      // Non-image attachments already reach agents through the path lines
      // ProviderService adds to the prompt.
      const session = yield* requireSession(input.threadId);
      return yield* session.runtime
        .sendTurn({
          ...(input.input !== undefined ? { input: input.input } : {}),
          ...(input.modelSelection?.instanceId === boundInstanceId
            ? { model: input.modelSelection.model }
            : {}),
        })
        .pipe(
          Effect.mapError((cause) => mapZcodeRuntimeError(input.threadId, "session/send", cause)),
        );
    },
  );

  const interruptTurn: ZCodeAdapterShape["interruptTurn"] = (threadId) =>
    requireSession(threadId).pipe(
      Effect.flatMap((session) => session.runtime.interruptTurn()),
      Effect.mapError((cause) =>
        cause._tag === "ProviderAdapterSessionNotFoundError"
          ? cause
          : mapZcodeRuntimeError(threadId, "session/stop", cause),
      ),
    );

  const respondToRequest: ZCodeAdapterShape["respondToRequest"] = () =>
    Effect.fail(
      new ProviderAdapterRequestError({
        provider: PROVIDER,
        method: "respondToRequest",
        detail: "ZCode approvals are not supported by this adapter.",
      }),
    );

  const respondToUserInput: ZCodeAdapterShape["respondToUserInput"] = () =>
    Effect.fail(
      new ProviderAdapterRequestError({
        provider: PROVIDER,
        method: "respondToUserInput",
        detail: "ZCode user input requests are not supported by this adapter.",
      }),
    );

  const stopSessionInternal = Effect.fn("ZCodeAdapter.stopSessionInternal")(function* (
    session: ZCodeAdapterSessionContext,
  ) {
    if (session.stopped) {
      return;
    }
    session.stopped = true;
    sessions.delete(session.threadId);
    yield* session.runtime.close.pipe(Effect.ignore);
    yield* Fiber.interrupt(session.eventFiber).pipe(Effect.ignore);
  });

  const stopSession: ZCodeAdapterShape["stopSession"] = (threadId) =>
    Effect.gen(function* () {
      const session = sessions.get(threadId);
      if (!session) {
        return;
      }
      yield* stopSessionInternal(session);
    });

  const readThread: ZCodeAdapterShape["readThread"] = (threadId) =>
    requireSession(threadId).pipe(
      Effect.flatMap((session) =>
        session.runtime.getSession.pipe(
          Effect.flatMap((providerSession) => {
            const sessionId = readZcodeResumeCursor(providerSession.resumeCursor)?.sessionId;
            if (!sessionId) {
              return Effect.succeed({ threadId, turns: [] });
            }
            // Coarse snapshot: one pseudo-turn per zcode message. T3 keeps its
            // own canonical history; this is only for thread rehydration.
            return Effect.flatMap(ensureClient(), (client) =>
              client.request("session/messages", { sessionId }).pipe(
                Effect.map((result) => {
                  const messages = asRecord(result)?.messages;
                  const turns = (Array.isArray(messages) ? messages : []).map((message, index) => {
                    const id = trimText(asRecord(message)?.messageId) ?? `message-${index}`;
                    return { id: TurnId.make(id), items: [message] };
                  });
                  return { threadId, turns };
                }),
              ),
            );
          }),
        ),
      ),
      Effect.mapError((cause) =>
        cause._tag === "ProviderAdapterSessionNotFoundError"
          ? cause
          : mapZcodeRuntimeError(threadId, "session/messages", cause),
      ),
    );

  const rollbackThread: ZCodeAdapterShape["rollbackThread"] = () =>
    Effect.fail(
      new ProviderAdapterValidationError({
        provider: PROVIDER,
        operation: "rollbackThread",
        issue: "ZCode does not support conversation rollback.",
      }),
    );

  const listSessions: ZCodeAdapterShape["listSessions"] = () =>
    Effect.forEach(
      Array.from(sessions.values()).filter((session) => !session.stopped),
      (session) => session.runtime.getSession,
      { concurrency: 1 },
    );

  const hasSession: ZCodeAdapterShape["hasSession"] = (threadId) =>
    Effect.succeed(Boolean(sessions.get(threadId) && !sessions.get(threadId)?.stopped));

  const stopAll: ZCodeAdapterShape["stopAll"] = () =>
    Effect.forEach(Array.from(sessions.values()), stopSessionInternal, {
      concurrency: 1,
      discard: true,
    }).pipe(Effect.asVoid);

  yield* Effect.acquireRelease(Effect.void, () =>
    stopAll().pipe(
      Effect.andThen(closeClient),
      Effect.andThen(Queue.shutdown(runtimeEventQueue)),
      Effect.ignore,
    ),
  );

  return {
    provider: PROVIDER,
    capabilities: {
      sessionModelSwitch: "in-session",
      supportsConversationRollback: false,
    },
    startSession,
    sendTurn,
    interruptTurn,
    respondToRequest,
    respondToUserInput,
    stopSession,
    listSessions,
    hasSession,
    readThread,
    rollbackThread,
    stopAll,
    get streamEvents() {
      return Stream.fromQueue(runtimeEventQueue);
    },
  } satisfies ZCodeAdapterShape;
});
