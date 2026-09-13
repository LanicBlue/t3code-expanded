/**
 * ZcodeSessionRuntime — one T3 thread's session state over the shared zcode
 * app-server protocol client.
 *
 * Unlike `CodexSessionRuntime`, this runtime owns NO child process: the zcode
 * protocol multiplexes many sessions over one app-server process (keyed by
 * `sessionId`), so the adapter (`Layers/ZCodeAdapter`) owns the single
 * `ZcodeProtocolClient` and fans notifications out to per-session runtimes via
 * `handleNotification`. If session interference ever shows up on the wire, the
 * escape hatch is one process per session — construct one client per runtime
 * and keep the rest of this file unchanged.
 *
 * Protocol facts this runtime relies on (probe-verified against zcode 0.16.5):
 *   - `session/create` `{workspace:{workspacePath, workspaceKey}, mode?}` →
 *     full snapshot; `mode` is accepted (reflected in `settings.mode.current`).
 *   - `session/resume` `{sessionId}` → full snapshot; unknown session →
 *     request error -32004 "Session not found: …".
 *   - `session/subscribe` `{sessionId, deliveryKind:"desktop-continuous"}`.
 *   - `session/send` `{sessionId, content}` → `{accepted, sessionId,
 *     stateRevision}` — the provider turn id arrives later on the
 *     `turn.started` notification, so T3 turn ids are synthesized here (same
 *     approach as the Claude adapter).
 *   - `session/stop` `{sessionId}` cancels the running turn (emits
 *     `turn.completed` with `resultType:"cancelled"`).
 *   - Turn failures arrive as a dedicated `turn.failed` event (payload
 *     `{error, turnPhase, inputId?}`) — NO `turn.completed` follows on that
 *     path. `turn.completed.resultType` is one of `success | cancelled |
 *     error_max_turns | error_max_budget | error_during_execution |
 *     error_max_tool_calls`; every `error_*` variant is a failure.
 *   - `session/setMode` `{sessionId, mode: plan|build|edit|yolo|auto}`.
 *   - `session/setModel` `{sessionId, model:{providerId, modelId}}`.
 *   - `session/close` `{sessionId}` → `{closed}`.
 *
 * @module provider/zcode/ZcodeSessionRuntime
 */
import {
  EventId,
  ProviderDriverKind,
  type ProviderEvent,
  type ProviderInstanceId,
  type ProviderSession,
  type ProviderTurnStartResult,
  RuntimeMode,
  ThreadId,
  TurnId,
} from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import {
  isZcodeModelUnavailableError,
  isZcodeSessionNotFoundError,
  readZcodeSessionSnapshot,
  type ZcodeProtocolClientShape,
  type ZcodeProtocolError,
  type ZcodeProtocolNotification,
  type ZcodeSessionSnapshot,
  zcodeModelRefToSlug,
  zcodeSlugToModelRef,
} from "./ZcodeProtocolClient.ts";

const PROVIDER = ProviderDriverKind.make("zcode");

/** Opaque `ProviderSession.resumeCursor` payload for zcode sessions. */
export const ZcodeResumeCursorSchema = Schema.Struct({
  sessionId: Schema.String,
});
export type ZcodeResumeCursor = typeof ZcodeResumeCursorSchema.Type;
const isZcodeResumeCursor = Schema.is(ZcodeResumeCursorSchema);

export function readZcodeResumeCursor(
  resumeCursor: ProviderSession["resumeCursor"],
): ZcodeResumeCursor | undefined {
  return isZcodeResumeCursor(resumeCursor) ? resumeCursor : undefined;
}

export class ZcodeSessionIdMissingError extends Schema.TaggedError<ZcodeSessionIdMissingError>()(
  "ZcodeSessionIdMissingError",
  {
    threadId: Schema.String,
  },
) {
  override get message(): string {
    return `zcode session is missing a provider session id for ${this.threadId}`;
  }
}

export class ZcodeUnexpectedPayloadError extends Schema.TaggedError<ZcodeUnexpectedPayloadError>()(
  "ZcodeUnexpectedPayloadError",
  {
    method: Schema.String,
  },
) {
  override get message(): string {
    return `zcode app-server returned an unexpected payload for ${this.method}.`;
  }
}

export class ZcodeSessionIdentifierError extends Schema.TaggedError<ZcodeSessionIdentifierError>()(
  "ZcodeSessionIdentifierError",
  {
    purpose: Schema.String,
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return `Failed to generate a ZCode identifier for ${this.purpose}.`;
  }
}

export type ZcodeSessionRuntimeError =
  | ZcodeProtocolError
  | ZcodeSessionIdMissingError
  | ZcodeUnexpectedPayloadError
  | ZcodeSessionIdentifierError;

export type ZcodeSessionMode = "plan" | "build" | "edit" | "yolo" | "auto";

/**
 * T3 RuntimeMode → zcode permission mode.
 *
 * TEMPORARY COMPROMISE (approval bridge TODO): zcode `build` asks the client
 * to approve every file edit via the `interaction/requestPermission` reverse
 * request; this driver answers unknown reverse requests with -32601, so in
 * `build` the agent could never modify a file. Until permission requests are
 * bridged onto T3's approval flow, every approval-carrying T3 mode maps to
 * `edit` (auto-accept edits). `full-access` keeps `yolo`. zcode's own `auto`
 * mode is unused — its semantics are unverified.
 */
export function runtimeModeToZcodeMode(input: RuntimeMode): ZcodeSessionMode {
  switch (input) {
    case "approval-required":
    case "auto-accept-edits":
    case "auto":
      return "edit";
    case "full-access":
    default:
      return "yolo";
  }
}

// ── Notification payload guards (loose) ────────────────────────────────

const ZcodeSessionEventParamsSchema = Schema.Struct({
  type: Schema.String,
  payload: Schema.optional(Schema.Unknown),
  sessionId: Schema.String,
  turnId: Schema.optional(Schema.String),
  seq: Schema.optional(Schema.Number),
  eventId: Schema.optional(Schema.String),
  timestamp: Schema.optional(Schema.Number),
});
const ZcodeStateUpdatedParamsSchema = Schema.Struct({
  patch: Schema.Struct({
    status: Schema.optional(Schema.String),
    mode: Schema.optional(Schema.Unknown),
    model: Schema.optional(Schema.Unknown),
  }),
  reason: Schema.optional(Schema.String),
  revision: Schema.optional(Schema.Number),
  sessionId: Schema.String,
});

export type ZcodeSessionEventParams = typeof ZcodeSessionEventParamsSchema.Type;
export type ZcodeStateUpdatedParams = typeof ZcodeStateUpdatedParamsSchema.Type;

const isZcodeSessionEventParams = Schema.is(ZcodeSessionEventParamsSchema);
const isZcodeStateUpdatedParams = Schema.is(ZcodeStateUpdatedParamsSchema);

export function readZcodeSessionEventParams(
  notification: ZcodeProtocolNotification,
): ZcodeSessionEventParams | undefined {
  if (notification.method !== "session/event") {
    return undefined;
  }
  return isZcodeSessionEventParams(notification.params) ? notification.params : undefined;
}

export function readZcodeStateUpdatedParams(
  notification: ZcodeProtocolNotification,
): ZcodeStateUpdatedParams | undefined {
  if (notification.method !== "state.updated") {
    return undefined;
  }
  return isZcodeStateUpdatedParams(notification.params) ? notification.params : undefined;
}

/** All other notification methods (telemetry, computer-use, …) are dropped. */
export function isZcodeRoutableNotificationMethod(method: string): boolean {
  return method === "session/event" || method === "state.updated";
}

// ── Runtime ────────────────────────────────────────────────────────────

export interface ZcodeSessionRuntimeOptions {
  readonly threadId: ThreadId;
  readonly providerInstanceId?: ProviderInstanceId;
  readonly cwd: string;
  readonly runtimeMode: RuntimeMode;
  /** Composite `<providerId>/<modelId>` slug. */
  readonly model?: string | undefined;
  /** Reasoning level (low/high/max); applied via `session/setThoughtLevel`. */
  readonly thoughtLevel?: string | undefined;
  readonly resumeCursor?: ZcodeResumeCursor | undefined;
}

export interface ZcodeSessionRuntimeServices {
  readonly client: ZcodeProtocolClientShape;
  /** Injectable for tests; defaults to the ambient `Crypto` service. */
  readonly crypto?: Crypto.Crypto | undefined;
}

export interface ZcodeSessionRuntimeSendTurnInput {
  readonly input?: string;
  /** Composite slug; applied via `session/setModel` before the send. */
  readonly model?: string;
  /** Reasoning level (low/high/max); applied via `session/setThoughtLevel`. */
  readonly thoughtLevel?: string;
}

export interface ZcodeSessionRuntimeShape {
  /** Create (or resume) + subscribe the provider session. */
  readonly start: () => Effect.Effect<ProviderSession, ZcodeSessionRuntimeError>;
  readonly getSession: Effect.Effect<ProviderSession>;
  readonly sendTurn: (
    input: ZcodeSessionRuntimeSendTurnInput,
  ) => Effect.Effect<ProviderTurnStartResult, ZcodeSessionRuntimeError>;
  readonly interruptTurn: () => Effect.Effect<void, ZcodeSessionRuntimeError>;
  readonly setModel: (model: string) => Effect.Effect<void, ZcodeSessionRuntimeError>;
  readonly setMode: (mode: ZcodeSessionMode) => Effect.Effect<void, ZcodeSessionRuntimeError>;
  readonly close: Effect.Effect<void>;
  /**
   * Route one notification already filtered to this runtime's session id.
   * Session-state changes are applied here; everything is re-emitted on
   * `events` for the adapter to map onto runtime events.
   */
  readonly handleNotification: (
    notification: ZcodeProtocolNotification,
  ) => Effect.Effect<void, ZcodeSessionRuntimeError>;
  readonly events: Stream.Stream<ProviderEvent>;
}

export const makeZcodeSessionRuntime = (
  options: ZcodeSessionRuntimeOptions,
  services: ZcodeSessionRuntimeServices,
): Effect.Effect<ZcodeSessionRuntimeShape, never, Crypto.Crypto> =>
  Effect.gen(function* () {
    const crypto = services.crypto ?? (yield* Crypto.Crypto);
    const client = services.client;
    const events = yield* Queue.unbounded<ProviderEvent>();
    const sessionCreatedAt = DateTime.formatIso(yield* DateTime.now);
    const initialSession: ProviderSession = {
      provider: PROVIDER,
      ...(options.providerInstanceId ? { providerInstanceId: options.providerInstanceId } : {}),
      status: "connecting",
      runtimeMode: options.runtimeMode,
      cwd: options.cwd,
      ...(options.model ? { model: options.model } : {}),
      threadId: options.threadId,
      ...(options.resumeCursor !== undefined ? { resumeCursor: options.resumeCursor } : {}),
      createdAt: sessionCreatedAt,
      updatedAt: sessionCreatedAt,
    };
    const sessionRef = yield* Ref.make<ProviderSession>(initialSession);
    const closedRef = yield* Ref.make(false);

    const nowIso = Effect.map(DateTime.now, DateTime.formatIso);
    const randomUUIDv4 = crypto.randomUUIDv4.pipe(
      Effect.mapError(
        (cause) => new ZcodeSessionIdentifierError({ purpose: "provider-event", cause }),
      ),
    );
    const emitEvent = (event: Omit<ProviderEvent, "id" | "provider" | "createdAt">) =>
      Effect.flatMap(Effect.all([randomUUIDv4, nowIso], { concurrency: 2 }), ([id, createdAt]) =>
        Queue.offer(events, {
          id: EventId.make(id),
          provider: PROVIDER,
          ...(options.providerInstanceId ? { providerInstanceId: options.providerInstanceId } : {}),
          createdAt,
          ...event,
        } satisfies ProviderEvent).pipe(Effect.asVoid),
      );

    const emitSessionEvent = (method: string, message: string) =>
      emitEvent({
        kind: "session",
        threadId: options.threadId,
        method,
        message,
      });

    const updateSession = (
      updates: Partial<ProviderSession> | ((session: ProviderSession) => Partial<ProviderSession>),
    ) =>
      Effect.flatMap(nowIso, (updatedAt) =>
        Ref.update(sessionRef, (session) => ({
          ...session,
          ...(typeof updates === "function" ? updates(session) : updates),
          updatedAt,
        })),
      );

    const readProviderSessionId = Effect.map(
      Ref.get(sessionRef),
      (session) => readZcodeResumeCursor(session.resumeCursor)?.sessionId,
    );

    const requireProviderSessionId = readProviderSessionId.pipe(
      Effect.flatMap((sessionId) =>
        sessionId === undefined
          ? Effect.fail(new ZcodeSessionIdMissingError({ threadId: options.threadId }))
          : Effect.succeed(sessionId),
      ),
    );

    const applySnapshot = (snapshot: ZcodeSessionSnapshot) =>
      updateSession((session) => {
        // A resumed session may legitimately be mid-turn: forcing "ready"
        // here would mislabel it and drop the active turn id, so preserve a
        // running session's state.
        const running = snapshot.session.status === "running";
        return {
          status: running ? "running" : "ready",
          activeTurnId: running ? session.activeTurnId : undefined,
          resumeCursor: { sessionId: snapshot.session.sessionId },
          ...(snapshot.session.model ? { model: zcodeModelRefToSlug(snapshot.session.model) } : {}),
        };
      });

    // ── start ──────────────────────────────────────────────────────────

    const start = Effect.fn("ZcodeSessionRuntime.start")(function* () {
      yield* emitSessionEvent("session/connecting", "Starting ZCode app-server session.");

      const requestedModel = options.model ? zcodeSlugToModelRef(options.model) : undefined;
      const createParams = {
        workspace: { workspacePath: options.cwd, workspaceKey: options.cwd },
        mode: runtimeModeToZcodeMode(options.runtimeMode),
      };

      const resumeSessionId = options.resumeCursor?.sessionId;
      const resumed: ZcodeSessionSnapshot | undefined = resumeSessionId
        ? yield* client.request("session/resume", { sessionId: resumeSessionId }).pipe(
            Effect.map(readZcodeSessionSnapshot),
            Effect.flatMap((snapshot) =>
              snapshot !== undefined
                ? Effect.succeed(snapshot)
                : Effect.fail(new ZcodeUnexpectedPayloadError({ method: "session/resume" })),
            ),
            // The persisted session may be gone from the shared store (zcode
            // GC, app data reset). Resume semantics beyond a plain
            // session/resume are unverified, so the fallback is a FRESH
            // session — replaying T3 history as a first prompt is a
            // deliberate non-goal here.
            Effect.catchIf(isZcodeSessionNotFoundError, (error) =>
              Effect.logWarning("zcode session resume fell back to fresh create", {
                threadId: options.threadId,
                resumeSessionId,
                cause: error,
              }).pipe(Effect.as(undefined)),
            ),
          )
        : undefined;

      let snapshot = resumed;
      if (snapshot === undefined) {
        const created = yield* client.request("session/create", createParams);
        snapshot = readZcodeSessionSnapshot(created);
        if (snapshot === undefined) {
          return yield* new ZcodeUnexpectedPayloadError({ method: "session/create" });
        }
      }

      const sessionId = snapshot.session.sessionId;

      if (resumed !== undefined) {
        // `session/resume` takes no mode argument, so a resumed session keeps
        // whatever mode it was created with — but T3 mode switches restart the
        // thread with a persisted cursor, so reconcile explicitly.
        // `settings.mode.current` is authoritative; the bare `session.mode`
        // field is unreliable (probes saw it stuck on "build") — when absent,
        // setMode is issued unconditionally, which is idempotent. The setMode
        // response is ignored: it cannot change the sessionId or model, and
        // swapping in its snapshot could drop status fields the resume
        // snapshot carried (e.g. a still-running turn).
        const desiredMode = runtimeModeToZcodeMode(options.runtimeMode);
        if (resumed.settings?.mode?.current !== desiredMode) {
          yield* client.request("session/setMode", { sessionId, mode: desiredMode });
        }
      }

      // Snapshot BEFORE subscribing: the resume cursor this stamps is what the
      // adapter's notification fan-out routes by, so applying it first closes
      // the window where events delivered right after `session/subscribe`
      // would find no owning session and be dropped.
      yield* applySnapshot(snapshot);
      yield* client.request("session/subscribe", {
        sessionId,
        deliveryKind: "desktop-continuous",
      });

      if (requestedModel !== undefined) {
        // Model preference is best-effort: a stale slug from an old catalog
        // must not fail the session start.
        yield* client
          .request("session/setModel", { sessionId, model: requestedModel })
          .pipe(Effect.catch(() => Effect.void));
      }
      if (options.thoughtLevel !== undefined) {
        // Same best-effort policy as the model above.
        yield* client
          .request("session/setThoughtLevel", {
            sessionId,
            thoughtLevel: options.thoughtLevel,
          })
          .pipe(Effect.catch(() => Effect.void));
      }

      yield* emitSessionEvent("session/ready", "ZCode app-server session ready.");
      yield* emitEvent({
        kind: "session",
        threadId: options.threadId,
        method: "session/started",
        payload: { sessionId, resumed: resumed !== undefined },
      });
      return yield* Ref.get(sessionRef);
    });

    // ── notification routing ───────────────────────────────────────────

    const handleSessionEvent = (params: ZcodeSessionEventParams) =>
      Effect.gen(function* () {
        // Terminal cases below clear activeTurnId; snapshot it BEFORE the
        // state mutation so the emitted event keeps its turn attribution.
        const before = yield* Ref.get(sessionRef);
        switch (params.type) {
          case "turn.started": {
            yield* updateSession((session) => ({
              status: "running",
              // Canonical T3 turn id when one is active (sendTurn stamped
              // it); otherwise adopt the wire turn id (promptless or
              // desk-started turn).
              activeTurnId:
                session.activeTurnId ??
                (params.turnId !== undefined ? TurnId.make(params.turnId) : undefined),
            }));
            break;
          }
          case "turn.completed": {
            yield* updateSession({
              status: "ready",
              activeTurnId: undefined,
            });
            break;
          }
          case "turn.failed": {
            // Failure paths do NOT emit `turn.completed` — without this case
            // the session would stay "running" forever (stale activeTurnId)
            // and T3's turn would never finish.
            yield* updateSession({
              status: "ready",
              activeTurnId: undefined,
            });
            break;
          }
          default:
            // Payload-only events (model.streaming, session.updated,
            // session.titleUpdated, …) carry no session state. Tool-call
            // event shapes are TODO — none observed on the wire yet.
            break;
        }

        const session = yield* Ref.get(sessionRef);
        // After a terminal event the live activeTurnId is already cleared —
        // fall back to the pre-mutation snapshot, then the wire turn id.
        const turnId =
          session.activeTurnId ??
          before.activeTurnId ??
          (params.turnId !== undefined ? TurnId.make(params.turnId) : undefined);
        yield* emitEvent({
          kind: "notification",
          threadId: options.threadId,
          ...(turnId ? { turnId } : {}),
          method: params.type,
          payload: params,
        });
      });

    const handleStateUpdated = (params: ZcodeStateUpdatedParams) =>
      Effect.gen(function* () {
        // Wire status enum: idle | running | waiting | paused | completed |
        // error (bundle-verified).
        switch (params.patch.status) {
          case "running": {
            yield* updateSession({ status: "running" });
            break;
          }
          case "idle":
          case "completed": {
            yield* updateSession((session) =>
              session.status === "running" ? { status: "ready" } : {},
            );
            break;
          }
          case "error": {
            // Fold into the turn-failure semantics of `turn.failed`: on
            // terminal errors no turn.completed/failed may follow, so clear
            // the active turn or the session sticks in "running" forever.
            // The session itself stays usable for the next prompt (a
            // user-cancelled turn also lands here with reason
            // "prompt_failed").
            yield* updateSession((session) =>
              session.status === "running" || session.activeTurnId !== undefined
                ? { status: "ready", activeTurnId: undefined }
                : {},
            );
            break;
          }
          default:
            // "waiting"/"paused" are interactive mid-turn states (user input /
            // permission gates) with no T3 bridge yet (see the mode-mapping
            // note on runtimeModeToZcodeMode) — the turn legitimately stays
            // running, so they are deliberately ignored.
            break;
        }
        yield* emitEvent({
          kind: "notification",
          threadId: options.threadId,
          method: "state.updated",
          payload: params,
        });
      });

    const handleNotification = (notification: ZcodeProtocolNotification) =>
      Effect.gen(function* () {
        if (yield* Ref.get(closedRef)) {
          return;
        }
        const sessionEvent = readZcodeSessionEventParams(notification);
        if (sessionEvent !== undefined) {
          yield* handleSessionEvent(sessionEvent);
          return;
        }
        const stateUpdated = readZcodeStateUpdatedParams(notification);
        if (stateUpdated !== undefined) {
          yield* handleStateUpdated(stateUpdated);
        }
      });

    // ── operations ─────────────────────────────────────────────────────

    /**
     * Re-materialize the provider session: fresh `session/create` + subscribe
     * + model/thought-level re-application. Heals the -32031 restore gate —
     * a historical session freezes the runtime-model config it was created
     * with, and once the catalog rotates (app update, provider revision bump)
     * resume can no longer restore it. There is no headless RPC that rebuilds
     * that envelope (`workspace/readState` strips provider credentials, so
     * `session/updateRuntimeModelConfig` cannot be fed), and `session/fork`
     * inherits the frozen config. A fresh session is the sanctioned recovery:
     * the stale transcript stays in the zcode store and T3's own thread
     * history is untouched — only the in-session context resets.
     */
    const rematerializeSession = Effect.fn("ZcodeSessionRuntime.rematerializeSession")(function* (
      requestedModel: string | undefined,
      thoughtLevel: string | undefined,
    ) {
      const modelRef =
        requestedModel !== undefined ? zcodeSlugToModelRef(requestedModel) : undefined;
      const created = yield* client.request("session/create", {
        workspace: { workspacePath: options.cwd, workspaceKey: options.cwd },
        mode: runtimeModeToZcodeMode(options.runtimeMode),
      });
      const snapshot = readZcodeSessionSnapshot(created);
      if (snapshot === undefined) {
        return yield* new ZcodeUnexpectedPayloadError({ method: "session/create" });
      }
      const freshId = snapshot.session.sessionId;
      yield* applySnapshot(snapshot);
      yield* client.request("session/subscribe", {
        sessionId: freshId,
        deliveryKind: "desktop-continuous",
      });
      if (modelRef !== undefined) {
        // Best-effort, same policy as start(): must not fail the recovery.
        yield* client
          .request("session/setModel", { sessionId: freshId, model: modelRef })
          .pipe(Effect.catch(() => Effect.void));
      }
      if (thoughtLevel !== undefined && thoughtLevel.trim().length > 0) {
        yield* client
          .request("session/setThoughtLevel", { sessionId: freshId, thoughtLevel })
          .pipe(Effect.catch(() => Effect.void));
      }
      yield* emitEvent({
        kind: "session",
        threadId: options.threadId,
        method: "session/started",
        payload: { sessionId: freshId, resumed: false },
      });
      return freshId;
    });

    const sendTurn = Effect.fn("ZcodeSessionRuntime.sendTurn")(function* (
      input: ZcodeSessionRuntimeSendTurnInput,
    ) {
      const sessionId = yield* requireProviderSessionId;
      let appliedModel: string | undefined;
      if (input.model !== undefined) {
        const ref = zcodeSlugToModelRef(input.model);
        if (ref !== undefined) {
          // Best-effort, same policy as start(): a stale slug from an old
          // catalog (server answers -32603 Unsupported model) must not fail
          // the send — continue with the session's current model instead.
          appliedModel = yield* client.request("session/setModel", { sessionId, model: ref }).pipe(
            Effect.as(input.model),
            Effect.catch((error) =>
              Effect.logWarning("zcode session/setModel failed; sending with the current model", {
                sessionId,
                model: input.model,
                cause: error,
              }).pipe(Effect.as(undefined)),
            ),
          );
        }
      }
      if (input.thoughtLevel !== undefined && input.thoughtLevel.trim().length > 0) {
        // Best-effort, same policy as the model: an unknown level must not
        // fail the send — continue with the session's current level.
        yield* client
          .request("session/setThoughtLevel", {
            sessionId,
            thoughtLevel: input.thoughtLevel,
          })
          .pipe(
            Effect.catch((error) =>
              Effect.logWarning(
                "zcode session/setThoughtLevel failed; sending with the current level",
                {
                  sessionId,
                  thoughtLevel: input.thoughtLevel,
                  cause: error,
                },
              ),
            ),
          );
      }
      // `session/send` returns only `{accepted, stateRevision}`; the provider
      // turn id arrives asynchronously on `turn.started`, so we mint the
      // canonical T3 turn id here (Claude adapter pattern). Wire turn ids are
      // folded onto the canonical id in `handleSessionEvent`.
      const turnId = TurnId.make(yield* randomUUIDv4);
      const previous = yield* Ref.get(sessionRef);
      // Stamp the turn BEFORE `session/send`: turn.started (and streaming)
      // can arrive before the send response, and without the canonical id in
      // place those notifications would be mis-attributed or dropped.
      yield* updateSession({
        status: "running",
        activeTurnId: turnId,
        ...(appliedModel ? { model: appliedModel } : {}),
      });
      const sendTurnRequest = () =>
        // Re-read the session id per attempt: the -32031 recovery below swaps
        // in a fresh session, and the retry must target it.
        Effect.flatMap(requireProviderSessionId, (sessionId) =>
          client.request("session/send", {
            sessionId,
            ...(input.input !== undefined ? { content: input.input } : {}),
          }),
        );
      yield* sendTurnRequest().pipe(
        // A historical task freezes the runtime-model config it was created
        // with; after the workspace catalog rotates, restore refuses to
        // continue and every send reports -32031. Neither session/setModel
        // nor session/fork clears it, and the runtime-model envelope cannot
        // be rebuilt headless — so re-materialize the session (history stays
        // in the zcode store; the turn replays on the fresh session) and
        // retry once. Any failure keeps the original error.
        Effect.catchIf(isZcodeModelUnavailableError, (error) =>
          rematerializeSession(input.model, input.thoughtLevel).pipe(
            // applySnapshot reset the running stamp; re-stamp the canonical
            // turn id so turn.started folds onto it (see B1-regression).
            Effect.tap(() =>
              updateSession({
                status: "running",
                activeTurnId: turnId,
                ...(appliedModel ? { model: appliedModel } : {}),
              }),
            ),
            Effect.andThen(sendTurnRequest()),
            Effect.catch(() => Effect.fail(error)),
          ),
        ),
        // The send itself is NOT best-effort: if it fails (after any heal
        // retry), revert the running state stamped above so the session is
        // not stuck mid-turn.
        Effect.catch((error) =>
          updateSession({
            status: previous.status,
            activeTurnId: previous.activeTurnId,
          }).pipe(Effect.andThen(Effect.fail(error))),
        ),
      );
      return {
        threadId: options.threadId,
        turnId,
        resumeCursor: { sessionId },
      } satisfies ProviderTurnStartResult;
    });

    const interruptTurn = Effect.fn("ZcodeSessionRuntime.interruptTurn")(function* () {
      const sessionId = yield* readProviderSessionId;
      if (sessionId === undefined) {
        return;
      }
      // `session/stop` cancels the session's running turn; the cancellation
      // surfaces as turn.completed with resultType "cancelled".
      yield* client.request("session/stop", { sessionId });
    });

    const setModel = Effect.fn("ZcodeSessionRuntime.setModel")(function* (model: string) {
      const sessionId = yield* requireProviderSessionId;
      const ref = zcodeSlugToModelRef(model);
      if (ref === undefined) {
        return;
      }
      const result = yield* client.request("session/setModel", { sessionId, model: ref });
      const snapshot = readZcodeSessionSnapshot(result);
      if (snapshot) {
        yield* applySnapshot(snapshot);
      }
      yield* updateSession({ model });
    });

    const setMode = Effect.fn("ZcodeSessionRuntime.setMode")(function* (mode: ZcodeSessionMode) {
      const sessionId = yield* requireProviderSessionId;
      const result = yield* client.request("session/setMode", { sessionId, mode });
      const snapshot = readZcodeSessionSnapshot(result);
      if (snapshot) {
        yield* applySnapshot(snapshot);
      }
    });

    const close = Effect.gen(function* () {
      const alreadyClosed = yield* Ref.getAndSet(closedRef, true);
      if (alreadyClosed) {
        return;
      }
      const sessionId = yield* readProviderSessionId;
      if (sessionId !== undefined) {
        yield* client.request("session/close", { sessionId }).pipe(Effect.ignore);
      }
      yield* updateSession({ status: "closed", activeTurnId: undefined });
      yield* emitSessionEvent("session/closed", "Session stopped").pipe(
        Effect.catch(() => Effect.logError("Failed to emit ZCode session closed event.")),
      );
      yield* Queue.shutdown(events);
    });

    return {
      start,
      getSession: Ref.get(sessionRef),
      sendTurn,
      interruptTurn,
      setModel,
      setMode,
      close,
      handleNotification,
      events: Stream.fromQueue(events),
    } satisfies ZcodeSessionRuntimeShape;
  });
