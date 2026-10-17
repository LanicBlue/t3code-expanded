/**
 * ZcodeAdapterV2 — orchestration-v2 adapter for the fork's zcode provider.
 *
 * Owns ONE shared zcode app-server process (via `ZcodeProtocolClient`) per
 * provider instance — the zcode protocol multiplexes sessions by `sessionId`,
 * which is how the ZCode desktop client uses it. The process spawns lazily on
 * the first `ensureThread` (constructing the adapter at server boot must not
 * pay for a process no thread asked for), respawns after the shared client
 * terminates, and is released when the driver-create scope closes.
 *
 * Each `openSession` owns one `ZcodeSessionRuntime` (created lazily by
 * `ensureThread`, the only place a native session comes into existence).
 * Server notifications are routed by `params.sessionId` to the owning runtime
 * at adapter level, and each runtime's `ProviderEvent` stream is projected
 * onto `ProviderAdapterV2Event` here.
 *
 * Protocol gaps preserved from the v1 adapter (see `provider/zcode/` module
 * notes): approvals/user-input are not bridged (`respondToRuntimeRequest`
 * fails; approval-carrying T3 modes map to zcode `edit` — see
 * `runtimeModeToZcodeMode` in ZcodeSessionRuntime), attachments cannot be
 * forwarded (`session/send` takes plain text), and there is no rollback/fork
 * RPC. zcode never creates app threads (no subagents), so no
 * `app_thread.created` is ever emitted.
 *
 * @module orchestration-v2/Adapters/ZcodeAdapterV2
 */
import {
  defaultInstanceIdForDriver,
  type ModelSelection,
  type OrchestrationV2ConversationMessage,
  type OrchestrationV2ExecutionNode,
  type OrchestrationV2ProviderCapabilities,
  type OrchestrationV2ProviderFailure,
  type OrchestrationV2ProviderSession,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2ProviderTurn,
  type OrchestrationV2TurnItem,
  ProviderDriverKind,
  type ProviderEvent,
  type ProviderInstanceId,
  type ProviderThreadId,
  type ProviderTurnId,
  type ThreadId,
  type ThreadTokenUsageSnapshot,
  TurnId,
  ZCodeSettings as ZCodeSettingsSchema,
  type ZCodeSettings,
} from "@t3tools/contracts";
import { getModelSelectionStringOptionValue } from "@t3tools/shared/model";
import * as Cause from "effect/Cause";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import { ChildProcessSpawner } from "effect/process";

import { ServerConfig } from "../../config.ts";
import { mergeProviderInstanceEnvironment } from "../../provider/ProviderInstanceEnvironment.ts";
import type {
  ZcodeProtocolClientShape,
  ZcodeProtocolError,
} from "../../provider/zcode/ZcodeProtocolClient.ts";
import { makeZcodeProtocolClient } from "../../provider/zcode/ZcodeProtocolClient.ts";
import {
  isZcodeRoutableNotificationMethod,
  makeZcodeSessionRuntime,
  readZcodeResumeCursor,
  type ZcodeSessionRuntimeOptions,
  type ZcodeSessionRuntimeShape,
} from "../../provider/zcode/ZcodeSessionRuntime.ts";
import { resolveZcodeLaunchArgs, zcodeLaunchArgv } from "../../provider/zcodeLaunchArgs.ts";
import { IdAllocatorV2, type IdAllocatorV2Shape } from "../IdAllocator.ts";
import { makeProviderFailure } from "../ProviderFailure.ts";
import { turnScopedSelectionTransition } from "../ProviderSelectionTransition.ts";
import * as ProviderAdapter from "../ProviderAdapter.ts";
import {
  ProviderAdapterDriverCreateError,
  type ProviderAdapterDriver,
  type ProviderAdapterDriverCreateInput,
} from "../ProviderAdapterDriver.ts";

export const ZCODE_PROVIDER = ProviderDriverKind.make("zcode");
export const ZCODE_DEFAULT_INSTANCE_ID = defaultInstanceIdForDriver(ZCODE_PROVIDER);
const DEFAULT_ZCODE_SETTINGS = Schema.decodeSync(ZCodeSettingsSchema)({});

// ── Capabilities ───────────────────────────────────────────────────────

/**
 * What the zcode app-server protocol actually supports (probe-verified; see
 * the module notes in `provider/zcode/`). Deliberately narrower than most
 * adapters: no native rollback/fork, no approvals bridge, no subagents.
 * Assistant text streams live via `model.streaming` text_deltas accumulated
 * per wire assistantMessageId; `message.upserted` (when delivered) is the
 * authoritative full-text overwrite, and `turn.completed.payload.response`
 * backstops a turn that produced no streamed segment at all.
 */
const ZcodeProviderCapabilitiesV2 = {
  sessions: {
    // One ZcodeSessionRuntime per openSession: the runtime binds exactly one
    // zcode session, so a second provider thread would need a second session.
    supportsMultipleProviderThreadsPerSession: false,
    // `session/setModel` switches the live session's model.
    supportsModelSwitchInSession: true,
    supportsProviderSwitchingViaHandoff: false,
    supportsRuntimeModeSwitchInSession: false,
    pendingRequestsSurviveRestart: false,
  },
  threads: {
    canCreateEmptyThread: true,
    canReadThreadSnapshot: true,
    canRollbackThread: false,
    canForkThread: false,
    canForkFromTurn: false,
    canForkFromSubagentThread: false,
    exposesNativeThreadId: true,
  },
  turns: {
    // `turn.started` carries a wire turn id, but `session/send` returns none —
    // the adapter mints provider turn ids from the run attempt.
    exposesNativeTurnId: false,
    emitsTurnStarted: true,
    emitsTurnCompleted: true,
    supportsInterrupt: true,
    supportsActiveSteering: true,
    supportsSteeringByInterruptRestart: false,
    supportsQueuedMessages: false,
    terminalStatusQuality: "strong",
  },
  streaming: {
    streamsAssistantText: true,
    streamsReasoning: false,
    streamsToolOutput: false,
    streamsPlanText: false,
    emitsMessageCompleted: true,
  },
  tools: {
    exposesToolItemIds: true,
    emitsToolStarted: true,
    emitsToolCompleted: true,
    emitsToolOutput: false,
    supportsMcpTools: true,
    supportsDynamicToolCallbacks: false,
  },
  approvals: {
    // zcode build mode gates edits behind interaction/requestPermission,
    // which the protocol client answers with -32601; nothing is bridgeable.
    supportsCommandApproval: false,
    supportsFileReadApproval: false,
    supportsFileChangeApproval: false,
    supportsApplyPatchApproval: false,
    approvalsHaveNativeRequestIds: false,
    approvalCallbacksAreLiveOnly: true,
    approvalsCanOriginateFromSubagents: false,
  },
  planning: {
    emitsPlanUpdated: false,
    emitsTodoList: false,
    emitsProposedPlan: false,
    supportsStructuredQuestions: false,
    planDeltasHaveItemIds: false,
  },
  subagents: {
    supportsSubagents: false,
    exposesSubagentThreadIds: false,
    emitsSubagentLifecycle: false,
    canWaitForSubagents: false,
    canCloseSubagents: false,
    canForkSubagentThread: false,
  },
  context: {
    acceptsSystemContext: false,
    acceptsDeveloperContext: false,
    acceptsSyntheticUserContext: true,
    canGenerateSummaries: false,
    canConsumeHandoffSummaries: false,
    supportsDeltaHandoff: false,
    supportsFullThreadHandoff: false,
    maxRecommendedHandoffChars: null,
  },
  checkpointing: {
    appCanCheckpointFilesystem: true,
    supportsNestedCheckpointScopes: false,
    providerCanRollbackConversation: false,
    providerRollbackReturnsSnapshot: false,
    providerCanReadConversationSnapshot: true,
  },
  identity: {
    nativeThreadIds: "strong",
    nativeTurnIds: "none",
    nativeItemIds: "strong",
    nativeRequestIds: "none",
  },
  runtimePolicy: {
    // The zcode session enforces its own permission mode (plan/edit/yolo);
    // T3 runtime modes map onto it at session create/resume.
    enforcement: "native",
  },
} satisfies OrchestrationV2ProviderCapabilities;

// ── Wire payload helpers (pure) ────────────────────────────────────────

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

function protocolError(detail: string): ProviderAdapter.ProviderAdapterProtocolError {
  return new ProviderAdapter.ProviderAdapterProtocolError({
    driver: ZCODE_PROVIDER,
    detail,
  });
}

/** `turn.failed`/`turn.completed` error details → an error message. */
export function readZcodeErrorMessage(
  payload: Record<string, unknown> | undefined,
): string | undefined {
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
 * `turn.completed.resultType` → the shared terminal status. The wire enum is
 * `success | cancelled | error_max_turns | error_max_budget |
 * error_during_execution | error_max_tool_calls` (bundle-verified); every
 * `error_*` variant is a failure. `cancelled` becomes `interrupted` when the
 * interruption came from T3's own `interruptTurn` (session/stop), else stays
 * `cancelled` (provider-initiated).
 */
export function zcodeResultTypeToTerminalStatus(
  resultType: string | undefined,
  interrupted: boolean,
): "completed" | "interrupted" | "cancelled" | "failed" {
  if (resultType !== undefined && resultType.startsWith("error_")) {
    return "failed";
  }
  if (resultType === "cancelled") {
    return interrupted ? "interrupted" : "cancelled";
  }
  return "completed";
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

function dateTimeFromIso(value: string | undefined, fallback: DateTime.Utc): DateTime.Utc {
  if (value === undefined) return fallback;
  try {
    return DateTime.makeUnsafe(value);
  } catch {
    return fallback;
  }
}

function providerRef(nativeId: string) {
  return { driver: ZCODE_PROVIDER, nativeId, strength: "strong" as const };
}

export interface ZcodeToolUpdatedWirePayload {
  readonly kind?: unknown;
  readonly toolCallId?: unknown;
  readonly toolName?: unknown;
  readonly result?: {
    readonly success?: unknown;
    readonly content?: unknown;
  };
}

export interface ZcodeToolNodeContext {
  /** Only the node-id derivation is needed; tests pass a bare implementation. */
  readonly idAllocator: Pick<IdAllocatorV2Shape["derive"], "nodeFromProviderItem">;
  readonly threadId: ThreadId;
  readonly runId: ProviderAdapter.ProviderAdapterV2TurnInput["runId"];
  readonly rootNodeId: ProviderAdapter.ProviderAdapterV2TurnInput["rootNodeId"];
  readonly providerThreadId: ProviderThreadId;
  readonly providerTurnId: ProviderTurnId;
  /** Remembers `toolName` per `toolCallId`: wire `result` events identify calls by id alone. */
  readonly toolCallNames: Map<string, string>;
}

/**
 * Map one zcode `tool.updated` session event onto an execution-node update.
 * The wire state machine per call is `scheduled` → `started` → `result` (plus
 * a terminal `batch` rollup that carries no per-call information — dropped).
 * Returns undefined for events that project onto nothing.
 */
export function projectZcodeToolNode(
  event: ProviderEvent,
  wire: ZcodeToolUpdatedWirePayload | undefined,
  context: ZcodeToolNodeContext,
  occurredAt: DateTime.Utc,
): OrchestrationV2ExecutionNode | undefined {
  if (wire === undefined) return undefined;
  const kind = typeof wire.kind === "string" ? wire.kind : undefined;
  const toolCallId = typeof wire.toolCallId === "string" ? wire.toolCallId : undefined;
  if (!toolCallId || (kind !== "scheduled" && kind !== "started" && kind !== "result")) {
    return undefined;
  }
  const wireToolName =
    typeof wire.toolName === "string" && wire.toolName.trim() ? wire.toolName : undefined;
  if (wireToolName) {
    context.toolCallNames.set(toolCallId, wireToolName);
  }
  // `result` carries no toolName — resolve the call's name from its scheduled
  // event so the terminal node keeps the call identity it opened with.
  if (context.toolCallNames.size > 1_024) context.toolCallNames.clear();
  if (kind === "result") context.toolCallNames.delete(toolCallId);
  return {
    id: context.idAllocator.nodeFromProviderItem({
      driver: ZCODE_PROVIDER,
      nativeItemId: toolCallId,
    }),
    threadId: context.threadId,
    runId: context.runId,
    parentNodeId: context.rootNodeId,
    rootNodeId: context.rootNodeId,
    kind: "tool_call",
    status:
      kind === "scheduled"
        ? "pending"
        : kind === "started"
          ? "running"
          : wire.result?.success === true
            ? "completed"
            : "failed",
    countsForRun: false,
    providerThreadId: context.providerThreadId,
    providerTurnId: context.providerTurnId,
    nativeItemRef: providerRef(toolCallId),
    runtimeRequestId: null,
    checkpointScopeId: null,
    startedAt: dateTimeFromIso(event.createdAt, occurredAt),
    completedAt: kind === "result" ? occurredAt : null,
  };
}

// ── Adapter ────────────────────────────────────────────────────────────

/**
 * The "reasoningEffort" select option (built by ZCodeProvider from
 * `settings.thoughtLevel`) re-stated on the turn's modelSelection — the same
 * convention Codex uses, so clients render one familiar selector.
 */
const readZcodeReasoningLevel = (
  modelSelection: ModelSelection | null | undefined,
): string | undefined => getModelSelectionStringOptionValue(modelSelection, "reasoningEffort");

const zcodeModelForInstance = (
  instanceId: ProviderInstanceId,
  modelSelection: ModelSelection | null | undefined,
): string | undefined =>
  modelSelection?.instanceId === instanceId ? modelSelection.model : undefined;

interface ActiveZcodeTurn {
  /**
   * Canonical turn id the ZcodeSessionRuntime minted for the latest
   * `session/send` (steering sends again and the runtime re-stamps its
   * active id, so this tracks the newest one).
   */
  runtimeTurnId: TurnId;
  providerTurn: OrchestrationV2ProviderTurn;
  readonly providerThreadId: ProviderThreadId;
  readonly threadId: ThreadId;
  readonly runId: ProviderAdapter.ProviderAdapterV2TurnInput["runId"];
  readonly runOrdinal: number;
  readonly rootNodeId: ProviderAdapter.ProviderAdapterV2TurnInput["rootNodeId"];
  readonly itemOrdinals: Map<string, number>;
  nextItemOrdinal: number;
  /**
   * Assistant text accumulated per wire `assistantMessageId` — each
   * `model.streaming` text_delta appends; `message.upserted` overwrites with
   * the authoritative full text. Keyed by the wire id so every emission
   * derives one stable message/node/item id triple per assistant segment.
   * `final` marks segments already emitted with streaming=false (done delta
   * or authoritative overwrite) so turn completion does not re-emit them.
   */
  readonly streamedText: Map<string, { text: string; startedAt: DateTime.Utc; final: boolean }>;
  interrupted: boolean;
  finalized: boolean;
}

/** Per-openSession state; one zcode session runtime per provider session. */
interface ZcodeSessionState {
  readonly threadId: ThreadId;
  runtime: ZcodeSessionRuntimeShape | null;
  providerThread: OrchestrationV2ProviderThread | null;
  activeTurn: ActiveZcodeTurn | null;
  readonly toolCallNames: Map<string, string>;
  stopped: boolean;
}

interface RegisteredZcodeSession {
  readonly state: ZcodeSessionState;
  /** Fail every active turn and mark the session errored (client death). */
  readonly terminate: (detail: string) => Effect.Effect<void>;
}

export interface ZcodeAdapterV2Options {
  readonly instanceId: ProviderInstanceId;
  readonly settings: ZCodeSettings;
  readonly environment: NodeJS.ProcessEnv;
  readonly idAllocator: IdAllocatorV2Shape;
  readonly serverConfig: ServerConfig["Service"];
  readonly crypto: Crypto.Crypto;
  readonly spawner: ChildProcessSpawner.ChildProcessSpawner["Service"];
  /**
   * Scope owning the shared app-server process and its fibers, released when
   * the driver-create scope closes (provider instance teardown).
   */
  readonly adapterScope: Scope.Scope;
  /** Test seam replacing the shared protocol client. */
  readonly makeClient?: (
    options: Parameters<typeof makeZcodeProtocolClient>[0],
  ) => Effect.Effect<
    ZcodeProtocolClientShape,
    ZcodeProtocolError,
    ChildProcessSpawner.ChildProcessSpawner | Scope.Scope
  >;
  /** Test seam replacing the per-thread session runtime. */
  readonly makeRuntime?: (
    options: ZcodeSessionRuntimeOptions,
    services: { readonly client: ZcodeProtocolClientShape },
  ) => Effect.Effect<ZcodeSessionRuntimeShape, never, Crypto.Crypto>;
}

export function makeZcodeAdapterV2(
  options: ZcodeAdapterV2Options,
): ProviderAdapter.ProviderAdapterV2Shape {
  const { idAllocator, serverConfig } = options;

  // Adapter-level shared-client state. Plain closure + unsafe semaphore:
  // `withPermit` serializes the check-spawn-store sequence (single flight),
  // and the death watcher drops the reference so the next call respawns.
  let sharedClient: ZcodeProtocolClientShape | null = null;
  const clientSemaphore = Semaphore.makeUnsafe(1);
  const registeredSessions = new Map<ThreadId, RegisteredZcodeSession>();

  const routeNotifications = Effect.fn("ZcodeAdapterV2.routeNotifications")(function* (
    client: ZcodeProtocolClientShape,
  ) {
    // One fan-out fiber per spawned process: notifications carry the zcode
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
        for (const registered of registeredSessions.values()) {
          if (registered.state.stopped || registered.state.runtime === null) {
            continue;
          }
          const current = yield* registered.state.runtime.getSession;
          const ownedSessionId = readZcodeResumeCursor(current.resumeCursor)?.sessionId;
          if (ownedSessionId === sessionId) {
            yield* registered.state.runtime.handleNotification(notification);
            return;
          }
        }
      }).pipe(
        Effect.catchCause((cause) =>
          Effect.logWarning("failed to route zcode notification", { cause }),
        ),
      ),
    ).pipe(Effect.forkIn(options.adapterScope), Effect.asVoid);
  });

  const ensureClient = Effect.fn("ZcodeAdapterV2.ensureClient")(function* () {
    if (sharedClient !== null) {
      return sharedClient;
    }
    return yield* clientSemaphore.withPermit(
      Effect.gen(function* () {
        if (sharedClient !== null) {
          return sharedClient;
        }
        const createClient = options.makeClient ?? makeZcodeProtocolClient;
        const client = yield* createClient({
          binaryPath: options.settings.binaryPath,
          launchArgs: zcodeLaunchArgv(
            resolveZcodeLaunchArgs(options.settings.launchArgs, options.environment),
          ),
          cwd: process.cwd(),
          environment: options.environment,
        }).pipe(
          Effect.provideService(Scope.Scope, options.adapterScope),
          Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, options.spawner),
        );
        sharedClient = client;
        // A dead process must not strand the provider instance: once the
        // client terminates (crash, external kill), drop the cached reference
        // so the next ensureClient respawns, and fail every registered
        // session's active turn so no run waits on a notification stream that
        // will never deliver its terminal event.
        yield* client.whenTerminated.pipe(
          Effect.flatMap(() =>
            Effect.sync(() => {
              if (sharedClient === client) {
                sharedClient = null;
              }
            }),
          ),
          Effect.flatMap(() =>
            Effect.forEach(
              Array.from(registeredSessions.values()),
              (registered) =>
                registered.terminate("zcode app-server process terminated unexpectedly."),
              { concurrency: 1, discard: true },
            ),
          ),
          Effect.forkIn(options.adapterScope),
        );
        yield* routeNotifications(client);
        return client;
      }),
    );
  });

  return ProviderAdapter.ProviderAdapterV2.of({
    instanceId: options.instanceId,
    driver: ZCODE_PROVIDER,
    getCapabilities: () => Effect.succeed(ZcodeProviderCapabilitiesV2),
    planSelectionTransition: () => Effect.succeed(turnScopedSelectionTransition()),
    openSession: Effect.fn("ZcodeAdapterV2.openSession")(
      function* (input: ProviderAdapter.ProviderAdapterV2OpenSessionInput) {
        const scope = yield* Effect.scope;
        const cwd = input.runtimePolicy.cwd ?? serverConfig.cwd;
        const events = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event, Cause.Done>();

        const now = yield* DateTime.now;
        let sessionEntity: OrchestrationV2ProviderSession = {
          id: input.providerSessionId,
          driver: ZCODE_PROVIDER,
          providerInstanceId: options.instanceId,
          status: "ready",
          cwd,
          model: zcodeModelForInstance(options.instanceId, input.modelSelection) ?? null,
          capabilities: ZcodeProviderCapabilitiesV2,
          createdAt: now,
          updatedAt: now,
          lastError: null,
        };
        const state: ZcodeSessionState = {
          threadId: input.threadId,
          runtime: null,
          providerThread: null,
          activeTurn: null,
          toolCallNames: new Map(),
          stopped: false,
        };
        const readRuntimeSession = Effect.suspend(() =>
          state.runtime === null ? Effect.succeed(null) : state.runtime.getSession,
        );

        const emitProviderEvent = (event: ProviderAdapter.ProviderAdapterV2Event) =>
          Queue.offer(events, event).pipe(Effect.asVoid);

        const updateProviderSession = (
          status: OrchestrationV2ProviderSession["status"],
          lastError: string | null = sessionEntity.lastError,
        ) =>
          Effect.gen(function* () {
            const updatedAt = yield* DateTime.now;
            sessionEntity = { ...sessionEntity, status, lastError, updatedAt };
            yield* emitProviderEvent({
              type: "provider_session.updated",
              driver: ZCODE_PROVIDER,
              providerSession: sessionEntity,
            });
          });

        const updateProviderThread = (patch: Partial<OrchestrationV2ProviderThread>) =>
          Effect.gen(function* () {
            if (state.providerThread === null) return;
            const updatedAt = yield* DateTime.now;
            state.providerThread = { ...state.providerThread, ...patch, updatedAt };
            yield* emitProviderEvent({
              type: "provider_thread.updated",
              driver: ZCODE_PROVIDER,
              providerThread: state.providerThread,
            });
          });

        const emitProviderTurn = (
          turn: ActiveZcodeTurn,
          status: OrchestrationV2ProviderTurn["status"],
          completedAt: DateTime.Utc | null,
        ) => {
          turn.providerTurn = { ...turn.providerTurn, status, completedAt };
          return emitProviderEvent({
            type: "provider_turn.updated",
            driver: ZCODE_PROVIDER,
            threadId: turn.threadId,
            providerTurn: turn.providerTurn,
          });
        };

        const itemOrdinal = (turn: ActiveZcodeTurn, nativeItemId: string): number => {
          const existing = turn.itemOrdinals.get(nativeItemId);
          if (existing !== undefined) return existing;
          const ordinal = (turn.nextItemOrdinal += 1);
          turn.itemOrdinals.set(nativeItemId, ordinal);
          return ordinal;
        };

        /**
         * Close out the active turn: terminal provider_turn.updated plus the
         * turn.terminal signal RunExecutionService waits on, then thread and
         * session settle back to idle.
         */
        const finalizeTurn = Effect.fn("ZcodeAdapterV2.finalizeTurn")(function* (
          turn: ActiveZcodeTurn,
          status: Extract<
            OrchestrationV2ProviderTurn["status"],
            "completed" | "interrupted" | "failed" | "cancelled"
          >,
          input: {
            readonly failure?: OrchestrationV2ProviderFailure | undefined;
            readonly threadDisposition?: "reusable" | "broken";
          } = {},
        ) {
          if (turn.finalized) return;
          turn.finalized = true;
          const threadDisposition = input.threadDisposition ?? "reusable";
          const completedAt = yield* DateTime.now;
          yield* emitProviderTurn(turn, status, completedAt);
          yield* updateProviderThread({
            status:
              status === "failed" ? (threadDisposition === "broken" ? "error" : "idle") : "idle",
            lastRunOrdinal: turn.runOrdinal,
          });
          if (state.activeTurn === turn) {
            state.activeTurn = null;
          }
          yield* updateProviderSession(
            status === "failed" ? "error" : "ready",
            status === "failed"
              ? (input.failure?.message ?? sessionEntity.lastError)
              : sessionEntity.lastError,
          );
          yield* emitProviderEvent(
            status === "failed"
              ? {
                  type: "turn.terminal",
                  driver: ZCODE_PROVIDER,
                  providerThreadId: turn.providerThreadId,
                  providerTurnId: turn.providerTurn.id,
                  runOrdinal: turn.runOrdinal,
                  failureItemOrdinal: itemOrdinal(turn, `terminal-failure:${turn.providerTurn.id}`),
                  status,
                  failure:
                    input.failure ??
                    makeProviderFailure({
                      message: sessionEntity.lastError ?? undefined,
                      class: "provider_error",
                    }),
                  threadDisposition,
                }
              : {
                  type: "turn.terminal",
                  driver: ZCODE_PROVIDER,
                  providerThreadId: turn.providerThreadId,
                  providerTurnId: turn.providerTurn.id,
                  runOrdinal: turn.runOrdinal,
                  status,
                  failure: null,
                  threadDisposition,
                },
          );
        });

        // ── ProviderEvent projection ──────────────────────────────────

        /**
         * Project one assistant text segment (OpenCode2 adapter pattern):
         * execution node + conversation message + turn item, all keyed off
         * the wire message id so re-emissions upsert the same ids. Called
         * with the accumulated text on every `model.streaming` text_delta
         * (streaming=true) and once more on finalize with streaming=false.
         */
        const emitAssistantMessage = Effect.fn("ZcodeAdapterV2.emitAssistantMessage")(function* (
          turn: ActiveZcodeTurn,
          nativeMessageId: string,
          text: string,
          input: {
            readonly streaming: boolean;
            readonly startedAt: DateTime.Utc;
            readonly occurredAt: DateTime.Utc;
          },
        ) {
          const messageId = idAllocator.derive.messageFromProviderItem({
            driver: ZCODE_PROVIDER,
            nativeItemId: nativeMessageId,
          });
          const nodeId = idAllocator.derive.nodeFromProviderItem({
            driver: ZCODE_PROVIDER,
            nativeItemId: nativeMessageId,
          });
          const completedAt = input.streaming ? null : input.occurredAt;
          yield* emitProviderEvent({
            type: "node.updated",
            driver: ZCODE_PROVIDER,
            node: {
              id: nodeId,
              threadId: turn.threadId,
              runId: turn.runId,
              parentNodeId: turn.rootNodeId,
              rootNodeId: turn.rootNodeId,
              kind: "assistant_message",
              status: input.streaming ? "running" : "completed",
              countsForRun: false,
              providerThreadId: turn.providerThreadId,
              providerTurnId: turn.providerTurn.id,
              nativeItemRef: providerRef(nativeMessageId),
              runtimeRequestId: null,
              checkpointScopeId: null,
              startedAt: input.startedAt,
              completedAt,
            },
          });
          yield* emitProviderEvent({
            type: "message.updated",
            driver: ZCODE_PROVIDER,
            message: {
              createdBy: "agent",
              creationSource: "provider",
              id: messageId,
              threadId: turn.threadId,
              runId: turn.runId,
              nodeId,
              role: "assistant",
              text,
              attachments: [],
              streaming: input.streaming,
              createdAt: input.startedAt,
              updatedAt: input.occurredAt,
            },
          });
          const turnItem: OrchestrationV2TurnItem = {
            id: idAllocator.derive.turnItemFromProviderItem({
              driver: ZCODE_PROVIDER,
              nativeItemId: nativeMessageId,
            }),
            threadId: turn.threadId,
            runId: turn.runId,
            nodeId,
            providerThreadId: turn.providerThreadId,
            providerTurnId: turn.providerTurn.id,
            nativeItemRef: providerRef(nativeMessageId),
            parentItemId: null,
            ordinal: itemOrdinal(turn, nativeMessageId),
            status: input.streaming ? "running" : "completed",
            title: null,
            startedAt: input.startedAt,
            completedAt,
            updatedAt: input.occurredAt,
            type: "assistant_message",
            messageId,
            text,
            streaming: input.streaming,
          };
          yield* emitProviderEvent({ type: "turn_item.updated", driver: ZCODE_PROVIDER, turnItem });
        });

        const handleRuntimeEvent = Effect.fn("ZcodeAdapterV2.handleRuntimeEvent")(function* (
          event: ProviderEvent,
        ) {
          const occurredAt = yield* DateTime.now;
          const turn = state.activeTurn;
          // Turn-scoped events carry the canonical runtime turn id stamped by
          // sendTurn; a different id is a desk-started/promptless turn T3 did
          // not start — nothing to attach it to.
          if (event.turnId !== undefined && turn !== null && event.turnId !== turn.runtimeTurnId) {
            return;
          }

          if (event.kind === "session") {
            switch (event.method) {
              case "session/connecting":
                if (sessionEntity.status !== "starting") {
                  yield* updateProviderSession("starting");
                }
                return;
              case "session/ready":
                if (sessionEntity.status === "starting") {
                  yield* updateProviderSession("ready");
                }
                return;
              case "session/started": {
                // The runtime stamped its resume cursor before emitting; bind
                // the provider thread's native ref to the live session id.
                const sessionId = readZcodeResumeCursor(
                  (yield* readRuntimeSession)?.resumeCursor,
                )?.sessionId;
                if (sessionId !== undefined) {
                  yield* updateProviderThread({
                    nativeThreadRef: {
                      driver: ZCODE_PROVIDER,
                      nativeId: sessionId,
                      strength: "strong",
                    },
                  });
                }
                return;
              }
              case "session/closed":
                if (sessionEntity.status !== "stopped") {
                  yield* updateProviderSession("stopped");
                }
                return;
              default:
                return;
            }
          }

          const payload = asRecord(event.payload);
          const wirePayload = asRecord(payload?.payload);

          switch (event.method) {
            case "turn.started": {
              if (sessionEntity.status !== "running") {
                yield* updateProviderSession("running");
              }
              return;
            }
            case "model.streaming": {
              if (turn === null) return;
              const kind = trimText(wirePayload?.kind);
              if (kind !== "text_delta") return;
              const nativeMessageId = trimText(wirePayload?.assistantMessageId);
              if (nativeMessageId === undefined) return;
              const delta = typeof wirePayload?.delta === "string" ? wirePayload.delta : "";
              const final = wirePayload?.done === true;
              const entry = turn.streamedText.get(nativeMessageId) ?? {
                text: "",
                startedAt: occurredAt,
                final: false,
              };
              entry.text += delta;
              entry.final = final;
              turn.streamedText.set(nativeMessageId, entry);
              if (entry.text.length === 0) return;
              yield* emitAssistantMessage(turn, nativeMessageId, entry.text, {
                streaming: !final,
                startedAt: entry.startedAt,
                occurredAt,
              });
              return;
            }
            case "message.upserted": {
              // Authoritative full-text overwrite when the CLI delivers it
              // (persistence/rehydration path). Only clearly-assistant
              // records project: `toolCalls` is assistant-only on the wire,
              // and payloads carrying `type` (system messages) or
              // `attachments` (user echo, already in T3's canonical
              // transcript) are skipped.
              if (turn === null) return;
              if (wirePayload?.toolCalls === undefined) return;
              const nativeMessageId =
                trimText(wirePayload?.messageId) ?? trimText(payload?.entityId);
              const text = typeof wirePayload?.content === "string" ? wirePayload.content : "";
              if (nativeMessageId === undefined || text.trim().length === 0) return;
              const entry = turn.streamedText.get(nativeMessageId) ?? {
                text: "",
                startedAt: occurredAt,
                final: false,
              };
              entry.text = text;
              entry.final = true;
              turn.streamedText.set(nativeMessageId, entry);
              yield* emitAssistantMessage(turn, nativeMessageId, text, {
                streaming: false,
                startedAt: entry.startedAt,
                occurredAt,
              });
              return;
            }
            case "tool.updated": {
              if (turn === null || state.providerThread === null) return;
              const node = projectZcodeToolNode(
                event,
                wirePayload as ZcodeToolUpdatedWirePayload,
                {
                  idAllocator: idAllocator.derive,
                  threadId: turn.threadId,
                  runId: turn.runId,
                  rootNodeId: turn.rootNodeId,
                  providerThreadId: turn.providerThreadId,
                  providerTurnId: turn.providerTurn.id,
                  toolCallNames: state.toolCallNames,
                },
                occurredAt,
              );
              if (node !== undefined) {
                yield* emitProviderEvent({ type: "node.updated", driver: ZCODE_PROVIDER, node });
              }
              return;
            }
            case "turn.completed": {
              if (turn === null) return;
              const resultType = trimText(wirePayload?.resultType);
              const status = zcodeResultTypeToTerminalStatus(resultType, turn.interrupted);
              // Close out the streamed assistant segments: a `done:true`
              // delta may not always arrive (stream cut short), so re-emit
              // every still-streaming entry with streaming=false. If the
              // turn produced NO streamed segment at all but the wire
              // completion carries a non-empty `response`, backstop with
              // that full text (segment id scoped to the provider turn).
              if (status !== "failed" && turn.streamedText.size === 0) {
                const response =
                  typeof wirePayload?.response === "string" ? wirePayload.response : "";
                if (response.trim().length > 0) {
                  const nativeMessageId = `turn:${turn.providerTurn.id}:response`;
                  turn.streamedText.set(nativeMessageId, {
                    text: response,
                    startedAt: occurredAt,
                    final: false,
                  });
                }
              }
              for (const [nativeMessageId, entry] of turn.streamedText) {
                if (entry.final) continue;
                yield* emitAssistantMessage(turn, nativeMessageId, entry.text, {
                  streaming: false,
                  startedAt: entry.startedAt,
                  occurredAt,
                });
              }
              turn.streamedText.clear();
              const errorMessage =
                status === "failed" ? readZcodeErrorMessage(wirePayload) : undefined;
              const usage = normalizeZcodeTokenUsage(asRecord(wirePayload?.usage));
              if (usage !== undefined) {
                turn.providerTurn = {
                  ...turn.providerTurn,
                  tokenUsage: {
                    usedTokens: usage.usedTokens,
                    ...(usage.inputTokens !== undefined ? { inputTokens: usage.inputTokens } : {}),
                    ...(usage.cachedInputTokens !== undefined
                      ? { cachedInputTokens: usage.cachedInputTokens }
                      : {}),
                    ...(usage.outputTokens !== undefined
                      ? { outputTokens: usage.outputTokens }
                      : {}),
                    ...(usage.reasoningOutputTokens !== undefined
                      ? { reasoningOutputTokens: usage.reasoningOutputTokens }
                      : {}),
                    updatedAt: DateTime.formatIso(occurredAt),
                  },
                };
              }
              yield* finalizeTurn(
                turn,
                status,
                status === "failed"
                  ? {
                      failure: makeProviderFailure({
                        message: errorMessage,
                        class: "provider_error",
                      }),
                    }
                  : {},
              );
              return;
            }
            case "turn.failed": {
              if (turn === null) return;
              // Failure paths emit `turn.failed` INSTEAD of `turn.completed`.
              // Close any partially-streamed assistant text so the UI does
              // not keep a forever-streaming item.
              for (const [nativeMessageId, entry] of turn.streamedText) {
                yield* emitAssistantMessage(turn, nativeMessageId, entry.text, {
                  streaming: false,
                  startedAt: entry.startedAt,
                  occurredAt,
                });
              }
              turn.streamedText.clear();
              yield* finalizeTurn(turn, "failed", {
                failure: makeProviderFailure({
                  message: readZcodeErrorMessage(wirePayload),
                  class: "provider_error",
                }),
              });
              return;
            }
            case "state.updated": {
              const patch = asRecord(payload?.patch);
              const wireStatus = trimText(patch?.status);
              // Wire status enum: idle | running | waiting | paused |
              // completed | error. "waiting"/"paused" are interactive states
              // with no T3 bridge (the turn stays running); "error" is
              // surfaced at the turn level via turn.failed — deliberately not
              // re-emitted here.
              if (wireStatus === "running" && sessionEntity.status !== "running") {
                yield* updateProviderSession("running");
              } else if (
                (wireStatus === "idle" || wireStatus === "completed") &&
                sessionEntity.status !== "ready"
              ) {
                yield* updateProviderSession("ready");
              }
              return;
            }
            case "session.titleUpdated": {
              const title = trimText(wirePayload?.title);
              if (title !== undefined) {
                yield* updateProviderThread({
                  nativeMetadata: { title, updatedAt: DateTime.formatIso(occurredAt) },
                });
              }
              return;
            }
            default:
              // model.streaming deltas, session.updated and unobserved event
              // types are intentionally not projected (see capabilities).
              return;
          }
        });

        // ── Shared-client registration ────────────────────────────────

        const registered: RegisteredZcodeSession = {
          state,
          terminate: (detail) =>
            Effect.gen(function* () {
              yield* updateProviderSession("error", detail);
              if (state.activeTurn !== null) {
                yield* finalizeTurn(state.activeTurn, "failed", {
                  failure: makeProviderFailure({ message: detail, class: "transport_error" }),
                  threadDisposition: "broken",
                });
              }
            }),
        };
        registeredSessions.set(input.threadId, registered);
        yield* Scope.addFinalizer(
          scope,
          Effect.gen(function* () {
            state.stopped = true;
            registeredSessions.delete(input.threadId);
            if (state.runtime !== null) {
              yield* state.runtime.close.pipe(Effect.ignore);
            }
            yield* Queue.end(events);
          }),
        );

        // ── Runtime surface ───────────────────────────────────────────

        const runtimeSession: ProviderAdapter.ProviderAdapterV2SessionRuntime = {
          instanceId: options.instanceId,
          driver: ZCODE_PROVIDER,
          providerSessionId: input.providerSessionId,
          providerSession: sessionEntity,
          events: Stream.fromEffectRepeat(Queue.take(events)),
          ensureThread: (threadInput) =>
            Effect.gen(function* () {
              // Idempotent: the runtime IS this session's single zcode
              // session, so a repeat call returns the row the first produced.
              if (state.runtime !== null && state.providerThread !== null) {
                return state.providerThread;
              }
              const client = yield* ensureClient();
              const createRuntime =
                options.makeRuntime ??
                ((
                  runtimeOptions: ZcodeSessionRuntimeOptions,
                  services: { readonly client: ZcodeProtocolClientShape },
                ) => makeZcodeSessionRuntime(runtimeOptions, services));
              const model = zcodeModelForInstance(options.instanceId, threadInput.modelSelection);
              const reasoningLevel = readZcodeReasoningLevel(threadInput.modelSelection);
              const resumeSessionId = threadInput.existingProviderThread?.nativeThreadRef?.nativeId;
              const runtime = yield* createRuntime(
                {
                  threadId: input.threadId,
                  providerInstanceId: options.instanceId,
                  cwd,
                  runtimeMode: threadInput.runtimePolicy.runtimeMode,
                  ...(model !== undefined
                    ? {
                        model,
                        ...(reasoningLevel !== undefined ? { thoughtLevel: reasoningLevel } : {}),
                      }
                    : {}),
                  ...(resumeSessionId != null
                    ? { resumeCursor: { sessionId: resumeSessionId } }
                    : {}),
                },
                { client },
              ).pipe(Effect.provideService(Crypto.Crypto, options.crypto));
              state.runtime = runtime;
              // The mapping fiber consumes the runtime's ProviderEvent stream
              // for this session's lifetime; the runtime's own close shuts its
              // queue, ending the fiber.
              yield* Stream.runForEach(runtime.events, handleRuntimeEvent).pipe(
                Effect.catchCause((cause) =>
                  Cause.hasInterrupts(cause)
                    ? Effect.interrupt
                    : Effect.logWarning("zcode runtime event projection failed", { cause }),
                ),
                Effect.forkIn(scope),
              );
              const started = yield* runtime.start();
              const sessionId = readZcodeResumeCursor(started.resumeCursor)?.sessionId;
              if (sessionId === undefined) {
                return yield* protocolError("zcode session/start returned no session id");
              }
              const startedAt = yield* DateTime.now;
              const nativeThreadRef = {
                driver: ZCODE_PROVIDER,
                nativeId: sessionId,
                strength: "strong" as const,
              };
              const existing = threadInput.existingProviderThread;
              // Bind the new native session to the caller's row when one was
              // handed over: a second live row per app thread would make
              // `activeProviderThreadId` flap between the two on every update.
              const providerThread: OrchestrationV2ProviderThread =
                existing === undefined
                  ? {
                      id: idAllocator.derive.providerThread({
                        driver: ZCODE_PROVIDER,
                        nativeThreadId: sessionId,
                        providerInstanceId: options.instanceId,
                      }),
                      driver: ZCODE_PROVIDER,
                      providerInstanceId: options.instanceId,
                      providerSessionId: input.providerSessionId,
                      appThreadId: input.threadId,
                      ownerNodeId: null,
                      nativeThreadRef,
                      nativeConversationHeadRef: null,
                      status: "idle",
                      firstRunOrdinal: null,
                      lastRunOrdinal: null,
                      handoffIds: [],
                      forkedFrom: null,
                      createdAt: startedAt,
                      updatedAt: startedAt,
                    }
                  : {
                      ...existing,
                      providerSessionId: input.providerSessionId,
                      nativeThreadRef,
                      nativeConversationHeadRef: null,
                      status: "idle",
                      updatedAt: startedAt,
                    };
              state.providerThread = providerThread;
              yield* emitProviderEvent({
                type: "provider_thread.updated",
                driver: ZCODE_PROVIDER,
                providerThread,
              });
              return providerThread;
            }).pipe(
              Effect.mapError(
                (cause) =>
                  new ProviderAdapter.ProviderAdapterEnsureThreadError({
                    driver: ZCODE_PROVIDER,
                    threadId: threadInput.threadId,
                    cause,
                  }),
              ),
            ),
          resumeThread: (threadInput) =>
            Effect.gen(function* () {
              const nativeId = threadInput.providerThread.nativeThreadRef?.nativeId;
              if (nativeId == null) {
                return yield* protocolError(
                  `zcode provider thread ${threadInput.providerThread.id} has no native session id`,
                );
              }
              // The single zcode session per openSession resumes in place:
              // ensureThread routes an existing native ref through
              // session/resume, so delegate with the caller's row attached.
              return yield* runtimeSession.ensureThread({
                threadId: threadInput.threadId ?? input.threadId,
                modelSelection: threadInput.modelSelection ?? input.modelSelection,
                runtimePolicy: threadInput.runtimePolicy ?? input.runtimePolicy,
                providerSessionId: input.providerSessionId,
                existingProviderThread: threadInput.providerThread,
              });
            }).pipe(
              Effect.mapError(
                (cause) =>
                  new ProviderAdapter.ProviderAdapterResumeThreadError({
                    driver: ZCODE_PROVIDER,
                    providerSessionId: input.providerSessionId,
                    providerThreadId: threadInput.providerThread.id,
                    cause,
                  }),
              ),
            ),
          startTurn: (turnInput) =>
            Effect.gen(function* () {
              const runtime = state.runtime;
              if (runtime === null) {
                return yield* protocolError(
                  `zcode provider thread ${turnInput.providerThread.id} has no session runtime`,
                );
              }
              if (state.activeTurn !== null) {
                return yield* protocolError(
                  `zcode provider thread ${turnInput.providerThread.id} already has an active turn`,
                );
              }
              // `session/send` takes plain text only; attachments cannot be
              // forwarded (non-image attachments already reach the agent via
              // the path lines T3 adds to the prompt).
              const model = zcodeModelForInstance(options.instanceId, turnInput.modelSelection);
              const reasoningLevel = readZcodeReasoningLevel(turnInput.modelSelection);
              const sent = yield* runtime.sendTurn({
                input: turnInput.message.text,
                ...(model !== undefined ? { model } : {}),
                ...(reasoningLevel !== undefined ? { thoughtLevel: reasoningLevel } : {}),
              });
              const startedAt = yield* DateTime.now;
              const providerTurn: OrchestrationV2ProviderTurn = {
                id: idAllocator.derive.providerTurn({
                  driver: ZCODE_PROVIDER,
                  nativeTurnId: `${turnInput.providerThread.id}:attempt:${turnInput.attemptId}`,
                }),
                providerThreadId: turnInput.providerThread.id,
                nodeId: turnInput.rootNodeId,
                runAttemptId: turnInput.attemptId,
                nativeTurnRef: null,
                ordinal: turnInput.providerTurnOrdinal,
                status: "running",
                startedAt,
                completedAt: null,
              };
              state.activeTurn = {
                runtimeTurnId: sent.turnId,
                providerTurn,
                providerThreadId: turnInput.providerThread.id,
                threadId: turnInput.threadId,
                runId: turnInput.runId,
                runOrdinal: turnInput.runOrdinal,
                rootNodeId: turnInput.rootNodeId,
                itemOrdinals: new Map(),
                nextItemOrdinal: turnInput.providerTurnOrdinal * 100,
                streamedText: new Map(),
                interrupted: false,
                finalized: false,
              };
              yield* emitProviderTurn(state.activeTurn, "running", null);
              yield* updateProviderThread({
                status: "active",
                firstRunOrdinal: state.providerThread?.firstRunOrdinal ?? turnInput.runOrdinal,
                lastRunOrdinal: turnInput.runOrdinal,
              });
              yield* updateProviderSession("running", null);
            }).pipe(
              Effect.mapError(
                (cause) =>
                  new ProviderAdapter.ProviderAdapterTurnStartError({
                    driver: ZCODE_PROVIDER,
                    threadId: turnInput.threadId,
                    providerThreadId: turnInput.providerThread.id,
                    runId: turnInput.runId,
                    cause,
                  }),
              ),
            ),
          steerTurn: (steerInput) =>
            Effect.gen(function* () {
              const turn = state.activeTurn;
              const runtime = state.runtime;
              if (
                runtime === null ||
                turn === null ||
                turn.providerTurn.id !== steerInput.providerTurnId
              ) {
                return yield* protocolError(
                  `zcode turn ${steerInput.providerTurnId} is not active`,
                );
              }
              // zcode has no dedicated steer RPC: sending again mid-turn
              // delivers the message to the running conversation. The runtime
              // re-stamps its active turn id on every send, so adopt it —
              // completion notifications fold onto the newest canonical id.
              const sent = yield* runtime.sendTurn({ input: steerInput.message.text });
              turn.runtimeTurnId = sent.turnId;
            }).pipe(
              Effect.mapError(
                (cause) =>
                  new ProviderAdapter.ProviderAdapterSteerRunError({
                    driver: ZCODE_PROVIDER,
                    providerThreadId: steerInput.providerThread.id,
                    providerTurnId: steerInput.providerTurnId,
                    cause,
                  }),
              ),
            ),
          interruptTurn: (interruptInput) =>
            Effect.gen(function* () {
              const turn = state.activeTurn;
              if (turn === null || turn.providerTurn.id !== interruptInput.providerTurnId) {
                return yield* protocolError(
                  `zcode turn ${interruptInput.providerTurnId} is not active`,
                );
              }
              turn.interrupted = true;
              const runtime = state.runtime;
              if (runtime !== null) {
                // `session/stop` cancels the running turn; the cancellation
                // surfaces as turn.completed with resultType "cancelled".
                yield* runtime.interruptTurn().pipe(Effect.asVoid);
              }
            }).pipe(
              Effect.mapError(
                (cause) =>
                  new ProviderAdapter.ProviderAdapterInterruptError({
                    driver: ZCODE_PROVIDER,
                    providerThreadId: interruptInput.providerThread.id,
                    providerTurnId: interruptInput.providerTurnId,
                    cause,
                  }),
              ),
            ),
          respondToRuntimeRequest: (requestInput) =>
            Effect.fail(
              new ProviderAdapter.ProviderAdapterRuntimeRequestResponseError({
                driver: ZCODE_PROVIDER,
                requestId: requestInput.requestId,
                cause: protocolError(
                  "ZCode approvals and user-input requests are not supported by this adapter.",
                ),
              }),
            ),
          readThreadSnapshot: (snapshotInput) =>
            Effect.gen(function* () {
              const runtime = state.runtime;
              const sessionId =
                snapshotInput.providerThread.nativeThreadRef?.nativeId ??
                readZcodeResumeCursor((yield* readRuntimeSession)?.resumeCursor)?.sessionId;
              if (runtime === null || sessionId === undefined) {
                return {
                  providerThread: snapshotInput.providerThread,
                  providerTurns: [],
                  messages: [],
                  runtimeRequests: [],
                } satisfies ProviderAdapter.ProviderAdapterV2ThreadSnapshot;
              }
              // Coarse snapshot: one conversation message per zcode message.
              // T3 keeps its own canonical history; this only rehydrates.
              const client = yield* ensureClient();
              const result = yield* client.request("session/messages", { sessionId });
              const now = yield* DateTime.now;
              const wireMessages = Array.isArray(asRecord(result)?.messages)
                ? (asRecord(result)?.messages as ReadonlyArray<unknown>)
                : [];
              const messages: ReadonlyArray<OrchestrationV2ConversationMessage> = wireMessages.map(
                (message, index) => {
                  const record = asRecord(message);
                  const role = record?.role === "user" ? "user" : "assistant";
                  const text = typeof record?.content === "string" ? record.content : "";
                  return {
                    createdBy: role === "user" ? ("user" as const) : ("agent" as const),
                    creationSource: "provider" as const,
                    id: idAllocator.derive.messageFromProviderItem({
                      driver: ZCODE_PROVIDER,
                      nativeItemId: trimText(record?.messageId) ?? `message-${index}`,
                    }),
                    threadId: snapshotInput.providerThread.appThreadId ?? input.threadId,
                    runId: null,
                    nodeId: null,
                    role,
                    text,
                    attachments: [],
                    streaming: false,
                    createdAt: now,
                    updatedAt: now,
                  } satisfies OrchestrationV2ConversationMessage;
                },
              );
              return {
                providerThread: snapshotInput.providerThread,
                providerTurns: [],
                messages,
                runtimeRequests: [],
                providerPayload: wireMessages,
              } satisfies ProviderAdapter.ProviderAdapterV2ThreadSnapshot;
            }).pipe(
              Effect.mapError(
                (cause) =>
                  new ProviderAdapter.ProviderAdapterReadThreadSnapshotError({
                    driver: ZCODE_PROVIDER,
                    providerThreadId: snapshotInput.providerThread.id,
                    cause,
                  }),
              ),
            ),
          rollbackThread: (rollbackInput) =>
            Effect.fail(
              new ProviderAdapter.ProviderAdapterRollbackThreadError({
                driver: ZCODE_PROVIDER,
                providerThreadId: rollbackInput.providerThread.id,
                checkpointId:
                  rollbackInput.target.type === "thread_start"
                    ? undefined
                    : rollbackInput.target.checkpointId,
                cause: protocolError("ZCode does not support conversation rollback."),
              }),
            ),
          forkThread: (forkInput) =>
            Effect.fail(
              new ProviderAdapter.ProviderAdapterForkThreadError({
                driver: ZCODE_PROVIDER,
                providerThreadId: forkInput.sourceProviderThread.id,
                cause: protocolError("ZCode does not support conversation forking."),
              }),
            ),
        };

        return runtimeSession;
      },
      (effect, input) =>
        effect.pipe(
          Effect.mapError(
            (cause) =>
              new ProviderAdapter.ProviderAdapterOpenSessionError({
                driver: ZCODE_PROVIDER,
                providerSessionId: input.providerSessionId,
                cause,
              }),
          ),
        ),
    ),
  });
}

// ── Driver wrapper ─────────────────────────────────────────────────────

export type ZcodeAdapterV2DriverEnv =
  | ChildProcessSpawner.ChildProcessSpawner
  | Crypto.Crypto
  | IdAllocatorV2
  | ServerConfig;

/**
 * Overrides the driver wrapper applies after resolving its services. The
 * ZCodeDriver passes the shadow-home environment here so the adapter spawns
 * the app-server against the isolated HOME (materialization itself stays in
 * the driver — the adapter never touches the filesystem layout).
 */
export interface ZcodeAdapterV2CreateOverrides {
  readonly environment?: NodeJS.ProcessEnv | undefined;
}

export const createZcodeAdapterV2 = (
  { instanceId, environment, enabled, config }: ProviderAdapterDriverCreateInput<ZCodeSettings>,
  overrides: ZcodeAdapterV2CreateOverrides = {},
) =>
  Effect.gen(function* () {
    const crypto = yield* Crypto.Crypto;
    const idAllocator = yield* IdAllocatorV2;
    const serverConfig = yield* ServerConfig;
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    // Dedicated scope for the shared app-server process and its fibers:
    // acquired in (and released with) the driver-create scope, which the
    // registry closes on instance teardown or config rebuild.
    const adapterScope = yield* Effect.acquireRelease(Scope.make(), (scope) =>
      Scope.close(scope, Exit.void).pipe(Effect.ignore),
    );
    return makeZcodeAdapterV2({
      instanceId,
      settings: { ...config, enabled },
      environment: overrides.environment ?? mergeProviderInstanceEnvironment(environment),
      idAllocator,
      serverConfig,
      crypto,
      spawner,
      adapterScope,
    });
  }).pipe(
    Effect.mapError(
      (cause) =>
        new ProviderAdapterDriverCreateError({
          driver: ZCODE_PROVIDER,
          instanceId,
          detail: "Failed to build the ZCode orchestration adapter.",
          cause,
        }),
    ),
  );

export const ZcodeAdapterV2Driver: ProviderAdapterDriver<ZCodeSettings, ZcodeAdapterV2DriverEnv> = {
  driverKind: ZCODE_PROVIDER,
  configSchema: ZCodeSettingsSchema,
  defaultConfig: (): ZCodeSettings => DEFAULT_ZCODE_SETTINGS,
  create: createZcodeAdapterV2,
};
