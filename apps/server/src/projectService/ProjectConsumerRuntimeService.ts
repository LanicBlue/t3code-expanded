/**
 * ProjectConsumerRuntimeService — the T3-side Consumer for Project Service
 * Work notices (issue #4).
 *
 * Owns exactly ONE vendored-SDK `ProjectConsumerRuntime` instance for the
 * configured client. The runtime is created when the integration is enabled
 * AND a credential is stored, closed otherwise, and reconfigured (never
 * duplicated) when the base URL or credential changes — the SDK enforces
 * single-connection semantics, so this service only ever swaps its single
 * instance. Agent inventory (`listAgents`) is answered live from settings on
 * every hello/inventory request, which is the SDK's re-advertisement
 * contract. Wakes (`wakeAgent`) route through the session router; resolving
 * means routed, never "the agent ran".
 *
 * The credential lives only in the hello frame this service sends to the
 * local gateway URL derived from the settings base URL — never in logs,
 * statuses, or routing state. Credential and protocol errors surface as
 * integration status and leave every current session untouched; the SDK's
 * reconnect backoff owns retries.
 *
 * @module ProjectConsumerRuntimeService
 */
import {
  ProjectConsumerRuntime,
  RuntimeConsumerWakeError,
  type ConsumerId,
  type ConsumerRuntimeState,
  type ConsumerSocketLike,
  type RuntimeConsumerAdapter,
  CONSUMER_CLIENT_LATEST,
} from "@lanicblue/project-consumer";
import {
  isLocalProjectServiceBaseUrl,
  type OrchestrationCommand,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as Schedule from "effect/Schedule";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";

import packageJson from "../../package.json" with { type: "json" };
import * as ServerSecretStore from "../auth/ServerSecretStore.ts";
import type { OrchestrationDispatchError } from "../orchestration/Errors.ts";
import * as OrchestrationEngine from "../orchestration/Services/OrchestrationEngine.ts";
import * as ProjectionSnapshotQuery from "../orchestration/Services/ProjectionSnapshotQuery.ts";
import * as ProviderInstanceRegistry from "../provider/Services/ProviderInstanceRegistry.ts";
import { forkParked } from "../serverActivation.ts";
import { getAutoBootstrapDefaultModelSelection } from "../serverRuntimeStartup.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as WorkspacePaths from "../workspace/WorkspacePaths.ts";
import { canonicalWorkspaceDirectory } from "./ProjectDirectoryKey.ts";
import * as ProjectServiceWorkClient from "./ProjectServiceWorkClient.ts";
import {
  makeProjectWorkSessionRouter,
  ProjectWorkRoutingError,
  type ProjectWorkSessionRouter,
} from "./ProjectWorkNoticeRouting.ts";

/** The gateway upgrade path Project Service serves the Consumer protocol on. */
export const CONSUMER_GATEWAY_WS_PATH = "/project/v1/consumer/ws";

/** Self-declared consumer id; the server overrides it with the authenticated clientId. */
const CONSUMER_ID = "t3" as ConsumerId;

/** Spacing of the delivery reconcile sweep (flow liveness A1). */
const RECONCILE_SWEEP_INTERVAL = Duration.minutes(3);

const normalizeBaseUrl = (baseUrl: string): string | null => {
  try {
    const url = new URL(baseUrl);
    if (url.protocol !== "http:" && url.protocol !== "https:") {
      return null;
    }
    return `${url.origin}${url.pathname.replace(/\/+$/, "")}`;
  } catch {
    return null;
  }
};

/** Derive the ws(s):// gateway URL from the local http(s):// service base URL. */
export const deriveConsumerGatewayUrl = (normalizedBaseUrl: string): string | null => {
  try {
    const url = new URL(normalizedBaseUrl);
    if (url.protocol !== "http:" && url.protocol !== "https:") {
      return null;
    }
    const scheme = url.protocol === "https:" ? "wss" : "ws";
    const basePath = url.pathname.replace(/\/+$/, "");
    return `${scheme}://${url.host}${basePath}${CONSUMER_GATEWAY_WS_PATH}`;
  } catch {
    return null;
  }
};

// ── Integration status ───────────────────────────────────────────

export const ProjectConsumerRuntimeStatusState = Schema.Literals([
  "disabled",
  "connecting",
  "connected",
  "reconnecting",
  "stopped",
]);
export type ProjectConsumerRuntimeStatusState = typeof ProjectConsumerRuntimeStatusState.Type;

/**
 * Live integration status. `detail` (disabled reasons) and `lastServerError`
 * (protocol rejections) are fixed-vocabulary strings — never response bodies
 * or credential material.
 */
export const ProjectConsumerRuntimeStatus = Schema.Struct({
  state: ProjectConsumerRuntimeStatusState,
  detail: Schema.optional(Schema.String),
  lastServerError: Schema.optional(
    Schema.NullOr(Schema.Struct({ code: Schema.String, message: Schema.String })),
  ),
});
export type ProjectConsumerRuntimeStatus = typeof ProjectConsumerRuntimeStatus.Type;

export interface ProjectConsumerRuntimeServiceShape {
  /**
   * Start the consumer runtime for the current settings and keep it
   * reconfigured across settings changes. Runs in the caller's scope; the
   * single SDK runtime is closed when the scope closes.
   */
  readonly start: () => Effect.Effect<void, never, Scope.Scope>;
  readonly getStatus: Effect.Effect<ProjectConsumerRuntimeStatus>;
  /** Exposed for observability/tests; routing goes through the SDK adapter. */
  readonly router: ProjectWorkSessionRouter;
}

export class ProjectConsumerRuntimeService extends Context.Service<
  ProjectConsumerRuntimeService,
  ProjectConsumerRuntimeServiceShape
>()("t3/projectService/ProjectConsumerRuntimeService") {}

/** SDK runtime options tests may inject (fake sockets, deterministic backoff). */
export interface ProjectConsumerRuntimeOverrides {
  readonly socketFactory?: (url: string) => ConsumerSocketLike;
  readonly now?: () => number;
  readonly backoff?: (attempt: number) => number;
  /** Delay before a self-stopped runtime is revived (tests shorten it). */
  readonly revivalDelayMs?: number;
  /** Spacing of the delivery reconcile sweep (tests shorten it). */
  readonly reconcileSweepIntervalMs?: number;
}

/** listAgents cannot answer; the SDK cycles the channel and retries. */
class ProjectConsumerInventoryUnavailableError extends Schema.TaggedErrorClass<ProjectConsumerInventoryUnavailableError>()(
  "ProjectConsumerInventoryUnavailableError",
  {},
) {
  override get message(): string {
    return "Project Service settings could not be read for agent inventory.";
  }
}

const internal = (detail: string) =>
  new ProjectWorkRoutingError({ code: "CONSUMER_INTERNAL", detail });

const dispatchFailure = (error: OrchestrationDispatchError): ProjectWorkRoutingError =>
  internal(`orchestration dispatch rejected the routing command (${error._tag})`);

const workQueryFailure = (
  error: ProjectServiceWorkClient.ProjectServiceWorkClientError,
): ProjectWorkRoutingError => internal(`authoritative Work query failed (${error._tag})`);

/**
 * A notice workspace directory that cannot be normalized is a routing
 * failure (the agent cannot act without a local project), not an internal
 * one: the detail names what the Project Service pointed at.
 */
const workspaceDirFailure = (error: WorkspacePaths.WorkspacePathsError): ProjectWorkRoutingError =>
  new ProjectWorkRoutingError({
    code: "AGENT_NOT_DISPATCHABLE",
    detail:
      error._tag === "WorkspaceRootNotExistsError"
        ? `the workspace directory the Project Service pointed at does not exist on this machine (${error.normalizedWorkspaceRoot})`
        : error._tag === "WorkspaceRootNotDirectoryError"
          ? `the workspace directory the Project Service pointed at is not a directory (${error.normalizedWorkspaceRoot})`
          : `the workspace directory the Project Service pointed at could not be inspected (${error._tag})`,
  });

/**
 * Credential-class Work rejections. The gateway handshake and the Work facet
 * path authenticate independently, so these must surface as integration
 * status — a bare "connected" state while every Work query is rejected
 * would mislead the operator.
 */
const WORK_CREDENTIAL_CODES: ReadonlySet<string> = new Set([
  "PROJECT_CLIENT_AUTHENTICATION_INVALID",
  "PROJECT_CLIENT_AUTHENTICATION_REQUIRED",
  "PROJECT_CLIENT_DISABLED",
]);

const isWorkPathCredentialError = (
  error: ProjectServiceWorkClient.ProjectServiceWorkClientError,
): error is ProjectServiceWorkClient.ProjectServiceWorkServiceRejectedError =>
  error._tag === "ProjectServiceWorkServiceRejectedError" && WORK_CREDENTIAL_CODES.has(error.code);

// ── SDK adapter (plain functions: the SDK owns the promise boundary) ──

/** Agents with project work enabled, advertised as stable id + display name. */
const listProjectConsumerAgents = Effect.fn("ProjectConsumerRuntime.listAgents")(function* (
  serverSettings: ServerSettings.ServerSettingsService["Service"],
) {
  const settings = yield* serverSettings.getSettings.pipe(
    Effect.mapError(() => new ProjectConsumerInventoryUnavailableError()),
  );
  return Object.entries(settings.logicalAgents)
    .filter(([, agent]) => agent.project.enabled)
    .map(([agentId, agent]) => ({ agentId, agentName: agent.agentName }));
});

const routeProjectConsumerWake = Effect.fn("ProjectConsumerRuntime.wakeAgent")(function* (
  router: ProjectWorkSessionRouter,
  input: {
    readonly agentId: string;
    readonly projectId: string;
    readonly projectName?: string;
    readonly workspaceDir?: string;
  },
) {
  yield* router
    .routeWake({
      agentId: input.agentId,
      projectId: input.projectId,
      ...(input.projectName !== undefined ? { projectName: input.projectName } : {}),
      ...(input.workspaceDir !== undefined ? { workspaceDir: input.workspaceDir } : {}),
    })
    .pipe(Effect.mapError((error) => new RuntimeConsumerWakeError(error.code, error.detail)));
});

const consumerAdapter = (
  serverSettings: ServerSettings.ServerSettingsService["Service"],
  router: ProjectWorkSessionRouter,
): RuntimeConsumerAdapter => ({
  listAgents: () => Effect.runPromise(listProjectConsumerAgents(serverSettings)),
  wakeAgent: (input) => Effect.runPromise(routeProjectConsumerWake(router, input)),
});

// ── Runtime status callbacks (module-level: Effect.runSync stays outside Effect code) ──

/**
 * Fire-and-forget an Effect from one of the SDK's synchronous state
 * callbacks (`stopped`, `connected`). Module-level so the Effect boundary
 * stays outside Effect code.
 */
const fireAndForgetFromCallback = (effect: Effect.Effect<void>): void => {
  void Effect.runPromise(effect);
};

const runtimeStatusCallbacks = (
  statusRef: Ref.Ref<ProjectConsumerRuntimeStatus>,
  hooks?: {
    /**
     * The runtime reached `stopped` WITHOUT the service closing it — the SDK's
     * own terminal classifications. The host schedules a revival so the
     * integration converges after e.g. a Project Service relaunch window.
     */
    readonly onSelfStopped: () => void;
    /**
     * The runtime (re)connected — a fresh hello was accepted. The host runs
     * the delivery reconcile sweep promptly instead of waiting for the
     * periodic tick: a reconnect after a restart or outage is exactly when
     * stranded open work needs recovering.
     */
    readonly onConnected?: () => void;
  },
) => ({
  onStateChange: (state: ConsumerRuntimeState) => {
    // Status only; the runtime owns its own reconnect loop: after a socket
    // drop it re-dials on its backoff, and a plain connection loss is never
    // classified terminal.
    Effect.runSync(
      Ref.update(statusRef, (current) => {
        const { state: _state, detail: _detail, ...rest } = current;
        return { ...rest, state };
      }),
    );
    if (state === "stopped") {
      hooks?.onSelfStopped();
    }
    if (state === "connected") {
      hooks?.onConnected?.();
    }
  },
  onServerError: (payload: { readonly code: string; readonly message: string }) => {
    // Surface protocol/credential rejections as integration status; sessions
    // and routing state are never touched. A terminal incompatibility stops
    // the runtime itself (SDK behavior) — the revival hook then heals it.
    Effect.runSync(
      Ref.update(statusRef, (current) => ({
        ...current,
        lastServerError: { code: payload.code, message: payload.message },
      })),
    );
  },
});

type DesiredConnection =
  | {
      readonly kind: "disabled";
      readonly detail: string;
      /** Read-failure closures are expected to be transient and retry. */
      readonly transient: boolean;
    }
  | { readonly kind: "active"; readonly url: string; readonly credential: string };

const make = (overrides?: ProjectConsumerRuntimeOverrides) =>
  Effect.gen(function* () {
    const serverSettings = yield* ServerSettings.ServerSettingsService;
    const secretStore = yield* ServerSecretStore.ServerSecretStore;
    const engine = yield* OrchestrationEngine.OrchestrationEngineService;
    const snapshotQuery = yield* ProjectionSnapshotQuery.ProjectionSnapshotQuery;
    const instanceRegistry = yield* ProviderInstanceRegistry.ProviderInstanceRegistry;
    const workClient = yield* ProjectServiceWorkClient.ProjectServiceWorkClient;
    const workspacePaths = yield* WorkspacePaths.WorkspacePaths;
    // R-free canonicalizer for the router deps seams: the shared helper's
    // FileSystem/Path requirements are satisfied once, here (issue #6 review).
    const fileSystem = yield* FileSystem.FileSystem;
    const pathService = yield* Path.Path;
    const canonicalWorkspaceDir = (directory: string) =>
      canonicalWorkspaceDirectory(directory).pipe(
        Effect.provideService(FileSystem.FileSystem, fileSystem),
        Effect.provideService(Path.Path, pathService),
      );
    const crypto = yield* Crypto.Crypto;

    const statusRef = yield* Ref.make<ProjectConsumerRuntimeStatus>({
      state: "disabled",
      detail: "not-started",
    });

    // A Work-path credential rejection must reach the integration status
    // (the WS handshake may still succeed while facet calls are rejected).
    // Routing sessions are never touched.
    const noteWorkPathCredentialError = (
      error: ProjectServiceWorkClient.ProjectServiceWorkClientError,
    ): Effect.Effect<void> =>
      isWorkPathCredentialError(error)
        ? Ref.update(statusRef, (current) => ({
            ...current,
            lastServerError: {
              code: error.code,
              message: "Project Service rejected the Work-path credential",
            },
          }))
        : Effect.void;

    // ── Session router (seams mapped onto live services) ──────────

    const router = yield* makeProjectWorkSessionRouter({
      readSettings: serverSettings.getSettings.pipe(
        Effect.mapError(() => internal("Project Service settings could not be read")),
      ),
      readThreadShell: (threadId) =>
        snapshotQuery
          .getThreadShellById(threadId)
          .pipe(Effect.mapError(() => internal("thread projection could not be read"))),
      readProjectShell: (projectId) =>
        snapshotQuery
          .getProjectShellById(projectId)
          .pipe(Effect.mapError(() => internal("project projection could not be read"))),
      resolveProviderDriver: (instanceId) =>
        instanceRegistry.getInstance(ProviderInstanceId.make(instanceId)).pipe(
          Effect.mapError(() => internal("provider registry could not be read")),
          Effect.map((instance) =>
            instance !== undefined && instance.enabled
              ? Option.some(instance.driverKind)
              : Option.none(),
          ),
        ),
      dispatchCommand: (command: OrchestrationCommand) =>
        engine.dispatch(command).pipe(Effect.asVoid, Effect.mapError(dispatchFailure)),
      // createIfMissing stays FALSE: a directory the Project Service points
      // at must already exist locally; routing never materializes it. The
      // canonicalization step (realpath + Windows case-fold) mirrors the
      // Project Service's canonical-root key so the notice spelling and the
      // stored workspaceRoots compare as one identity (issue #6 review).
      normalizeWorkspaceDir: (workspaceDir) =>
        workspacePaths.normalizeWorkspaceRoot(workspaceDir).pipe(
          Effect.flatMap((normalized) => canonicalWorkspaceDir(normalized)),
          Effect.mapError(workspaceDirFailure),
        ),
      getActiveProjectByWorkspaceRoot: (workspaceRoot) =>
        snapshotQuery
          .getActiveProjectByWorkspaceRoot(workspaceRoot)
          .pipe(Effect.mapError(() => internal("project projection could not be read"))),
      canonicalizeWorkspaceRoot: (workspaceRoot) => canonicalWorkspaceDir(workspaceRoot),
      listActiveProjectRoots: () =>
        snapshotQuery.getSnapshot().pipe(
          Effect.map((snapshot) =>
            snapshot.projects
              .filter((project) => project.deletedAt === null)
              .map((project) => ({
                projectId: project.id,
                workspaceRoot: project.workspaceRoot,
              })),
          ),
          Effect.mapError(() => internal("project projection could not be read")),
        ),
      createdProjectDefaultModelSelection: getAutoBootstrapDefaultModelSelection(),
      listOpenAssignedWork: (input) =>
        Effect.gen(function* () {
          const projectGeneration = yield* workClient.getProjectGeneration(input.projectId);
          const runs = yield* workClient.listMy({
            projectId: input.projectId,
            projectGeneration,
            agentId: input.agentId,
          });
          return runs.filter((run) => run.state === "open");
        }).pipe(Effect.tapError(noteWorkPathCredentialError), Effect.mapError(workQueryFailure)),
      nowIso: Effect.map(DateTime.now, DateTime.formatIso),
      newId: crypto.randomUUIDv4.pipe(Effect.orDie),
    });

    // ── SDK runtime lifecycle ─────────────────────────────────────

    const runtimeRef = yield* Ref.make<ProjectConsumerRuntime | null>(null);
    const runtimeSignatureRef = yield* Ref.make<string | null>(null);
    const adapter = consumerAdapter(serverSettings, router);

    // Set while the SERVICE closes a runtime itself (replace, disable,
    // finalizer) so the runtime's synchronous `stopped` callback can tell an
    // intentional close from a self-stop. Plain synchronous state: the close
    // and its callback run on one thread, inside the close() call.
    let selfClosing = false;
    const closeRuntime = (runtime: ProjectConsumerRuntime | null): void => {
      if (runtime === null) {
        return;
      }
      selfClosing = true;
      try {
        runtime.close();
      } finally {
        selfClosing = false;
      }
    };

    const resolveDesired: Effect.Effect<DesiredConnection> = Effect.gen(function* () {
      const settings = yield* serverSettings.getSettings.pipe(Effect.orElseSucceed(() => null));
      if (settings === null) {
        return { kind: "disabled", detail: "integration-unreadable", transient: true } as const;
      }
      const client = settings.projectServiceClient;
      if (!client.enabled) {
        return { kind: "disabled", detail: "integration-disabled", transient: false } as const;
      }
      // Defense in depth: write validation already restricts the base URL,
      // but settings.json is editable out of band and the credential in the
      // hello goes to whatever host this URL names.
      if (!isLocalProjectServiceBaseUrl(client.baseUrl)) {
        return { kind: "disabled", detail: "invalid-base-url", transient: false } as const;
      }
      const base = normalizeBaseUrl(client.baseUrl);
      if (base === null) {
        return { kind: "disabled", detail: "invalid-base-url", transient: false } as const;
      }
      if (!client.credentialSet) {
        return { kind: "disabled", detail: "no-credential", transient: false } as const;
      }
      const secret = yield* secretStore
        .get(ServerSettings.PROJECT_SERVICE_CREDENTIAL_SECRET_NAME)
        .pipe(Effect.orElseSucceed(() => Option.none<Uint8Array>()));
      if (Option.isNone(secret)) {
        // credentialSet says a secret exists but it could not be read —
        // a rotation window or transient store failure. Retry.
        return { kind: "disabled", detail: "no-credential", transient: true } as const;
      }
      const url = deriveConsumerGatewayUrl(base);
      if (url === null) {
        return { kind: "disabled", detail: "invalid-base-url", transient: false } as const;
      }
      return {
        kind: "active",
        url,
        credential: new TextDecoder().decode(secret.value),
      } as const;
    });

    // A transient settings/secret read failure must not strand the consumer
    // offline: closures marked transient schedule exactly one delayed
    // re-sync (syncRuntime only re-runs on settings changes otherwise).
    // Deliberate configuration (disabled, no credential configured) never
    // retries. At most one retry is pending at a time.
    const RETRY_SYNC_DELAY = Duration.seconds(10);
    const retryScheduledRef = yield* Ref.make(false);
    const scheduleRetrySync: Effect.Effect<void, never, Scope.Scope> = Effect.gen(function* () {
      if (yield* Ref.getAndSet(retryScheduledRef, true)) {
        return;
      }
      yield* forkParked(
        Effect.sleep(RETRY_SYNC_DELAY).pipe(
          Effect.flatMap(() => Ref.set(retryScheduledRef, false)),
          Effect.flatMap(() => syncRuntime),
        ),
      );
    });

    // Serialized by construction: the initial sync runs to completion inside
    // start(), and settings changes arrive through one stream consumer.
    const syncRuntime: Effect.Effect<void, never, Scope.Scope> = Effect.gen(function* () {
      const desired = yield* resolveDesired;
      if (desired.kind === "disabled") {
        const previous = yield* Ref.getAndSet(runtimeRef, null);
        yield* Ref.set(runtimeSignatureRef, null);
        closeRuntime(previous);
        yield* Ref.set(statusRef, { state: "disabled", detail: desired.detail });
        if (desired.transient) {
          yield* scheduleRetrySync;
        }
        return;
      }
      const signature = `${desired.url}\n${desired.credential}`;
      const currentSignature = yield* Ref.get(runtimeSignatureRef);
      if (currentSignature === signature) {
        return;
      }
      // Exactly one runtime per client: replace the old instance before the
      // new one opens its socket.
      const previous = yield* Ref.getAndSet(runtimeRef, null);
      closeRuntime(previous);
      const runtime = new ProjectConsumerRuntime({
        url: desired.url,
        consumerId: CONSUMER_ID,
        // The hello's client.version feeds the service's SDK compatibility
        // ladder, which compares SDK versions — the t3 package version
        // (0.0.33) semantically ranks below SDK 0.1.0 and shows "unsupported".
        client: { name: "t3-code", version: CONSUMER_CLIENT_LATEST },
        adapter,
        serviceKey: desired.credential,
        runtime: process.version,
        ...(overrides?.socketFactory !== undefined
          ? { socketFactory: overrides.socketFactory }
          : {}),
        ...(overrides?.now !== undefined ? { now: overrides.now } : {}),
        ...(overrides?.backoff !== undefined ? { backoff: overrides.backoff } : {}),
        ...statusCallbacks,
      });
      yield* Ref.set(runtimeRef, runtime);
      yield* Ref.set(runtimeSignatureRef, signature);
      runtime.start();
    }).pipe(
      Effect.catchCause((cause) =>
        Effect.logWarning("Project Consumer runtime sync failed", {
          cause: Cause.pretty(cause),
        }),
      ),
    );

    // A runtime that stopped ITSELF (the SDK's terminal protocol
    // classifications — e.g. a Project Service relaunch window that briefly
    // spoke an incompatible protocol) must not strand the integration
    // offline: a plain connection drop never reaches this path (the SDK
    // re-dials on its own backoff), but a self-stop would leave ZERO
    // connections until a settings change or restart. While the desired
    // connection is active, exactly one delayed re-sync force-rebuilds the
    // runtime; a still-incompatible server simply stops again and the cycle
    // repeats at the same gentle cadence as the transient closures above.
    const layerScope = yield* Effect.scope;
    const revivalPendingRef = yield* Ref.make(false);
    const revivalDelay =
      overrides?.revivalDelayMs !== undefined
        ? Duration.millis(overrides.revivalDelayMs)
        : RETRY_SYNC_DELAY;
    const scheduleRuntimeRevival: Effect.Effect<void> = Effect.gen(function* () {
      if (yield* Ref.getAndSet(revivalPendingRef, true)) {
        return;
      }
      yield* forkParked(
        Effect.sleep(revivalDelay).pipe(
          Effect.flatMap(() => Ref.set(revivalPendingRef, false)),
          // Clear the signature so the re-sync rebuilds even though the
          // desired connection (URL + credential) is unchanged.
          Effect.flatMap(() => Ref.set(runtimeSignatureRef, null)),
          Effect.flatMap(() => syncRuntime),
        ),
      );
    }).pipe(Effect.provideService(Scope.Scope, layerScope));

    // ── Delivery reconcile sweep (flow liveness A1) ───────────────
    // A notice is ACKed the moment it routes; nothing upstream re-fires it.
    // Work can therefore strand silently when the turn-end drain never fired
    // (busy-coalesced work recorded against a turn whose events coalesced
    // past the busy window) or when a restart emptied the in-memory session
    // registry. The sweep re-derives every (Project Service project ×
    // project-enabled logical agent) pair and asks the router to reconcile
    // each against the authoritative open work. It only ever delivers what
    // no live session covers — the router's lastDeliveredHeadRunId invariant
    // keeps the sweep from nagging an agent that was already told.
    const reconcileSweepInterval =
      overrides?.reconcileSweepIntervalMs !== undefined
        ? Duration.millis(overrides.reconcileSweepIntervalMs)
        : RECONCILE_SWEEP_INTERVAL;
    const reconcileSweep: Effect.Effect<void> = Effect.gen(function* () {
      const status = yield* Ref.get(statusRef);
      if (status.state !== "connected") {
        // Nothing to reconcile against while the channel is down; the
        // SDK's reconnect owns getting back here.
        return;
      }
      const agents = yield* listProjectConsumerAgents(serverSettings).pipe(Effect.result);
      if (agents._tag === "Failure") {
        // Settings unreadable right now; the sweep retries on its next tick
        // (and the SDK cycles inventory for the same condition).
        return;
      }
      const agentIds = agents.success.map((agent) => agent.agentId);
      if (agentIds.length === 0) {
        return;
      }
      const projects = yield* workClient.listProjects().pipe(Effect.result);
      if (projects._tag === "Failure") {
        yield* Effect.logWarning(
          "Project Work reconcile sweep could not list Project Service projects",
          { error: projects.failure._tag },
        );
        return;
      }
      for (const project of projects.success) {
        for (const agentId of agentIds) {
          yield* router.reconcileOpenWork({
            agentId,
            projectId: project.projectId,
            projectName: project.name,
            workspaceDir: project.workspaceDir,
          });
        }
      }
    });

    const statusCallbacks = runtimeStatusCallbacks(statusRef, {
      onSelfStopped: () => {
        if (!selfClosing) {
          fireAndForgetFromCallback(scheduleRuntimeRevival);
        }
      },
      onConnected: () => {
        fireAndForgetFromCallback(reconcileSweep);
      },
    });

    const start: ProjectConsumerRuntimeServiceShape["start"] = () =>
      Effect.gen(function* () {
        yield* Effect.addFinalizer(() =>
          Effect.gen(function* () {
            const runtime = yield* Ref.getAndSet(runtimeRef, null);
            yield* Ref.set(runtimeSignatureRef, null);
            closeRuntime(runtime);
          }),
        );
        yield* syncRuntime;
        // Settings reactions: reconfigure the single runtime when the client
        // enablement, base URL, or credential changes. logicalAgents changes
        // need no restart — inventory is answered live from settings.
        const changes = yield* serverSettings.subscribeChanges;
        yield* forkParked(Stream.runForEach(changes, () => syncRuntime));
        // Turn-finish observation: the engine's domain events drive the
        // coalesced post-turn aggregate deliveries.
        yield* forkParked(
          Stream.runForEach(engine.streamDomainEvents, (event) =>
            event.aggregateKind === "thread"
              ? router.onThreadEvent(ThreadId.make(event.aggregateId))
              : Effect.void,
          ),
        );
        // Delivery reconcile sweep (flow liveness A1): a periodic pass that
        // repairs stranded open work. The first execution runs before the
        // channel is up and no-ops on the status guard; every (re)connection
        // also triggers a prompt pass through the status callback above.
        yield* forkParked(
          reconcileSweep.pipe(
            Effect.catchCause((cause) =>
              Effect.logWarning("Project Work reconcile sweep failed", {
                cause: Cause.pretty(cause),
              }),
            ),
            Effect.repeat(Schedule.spaced(reconcileSweepInterval)),
          ),
        );
      });

    return {
      start,
      getStatus: Ref.get(statusRef),
      router,
    } satisfies ProjectConsumerRuntimeServiceShape;
  });

export const layerWithOptions = (overrides?: ProjectConsumerRuntimeOverrides) =>
  Layer.effect(ProjectConsumerRuntimeService, make(overrides));

export const layer = layerWithOptions();
