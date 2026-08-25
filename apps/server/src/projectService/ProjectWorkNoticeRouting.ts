/**
 * Session routing for Project Service Work notices (issues #4 and #6).
 *
 * Routing policy plus an in-memory per-(agentId, projectId) session state
 * machine. A notice is a TRIGGER, never a verbatim payload: every delivered
 * message is derived from an authoritative Work query at delivery time (the
 * queue-head wake message from `AssignedWorkQueue.ts`). The local pending set is debounce/dedup
 * state only — Work lifecycle is never mirrored into T3.
 *
 * The routing key is `logicalAgentId + Project Service projectId` — plus the
 * flow-instance key when the agent's session scope is "flow-instance", which
 * gives each instance's work its own session; names in either direction are
 * display metadata and never participate in the mapping.
 * The T3 project is resolved from the notice's WORKSPACE DIRECTORY: the
 * active local project keyed by that directory is reused, and a missing one
 * is created on the spot (issue #6's auto-reuse/auto-create rule), so no
 * per-agent project configuration exists. The vendored SDK runtime
 * deduplicates noticeIds (same-notice replays never re-wake), so this module
 * coalesces at the routing-key level only: many distinct notices for one key
 * collapse into at most one aggregate notification per busy period.
 *
 * Flow liveness: a notice is ACKed the moment it routes, so the router itself
 * repairs what the event-driven path can strand. The turn-end drain reminds
 * an agent exactly ONCE when it finished the aggregate turn without
 * submitting the still-open head (the "finished and stopped" case), and the
 * reconcile sweep (`reconcileOpenWork`) re-delivers open work that no live
 * session covers — a restart that emptied the registry, a dead session
 * thread, or a turn-end event the drain never saw. Both preserve the no-nag
 * invariant through `lastDeliveredHeadRunId`: a head the session already
 * delivered is never re-delivered (a restart's fresh state is the one
 * deliberate re-delivery, because the ACKed notice will never re-fire).
 *
 * @module ProjectWorkNoticeRouting
 */
import {
  CommandId,
  DEFAULT_MODEL,
  DEFAULT_MODEL_BY_PROVIDER,
  type LogicalAgentId,
  MessageId,
  type ModelSelection,
  type OrchestrationCommand,
  type OrchestrationProject,
  type OrchestrationProjectShell,
  type OrchestrationThreadShell,
  ProjectId,
  type ProjectWorkSessionScope,
  type ProviderDriverKind,
  type ServerSettings,
  ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";

import {
  type AssignedWorkQueueEntry,
  assignedWorkWakeMessage,
  currentAssignedWork,
  flowInstanceNameOf,
  orderAssignedWorkQueue,
  partitionOpenWork,
  runsForFlowInstance,
} from "./AssignedWorkQueue.ts";

const DEFAULT_ROUTING_RUNTIME_MODE = "full-access" as const;
const DEFAULT_ROUTING_INTERACTION_MODE = "default" as const;

// ── Session worktree binding ────────────────────────────────────

/**
 * The execution workspace a work session should run in for this queue head:
 * the Project Service's managed worktree path when one is bound to the run,
 * else null (the session then runs in the project root — the same fallback
 * `resolveThreadWorkspaceCwd` applies to unbound threads). Absent facts are
 * UNKNOWN, never project-root: a run whose policy is not
 * `managed-worktree` (older server, degraded registry read, or standalone
 * work) stays unbound rather than pinning a guessed path.
 */
export const worktreeBindingFor = (current: AssignedWorkQueueEntry | null): string | null => {
  if (current === null || current.workspacePolicy !== "managed-worktree") return null;
  const path = current.workspacePath?.trim() ?? "";
  return path.length > 0 ? path : null;
};

// ── Errors ───────────────────────────────────────────────────────

/** Routing failures carry the Consumer wire's failure-code vocabulary. */
export class ProjectWorkRoutingError extends Schema.TaggedErrorClass<ProjectWorkRoutingError>()(
  "ProjectWorkRoutingError",
  {
    code: Schema.Literals(["AGENT_NOT_FOUND", "AGENT_NOT_DISPATCHABLE", "CONSUMER_INTERNAL"]),
    detail: Schema.String,
  },
) {
  override get message(): string {
    return `${this.code}: ${this.detail}`;
  }
}

// ── Settings-level routing resolution ────────────────────────────

export interface ProjectWorkRoutingTarget {
  readonly logicalAgentId: LogicalAgentId;
  readonly agentName: string;
  readonly providerInstanceId: string;
  /** Agent-level reasoning effort; null follows the model default. */
  readonly thinkLevel: string | null;
  /** Agent-level model override; null follows the usual resolution. */
  readonly modelOverride: ModelSelection | null;
  /** The Project Service project the notice named. */
  readonly projectServiceProjectId: string;
  /** Display name from the NOTICE (never a local binding); may be blank. */
  readonly projectName: string;
  /** The local project resolved for the notice's workspace directory. */
  readonly t3ProjectId: ProjectId;
  /** The agent's configured session scope for this work. */
  readonly sessionScope: ProjectWorkSessionScope;
}

/** Agent-level routing facts before the workspace directory is resolved. */
export interface ProjectWorkAgentRouting {
  readonly logicalAgentId: LogicalAgentId;
  readonly agentName: string;
  readonly providerInstanceId: string;
  readonly thinkLevel: string | null;
  readonly modelOverride: ModelSelection | null;
  readonly projectServiceProjectId: string;
  readonly projectName: string;
  readonly sessionScope: ProjectWorkSessionScope;
}

export type ProjectWorkRoutingResolution =
  | { readonly ok: true; readonly routing: ProjectWorkAgentRouting }
  | { readonly ok: false; readonly code: ProjectWorkRoutingError["code"]; readonly detail: string };

/**
 * Map `agentId` onto the configured agent and carry the notice's project
 * facts. Reads stay structural (duplicates and unknowns resolve to failures,
 * never crashes) because settings.json can be hand-edited between
 * validations. The notice's `workspaceDir` presence is checked here too: a
 * pre-V4 server never sends one, and without it no local project can be
 * resolved.
 */
export const resolveProjectWorkRouting = (
  settings: ServerSettings,
  input: ProjectWorkWakeInput,
): ProjectWorkRoutingResolution => {
  if (!settings.projectServiceClient.enabled) {
    return {
      ok: false,
      code: "AGENT_NOT_FOUND",
      detail: "the Project Service integration is disabled",
    };
  }
  const agent = Object.entries(settings.logicalAgents).find(([id]) => id === input.agentId);
  if (agent === undefined) {
    return {
      ok: false,
      code: "AGENT_NOT_FOUND",
      detail: `agent ${input.agentId} is not configured`,
    };
  }
  const [logicalAgentId, agentConfig] = agent;
  if (!agentConfig.project.enabled) {
    return {
      ok: false,
      code: "AGENT_NOT_FOUND",
      detail: `project work is disabled for agent ${input.agentId}`,
    };
  }
  if (input.workspaceDir === undefined || input.workspaceDir.trim().length === 0) {
    return {
      ok: false,
      code: "AGENT_NOT_DISPATCHABLE",
      detail:
        `the Project Service notice for project ${input.projectId} carried no workspace directory; ` +
        "routing by workspace directory requires Project Service protocol V4 (consumer SDK 0.5.0)",
    };
  }
  return {
    ok: true,
    routing: {
      logicalAgentId: logicalAgentId as LogicalAgentId,
      agentName: agentConfig.agentName,
      providerInstanceId: agentConfig.providerInstanceId,
      thinkLevel: agentConfig.thinkLevel,
      modelOverride: agentConfig.modelOverride,
      projectServiceProjectId: input.projectId,
      projectName: input.projectName ?? "",
      sessionScope: agentConfig.project.sessionScope,
    },
  };
};

// ── Thread classification ────────────────────────────────────────

/**
 * A session is busy while a turn runs or a provider session is starting/
 * running. This mirrors the client's own activity derivation: leaving the
 * "running" session status is the authoritative turn-end signal.
 */
export const isWorkThreadBusy = (shell: OrchestrationThreadShell): boolean =>
  (shell.session !== null &&
    (shell.session.status === "starting" || shell.session.status === "running")) ||
  shell.latestTurn?.state === "running";

/** Session statuses that mean a notification turn can never still engage. */
const TERMINAL_SESSION_STATUSES: ReadonlySet<string> = new Set(["stopped", "error", "interrupted"]);

/**
 * Whether the thread's latest turn PROVABLY ran and settled AFTER the last
 * aggregate this session delivered — the shell-fact form of the event-driven
 * `seenBusy` observation. The projector writes `latestTurn` only when a
 * session starts running a turn (a queued request leaves the PREVIOUS turn
 * in place), so a settled latestTurn alone cannot mean "our turn finished":
 * only one that settled strictly after our dispatch timestamp can. This is
 * what survives a coalesced event burst that never let the router observe
 * the busy window — the busy-coalesce stranding — and lets the next event
 * (or the reconcile sweep) perform the missed drain.
 */
const workThreadTurnSettledAfterDelivery = (
  shell: OrchestrationThreadShell,
  deliveredAtIso: string | null,
): boolean => {
  if (deliveredAtIso === null) {
    return false;
  }
  const latest = shell.latestTurn;
  if (
    latest === null ||
    latest.startedAt === null ||
    latest.state === "running" ||
    latest.completedAt === null
  ) {
    return false;
  }
  const settledAt = Date.parse(latest.completedAt);
  const deliveredAt = Date.parse(deliveredAtIso);
  return !Number.isNaN(settledAt) && !Number.isNaN(deliveredAt) && settledAt > deliveredAt;
};

/** Archived sessions are no longer current (deleted ones read as absent). */
export const isWorkThreadCurrent = (shell: OrchestrationThreadShell): boolean =>
  shell.archivedAt === null;

// ── Aggregate notification ───────────────────────────────────────

/**
 * The provider-option id each driver reads for reasoning effort. Drivers
 * without a known effort option (grok, opencode) get none — thinkLevel then
 * only applies where it can be honored.
 */
const EFFORT_OPTION_ID_BY_DRIVER: Readonly<Record<string, string>> = {
  claudeAgent: "effort",
  cursor: "reasoning",
  codex: "reasoningEffort",
};

/**
 * Add the agent's thinkLevel as the driver's effort option unless the
 * selection already carries one (explicit beats generic). Returns null when
 * there is nothing to carry so callers can omit `options` entirely.
 */
export const applyThinkLevelToOptions = (
  options: ModelSelection["options"] | undefined,
  thinkLevel: string | null,
  driverKind: string,
): ModelSelection["options"] | null => {
  const effortOptionId = EFFORT_OPTION_ID_BY_DRIVER[driverKind];
  const level = thinkLevel?.trim() ?? "";
  if (effortOptionId === undefined || level.length === 0) {
    return options ?? null;
  }
  const existing = options ?? [];
  if (existing.some((option) => option.id === effortOptionId)) {
    return existing;
  }
  return [...existing, { id: effortOptionId, value: level }];
};

/**
 * Title for sessions this integration creates; display-only. The agent name
 * alone — the T3 UI already groups sessions by project, so the project half
 * would only duplicate the grouping.
 */
export const workSessionThreadTitle = (agentName: string): string => agentName.trim();

/**
 * Title for a flow-instance-scoped session: the agent name plus the
 * instance's name, because several such sessions share one project and the
 * title is the only human-visible discriminator between them. A blank
 * instance name falls back to the agent name alone (the title must stay
 * non-empty).
 */
export const flowInstanceWorkSessionThreadTitle = (
  agentName: string,
  instanceName: string | null,
): string => (instanceName === null ? agentName.trim() : `${agentName.trim()} · ${instanceName}`);

// ── Router ───────────────────────────────────────────────────────

/** What a wake carries into the router; SDK notice facts minus transport detail. */
export interface ProjectWorkWakeInput {
  readonly agentId: string;
  readonly projectId: string;
  /** Display name from the notice; absent when the authority had none. */
  readonly projectName?: string;
  /**
   * The notice's workspace directory — the lookup key for the local project
   * (issue #6). Absent on a pre-V4 server; routing then fails with a detail
   * naming the requirement.
   */
  readonly workspaceDir?: string;
}

/** Seams the router needs from its host; failures carry the wire failure code. */
export interface ProjectWorkSessionRouterDeps {
  readonly readSettings: Effect.Effect<ServerSettings, ProjectWorkRoutingError>;
  readonly readThreadShell: (
    threadId: ThreadId,
  ) => Effect.Effect<Option.Option<OrchestrationThreadShell>, ProjectWorkRoutingError>;
  readonly readProjectShell: (
    projectId: ProjectId,
  ) => Effect.Effect<Option.Option<OrchestrationProjectShell>, ProjectWorkRoutingError>;
  /** `None` when the instance is unknown to the provider registry. */
  readonly resolveProviderDriver: (
    instanceId: string,
  ) => Effect.Effect<Option.Option<ProviderDriverKind>, ProjectWorkRoutingError>;
  readonly dispatchCommand: (
    command: OrchestrationCommand,
  ) => Effect.Effect<void, ProjectWorkRoutingError>;
  /**
   * Normalize AND canonicalize a notice workspace directory WITHOUT creating
   * it: a directory the Project Service points at must already exist on this
   * machine, and the returned value is the canonical lookup key (realpath +
   * Windows case-fold, mirroring the Project Service's canonical-root key) —
   * also the workspaceRoot auto-created projects persist.
   */
  readonly normalizeWorkspaceDir: (
    workspaceDir: string,
  ) => Effect.Effect<string, ProjectWorkRoutingError>;
  /** The active local project keyed by a normalized workspace root. */
  readonly getActiveProjectByWorkspaceRoot: (
    workspaceRoot: string,
  ) => Effect.Effect<Option.Option<OrchestrationProject>, ProjectWorkRoutingError>;
  /**
   * Canonicalize a STORED workspace root for key comparison (issue #6 review):
   * legacy roots may carry symlink spellings the exact-root query cannot see.
   * Best-effort; never fails.
   */
  readonly canonicalizeWorkspaceRoot: (workspaceRoot: string) => Effect.Effect<string>;
  /**
   * Active local projects (id + stored root) for the canonical-key reuse scan
   * that backs the exact-root query.
   */
  readonly listActiveProjectRoots: () => Effect.Effect<
    ReadonlyArray<{ readonly projectId: ProjectId; readonly workspaceRoot: string }>,
    ProjectWorkRoutingError
  >;
  /** Default model selection for auto-created projects (bootstrap precedent). */
  readonly createdProjectDefaultModelSelection: ModelSelection;
  /**
   * Authoritative assigned Work runs for the agent+project, queried now. The
   * wake message derives the queue head (and depth) from this answer at
   * delivery time — a notice is still a trigger, never a payload.
   */
  readonly listOpenAssignedWork: (
    input: ProjectWorkWakeInput,
  ) => Effect.Effect<ReadonlyArray<AssignedWorkQueueEntry>, ProjectWorkRoutingError>;
  readonly nowIso: Effect.Effect<string>;
  readonly newId: Effect.Effect<string>;
}

export interface ProjectWorkSessionSnapshot {
  readonly agentId: string;
  readonly projectId: string;
  /** The flow instance this session owns (null = the whole project queue). */
  readonly flowInstanceKey: string | null;
  readonly threadId: ThreadId;
  readonly phase: "idle" | "notifying";
  readonly pendingWork: boolean;
  /** The worktree the session's thread is bound to (null = project root). */
  readonly boundWorktreePath: string | null;
}

export interface ProjectWorkSessionRouter {
  /**
   * Route one notice by agentId + projectId. Resolving means T3 accepted and
   * routed the notice (session created, notification delivered, or work
   * recorded against a busy session) — never that the agent ran the work.
   */
  readonly routeWake: (input: ProjectWorkWakeInput) => Effect.Effect<void, ProjectWorkRoutingError>;
  /**
   * Re-evaluate a thread after an orchestration event: a finished turn with
   * recorded work delivers at most one aggregate notification. Never fails.
   */
  readonly onThreadEvent: (threadId: ThreadId) => Effect.Effect<void>;
  /**
   * One reconcile pass for a (agentId, projectId) routing key (flow liveness
   * A1): re-derive coverage from the authoritative open work and repair what
   * the event-driven path can strand — no session at all (a restart emptied
   * the registry while the notice was already ACKed upstream), a dead or
   * archived session thread, a missed turn-end drain, or a queue head that
   * advanced past the last delivered aggregate. Serialized exactly like
   * `routeWake` and never fails; a head the session already delivered is
   * never re-delivered (the no-nag invariant).
   */
  readonly reconcileOpenWork: (input: ProjectWorkWakeInput) => Effect.Effect<void>;
  /** Test/observability view of the current-session registry. */
  readonly snapshotSessions: Effect.Effect<ReadonlyArray<ProjectWorkSessionSnapshot>>;
}

/**
 * One current session per `agentId + projectId`. `notifying` spans the window
 * from dispatching the aggregate turn until its turn is observed finished, so
 * the queued-turn gap (command committed, provider session not yet started)
 * still counts as busy and no second notification can be dispatched into it.
 */
interface RoutedSession {
  readonly threadId: ThreadId;
  /** The wake facts this session was routed under (re-resolution input). */
  readonly input: ProjectWorkWakeInput;
  /**
   * The run universe this session owns, fixed at creation: null = every run
   * of the (agent, project) queue (project scope); a string = one flow
   * instance's runs, with "" as the legacy bucket for runs without instance
   * identity. A session only drives while the agent's CONFIGURED scope
   * matches this universe — the universe guard in processThreadEvent parks
   * it the moment a settings toggle moves the work to other sessions.
   */
  readonly flowInstanceKey: string | null;
  phase: "idle" | "notifying";
  pendingWork: boolean;
  /**
   * Whether the current notification turn was observed busy. The aggregate's
   * turn only counts as finished after the thread engaged (or its session
   * proved idle without it), never during the queued-turn window between
   * dispatch and session start — that gap would otherwise let a second
   * notification dispatch on top of the queued first one.
   */
  seenBusy: boolean;
  /**
   * The worktree the session's thread is bound to (null = project root).
   * The binding follows the queue head's execution workspace: when the
   * current work moves to a different managed worktree, the thread is
   * re-bound in place and its session restarts in the new cwd next turn.
   */
  boundWorktreePath: string | null;
  /**
   * The head run the last DELIVERED aggregate named (null before the first
   * delivery). Drives the post-turn drain: notices are per-run and never
   * re-fire when the head rotates, so the router itself advances the queue
   * once the head the agent was last told about has moved on. A head equal
   * to this value is never re-delivered by a notice or by the reconcile
   * sweep — an agent that has not submitted its current work is reminded at
   * most once instead (`remindedHeadRunId`).
   */
  lastDeliveredHeadRunId: string | null;
  /**
   * When the last DELIVERED aggregate was dispatched (null before the first
   * delivery). The queued-turn guard's shell-fact escape: a latestTurn that
   * settled strictly after this timestamp proves OUR turn ran and finished
   * even when the busy window was never observed (coalesced event burst).
   */
  lastDeliveredAtIso: string | null;
  /**
   * The head run the ONE turn-end reminder already named (null = no reminder
   * spent for the current head). When the aggregate turn finishes with the
   * same head still open, the agent ended its turn without submitting: the
   * drain delivers exactly one reminder for that run. A head change resets
   * the budget — the new head gets its own single reminder.
   */
  remindedHeadRunId: string | null;
}

/**
 * The registry key: opaque, never parsed back (snapshots read their facts
 * off the RoutedSession). Project scope is the two-segment key; a flow
 * instance appends its key as a third segment ("" — the legacy bucket —
 * yields a trailing separator, still distinct from the plain key).
 */
const routingKeyOf = (agentId: string, projectId: string, flowInstanceKey: string | null): string =>
  flowInstanceKey === null
    ? `${agentId}::${projectId}`
    : `${agentId}::${projectId}::${flowInstanceKey}`;

export const makeProjectWorkSessionRouter = Effect.fn("makeProjectWorkSessionRouter")(function* (
  deps: ProjectWorkSessionRouterDeps,
): Effect.fn.Return<ProjectWorkSessionRouter> {
  const sessionsRef = yield* Ref.make(new Map<string, RoutedSession>());

  // One in-flight routing pass per key. The SDK may deliver notices
  // concurrently (replay bursts deliver several back-to-back), and every pass
  // spans async seams — the authoritative Work query and orchestration
  // dispatch. Without serialization, two passes for one key interleave at
  // those seams and create duplicate sessions or stack duplicate aggregates.
  // A late pass simply observes the post-dispatch state instead of racing it.
  const keySemaphoresRef = yield* Ref.make(new Map<string, Semaphore.Semaphore>());
  const withKeySerialization = <A, E, R>(
    key: string,
    self: Effect.Effect<A, E, R>,
  ): Effect.Effect<A, E, R> =>
    Effect.flatMap(
      Ref.modify(keySemaphoresRef, (semaphores) => {
        const existing = semaphores.get(key);
        if (existing !== undefined) {
          return [existing, semaphores];
        }
        const created = Semaphore.makeUnsafe(1);
        semaphores.set(key, created);
        return [created, semaphores];
      }),
      (semaphore) => semaphore.withPermits(1)(self),
    );

  const getSession = (key: string): Effect.Effect<RoutedSession | undefined> =>
    Ref.get(sessionsRef).pipe(Effect.map((sessions) => sessions.get(key)));

  const mutateSession = (
    key: string,
    mutate: (session: RoutedSession) => RoutedSession,
  ): Effect.Effect<void> =>
    Ref.modify(sessionsRef, (sessions) => {
      const session = sessions.get(key);
      if (session !== undefined) {
        sessions.set(key, mutate(session));
      }
      return [undefined, sessions];
    });

  const putSession = (key: string, session: RoutedSession): Effect.Effect<void> =>
    Ref.modify(sessionsRef, (sessions) => {
      sessions.set(key, session);
      return [undefined, sessions];
    });

  const dropSession = (key: string): Effect.Effect<void> =>
    Ref.modify(sessionsRef, (sessions) => {
      sessions.delete(key);
      return [undefined, sessions];
    });

  // The model a created session runs on: the agent's provider instance is
  // fixed by routing; the model (and its provider options — an override or
  // project default carrying an effort selection keeps it) falls back from
  // the agent's override to the project's default (same-instance guard) to
  // the driver default. `thinkLevel` is the agent-level effort knob: it is
  // applied as the option id the driver reads, and only when the resolved
  // selection does not already carry one — an explicit option is the more
  // specific intent and wins.
  const resolveModelSelection = Effect.fn("ProjectWorkSessionRouter.resolveModelSelection")(
    function* (
      target: ProjectWorkRoutingTarget,
      project: OrchestrationProjectShell,
    ): Effect.fn.Return<ModelSelection, ProjectWorkRoutingError> {
      const driverKind = yield* deps.resolveProviderDriver(target.providerInstanceId);
      if (Option.isNone(driverKind)) {
        return yield* new ProjectWorkRoutingError({
          code: "AGENT_NOT_DISPATCHABLE",
          detail: `provider instance ${target.providerInstanceId} for agent ${target.logicalAgentId} is not available`,
        });
      }
      const override =
        target.modelOverride !== null &&
        target.modelOverride.instanceId === target.providerInstanceId
          ? target.modelOverride
          : null;
      const projectDefault =
        project.defaultModelSelection !== null &&
        project.defaultModelSelection.instanceId === target.providerInstanceId
          ? project.defaultModelSelection
          : null;
      const base = override ?? projectDefault;
      const model = base?.model ?? DEFAULT_MODEL_BY_PROVIDER[driverKind.value] ?? DEFAULT_MODEL;
      const appliedOptions =
        applyThinkLevelToOptions(base?.options, target.thinkLevel, driverKind.value) ?? null;
      return {
        instanceId: target.providerInstanceId as ModelSelection["instanceId"],
        model,
        ...(appliedOptions !== null ? { options: appliedOptions } : {}),
      } satisfies ModelSelection;
    },
  );

  /**
   * Canonical-key reuse scan (issue #6 review): a STORED workspace root may
   * carry a symlink spelling (/tmp vs /private/tmp) the exact-root query
   * cannot see, which would fork a second project for the same physical
   * directory. `workspaceRoot` arrives already canonical (normalizeWorkspaceDir),
   * so canonicalizing each stored root and comparing keys finds those too.
   */
  const findProjectByCanonicalRoot = Effect.fn(
    "ProjectWorkSessionRouter.findProjectByCanonicalRoot",
  )(function* (
    workspaceRoot: string,
  ): Effect.fn.Return<Option.Option<ProjectId>, ProjectWorkRoutingError> {
    const actives = yield* deps.listActiveProjectRoots();
    for (const active of actives) {
      const key = yield* deps.canonicalizeWorkspaceRoot(active.workspaceRoot);
      if (key === workspaceRoot) {
        return Option.some(active.projectId);
      }
    }
    return Option.none();
  });

  /**
   * Resolve the T3 project for a normalized workspace root: REUSE the active
   * project keyed by the directory, or CREATE one when none exists. A
   * concurrent wake for a different Project Service project on the same
   * directory can win the create; the engine's active-workspaceRoot-taken
   * invariant then fails this dispatch and the winner is reused instead of
   * failing the wake. `create: false` (deferred event paths) resolves
   * reuse-only so no project is ever created outside a wake.
   */
  const resolveProjectForRoot = Effect.fn("ProjectWorkSessionRouter.resolveProjectForRoot")(
    function* (
      agentRouting: ProjectWorkAgentRouting,
      workspaceRoot: string,
      create: boolean,
    ): Effect.fn.Return<ProjectId, ProjectWorkRoutingError> {
      const existing = yield* deps.getActiveProjectByWorkspaceRoot(workspaceRoot);
      if (Option.isSome(existing)) {
        return existing.value.id;
      }
      // Symlink-spelled stored roots reuse here too — before any create.
      const canonicalExisting = yield* findProjectByCanonicalRoot(workspaceRoot);
      if (Option.isSome(canonicalExisting)) {
        return canonicalExisting.value;
      }
      if (!create) {
        return yield* new ProjectWorkRoutingError({
          code: "AGENT_NOT_DISPATCHABLE",
          detail: `no T3 project exists for workspace root ${workspaceRoot} of Project Service project ${agentRouting.projectServiceProjectId}`,
        });
      }
      const t3ProjectId = ProjectId.make(yield* deps.newId);
      // The auto-created project's title reuses the Project Service project's
      // name verbatim (falling back to its stable id on a name-less notice) —
      // one naming authority, no directory-derived alternatives.
      const noticeName = agentRouting.projectName.trim();
      const title = noticeName.length > 0 ? noticeName : agentRouting.projectServiceProjectId;
      const created = yield* deps
        .dispatchCommand({
          type: "project.create",
          commandId: CommandId.make(yield* deps.newId),
          projectId: t3ProjectId,
          title,
          workspaceRoot,
          defaultModelSelection: deps.createdProjectDefaultModelSelection,
          createdAt: yield* deps.nowIso,
        })
        .pipe(Effect.result);
      if (created._tag === "Failure") {
        // The directory was taken between the query and the create (the
        // active-workspaceRoot-taken invariant): reuse whatever won. Any other
        // failure with no project at the root propagates to the wake ACK.
        const raced = yield* deps.getActiveProjectByWorkspaceRoot(workspaceRoot);
        if (Option.isSome(raced)) {
          return raced.value.id;
        }
        const racedCanonical = yield* findProjectByCanonicalRoot(workspaceRoot);
        if (Option.isSome(racedCanonical)) {
          return racedCanonical.value;
        }
        return yield* created.failure;
      }
      // The dispatch transaction projects the project; read it back to prove
      // the persisted state before a session is created under it.
      const shell = yield* deps.readProjectShell(t3ProjectId);
      if (Option.isNone(shell)) {
        return yield* new ProjectWorkRoutingError({
          code: "CONSUMER_INTERNAL",
          detail: `T3 project created for workspace root ${workspaceRoot} could not be read back`,
        });
      }
      return t3ProjectId;
    },
  );

  // Agent eligibility (settings) plus the workspace-directory project
  // resolution, lifted onto the routing target.
  const resolveRoutingTarget = Effect.fn("ProjectWorkSessionRouter.resolveRoutingTarget")(
    function* (
      input: ProjectWorkWakeInput,
      options?: { readonly create: boolean },
    ): Effect.fn.Return<ProjectWorkRoutingTarget, ProjectWorkRoutingError> {
      const settings = yield* deps.readSettings;
      const resolution = resolveProjectWorkRouting(settings, input);
      if (!resolution.ok) {
        return yield* new ProjectWorkRoutingError({
          code: resolution.code,
          detail: resolution.detail,
        });
      }
      const workspaceRoot = yield* deps.normalizeWorkspaceDir(input.workspaceDir as string);
      const t3ProjectId = yield* resolveProjectForRoot(
        resolution.routing,
        workspaceRoot,
        options?.create ?? true,
      );
      return {
        logicalAgentId: resolution.routing.logicalAgentId,
        agentName: resolution.routing.agentName,
        providerInstanceId: resolution.routing.providerInstanceId,
        thinkLevel: resolution.routing.thinkLevel,
        modelOverride: resolution.routing.modelOverride,
        projectServiceProjectId: resolution.routing.projectServiceProjectId,
        projectName: resolution.routing.projectName,
        t3ProjectId,
        sessionScope: resolution.routing.sessionScope,
      };
    },
  );

  // Deliver the aggregate for runs the caller already holds, SESSION-SCOPED
  // (the open-work count always comes from an authoritative query; flow
  // instance sessions pass their partition, project sessions the whole list).
  // Resolves to the dispatch timestamp when a notification turn was actually
  // dispatched (null when there was nothing to say) — callers record it so
  // the queued-turn guard can tell OUR turn's settlement from a previous
  // turn's.
  const deliverAggregate = Effect.fn("ProjectWorkSessionRouter.deliverAggregate")(function* (
    target: ProjectWorkRoutingTarget,
    threadId: ThreadId,
    runs: ReadonlyArray<AssignedWorkQueueEntry>,
  ): Effect.fn.Return<string | null, ProjectWorkRoutingError> {
    const ordered = orderAssignedWorkQueue(runs);
    const current = ordered.at(0);
    if (current === undefined) {
      // The authoritative query no longer shows open work: nothing to
      // say. Accept silently instead of notifying about zero items.
      return null;
    }
    const createdAt = yield* deps.nowIso;
    yield* deps.dispatchCommand({
      type: "thread.turn.start",
      commandId: CommandId.make(yield* deps.newId),
      threadId,
      message: {
        messageId: MessageId.make(yield* deps.newId),
        role: "user",
        text: assignedWorkWakeMessage({ current, queued: ordered.length - 1 }),
        attachments: [],
      },
      runtimeMode: DEFAULT_ROUTING_RUNTIME_MODE,
      interactionMode: DEFAULT_ROUTING_INTERACTION_MODE,
      createdAt,
    });
    return createdAt;
  });

  // Creates the next current session and delivers the aggregate as its
  // first turn. Used for missing sessions and archived ones (an archived
  // session is no longer current, so a NEW session becomes it). `runs`,
  // when the caller already holds the authoritative query, must already be
  // scoped to `flowInstanceKey`.
  const createCurrentSession = Effect.fn("ProjectWorkSessionRouter.createCurrentSession")(
    function* (
      key: string,
      input: ProjectWorkWakeInput,
      target: ProjectWorkRoutingTarget,
      flowInstanceKey: string | null,
      knownRuns?: ReadonlyArray<AssignedWorkQueueEntry>,
    ): Effect.fn.Return<void, ProjectWorkRoutingError> {
      const project = yield* deps.readProjectShell(target.t3ProjectId);
      if (Option.isNone(project)) {
        return yield* new ProjectWorkRoutingError({
          code: "AGENT_NOT_DISPATCHABLE",
          detail: `T3 project ${target.t3ProjectId} resolved for Project Service project ${target.projectServiceProjectId} does not exist`,
        });
      }
      const openRuns =
        knownRuns ??
        runsForFlowInstance(
          yield* deps.listOpenAssignedWork({
            agentId: target.logicalAgentId,
            projectId: target.projectServiceProjectId,
          }),
          flowInstanceKey,
        );
      if (currentAssignedWork(openRuns) === null) {
        // No session is needed for work that no longer exists.
        return;
      }
      const ordered = orderAssignedWorkQueue(openRuns);
      const binding = worktreeBindingFor(ordered[0] ?? null);
      const modelSelection = yield* resolveModelSelection(target, project.value);
      const threadId = ThreadId.make(yield* deps.newId);
      const createdAt = yield* deps.nowIso;
      yield* deps.dispatchCommand({
        type: "thread.create",
        commandId: CommandId.make(yield* deps.newId),
        threadId,
        projectId: target.t3ProjectId,
        title:
          flowInstanceKey === null
            ? workSessionThreadTitle(target.agentName)
            : flowInstanceWorkSessionThreadTitle(
                target.agentName,
                ordered[0] !== undefined ? flowInstanceNameOf(ordered[0]) : null,
              ),
        logicalAgentId: target.logicalAgentId,
        modelSelection,
        runtimeMode: DEFAULT_ROUTING_RUNTIME_MODE,
        interactionMode: DEFAULT_ROUTING_INTERACTION_MODE,
        branch: null,
        worktreePath: binding,
        createdAt,
      });
      // The created session is current from the moment the thread exists;
      // an aggregate delivery failure leaves it idle so a redelivered
      // notice resumes on this session instead of creating another.
      yield* putSession(key, {
        threadId,
        input,
        flowInstanceKey,
        phase: "idle",
        pendingWork: false,
        seenBusy: false,
        boundWorktreePath: binding,
        lastDeliveredHeadRunId: null,
        lastDeliveredAtIso: null,
        remindedHeadRunId: null,
      });
      const deliveredAt = yield* deliverAggregate(target, threadId, ordered);
      yield* mutateSession(key, (session) => ({
        ...session,
        phase: deliveredAt !== null ? "notifying" : "idle",
        seenBusy: false,
        lastDeliveredAtIso: deliveredAt,
        lastDeliveredHeadRunId:
          deliveredAt !== null && ordered[0] !== undefined
            ? ordered[0].runId
            : session.lastDeliveredHeadRunId,
      }));
    },
  );

  // Deliver at most one aggregate for recorded work. Called with the
  // pending flag already set; the deferred variant (after a turn) has no
  // ACK path, so its failures are contained here. Also re-binds the
  // session's thread when the current work's execution workspace moved
  // (same managed worktree keeps the session; a different one follows it
  // in place — thread.meta.update restarts the session in the new cwd).
  // `knownRuns`, when passed, must already be scoped to the session's flow
  // instance; the query branch scopes itself.
  const flushRecordedWork = Effect.fn("ProjectWorkSessionRouter.flushRecordedWork")(function* (
    key: string,
    target: ProjectWorkRoutingTarget,
    deferred: boolean,
    knownRuns?: ReadonlyArray<AssignedWorkQueueEntry>,
  ): Effect.fn.Return<void, ProjectWorkRoutingError> {
    const session = yield* getSession(key);
    if (session === undefined) {
      return;
    }
    const runs =
      knownRuns ??
      runsForFlowInstance(
        yield* deps.listOpenAssignedWork({
          agentId: target.logicalAgentId,
          projectId: target.projectServiceProjectId,
        }),
        session.flowInstanceKey,
      );
    const ordered = orderAssignedWorkQueue(runs);
    const current = ordered[0] ?? null;
    // Rebind first: the aggregate delivered on the rebound thread names the
    // work the next turn does, so the turn must already run in that cwd.
    const binding = worktreeBindingFor(current);
    if (current !== null && binding !== session.boundWorktreePath) {
      const rebindCommand = {
        type: "thread.meta.update",
        commandId: CommandId.make(yield* deps.newId),
        threadId: session.threadId,
        worktreePath: binding,
      } as const;
      if (deferred) {
        const rebind = yield* deps.dispatchCommand(rebindCommand).pipe(Effect.result);
        if (rebind._tag === "Failure") {
          // Contained like a delivery failure: the work stays recorded for
          // the next event.
          yield* mutateSession(key, (s) => ({ ...s, phase: "idle", seenBusy: false }));
          yield* Effect.logWarning(
            "deferred Project Work rebind could not be dispatched; the work stays pending for the next event",
            {
              agentId: target.logicalAgentId,
              projectId: target.projectServiceProjectId,
              code: rebind.failure.code,
            },
          );
          return;
        }
      } else {
        // Synchronous wake path: the failure reaches the wire ACK so
        // Project Service repairs and redelivers the notice.
        yield* deps.dispatchCommand(rebindCommand);
      }
      yield* mutateSession(key, (s) => ({ ...s, boundWorktreePath: binding }));
    }
    if (deferred) {
      const delivered = yield* deliverAggregate(target, session.threadId, ordered).pipe(
        Effect.result,
      );
      if (delivered._tag === "Failure") {
        // No ACK exists, and the notices behind this pending flag were
        // already ACKed — Project Service will not redeliver them, so the
        // work KEEPS being recorded (the next thread event or wake
        // retries); clearing it would lose the only notification the
        // agent would ever get.
        yield* mutateSession(key, (s) => ({ ...s, phase: "idle", seenBusy: false }));
        yield* Effect.logWarning(
          "deferred Project Work notification could not be delivered; it stays pending for the next event",
          {
            agentId: target.logicalAgentId,
            projectId: target.projectServiceProjectId,
            code: delivered.failure.code,
          },
        );
        return;
      }
      yield* mutateSession(key, (s) => ({
        ...s,
        pendingWork: false,
        phase: delivered.success !== null ? "notifying" : "idle",
        seenBusy: false,
        ...(delivered.success !== null && current !== null
          ? {
              lastDeliveredAtIso: delivered.success,
              // A head change spends the old run's reminder budget and arms
              // the new head's (the same head re-delivered by a fresh notice
              // keeps the marker — one reminder per run, never per notice).
              ...(current.runId !== s.lastDeliveredHeadRunId
                ? { lastDeliveredHeadRunId: current.runId, remindedHeadRunId: null }
                : {}),
            }
          : {}),
      }));
      return;
    }
    const dispatchedAt = yield* deliverAggregate(target, session.threadId, ordered);
    yield* mutateSession(key, (s) => ({
      ...s,
      pendingWork: false,
      phase: dispatchedAt !== null ? "notifying" : "idle",
      seenBusy: false,
      ...(dispatchedAt !== null && current !== null
        ? {
            lastDeliveredAtIso: dispatchedAt,
            ...(current.runId !== s.lastDeliveredHeadRunId
              ? { lastDeliveredHeadRunId: current.runId, remindedHeadRunId: null }
              : {}),
          }
        : {}),
    }));
  });

  // A3 turn-end reminder (flow liveness): the aggregate turn provably ran
  // and finished, yet the SAME head is still open — the agent ended its turn
  // without submitting. Deliver exactly one reminder for that run. The caller
  // has already checked the once-per-run budget (remindedHeadRunId).
  const remindOpenHead = Effect.fn("ProjectWorkSessionRouter.remindOpenHead")(function* (
    key: string,
    target: ProjectWorkRoutingTarget,
    threadId: ThreadId,
    runs: ReadonlyArray<AssignedWorkQueueEntry>,
  ): Effect.fn.Return<void> {
    const ordered = orderAssignedWorkQueue(runs);
    const current = ordered[0] ?? null;
    if (current === null) {
      return;
    }
    const delivered = yield* deliverAggregate(target, threadId, ordered).pipe(Effect.result);
    if (delivered._tag === "Failure") {
      // Contained like a deferred delivery failure: the marker is NOT spent,
      // but only the reconcile sweep gives this path another chance — the
      // operator-visible warning names the run either way.
      yield* mutateSession(key, (s) => ({ ...s, phase: "idle", seenBusy: false }));
      yield* Effect.logWarning(
        "Project Work turn-end reminder could not be delivered; the reconcile sweep retries",
        {
          agentId: target.logicalAgentId,
          projectId: target.projectServiceProjectId,
          runId: current.runId,
          code: delivered.failure.code,
        },
      );
      return;
    }
    yield* mutateSession(key, (s) => ({
      ...s,
      phase: delivered.success !== null ? "notifying" : "idle",
      seenBusy: false,
      ...(delivered.success !== null
        ? { remindedHeadRunId: current.runId, lastDeliveredAtIso: delivered.success }
        : {}),
    }));
    if (delivered.success !== null) {
      yield* Effect.logWarning(
        "Project Work turn ended without a submit; delivered the single reminder for the still-open head",
        {
          agentId: target.logicalAgentId,
          projectId: target.projectServiceProjectId,
          runId: current.runId,
        },
      );
    }
  });

  // The per-key wake body: the session state machine for one routing key.
  // `knownRuns`, when the driver already queried the authoritative work list,
  // is scoped to `flowInstanceKey`.
  const routeWakeForKey = Effect.fn("ProjectWorkSessionRouter.routeWakeForKey")(function* (
    key: string,
    input: ProjectWorkWakeInput,
    target: ProjectWorkRoutingTarget,
    flowInstanceKey: string | null,
    knownRuns?: ReadonlyArray<AssignedWorkQueueEntry>,
  ): Effect.fn.Return<void, ProjectWorkRoutingError> {
    const session = yield* getSession(key);

    if (session === undefined) {
      return yield* createCurrentSession(key, input, target, flowInstanceKey, knownRuns);
    }

    const shell = yield* deps.readThreadShell(session.threadId);
    if (Option.isNone(shell) || !isWorkThreadCurrent(shell.value)) {
      // Archived or gone: no longer current; a new session takes over.
      yield* dropSession(key);
      return yield* createCurrentSession(key, input, target, flowInstanceKey, knownRuns);
    }
    // Busy sessions (a turn running, or our aggregate still in flight) get
    // the work recorded — never an interrupt.
    if (session.phase === "notifying" || isWorkThreadBusy(shell.value)) {
      yield* mutateSession(key, (current) => ({ ...current, pendingWork: true }));
      return;
    }
    yield* mutateSession(key, (current) => ({ ...current, pendingWork: true }));
    return yield* flushRecordedWork(key, target, false, knownRuns);
  });

  /**
   * Route one notice. Flow-instance scope queries the authoritative work
   * list ONCE per wake and runs the identical per-key body for each open
   * partition (a busy partition still costs this one query; its failure
   * fails the ACK so the service repairs and redelivers, like the create
   * path). Project scope is the historical single-key path.
   */
  const routeWake = Effect.fn("ProjectWorkSessionRouter.routeWake")(function* (
    input: ProjectWorkWakeInput,
  ): Effect.fn.Return<void, ProjectWorkRoutingError> {
    const target = yield* resolveRoutingTarget(input);
    if (target.sessionScope === "flow-instance") {
      const runs = yield* deps.listOpenAssignedWork({
        agentId: target.logicalAgentId,
        projectId: target.projectServiceProjectId,
      });
      for (const [instanceKey, partitionRuns] of partitionOpenWork(runs)) {
        const key = routingKeyOf(input.agentId, input.projectId, instanceKey);
        yield* withKeySerialization(
          key,
          routeWakeForKey(key, input, target, instanceKey, partitionRuns),
        );
      }
      return;
    }
    const key = routingKeyOf(input.agentId, input.projectId, null);
    return yield* withKeySerialization(key, routeWakeForKey(key, input, target, null, undefined));
  });

  // One thread event's work for a single routing key. Re-reads the session:
  // state may have advanced while this pass waited on the key's semaphore.
  const processThreadEvent = Effect.fn("ProjectWorkSessionRouter.processThreadEvent")(function* (
    key: string,
  ): Effect.fn.Return<void> {
    const session = yield* getSession(key);
    if (session === undefined) {
      return;
    }
    if (session.phase !== "notifying" && !session.pendingWork) {
      return;
    }
    const shell = yield* deps.readThreadShell(session.threadId).pipe(Effect.result);
    if (shell._tag === "Failure") {
      return;
    }
    if (Option.isSome(shell.success) && isWorkThreadBusy(shell.success.value)) {
      // The notification turn engaged; remember it so the FIRST
      // not-busy observation afterwards is the real turn end.
      yield* mutateSession(key, (current) => ({ ...current, seenBusy: true }));
      return;
    }
    if (
      session.phase === "notifying" &&
      !session.seenBusy &&
      (Option.isNone(shell.success) ||
        (!workThreadTurnSettledAfterDelivery(shell.success.value, session.lastDeliveredAtIso) &&
          (shell.success.value.session === null ||
            !TERMINAL_SESSION_STATUSES.has(shell.success.value.session.status))))
    ) {
      // Queued-turn window: the aggregate was dispatched but its turn has
      // not engaged yet (no session, a resting/ready session, or a turn
      // still pending) — the turn has not finished, so recorded work must
      // keep waiting (dispatching again would stack a second notification
      // on the queued first one). Only a session that DIED without ever
      // running the turn (stopped/error/interrupted), or a latest turn that
      // settled strictly AFTER our delivery, counts as lapsed.
      return;
    }
    // Re-resolution uses the wake facts the session was routed under, in
    // reuse-only mode: a deferred event must never create a project.
    const target = yield* resolveRoutingTarget(session.input, { create: false }).pipe(
      Effect.result,
    );
    if (target._tag === "Failure") {
      // Routing went away (agent unbound, integration disabled, project
      // deleted): the session simply stops being driven; nothing is deleted.
      yield* mutateSession(key, (current) => ({ ...current, phase: "idle" }));
      return;
    }
    if ((target.success.sessionScope === "project") !== (session.flowInstanceKey === null)) {
      // Universe guard: the agent's configured scope moved on, so this
      // session's run universe is owned by other sessions now (this is the
      // one entry point driven by thread events rather than by a freshly
      // computed key). Park it — no delivery, no reminder; the no-nag
      // markers stay so a toggle back resumes without re-delivering the
      // current head.
      yield* mutateSession(key, (current) => ({
        ...current,
        phase: "idle",
        pendingWork: false,
        seenBusy: false,
      }));
      return;
    }
    if (session.pendingWork) {
      yield* flushRecordedWork(key, target.success, true).pipe(Effect.ignore);
    } else {
      // Drain: this is the end of the aggregate turn. Notices are per-run
      // and never re-fire when the queue head rotates (the Project Service
      // records the next run's notice as already delivered), so the router
      // itself advances the queue when the head moved past the one the
      // agent was last told about. An unchanged head means the agent
      // finished its turn WITHOUT submitting (the run is still open and
      // still the head): exactly ONE reminder, then the session rests —
      // the same run is never reminded twice.
      const runs = yield* deps
        .listOpenAssignedWork({
          agentId: target.success.logicalAgentId,
          projectId: target.success.projectServiceProjectId,
        })
        .pipe(Effect.result);
      if (runs._tag === "Success") {
        const nextHead = orderAssignedWorkQueue(
          runsForFlowInstance(runs.success, session.flowInstanceKey),
        )[0];
        if (nextHead !== undefined && nextHead.runId !== session.lastDeliveredHeadRunId) {
          yield* flushRecordedWork(key, target.success, true, runs.success).pipe(Effect.ignore);
          return;
        }
        if (
          nextHead !== undefined &&
          nextHead.runId === session.lastDeliveredHeadRunId &&
          session.remindedHeadRunId !== nextHead.runId
        ) {
          yield* remindOpenHead(key, target.success, session.threadId, runs.success);
          return;
        }
      }
      yield* mutateSession(key, (current) => ({ ...current, phase: "idle" }));
    }
  });

  const onThreadEvent = Effect.fn("ProjectWorkSessionRouter.onThreadEvent")(function* (
    threadId: ThreadId,
  ): Effect.fn.Return<void> {
    const sessions = yield* Ref.get(sessionsRef);
    for (const [key, session] of sessions.entries()) {
      if (session.threadId !== threadId) {
        continue;
      }
      if (session.phase !== "notifying" && !session.pendingWork) {
        continue;
      }
      yield* withKeySerialization(key, processThreadEvent(key)).pipe(Effect.ignore);
    }
  });

  // A1 reconcile sweep (flow liveness): one pass that repairs the strandings
  // the event-driven machinery can leave behind. Project Service ACKs a
  // notice the moment it routes, so nothing upstream re-fires it: a restart
  // that emptied this registry, a session thread that died or was archived,
  // a turn-end event the drain never saw, or a queue head that advanced past
  // the last delivered aggregate would all strand the run forever. The pass
  // is serialized exactly like routeWake (it runs INSIDE the key's permit;
  // the helpers it calls must never re-acquire it) and never fails. The
  // per-key body below is driven once per routing key: the plain key in
  // project scope, one key per open flow instance in flow-instance scope.
  const sweepBody = (
    input: ProjectWorkWakeInput,
    key: string,
    flowInstanceKey: string | null,
    knownRuns?: ReadonlyArray<AssignedWorkQueueEntry>,
  ): Effect.Effect<void> =>
    withKeySerialization(
      key,
      Effect.gen(function* () {
        const session = yield* getSession(key);
        // The registry's session when it still backs a live, uncontested
        // thread; a gone or archived thread recovers exactly like routeWake
        // would (drop, and let a fresh session take over below when work is
        // still open).
        let liveSession: RoutedSession | null = null;
        if (session !== undefined) {
          const shell = yield* deps.readThreadShell(session.threadId).pipe(Effect.result);
          if (shell._tag === "Failure") {
            // Projection unreadable: this pass stays silent and the next
            // sweep retries.
            return;
          }
          if (Option.isNone(shell.success) || !isWorkThreadCurrent(shell.success.value)) {
            yield* dropSession(key);
          } else if (session.phase === "notifying" || session.pendingWork) {
            // The event-driven state machine owns this key, but its turn-end
            // event may never have arrived (the busy window can coalesce
            // past every shell read). processThreadEvent is a no-op while
            // the turn is genuinely in flight or still queued, and performs
            // the missed drain (pending flush, queue advancement, or the one
            // reminder) when the turn provably settled after our delivery.
            yield* processThreadEvent(key);
            return;
          } else if (isWorkThreadBusy(shell.success.value)) {
            // An agent turn is genuinely running: the drain and reminder own
            // the follow-up — a sweep never interrupts.
            return;
          } else {
            liveSession = session;
          }
        }

        let runs: ReadonlyArray<AssignedWorkQueueEntry>;
        if (knownRuns !== undefined) {
          runs = knownRuns;
        } else {
          const queried = yield* deps.listOpenAssignedWork(input).pipe(Effect.result);
          if (queried._tag === "Failure") {
            yield* Effect.logWarning(
              "Project Work reconcile sweep could not query open work; retrying on the next sweep",
              {
                agentId: input.agentId,
                projectId: input.projectId,
                code: queried.failure.code,
              },
            );
            return;
          }
          runs = runsForFlowInstance(queried.success, flowInstanceKey);
        }
        const head = orderAssignedWorkQueue(runs)[0];

        if (liveSession !== null) {
          // The session exists, is idle, and nothing is pending. The no-nag
          // invariant: a head the session already delivered (or no open work
          // at all) stays quiet — the one reminder owns the not-submitted
          // case.
          if (head === undefined || head.runId === liveSession.lastDeliveredHeadRunId) {
            return;
          }
          // The head advanced past the one the agent was last told about and
          // the drain event was missed: deliver the advancement exactly as
          // the drain would have.
          yield* Effect.logWarning(
            "Project Work reconcile sweep found an open head the session was never told about; delivering",
            {
              agentId: input.agentId,
              projectId: input.projectId,
              runId: head.runId,
            },
          );
          const target = yield* resolveRoutingTarget(liveSession.input, {
            create: false,
          }).pipe(Effect.result);
          if (target._tag === "Failure") {
            yield* Effect.logWarning(
              "Project Work reconcile sweep could not resolve routing for stranded open work",
              {
                agentId: input.agentId,
                projectId: input.projectId,
                code: target.failure.code,
                detail: target.failure.detail,
              },
            );
            return;
          }
          yield* flushRecordedWork(key, target.success, true, runs).pipe(Effect.ignore);
          return;
        }

        if (head === undefined) {
          return;
        }
        // No live session covers the open head: either the registry is fresh
        // (a restart — the ACKed notice will never re-fire, so this one
        // re-delivery is the repair) or the old session's thread is gone.
        // Either way the sweep itself wakes, through the same serialized
        // delivery a notice takes.
        yield* Effect.logWarning(
          "Project Work reconcile sweep found open work no session covers; delivering a wake",
          {
            agentId: input.agentId,
            projectId: input.projectId,
            runId: head.runId,
          },
        );
        const target = yield* resolveRoutingTarget(input).pipe(Effect.result);
        if (target._tag === "Failure") {
          yield* Effect.logWarning(
            "Project Work reconcile sweep could not resolve routing for stranded open work",
            {
              agentId: input.agentId,
              projectId: input.projectId,
              code: target.failure.code,
              detail: target.failure.detail,
            },
          );
          return;
        }
        const created = yield* createCurrentSession(
          key,
          input,
          target.success,
          flowInstanceKey,
          knownRuns,
        ).pipe(Effect.result);
        if (created._tag === "Failure") {
          yield* Effect.logWarning(
            "Project Work reconcile sweep could not deliver its wake; retrying on the next sweep",
            {
              agentId: input.agentId,
              projectId: input.projectId,
              code: created.failure.code,
              detail: created.failure.detail,
            },
          );
        }
      }),
    );

  // The sweep driver: the agent's CONFIGURED scope decides which keys this
  // pass covers. Unreadable settings or an unroutable agent read as
  // "project" so the plain key's upkeep never stops while routing is broken.
  const reconcileOpenWork = Effect.fn("ProjectWorkSessionRouter.reconcileOpenWork")(function* (
    input: ProjectWorkWakeInput,
  ): Effect.fn.Return<void> {
    const scope = yield* deps.readSettings.pipe(
      Effect.result,
      Effect.map((read) => {
        if (read._tag === "Failure") {
          return "project" as const;
        }
        const resolution = resolveProjectWorkRouting(read.success, input);
        return resolution.ok ? resolution.routing.sessionScope : ("project" as const);
      }),
    );
    if (scope === "flow-instance") {
      const runs = yield* deps.listOpenAssignedWork(input).pipe(Effect.result);
      if (runs._tag === "Failure") {
        yield* Effect.logWarning(
          "Project Work reconcile sweep could not query open work; retrying on the next sweep",
          {
            agentId: input.agentId,
            projectId: input.projectId,
            code: runs.failure.code,
          },
        );
        return;
      }
      for (const [instanceKey, partitionRuns] of partitionOpenWork(runs.success)) {
        yield* sweepBody(
          input,
          routingKeyOf(input.agentId, input.projectId, instanceKey),
          instanceKey,
          partitionRuns,
        );
      }
      return;
    }
    yield* sweepBody(input, routingKeyOf(input.agentId, input.projectId, null), null, undefined);
  });

  const snapshotSessions: ProjectWorkSessionRouter["snapshotSessions"] = Ref.get(sessionsRef).pipe(
    Effect.map((sessions) =>
      [...sessions.values()].map((session) => ({
        agentId: session.input.agentId,
        projectId: session.input.projectId,
        flowInstanceKey: session.flowInstanceKey,
        threadId: session.threadId,
        phase: session.phase,
        pendingWork: session.pendingWork,
        boundWorktreePath: session.boundWorktreePath,
      })),
    ),
  );

  return { routeWake, onThreadEvent, reconcileOpenWork, snapshotSessions };
});
