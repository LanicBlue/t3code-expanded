/**
 * FlowSessionFinalization — the flow-end intake and pending-finalization
 * driver (Project Service flow session finalization design).
 *
 * Project Service 0.11.0 has no push surface for a flow instance reaching a
 * terminal state, so the terminal observation is the authoritative instance
 * LIST itself (the formal contract, not a degraded fallback): each sweep
 * polls `GET /:id/flow/instances`, and an instance whose terminal marker
 * (`ended`) appears IS the event. Its identity is deterministic —
 * `flow-ended:<instanceId>:<completedByEventId>` — so a re-observation
 * (duplicate sweeps, restarts) is the SAME event, and the ledger's
 * (event_id, agent_id) uniqueness absorbs it: the durable row, written
 * BEFORE anything is driven, is the commit point (no PS ACK exists or is
 * required); a process exit after it cannot lose the finalization — the
 * pending row replays at the next startup sweep.
 *
 * Intake associates the instance with this consumer's sessions in restart-
 * safe order:
 *
 * 1. the live routing registry (the in-memory per-key session);
 * 2. the persistent instance→thread association ledger, recorded by the
 *    routing path while the instance's work was still open — the Project
 *    Service's run lists only answer OPEN runs, so after the run closes no
 *    live API can recover this fact. The persisted thread is corrected
 *    against current facts (it must still exist and carry this agent's
 *    stamp) before it is trusted;
 * 3. the open-run worktree scan, which only ever fires when a run is
 *    genuinely still open (a terminal observation racing live work).
 *
 * No association fact at all records the event as a successful no-op — but
 * only when every path was actually readable; a degraded read skips (the
 * next sweep retries) rather than recording a premature no-op, and a
 * recorded no-op stays overturnable: a late association fact upgrades the
 * same event's row back to pending and re-drives it.
 *
 * @module FlowSessionFinalization
 */
import type { OrchestrationThreadShell, ServerSettings } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import { flowInstanceKeyOf } from "./AssignedWorkQueue.ts";
import type {
  FlowInstanceFinalizationInput,
  FlowInstanceFinalizationOutcome,
  ProjectWorkSessionSnapshot,
} from "./ProjectWorkNoticeRouting.ts";
import type {
  ProjectFlowInstanceRecord,
  ProjectServiceProjectRecord,
  ProjectWorkRunRecord,
} from "./ProjectServiceWorkClient.ts";
import type { ProjectFlowFinalizationRecord } from "../persistence/ProjectFlowFinalization.ts";

/** The single failure vocabulary of this module's dependency seams. */
export class FlowFinalizationDependencyError extends Schema.TaggedErrorClass<FlowFinalizationDependencyError>()(
  "FlowFinalizationDependencyError",
  {
    source: Schema.Literals(["store", "settings", "work-client", "projection"]),
    detail: Schema.String,
  },
) {
  override get message(): string {
    return `${this.source}: ${this.detail}`;
  }
}

/** The Project Service reads the intake needs (subset of the Work client). */
export interface FlowFinalizationWorkReads {
  readonly listProjects: () => Effect.Effect<
    readonly ProjectServiceProjectRecord[],
    FlowFinalizationDependencyError
  >;
  readonly getProjectGeneration: (
    projectId: string,
  ) => Effect.Effect<number, FlowFinalizationDependencyError>;
  readonly listMy: (input: {
    readonly projectId: string;
    readonly projectGeneration: number;
    readonly agentId: string;
  }) => Effect.Effect<ReadonlyArray<ProjectWorkRunRecord>, FlowFinalizationDependencyError>;
  readonly listFlowInstances: (input: {
    readonly projectId: string;
  }) => Effect.Effect<readonly ProjectFlowInstanceRecord[], FlowFinalizationDependencyError>;
}

/** Seams the finalization intake and driver need from their host. */
export interface FlowSessionFinalizationDeps {
  /** Durable ledger: the eventId idempotency and the restart-surviving pending set. */
  readonly store: {
    readonly record: (input: {
      readonly instanceId: string;
      readonly agentId: string;
      readonly psProjectId: string;
      readonly eventId: string;
      readonly threadId: string | null;
      readonly createdAt: string;
    }) => Effect.Effect<"recorded" | "exists" | "upgraded", FlowFinalizationDependencyError>;
    readonly listPending: () => Effect.Effect<
      ReadonlyArray<ProjectFlowFinalizationRecord>,
      FlowFinalizationDependencyError
    >;
    readonly markDone: (input: {
      readonly eventId: string;
      readonly agentId: string;
      readonly resolvedAt: string;
    }) => Effect.Effect<void, FlowFinalizationDependencyError>;
  };
  readonly readSettings: Effect.Effect<ServerSettings, FlowFinalizationDependencyError>;
  /** The live routing registry view (session lookup fast path). */
  readonly snapshotSessions: Effect.Effect<
    ReadonlyArray<ProjectWorkSessionSnapshot>,
    FlowFinalizationDependencyError
  >;
  /** All thread shells (current-fact correction of persisted associations). */
  readonly readThreadShells: Effect.Effect<
    ReadonlyArray<OrchestrationThreadShell>,
    FlowFinalizationDependencyError
  >;
  /**
   * The persistent instance→thread association ledger — the restart-safe
   * association fact the routing path recorded while the work was open.
   */
  readonly resolveSessionRoute: (input: {
    readonly instanceId: string;
    readonly agentId: string;
  }) => Effect.Effect<string | null, FlowFinalizationDependencyError>;
  /** The finalization drive itself (owned by the session router). */
  readonly finalizeFlowInstance: (
    input: FlowInstanceFinalizationInput,
  ) => Effect.Effect<FlowInstanceFinalizationOutcome, never>;
  readonly nowIso: Effect.Effect<string>;
  readonly workReads: FlowFinalizationWorkReads;
}

export interface FlowSessionFinalization {
  /**
   * One intake pass (sweep cadence): enumerate Project Service projects,
   * derive terminal observations for instances whose `ended` marker
   * appeared, associate sessions, and record finalizations idempotently.
   * Never fails.
   */
  readonly intakeSweep: () => Effect.Effect<void>;
  /** Drive every pending finalization once (the startup replay + ticks). */
  readonly drivePending: () => Effect.Effect<void>;
  /** Drive the pending finalizations of one thread (turn-end trigger). */
  readonly drivePendingForThread: (threadId: string) => Effect.Effect<void>;
}

const projectEnabledAgents = (settings: ServerSettings): ReadonlyArray<string> =>
  Object.entries(settings.logicalAgents)
    .filter(([, agent]) => agent.project.enabled)
    .map(([agentId]) => agentId);

/**
 * Validate a persisted association against CURRENT facts: the recorded thread
 * must still exist and still carry this agent's stamp. A deleted thread
 * invalidates the association (the intake then records the no-op); an
 * archived one stays associated — the drive itself treats gone/archived as
 * completed. Rebinding needs no separate check here: the DRIVE recomputes
 * the workspace binding from current open work, so a late old-flow
 * association can never pull a re-bound session back.
 */
const persistedRouteThread = (
  threadId: string | null,
  agentId: string,
  threadShells: ReadonlyArray<OrchestrationThreadShell>,
): string | null => {
  if (threadId === null) {
    return null;
  }
  const shell = threadShells.find((candidate) => candidate.id === threadId);
  if (shell === undefined) {
    return null;
  }
  return shell.logicalAgentId === agentId ? shell.id : null;
};

/**
 * The session thread associated with a terminal instance for one agent:
 * the live registry entry first (the instance's own when one exists, else the
 * project-scope session that works every instance), then the persisted
 * instance→thread association corrected against current thread facts, then —
 * only for genuinely still-open work racing the terminal observation — the
 * open-run worktree scan. The persisted association is what survives a
 * restart AND the run's closure, which is exactly when both other paths are
 * empty.
 */
const findAssociatedThread = (
  instanceId: string,
  agentId: string,
  psProjectId: string,
  sessions: ReadonlyArray<ProjectWorkSessionSnapshot>,
  threadShells: ReadonlyArray<OrchestrationThreadShell>,
  runs: ReadonlyArray<ProjectWorkRunRecord>,
  persistedRoute: string | null,
): string | null => {
  const registryMatch =
    sessions.find(
      (session) =>
        session.agentId === agentId &&
        session.projectId === psProjectId &&
        session.flowInstanceKey === instanceId,
    ) ??
    sessions.find(
      (session) =>
        session.agentId === agentId &&
        session.projectId === psProjectId &&
        session.flowInstanceKey === null,
    );
  if (registryMatch !== undefined) {
    return registryMatch.threadId;
  }
  if (persistedRoute !== null) {
    return persistedRoute;
  }
  const instanceWorktree = runs.find(
    (run) =>
      flowInstanceKeyOf(run) === instanceId &&
      run.workspacePolicy === "managed-worktree" &&
      typeof run.workspacePath === "string" &&
      run.workspacePath.trim().length > 0,
  )?.workspacePath;
  if (instanceWorktree === undefined) {
    return null;
  }
  const bound = threadShells
    .filter(
      (shell) =>
        shell.logicalAgentId === agentId &&
        shell.archivedAt === null &&
        shell.worktreePath === instanceWorktree,
    )
    .toSorted((left, right) => left.createdAt.localeCompare(right.createdAt))
    .at(-1);
  return bound === undefined ? null : bound.id;
};

/** The deterministic event identity of one instance's terminal transition. */
export const flowInstanceEndedEventId = (instance: ProjectFlowInstanceRecord): string =>
  `flow-ended:${instance.instanceId}:${instance.ended?.completedByEventId ?? "unknown"}`;

export const makeFlowSessionFinalization = (
  deps: FlowSessionFinalizationDeps,
): FlowSessionFinalization => {
  // Drive one pending row; completed rows are finished in the ledger.
  const driveRow = Effect.fn("FlowSessionFinalization.driveRow")(function* (
    row: ProjectFlowFinalizationRecord,
  ): Effect.fn.Return<void> {
    if (row.threadId === null) {
      // A no-op record (no session at intake) never drives.
      return;
    }
    const outcome = yield* deps.finalizeFlowInstance({
      agentId: row.agentId,
      projectId: row.psProjectId,
      instanceKey: row.instanceId,
      threadId: row.threadId,
    });
    if (outcome.kind === "waiting") {
      yield* Effect.logDebug("flow finalization waits; retried on the next event or sweep", {
        instanceId: row.instanceId,
        agentId: row.agentId,
        reason: outcome.reason,
      });
      return;
    }
    const marked = yield* deps.store
      .markDone({
        eventId: row.eventId,
        agentId: row.agentId,
        resolvedAt: yield* deps.nowIso,
      })
      .pipe(Effect.result);
    if (marked._tag === "Failure") {
      // The finalization itself landed; only the ledger write failed. The
      // next drive re-runs it (settle re-emission and rebind diffing are
      // idempotent), so the row is retried rather than stranded.
      yield* Effect.logWarning(
        "flow finalization completed but its ledger update failed; retrying on the next sweep",
        { instanceId: row.instanceId, agentId: row.agentId },
      );
    }
  });

  const driveRows = (rows: ReadonlyArray<ProjectFlowFinalizationRecord>) =>
    Effect.forEach(rows, (row) => driveRow(row), { discard: true });

  const intakeSweep = Effect.fn("FlowSessionFinalization.intakeSweep")(
    function* (): Effect.fn.Return<void> {
      const settings = yield* deps.readSettings.pipe(Effect.result);
      if (settings._tag === "Failure") {
        return;
      }
      const agents = projectEnabledAgents(settings.success);
      if (agents.length === 0) {
        return;
      }
      const projects = yield* deps.workReads.listProjects().pipe(Effect.result);
      if (projects._tag === "Failure") {
        yield* Effect.logDebug("flow finalization intake could not list Project Service projects");
        return;
      }
      // The thread-shell scan is what corrects persisted associations; and
      // without it the worktree scan cannot run either — a failed read skips
      // the whole pass rather than recording premature no-ops.
      const threadShells = yield* deps.readThreadShells.pipe(Effect.result);
      if (threadShells._tag === "Failure") {
        yield* Effect.logDebug("flow finalization intake could not read thread shells");
        return;
      }
      // The registry view is the fast path only; a failed read reads as "no
      // registry" (the persisted association still associates).
      const sessions = yield* deps.snapshotSessions.pipe(
        Effect.result,
        Effect.map((read) => (read._tag === "Success" ? read.success : [])),
      );
      for (const project of projects.success) {
        const instances = yield* deps.workReads
          .listFlowInstances({ projectId: project.projectId })
          .pipe(Effect.result);
        if (instances._tag === "Failure") {
          yield* Effect.logDebug("flow finalization intake could not list flow instances", {
            projectId: project.projectId,
          });
          continue;
        }
        // `ended` is terminal — the instances to finalize. The eventId is
        // derived from the terminal marker, so every re-observation of the
        // same ended instance is the SAME event.
        const ended = instances.success.filter(
          (instance) => instance.ended !== null && instance.instanceId.trim().length > 0,
        );
        if (ended.length === 0) {
          continue;
        }
        const generation = yield* deps.workReads
          .getProjectGeneration(project.projectId)
          .pipe(Effect.result);
        if (generation._tag === "Failure") {
          continue;
        }
        for (const agentId of agents) {
          const runs = yield* deps.workReads
            .listMy({
              projectId: project.projectId,
              projectGeneration: generation.success,
              agentId,
            })
            .pipe(Effect.result);
          // Without the agent's runs the worktree scan cannot run; skip the
          // agent this pass rather than recording premature no-ops.
          if (runs._tag === "Failure") {
            continue;
          }
          for (const instance of ended) {
            // The persisted association (restart-safe): read it, then correct
            // it against the current thread facts. An unreadable ledger skips
            // the instance — a missing row and a failed read must not blur.
            const route = yield* deps
              .resolveSessionRoute({
                instanceId: instance.instanceId,
                agentId,
              })
              .pipe(Effect.result);
            if (route._tag === "Failure") {
              yield* Effect.logDebug(
                "flow finalization intake could not read the session-route ledger; retrying on the next sweep",
                { instanceId: instance.instanceId, agentId },
              );
              continue;
            }
            const threadId = findAssociatedThread(
              instance.instanceId,
              agentId,
              project.projectId,
              sessions,
              threadShells.success,
              runs.success,
              persistedRouteThread(route.success, agentId, threadShells.success),
            );
            // Record BEFORE driving (and before any no-op): the durable row
            // is both the eventId idempotency and the commit point — once
            // written, a process exit cannot lose the finalization.
            const recorded = yield* deps.store
              .record({
                instanceId: instance.instanceId,
                agentId,
                psProjectId: project.projectId,
                eventId: flowInstanceEndedEventId(instance),
                threadId,
                createdAt: yield* deps.nowIso,
              })
              .pipe(Effect.result);
            if (recorded._tag === "Failure") {
              yield* Effect.logWarning(
                "flow finalization intake could not record its ledger row; retrying on the next sweep",
                { instanceId: instance.instanceId, agentId },
              );
            } else if (recorded.success === "upgraded") {
              // A premature no-op just gained its session: drive it now
              // rather than waiting for the next sweep tick.
              yield* Effect.logInfo(
                "flow finalization no-op overturned by a late session association",
                { instanceId: instance.instanceId, agentId },
              );
              yield* driveRow({
                instanceId: instance.instanceId,
                agentId,
                psProjectId: project.projectId,
                eventId: flowInstanceEndedEventId(instance),
                threadId,
                state: "pending",
                createdAt: "",
                resolvedAt: null,
              });
            }
          }
        }
      }
    },
  );

  const drivePending = Effect.fn("FlowSessionFinalization.drivePending")(
    function* (): Effect.fn.Return<void> {
      const rows = yield* deps.store.listPending().pipe(Effect.result);
      if (rows._tag === "Failure") {
        yield* Effect.logDebug("flow finalization ledger could not be read");
        return;
      }
      yield* driveRows(rows.success);
    },
  );

  const drivePendingForThread = Effect.fn("FlowSessionFinalization.drivePendingForThread")(
    function* (threadId: string): Effect.fn.Return<void> {
      const rows = yield* deps.store.listPending().pipe(Effect.result);
      if (rows._tag === "Failure") {
        return;
      }
      yield* driveRows(rows.success.filter((row) => row.threadId === threadId));
    },
  );

  return {
    intakeSweep: () => intakeSweep(),
    drivePending: () => drivePending(),
    drivePendingForThread: (threadId: string) => drivePendingForThread(threadId),
  };
};
