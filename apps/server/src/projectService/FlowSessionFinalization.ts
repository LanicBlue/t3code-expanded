/**
 * FlowSessionFinalization — the flow-end intake and pending-finalization
 * driver (Project Service flow session finalization design).
 *
 * Project Service 0.11.0 has no push notification for a flow instance
 * reaching a terminal state, so the terminal notification is DERIVED exactly
 * the way work notices already are (a trigger, never a verbatim payload):
 * each sweep queries the authoritative instance list, and an instance whose
 * terminal marker (`ended`) has appeared since the last observation IS the
 * notification. Its event identity is deterministic —
 * `flow-ended:<instanceId>:<completedByEventId>` — so re-observing the same
 * terminal instance (duplicate sweeps, restarts) is the duplicate
 * notification the eventId idempotency absorbs: the durable
 * (instanceId, agentId) ledger row is the ACK state, written BEFORE anything
 * is driven, so a process exit after the ACK can never lose the finalization
 * — the pending row replays at the next startup sweep.
 *
 * Intake associates the instance with this consumer's sessions two ways: the
 * live routing registry (the in-memory per-key session), and — because a
 * restart empties that registry — the instance's managed worktree: the runs
 * PS still reports for the instance name the worktree, and the thread bound
 * to it (with the agent stamped) is the session that ran the instance's
 * work. No session at all records the event as a successful no-op — but only
 * when both association paths were actually readable; a degraded read skips
 * (the next sweep retries) rather than recording a premature no-op.
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
    }) => Effect.Effect<"recorded" | "exists", FlowFinalizationDependencyError>;
    readonly listPending: () => Effect.Effect<
      ReadonlyArray<ProjectFlowFinalizationRecord>,
      FlowFinalizationDependencyError
    >;
    readonly markDone: (input: {
      readonly instanceId: string;
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
  /** All thread shells (the restart-safe worktree→session association scan). */
  readonly readThreadShells: Effect.Effect<
    ReadonlyArray<OrchestrationThreadShell>,
    FlowFinalizationDependencyError
  >;
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
   * derive terminal notifications for instances whose `ended` marker
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
 * The session thread associated with a terminal instance for one agent: the
 * live registry entry first (the instance's own when one exists, else the
 * project-scope session that works every instance), then the worktree scan —
 * the instance's managed worktree, resolved from the runs PS still reports,
 * matches the thread bound to it with this agent stamped. That scan is what
 * survives a restart, which is exactly when the registry cannot help.
 */
const findAssociatedThread = (
  instanceId: string,
  agentId: string,
  psProjectId: string,
  sessions: ReadonlyArray<ProjectWorkSessionSnapshot>,
  threadShells: ReadonlyArray<OrchestrationThreadShell>,
  runs: ReadonlyArray<ProjectWorkRunRecord>,
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
        instanceId: row.instanceId,
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
      // The thread-shell scan is what makes intake restart-safe; without it a
      // no-op record could strand a discoverable session, so a failed read
      // skips the whole pass rather than recording premature no-ops.
      const threadShells = yield* deps.readThreadShells.pipe(Effect.result);
      if (threadShells._tag === "Failure") {
        yield* Effect.logDebug("flow finalization intake could not read thread shells");
        return;
      }
      // The registry view is the fast path only; a failed read reads as "no
      // registry" (the scan still associates).
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
        // same ended instance is the SAME notification.
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
            const threadId = findAssociatedThread(
              instance.instanceId,
              agentId,
              project.projectId,
              sessions,
              threadShells.success,
              runs.success,
            );
            // Record BEFORE driving (and before any no-op): the durable row
            // is both the eventId idempotency and the ACK — once written, a
            // process exit cannot lose the finalization.
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
