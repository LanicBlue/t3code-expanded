import { assert, describe, it } from "@effect/vitest";
import {
  LogicalAgentId,
  type OrchestrationThreadShell,
  ProjectId,
  ProviderInstanceId,
  ServerSettings,
  ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";

import {
  FlowFinalizationDependencyError,
  flowInstanceEndedEventId,
  makeFlowSessionFinalization,
  type FlowSessionFinalizationDeps,
} from "./FlowSessionFinalization.ts";
import type { ProjectWorkSessionSnapshot } from "./ProjectWorkNoticeRouting.ts";
import type {
  ProjectFlowInstanceRecord,
  ProjectServiceProjectRecord,
  ProjectWorkRunRecord,
} from "./ProjectServiceWorkClient.ts";
import type { ProjectFlowFinalizationRecord } from "../persistence/ProjectFlowFinalization.ts";

const AGENT_ID = "ag_primary";
const PS_PROJECT_ID = "ps_proj_1";
const PS_WORKSPACE_DIR = "/tmp/registry";
const ISO = "2026-08-25T10:00:00.000Z";
const INSTANCE_ID = "fi_alpha";
const INSTANCE_WORKTREE = "/wt/alpha";

const endedInstance = (
  instanceId = INSTANCE_ID,
  completedByEventId = "evt_term_1",
): ProjectFlowInstanceRecord => ({
  instanceId,
  name: instanceId,
  ended: { disposition: "completed", completedByEventId },
});

const liveInstance = (instanceId: string): ProjectFlowInstanceRecord => ({
  instanceId,
  name: instanceId,
  ended: null,
});

const serviceProject = (): ProjectServiceProjectRecord => ({
  projectId: PS_PROJECT_ID,
  name: "Registry",
  workspaceDir: PS_WORKSPACE_DIR,
});

const instanceRun = (
  state: "open" | "completed",
  workspacePath = INSTANCE_WORKTREE,
): ProjectWorkRunRecord => ({
  runId: `run_${state}`,
  positionId: "position_1",
  runRevision: "run:1",
  state,
  task: { prompt: "work", instance: { instanceId: INSTANCE_ID, name: INSTANCE_ID, iteration: 1 } },
  createdAt: "2026-08-25T09:00:00.000Z",
  ...(state === "completed" ? { resolvedAt: ISO } : {}),
  workspacePolicy: "managed-worktree",
  workspacePath,
});

const workThreadShell = (
  threadId: string,
  worktreePath: string | null,
  createdAt = ISO,
): OrchestrationThreadShell => ({
  id: ThreadId.make(threadId),
  projectId: ProjectId.make("t3_proj_1"),
  title: threadId,
  logicalAgentId: LogicalAgentId.make(AGENT_ID),
  modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5.6" },
  runtimeMode: "full-access",
  interactionMode: "default",
  branch: null,
  worktreePath,
  latestTurn: null,
  createdAt,
  updatedAt: createdAt,
  archivedAt: null,
  settledOverride: null,
  settledAt: null,
  session: null,
  latestUserMessageAt: null,
  hasPendingApprovals: false,
  hasPendingUserInput: false,
  hasActionableProposedPlan: false,
});

const registrySession = (
  threadId: string,
  flowInstanceKey: string | null,
): ProjectWorkSessionSnapshot => ({
  agentId: AGENT_ID,
  projectId: PS_PROJECT_ID,
  flowInstanceKey,
  threadId: ThreadId.make(threadId),
  phase: "idle",
  pendingWork: false,
  boundWorktreePath: null,
  workspaceDir: null,
});

const settings = (): ServerSettings =>
  ({
    ...({} as ServerSettings),
    logicalAgents: {
      [LogicalAgentId.make(AGENT_ID)]: {
        agentName: "Primary",
        providerInstanceId: ProviderInstanceId.make("codex"),
        persona: "",
        thinkLevel: null,
        modelOverride: null,
        project: { enabled: true, sessionScope: "flow-instance", sessionRetention: "settle" },
      },
    },
  }) as ServerSettings;

interface Harness {
  readonly rows: Map<string, ProjectFlowFinalizationRecord>;
  readonly driven: Array<{
    readonly instanceId: string;
    readonly agentId: string;
    readonly threadId: string;
  }>;
  setInstances(instances: ReadonlyArray<ProjectFlowInstanceRecord>): void;
  setRuns(runs: ReadonlyArray<ProjectWorkRunRecord> | null): void;
  setSessions(sessions: ReadonlyArray<ProjectWorkSessionSnapshot>): void;
  setThreadShells(shells: ReadonlyArray<OrchestrationThreadShell> | null): void;
  setRoutes(
    routes: ReadonlyArray<{ readonly instanceId: string; readonly threadId: string }>,
  ): void;
  failRoutes(shouldFail: boolean): void;
  setOutcome(outcome: { kind: "completed" } | { kind: "waiting"; reason: string }): void;
  failWorkReads(shouldFail: boolean): void;
}

const makeHarness = () => {
  const rows = new Map<string, ProjectFlowFinalizationRecord>();
  const driven: Harness["driven"] = [];
  let instances: ReadonlyArray<ProjectFlowInstanceRecord> = [];
  let runs: ReadonlyArray<ProjectWorkRunRecord> | null = [];
  let sessions: ReadonlyArray<ProjectWorkSessionSnapshot> = [];
  let threadShells: ReadonlyArray<OrchestrationThreadShell> | null = [];
  let routes: ReadonlyArray<{ readonly instanceId: string; readonly threadId: string }> = [];
  let routesFail = false;
  let outcome: { kind: "completed" } | { kind: "waiting"; reason: string } = {
    kind: "completed",
  };
  let workReadsFail = false;

  const readFailure = (source: "store" | "settings" | "work-client" | "projection") =>
    new FlowFinalizationDependencyError({ source, detail: "harness failure" });

  const deps: FlowSessionFinalizationDeps = {
    store: {
      record: (input) =>
        Effect.sync(() => {
          const key = `${input.eventId}\n${input.agentId}`;
          const existing = rows.get(key);
          if (existing === undefined) {
            rows.set(key, {
              instanceId: input.instanceId,
              agentId: input.agentId,
              psProjectId: input.psProjectId,
              eventId: input.eventId,
              threadId: input.threadId,
              state: input.threadId === null ? "done" : "pending",
              createdAt: input.createdAt,
              resolvedAt: input.threadId === null ? input.createdAt : null,
            });
            // A newer terminal event supersedes the older finalization of
            // the same (instance, agent).
            for (const [olderKey, older] of rows) {
              if (
                older.instanceId === input.instanceId &&
                older.agentId === input.agentId &&
                older.eventId !== input.eventId &&
                older.state !== "superseded"
              ) {
                rows.set(olderKey, { ...older, state: "superseded" });
              }
            }
            return "recorded" as const;
          }
          if (input.threadId !== null && existing.threadId === null && existing.state === "done") {
            rows.set(key, {
              ...existing,
              threadId: input.threadId,
              state: "pending",
              resolvedAt: null,
            });
            return "upgraded" as const;
          }
          return "exists" as const;
        }),
      listPending: () =>
        Effect.sync(() => [...rows.values()].filter((row) => row.state === "pending")),
      markDone: (input) =>
        Effect.sync(() => {
          const key = `${input.eventId}\n${input.agentId}`;
          const row = rows.get(key);
          if (row !== undefined && row.state === "pending") {
            rows.set(key, { ...row, state: "done", resolvedAt: input.resolvedAt });
          }
        }),
    },
    readSettings: Effect.succeed(settings()),
    snapshotSessions: Effect.sync(() => sessions),
    // Suspend so every call re-reads the mutable fixture.
    readThreadShells: Effect.suspend(() =>
      threadShells === null ? Effect.fail(readFailure("projection")) : Effect.succeed(threadShells),
    ),
    resolveSessionRoute: (input) =>
      Effect.suspend(() =>
        routesFail
          ? Effect.fail(readFailure("store"))
          : Effect.succeed(
              routes.find((route) => route.instanceId === input.instanceId)?.threadId ?? null,
            ),
      ),
    finalizeFlowInstance: (input) =>
      Effect.sync(() => {
        driven.push({
          instanceId: input.instanceKey,
          agentId: input.agentId,
          threadId: input.threadId,
        });
        return outcome as never;
      }),
    nowIso: Effect.succeed(ISO),
    workReads: {
      listProjects: () =>
        workReadsFail
          ? Effect.fail(readFailure("work-client"))
          : Effect.succeed([serviceProject()]),
      getProjectGeneration: () =>
        workReadsFail ? Effect.fail(readFailure("work-client")) : Effect.succeed(7),
      listMy: () =>
        workReadsFail || runs === null
          ? Effect.fail(readFailure("work-client"))
          : Effect.succeed(runs),
      listFlowInstances: () =>
        workReadsFail ? Effect.fail(readFailure("work-client")) : Effect.succeed(instances),
    },
  };

  const service = makeFlowSessionFinalization(deps);

  return {
    rows,
    driven,
    service,
    setInstances: (next: ReadonlyArray<ProjectFlowInstanceRecord>) => {
      instances = next;
    },
    setRuns: (next: ReadonlyArray<ProjectWorkRunRecord> | null) => {
      runs = next;
    },
    setSessions: (next: ReadonlyArray<ProjectWorkSessionSnapshot>) => {
      sessions = next;
    },
    setThreadShells: (next: ReadonlyArray<OrchestrationThreadShell> | null) => {
      threadShells = next;
    },
    setRoutes: (
      next: ReadonlyArray<{ readonly instanceId: string; readonly threadId: string }>,
    ) => {
      routes = next;
    },
    failRoutes: (shouldFail: boolean) => {
      routesFail = shouldFail;
    },
    setOutcome: (next: { kind: "completed" } | { kind: "waiting"; reason: string }) => {
      outcome = next;
    },
    failWorkReads: (shouldFail: boolean) => {
      workReadsFail = shouldFail;
    },
  };
};

describe("flowInstanceEndedEventId", () => {
  it("derives a stable identity from the terminal marker", () => {
    assert.strictEqual(
      flowInstanceEndedEventId(endedInstance()),
      `flow-ended:${INSTANCE_ID}:evt_term_1`,
    );
    // Re-observation of the same terminal instance is the SAME event.
    assert.strictEqual(
      flowInstanceEndedEventId(endedInstance()),
      flowInstanceEndedEventId(endedInstance()),
    );
  });
});

describe("FlowSessionFinalization intake", () => {
  it.effect("records a pending finalization from the registry session, once", () =>
    Effect.gen(function* () {
      const harness = makeHarness();
      harness.setInstances([endedInstance(), liveInstance("fi_live")]);
      // The registry's live entry for the instance's session.
      harness.setSessions([registrySession("thread_alpha", INSTANCE_ID)]);
      harness.setRuns([instanceRun("completed")]);

      yield* harness.service.intakeSweep();

      assert.strictEqual(harness.rows.size, 1);
      const row = harness.rows.get(`flow-ended:${INSTANCE_ID}:evt_term_1\n${AGENT_ID}`);
      assert.strictEqual(row?.state, "pending");
      assert.strictEqual(row?.threadId, "thread_alpha");
      assert.strictEqual(row?.eventId, `flow-ended:${INSTANCE_ID}:evt_term_1`);

      // The duplicate notification (a second sweep over the same ended
      // instance) records nothing new.
      yield* harness.service.intakeSweep();
      assert.strictEqual(harness.rows.size, 1);
      // The live instance is never recorded.
      assert.isUndefined(harness.rows.get(`flow-ended:fi_live:x\n${AGENT_ID}`));
    }),
  );

  it.effect("restart: the persisted association resolves the session after the runs closed", () =>
    Effect.gen(function* () {
      const harness = makeHarness();
      harness.setInstances([endedInstance()]);
      // What a restart actually leaves: the registry is empty, and the
      // Project Service's listMy answers OPEN runs only — the ended
      // instance's runs are closed and CANNOT appear (the old worktree
      // fallback found nothing here). The persisted instance→thread route,
      // recorded while the work was still open, is the surviving fact.
      harness.setSessions([]);
      harness.setRuns([]);
      harness.setThreadShells([workThreadShell("thread_route", INSTANCE_WORKTREE)]);
      harness.setRoutes([{ instanceId: INSTANCE_ID, threadId: "thread_route" }]);

      yield* harness.service.intakeSweep();

      const row = harness.rows.get(`flow-ended:${INSTANCE_ID}:evt_term_1\n${AGENT_ID}`);
      assert.strictEqual(row?.state, "pending");
      assert.strictEqual(row?.threadId, "thread_route");

      // The startup replay drives it to completion.
      yield* harness.service.drivePending();
      assert.deepEqual(harness.driven, [
        { instanceId: INSTANCE_ID, agentId: AGENT_ID, threadId: "thread_route" },
      ]);
    }),
  );

  it.effect("a persisted route to a deleted or foreign-agent thread is corrected away", () =>
    Effect.gen(function* () {
      const harness = makeHarness();
      harness.setInstances([endedInstance(), endedInstance("fi_beta", "evt_beta")]);
      harness.setSessions([]);
      harness.setRuns([]);
      // thread_gone no longer exists; thread_foreign exists but carries a
      // different agent's stamp — neither is a usable current fact.
      harness.setThreadShells([
        { ...workThreadShell("thread_foreign", INSTANCE_WORKTREE), logicalAgentId: null },
      ]);
      harness.setRoutes([
        { instanceId: INSTANCE_ID, threadId: "thread_gone" },
        { instanceId: "fi_beta", threadId: "thread_foreign" },
      ]);

      yield* harness.service.intakeSweep();

      const row = harness.rows.get(`flow-ended:${INSTANCE_ID}:evt_term_1\n${AGENT_ID}`);
      assert.isNull(row?.threadId ?? null);
      assert.strictEqual(row?.state, "done");
      const beta = harness.rows.get(`flow-ended:fi_beta:evt_beta\n${AGENT_ID}`);
      assert.isNull(beta?.threadId ?? null);
      assert.strictEqual(beta?.state, "done");
    }),
  );

  it.effect("a no-op is overturned when the association arrives late for the same event", () =>
    Effect.gen(function* () {
      const harness = makeHarness();
      harness.setInstances([endedInstance()]);
      harness.setSessions([]);
      harness.setRuns([]);
      harness.setThreadShells([workThreadShell("thread_late", INSTANCE_WORKTREE)]);
      // First sweep: no association fact anywhere — a no-op is recorded.
      yield* harness.service.intakeSweep();
      const key = `flow-ended:${INSTANCE_ID}:evt_term_1\n${AGENT_ID}`;
      assert.isNull(harness.rows.get(key)?.threadId ?? null);
      assert.strictEqual(harness.rows.get(key)?.state, "done");
      assert.lengthOf(harness.driven, 0);

      // The routing delivery raced the first sweep and lands now: the
      // persisted association appears for the SAME event, and the recorded
      // no-op must be OVERTURNED — upgraded back to pending and driven
      // IMMEDIATELY within the same sweep, not stranded as done.
      harness.setRoutes([{ instanceId: INSTANCE_ID, threadId: "thread_late" }]);
      yield* harness.service.intakeSweep();

      const overturned = harness.rows.get(key);
      assert.strictEqual(overturned?.threadId, "thread_late");
      assert.deepEqual(harness.driven, [
        { instanceId: INSTANCE_ID, agentId: AGENT_ID, threadId: "thread_late" },
      ]);
      // The completed drive finished the resurrected row.
      assert.strictEqual(overturned?.state, "done");
    }),
  );

  it.effect("no discoverable session records a successful no-op", () =>
    Effect.gen(function* () {
      const harness = makeHarness();
      harness.setInstances([endedInstance()]);
      harness.setSessions([]);
      harness.setRuns([instanceRun("completed", undefined)]);
      harness.setThreadShells([]);

      yield* harness.service.intakeSweep();

      const row = harness.rows.get(`flow-ended:${INSTANCE_ID}:evt_term_1\n${AGENT_ID}`);
      assert.isNull(row?.threadId ?? null);
      assert.strictEqual(row?.state, "done");
      assert.strictEqual(harness.driven.length, 0);
    }),
  );

  it.effect("degraded reads skip instead of recording premature no-ops", () =>
    Effect.gen(function* () {
      const harness = makeHarness();
      harness.setInstances([endedInstance()]);

      // Work reads down: no rows.
      harness.failWorkReads(true);
      yield* harness.service.intakeSweep();
      assert.strictEqual(harness.rows.size, 0);

      // Projection shells unreadable: no rows (the scan could not run).
      harness.failWorkReads(false);
      harness.setThreadShells(null);
      yield* harness.service.intakeSweep();
      assert.strictEqual(harness.rows.size, 0);

      // The agent's run list unreadable: still no rows.
      harness.setThreadShells([]);
      harness.setRuns(null);
      yield* harness.service.intakeSweep();
      assert.strictEqual(harness.rows.size, 0);

      // The session-route ledger unreadable: a missing row and a failed
      // read must not blur — skip and retry rather than record a no-op.
      harness.setRuns([]);
      harness.failRoutes(true);
      yield* harness.service.intakeSweep();
      assert.strictEqual(harness.rows.size, 0);
    }),
  );

  it.effect("an open run racing the terminal observation still associates by worktree", () =>
    Effect.gen(function* () {
      const harness = makeHarness();
      harness.setInstances([endedInstance()]);
      harness.setSessions([]);
      // The terminal marker raced a genuinely still-open run: listMy DOES
      // answer it, and the thread bound to its worktree is the session.
      harness.setRuns([instanceRun("open")]);
      harness.setThreadShells([
        workThreadShell("thread_stale", INSTANCE_WORKTREE, "2026-08-25T09:00:00.000Z"),
        workThreadShell("thread_alpha", INSTANCE_WORKTREE, "2026-08-25T09:30:00.000Z"),
      ]);

      yield* harness.service.intakeSweep();

      const row = harness.rows.get(`flow-ended:${INSTANCE_ID}:evt_term_1\n${AGENT_ID}`);
      // Newest bound thread wins when several carry the agent (orphaned
      // predecessors sort behind the current one).
      assert.strictEqual(row?.threadId, "thread_alpha");
      assert.strictEqual(row?.state, "pending");
    }),
  );
});

describe("FlowSessionFinalization drive", () => {
  it.effect("a completed drive finishes the ledger row exactly once", () =>
    Effect.gen(function* () {
      const harness = makeHarness();
      harness.setInstances([endedInstance()]);
      harness.setSessions([registrySession("thread_alpha", INSTANCE_ID)]);
      harness.setRuns([instanceRun("completed")]);
      yield* harness.service.intakeSweep();
      harness.setOutcome({ kind: "completed" });

      yield* harness.service.drivePending();

      assert.deepEqual(harness.driven, [
        { instanceId: INSTANCE_ID, agentId: AGENT_ID, threadId: "thread_alpha" },
      ]);
      assert.strictEqual(
        harness.rows.get(`flow-ended:${INSTANCE_ID}:evt_term_1\n${AGENT_ID}`)?.state,
        "done",
      );

      // The restart replay: a done row never drives again.
      yield* harness.service.drivePending();
      assert.lengthOf(harness.driven, 1);
    }),
  );

  it.effect("a waiting drive keeps the row pending and retries on the next pass", () =>
    Effect.gen(function* () {
      const harness = makeHarness();
      harness.setInstances([endedInstance()]);
      harness.setSessions([registrySession("thread_alpha", INSTANCE_ID)]);
      harness.setRuns([instanceRun("completed")]);
      yield* harness.service.intakeSweep();
      harness.setOutcome({ kind: "waiting", reason: "session-busy" });

      yield* harness.service.drivePending();
      assert.strictEqual(
        harness.rows.get(`flow-ended:${INSTANCE_ID}:evt_term_1\n${AGENT_ID}`)?.state,
        "pending",
      );

      harness.setOutcome({ kind: "completed" });
      yield* harness.service.drivePending();
      assert.strictEqual(
        harness.rows.get(`flow-ended:${INSTANCE_ID}:evt_term_1\n${AGENT_ID}`)?.state,
        "done",
      );
    }),
  );

  it.effect("the turn-end trigger drives only the thread's own rows", () =>
    Effect.gen(function* () {
      const harness = makeHarness();
      harness.setInstances([endedInstance(), endedInstance("fi_beta", "evt_term_2")]);
      harness.setSessions([
        registrySession("thread_alpha", INSTANCE_ID),
        registrySession("thread_beta", "fi_beta"),
      ]);
      harness.setRuns([
        { ...instanceRun("completed"), task: { prompt: "work" } },
        {
          ...instanceRun("completed"),
          runId: "run_beta",
          task: {
            prompt: "beta",
            instance: { instanceId: "fi_beta", name: "fi_beta", iteration: 1 },
          },
        },
      ]);
      yield* harness.service.intakeSweep();
      harness.setOutcome({ kind: "completed" });

      yield* harness.service.drivePendingForThread("thread_beta");

      assert.deepEqual(
        harness.driven.map((drive) => drive.threadId),
        ["thread_beta"],
      );
      assert.strictEqual(
        harness.rows.get(`flow-ended:${INSTANCE_ID}:evt_term_1\n${AGENT_ID}`)?.state,
        "pending",
      );
      assert.strictEqual(
        harness.rows.get(`flow-ended:fi_beta:evt_term_2\n${AGENT_ID}`)?.state,
        "done",
      );
    }),
  );

  it.effect("a reopened instance's newer event supersedes the older finalization", () =>
    Effect.gen(function* () {
      const harness = makeHarness();
      harness.setSessions([registrySession("thread_alpha", INSTANCE_ID)]);
      harness.setRuns([instanceRun("completed")]);
      harness.setThreadShells([]);

      // First terminal observation.
      harness.setInstances([endedInstance()]);
      yield* harness.service.intakeSweep();

      // The theoretical reopen: the same instance ends again under a new
      // completedByEventId. The newer event's finalization drives; the
      // older row is superseded and never drives again.
      harness.setInstances([endedInstance(INSTANCE_ID, "evt_term_2")]);
      yield* harness.service.intakeSweep();

      assert.strictEqual(
        harness.rows.get(`flow-ended:${INSTANCE_ID}:evt_term_1\n${AGENT_ID}`)?.state,
        "superseded",
      );
      const current = harness.rows.get(`flow-ended:${INSTANCE_ID}:evt_term_2\n${AGENT_ID}`);
      assert.strictEqual(current?.state, "pending");
      assert.strictEqual(current?.threadId, "thread_alpha");

      harness.setOutcome({ kind: "completed" });
      yield* harness.service.drivePending();
      assert.deepEqual(
        harness.driven.map((drive) => drive.threadId),
        ["thread_alpha"],
      );
      assert.strictEqual(
        harness.rows.get(`flow-ended:${INSTANCE_ID}:evt_term_2\n${AGENT_ID}`)?.state,
        "done",
      );
    }),
  );
});
