import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, assert, it } from "@effect/vitest";
import {
  DEFAULT_MODEL,
  DEFAULT_SERVER_SETTINGS,
  type LogicalAgentConfig,
  LogicalAgentId,
  type ModelSelection,
  type OrchestrationCommand,
  type OrchestrationProject,
  type OrchestrationSession,
  type OrchestrationThreadShell,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  type ServerSettings,
  ThreadId,
  TurnId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";

import { type AssignedWorkQueueEntry } from "./AssignedWorkQueue.ts";
import {
  makeProjectWorkSessionRouter,
  type ProjectWorkSessionRouter,
  ProjectWorkRoutingError,
} from "./ProjectWorkNoticeRouting.ts";
import {
  isWorkSessionSafeToRetire,
  makeFileProjectRetirementLedgerStore,
  makeProjectRetirementHandler,
  RETIREMENT_QUEUED_TURN_START_GRACE_MS,
  type ProjectRetirementHandler,
} from "./ProjectRetirement.ts";

const AGENT_ID = "ag_primary";
const PS_PROJECT_ID = "ps_proj_1";
/** Another Project Service project at ANOTHER directory — out of scope. */
const OTHER_PS_PROJECT_ID = "ps_proj_other";
const OTHER_WORKSPACE_DIR = "/tmp/retirement-registry-other";
const OTHER_T3_PROJECT_ID = "t3_proj_other";
const PROVIDER_INSTANCE = "codex-main";
const ISO = "2026-08-26T12:00:00.000Z";
const ISO_NEXT = "2026-08-26T12:00:01.000Z";
/** ISO_NEXT minus the queued-turn-start grace window, minus one second. */
const ISO_STALE = "2026-08-26T11:58:00.000Z";
/** The notice's workspace directory — always an existing directory on disk. */
const WORKSPACE_DIR = "/tmp/retirement-registry";
const T3_PROJECT_ID = "t3_proj_1";

const makeSettings = (): ServerSettings => {
  const agents: Record<string, LogicalAgentConfig> = {
    [LogicalAgentId.make(AGENT_ID)]: {
      agentName: "Primary Agent",
      providerInstanceId: ProviderInstanceId.make(PROVIDER_INSTANCE),
      persona: "",
      thinkLevel: null,
      modelOverride: null,
      project: { enabled: true, sessionScope: "project", sessionRetention: "settle" },
    },
  };
  return {
    ...DEFAULT_SERVER_SETTINGS,
    projectServiceClient: {
      enabled: true,
      baseUrl: "http://127.0.0.1:7600",
      keyIdHint: "key-1",
      credentialSet: true,
      missionsEnabled: false,
    },
    logicalAgents: agents as ServerSettings["logicalAgents"],
  };
};

const makeThreadShell = (
  threadId: string,
  projectId: string,
  mutations?: (shell: OrchestrationThreadShell) => OrchestrationThreadShell,
): OrchestrationThreadShell => {
  const base: OrchestrationThreadShell = {
    id: ThreadId.make(threadId),
    projectId: ProjectId.make(projectId),
    title: "Project Work — Registry",
    logicalAgentId: LogicalAgentId.make(AGENT_ID),
    modelSelection: {
      instanceId: ProviderInstanceId.make(PROVIDER_INSTANCE),
      model: DEFAULT_MODEL,
    },
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    latestTurn: null,
    createdAt: ISO,
    updatedAt: ISO,
    archivedAt: null,
    settledOverride: null,
    settledAt: null,
    session: null,
    latestUserMessageAt: null,
    hasPendingApprovals: false,
    hasPendingUserInput: false,
    hasActionableProposedPlan: false,
  };
  return mutations === undefined ? base : mutations(base);
};

const runningSession = (threadId: string): OrchestrationSession => ({
  threadId: ThreadId.make(threadId),
  status: "running",
  providerName: "codex",
  runtimeMode: "full-access",
  activeTurnId: null,
  lastError: null,
  updatedAt: ISO,
});

const readySession = (threadId: string): OrchestrationSession => ({
  ...runningSession(threadId),
  status: "ready",
});

/** The settled turn of an idle session: adopted, ran, finished. */
const settledTurn = (): NonNullable<OrchestrationThreadShell["latestTurn"]> => ({
  turnId: TurnId.make("turn_settled"),
  state: "completed",
  requestedAt: ISO,
  startedAt: ISO,
  completedAt: ISO_NEXT,
  assistantMessageId: null,
});

/** Mark a session idle-and-safe, as a finished wake turn would leave it. */
const settleThread = (shell: OrchestrationThreadShell): OrchestrationThreadShell => ({
  ...shell,
  session: readySession(shell.id),
  latestTurn: settledTurn(),
  latestUserMessageAt: ISO,
  hasPendingApprovals: false,
  hasPendingUserInput: false,
});

const syntheticOpenRun = (runId: string): AssignedWorkQueueEntry => ({
  runId,
  positionId: `position-${runId}`,
  runRevision: "run:1",
  state: "open",
  agentId: AGENT_ID,
  task: { prompt: "工作: do the thing" },
  createdAt: ISO,
});

interface Harness {
  /** The Project Work router — real, so routing effects are observable. */
  readonly router: ProjectWorkSessionRouter;
  readonly retirement: ProjectRetirementHandler;
  readonly commands: OrchestrationCommand[];
  readonly putThread: (shell: OrchestrationThreadShell) => void;
  readonly getThread: (threadId: string) => OrchestrationThreadShell | undefined;
  readonly setOpenWork: (runs: AssignedWorkQueueEntry[] | null) => void;
  readonly putProjectServiceProject: (projectId: string, workspaceDir: string) => void;
  readonly ledgerPath: string;
  /** Build a fresh handler over the same fakes + ledger file (a restart). */
  readonly restart: () => Effect.Effect<
    ProjectRetirementHandler,
    never,
    FileSystem.FileSystem | Path.Path
  >;
}

const CREATED_PROJECT_MODEL_SELECTION: ModelSelection = {
  instanceId: ProviderInstanceId.make(PROVIDER_INSTANCE),
  model: DEFAULT_MODEL,
};

/**
 * The shared fake projection + engine: one thread map, one project map, one
 * command journal, and one dispatch that applies the commands the real
 * engine would project (create/delete only — tests set richer shell facts
 * directly). Both the REAL wake router and the retirement handler ride it.
 */
const makeHarness = (
  ledgerPath: string,
): Effect.Effect<Harness, never, FileSystem.FileSystem | Path.Path> =>
  Effect.gen(function* () {
    const commands: OrchestrationCommand[] = [];
    const threads = new Map<string, OrchestrationThreadShell>();
    const projects = new Map<string, OrchestrationProject>();
    const psProjects = new Map<string, string>();
    const existingDirs = new Set<string>([WORKSPACE_DIR, OTHER_WORKSPACE_DIR]);
    let openRuns: AssignedWorkQueueEntry[] | null = null;
    let idCounter = 0;
    const nextId = () => {
      idCounter += 1;
      return `id-${idCounter}`;
    };

    // Two read seams, mirroring the production pair: the router reads
    // active-only shells (getThreadShellById filters archived threads), while
    // the retirement executor reads archived shells too
    // (getThreadShellByIdIncludingArchived) because its deletion plan includes
    // them. Deleted threads are absent from the map in both.
    const readActiveThreadShell = (threadId: ThreadId) =>
      Effect.succeed(
        threads.has(threadId) &&
          (threads.get(threadId) as OrchestrationThreadShell).archivedAt === null
          ? Option.some(threads.get(threadId) as OrchestrationThreadShell)
          : Option.none(),
      );

    const readThreadShellIncludingArchived = (threadId: ThreadId) =>
      Effect.succeed(
        threads.has(threadId)
          ? Option.some(threads.get(threadId) as OrchestrationThreadShell)
          : Option.none(),
      );

    const dispatchCommand = (command: OrchestrationCommand) =>
      Effect.sync(() => {
        commands.push(command);
        switch (command.type) {
          case "project.create":
            projects.set(command.projectId, {
              id: command.projectId,
              title: command.title,
              workspaceRoot: command.workspaceRoot,
              defaultModelSelection: command.defaultModelSelection ?? null,
              scripts: [],
              createdAt: command.createdAt,
              updatedAt: command.createdAt,
              deletedAt: null,
            });
            break;
          case "thread.create":
            threads.set(
              command.threadId,
              makeThreadShell(command.threadId, command.projectId, (shell) => ({
                ...shell,
                logicalAgentId: command.logicalAgentId ?? null,
              })),
            );
            break;
          case "thread.delete":
            threads.delete(command.threadId);
            break;
          default:
            break;
        }
      }).pipe(
        Effect.mapError(
          () =>
            new ProjectWorkRoutingError({ code: "CONSUMER_INTERNAL", detail: "dispatch failed" }),
        ),
      );

    const getActiveProjectByWorkspaceRoot = (workspaceRoot: string) => {
      const active = [...projects.values()].find(
        (project) => project.workspaceRoot === workspaceRoot && project.deletedAt === null,
      );
      return Effect.succeed(active === undefined ? Option.none() : Option.some(active));
    };

    const normalizeWorkspaceDir = (workspaceDir: string) =>
      existingDirs.has(workspaceDir)
        ? Effect.succeed(workspaceDir)
        : Effect.fail(
            new ProjectWorkRoutingError({
              code: "AGENT_NOT_DISPATCHABLE",
              detail: `the workspace directory does not exist (${workspaceDir})`,
            }),
          );

    const listActiveProjectRoots = () =>
      Effect.succeed(
        [...projects.values()]
          .filter((project) => project.deletedAt === null)
          .map((project) => ({ projectId: project.id, workspaceRoot: project.workspaceRoot })),
      );

    const seedProject = (projectId: string, workspaceRoot: string) => {
      projects.set(projectId, {
        id: ProjectId.make(projectId),
        title: `Project ${projectId}`,
        workspaceRoot,
        defaultModelSelection: null,
        scripts: [],
        createdAt: ISO,
        updatedAt: ISO,
        deletedAt: null,
      } satisfies OrchestrationProject);
    };
    seedProject(T3_PROJECT_ID, WORKSPACE_DIR);
    seedProject(OTHER_T3_PROJECT_ID, OTHER_WORKSPACE_DIR);

    const router = yield* makeProjectWorkSessionRouter({
      readSettings: Effect.succeed(makeSettings()),
      readThreadShell: readActiveThreadShell,
      readProjectShell: (projectId) =>
        Effect.succeed(
          projects.has(projectId)
            ? Option.some({
                id: (projects.get(projectId) as OrchestrationProject).id,
                title: (projects.get(projectId) as OrchestrationProject).title,
                workspaceRoot: (projects.get(projectId) as OrchestrationProject).workspaceRoot,
                defaultModelSelection: (projects.get(projectId) as OrchestrationProject)
                  .defaultModelSelection,
                scripts: (projects.get(projectId) as OrchestrationProject).scripts,
                createdAt: (projects.get(projectId) as OrchestrationProject).createdAt,
                updatedAt: (projects.get(projectId) as OrchestrationProject).updatedAt,
              })
            : Option.none(),
        ),
      resolveProviderDriver: (instanceId) =>
        Effect.succeed(
          instanceId === PROVIDER_INSTANCE
            ? Option.some(ProviderDriverKind.make("codex"))
            : Option.none(),
        ),
      normalizeWorkspaceDir,
      getActiveProjectByWorkspaceRoot,
      canonicalizeWorkspaceRoot: (workspaceRoot) => Effect.succeed(workspaceRoot),
      listActiveProjectRoots,
      createdProjectDefaultModelSelection: CREATED_PROJECT_MODEL_SELECTION,
      listOpenAssignedWork: () => Effect.succeed(openRuns ?? []),
      // The finalization ledger write never fires in these tests; the dep is
      // required by the shared router seam regardless.
      recordFlowSessionRoute: () => Effect.void,
      dispatchCommand,
      nowIso: Effect.succeed(ISO),
      newId: Effect.sync(nextId),
    });

    const makeRetirement = (): Effect.Effect<
      ProjectRetirementHandler,
      never,
      FileSystem.FileSystem | Path.Path
    > =>
      Effect.gen(function* () {
        const ledgerStore = yield* makeFileProjectRetirementLedgerStore(ledgerPath);
        return yield* makeProjectRetirementHandler({
          loadLedger: ledgerStore.load,
          storeLedger: ledgerStore.store,
          readThreadShell: readThreadShellIncludingArchived,
          listProjectThreadShells: (projectId) =>
            Effect.succeed(
              [...threads.values()].filter((thread) => thread.projectId === projectId),
            ),
          routingSessions: router.snapshotSessions,
          dropProjectSessions: router.dropProjectSessions,
          listProjectServiceProjects: Effect.succeed(
            [...psProjects.entries()].map(([projectId, workspaceDir]) => ({
              projectId,
              workspaceDir,
            })),
          ),
          normalizeWorkspaceDir,
          getActiveProjectByWorkspaceRoot,
          canonicalizeWorkspaceRoot: (workspaceRoot) => Effect.succeed(workspaceRoot),
          listActiveProjectRoots,
          dispatchCommand,
          nowIso: Effect.succeed(ISO_NEXT),
          newId: Effect.sync(nextId),
        });
      });

    return {
      router,
      retirement: yield* makeRetirement(),
      commands,
      putThread: (shell) => threads.set(shell.id, shell),
      getThread: (threadId) => threads.get(threadId),
      setOpenWork: (runs) => {
        openRuns = runs;
      },
      putProjectServiceProject: (projectId, workspaceDir) => {
        psProjects.set(projectId, workspaceDir);
      },
      ledgerPath,
      restart: makeRetirement,
    } satisfies Harness;
  });

const countThreadDeletes = (commands: ReadonlyArray<OrchestrationCommand>): number =>
  commands.filter((command) => command.type === "thread.delete").length;

const countProjectCreates = (commands: ReadonlyArray<OrchestrationCommand>): number =>
  commands.filter((command) => command.type === "project.create").length;

const readLedgerRecords = (ledgerPath: string) =>
  makeFileProjectRetirementLedgerStore(ledgerPath).pipe(Effect.flatMap((store) => store.load));

const makeTempLedgerPath = Effect.gen(function* () {
  const path = yield* Path.Path;
  const fs = yield* FileSystem.FileSystem;
  const tempDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3code-retirement-" });
  return path.join(tempDir, "retirement-ledger.json");
});

it.layer(NodeServices.layer, { excludeTestServices: true })("ProjectRetirement", (it) => {
  describe("isWorkSessionSafeToRetire", () => {
    it.effect("blocks a session blocked on a human answer", () =>
      Effect.sync(() => {
        const blocked = makeThreadShell("t_blocked", T3_PROJECT_ID, (shell) => ({
          ...shell,
          hasPendingUserInput: true,
        }));
        assert.isFalse(isWorkSessionSafeToRetire(blocked, { now: ISO_NEXT }));
      }),
    );

    it.effect("blocks a running session and a queued turn start, allows the stale case", () =>
      Effect.sync(() => {
        const running = makeThreadShell("t_running", T3_PROJECT_ID, (shell) => ({
          ...shell,
          session: runningSession(shell.id),
        }));
        assert.isFalse(isWorkSessionSafeToRetire(running, { now: ISO_NEXT }));
        // A user message no turn adopted yet, inside the grace window.
        const queued = makeThreadShell("t_queued", T3_PROJECT_ID, (shell) => ({
          ...shell,
          latestUserMessageAt: ISO_NEXT,
        }));
        assert.isFalse(isWorkSessionSafeToRetire(queued, { now: ISO_NEXT }));
        // The same unadopted message beyond the grace window is stale, not
        // pending work — the deletion may proceed.
        assert.isAbove(
          Date.parse(ISO_NEXT) - Date.parse(ISO_STALE),
          RETIREMENT_QUEUED_TURN_START_GRACE_MS,
        );
        const stale = makeThreadShell("t_stale", T3_PROJECT_ID, (shell) => ({
          ...shell,
          latestUserMessageAt: ISO_STALE,
        }));
        assert.isTrue(isWorkSessionSafeToRetire(stale, { now: ISO_NEXT }));
      }),
    );

    it.effect("allows an idle session with a settled turn", () =>
      Effect.sync(() => {
        const idle = settleThread(makeThreadShell("t_idle", T3_PROJECT_ID));
        assert.isTrue(isWorkSessionSafeToRetire(idle, { now: ISO_NEXT }));
      }),
    );
  });

  describe("handleRetiredNotice", () => {
    it.effect("deletes every routed session, clears routing, and ACKs", () =>
      Effect.gen(function* () {
        const harness = yield* makeHarness(yield* makeTempLedgerPath);

        // A live routed session through the real router (open work -> wake).
        harness.putProjectServiceProject(PS_PROJECT_ID, WORKSPACE_DIR);
        harness.setOpenWork([syntheticOpenRun("run-1")]);
        yield* harness.router.routeWake({
          agentId: AGENT_ID,
          projectId: PS_PROJECT_ID,
          workspaceDir: WORKSPACE_DIR,
        });
        const liveThreadId = (yield* harness.router.snapshotSessions)[0]?.threadId;
        assert.isDefined(liveThreadId);
        harness.putThread(
          settleThread(harness.getThread(liveThreadId as string) as OrchestrationThreadShell),
        );
        // A routed session only the durable shell scan can find (the
        // registry lost it — e.g. a restart since the wake), archived.
        harness.putThread(
          makeThreadShell("t_archived_routed", T3_PROJECT_ID, (shell) =>
            settleThread({ ...shell, archivedAt: ISO }),
          ),
        );
        // A human thread in the same T3 project: never a work session.
        harness.putThread(
          makeThreadShell("t_human", T3_PROJECT_ID, (shell) => ({
            ...shell,
            logicalAgentId: null,
          })),
        );
        // Another Project Service project's session: out of scope.
        harness.putThread(settleThread(makeThreadShell("t_other", OTHER_T3_PROJECT_ID)));

        const outcome = yield* harness.retirement.handleRetiredNotice({
          noticeId: "notice-1",
          projectId: PS_PROJECT_ID,
          workspaceDir: WORKSPACE_DIR,
        });

        assert.deepStrictEqual(outcome.status, "acked");
        assert.deepStrictEqual(
          new Set(outcome.status === "acked" ? outcome.deletedThreadIds : []),
          new Set([liveThreadId, ThreadId.make("t_archived_routed")]),
        );
        // The human thread and the other project's session survive; so do
        // BOTH T3 projects — retirement never deletes the local project.
        assert.isDefined(harness.getThread("t_human"));
        assert.isDefined(harness.getThread("t_other"));
        assert.strictEqual(countThreadDeletes(harness.commands), 2);
        // Routing is cleared: nothing routes for the retired project.
        assert.isEmpty(yield* harness.router.snapshotSessions);
        // The ledger record is acked durably.
        const records = yield* readLedgerRecords(harness.ledgerPath);
        assert.strictEqual(records.length, 1);
        assert.strictEqual(records[0]?.status, "acked");
      }),
    );

    it.effect("deletes an archived work session via thread.delete instead of marking it gone", () =>
      Effect.gen(function* () {
        const harness = yield* makeHarness(yield* makeTempLedgerPath);
        harness.putProjectServiceProject(PS_PROJECT_ID, WORKSPACE_DIR);
        // An archived routed session ONLY the durable shell scan can find —
        // the shape a restart leaves most often. The router's active-only
        // read cannot see it; the executor's archived-including read must.
        harness.putThread(
          makeThreadShell("t_archived_only", T3_PROJECT_ID, (shell) =>
            settleThread({ ...shell, archivedAt: ISO }),
          ),
        );

        const outcome = yield* harness.retirement.handleRetiredNotice({
          noticeId: "notice-archived",
          projectId: PS_PROJECT_ID,
          workspaceDir: WORKSPACE_DIR,
        });

        // The archived session was REALLY deleted: a thread.delete was
        // dispatched for it and the shell is gone. (The production bug this
        // pins: the executor's read filtered archived threads, returned
        // None, and the notice was ACKed as deleted with no dispatch.)
        assert.deepStrictEqual(outcome.status, "acked");
        assert.deepStrictEqual(outcome.status === "acked" ? outcome.deletedThreadIds : [], [
          ThreadId.make("t_archived_only"),
        ]);
        const archivedDelete = harness.commands.find(
          (command) =>
            command.type === "thread.delete" &&
            command.threadId === ThreadId.make("t_archived_only"),
        );
        assert.isDefined(archivedDelete);
        assert.isUndefined(harness.getThread("t_archived_only"));
        const records = yield* readLedgerRecords(harness.ledgerPath);
        assert.strictEqual(records[0]?.status, "acked");
        assert.deepStrictEqual(records[0]?.deletedThreadIds, [ThreadId.make("t_archived_only")]);
      }),
    );

    it.effect("a redelivered noticeId repeats no side effects", () =>
      Effect.gen(function* () {
        const harness = yield* makeHarness(yield* makeTempLedgerPath);
        harness.putProjectServiceProject(PS_PROJECT_ID, WORKSPACE_DIR);
        harness.putThread(settleThread(makeThreadShell("t_routed_1", T3_PROJECT_ID)));

        const first = yield* harness.retirement.handleRetiredNotice({
          noticeId: "notice-1",
          projectId: PS_PROJECT_ID,
          workspaceDir: WORKSPACE_DIR,
        });
        assert.strictEqual(first.status, "acked");
        const deletesAfterFirst = countThreadDeletes(harness.commands);

        const second = yield* harness.retirement.handleRetiredNotice({
          noticeId: "notice-1",
          projectId: PS_PROJECT_ID,
          workspaceDir: WORKSPACE_DIR,
        });
        assert.strictEqual(second.status, "acked");
        assert.strictEqual(countThreadDeletes(harness.commands), deletesAfterFirst);
      }),
    );

    it.effect("a session that is running or awaits input blocks deletion and the ACK", () =>
      Effect.gen(function* () {
        const harness = yield* makeHarness(yield* makeTempLedgerPath);
        harness.putProjectServiceProject(PS_PROJECT_ID, WORKSPACE_DIR);
        harness.putThread(
          makeThreadShell("t_running", T3_PROJECT_ID, (shell) => ({
            ...shell,
            session: runningSession(shell.id),
          })),
        );
        harness.putThread(
          makeThreadShell("t_awaiting", T3_PROJECT_ID, (shell) => ({
            ...shell,
            hasPendingApprovals: true,
          })),
        );

        const outcome = yield* harness.retirement.handleRetiredNotice({
          noticeId: "notice-2",
          projectId: PS_PROJECT_ID,
          workspaceDir: WORKSPACE_DIR,
        });
        assert.deepStrictEqual(outcome.status, "waiting");
        assert.strictEqual(countThreadDeletes(harness.commands), 0);
        assert.isDefined(harness.getThread("t_running"));
        assert.isDefined(harness.getThread("t_awaiting"));
        // The record stays pending durably — the notice was not ACKed.
        const records = yield* readLedgerRecords(harness.ledgerPath);
        assert.strictEqual(records[0]?.status, "pending");

        // The sessions settle; the resume pass finishes the cleanup.
        harness.putThread(settleThread(harness.getThread("t_running") as OrchestrationThreadShell));
        harness.putThread(
          settleThread(harness.getThread("t_awaiting") as OrchestrationThreadShell),
        );
        yield* harness.retirement.resumePending;
        assert.isUndefined(harness.getThread("t_running"));
        assert.isUndefined(harness.getThread("t_awaiting"));
        assert.strictEqual(countThreadDeletes(harness.commands), 2);
        const ackedRecords = yield* readLedgerRecords(harness.ledgerPath);
        assert.strictEqual(ackedRecords[0]?.status, "acked");
        // The redelivered notice now resolves as done, with no new deletes.
        const redelivered = yield* harness.retirement.handleRetiredNotice({
          noticeId: "notice-2",
          projectId: PS_PROJECT_ID,
          workspaceDir: WORKSPACE_DIR,
        });
        assert.strictEqual(redelivered.status, "acked");
        assert.strictEqual(countThreadDeletes(harness.commands), 2);
      }),
    );

    it.effect("a restart resumes a pending cleanup from the ledger alone", () =>
      Effect.gen(function* () {
        const ledgerPath = yield* makeTempLedgerPath;
        const harness = yield* makeHarness(ledgerPath);
        harness.putProjectServiceProject(PS_PROJECT_ID, WORKSPACE_DIR);
        harness.putThread(
          makeThreadShell("t_blocked_restart", T3_PROJECT_ID, (shell) => ({
            ...shell,
            session: runningSession(shell.id),
          })),
        );
        const outcome = yield* harness.retirement.handleRetiredNotice({
          noticeId: "notice-3",
          projectId: PS_PROJECT_ID,
          workspaceDir: WORKSPACE_DIR,
        });
        assert.strictEqual(outcome.status, "waiting");

        // The blocked session settles and the process restarts: a fresh
        // handler over the same ledger file. The plan on disk is the only
        // memory of the un-ACKed notice.
        harness.putThread(
          settleThread(harness.getThread("t_blocked_restart") as OrchestrationThreadShell),
        );
        const restarted = yield* harness.restart();
        yield* restarted.resumePending;
        assert.isUndefined(harness.getThread("t_blocked_restart"));
        assert.strictEqual(countThreadDeletes(harness.commands), 1);
        const records = yield* readLedgerRecords(ledgerPath);
        assert.strictEqual(records[0]?.status, "acked");
      }),
    );

    it.effect("Work arriving after the retirement creates a new session", () =>
      Effect.gen(function* () {
        const harness = yield* makeHarness(yield* makeTempLedgerPath);
        harness.putProjectServiceProject(PS_PROJECT_ID, WORKSPACE_DIR);
        harness.setOpenWork([syntheticOpenRun("run-1")]);
        yield* harness.router.routeWake({
          agentId: AGENT_ID,
          projectId: PS_PROJECT_ID,
          workspaceDir: WORKSPACE_DIR,
        });
        const firstThreadId = (yield* harness.router.snapshotSessions)[0]?.threadId;
        assert.isDefined(firstThreadId);
        harness.putThread(
          settleThread(harness.getThread(firstThreadId as string) as OrchestrationThreadShell),
        );

        const outcome = yield* harness.retirement.handleRetiredNotice({
          noticeId: "notice-4",
          projectId: PS_PROJECT_ID,
          workspaceDir: WORKSPACE_DIR,
        });
        assert.strictEqual(outcome.status, "acked");
        assert.isUndefined(harness.getThread(firstThreadId as string));

        // The Project Service sends Work for the project again: a fresh
        // session routes under the SAME T3 project (no second create).
        harness.setOpenWork([syntheticOpenRun("run-2")]);
        const createsBefore = countProjectCreates(harness.commands);
        yield* harness.router.routeWake({
          agentId: AGENT_ID,
          projectId: PS_PROJECT_ID,
          workspaceDir: WORKSPACE_DIR,
        });
        const sessions = yield* harness.router.snapshotSessions;
        assert.strictEqual(sessions.length, 1);
        assert.notStrictEqual(sessions[0]?.threadId, firstThreadId);
        assert.strictEqual(countProjectCreates(harness.commands), createsBefore);
      }),
    );

    it.effect("a notice with nothing local to delete ACKs immediately", () =>
      Effect.gen(function* () {
        const harness = yield* makeHarness(yield* makeTempLedgerPath);
        const outcome = yield* harness.retirement.handleRetiredNotice({
          noticeId: "notice-5",
          projectId: PS_PROJECT_ID,
          workspaceDir: WORKSPACE_DIR,
        });
        assert.strictEqual(outcome.status, "acked");
        assert.isEmpty(outcome.status === "acked" ? outcome.deletedThreadIds : ["x"]);
        assert.strictEqual(countThreadDeletes(harness.commands), 0);
      }),
    );
  });
});
