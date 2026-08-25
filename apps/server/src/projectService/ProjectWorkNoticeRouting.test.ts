import { describe, assert, it } from "@effect/vitest";
import {
  DEFAULT_MODEL,
  DEFAULT_SERVER_SETTINGS,
  type LogicalAgentConfig,
  LogicalAgentId,
  type ModelSelection,
  type OrchestrationCommand,
  type OrchestrationProject,
  type OrchestrationProjectShell,
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
import * as Option from "effect/Option";

import {
  applyThinkLevelToOptions,
  flowInstanceWorkSessionThreadTitle,
  makeProjectWorkSessionRouter,
  type ProjectWorkSessionRouter,
  ProjectWorkRoutingError,
  resolveProjectWorkRouting,
  workSessionThreadTitle,
  worktreeBindingFor,
  type ProjectWorkSessionRouterDeps,
  type ProjectWorkWakeInput,
} from "./ProjectWorkNoticeRouting.ts";
import { type AssignedWorkQueueEntry, assignedWorkWakeMessage } from "./AssignedWorkQueue.ts";

const AGENT_ID = "ag_primary";
const OTHER_AGENT_ID = "ag_secondary";
const PS_PROJECT_ID = "ps_proj_1";
const PROVIDER_INSTANCE = "codex-main";
const ISO = "2026-08-14T12:00:00.000Z";
/** The notice's workspace directory — always an existing directory on disk. */
const WORKSPACE_DIR = "/tmp/registry";

const makeSettings = (mutations?: (settings: ServerSettings) => ServerSettings) => {
  const agents: Record<string, LogicalAgentConfig> = {
    [LogicalAgentId.make(AGENT_ID)]: {
      agentName: "Primary Agent",
      providerInstanceId: ProviderInstanceId.make(PROVIDER_INSTANCE),
      persona: "",
      thinkLevel: null,
      modelOverride: null,
      project: { enabled: true, sessionScope: "project", sessionRetention: "settle" },
    },
    [LogicalAgentId.make(OTHER_AGENT_ID)]: {
      agentName: "Secondary Agent",
      providerInstanceId: ProviderInstanceId.make("other-instance"),
      persona: "",
      thinkLevel: null,
      modelOverride: null,
      project: { enabled: false, sessionScope: "project", sessionRetention: "settle" },
    },
  };
  const base: ServerSettings = {
    ...DEFAULT_SERVER_SETTINGS,
    projectServiceClient: {
      enabled: true,
      baseUrl: "http://127.0.0.1:7600",
      keyIdHint: "key-1",
      credentialSet: true,
    },
    logicalAgents: agents as ServerSettings["logicalAgents"],
  };
  return mutations === undefined ? base : mutations(base);
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
    modelSelection: {
      instanceId: ProviderInstanceId.make(PROVIDER_INSTANCE),
      model: "gpt-5.6-sol",
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

const stoppedSession = (threadId: string): OrchestrationSession => ({
  ...runningSession(threadId),
  status: "stopped",
});

/**
 * A latestTurn that PROVABLY ran and settled strictly AFTER the harness's
 * delivery timestamp (ISO) — the shell-fact form of the busy-window
 * observation the event stream may have coalesced past. The projector keeps
 * a queued turn's predecessor in place, so a shell WITHOUT this fact means
 * the aggregate's turn never engaged (the queued-turn window).
 */
const settledTurn = (): NonNullable<OrchestrationThreadShell["latestTurn"]> => ({
  turnId: TurnId.make("turn_settled"),
  state: "completed",
  requestedAt: ISO,
  startedAt: ISO,
  completedAt: "2026-08-14T12:00:01.000Z",
  assistantMessageId: null,
});

interface Harness {
  readonly router: ProjectWorkSessionRouter;
  readonly commands: OrchestrationCommand[];
  readonly setSettings: (settings: ServerSettings) => void;
  readonly putThread: (shell: OrchestrationThreadShell) => void;
  readonly removeThread: (threadId: string) => void;
  readonly setOpenWorkCount: (count: number) => void;
  /** Replace the synthetic runs entirely (workspace facts, ordering, ids). */
  readonly setOpenRuns: (runs: AssignedWorkQueueEntry[] | null) => void;
  readonly failWorkCount: (shouldFail: boolean) => void;
  readonly countCalls: () => number;
  readonly failDispatch: (shouldFail: boolean) => void;
  readonly removeProvider: () => void;
  /** Remove a directory from the (simulated) disk. */
  readonly removeDir: (dir: string) => void;
  /** Add a directory to the (simulated) disk. */
  readonly addDir: (dir: string) => void;
  /** Seed an active project at a workspace root, as the engine's projection would. */
  readonly putProject: (id: string, workspaceRoot: string) => void;
  /**
   * Register a symlink spelling: canonicalizeWorkspaceRoot maps `stored` to
   * `canonical` (issue #6 review — legacy roots may not be canonical).
   */
  readonly aliasRoot: (stored: string, canonical: string) => void;
  /** Archive/delete a project, as the engine's projection would. */
  readonly removeProject: (id: string) => void;
  /**
   * Arm the concurrent-create race: the next project.create dispatch first
   * projects ANOTHER project at the same root (the concurrent winner) and
   * then fails with the engine's active-workspaceRoot-taken invariant.
   */
  readonly armCreateRace: () => void;
  /** Simulated latency on the authoritative Work query (real ms, it.live). */
  readonly setWorkCountDelayMs: (ms: number) => void;
  /** Simulated latency on orchestration dispatch (real ms, it.live). */
  readonly setDispatchDelayMs: (ms: number) => void;
}

const CREATED_PROJECT_MODEL_SELECTION: ModelSelection = {
  instanceId: ProviderInstanceId.make(PROVIDER_INSTANCE),
  model: DEFAULT_MODEL,
};

const makeHarness = (settings: ServerSettings): Effect.Effect<Harness> =>
  Effect.gen(function* () {
    const commands: OrchestrationCommand[] = [];
    let currentSettings = settings;
    const threads = new Map<string, OrchestrationThreadShell>();
    // The simulated filesystem: directories that exist for normalization.
    const existingDirs = new Set<string>([WORKSPACE_DIR]);
    // The projection of active projects, keyed twice: by id and by root.
    const projectsById = new Map<string, OrchestrationProject>();
    // Symlink spellings a stored root canonicalizes to (review fixture).
    const canonicalAliases = new Map<string, string>();
    const providers = new Map<string, ProviderDriverKind>([
      [PROVIDER_INSTANCE, ProviderDriverKind.make("codex")],
    ]);
    let openWorkCount = 2;
    // When set, the authoritative Work query answers these runs instead of
    // the synthetic count fixture (workspace-facts and drain scenarios).
    let openRunsOverride: AssignedWorkQueueEntry[] | null = null;
    // Deterministic fake runs for the queue: oldest-first by construction.
    const syntheticOpenRuns = (count: number): Array<AssignedWorkQueueEntry> =>
      Array.from({ length: count }, (_, index) => ({
        runId: `run-${count - index}`,
        positionId: `position-${count - index}`,
        runRevision: `run:${index + 1}`,
        state: "open",
        agentId: AGENT_ID,
        task: { prompt: `工作 ${count - index}: do the thing` },
        createdAt: `2026-08-21T00:00:${String(10 + index).padStart(2, "0")}Z`,
      }));
    let workCountShouldFail = false;
    let dispatchShouldFail = false;
    let createRaceArmed = false;
    let workCountDelayMs = 0;
    let dispatchDelayMs = 0;
    let countCalls = 0;
    let idCounter = 0;

    const shellFor = (project: OrchestrationProject): OrchestrationProjectShell => ({
      id: project.id,
      title: project.title,
      workspaceRoot: project.workspaceRoot,
      defaultModelSelection: project.defaultModelSelection,
      scripts: project.scripts,
      createdAt: project.createdAt,
      updatedAt: project.updatedAt,
    });

    const deps: ProjectWorkSessionRouterDeps = {
      readSettings: Effect.sync(() => currentSettings),
      readThreadShell: (threadId) =>
        Effect.succeed(
          threads.has(threadId)
            ? Option.some(threads.get(threadId) as OrchestrationThreadShell)
            : Option.none(),
        ),
      readProjectShell: (projectId) =>
        Effect.succeed(
          projectsById.has(projectId)
            ? Option.some(shellFor(projectsById.get(projectId) as OrchestrationProject))
            : Option.none(),
        ),
      resolveProviderDriver: (instanceId) =>
        Effect.succeed(
          providers.has(instanceId)
            ? Option.some(providers.get(instanceId) as ProviderDriverKind)
            : Option.none(),
        ),
      // WorkspacePaths with createIfMissing FALSE: the directory must exist.
      normalizeWorkspaceDir: (workspaceDir) =>
        Effect.succeed(workspaceDir).pipe(
          Effect.flatMap((normalized) =>
            existingDirs.has(normalized)
              ? Effect.succeed(normalized)
              : Effect.fail(
                  new ProjectWorkRoutingError({
                    code: "AGENT_NOT_DISPATCHABLE",
                    detail: `the workspace directory the Project Service pointed at does not exist on this machine (${normalized})`,
                  }),
                ),
          ),
        ),
      getActiveProjectByWorkspaceRoot: (workspaceRoot) => {
        const active = [...projectsById.values()].find(
          (project) => project.workspaceRoot === workspaceRoot && project.deletedAt === null,
        );
        return Effect.succeed(active === undefined ? Option.none() : Option.some(active));
      },
      canonicalizeWorkspaceRoot: (workspaceRoot) =>
        Effect.succeed(canonicalAliases.get(workspaceRoot) ?? workspaceRoot),
      listActiveProjectRoots: () =>
        Effect.succeed(
          [...projectsById.values()]
            .filter((project) => project.deletedAt === null)
            .map((project) => ({ projectId: project.id, workspaceRoot: project.workspaceRoot })),
        ),
      createdProjectDefaultModelSelection: CREATED_PROJECT_MODEL_SELECTION,
      dispatchCommand: (command) => {
        if (dispatchShouldFail) {
          return Effect.fail(
            new ProjectWorkRoutingError({ code: "CONSUMER_INTERNAL", detail: "dispatch failed" }),
          );
        }
        // The dispatch transaction: the engine's active-workspaceRoot-taken
        // invariant (exactly one active project per root) is enforced AT
        // COMMIT TIME, so a concurrent wake that queried before the winner
        // committed loses its create here — after any simulated latency.
        const attempt = (): Effect.Effect<void, ProjectWorkRoutingError> => {
          if (command.type === "project.create") {
            const conflicting = [...projectsById.values()].find(
              (project) =>
                project.workspaceRoot === command.workspaceRoot &&
                project.deletedAt === null &&
                project.id !== command.projectId,
            );
            if (conflicting !== undefined || createRaceArmed) {
              if (createRaceArmed) {
                // The armed race seeds a DIFFERENT winner, as a concurrent
                // wake for another Project Service project on the same
                // directory would.
                createRaceArmed = false;
                projectsById.set("t3_proj_raced", {
                  id: ProjectId.make("t3_proj_raced"),
                  title: "Raced Winner",
                  workspaceRoot: command.workspaceRoot,
                  defaultModelSelection: null,
                  scripts: [],
                  createdAt: ISO,
                  updatedAt: ISO,
                  deletedAt: null,
                });
              }
              return Effect.fail(
                new ProjectWorkRoutingError({
                  code: "CONSUMER_INTERNAL",
                  detail:
                    "orchestration dispatch rejected the routing command (workspaceRoot taken)",
                }),
              );
            }
          }
          return Effect.sync(() => {
            commands.push(command);
            // The engine projects inside the dispatch transaction: by the
            // time dispatch resolves, the state is readable.
            if (command.type === "project.create") {
              projectsById.set(command.projectId as string, {
                id: command.projectId,
                title: command.title,
                workspaceRoot: command.workspaceRoot,
                defaultModelSelection: command.defaultModelSelection ?? null,
                scripts: [],
                createdAt: command.createdAt,
                updatedAt: command.createdAt,
                deletedAt: null,
              });
            }
            if (command.type === "thread.create") {
              threads.set(
                command.threadId,
                makeThreadShell(command.threadId, String(command.projectId)),
              );
            }
          });
        };
        return dispatchDelayMs > 0
          ? Effect.sleep(dispatchDelayMs).pipe(Effect.flatMap(attempt))
          : attempt();
      },
      listOpenAssignedWork: () => {
        countCalls += 1;
        const outcome = workCountShouldFail
          ? Effect.fail(
              new ProjectWorkRoutingError({
                code: "CONSUMER_INTERNAL",
                detail: "authoritative Work query failed",
              }),
            )
          : Effect.succeed(openRunsOverride ?? syntheticOpenRuns(openWorkCount));
        return workCountDelayMs > 0
          ? Effect.sleep(workCountDelayMs).pipe(Effect.flatMap(() => outcome))
          : outcome;
      },
      nowIso: Effect.succeed(ISO),
      newId: Effect.sync(() => {
        idCounter += 1;
        return `id-${idCounter}`;
      }),
    };

    const router = yield* makeProjectWorkSessionRouter(deps);

    return {
      router,
      commands,
      setSettings: (next) => {
        currentSettings = next;
      },
      putThread: (shell) => {
        threads.set(shell.id, shell);
      },
      removeThread: (threadId) => {
        threads.delete(threadId);
      },
      setOpenWorkCount: (count) => {
        openWorkCount = count;
      },
      setOpenRuns: (runs) => {
        openRunsOverride = runs;
      },
      failWorkCount: (shouldFail) => {
        workCountShouldFail = shouldFail;
      },
      countCalls: () => countCalls,
      failDispatch: (shouldFail) => {
        dispatchShouldFail = shouldFail;
      },
      removeProvider: () => {
        providers.delete(PROVIDER_INSTANCE);
      },
      removeDir: (dir) => {
        existingDirs.delete(dir);
      },
      addDir: (dir) => {
        existingDirs.add(dir);
      },
      putProject: (id, workspaceRoot) => {
        projectsById.set(id, {
          id: ProjectId.make(id),
          title: "Registry",
          workspaceRoot,
          defaultModelSelection: null,
          scripts: [],
          createdAt: ISO,
          updatedAt: ISO,
          deletedAt: null,
        });
      },
      removeProject: (id) => {
        projectsById.delete(id);
      },
      aliasRoot: (stored, canonical) => {
        canonicalAliases.set(stored, canonical);
      },
      armCreateRace: () => {
        createRaceArmed = true;
      },
      setWorkCountDelayMs: (ms) => {
        workCountDelayMs = ms;
      },
      setDispatchDelayMs: (ms) => {
        dispatchDelayMs = ms;
      },
    };
  });

const turnStarts = (commands: OrchestrationCommand[]) =>
  commands.filter((command) => command.type === "thread.turn.start");
const threadCreates = (commands: OrchestrationCommand[]) =>
  commands.filter((command) => command.type === "thread.create");
const projectCreates = (commands: OrchestrationCommand[]) =>
  commands.filter((command) => command.type === "project.create");

/**
 * A wake's notice facts. Optional fields may be explicitly UNSET (a pre-V4
 * notice carries no workspaceDir; some notices carry no name), so the
 * mutation values include `undefined` and only present keys ride the input.
 */
const wakeInput = (mutations?: {
  readonly agentId?: string;
  readonly projectName?: string | undefined;
  readonly workspaceDir?: string | undefined;
}): ProjectWorkWakeInput => {
  const projectName: string | undefined =
    mutations !== undefined && "projectName" in mutations ? mutations.projectName : "Registry";
  const workspaceDir: string | undefined =
    mutations !== undefined && "workspaceDir" in mutations ? mutations.workspaceDir : WORKSPACE_DIR;
  return {
    agentId: mutations?.agentId ?? AGENT_ID,
    projectId: PS_PROJECT_ID,
    ...(projectName !== undefined ? { projectName } : {}),
    ...(workspaceDir !== undefined ? { workspaceDir } : {}),
  };
};

const wake = (router: ProjectWorkSessionRouter) => router.routeWake(wakeInput());

it("routing resolution is id-based and structural", () => {
  {
    const settings = makeSettings();

    const resolved = resolveProjectWorkRouting(settings, wakeInput());
    assert.isTrue(resolved.ok);
    assert.deepEqual(resolved.ok && resolved.routing, {
      logicalAgentId: LogicalAgentId.make(AGENT_ID),
      agentName: "Primary Agent",
      providerInstanceId: PROVIDER_INSTANCE,
      thinkLevel: null,
      modelOverride: null,
      projectServiceProjectId: PS_PROJECT_ID,
      projectName: "Registry",
      sessionScope: "project",
      sessionRetention: "settle",
    });
    // The notice's project facts ride through; a name-less notice stays blank.
    const nameless = resolveProjectWorkRouting(settings, wakeInput({ projectName: undefined }));
    assert.isTrue(nameless.ok);
    if (nameless.ok) {
      assert.strictEqual(nameless.routing.projectName, "");
    }

    assert.isFalse(resolveProjectWorkRouting(settings, wakeInput({ agentId: "ag_missing" })).ok);
    // Project-disabled agents are not routable (nor advertised).
    assert.isFalse(resolveProjectWorkRouting(settings, wakeInput({ agentId: OTHER_AGENT_ID })).ok);
    // Disabled integration fails routing for every agent.
    assert.isFalse(
      resolveProjectWorkRouting(
        makeSettings((base) => ({
          ...base,
          projectServiceClient: { ...base.projectServiceClient, enabled: false },
        })),
        wakeInput(),
      ).ok,
    );
    // A pre-V4 notice carries no workspace directory: an explicit failure
    // naming the requirement, never a silent binding fallback.
    const preV4 = resolveProjectWorkRouting(settings, wakeInput({ workspaceDir: undefined }));
    assert.isFalse(preV4.ok);
    assert.match(!preV4.ok && preV4.detail, /workspace directory.*protocol V4/);
    assert.strictEqual(!preV4.ok && preV4.code, "AGENT_NOT_DISPATCHABLE");
    // A blank directory is as good as none.
    assert.isFalse(resolveProjectWorkRouting(settings, wakeInput({ workspaceDir: "  " })).ok);
  }
});

it("notification message and session title formats", () => {
  const head = (prompt: string, createdAt: string): AssignedWorkQueueEntry => ({
    runId: `run-${createdAt}`,
    positionId: "position-1",
    runRevision: "run:1",
    state: "open",
    task: { prompt },
    createdAt,
  });
  // The wake message leads with the CURRENT work's task summary and says how
  // deep the queue is behind it — the agent wakes already knowing its work.
  assert.strictEqual(
    assignedWorkWakeMessage({ current: head("分诊：修复登录", "2026-08-21T00:00:01Z"), queued: 2 }),
    "Your current work: 分诊：修复登录. 2 more items waiting behind it. Use the t3-code Agent Project tools (project_work_list, project_doc_read, project_doc_write, project_doc_edit, and project_work_submit) to inspect and complete the current work first. Do not use human PS Control tools for Agent Work.",
  );
  assert.strictEqual(
    assignedWorkWakeMessage({ current: head("分诊：修复登录", "2026-08-21T00:00:01Z"), queued: 0 }),
    "Your current work: 分诊：修复登录. Use the t3-code Agent Project tools (project_work_list, project_doc_read, project_doc_write, project_doc_edit, and project_work_submit) to inspect and complete the current work first. Do not use human PS Control tools for Agent Work.",
  );
  // The agent name alone titles the session — the UI already groups sessions
  // by project, so same-project sessions stay distinguishable by agent.
  assert.strictEqual(workSessionThreadTitle("coder"), "coder");
  // Whitespace trims; an agent name is never blank in practice, but the title
  // must not carry stray padding either way.
  assert.strictEqual(workSessionThreadTitle("  coder  "), "coder");
});

describe("wake message and agent-level model parameters", () => {
  it("the wake message is the bare aggregate regardless of persona (persona rides the system prompt now)", () =>
    Effect.gen(function* () {
      const settings = makeSettings((base) => ({
        ...base,
        logicalAgents: {
          ...base.logicalAgents,
          [LogicalAgentId.make(AGENT_ID)]: {
            ...base.logicalAgents[LogicalAgentId.make(AGENT_ID)]!,
            persona: "You are the primary agent; be terse.",
          },
        },
      }));
      const harness = yield* makeHarness(settings);
      yield* wake(harness.router);
      assert.strictEqual(
        turnStarts(harness.commands)[0]?.type === "thread.turn.start" &&
          turnStarts(harness.commands)[0]?.message.text,
        assignedWorkWakeMessage({
          current: {
            runId: "run-2",
            positionId: "position-2",
            runRevision: "run:1",
            state: "open",
            agentId: AGENT_ID,
            task: { prompt: "工作 2: do the thing" },
            createdAt: "2026-08-21T00:00:10Z",
          },
          queued: 1,
        }),
      );
    }));

  it("thinkLevel maps to the driver's effort option on the created thread", () =>
    Effect.gen(function* () {
      const settings = makeSettings((base) => ({
        ...base,
        logicalAgents: {
          ...base.logicalAgents,
          [LogicalAgentId.make(AGENT_ID)]: {
            ...base.logicalAgents[LogicalAgentId.make(AGENT_ID)]!,
            thinkLevel: "high",
          },
        },
      }));
      const harness = yield* makeHarness(settings);
      yield* wake(harness.router);
      const created = threadCreates(harness.commands)[0];
      assert.deepEqual(
        created?.type === "thread.create" && created.modelSelection.options,
        // The harness resolves PROVIDER_INSTANCE as a codex instance, so the
        // codex effort option id applies.
        [{ id: "reasoningEffort", value: "high" }],
      );
    }));

  it("an explicit effort option on the model override beats thinkLevel", () =>
    Effect.gen(function* () {
      const settings = makeSettings((base) => ({
        ...base,
        logicalAgents: {
          ...base.logicalAgents,
          [LogicalAgentId.make(AGENT_ID)]: {
            ...base.logicalAgents[LogicalAgentId.make(AGENT_ID)]!,
            thinkLevel: "low",
            modelOverride: {
              instanceId: ProviderInstanceId.make(PROVIDER_INSTANCE),
              model: "override-model",
              options: [{ id: "reasoningEffort", value: "max" }],
            },
          },
        },
      }));
      const harness = yield* makeHarness(settings);
      yield* wake(harness.router);
      const created = threadCreates(harness.commands)[0];
      assert.deepEqual(created?.type === "thread.create" && created.modelSelection.options, [
        { id: "reasoningEffort", value: "max" },
      ]);
    }));

  it("agent modelOverride targeting the agent's instance wins over the project default", () =>
    Effect.gen(function* () {
      const settings = makeSettings((base) => ({
        ...base,
        logicalAgents: {
          ...base.logicalAgents,
          [LogicalAgentId.make(AGENT_ID)]: {
            ...base.logicalAgents[LogicalAgentId.make(AGENT_ID)]!,
            modelOverride: {
              instanceId: ProviderInstanceId.make(PROVIDER_INSTANCE),
              model: "override-model",
            },
          },
        },
      }));
      const harness = yield* makeHarness(settings);
      yield* wake(harness.router);
      const created = threadCreates(harness.commands)[0];
      assert.strictEqual(
        created?.type === "thread.create" && created.modelSelection.model,
        "override-model",
      );
    }));
});

it.effect("missing project: the wake creates it under the notice's directory", () =>
  Effect.gen(function* () {
    const harness = yield* makeHarness(makeSettings());

    yield* wake(harness.router);

    // One auto-created project: caller-minted id, the notice's name as the
    // title, the notice's directory as the root, the bootstrap model default.
    assert.lengthOf(projectCreates(harness.commands), 1);
    const created = projectCreates(harness.commands)[0];
    assert.strictEqual(created?.type === "project.create" && created.workspaceRoot, WORKSPACE_DIR);
    assert.strictEqual(created?.type === "project.create" && created.title, "Registry");
    const createdModel =
      created?.type === "project.create" && created.defaultModelSelection !== null
        ? created.defaultModelSelection?.model
        : undefined;
    assert.strictEqual(createdModel, DEFAULT_MODEL);
    const t3ProjectId = created?.type === "project.create" ? String(created.projectId) : "";

    // The session and its aggregate live under the RESOLVED project.
    assert.lengthOf(threadCreates(harness.commands), 1);
    assert.strictEqual(
      threadCreates(harness.commands)[0]?.type === "thread.create" &&
        String(threadCreates(harness.commands)[0]?.projectId),
      t3ProjectId,
    );
    assert.strictEqual(
      threadCreates(harness.commands)[0]?.type === "thread.create" &&
        threadCreates(harness.commands)[0]?.title,
      "Primary Agent",
    );
    // The thread is stamped with the logical agent the wake routed to, so
    // MCP tool calls resolve identity from the session, not the instance.
    assert.strictEqual(
      threadCreates(harness.commands)[0]?.type === "thread.create" &&
        String(threadCreates(harness.commands)[0]?.logicalAgentId),
      AGENT_ID,
    );
    assert.lengthOf(turnStarts(harness.commands), 1);
    assert.strictEqual(
      turnStarts(harness.commands)[0]?.type === "thread.turn.start" &&
        turnStarts(harness.commands)[0]?.message.text,
      assignedWorkWakeMessage({
        current: {
          runId: "run-2",
          positionId: "position-2",
          runRevision: "run:1",
          state: "open",
          agentId: AGENT_ID,
          task: { prompt: "工作 2: do the thing" },
          createdAt: "2026-08-21T00:00:10Z",
        },
        queued: 1,
      }),
    );

    const sessions = yield* harness.router.snapshotSessions;
    assert.lengthOf(sessions, 1);
    assert.strictEqual(sessions[0]?.phase, "notifying");
  }),
);

it.effect("second wake REUSES the created project — no second create", () =>
  Effect.gen(function* () {
    const harness = yield* makeHarness(makeSettings());
    yield* wake(harness.router);
    const [created] = threadCreates(harness.commands);
    const threadId = created?.type === "thread.create" ? created.threadId : undefined;
    assert.isDefined(threadId);

    // The aggregate's turn engaged, then finished: the session is idle again.
    const engage = (status: "running" | "ready") =>
      harness.putThread(
        makeThreadShell(threadId as string, String(created?.projectId), (shell) => ({
          ...shell,
          session: { ...runningSession(threadId as string), status },
        })),
      );
    engage("running");
    yield* harness.router.onThreadEvent(threadId as never);
    engage("ready");
    yield* harness.router.onThreadEvent(threadId as never);

    yield* wake(harness.router);

    assert.lengthOf(projectCreates(harness.commands), 1);
    assert.lengthOf(threadCreates(harness.commands), 1);
    assert.lengthOf(turnStarts(harness.commands), 2);
  }),
);

it.effect("existing active project at the root is REUSED across wakes", () =>
  Effect.gen(function* () {
    const harness = yield* makeHarness(makeSettings());
    harness.putProject("t3_existing", WORKSPACE_DIR);

    yield* wake(harness.router);
    yield* wake(harness.router);

    assert.isEmpty(projectCreates(harness.commands));
    assert.lengthOf(threadCreates(harness.commands), 1);
    assert.strictEqual(
      threadCreates(harness.commands)[0]?.type === "thread.create" &&
        String(threadCreates(harness.commands)[0]?.projectId),
      "t3_existing",
    );
  }),
);

it.effect(
  "issue #6 review: a symlink-spelled stored root is REUSED by canonical key, never duplicated",
  () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness(makeSettings());
      // The legacy project was created through the symlink spelling; the notice
      // (Project Service V4) carries the realpath'd form. The exact-root query
      // misses — without the canonical-key scan a SECOND project would fork.
      harness.putProject("t3_legacy", "/tmp/sym-root");
      harness.aliasRoot("/tmp/sym-root", "/private/tmp/sym-root");
      harness.addDir("/private/tmp/sym-root");

      const target = wakeInput({ workspaceDir: "/private/tmp/sym-root" });
      yield* harness.router.routeWake(target);
      yield* harness.router.routeWake(target);

      assert.isEmpty(projectCreates(harness.commands));
      assert.lengthOf(threadCreates(harness.commands), 1);
      assert.strictEqual(
        threadCreates(harness.commands)[0]?.type === "thread.create" &&
          String(threadCreates(harness.commands)[0]?.projectId),
        "t3_legacy",
      );
    }),
);

it.effect("a name-less notice titles the project from the stable Project Service id", () =>
  Effect.gen(function* () {
    const harness = yield* makeHarness(makeSettings());

    yield* harness.router.routeWake(wakeInput({ projectName: undefined }));

    // One naming authority: the project title reuses the Project Service
    // name; with no name to reuse the stable id stands in — the directory
    // never leaks into titles.
    const created = projectCreates(harness.commands)[0];
    assert.strictEqual(created?.type === "project.create" && created.title, PS_PROJECT_ID);
    // The thread title is the agent name alone regardless.
    assert.strictEqual(
      threadCreates(harness.commands)[0]?.type === "thread.create" &&
        threadCreates(harness.commands)[0]?.title,
      "Primary Agent",
    );
  }),
);

it.effect("directory missing on disk fails the wake with a routing detail", () =>
  Effect.gen(function* () {
    const harness = yield* makeHarness(makeSettings());
    harness.removeDir(WORKSPACE_DIR);

    const failure = yield* wake(harness.router).pipe(Effect.flip);
    assert.strictEqual(failure.code, "AGENT_NOT_DISPATCHABLE");
    assert.match(failure.detail, /does not exist on this machine/);
    // Nothing was created or delivered.
    assert.isEmpty(harness.commands);
    assert.isEmpty(yield* harness.router.snapshotSessions);
  }),
);

it.effect("concurrent-create invariant loss falls back to the winner", () =>
  Effect.gen(function* () {
    const harness = yield* makeHarness(makeSettings());
    harness.armCreateRace();

    yield* wake(harness.router);

    // The racing create failed, the winner is reused: the wake still routed.
    assert.isEmpty(projectCreates(harness.commands));
    assert.lengthOf(threadCreates(harness.commands), 1);
    assert.strictEqual(
      threadCreates(harness.commands)[0]?.type === "thread.create" &&
        String(threadCreates(harness.commands)[0]?.projectId),
      "t3_proj_raced",
    );
    assert.lengthOf(turnStarts(harness.commands), 1);
  }),
);

it.effect("idle session: a replayed wake for the same Work set stays silent", () =>
  Effect.gen(function* () {
    const harness = yield* makeHarness(makeSettings());
    yield* wake(harness.router);
    const [created] = threadCreates(harness.commands);
    const threadId = created?.type === "thread.create" ? created.threadId : undefined;
    const projectId = String(created?.projectId);
    assert.isDefined(threadId);

    // The aggregate's turn engaged, then finished: the unchanged head draws
    // the one A3 reminder (turn ended without a submit), and the reminder's
    // own turn must also run out before the session is idle again.
    const engage = (status: "running" | "ready") =>
      harness.putThread(
        makeThreadShell(threadId as string, projectId, (shell) => ({
          ...shell,
          session: { ...runningSession(threadId as string), status },
        })),
      );
    engage("running");
    yield* harness.router.onThreadEvent(threadId as never);
    engage("ready");
    yield* harness.router.onThreadEvent(threadId as never);
    assert.lengthOf(turnStarts(harness.commands), 2);
    engage("running");
    yield* harness.router.onThreadEvent(threadId as never);
    engage("ready");
    yield* harness.router.onThreadEvent(threadId as never);
    assert.strictEqual((yield* harness.router.snapshotSessions)[0]?.phase, "idle");

    yield* wake(harness.router);
    assert.lengthOf(turnStarts(harness.commands), 2);

    // An idle, already-notified session with no new work stays quiet.
    harness.putThread(
      makeThreadShell(threadId as string, projectId, (shell) => ({
        ...shell,
        session: readySession(threadId as string),
      })),
    );
    yield* harness.router.onThreadEvent(threadId as never);
    assert.lengthOf(turnStarts(harness.commands), 2);
  }),
);

it.effect("busy session: work is recorded, never interrupted, then coalesced after the turn", () =>
  Effect.gen(function* () {
    const harness = yield* makeHarness(makeSettings());
    yield* wake(harness.router);
    const [created] = threadCreates(harness.commands);
    const threadId = created?.type === "thread.create" ? created.threadId : null;
    const projectId = String(created?.projectId);
    assert.isNotNull(threadId);
    assert.lengthOf(turnStarts(harness.commands), 1);

    // The aggregate turn is running.
    harness.putThread(
      makeThreadShell(threadId as string, projectId, (shell) => ({
        ...shell,
        session: runningSession(threadId as string),
      })),
    );

    // A new Work appears behind the current one. Replayed notices for that
    // same authoritative set coalesce into one pending update.
    harness.setOpenWorkCount(3);
    yield* wake(harness.router);
    yield* wake(harness.router);
    yield* wake(harness.router);
    assert.lengthOf(turnStarts(harness.commands), 1);
    let sessions = yield* harness.router.snapshotSessions;
    assert.isTrue(sessions[0]?.pendingWork);

    // Mid-turn events change nothing.
    yield* harness.router.onThreadEvent(threadId as never);
    assert.lengthOf(turnStarts(harness.commands), 1);

    // Turn finishes: exactly ONE coalesced aggregate, refreshed from the
    // authoritative count.
    const countBefore = harness.countCalls();
    harness.putThread(
      makeThreadShell(threadId as string, projectId, (shell) => ({
        ...shell,
        session: readySession(threadId as string),
      })),
    );
    yield* harness.router.onThreadEvent(threadId as never);
    assert.lengthOf(turnStarts(harness.commands), 2);
    assert.strictEqual(harness.countCalls(), countBefore + 1);
    assert.strictEqual(
      turnStarts(harness.commands)[1]?.type === "thread.turn.start" &&
        turnStarts(harness.commands)[1]?.message.text,
      assignedWorkWakeMessage({
        current: {
          runId: "run-3",
          positionId: "position-3",
          runRevision: "run:1",
          state: "open",
          agentId: AGENT_ID,
          task: { prompt: "工作 3: do the thing" },
          createdAt: "2026-08-21T00:00:10Z",
        },
        queued: 2,
      }),
    );

    sessions = yield* harness.router.snapshotSessions;
    assert.isFalse(sessions[0]?.pendingWork);
    assert.strictEqual(sessions[0]?.phase, "notifying");
  }),
);

it.live("concurrent replay burst: one key, exactly one session and one aggregate", () =>
  Effect.gen(function* () {
    const harness = yield* makeHarness(makeSettings());
    // The authoritative Work query is slow enough that both wakes overlap —
    // a replay burst delivers several notices back-to-back on one channel.
    harness.setWorkCountDelayMs(30);
    yield* Effect.all([wake(harness.router), wake(harness.router)], {
      concurrency: "unbounded",
    });
    assert.lengthOf(projectCreates(harness.commands), 1);
    assert.lengthOf(threadCreates(harness.commands), 1);
    assert.lengthOf(turnStarts(harness.commands), 1);
  }),
);

it.live("a wake arriving inside the flush window cannot double-dispatch", () =>
  Effect.gen(function* () {
    const harness = yield* makeHarness(makeSettings());
    // Dispatch is slow enough that a second wake lands between the first
    // pass's dispatch and its state mutation.
    harness.setDispatchDelayMs(40);
    yield* Effect.all(
      [wake(harness.router), Effect.sleep(10).pipe(Effect.flatMap(() => wake(harness.router)))],
      { concurrency: "unbounded" },
    );
    assert.lengthOf(projectCreates(harness.commands), 1);
    assert.lengthOf(threadCreates(harness.commands), 1);
    assert.lengthOf(turnStarts(harness.commands), 1);
  }),
);

it.effect("deferred delivery failure keeps pending work and redelivers on the next event", () =>
  Effect.gen(function* () {
    const harness = yield* makeHarness(makeSettings());
    yield* wake(harness.router);
    const [created] = threadCreates(harness.commands);
    const threadId = created?.type === "thread.create" ? created.threadId : null;
    const projectId = String(created?.projectId);
    assert.isNotNull(threadId);

    // Work arrives while the aggregate turn runs; the turn then engages.
    harness.putThread(
      makeThreadShell(threadId as string, projectId, (shell) => ({
        ...shell,
        session: runningSession(threadId as string),
      })),
    );
    harness.setOpenWorkCount(3);
    yield* wake(harness.router);
    yield* harness.router.onThreadEvent(threadId as never);
    assert.isTrue((yield* harness.router.snapshotSessions)[0]?.pendingWork);

    // The turn finishes but the authoritative query fails at exactly that
    // moment: the already-ACKed work must stay pending, never dropped.
    harness.failWorkCount(true);
    harness.putThread(
      makeThreadShell(threadId as string, projectId, (shell) => ({
        ...shell,
        session: readySession(threadId as string),
      })),
    );
    yield* harness.router.onThreadEvent(threadId as never);
    assert.lengthOf(turnStarts(harness.commands), 1);
    assert.isTrue((yield* harness.router.snapshotSessions)[0]?.pendingWork);

    // The next event finds the query recovered and delivers the aggregate.
    harness.failWorkCount(false);
    yield* harness.router.onThreadEvent(threadId as never);
    assert.lengthOf(turnStarts(harness.commands), 2);
    assert.isFalse((yield* harness.router.snapshotSessions)[0]?.pendingWork);
  }),
);

it.effect("queued-turn window: a second notice waits until the first turn engages", () =>
  Effect.gen(function* () {
    const harness = yield* makeHarness(makeSettings());
    yield* wake(harness.router);
    const [created] = threadCreates(harness.commands);
    const threadId = created?.type === "thread.create" ? created.threadId : null;
    const projectId = String(created?.projectId);

    // The aggregate was dispatched but no provider session engaged yet.
    harness.putThread(makeThreadShell(threadId as string, projectId));
    harness.setOpenWorkCount(3);
    yield* wake(harness.router);
    assert.lengthOf(turnStarts(harness.commands), 1);

    // An event lands while still queued — nothing may dispatch.
    yield* harness.router.onThreadEvent(threadId as never);
    assert.lengthOf(turnStarts(harness.commands), 1);

    // A ready-but-resting session is still not a finished turn.
    harness.putThread(
      makeThreadShell(threadId as string, projectId, (shell) => ({
        ...shell,
        session: readySession(threadId as string),
      })),
    );
    yield* harness.router.onThreadEvent(threadId as never);
    assert.lengthOf(turnStarts(harness.commands), 1);

    // The turn engages, then finishes: the recorded work delivers once.
    harness.putThread(
      makeThreadShell(threadId as string, projectId, (shell) => ({
        ...shell,
        session: runningSession(threadId as string),
      })),
    );
    yield* harness.router.onThreadEvent(threadId as never);
    harness.putThread(
      makeThreadShell(threadId as string, projectId, (shell) => ({
        ...shell,
        session: readySession(threadId as string),
      })),
    );
    yield* harness.router.onThreadEvent(threadId as never);
    assert.lengthOf(turnStarts(harness.commands), 2);
  }),
);

it.effect("archived session: no longer current, a new session is created", () =>
  Effect.gen(function* () {
    const harness = yield* makeHarness(makeSettings());
    yield* wake(harness.router);
    const [first] = threadCreates(harness.commands);
    const firstThreadId = first?.type === "thread.create" ? first.threadId : null;
    const projectId = String(first?.projectId);

    harness.putThread(
      makeThreadShell(firstThreadId as string, projectId, (shell) => ({
        ...shell,
        archivedAt: ISO,
      })),
    );
    yield* wake(harness.router);

    assert.lengthOf(threadCreates(harness.commands), 2);
    const second = threadCreates(harness.commands)[1];
    const secondThreadId = second?.type === "thread.create" ? second.threadId : null;
    assert.notStrictEqual(secondThreadId, firstThreadId);
    // The new session stays under the SAME resolved project.
    assert.strictEqual(second?.type === "thread.create" && String(second.projectId), projectId);
    // The new session received its aggregate notification.
    assert.lengthOf(turnStarts(harness.commands), 2);
    assert.strictEqual(
      turnStarts(harness.commands)[1]?.type === "thread.turn.start" &&
        turnStarts(harness.commands)[1]?.threadId,
      secondThreadId,
    );

    // A deleted session reads as absent and is treated the same way.
    harness.removeThread(secondThreadId as string);
    yield* wake(harness.router);
    assert.lengthOf(threadCreates(harness.commands), 3);
  }),
);

it.effect("zero authoritative work: no session is created and nothing is delivered", () =>
  Effect.gen(function* () {
    const harness = yield* makeHarness(makeSettings());
    harness.setOpenWorkCount(0);

    yield* wake(harness.router);

    // Routing resolved the directory first, so the local project exists for
    // the next real notice — but no session and no notification came of it.
    assert.lengthOf(projectCreates(harness.commands), 1);
    assert.isEmpty(threadCreates(harness.commands));
    assert.isEmpty(turnStarts(harness.commands));
    assert.isEmpty(yield* harness.router.snapshotSessions);
  }),
);

it.effect("authoritative count is re-queried at delivery time", () =>
  Effect.gen(function* () {
    const harness = yield* makeHarness(makeSettings());
    yield* wake(harness.router);
    const [created] = threadCreates(harness.commands);
    const threadId = created?.type === "thread.create" ? created.threadId : null;
    const projectId = String(created?.projectId);

    // Work resolved between notice and delivery: the deferred aggregate is
    // silent about zero items instead of waking the agent for nothing.
    harness.putThread(
      makeThreadShell(threadId as string, projectId, (shell) => ({
        ...shell,
        session: runningSession(threadId as string),
      })),
    );
    harness.setOpenWorkCount(3);
    yield* wake(harness.router);
    yield* harness.router.onThreadEvent(threadId as never); // engaged: running
    harness.setOpenWorkCount(0);
    harness.putThread(
      makeThreadShell(threadId as string, projectId, (shell) => ({
        ...shell,
        session: readySession(threadId as string),
      })),
    );
    yield* harness.router.onThreadEvent(threadId as never);

    assert.lengthOf(turnStarts(harness.commands), 1);
    const sessions = yield* harness.router.snapshotSessions;
    assert.strictEqual(sessions[0]?.phase, "idle");
    assert.isFalse(sessions[0]?.pendingWork);
  }),
);

it.effect("renames are display-only: routing keys do not change", () =>
  Effect.gen(function* () {
    const harness = yield* makeHarness(makeSettings());
    yield* wake(harness.router);
    const [created] = threadCreates(harness.commands);
    const threadId = created?.type === "thread.create" ? created.threadId : null;
    const projectId = String(created?.projectId);

    // Rename the agent: same ids must route to the same session.
    harness.setSettings(
      makeSettings((base) => ({
        ...base,
        logicalAgents: {
          ...base.logicalAgents,
          [LogicalAgentId.make(AGENT_ID)]: {
            ...base.logicalAgents[LogicalAgentId.make(AGENT_ID) as never],
            agentName: "Renamed Agent",
          },
        } as ServerSettings["logicalAgents"],
      })),
    );
    harness.putThread(
      makeThreadShell(threadId as string, projectId, (shell) => ({
        ...shell,
        session: runningSession(threadId as string),
      })),
    );
    yield* harness.router.onThreadEvent(threadId as never);
    harness.putThread(
      makeThreadShell(threadId as string, projectId, (shell) => ({
        ...shell,
        session: readySession(threadId as string),
      })),
    );
    yield* harness.router.onThreadEvent(threadId as never);

    yield* wake(harness.router);

    // No new session was created; the aggregate landed in the same thread.
    assert.lengthOf(threadCreates(harness.commands), 1);
    assert.strictEqual(
      turnStarts(harness.commands)[1]?.type === "thread.turn.start" &&
        turnStarts(harness.commands)[1]?.threadId,
      threadId,
    );
  }),
);

it.effect("routing failures map onto the wire failure codes", () =>
  Effect.gen(function* () {
    const harness = yield* makeHarness(makeSettings());

    // Unknown agent.
    const unknown = yield* harness.router
      .routeWake(wakeInput({ agentId: "ag_missing" }))
      .pipe(Effect.flip);
    assert.strictEqual(unknown.code, "AGENT_NOT_FOUND");

    // Pre-V4 notice: agent cannot act without a workspace directory.
    const preV4 = yield* harness.router
      .routeWake(wakeInput({ workspaceDir: undefined }))
      .pipe(Effect.flip);
    assert.strictEqual(preV4.code, "AGENT_NOT_DISPATCHABLE");
    assert.match(preV4.detail, /protocol V4/);

    // Agent not dispatchable: provider instance is gone.
    harness.removeProvider();
    const noProvider = yield* wake(harness.router).pipe(Effect.flip);
    assert.strictEqual(noProvider.code, "AGENT_NOT_DISPATCHABLE");
  }),
);

it.effect("authoritative query failure fails the wake so the service can redeliver", () =>
  Effect.gen(function* () {
    const harness = yield* makeHarness(makeSettings());
    harness.failWorkCount(true);

    const failure = yield* wake(harness.router).pipe(Effect.flip);
    assert.strictEqual(failure.code, "CONSUMER_INTERNAL");
    // The workspace project resolved, but nothing routable came of the wake.
    assert.lengthOf(projectCreates(harness.commands), 1);
    assert.isEmpty(threadCreates(harness.commands));
    assert.isEmpty(turnStarts(harness.commands));

    // A failed wake stays recoverable: the redelivered notice routes.
    harness.failWorkCount(false);
    yield* wake(harness.router);
    assert.lengthOf(projectCreates(harness.commands), 1);
    assert.lengthOf(threadCreates(harness.commands), 1);
  }),
);

it.effect("dispatch failure fails the wake and leaves the created session recoverable", () =>
  Effect.gen(function* () {
    const harness = yield* makeHarness(makeSettings());
    yield* wake(harness.router);
    const [created] = threadCreates(harness.commands);
    const threadId = created?.type === "thread.create" ? created.threadId : null;
    const projectId = String(created?.projectId);
    harness.putThread(
      makeThreadShell(threadId as string, projectId, (shell) => ({
        ...shell,
        session: runningSession(threadId as string),
      })),
    );
    yield* harness.router.onThreadEvent(threadId as never);
    harness.putThread(
      makeThreadShell(threadId as string, projectId, (shell) => ({
        ...shell,
        session: readySession(threadId as string),
      })),
    );
    yield* harness.router.onThreadEvent(threadId as never);
    // The unchanged head drew the one A3 reminder; let its turn settle too so
    // the failure below meets an idle session (a notifying one would only
    // coalesce the wake instead of surfacing the dispatch failure).
    yield* runAggregateTurn(harness, threadId as string, projectId);

    harness.failDispatch(true);
    harness.setOpenWorkCount(3);
    const failure = yield* wake(harness.router).pipe(Effect.flip);
    assert.strictEqual(failure.code, "CONSUMER_INTERNAL");

    // The session survives the failure; the next notice delivers on it.
    harness.failDispatch(false);
    yield* wake(harness.router);
    assert.lengthOf(threadCreates(harness.commands), 1);
    assert.lengthOf(turnStarts(harness.commands), 3);
  }),
);

it.effect("restart: a fresh router delivers exactly one new aggregate for a replayed notice", () =>
  Effect.gen(function* () {
    // Process A routed a notice before the restart.
    const first = yield* makeHarness(makeSettings());
    yield* wake(first.router);
    const [created] = threadCreates(first.commands);
    const oldThreadId = created?.type === "thread.create" ? created.threadId : null;
    const projectId = String(created?.projectId);
    assert.lengthOf(turnStarts(first.commands), 1);

    // Process B (fresh in-memory state) receives the still-pending replay.
    // The pre-restart thread stays idle and untouched; the replay creates
    // the next current session exactly once and delivers ONE aggregate.
    const second = yield* makeHarness(makeSettings());
    second.putProject(projectId, WORKSPACE_DIR);
    second.putThread(
      makeThreadShell(oldThreadId as string, projectId, (shell) => ({
        ...shell,
        session: readySession(oldThreadId as string),
      })),
    );
    yield* wake(second.router);
    yield* wake(second.router).pipe(Effect.flip, Effect.ignore);

    assert.lengthOf(threadCreates(second.commands), 1);
    assert.lengthOf(turnStarts(second.commands), 1);
  }),
);

it.effect("dying session without a turn: a queued notification is released", () =>
  Effect.gen(function* () {
    const harness = yield* makeHarness(makeSettings());
    yield* wake(harness.router);
    const [created] = threadCreates(harness.commands);
    const threadId = created?.type === "thread.create" ? created.threadId : null;
    const projectId = String(created?.projectId);

    // The aggregate dispatched, the session died before engaging, and more
    // work arrived: the lapsed notification must not block forever.
    harness.putThread(
      makeThreadShell(threadId as string, projectId, (shell) => ({
        ...shell,
        session: stoppedSession(threadId as string),
      })),
    );
    harness.setOpenWorkCount(3);
    yield* wake(harness.router);
    yield* harness.router.onThreadEvent(threadId as never);
    assert.lengthOf(turnStarts(harness.commands), 2);
  }),
);

it.effect("a deferred event after the resolved project vanished never re-creates it", () =>
  Effect.gen(function* () {
    const harness = yield* makeHarness(makeSettings());
    yield* wake(harness.router);
    const [created] = threadCreates(harness.commands);
    const threadId = created?.type === "thread.create" ? created.threadId : null;
    const projectId = String(created?.projectId);

    // More work arrives while busy, and the aggregate's turn engages.
    harness.putThread(
      makeThreadShell(threadId as string, projectId, (shell) => ({
        ...shell,
        session: runningSession(threadId as string),
      })),
    );
    harness.setOpenWorkCount(3);
    yield* wake(harness.router);
    yield* harness.router.onThreadEvent(threadId as never);

    // The resolved project is then deleted out from under the session
    // before the deferred flush after the turn finishes.
    harness.removeProject(projectId);
    harness.putThread(
      makeThreadShell(threadId as string, projectId, (shell) => ({
        ...shell,
        session: readySession(threadId as string),
      })),
    );
    yield* harness.router.onThreadEvent(threadId as never);

    // The deferred path is reuse-only: no project.create, no aggregate, and
    // the session simply stops being driven.
    assert.lengthOf(projectCreates(harness.commands), 1);
    assert.lengthOf(turnStarts(harness.commands), 1);
    const sessions = yield* harness.router.snapshotSessions;
    assert.strictEqual(sessions[0]?.phase, "idle");
    assert.isTrue(sessions[0]?.pendingWork);
  }),
);

// ── Step ②: execution-workspace binding (session cwd follows the queue head) ──

const WT_A = "/tmp/registry/.project/worktrees/ew_aaa";
const WT_B = "/tmp/registry/.project/worktrees/ew_bbb";

/** A run carrying execution-workspace facts, oldest-first by construction. */
const workspaceRun = (overrides: Partial<AssignedWorkQueueEntry>): AssignedWorkQueueEntry => ({
  runId: "run_wt",
  positionId: "position_wt",
  runRevision: "run:1",
  state: "open",
  agentId: AGENT_ID,
  task: { prompt: "do the thing" },
  createdAt: "2026-08-21T00:00:10.000Z",
  ...overrides,
});

const threadMetaUpdates = (commands: OrchestrationCommand[]) =>
  commands.filter((command) => command.type === "thread.meta.update");

/** Engage the aggregate turn (running) and then finish it (ready). */
const runAggregateTurn = (
  harness: Harness,
  threadId: string,
  projectId: string,
): Effect.Effect<void> =>
  Effect.gen(function* () {
    harness.putThread(
      makeThreadShell(threadId, projectId, (shell) => ({
        ...shell,
        session: runningSession(threadId),
      })),
    );
    yield* harness.router.onThreadEvent(threadId as never);
    harness.putThread(
      makeThreadShell(threadId, projectId, (shell) => ({
        ...shell,
        session: readySession(threadId),
      })),
    );
    yield* harness.router.onThreadEvent(threadId as never);
  });

describe("execution-workspace binding", () => {
  it("worktreeBindingFor binds only managed-worktree paths", () => {
    assert.strictEqual(
      worktreeBindingFor(
        workspaceRun({ workspacePolicy: "managed-worktree", workspacePath: WT_A }),
      ),
      WT_A,
    );
    // Unknown is never project-root: absent facts, non-managed policies, and
    // blank paths all stay unbound.
    assert.strictEqual(worktreeBindingFor(workspaceRun({})), null);
    assert.strictEqual(
      worktreeBindingFor(
        workspaceRun({ workspacePolicy: "project-root", workspacePath: "/tmp/registry" }),
      ),
      null,
    );
    assert.strictEqual(
      worktreeBindingFor(
        workspaceRun({ workspacePolicy: "managed-worktree", workspacePath: "   " }),
      ),
      null,
    );
    assert.strictEqual(worktreeBindingFor(null), null);
  });

  it.effect("a created session binds the queue HEAD's managed worktree", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness(makeSettings());
      harness.setOpenRuns([
        workspaceRun({
          runId: "run_a",
          createdAt: "2026-08-21T00:00:10.000Z",
          workspacePolicy: "managed-worktree",
          workspacePath: WT_A,
        }),
        workspaceRun({
          runId: "run_b",
          createdAt: "2026-08-21T00:00:11.000Z",
          workspacePolicy: "managed-worktree",
          workspacePath: WT_B,
        }),
      ]);
      yield* wake(harness.router);
      const created = threadCreates(harness.commands)[0];
      assert.strictEqual(created?.type === "thread.create" && created.worktreePath, WT_A);
      const sessions = yield* harness.router.snapshotSessions;
      assert.strictEqual(sessions[0]?.boundWorktreePath, WT_A);
    }),
  );

  it.effect("runs without workspace facts stay unbound (project root)", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness(makeSettings());
      yield* wake(harness.router);
      const created = threadCreates(harness.commands)[0];
      assert.strictEqual(created?.type === "thread.create" && created.worktreePath, null);
      const sessions = yield* harness.router.snapshotSessions;
      assert.strictEqual(sessions[0]?.boundWorktreePath, null);
    }),
  );

  it.effect("a head moving to another worktree REBINDS the session in place", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness(makeSettings());
      harness.setOpenRuns([
        workspaceRun({
          runId: "run_a",
          workspacePolicy: "managed-worktree",
          workspacePath: WT_A,
        }),
      ]);
      yield* wake(harness.router);
      const created = threadCreates(harness.commands)[0];
      const threadId = created?.type === "thread.create" ? created.threadId : undefined;
      const projectId = created?.type === "thread.create" ? String(created.projectId) : "";
      assert.isDefined(threadId);
      // Two full cycles: the first turn end draws the unchanged-head reminder
      // (run_a still open), the reminder's turn settles it back to idle.
      yield* runAggregateTurn(harness, threadId as string, projectId);
      yield* runAggregateTurn(harness, threadId as string, projectId);

      // The next work runs in a different managed worktree: a subsequent
      // wake rebinds the SAME thread (thread.meta.update), never a second
      // session.
      harness.setOpenRuns([
        workspaceRun({
          runId: "run_c",
          workspacePolicy: "managed-worktree",
          workspacePath: WT_B,
        }),
      ]);
      yield* wake(harness.router);

      assert.lengthOf(threadCreates(harness.commands), 1);
      const rebinding = threadMetaUpdates(harness.commands);
      assert.lengthOf(rebinding, 1);
      assert.strictEqual(
        rebinding[0]?.type === "thread.meta.update" && rebinding[0].worktreePath,
        WT_B,
      );
      assert.strictEqual(
        rebinding[0]?.type === "thread.meta.update" && String(rebinding[0].threadId),
        threadId,
      );
      const sessions = yield* harness.router.snapshotSessions;
      assert.strictEqual(sessions[0]?.boundWorktreePath, WT_B);
      assert.lengthOf(turnStarts(harness.commands), 3);
    }),
  );

  it.effect("the same worktree does not rebind", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness(makeSettings());
      harness.setOpenRuns([
        workspaceRun({
          runId: "run_a",
          workspacePolicy: "managed-worktree",
          workspacePath: WT_A,
        }),
        workspaceRun({
          runId: "run_b",
          createdAt: "2026-08-21T00:00:11.000Z",
          workspacePolicy: "managed-worktree",
          workspacePath: WT_A,
        }),
      ]);
      yield* wake(harness.router);
      const created = threadCreates(harness.commands)[0];
      const threadId = created?.type === "thread.create" ? created.threadId : undefined;
      const projectId = created?.type === "thread.create" ? String(created.projectId) : "";
      yield* runAggregateTurn(harness, threadId as string, projectId);

      // Head advanced within the same instance's worktree: the session stays
      // bound where it is.
      harness.setOpenRuns([
        workspaceRun({
          runId: "run_b",
          workspacePolicy: "managed-worktree",
          workspacePath: WT_A,
        }),
      ]);
      yield* wake(harness.router);

      assert.isEmpty(threadMetaUpdates(harness.commands));
      assert.lengthOf(threadCreates(harness.commands), 1);
    }),
  );

  it.effect("turn end ADVANCES the queue when the head moved on (drain)", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness(makeSettings());
      harness.setOpenRuns([
        workspaceRun({
          runId: "run_a",
          task: { prompt: "第一件工作" },
          workspacePolicy: "managed-worktree",
          workspacePath: WT_A,
        }),
      ]);
      yield* wake(harness.router);
      const created = threadCreates(harness.commands)[0];
      const threadId = created?.type === "thread.create" ? created.threadId : undefined;
      const projectId = created?.type === "thread.create" ? String(created.projectId) : "";

      // The agent submitted run_a; the head rotated to another instance's
      // worktree. No new notice arrives (notices are per-run), so the drain
      // must deliver the next head itself — and rebind to its worktree.
      harness.setOpenRuns([
        workspaceRun({
          runId: "run_z",
          task: { prompt: "第二件工作" },
          workspacePolicy: "managed-worktree",
          workspacePath: WT_B,
        }),
      ]);
      yield* runAggregateTurn(harness, threadId as string, projectId);

      assert.lengthOf(turnStarts(harness.commands), 2);
      const secondMessage =
        turnStarts(harness.commands)[1]?.type === "thread.turn.start"
          ? turnStarts(harness.commands)[1]?.message.text
          : null;
      assert.include(secondMessage, "第二件工作");
      const rebinding = threadMetaUpdates(harness.commands);
      assert.lengthOf(rebinding, 1);
      assert.strictEqual(
        rebinding[0]?.type === "thread.meta.update" && rebinding[0].worktreePath,
        WT_B,
      );
      const sessions = yield* harness.router.snapshotSessions;
      assert.strictEqual(sessions[0]?.phase, "notifying");
    }),
  );

  it.effect("turn end with an UNCHANGED head reminds exactly once (A3)", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness(makeSettings());
      harness.setOpenRuns([
        workspaceRun({
          runId: "run_a",
          workspacePolicy: "managed-worktree",
          workspacePath: WT_A,
        }),
      ]);
      yield* wake(harness.router);
      const created = threadCreates(harness.commands)[0];
      const threadId = created?.type === "thread.create" ? created.threadId : undefined;
      const projectId = created?.type === "thread.create" ? String(created.projectId) : "";

      // The agent finished its turn without submitting: the head is still the
      // one it was told about. Exactly ONE reminder wake fires — the run is
      // still open and still the head, so the agent's turn provably produced
      // nothing.
      yield* runAggregateTurn(harness, threadId as string, projectId);
      assert.lengthOf(turnStarts(harness.commands), 2);
      const reminder =
        turnStarts(harness.commands)[1]?.type === "thread.turn.start"
          ? turnStarts(harness.commands)[1]?.message.text
          : null;
      assert.strictEqual(
        reminder,
        assignedWorkWakeMessage({
          current: workspaceRun({
            runId: "run_a",
            workspacePolicy: "managed-worktree",
            workspacePath: WT_A,
          }),
          queued: 0,
        }),
      );
      const afterReminder = yield* harness.router.snapshotSessions;
      assert.strictEqual(afterReminder[0]?.phase, "notifying");

      // The reminder turn also ends without a submit: the SAME run is never
      // reminded twice — the session rests instead of nagging.
      yield* runAggregateTurn(harness, threadId as string, projectId);
      assert.lengthOf(turnStarts(harness.commands), 2);
      assert.isEmpty(threadMetaUpdates(harness.commands));
      const sessions = yield* harness.router.snapshotSessions;
      assert.strictEqual(sessions[0]?.phase, "idle");

      // A head CHANGE re-arms the reminder budget: the new run gets its own
      // single reminder when its turn also ends unsubmitted.
      harness.setOpenRuns([
        workspaceRun({
          runId: "run_b",
          workspacePolicy: "managed-worktree",
          workspacePath: WT_A,
        }),
      ]);
      yield* wake(harness.router);
      yield* runAggregateTurn(harness, threadId as string, projectId);
      yield* runAggregateTurn(harness, threadId as string, projectId);
      assert.lengthOf(turnStarts(harness.commands), 4);
    }),
  );
});

// ── Flow liveness: the reconcile sweep (A1) and its no-nag invariant ──

describe("delivery reconcile sweep", () => {
  it.effect("restart: open work no session covers gets exactly one synthetic wake", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness(makeSettings());

      // Fresh registry, as after a restart: the notice was ACKed upstream and
      // will never re-fire, yet open work exists.
      yield* harness.router.reconcileOpenWork(wakeInput());

      assert.lengthOf(threadCreates(harness.commands), 1);
      assert.lengthOf(turnStarts(harness.commands), 1);
      assert.strictEqual((yield* harness.router.snapshotSessions)[0]?.phase, "notifying");

      // No-nag: the delivered head is never re-delivered by later sweeps.
      yield* harness.router.reconcileOpenWork(wakeInput());
      yield* harness.router.reconcileOpenWork(wakeInput());
      assert.lengthOf(turnStarts(harness.commands), 1);
    }),
  );

  it.effect("no open work: the sweep stays silent and creates nothing", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness(makeSettings());
      harness.setOpenWorkCount(0);

      yield* harness.router.reconcileOpenWork(wakeInput());

      assert.isEmpty(harness.commands);
      assert.isEmpty(yield* harness.router.snapshotSessions);
    }),
  );

  it.effect("busy-coalesced work whose turn-end events were lost is flushed by the sweep", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness(makeSettings());
      yield* wake(harness.router);
      const [created] = threadCreates(harness.commands);
      const threadId = created?.type === "thread.create" ? created.threadId : null;
      const projectId = String(created?.projectId);
      assert.lengthOf(turnStarts(harness.commands), 1);

      // The aggregate turn engaged, a further notice coalesced against it,
      // and the thread then settled — but the turn-end events never reached
      // the router (the busy window was coalesced past). Only the shell's
      // settled-after-delivery latestTurn and the sweep can release the
      // pending work: the notice is already ACKed.
      harness.putThread(
        makeThreadShell(threadId as string, projectId, (shell) => ({
          ...shell,
          session: runningSession(threadId as string),
        })),
      );
      harness.setOpenWorkCount(3);
      yield* wake(harness.router);
      assert.isTrue((yield* harness.router.snapshotSessions)[0]?.pendingWork);
      harness.putThread(
        makeThreadShell(threadId as string, projectId, (shell) => ({
          ...shell,
          session: readySession(threadId as string),
          latestTurn: settledTurn(),
        })),
      );

      yield* harness.router.reconcileOpenWork(wakeInput());

      // The missed drain performed: exactly one coalesced aggregate.
      assert.lengthOf(turnStarts(harness.commands), 2);
      const sessions = yield* harness.router.snapshotSessions;
      assert.isFalse(sessions[0]?.pendingWork);
      assert.strictEqual(sessions[0]?.phase, "notifying");
    }),
  );

  it.effect("a settled turn whose drain was lost gets its reminder from the sweep", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness(makeSettings());
      yield* wake(harness.router);
      const [created] = threadCreates(harness.commands);
      const threadId = created?.type === "thread.create" ? created.threadId : null;
      const projectId = String(created?.projectId);
      assert.lengthOf(turnStarts(harness.commands), 1);

      // The turn engaged and settled with the head unchanged — and not one
      // event reached the router. The shell's settled-after-delivery turn is
      // the only evidence; the sweep performs the drain, which is the one
      // A3 reminder.
      harness.putThread(
        makeThreadShell(threadId as string, projectId, (shell) => ({
          ...shell,
          session: readySession(threadId as string),
          latestTurn: settledTurn(),
        })),
      );
      yield* harness.router.reconcileOpenWork(wakeInput());
      assert.lengthOf(turnStarts(harness.commands), 2);

      // The reminder's own turn has not engaged yet: a further sweep re-runs
      // the drain, finds the reminder already spent, and rests the session
      // instead of nagging.
      yield* harness.router.reconcileOpenWork(wakeInput());
      assert.lengthOf(turnStarts(harness.commands), 2);
      assert.strictEqual((yield* harness.router.snapshotSessions)[0]?.phase, "idle");
    }),
  );

  it.effect("a head that advanced past a lost drain is delivered by the sweep", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness(makeSettings());
      harness.setOpenRuns([workspaceRun({ runId: "run_a", task: { prompt: "第一件工作" } })]);
      yield* wake(harness.router);
      const [created] = threadCreates(harness.commands);
      const threadId = created?.type === "thread.create" ? created.threadId : null;
      const projectId = String(created?.projectId);

      // The aggregate turn ends (drain draws the reminder), and the reminder
      // turn settles with the head ROTATED — but its drain event is lost.
      yield* runAggregateTurn(harness, threadId as string, projectId);
      harness.setOpenRuns([workspaceRun({ runId: "run_z", task: { prompt: "第二件工作" } })]);
      harness.putThread(
        makeThreadShell(threadId as string, projectId, (shell) => ({
          ...shell,
          session: readySession(threadId as string),
          latestTurn: settledTurn(),
        })),
      );

      yield* harness.router.reconcileOpenWork(wakeInput());

      assert.lengthOf(turnStarts(harness.commands), 3);
      const delivered =
        turnStarts(harness.commands)[2]?.type === "thread.turn.start"
          ? turnStarts(harness.commands)[2]?.message.text
          : null;
      assert.include(delivered, "第二件工作");
    }),
  );

  it.effect("an archived session thread is replaced by the sweep when work is open", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness(makeSettings());
      yield* wake(harness.router);
      const [created] = threadCreates(harness.commands);
      const threadId = created?.type === "thread.create" ? created.threadId : null;
      const projectId = String(created?.projectId);

      // The session's thread was archived with open work still pending: the
      // sweep routes a fresh session exactly like a wake would.
      harness.putThread(
        makeThreadShell(threadId as string, projectId, (shell) => ({
          ...shell,
          archivedAt: ISO,
        })),
      );
      yield* harness.router.reconcileOpenWork(wakeInput());

      assert.lengthOf(threadCreates(harness.commands), 2);
      assert.lengthOf(turnStarts(harness.commands), 2);
      assert.notStrictEqual(
        threadCreates(harness.commands)[1]?.type === "thread.create"
          ? threadCreates(harness.commands)[1]?.threadId
          : null,
        threadId,
      );
    }),
  );
});

describe("flow-instance session scope", () => {
  const INST_A = "fi_alpha";
  const INST_B = "fi_beta";

  /** Project-work settings with the agent scoped to one session per instance. */
  const makeFlowSettings = () =>
    makeSettings((base) => ({
      ...base,
      logicalAgents: {
        ...base.logicalAgents,
        [LogicalAgentId.make(AGENT_ID)]: {
          agentName: "Primary Agent",
          providerInstanceId: ProviderInstanceId.make(PROVIDER_INSTANCE),
          persona: "",
          thinkLevel: null,
          modelOverride: null,
          project: {
            enabled: true,
            sessionScope: "flow-instance" as const,
            sessionRetention: "settle",
          },
        },
      },
    }));

  /** A run owned by one flow instance (PS v2+ task snapshots carry instance). */
  const flowRun = (
    instanceId: string,
    name: string,
    overrides: Partial<AssignedWorkQueueEntry> = {},
  ): AssignedWorkQueueEntry => ({
    runId: `run_${instanceId}`,
    positionId: `position_${instanceId}`,
    runRevision: "run:1",
    state: "open",
    agentId: AGENT_ID,
    task: { prompt: `work for ${name}`, instance: { instanceId, name, iteration: 1 } },
    createdAt: "2026-08-21T00:00:10.000Z",
    ...overrides,
  });

  /** A run without instance identity (the legacy bucket). */
  const legacyRun = (runId: string, createdAt: string): AssignedWorkQueueEntry => ({
    runId,
    positionId: `position_${runId}`,
    runRevision: "run:1",
    state: "open",
    agentId: AGENT_ID,
    task: { prompt: `legacy work ${runId}` },
    createdAt,
  });

  /**
   * Run the aggregate turn to rest: engage + settle twice — the unchanged
   * head draws the one reminder, whose own turn must also settle before the
   * session is idle again.
   */
  const settleToIdle = (
    harness: Harness,
    threadId: string,
    projectId: string,
  ): Effect.Effect<void> =>
    runAggregateTurn(harness, threadId, projectId).pipe(
      Effect.flatMap(() => runAggregateTurn(harness, threadId, projectId)),
    );

  const titleOf = (command: OrchestrationCommand): string =>
    command.type === "thread.create" ? command.title : "";

  it("the instance-scoped title composes agent and instance name, falling back on blank", () => {
    assert.strictEqual(
      flowInstanceWorkSessionThreadTitle("Primary Agent", "Alpha"),
      "Primary Agent · Alpha",
    );
    assert.strictEqual(
      flowInstanceWorkSessionThreadTitle("  Primary Agent  ", null),
      "Primary Agent",
    );
  });

  it.effect("one wake, work across two instances: one session each in the SAME T3 project", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness(makeFlowSettings());
      const runA1 = flowRun(INST_A, "Alpha", {
        runId: "run_a1",
        createdAt: "2026-08-21T00:00:10.000Z",
      });
      const runA2 = flowRun(INST_A, "Alpha", {
        runId: "run_a2",
        createdAt: "2026-08-21T00:00:11.000Z",
      });
      const runB1 = flowRun(INST_B, "Beta", {
        runId: "run_b1",
        createdAt: "2026-08-21T00:00:12.000Z",
      });
      harness.setOpenRuns([runA1, runA2, runB1]);

      yield* wake(harness.router);

      const creates = threadCreates(harness.commands);
      assert.lengthOf(creates, 2);
      assert.lengthOf(projectCreates(harness.commands), 1);
      // Same resolved project; titles carry the instance name so the two
      // parallel sessions stay distinguishable in the UI.
      assert.deepEqual(creates.map(titleOf).sort(), [
        "Primary Agent · Alpha",
        "Primary Agent · Beta",
      ]);
      assert.strictEqual(
        new Set(
          creates.map((command) =>
            command.type === "thread.create" ? String(command.projectId) : "",
          ),
        ).size,
        1,
      );
      // Each aggregate names ITS partition's head and queue depth only.
      assert.deepEqual(
        turnStarts(harness.commands)
          .map((command) => (command.type === "thread.turn.start" ? command.message.text : ""))
          .sort(),
        [
          assignedWorkWakeMessage({ current: runA1, queued: 1 }),
          assignedWorkWakeMessage({ current: runB1, queued: 0 }),
        ].sort(),
      );
      assert.deepEqual(
        (yield* harness.router.snapshotSessions).map((session) => session.flowInstanceKey).sort(),
        [INST_A, INST_B],
      );
    }),
  );

  it.effect("a second wake for the SAME instance reuses its session's thread", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness(makeFlowSettings());
      harness.setOpenRuns([
        flowRun(INST_A, "Alpha", { runId: "run_a1", createdAt: "2026-08-21T00:00:10.000Z" }),
        flowRun(INST_B, "Beta", { runId: "run_b1", createdAt: "2026-08-21T00:00:11.000Z" }),
      ]);
      yield* wake(harness.router);
      const alphaThread = threadCreates(harness.commands).find(
        (command) => titleOf(command) === "Primary Agent · Alpha",
      );
      const threadId = alphaThread?.type === "thread.create" ? alphaThread.threadId : null;
      const projectId = String(alphaThread?.projectId);
      assert.isNotNull(threadId);
      yield* settleToIdle(harness, threadId as string, projectId);

      yield* wake(harness.router);

      // No third session: Alpha's wake resumed on the existing thread. The
      // four turns are the two initial aggregates, Alpha's settle reminder,
      // and the resumed Alpha aggregate (Beta was still notifying, so its
      // wake only recorded).
      assert.lengthOf(threadCreates(harness.commands), 2);
      assert.lengthOf(turnStarts(harness.commands), 4);
      const alphaTurns = turnStarts(harness.commands).filter(
        (command) =>
          command.type === "thread.turn.start" && String(command.threadId) === String(threadId),
      );
      assert.lengthOf(alphaTurns, 3);
    }),
  );

  it.effect("a busy instance records its work; the OTHER instance's session delivers", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness(makeFlowSettings());
      harness.setOpenRuns([
        flowRun(INST_A, "Alpha", { runId: "run_a1", createdAt: "2026-08-21T00:00:10.000Z" }),
        flowRun(INST_B, "Beta", { runId: "run_b1", createdAt: "2026-08-21T00:00:11.000Z" }),
      ]);
      yield* wake(harness.router);
      const byTitle = new Map(
        threadCreates(harness.commands).map((command) => [
          titleOf(command),
          command.type === "thread.create" ? command.threadId : null,
        ]),
      );
      const alphaThread = byTitle.get("Primary Agent · Alpha") as string;
      const betaThread = byTitle.get("Primary Agent · Beta") as string;
      const projectId = String(threadCreates(harness.commands)[0]?.projectId);
      yield* settleToIdle(harness, alphaThread, projectId);
      yield* settleToIdle(harness, betaThread, projectId);

      // Alpha's turn is running again when a fresh notice lands.
      harness.putThread(
        makeThreadShell(alphaThread, projectId, (shell) => ({
          ...shell,
          session: runningSession(alphaThread),
        })),
      );
      yield* wake(harness.router);

      const turns = turnStarts(harness.commands);
      // Two initial aggregates + two settle reminders + Beta's resumed
      // aggregate — and the resumed one lands ONLY on Beta's thread.
      assert.lengthOf(turns, 5);
      assert.strictEqual(
        turns.at(-1)?.type === "thread.turn.start" ? String(turns.at(-1)?.threadId) : null,
        String(betaThread),
      );
      const alphaTurns = turns.filter(
        (command) =>
          command.type === "thread.turn.start" && String(command.threadId) === String(alphaThread),
      );
      assert.lengthOf(alphaTurns, 2);
      const sessions = yield* harness.router.snapshotSessions;
      const alpha = sessions.find((session) => session.flowInstanceKey === INST_A);
      assert.isTrue(alpha?.pendingWork);
    }),
  );

  it.effect(
    "runs without instance identity share one legacy session beside the instance sessions",
    () =>
      Effect.gen(function* () {
        const harness = yield* makeHarness(makeFlowSettings());
        harness.setOpenRuns([
          legacyRun("run_legacy_1", "2026-08-21T00:00:10.000Z"),
          legacyRun("run_legacy_2", "2026-08-21T00:00:11.000Z"),
          flowRun(INST_A, "Alpha", { runId: "run_a1", createdAt: "2026-08-21T00:00:12.000Z" }),
        ]);

        yield* wake(harness.router);

        assert.deepEqual(threadCreates(harness.commands).map(titleOf).sort(), [
          "Primary Agent",
          "Primary Agent · Alpha",
        ]);
        assert.include(
          turnStarts(harness.commands)
            .map((command) => (command.type === "thread.turn.start" ? command.message.text : ""))
            .join("\n"),
          "1 more item waiting behind it",
        );
        assert.deepEqual(
          (yield* harness.router.snapshotSessions).map((session) => session.flowInstanceKey).sort(),
          ["", INST_A],
        );
      }),
  );

  it.effect("a head advancing in ONE instance advances only that instance's session", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness(makeFlowSettings());
      harness.setOpenRuns([
        flowRun(INST_A, "Alpha", { runId: "run_a1", createdAt: "2026-08-21T00:00:10.000Z" }),
        flowRun(INST_A, "Alpha", { runId: "run_a2", createdAt: "2026-08-21T00:00:11.000Z" }),
        flowRun(INST_B, "Beta", { runId: "run_b1", createdAt: "2026-08-21T00:00:12.000Z" }),
      ]);
      yield* wake(harness.router);
      const byTitle = new Map(
        threadCreates(harness.commands).map((command) => [
          titleOf(command),
          command.type === "thread.create" ? command.threadId : null,
        ]),
      );
      const alphaThread = byTitle.get("Primary Agent · Alpha") as string;
      const betaThread = byTitle.get("Primary Agent · Beta") as string;
      const projectId = String(threadCreates(harness.commands)[0]?.projectId);

      // Alpha's aggregate engaged; its head then advanced (run_a1 resolved).
      harness.putThread(
        makeThreadShell(alphaThread, projectId, (shell) => ({
          ...shell,
          session: runningSession(alphaThread),
        })),
      );
      yield* harness.router.onThreadEvent(alphaThread as never);
      harness.setOpenRuns([
        flowRun(INST_A, "Alpha", { runId: "run_a2", createdAt: "2026-08-21T00:00:11.000Z" }),
        flowRun(INST_B, "Beta", { runId: "run_b1", createdAt: "2026-08-21T00:00:12.000Z" }),
      ]);
      harness.putThread(
        makeThreadShell(alphaThread, projectId, (shell) => ({
          ...shell,
          session: readySession(alphaThread),
        })),
      );
      yield* harness.router.onThreadEvent(alphaThread as never);

      // Alpha's thread got exactly one advancement naming run_a2; Beta's
      // thread was never touched by Alpha's drain.
      const alphaTurns = turnStarts(harness.commands).filter(
        (command) =>
          command.type === "thread.turn.start" && String(command.threadId) === String(alphaThread),
      );
      assert.lengthOf(alphaTurns, 2);
      assert.include(
        alphaTurns[1]?.type === "thread.turn.start" ? alphaTurns[1]?.message.text : "",
        "work for Alpha",
      );
      const betaTurns = turnStarts(harness.commands).filter(
        (command) =>
          command.type === "thread.turn.start" && String(command.threadId) === String(betaThread),
      );
      assert.lengthOf(betaTurns, 1);
    }),
  );

  it.effect("each instance's session binds its own head's managed worktree", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness(makeFlowSettings());
      harness.setOpenRuns([
        flowRun(INST_A, "Alpha", {
          runId: "run_a1",
          createdAt: "2026-08-21T00:00:10.000Z",
          workspacePolicy: "managed-worktree",
          workspacePath: WT_A,
        }),
        flowRun(INST_B, "Beta", {
          runId: "run_b1",
          createdAt: "2026-08-21T00:00:11.000Z",
          workspacePolicy: "managed-worktree",
          workspacePath: WT_B,
        }),
      ]);

      yield* wake(harness.router);

      const bindingByTitle = new Map(
        threadCreates(harness.commands).map((command) => [
          titleOf(command),
          command.type === "thread.create" ? command.worktreePath : null,
        ]),
      );
      assert.strictEqual(bindingByTitle.get("Primary Agent · Alpha"), WT_A);
      assert.strictEqual(bindingByTitle.get("Primary Agent · Beta"), WT_B);
      const sessions = yield* harness.router.snapshotSessions;
      assert.strictEqual(
        sessions.find((session) => session.flowInstanceKey === INST_A)?.boundWorktreePath,
        WT_A,
      );
      assert.strictEqual(
        sessions.find((session) => session.flowInstanceKey === INST_B)?.boundWorktreePath,
        WT_B,
      );
    }),
  );

  it.effect("restart: the sweep rebuilds one session per instance, then stays quiet", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness(makeFlowSettings());
      harness.setOpenRuns([
        flowRun(INST_A, "Alpha", { runId: "run_a1", createdAt: "2026-08-21T00:00:10.000Z" }),
        flowRun(INST_B, "Beta", { runId: "run_b1", createdAt: "2026-08-21T00:00:11.000Z" }),
      ]);

      yield* harness.router.reconcileOpenWork(wakeInput());

      assert.lengthOf(threadCreates(harness.commands), 2);
      assert.lengthOf(turnStarts(harness.commands), 2);
      assert.deepEqual(
        (yield* harness.router.snapshotSessions).map((session) => session.flowInstanceKey).sort(),
        [INST_A, INST_B],
      );

      // No-nag per instance: later sweeps re-deliver nothing.
      yield* harness.router.reconcileOpenWork(wakeInput());
      yield* harness.router.reconcileOpenWork(wakeInput());
      assert.lengthOf(turnStarts(harness.commands), 2);
    }),
  );

  it.effect("toggling project→flow-instance PARKS the old shared session instead of chaining", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness(makeSettings());
      harness.setOpenRuns([
        flowRun(INST_A, "Alpha", { runId: "run_a1", createdAt: "2026-08-21T00:00:10.000Z" }),
        flowRun(INST_B, "Beta", { runId: "run_b1", createdAt: "2026-08-21T00:00:11.000Z" }),
      ]);
      yield* wake(harness.router);
      const [created] = threadCreates(harness.commands);
      const threadId = created?.type === "thread.create" ? created.threadId : null;
      const projectId = String(created?.projectId);
      assert.lengthOf(turnStarts(harness.commands), 1);

      // The scope flips while the shared session's aggregate turn is still
      // in flight. Its turn then runs out: without the universe guard the
      // drain would draw the one reminder (same head still open) and keep
      // chaining the whole queue into the OLD thread.
      harness.setSettings(makeFlowSettings());
      yield* runAggregateTurn(harness, threadId as string, projectId);

      assert.lengthOf(turnStarts(harness.commands), 1);
      const parked = (yield* harness.router.snapshotSessions).find(
        (session) => session.flowInstanceKey === null,
      );
      assert.strictEqual(parked?.phase, "idle");
      assert.isFalse(parked?.pendingWork);

      // The next wake routes each instance onto its OWN session; the parked
      // thread keeps exactly its one pre-toggle aggregate and is never
      // spoken to again.
      yield* wake(harness.router);
      assert.lengthOf(threadCreates(harness.commands), 3);
      assert.lengthOf(turnStarts(harness.commands), 3);
      const parkedThreadTurns = turnStarts(harness.commands).filter(
        (command) =>
          command.type === "thread.turn.start" && String(command.threadId) === String(threadId),
      );
      assert.lengthOf(parkedThreadTurns, 1);
    }),
  );

  it.effect("toggling flow-instance→project parks the instance sessions symmetrically", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness(makeFlowSettings());
      harness.setOpenRuns([
        flowRun(INST_A, "Alpha", { runId: "run_a1", createdAt: "2026-08-21T00:00:10.000Z" }),
        flowRun(INST_B, "Beta", { runId: "run_b1", createdAt: "2026-08-21T00:00:11.000Z" }),
      ]);
      yield* wake(harness.router);
      assert.lengthOf(threadCreates(harness.commands), 2);
      const alphaThread = threadCreates(harness.commands).find(
        (command) => titleOf(command) === "Primary Agent · Alpha",
      );
      const threadId = alphaThread?.type === "thread.create" ? alphaThread.threadId : null;
      const projectId = String(alphaThread?.projectId);

      // Flip back to the shared scope mid-flight; Alpha's turn then runs out
      // and its session parks without drawing its reminder.
      harness.setSettings(makeSettings());
      yield* runAggregateTurn(harness, threadId as string, projectId);
      assert.lengthOf(turnStarts(harness.commands), 2);

      // The next wake creates the ONE shared session covering every run.
      yield* wake(harness.router);
      assert.lengthOf(threadCreates(harness.commands), 3);
      const sharedTurn = turnStarts(harness.commands)[2];
      assert.strictEqual(
        sharedTurn?.type === "thread.turn.start" ? sharedTurn.message.text : null,
        assignedWorkWakeMessage({
          current: flowRun(INST_A, "Alpha", {
            runId: "run_a1",
            createdAt: "2026-08-21T00:00:10.000Z",
          }),
          queued: 1,
        }),
      );
    }),
  );

  it.effect("a wake with zero open work resolves the project and creates no session", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness(makeFlowSettings());
      harness.setOpenWorkCount(0);

      yield* wake(harness.router);

      assert.lengthOf(projectCreates(harness.commands), 1);
      assert.isEmpty(threadCreates(harness.commands));
      assert.isEmpty(turnStarts(harness.commands));
      assert.isEmpty(yield* harness.router.snapshotSessions);
    }),
  );

  it.effect("a blank instance name falls back to the agent-name title", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness(makeFlowSettings());
      harness.setOpenRuns([
        flowRun(INST_A, "   ", { runId: "run_a1", createdAt: "2026-08-21T00:00:10.000Z" }),
      ]);

      yield* wake(harness.router);

      assert.deepEqual(threadCreates(harness.commands).map(titleOf), ["Primary Agent"]);
    }),
  );
});

describe("applyThinkLevelToOptions", () => {
  it("maps the driver's effort option id and leaves explicit selections alone", () => {
    assert.deepEqual(applyThinkLevelToOptions(undefined, "high", "claudeAgent"), [
      { id: "effort", value: "high" },
    ]);
    assert.deepEqual(applyThinkLevelToOptions(undefined, "high", "codex"), [
      { id: "reasoningEffort", value: "high" },
    ]);
    // Cursor rides its own reasoning option id.
    assert.deepEqual(applyThinkLevelToOptions(undefined, "high", "cursor"), [
      { id: "reasoning", value: "high" },
    ]);
    // Drivers without a known effort option carry no thinkLevel at all.
    assert.isNull(applyThinkLevelToOptions(undefined, "high", "grok"));
    // Blank thinkLevel = follow the model default.
    assert.isNull(applyThinkLevelToOptions(undefined, null, "codex"));
    assert.isNull(applyThinkLevelToOptions(undefined, "   ", "claudeAgent"));
    // An explicit effort option is the more specific intent and wins.
    assert.deepEqual(
      applyThinkLevelToOptions([{ id: "effort", value: "low" }], "high", "claudeAgent"),
      [{ id: "effort", value: "low" }],
    );
    // Non-effort options ride along untouched.
    assert.deepEqual(
      applyThinkLevelToOptions([{ id: "fastMode", value: true }], "high", "claudeAgent"),
      [
        { id: "fastMode", value: true },
        { id: "effort", value: "high" },
      ],
    );
  });
});

// ── Flow-instance finalization drive (flow-end design) ───────────

describe("flow-instance finalization drive", () => {
  const INST_A = "fi_alpha";
  const INST_B = "fi_beta";
  const WT_A = "/wt/alpha";
  const WT_B = "/wt/beta";

  const settleCommands = (commands: OrchestrationCommand[]) =>
    commands.filter((command) => command.type === "thread.settle");
  const deleteCommands = (commands: OrchestrationCommand[]) =>
    commands.filter((command) => command.type === "thread.delete");
  const metaUpdates = (commands: OrchestrationCommand[]) =>
    commands.filter((command) => command.type === "thread.meta.update");

  const makeScopedSettings = (
    sessionScope: "project" | "flow-instance",
    sessionRetention: "settle" | "delete" = "settle",
  ) =>
    makeSettings((base) => ({
      ...base,
      logicalAgents: {
        ...base.logicalAgents,
        [LogicalAgentId.make(AGENT_ID)]: {
          agentName: "Primary Agent",
          providerInstanceId: ProviderInstanceId.make(PROVIDER_INSTANCE),
          persona: "",
          thinkLevel: null,
          modelOverride: null,
          project: { enabled: true, sessionScope, sessionRetention },
        },
      },
    }));

  const instanceRun = (
    instanceId: string,
    runId: string,
    overrides: Partial<AssignedWorkQueueEntry> = {},
  ): AssignedWorkQueueEntry => ({
    runId,
    positionId: `position_${runId}`,
    runRevision: "run:1",
    state: "open",
    agentId: AGENT_ID,
    task: { prompt: `work ${runId}`, instance: { instanceId, name: instanceId, iteration: 1 } },
    createdAt: "2026-08-21T00:00:10.000Z",
    ...overrides,
  });

  /** A project-scope queue head running in a managed worktree. */
  const worktreeRun = (runId: string, workspacePath: string): AssignedWorkQueueEntry => ({
    runId,
    positionId: `position_${runId}`,
    runRevision: "run:1",
    state: "open",
    agentId: AGENT_ID,
    task: { prompt: `work ${runId}` },
    createdAt: "2026-08-21T00:00:10.000Z",
    workspacePolicy: "managed-worktree",
    workspacePath,
  });

  /** Route one wake under flow-instance scope and return the instance's session thread. */
  const routedInstanceSession = (
    harness: Harness,
    instanceId: string,
  ): { readonly threadId: string; readonly projectId: string } => {
    const create = threadCreates(harness.commands).find(
      (command) => command.type === "thread.create" && command.title.includes(instanceId),
    );
    assert.isDefined(create);
    return create !== undefined && create.type === "thread.create"
      ? { threadId: String(create.threadId), projectId: String(create.projectId) }
      : { threadId: "", projectId: "" };
  };

  it.effect(
    "flow-instance scope: settles the safely-idle session and removes the routing record",
    () =>
      Effect.gen(function* () {
        const harness = yield* makeHarness(makeScopedSettings("flow-instance"));
        harness.setOpenRuns([instanceRun(INST_A, "run_a1")]);
        yield* wake(harness.router);
        const { threadId } = routedInstanceSession(harness, INST_A);
        harness.setOpenRuns([]);

        const outcome = yield* harness.router.finalizeFlowInstance({
          agentId: AGENT_ID,
          projectId: PS_PROJECT_ID,
          instanceKey: INST_A,
          threadId,
        });

        assert.deepEqual(outcome, { kind: "completed" });
        assert.lengthOf(settleCommands(harness.commands), 1);
        const settle = settleCommands(harness.commands)[0];
        assert.ok(settle !== undefined && settle.type === "thread.settle");
        if (settle.type === "thread.settle") {
          assert.strictEqual(String(settle.threadId), threadId);
        }
        assert.isUndefined(deleteCommands(harness.commands)[0]);
        // The instance's Project Work routing record is gone.
        assert.deepEqual(
          (yield* harness.router.snapshotSessions).map((session) => session.flowInstanceKey),
          [],
        );
      }),
  );

  it.effect("flow-instance scope: a running session, pending input, or queued turn waits", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness(makeScopedSettings("flow-instance"));
      harness.setOpenRuns([instanceRun(INST_A, "run_a1")]);
      yield* wake(harness.router);
      const { threadId, projectId } = routedInstanceSession(harness, INST_A);
      harness.setOpenRuns([]);

      // Running session: never settled into.
      harness.putThread(
        makeThreadShell(threadId, projectId, (shell) => ({
          ...shell,
          session: runningSession(threadId),
        })),
      );
      assert.deepEqual(
        yield* harness.router.finalizeFlowInstance({
          agentId: AGENT_ID,
          projectId: PS_PROJECT_ID,
          instanceKey: INST_A,
          threadId,
        }),
        { kind: "waiting", reason: "session-busy" },
      );
      assert.lengthOf(settleCommands(harness.commands), 0);

      // Blocked-on-you approval: waiting, not settled.
      harness.putThread(
        makeThreadShell(threadId, projectId, (shell) => ({
          ...shell,
          session: readySession(threadId),
          hasPendingApprovals: true,
        })),
      );
      assert.deepEqual(
        yield* harness.router.finalizeFlowInstance({
          agentId: AGENT_ID,
          projectId: PS_PROJECT_ID,
          instanceKey: INST_A,
          threadId,
        }),
        { kind: "waiting", reason: "session-busy" },
      );

      // Pending user input: waiting, not settled.
      harness.putThread(
        makeThreadShell(threadId, projectId, (shell) => ({
          ...shell,
          hasPendingUserInput: true,
        })),
      );
      assert.deepEqual(
        yield* harness.router.finalizeFlowInstance({
          agentId: AGENT_ID,
          projectId: PS_PROJECT_ID,
          instanceKey: INST_A,
          threadId,
        }),
        { kind: "waiting", reason: "session-busy" },
      );
      assert.lengthOf(settleCommands(harness.commands), 0);
    }),
  );

  it.effect("flow-instance scope: new open work for the instance keeps the session alive", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness(makeScopedSettings("flow-instance"));
      harness.setOpenRuns([instanceRun(INST_A, "run_a1")]);
      yield* wake(harness.router);
      const { threadId } = routedInstanceSession(harness, INST_A);
      // The terminal observation raced a new open work notice for the same
      // instance: the session keeps serving it.
      harness.setOpenRuns([instanceRun(INST_A, "run_a2")]);

      assert.deepEqual(
        yield* harness.router.finalizeFlowInstance({
          agentId: AGENT_ID,
          projectId: PS_PROJECT_ID,
          instanceKey: INST_A,
          threadId,
        }),
        { kind: "waiting", reason: "open-work" },
      );
      assert.lengthOf(settleCommands(harness.commands), 0);
    }),
  );

  it.effect("delete retention: the settle lands first, the delete rides behind it", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness(makeScopedSettings("flow-instance", "delete"));
      harness.setOpenRuns([instanceRun(INST_A, "run_a1")]);
      yield* wake(harness.router);
      const { threadId } = routedInstanceSession(harness, INST_A);
      harness.setOpenRuns([]);

      const outcome = yield* harness.router.finalizeFlowInstance({
        agentId: AGENT_ID,
        projectId: PS_PROJECT_ID,
        instanceKey: INST_A,
        threadId,
      });

      assert.deepEqual(outcome, { kind: "completed" });
      // The settle (whose decider guards prove safe idle) strictly precedes
      // the delete — a running or human-blocked session is never deleted.
      const settleIndex = harness.commands.findIndex((command) => command.type === "thread.settle");
      const deleteIndex = harness.commands.findIndex((command) => command.type === "thread.delete");
      assert.isAtLeast(settleIndex, 0);
      assert.strictEqual(deleteIndex, settleIndex + 1);
      assert.deepEqual(
        (yield* harness.router.snapshotSessions).map((session) => session.flowInstanceKey),
        [],
      );
    }),
  );

  it.effect("a gone or archived session completes and drops the routing record", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness(makeScopedSettings("flow-instance"));
      harness.setOpenRuns([instanceRun(INST_A, "run_a1")]);
      yield* wake(harness.router);
      const { threadId, projectId } = routedInstanceSession(harness, INST_A);
      harness.setOpenRuns([]);

      // Deleted sessions read as absent; archived ones as no longer current.
      harness.putThread(
        makeThreadShell(threadId, projectId, (shell) => ({
          ...shell,
          archivedAt: "2026-08-21T00:00:20.000Z",
        })),
      );
      assert.deepEqual(
        yield* harness.router.finalizeFlowInstance({
          agentId: AGENT_ID,
          projectId: PS_PROJECT_ID,
          instanceKey: INST_A,
          threadId,
        }),
        { kind: "completed" },
      );
      assert.lengthOf(settleCommands(harness.commands), 0);
      assert.deepEqual(
        (yield* harness.router.snapshotSessions).map((session) => session.flowInstanceKey),
        [],
      );

      // And an absent thread (deleted outright) completes the same way.
      harness.removeThread(threadId);
      assert.deepEqual(
        yield* harness.router.finalizeFlowInstance({
          agentId: AGENT_ID,
          projectId: PS_PROJECT_ID,
          instanceKey: INST_A,
          threadId,
        }),
        { kind: "completed" },
      );
    }),
  );

  it.effect("restart: with the registry empty, the recorded thread still settles", () =>
    Effect.gen(function* () {
      // The registry never reconstitutes an ended instance's session; the
      // intake-resolved threadId is the fallback the drive settles.
      const harness = yield* makeHarness(makeScopedSettings("flow-instance"));
      harness.putThread(makeThreadShell("thread_restart_1", "t3_proj_1"));
      assert.deepEqual(
        yield* harness.router.finalizeFlowInstance({
          agentId: AGENT_ID,
          projectId: PS_PROJECT_ID,
          instanceKey: INST_A,
          threadId: "thread_restart_1",
        }),
        { kind: "completed" },
      );
      assert.lengthOf(settleCommands(harness.commands), 1);
    }),
  );

  it.effect("project scope: no remaining work returns the session to the project root", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness(makeScopedSettings("project"));
      harness.setOpenRuns([worktreeRun("run_1", WT_A)]);
      yield* wake(harness.router);
      const created = threadCreates(harness.commands)[0];
      const threadId = created?.type === "thread.create" ? String(created.threadId) : "";
      const projectId = created?.type === "thread.create" ? String(created.projectId) : "";
      // The projected binding the engine would hold after the session ran in
      // the instance's managed worktree.
      harness.putThread(
        makeThreadShell(threadId, projectId, (shell) => ({ ...shell, worktreePath: WT_A })),
      );
      harness.setOpenRuns([]);

      assert.deepEqual(
        yield* harness.router.finalizeFlowInstance({
          agentId: AGENT_ID,
          projectId: PS_PROJECT_ID,
          instanceKey: INST_A,
          threadId,
        }),
        { kind: "completed" },
      );
      const rebinds = metaUpdates(harness.commands);
      assert.lengthOf(rebinds, 1);
      assert.strictEqual(
        rebinds[0]?.type === "thread.meta.update" ? rebinds[0].worktreePath : undefined,
        null,
      );
      // No settle, no delete: project sessions are long-lived.
      assert.lengthOf(settleCommands(harness.commands), 0);
      assert.lengthOf(deleteCommands(harness.commands), 0);
    }),
  );

  it.effect(
    "project scope: remaining work rebinds to ITS workspace — a late notice never pulls the session back to root",
    () =>
      Effect.gen(function* () {
        const harness = yield* makeHarness(makeScopedSettings("project"));
        harness.setOpenRuns([worktreeRun("run_1", WT_A)]);
        yield* wake(harness.router);
        const created = threadCreates(harness.commands)[0];
        const threadId = created?.type === "thread.create" ? String(created.threadId) : "";
        const projectId = created?.type === "thread.create" ? String(created.projectId) : "";
        // A newer flow's work arrived and the session is already bound to its
        // workspace; the OLD instance's terminal notification arrives late.
        harness.putThread(
          makeThreadShell(threadId, projectId, (shell) => ({ ...shell, worktreePath: WT_B })),
        );
        harness.setOpenRuns([
          instanceRun(INST_B, "run_b1", {
            runId: "run_b1",
            workspacePolicy: "managed-worktree",
            workspacePath: WT_B,
          }),
        ]);

        const outcome = yield* harness.router.finalizeFlowInstance({
          agentId: AGENT_ID,
          projectId: PS_PROJECT_ID,
          instanceKey: INST_A,
          threadId,
        });

        // The current facts keep the newer binding: nothing is dispatched.
        assert.deepEqual(outcome, { kind: "completed" });
        assert.lengthOf(metaUpdates(harness.commands), 0);
        assert.lengthOf(settleCommands(harness.commands), 0);
      }),
  );

  it.effect("project scope: a still-running turn defers the recompute", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness(makeScopedSettings("project"));
      harness.setOpenRuns([worktreeRun("run_1", WT_A)]);
      yield* wake(harness.router);
      const created = threadCreates(harness.commands)[0];
      const threadId = created?.type === "thread.create" ? String(created.threadId) : "";
      const projectId = created?.type === "thread.create" ? String(created.projectId) : "";
      harness.putThread(
        makeThreadShell(threadId, projectId, (shell) => ({
          ...shell,
          worktreePath: WT_A,
          session: runningSession(threadId),
        })),
      );
      harness.setOpenRuns([]);

      assert.deepEqual(
        yield* harness.router.finalizeFlowInstance({
          agentId: AGENT_ID,
          projectId: PS_PROJECT_ID,
          instanceKey: INST_A,
          threadId,
        }),
        { kind: "waiting", reason: "session-busy" },
      );
      assert.lengthOf(metaUpdates(harness.commands), 0);
    }),
  );

  it.effect(
    "project scope: registry empty with open work waits for the sweep; empty with none rebinds the recorded thread",
    () =>
      Effect.gen(function* () {
        const harness = yield* makeHarness(makeScopedSettings("project"));
        harness.setOpenRuns([worktreeRun("run_next", WT_B)]);

        // Registry empty (a restart) while open work exists: the sweep owns
        // creating the next session; the drive must not fork the binding.
        harness.putThread(makeThreadShell("thread_orphan", "t3_proj_1"));
        assert.deepEqual(
          yield* harness.router.finalizeFlowInstance({
            agentId: AGENT_ID,
            projectId: PS_PROJECT_ID,
            instanceKey: INST_A,
            threadId: "thread_orphan",
          }),
          { kind: "waiting", reason: "open-work" },
        );

        // No open work at all: the recorded thread returns to the root.
        harness.putThread(
          makeThreadShell("thread_orphan", "t3_proj_1", (shell) => ({
            ...shell,
            worktreePath: WT_A,
          })),
        );
        harness.setOpenRuns([]);
        assert.deepEqual(
          yield* harness.router.finalizeFlowInstance({
            agentId: AGENT_ID,
            projectId: PS_PROJECT_ID,
            instanceKey: INST_A,
            threadId: "thread_orphan",
          }),
          { kind: "completed" },
        );
        const rebinds = metaUpdates(harness.commands);
        assert.lengthOf(rebinds, 1);
        assert.strictEqual(
          rebinds[0]?.type === "thread.meta.update" ? rebinds[0].worktreePath : undefined,
          null,
        );
      }),
  );

  it.effect("a scope toggle between intake and drive: the CURRENT settings win", () =>
    Effect.gen(function* () {
      // Recorded under flow-instance scope, driven after the agent moved to
      // project scope: the recompute path runs, not the retention path.
      const harness = yield* makeHarness(makeScopedSettings("project"));
      harness.putThread(
        makeThreadShell("thread_moved", "t3_proj_1", (shell) => ({
          ...shell,
          worktreePath: WT_A,
        })),
      );
      harness.setOpenRuns([]);
      assert.deepEqual(
        yield* harness.router.finalizeFlowInstance({
          agentId: AGENT_ID,
          projectId: PS_PROJECT_ID,
          instanceKey: INST_A,
          threadId: "thread_moved",
        }),
        { kind: "completed" },
      );
      assert.lengthOf(settleCommands(harness.commands), 0);
      assert.lengthOf(metaUpdates(harness.commands), 1);
    }),
  );

  it.effect("an unknown or project-disabled agent waits as routing-unavailable", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness(
        makeSettings((base) => ({
          ...base,
          logicalAgents: {
            ...base.logicalAgents,
            [LogicalAgentId.make("ag_gone")]: {
              agentName: "Gone",
              providerInstanceId: ProviderInstanceId.make(PROVIDER_INSTANCE),
              persona: "",
              thinkLevel: null,
              modelOverride: null,
              project: { enabled: false, sessionScope: "project", sessionRetention: "settle" },
            },
          },
        })),
      );
      assert.deepEqual(
        yield* harness.router.finalizeFlowInstance({
          agentId: "ag_gone",
          projectId: PS_PROJECT_ID,
          instanceKey: INST_A,
          threadId: "thread_any",
        }),
        { kind: "waiting", reason: "routing-unavailable" },
      );
      assert.deepEqual(
        yield* harness.router.finalizeFlowInstance({
          agentId: "ag_missing",
          projectId: PS_PROJECT_ID,
          instanceKey: INST_A,
          threadId: "thread_any",
        }),
        { kind: "waiting", reason: "routing-unavailable" },
      );
    }),
  );
});
