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
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";

import {
  aggregateWorkNotificationMessage,
  composeWorkWakeMessage,
  makeProjectWorkSessionRouter,
  type ProjectWorkSessionRouter,
  ProjectWorkRoutingError,
  resolveProjectWorkRouting,
  workSessionThreadTitle,
  type ProjectWorkSessionRouterDeps,
  type ProjectWorkWakeInput,
} from "./ProjectWorkNoticeRouting.ts";

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
      modelOverride: null,
      project: { enabled: true },
    },
    [LogicalAgentId.make(OTHER_AGENT_ID)]: {
      agentName: "Secondary Agent",
      providerInstanceId: ProviderInstanceId.make("other-instance"),
      persona: "",
      modelOverride: null,
      project: { enabled: false },
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

interface Harness {
  readonly router: ProjectWorkSessionRouter;
  readonly commands: OrchestrationCommand[];
  readonly setSettings: (settings: ServerSettings) => void;
  readonly putThread: (shell: OrchestrationThreadShell) => void;
  readonly removeThread: (threadId: string) => void;
  readonly setOpenWorkCount: (count: number) => void;
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
      countOpenAssignedWork: () => {
        countCalls += 1;
        const outcome = workCountShouldFail
          ? Effect.fail(
              new ProjectWorkRoutingError({
                code: "CONSUMER_INTERNAL",
                detail: "authoritative Work query failed",
              }),
            )
          : Effect.succeed(openWorkCount);
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
      persona: "",
      modelOverride: null,
      projectServiceProjectId: PS_PROJECT_ID,
      projectName: "Registry",
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
  assert.strictEqual(
    aggregateWorkNotificationMessage(3),
    "There are 3 assigned Work items waiting. Use the Project tools to inspect them.",
  );
  assert.strictEqual(
    aggregateWorkNotificationMessage(1),
    "There is 1 assigned Work item waiting. Use the Project tools to inspect it.",
  );
  assert.strictEqual(workSessionThreadTitle("Registry", PS_PROJECT_ID), "Project Work — Registry");
  // A blank project name falls back to the stable id, never an empty title.
  assert.strictEqual(
    workSessionThreadTitle("  ", PS_PROJECT_ID),
    `Project Work — ${PS_PROJECT_ID}`,
  );
});

describe("composeWorkWakeMessage", () => {
  it("returns the bare aggregate when the agent has no persona", () => {
    assert.equal(composeWorkWakeMessage("", 2), aggregateWorkNotificationMessage(2));
    assert.equal(composeWorkWakeMessage("   ", 1), aggregateWorkNotificationMessage(1));
  });

  it("prepends the persona as agent directives", () => {
    const message = composeWorkWakeMessage("You are the reviewer.", 3);
    assert.isTrue(
      message.startsWith("<agent_directives>\nYou are the reviewer.\n</agent_directives>\n\n"),
    );
    assert.isTrue(message.endsWith(aggregateWorkNotificationMessage(3)));
  });

  it("persona rides the delivered aggregate turn", () =>
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
        composeWorkWakeMessage("You are the primary agent; be terse.", 2),
      );
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
      "Project Work — Registry",
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
      aggregateWorkNotificationMessage(2),
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

it.effect("a name-less notice titles the project from the directory", () =>
  Effect.gen(function* () {
    const harness = yield* makeHarness(makeSettings());

    yield* harness.router.routeWake(wakeInput({ projectName: undefined }));

    const created = projectCreates(harness.commands)[0];
    assert.strictEqual(created?.type === "project.create" && created.title, "registry");
    // The thread title falls back to the stable Project Service id.
    assert.strictEqual(
      threadCreates(harness.commands)[0]?.type === "thread.create" &&
        threadCreates(harness.commands)[0]?.title,
      `Project Work — ${PS_PROJECT_ID}`,
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

it.effect("idle session: one more wake delivers exactly one more aggregate", () =>
  Effect.gen(function* () {
    const harness = yield* makeHarness(makeSettings());
    yield* wake(harness.router);
    const [created] = threadCreates(harness.commands);
    const threadId = created?.type === "thread.create" ? created.threadId : undefined;
    const projectId = String(created?.projectId);
    assert.isDefined(threadId);

    // The aggregate's turn engaged, then finished.
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

    // Several notices arrive while busy: all recorded, none dispatched.
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
      aggregateWorkNotificationMessage(2),
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

    harness.failDispatch(true);
    const failure = yield* wake(harness.router).pipe(Effect.flip);
    assert.strictEqual(failure.code, "CONSUMER_INTERNAL");

    // The session survives the failure; the next notice delivers on it.
    harness.failDispatch(false);
    yield* wake(harness.router);
    assert.lengthOf(threadCreates(harness.commands), 1);
    assert.lengthOf(turnStarts(harness.commands), 2);
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
