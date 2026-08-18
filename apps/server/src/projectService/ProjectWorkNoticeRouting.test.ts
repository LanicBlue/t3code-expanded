import { assert, it } from "@effect/vitest";
import {
  DEFAULT_SERVER_SETTINGS,
  type LogicalAgentConfig,
  LogicalAgentId,
  type OrchestrationCommand,
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
  makeProjectWorkSessionRouter,
  type ProjectWorkSessionRouter,
  ProjectWorkRoutingError,
  resolveProjectWorkRouting,
  workSessionThreadTitle,
  type ProjectWorkSessionRouterDeps,
} from "./ProjectWorkNoticeRouting.ts";

const AGENT_ID = "ag_primary";
const OTHER_AGENT_ID = "ag_secondary";
const PS_PROJECT_ID = "ps_proj_1";
const T3_PROJECT_ID = "t3_proj_9";
const PROVIDER_INSTANCE = "codex-main";
const ISO = "2026-08-14T12:00:00.000Z";

const makeSettings = (mutations?: (settings: ServerSettings) => ServerSettings) => {
  const agents: Record<string, LogicalAgentConfig> = {
    [LogicalAgentId.make(AGENT_ID)]: {
      agentName: "Primary Agent",
      providerInstanceId: ProviderInstanceId.make(PROVIDER_INSTANCE),
      project: { enabled: true },
      projectBindings: [
        {
          projectId: PS_PROJECT_ID,
          projectName: "Registry",
          t3ProjectId: ProjectId.make(T3_PROJECT_ID),
        },
      ],
    },
    [LogicalAgentId.make(OTHER_AGENT_ID)]: {
      agentName: "Secondary Agent",
      providerInstanceId: ProviderInstanceId.make("other-instance"),
      project: { enabled: false },
      projectBindings: [],
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
  mutations?: (shell: OrchestrationThreadShell) => OrchestrationThreadShell,
): OrchestrationThreadShell => {
  const base: OrchestrationThreadShell = {
    id: ThreadId.make(threadId),
    projectId: ProjectId.make(T3_PROJECT_ID),
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

const makeProjectShell = (): OrchestrationProjectShell => ({
  id: ProjectId.make(T3_PROJECT_ID),
  title: "Registry",
  workspaceRoot: "/tmp/registry",
  defaultModelSelection: null,
  scripts: [],
  createdAt: ISO,
  updatedAt: ISO,
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
  /** Simulated latency on the authoritative Work query (real ms, it.live). */
  readonly setWorkCountDelayMs: (ms: number) => void;
  /** Simulated latency on orchestration dispatch (real ms, it.live). */
  readonly setDispatchDelayMs: (ms: number) => void;
}

const makeHarness = (settings: ServerSettings): Effect.Effect<Harness> =>
  Effect.gen(function* () {
    const commands: OrchestrationCommand[] = [];
    let currentSettings = settings;
    const threads = new Map<string, OrchestrationThreadShell>();
    const providers = new Map<string, ProviderDriverKind>([
      [PROVIDER_INSTANCE, ProviderDriverKind.make("codex")],
    ]);
    let openWorkCount = 2;
    let workCountShouldFail = false;
    let dispatchShouldFail = false;
    let workCountDelayMs = 0;
    let dispatchDelayMs = 0;
    let countCalls = 0;
    let idCounter = 0;

    const deps: ProjectWorkSessionRouterDeps = {
      readSettings: Effect.sync(() => currentSettings),
      readThreadShell: (threadId) =>
        Effect.succeed(
          threads.has(threadId)
            ? Option.some(threads.get(threadId) as OrchestrationThreadShell)
            : Option.none(),
        ),
      readProjectShell: () => Effect.succeed(Option.some(makeProjectShell())),
      resolveProviderDriver: (instanceId) =>
        Effect.succeed(
          providers.has(instanceId)
            ? Option.some(providers.get(instanceId) as ProviderDriverKind)
            : Option.none(),
        ),
      dispatchCommand: (command) =>
        dispatchShouldFail
          ? Effect.fail(
              new ProjectWorkRoutingError({ code: "CONSUMER_INTERNAL", detail: "dispatch failed" }),
            )
          : dispatchDelayMs > 0
            ? Effect.sleep(dispatchDelayMs).pipe(
                Effect.flatMap(() =>
                  Effect.sync(() => {
                    commands.push(command);
                    if (command.type === "thread.create") {
                      // The engine projects inside the dispatch transaction:
                      // by the time dispatch resolves, the thread is readable.
                      threads.set(command.threadId, makeThreadShell(command.threadId));
                    }
                  }),
                ),
              )
            : Effect.sync(() => {
                commands.push(command);
                if (command.type === "thread.create") {
                  // The engine projects inside the dispatch transaction:
                  // by the time dispatch resolves, the thread is readable.
                  threads.set(command.threadId, makeThreadShell(command.threadId));
                }
              }),
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

const wake = (router: ProjectWorkSessionRouter) =>
  router.routeWake({ agentId: AGENT_ID, projectId: PS_PROJECT_ID });

it("routing resolution is id-based and structural", () => {
  {
    const settings = makeSettings();

    const resolved = resolveProjectWorkRouting(settings, AGENT_ID, PS_PROJECT_ID);
    assert.isTrue(resolved.ok);
    assert.strictEqual(resolved.ok && resolved.target.t3ProjectId, T3_PROJECT_ID);

    assert.isFalse(resolveProjectWorkRouting(settings, "ag_missing", PS_PROJECT_ID).ok);
    // Project-disabled agents are not routable (nor advertised).
    assert.isFalse(resolveProjectWorkRouting(settings, OTHER_AGENT_ID, PS_PROJECT_ID).ok);
    // Unbound Project Service projects are not routable.
    assert.isFalse(resolveProjectWorkRouting(settings, AGENT_ID, "ps_other").ok);
    // Disabled integration fails routing for every agent.
    assert.isFalse(
      resolveProjectWorkRouting(
        makeSettings((base) => ({
          ...base,
          projectServiceClient: { ...base.projectServiceClient, enabled: false },
        })),
        AGENT_ID,
        PS_PROJECT_ID,
      ).ok,
    );
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

it.effect(
  "missing session: creates it with the configured binding and delivers the aggregate",
  () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness(makeSettings());

      yield* wake(harness.router);

      assert.lengthOf(threadCreates(harness.commands), 1);
      const created = threadCreates(harness.commands)[0];
      assert.strictEqual(
        created?.type === "thread.create" && String(created.projectId),
        T3_PROJECT_ID,
      );
      assert.strictEqual(
        created?.type === "thread.create" && created.title,
        "Project Work — Registry",
      );
      assert.strictEqual(
        created?.type === "thread.create" && String(created.modelSelection.instanceId),
        PROVIDER_INSTANCE,
      );
      // Driver default model: the project has no instance-matching default.
      assert.strictEqual(
        created?.type === "thread.create" && created.modelSelection.model,
        "gpt-5.6-sol",
      );

      assert.lengthOf(turnStarts(harness.commands), 1);
      const turn = turnStarts(harness.commands)[0];
      assert.strictEqual(
        turn?.type === "thread.turn.start" && turn.message.text,
        aggregateWorkNotificationMessage(2),
      );

      const sessions = yield* harness.router.snapshotSessions;
      assert.lengthOf(sessions, 1);
      assert.strictEqual(sessions[0]?.phase, "notifying");
      assert.strictEqual(
        sessions[0]?.threadId,
        created?.type === "thread.create" && created.threadId,
      );
    }),
);

it.effect("idle session: one more wake delivers exactly one more aggregate", () =>
  Effect.gen(function* () {
    const harness = yield* makeHarness(makeSettings());
    yield* wake(harness.router);
    const [created] = threadCreates(harness.commands);
    const threadId = created?.type === "thread.create" ? created.threadId : undefined;
    assert.isDefined(threadId);

    // The aggregate's turn engaged, then finished.
    const engage = (status: "running" | "ready") =>
      harness.putThread(
        makeThreadShell(threadId as string, (shell) => ({
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
      makeThreadShell(threadId as string, (shell) => ({
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
    assert.isNotNull(threadId);
    assert.lengthOf(turnStarts(harness.commands), 1);

    // The aggregate turn is running.
    harness.putThread(
      makeThreadShell(threadId as string, (shell) => ({
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
      makeThreadShell(threadId as string, (shell) => ({
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
    assert.isNotNull(threadId);

    // Work arrives while the aggregate turn runs; the turn then engages.
    harness.putThread(
      makeThreadShell(threadId as string, (shell) => ({
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
      makeThreadShell(threadId as string, (shell) => ({
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

    // The aggregate was dispatched but no provider session engaged yet.
    harness.putThread(makeThreadShell(threadId as string));
    yield* wake(harness.router);
    assert.lengthOf(turnStarts(harness.commands), 1);

    // An event lands while still queued — nothing may dispatch.
    yield* harness.router.onThreadEvent(threadId as never);
    assert.lengthOf(turnStarts(harness.commands), 1);

    // A ready-but-resting session is still not a finished turn.
    harness.putThread(
      makeThreadShell(threadId as string, (shell) => ({
        ...shell,
        session: readySession(threadId as string),
      })),
    );
    yield* harness.router.onThreadEvent(threadId as never);
    assert.lengthOf(turnStarts(harness.commands), 1);

    // The turn engages, then finishes: the recorded work delivers once.
    harness.putThread(
      makeThreadShell(threadId as string, (shell) => ({
        ...shell,
        session: runningSession(threadId as string),
      })),
    );
    yield* harness.router.onThreadEvent(threadId as never);
    harness.putThread(
      makeThreadShell(threadId as string, (shell) => ({
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

    harness.putThread(
      makeThreadShell(firstThreadId as string, (shell) => ({
        ...shell,
        archivedAt: ISO,
      })),
    );
    yield* wake(harness.router);

    assert.lengthOf(threadCreates(harness.commands), 2);
    const second = threadCreates(harness.commands)[1];
    const secondThreadId = second?.type === "thread.create" ? second.threadId : null;
    assert.notStrictEqual(secondThreadId, firstThreadId);
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

    assert.isEmpty(harness.commands);
    assert.isEmpty(yield* harness.router.snapshotSessions);
  }),
);

it.effect("authoritative count is re-queried at delivery time", () =>
  Effect.gen(function* () {
    const harness = yield* makeHarness(makeSettings());
    yield* wake(harness.router);
    const [created] = threadCreates(harness.commands);
    const threadId = created?.type === "thread.create" ? created.threadId : null;

    // Work resolved between notice and delivery: the deferred aggregate is
    // silent about zero items instead of waking the agent for nothing.
    harness.putThread(
      makeThreadShell(threadId as string, (shell) => ({
        ...shell,
        session: runningSession(threadId as string),
      })),
    );
    yield* wake(harness.router);
    yield* harness.router.onThreadEvent(threadId as never); // engaged: running
    harness.setOpenWorkCount(0);
    harness.putThread(
      makeThreadShell(threadId as string, (shell) => ({
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

    // Rename both sides: same ids must route to the same session.
    harness.setSettings(
      makeSettings((base) => ({
        ...base,
        logicalAgents: {
          ...base.logicalAgents,
          [LogicalAgentId.make(AGENT_ID)]: {
            ...base.logicalAgents[LogicalAgentId.make(AGENT_ID) as never],
            agentName: "Renamed Agent",
            projectBindings: [
              {
                projectId: PS_PROJECT_ID,
                projectName: "Renamed Registry",
                t3ProjectId: ProjectId.make(T3_PROJECT_ID),
              },
            ],
          },
        } as ServerSettings["logicalAgents"],
      })),
    );
    harness.putThread(
      makeThreadShell(threadId as string, (shell) => ({
        ...shell,
        session: runningSession(threadId as string),
      })),
    );
    yield* harness.router.onThreadEvent(threadId as never);
    harness.putThread(
      makeThreadShell(threadId as string, (shell) => ({
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
      .routeWake({ agentId: "ag_missing", projectId: PS_PROJECT_ID })
      .pipe(Effect.flip);
    assert.strictEqual(unknown.code, "AGENT_NOT_FOUND");

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
    // Nothing was created or delivered.
    assert.isEmpty(harness.commands);

    // A failed wake stays recoverable: the redelivered notice routes.
    harness.failWorkCount(false);
    yield* wake(harness.router);
    assert.lengthOf(threadCreates(harness.commands), 1);
  }),
);

it.effect("dispatch failure fails the wake and leaves the created session recoverable", () =>
  Effect.gen(function* () {
    const harness = yield* makeHarness(makeSettings());
    yield* wake(harness.router);
    const [created] = threadCreates(harness.commands);
    const threadId = created?.type === "thread.create" ? created.threadId : null;
    harness.putThread(
      makeThreadShell(threadId as string, (shell) => ({
        ...shell,
        session: runningSession(threadId as string),
      })),
    );
    yield* harness.router.onThreadEvent(threadId as never);
    harness.putThread(
      makeThreadShell(threadId as string, (shell) => ({
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
    assert.lengthOf(turnStarts(first.commands), 1);

    // Process B (fresh in-memory state) receives the still-pending replay.
    // The pre-restart thread stays idle and untouched; the replay creates
    // the next current session exactly once and delivers ONE aggregate.
    const second = yield* makeHarness(makeSettings());
    second.putThread(
      makeThreadShell(oldThreadId as string, (shell) => ({
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

    // The aggregate dispatched, the session died before engaging, and more
    // work arrived: the lapsed notification must not block forever.
    harness.putThread(
      makeThreadShell(threadId as string, (shell) => ({
        ...shell,
        session: stoppedSession(threadId as string),
      })),
    );
    yield* wake(harness.router);
    yield* harness.router.onThreadEvent(threadId as never);
    assert.lengthOf(turnStarts(harness.commands), 2);
  }),
);
