import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import { RuntimeConsumerWakeError } from "@lanicblue/project-consumer";
import {
  DEFAULT_SERVER_SETTINGS,
  type LogicalAgentConfig,
  LogicalAgentId,
  type OrchestrationThreadShell,
  ProjectId,
  ProviderInstanceId,
  type ServerSettings,
  ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";

import type { ServerSettingsService } from "../serverSettings.ts";
import { routeProjectConsumerMissionEnded } from "./ProjectConsumerRuntimeService.ts";
import type {
  FlowInstanceFinalizationInput,
  FlowInstanceFinalizationOutcome,
  ProjectWorkSessionSnapshot,
} from "./ProjectWorkNoticeRouting.ts";
import { ProjectWorkRoutingError } from "./ProjectWorkNoticeRouting.ts";
import {
  makeFileMissionEndedLedgerStore,
  makeMissionSessionSettlementHandler,
  MissionEndedLedgerError,
  type MissionEndedNoticeInput,
  type MissionSessionSettlementDeps,
  type MissionSessionSettlementHandler,
} from "./MissionSessionSettlement.ts";

const AGENT_ID = "ag_primary";
const AGENT_ID_2 = "ag_secondary";
const PS_PROJECT_ID = "ps_proj_1";
const ISO = "2026-08-27T12:00:00.000Z";
const ISO_NEXT = "2026-08-27T12:00:01.000Z";
const GROUP = "ms_abc123";
const THREAD_1 = ThreadId.make("thread_registry");
const THREAD_2 = ThreadId.make("thread_persisted");

const makeSettings = (missionsEnabled: boolean): ServerSettings => {
  const agent = (agentId: string): LogicalAgentConfig => ({
    agentName: agentId,
    providerInstanceId: ProviderInstanceId.make("codex-main"),
    persona: "",
    thinkLevel: null,
    modelOverride: null,
    project: { enabled: true, sessionScope: "flow-instance", sessionRetention: "settle" },
  });
  return {
    ...DEFAULT_SERVER_SETTINGS,
    projectServiceClient: {
      enabled: true,
      baseUrl: "http://127.0.0.1:7600",
      keyIdHint: "key-1",
      credentialSet: true,
      missionsEnabled,
    },
    logicalAgents: {
      [LogicalAgentId.make(AGENT_ID)]: agent(AGENT_ID),
      [LogicalAgentId.make(AGENT_ID_2)]: agent(AGENT_ID_2),
    } as ServerSettings["logicalAgents"],
  };
};

const makeThreadShell = (threadId: string, agentId: string): OrchestrationThreadShell =>
  ({
    id: ThreadId.make(threadId),
    projectId: ProjectId.make("t3_proj_1"),
    title: "Mission work",
    logicalAgentId: LogicalAgentId.make(agentId),
    modelSelection: { instanceId: ProviderInstanceId.make("codex-main"), model: "gpt-5" },
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
  }) as OrchestrationThreadShell;

const registrySession = (
  agentId: string,
  group: string | null,
  threadId: ThreadId,
): ProjectWorkSessionSnapshot => ({
  agentId,
  projectId: PS_PROJECT_ID,
  flowInstanceKey: group,
  threadId,
  phase: "idle",
  pendingWork: false,
  boundWorktreePath: null,
  workspaceDir: "/tmp/mission-settlement",
});

const NOTICE: MissionEndedNoticeInput = {
  noticeId: "mne_<sha-1>",
  missionId: GROUP,
  group: GROUP,
  disposition: "completed",
  outcome: "implementation-ready",
  workspacePolicy: "managed-worktree",
  workspaceRef: "wt-1",
};

interface Harness {
  readonly handler: MissionSessionSettlementHandler;
  /** A fresh handler over the SAME ledger file (a restart). */
  readonly restart: () => Effect.Effect<
    MissionSessionSettlementHandler,
    never,
    FileSystem.FileSystem | Path.Path
  >;
  readonly drives: Array<FlowInstanceFinalizationInput>;
  /** Script the drive's next outcomes (consumed in order; default completes). */
  readonly driveOutcomes: Array<FlowInstanceFinalizationOutcome>;
  readonly putThread: (shell: OrchestrationThreadShell) => void;
  readonly setRegistrySessions: (sessions: ProjectWorkSessionSnapshot[]) => void;
  readonly setRoute: (agentId: string, group: string, threadId: string) => void;
  readonly ledgerPath: string;
}

const makeHarness = (
  ledgerPath: string,
  settings: ServerSettings,
): Effect.Effect<Harness, never, FileSystem.FileSystem | Path.Path> =>
  Effect.gen(function* () {
    const drives: FlowInstanceFinalizationInput[] = [];
    const driveOutcomes: FlowInstanceFinalizationOutcome[] = [];
    const threads = new Map<string, OrchestrationThreadShell>();
    let registrySessions: ProjectWorkSessionSnapshot[] = [];
    const routes = new Map<string, { readonly threadId: string; readonly psProjectId: string }>();

    const makeHandler = (): Effect.Effect<
      MissionSessionSettlementHandler,
      never,
      FileSystem.FileSystem | Path.Path
    > =>
      Effect.gen(function* () {
        const ledgerStore = yield* makeFileMissionEndedLedgerStore(ledgerPath);
        return yield* makeMissionSessionSettlementHandler({
          loadLedger: ledgerStore.load,
          storeLedger: ledgerStore.store,
          readSettings: Effect.succeed(settings),
          routingSessions: Effect.sync(() => registrySessions),
          readThreadShell: (threadId) =>
            Effect.succeed(
              threads.has(threadId)
                ? Option.some(threads.get(threadId) as OrchestrationThreadShell)
                : Option.none(),
            ),
          resolveSessionRoute: (input) =>
            Effect.succeed(routes.get(`${input.agentId}\n${input.instanceId}`) ?? null),
          settleWorkGroupSession: (input) =>
            Effect.sync(() => {
              drives.push(input);
              return driveOutcomes.shift() ?? { kind: "completed" };
            }),
          nowIso: Effect.succeed(ISO_NEXT),
        });
      });

    return {
      handler: yield* makeHandler(),
      restart: makeHandler,
      drives,
      driveOutcomes,
      putThread: (shell) => threads.set(shell.id, shell),
      setRegistrySessions: (sessions) => {
        registrySessions = sessions;
      },
      setRoute: (agentId, group, threadId) =>
        routes.set(`${agentId}\n${group}`, { threadId, psProjectId: PS_PROJECT_ID }),
      ledgerPath,
    } satisfies Harness;
  });

const makeTempLedgerPath = Effect.gen(function* () {
  const path = yield* Path.Path;
  const fs = yield* FileSystem.FileSystem;
  const tempDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3code-mission-ended-" });
  return path.join(tempDir, "mission-ended-ledger.json");
});

const settingsServiceOf = (settings: ServerSettings): ServerSettingsService["Service"] =>
  ({ getSettings: Effect.succeed(settings) }) as unknown as ServerSettingsService["Service"];

it.layer(NodeServices.layer)("MissionSessionSettlement", (it) => {
  it.effect("settles the group's sessions once per noticeId across redeliveries", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness(yield* makeTempLedgerPath, makeSettings(true));
      // One association from the live registry, one from the persisted route
      // ledger (the restart-safe half) — the union is the plan.
      harness.setRegistrySessions([registrySession(AGENT_ID, GROUP, THREAD_1)]);
      harness.setRoute(AGENT_ID_2, GROUP, THREAD_2);
      harness.putThread(makeThreadShell(THREAD_2, AGENT_ID_2));

      const first = yield* harness.handler.handleMissionEndedNotice(NOTICE);
      if (first.status !== "acked") {
        assert.fail(`expected acked, got ${first.status}`);
      }
      assert.deepEqual(
        harness.drives.map((drive) => drive.threadId).toSorted(),
        // Sorted order: "thread_persisted" before "thread_registry".
        [THREAD_2, THREAD_1],
      );
      // The drive addresses the group: instanceKey is the mission's group and
      // the project/agent facts ride the association.
      for (const drive of harness.drives) {
        assert.equal(drive.instanceKey, GROUP);
        assert.equal(drive.projectId, PS_PROJECT_ID);
      }

      // Redelivery of the same noticeId is the SAME event: acked from the
      // ledger, zero new side effects.
      const drivesBefore = harness.drives.length;
      const second = yield* harness.handler.handleMissionEndedNotice(NOTICE);
      if (second.status !== "acked") {
        assert.fail(`expected acked, got ${second.status}`);
      }
      assert.equal(harness.drives.length, drivesBefore);
    }),
  );

  it.effect("keeps the notice un-ACKed while a session is blocked, and the resume finishes it", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness(yield* makeTempLedgerPath, makeSettings(true));
      harness.setRegistrySessions([registrySession(AGENT_ID, GROUP, THREAD_1)]);
      // The first drive is blocked on a safe idle state; later drives complete.
      harness.driveOutcomes.push({ kind: "waiting", reason: "session-busy" });

      const blocked = yield* harness.handler.handleMissionEndedNotice(NOTICE);
      if (blocked.status !== "waiting") {
        assert.fail(`expected waiting, got ${blocked.status}`);
      }
      assert.deepEqual(blocked.blockedThreadIds, [THREAD_1]);

      // The thread turns safe: the sweep / thread-event retry drives resume.
      yield* harness.handler.resumePending;
      const resumed = yield* harness.handler.handleMissionEndedNotice(NOTICE);
      if (resumed.status !== "acked") {
        assert.fail(`expected acked, got ${resumed.status}`);
      }
      assert.equal(harness.drives.length, 2);
    }),
  );

  it.effect("a restart resumes an unfinished settlement from the ledger file alone", () =>
    Effect.gen(function* () {
      const ledgerPath = yield* makeTempLedgerPath;
      const harness = yield* makeHarness(ledgerPath, makeSettings(true));
      harness.setRegistrySessions([registrySession(AGENT_ID, GROUP, THREAD_1)]);
      harness.driveOutcomes.push({ kind: "waiting", reason: "session-busy" });
      const blocked = yield* harness.handler.handleMissionEndedNotice(NOTICE);
      if (blocked.status !== "waiting") {
        assert.fail(`expected waiting, got ${blocked.status}`);
      }

      // The process restarts: the registry is empty and the notice will never
      // re-fire (a delivered frame redelivers only until ACKed). The
      // PERSISTED route row carries the association into the fresh handler.
      const restarted = yield* harness.restart();
      harness.setRegistrySessions([]);
      harness.setRoute(AGENT_ID, GROUP, THREAD_1);
      yield* restarted.resumePending;

      const afterRestart = yield* restarted.handleMissionEndedNotice(NOTICE);
      if (afterRestart.status !== "acked") {
        assert.fail(`expected acked, got ${afterRestart.status}`);
      }
      assert.deepEqual(harness.drives.map((drive) => drive.threadId), [THREAD_1, THREAD_1]);
    }),
  );

  it.effect(
    "plans only live, correctly-stamped associations; a mission never worked is an immediate no-op ACK",
    () =>
      Effect.gen(function* () {
        const harness = yield* makeHarness(yield* makeTempLedgerPath, makeSettings(true));
        // Registry sessions of OTHER groups (and project-scope sessions)
        // never join the plan.
        harness.setRegistrySessions([
          registrySession(AGENT_ID, "ms_other", ThreadId.make("thread_other")),
          registrySession(AGENT_ID, null, ThreadId.make("thread_project_scope")),
        ]);
        // A persisted row whose thread is gone, and one whose thread no longer
        // carries the agent stamp, are corrected out of the plan.
        harness.setRoute(AGENT_ID, GROUP, "thread_deleted");
        harness.setRoute(AGENT_ID_2, GROUP, "thread_restamped");
        harness.putThread({
          ...makeThreadShell("thread_restamped", AGENT_ID_2),
          logicalAgentId: null,
        });

        const outcome = yield* harness.handler.handleMissionEndedNotice(NOTICE);
        if (outcome.status !== "acked") {
          assert.fail(`expected acked, got ${outcome.status}`);
        }
        assert.deepEqual(outcome.settledThreadIds, []);
        assert.equal(harness.drives.length, 0);

        // And a mission no fact at all mentions: acked as a no-op, no drives.
        const unknown: MissionEndedNoticeInput = {
          ...NOTICE,
          noticeId: "mne_<sha-2>",
          group: "ms_unworked",
          missionId: "ms_unworked",
        };
        const noop = yield* harness.handler.handleMissionEndedNotice(unknown);
        if (noop.status !== "acked") {
          assert.fail(`expected acked, got ${noop.status}`);
        }
        assert.equal(harness.drives.length, 0);
      }),
  );

  it.effect("the intake gate: a mission frame while missionsEnabled is off is rejected, never activated", () =>
    Effect.gen(function* () {
      let handled = 0;
      const stubHandler: MissionSessionSettlementHandler = {
        handleMissionEndedNotice: () =>
          Effect.sync(() => {
            handled += 1;
            return { status: "acked" as const, settledThreadIds: [] };
          }),
        resumePending: Effect.void,
      };

      const rejected = yield* Effect.flip(
        routeProjectConsumerMissionEnded(stubHandler, settingsServiceOf(makeSettings(false)), NOTICE),
      );
      assert.ok(rejected instanceof RuntimeConsumerWakeError);
      assert.equal(rejected.code, "AGENT_NOT_DISPATCHABLE");
      assert.match(rejected.message, /missionsEnabled/);
      assert.equal(handled, 0);

      // Gate on: the frame reaches the handler and resolving ACKs it.
      yield* routeProjectConsumerMissionEnded(
        stubHandler,
        settingsServiceOf(makeSettings(true)),
        NOTICE,
      );
      assert.equal(handled, 1);

      // Gate on but the settlement is blocked: AGENT_BUSY so the Project
      // Service redelivers (resolving is the ACK, same as retirement).
      const stubWaiting: MissionSessionSettlementHandler = {
        handleMissionEndedNotice: () =>
          Effect.succeed({ status: "waiting" as const, blockedThreadIds: [THREAD_1] }),
        resumePending: Effect.void,
      };
      const busy = yield* Effect.flip(
        routeProjectConsumerMissionEnded(
          stubWaiting,
          settingsServiceOf(makeSettings(true)),
          NOTICE,
        ),
      );
      assert.equal(busy.code, "AGENT_BUSY");
    }),
  );

  it.effect("an unreadable ledger surfaces as a CONSUMER_INTERNAL routing failure", () =>
    Effect.gen(function* () {
      const deps: MissionSessionSettlementDeps = {
        loadLedger: Effect.fail(
          new MissionEndedLedgerError({ operation: "read", ledgerPath: "/gone/ledger.json" }),
        ),
        storeLedger: () => Effect.void,
        readSettings: Effect.succeed(makeSettings(true)),
        routingSessions: Effect.succeed([]),
        readThreadShell: () => Effect.succeed(Option.none()),
        resolveSessionRoute: () => Effect.succeed(null),
        settleWorkGroupSession: () => Effect.succeed({ kind: "completed" }),
        nowIso: Effect.succeed(ISO),
      };
      const handler = yield* makeMissionSessionSettlementHandler(deps);
      const failure = yield* Effect.flip(handler.handleMissionEndedNotice(NOTICE));
      assert.equal(failure._tag, "ProjectWorkRoutingError");
      if (failure._tag === "ProjectWorkRoutingError") {
        assert.equal(failure.code, "CONSUMER_INTERNAL");
      }
    }),
  );
});
