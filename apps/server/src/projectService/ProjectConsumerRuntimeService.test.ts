import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import {
  CONSUMER_CAPABILITY_MISSION_V1,
  consumerWireAdapter,
  decodeConsumerGatewayFrame,
  type ConsumerGatewayMessage,
  type ConsumerSocketLike,
  newMessageId,
  workAvailableNoticeId,
} from "@lanicblue/project-consumer";
import {
  type LogicalAgentConfig,
  LogicalAgentId,
  type OrchestrationCommand,
  type OrchestrationEvent,
  type OrchestrationThreadShell,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
import type { OrchestrationSession } from "@t3tools/contracts";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import { FetchHttpClient } from "effect/unstable/http";

import * as ServerSecretStore from "../auth/ServerSecretStore.ts";
import * as ServerConfig from "../config.ts";
import * as OrchestrationEngine from "../orchestration/Services/OrchestrationEngine.ts";
import * as ProjectionSnapshotQuery from "../orchestration/Services/ProjectionSnapshotQuery.ts";
import * as ProviderInstanceRegistry from "../provider/Services/ProviderInstanceRegistry.ts";
import * as ProjectWorkSessionRouteStore from "../persistence/ProjectWorkSessionRoute.ts";
import * as ServerSettingsModule from "../serverSettings.ts";
import * as WorkspacePaths from "../workspace/WorkspacePaths.ts";
import * as ProjectConsumerRuntime from "./ProjectConsumerRuntimeService.ts";
import * as ProjectServiceWorkClient from "./ProjectServiceWorkClient.ts";

const CREDENTIAL = "psk_key-1.s3cret";
const AGENT_ID = "ag_one";
const PS_PROJECT_ID = "ps_proj_1";
const ISO = "2026-08-14T12:00:00.000Z";
/** The notice's workspace directory — the one directory on the fake disk. */
const WORKSPACE_DIR = "/tmp/registry";

// ── Fake client socket + gateway (the SDK drives the client side) ──

/** Deterministic fake socket; close() does not auto-fire onclose. */
class FakeSocket implements ConsumerSocketLike {
  readonly sent: string[] = [];
  closed = false;
  onopen: (() => void) | null = null;
  onmessage: ((event: { readonly data: unknown }) => void) | null = null;
  onclose: (() => void) | null = null;
  onerror: (() => void) | null = null;

  send(data: string): void {
    if (this.closed) throw new Error("socket is closed");
    this.sent.push(data);
  }

  close(): void {
    this.closed = true;
  }

  fireOpen(): void {
    this.onopen?.();
  }

  fireClose(): void {
    this.onclose?.();
  }

  receive(message: ConsumerGatewayMessage, version: 1 | 3 | 4 = 1): void {
    this.onmessage?.({ data: consumerWireAdapter(version).encode(message) });
  }

  sentMessages(): ConsumerGatewayMessage[] {
    return this.sent.map((frame) => decodeConsumerGatewayFrame(frame).message);
  }
}

interface GatewayRecording {
  readonly sockets: FakeSocket[];
  readonly hellos: Array<{
    readonly credential?: string;
    readonly capabilities: ReadonlyArray<string>;
    readonly agents: ReadonlyArray<{ readonly agentId: string; readonly displayName?: string }>;
    readonly url: string;
  }>;
  readonly acks: string[];
  readonly failures: Array<{ readonly noticeId: string; readonly code: string }>;
}

const makeGateway = () => {
  const sockets: FakeSocket[] = [];
  const hellos: GatewayRecording["hellos"] = [];
  const acks: string[] = [];
  const failures: GatewayRecording["failures"] = [];
  // The URL the factory last saw — asserted for ws-derivation.
  let socketUrl = "";

  const welcomeFor = (socket: FakeSocket) => {
    socket.receive({
      type: "welcome",
      id: newMessageId(),
      sentAt: ISO,
      payload: {
        serverVersion: "0.5.0",
        // V4: the notice's project block may carry workspaceDir (issue #18).
        protocol: { selected: 4, current: 4, minSupported: 1 },
        capabilities: [],
        client: {
          latest: "0.5.0",
          recommended: "0.5.0",
          minSupported: "0.1.0",
          status: "current",
          deprecation: null,
        },
        inventoryAccepted: true,
        replay: "none",
      },
    });
  };

  const handleClientFrame = (socket: FakeSocket, frame: string) => {
    const message = decodeConsumerGatewayFrame(frame).message;
    switch (message.type) {
      case "hello":
        hellos.push({
          ...(message.payload.credential !== undefined
            ? { credential: message.payload.credential }
            : {}),
          capabilities: [...message.payload.capabilities],
          agents: message.payload.agents.map((agent) => ({
            agentId: String(agent.agentId),
            ...(agent.displayName !== undefined ? { displayName: agent.displayName } : {}),
          })),
          url: socketUrl,
        });
        welcomeFor(socket);
        return;
      case "delivery.ack":
        acks.push(message.payload.noticeId);
        return;
      case "delivery.failure":
        failures.push({ noticeId: message.payload.noticeId, code: message.payload.code });
        return;
      default:
        return;
    }
  };

  const factory = (url: string): FakeSocket => {
    socketUrl = url;
    const socket = new FakeSocket();
    sockets.push(socket);
    const originalSend = socket.send.bind(socket);
    socket.send = (data: string) => {
      originalSend(data);
      handleClientFrame(socket, data);
    };
    return socket;
  };

  return {
    factory,
    recording: { sockets, hellos, acks, failures } satisfies GatewayRecording,
    /** Server → client structured work.available (V4 notice shape). */
    sendWorkAvailable: (
      socket: FakeSocket,
      input: { readonly runId: string; readonly occupancyRevision?: number },
    ) => {
      socket.receive(
        {
          type: "work.available",
          id: newMessageId(),
          sentAt: ISO,
          payload: {
            noticeId: workAvailableNoticeId(
              PS_PROJECT_ID,
              input.runId,
              input.occupancyRevision ?? 1,
            ),
            project: {
              projectId: PS_PROJECT_ID,
              projectName: "Registry",
              workspaceDir: WORKSPACE_DIR,
            },
            agent: { agentId: AGENT_ID, agentName: "Build agent" },
            positionId: "pos_1",
            runId: input.runId,
            occupancyRevision: input.occupancyRevision ?? 1,
            runRevision: 1,
            openedAt: ISO,
          } as never,
        },
        4,
      );
    },
    /** Server → client gateway error (encoded on the negotiated V4 wire). */
    sendServerError: (socket: FakeSocket, code: string, message: string) => {
      socket.receive(
        { type: "error", id: newMessageId(), sentAt: ISO, payload: { code, message } },
        4,
      );
    },
    requestInventory: (socket: FakeSocket) => {
      socket.receive({
        type: "inventory.request",
        id: newMessageId(),
        sentAt: ISO,
        payload: { reason: "reconcile" },
      });
    },
  };
};

/** Pumps the SDK's promise continuation chains (wake → ack/failure). */
const flush = async (): Promise<void> => {
  for (let tick = 0; tick < 30; tick += 1) {
    await Promise.resolve();
  }
};

/**
 * Flush plus a real-time hop: the routing seam's canonicalization does REAL
 * filesystem work (realpath on the libuv threadpool), whose promises resolve
 * on a macrotask horizon microtasks alone never reach — Effect.sleep under
 * it.live crosses it, then the microtask drain settles the wake chain.
 */
const settleWake = (): Effect.Effect<void> =>
  Effect.sleep(Duration.millis(10)).pipe(Effect.flatMap(() => Effect.promise(flush)));

/**
 * Real-time wait for the SDK's own timers (hello/notice promise chains and
 * the 10ms reconnect backoff). These tests run under `it.live` because the
 * SDK rides real timers the Effect test clock cannot drive.
 */
const waitForSdk = (ms: number): Effect.Effect<void> => Effect.sleep(Duration.millis(ms));

// ── Fake orchestration seams ─────────────────────────────────────

const threadShellFor = (threadId: string): OrchestrationThreadShell => ({
  id: ThreadId.make(threadId),
  projectId: ProjectId.make("t3_proj_autocreated"),
  title: "Project Work — Registry",
  modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5.6-sol" },
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
});

const sessionWithStatus = (
  threadId: string,
  status: "running" | "ready",
): OrchestrationSession => ({
  threadId: ThreadId.make(threadId),
  status,
  providerName: "codex",
  runtimeMode: "full-access",
  activeTurnId: null,
  lastError: null,
  updatedAt: ISO,
});

const makeFakes = (
  gateway: ReturnType<typeof makeGateway>,
  options?: { readonly reconcileSweepIntervalMs?: number },
) =>
  Effect.gen(function* () {
    const commands: OrchestrationCommand[] = [];
    const threads = new Map<string, OrchestrationThreadShell>();
    // The projection of auto-created projects, keyed by id and by root.
    const projects = new Map<string, { readonly id: string; readonly workspaceRoot: string }>();
    const eventQueue = yield* Queue.unbounded<OrchestrationEvent>();
    let openRunCount = 2;
    let runsFailure: ProjectServiceWorkClient.ProjectServiceWorkClientError | null = null;
    // The Project Service project list the reconcile sweep enumerates.
    let serviceProjects: ProjectServiceWorkClient.ProjectServiceProjectRecord[] = [];

    // Dispatched commands become visible thread/project state, mirroring the
    // projection (a notification turn.start makes the session busy).
    const dispatch = (command: OrchestrationCommand) =>
      Effect.sync(() => {
        commands.push(command);
        if (command.type === "project.create") {
          projects.set(String(command.projectId), {
            id: String(command.projectId),
            workspaceRoot: command.workspaceRoot,
          });
        }
        if (command.type === "thread.create") {
          threads.set(command.threadId, threadShellFor(command.threadId));
        }
        if (command.type === "thread.turn.start") {
          const shell = threads.get(command.threadId);
          if (shell !== undefined) {
            threads.set(command.threadId, {
              ...shell,
              session: sessionWithStatus(command.threadId, "running"),
            });
          }
        }
      });

    const sessionSetEvent = (threadId: string, status: "running" | "ready"): OrchestrationEvent =>
      ({
        sequence: commands.length,
        eventId: `evt-${commands.length}`,
        aggregateKind: "thread",
        aggregateId: ThreadId.make(threadId),
        occurredAt: ISO,
        commandId: null,
        causationEventId: null,
        correlationId: null,
        metadata: {},
        type: "thread.session-set",
        payload: {
          threadId: ThreadId.make(threadId),
          session: sessionWithStatus(threadId, status),
        },
      }) as unknown as OrchestrationEvent;

    const engineLayer = Layer.succeed(OrchestrationEngine.OrchestrationEngineService, {
      readEvents: () => Stream.empty,
      dispatch: (command: OrchestrationCommand) =>
        dispatch(command).pipe(Effect.as({ sequence: commands.length })),
      streamDomainEvents: Stream.fromQueue(eventQueue),
      latestSequence: Effect.succeed(commands.length),
    } as OrchestrationEngine.OrchestrationEngineService["Service"]);

    const snapshotQueryLayer = Layer.succeed(ProjectionSnapshotQuery.ProjectionSnapshotQuery, {
      getSnapshot: () =>
        Effect.succeed({
          snapshotSequence: commands.length,
          projects: [...projects.values()].map((project) => ({
            id: ProjectId.make(project.id),
            title: "Registry",
            workspaceRoot: project.workspaceRoot,
            defaultModelSelection: null,
            scripts: [],
            createdAt: ISO,
            updatedAt: ISO,
            deletedAt: null,
          })),
          threads: [],
          updatedAt: ISO,
        }),
      getShellSnapshot: () =>
        Effect.succeed({
          snapshotSequence: commands.length,
          projects: [],
          threads: [...threads.values()].map((shell) => ({
            ...shell,
          })),
          updatedAt: ISO,
        }),
      getThreadShellById: (threadId: ThreadId) =>
        Effect.succeed(
          threads.has(threadId)
            ? Option.some(threads.get(threadId) as OrchestrationThreadShell)
            : Option.none(),
        ),
      getActiveProjectByWorkspaceRoot: (workspaceRoot: string) => {
        const active = [...projects.values()].find(
          (project) => project.workspaceRoot === workspaceRoot,
        );
        return Effect.succeed(
          active === undefined
            ? Option.none()
            : Option.some({
                id: ProjectId.make(active.id),
                title: "Registry",
                workspaceRoot: active.workspaceRoot,
                defaultModelSelection: null,
                scripts: [],
                createdAt: ISO,
                updatedAt: ISO,
                deletedAt: null,
              }),
        );
      },
      getProjectShellById: (projectId: ProjectId) =>
        Effect.succeed(
          projects.has(projectId)
            ? Option.some({
                id: projectId,
                title: "Registry",
                workspaceRoot: (
                  projects.get(projectId as string) as { readonly workspaceRoot: string }
                ).workspaceRoot,
                defaultModelSelection: null,
                scripts: [],
                createdAt: ISO,
                updatedAt: ISO,
              })
            : Option.none(),
        ),
    } as unknown as ProjectionSnapshotQuery.ProjectionSnapshotQuery["Service"]);

    // The fake disk: exactly one directory the Project Service may point at.
    const workspacePathsLayer = Layer.succeed(WorkspacePaths.WorkspacePaths, {
      normalizeWorkspaceRoot: (workspaceRoot: string) =>
        workspaceRoot === WORKSPACE_DIR
          ? Effect.succeed(WORKSPACE_DIR)
          : Effect.fail(
              new WorkspacePaths.WorkspaceRootNotExistsError({
                workspaceRoot,
                normalizedWorkspaceRoot: workspaceRoot,
              }),
            ),
      resolveRelativePathWithinRoot: () => Effect.die("unused"),
    } as WorkspacePaths.WorkspacePaths["Service"]);

    const codexInstance = {
      instanceId: ProviderInstanceId.make("codex"),
      driverKind: "codex",
      continuationIdentity: { driverKind: "codex", continuationKey: "codex:instance:codex" },
      displayName: "Codex",
      enabled: true,
    } as never;

    const registryLayer = Layer.succeed(ProviderInstanceRegistry.ProviderInstanceRegistry, {
      getInstance: () => Effect.succeed(codexInstance),
      listInstances: Effect.succeed([codexInstance]),
      listUnavailable: Effect.succeed([]),
      streamChanges: Stream.empty,
      subscribeChanges: Effect.succeed({} as never),
    } as unknown as ProviderInstanceRegistry.ProviderInstanceRegistry["Service"]);

    const workClientLayer = Layer.succeed(ProjectServiceWorkClient.ProjectServiceWorkClient, {
      listProjects: () => Effect.succeed(serviceProjects),
      getProjectGeneration: () => Effect.succeed(7),
      listPositions: () => Effect.succeed([]),
      listMy: () =>
        runsFailure !== null
          ? Effect.fail(runsFailure)
          : Effect.succeed(
              Array.from({ length: openRunCount }, (_, index) => ({
                runId: `run_${index + 1}`,
                positionId: "pos_1",
                runRevision: 1,
                state: "open",
                task: { prompt: `work item ${index + 1}` },
                createdAt: `2026-08-21T00:00:${String(10 + index).padStart(2, "0")}Z`,
              })),
            ),
      getRun: () => Effect.succeed(null),
      submitRun: () => Effect.succeed(null),
      getOperation: () => Effect.succeed(null),
    } as unknown as ProjectServiceWorkClient.ProjectServiceWorkClient["Service"]);

    // In-memory session-route ledger: the persisted work-group→thread
    // association the delivery path writes and the mission-ended settlement
    // reads (the flow-finalization ledger's drivers are deleted, Phase 7).
    const sessionRoutes = new Map<
      string,
      ProjectWorkSessionRouteStore.ProjectWorkSessionRouteRecord
    >();
    const sessionRouteStoreLayer = Layer.succeed(
      ProjectWorkSessionRouteStore.ProjectFlowSessionRouteStore,
      {
        record: (input: ProjectWorkSessionRouteStore.RecordProjectWorkSessionRouteInput) =>
          Effect.sync(() => {
            sessionRoutes.set(`${input.instanceId}\n${input.agentId}`, input);
          }),
        find: (input: ProjectWorkSessionRouteStore.FindProjectWorkSessionRouteInput) =>
          Effect.succeed(sessionRoutes.get(`${input.instanceId}\n${input.agentId}`) ?? null),
      } as unknown as ProjectWorkSessionRouteStore.ProjectFlowSessionRouteStore["Service"],
    );

    const configLayer = Layer.fresh(
      ServerConfig.layerTest(process.cwd(), { prefix: "t3code-ps-consumer-runtime-" }),
    );
    const secretStoreLayer = ServerSecretStore.layer.pipe(Layer.provideMerge(configLayer));
    const settingsLayer = ServerSettingsModule.layer.pipe(Layer.provideMerge(secretStoreLayer));

    const serviceLayer = ProjectConsumerRuntime.layerWithOptions({
      socketFactory: gateway.factory,
      backoff: () => 10,
      revivalDelayMs: 20,
      ...(options?.reconcileSweepIntervalMs !== undefined
        ? { reconcileSweepIntervalMs: options.reconcileSweepIntervalMs }
        : {}),
    }).pipe(
      Layer.provideMerge(engineLayer),
      Layer.provideMerge(snapshotQueryLayer),
      Layer.provideMerge(workspacePathsLayer),
      Layer.provideMerge(registryLayer),
      Layer.provideMerge(workClientLayer),
      Layer.provideMerge(sessionRouteStoreLayer),
      Layer.provideMerge(settingsLayer),
      // Platform services folded in so each it.live test provides one layer.
      Layer.provideMerge(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer)),
    );

    return {
      commands,
      threads,
      eventQueue,
      openRunCount: () => openRunCount,
      setOpenRunCount: (count: number) => {
        openRunCount = count;
      },
      setServiceProjects: (list: ProjectServiceWorkClient.ProjectServiceProjectRecord[] | null) => {
        serviceProjects = list ?? [];
      },
      failRuns: (failure: ProjectServiceWorkClient.ProjectServiceWorkClientError | null) => {
        runsFailure = failure;
      },
      sessionSetEvent,
      layer: serviceLayer,
    };
  });

// Shared setup: enabled client + credential + one project-enabled agent.
const configureIntegration = Effect.gen(function* () {
  const serverSettings = yield* ServerSettingsModule.ServerSettingsService;
  yield* serverSettings.updateSettings({
    projectServiceClient: { enabled: true, newCredential: CREDENTIAL },
  });
  const agents: Record<string, LogicalAgentConfig> = {
    [LogicalAgentId.make(AGENT_ID)]: {
      agentName: "Build agent",
      providerInstanceId: ProviderInstanceId.make("codex"),
      persona: "",
      thinkLevel: null,
      modelOverride: null,
      project: { enabled: true, sessionScope: "project", sessionRetention: "settle" },
    },
  };
  yield* serverSettings.updateSettings({
    logicalAgents: agents as never,
  });
});

const turnStartCommands = (commands: ReadonlyArray<OrchestrationCommand>) =>
  commands.filter((command) => command.type === "thread.turn.start");
const threadCreateCommands = (commands: ReadonlyArray<OrchestrationCommand>) =>
  commands.filter((command) => command.type === "thread.create");
const projectCreateCommands = (commands: ReadonlyArray<OrchestrationCommand>) =>
  commands.filter((command) => command.type === "project.create");

describe("ProjectConsumerRuntimeService", () => {
  it.live("disabled integration: no connection is made", () =>
    Effect.gen(function* () {
      const gateway = makeGateway();
      const fakes = yield* makeFakes(gateway);

      yield* Effect.gen(function* () {
        const service = yield* ProjectConsumerRuntime.ProjectConsumerRuntimeService;
        yield* service.start();
        assert.isEmpty(gateway.recording.sockets);
        const status = yield* service.getStatus;
        assert.strictEqual(status.state, "disabled");
        assert.isDefined(status.detail);
      }).pipe(Effect.provide(fakes.layer), Effect.scoped);
    }),
  );

  it.live(
    "enabled client: one connection, derived ws URL, credential in hello, live inventory",
    () =>
      Effect.gen(function* () {
        const gateway = makeGateway();
        const fakes = yield* makeFakes(gateway);

        yield* Effect.gen(function* () {
          yield* configureIntegration;
          const service = yield* ProjectConsumerRuntime.ProjectConsumerRuntimeService;
          yield* service.start();
          const socket = gateway.recording.sockets[0];
          assert.isDefined(socket);
          socket?.fireOpen();
          yield* waitForSdk(40);

          // Derived from the local base URL: the gateway upgrade path.
          const hello = gateway.recording.hellos[0];
          assert.isDefined(hello);
          assert.match(hello?.url ?? "", /ws:\/\/127\.0\.0\.1:7600\/project\/v1\/consumer\/ws$/);
          // The credential rides the hello and nowhere else.
          assert.strictEqual(hello?.credential, CREDENTIAL);
          // Advertised inventory: stable id + display name of enabled agents.
          assert.deepEqual(hello?.agents, [{ agentId: AGENT_ID, displayName: "Build agent" }]);

          // Inventory requests are answered live from settings (the SDK's
          // re-advertisement contract — no reconnect needed).
          gateway.requestInventory(socket as FakeSocket);
          yield* waitForSdk(40);
          const snapshot = (socket as FakeSocket)
            .sentMessages()
            .find((message) => message.type === "inventory.snapshot");
          assert.isDefined(snapshot);
          assert.strictEqual(
            snapshot?.type === "inventory.snapshot" && String(snapshot.payload.agents[0]?.agentId),
            AGENT_ID,
          );

          assert.strictEqual(
            yield* service.getStatus.pipe(Effect.map((status) => status.state)),
            "connected",
          );
        }).pipe(Effect.provide(fakes.layer), Effect.scoped);
      }),
  );

  it.live(
    "missionsEnabled gate (dual state): off omits mission.v1 from the hello + rejects a mission frame; on re-syncs, declares, and settles a mission.ended notice",
    () =>
      Effect.gen(function* () {
        const gateway = makeGateway();
        const fakes = yield* makeFakes(gateway);

        yield* Effect.gen(function* () {
          const serverSettings = yield* ServerSettingsModule.ServerSettingsService;
          yield* configureIntegration;
          // work-mission-v5 Phase 7: the gate default is ON — the off-arm now
          // pins the explicit off-switch instead of relying on the absent key.
          yield* serverSettings.updateSettings({
            projectServiceClient: { missionsEnabled: false },
          });
          const service = yield* ProjectConsumerRuntime.ProjectConsumerRuntimeService;
          yield* service.start();

          // GATE OFF (explicit): the SDK 0.14 registry carries mission.v1,
          // so the gate's job is to FILTER it out of the advertised set.
          let socket = gateway.recording.sockets[0];
          assert.isDefined(socket);
          socket?.fireOpen();
          yield* waitForSdk(40);
          const offHello = gateway.recording.hellos[0];
          assert.isDefined(offHello);
          assert.notInclude(offHello?.capabilities, CONSUMER_CAPABILITY_MISSION_V1);
          // The rest of the SDK registry still rides the hello.
          assert.include(offHello?.capabilities, "work.available.v1");

          // A mission frame on a gated-off connection is the T3-side signature
          // gate: rejected as not-dispatchable (never ACKed) so the Project
          // Service redelivers — a mismatched server or a mid-connection flip
          // cannot silently drop the notice.
          const gatedOffNoticeId = "mne_" + "a".repeat(32);
          (socket as FakeSocket).receive({
            type: "mission.ended",
            id: newMessageId(),
            sentAt: ISO,
            payload: {
              noticeId: gatedOffNoticeId,
              missionId: "ms_" + "0".repeat(32),
              group: "ms_" + "0".repeat(32),
              disposition: "completed",
              outcome: null,
              workspacePolicy: "project-root",
            },
          });
          yield* waitForSdk(40);
          assert.include(
            gateway.recording.failures.map((f) => f.noticeId),
            gatedOffNoticeId,
          );
          assert.include(
            gateway.recording.failures.find((f) => f.noticeId === gatedOffNoticeId)?.code,
            "AGENT_NOT_DISPATCHABLE",
          );
          assert.notInclude(gateway.recording.acks, gatedOffNoticeId);

          // GATE ON: flipping the setting re-syncs the single runtime (the
          // signature carries the gate) and the rebuilt hello DECLARES the
          // mission.v1 capability.
          yield* serverSettings.updateSettings({
            projectServiceClient: { missionsEnabled: true },
          });
          yield* waitForSdk(60);
          socket = gateway.recording.sockets[1] ?? socket;
          (socket as FakeSocket).fireOpen();
          yield* waitForSdk(40);
          const onHello = gateway.recording.hellos[1];
          assert.isDefined(onHello);
          assert.include(onHello?.capabilities, CONSUMER_CAPABILITY_MISSION_V1);

          // The declared connection settles a mission.ended notice: the SDK's
          // onMissionEnded runs the settlement (empty plan — no sessions
          // recorded for this mission) and the notice is ACKed.
          const settledNoticeId = "mne_" + "b".repeat(32);
          (socket as FakeSocket).receive({
            type: "mission.ended",
            id: newMessageId(),
            sentAt: ISO,
            payload: {
              noticeId: settledNoticeId,
              missionId: "ms_" + "0".repeat(32),
              group: "ms_" + "0".repeat(32),
              disposition: "completed",
              outcome: null,
              workspacePolicy: "project-root",
            },
          });
          yield* waitForSdk(60);
          assert.include(gateway.recording.acks, settledNoticeId);
          assert.notInclude(
            gateway.recording.failures.map((f) => f.noticeId),
            settledNoticeId,
          );
        }).pipe(Effect.provide(fakes.layer), Effect.scoped);
      }),
  );

  it.live("notice auto-creates the workspace project, routes a session, and ACKs routing", () =>
    Effect.gen(function* () {
      const gateway = makeGateway();
      const fakes = yield* makeFakes(gateway);

      yield* Effect.gen(function* () {
        yield* configureIntegration;
        const service = yield* ProjectConsumerRuntime.ProjectConsumerRuntimeService;
        yield* service.start();
        const socket = gateway.recording.sockets[0];
        assert.isDefined(socket);
        socket?.fireOpen();
        yield* waitForSdk(40);

        gateway.sendWorkAvailable(socket as FakeSocket, { runId: "run_1" });
        yield* settleWake();

        // Issue #6: the notice's workspace directory auto-created the T3
        // project (notice name as the title) and the session lives under it.
        const project = projectCreateCommands(fakes.commands)[0];
        assert.isDefined(project);
        assert.strictEqual(
          project?.type === "project.create" && project.workspaceRoot,
          WORKSPACE_DIR,
        );
        assert.strictEqual(project?.type === "project.create" && project.title, "Registry");
        const t3ProjectId = project?.type === "project.create" ? String(project.projectId) : "";
        assert.strictEqual(
          threadCreateCommands(fakes.commands)[0]?.type === "thread.create" &&
            String(threadCreateCommands(fakes.commands)[0]?.projectId),
          t3ProjectId,
        );

        const turn = turnStartCommands(fakes.commands)[0];
        assert.strictEqual(
          turn?.type === "thread.turn.start" && turn.message.text,
          "Your current work: work item 1. 1 more item waiting behind it. Use the t3-code Agent Project tools (project_work_list, project_doc_read, project_doc_write, project_doc_edit, and project_work_submit) to inspect and complete the current work first. Do not use human PS Control tools for Agent Work.",
        );

        // ACK means routed: exactly one, after the wake settled.
        assert.deepEqual(gateway.recording.acks, [
          workAvailableNoticeId(PS_PROJECT_ID, "run_1", 1),
        ]);
        assert.isEmpty(gateway.recording.failures);
        const sessions = yield* service.router.snapshotSessions;
        assert.lengthOf(sessions, 1);
      }).pipe(Effect.provide(fakes.layer), Effect.scoped);
    }),
  );

  it.live("a notice whose directory is missing on disk fails with a routing code", () =>
    Effect.gen(function* () {
      const gateway = makeGateway();
      const fakes = yield* makeFakes(gateway);

      yield* Effect.gen(function* () {
        yield* configureIntegration;
        yield* ProjectConsumerRuntime.ProjectConsumerRuntimeService.pipe(
          Effect.flatMap((service) => service.start()),
        );
        const socket = gateway.recording.sockets[0];
        assert.isDefined(socket);
        socket?.fireOpen();
        yield* waitForSdk(40);

        // The Project Service points at a directory this machine does not
        // have: the wake fails AGENT_NOT_DISPATCHABLE with the directory in
        // the detail; no project or session is created.
        socket?.receive(
          {
            type: "work.available",
            id: newMessageId(),
            sentAt: ISO,
            payload: {
              noticeId: workAvailableNoticeId(PS_PROJECT_ID, "run_404", 1),
              project: {
                projectId: PS_PROJECT_ID,
                projectName: "Nowhere",
                workspaceDir: "/tmp/does-not-exist",
              },
              agent: { agentId: AGENT_ID, agentName: "Build agent" },
              positionId: "pos_1",
              runId: "run_404",
              occupancyRevision: 1,
              runRevision: 1,
              openedAt: ISO,
            } as never,
          },
          4,
        );
        yield* settleWake();

        assert.isEmpty(projectCreateCommands(fakes.commands));
        assert.isEmpty(threadCreateCommands(fakes.commands));
        assert.deepEqual(gateway.recording.failures, [
          {
            noticeId: workAvailableNoticeId(PS_PROJECT_ID, "run_404", 1),
            code: "AGENT_NOT_DISPATCHABLE",
          },
        ]);
      }).pipe(Effect.provide(fakes.layer), Effect.scoped);
    }),
  );

  it.live("a pre-V4 notice without workspaceDir fails with the V4 requirement", () =>
    Effect.gen(function* () {
      const gateway = makeGateway();
      const fakes = yield* makeFakes(gateway);

      yield* Effect.gen(function* () {
        yield* configureIntegration;
        yield* ProjectConsumerRuntime.ProjectConsumerRuntimeService.pipe(
          Effect.flatMap((service) => service.start()),
        );
        const socket = gateway.recording.sockets[0];
        assert.isDefined(socket);
        socket?.fireOpen();
        yield* waitForSdk(40);

        // A V3-shaped notice (no workspaceDir field): routing must name the
        // protocol requirement instead of guessing a project.
        socket?.receive(
          {
            type: "work.available",
            id: newMessageId(),
            sentAt: ISO,
            payload: {
              noticeId: workAvailableNoticeId(PS_PROJECT_ID, "run_v3", 1),
              project: { projectId: PS_PROJECT_ID, projectName: "Registry" },
              agent: { agentId: AGENT_ID, agentName: "Build agent" },
              positionId: "pos_1",
              runId: "run_v3",
              occupancyRevision: 1,
              runRevision: 1,
              openedAt: ISO,
            } as never,
          },
          3,
        );
        yield* settleWake();

        assert.isEmpty(projectCreateCommands(fakes.commands));
        assert.lengthOf(gateway.recording.failures, 1);
        assert.strictEqual(gateway.recording.failures[0]?.code, "AGENT_NOT_DISPATCHABLE");
      }).pipe(Effect.provide(fakes.layer), Effect.scoped);
    }),
  );

  it.live("replayed noticeId does not wake twice; a busy session coalesces distinct notices", () =>
    Effect.gen(function* () {
      const gateway = makeGateway();
      const fakes = yield* makeFakes(gateway);

      yield* Effect.gen(function* () {
        yield* configureIntegration;
        yield* ProjectConsumerRuntime.ProjectConsumerRuntimeService.pipe(
          Effect.flatMap((service) => service.start()),
        );
        const socket = gateway.recording.sockets[0];
        assert.isDefined(socket);
        socket?.fireOpen();
        yield* waitForSdk(40);

        gateway.sendWorkAvailable(socket as FakeSocket, { runId: "run_1" });
        yield* settleWake();

        // Same deterministic noticeId again on the SAME channel: the SDK
        // ACKs again without re-waking.
        gateway.sendWorkAvailable(socket as FakeSocket, { runId: "run_1" });
        yield* settleWake();
        // A different run is a NEW notice; its session is busy (the first
        // aggregate's turn is running), so it is only recorded.
        gateway.sendWorkAvailable(socket as FakeSocket, { runId: "run_2" });
        yield* settleWake();

        assert.lengthOf(projectCreateCommands(fakes.commands), 1);
        assert.lengthOf(threadCreateCommands(fakes.commands), 1);
        assert.lengthOf(turnStartCommands(fakes.commands), 1);
        assert.isEmpty(gateway.recording.failures);
      }).pipe(Effect.provide(fakes.layer), Effect.scoped);
    }),
  );

  it.live("reconnect replays do not multiply notifications", () =>
    Effect.gen(function* () {
      const gateway = makeGateway();
      const fakes = yield* makeFakes(gateway);

      yield* Effect.gen(function* () {
        yield* configureIntegration;
        yield* ProjectConsumerRuntime.ProjectConsumerRuntimeService.pipe(
          Effect.flatMap((service) => service.start()),
        );
        const first = gateway.recording.sockets[0];
        assert.isDefined(first);
        first?.fireOpen();
        yield* waitForSdk(40);

        gateway.sendWorkAvailable(first as FakeSocket, { runId: "run_1" });
        yield* settleWake();
        assert.lengthOf(turnStartCommands(fakes.commands), 1);

        // The channel drops; the SDK reconnects (10ms backoff) and Project
        // Service replays the same deterministic notice.
        first?.fireClose();
        yield* waitForSdk(80);
        const second = gateway.recording.sockets[1];
        assert.isDefined(second);
        second?.fireOpen();
        yield* waitForSdk(40);

        gateway.sendWorkAvailable(second as FakeSocket, { runId: "run_1" });
        yield* settleWake();

        // Same runtime instance: the routed set deduplicated the replay —
        // no second wake, no second aggregate, no re-created project.
        assert.lengthOf(turnStartCommands(fakes.commands), 1);
        assert.lengthOf(threadCreateCommands(fakes.commands), 1);
        assert.lengthOf(projectCreateCommands(fakes.commands), 1);
      }).pipe(Effect.provide(fakes.layer), Effect.scoped);
    }),
  );

  it.live(
    "a severed connection re-dials and re-hellos with the existing backoff — never terminal",
    () =>
      Effect.gen(function* () {
        const gateway = makeGateway();
        const fakes = yield* makeFakes(gateway);

        yield* Effect.gen(function* () {
          yield* configureIntegration;
          const service = yield* ProjectConsumerRuntime.ProjectConsumerRuntimeService;
          yield* service.start();
          const first = gateway.recording.sockets[0];
          assert.isDefined(first);
          first?.fireOpen();
          yield* waitForSdk(40);
          assert.lengthOf(gateway.recording.hellos, 1);
          assert.strictEqual(
            yield* service.getStatus.pipe(Effect.map((status) => status.state)),
            "connected",
          );

          // The Project Service dies: the server side of the live socket
          // drops. A plain connection loss must re-dial on the runtime's own
          // backoff (10ms here) — no terminal classification.
          first?.fireClose();
          yield* waitForSdk(60);

          // The 10ms backoff has already re-dialed: a fresh socket exists and
          // the runtime is mid-handshake, never stopped.
          assert.strictEqual(
            yield* service.getStatus.pipe(Effect.map((status) => status.state)),
            "connecting",
          );
          const second = gateway.recording.sockets[1];
          assert.isDefined(second);

          // The re-dial converges: fresh hello, fresh welcome, connected.
          second?.fireOpen();
          yield* waitForSdk(40);
          assert.lengthOf(gateway.recording.hellos, 2);
          assert.strictEqual(gateway.recording.hellos[1]?.credential, CREDENTIAL);
          assert.strictEqual(
            yield* service.getStatus.pipe(Effect.map((status) => status.state)),
            "connected",
          );
        }).pipe(Effect.provide(fakes.layer), Effect.scoped);
      }),
  );

  it.live(
    "a runtime that stopped itself on a terminal classification is revived and re-hellos",
    () =>
      Effect.gen(function* () {
        const gateway = makeGateway();
        const fakes = yield* makeFakes(gateway);

        yield* Effect.gen(function* () {
          yield* configureIntegration;
          const service = yield* ProjectConsumerRuntime.ProjectConsumerRuntimeService;
          yield* service.start();
          const first = gateway.recording.sockets[0];
          assert.isDefined(first);
          first?.fireOpen();
          yield* waitForSdk(40);
          assert.lengthOf(gateway.recording.hellos, 1);

          // The gateway answers a terminal incompatibility: the SDK closes
          // ITSELF (state stopped) — the class of self-stop that used to
          // strand the integration with ZERO connections until restart.
          gateway.sendServerError(
            first as FakeSocket,
            "CONSUMER_PROTOCOL_INCOMPATIBLE",
            "server cannot speak this protocol",
          );
          yield* waitForSdk(10);
          assert.isTrue((first as FakeSocket).closed);
          assert.strictEqual(
            yield* service.getStatus.pipe(Effect.map((status) => status.state)),
            "stopped",
          );
          assert.strictEqual(
            (yield* service.getStatus).lastServerError?.code,
            "CONSUMER_PROTOCOL_INCOMPATIBLE",
          );

          // The host's revival re-sync force-rebuilds the runtime (here
          // after the shortened 20ms delay): a fresh dial + hello converges
          // once the relaunched server speaks again.
          yield* waitForSdk(80);
          const second = gateway.recording.sockets[1];
          assert.isDefined(second);
          second?.fireOpen();
          yield* waitForSdk(40);
          assert.lengthOf(gateway.recording.hellos, 2);
          assert.strictEqual(
            yield* service.getStatus.pipe(Effect.map((status) => status.state)),
            "connected",
          );
        }).pipe(Effect.provide(fakes.layer), Effect.scoped);
      }),
  );

  it.live("restart delivers one new aggregate for a still-pending replayed notice", () =>
    Effect.gen(function* () {
      const gateway = makeGateway();
      const fakes = yield* makeFakes(gateway);

      // Instance A routes the notice, then the process restarts.
      yield* Effect.gen(function* () {
        yield* configureIntegration;
        yield* ProjectConsumerRuntime.ProjectConsumerRuntimeService.pipe(
          Effect.flatMap((service) => service.start()),
        );
        const socket = gateway.recording.sockets[0];
        assert.isDefined(socket);
        socket?.fireOpen();
        yield* waitForSdk(40);
        gateway.sendWorkAvailable(socket as FakeSocket, { runId: "run_1" });
        yield* settleWake();
        assert.lengthOf(turnStartCommands(fakes.commands), 1);
      }).pipe(Effect.provide(fakes.layer), Effect.scoped);

      // Instance B (fresh runtime AND fresh routing state) receives the
      // replay of the still-pending notice: it reuses the persisted project
      // (no second create) and delivers exactly ONE aggregate — the
      // pre-restart session is left untouched, nothing multiplies.
      yield* Effect.gen(function* () {
        yield* configureIntegration;
        yield* ProjectConsumerRuntime.ProjectConsumerRuntimeService.pipe(
          Effect.flatMap((service) => service.start()),
        );
        const socket = gateway.recording.sockets.at(-1);
        assert.isDefined(socket);
        socket?.fireOpen();
        yield* waitForSdk(40);
        gateway.sendWorkAvailable(socket as FakeSocket, { runId: "run_1" });
        yield* settleWake();

        assert.lengthOf(projectCreateCommands(fakes.commands), 1);
        assert.lengthOf(threadCreateCommands(fakes.commands), 2);
        assert.lengthOf(turnStartCommands(fakes.commands), 2);
      }).pipe(Effect.provide(fakes.layer), Effect.scoped);
    }),
  );

  it.live("the sweep recovers open work stranded by a restart without any replayed notice", () =>
    Effect.gen(function* () {
      const gateway = makeGateway();
      const fakes = yield* makeFakes(gateway);
      fakes.setServiceProjects([
        { projectId: PS_PROJECT_ID, name: "Registry", workspaceDir: WORKSPACE_DIR },
      ]);

      // Instance A routes a notice, then the process restarts.
      yield* Effect.gen(function* () {
        yield* configureIntegration;
        yield* ProjectConsumerRuntime.ProjectConsumerRuntimeService.pipe(
          Effect.flatMap((service) => service.start()),
        );
        const socket = gateway.recording.sockets[0];
        assert.isDefined(socket);
        socket?.fireOpen();
        yield* waitForSdk(40);
        gateway.sendWorkAvailable(socket as FakeSocket, { runId: "run_1" });
        yield* settleWake();
        assert.lengthOf(turnStartCommands(fakes.commands), 1);
      }).pipe(Effect.provide(fakes.layer), Effect.scoped);

      // Instance B: fresh routing registry, and the notice never replays
      // (Project Service already ACKed it). Reaching connected triggers the
      // reconcile sweep, which finds the still-open work uncovered by any
      // session and delivers exactly one synthetic wake.
      yield* Effect.gen(function* () {
        yield* configureIntegration;
        yield* ProjectConsumerRuntime.ProjectConsumerRuntimeService.pipe(
          Effect.flatMap((service) => service.start()),
        );
        const socket = gateway.recording.sockets.at(-1);
        assert.isDefined(socket);
        socket?.fireOpen();
        yield* waitForSdk(60);
        yield* settleWake();

        assert.lengthOf(projectCreateCommands(fakes.commands), 1);
        assert.lengthOf(threadCreateCommands(fakes.commands), 2);
        assert.lengthOf(turnStartCommands(fakes.commands), 2);

        // No-nag across a reconnect: the sweep re-runs on the fresh
        // connection, finds the delivered head, and stays quiet.
        (gateway.recording.sockets.at(-1) as FakeSocket).fireClose();
        yield* waitForSdk(80);
        const redialed = gateway.recording.sockets.at(-1);
        assert.isDefined(redialed);
        redialed?.fireOpen();
        yield* waitForSdk(60);
        yield* settleWake();
        assert.lengthOf(turnStartCommands(fakes.commands), 2);
      }).pipe(Effect.provide(fakes.layer), Effect.scoped);
    }),
  );

  it.live("the periodic sweep recovers work that strands between connections", () =>
    Effect.gen(function* () {
      const gateway = makeGateway();
      const fakes = yield* makeFakes(gateway, { reconcileSweepIntervalMs: 40 });

      yield* Effect.gen(function* () {
        yield* configureIntegration;
        yield* ProjectConsumerRuntime.ProjectConsumerRuntimeService.pipe(
          Effect.flatMap((service) => service.start()),
        );
        const socket = gateway.recording.sockets[0];
        assert.isDefined(socket);
        socket?.fireOpen();
        yield* waitForSdk(40);

        // Connected, but the Project Service project list is empty: the
        // on-connect sweep has nothing to reconcile.
        assert.isEmpty(turnStartCommands(fakes.commands));

        // The project (with open work behind it) appears later — as after a
        // stranded busy-coalesce whose notice never re-fires. No new
        // connection event arrives; only the periodic tick can recover it.
        fakes.setServiceProjects([
          { projectId: PS_PROJECT_ID, name: "Registry", workspaceDir: WORKSPACE_DIR },
        ]);
        yield* waitForSdk(160);

        assert.lengthOf(projectCreateCommands(fakes.commands), 1);
        assert.lengthOf(threadCreateCommands(fakes.commands), 1);
        assert.lengthOf(turnStartCommands(fakes.commands), 1);
        // Later ticks do not multiply the delivery.
        yield* waitForSdk(120);
        assert.lengthOf(turnStartCommands(fakes.commands), 1);
      }).pipe(Effect.provide(fakes.layer), Effect.scoped);
    }),
  );

  it.live("busy session delivers one coalesced aggregate after the turn", () =>
    Effect.gen(function* () {
      const gateway = makeGateway();
      const fakes = yield* makeFakes(gateway);

      yield* Effect.gen(function* () {
        yield* configureIntegration;
        yield* ProjectConsumerRuntime.ProjectConsumerRuntimeService.pipe(
          Effect.flatMap((service) => service.start()),
        );
        const socket = gateway.recording.sockets[0];
        assert.isDefined(socket);
        socket?.fireOpen();
        yield* waitForSdk(40);

        gateway.sendWorkAvailable(socket as FakeSocket, { runId: "run_1" });
        yield* settleWake();

        const created = threadCreateCommands(fakes.commands)[0];
        const threadId = created?.type === "thread.create" ? (created.threadId as string) : null;
        assert.isNotNull(threadId);

        // The aggregate turn engaged (running) before more work arrives.
        yield* Queue.offer(fakes.eventQueue, fakes.sessionSetEvent(threadId as string, "running"));
        yield* waitForSdk(20);

        // More work arrives while busy: recorded, no interrupt.
        gateway.sendWorkAvailable(socket as FakeSocket, { runId: "run_2" });
        yield* settleWake();
        assert.lengthOf(turnStartCommands(fakes.commands), 1);

        // The turn finishes: the engine event drives ONE coalesced aggregate,
        // refreshed from the authoritative count.
        fakes.threads.set(threadId as string, {
          ...threadShellFor(threadId as string),
          session: sessionWithStatus(threadId as string, "ready"),
        });
        yield* Queue.offer(fakes.eventQueue, fakes.sessionSetEvent(threadId as string, "ready"));
        yield* waitForSdk(40);

        const turns = turnStartCommands(fakes.commands);
        assert.lengthOf(turns, 2);
        assert.strictEqual(
          turns[1]?.type === "thread.turn.start" && turns[1]?.message.text,
          `Your current work: work item 1. ${
            fakes.openRunCount() - 1
          } more item${fakes.openRunCount() - 1 === 1 ? "" : "s"} waiting behind it. Use the t3-code Agent Project tools (project_work_list, project_doc_read, project_doc_write, project_doc_edit, and project_work_submit) to inspect and complete the current work first. Do not use human PS Control tools for Agent Work.`,
        );
        assert.isEmpty(gateway.recording.failures);
      }).pipe(Effect.provide(fakes.layer), Effect.scoped);
    }),
  );

  it.live("credential failure preserves the current session and reports a failure code", () =>
    Effect.gen(function* () {
      const gateway = makeGateway();
      const fakes = yield* makeFakes(gateway);

      yield* Effect.gen(function* () {
        yield* configureIntegration;
        const service = yield* ProjectConsumerRuntime.ProjectConsumerRuntimeService;
        yield* service.start();
        const socket = gateway.recording.sockets[0];
        assert.isDefined(socket);
        socket?.fireOpen();
        yield* waitForSdk(40);

        gateway.sendWorkAvailable(socket as FakeSocket, { runId: "run_1" });
        yield* settleWake();
        const sessionsBefore = yield* service.router.snapshotSessions;
        assert.lengthOf(sessionsBefore, 1);

        // The notification turn engaged and finished; the unchanged head then
        // drew the one A3 reminder, whose turn must also settle before the
        // session is idle again — then the credential stops working between
        // notices.
        const threadId = sessionsBefore[0]?.threadId as string;
        yield* Queue.offer(fakes.eventQueue, fakes.sessionSetEvent(threadId, "running"));
        yield* waitForSdk(20);
        fakes.threads.set(threadId, {
          ...threadShellFor(threadId),
          session: sessionWithStatus(threadId, "ready"),
        });
        yield* Queue.offer(fakes.eventQueue, fakes.sessionSetEvent(threadId, "ready"));
        yield* waitForSdk(20);
        yield* Queue.offer(fakes.eventQueue, fakes.sessionSetEvent(threadId, "running"));
        yield* waitForSdk(20);
        fakes.threads.set(threadId, {
          ...threadShellFor(threadId),
          session: sessionWithStatus(threadId, "ready"),
        });
        yield* Queue.offer(fakes.eventQueue, fakes.sessionSetEvent(threadId, "ready"));
        yield* waitForSdk(20);
        fakes.failRuns(
          new ProjectServiceWorkClient.ProjectServiceWorkServiceRejectedError({
            code: "PROJECT_CLIENT_AUTHENTICATION_INVALID",
            status: 401,
            message: "credential rejected",
          }),
        );

        gateway.sendWorkAvailable(socket as FakeSocket, { runId: "run_9" });
        yield* settleWake();

        // Routing failed with a protocol failure code; the session survived.
        assert.lengthOf(gateway.recording.failures, 1);
        assert.strictEqual(gateway.recording.failures[0]?.code, "CONSUMER_INTERNAL");
        const sessionsAfter = yield* service.router.snapshotSessions;
        assert.deepEqual(
          sessionsAfter.map((session) => session.threadId),
          sessionsBefore.map((session) => session.threadId),
        );
      }).pipe(Effect.provide(fakes.layer), Effect.scoped);
    }),
  );

  it.live("disabling the integration closes the single connection", () =>
    Effect.gen(function* () {
      const gateway = makeGateway();
      const fakes = yield* makeFakes(gateway);

      yield* Effect.gen(function* () {
        yield* configureIntegration;
        const service = yield* ProjectConsumerRuntime.ProjectConsumerRuntimeService;
        yield* service.start();
        const socket = gateway.recording.sockets[0];
        assert.isDefined(socket);
        socket?.fireOpen();
        yield* waitForSdk(40);
        assert.strictEqual(
          yield* service.getStatus.pipe(Effect.map((status) => status.state)),
          "connected",
        );

        const serverSettings = yield* ServerSettingsModule.ServerSettingsService;
        yield* serverSettings.updateSettings({ projectServiceClient: { enabled: false } });
        yield* waitForSdk(40);

        assert.isTrue((socket as FakeSocket).closed);
        const status = yield* service.getStatus;
        assert.strictEqual(status.state, "disabled");
        // One socket was ever created for this client.
        assert.lengthOf(gateway.recording.sockets, 1);
      }).pipe(Effect.provide(fakes.layer), Effect.scoped);
    }),
  );

  it.live("credential rotation closes the old connection before exactly one new one opens", () =>
    Effect.gen(function* () {
      const gateway = makeGateway();
      const fakes = yield* makeFakes(gateway);

      yield* Effect.gen(function* () {
        yield* configureIntegration;
        const service = yield* ProjectConsumerRuntime.ProjectConsumerRuntimeService;
        yield* service.start();
        const first = gateway.recording.sockets[0];
        assert.isDefined(first);
        first?.fireOpen();
        yield* waitForSdk(40);

        // Rotating the credential reconfigures the SINGLE runtime — the
        // old connection is closed and one new one takes over.
        const serverSettings = yield* ServerSettingsModule.ServerSettingsService;
        yield* serverSettings.updateSettings({
          projectServiceClient: { enabled: true, newCredential: "psk_key-2.rotated" },
        });
        yield* waitForSdk(60);

        assert.lengthOf(gateway.recording.sockets, 2);
        assert.isTrue((first as FakeSocket).closed);
        const second = gateway.recording.sockets[1];
        assert.isDefined(second);
        second?.fireOpen();
        yield* waitForSdk(40);
        assert.isFalse((second as FakeSocket).closed);
        assert.lengthOf(gateway.recording.hellos, 2);
        assert.notStrictEqual(gateway.recording.hellos[1]?.credential, CREDENTIAL);
      }).pipe(Effect.provide(fakes.layer), Effect.scoped);
    }),
  );

  it.live("only project-enabled agents are advertised", () =>
    Effect.gen(function* () {
      const gateway = makeGateway();
      const fakes = yield* makeFakes(gateway);

      yield* Effect.gen(function* () {
        yield* configureIntegration;
        // A second agent with project work disabled must never be advertised.
        const serverSettings = yield* ServerSettingsModule.ServerSettingsService;
        yield* serverSettings.updateSettings({
          logicalAgents: {
            [LogicalAgentId.make(AGENT_ID)]: {
              agentName: "Build agent",
              providerInstanceId: ProviderInstanceId.make("codex"),
              project: { enabled: true },
            },
            [LogicalAgentId.make("ag_disabled")]: {
              agentName: "Off agent",
              providerInstanceId: ProviderInstanceId.make("codex"),
              project: { enabled: false },
            },
          } as never,
        });
        const service = yield* ProjectConsumerRuntime.ProjectConsumerRuntimeService;
        yield* service.start();
        gateway.recording.sockets[0]?.fireOpen();
        yield* waitForSdk(40);

        const hello = gateway.recording.hellos[0];
        assert.isDefined(hello);
        assert.deepEqual(
          hello?.agents.map((agent) => agent.agentId),
          [AGENT_ID],
        );
      }).pipe(Effect.provide(fakes.layer), Effect.scoped);
    }),
  );
});
