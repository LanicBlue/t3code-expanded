import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import {
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
import * as ServerSettingsModule from "../serverSettings.ts";
import * as ProjectConsumerRuntime from "./ProjectConsumerRuntimeService.ts";
import * as ProjectServiceWorkClient from "./ProjectServiceWorkClient.ts";

const CREDENTIAL = "psk_key-1.s3cret";
const AGENT_ID = "ag_one";
const PS_PROJECT_ID = "ps_proj_1";
const T3_PROJECT_ID = "t3_proj_9";
const ISO = "2026-08-14T12:00:00.000Z";

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

  receive(message: ConsumerGatewayMessage, version: 1 | 3 = 1): void {
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
        serverVersion: "0.4.0",
        protocol: { selected: 3, current: 3, minSupported: 1 },
        capabilities: [],
        client: {
          latest: "0.4.0",
          recommended: "0.4.0",
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
    /** Server → client structured work.available (V3 notice shape). */
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
            project: { projectId: PS_PROJECT_ID, projectName: "Registry" },
            agent: { agentId: AGENT_ID, agentName: "Build agent" },
            positionId: "pos_1",
            runId: input.runId,
            occupancyRevision: input.occupancyRevision ?? 1,
            runRevision: 1,
            openedAt: ISO,
          } as never,
        },
        3,
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
 * Real-time wait for the SDK's own timers (hello/notice promise chains and
 * the 10ms reconnect backoff). These tests run under `it.live` because the
 * SDK rides real timers the Effect test clock cannot drive.
 */
const waitForSdk = (ms: number): Effect.Effect<void> => Effect.sleep(Duration.millis(ms));

// ── Fake orchestration seams ─────────────────────────────────────

const threadShellFor = (threadId: string): OrchestrationThreadShell => ({
  id: ThreadId.make(threadId),
  projectId: ProjectId.make(T3_PROJECT_ID),
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

const makeFakes = (gateway: ReturnType<typeof makeGateway>) =>
  Effect.gen(function* () {
    const commands: OrchestrationCommand[] = [];
    const threads = new Map<string, OrchestrationThreadShell>();
    const eventQueue = yield* Queue.unbounded<OrchestrationEvent>();
    let openRunCount = 2;
    let runsFailure: ProjectServiceWorkClient.ProjectServiceWorkClientError | null = null;

    // Dispatched commands become visible thread state, mirroring the
    // projection (a notification turn.start makes the session busy).
    const dispatch = (command: OrchestrationCommand) =>
      Effect.sync(() => {
        commands.push(command);
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
      getThreadShellById: (threadId: ThreadId) =>
        Effect.succeed(
          threads.has(threadId)
            ? Option.some(threads.get(threadId) as OrchestrationThreadShell)
            : Option.none(),
        ),
      getProjectShellById: () =>
        Effect.succeed(
          Option.some({
            id: ProjectId.make(T3_PROJECT_ID),
            title: "Registry",
            workspaceRoot: "/tmp/registry",
            defaultModelSelection: null,
            scripts: [],
            createdAt: ISO,
            updatedAt: ISO,
          }),
        ),
    } as unknown as ProjectionSnapshotQuery.ProjectionSnapshotQuery["Service"]);

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
    } as ProviderInstanceRegistry.ProviderInstanceRegistry["Service"]);

    const workClientLayer = Layer.succeed(ProjectServiceWorkClient.ProjectServiceWorkClient, {
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
                task: {},
                createdAt: ISO,
              })),
            ),
      getRun: () => Effect.succeed(null),
      submitRun: () => Effect.succeed(null),
      getOperation: () => Effect.succeed(null),
    } as unknown as ProjectServiceWorkClient.ProjectServiceWorkClient["Service"]);

    const configLayer = Layer.fresh(
      ServerConfig.layerTest(process.cwd(), { prefix: "t3code-ps-consumer-runtime-" }),
    );
    const secretStoreLayer = ServerSecretStore.layer.pipe(Layer.provideMerge(configLayer));
    const settingsLayer = ServerSettingsModule.layer.pipe(Layer.provideMerge(secretStoreLayer));

    const serviceLayer = ProjectConsumerRuntime.layerWithOptions({
      socketFactory: gateway.factory,
      backoff: () => 10,
    }).pipe(
      Layer.provideMerge(engineLayer),
      Layer.provideMerge(snapshotQueryLayer),
      Layer.provideMerge(registryLayer),
      Layer.provideMerge(workClientLayer),
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
      project: { enabled: true },
      projectBindings: [
        {
          projectId: PS_PROJECT_ID,
          projectName: "Registry",
          t3ProjectId: ProjectId.make(T3_PROJECT_ID),
        },
      ],
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

  it.live("notice routes to a created session and ACKs routing, not completion", () =>
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
        yield* Effect.promise(flush);

        const turn = turnStartCommands(fakes.commands)[0];
        assert.strictEqual(
          turn?.type === "thread.turn.start" && turn.message.text,
          "There are 2 assigned Work items waiting. Use the Project tools to inspect them.",
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
        yield* Effect.promise(flush);

        // Same deterministic noticeId again on the SAME channel: the SDK
        // ACKs again without re-waking.
        gateway.sendWorkAvailable(socket as FakeSocket, { runId: "run_1" });
        yield* Effect.promise(flush);
        // A different run is a NEW notice; its session is busy (the first
        // aggregate's turn is running), so it is only recorded.
        gateway.sendWorkAvailable(socket as FakeSocket, { runId: "run_2" });
        yield* Effect.promise(flush);

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
        yield* Effect.promise(flush);
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
        yield* Effect.promise(flush);

        // Same runtime instance: the routed set deduplicated the replay —
        // no second wake, no second aggregate.
        assert.lengthOf(turnStartCommands(fakes.commands), 1);
        assert.lengthOf(threadCreateCommands(fakes.commands), 1);
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
        yield* Effect.promise(flush);
        assert.lengthOf(turnStartCommands(fakes.commands), 1);
      }).pipe(Effect.provide(fakes.layer), Effect.scoped);

      // Instance B (fresh runtime AND fresh routing state) receives the
      // replay of the still-pending notice: it creates the next current
      // session exactly once and delivers exactly ONE aggregate — the
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
        yield* Effect.promise(flush);

        assert.lengthOf(threadCreateCommands(fakes.commands), 2);
        assert.lengthOf(turnStartCommands(fakes.commands), 2);
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
        yield* Effect.promise(flush);

        const created = threadCreateCommands(fakes.commands)[0];
        const threadId = created?.type === "thread.create" ? (created.threadId as string) : null;
        assert.isNotNull(threadId);

        // The aggregate turn engaged (running) before more work arrives.
        yield* Queue.offer(fakes.eventQueue, fakes.sessionSetEvent(threadId as string, "running"));
        yield* waitForSdk(20);

        // More work arrives while busy: recorded, no interrupt.
        gateway.sendWorkAvailable(socket as FakeSocket, { runId: "run_2" });
        yield* Effect.promise(flush);
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
          `There are ${fakes.openRunCount()} assigned Work items waiting. Use the Project tools to inspect them.`,
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
        yield* Effect.promise(flush);
        const sessionsBefore = yield* service.router.snapshotSessions;
        assert.lengthOf(sessionsBefore, 1);

        // The notification turn engaged and finished, so the session is
        // idle again — then the credential stops working between notices.
        const threadId = sessionsBefore[0]?.threadId as string;
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
        yield* Effect.promise(flush);

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
              projectBindings: [
                {
                  projectId: PS_PROJECT_ID,
                  projectName: "Registry",
                  t3ProjectId: ProjectId.make(T3_PROJECT_ID),
                },
              ],
            },
            [LogicalAgentId.make("ag_disabled")]: {
              agentName: "Off agent",
              providerInstanceId: ProviderInstanceId.make("codex"),
              project: { enabled: false },
              projectBindings: [],
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
