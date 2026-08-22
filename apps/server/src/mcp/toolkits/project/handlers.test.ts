import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import {
  EnvironmentId,
  type OrchestrationThreadShell,
  type ServerSettingsError,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";

import * as ProjectionSnapshotQuery from "../../../orchestration/Services/ProjectionSnapshotQuery.ts";
import * as ProjectServiceWorkClient from "../../../projectService/ProjectServiceWorkClient.ts";
import * as ServerSettings from "../../../serverSettings.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";
import { ProjectWorkToolkitHandlers, mapProjectServiceError } from "./handlers.ts";

/** Compare tagged errors by their own enumerable fields (schema classes carry runtime extras). */
const plainError = (error: object): Record<string, unknown> =>
  Object.fromEntries(Object.entries(error).filter(([, value]) => typeof value !== "function"));

const threadId = ThreadId.make("thread-1");
const t3ProjectId = ProjectId.make("t3-project-1");
/** The thread project's workspace root; the PS list registers the same directory. */
const WORKSPACE_ROOT = "/w/registry";

const invocationFor = (
  capabilities: ReadonlySet<McpInvocationContext.McpCapability> = new Set(["preview"]),
): McpInvocationContext.McpInvocationScope => ({
  environmentId: EnvironmentId.make("environment-1"),
  threadId,
  providerSessionId: "provider-session-1",
  providerInstanceId: ProviderInstanceId.make("codex"),
  capabilities,
  issuedAt: 1_000,
});

const threadShell: OrchestrationThreadShell = {
  id: threadId,
  projectId: t3ProjectId,
  title: "Thread",
  modelSelection: {
    instanceId: ProviderInstanceId.make("codex"),
    model: "model",
    options: [],
  },
  runtimeMode: "full-access",
  interactionMode: "default",
  branch: null,
  worktreePath: null,
  latestTurn: null,
  createdAt: "2026-08-01T00:00:00.000Z",
  updatedAt: "2026-08-01T00:00:00.000Z",
  archivedAt: null,
  settledOverride: null,
  settledAt: null,
  session: null,
  latestUserMessageAt: null,
  hasPendingApprovals: false,
  hasPendingUserInput: false,
  hasActionableProposedPlan: false,
};

const snapshotQueryLayer = Layer.succeed(ProjectionSnapshotQuery.ProjectionSnapshotQuery, {
  getCommandReadModel: () => Effect.die("unused"),
  getSnapshot: () => Effect.die("unused"),
  getShellSnapshot: () => Effect.die("unused"),
  getArchivedShellSnapshot: () => Effect.die("unused"),
  getSnapshotSequence: () => Effect.die("unused"),
  getCounts: () => Effect.die("unused"),
  getActiveProjectByWorkspaceRoot: () => Effect.die("unused"),
  getProjectShellById: (id) =>
    Effect.succeed(
      id === t3ProjectId
        ? Option.some({
            id: t3ProjectId,
            title: "Registry",
            workspaceRoot: WORKSPACE_ROOT,
            defaultModelSelection: null,
            scripts: [],
            createdAt: "2026-08-01T00:00:00.000Z",
            updatedAt: "2026-08-01T00:00:00.000Z",
          })
        : Option.none(),
    ),
  getFirstActiveThreadIdByProjectId: () => Effect.die("unused"),
  getThreadCheckpointContext: () => Effect.die("unused"),
  getFullThreadDiffContext: () => Effect.die("unused"),
  getThreadShellById: (id) =>
    Effect.succeed(id === threadId ? Option.some(threadShell) : Option.none()),
  getThreadDetailById: () => Effect.die("unused"),
  getThreadDetailSnapshot: () => Effect.die("unused"),
  searchThreads: () => Effect.die("unused"),
});

const agentSettings = (projectEnabled: boolean) => ({
  projectServiceClient: { enabled: true },
  logicalAgents: {
    ag_one: {
      agentName: "Agent One",
      providerInstanceId: ProviderInstanceId.make("codex"),
      project: { enabled: projectEnabled },
    },
  },
});

const settingsLayer = ServerSettings.ServerSettingsService.layerTest(agentSettings(true));

const COMMITTED_OPERATION = {
  status: "committed",
  operationId: "op_generated",
  kind: "work.execute",
  requestDigest: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
  result: { runId: "run_9", state: "completed" },
  revision: "run:4",
  createdAt: "2026-08-01T00:00:59.000Z",
  resolvedAt: "2026-08-01T00:01:00.000Z",
} as const;

const RUN_VIEW = {
  runId: "run_9",
  positionId: "pos_1",
  runRevision: "run:3",
  state: "open",
  agentId: "ag_one",
  task: { prompt: "Summarize" },
  createdAt: "2026-08-01T00:00:00.000Z",
} as const;

const POSITION_VIEW = {
  positionId: "pos_1",
  displayName: "Summarizer",
  assignmentRevision: "position:5",
} as const;

interface CapturedSubmit {
  readonly projectId: string;
  readonly projectGeneration: number;
  readonly runId: string;
  readonly expectedRunRevision: string;
  readonly expectedAssignmentRevision: string;
  readonly agentId: string;
  readonly result: Readonly<Record<string, unknown>>;
  readonly operationId: string;
  readonly idempotencyKey: string;
}

interface CapturedSpawn {
  readonly projectId: string;
  readonly idempotencyKey: string;
  readonly definitionId: string;
  readonly name: string;
}

const docWritesRef: { path: string; operation: string }[] = [];
let docWriteError: ProjectServiceWorkClient.ProjectServiceWorkClientError | undefined;

const makeWorkClientLayer = (overrides?: {
  readonly runs?: readonly ProjectServiceWorkClient.ProjectWorkRunRecord[];
  readonly positions?: readonly ProjectServiceWorkClient.ProjectWorkPositionRecord[];
  readonly run?: ProjectServiceWorkClient.ProjectWorkRunRecord | null;
  readonly serviceProjects?: readonly ProjectServiceWorkClient.ProjectServiceProjectRecord[];
  readonly spawnError?: ProjectServiceWorkClient.ProjectServiceWorkClientError;
  readonly spawnPending?: boolean;
  readonly documentWriteError?: ProjectServiceWorkClient.ProjectServiceWorkClientError;
}) => {
  docWriteError = overrides?.documentWriteError;
  const submitted: CapturedSubmit[] = [];
  const spawned: CapturedSpawn[] = [];
  const docWrites = docWritesRef;
  const service = ProjectServiceWorkClient.ProjectServiceWorkClient.of({
    // The directory-keyed mapping: one service project registered at the
    // thread project's workspace root.
    listProjects: () =>
      Effect.succeed(
        overrides?.serviceProjects ?? [
          { projectId: "proj_ps_1", name: "PS Project", workspaceDir: WORKSPACE_ROOT },
        ],
      ),
    getProjectGeneration: (projectId) => Effect.succeed(projectId === "proj_ps_1" ? 7 : 0),
    listMy: (input) =>
      Effect.succeed(
        (overrides?.runs ?? [RUN_VIEW]).map((run) => ({
          ...run,
          agentId: run.agentId ?? input.agentId,
        })),
      ),
    listPositions: () => Effect.succeed(overrides?.positions ?? [POSITION_VIEW]),
    getRun: () => Effect.succeed(overrides?.run === undefined ? RUN_VIEW : overrides.run),
    submitRun: (input, operation) => {
      submitted.push({
        projectId: input.projectId,
        projectGeneration: input.projectGeneration,
        runId: input.runId,
        expectedRunRevision: input.expectedRunRevision,
        expectedAssignmentRevision: input.expectedAssignmentRevision,
        agentId: input.agentId,
        result: input.result,
        operationId: operation.operationId,
        idempotencyKey: operation.idempotencyKey,
      });
      return Effect.succeed<ProjectServiceWorkClient.ProjectWorkOperationRecord>(
        COMMITTED_OPERATION,
      );
    },
    getOperation: () => Effect.succeed(COMMITTED_OPERATION),
    readFlowDocument: (input: { readonly path: string }) =>
      Effect.succeed({
        content: "decision: ship it",
        revision: "doc:1",
        displayPath: `flow://project/fi_1/${input.path}`,
        size: 16,
      }),
    writeFlowDocument: (input: { readonly path: string; readonly operation: string }) => {
      docWrites.push({ ...input });
      if (docWriteError !== undefined) {
        return Effect.fail(docWriteError);
      }
      return Effect.succeed({
        documentReceiptId: "document:" + "a".repeat(64),
        revision: "doc:2",
        displayPath: `flow://project/fi_1/${input.path}`,
      });
    },
    startFlow: (input) => {
      spawned.push({ ...input });
      if (overrides?.spawnError !== undefined) {
        return Effect.fail(overrides.spawnError);
      }
      if (overrides?.spawnPending === true) {
        return Effect.succeed<ProjectServiceWorkClient.ProjectFlowSpawnOutcome>({
          status: "pending",
          operationId: "rest-flow-spawn-pending",
        });
      }
      return Effect.succeed<ProjectServiceWorkClient.ProjectFlowSpawnOutcome>({
        status: "committed",
        instanceId: "fin_spawn_1",
        eventId: "evt_spawn_1",
      });
    },
  });
  return {
    submitted,
    spawned,
    docWrites,
    layer: Layer.succeed(ProjectServiceWorkClient.ProjectServiceWorkClient, service),
  };
};

const withHandlerLayers =
  (options?: {
    readonly workClientLayer?: Layer.Layer<ProjectServiceWorkClient.ProjectServiceWorkClient>;
    readonly capabilities?: ReadonlySet<McpInvocationContext.McpCapability>;
    readonly settingsLayer?: Layer.Layer<ServerSettings.ServerSettingsService, ServerSettingsError>;
  }) =>
  <A, E>(
    effect: Effect.Effect<
      A,
      E,
      | ProjectServiceWorkClient.ProjectServiceWorkClient
      | ServerSettings.ServerSettingsService
      | ProjectionSnapshotQuery.ProjectionSnapshotQuery
      | McpInvocationContext.McpInvocationContext
      | FileSystem.FileSystem
      | Path.Path
      | Crypto.Crypto
    >,
  ) =>
    effect.pipe(
      Effect.provideService(
        McpInvocationContext.McpInvocationContext,
        invocationFor(options?.capabilities),
      ),
      Effect.provide(
        Layer.mergeAll(
          options?.workClientLayer ?? makeWorkClientLayer().layer,
          options?.settingsLayer ?? settingsLayer,
          snapshotQueryLayer,
        ),
      ),
    );

it.layer(NodeServices.layer)("ProjectWorkToolkit handlers", (it) => {
  it.effect("project_work_submit sends the settings-derived agent and project identity", () => {
    const { submitted, layer } = makeWorkClientLayer();

    return Effect.gen(function* () {
      const operation = yield* ProjectWorkToolkitHandlers.project_work_submit({
        runId: "run_9",
        runRevision: "run:3",
        assignmentRevision: "position:5",
        result: { kind: "standalone", output: "done" },
      }).pipe(withHandlerLayers({ workClientLayer: layer }));

      assert.equal(operation.status, "committed");
      assert.equal(submitted.length, 1);
      const call = submitted[0];
      // Identity the SERVER injected — none of it appears in the arguments.
      assert.equal(call?.agentId, "ag_one");
      assert.equal(call?.projectId, "proj_ps_1");
      assert.equal(call?.projectGeneration, 7);
      // Business data the AGENT supplied, verbatim.
      assert.equal(call?.runId, "run_9");
      assert.equal(call?.expectedRunRevision, "run:3");
      assert.equal(call?.expectedAssignmentRevision, "position:5");
      assert.deepEqual(call?.result, { kind: "standalone", output: "done" });
      // The recovery handle is server-generated, not agent-visible input.
      assert.match(call?.operationId ?? "", /.+/);
      assert.match(call?.idempotencyKey ?? "", /.+/);
    });
  });

  it.effect("project_work_list joins assignment revisions and serves browser-off sessions", () => {
    const { layer } = makeWorkClientLayer({
      runs: [
        RUN_VIEW,
        { ...RUN_VIEW, runId: "run_orphan", positionId: "pos_gone", runRevision: "run:2" },
      ],
    });

    return Effect.gen(function* () {
      // The credential was minted browser-only; live settings still grant the
      // project scopes, so the tool answers without disturbing the session.
      const result = yield* ProjectWorkToolkitHandlers.project_work_list().pipe(
        withHandlerLayers({ workClientLayer: layer, capabilities: new Set(["preview"]) }),
      );

      assert.equal(result.projectGeneration, 7);
      // Queue projection: only the current work is visible (run_9 sorts
      // first — same createdAt, runId tie-break); the second run waits.
      assert.deepEqual(result.runs[0], {
        runId: "run_9",
        positionId: "pos_1",
        runRevision: "run:3",
        assignmentRevision: "position:5",
        agentId: "ag_one",
        state: "open",
        task: { prompt: "Summarize" },
        createdAt: "2026-08-01T00:00:00.000Z",
      });
      assert.isUndefined(result.runs[1]);
      assert.equal(result.queuedWorkCount, 1);
      assert.deepEqual(result.positions, [POSITION_VIEW]);
      // Workspace facts ride the current-work item verbatim (PS 0.8.0).
      const { layer: wsLayer } = makeWorkClientLayer({
        runs: [{ ...RUN_VIEW, workspacePolicy: "managed-worktree", workspacePath: "/tmp/wt-1" }],
      });
      const wsResult = yield* ProjectWorkToolkitHandlers.project_work_list().pipe(
        withHandlerLayers({ workClientLayer: wsLayer, capabilities: new Set(["preview"]) }),
      );
      assert.equal(wsResult.runs[0]?.workspacePolicy, "managed-worktree");
      assert.equal(wsResult.runs[0]?.workspacePath, "/tmp/wt-1");
    });
  });

  it.effect(
    "project_flow_start spawns through the derived identity with the name as the only task pointer",
    () => {
      const { spawned, layer } = makeWorkClientLayer();

      return Effect.gen(function* () {
        const child = yield* ProjectWorkToolkitHandlers.project_flow_start({
          definitionId: "bounded-delivery",
          name: "login bug fix",
        }).pipe(withHandlerLayers({ workClientLayer: layer }));

        assert.equal(child.status, "committed");
        if (child.status !== "committed") throw new Error("unreachable");
        assert.equal(child.instanceId, "fin_spawn_1");
        assert.equal(spawned.length, 1);
        const call = spawned[0];
        // The project comes from the directory-keyed mapping, never the arguments.
        assert.equal(call?.projectId, "proj_ps_1");
        assert.equal(call?.definitionId, "bounded-delivery");
        // v2 (D3): name-only dispatch — the child's authored start-work prompt
        // interpolates {instance.name}; there is no prompt-override surface.
        assert.equal(call?.name, "login bug fix");
        assert.deepEqual(
          Object.keys(call ?? {}).filter((key) => key === "promptOverrides"),
          [],
        );
        // Fresh idempotency key per invocation: a retry after failure is a NEW
        // spawn, never a silent replay of the original arguments.
        assert.match(call?.idempotencyKey ?? "", /^[0-9a-f-]{36}$/);
      });
    },
  );

  it.effect(
    "project_flow_start passes a still-pending construction through as a pollable state, never an error",
    () => {
      const { layer } = makeWorkClientLayer({ spawnPending: true });

      return Effect.gen(function* () {
        const outcome = yield* ProjectWorkToolkitHandlers.project_flow_start({
          definitionId: "architectural-delivery",
          name: "rfc child",
        }).pipe(withHandlerLayers({ workClientLayer: layer }));

        // Ruling ②': pending is a STATUS with the operationId to poll — the
        // agent polls project_operation_get; a re-invocation with a fresh key
        // would mint a duplicate child.
        assert.deepEqual(outcome, {
          status: "pending",
          operationId: "rest-flow-spawn-pending",
        });
      });
    },
  );

  it.effect(
    "project_flow_start answers the spawn-refusal error, not an authentication failure",
    () => {
      const { layer } = makeWorkClientLayer({
        spawnError: new ProjectServiceWorkClient.ProjectServiceWorkServiceRejectedError({
          code: "PROJECT_CONSUMER_SPAWN_NOT_AUTHORIZED",
          status: 403,
          message: "this Flow Definition did not opt in to consumer spawning",
        }),
      });

      return Effect.gen(function* () {
        const failure = yield* ProjectWorkToolkitHandlers.project_flow_start({
          definitionId: "closed",
          name: "refused",
        }).pipe(withHandlerLayers({ workClientLayer: layer }), Effect.flip);

        // The 403 bucket stays reserved for credential problems; the refusal is
        // about the DEFINITION's opt-in.
        assert.deepEqual(plainError(failure), {
          _tag: "ProjectFlowSpawnRefusedError",
          code: "PROJECT_CONSUMER_SPAWN_NOT_AUTHORIZED",
          serviceMessage: "this Flow Definition did not opt in to consumer spawning",
        });
      });
    },
  );

  it.effect("project_doc_read returns the document through the derived identity", () => {
    const { layer } = makeWorkClientLayer();

    return Effect.gen(function* () {
      const document = yield* ProjectWorkToolkitHandlers.project_doc_read({
        runId: "run_9",
        path: "decision.md",
      }).pipe(withHandlerLayers({ workClientLayer: layer }));

      assert.include(document.content, "ship it");
      assert.include(document.displayPath, "decision.md");
    });
  });

  it.effect(
    "project_doc_write notarizes content and maps slot-right denial to the document error",
    () => {
      const denied = new ProjectServiceWorkClient.ProjectServiceWorkServiceRejectedError({
        code: "FLOW_DOCUMENT_PERMISSION_DENIED",
        status: 403,
        message: "slot rights do not cover this document",
      });
      const { docWrites, layer } = makeWorkClientLayer({ documentWriteError: denied });

      return Effect.gen(function* () {
        const failure = yield* ProjectWorkToolkitHandlers.project_doc_write({
          runId: "run_9",
          path: "decision.md",
          operation: "update",
          content: "# decision\nship it\n",
        }).pipe(withHandlerLayers({ workClientLayer: layer }), Effect.flip);

        // The 403 stays reserved for credential problems; this denial is about
        // the run's slot rights.
        assert.deepEqual(plainError(failure), {
          _tag: "ProjectFlowDocumentDeniedError",
          code: "FLOW_DOCUMENT_PERMISSION_DENIED",
          serviceMessage: "slot rights do not cover this document",
        });
      });
    },
  );

  it.effect(
    "project_doc_write rejects a delete that carries content and a create without it",
    () => {
      const { layer } = makeWorkClientLayer();

      return Effect.gen(function* () {
        const withContent = yield* ProjectWorkToolkitHandlers.project_doc_write({
          runId: "run_9",
          path: "decision.md",
          operation: "delete",
          content: "stale",
        }).pipe(withHandlerLayers({ workClientLayer: layer }), Effect.flip);
        assert.deepEqual(plainError(withContent), {
          _tag: "ProjectWorkRejectedError",
          code: "PROJECT_DOC_CONTENT_INVALID",
          status: 0,
          serviceMessage: "delete must not carry content",
        });

        const withoutContent = yield* ProjectWorkToolkitHandlers.project_doc_write({
          runId: "run_9",
          path: "decision.md",
          operation: "create",
        }).pipe(withHandlerLayers({ workClientLayer: layer }), Effect.flip);
        assert.equal(
          (plainError(withoutContent) as { serviceMessage?: string }).serviceMessage,
          "content (non-empty UTF-8 text) is required for create/update",
        );
      });
    },
  );

  it.effect("project_doc_write base64-carries the content through the derived identity", () => {
    const { docWrites, layer } = makeWorkClientLayer();

    return Effect.gen(function* () {
      const receipt = yield* ProjectWorkToolkitHandlers.project_doc_write({
        runId: "run_9",
        path: "decision.md",
        operation: "create",
        content: "# decision\nship it\n",
      }).pipe(withHandlerLayers({ workClientLayer: layer }));

      assert.match(receipt.documentReceiptId, /^document:[0-9a-f]{64}$/);
    });
  });

  it.effect("project_work_get answers a structured not-found when the service returns null", () => {
    const { layer } = makeWorkClientLayer({ run: null });

    return Effect.gen(function* () {
      const failure = yield* ProjectWorkToolkitHandlers.project_work_get({ runId: "run_9" }).pipe(
        withHandlerLayers({ workClientLayer: layer }),
        Effect.flip,
      );

      assert.deepEqual(plainError(failure), {
        _tag: "ProjectWorkNotFoundError",
        code: "PROJECT_WORK_RUN_NOT_FOUND",
        kind: "run",
      });
    });
  });

  it.effect(
    "project tools answer a structured unavailability when the agent has project work off",
    () => {
      const { layer } = makeWorkClientLayer();

      return Effect.gen(function* () {
        const failure = yield* ProjectWorkToolkitHandlers.project_work_list().pipe(
          withHandlerLayers({
            workClientLayer: layer,
            settingsLayer: ServerSettings.ServerSettingsService.layerTest(agentSettings(false)),
          }),
          Effect.flip,
        );

        assert.deepEqual(plainError(failure), {
          _tag: "ProjectWorkUnavailableError",
          reason: "agent-project-disabled",
        });
      });
    },
  );

  it.effect(
    "project tools answer a structured unavailability when the project directory is not registered",
    () => {
      // The service list knows a DIFFERENT directory: the session's project
      // has no Project Service counterpart, so the identity cannot be derived.
      const { layer } = makeWorkClientLayer({
        serviceProjects: [
          { projectId: "proj_elsewhere", name: "Elsewhere", workspaceDir: "/w/elsewhere" },
        ],
      });

      return Effect.gen(function* () {
        const failure = yield* ProjectWorkToolkitHandlers.project_work_list().pipe(
          withHandlerLayers({ workClientLayer: layer }),
          Effect.flip,
        );

        assert.deepEqual(plainError(failure), {
          _tag: "ProjectWorkUnavailableError",
          reason: "project-not-registered",
        });
      });
    },
  );

  // ── FIFO queue discipline ────────────────────────────────────────

  const FIFO_RUNS = [
    { ...RUN_VIEW, runId: "run_newest", createdAt: "2026-08-01T00:00:09.000Z" },
    {
      ...RUN_VIEW,
      runId: "run_tie_b",
      runRevision: "run:1",
      createdAt: "2026-08-01T00:00:02.000Z",
    },
    {
      ...RUN_VIEW,
      runId: "run_tie_a",
      runRevision: "run:1",
      createdAt: "2026-08-01T00:00:02.000Z",
    },
    {
      ...RUN_VIEW,
      runId: "run_completed",
      state: "completed" as const,
      createdAt: "2026-08-01T00:00:01.000Z",
    },
  ] as const;

  it.effect("project_work_list surfaces only the oldest open run (queue head)", () => {
    const { layer } = makeWorkClientLayer({ runs: [...FIFO_RUNS] });
    return Effect.gen(function* () {
      const result = yield* ProjectWorkToolkitHandlers.project_work_list().pipe(
        withHandlerLayers({ workClientLayer: layer, capabilities: new Set(["preview"]) }),
      );
      // run_tie_a wins the createdAt tie over run_tie_b by runId order;
      // the completed run is never in the queue.
      assert.equal(result.runs.length, 1);
      assert.equal(result.runs[0]?.runId, "run_tie_a");
      assert.equal(result.queuedWorkCount, 2);
    });
  });

  it.effect("project_work_submit rejects a run that is not the current work", () => {
    const { layer, submitted } = makeWorkClientLayer({ runs: [...FIFO_RUNS] });
    return Effect.gen(function* () {
      const failure = yield* Effect.flip(
        ProjectWorkToolkitHandlers.project_work_submit({
          runId: "run_newest",
          runRevision: "run:3",
          assignmentRevision: "position:5",
          result: { kind: "after", message: "done" },
        }).pipe(withHandlerLayers({ workClientLayer: layer, capabilities: new Set(["preview"]) })),
      );
      assert.equal(failure._tag, "ProjectWorkNotCurrentError");
      // Nothing was minted or sent to the service.
      assert.equal(submitted.length, 0);
    });
  });

  it.effect("project_work_submit accepts the queue head and advances nothing else", () => {
    const { layer, submitted } = makeWorkClientLayer({ runs: [...FIFO_RUNS] });
    return Effect.gen(function* () {
      const operation = yield* ProjectWorkToolkitHandlers.project_work_submit({
        runId: "run_tie_a",
        runRevision: "run:1",
        assignmentRevision: "position:5",
        result: { kind: "after", message: "done" },
      }).pipe(withHandlerLayers({ workClientLayer: layer, capabilities: new Set(["preview"]) }));
      assert.equal(operation.status, "committed");
      assert.equal(submitted[0]?.runId, "run_tie_a");
    });
  });

  it.effect("project_work_get and the doc tools enforce the queue head", () => {
    const { layer } = makeWorkClientLayer({ runs: [...FIFO_RUNS] });
    return Effect.gen(function* () {
      const getFailure = yield* Effect.flip(
        ProjectWorkToolkitHandlers.project_work_get({ runId: "run_tie_b" }).pipe(
          withHandlerLayers({ workClientLayer: layer, capabilities: new Set(["preview"]) }),
        ),
      );
      assert.equal(getFailure._tag, "ProjectWorkNotCurrentError");

      const readFailure = yield* Effect.flip(
        ProjectWorkToolkitHandlers.project_doc_read({
          runId: "run_tie_b",
          path: "decision.md",
        }).pipe(withHandlerLayers({ workClientLayer: layer, capabilities: new Set(["preview"]) })),
      );
      assert.equal(readFailure._tag, "ProjectWorkNotCurrentError");

      const writeFailure = yield* Effect.flip(
        ProjectWorkToolkitHandlers.project_doc_write({
          runId: "run_tie_b",
          path: "decision.md",
          operation: "update",
          content: "text",
        }).pipe(withHandlerLayers({ workClientLayer: layer, capabilities: new Set(["preview"]) })),
      );
      assert.equal(writeFailure._tag, "ProjectWorkNotCurrentError");

      // The head flows through every tool unchanged.
      const document = yield* ProjectWorkToolkitHandlers.project_doc_read({
        runId: "run_tie_a",
        path: "decision.md",
      }).pipe(withHandlerLayers({ workClientLayer: layer, capabilities: new Set(["preview"]) }));
      assert.equal(document.content, "decision: ship it");
    });
  });

  it.effect("an empty queue leaves the verdict to the service (gate stays silent)", () => {
    // An empty list is not proof a run is gone — the vendored SDK silently
    // coerces a shape-drifted list to empty — so "not found" stays the
    // SERVICE's taxonomy: the call passes the gate and the service answers.
    const { layer, submitted } = makeWorkClientLayer({
      runs: [{ ...RUN_VIEW, state: "completed" as const }],
    });
    return Effect.gen(function* () {
      const operation = yield* ProjectWorkToolkitHandlers.project_work_submit({
        runId: "run_9",
        runRevision: "run:3",
        assignmentRevision: "position:5",
        result: { kind: "after", message: "done" },
      }).pipe(withHandlerLayers({ workClientLayer: layer, capabilities: new Set(["preview"]) }));
      assert.equal(operation.status, "committed");
      assert.equal(submitted.length, 1);
    });
  });

  it.effect("project_work_list with no open work answers the empty-queue shape", () => {
    const { layer } = makeWorkClientLayer({
      runs: [{ ...RUN_VIEW, state: "completed" as const }],
    });
    return Effect.gen(function* () {
      const result = yield* ProjectWorkToolkitHandlers.project_work_list().pipe(
        withHandlerLayers({ workClientLayer: layer, capabilities: new Set(["preview"]) }),
      );
      assert.deepEqual(result.runs, []);
      assert.equal(result.queuedWorkCount, 0);
      assert.deepEqual(result.positions, []);
    });
  });
});

describe("mapProjectServiceError", () => {
  const rejected = (code: string, status: number, message = code) =>
    new ProjectServiceWorkClient.ProjectServiceWorkServiceRejectedError({ code, status, message });

  it("maps credential and executor-identity rejections to authentication errors", () => {
    const invalid = mapProjectServiceError(
      rejected("PROJECT_CLIENT_AUTHENTICATION_INVALID", 401),
      undefined,
    );
    const disabled = mapProjectServiceError(rejected("PROJECT_CLIENT_DISABLED", 403), undefined);
    const foreign = mapProjectServiceError(
      rejected("PROJECT_WORK_EXECUTOR_IDENTITY_MISMATCH", 403),
      undefined,
    );
    assert.deepEqual(plainError(invalid), {
      _tag: "ProjectWorkAuthenticationError",
      code: "PROJECT_CLIENT_AUTHENTICATION_INVALID",
      status: 401,
    });
    assert.deepEqual(plainError(disabled), {
      _tag: "ProjectWorkAuthenticationError",
      code: "PROJECT_CLIENT_DISABLED",
      status: 403,
    });
    assert.deepEqual(plainError(foreign), {
      _tag: "ProjectWorkAuthenticationError",
      code: "PROJECT_WORK_EXECUTOR_IDENTITY_MISMATCH",
      status: 403,
    });
  });

  it("maps stale revisions to a conflict pointing at project_work_list", () => {
    const conflict = mapProjectServiceError(
      rejected("WORK_CONTROL_CONFLICT", 409, "run revision is stale"),
      undefined,
    );
    assert.deepEqual(plainError(conflict), {
      _tag: "ProjectWorkConflictError",
      code: "WORK_CONTROL_CONFLICT",
      serviceMessage: "run revision is stale",
      hint: "project_work_list",
    });
  });

  it("maps uncertain outcomes to operation recovery", () => {
    const transport = mapProjectServiceError(
      new ProjectServiceWorkClient.ProjectServiceWorkTransportError({ operationId: "op_1" }),
      { operationId: "op_1" },
    );
    assert.deepEqual(plainError(transport), {
      _tag: "ProjectWorkUncertainError",
      code: "PROJECT_WORK_TRANSPORT_UNCERTAIN",
      operationId: "op_1",
      hint: "project_operation_get",
    });

    const uncertain = mapProjectServiceError(rejected("PROJECT_WORK_UNCERTAIN", 500), {
      operationId: "op_2",
    });
    assert.deepEqual(plainError(uncertain), {
      _tag: "ProjectWorkUncertainError",
      code: "PROJECT_WORK_UNCERTAIN",
      operationId: "op_2",
      hint: "project_operation_get",
    });

    // Without submit context there is no handle to echo: omit the field
    // rather than hand the agent an empty unusable operationId.
    const handleless = mapProjectServiceError(
      rejected("PROJECT_OPERATION_UNCERTAIN", 500),
      undefined,
    );
    assert.equal(handleless._tag, "ProjectWorkUncertainError");
    if (handleless._tag === "ProjectWorkUncertainError") {
      assert.equal(handleless.operationId, undefined);
    }
  });

  it("maps unavailable, incompatible, not-found, and remaining rejections", () => {
    assert.deepEqual(
      plainError(mapProjectServiceError(rejected("PROJECT_WORK_BUSY", 503), undefined)),
      { _tag: "ProjectWorkUnavailableError", reason: "service-unavailable" },
    );
    assert.deepEqual(
      plainError(
        mapProjectServiceError(
          new ProjectServiceWorkClient.ProjectServiceWorkTransportError({}),
          undefined,
        ),
      ),
      { _tag: "ProjectWorkUnavailableError", reason: "service-unreachable" },
    );
    assert.deepEqual(
      plainError(
        mapProjectServiceError(
          new ProjectServiceWorkClient.ProjectServiceWorkApiIncompatibleError({
            code: "PROJECT_WORK_RESPONSE_SHAPE",
          }),
          undefined,
        ),
      ),
      { _tag: "ProjectWorkIncompatibleError", code: "PROJECT_WORK_RESPONSE_SHAPE" },
    );
    assert.deepEqual(
      plainError(mapProjectServiceError(rejected("PROJECT_NOT_FOUND", 404), undefined)),
      { _tag: "ProjectWorkNotFoundError", code: "PROJECT_NOT_FOUND", kind: "project" },
    );
    assert.deepEqual(
      plainError(mapProjectServiceError(rejected("WORK_CONTROL_NOT_FOUND", 404), undefined)),
      { _tag: "ProjectWorkNotFoundError", code: "WORK_CONTROL_NOT_FOUND", kind: "run" },
    );
    // What is missing is named by the code, not by the calling tool: a run
    // 404 during a submit (operation context present) is still a missing run.
    assert.deepEqual(
      plainError(
        mapProjectServiceError(rejected("PROJECT_WORK_RUN_NOT_FOUND", 404), {
          operationId: "op_9",
        }),
      ),
      { _tag: "ProjectWorkNotFoundError", code: "PROJECT_WORK_RUN_NOT_FOUND", kind: "run" },
    );
    assert.deepEqual(
      plainError(mapProjectServiceError(rejected("PROJECT_WORK_INPUT_INVALID", 400), undefined)),
      {
        _tag: "ProjectWorkRejectedError",
        code: "PROJECT_WORK_INPUT_INVALID",
        status: 400,
        serviceMessage: "PROJECT_WORK_INPUT_INVALID",
      },
    );
  });
});
