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
  getProjectShellById: () => Effect.die("unused"),
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
      projectBindings: [{ projectId: "proj_ps_1", projectName: "PS Project", t3ProjectId }],
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
  runRevision: 3,
  state: "open",
  agentId: "ag_one",
  task: { prompt: "Summarize" },
  createdAt: "2026-08-01T00:00:00.000Z",
} as const;

const POSITION_VIEW = {
  positionId: "pos_1",
  displayName: "Summarizer",
  assignmentRevision: 5,
} as const;

interface CapturedSubmit {
  readonly projectId: string;
  readonly projectGeneration: number;
  readonly runId: string;
  readonly expectedRunRevision: number;
  readonly expectedAssignmentRevision: number;
  readonly agentId: string;
  readonly result: Readonly<Record<string, unknown>>;
  readonly operationId: string;
  readonly idempotencyKey: string;
}

const makeWorkClientLayer = (overrides?: {
  readonly runs?: readonly ProjectServiceWorkClient.ProjectWorkRunRecord[];
  readonly positions?: readonly ProjectServiceWorkClient.ProjectWorkPositionRecord[];
  readonly run?: ProjectServiceWorkClient.ProjectWorkRunRecord | null;
}) => {
  const submitted: CapturedSubmit[] = [];
  const service = ProjectServiceWorkClient.ProjectServiceWorkClient.of({
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
  });
  return {
    submitted,
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
        runRevision: 3,
        assignmentRevision: 5,
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
      assert.equal(call?.expectedRunRevision, 3);
      assert.equal(call?.expectedAssignmentRevision, 5);
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
        { ...RUN_VIEW, runId: "run_orphan", positionId: "pos_gone", runRevision: 2 },
      ],
    });

    return Effect.gen(function* () {
      // The credential was minted browser-only; live settings still grant the
      // project scopes, so the tool answers without disturbing the session.
      const result = yield* ProjectWorkToolkitHandlers.project_work_list().pipe(
        withHandlerLayers({ workClientLayer: layer, capabilities: new Set(["preview"]) }),
      );

      assert.equal(result.projectGeneration, 7);
      assert.deepEqual(result.runs[0], {
        runId: "run_9",
        positionId: "pos_1",
        runRevision: 3,
        assignmentRevision: 5,
        agentId: "ag_one",
        state: "open",
        task: { prompt: "Summarize" },
        createdAt: "2026-08-01T00:00:00.000Z",
      });
      assert.isNull(result.runs[1]?.assignmentRevision);
      assert.deepEqual(result.positions, [POSITION_VIEW]);
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
