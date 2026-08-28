import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";

import * as ProjectionSnapshotQuery from "../../../orchestration/Services/ProjectionSnapshotQuery.ts";
import * as ProjectServiceWorkClient from "../../../projectService/ProjectServiceWorkClient.ts";
import { canonicalWorkspaceDirectory } from "../../../projectService/ProjectDirectoryKey.ts";
import {
  currentAssignedWork,
  orderAssignedWorkQueue,
} from "../../../projectService/AssignedWorkQueue.ts";
import * as ServerSettings from "../../../serverSettings.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";
import * as Tools from "./tools.ts";
import type { ProjectToolExecutionContext } from "./context.ts";
import { resolveProjectToolContext } from "./context.ts";

type ProjectWorkToolError = Tools.ProjectWorkError;
type ClientError = ProjectServiceWorkClient.ProjectServiceWorkClientError;
type CallRequirements =
  | McpInvocationContext.McpInvocationContext
  | ProjectServiceWorkClient.ProjectServiceWorkClient
  | ServerSettings.ServerSettingsService
  | ProjectionSnapshotQuery.ProjectionSnapshotQuery
  | FileSystem.FileSystem
  | Path.Path
  | Crypto.Crypto;

/** Status codes the service uses for credential/executor-identity rejections. */
const isAuthenticationStatus = (status: number): boolean => status === 401 || status === 403;

/** Revision/assignment fences and other optimistic-concurrency rejections. */
const isConflict = (code: string, status: number): boolean =>
  status === 409 || /CONFLICT|MISMATCH|GUARD|STALE/.test(code);

const isUnavailable = (code: string, status: number): boolean =>
  status === 503 || code.includes("UNAVAILABLE") || code.includes("BUSY");

/**
 * Error boundary: service/SDK failures become structured MCP errors with the
 * service's own code preserved. Authentication failures carry no session
 * action — the credential problem is between the two servers.
 */
export const mapProjectServiceError = (
  error: ClientError,
  operation: { readonly operationId: string } | undefined,
): ProjectWorkToolError => {
  switch (error._tag) {
    case "ProjectServiceWorkUnavailableError":
      return new Tools.ProjectWorkUnavailableError({ reason: error.reason });
    case "ProjectServiceWorkApiIncompatibleError":
      return new Tools.ProjectWorkIncompatibleError({ code: error.code });
    case "ProjectServiceWorkTransportError":
      // A submit that may have landed is recoverable; a read that never
      // completed is simply unreachable.
      return operation !== undefined
        ? new Tools.ProjectWorkUncertainError({
            code: "PROJECT_WORK_TRANSPORT_UNCERTAIN",
            operationId: operation.operationId,
            hint: "project_operation_get",
          })
        : new Tools.ProjectWorkUnavailableError({ reason: "service-unreachable" });
    case "ProjectServiceWorkServiceRejectedError": {
      const { code, status, message, details } = error;
      // Flow-document failures are their own bucket BEFORE the generic
      // NOT_FOUND/CONFLICT classes: the right recovery reads the DOCUMENT —
      // the generic project_work_list hint would misdirect the agent.
      if (code.startsWith("FLOW_DOCUMENT_")) {
        const detailFields =
          details === undefined ? {} : { details: details as Record<string, unknown> };
        if (code === "FLOW_DOCUMENT_PERMISSION_DENIED") {
          return new Tools.ProjectFlowDocumentDeniedError({
            code,
            serviceMessage: message,
            ...detailFields,
          });
        }
        if (code.endsWith("_NOT_FOUND")) {
          const path = typeof details?.path === "string" ? details.path : "the document";
          return new Tools.ProjectFlowDocumentNotFoundError({
            code,
            path,
            serviceMessage: message,
            ...detailFields,
          });
        }
        if (isConflict(code, status)) {
          // An old PS predating the native write op answers REQUEST_FAILED on
          // "write" — name the upgrade instead of probing or retrying.
          const upgrade =
            code === "FLOW_DOCUMENT_REQUEST_FAILED"
              ? " This Project Service may predate the native write operation; ask the operator to upgrade Project Service."
              : "";
          return new Tools.ProjectFlowDocumentConflictError({
            code,
            serviceMessage: message + upgrade,
            ...detailFields,
          });
        }
        return new Tools.ProjectWorkRejectedError({
          code,
          status,
          serviceMessage: message,
          ...detailFields,
        });
      }
      if (isAuthenticationStatus(status)) {
        return new Tools.ProjectWorkAuthenticationError({ code, status });
      }
      if (code === "PROJECT_NOT_FOUND") {
        return new Tools.ProjectWorkNotFoundError({ code, kind: "project" });
      }
      if (code.includes("NOT_FOUND")) {
        return new Tools.ProjectWorkNotFoundError({
          code,
          // The code names what is missing (…RUN_NOT_FOUND vs
          // …OPERATION_NOT_FOUND); which tool made the call does not.
          kind: code.includes("OPERATION") ? "operation" : "run",
        });
      }
      if (isConflict(code, status)) {
        return new Tools.ProjectWorkConflictError({
          code,
          serviceMessage: message,
          hint: "project_work_list",
        });
      }
      if (isUnavailable(code, status)) {
        return new Tools.ProjectWorkUnavailableError({ reason: "service-unavailable" });
      }
      if (code.includes("UNCERTAIN")) {
        return new Tools.ProjectWorkUncertainError({
          code,
          ...(operation === undefined ? {} : { operationId: operation.operationId }),
          hint: "project_operation_get",
        });
      }
      if (code.includes("VERSION") || code.includes("PROTOCOL")) {
        return new Tools.ProjectWorkIncompatibleError({ code });
      }
      return new Tools.ProjectWorkRejectedError({
        code,
        status,
        serviceMessage: message,
        ...(details === undefined ? {} : { details: details as Record<string, unknown> }),
      });
    }
  }
};

/**
 * Resolve the trusted execution context for this call from the session plus
 * live settings: logical agent (provider-instance routing), T3 project (the
 * thread's project and its workspace directory), the Project Service project
 * registered for that directory, and the live capability scopes. Tool
 * arguments contribute nothing here.
 */
const resolveContext = Effect.fn("ProjectWorkToolkit.resolveContext")(function* (
  requiredCapability: "project.work.read" | "project.work.write",
): Effect.fn.Return<ProjectToolExecutionContext, ProjectWorkToolError, CallRequirements> {
  const invocation = yield* McpInvocationContext.McpInvocationContext;
  const settingsService = yield* ServerSettings.ServerSettingsService;
  const snapshotQuery = yield* ProjectionSnapshotQuery.ProjectionSnapshotQuery;
  const workClient = yield* ProjectServiceWorkClient.ProjectServiceWorkClient;
  // Local failures degrade to structured unavailability, never a raw error.
  const settings = yield* settingsService.getSettings.pipe(
    Effect.mapError(
      () => new Tools.ProjectWorkUnavailableError({ reason: "integration-unreadable" }),
    ),
  );
  const shell = yield* snapshotQuery
    .getThreadShellById(invocation.threadId)
    .pipe(
      Effect.mapError(
        () => new Tools.ProjectWorkUnavailableError({ reason: "thread-unavailable" }),
      ),
    );
  const t3ProjectId = Option.isSome(shell) ? shell.value.projectId : undefined;
  const project =
    t3ProjectId === undefined
      ? Option.none()
      : yield* snapshotQuery
          .getProjectShellById(t3ProjectId)
          .pipe(
            Effect.mapError(
              () => new Tools.ProjectWorkUnavailableError({ reason: "project-unavailable" }),
            ),
          );
  const serviceProjects = yield* workClient
    .listProjects()
    .pipe(Effect.mapError((error) => mapProjectServiceError(error, undefined)));
  // Canonicalize the STORED root before directory matching (issue #6 review):
  // the Project Service dir arrives canonical (realpath + Windows case-fold),
  // and a stored root may carry a legacy symlink spelling — one key function
  // on both sides, or matching forks. Best-effort and never failing: a root
  // that cannot be canonicalized still compares by its resolved spelling.
  const workspaceRootOption = Option.isSome(project)
    ? yield* canonicalWorkspaceDirectory(project.value.workspaceRoot).pipe(Effect.option)
    : Option.none();
  const resolution = resolveProjectToolContext({
    settings,
    providerInstanceId: invocation.providerInstanceId,
    logicalAgentId: invocation.logicalAgentId,
    t3ProjectId,
    workspaceRoot: Option.getOrUndefined(workspaceRootOption),
    serviceProjects,
  });
  if (!resolution.ok) {
    return yield* new Tools.ProjectWorkUnavailableError({ reason: resolution.reason });
  }
  if (!resolution.context.capabilities.has(requiredCapability)) {
    return yield* new Tools.ProjectWorkUnavailableError({ reason: "capability-disabled" });
  }
  return resolution.context;
});

/**
 * The queue-head rule, enforced: every run-addressing tool may only touch the
 * CURRENT work (oldest open run). Derived fresh per call from the service's
 * authoritative answer — T3 stores no queue state. Returns the resolved
 * current work, or fails with a structured error the agent can act on
 * (not-current / no-open-work), so a wrong target can never reach the service.
 */
const resolveCurrentWorkRun = Effect.fn("ProjectWorkToolkit.resolveCurrentWorkRun")(function* (
  context: ProjectToolExecutionContext,
): Effect.fn.Return<
  ProjectServiceWorkClient.ProjectWorkRunRecord | null,
  ProjectWorkToolError,
  CallRequirements
> {
  const runs = yield* fetchCurrentRuns(context);
  return currentAssignedWork(runs);
});

/** The agent's runs as the service sees them right now (all states). */
const fetchCurrentRuns = Effect.fn("ProjectWorkToolkit.fetchCurrentRuns")(function* (
  context: ProjectToolExecutionContext,
): Effect.fn.Return<
  ReadonlyArray<ProjectServiceWorkClient.ProjectWorkRunRecord>,
  ProjectWorkToolError,
  CallRequirements
> {
  const client = yield* ProjectServiceWorkClient.ProjectServiceWorkClient;
  const generation = yield* client
    .getProjectGeneration(context.projectServiceProjectId)
    .pipe(Effect.mapError((error) => mapProjectServiceError(error, undefined)));
  return yield* client
    .listMy({
      projectId: context.projectServiceProjectId,
      projectGeneration: generation,
      agentId: context.logicalAgentId,
    })
    .pipe(Effect.mapError((error) => mapProjectServiceError(error, undefined)));
});

/**
 * Reject a run-addressing argument that is not the current work. When the
 * queue is EMPTY the gate stays silent and the service renders its own
 * verdict: an empty list is not proof the run is gone (the vendored SDK
 * silently coerces a shape-drifted list to empty), and "not found" is the
 * service's taxonomy to hand out, not the gate's.
 */
const requireCurrentRun = (
  current: ProjectServiceWorkClient.ProjectWorkRunRecord | null,
  inputRunId: string,
): Effect.Effect<void, Tools.ProjectWorkNotCurrentError> =>
  current !== null && current.runId !== inputRunId
    ? Effect.fail(
        new Tools.ProjectWorkNotCurrentError({ runId: inputRunId, hint: "project_work_list" }),
      )
    : Effect.void;

const handlers = {
  project_work_list: () =>
    Effect.gen(function* () {
      const context = yield* resolveContext("project.work.read");
      const client = yield* ProjectServiceWorkClient.ProjectServiceWorkClient;
      const generation = yield* client
        .getProjectGeneration(context.projectServiceProjectId)
        .pipe(Effect.mapError((error) => mapProjectServiceError(error, undefined)));
      const workContext = {
        projectId: context.projectServiceProjectId,
        projectGeneration: generation,
      };
      const [runs, positions] = yield* Effect.all([
        client.listMy({ ...workContext, agentId: context.logicalAgentId }),
        client.listPositions(workContext),
      ]).pipe(Effect.mapError((error) => mapProjectServiceError(error, undefined)));
      const assignmentByPosition = new Map(
        positions.map((position) => [position.positionId, position.assignmentRevision]),
      );
      // Queue projection: only the current work is visible; the rest wait.
      const queue = orderAssignedWorkQueue(runs);
      const current = queue.at(0);
      return {
        projectGeneration: generation,
        runs:
          current === undefined
            ? []
            : [
                {
                  runId: current.runId,
                  positionId: current.positionId,
                  runRevision: current.runRevision,
                  assignmentRevision: assignmentByPosition.get(current.positionId) ?? null,
                  agentId: current.agentId ?? context.logicalAgentId,
                  state: current.state,
                  task: current.task,
                  createdAt: current.createdAt,
                  ...(current.workspacePolicy === undefined
                    ? {}
                    : { workspacePolicy: current.workspacePolicy }),
                  ...(current.workspacePath === undefined
                    ? {}
                    : { workspacePath: current.workspacePath }),
                  ...(current.action === undefined ? {} : { action: current.action }),
                  ...(current.visit === undefined ? {} : { visit: current.visit }),
                },
              ],
        queuedWorkCount: Math.max(0, queue.length - 1),
        positions:
          current === undefined
            ? []
            : positions
                .filter((position) => position.positionId === current.positionId)
                .map((position) => ({
                  positionId: position.positionId,
                  displayName: position.displayName,
                  assignmentRevision: position.assignmentRevision,
                })),
      };
    }),
  project_work_get: (input: { readonly runId: string }) =>
    Effect.gen(function* () {
      const context = yield* resolveContext("project.work.read");
      const current = yield* resolveCurrentWorkRun(context);
      yield* requireCurrentRun(current, input.runId);
      const client = yield* ProjectServiceWorkClient.ProjectServiceWorkClient;
      const generation = yield* client
        .getProjectGeneration(context.projectServiceProjectId)
        .pipe(Effect.mapError((error) => mapProjectServiceError(error, undefined)));
      const run = yield* client
        .getRun({
          projectId: context.projectServiceProjectId,
          projectGeneration: generation,
          runId: input.runId,
          agentId: context.logicalAgentId,
        })
        .pipe(Effect.mapError((error) => mapProjectServiceError(error, undefined)));
      if (run === null) {
        return yield* new Tools.ProjectWorkNotFoundError({
          code: "PROJECT_WORK_RUN_NOT_FOUND",
          kind: "run",
        });
      }
      return run;
    }),
  project_work_submit: (input: {
    readonly runId: string;
    readonly runRevision: string;
    readonly assignmentRevision: string;
    readonly result: Readonly<Record<string, unknown>>;
  }) =>
    Effect.gen(function* () {
      const context = yield* resolveContext("project.work.write");
      const current = yield* resolveCurrentWorkRun(context);
      yield* requireCurrentRun(current, input.runId);
      const client = yield* ProjectServiceWorkClient.ProjectServiceWorkClient;
      const crypto = yield* Crypto.Crypto;
      const operation = {
        operationId: yield* crypto.randomUUIDv4.pipe(Effect.orDie),
        idempotencyKey: yield* crypto.randomUUIDv4.pipe(Effect.orDie),
      };
      const generation = yield* client
        .getProjectGeneration(context.projectServiceProjectId)
        .pipe(Effect.mapError((error) => mapProjectServiceError(error, operation)));
      const record = yield* client
        .submitRun(
          {
            projectId: context.projectServiceProjectId,
            projectGeneration: generation,
            runId: input.runId,
            expectedRunRevision: input.runRevision,
            expectedAssignmentRevision: input.assignmentRevision,
            agentId: context.logicalAgentId,
            result: input.result,
          },
          operation,
        )
        .pipe(Effect.mapError((error) => mapProjectServiceError(error, operation)));
      // A rejected submit reaches the model as a FAILURE, not a success-shaped
      // record: only failure message text is guaranteed model-visible at the
      // MCP boundary, and the envelope now carries the field-level why.
      // project_operation_get keeps returning raw records for recovery.
      if (record.status === "rejected") {
        return yield* new Tools.ProjectWorkRejectedError({
          code: record.error.code,
          status: 409,
          serviceMessage: `The submission was rejected (${record.error.code}): ${record.error.message} Call project_work_list for the current revisions and contract, then resubmit.`,
          ...(record.error.details === undefined
            ? {}
            : { details: record.error.details as Record<string, unknown> }),
        });
      }
      return record;
    }),
  project_operation_get: (input: { readonly operationId: string }) =>
    Effect.gen(function* () {
      yield* resolveContext("project.work.read");
      const client = yield* ProjectServiceWorkClient.ProjectServiceWorkClient;
      const operation = yield* client
        .getOperation(input.operationId)
        .pipe(Effect.mapError((error) => mapProjectServiceError(error, undefined)));
      if (operation === null) {
        return yield* new Tools.ProjectWorkNotFoundError({
          code: "PROJECT_OPERATION_NOT_FOUND",
          kind: "operation",
        });
      }
      return operation;
    }),
  project_doc_read: (input: { readonly runId: string; readonly path: string }) =>
    Effect.gen(function* () {
      const context = yield* resolveContext("project.work.read");
      const current = yield* resolveCurrentWorkRun(context);
      yield* requireCurrentRun(current, input.runId);
      const client = yield* ProjectServiceWorkClient.ProjectServiceWorkClient;
      return yield* client
        .readFlowDocument({
          projectId: context.projectServiceProjectId,
          runId: input.runId,
          agentId: context.logicalAgentId,
          path: input.path,
        })
        .pipe(Effect.mapError((error) => mapProjectServiceError(error, undefined)));
    }),
  project_doc_write: (input: {
    readonly runId: string;
    readonly path: string;
    readonly content: string;
  }) =>
    Effect.gen(function* () {
      // The notary's own contract: non-empty UTF-8 data — reject the mismatch
      // HERE with a typed error, before any key is minted.
      if (typeof input.content !== "string" || input.content.length === 0) {
        return yield* new Tools.ProjectWorkRejectedError({
          code: "PROJECT_DOC_CONTENT_INVALID",
          status: 0,
          serviceMessage: "content (non-empty UTF-8 text) is required",
        });
      }
      const context = yield* resolveContext("project.work.write");
      const current = yield* resolveCurrentWorkRun(context);
      yield* requireCurrentRun(current, input.runId);
      const client = yield* ProjectServiceWorkClient.ProjectServiceWorkClient;
      const crypto = yield* Crypto.Crypto;
      // Fresh key per call: a retried write is a new notarized write (the
      // receipt layer replays only same-key+same-digest), never a silent replay.
      const idempotencyKey = yield* crypto.randomUUIDv4.pipe(Effect.orDie);
      // "write" is the PS-native upsert (apiMinor 1): create-or-overwrite in
      // one server-side step — no existence probing, no create/update guess.
      return yield* client
        .writeFlowDocument({
          projectId: context.projectServiceProjectId,
          runId: input.runId,
          agentId: context.logicalAgentId,
          idempotencyKey,
          path: input.path,
          operation: "write",
          data: Buffer.from(input.content, "utf8").toString("base64"),
        })
        .pipe(Effect.mapError((error) => mapProjectServiceError(error, undefined)));
    }),
  project_doc_edit: (input: {
    readonly runId: string;
    readonly path: string;
    readonly old_string: string;
    readonly new_string: string;
    readonly replaceAll?: boolean | undefined;
  }) =>
    Effect.gen(function* () {
      const context = yield* resolveContext("project.work.write");
      const current = yield* resolveCurrentWorkRun(context);
      yield* requireCurrentRun(current, input.runId);
      const client = yield* ProjectServiceWorkClient.ProjectServiceWorkClient;
      const crypto = yield* Crypto.Crypto;
      // Read → exact-match replace → notarized write-back. The match rules
      // are the general Edit semantics: exactly one occurrence, or
      // replace_all; the errors carry the document size so the model knows
      // what it is matching against.
      const document = yield* client
        .readFlowDocument({
          projectId: context.projectServiceProjectId,
          runId: input.runId,
          agentId: context.logicalAgentId,
          path: input.path,
        })
        .pipe(Effect.mapError((error) => mapProjectServiceError(error, undefined)));
      const lines = document.content.split("\n").length;
      const occurrences = document.content.split(input.old_string).length - 1;
      if (occurrences === 0) {
        return yield* new Tools.ProjectDocEditNoMatchError({
          path: input.path,
          documentLines: lines,
        });
      }
      if (occurrences > 1 && input.replaceAll !== true) {
        return yield* new Tools.ProjectDocEditAmbiguousMatchError({
          path: input.path,
          matchCount: occurrences,
        });
      }
      const content =
        input.replaceAll === true
          ? document.content.split(input.old_string).join(input.new_string)
          : document.content.replace(input.old_string, input.new_string);
      if (content.length === 0) {
        return yield* new Tools.ProjectWorkRejectedError({
          code: "PROJECT_DOC_CONTENT_INVALID",
          status: 0,
          serviceMessage:
            "the edit would empty the document; a flow document cannot be empty — use project_doc_delete instead",
        });
      }
      const idempotencyKey = yield* crypto.randomUUIDv4.pipe(Effect.orDie);
      const written = yield* client
        .writeFlowDocument({
          projectId: context.projectServiceProjectId,
          runId: input.runId,
          agentId: context.logicalAgentId,
          idempotencyKey,
          path: input.path,
          operation: "write",
          data: Buffer.from(content, "utf8").toString("base64"),
        })
        .pipe(Effect.mapError((error) => mapProjectServiceError(error, undefined)));
      return { ...written, replacements: input.replaceAll === true ? occurrences : 1 };
    }),
  project_doc_delete: (input: { readonly runId: string; readonly path: string }) =>
    Effect.gen(function* () {
      const context = yield* resolveContext("project.work.write");
      const current = yield* resolveCurrentWorkRun(context);
      yield* requireCurrentRun(current, input.runId);
      const client = yield* ProjectServiceWorkClient.ProjectServiceWorkClient;
      const crypto = yield* Crypto.Crypto;
      const idempotencyKey = yield* crypto.randomUUIDv4.pipe(Effect.orDie);
      return yield* client
        .writeFlowDocument({
          projectId: context.projectServiceProjectId,
          runId: input.runId,
          agentId: context.logicalAgentId,
          idempotencyKey,
          path: input.path,
          operation: "delete",
        })
        .pipe(Effect.mapError((error) => mapProjectServiceError(error, undefined)));
    }),
} satisfies Parameters<typeof Tools.ProjectWorkToolkit.toLayer>[0];

/** Exposed for tests; the Live layer is what the MCP server consumes. */
export const ProjectWorkToolkitHandlers = handlers;

export const ProjectWorkToolkitHandlersLive = Tools.ProjectWorkToolkit.toLayer(handlers);
