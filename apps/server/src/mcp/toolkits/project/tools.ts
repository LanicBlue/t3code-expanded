import { PositiveInt } from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import { Tool, Toolkit } from "effect/unstable/ai";

import * as ProjectionSnapshotQuery from "../../../orchestration/Services/ProjectionSnapshotQuery.ts";
import * as ProjectServiceWorkClient from "../../../projectService/ProjectServiceWorkClient.ts";
import * as ServerSettings from "../../../serverSettings.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";
import * as Context from "./context.ts";

const dependencies = [
  McpInvocationContext.McpInvocationContext,
  ProjectServiceWorkClient.ProjectServiceWorkClient,
  ServerSettings.ServerSettingsService,
  ProjectionSnapshotQuery.ProjectionSnapshotQuery,
  FileSystem.FileSystem,
  Path.Path,
  Crypto.Crypto,
];

// ── Tool errors (the MCP-facing error boundary) ──────────────────

export class ProjectWorkUnavailableError extends Schema.TaggedErrorClass<ProjectWorkUnavailableError>()(
  "ProjectWorkUnavailableError",
  { reason: Context.ProjectWorkUnavailabilityReason },
) {
  override get message(): string {
    return Context.projectWorkUnavailabilityMessage(this.reason);
  }
}

/** Credential or executor-identity rejection. Never touches the session. */
export class ProjectWorkAuthenticationError extends Schema.TaggedErrorClass<ProjectWorkAuthenticationError>()(
  "ProjectWorkAuthenticationError",
  { code: Schema.String, status: Schema.Int },
) {
  override get message(): string {
    return `The Project Service rejected this client's credential or agent identity (${this.code}).`;
  }
}

/** Stale run/assignment revision: re-read current revisions with project_work_list. */
export class ProjectWorkConflictError extends Schema.TaggedErrorClass<ProjectWorkConflictError>()(
  "ProjectWorkConflictError",
  {
    code: Schema.String,
    serviceMessage: Schema.String,
    hint: Schema.Literals(["project_work_list"]),
  },
) {
  override get message(): string {
    return `${this.serviceMessage} Call project_work_list for the current revisions.`;
  }
}

/** A submit's outcome is unknown: recover it via project_operation_get. */
export class ProjectWorkUncertainError extends Schema.TaggedErrorClass<ProjectWorkUncertainError>()(
  "ProjectWorkUncertainError",
  {
    code: Schema.String,
    // Absent when the service reports an uncertain outcome without our
    // submit context (a read-path recovery): no handle exists to echo.
    operationId: Schema.optional(Schema.String),
    hint: Schema.Literals(["project_operation_get"]),
  },
) {
  override get message(): string {
    return this.operationId === undefined
      ? `The outcome is uncertain (${this.code}). Call project_operation_get to recover it.`
      : `The submit outcome is uncertain (${this.code}). Call project_operation_get with operationId to recover it.`;
  }
}

export class ProjectWorkNotFoundError extends Schema.TaggedErrorClass<ProjectWorkNotFoundError>()(
  "ProjectWorkNotFoundError",
  { code: Schema.String, kind: Schema.Literals(["run", "operation", "project"]) },
) {
  override get message(): string {
    return `The Project Service has no such ${this.kind} for this agent (${this.code}).`;
  }
}

/** Incompatible SDK/API/protocol generation. */
export class ProjectWorkIncompatibleError extends Schema.TaggedErrorClass<ProjectWorkIncompatibleError>()(
  "ProjectWorkIncompatibleError",
  { code: Schema.String },
) {
  override get message(): string {
    return `The Project Service API is incompatible with this client (${this.code}).`;
  }
}

/** Any other typed service rejection; code and status preserved verbatim. */
export class ProjectWorkRejectedError extends Schema.TaggedErrorClass<ProjectWorkRejectedError>()(
  "ProjectWorkRejectedError",
  { code: Schema.String, status: Schema.Int, serviceMessage: Schema.String },
) {
  override get message(): string {
    return this.serviceMessage;
  }
}

export const ProjectWorkError = Schema.Union([
  ProjectWorkUnavailableError,
  ProjectWorkAuthenticationError,
  ProjectWorkConflictError,
  ProjectWorkUncertainError,
  ProjectWorkNotFoundError,
  ProjectWorkIncompatibleError,
  ProjectWorkRejectedError,
]);
export type ProjectWorkError = typeof ProjectWorkError.Type;

// ── Input schemas (business data only — never identity) ──────────

// NOT Schema.Struct({}): an empty struct serializes as anyOf[object, array],
// which violates the MCP tools/list contract (inputSchema must be an object
// schema). claude-code rejects the ENTIRE tool list over that one schema and
// the agent sees zero Project tools (issue #7). Tool.EmptyParams is the
// SDK's own no-argument schema and serializes as a plain object type.
export const ProjectWorkListInput = Tool.EmptyParams;

export const ProjectWorkGetInput = Schema.Struct({
  runId: Schema.String.annotate({
    description: "Identifier of the Work run, exactly as returned by project_work_list.",
  }),
});

export const ProjectWorkSubmitInput = Schema.Struct({
  runId: Schema.String.annotate({
    description: "Identifier of the Work run to complete, as returned by project_work_list.",
  }),
  runRevision: Schema.String.annotate({
    description:
      'The run\'s current revision token from project_work_list (e.g. "run:4"); pass it back verbatim — a stale value is rejected as a conflict.',
  }),
  assignmentRevision: Schema.String.annotate({
    description:
      'The position\'s current assignment revision token from project_work_list (e.g. "position:2"); pass it back verbatim — a stale value is rejected as a conflict.',
  }),
  result: Schema.Record(Schema.String, Schema.Unknown).annotate({
    description: "The work result payload; its shape is defined by the position's work definition.",
  }),
});

export const ProjectOperationGetInput = Schema.Struct({
  operationId: Schema.String.annotate({
    description:
      "The operation identifier returned by project_work_submit or surfaced by its uncertain-outcome error.",
  }),
});

// ── Result schemas ───────────────────────────────────────────────

const ProjectWorkListItem = Schema.Struct({
  runId: Schema.String,
  positionId: Schema.String,
  runRevision: Schema.String,
  assignmentRevision: Schema.NullOr(Schema.String),
  agentId: Schema.String,
  state: Schema.Literals(["open", "completed", "superseded", "cancelled"]),
  task: Schema.Record(Schema.String, Schema.Unknown),
  createdAt: Schema.String,
});

const ProjectWorkPositionItem = Schema.Struct({
  positionId: Schema.String,
  displayName: Schema.String,
  assignmentRevision: Schema.String,
});

export const ProjectWorkListResult = Schema.Struct({
  projectGeneration: PositiveInt,
  runs: Schema.Array(ProjectWorkListItem),
  positions: Schema.Array(ProjectWorkPositionItem),
});

// ── Tools ────────────────────────────────────────────────────────

export const ProjectWorkListTool = Tool.make("project_work_list", {
  description:
    "List this agent's assigned Work runs in the session project's Project Service project (matched by the project's workspace directory), together with the project's positions and each position's current assignment revision. The revision pair on every item is what project_work_submit expects; no identity is required or accepted.",
  parameters: ProjectWorkListInput,
  success: ProjectWorkListResult,
  failure: ProjectWorkError,
  dependencies,
})
  .annotate(Tool.Title, "List assigned Project work")
  .annotate(Tool.Readonly, true)
  .annotate(Tool.Destructive, false)
  .annotate(Tool.Idempotent, true)
  .annotate(Tool.OpenWorld, true);

export const ProjectWorkGetTool = Tool.make("project_work_get", {
  description:
    "Fetch one Work run by its identifier. Returns only runs the Project Service can see for this agent's client; a run that is not yours or not open answers a structured not-found error.",
  parameters: ProjectWorkGetInput,
  success: ProjectServiceWorkClient.ProjectWorkRunRecord,
  failure: ProjectWorkError,
  dependencies,
})
  .annotate(Tool.Title, "Get Project work run")
  .annotate(Tool.Readonly, true)
  .annotate(Tool.Destructive, false)
  .annotate(Tool.Idempotent, true)
  .annotate(Tool.OpenWorld, true);

export const ProjectWorkSubmitTool = Tool.make("project_work_submit", {
  description:
    "Complete a Work run with the result payload, guarded by the run and assignment revisions from project_work_list. The server derives the executor identity from the session; arguments never carry it. On a stale revision the service answers a conflict — re-list the work. If the outcome is uncertain, the error carries an operationId for project_operation_get.",
  parameters: ProjectWorkSubmitInput,
  success: ProjectServiceWorkClient.ProjectWorkOperationRecord,
  failure: ProjectWorkError,
  dependencies,
})
  .annotate(Tool.Title, "Submit Project work result")
  .annotate(Tool.Destructive, true)
  .annotate(Tool.OpenWorld, true);

export const ProjectOperationGetTool = Tool.make("project_operation_get", {
  description:
    "Recover the status of a Project Service operation by identifier, typically the operationId from project_work_submit. Returns the operation record, including its committed result or rejection error.",
  parameters: ProjectOperationGetInput,
  success: ProjectServiceWorkClient.ProjectWorkOperationRecord,
  failure: ProjectWorkError,
  dependencies,
})
  .annotate(Tool.Title, "Get Project operation")
  .annotate(Tool.Readonly, true)
  .annotate(Tool.Destructive, false)
  .annotate(Tool.Idempotent, true)
  .annotate(Tool.OpenWorld, true);

export const ProjectWorkToolkit = Toolkit.make(
  ProjectWorkListTool,
  ProjectWorkGetTool,
  ProjectWorkSubmitTool,
  ProjectOperationGetTool,
);
