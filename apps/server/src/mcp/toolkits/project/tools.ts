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

/**
 * The definition refused consumer spawning: only flow definitions whose ACTIVE
 * version opted in (`consumerStartable`) can be started by this client. Not a
 * credential problem — the definition must be re-published with the opt-in.
 */
export class ProjectFlowSpawnRefusedError extends Schema.TaggedErrorClass<ProjectFlowSpawnRefusedError>()(
  "ProjectFlowSpawnRefusedError",
  { code: Schema.String, serviceMessage: Schema.String },
) {
  override get message(): string {
    return `${this.serviceMessage} Only definitions whose active version opts in (consumerStartable) can be started.`;
  }
}

/**
 * The addressed run's slot rights do not cover the document operation — a
 * rights question, not a credential one (the 403 bucket stays reserved for
 * credential problems).
 */
export class ProjectFlowDocumentDeniedError extends Schema.TaggedErrorClass<ProjectFlowDocumentDeniedError>()(
  "ProjectFlowDocumentDeniedError",
  { code: Schema.String, serviceMessage: Schema.String },
) {
  override get message(): string {
    return `${this.serviceMessage} The run's slot rights do not cover this document operation.`;
  }
}

export const ProjectWorkError = Schema.Union([
  ProjectWorkUnavailableError,
  ProjectWorkAuthenticationError,
  ProjectWorkConflictError,
  ProjectWorkUncertainError,
  ProjectWorkNotFoundError,
  ProjectWorkIncompatibleError,
  ProjectFlowSpawnRefusedError,
  ProjectFlowDocumentDeniedError,
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

export const ProjectFlowStartInput = Schema.Struct({
  definitionId: Schema.String.annotate({
    description:
      'The flow definition to start, e.g. "spike-probe". Only definitions whose active version opted in to consumer spawning answer; others refuse with a structured error.',
  }),
  name: Schema.String.annotate({
    description: "A short human-readable name for the new flow instance.",
  }),
  promptOverrides: Schema.optional(
    Schema.Record(Schema.String, Schema.String).annotate({
      description:
        'Task injection: workDefinitionId → prompt, applied to the definition\'s works BEFORE the instance is created (e.g. { "spike-probe.perform-work": "TASK: fix login bug in auth.ts" }). Prompts are definition-level, so spawn the same definition sequentially, never in parallel.',
    }),
  ),
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

export const ProjectFlowStartTool = Tool.make("project_flow_start", {
  description:
    "Start a new flow instance in the session project (tree branching): a one-way fork into a child flow — the parent flow does not wait for it or observe its completion. Use it to hand follow-up work to a differently-shaped process instead of looping back (e.g. after triage, start the spike/bounded/architectural delivery flow). Optionally inject task prompts into the child's works before it is created. Same-definition spawns are sequential (prompts are definition-level).",
  parameters: ProjectFlowStartInput,
  success: ProjectServiceWorkClient.ProjectFlowSpawnRecord,
  failure: ProjectWorkError,
  dependencies,
})
  .annotate(Tool.Title, "Start a Project flow instance")
  .annotate(Tool.Destructive, true)
  .annotate(Tool.OpenWorld, true);

export const ProjectDocReadInput = Schema.Struct({
  runId: Schema.String.annotate({
    description:
      "One of your OPEN work runs (from project_work_list) whose slot grants read rights over the document.",
  }),
  path: Schema.String.annotate({
    description:
      'The instance-relative document path shown as flow://project/<instance>/<path>, e.g. "decision.md".',
  }),
});

export const ProjectDocWriteInput = Schema.Struct({
  runId: Schema.String.annotate({
    description:
      "One of your OPEN work runs (from project_work_list) whose slot grants WRITE rights over the document.",
  }),
  path: Schema.String.annotate({
    description:
      'The instance-relative document path, e.g. "decision.md". Must be a document the flow definition declares.',
  }),
  operation: Schema.Literals(["create", "update", "delete"]).annotate({
    description: "create requires the file to be absent; update/delete require it to exist.",
  }),
  content: Schema.optional(
    Schema.String.annotate({
      description:
        "The full document content as UTF-8 text — REQUIRED for create/update, must be ABSENT for delete.",
    }),
  ),
});

export const ProjectDocReadTool = Tool.make("project_doc_read", {
  description:
    "Read a flow document of the session project through one of your open work runs — the run's slot rights decide readability (a read-only slot cannot write, a write-only slot cannot read). Documents are the handoff channel between works: read what an earlier work recorded, e.g. the triage decision. Editing the file on disk directly does NOT count — only the notarized path mints receipts.",
  parameters: ProjectDocReadInput,
  success: ProjectServiceWorkClient.ProjectFlowDocumentRecord,
  failure: ProjectWorkError,
  dependencies,
})
  .annotate(Tool.Title, "Read a Project flow document")
  .annotate(Tool.Readonly, true)
  .annotate(Tool.Destructive, false)
  .annotate(Tool.Idempotent, true)
  .annotate(Tool.OpenWorld, true);

export const ProjectDocWriteTool = Tool.make("project_doc_write", {
  description:
    "Write a flow document of the session project through one of your open work runs (notarized): returns a documentReceiptId you then pass in project_work_submit's result as documentReceiptIds — the submit validates it against the run's slot rights. This is how work hands its output to later works (e.g. the triage decision the dispatcher reads). Write the FULL content; the operation is create (file absent) or update. The disk file itself is not the contract — without the receipt the completion does not count.",
  parameters: ProjectDocWriteInput,
  success: ProjectServiceWorkClient.ProjectFlowDocumentWriteRecord,
  failure: ProjectWorkError,
  dependencies,
})
  .annotate(Tool.Title, "Write a Project flow document (notarized)")
  .annotate(Tool.Destructive, true)
  .annotate(Tool.OpenWorld, true);

export const ProjectWorkToolkit = Toolkit.make(
  ProjectWorkListTool,
  ProjectWorkGetTool,
  ProjectWorkSubmitTool,
  ProjectOperationGetTool,
  ProjectFlowStartTool,
  ProjectDocReadTool,
  ProjectDocWriteTool,
);
