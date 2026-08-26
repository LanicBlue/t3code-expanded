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
  {
    code: Schema.String,
    status: Schema.Int,
    serviceMessage: Schema.String,
    details: Schema.optional(Schema.Record(Schema.String, Schema.Unknown)),
  },
) {
  override get message(): string {
    return `${this.serviceMessage}${renderDetails(this.details)}`;
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
  {
    code: Schema.String,
    serviceMessage: Schema.String,
    details: Schema.optional(Schema.Record(Schema.String, Schema.Unknown)),
  },
) {
  override get message(): string {
    return `${this.serviceMessage} The run's slot rights do not cover this document operation.${renderDetails(this.details)}`;
  }
}

/**
 * A flow document path the service cannot resolve (absent, undeclared, or a
 * directory) — the message tells the model whether the path is wrong or the
 * document just is not written yet, and points at the read tool to check.
 */
export class ProjectFlowDocumentNotFoundError extends Schema.TaggedErrorClass<ProjectFlowDocumentNotFoundError>()(
  "ProjectFlowDocumentNotFoundError",
  {
    code: Schema.String,
    path: Schema.String,
    serviceMessage: Schema.String,
    details: Schema.optional(Schema.Record(Schema.String, Schema.Unknown)),
  },
) {
  override get message(): string {
    return `${this.serviceMessage} Call project_doc_read on "${this.path}" to confirm the path and contents.${renderDetails(this.details)}`;
  }
}

/**
 * A flow-document write conflict (idempotency digest mismatch or authority
 * CAS) — the correct recovery is a fresh tool call, never project_work_list
 * (work revisions are unrelated to document revisions).
 */
export class ProjectFlowDocumentConflictError extends Schema.TaggedErrorClass<ProjectFlowDocumentConflictError>()(
  "ProjectFlowDocumentConflictError",
  {
    code: Schema.String,
    serviceMessage: Schema.String,
    details: Schema.optional(Schema.Record(Schema.String, Schema.Unknown)),
  },
) {
  override get message(): string {
    return `${this.serviceMessage} The document changed or the retry reused a spent idempotency key; call project_doc_read for the current revision, then repeat the tool call (it mints a fresh key).${renderDetails(this.details)}`;
  }
}

/** project_doc_edit found no occurrence of old_string in the document. */
export class ProjectDocEditNoMatchError extends Schema.TaggedErrorClass<ProjectDocEditNoMatchError>()(
  "ProjectDocEditNoMatchError",
  { path: Schema.String, documentLines: Schema.Int },
) {
  override get message(): string {
    return `old_string was not found in "${this.path}" (the document currently has ${this.documentLines} lines). Call project_doc_read and copy old_string exactly, including whitespace and indentation.`;
  }
}

/** project_doc_edit found old_string more than once and replace_all was not set. */
export class ProjectDocEditAmbiguousMatchError extends Schema.TaggedErrorClass<ProjectDocEditAmbiguousMatchError>()(
  "ProjectDocEditAmbiguousMatchError",
  { path: Schema.String, matchCount: Schema.Int },
) {
  override get message(): string {
    return `old_string matches ${this.matchCount} places in "${this.path}". Include more surrounding lines to make it unique, or pass replace_all: true to replace every occurrence.`;
  }
}

/**
 * Renders structured service details into the model-visible message text —
 * the MCP boundary sends only `message` on failure, so the facts must ride in
 * the string. Key=value pairs, capped so long lists cannot blow the envelope.
 */
export const renderDetails = (details: Readonly<Record<string, unknown>> | undefined): string => {
  if (details === undefined) return "";
  const entries = Object.entries(details)
    .filter(([, value]) => value !== undefined)
    .map(([key, value]) => `${key}=${Array.isArray(value) ? value.join(", ") : String(value)}`);
  if (entries.length === 0) return "";
  const rendered = ` Details: ${entries.join("; ")}.`;
  return rendered.length > 600 ? `${rendered.slice(0, 599)}…` : rendered;
};

/**
 * The addressed run is not the CURRENT work (the head of the agent's queue):
 * work is obtained and submitted strictly in arrival order, so this is a
 * sequencing rejection, not a credential or conflict one. Re-list to see the
 * current work.
 */
export class ProjectWorkNotCurrentError extends Schema.TaggedErrorClass<ProjectWorkNotCurrentError>()(
  "ProjectWorkNotCurrentError",
  {
    runId: Schema.String,
    hint: Schema.Literals(["project_work_list"]),
  },
) {
  override get message(): string {
    return `Run '${this.runId}' is not the current work. Work is handled strictly in order; call project_work_list for the current one.`;
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
  ProjectFlowDocumentNotFoundError,
  ProjectFlowDocumentConflictError,
  ProjectDocEditNoMatchError,
  ProjectDocEditAmbiguousMatchError,
  ProjectWorkNotCurrentError,
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
    description:
      'The instance name — THE task pointer: the child\'s start work prompt interpolates {instance.name}, so put the concrete task here (e.g. "fix login bug in auth.ts"). This is the only context you can inject; there is no prompt-override surface.',
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
  /** Where this run's work happens (managed worktree path / project root). */
  workspacePolicy: Schema.optional(Schema.String),
  workspacePath: Schema.optional(Schema.String),
  /**
   * THE completion contract for this run (PS apiMinor 2): the submit shape it
   * accepts, the transitions a state submission chooses among (with target
   * states), where a gate's accept/reject send the instance, and the display
   * paths of the run's document rights. Absent on older PS deployments.
   */
  action: Schema.optional(ProjectServiceWorkClient.ProjectWorkActionRecord),
});

const ProjectWorkPositionItem = Schema.Struct({
  positionId: Schema.String,
  displayName: Schema.String,
  assignmentRevision: Schema.String,
});

export const ProjectWorkListResult = Schema.Struct({
  projectGeneration: PositiveInt,
  /** Only the CURRENT work (oldest open run) — the queue is worked in order. */
  runs: Schema.Array(ProjectWorkListItem),
  /** Open works waiting behind the current one (they are not listed yet). */
  queuedWorkCount: Schema.Number,
  positions: Schema.Array(ProjectWorkPositionItem),
});

// ── Tools ────────────────────────────────────────────────────────

export const ProjectWorkListTool = Tool.make("project_work_list", {
  description:
    "List this agent's assigned Work in the session project's Project Service project (matched by the project's workspace directory). Work is delivered strictly in arrival order: the result carries ONLY the current work (the oldest open run, with the revision pair project_work_submit expects); queuedWorkCount says how many wait behind it and those are not visible until the current one is submitted. No identity is required or accepted.",
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
    "Fetch one Work run by its identifier — must be the CURRENT work from project_work_list (work is handled strictly in order). A run that is not yours or not current answers a structured error.",
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
    'Complete the CURRENT Work run with the result payload, guarded by the run and assignment revisions from project_work_list. Match the run\'s completion contract — the action field on the run project_work_list returns spells it out (kind, the transitions with their target states, the gate outcomes, the document paths). In short: a STATE work submits {"kind":"after", "message"?} — add "transitionId" only when action.transitions lists more than one — or {"kind":"abandon", "message"} to abandon the whole instance (the reason is mandatory; an abandon gate reviews it, and a rejection bounces the state for one more attempt); a TERMINAL work submits {"kind":"terminal", "message"?} which ends the instance; a GATE work submits {"kind":"before", "outcome":"accept"|"reject"} — reject REQUIRES "feedback" and sends the SAME instance back for rework (action.review.rejectTo names where): a fresh run re-delivers automatically, and its prompt carries a \'Rework context\' block (attempt ordinal + prior gate feedback) that the next attempt must address. Only the current work (queue head) may be submitted — anything else answers a not-current error; re-list. The server derives the executor identity from the session; arguments never carry it. On a stale revision the service answers a conflict — re-list. If the outcome is uncertain, the error carries an operationId for project_operation_get. After a successful submit, call project_work_list again: the next work may now be current.',
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
    "Start a new flow instance in the session project (tree branching): a one-way fork into a child flow — the parent flow does not wait for it or observe its completion. Use it to hand follow-up work to a differently-shaped process instead of looping back (e.g. after triage, start the spike/bounded/architectural delivery flow). The instance NAME is the one and only piece of context you inject: the child's authored start-work prompt interpolates {instance.name} and teaches itself the task from it (the run view also carries instance:{instanceId,name}). Richer hand-off context? Write a project document first and reference it by path in the name — never expect to inject prompts. The server constructs the child in the background: the result is either committed (the child instanceId) or pending — construction still in flight under the returned operationId. On pending, POLL project_operation_get with that operationId; NEVER call project_flow_start again for the same logical child (a fresh call mints a fresh idempotency key and would duplicate the child).",
  parameters: ProjectFlowStartInput,
  success: ProjectServiceWorkClient.ProjectFlowSpawnOutcome,
  failure: ProjectWorkError,
  dependencies,
})
  .annotate(Tool.Title, "Start a Project flow instance")
  .annotate(Tool.Destructive, true)
  .annotate(Tool.OpenWorld, true);

export const ProjectDocReadInput = Schema.Struct({
  runId: Schema.String.annotate({
    description:
      "The CURRENT work run from project_work_list (work is handled strictly in order) — its slot grants the rights for this read.",
  }),
  path: Schema.String.annotate({
    description:
      'The instance-relative document path shown as flow://project/<instance>/<path>, e.g. "decision.md".',
  }),
});

export const ProjectDocWriteInput = Schema.Struct({
  runId: Schema.String.annotate({
    description:
      "The CURRENT work run from project_work_list (work is handled strictly in order) — its slot grants the WRITE rights for this operation.",
  }),
  path: Schema.String.annotate({
    description:
      'The instance-relative document path, e.g. "decision.md". Must be a document the flow definition declares.',
  }),
  content: Schema.String.annotate({
    description:
      "The FULL document content as UTF-8 text. Write is an upsert: it creates the document when absent and overwrites it when present — no create/update choice, no existence guessing. (A document cannot be emptied; use project_doc_delete instead.)",
  }),
});

export const ProjectDocEditInput = Schema.Struct({
  runId: Schema.String.annotate({
    description:
      "The CURRENT work run from project_work_list (work is handled strictly in order) — its slot grants the WRITE rights for this operation.",
  }),
  path: Schema.String.annotate({
    description: 'The instance-relative document path of an EXISTING document, e.g. "decision.md".',
  }),
  old_string: Schema.String.annotate({
    description:
      "The exact text to replace — copied verbatim from project_doc_read, including whitespace and indentation. It must match exactly ONE place in the document unless replace_all is true.",
  }),
  new_string: Schema.String.annotate({
    description:
      "The replacement text. May be empty (a deletion) as long as the document does not become empty — use project_doc_delete for that.",
  }),
  replaceAll: Schema.optional(
    Schema.Boolean.annotate({
      description: "Replace EVERY occurrence of old_string instead of requiring a unique match.",
    }),
  ),
});

export const ProjectDocDeleteInput = Schema.Struct({
  runId: Schema.String.annotate({
    description:
      "The CURRENT work run from project_work_list (work is handled strictly in order) — its slot grants the WRITE rights for this operation.",
  }),
  path: Schema.String.annotate({
    description:
      "The instance-relative document path of the document to delete; it must already exist.",
  }),
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
    "Write a flow document of the session project through one of your open work runs (notarized), like a normal file Write: send the FULL content and it creates the document when absent or overwrites it when present — there is no create/update choice to make. Returns a documentReceiptId you then pass in project_work_submit's result as documentReceiptIds — the submit validates it against the run's slot rights. This is how work hands its output to later works. The disk file itself is not the contract — without the receipt the completion does not count.",
  parameters: ProjectDocWriteInput,
  success: ProjectServiceWorkClient.ProjectFlowDocumentWriteRecord,
  failure: ProjectWorkError,
  dependencies,
})
  .annotate(Tool.Title, "Write a Project flow document (notarized)")
  .annotate(Tool.Destructive, true)
  .annotate(Tool.OpenWorld, true);

export const ProjectDocEditTool = Tool.make("project_doc_edit", {
  description:
    "Edit a flow document through one of your open work runs (notarized), like a normal file Edit: old_string must match exactly one place in the document (or set replace_all to replace every occurrence); the replacement is written back through the notary path and returns the fresh documentReceiptId to pass in project_work_submit's documentReceiptIds. Prefer this over project_doc_write when changing part of a large document. The document must already exist — call project_doc_read first to copy old_string exactly.",
  parameters: ProjectDocEditInput,
  success: ProjectServiceWorkClient.ProjectFlowDocumentEditRecord,
  failure: ProjectWorkError,
  dependencies,
})
  .annotate(Tool.Title, "Edit a Project flow document (notarized)")
  .annotate(Tool.Destructive, true)
  .annotate(Tool.OpenWorld, true);

export const ProjectDocDeleteTool = Tool.make("project_doc_delete", {
  description:
    "Delete a flow document through one of your open work runs (notarized). The document must already exist; the receipt handed back carries no successor revision. Use this instead of writing empty content — a document cannot be emptied by write.",
  parameters: ProjectDocDeleteInput,
  success: ProjectServiceWorkClient.ProjectFlowDocumentWriteRecord,
  failure: ProjectWorkError,
  dependencies,
})
  .annotate(Tool.Title, "Delete a Project flow document (notarized)")
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
  ProjectDocEditTool,
  ProjectDocDeleteTool,
);
