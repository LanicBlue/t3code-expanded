import { PositiveInt, ProjectWorkVisitView } from "@t3tools/contracts";
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
      'The run\'s current revision token from project_work_list (e.g. "run:4"); pass it back verbatim — a stale value is rejected as a conflict. This mission-revision CAS is the only fence.',
  }),
  result: Schema.Record(Schema.String, Schema.Unknown).annotate({
    description:
      'The work result payload — the shape the run\'s completion contract calls for. Visit runs: {"outcome": one of action.outcomes (the run\'s action field project_work_list returns; "abandon" is always a reserved extra when action.abandonAvailable is true), "nextNode"?: a target workKey — action.candidates lists the in-contract ones, "reason": REQUIRED whenever nextNode is outside the candidates (off-contract needs its why), "feedback"?: context for a rework hop, "documentReceiptIds"?: receipts from project_doc_write/edit}. The server validates the result against the contract and rejects mismatches with a structured error.',
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
  /**
   * work-mission-v6: the run's task snapshot (prompt/documents/rights) comes
   * from the detail read — always present on the hydrated current work, but
   * optional on the wire for degraded reads; the top-level `mission` identity
   * covers the summary shape.
   */
  mission: Schema.optional(
    Schema.Struct({ id: Schema.String, name: Schema.String, objective: Schema.String }),
  ),
  task: Schema.optional(Schema.Record(Schema.String, Schema.Unknown)),
  createdAt: Schema.String,
  /** Where this run's work happens (managed worktree path / project root). */
  workspacePolicy: Schema.optional(Schema.String),
  workspacePath: Schema.optional(Schema.String),
  /**
   * THE completion contract for this run (PS apiMinor 2): the visit action
   * view — the station's outcome vocabulary, the in-contract candidate next
   * stations, and whether the reserved "abandon" alternative is open. The
   * flow-era state/gate/terminal kinds are gone with the flow line; kind is
   * always "visit" on this PS generation.
   */
  action: Schema.optional(ProjectServiceWorkClient.ProjectWorkActionRecord),
  /**
   * The decoded VISIT view of the run (mission population, §6.1): mission /
   * work-station facts plus the same visit completion contract as `action`,
   * decoded once at the trust boundary. Absent on runs outside the visit
   * population.
   */
  visit: Schema.optional(ProjectWorkVisitView),
});

const ProjectWorkPositionItem = Schema.Struct({
  positionId: Schema.String,
  displayName: Schema.String,
  /** Null on work-mission-v6.2: the assignment CAS is gone (occupancy token). */
  assignmentRevision: Schema.NullOr(Schema.String),
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
    "List this agent's assigned Work in the session project's Project Service project (matched by the project's workspace directory). Work is delivered strictly in arrival order: the result carries ONLY the current work (the oldest open run, with the runRevision project_work_submit expects); queuedWorkCount says how many wait behind it and those are not visible until the current one is submitted. No identity is required or accepted.",
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
    'Complete the CURRENT Work run with the result payload, guarded by the runRevision from project_work_list (the mission-revision CAS — the only fence). Match the run\'s completion contract — the action field on the run project_work_list returns spells it out: kind is always "visit" (the station outcome vocabulary plus the candidate next stations). A VISIT work submits {"outcome": <one of action.outcomes>, "nextNode"?: <target workKey>, "reason"?, "feedback"?, "documentReceiptIds"?}: outcome picks from THIS station\'s vocabulary; nextNode is YOUR routing choice — action.candidates lists the in-contract targets as a hint, not a constraint, but a nextNode outside the candidates is off-contract and REQUIRES "reason" (the choice is semantics and gets recorded); omit nextNode only when the contract leaves exactly one continuation or the outcome is terminal; "feedback" carries rework context when the outcome sends the mission back. When action.abandonAvailable is true, {"outcome":"abandon"} is the reserved way to end the work early — carry the why in "feedback". Only the current work (queue head) may be submitted — anything else answers a not-current error; re-list. The server derives the executor identity from the session; arguments never carry it. On a stale revision the service answers a conflict — re-list. If the outcome is uncertain, the error carries an operationId for project_operation_get. After a successful submit, call project_work_list again: the next work may now be current.',
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
      'The instance-relative document path, e.g. "decision.md". Must be a path the run\'s mission contract declares (documentsResolved).',
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
    "Delete a flow document through one of your open work runs (notarized). UNSUPPORTED on work-mission-v6: mission documents are contract-declared and cannot be deleted — this tool answers a typed rejection; overwrite with project_doc_write instead.",
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
  ProjectDocReadTool,
  ProjectDocWriteTool,
  ProjectDocEditTool,
  ProjectDocDeleteTool,
);
