/**
 * Outbound Project Service Work client for the agent-facing MCP tools.
 *
 * Rides the vendored `@lanicblue/project-consumer` SDK over the loopback
 * service URL. Every call resolves the base URL from settings and the raw
 * credential from the server secret store server-side: the credential exists
 * only inside this module's Authorization header and never enters a tool
 * schema, result, error, or log line.
 *
 * @module ProjectServiceWorkClient
 */
import {
  isLocalProjectServiceBaseUrl,
  PositiveInt,
  ProjectWorkVisitActionRecord,
  ProjectWorkVisitView,
} from "@t3tools/contracts";
import {
  ProjectConsumerWorkClient,
  ProjectConsumerWorkClientError,
} from "@lanicblue/project-consumer";
import type { ProjectConsumerWorkTransport } from "@lanicblue/project-consumer";
import * as Context from "effect/Context";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import { HttpClient, HttpClientRequest } from "effect/unstable/http";

import * as ServerSecretStore from "../auth/ServerSecretStore.ts";
import * as ServerSettings from "../serverSettings.ts";

const FACET_TIMEOUT = Duration.seconds(10);

// ── Client-side errors ───────────────────────────────────────────

/** The integration is not usable right now: disabled, unconfigured, or unreadable. */
export class ProjectServiceWorkUnavailableError extends Schema.TaggedErrorClass<ProjectServiceWorkUnavailableError>()(
  "ProjectServiceWorkUnavailableError",
  {
    reason: Schema.Literals([
      "integration-disabled",
      "integration-unreadable",
      "invalid-base-url",
      "no-credential",
    ]),
  },
) {
  override get message(): string {
    switch (this.reason) {
      case "integration-disabled":
        return "The Project Service integration is disabled.";
      case "integration-unreadable":
        return "The Project Service settings or credential could not be read.";
      case "invalid-base-url":
        return "The Project Service base URL is not a local http(s) endpoint.";
      case "no-credential":
        return "No Project Service client credential is stored.";
    }
  }
}

/** The service answered with a shape this SDK generation cannot interpret. */
export class ProjectServiceWorkApiIncompatibleError extends Schema.TaggedErrorClass<ProjectServiceWorkApiIncompatibleError>()(
  "ProjectServiceWorkApiIncompatibleError",
  { code: Schema.String },
) {
  override get message(): string {
    return `The Project Service API is incompatible with this client (${this.code}).`;
  }
}

/** The call could not complete; for a submit, the outcome may still land under `operationId`. */
export class ProjectServiceWorkTransportError extends Schema.TaggedErrorClass<ProjectServiceWorkTransportError>()(
  "ProjectServiceWorkTransportError",
  { operationId: Schema.optional(Schema.String) },
) {
  override get message(): string {
    return this.operationId === undefined
      ? "The Project Service could not be reached."
      : "The submit outcome is uncertain; recover it with project_operation_get.";
  }
}

/** The service rejected the call with a typed envelope; `code`/`status` are preserved verbatim. */
export class ProjectServiceWorkServiceRejectedError extends Schema.TaggedErrorClass<ProjectServiceWorkServiceRejectedError>()(
  "ProjectServiceWorkServiceRejectedError",
  {
    code: Schema.String,
    status: Schema.Int,
    message: Schema.String,
    /** Structured facts the service attached (allowed fields, permitted transitions, …). */
    details: Schema.optional(Schema.Record(Schema.String, Schema.Unknown)),
  },
) {}

export type ProjectServiceWorkClientError =
  | ProjectServiceWorkUnavailableError
  | ProjectServiceWorkApiIncompatibleError
  | ProjectServiceWorkTransportError
  | ProjectServiceWorkServiceRejectedError;

const isServiceRejected = Schema.is(ProjectServiceWorkServiceRejectedError);

// ── Wire records (the subset the tools surface; extra fields drop) ──

/**
 * The wire's aggregate revisions are OPAQUE STRING TOKENS ("run:4",
 * "position:2") — the service's token() parser rejects bare numbers, so every
 * record field and submit fence carries the token verbatim (issue #6 E2E:
 * decoding them as PositiveInt rejected every real response).
 */
const AggregateRevisionSchema = Schema.String;

export const ProjectWorkPositionRecord = Schema.Struct({
  positionId: Schema.String,
  displayName: Schema.String,
  /**
   * work-mission-v6.2: positions answer the OCCUPANCY token — "last-write-wins
   * — no assignment CAS on the wire"; the assignmentRevision counter is gone
   * (it is not part of any fence anymore). Older generations may still answer
   * the counter; both decode, neither is required.
   */
  assignmentRevision: Schema.optional(AggregateRevisionSchema),
  occupancy: Schema.optional(Schema.Int),
});
export type ProjectWorkPositionRecord = typeof ProjectWorkPositionRecord.Type;

/**
 * The run's completion contract as PS projects it (apiMinor 2): the visit
 * action view — the station's outcome vocabulary (`outcomes`), the
 * in-contract next-station candidates, and whether the reserved "abandon"
 * alternative is open. work-mission-v5 Phase 7 narrowed this to the SDK 0.16
 * `WorkRunActionView` (kind is always "visit"): the flow-era state/gate/
 * terminal arms have no producer on the service anymore, so a run record
 * carrying any other kind is an incompatibility, never silently-dropped
 * facts. One schema with contracts' visit action record — the run-level
 * `action` field and the visit view's action block are the SAME wire fact.
 */
export const ProjectWorkActionRecord = ProjectWorkVisitActionRecord;
export type ProjectWorkActionRecord = ProjectWorkVisitActionRecord;

/**
 * work-mission-v6: `work-runs/my` answers SUMMARY projections — the mission
 * identity rides the TOP level (id/name/objective), and there is no task
 * snapshot: the prompt/documents/rights are the detail read's payload
 * (`getRun`). listMy hydrates open runs through getRun below, so consumers
 * see one full shape; the block stays decodeable for the un-hydrated pass.
 */
const SummaryMissionBlock = Schema.Struct({
  id: Schema.String,
  name: Schema.String,
  objective: Schema.String,
});

export const ProjectWorkRunRecord = Schema.Struct({
  runId: Schema.String,
  positionId: Schema.String,
  runRevision: AggregateRevisionSchema,
  state: Schema.Literals(["open", "completed", "superseded", "cancelled"]),
  agentId: Schema.optional(Schema.String),
  /** The v6 list projection's top-level mission identity (see above). */
  mission: Schema.optional(SummaryMissionBlock),
  /** Absent on v6 summary projections; present on detail views. */
  task: Schema.optional(Schema.Record(Schema.String, Schema.Unknown)),
  createdAt: Schema.String,
  resolvedAt: Schema.optional(Schema.NullOr(Schema.String)),
  /**
   * Execution-workspace facts (PS 0.8.0): where this run's work happens —
   * a managed worktree path for flow-instance work, the project root for
   * standalone work. Absent on older servers or when the registry read
   * degraded; "absent" means unknown, never project-root.
   */
  workspacePolicy: Schema.optional(Schema.String),
  workspacePath: Schema.optional(Schema.String),
  action: Schema.optional(ProjectWorkActionRecord),
  /**
   * The decoded VISIT view of `task` (work-mission-v5 population, capability
   * mission.v1): present iff the task carries a `mission` block — the
   * discriminator against the legacy flow population (`task.instance`).
   * Client-side projection: the wire carries the blocks inside `task`; this
   * field is the same facts decoded once at the trust boundary.
   */
  visit: Schema.optional(ProjectWorkVisitView),
});
export type ProjectWorkRunRecord = typeof ProjectWorkRunRecord.Type;

/**
 * Decode the visit view out of a run: null when the task has no `mission`
 * block (not this population — the Project Service's run surface is
 * visit-only since it removed the flow stack, work-mission-v5 Phase 7), the
 * decoded view when it does. The mission/work blocks live in the task
 * snapshot; the completion contract is the run's TOP-LEVEL `action` field
 * (the SDK's WorkRunView carries it there — one fact, one home). The decode
 * REBUILDS the record, so the view carries exactly the pinned §6.1 blocks —
 * never the whole task (the prompt and other task facts stay on `task`,
 * where they belong). A task that DECLARES the mission population but does
 * not decode against the pinned shape — mission/work blocks missing, or the
 * visit action absent — is an incompatibility like any other bad shape,
 * never a silently-ignored block.
 */
const VisitTaskBlocks = Schema.Struct({
  mission: ProjectWorkVisitView.fields.mission,
  work: ProjectWorkVisitView.fields.work,
});
const decodeVisitTaskBlocks = Schema.decodeUnknownSync(VisitTaskBlocks);
const visitViewOfRun = (run: ProjectWorkRunRecord): ProjectWorkVisitView | null => {
  // A v6 summary projection has no task snapshot — no visit contract to
  // decode (the hydrated form replaces it before consumers see it).
  if (run.task === undefined) {
    return null;
  }
  const mission = run.task.mission;
  if (typeof mission !== "object" || mission === null) {
    return null;
  }
  try {
    const blocks = decodeVisitTaskBlocks(run.task);
    // The visit population without its completion contract is a broken
    // projection — PS emits `action` unconditionally on visit run views.
    if (run.action === undefined) {
      throw new Error("visit run without action");
    }
    return { ...blocks, action: run.action };
  } catch {
    throw new ProjectServiceWorkApiIncompatibleError({ code: "PROJECT_WORK_RESPONSE_SHAPE" });
  }
};

/**
 * A Project Service project record (the ordinary-client-legal project info
 * read, GET /project/v1/). `workspaceDir` is the directory-keyed lookup the
 * MCP tools use to map a local project onto its Project Service project.
 */
const ServiceProjectRecord = Schema.Struct({
  id: Schema.String,
  name: Schema.String,
  workspaceDir: Schema.String,
});

export const ProjectServiceProjectRecord = Schema.Struct({
  projectId: Schema.String,
  name: Schema.String,
  workspaceDir: Schema.String,
});
export type ProjectServiceProjectRecord = typeof ProjectServiceProjectRecord.Type;

/** Explicit projection: the service's `id` becomes the wire-stable `projectId`. */
const projectServiceProjectRecord = (
  record: Schema.Schema.Type<typeof ServiceProjectRecord>,
): ProjectServiceProjectRecord => ({
  projectId: record.id,
  name: record.name,
  workspaceDir: record.workspaceDir,
});

const ServiceErrorEnvelope = Schema.Struct({
  category: Schema.String,
  code: Schema.String,
  message: Schema.String,
  details: Schema.optional(Schema.Record(Schema.String, Schema.Unknown)),
});

const EnvelopeError = Schema.Struct({
  code: Schema.String,
  message: Schema.String,
});

/** A failure envelope on routes this module calls directly (not via the SDK). */
const FailureEnvelope = Schema.Struct({ ok: Schema.Literals([false]), error: EnvelopeError });

/** The wire shape the SDK delivers (base64 data); decoded before the tool sees it. */
const WIRE_FLOW_DOCUMENT_RECORD = Schema.Struct({
  data: Schema.String,
  /** work-mission-v6 reads carry no revision — mission documents are immutable reads; only notarized writes mint receipts. */
  revision: Schema.optional(Schema.String),
  displayPath: Schema.String,
  size: Schema.optional(Schema.Int),
});

/**
 * A read flow document: `content` is the decoded UTF-8 text (the wire carries
 * base64; the MCP boundary never hands the model raw base64). Revision is
 * null on work-mission-v6 reads (only writes produce revision facts).
 */
export const ProjectFlowDocumentRecord = Schema.Struct({
  content: Schema.String,
  revision: Schema.NullOr(Schema.String),
  displayPath: Schema.String,
  size: Schema.Int,
});
export type ProjectFlowDocumentRecord = typeof ProjectFlowDocumentRecord.Type;

/** A notarized write: the receipt submit's documentReceiptIds validation
 * accepts. Revision is null after delete (no successor revision). */
export const ProjectFlowDocumentWriteRecord = Schema.Struct({
  documentReceiptId: Schema.String,
  revision: Schema.NullOr(Schema.String),
  displayPath: Schema.String,
});
export type ProjectFlowDocumentWriteRecord = typeof ProjectFlowDocumentWriteRecord.Type;

/** A notarized edit: the write receipt plus how many replacements landed. */
export const ProjectFlowDocumentEditRecord = Schema.Struct({
  documentReceiptId: Schema.String,
  revision: Schema.NullOr(Schema.String),
  displayPath: Schema.String,
  replacements: Schema.Int,
});
export type ProjectFlowDocumentEditRecord = typeof ProjectFlowDocumentEditRecord.Type;

export const ProjectWorkOperationRecord = Schema.Union([
  Schema.Struct({
    status: Schema.Literals(["pending"]),
    operationId: Schema.String,
    kind: Schema.String,
    requestDigest: Schema.String,
    createdAt: Schema.String,
  }),
  Schema.Struct({
    status: Schema.Literals(["committed"]),
    operationId: Schema.String,
    kind: Schema.String,
    requestDigest: Schema.String,
    result: Schema.Unknown,
    // The service stores aggregate revisions as opaque strings ("run:4"),
    // matching the SDK's own token() validation — never a bare number.
    revision: Schema.optional(Schema.String),
    createdAt: Schema.String,
    resolvedAt: Schema.String,
  }),
  Schema.Struct({
    status: Schema.Literals(["rejected"]),
    operationId: Schema.String,
    kind: Schema.String,
    requestDigest: Schema.String,
    error: ServiceErrorEnvelope,
    createdAt: Schema.String,
    resolvedAt: Schema.String,
  }),
]);
export type ProjectWorkOperationRecord = typeof ProjectWorkOperationRecord.Type;

/**
 * Explicit projections: the service views carry Client-side identity
 * (projectId, executorRef, workspaceRef) the agent must never see, so records
 * are rebuilt field-by-field rather than passed through.
 */
const projectRunRecord = (run: ProjectWorkRunRecord): ProjectWorkRunRecord => {
  // The visit view derives from the run (the task's mission/work blocks plus
  // the run-level action field); decoding here puts every later consumer
  // — queue keys, session routing, the MCP tools — behind one boundary.
  const visit = visitViewOfRun(run);
  return {
    runId: run.runId,
    positionId: run.positionId,
    runRevision: run.runRevision,
    state: run.state,
    ...(run.agentId === undefined ? {} : { agentId: run.agentId }),
    ...(run.mission === undefined ? {} : { mission: run.mission }),
    ...(run.task === undefined ? {} : { task: run.task }),
    createdAt: run.createdAt,
    ...(run.resolvedAt === undefined ? {} : { resolvedAt: run.resolvedAt }),
    ...(run.workspacePolicy === undefined ? {} : { workspacePolicy: run.workspacePolicy }),
    ...(run.workspacePath === undefined ? {} : { workspacePath: run.workspacePath }),
    // The completion contract (PS apiMinor 2) is agent-facing like the rest of
    // this projection — absent on older PS stays absent.
    ...(run.action === undefined ? {} : { action: run.action }),
    ...(visit === null ? {} : { visit }),
  };
};

const projectPositionRecord = (position: ProjectWorkPositionRecord): ProjectWorkPositionRecord => ({
  positionId: position.positionId,
  displayName: position.displayName,
  ...(position.assignmentRevision === undefined
    ? {}
    : { assignmentRevision: position.assignmentRevision }),
  ...(position.occupancy === undefined ? {} : { occupancy: position.occupancy }),
});

const projectOperationRecord = (
  operation: ProjectWorkOperationRecord,
): ProjectWorkOperationRecord =>
  operation.status === "pending"
    ? {
        status: operation.status,
        operationId: operation.operationId,
        kind: operation.kind,
        requestDigest: operation.requestDigest,
        createdAt: operation.createdAt,
      }
    : operation.status === "committed"
      ? {
          status: operation.status,
          operationId: operation.operationId,
          kind: operation.kind,
          requestDigest: operation.requestDigest,
          result: operation.result,
          ...(operation.revision === undefined ? {} : { revision: operation.revision }),
          createdAt: operation.createdAt,
          resolvedAt: operation.resolvedAt,
        }
      : {
          status: operation.status,
          operationId: operation.operationId,
          kind: operation.kind,
          requestDigest: operation.requestDigest,
          error: operation.error,
          createdAt: operation.createdAt,
          resolvedAt: operation.resolvedAt,
        };

// ── Service ──────────────────────────────────────────────────────

interface WorkCallContext {
  readonly projectId: string;
  readonly projectGeneration: number;
}

export class ProjectServiceWorkClient extends Context.Service<
  ProjectServiceWorkClient,
  {
    /**
     * The Project Service project list (GET /project/v1/) — the
     * ordinary-client-legal project info read the MCP tools use to map a
     * local project's directory onto its Project Service project.
     */
    readonly listProjects: () => Effect.Effect<
      readonly ProjectServiceProjectRecord[],
      ProjectServiceWorkClientError
    >;
    /** Current generation of the bound project; every Work read is fenced to it. */
    readonly getProjectGeneration: (
      projectId: string,
    ) => Effect.Effect<number, ProjectServiceWorkClientError>;
    readonly listPositions: (
      input: WorkCallContext,
    ) => Effect.Effect<readonly ProjectWorkPositionRecord[], ProjectServiceWorkClientError>;
    readonly listMy: (
      input: WorkCallContext & { readonly agentId: string },
    ) => Effect.Effect<readonly ProjectWorkRunRecord[], ProjectServiceWorkClientError>;
    readonly getRun: (
      input: WorkCallContext & {
        readonly runId: string;
        readonly agentId: string;
      },
    ) => Effect.Effect<ProjectWorkRunRecord | null, ProjectServiceWorkClientError>;
    /**
     * work-mission-v6: the submit addresses the MISSION — `runId` IS the
     * ms_-shaped missionId — and the mission revision (expectedRunRevision)
     * is the SOLE CAS fence; there is no assignment revision anymore.
     */
    readonly submitRun: (
      input: WorkCallContext & {
        readonly runId: string;
        readonly expectedRunRevision: string;
        readonly agentId: string;
        readonly result: Readonly<Record<string, unknown>>;
      },
      /** Server-generated recovery handle: surfaced on the uncertain path only. */
      operation: { readonly operationId: string; readonly idempotencyKey: string },
    ) => Effect.Effect<ProjectWorkOperationRecord, ProjectServiceWorkClientError>;
    readonly getOperation: (
      operationId: string,
    ) => Effect.Effect<ProjectWorkOperationRecord | null, ProjectServiceWorkClientError>;
    /**
     * Flow-document notary (by-run addressing): read a document through one of
     * the agent's open runs — the run's slot rights decide readability.
     */
    readonly readFlowDocument: (input: {
      readonly projectId: string;
      readonly runId: string;
      readonly agentId: string;
      readonly path: string;
    }) => Effect.Effect<ProjectFlowDocumentRecord, ProjectServiceWorkClientError>;
    /**
     * Notarized document write: returns the durable documentReceiptId to hand
     * to project_work_submit's result (documentReceiptIds) — the completion
     * validates it against the run's slot rights; only this path can mint one.
     *
     * work-mission-v6: the write is MISSION-addressed and documentId-keyed —
     * the mission contract's declaration fixes the path, so callers resolve
     * their path against the run detail's documentsResolved and pass its id.
     * There is no operation enum (one upsert semantic) and no delete.
     */
    readonly writeFlowDocument: (input: {
      readonly projectId: string;
      readonly runId: string;
      readonly agentId: string;
      readonly idempotencyKey: string;
      readonly documentId: string;
      readonly data?: string;
    }) => Effect.Effect<ProjectFlowDocumentWriteRecord, ProjectServiceWorkClientError>;
  }
>()("t3/projectService/ProjectServiceWorkClient") {}

interface ResolvedEndpoint {
  readonly baseUrl: string;
  readonly credential: string;
}

const normalizeBaseUrl = (baseUrl: string): string | null => {
  try {
    const url = new URL(baseUrl);
    if (url.protocol !== "http:" && url.protocol !== "https:") {
      return null;
    }
    return `${url.origin}${url.pathname.replace(/\/+$/, "")}`;
  } catch {
    return null;
  }
};

/** Decode at the trust boundary: an unexpected shape is an incompatibility, never a silent any. */
const decodeOrIncompatible = <A>(schema: Schema.Schema<A>, value: unknown): A => {
  const guard = Schema.is(schema);
  if (!guard(value)) {
    throw new ProjectServiceWorkApiIncompatibleError({ code: "PROJECT_WORK_RESPONSE_SHAPE" });
  }
  return value;
};

const isFailureEnvelope = Schema.is(FailureEnvelope);
const isUnavailable = Schema.is(ProjectServiceWorkUnavailableError);
const isApiIncompatible = Schema.is(ProjectServiceWorkApiIncompatibleError);
const isTransport = Schema.is(ProjectServiceWorkTransportError);

export const make = Effect.gen(function* () {
  const httpClient = yield* HttpClient.HttpClient;
  const serverSettings = yield* ServerSettings.ServerSettingsService;
  const secretStore = yield* ServerSecretStore.ServerSecretStore;

  // Settings carry only the redacted view; the raw credential is fetched from
  // the secret store per call so a rotated key takes effect immediately.
  const resolveEndpoint = Effect.gen(function* () {
    const settings = yield* serverSettings.getSettings.pipe(
      Effect.mapError(
        () => new ProjectServiceWorkUnavailableError({ reason: "integration-unreadable" }),
      ),
    );
    const client = settings.projectServiceClient;
    if (!client.enabled) {
      return yield* new ProjectServiceWorkUnavailableError({ reason: "integration-disabled" });
    }
    // Defense in depth: write validation already restricts the base URL, but
    // settings.json is editable out of band and this module sends the
    // credential to whatever host it names.
    if (!isLocalProjectServiceBaseUrl(client.baseUrl)) {
      return yield* new ProjectServiceWorkUnavailableError({ reason: "invalid-base-url" });
    }
    const base = normalizeBaseUrl(client.baseUrl);
    if (base === null) {
      return yield* new ProjectServiceWorkUnavailableError({ reason: "invalid-base-url" });
    }
    if (!client.credentialSet) {
      return yield* new ProjectServiceWorkUnavailableError({ reason: "no-credential" });
    }
    const secret = yield* secretStore
      .get(ServerSettings.PROJECT_SERVICE_CREDENTIAL_SECRET_NAME)
      .pipe(
        Effect.mapError(
          () => new ProjectServiceWorkUnavailableError({ reason: "integration-unreadable" }),
        ),
      );
    if (Option.isNone(secret)) {
      return yield* new ProjectServiceWorkUnavailableError({ reason: "no-credential" });
    }
    return { baseUrl: base, credential: new TextDecoder().decode(secret.value) };
  });

  const facetRequest = (
    method: string,
    url: string,
    body: unknown,
    credential: string,
  ): Effect.Effect<
    { readonly status: number; readonly ok: boolean; readonly value: unknown },
    ProjectServiceWorkTransportError
  > =>
    Effect.gen(function* () {
      const base =
        method === "GET"
          ? Effect.succeed(HttpClientRequest.get(url))
          : HttpClientRequest.bodyJson(HttpClientRequest.post(url), body ?? {});
      const request = yield* base.pipe(
        Effect.mapError(() => new ProjectServiceWorkTransportError({})),
      );
      const authorized = request.pipe(
        HttpClientRequest.setHeader("accept", "application/json"),
        HttpClientRequest.setHeader("authorization", `Bearer ${credential}`),
      );
      const response = yield* httpClient.execute(authorized).pipe(
        Effect.timeoutOption(FACET_TIMEOUT),
        Effect.mapError(() => new ProjectServiceWorkTransportError({})),
      );
      if (Option.isNone(response)) {
        return yield* new ProjectServiceWorkTransportError({});
      }
      const parsed = yield* response.value.json.pipe(
        Effect.mapError(() => new ProjectServiceWorkTransportError({})),
      );
      const { status } = response.value;
      return { status, ok: status >= 200 && status < 300, value: parsed };
    });

  // The transport the vendored SDK rides. Rejections surface as this module's
  // typed transport error so the SDK's own class stays reserved for typed
  // service envelopes.
  const transportFor = (endpoint: ResolvedEndpoint): ProjectConsumerWorkTransport => ({
    facet: <T>(
      method: string,
      path: string,
      body?: unknown,
      _timeoutMs?: number | undefined,
    ): Promise<{ readonly status: number; readonly ok: boolean; readonly value: T }> =>
      // The transport boundary carries raw JSON; the SDK owns decoding `value`
      // against its own schemas — hence the single unavoidable `value` cast.
      // `status` and `ok` are computed, never asserted.
      facetRequest(method, `${endpoint.baseUrl}${path}`, body, endpoint.credential).pipe(
        Effect.runPromise,
      ) as Promise<{ readonly status: number; readonly ok: boolean; readonly value: T }>,
  });

  const classify = (
    error: unknown,
    operation: { readonly operationId: string } | undefined,
  ): ProjectServiceWorkClientError => {
    if (isUnavailable(error) || isApiIncompatible(error)) {
      return error;
    }
    if (error instanceof ProjectConsumerWorkClientError) {
      return new ProjectServiceWorkServiceRejectedError({
        code: error.code,
        status: error.status,
        message: error.message,
        ...(error.details === undefined
          ? {}
          : { details: error.details as Record<string, unknown> }),
      });
    }
    if (isTransport(error) && operation === undefined) {
      return error;
    }
    return new ProjectServiceWorkTransportError(
      operation === undefined ? {} : { operationId: operation.operationId },
    );
  };

  const withClient = Effect.fn("ProjectServiceWorkClient.withClient")(function* <A>(
    operation: { readonly operationId: string } | undefined,
    invoke: (client: ProjectConsumerWorkClient) => Promise<unknown>,
    decode: (value: unknown) => A,
  ) {
    const endpoint = yield* resolveEndpoint;
    const client = new ProjectConsumerWorkClient({ httpClient: transportFor(endpoint) });
    return yield* Effect.tryPromise({
      try: async () => decode(await invoke(client)),
      catch: (error) => classify(error, operation),
    });
  });

  // A non-array body is a shape violation like any undecodable record — never
  // a silent [], which callers would read as "nothing registered" (issue #6
  // review). The throw stays inside the callers' catch boundaries.
  const decodeArrayOrIncompatible = <A>(schema: Schema.Schema<A>, value: unknown): readonly A[] => {
    if (!Array.isArray(value)) {
      throw new ProjectServiceWorkApiIncompatibleError({ code: "PROJECT_WORK_RESPONSE_SHAPE" });
    }
    return value.map((item) => decodeOrIncompatible(schema, item));
  };

  // GET /project/v1/ answers a bare ProjectRecord[] (id/name/workspaceDir);
  // extra fields drop at this trust boundary like on every other read. Unlike
  // the SDK-backed reads this facet path has no withClient catch, so the
  // decode runs inside Effect.try — a bad shape is a TYPED incompatibility
  // failure, never a Die defect escaping the Effect.fn body (issue #6 review).
  const listProjects = Effect.fn("ProjectServiceWorkClient.listProjects")(function* () {
    const endpoint = yield* resolveEndpoint;
    const { status, value } = yield* facetRequest(
      "GET",
      `${endpoint.baseUrl}/project/v1/`,
      undefined,
      endpoint.credential,
    );
    if (status < 200 || status >= 300) {
      if (isFailureEnvelope(value)) {
        return yield* new ProjectServiceWorkServiceRejectedError({
          code: value.error.code,
          status,
          message: value.error.message,
        });
      }
      return yield* new ProjectServiceWorkTransportError({});
    }
    const records = yield* Effect.try({
      try: () => decodeArrayOrIncompatible(ServiceProjectRecord, value),
      catch: (cause) =>
        isApiIncompatible(cause)
          ? cause
          : new ProjectServiceWorkApiIncompatibleError({ code: "PROJECT_WORK_RESPONSE_SHAPE" }),
    });
    return records.map(projectServiceProjectRecord);
  });

  const getProjectGeneration = Effect.fn("ProjectServiceWorkClient.getProjectGeneration")(
    function* (projectId: string) {
      const endpoint = yield* resolveEndpoint;
      const { status, value } = yield* facetRequest(
        "GET",
        `${endpoint.baseUrl}/project/v1/${encodeURIComponent(projectId)}`,
        undefined,
        endpoint.credential,
      );
      if (status === 404) {
        return yield* new ProjectServiceWorkServiceRejectedError({
          code: "PROJECT_NOT_FOUND",
          status,
          message: "The bound Project Service project does not exist.",
        });
      }
      if (status < 200 || status >= 300) {
        // This route answers a raw identity document, but its failures still
        // come back as typed envelopes — preserve the service's own code.
        if (isFailureEnvelope(value)) {
          return yield* new ProjectServiceWorkServiceRejectedError({
            code: value.error.code,
            status,
            message: value.error.message,
          });
        }
        return yield* new ProjectServiceWorkTransportError({});
      }
      return decodeOrIncompatible(Schema.Struct({ projectGeneration: PositiveInt }), value)
        .projectGeneration;
    },
  );

  // The SDK's input types carry its own opaque brands for wire ids; the values
  // here come from trusted server-side settings (never from tool arguments),
  // so each call site crosses the brand exactly once via `as`.
  const listPositionsArgs = (input: WorkCallContext) =>
    ({
      projectId: input.projectId,
      projectGeneration: input.projectGeneration,
    }) as Parameters<ProjectConsumerWorkClient["listPositions"]>[0];
  const listMyArgs = (input: WorkCallContext & { readonly agentId: string }) =>
    ({
      projectId: input.projectId,
      projectGeneration: input.projectGeneration,
      agentId: input.agentId,
    }) as Parameters<ProjectConsumerWorkClient["listMy"]>[0];
  const getRunArgs = (
    input: WorkCallContext & { readonly runId: string; readonly agentId: string },
  ) =>
    ({
      projectId: input.projectId,
      projectGeneration: input.projectGeneration,
      runId: input.runId,
      agentId: input.agentId,
    }) as Parameters<ProjectConsumerWorkClient["getRun"]>[0];

  /** One run's full (detail) view — the task snapshot and visit contract. */
  const runDetail = (
    input: WorkCallContext & { readonly runId: string; readonly agentId: string },
  ) =>
    withClient(
      undefined,
      (client) => client.getRun(getRunArgs(input)),
      (value) =>
        value === null || value === undefined
          ? null
          : projectRunRecord(decodeOrIncompatible(ProjectWorkRunRecord, value)),
    );

  return ProjectServiceWorkClient.of({
    listProjects,
    getProjectGeneration,
    listPositions: (input) =>
      withClient(
        undefined,
        (client) => client.listPositions(listPositionsArgs(input)),
        (value) =>
          decodeArrayOrIncompatible(ProjectWorkPositionRecord, value).map(projectPositionRecord),
      ),
    /**
     * work-mission-v6: the service answers this list with SUMMARY
     * projections (top-level mission identity, no task snapshot — the
     * prompt/documents/rights are the detail read's payload). Every OPEN run
     * is hydrated through getRun so queue keys, the visit contract, and
     * prompts read one full shape everywhere downstream. A detail 404 means
     * the run resolved between the two reads: the run drops out — a fresh
     * list would not have shown it open either. Any other detail failure
     * fails the whole call like any work-query failure (redelivered later).
     */
    listMy: (input) =>
      Effect.gen(function* () {
        const summaries = yield* withClient(
          undefined,
          (client) => client.listMy(listMyArgs(input)),
          (value) => decodeArrayOrIncompatible(ProjectWorkRunRecord, value).map(projectRunRecord),
        );
        const runs: Array<ProjectWorkRunRecord> = [];
        for (const run of summaries) {
          if (run.state !== "open") {
            runs.push(run);
            continue;
          }
          const detail = yield* runDetail({ ...input, runId: run.runId }).pipe(
            Effect.catchIf(
              (error): error is ProjectServiceWorkServiceRejectedError =>
                isServiceRejected(error) && error.status === 404,
              () => Effect.succeed(null),
            ),
          );
          if (detail !== null) {
            runs.push(detail);
          }
        }
        return runs;
      }),
    getRun: (input) => runDetail(input),
    /**
     * work-mission-v6: run.submit addresses the MISSION (runId IS the
     * ms_-shaped missionId) and the mission revision is the SOLE CAS fence.
     * The vendored SDK 0.16 still builds the pre-v6 command (runId +
     * expectedAssignmentRevision), which v6's exact-field validation rejects —
     * so the submit rides the facet transport directly (the listProjects
     * precedent); the frozen tarball is bypassed, not patched.
     */
    submitRun: (input, operation) =>
      Effect.gen(function* () {
        const endpoint = yield* resolveEndpoint;
        const transportError = () =>
          new ProjectServiceWorkTransportError({ operationId: operation.operationId });
        const { status, value } = yield* facetRequest(
          "POST",
          `${endpoint.baseUrl}/project/v1/${encodeURIComponent(input.projectId)}/work/execute`,
          {
            operationId: operation.operationId,
            idempotencyKey: operation.idempotencyKey,
            projectGeneration: input.projectGeneration,
            command: {
              kind: "run.submit",
              missionId: input.runId,
              expectedRunRevision: input.expectedRunRevision,
              agentId: input.agentId,
              result: input.result,
            },
          },
          endpoint.credential,
        ).pipe(Effect.mapError(() => transportError()));
        if (status < 200 || status >= 300) {
          if (isFailureEnvelope(value)) {
            return yield* new ProjectServiceWorkServiceRejectedError({
              code: value.error.code,
              status,
              message: value.error.message,
            });
          }
          return yield* transportError();
        }
        // The route answers the {ok, result} envelope; the operation record
        // rides `result`. A synchronous rejection is a REJECTED RECORD (HTTP
        // 200), not a failure envelope — the handler surfaces those.
        const record = (value as { readonly result?: unknown }).result;
        return yield* Effect.try({
          try: () =>
            projectOperationRecord(decodeOrIncompatible(ProjectWorkOperationRecord, record)),
          catch: (cause) =>
            isApiIncompatible(cause)
              ? cause
              : new ProjectServiceWorkApiIncompatibleError({ code: "PROJECT_WORK_RESPONSE_SHAPE" }),
        });
      }),
    getOperation: (operationId) =>
      withClient(
        undefined,
        (client) =>
          client.getOperation(
            operationId as Parameters<ProjectConsumerWorkClient["getOperation"]>[0],
          ),
        (value) =>
          value === null || value === undefined
            ? null
            : projectOperationRecord(decodeOrIncompatible(ProjectWorkOperationRecord, value)),
      ),
    /**
     * work-mission-v6: the read is path-keyed (`/documents/by-run/:runId/*`)
     * but carries no revision — the vendored SDK 0.16 validates a required
     * revision string and rejects every real v6 answer, so this rides the
     * facet transport directly (the frozen tarball is bypassed, not patched).
     */
    readFlowDocument: (input) =>
      Effect.gen(function* () {
        const endpoint = yield* resolveEndpoint;
        const path = input.path.replace(/^\/+/, "");
        const { status, value } = yield* facetRequest(
          "GET",
          `${endpoint.baseUrl}/project/v1/${encodeURIComponent(input.projectId)}` +
            `/mission/documents/by-run/${encodeURIComponent(input.runId)}/${path}` +
            `?agentId=${encodeURIComponent(input.agentId)}`,
          undefined,
          endpoint.credential,
        );
        if (status < 200 || status >= 300) {
          if (isFailureEnvelope(value)) {
            return yield* new ProjectServiceWorkServiceRejectedError({
              code: value.error.code,
              status,
              message: value.error.message,
            });
          }
          return yield* new ProjectServiceWorkTransportError({});
        }
        const wire = decodeOrIncompatible(
          WIRE_FLOW_DOCUMENT_RECORD,
          (value as { readonly result?: unknown }).result,
        );
        const data = Buffer.from(wire.data, "base64");
        return {
          content: data.toString("utf8"),
          revision: wire.revision ?? null,
          displayPath: wire.displayPath,
          size: wire.size ?? data.byteLength,
        };
      }),
    /**
     * work-mission-v6: the notarized write is documentId-keyed (the mission
     * contract's declaration fixes the path) — {idempotencyKey, documentId,
     * data(base64)} — and the receipt answers {documentReceiptId, displayPath}
     * with no successor revision. Same SDK-bypass rationale as the read.
     */
    writeFlowDocument: (input) =>
      Effect.gen(function* () {
        const endpoint = yield* resolveEndpoint;
        const { status, value } = yield* facetRequest(
          "POST",
          `${endpoint.baseUrl}/project/v1/${encodeURIComponent(input.projectId)}` +
            `/mission/documents/by-run/${encodeURIComponent(input.runId)}`,
          {
            idempotencyKey: input.idempotencyKey,
            documentId: input.documentId,
            agentId: input.agentId,
            ...(input.data === undefined ? {} : { data: input.data }),
          },
          endpoint.credential,
        );
        if (status < 200 || status >= 300) {
          if (isFailureEnvelope(value)) {
            return yield* new ProjectServiceWorkServiceRejectedError({
              code: value.error.code,
              status,
              message: value.error.message,
            });
          }
          return yield* new ProjectServiceWorkTransportError({});
        }
        const receipt = decodeOrIncompatible(
          Schema.Struct({ documentReceiptId: Schema.String, displayPath: Schema.String }),
          (value as { readonly result?: unknown }).result,
        );
        return {
          documentReceiptId: receipt.documentReceiptId,
          revision: null,
          displayPath: receipt.displayPath,
        };
      }),
  });
});

export const layer = Layer.effect(ProjectServiceWorkClient, make);
