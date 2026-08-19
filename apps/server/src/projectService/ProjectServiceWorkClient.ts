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
import { isLocalProjectServiceBaseUrl, PositiveInt } from "@t3tools/contracts";
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
  { code: Schema.String, status: Schema.Int, message: Schema.String },
) {}

export type ProjectServiceWorkClientError =
  | ProjectServiceWorkUnavailableError
  | ProjectServiceWorkApiIncompatibleError
  | ProjectServiceWorkTransportError
  | ProjectServiceWorkServiceRejectedError;

// ── Wire records (the subset the tools surface; extra fields drop) ──

const AggregateRevisionSchema = PositiveInt;

export const ProjectWorkPositionRecord = Schema.Struct({
  positionId: Schema.String,
  displayName: Schema.String,
  assignmentRevision: AggregateRevisionSchema,
});
export type ProjectWorkPositionRecord = typeof ProjectWorkPositionRecord.Type;

export const ProjectWorkRunRecord = Schema.Struct({
  runId: Schema.String,
  positionId: Schema.String,
  runRevision: AggregateRevisionSchema,
  state: Schema.Literals(["open", "completed", "superseded", "cancelled"]),
  agentId: Schema.optional(Schema.String),
  task: Schema.Record(Schema.String, Schema.Unknown),
  createdAt: Schema.String,
  resolvedAt: Schema.optional(Schema.NullOr(Schema.String)),
});
export type ProjectWorkRunRecord = typeof ProjectWorkRunRecord.Type;

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
});

const EnvelopeError = Schema.Struct({
  code: Schema.String,
  message: Schema.String,
});

/** A failure envelope on routes this module calls directly (not via the SDK). */
const FailureEnvelope = Schema.Struct({ ok: Schema.Literals([false]), error: EnvelopeError });

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
const projectRunRecord = (run: ProjectWorkRunRecord): ProjectWorkRunRecord => ({
  runId: run.runId,
  positionId: run.positionId,
  runRevision: run.runRevision,
  state: run.state,
  ...(run.agentId === undefined ? {} : { agentId: run.agentId }),
  task: run.task,
  createdAt: run.createdAt,
  ...(run.resolvedAt === undefined ? {} : { resolvedAt: run.resolvedAt }),
});

const projectPositionRecord = (position: ProjectWorkPositionRecord): ProjectWorkPositionRecord => ({
  positionId: position.positionId,
  displayName: position.displayName,
  assignmentRevision: position.assignmentRevision,
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
    readonly submitRun: (
      input: WorkCallContext & {
        readonly runId: string;
        readonly expectedRunRevision: number;
        readonly expectedAssignmentRevision: number;
        readonly agentId: string;
        readonly result: Readonly<Record<string, unknown>>;
      },
      /** Server-generated recovery handle: surfaced on the uncertain path only. */
      operation: { readonly operationId: string; readonly idempotencyKey: string },
    ) => Effect.Effect<ProjectWorkOperationRecord, ProjectServiceWorkClientError>;
    readonly getOperation: (
      operationId: string,
    ) => Effect.Effect<ProjectWorkOperationRecord | null, ProjectServiceWorkClientError>;
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
  const submitRunArgs = (
    input: WorkCallContext & {
      readonly runId: string;
      readonly expectedRunRevision: number;
      readonly expectedAssignmentRevision: number;
      readonly agentId: string;
      readonly result: Readonly<Record<string, unknown>>;
    },
    operation: { readonly operationId: string; readonly idempotencyKey: string },
  ) =>
    ({
      projectId: input.projectId,
      meta: {
        operationId: operation.operationId,
        idempotencyKey: operation.idempotencyKey,
        projectGeneration: input.projectGeneration,
      },
      runId: input.runId,
      expectedRunRevision: input.expectedRunRevision,
      expectedAssignmentRevision: input.expectedAssignmentRevision,
      agentId: input.agentId,
      result: input.result,
    }) as unknown as Parameters<ProjectConsumerWorkClient["submitRun"]>[0];

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
    listMy: (input) =>
      withClient(
        undefined,
        (client) => client.listMy(listMyArgs(input)),
        (value) => decodeArrayOrIncompatible(ProjectWorkRunRecord, value).map(projectRunRecord),
      ),
    getRun: (input) =>
      withClient(
        undefined,
        (client) => client.getRun(getRunArgs(input)),
        (value) =>
          value === null || value === undefined
            ? null
            : projectRunRecord(decodeOrIncompatible(ProjectWorkRunRecord, value)),
      ),
    submitRun: (input, operation) =>
      withClient(
        operation,
        (client) => client.submitRun(submitRunArgs(input, operation)),
        (value) => projectOperationRecord(decodeOrIncompatible(ProjectWorkOperationRecord, value)),
      ),
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
  });
});

export const layer = Layer.effect(ProjectServiceWorkClient, make);
