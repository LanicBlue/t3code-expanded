import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import {
  HttpClient,
  HttpClientError,
  HttpClientRequest,
  HttpClientResponse,
} from "effect/unstable/http";

import * as ServerSecretStore from "../auth/ServerSecretStore.ts";
import * as ServerConfig from "../config.ts";
import * as ServerSettingsModule from "../serverSettings.ts";
import * as ProjectServiceWorkClient from "./ProjectServiceWorkClient.ts";

/** Compare tagged errors by their own enumerable fields (schema classes carry runtime extras). */
const plainError = (error: object): Record<string, unknown> =>
  Object.fromEntries(Object.entries(error).filter(([, value]) => typeof value !== "function"));

/** Deep scan for leaked credential material without JSON.stringify. */
const containsValue = (value: unknown, needle: string): boolean => {
  if (typeof value === "string") return value.includes(needle);
  if (Array.isArray(value)) return value.some((item) => containsValue(item, needle));
  if (value !== null && typeof value === "object") {
    return Object.values(value).some((item) => containsValue(item, needle));
  }
  return false;
};

const CREDENTIAL = "psk_key-1.s3cret";

type Responder = (
  request: HttpClientRequest.HttpClientRequest,
) => Response | HttpClientError.HttpClientError;

const makeHttpClient = (respond: Responder) => {
  const requests: HttpClientRequest.HttpClientRequest[] = [];
  const client = HttpClient.make((request) => {
    requests.push(request);
    const outcome = respond(request);
    return outcome instanceof Response
      ? Effect.succeed(HttpClientResponse.fromWeb(request, outcome))
      : Effect.fail(outcome);
  });
  return { client, requests };
};

const decodeJsonText = Schema.decodeUnknownSync(Schema.fromJsonString(Schema.Unknown));

const jsonBodyOf = (
  request: HttpClientRequest.HttpClientRequest | undefined,
): Record<string, unknown> => {
  const body = request?.body as { readonly _tag?: string; readonly body?: Uint8Array } | undefined;
  if (body?._tag !== "Uint8Array" || !(body.body instanceof Uint8Array)) return {};
  const decoded = decodeJsonText(new TextDecoder().decode(body.body));
  return (decoded ?? {}) as Record<string, unknown>;
};

const transportFailure: Responder = (request) =>
  new HttpClientError.HttpClientError({
    reason: new HttpClientError.TransportError({ request, cause: new Error("connection refused") }),
  });

const IDENTITY = {
  projectId: "proj_ps_1",
  projectGeneration: 7,
  name: "PS",
  workspaceRef: "ws",
  lifecycle: "active",
};

const RUN_VIEW = {
  runId: "run_9",
  projectId: "proj_ps_1",
  projectGeneration: 7,
  positionId: "pos_1",
  workspaceRef: "ws",
  state: "open",
  runRevision: "run:3",
  agentId: "ag_one",
  executorRef: "client-1:ag_one",
  task: { prompt: "Summarize the wiki" },
  createdAt: "2026-08-01T00:00:00.000Z",
  resolvedAt: null,
  // apiMinor 2: the completion contract rides the run view; the client-side
  // projection must carry it through to the MCP tools. SDK 0.16 generation:
  // the visit-only action view.
  action: {
    kind: "visit",
    outcomes: ["implementation-ready"],
    candidates: ["validation"],
    abandonAvailable: true,
  },
};

const POSITION_VIEW = {
  positionId: "pos_1",
  projectId: "proj_ps_1",
  owner: { type: "standalone", workDefinitionId: "wd_1" },
  displayName: "Summarizer",
  executor: { type: "agent", executorRef: "client-1:ag_one" },
  assignmentRevision: "position:5",
};

/**
 * A visit-population run view per the pinned work-mission-v5 §6.1 contract
 * and the SDK 0.16 WorkRunView: task.mission replaces task.instance,
 * task.work carries the workspace group, and the run-level `action` field is
 * the visit completion contract (outcome vocabulary + candidate next
 * stations) — action rides BESIDE task on the wire, never inside it.
 */
const VISIT_RUN_VIEW = {
  runId: "run_v1",
  projectId: "proj_ps_1",
  projectGeneration: 7,
  positionId: "pos_implement",
  workspaceRef: "wt-ms_a",
  state: "open",
  runRevision: "run:9",
  agentId: "ag_one",
  executorRef: "client-1:ag_one",
  task: {
    prompt: "以 design.md 为唯一需求 authority 实现登录修复",
    mission: { id: "ms_a", name: "Release v2", objective: "Ship the release" },
    work: { group: "ms_a", workKey: "implement", iteration: 1 },
    executor: { type: "agent", executorRef: "client-1:ag_one" },
  },
  action: {
    kind: "visit",
    outcomes: ["implementation-ready", "design-clarification"],
    candidates: ["validation"],
    abandonAvailable: true,
  },
  createdAt: "2026-08-27T00:00:00.000Z",
  resolvedAt: null,
};

const OPERATION_VIEW = {
  operationId: "op_123",
  kind: "work.execute",
  status: "committed",
  requestDigest: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
  result: { runId: "run_9", state: "completed" },
  // The service stores aggregate revisions as opaque strings, never numbers.
  revision: "run:4",
  resolvedAt: "2026-08-01T00:01:00.000Z",
  createdAt: "2026-08-01T00:00:59.000Z",
};

/**
 * work-mission-v6 list projections (GET work-runs/my): the mission identity
 * rides the TOP level and there is NO task snapshot — the prompt/documents/
 * rights are the detail read's payload. listMy hydrates open runs through
 * getRun (run_v1's detail is VISIT_RUN_VIEW); completed summaries pass through.
 */
const RUN_SUMMARY_OPEN = {
  runId: "run_v1",
  projectId: "proj_ps_1",
  projectGeneration: 7,
  positionId: "pos_implement",
  state: "open",
  mission: { id: "ms_a", name: "Release v2", objective: "Ship the release" },
  iteration: 1,
  executor: { type: "agent", executorRef: "client-1:ag_one" },
  runRevision: "run:9",
  createdAt: "2026-08-27T00:00:00.000Z",
  resolvedAt: null,
  runningSeconds: 60,
  stationSeconds: 30,
};

const RUN_SUMMARY_COMPLETED = {
  ...RUN_SUMMARY_OPEN,
  runId: "run_c1",
  positionId: "pos_review",
  state: "completed",
  mission: { id: "ms_b", name: "Review v2", objective: "Review the release" },
  runRevision: "run:2",
  resolvedAt: "2026-08-27T00:10:00.000Z",
};

const okBody = (result: unknown) => Response.json({ ok: true, result });

/** Happy-path routes; an `override` responder wins over every route. */
const serviceByPath =
  (override?: Responder): Responder =>
  (request) => {
    if (override !== undefined) return override(request);
    const url = new URL(request.url);
    if (url.pathname === "/project/v1/proj_ps_1") return okBody(IDENTITY);
    if (url.pathname === "/project/v1/proj_ps_1/work-runs/my") return okBody([RUN_VIEW]);
    if (url.pathname === "/project/v1/proj_ps_1/work-runs/run_9") return okBody(RUN_VIEW);
    if (url.pathname === "/project/v1/proj_ps_1/work-positions") return okBody([POSITION_VIEW]);
    if (url.pathname === "/project/v1/proj_ps_1/work/execute") return okBody(OPERATION_VIEW);
    if (url.pathname === "/project/v1/operations/op_123") return okBody(OPERATION_VIEW);
    return transportFailure(request);
  };

const makeLayer = (client: HttpClient.HttpClient) => {
  const configLayer = Layer.fresh(
    ServerConfig.layerTest(process.cwd(), { prefix: "t3code-ps-work-client-" }),
  );
  const secretStoreLayer = ServerSecretStore.layer.pipe(Layer.provideMerge(configLayer));
  const settingsLayer = ServerSettingsModule.layer.pipe(Layer.provideMerge(secretStoreLayer));
  return ProjectServiceWorkClient.layer.pipe(
    Layer.provideMerge(settingsLayer),
    Layer.provide(Layer.succeed(HttpClient.HttpClient, client)),
  );
};

const enableClient = Effect.gen(function* () {
  const serverSettings = yield* ServerSettingsModule.ServerSettingsService;
  yield* serverSettings.updateSettings({
    projectServiceClient: { enabled: true, newCredential: CREDENTIAL },
  });
});

it.layer(NodeServices.layer)("ProjectServiceWorkClient", (it) => {
  it.effect("resolves the generation, carries the bearer credential, and unwraps envelopes", () => {
    const { client, requests } = makeHttpClient(serviceByPath());

    return Effect.gen(function* () {
      yield* enableClient;
      const work = yield* ProjectServiceWorkClient.ProjectServiceWorkClient;

      const runs = yield* work.listMy({
        projectId: "proj_ps_1",
        projectGeneration: 7,
        agentId: "ag_one",
      });
      const positions = yield* work.listPositions({ projectId: "proj_ps_1", projectGeneration: 7 });

      assert.deepEqual(runs, [
        {
          runId: "run_9",
          positionId: "pos_1",
          runRevision: "run:3",
          state: "open",
          agentId: "ag_one",
          task: { prompt: "Summarize the wiki" },
          createdAt: "2026-08-01T00:00:00.000Z",
          resolvedAt: null,
          action: {
            kind: "visit",
            outcomes: ["implementation-ready"],
            candidates: ["validation"],
            abandonAvailable: true,
          },
        },
      ]);
      assert.deepEqual(positions, [
        { positionId: "pos_1", displayName: "Summarizer", assignmentRevision: "position:5" },
      ]);

      const listMyRequest = requests.find((request) => request.url.includes("/work-runs/my"));
      assert.strictEqual(listMyRequest?.headers["authorization"], `Bearer ${CREDENTIAL}`);
      assert.match(listMyRequest?.url ?? "", /projectGeneration=7/);
      assert.match(listMyRequest?.url ?? "", /agentId=ag_one/);
    }).pipe(Effect.provide(makeLayer(client)));
  });

  it.effect("decodes v6.2 positions — the occupancy token, no assignment CAS", () => {
    // work-mission-v6.2: positions answer `occupancy` (hash of the executor
    // ref) and dropped the assignmentRevision counter ("last-write-wins — no
    // assignment CAS on the wire"); the decode must not require the counter.
    const v6Positions: Responder = (request) => {
      const url = new URL(request.url);
      if (url.pathname === "/project/v1/proj_ps_1/work-positions") {
        return okBody([
          {
            positionId: "pos_1",
            projectId: "proj_ps_1",
            owner: { type: "project" },
            displayName: "Summarizer",
            executor: { type: "agent", executorRef: "client-1:ag_one" },
            occupancy: 2656310283,
          },
        ]);
      }
      return transportFailure(request);
    };
    const { client } = makeHttpClient(serviceByPath(v6Positions));

    return Effect.gen(function* () {
      yield* enableClient;
      const work = yield* ProjectServiceWorkClient.ProjectServiceWorkClient;

      const positions = yield* work.listPositions({ projectId: "proj_ps_1", projectGeneration: 7 });
      assert.deepEqual(positions, [
        { positionId: "pos_1", displayName: "Summarizer", occupancy: 2656310283 },
      ]);
    }).pipe(Effect.provide(makeLayer(client)));
  });

  it.effect("reads and writes mission documents on the v6 routes", () => {
    const content = "# decision\nship it\n";
    const encoded = Buffer.from(content, "utf8").toString("base64");
    const routes: Responder = (request) => {
      const url = new URL(request.url);
      if (
        url.pathname ===
        "/project/v1/proj_ps_1/mission/documents/by-run/run_9/design/TARGET-BLUEPRINT.md"
      ) {
        return okBody({
          // work-mission-v6 reads carry no revision.
          displayPath: "mission://project/ms_1/design/TARGET-BLUEPRINT.md",
          size: 18,
          data: encoded,
        });
      }
      if (url.pathname === "/project/v1/proj_ps_1/mission/documents/by-run/run_9") {
        return okBody({
          documentReceiptId: "rcpt_1",
          displayPath: "mission://project/ms_1/<decision>",
        });
      }
      return transportFailure(request);
    };
    const { client, requests } = makeHttpClient(serviceByPath(routes));

    return Effect.gen(function* () {
      yield* enableClient;
      const work = yield* ProjectServiceWorkClient.ProjectServiceWorkClient;

      const doc = yield* work.readFlowDocument({
        projectId: "proj_ps_1",
        runId: "run_9",
        agentId: "ag_one",
        path: "design/TARGET-BLUEPRINT.md",
      });
      assert.deepEqual(doc, {
        content,
        revision: null,
        displayPath: "mission://project/ms_1/design/TARGET-BLUEPRINT.md",
        size: 18,
      });

      const receipt = yield* work.writeFlowDocument({
        projectId: "proj_ps_1",
        runId: "run_9",
        agentId: "ag_one",
        idempotencyKey: "idem_1",
        documentId: "decision",
        data: encoded,
      });
      assert.deepEqual(receipt, {
        documentReceiptId: "rcpt_1",
        revision: null,
        displayPath: "mission://project/ms_1/<decision>",
      });
      // The write is documentId-keyed — the pre-v6 path/operation pair is gone.
      const writeRequest = requests.find(
        (request) =>
          request.url.includes("/mission/documents/by-run/run_9") && request.method === "POST",
      );
      const body = jsonBodyOf(writeRequest);
      assert.deepEqual(body, {
        idempotencyKey: "idem_1",
        documentId: "decision",
        agentId: "ag_one",
        data: encoded,
      });
    }).pipe(Effect.provide(makeLayer(client)));
  });

  it.effect("decodes the mission visit view onto the run record", () => {
    const visitByPath: Responder = (request) => {
      const url = new URL(request.url);
      if (url.pathname === "/project/v1/proj_ps_1/work-runs/my") return okBody([VISIT_RUN_VIEW]);
      if (url.pathname === "/project/v1/proj_ps_1/work-runs/run_v1") return okBody(VISIT_RUN_VIEW);
      return transportFailure(request);
    };
    const { client } = makeHttpClient(serviceByPath(visitByPath));

    return Effect.gen(function* () {
      yield* enableClient;
      const work = yield* ProjectServiceWorkClient.ProjectServiceWorkClient;

      const runs = yield* work.listMy({
        projectId: "proj_ps_1",
        projectGeneration: 7,
        agentId: "ag_one",
      });
      // The visit view rides the record as the DECODED projection of the
      // task's mission/work blocks plus the run-level action field; the raw
      // task passes through untouched (the prompt and other task facts stay
      // where the wire put them).
      assert.deepEqual(runs, [
        {
          runId: "run_v1",
          positionId: "pos_implement",
          runRevision: "run:9",
          state: "open",
          agentId: "ag_one",
          task: VISIT_RUN_VIEW.task,
          createdAt: "2026-08-27T00:00:00.000Z",
          resolvedAt: null,
          action: {
            kind: "visit",
            outcomes: ["implementation-ready", "design-clarification"],
            candidates: ["validation"],
            abandonAvailable: true,
          },
          visit: {
            mission: { id: "ms_a", name: "Release v2", objective: "Ship the release" },
            work: { group: "ms_a", workKey: "implement", iteration: 1 },
            action: {
              kind: "visit",
              outcomes: ["implementation-ready", "design-clarification"],
              candidates: ["validation"],
              abandonAvailable: true,
            },
          },
        },
      ]);

      const run = yield* work.getRun({
        projectId: "proj_ps_1",
        projectGeneration: 7,
        runId: "run_v1",
        agentId: "ag_one",
      });
      assert.deepEqual(run?.visit?.mission.id, "ms_a");
      assert.deepEqual(run?.visit?.work.group, "ms_a");
    }).pipe(Effect.provide(makeLayer(client)));
  });

  it.effect("hydrates v6 summary list projections through the detail read", () => {
    const summaryRoutes: Responder = (request) => {
      const url = new URL(request.url);
      if (url.pathname === "/project/v1/proj_ps_1/work-runs/my")
        return okBody([RUN_SUMMARY_COMPLETED, RUN_SUMMARY_OPEN]);
      if (url.pathname === "/project/v1/proj_ps_1/work-runs/run_v1") return okBody(VISIT_RUN_VIEW);
      return transportFailure(request);
    };
    const { client, requests } = makeHttpClient(serviceByPath(summaryRoutes));

    return Effect.gen(function* () {
      yield* enableClient;
      const work = yield* ProjectServiceWorkClient.ProjectServiceWorkClient;

      const runs = yield* work.listMy({
        projectId: "proj_ps_1",
        projectGeneration: 7,
        agentId: "ag_one",
      });
      // The OPEN summary is replaced by its hydrated detail — task snapshot,
      // visit contract, everything downstream reads.
      assert.deepEqual(runs, [
        {
          runId: "run_c1",
          positionId: "pos_review",
          runRevision: "run:2",
          state: "completed",
          mission: { id: "ms_b", name: "Review v2", objective: "Review the release" },
          createdAt: "2026-08-27T00:00:00.000Z",
          resolvedAt: "2026-08-27T00:10:00.000Z",
        },
        {
          runId: "run_v1",
          positionId: "pos_implement",
          runRevision: "run:9",
          state: "open",
          agentId: "ag_one",
          task: VISIT_RUN_VIEW.task,
          createdAt: "2026-08-27T00:00:00.000Z",
          resolvedAt: null,
          action: {
            kind: "visit",
            outcomes: ["implementation-ready", "design-clarification"],
            candidates: ["validation"],
            abandonAvailable: true,
          },
          visit: {
            mission: { id: "ms_a", name: "Release v2", objective: "Ship the release" },
            work: { group: "ms_a", workKey: "implement", iteration: 1 },
            action: {
              kind: "visit",
              outcomes: ["implementation-ready", "design-clarification"],
              candidates: ["validation"],
              abandonAvailable: true,
            },
          },
        },
      ]);
      // Exactly one detail read, for the open run only, fenced to the same
      // generation and agent scope as the list call.
      const detailRequests = requests.filter((request) => request.url.includes("/work-runs/run_"));
      assert.lengthOf(detailRequests, 1);
      assert.match(detailRequests[0]?.url ?? "", /work-runs\/run_v1/);
      assert.match(detailRequests[0]?.url ?? "", /projectGeneration=7/);
      assert.match(detailRequests[0]?.url ?? "", /agentId=ag_one/);
      assert.strictEqual(detailRequests[0]?.headers["authorization"], `Bearer ${CREDENTIAL}`);
    }).pipe(Effect.provide(makeLayer(client)));
  });

  it.effect("drops an open summary whose detail read 404s, but fails on other rejections", () => {
    // The run resolved between the list and the detail read: the 404 drops it
    // (a fresh list would not have shown it open). Any other rejection —
    // here a 403 — fails the whole call like any work-query failure.
    const gone =
      (detailStatus: number): Responder =>
      (request) => {
        const url = new URL(request.url);
        if (url.pathname === "/project/v1/proj_ps_1/work-runs/my")
          return okBody([RUN_SUMMARY_OPEN]);
        if (url.pathname === "/project/v1/proj_ps_1/work-runs/run_v1")
          return Response.json(
            {
              ok: false,
              error: { code: "PROJECT_WORK_RUN_NOT_FOUND", message: "the run is gone" },
            },
            { status: detailStatus },
          );
        return transportFailure(request);
      };

    const droppedOn404 = Effect.gen(function* () {
      yield* enableClient;
      const work = yield* ProjectServiceWorkClient.ProjectServiceWorkClient;
      const dropped = yield* work.listMy({
        projectId: "proj_ps_1",
        projectGeneration: 7,
        agentId: "ag_one",
      });
      assert.deepEqual(dropped, []);
    }).pipe(Effect.provide(makeLayer(makeHttpClient(gone(404)).client)));

    const failsOn403 = Effect.gen(function* () {
      yield* enableClient;
      const work = yield* ProjectServiceWorkClient.ProjectServiceWorkClient;
      const failure = yield* work
        .listMy({ projectId: "proj_ps_1", projectGeneration: 7, agentId: "ag_one" })
        .pipe(Effect.flip);
      assert.deepEqual(plainError(failure), {
        _tag: "ProjectServiceWorkServiceRejectedError",
        code: "PROJECT_WORK_RUN_NOT_FOUND",
        status: 403,
      });
      assert.equal(failure.message, "the run is gone");
    }).pipe(Effect.provide(makeLayer(makeHttpClient(gone(403)).client)));

    return Effect.gen(function* () {
      yield* droppedOn404;
      yield* failsOn403;
    });
  });

  it.effect(
    "a task that declares the mission population but fails the pinned shape is an incompatibility",
    () => {
      const halfMission = {
        ...VISIT_RUN_VIEW,
        task: {
          prompt: "x",
          // mission present (the population discriminator) but the work block
          // is missing — not a silently-ignored block.
          mission: { id: "ms_a", name: "Release v2", objective: "Ship the release" },
        },
      };
      // The completion contract lives in the run-level action field: a visit
      // run without it is a broken projection, not a degraded-but-OK read.
      const actionless = { ...VISIT_RUN_VIEW, action: undefined };
      const { client } = makeHttpClient(
        serviceByPath((request) => {
          const url = new URL(request.url);
          const body = url.pathname.endsWith("/work-runs/my") ? [halfMission, actionless] : null;
          if (body !== null) return okBody(body);
          return transportFailure(request);
        }),
      );

      return Effect.gen(function* () {
        yield* enableClient;
        const work = yield* ProjectServiceWorkClient.ProjectServiceWorkClient;

        const failure = yield* work
          .listMy({ projectId: "proj_ps_1", projectGeneration: 7, agentId: "ag_one" })
          .pipe(Effect.flip);
        assert.deepEqual(plainError(failure), {
          _tag: "ProjectServiceWorkApiIncompatibleError",
          code: "PROJECT_WORK_RESPONSE_SHAPE",
        });
      }).pipe(Effect.provide(makeLayer(client)));
    },
  );

  it.effect("never leaks the raw credential into any result or error payload", () => {
    const { client } = makeHttpClient(
      serviceByPath(() =>
        Response.json(
          {
            ok: false,
            error: {
              code: "PROJECT_CLIENT_AUTHENTICATION_INVALID",
              message: "unknown or invalid service-client credential",
            },
          },
          { status: 401 },
        ),
      ),
    );

    return Effect.gen(function* () {
      yield* enableClient;
      const work = yield* ProjectServiceWorkClient.ProjectServiceWorkClient;

      const failure = yield* work
        .listMy({ projectId: "proj_ps_1", projectGeneration: 7, agentId: "ag_one" })
        .pipe(Effect.flip);

      assert.include(failure._tag, "ServiceRejected");
      assert.strictEqual(
        (failure as { readonly code?: string }).code,
        "PROJECT_CLIENT_AUTHENTICATION_INVALID",
      );
      assert.isFalse(containsValue(failure, "psk_"));
      assert.isFalse(containsValue(failure, "s3cret"));
    }).pipe(Effect.provide(makeLayer(client)));
  });

  it.effect(
    "submits with the server-supplied operation handle and decodes the operation record",
    () => {
      const { client, requests } = makeHttpClient(serviceByPath());

      return Effect.gen(function* () {
        yield* enableClient;
        const work = yield* ProjectServiceWorkClient.ProjectServiceWorkClient;

        const operation = yield* work.submitRun(
          {
            projectId: "proj_ps_1",
            projectGeneration: 7,
            runId: "run_9",
            expectedRunRevision: "run:3",
            agentId: "ag_one",
            result: { kind: "standalone", output: "done" },
          },
          { operationId: "op_123", idempotencyKey: "idem_123" },
        );

        assert.equal(operation.status, "committed");
        if (operation.status === "committed") {
          assert.equal(operation.operationId, "op_123");
          assert.equal(operation.revision, "run:4");
        }
        const submit = requests.find((request) => request.url.includes("/work/execute"));
        assert.strictEqual(submit?.headers["authorization"], `Bearer ${CREDENTIAL}`);
        const body = jsonBodyOf(submit);
        assert.equal(body.operationId, "op_123");
        assert.equal(body.idempotencyKey, "idem_123");
        // work-mission-v6: the command addresses the MISSION (runId IS the
        // missionId) and the mission revision is the SOLE fence — the
        // pre-v6 runId/expectedAssignmentRevision pair must be gone.
        assert.deepEqual(body.command, {
          kind: "run.submit",
          missionId: "run_9",
          expectedRunRevision: "run:3",
          agentId: "ag_one",
          result: { kind: "standalone", output: "done" },
        });
      }).pipe(Effect.provide(makeLayer(client)));
    },
  );

  it.effect("marks a submit lost in transport as uncertain under the operation id", () => {
    const { client } = makeHttpClient(transportFailure);

    return Effect.gen(function* () {
      yield* enableClient;
      const work = yield* ProjectServiceWorkClient.ProjectServiceWorkClient;

      const failure = yield* work
        .submitRun(
          {
            projectId: "proj_ps_1",
            projectGeneration: 7,
            runId: "run_9",
            expectedRunRevision: "run:3",
            agentId: "ag_one",
            result: {},
          },
          { operationId: "op_123", idempotencyKey: "idem_123" },
        )
        .pipe(Effect.flip);

      assert.deepEqual(plainError(failure), {
        _tag: "ProjectServiceWorkTransportError",
        operationId: "op_123",
      });
    }).pipe(Effect.provide(makeLayer(client)));
  });

  it.effect("treats a lost read as plain unreachability", () => {
    const { client } = makeHttpClient(transportFailure);

    return Effect.gen(function* () {
      yield* enableClient;
      const work = yield* ProjectServiceWorkClient.ProjectServiceWorkClient;

      const failure = yield* work
        .listPositions({ projectId: "proj_ps_1", projectGeneration: 7 })
        .pipe(Effect.flip);

      assert.deepEqual(plainError(failure), { _tag: "ProjectServiceWorkTransportError" });
    }).pipe(Effect.provide(makeLayer(client)));
  });

  it.effect("translates a typed rejection envelope verbatim", () => {
    const { client } = makeHttpClient(
      serviceByPath(() =>
        Response.json(
          {
            ok: false,
            error: { code: "WORK_CONTROL_CONFLICT", message: "run revision is stale" },
          },
          { status: 409 },
        ),
      ),
    );

    return Effect.gen(function* () {
      yield* enableClient;
      const work = yield* ProjectServiceWorkClient.ProjectServiceWorkClient;

      const failure = yield* work
        .listMy({ projectId: "proj_ps_1", projectGeneration: 7, agentId: "ag_one" })
        .pipe(Effect.flip);

      assert.deepEqual(plainError(failure), {
        _tag: "ProjectServiceWorkServiceRejectedError",
        code: "WORK_CONTROL_CONFLICT",
        status: 409,
      });
      // The service's message rides the Error surface, not an own field.
      assert.equal(failure.message, "run revision is stale");
    }).pipe(Effect.provide(makeLayer(client)));
  });

  it.effect("answers API-incompatible when the service shape drifts", () => {
    const { client } = makeHttpClient(
      serviceByPath(() => okBody([{ runId: "run_9", positionId: "pos_1" }])),
    );

    return Effect.gen(function* () {
      yield* enableClient;
      const work = yield* ProjectServiceWorkClient.ProjectServiceWorkClient;

      const failure = yield* work
        .listMy({ projectId: "proj_ps_1", projectGeneration: 7, agentId: "ag_one" })
        .pipe(Effect.flip);

      assert.deepEqual(plainError(failure), {
        _tag: "ProjectServiceWorkApiIncompatibleError",
        code: "PROJECT_WORK_RESPONSE_SHAPE",
      });
    }).pipe(Effect.provide(makeLayer(client)));
  });

  it.effect(
    "issue #6 review: listProjects decodes the project list and projects id onto projectId",
    () => {
      // GET /project/v1/ answers the BARE ProjectRecord[] (registry.listProjects),
      // not an ok/result envelope like the SDK-backed reads.
      const { client } = makeHttpClient(
        serviceByPath(() =>
          Response.json([
            { id: "proj_ps_1", name: "Zero Core", workspaceDir: "/workspaces/zero-core" },
          ]),
        ),
      );

      return Effect.gen(function* () {
        yield* enableClient;
        const work = yield* ProjectServiceWorkClient.ProjectServiceWorkClient;

        const projects = yield* work.listProjects();

        assert.deepEqual(projects, [
          { projectId: "proj_ps_1", name: "Zero Core", workspaceDir: "/workspaces/zero-core" },
        ]);
      }).pipe(Effect.provide(makeLayer(client)));
    },
  );

  it.effect(
    "issue #6 review: a non-array project list is a TYPED incompatibility, never a silent []",
    () => {
      const { client } = makeHttpClient(serviceByPath(() => okBody({ projects: [] })));

      return Effect.gen(function* () {
        yield* enableClient;
        const work = yield* ProjectServiceWorkClient.ProjectServiceWorkClient;

        const failure = yield* work.listProjects().pipe(Effect.flip);

        assert.deepEqual(plainError(failure), {
          _tag: "ProjectServiceWorkApiIncompatibleError",
          code: "PROJECT_WORK_RESPONSE_SHAPE",
        });
      }).pipe(Effect.provide(makeLayer(client)));
    },
  );

  it.effect(
    "issue #6 review: an undecodable project record fails as a typed incompatibility, not a Die defect",
    () => {
      // workspaceDir missing — an older/other-shaped server answering 2xx.
      const { client } = makeHttpClient(
        serviceByPath(() => okBody([{ id: "proj_ps_1", name: "Zero Core" }])),
      );

      return Effect.gen(function* () {
        yield* enableClient;
        const work = yield* ProjectServiceWorkClient.ProjectServiceWorkClient;

        const failure = yield* work.listProjects().pipe(Effect.flip);

        assert.deepEqual(plainError(failure), {
          _tag: "ProjectServiceWorkApiIncompatibleError",
          code: "PROJECT_WORK_RESPONSE_SHAPE",
        });
      }).pipe(Effect.provide(makeLayer(client)));
    },
  );

  it.effect("maps a missing bound project to the typed PROJECT_NOT_FOUND rejection", () => {
    const { client } = makeHttpClient(
      serviceByPath(() => Response.json({ message: "Project not found" }, { status: 404 })),
    );

    return Effect.gen(function* () {
      yield* enableClient;
      const work = yield* ProjectServiceWorkClient.ProjectServiceWorkClient;

      const failure = yield* work.getProjectGeneration("proj_ps_1").pipe(Effect.flip);

      assert.deepEqual(plainError(failure), {
        _tag: "ProjectServiceWorkServiceRejectedError",
        code: "PROJECT_NOT_FOUND",
        status: 404,
      });
      assert.equal(failure.message, "The bound Project Service project does not exist.");
    }).pipe(Effect.provide(makeLayer(client)));
  });

  it.effect("preserves the typed envelope from a failed generation probe", () => {
    const { client } = makeHttpClient(
      serviceByPath(() =>
        Response.json(
          {
            ok: false,
            error: { code: "PROJECT_CLIENT_DISABLED", message: "service client is disabled" },
          },
          { status: 403 },
        ),
      ),
    );

    return Effect.gen(function* () {
      yield* enableClient;
      const work = yield* ProjectServiceWorkClient.ProjectServiceWorkClient;

      const failure = yield* work.getProjectGeneration("proj_ps_1").pipe(Effect.flip);

      assert.deepEqual(plainError(failure), {
        _tag: "ProjectServiceWorkServiceRejectedError",
        code: "PROJECT_CLIENT_DISABLED",
        status: 403,
      });
      assert.equal(failure.message, "service client is disabled");
    }).pipe(Effect.provide(makeLayer(client)));
  });

  it.effect("translates a service null into a null run and operation", () => {
    const { client } = makeHttpClient((request) => {
      const url = new URL(request.url);
      if (
        url.pathname === "/project/v1/proj_ps_1/work-runs/run_9" ||
        url.pathname === "/project/v1/operations/op_123"
      ) {
        return okBody(null);
      }
      return serviceByPath()(request);
    });

    return Effect.gen(function* () {
      yield* enableClient;
      const work = yield* ProjectServiceWorkClient.ProjectServiceWorkClient;

      assert.strictEqual(
        yield* work.getRun({
          projectId: "proj_ps_1",
          projectGeneration: 7,
          runId: "run_9",
          agentId: "ag_one",
        }),
        null,
      );
      assert.strictEqual(yield* work.getOperation("op_123"), null);
    }).pipe(Effect.provide(makeLayer(client)));
  });

  it.effect("refuses to send the credential to a non-local base URL", () => {
    const { client, requests } = makeHttpClient(serviceByPath());

    return Effect.gen(function* () {
      const config = yield* ServerConfig.ServerConfig;
      const fileSystem = yield* FileSystem.FileSystem;
      // The write path rejects this; the test is the second line of defense
      // for hand-edited settings.
      yield* fileSystem.writeFileString(
        config.settingsPath,
        '{"projectServiceClient":{"enabled":true,"baseUrl":"https://ps.example.com","keyIdHint":"key-1","credentialSet":true}}',
      );
      yield* fileSystem.writeFile(
        `${config.secretsDir}/project-service-client-credential.bin`,
        new TextEncoder().encode(CREDENTIAL),
      );

      const work = yield* ProjectServiceWorkClient.ProjectServiceWorkClient;
      const failure = yield* work
        .listPositions({ projectId: "proj_ps_1", projectGeneration: 7 })
        .pipe(Effect.flip);

      assert.deepEqual(plainError(failure), {
        _tag: "ProjectServiceWorkUnavailableError",
        reason: "invalid-base-url",
      });
      assert.strictEqual(requests.length, 0);
    }).pipe(Effect.provide(makeLayer(client)));
  });

  it.effect("answers no-credential when the integration is enabled but unconfigured", () => {
    const { client, requests } = makeHttpClient(serviceByPath());

    return Effect.gen(function* () {
      const serverSettings = yield* ServerSettingsModule.ServerSettingsService;
      yield* serverSettings.updateSettings({ projectServiceClient: { enabled: true } });

      const work = yield* ProjectServiceWorkClient.ProjectServiceWorkClient;
      const failure = yield* work
        .listPositions({ projectId: "proj_ps_1", projectGeneration: 7 })
        .pipe(Effect.flip);

      assert.deepEqual(plainError(failure), {
        _tag: "ProjectServiceWorkUnavailableError",
        reason: "no-credential",
      });
      assert.strictEqual(requests.length, 0);
    }).pipe(Effect.provide(makeLayer(client)));
  });
});
