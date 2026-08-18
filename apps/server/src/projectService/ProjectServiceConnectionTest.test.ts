import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as TestClock from "effect/testing/TestClock";
import {
  HttpClient,
  HttpClientError,
  HttpClientRequest,
  HttpClientResponse,
} from "effect/unstable/http";

import * as ServerSecretStore from "../auth/ServerSecretStore.ts";
import * as ServerConfig from "../config.ts";
import * as ServerSettingsModule from "../serverSettings.ts";
import * as ProjectServiceConnectionTest from "./ProjectServiceConnectionTest.ts";

type Responder = (
  request: HttpClientRequest.HttpClientRequest,
) => Response | HttpClientError.HttpClientError;

const makeHttpClient = (
  respond: Responder,
): {
  readonly client: HttpClient.HttpClient;
  readonly requests: HttpClientRequest.HttpClientRequest[];
} => {
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

const transportFailure: Responder = (request) =>
  new HttpClientError.HttpClientError({
    reason: new HttpClientError.TransportError({
      request,
      cause: new Error("connection refused"),
    }),
  });

// Health is unauthenticated; the project list requires the bearer credential.
const healthOk: Responder = (request) =>
  request.url.endsWith("/project/v1/health")
    ? Response.json({ ok: true, status: "ok" })
    : transportFailure(request);

const makeLayer = (client: HttpClient.HttpClient) => {
  const configLayer = Layer.fresh(
    ServerConfig.layerTest(process.cwd(), { prefix: "t3code-ps-connection-test-" }),
  );
  const secretStoreLayer = ServerSecretStore.layer.pipe(Layer.provideMerge(configLayer));
  const settingsLayer = ServerSettingsModule.layer.pipe(Layer.provideMerge(secretStoreLayer));
  return ProjectServiceConnectionTest.layer.pipe(
    Layer.provideMerge(settingsLayer),
    Layer.provide(Layer.succeed(HttpClient.HttpClient, client)),
  );
};

it.layer(NodeServices.layer)("ProjectServiceConnectionTest", (it) => {
  it.effect("reports a fully successful authenticated round trip", () => {
    const { client, requests } = makeHttpClient((request) =>
      request.url.endsWith("/project/v1/")
        ? Response.json([{ id: "proj_1", name: "Wiki" }])
        : healthOk(request),
    );

    return Effect.gen(function* () {
      const serverSettings = yield* ServerSettingsModule.ServerSettingsService;
      yield* serverSettings.updateSettings({
        projectServiceClient: { enabled: true, newCredential: "psk_key-1.s3cret" },
      });
      const connectionTest = yield* ProjectServiceConnectionTest.ProjectServiceConnectionTest;

      const result = yield* connectionTest.testConnection;

      assert.deepEqual(result, {
        reachable: true,
        authenticated: true,
        apiCompatible: true,
        detail: "Authenticated and API compatible",
      });
      const listRequest = requests.find((request) => request.url.endsWith("/project/v1/"));
      assert.strictEqual(listRequest?.headers["authorization"], "Bearer psk_key-1.s3cret");
      const healthRequest = requests.find((request) => request.url.endsWith("/project/v1/health"));
      assert.strictEqual(healthRequest?.headers["authorization"], undefined);
    }).pipe(Effect.provide(makeLayer(client)));
  });

  it.effect("reports a rejected credential without mutating settings or the secret", () => {
    const { client } = makeHttpClient((request) =>
      request.url.endsWith("/project/v1/")
        ? new Response("nope", { status: 401 })
        : healthOk(request),
    );

    return Effect.gen(function* () {
      const serverSettings = yield* ServerSettingsModule.ServerSettingsService;
      yield* serverSettings.updateSettings({
        projectServiceClient: { enabled: true, newCredential: "psk_key-1.s3cret" },
      });
      const config = yield* ServerConfig.ServerConfig;
      const fileSystem = yield* FileSystem.FileSystem;
      const settingsJsonBefore = yield* fileSystem.readFileString(config.settingsPath);
      const secretBefore = yield* fileSystem.readFile(
        `${config.secretsDir}/project-service-client-credential.bin`,
      );

      const connectionTest = yield* ProjectServiceConnectionTest.ProjectServiceConnectionTest;
      const result = yield* connectionTest.testConnection;

      assert.deepEqual(result, {
        reachable: true,
        authenticated: false,
        apiCompatible: false,
        detail: "Credential rejected",
      });
      assert.strictEqual(yield* fileSystem.readFileString(config.settingsPath), settingsJsonBefore);
      assert.strictEqual(
        new TextDecoder().decode(
          yield* fileSystem.readFile(`${config.secretsDir}/project-service-client-credential.bin`),
        ),
        new TextDecoder().decode(secretBefore),
      );
    }).pipe(Effect.provide(makeLayer(client)));
  });

  it.effect("reports an unreachable service", () => {
    const { client } = makeHttpClient(transportFailure);

    return Effect.gen(function* () {
      const serverSettings = yield* ServerSettingsModule.ServerSettingsService;
      yield* serverSettings.updateSettings({
        projectServiceClient: { enabled: true, newCredential: "psk_key-1.s3cret" },
      });
      const connectionTest = yield* ProjectServiceConnectionTest.ProjectServiceConnectionTest;

      assert.deepEqual(yield* connectionTest.testConnection, {
        reachable: false,
        authenticated: false,
        apiCompatible: false,
        detail: "Project Service unreachable",
      });
    }).pipe(Effect.provide(makeLayer(client)));
  });

  it.effect("stops after the health probe when no credential is stored", () => {
    const { client, requests } = makeHttpClient(healthOk);

    return Effect.gen(function* () {
      const connectionTest = yield* ProjectServiceConnectionTest.ProjectServiceConnectionTest;

      assert.deepEqual(yield* connectionTest.testConnection, {
        reachable: true,
        authenticated: false,
        apiCompatible: false,
        detail: "No credential stored",
      });
      assert.strictEqual(requests.length, 1);
    }).pipe(Effect.provide(makeLayer(client)));
  });

  it.effect("flags a 200 project list that is not a list", () => {
    const { client } = makeHttpClient((request) =>
      request.url.endsWith("/project/v1/")
        ? Response.json({ unexpected: true })
        : healthOk(request),
    );

    return Effect.gen(function* () {
      const serverSettings = yield* ServerSettingsModule.ServerSettingsService;
      yield* serverSettings.updateSettings({
        projectServiceClient: { enabled: true, newCredential: "psk_key-1.s3cret" },
      });
      const connectionTest = yield* ProjectServiceConnectionTest.ProjectServiceConnectionTest;

      assert.deepEqual(yield* connectionTest.testConnection, {
        reachable: true,
        authenticated: true,
        apiCompatible: false,
        detail: "Project list response was not a list",
      });
    }).pipe(Effect.provide(makeLayer(client)));
  });

  it.effect(
    "degrades to unreachable when the project list body stalls",
    () => {
      // 200 headers, then a body that never completes: without a budget over
      // the whole probe, the body read would hang the RPC forever.
      const stalledBody = new Response(
        new ReadableStream({
          start(controller) {
            controller.enqueue(new TextEncoder().encode('[{"id":'));
          },
        }),
        { status: 200, headers: { "content-type": "application/json" } },
      );
      const { client } = makeHttpClient((request) =>
        request.url.endsWith("/project/v1/") ? stalledBody : healthOk(request),
      );

      return Effect.gen(function* () {
        const serverSettings = yield* ServerSettingsModule.ServerSettingsService;
        yield* serverSettings.updateSettings({
          projectServiceClient: { enabled: true, newCredential: "psk_key-1.s3cret" },
        });
        const connectionTest = yield* ProjectServiceConnectionTest.ProjectServiceConnectionTest;

        // it.effect runs on the test clock; advance it from a side fiber
        // until the probe budget expires.
        const ticker = yield* Effect.forkChild(
          Effect.forever(
            Effect.gen(function* () {
              yield* Effect.yieldNow;
              yield* TestClock.adjust(Duration.seconds(1));
            }),
          ),
        );
        const result = yield* connectionTest.testConnection;
        yield* Fiber.interrupt(ticker);

        assert.deepEqual(result, {
          reachable: false,
          authenticated: false,
          apiCompatible: false,
          detail: "Project Service unreachable",
        });
      }).pipe(Effect.provide(makeLayer(client)));
    },
    15_000,
  );

  it.effect("refuses to send the credential to a non-local base URL", () => {
    const { client, requests } = makeHttpClient(healthOk);

    return Effect.gen(function* () {
      const config = yield* ServerConfig.ServerConfig;
      const fileSystem = yield* FileSystem.FileSystem;
      // The write path rejects this, so seed it out of band — the test is
      // the second line of defense for hand-edited settings.
      yield* fileSystem.writeFileString(
        config.settingsPath,
        '{"projectServiceClient":{"enabled":true,"baseUrl":"https://ps.example.com","keyIdHint":"key-1","credentialSet":true}}',
      );
      yield* fileSystem.writeFile(
        `${config.secretsDir}/project-service-client-credential.bin`,
        new TextEncoder().encode("psk_key-1.s3cret"),
      );

      const connectionTest = yield* ProjectServiceConnectionTest.ProjectServiceConnectionTest;
      const result = yield* connectionTest.testConnection;

      assert.deepEqual(result, {
        reachable: false,
        authenticated: false,
        apiCompatible: false,
        detail: "non-local baseUrl rejected",
      });
      assert.strictEqual(requests.length, 0);
    }).pipe(Effect.provide(makeLayer(client)));
  });
});
