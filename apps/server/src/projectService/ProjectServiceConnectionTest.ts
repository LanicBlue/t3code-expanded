/**
 * Connection test for the configured Project Service.
 *
 * An unauthenticated health probe establishes reachability; an authenticated
 * project-list round-trip then proves both the credential and API
 * compatibility. Read-only by construction: nothing here writes settings,
 * secrets, sessions, or threads — a rejected credential is a result, not a
 * mutation. Detail strings are fixed vocabulary (status codes only) so no
 * response body or credential material can leak into results or logs.
 *
 * @module ProjectServiceConnectionTest
 */
import {
  isLocalProjectServiceBaseUrl,
  type ProjectServiceConnectionTestResult,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import { HttpClient, HttpClientRequest } from "effect/unstable/http";
import type { HttpClientError } from "effect/unstable/http/HttpClientError";
import type { HttpClientResponse } from "effect/unstable/http/HttpClientResponse";

import * as ServerSecretStore from "../auth/ServerSecretStore.ts";
import * as ServerSettings from "../serverSettings.ts";

const CONNECTION_TEST_TIMEOUT = Duration.seconds(5);

const UNREACHABLE_DETAIL = "Project Service unreachable";
const INVALID_BASE_URL_DETAIL = "Base URL is not a valid http(s) URL";
const NON_LOCAL_BASE_URL_DETAIL = "non-local baseUrl rejected";
const NO_CREDENTIAL_DETAIL = "No credential stored";
const OK_DETAIL = "Authenticated and API compatible";
const INVALID_CREDENTIAL_DETAIL = "Credential rejected";
const NOT_A_LIST_DETAIL = "Project list response was not a list";
const HEALTH_FAILED_PREFIX = "Health check failed with status ";
const SERVICE_ERROR_PREFIX = "Project list request failed with status ";
const LOCAL_FAILURE_PREFIX = "Connection test could not run: ";
/** Oldest Project Service this T3 speaks to: apiMinor 1 added the
 * flow-document `write` upsert the doc tools send unconditionally. */
const PROJECT_SERVICE_MIN_API_MINOR = 1;
const TOO_OLD_PREFIX = "Project Service is too old for this T3 (apiMinor ";
const TOO_OLD_SUFFIX = "); deploy Project Service first, then re-test";

export class ProjectServiceConnectionTest extends Context.Service<
  ProjectServiceConnectionTest,
  {
    readonly testConnection: Effect.Effect<ProjectServiceConnectionTestResult>;
  }
>()("t3/projectService/ProjectServiceConnectionTest") {}

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

/** Project Service wraps every response in a `{ok, result|error}` envelope
 * (http-server `jsonBody`); the health fields — apiMinor included — live
 * inside `result`. A body without `result` is read as-is so the probe still
 * parses against a hypothetically de-enveloped health route. */
const healthSourceOf = (body: unknown): unknown =>
  typeof body === "object" && body !== null && "result" in body
    ? (body as { readonly result: unknown }).result
    : body;

const result = (
  reachable: boolean,
  authenticated: boolean,
  apiCompatible: boolean,
  detail: string,
): ProjectServiceConnectionTestResult => ({ reachable, authenticated, apiCompatible, detail });

export const make = Effect.gen(function* () {
  const httpClient = yield* HttpClient.HttpClient;
  const serverSettings = yield* ServerSettings.ServerSettingsService;
  const secretStore = yield* ServerSecretStore.ServerSecretStore;

  // Any transport failure (refused, DNS, TLS, timeout) counts as simply
  // unreachable; status codes are distinguished by the read callback. The
  // timeout budget covers the whole probe — request, body read, and parse —
  // so a service that answers headers then stalls still resolves.
  const probe = <A>(
    url: string,
    read: (response: HttpClientResponse) => Effect.Effect<A, HttpClientError>,
    authorization?: string,
  ): Effect.Effect<A | null> => {
    const request = HttpClientRequest.get(url).pipe(
      HttpClientRequest.setHeader("accept", "application/json"),
    );
    return httpClient
      .execute(
        authorization === undefined
          ? request
          : request.pipe(HttpClientRequest.setHeader("authorization", `Bearer ${authorization}`)),
      )
      .pipe(
        Effect.flatMap(read),
        Effect.timeoutOption(CONNECTION_TEST_TIMEOUT),
        Effect.map(Option.getOrElse(() => null)),
        Effect.orElseSucceed(() => null),
      );
  };

  // A settings or secret-store failure degrades to a failed result rather
  // than an error: callers surface one shape, and the tag alone carries what
  // broke locally (no message bodies).
  const testConnection: ProjectServiceConnectionTest["Service"]["testConnection"] = Effect.gen(
    function* () {
      const settings = yield* serverSettings.getSettings;
      const client = settings.projectServiceClient;

      const base = normalizeBaseUrl(client.baseUrl);
      if (base === null) {
        return result(false, false, false, INVALID_BASE_URL_DETAIL);
      }
      // Defense in depth: write validation already restricts the base URL,
      // but settings.json is editable out of band and the probe would send
      // the stored credential to whatever host it names.
      if (!isLocalProjectServiceBaseUrl(client.baseUrl)) {
        return result(false, false, false, NON_LOCAL_BASE_URL_DETAIL);
      }

      // The health probe also captures the advertised apiMinor for the
      // compatibility check that follows authentication — a 200 body that
      // will not parse degrades to "unknown", never to an error.
      const health = yield* probe(`${base}/project/v1/health`, (response) =>
        response.json.pipe(
          Effect.map((body): { readonly status: number; readonly apiMinor: number | null } => {
            const healthBody = healthSourceOf(body);
            return {
              status: response.status,
              apiMinor:
                typeof healthBody === "object" &&
                healthBody !== null &&
                typeof (healthBody as { readonly apiMinor?: unknown }).apiMinor === "number"
                  ? (healthBody as { readonly apiMinor: number }).apiMinor
                  : null,
            };
          }),
          Effect.catch(() => Effect.succeed({ status: response.status, apiMinor: null })),
        ),
      );
      if (health === null) {
        return result(false, false, false, UNREACHABLE_DETAIL);
      }
      if (health.status !== 200) {
        return result(true, false, false, `${HEALTH_FAILED_PREFIX}${health.status}`);
      }

      if (!client.credentialSet) {
        return result(true, false, false, NO_CREDENTIAL_DETAIL);
      }
      const credential = yield* secretStore.get(
        ServerSettings.PROJECT_SERVICE_CREDENTIAL_SECRET_NAME,
      );
      if (Option.isNone(credential)) {
        return result(true, false, false, NO_CREDENTIAL_DETAIL);
      }

      const list = yield* probe(
        `${base}/project/v1/`,
        (response) =>
          response.status === 200
            ? response.json.pipe(
                Effect.map(
                  (body): ProjectServiceConnectionTestResult =>
                    Array.isArray(body)
                      ? health.apiMinor !== null && health.apiMinor < PROJECT_SERVICE_MIN_API_MINOR
                        ? result(
                            true,
                            true,
                            false,
                            `${TOO_OLD_PREFIX}${health.apiMinor}${TOO_OLD_SUFFIX}`,
                          )
                        : health.apiMinor === null
                          ? result(true, true, false, `${TOO_OLD_PREFIX}unknown${TOO_OLD_SUFFIX}`)
                          : result(true, true, true, OK_DETAIL)
                      : result(true, true, false, NOT_A_LIST_DETAIL),
                ),
                // An unparseable 200 body is a compatibility failure, not an
                // error — same vocabulary as a well-formed non-list body.
                Effect.orElseSucceed(() => result(true, true, false, NOT_A_LIST_DETAIL)),
              )
            : Effect.succeed(
                response.status === 401 || response.status === 403
                  ? result(true, false, false, INVALID_CREDENTIAL_DETAIL)
                  : result(true, false, false, `${SERVICE_ERROR_PREFIX}${response.status}`),
              ),
        new TextDecoder().decode(credential.value),
      );
      if (list === null) {
        return result(false, false, false, UNREACHABLE_DETAIL);
      }
      return list;
    },
  ).pipe(
    Effect.catch((error: { readonly _tag: string }) =>
      Effect.succeed(result(false, false, false, `${LOCAL_FAILURE_PREFIX}${error._tag}`)),
    ),
  );

  return { testConnection } satisfies ProjectServiceConnectionTest["Service"];
});

export const layer = Layer.effect(ProjectServiceConnectionTest, make);
