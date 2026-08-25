/**
 * Project Service contracts — the settings and connection-test payloads for
 * the external Project Service a T3 environment integrates with.
 *
 * One client per environment. The client credential is created at the service
 * (by hand via its WebUI or API) and pasted into T3; T3 only stores and tests
 * it. The raw credential lives solely in the server secret store — settings
 * payloads carry a `keyIdHint` plus a `credentialSet` flag and nothing else,
 * and the write path accepts the raw value exactly once per update.
 *
 * Logical agents are the T3-side identities that act through a provider
 * instance. `LogicalAgentId` is generated once and immutable; everything else
 * on an agent is mutable. Agents carry no per-project configuration: Work
 * notices route by the notice's workspace directory, and T3 reuses or creates
 * the matching local project on the fly.
 *
 * @module projectService
 */
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { TrimmedNonEmptyString, TrimmedString } from "./baseSchemas.ts";
import { ProviderInstanceId } from "./providerInstance.ts";
import { ModelSelection } from "./orchestration.ts";

// ── Client credential ────────────────────────────────────────────

export const PROJECT_SERVICE_CREDENTIAL_PREFIX = "psk_";

export interface ProjectServiceCredential {
  readonly keyId: string;
  readonly secret: string;
}

/**
 * Split a pasted `psk_<keyId>.<secret>` credential. Returns null unless both
 * halves are non-empty; callers decide whether an empty string means "keep the
 * stored credential" (it never reaches this parser).
 */
export function parseProjectServiceCredential(value: string): ProjectServiceCredential | null {
  if (!value.startsWith(PROJECT_SERVICE_CREDENTIAL_PREFIX)) {
    return null;
  }
  const remainder = value.slice(PROJECT_SERVICE_CREDENTIAL_PREFIX.length);
  const separator = remainder.indexOf(".");
  if (separator <= 0 || separator === remainder.length - 1) {
    return null;
  }
  return { keyId: remainder.slice(0, separator), secret: remainder.slice(separator + 1) };
}

// ── Client settings ──────────────────────────────────────────────

/** Local loopback deployment this fork integrates with. */
export const DEFAULT_PROJECT_SERVICE_BASE_URL = "http://127.0.0.1:7600";

const isLocalIpv4 = (octets: ReadonlyArray<number>): boolean => {
  const [first, second] = octets;
  return (
    first === 127 ||
    first === 10 ||
    (first === 172 && second !== undefined && second >= 16 && second <= 31) ||
    (first === 192 && second === 168)
  );
};

const parseIpv4 = (hostname: string): ReadonlyArray<number> | null => {
  const parts = hostname.split(".");
  if (parts.length !== 4) return null;
  const octets: number[] = [];
  for (const part of parts) {
    if (!/^\d{1,3}$/.test(part)) return null;
    const octet = Number(part);
    if (octet > 255) return null;
    octets.push(octet);
  }
  return octets;
};

/**
 * v1 accepts only loopback or private-network Project Service endpoints: the
 * stored client credential is sent to this URL, so it must never resolve to a
 * host an unauthorized actor controls. Unparseable URLs and non-http(s)
 * schemes are rejected too.
 */
export function isLocalProjectServiceBaseUrl(baseUrl: string): boolean {
  let url: URL;
  try {
    url = new URL(baseUrl);
  } catch {
    return false;
  }
  if (url.protocol !== "http:" && url.protocol !== "https:") {
    return false;
  }
  if (url.hostname === "localhost" || url.hostname === "[::1]") {
    return true;
  }
  // Node keeps IPv6 hostnames bracketed; IPv4-mapped addresses compare as IPv4.
  const hostname = url.hostname.replace(/^\[(.*)\]$/, "$1").replace(/^::ffff:/i, "");
  if (hostname === "::1") {
    return true;
  }
  const octets = parseIpv4(hostname);
  return octets !== null && isLocalIpv4(octets);
}

/**
 * Stored (and client-visible) client state. This is already the redacted
 * view: the raw credential only ever exists in the server secret store, so
 * `keyIdHint` + `credentialSet` is the full credential state there is.
 */
export const ProjectServiceClientSettings = Schema.Struct({
  enabled: Schema.Boolean.pipe(Schema.withDecodingDefault(Effect.succeed(false))),
  baseUrl: TrimmedString.pipe(
    Schema.withDecodingDefault(Effect.succeed(DEFAULT_PROJECT_SERVICE_BASE_URL)),
  ),
  /** psk_ keyId prefix of the stored credential — a public lookup key. */
  keyIdHint: TrimmedString.pipe(Schema.withDecodingDefault(Effect.succeed(""))),
  credentialSet: Schema.Boolean.pipe(Schema.withDecodingDefault(Effect.succeed(false))),
});
export type ProjectServiceClientSettings = typeof ProjectServiceClientSettings.Type;

/**
 * Write path for the client settings. `newCredential` is write-only: it is
 * accepted on update, stored to the secret store, and stripped before
 * anything persists or echoes back. Omitting it keeps the stored credential;
 * `clearCredential` removes it.
 */
export const ProjectServiceClientSettingsPatch = Schema.Struct({
  enabled: Schema.optionalKey(Schema.Boolean),
  baseUrl: Schema.optionalKey(TrimmedString),
  newCredential: Schema.optionalKey(TrimmedNonEmptyString),
  clearCredential: Schema.optionalKey(Schema.Boolean),
});
export type ProjectServiceClientSettingsPatch = typeof ProjectServiceClientSettingsPatch.Type;

// ── Logical agents ───────────────────────────────────────────────

// Identity moved to providerInstance.ts (beside ProviderInstanceId) so
// orchestration payloads can reference it without an import cycle; the
// re-export keeps every existing import path working.
export { LogicalAgentId } from "./providerInstance.ts";
import { LogicalAgentId } from "./providerInstance.ts";

export const LOGICAL_AGENT_PERSONA_MAX_CHARS = 4000;

/**
 * How Project Service work maps onto sessions: "project" routes all work of
 * one (agent, project) onto ONE session; "flow-instance" gives each flow
 * instance's work its own session, so concurrent instances run in parallel
 * threads of the same T3 project.
 */
export const ProjectWorkSessionScope = Schema.Literals(["project", "flow-instance"]);
export type ProjectWorkSessionScope = typeof ProjectWorkSessionScope.Type;

export const LogicalAgentConfig = Schema.Struct({
  /** Mutable display name; changing it changes nothing else. */
  agentName: TrimmedNonEmptyString,
  /** Routing to a provider instance; no provider credential is duplicated. */
  providerInstanceId: ProviderInstanceId,
  /**
   * Role directive for this agent: WHO it is and HOW it should work (the
   * Project Service work prompt is WHAT to do — the two compose). Folded
   * into the system prompt of the agent's sessions at start time; empty
   * string = no directive.
   */
  persona: TrimmedString.check(Schema.isMaxLength(LOGICAL_AGENT_PERSONA_MAX_CHARS)).pipe(
    Schema.withDecodingDefault(Effect.succeed("")),
  ),
  /**
   * Optional reasoning-effort level for this agent's sessions (values like
   * "low" | "medium" | "high" | "xhigh" | "max"; each driver drops values
   * its model does not support). Applied as the provider option the
   * agent's driver reads (Claude "effort", Codex "reasoningEffort") when
   * the resolved model selection does not already carry one; null = follow
   * the model default. A blank string is tolerated (treated as null at
   * use time) so a hand-edited settings.json cannot brick the whole file.
   */
  thinkLevel: Schema.NullOr(TrimmedString.check(Schema.isMaxLength(24))).pipe(
    Schema.withDecodingDefault(Effect.succeed(null)),
  ),
  /**
   * Optional model selection that wins over the project default for this
   * agent's wake sessions; null = follow the usual resolution.
   */
  modelOverride: Schema.NullOr(ModelSelection).pipe(
    Schema.withDecodingDefault(Effect.succeed(null)),
  ),
  project: Schema.Struct({
    enabled: Schema.Boolean.pipe(Schema.withDecodingDefault(Effect.succeed(false))),
    /**
     * Session routing for this agent's Project Service work: "project"
     * (default) = all work of one (agent, project) on ONE session;
     * "flow-instance" = each flow instance's work gets its own session (runs
     * whose task snapshot carries no instance identity share one legacy
     * session).
     */
    sessionScope: ProjectWorkSessionScope.pipe(
      Schema.withDecodingDefault(Effect.succeed("project")),
    ),
  }).pipe(Schema.withDecodingDefault(Effect.succeed({}))),
});
export type LogicalAgentConfig = typeof LogicalAgentConfig.Type;

/** Map shape for `ServerSettings.logicalAgents`, keyed by `LogicalAgentId`. */
export const LogicalAgentConfigMap = Schema.Record(LogicalAgentId, LogicalAgentConfig);
export type LogicalAgentConfigMap = typeof LogicalAgentConfigMap.Type;

// ── Connection test ──────────────────────────────────────────────

/**
 * Result of the authenticated round-trip against the configured service:
 * an unauthenticated health probe establishes reachability, then an
 * authenticated project-list request proves both the credential and API
 * compatibility. `detail` is a fixed-vocabulary status line — it never
 * carries response bodies or credential material.
 */
export const ProjectServiceConnectionTestResult = Schema.Struct({
  reachable: Schema.Boolean,
  authenticated: Schema.Boolean,
  apiCompatible: Schema.Boolean,
  detail: TrimmedNonEmptyString,
});
export type ProjectServiceConnectionTestResult = typeof ProjectServiceConnectionTestResult.Type;
