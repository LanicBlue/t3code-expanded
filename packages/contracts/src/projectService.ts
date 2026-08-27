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
  /**
   * Feature gate for the work-mission-v5 wire population (capability
   * `mission.v1`): while false T3 never declares the capability, never
   * receives mission frames, and rejects a `mission.ended` notice as
   * not-activated. work-mission-v5 Phase 7: the default is ON (the mission
   * line is THE line — design §10 Phase 7 precondition ①). An explicit
   * `false` remains the drain/rollback off-switch. Absent = true.
   */
  missionsEnabled: Schema.Boolean.pipe(Schema.withDecodingDefault(Effect.succeed(true))),
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
  missionsEnabled: Schema.optionalKey(Schema.Boolean),
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

/**
 * What happens to a flow-instance session when its flow instance reaches a
 * terminal state and the session is safely idle: "settle" (default) parks the
 * session as done work that stays in the project; "delete" removes the
 * temporary session entirely — only after the settle-safety conditions hold
 * (never while the session runs a turn or waits on human input).
 */
export const ProjectWorkSessionRetention = Schema.Literals(["settle", "delete"]);
export type ProjectWorkSessionRetention = typeof ProjectWorkSessionRetention.Type;

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
    /**
     * Retention for a flow-instance session whose instance reached a
     * terminal state: "settle" (default) keeps the session as settled work;
     * "delete" removes the temporary session once it is safely idle.
     * Project-scope sessions are long-lived and are never settled or
     * deleted by flow finalization regardless of this setting.
     */
    sessionRetention: ProjectWorkSessionRetention.pipe(
      Schema.withDecodingDefault(Effect.succeed("settle")),
    ),
  }).pipe(Schema.withDecodingDefault(Effect.succeed({}))),
});
export type LogicalAgentConfig = typeof LogicalAgentConfig.Type;

/** Map shape for `ServerSettings.logicalAgents`, keyed by `LogicalAgentId`. */
export const LogicalAgentConfigMap = Schema.Record(LogicalAgentId, LogicalAgentConfig);
export type LogicalAgentConfigMap = typeof LogicalAgentConfigMap.Type;

// ── work-mission-v5 visit view (capability `mission.v1`) ─────────
// The SHAPE source of truth for every block below is the vendored SDK 0.14
// (WorkRunMissionTaskView / WorkRunWorkTaskView / the "visit" arm of
// WorkRunActionView / MissionVisitSubmitResult). These Effect schemas exist
// because each trust boundary DECODES — apps/server's vendoredSdkIntegrity
// test pins the assignability against the SDK types so the two cannot drift.

/**
 * The mission a visit run belongs to — the `task.mission` block of the
 * work-mission-v5 visit view (design §6.1, SDK 0.14). Replaces the flow
 * shape's `task.instance` as the run's task identity: `id` is the stable
 * mission identity (also the workspace grouping key's source), `name` and
 * `objective` are display/task text.
 */
export const ProjectWorkMissionRecord = Schema.Struct({
  id: Schema.String,
  name: Schema.String,
  objective: Schema.String,
});
export type ProjectWorkMissionRecord = typeof ProjectWorkMissionRecord.Type;

/**
 * The work-station facts of a visit run — the `task.work` block of the visit
 * view. `group` is the workspace grouping key (missionId for mission work);
 * `workKey` names the station; `iteration` is the visit's round at that
 * station.
 */
export const ProjectWorkVisitWorkRecord = Schema.Struct({
  group: Schema.String,
  workKey: Schema.String,
  iteration: Schema.Number,
});
export type ProjectWorkVisitWorkRecord = typeof ProjectWorkVisitWorkRecord.Type;

/**
 * The visit completion contract — the `task.action` block of the visit view:
 * `outcomes` is THIS station's vocabulary (the submit `outcome` domain),
 * `candidates` the in-contract target stations for the current pending state.
 * Candidates are a choice HINT, not a constraint — off-contract targets are
 * legal within the station's handoff permission and require a submit `reason`.
 */
export const ProjectWorkVisitActionRecord = Schema.Struct({
  kind: Schema.Literals(["visit"]),
  outcomes: Schema.Array(Schema.String),
  candidates: Schema.Array(Schema.String),
});
export type ProjectWorkVisitActionRecord = typeof ProjectWorkVisitActionRecord.Type;

/**
 * The decoded visit view of a run's `task` snapshot (the work-mission-v5
 * population). Present iff `task.mission` is — the discriminator this client
 * keys on (`task.instance` frames are structurally gone: the Project Service
 * removed the flow stack in work-mission-v5 Phase 7, so a task without a
 * mission block is standalone work).
 */
export const ProjectWorkVisitView = Schema.Struct({
  mission: ProjectWorkMissionRecord,
  work: ProjectWorkVisitWorkRecord,
  action: ProjectWorkVisitActionRecord,
});
export type ProjectWorkVisitView = typeof ProjectWorkVisitView.Type;

/**
 * The submit result vocabulary for a visit run (run.submit / user-complete
 * result, envelope unchanged): `outcome` picks from the station's
 * `action.outcomes`; `nextNode` is the agent-chosen next station (PS judges
 * permission); `reason` is REQUIRED when `nextNode` is off-contract;
 * `feedback` rides a rework hop; `documentReceiptIds` are the notarized write
 * receipts (work-mission-v5 Phase 6 / SDK 0.15.0: real `document:<sha256>`
 * ids minted by the by-run mission document notary — receipt v3,
 * mission-addressed; PS validates each against the visit's frozen write
 * domain and freezes {documentId,keyHash} rows onto the visit). The flow
 * population keeps its own result shapes ({"kind":"after"},
 * gates, terminals) — the two never mix on one run.
 */
export const ProjectWorkVisitSubmitResult = Schema.Struct({
  outcome: Schema.String,
  nextNode: Schema.optional(Schema.String),
  reason: Schema.optional(Schema.String),
  feedback: Schema.optional(Schema.String),
  documentReceiptIds: Schema.optional(Schema.Array(Schema.String)),
});
export type ProjectWorkVisitSubmitResult = typeof ProjectWorkVisitSubmitResult.Type;

/**
 * The `mission.ended` push frame's facts (minus the envelope `type`), as the
 * settlement intake reads them: `group` is the workspace grouping key the
 * sessions live under, `disposition` the mission's end state. The noticeId is
 * the idempotency key; the durable local ledger record is the ACK commit
 * point (same semantics as the fen_ flow-ended deliveries).
 *
 * Single source of truth for the SHAPE is the vendored SDK 0.14's
 * `MissionEndedNotice` (apps/server's vendoredSdkIntegrity test pins the
 * assignability, so an SDK drift fails the suite). This Effect schema exists
 * because the intake DECODES at its trust boundary — a plain interface
 * re-export cannot. The fields beyond the four the settlement reads
 * (workspacePolicy/workspaceRef/outcome) decode tolerantly: the SDK runtime
 * has already exact-parsed the frame before the adapter hook sees it, so the
 * tolerance only matters for hand-fed test fixtures.
 */
export const MissionEndedNoticeFacts = Schema.Struct({
  noticeId: Schema.String,
  missionId: Schema.String,
  group: Schema.String,
  disposition: Schema.Literals(["completed", "abandoned", "deleted"]),
  outcome: Schema.optional(Schema.NullOr(Schema.String)),
  workspacePolicy: Schema.optional(Schema.String),
  workspaceRef: Schema.optional(Schema.NullOr(Schema.String)),
});
export type MissionEndedNoticeFacts = typeof MissionEndedNoticeFacts.Type;

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
