import {
  LogicalAgentId,
  type LogicalAgentConfig,
  type ProjectServiceConnectionTestResult,
  type ProjectServiceProjectBinding,
} from "@t3tools/contracts";

/**
 * Pure helpers for the Services settings panel. Kept module-local to the
 * panel's rendering concerns: draft-to-config conversion, credential paste
 * validation, and connection-test presentation.
 */

/** Stable id for a new agent, minted once at creation. */
export function makeLogicalAgentId(randomUuid: string): LogicalAgentId {
  return LogicalAgentId.make(`ag_${randomUuid}`);
}

export function makeEmptyLogicalAgentConfig(
  providerInstanceId: LogicalAgentConfig["providerInstanceId"],
): LogicalAgentConfig {
  return {
    agentName: "New agent",
    providerInstanceId,
    project: { enabled: false },
    projectBindings: [],
  };
}

export function isValidCredentialPaste(value: string): boolean {
  return value.length === 0 || /^psk_[^.]+\..+/.test(value);
}

export function nextAgentMapWithAgent(
  current: Readonly<Record<string, LogicalAgentConfig>>,
  agentId: string,
  agent: LogicalAgentConfig,
): Record<string, LogicalAgentConfig> {
  return { ...current, [agentId]: agent };
}

export function nextAgentMapWithoutAgent(
  current: Readonly<Record<string, LogicalAgentConfig>>,
  agentId: string,
): Record<string, LogicalAgentConfig> {
  const { [agentId]: _removed, ...rest } = current;
  return rest;
}

/** Bindings are keyed by the t3ProjectId+projectId pair. */
export function isSameBinding(
  a: ProjectServiceProjectBinding,
  b: ProjectServiceProjectBinding,
): boolean {
  return a.t3ProjectId === b.t3ProjectId && a.projectId === b.projectId;
}

/**
 * Add (or refresh) one project binding: re-adding an existing
 * t3ProjectId+projectId pair updates it in place instead of duplicating it.
 */
export function nextAgentConfigWithBinding(
  agent: LogicalAgentConfig,
  binding: ProjectServiceProjectBinding,
): LogicalAgentConfig {
  const index = agent.projectBindings.findIndex((candidate) => isSameBinding(candidate, binding));
  return {
    ...agent,
    projectBindings:
      index === -1
        ? [...agent.projectBindings, binding]
        : agent.projectBindings.map((candidate, candidateIndex) =>
            candidateIndex === index ? binding : candidate,
          ),
  };
}

export const CONNECTION_TEST_STATUS_LABELS: Readonly<
  Record<"ok" | "reachable" | "authFailed" | "unconfigured" | "failed", string>
> = {
  ok: "Connected",
  reachable: "Reached service",
  authFailed: "Authentication failed",
  unconfigured: "Not configured",
  failed: "Failed",
};

/**
 * Coarse status for the result badge; detail carries the specifics. A
 * reachable service that did not authenticate splits by credential state:
 * rejected vs never configured.
 */
export function connectionTestStatus(input: {
  readonly result: ProjectServiceConnectionTestResult;
  readonly credentialSet: boolean;
}): "ok" | "reachable" | "authFailed" | "unconfigured" | "failed" {
  if (input.result.apiCompatible) return "ok";
  if (input.result.authenticated) return "reachable";
  if (!input.result.reachable) return "failed";
  return input.credentialSet ? "authFailed" : "unconfigured";
}

export function credentialStatusLabel(input: {
  readonly credentialSet: boolean;
  readonly keyIdHint: string;
}): string {
  return input.credentialSet ? `Credential psk_${input.keyIdHint}` : "No credential stored";
}
