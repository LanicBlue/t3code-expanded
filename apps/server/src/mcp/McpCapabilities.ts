import type { LogicalAgentId, ProviderInstanceId, ServerSettings } from "@t3tools/contracts";

import type * as McpInvocationContext from "./McpInvocationContext.ts";

/**
 * Derives the MCP capability scopes a provider session may use, from live
 * settings. Each scope family is independent: withholding Browser access
 * removes only `preview`, and the Project scopes ride the Project Service
 * client + logical-agent settings. Wiki scopes stay ungranted until that
 * capability round lands.
 *
 * Called both when a credential is minted and per Project tool call, so
 * capability changes take effect on the next call without disturbing the
 * session (the frozen set on an issued credential never removes live scopes).
 */
export const deriveMcpCapabilities = (
  settings: ServerSettings,
  providerInstanceId: ProviderInstanceId,
  logicalAgentId?: LogicalAgentId | undefined,
): ReadonlySet<McpInvocationContext.McpCapability> => {
  const capabilities = new Set<McpInvocationContext.McpCapability>();
  if (settings.enableAgentBrowserAccess) {
    capabilities.add("preview");
  }
  if (projectWorkEnabled(settings, providerInstanceId, logicalAgentId)) {
    capabilities.add("project.work.read");
    capabilities.add("project.work.write");
  }
  return capabilities;
};

/**
 * True when this session may use Project work tools.
 *
 * A BOUND session (wake threads carry their logical agent) answers for THAT
 * agent: several Project-enabled agents may now ride one provider instance,
 * each under its own binding. An UNBOUND session (human-created thread)
 * keeps the legacy instance-wide answer, granted only while it stays
 * unambiguous — exactly one Project-enabled agent on the instance. With two
 * or more, an unbound session gets nothing rather than silently impersonating
 * whichever agent happened to sort first.
 */
export const projectWorkEnabled = (
  settings: ServerSettings,
  providerInstanceId: ProviderInstanceId,
  logicalAgentId?: LogicalAgentId | undefined,
): boolean => {
  if (!settings.projectServiceClient.enabled) return false;
  const eligible = Object.entries(settings.logicalAgents).filter(
    ([, agent]) => agent.providerInstanceId === providerInstanceId && agent.project.enabled,
  );
  if (logicalAgentId === undefined) {
    return eligible.length === 1;
  }
  return eligible.some(([id]) => id === logicalAgentId);
};
