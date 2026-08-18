import type { ProviderInstanceId, ServerSettings } from "@t3tools/contracts";

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
): ReadonlySet<McpInvocationContext.McpCapability> => {
  const capabilities = new Set<McpInvocationContext.McpCapability>();
  if (settings.enableAgentBrowserAccess) {
    capabilities.add("preview");
  }
  if (projectWorkEnabled(settings, providerInstanceId)) {
    capabilities.add("project.work.read");
    capabilities.add("project.work.write");
  }
  return capabilities;
};

/** True when the Project Service client is on and some logical agent routed to this provider instance has Project work enabled. */
export const projectWorkEnabled = (
  settings: ServerSettings,
  providerInstanceId: ProviderInstanceId,
): boolean =>
  settings.projectServiceClient.enabled &&
  Object.values(settings.logicalAgents).some(
    (agent) => agent.providerInstanceId === providerInstanceId && agent.project.enabled,
  );
