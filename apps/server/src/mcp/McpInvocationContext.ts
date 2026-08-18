import {
  type EnvironmentId,
  PreviewAutomationUnavailableError,
  type ProviderInstanceId,
  type ThreadId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";

/**
 * Independent MCP scopes. `preview` rides the Browser setting; the
 * `project.work.*` scopes ride the Project Service client + logical-agent
 * settings and are granted independently of it. The `wiki.*` scopes are
 * reserved for the wiki capability round: modeled here so granting them later
 * needs no shape change, but nothing grants or checks them yet.
 */
export type McpCapability =
  | "preview"
  | "project.work.read"
  | "project.work.write"
  | "wiki.read"
  | "wiki.write";

export interface McpInvocationScope {
  readonly environmentId: EnvironmentId;
  readonly threadId: ThreadId;
  readonly providerSessionId: string;
  readonly providerInstanceId: ProviderInstanceId;
  readonly capabilities: ReadonlySet<McpCapability>;
  readonly issuedAt: number;
}

export class McpInvocationContext extends Context.Service<
  McpInvocationContext,
  McpInvocationScope
>()("t3/mcp/McpInvocationContext") {}

/**
 * Gate for the preview toolkit. The other capability scopes gate inside their
 * own toolkits against live settings, so they never depend on the snapshot a
 * credential was minted with.
 */
export const requireMcpCapability = Effect.fn("mcp.requireCapability")(function* (
  capability: Extract<McpCapability, "preview">,
) {
  const invocation = yield* McpInvocationContext;
  if (!invocation.capabilities.has(capability)) {
    return yield* new PreviewAutomationUnavailableError({
      capability,
      environmentId: invocation.environmentId,
      threadId: invocation.threadId,
      providerSessionId: invocation.providerSessionId,
      providerInstanceId: invocation.providerInstanceId,
    });
  }
  return invocation;
});
