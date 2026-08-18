import type {
  LogicalAgentId,
  ProjectId,
  ProviderInstanceId,
  ServerSettings,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";

import * as McpCapabilities from "../../McpCapabilities.ts";
import type { McpCapability } from "../../McpInvocationContext.ts";

/** Why a Project tool call cannot reach the service, each with a user-actionable meaning. */
export const ProjectWorkUnavailabilityReason = Schema.Literals([
  "integration-disabled",
  "integration-unreadable",
  "invalid-base-url",
  "no-credential",
  "thread-unavailable",
  "agent-unbound",
  "agent-ambiguous",
  "binding-ambiguous",
  "agent-project-disabled",
  "no-binding",
  "capability-disabled",
  "service-unreachable",
  "service-unavailable",
]);
export type ProjectWorkUnavailabilityReason = typeof ProjectWorkUnavailabilityReason.Type;

export const projectWorkUnavailabilityMessage = (
  reason: ProjectWorkUnavailabilityReason,
): string => {
  switch (reason) {
    case "integration-disabled":
      return "The Project Service integration is disabled for this server.";
    case "integration-unreadable":
      return "The Project Service settings or credential could not be read.";
    case "invalid-base-url":
      return "The Project Service base URL is not a local http(s) endpoint.";
    case "no-credential":
      return "No Project Service client credential is stored.";
    case "thread-unavailable":
      return "This session's thread is no longer available, so its project cannot be resolved.";
    case "agent-unbound":
      return "No logical agent for this session's provider instance has Project work configured.";
    case "agent-ambiguous":
      return "Multiple logical agents are bound to this session's project; the session cannot pick one.";
    case "binding-ambiguous":
      return "This session's logical agent binds the same T3 project to multiple Project Service projects; fix the duplicate binding in settings.";
    case "agent-project-disabled":
      return "Project work is disabled for this session's logical agent.";
    case "no-binding":
      return "This session's T3 project is not bound to a Project Service project for its logical agent.";
    case "capability-disabled":
      return "The required Project work capability is not enabled for this session.";
    case "service-unreachable":
      return "The Project Service could not be reached.";
    case "service-unavailable":
      return "The Project Service is currently unavailable.";
  }
};

/**
 * The trusted execution context a Project tool runs under. Every identity
 * field is resolved server-side from the session + settings; tool arguments
 * can neither supply nor override any of them.
 */
export interface ProjectToolExecutionContext {
  /** The logical agent this session acts as, from the #5 settings routing. */
  readonly logicalAgentId: LogicalAgentId;
  /** The session thread's T3 project. */
  readonly t3ProjectId: ProjectId;
  /** The Project Service project bound to that T3 project for this agent. */
  readonly projectServiceProjectId: string;
  /** Live capability scopes (independent of the Browser preview scope). */
  readonly capabilities: ReadonlySet<McpCapability>;
}

export type ProjectToolContextResolution =
  | { readonly ok: true; readonly context: ProjectToolExecutionContext }
  | { readonly ok: false; readonly reason: ProjectWorkUnavailabilityReason };

const bindingFor = (
  agent: {
    readonly projectBindings: ReadonlyArray<{
      readonly t3ProjectId: ProjectId;
      readonly projectId: string;
    }>;
  },
  t3ProjectId: ProjectId,
): { readonly t3ProjectId: ProjectId; readonly projectId: string } | "ambiguous" | undefined => {
  // Settings writes reject duplicate bindings for one agent, but settings.json
  // can be hand-edited; resolution must not silently pick the first entry.
  const matches = agent.projectBindings.filter((binding) => binding.t3ProjectId === t3ProjectId);
  return matches.length > 1 ? "ambiguous" : matches[0];
};

/**
 * Resolve the trusted execution context for a Project tool call from live
 * settings plus the session's provider instance and thread project. Derived
 * per call, so capability or binding changes take effect immediately without
 * disturbing the session.
 */
export const resolveProjectToolContext = (input: {
  readonly settings: ServerSettings;
  readonly providerInstanceId: ProviderInstanceId;
  readonly t3ProjectId: ProjectId | undefined;
}): ProjectToolContextResolution => {
  if (!input.settings.projectServiceClient.enabled) {
    return { ok: false, reason: "integration-disabled" };
  }
  if (input.t3ProjectId === undefined) {
    return { ok: false, reason: "thread-unavailable" };
  }
  const t3ProjectId = input.t3ProjectId;
  const routed = Object.entries(input.settings.logicalAgents).filter(
    ([, agent]) => agent.providerInstanceId === input.providerInstanceId,
  );
  if (routed.length === 0) {
    return { ok: false, reason: "agent-unbound" };
  }
  // Among the agents routed to this provider instance, only those bound to
  // this T3 project are eligible; more than one is a genuine ambiguity the
  // server must not silently break.
  const bound = routed.filter(([, agent]) => bindingFor(agent, t3ProjectId) !== undefined);
  if (bound.length > 1) {
    return { ok: false, reason: "agent-ambiguous" };
  }
  const selected = bound[0] ?? routed[0];
  if (selected === undefined) {
    return { ok: false, reason: "agent-unbound" };
  }
  const [agentId, agent] = selected;
  if (!agent.project.enabled) {
    return { ok: false, reason: "agent-project-disabled" };
  }
  const binding = bindingFor(agent, t3ProjectId);
  if (binding === undefined) {
    return { ok: false, reason: "no-binding" };
  }
  if (binding === "ambiguous") {
    return { ok: false, reason: "binding-ambiguous" };
  }
  return {
    ok: true,
    context: {
      logicalAgentId: agentId as LogicalAgentId,
      t3ProjectId,
      projectServiceProjectId: binding.projectId,
      capabilities: McpCapabilities.deriveMcpCapabilities(input.settings, input.providerInstanceId),
    },
  };
};
