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
  "project-unavailable",
  "agent-ambiguous",
  "agent-project-disabled",
  "project-not-registered",
  "project-ambiguous",
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
    case "project-unavailable":
      return "This session's T3 project is no longer available, so its workspace directory cannot be resolved.";
    case "agent-ambiguous":
      return "Multiple logical agents for this session's provider instance have Project work enabled; the session cannot pick one.";
    case "agent-project-disabled":
      return "No logical agent for this session's provider instance has Project work enabled.";
    case "project-not-registered":
      return "This session's project directory is not registered with the Project Service.";
    case "project-ambiguous":
      return "Multiple Project Service projects share this session's project directory; the session cannot pick one.";
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
  /** The Project Service project registered for that project's directory. */
  readonly projectServiceProjectId: string;
  /** Live capability scopes (independent of the Browser preview scope). */
  readonly capabilities: ReadonlySet<McpCapability>;
}

export type ProjectToolContextResolution =
  | { readonly ok: true; readonly context: ProjectToolExecutionContext }
  | { readonly ok: false; readonly reason: ProjectWorkUnavailabilityReason };

/** A Project Service project as the directory-keyed mapping sees it. */
export interface ProjectServiceDirectoryProject {
  readonly projectId: string;
  readonly workspaceDir: string;
}

/**
 * Normalize a directory for matching. Both sides arrive CANONICALIZED at the
 * seam (issue #6 review): the Project Service serves workspaceDir realpath'd
 * (+ Windows case-fold), and handlers.resolveContext passes the stored T3
 * workspaceRoot through the same canonicalWorkspaceDirectory key first — so
 * what remains here is lexical tolerance for separator and trailing-slash
 * differences only.
 */
const normalizeDirectoryKey = (directory: string): string =>
  directory.replaceAll("\\", "/").replace(/\/+$/, "");

/**
 * Resolve the trusted execution context for a Project tool call from live
 * settings, the session's thread project, and the Project Service project
 * list. The Project Service project is matched by the session project's
 * WORKSPACE DIRECTORY — the same key notice routing auto-creates/reuses
 * local projects on — so no per-agent project configuration is consulted.
 * Derived per call, so capability or registration changes take effect
 * immediately without disturbing the session.
 */
export const resolveProjectToolContext = (input: {
  readonly settings: ServerSettings;
  readonly providerInstanceId: ProviderInstanceId;
  readonly t3ProjectId: ProjectId | undefined;
  /** The thread's T3 project workspace root; undefined when unreadable. */
  readonly workspaceRoot: string | undefined;
  /** The Project Service project list (ordinary-client-legal read). */
  readonly serviceProjects: ReadonlyArray<ProjectServiceDirectoryProject>;
}): ProjectToolContextResolution => {
  if (!input.settings.projectServiceClient.enabled) {
    return { ok: false, reason: "integration-disabled" };
  }
  if (input.t3ProjectId === undefined) {
    return { ok: false, reason: "thread-unavailable" };
  }
  const t3ProjectId = input.t3ProjectId;
  if (input.workspaceRoot === undefined) {
    return { ok: false, reason: "project-unavailable" };
  }
  // Same eligibility rule as McpCapabilities.projectWorkEnabled: the agents
  // routed to this provider instance with Project work enabled. None means
  // the session has no Project identity; more than one is a genuine
  // ambiguity the server must not silently break.
  const eligible = Object.entries(input.settings.logicalAgents).filter(
    ([, agent]) => agent.providerInstanceId === input.providerInstanceId && agent.project.enabled,
  );
  if (eligible.length === 0) {
    return { ok: false, reason: "agent-project-disabled" };
  }
  if (eligible.length > 1) {
    return { ok: false, reason: "agent-ambiguous" };
  }
  const selected = eligible[0];
  if (selected === undefined) {
    return { ok: false, reason: "agent-project-disabled" };
  }
  const [agentId] = selected;
  const directoryKey = normalizeDirectoryKey(input.workspaceRoot);
  const matches = input.serviceProjects.filter(
    (project) => normalizeDirectoryKey(project.workspaceDir) === directoryKey,
  );
  if (matches.length === 0) {
    return { ok: false, reason: "project-not-registered" };
  }
  if (matches.length > 1) {
    return { ok: false, reason: "project-ambiguous" };
  }
  const registered = matches[0];
  if (registered === undefined) {
    return { ok: false, reason: "project-not-registered" };
  }
  return {
    ok: true,
    context: {
      logicalAgentId: agentId as LogicalAgentId,
      t3ProjectId,
      projectServiceProjectId: registered.projectId,
      capabilities: McpCapabilities.deriveMcpCapabilities(input.settings, input.providerInstanceId),
    },
  };
};
