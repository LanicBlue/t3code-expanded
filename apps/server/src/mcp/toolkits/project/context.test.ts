import { describe, expect, it } from "vite-plus/test";
import {
  DEFAULT_SERVER_SETTINGS,
  type LogicalAgentId,
  ProjectId,
  ProviderInstanceId,
  type ServerSettings,
} from "@t3tools/contracts";

import { resolveProjectToolContext } from "./context.ts";

const providerInstanceId = ProviderInstanceId.make("codex");
const otherInstance = ProviderInstanceId.make("claude-agent");
const t3ProjectId = ProjectId.make("t3-project-1");
const otherT3ProjectId = ProjectId.make("t3-project-2");

const agent = (overrides?: Partial<ServerSettings["logicalAgents"][LogicalAgentId]>) => ({
  agentName: "Agent One",
  providerInstanceId,
  project: { enabled: true },
  projectBindings: [{ projectId: "proj_ps_1", projectName: "PS Project", t3ProjectId }],
  ...overrides,
});

const settingsWith = (
  logicalAgents: Record<string, ReturnType<typeof agent>>,
  enabled = true,
): ServerSettings => ({
  ...DEFAULT_SERVER_SETTINGS,
  projectServiceClient: { ...DEFAULT_SERVER_SETTINGS.projectServiceClient, enabled },
  // Object-literal keys lose the branded Record key; restore it at one cast.
  logicalAgents: logicalAgents as ServerSettings["logicalAgents"],
});

describe("resolveProjectToolContext", () => {
  it("resolves the logical agent, T3 project, bound service project, and live capabilities", () => {
    const resolution = resolveProjectToolContext({
      settings: settingsWith({ ag_one: agent() }),
      providerInstanceId,
      t3ProjectId,
    });

    expect(resolution).toEqual({
      ok: true,
      context: {
        logicalAgentId: "ag_one",
        t3ProjectId,
        projectServiceProjectId: "proj_ps_1",
        capabilities: new Set(["preview", "project.work.read", "project.work.write"]),
      },
    });
  });

  it("grants project scopes with browser access withheld — the scopes are independent", () => {
    const resolution = resolveProjectToolContext({
      settings: { ...settingsWith({ ag_one: agent() }), enableAgentBrowserAccess: false },
      providerInstanceId,
      t3ProjectId,
    });

    expect(resolution.ok).toBe(true);
    if (resolution.ok) {
      expect([...resolution.context.capabilities]).toEqual([
        "project.work.read",
        "project.work.write",
      ]);
    }
  });

  it("answers integration-disabled when the client is off, regardless of agents", () => {
    const resolution = resolveProjectToolContext({
      settings: settingsWith({ ag_one: agent() }, false),
      providerInstanceId,
      t3ProjectId,
    });

    expect(resolution).toEqual({ ok: false, reason: "integration-disabled" });
  });

  it("answers thread-unavailable when the thread's project cannot be resolved", () => {
    const resolution = resolveProjectToolContext({
      settings: settingsWith({ ag_one: agent() }),
      providerInstanceId,
      t3ProjectId: undefined,
    });

    expect(resolution).toEqual({ ok: false, reason: "thread-unavailable" });
  });

  it("answers agent-unbound when no logical agent is routed to this provider instance", () => {
    const resolution = resolveProjectToolContext({
      settings: settingsWith({ ag_one: { ...agent(), providerInstanceId: otherInstance } }),
      providerInstanceId,
      t3ProjectId,
    });

    expect(resolution).toEqual({ ok: false, reason: "agent-unbound" });
  });

  it("answers agent-project-disabled when the routed agent has project work off", () => {
    const resolution = resolveProjectToolContext({
      settings: settingsWith({ ag_one: { ...agent(), project: { enabled: false } } }),
      providerInstanceId,
      t3ProjectId,
    });

    expect(resolution).toEqual({ ok: false, reason: "agent-project-disabled" });
  });

  it("answers no-binding when the session's T3 project is not bound for the agent", () => {
    const resolution = resolveProjectToolContext({
      settings: settingsWith({ ag_one: agent() }),
      providerInstanceId,
      t3ProjectId: otherT3ProjectId,
    });

    expect(resolution).toEqual({ ok: false, reason: "no-binding" });
  });

  it("prefers the routed agent bound to this T3 project when several share an instance", () => {
    const resolution = resolveProjectToolContext({
      settings: settingsWith({
        ag_one: agent(),
        ag_two: {
          ...agent(),
          projectBindings: [
            { projectId: "proj_ps_2", projectName: "Other", t3ProjectId: otherT3ProjectId },
          ],
        },
      }),
      providerInstanceId,
      t3ProjectId,
    });

    expect(resolution.ok).toBe(true);
    if (resolution.ok) {
      expect(resolution.context.logicalAgentId).toBe("ag_one");
      expect(resolution.context.projectServiceProjectId).toBe("proj_ps_1");
    }
  });

  it("answers agent-ambiguous when two routed agents bind the same T3 project", () => {
    const resolution = resolveProjectToolContext({
      settings: settingsWith({
        ag_one: agent(),
        ag_two: {
          ...agent(),
          projectBindings: [{ projectId: "proj_ps_2", projectName: "Also Bound", t3ProjectId }],
        },
      }),
      providerInstanceId,
      t3ProjectId,
    });

    expect(resolution).toEqual({ ok: false, reason: "agent-ambiguous" });
  });

  it("answers binding-ambiguous when one agent binds the same T3 project twice", () => {
    // Settings writes reject this, but settings.json can be hand-edited;
    // resolution must not silently pick the first entry.
    const resolution = resolveProjectToolContext({
      settings: settingsWith({
        ag_one: {
          ...agent(),
          projectBindings: [
            { projectId: "proj_ps_1", projectName: "PS Project", t3ProjectId },
            { projectId: "proj_ps_2", projectName: "Other Project", t3ProjectId },
          ],
        },
      }),
      providerInstanceId,
      t3ProjectId,
    });

    expect(resolution).toEqual({ ok: false, reason: "binding-ambiguous" });
  });
});
