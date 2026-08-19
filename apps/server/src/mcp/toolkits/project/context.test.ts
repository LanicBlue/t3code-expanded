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
const WORKSPACE_ROOT = "/srv/registry";

const agent = (overrides?: Partial<ServerSettings["logicalAgents"][LogicalAgentId]>) => ({
  agentName: "Agent One",
  providerInstanceId,
  persona: "",
  modelOverride: null,
  project: { enabled: true },
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

const serviceProjects = (dirs: ReadonlyArray<string>) =>
  dirs.map((workspaceDir, index) => ({
    projectId: `proj_ps_${index + 1}`,
    workspaceDir,
  }));

const resolve = (input: {
  readonly logicalAgents: Record<string, ReturnType<typeof agent>>;
  /** The session's bound logical agent; omitted for unbound sessions. */
  readonly logicalAgentId?: string | undefined;
  /** `undefined` simulates a project shell whose root could not be read. */
  readonly workspaceRoot?: string | undefined;
  readonly serviceProjects: ReadonlyArray<{
    readonly projectId: string;
    readonly workspaceDir: string;
  }>;
  readonly enabled?: boolean;
}) =>
  resolveProjectToolContext({
    settings: settingsWith(input.logicalAgents, input.enabled ?? true),
    providerInstanceId,
    ...(input.logicalAgentId === undefined
      ? {}
      : { logicalAgentId: input.logicalAgentId as LogicalAgentId }),
    t3ProjectId,
    workspaceRoot: "workspaceRoot" in input ? input.workspaceRoot : WORKSPACE_ROOT,
    serviceProjects: input.serviceProjects,
  });

describe("resolveProjectToolContext", () => {
  it("resolves the logical agent, T3 project, directory-registered service project, and live capabilities", () => {
    const resolution = resolve({
      logicalAgents: { ag_one: agent() },
      serviceProjects: serviceProjects(["/srv/unrelated", "/srv/registry"]),
    });

    expect(resolution).toEqual({
      ok: true,
      context: {
        logicalAgentId: "ag_one",
        t3ProjectId,
        projectServiceProjectId: "proj_ps_2",
        capabilities: new Set(["preview", "project.work.read", "project.work.write"]),
      },
    });
  });

  it("grants project scopes with browser access withheld — the scopes are independent", () => {
    const resolution = resolveProjectToolContext({
      settings: { ...settingsWith({ ag_one: agent() }), enableAgentBrowserAccess: false },
      providerInstanceId,
      t3ProjectId,
      workspaceRoot: WORKSPACE_ROOT,
      serviceProjects: serviceProjects([WORKSPACE_ROOT]),
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
    const resolution = resolve({
      logicalAgents: { ag_one: agent() },
      serviceProjects: serviceProjects([WORKSPACE_ROOT]),
      enabled: false,
    });

    expect(resolution).toEqual({ ok: false, reason: "integration-disabled" });
  });

  it("answers thread-unavailable when the thread's project cannot be resolved", () => {
    const resolution = resolveProjectToolContext({
      settings: settingsWith({ ag_one: agent() }),
      providerInstanceId,
      t3ProjectId: undefined,
      workspaceRoot: undefined,
      serviceProjects: serviceProjects([WORKSPACE_ROOT]),
    });

    expect(resolution).toEqual({ ok: false, reason: "thread-unavailable" });
  });

  it("answers project-unavailable when the thread's project shell cannot be read", () => {
    const resolution = resolve({
      logicalAgents: { ag_one: agent() },
      workspaceRoot: undefined,
      serviceProjects: serviceProjects([WORKSPACE_ROOT]),
    });

    expect(resolution).toEqual({ ok: false, reason: "project-unavailable" });
  });

  it("answers agent-project-disabled when no agent on this instance has project work enabled", () => {
    const resolution = resolve({
      logicalAgents: {
        ag_one: { ...agent(), providerInstanceId: otherInstance },
        ag_far: { ...agent(), project: { enabled: false } },
      },
      serviceProjects: serviceProjects([WORKSPACE_ROOT]),
    });

    expect(resolution).toEqual({ ok: false, reason: "agent-project-disabled" });
  });

  it("answers agent-ambiguous for UNBOUND sessions when several agents on this instance have project work enabled", () => {
    const resolution = resolve({
      logicalAgents: { ag_one: agent(), ag_two: agent() },
      serviceProjects: serviceProjects([WORKSPACE_ROOT]),
    });

    expect(resolution).toEqual({ ok: false, reason: "agent-ambiguous" });
  });

  it("a BOUND session resolves its own agent when several share the instance", () => {
    const first = resolve({
      logicalAgents: { ag_one: agent(), ag_two: agent() },
      logicalAgentId: "ag_one",
      serviceProjects: serviceProjects([WORKSPACE_ROOT]),
    });
    const second = resolve({
      logicalAgents: { ag_one: agent(), ag_two: agent() },
      logicalAgentId: "ag_two",
      serviceProjects: serviceProjects([WORKSPACE_ROOT]),
    });
    expect(first).toMatchObject({ ok: true, context: { logicalAgentId: "ag_one" } });
    expect(second).toMatchObject({ ok: true, context: { logicalAgentId: "ag_two" } });
  });

  it("a bound session whose agent lacks Project work answers agent-project-disabled", () => {
    const resolution = resolve({
      logicalAgents: {
        ag_one: agent(),
        ag_off: { ...agent(), project: { enabled: false } },
      },
      logicalAgentId: "ag_off",
      serviceProjects: serviceProjects([WORKSPACE_ROOT]),
    });
    expect(resolution).toEqual({ ok: false, reason: "agent-project-disabled" });
  });

  it("a bound session whose agent rides another instance answers agent-project-disabled", () => {
    const resolution = resolve({
      logicalAgents: { ag_one: agent(), ag_far: { ...agent(), providerInstanceId: otherInstance } },
      logicalAgentId: "ag_far",
      serviceProjects: serviceProjects([WORKSPACE_ROOT]),
    });
    expect(resolution).toEqual({ ok: false, reason: "agent-project-disabled" });
  });

  it("answers project-not-registered when no Project Service project shares the directory", () => {
    const resolution = resolve({
      logicalAgents: { ag_one: agent() },
      serviceProjects: serviceProjects(["/srv/other", "/srv/third"]),
    });

    expect(resolution).toEqual({ ok: false, reason: "project-not-registered" });
  });

  it("answers project-ambiguous when several Project Service projects share the directory", () => {
    const resolution = resolve({
      logicalAgents: { ag_one: agent() },
      serviceProjects: serviceProjects([WORKSPACE_ROOT, "/srv/other", `${WORKSPACE_ROOT}/`]),
    });

    expect(resolution).toEqual({ ok: false, reason: "project-ambiguous" });
  });

  it("matches the directory lexically — trailing slashes fold, different paths and cases do not", () => {
    const same = resolve({
      logicalAgents: { ag_one: agent() },
      serviceProjects: [{ projectId: "proj_slash", workspaceDir: `${WORKSPACE_ROOT}/` }],
    });
    expect(same.ok).toBe(true);
    if (same.ok) {
      expect(same.context.projectServiceProjectId).toBe("proj_slash");
    }

    // A different absolute path — even a Windows-style spelling of a similar
    // tree — is not the session's directory.
    const foreign = resolve({
      logicalAgents: { ag_one: agent() },
      serviceProjects: [{ projectId: "proj_win", workspaceDir: "C:\\srv\\registry\\" }],
    });
    expect(foreign).toEqual({ ok: false, reason: "project-not-registered" });

    // Case is significant: both sides are server-normalized absolute paths.
    const cased = resolve({
      logicalAgents: { ag_one: agent() },
      serviceProjects: [{ projectId: "proj_case", workspaceDir: "/Srv/Registry" }],
    });
    expect(cased).toEqual({ ok: false, reason: "project-not-registered" });
  });
});
