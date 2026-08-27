import { describe, expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";

import { ProviderInstanceId } from "./providerInstance.ts";
import {
  DEFAULT_PROJECT_SERVICE_BASE_URL,
  isLocalProjectServiceBaseUrl,
  LogicalAgentConfig,
  LogicalAgentId,
  MissionEndedNoticeFacts,
  parseProjectServiceCredential,
  ProjectServiceClientSettings,
  ProjectWorkVisitSubmitResult,
  ProjectWorkVisitView,
} from "./projectService.ts";
import { DEFAULT_SERVER_SETTINGS, ServerSettings, ServerSettingsPatch } from "./settings.ts";

const decodeLogicalAgentId = Schema.decodeUnknownSync(LogicalAgentId);
const decodeLogicalAgentConfig = Schema.decodeUnknownSync(LogicalAgentConfig);
const decodeClientSettings = Schema.decodeUnknownSync(ProjectServiceClientSettings);
const decodeServerSettings = Schema.decodeUnknownSync(ServerSettings);
const decodeServerSettingsPatch = Schema.decodeUnknownSync(ServerSettingsPatch);
const encodeServerSettings = Schema.encodeSync(ServerSettings);

describe("parseProjectServiceCredential", () => {
  it("splits a well-formed credential", () => {
    expect(parseProjectServiceCredential("psk_key-1.OPAQUE_SECRET.abc")).toEqual({
      keyId: "key-1",
      secret: "OPAQUE_SECRET.abc",
    });
  });

  it("rejects values without the psk_ prefix or a separator", () => {
    expect(parseProjectServiceCredential("sk_key.secret")).toBeNull();
    expect(parseProjectServiceCredential("psk_noseparator")).toBeNull();
    expect(parseProjectServiceCredential("psk_.secret")).toBeNull();
    expect(parseProjectServiceCredential("psk_keyId.")).toBeNull();
  });
});

describe("LogicalAgentId", () => {
  it("accepts ag_-prefixed slugs", () => {
    expect(decodeLogicalAgentId("ag_0f3b2c")).toBe("ag_0f3b2c");
    expect(decodeLogicalAgentId("ag_my-agent_2")).toBe("ag_my-agent_2");
  });

  it("rejects ids without the ag_ prefix", () => {
    expect(() => decodeLogicalAgentId("0f3b2c")).toThrow();
    expect(() => decodeLogicalAgentId("agent-1")).toThrow();
    expect(() => decodeLogicalAgentId("ag_")).toThrow();
  });
});

describe("ProjectServiceClientSettings", () => {
  it("decodes the disabled loopback default", () => {
    expect(decodeClientSettings({})).toEqual({
      enabled: false,
      baseUrl: DEFAULT_PROJECT_SERVICE_BASE_URL,
      keyIdHint: "",
      credentialSet: false,
      // work-mission-v5 Phase 7: the mission.v1 gate default is ON — the
      // mission line is THE line; an explicit false is the off-switch.
      missionsEnabled: true,
    });
  });

  it("decodes an explicitly enabled mission gate", () => {
    expect(
      decodeClientSettings({ enabled: true, missionsEnabled: true, credentialSet: true }),
    ).toEqual({
      enabled: true,
      baseUrl: DEFAULT_PROJECT_SERVICE_BASE_URL,
      keyIdHint: "",
      credentialSet: true,
      missionsEnabled: true,
    });
  });
});

describe("work-mission-v5 wire types", () => {
  const decodeVisitView = Schema.decodeUnknownSync(ProjectWorkVisitView);

  it("decodes the pinned §6.1 visit view: mission, work, visit action", () => {
    expect(
      decodeVisitView({
        mission: { id: "ms_abc", name: "Release v2", objective: "Ship the release" },
        work: { group: "ms_abc", workKey: "implement", iteration: 2 },
        action: { kind: "visit", outcomes: ["implementation-ready"], candidates: ["validation"] },
      }),
    ).toEqual({
      mission: { id: "ms_abc", name: "Release v2", objective: "Ship the release" },
      work: { group: "ms_abc", workKey: "implement", iteration: 2 },
      action: { kind: "visit", outcomes: ["implementation-ready"], candidates: ["validation"] },
    });
  });

  it("rejects a non-visit action kind and an outcomeless submit result", () => {
    expect(() =>
      decodeVisitView({
        mission: { id: "ms_abc", name: "n", objective: "o" },
        work: { group: "ms_abc", workKey: "implement", iteration: 1 },
        action: { kind: "state", abandonAvailable: true },
      }),
    ).toThrow();
    expect(() =>
      Schema.decodeUnknownSync(ProjectWorkVisitSubmitResult)({ nextNode: "design" }),
    ).toThrow();
    expect(() =>
      Schema.decodeUnknownSync(MissionEndedNoticeFacts)({
        noticeId: "mne_x",
        missionId: "ms_abc",
        group: "ms_abc",
        disposition: "exploded",
      }),
    ).toThrow();
    expect(
      Schema.decodeUnknownSync(MissionEndedNoticeFacts)({
        noticeId: "mne_x",
        missionId: "ms_abc",
        group: "ms_abc",
        disposition: "abandoned",
      }),
    ).toEqual({
      noticeId: "mne_x",
      missionId: "ms_abc",
      group: "ms_abc",
      disposition: "abandoned",
    });
  });
});

describe("isLocalProjectServiceBaseUrl", () => {
  it("accepts loopback and private-range hosts", () => {
    expect(isLocalProjectServiceBaseUrl(DEFAULT_PROJECT_SERVICE_BASE_URL)).toBe(true);
    expect(isLocalProjectServiceBaseUrl("http://localhost:7600")).toBe(true);
    expect(isLocalProjectServiceBaseUrl("http://127.1.2.3:7600/project/")).toBe(true);
    expect(isLocalProjectServiceBaseUrl("http://[::1]:7600")).toBe(true);
    expect(isLocalProjectServiceBaseUrl("http://10.0.0.5:7600")).toBe(true);
    expect(isLocalProjectServiceBaseUrl("http://172.16.0.1:7600")).toBe(true);
    expect(isLocalProjectServiceBaseUrl("http://172.31.255.255:7600")).toBe(true);
    expect(isLocalProjectServiceBaseUrl("https://192.168.1.20")).toBe(true);
  });

  it("rejects public hosts, out-of-range privates, and non-http(s) or unparseable URLs", () => {
    expect(isLocalProjectServiceBaseUrl("http://example.com")).toBe(false);
    expect(isLocalProjectServiceBaseUrl("http://8.8.8.8:7600")).toBe(false);
    expect(isLocalProjectServiceBaseUrl("http://172.32.0.1:7600")).toBe(false);
    expect(isLocalProjectServiceBaseUrl("http://172.15.255.255:7600")).toBe(false);
    expect(isLocalProjectServiceBaseUrl("http://193.168.1.1:7600")).toBe(false);
    expect(isLocalProjectServiceBaseUrl("ftp://127.0.0.1:7600")).toBe(false);
    expect(isLocalProjectServiceBaseUrl("not a url")).toBe(false);
    expect(isLocalProjectServiceBaseUrl("")).toBe(false);
  });
});

describe("ServerSettings project service surfaces", () => {
  it("defaults the client and the agent map", () => {
    expect(DEFAULT_SERVER_SETTINGS.projectServiceClient.credentialSet).toBe(false);
    expect(DEFAULT_SERVER_SETTINGS.logicalAgents).toEqual({});
  });

  it("round-trips agents through encode", () => {
    const decoded = decodeServerSettings({
      logicalAgents: {
        ag_main: {
          agentName: "Build agent",
          providerInstanceId: "codex",
          project: { enabled: true },
        },
      },
    });
    const agent = Object.values(decoded.logicalAgents)[0];
    expect(agent).toEqual({
      agentName: "Build agent",
      providerInstanceId: "codex",
      persona: "",
      thinkLevel: null,
      modelOverride: null,
      project: { enabled: true, sessionScope: "project", sessionRetention: "settle" },
    });
    expect(encodeServerSettings(decoded).logicalAgents).toEqual(decoded.logicalAgents);
  });

  it("fills agent defaults on decode", () => {
    expect(
      decodeLogicalAgentConfig({
        agentName: "A",
        providerInstanceId: ProviderInstanceId.make("codex"),
      }),
    ).toEqual({
      agentName: "A",
      providerInstanceId: "codex",
      persona: "",
      thinkLevel: null,
      modelOverride: null,
      project: { enabled: false, sessionScope: "project", sessionRetention: "settle" },
    });
  });

  it("tolerates a blank hand-edited thinkLevel instead of rejecting the file", () => {
    // A hand-edited "" must decode (treated as no think level at use time);
    // rejecting it would take the whole settings file down.
    expect(
      decodeLogicalAgentConfig({
        agentName: "A",
        providerInstanceId: ProviderInstanceId.make("codex"),
        thinkLevel: "",
      }).thinkLevel,
    ).toBe("");
    expect(
      decodeLogicalAgentConfig({
        agentName: "A",
        providerInstanceId: ProviderInstanceId.make("codex"),
        thinkLevel: "high",
      }).thinkLevel,
    ).toBe("high");
  });

  it("decodes the agent's session scope, defaulting to one session per project", () => {
    expect(
      decodeLogicalAgentConfig({ agentName: "A", providerInstanceId: "codex" }).project
        .sessionScope,
    ).toBe("project");
    expect(
      decodeLogicalAgentConfig({
        agentName: "A",
        providerInstanceId: "codex",
        project: { enabled: true, sessionScope: "flow-instance", sessionRetention: "settle" },
      }).project,
    ).toEqual({ enabled: true, sessionScope: "flow-instance", sessionRetention: "settle" });
  });

  it("strips a stale projectBindings key from a hand-edited settings.json silently", () => {
    // Issue #6 deleted the bindings surface; a live settings.json written by
    // an older build still carries the key. Decode must parse the rest of the
    // agent and drop the unknown key rather than reject the whole file.
    const decoded = decodeServerSettings({
      logicalAgents: {
        ag_main: {
          agentName: "Build agent",
          providerInstanceId: "codex",
          project: { enabled: true },
          projectBindings: [
            { projectId: "proj_9", projectName: "Wiki migration", t3ProjectId: "proj-local-1" },
          ],
        },
      },
    });
    const agent = decoded.logicalAgents[LogicalAgentId.make("ag_main")];
    expect(agent).toEqual({
      agentName: "Build agent",
      providerInstanceId: "codex",
      persona: "",
      thinkLevel: null,
      modelOverride: null,
      project: { enabled: true, sessionScope: "project", sessionRetention: "settle" },
    });
    expect(JSON.stringify(decoded)).not.toContain("projectBindings");
  });
});

describe("ServerSettingsPatch project service surfaces", () => {
  it("accepts a write-only credential on the patch", () => {
    expect(
      decodeServerSettingsPatch({
        projectServiceClient: { enabled: true, newCredential: "psk_k.s3cret" },
      }),
    ).toEqual({
      projectServiceClient: { enabled: true, newCredential: "psk_k.s3cret" },
    });
  });

  it("accepts a whole-map agent replacement", () => {
    const patch = decodeServerSettingsPatch({
      logicalAgents: {
        ag_one: { agentName: "One", providerInstanceId: "codex" },
      },
    });
    expect(Object.keys(patch.logicalAgents ?? {})).toEqual(["ag_one"]);
  });

  it("strips a stale projectBindings key from an agent patch", () => {
    // A client built before issue #6 may still echo the deleted key back in
    // a whole-map agent replacement; the patch must apply with the key
    // dropped, not be rejected.
    const patch = decodeServerSettingsPatch({
      logicalAgents: {
        ag_one: {
          agentName: "One",
          providerInstanceId: "codex",
          projectBindings: [{ projectId: "", projectName: "x", t3ProjectId: "p" }],
        },
      },
    });
    expect(patch.logicalAgents?.[LogicalAgentId.make("ag_one")]).toEqual({
      agentName: "One",
      providerInstanceId: "codex",
      persona: "",
      thinkLevel: null,
      modelOverride: null,
      project: { enabled: false, sessionScope: "project", sessionRetention: "settle" },
    });
  });
});
