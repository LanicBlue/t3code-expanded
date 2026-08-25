import { describe, expect, it } from "vite-plus/test";
import { ProviderInstanceId } from "@t3tools/contracts";
import {
  CONNECTION_TEST_STATUS_LABELS,
  connectionTestStatus,
  credentialStatusLabel,
  isValidCredentialPaste,
  makeEmptyLogicalAgentConfig,
  makeLogicalAgentId,
  nextAgentMapWithAgent,
  nextAgentMapWithoutAgent,
} from "./ServicesSettings.logic";

describe("makeLogicalAgentId", () => {
  it("mints a stable ag_-prefixed id from a random seed", () => {
    expect(makeLogicalAgentId("0f3b-2c1a-9d8e")).toBe("ag_0f3b-2c1a-9d8e");
  });
});

describe("makeEmptyLogicalAgentConfig", () => {
  it("seeds a disabled agent with no per-project configuration", () => {
    expect(makeEmptyLogicalAgentConfig(ProviderInstanceId.make("codex"))).toEqual({
      agentName: "New agent",
      providerInstanceId: "codex",
      persona: "",
      thinkLevel: null,
      modelOverride: null,
      project: { enabled: false, sessionScope: "project", sessionRetention: "settle" },
    });
  });
});

describe("isValidCredentialPaste", () => {
  it("accepts an empty paste and psk_ credentials", () => {
    expect(isValidCredentialPaste("")).toBe(true);
    expect(isValidCredentialPaste("psk_key-1.s3cret")).toBe(true);
  });

  it("rejects pastes that cannot be a credential", () => {
    expect(isValidCredentialPaste("psk_key-1")).toBe(false);
    expect(isValidCredentialPaste("sk_key-1.s3cret")).toBe(false);
    expect(isValidCredentialPaste("psk_.s3cret")).toBe(false);
  });
});

describe("agent map updates", () => {
  const agent = makeEmptyLogicalAgentConfig(ProviderInstanceId.make("codex"));

  it("adds and removes agents by id without disturbing others", () => {
    const withOne = nextAgentMapWithAgent({}, "ag_one", agent);
    const withTwo = nextAgentMapWithAgent(withOne, "ag_two", agent);
    expect(Object.keys(withTwo)).toEqual(["ag_one", "ag_two"]);

    const withoutOne = nextAgentMapWithoutAgent(withTwo, "ag_one");
    expect(Object.keys(withoutOne)).toEqual(["ag_two"]);
  });

  it("carries only the renamed field on rename", () => {
    const withAgent = nextAgentMapWithAgent(
      {},
      "ag_stable",
      makeEmptyLogicalAgentConfig(ProviderInstanceId.make("codex")),
    );
    const renamed = nextAgentMapWithAgent(withAgent, "ag_stable", {
      ...withAgent.ag_stable!,
      agentName: "Review agent",
    });

    expect(Object.keys(renamed)).toEqual(["ag_stable"]);
    expect(renamed.ag_stable).toEqual({ ...withAgent.ag_stable, agentName: "Review agent" });
  });
});

describe("credential + connection presentation", () => {
  it("labels stored credential state without secret material", () => {
    expect(credentialStatusLabel({ credentialSet: true, keyIdHint: "key-1" })).toBe(
      "Credential psk_key-1",
    );
    expect(credentialStatusLabel({ credentialSet: false, keyIdHint: "" })).toBe(
      "No credential stored",
    );
  });

  it("classifies connection results coarsely", () => {
    expect(
      connectionTestStatus({
        result: { reachable: true, authenticated: true, apiCompatible: true, detail: "x" },
        credentialSet: true,
      }),
    ).toBe("ok");
    expect(
      connectionTestStatus({
        result: { reachable: true, authenticated: true, apiCompatible: false, detail: "x" },
        credentialSet: true,
      }),
    ).toBe("reachable");
    expect(
      connectionTestStatus({
        result: { reachable: false, authenticated: false, apiCompatible: false, detail: "x" },
        credentialSet: true,
      }),
    ).toBe("failed");
  });

  it("splits an unauthenticated result by stored credential state", () => {
    const rejected = {
      result: { reachable: true, authenticated: false, apiCompatible: false, detail: "x" },
    } as const;
    expect(connectionTestStatus({ ...rejected, credentialSet: true })).toBe("authFailed");
    expect(connectionTestStatus({ ...rejected, credentialSet: false })).toBe("unconfigured");
    expect(CONNECTION_TEST_STATUS_LABELS.authFailed).toBe("Authentication failed");
    expect(CONNECTION_TEST_STATUS_LABELS.unconfigured).toBe("Not configured");
  });
});
