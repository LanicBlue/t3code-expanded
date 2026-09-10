import { ProviderInstanceId } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";
import {
  appendMember,
  appendMemberFromTemplate,
  type ImBridgeInstanceEntry,
  type ImBridgeMember,
  type ImBridgeMembers,
  memberOptionSelections,
  memberOptionsFromSelections,
  newMemberId,
  patchMember,
  removeMember,
} from "./ImBridgeSettings.logic";

const makeEntry = (
  overrides: Partial<ImBridgeInstanceEntry> & Pick<ImBridgeInstanceEntry, "instanceId">,
): ImBridgeInstanceEntry => ({
  displayName: overrides.instanceId,
  enabled: true,
  isAvailable: true,
  models: [],
  ...overrides,
});

const codex = makeEntry({
  instanceId: ProviderInstanceId.make("codex"),
  models: [
    { slug: "gpt-5.6-luna", name: "Luna", isDefault: true },
    { slug: "gpt-5.6-mini", name: "Mini" },
  ],
});
const claude = makeEntry({
  instanceId: ProviderInstanceId.make("claude_personal"),
  displayName: "Claude Personal",
  models: [{ slug: "sonnet", name: "Sonnet" }],
});
const disabledGrok = makeEntry({
  instanceId: ProviderInstanceId.make("grok"),
  enabled: false,
  models: [{ slug: "grok-4", name: "Grok 4" }],
});

const makeMembers = (...ids: string[]): ImBridgeMembers =>
  ids.map((id) => ({
    id,
    instanceId: ProviderInstanceId.make("codex"),
    model: "gpt-5.6-luna",
    runtimeMode: "full-access" as const,
  }));

describe("newMemberId", () => {
  it("generates opaque m-<hex> ids that satisfy the bridge's member id shape", () => {
    const id = newMemberId();
    expect(id).toMatch(/^m-[0-9a-f]{8}$/);
    expect(newMemberId()).not.toBe(id);
  });
});

describe("appendMember", () => {
  it("targets the first enabled and available instance, disabled until configured", () => {
    expect(
      appendMember([], [disabledGrok, codex, claude], { idGenerator: () => "aaaaaaaa" }),
    ).toEqual([
      {
        id: "m-aaaaaaaa",
        instanceId: "codex",
        model: "gpt-5.6-luna",
        runtimeMode: "full-access",
        enabled: false,
      },
    ]);
  });

  it("falls back to the first instance with models and redraws colliding generated ids", () => {
    const members = makeMembers("m-taken000");
    const appended = appendMember(members, [disabledGrok], {
      idGenerator: (() => {
        let calls = 0;
        return () => (calls++ === 0 ? "taken000" : "fresh1111");
      })(),
    });
    expect(appended).toHaveLength(2);
    expect(appended[1]).toMatchObject({ id: "m-fresh1111", instanceId: "grok", model: "grok-4" });
  });

  it("leaves the roster unchanged when no instance reports a model", () => {
    const modelless = makeEntry({ instanceId: ProviderInstanceId.make("empty") });
    expect(appendMember([], [modelless])).toEqual([]);
    expect(appendMember([], [])).toEqual([]);
  });
});

describe("appendMemberFromTemplate", () => {
  it("prefills displayName and persona on a disabled row with default instance/model", () => {
    const [row] = appendMemberFromTemplate([], [disabledGrok, codex], {
      name: "软件架构师",
      prompt: "你是软件架构师……",
    });
    expect(row?.id).toMatch(/^m-[0-9a-f]{8}$/);
    expect(row).toMatchObject({
      displayName: "软件架构师",
      persona: "你是软件架构师……",
      instanceId: "codex",
      model: "gpt-5.6-luna",
      runtimeMode: "full-access",
      enabled: false,
    });
  });

  it("passes through unchanged when no instance reports a model", () => {
    const members = makeMembers("m-member000");
    expect(appendMemberFromTemplate(members, [], { name: "x", prompt: "y" })).toBe(members);
  });
});

describe("patchMember", () => {
  it("patches only the addressed row", () => {
    const members = makeMembers("alice", "bob");
    expect(patchMember(members, 0, { runtimeMode: "auto" })).toEqual([
      { ...members[0], runtimeMode: "auto" },
      members[1],
    ]);
    expect(patchMember(members, 1, { enabled: false, model: "sonnet" })).toEqual([
      members[0],
      { ...members[1], enabled: false, model: "sonnet" },
    ]);
  });

  it("ignores out-of-range indexes", () => {
    const members = makeMembers("alice");
    expect(patchMember(members, -1, { id: "x" })).toBe(members);
    expect(patchMember(members, 1, { id: "x" })).toBe(members);
  });

  it("clears a key entirely when the patch value is undefined", () => {
    const members: ImBridgeMembers = [
      {
        id: "alice",
        instanceId: ProviderInstanceId.make("codex"),
        model: "gpt-5.6-luna",
        options: { effort: "high" },
        runtimeMode: "auto",
      },
    ];
    const patched = patchMember(members, 0, { model: "gpt-5.6-mini", options: undefined });
    expect(patched[0]).toEqual({
      id: "alice",
      instanceId: "codex",
      model: "gpt-5.6-mini",
      runtimeMode: "auto",
    });
    expect("options" in (patched[0] as ImBridgeMember)).toBe(false);
  });
});

describe("removeMember", () => {
  it("removes the addressed row and ignores out-of-range indexes", () => {
    const members = makeMembers("alice", "bob");
    expect(removeMember(members, 0)).toEqual([members[1]]);
    expect(removeMember(members, 5)).toBe(members);
  });
});

describe("memberOptionSelections", () => {
  it("keeps string and boolean values and drops anything else", () => {
    expect(
      memberOptionSelections({
        effort: "high",
        thinking: true,
        fastMode: false,
        count: 2,
        missing: null,
        nested: { effort: "high" },
      }),
    ).toEqual([
      { id: "effort", value: "high" },
      { id: "thinking", value: true },
      { id: "fastMode", value: false },
    ]);
  });

  it("trims string values and skips empty ones", () => {
    expect(memberOptionSelections(undefined)).toEqual([]);
    expect(memberOptionSelections({})).toEqual([]);
    expect(memberOptionSelections({ effort: "   " })).toEqual([]);
    expect(memberOptionSelections({ effort: " high " })).toEqual([{ id: "effort", value: "high" }]);
  });
});

describe("memberOptionsFromSelections", () => {
  it("writes selections as an id-to-value record", () => {
    expect(
      memberOptionsFromSelections([
        { id: "effort", value: "high" },
        { id: "thinking", value: true },
      ]),
    ).toEqual({ effort: "high", thinking: true });
  });

  it("returns undefined when there is nothing to persist", () => {
    expect(memberOptionsFromSelections(undefined)).toBeUndefined();
    expect(memberOptionsFromSelections(null)).toBeUndefined();
    expect(memberOptionsFromSelections([])).toBeUndefined();
  });
});
