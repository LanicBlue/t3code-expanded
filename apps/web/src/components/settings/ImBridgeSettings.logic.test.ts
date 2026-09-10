import { ProviderInstanceId } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";
import {
  appendMember,
  type ImBridgeInstanceEntry,
  type ImBridgeMember,
  type ImBridgeMembers,
  memberIdsDuplicate,
  memberOptionSelections,
  memberOptionsFromSelections,
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

describe("appendMember", () => {
  it("targets the first enabled and available instance, disabled until configured", () => {
    expect(appendMember([], [disabledGrok, codex, claude])).toEqual([
      {
        id: "member-1",
        instanceId: "codex",
        model: "gpt-5.6-luna",
        runtimeMode: "full-access",
        enabled: false,
      },
    ]);
  });

  it("falls back to the first instance with models and avoids taken placeholder ids", () => {
    const members = makeMembers("member-1", "member-2");
    const appended = appendMember(members, [disabledGrok]);
    expect(appended).toHaveLength(3);
    expect(appended[2]).toMatchObject({ id: "member-3", instanceId: "grok", model: "grok-4" });
  });

  it("leaves the roster unchanged when no instance reports a model", () => {
    const modelless = makeEntry({ instanceId: ProviderInstanceId.make("empty") });
    expect(appendMember([], [modelless])).toEqual([]);
    expect(appendMember([], [])).toEqual([]);
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

describe("memberIdsDuplicate", () => {
  it("flags every row sharing an id and no others", () => {
    const members = makeMembers("alice", "bob", "alice");
    expect(memberIdsDuplicate(members, 0)).toBe(true);
    expect(memberIdsDuplicate(members, 1)).toBe(false);
    expect(memberIdsDuplicate(members, 2)).toBe(true);
    expect(memberIdsDuplicate(members, 3)).toBe(false);
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
