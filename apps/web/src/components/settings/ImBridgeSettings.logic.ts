/**
 * Pure helpers for the IM Bridge settings panel. The member roster is always
 * committed as one whole array (whole-section replace), so every helper here
 * returns the complete next members list rather than a per-row delta.
 *
 * The instance+model selection reuses the composer's picker data flow
 * (`ProviderModelPicker` + `getCustomModelOptionsByInstance`), so model
 * options are no longer projected here; only the member `options` record —
 * which stores `ProviderOptionSelections` in the legacy object shape the
 * schema's `Record<string, unknown>` passthrough accepts — needs helpers.
 *
 * @module ImBridgeSettings.logic
 */
import {
  DEFAULT_RUNTIME_MODE,
  type ImBridgeMemberSettings,
  type ProviderOptionSelection,
  type RuntimeMode,
} from "@t3tools/contracts";
import { imBridgeMemberIdOfThreadId } from "@t3tools/contracts/settings";

/** The instance facts the member editor needs; satisfied by `ProviderInstanceEntry`. */
export interface ImBridgeInstanceEntry {
  readonly instanceId: ImBridgeMemberSettings["instanceId"];
  readonly displayName: string;
  readonly enabled: boolean;
  readonly isAvailable: boolean;
  readonly models: ReadonlyArray<{
    readonly slug: string;
    readonly name: string;
    readonly isDefault?: boolean | undefined;
  }>;
}

export type ImBridgeMember = ImBridgeMemberSettings;
export type ImBridgeMembers = ReadonlyArray<ImBridgeMember>;

/**
 * A member patch where every field also accepts an explicit `undefined` to
 * clear the key — `Partial` alone cannot express that under
 * `exactOptionalPropertyTypes`.
 */
export type ImBridgeMemberPatch = {
  readonly [K in keyof ImBridgeMember]?: ImBridgeMember[K] | undefined;
};

/** Labels match the composer's Access menu so the four modes read the same everywhere. */
export const RUNTIME_MODE_OPTIONS: ReadonlyArray<{
  readonly value: RuntimeMode;
  readonly label: string;
}> = [
  { value: "approval-required", label: "Supervised" },
  { value: "auto-accept-edits", label: "Auto-accept edits" },
  { value: "auto", label: "Auto" },
  { value: "full-access", label: "Full access" },
];

function defaultModelFor(entry: ImBridgeInstanceEntry | undefined): string {
  if (!entry) return "";
  return entry.models.find((model) => model.isDefault)?.slug ?? entry.models[0]?.slug ?? "";
}

/**
 * Generate a fresh member id — an opaque, filename-safe machine key
 * (`m-<8 hex>`, the shape im and the bridge's thread ids require). Ids are
 * never hand-edited: the user surface is `displayName`, so a member's IM
 * identity is born once and never renamed.
 */
export function newMemberId(generate: () => string = randomHex8): string {
  return `m-${generate()}`;
}

const randomHex8 = () => {
  const bytes = crypto.getRandomValues(new Uint8Array(4));
  return Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");
};

/**
 * Append a member targeting the first enabled+available instance (falling
 * back to the first instance with models) and that instance's default model.
 * Returns the list unchanged when no instance reports a model, because a
 * member with an empty model slug cannot be persisted.
 *
 * The row starts disabled with a generated id and an empty name: the bridge
 * joins only enabled members, so drafts never reach IM.
 */
export function appendMember(
  members: ImBridgeMembers,
  entries: ReadonlyArray<ImBridgeInstanceEntry>,
  options: { readonly idGenerator?: () => string } = {},
): ImBridgeMembers {
  const entry =
    entries.find(
      (candidate) => candidate.enabled && candidate.isAvailable && candidate.models.length > 0,
    ) ?? entries.find((candidate) => candidate.models.length > 0);
  const model = defaultModelFor(entry);
  if (!entry || model.length === 0) return members;
  const taken = new Set(members.map((member) => member.id));
  let id = newMemberId(options.idGenerator);
  while (taken.has(id)) id = newMemberId(options.idGenerator);
  return [
    ...members,
    {
      id,
      instanceId: entry.instanceId,
      model,
      runtimeMode: DEFAULT_RUNTIME_MODE,
      enabled: false,
    },
  ];
}

/**
 * Append a member from a marketplace template: a normal append (default
 * instance/model, disabled until configured) with the template's name as the
 * display label and its prompt body as the persona. Returns the list
 * unchanged when no instance reports a model (same guard as appendMember).
 */
export function appendMemberFromTemplate(
  members: ImBridgeMembers,
  entries: ReadonlyArray<ImBridgeInstanceEntry>,
  template: { readonly name: string; readonly prompt: string },
): ImBridgeMembers {
  const appended = appendMember(members, entries);
  if (appended.length === members.length) return members;
  return patchMember(appended, appended.length - 1, {
    displayName: template.name,
    persona: template.prompt,
  });
}

/**
 * Replace the member at `index` with the patched fields; out-of-range indexes
 * are a no-op. A patch value of `undefined` clears the key (used to drop a
 * member's `options` when its model changes), so the patched member never
 * carries an explicitly-undefined own key.
 */
export function patchMember(
  members: ImBridgeMembers,
  index: number,
  patch: ImBridgeMemberPatch,
): ImBridgeMembers {
  if (index < 0 || index >= members.length) return members;
  return members.map((member, position) => {
    if (position !== index) return member;
    const next: Record<string, unknown> = { ...member };
    for (const [key, value] of Object.entries(patch)) {
      if (value === undefined) {
        delete next[key];
      } else {
        next[key] = value;
      }
    }
    return next as ImBridgeMember;
  });
}

/** Remove the member at `index`; out-of-range indexes are a no-op. */
export function removeMember(members: ImBridgeMembers, index: number): ImBridgeMembers {
  if (index < 0 || index >= members.length) return members;
  return members.filter((_, position) => position !== index);
}

/** Shell-thread fields the liveness rule needs; satisfied by shell snapshot rows. */
export interface ImBridgeThreadLike {
  readonly id: string;
  readonly settledAt: string | null;
  readonly settledOverride: string | null;
}

/**
 * The member's mission threads that are still live (`im-<memberId>-ms_*`,
 * neither settled nor settle-overridden) — the delete confirmation's
 * "N 个进行中会话" and nothing else: settled threads no longer strand on
 * removal, so they are not worth warning about.
 */
export function liveImBridgeThreadCount(
  threads: ReadonlyArray<ImBridgeThreadLike>,
  memberId: string,
): number {
  return threads.filter(
    (thread) =>
      imBridgeMemberIdOfThreadId(thread.id) === memberId &&
      thread.settledAt === null &&
      thread.settledOverride !== "settled",
  ).length;
}

/**
 * Read a member's stored `options` record as the `ProviderOptionSelection`
 * list the traits picker consumes. Mirrors the contracts decoder's legacy
 * object→array coercion: only non-empty trimmed strings and booleans survive,
 * so a hand-edited or stale record cannot smuggle junk values into a pick.
 */
export function memberOptionSelections(
  options: ImBridgeMember["options"],
): Array<ProviderOptionSelection> {
  const selections: Array<ProviderOptionSelection> = [];
  for (const [id, value] of Object.entries(options ?? {})) {
    if (typeof value === "string") {
      const trimmed = value.trim();
      if (trimmed.length > 0) selections.push({ id, value: trimmed });
    } else if (typeof value === "boolean") {
      selections.push({ id, value });
    }
  }
  return selections;
}

/**
 * Write traits-picker selections back into the member's `options` field as
 * the legacy object shape of `ProviderOptionSelections` (`{ [id]: value }`),
 * which both the `Record<string, unknown>` passthrough schema and the
 * `ProviderOptionSelections` decoder accept. Returns `undefined` when there
 * is nothing worth persisting so the key is dropped entirely.
 */
export function memberOptionsFromSelections(
  selections: ReadonlyArray<ProviderOptionSelection> | null | undefined,
): Record<string, unknown> | undefined {
  if (!selections || selections.length === 0) return undefined;
  const options: Record<string, unknown> = {};
  for (const { id, value } of selections) {
    options[id] = value;
  }
  return options;
}
