/**
 * IM Bridge settings — the member roster an external IM bridge runs as T3
 * threads. The bridge joins one thread per member in every IM workspace it
 * serves and re-reads this section on its reconcile tick, so edits made here
 * take effect there on the next tick rather than immediately.
 *
 * @module ImBridgeSettings
 */
import { useAtomValue } from "@effect/atom-react";
import * as Equal from "effect/Equal";
import { PlusIcon, Trash2Icon } from "lucide-react";
import { useEffect, useMemo, useState } from "react";
import type { RuntimeMode, ServerProvider } from "@t3tools/contracts";
import type { UnifiedSettings } from "@t3tools/contracts/settings";
import { useNavigate } from "@tanstack/react-router";

import {
  PRIMARY_SETTINGS_UNAVAILABLE_MESSAGE,
  usePrimarySettings,
  usePrimarySettingsAvailable,
  useUpdatePrimarySettings,
} from "../../hooks/useSettings";
import { getCustomModelOptionsByInstance } from "../../modelSelection";
import { cn } from "../../lib/utils";
import {
  applyProviderInstanceSettings,
  deriveProviderInstanceEntries,
  type ProviderInstanceEntry,
  sortProviderInstanceEntries,
} from "../../providerInstances";
import { primaryServerProvidersAtom } from "../../state/server";
import { usePrimaryEnvironmentId } from "../../state/environments";
import { ProviderModelPicker } from "../chat/ProviderModelPicker";
import { TraitsPicker } from "../chat/TraitsPicker";
import { Button } from "../ui/button";
import { Checkbox } from "../ui/checkbox";
import { DraftInput } from "../ui/draft-input";
import { Select, SelectItem, SelectPopup, SelectTrigger, SelectValue } from "../ui/select";
import {
  appendMember,
  type ImBridgeMember,
  type ImBridgeMembers,
  type ImBridgeMemberPatch,
  memberIdsDuplicate,
  memberOptionSelections,
  memberOptionsFromSelections,
  patchMember,
  removeMember,
  RUNTIME_MODE_OPTIONS,
} from "./ImBridgeSettings.logic";
import {
  SETTINGS_PICKER_TRIGGER_CLASSNAME,
  SettingsPageContainer,
  SettingsSection,
} from "./settingsLayout";
import { searchableSetting } from "./settingsSearch";

export function ImBridgeSettingsPanel() {
  return (
    <SettingsPageContainer>
      <ImBridgeMembersSection />
    </SettingsPageContainer>
  );
}

function ImBridgeMembersSection() {
  const settings = usePrimarySettings();
  const imBridge = usePrimarySettings((current) => current.imBridge);
  const updateSettings = useUpdatePrimarySettings();
  const primarySettingsAvailable = usePrimarySettingsAvailable();
  const providers = useAtomValue(primaryServerProvidersAtom);

  const entries = sortProviderInstanceEntries(
    applyProviderInstanceSettings(deriveProviderInstanceEntries(providers), settings),
  );

  // The roster commits as one whole array. While a row's id collides with
  // another, the edit is kept as a local draft and the write is withheld, so
  // the server's unique-id check never sees an invalid roster. The draft
  // clears once the persisted roster catches up with a committed write.
  const persistedMembers = imBridge.members;
  const [draftMembers, setDraftMembers] = useState<ImBridgeMembers | null>(null);
  const members = draftMembers ?? persistedMembers;
  useEffect(() => {
    if (draftMembers !== null && Equal.equals(draftMembers, persistedMembers)) {
      setDraftMembers(null);
    }
  }, [draftMembers, persistedMembers]);

  const commitMembers = (next: ImBridgeMembers) => {
    setDraftMembers(next);
    if (next.some((_, index) => memberIdsDuplicate(next, index))) return;
    updateSettings({ imBridge: { members: next } });
  };

  const canAppend = entries.some((entry) => entry.models.length > 0);

  return (
    <SettingsSection {...searchableSetting("im-bridge")} title="IM 成员">
      <div
        className={cn("px-3 pb-4 pt-1 sm:px-4", !primarySettingsAvailable && "opacity-50")}
        inert={!primarySettingsAvailable}
      >
        <p className="px-1 pb-2 text-[12px] text-muted-foreground/80">
          这些成员由外部 IM 桥以 T3 线程执行，改动在桥的下一个 reconcile tick（≤60s）生效。桥只 join
          勾选「启用」的成员：新行默认未启用，改好 id / 实例 / 模型后再勾选，占位 id
          与中途改名才不会作为成员留在 IM 里。
        </p>
        {members.length === 0 ? (
          <p className="py-6 text-center text-[13px] text-muted-foreground/80">
            尚未声明成员——加一行，桥会在每个 IM 工作区 join 该成员。
          </p>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full min-w-[680px] text-left text-[13px]">
              <thead className="border-b border-border/60 text-[11px] uppercase tracking-[0.08em] text-muted-foreground/70">
                <tr>
                  <th className="px-2 py-2.5 font-semibold">成员 id</th>
                  <th className="px-2 py-2.5 font-semibold">实例 / 模型</th>
                  <th className="px-2 py-2.5 font-semibold">权限</th>
                  <th className="px-2 py-2.5 font-semibold">启用</th>
                  <th className="w-px px-2 py-2.5" />
                </tr>
              </thead>
              <tbody className="divide-y divide-border/60">
                {members.map((member, index) => (
                  <ImBridgeMemberRow
                    key={`${index}:${member.id}`}
                    member={member}
                    entries={entries}
                    settings={settings}
                    providers={providers}
                    duplicateId={memberIdsDuplicate(members, index)}
                    onPatch={(patch) => commitMembers(patchMember(members, index, patch))}
                    onRemove={() => commitMembers(removeMember(members, index))}
                  />
                ))}
              </tbody>
            </table>
          </div>
        )}
        <div className="flex items-center justify-between gap-4 pt-3">
          <Button
            size="xs"
            variant="outline"
            disabled={!canAppend}
            onClick={() => commitMembers(appendMember(members, entries))}
          >
            <PlusIcon className="size-3" aria-hidden />
            加成员
          </Button>
          {!primarySettingsAvailable ? (
            <p className="text-xs text-muted-foreground">{PRIMARY_SETTINGS_UNAVAILABLE_MESSAGE}</p>
          ) : null}
        </div>
      </div>
    </SettingsSection>
  );
}

function ImBridgeMemberRow({
  member,
  entries,
  settings,
  providers,
  duplicateId,
  onPatch,
  onRemove,
}: {
  readonly member: ImBridgeMember;
  readonly entries: ReadonlyArray<ProviderInstanceEntry>;
  readonly settings: UnifiedSettings;
  readonly providers: ReadonlyArray<ServerProvider>;
  readonly duplicateId: boolean;
  readonly onPatch: (patch: ImBridgeMemberPatch) => void;
  readonly onRemove: () => void;
}) {
  const navigate = useNavigate();
  const environmentId = usePrimaryEnvironmentId();
  const instanceEntry = entries.find((entry) => entry.instanceId === member.instanceId) ?? null;
  // Built exactly like the composer's picker input, so a member's row offers
  // the same instance rail + searchable model combobox, custom models
  // included; a member whose model left the instance's catalog keeps it as an
  // unavailable row (opencode/antigravity) instead of being silently healed.
  const modelOptionsByInstance = useMemo(
    () => getCustomModelOptionsByInstance(settings, providers, member.instanceId, member.model),
    [settings, providers, member.instanceId, member.model],
  );
  const runtimeModeLabel =
    RUNTIME_MODE_OPTIONS.find((option) => option.value === member.runtimeMode)?.label ??
    member.runtimeMode;

  return (
    <tr>
      <td className="px-2 py-2.5 align-top">
        <DraftInput
          size="sm"
          value={member.id}
          aria-label={`成员 ${member.id} 的 id`}
          aria-invalid={duplicateId || undefined}
          onCommit={(next) => {
            const id = next.trim();
            if (id.length === 0) return;
            onPatch({ id });
          }}
        />
        {duplicateId ? (
          <p className="pt-1 text-xs text-destructive">id 与其他成员重复，修复后才会保存。</p>
        ) : null}
      </td>
      <td className="px-2 py-2.5 align-top">
        <div className="flex flex-wrap items-center gap-1.5">
          <ProviderModelPicker
            activeInstanceId={member.instanceId}
            model={member.model}
            lockedProvider={null}
            instanceEntries={entries}
            modelOptionsByInstance={modelOptionsByInstance}
            triggerVariant="outline"
            triggerClassName={SETTINGS_PICKER_TRIGGER_CLASSNAME}
            triggerAriaLabel={`成员 ${member.id} 的实例与模型`}
            {...(environmentId
              ? {
                  onOpenProviderSetup: (instanceId) => {
                    void navigate({
                      to: "/settings/providers",
                      search: { environmentId, instanceId },
                    });
                  },
                }
              : {})}
            onInstanceModelChange={(instanceId, model) => {
              // Option selections belong to the previous model's descriptors;
              // drop them so the new model starts from its own defaults.
              onPatch({ instanceId, model, options: undefined });
            }}
          />
          {instanceEntry ? (
            <TraitsPicker
              provider={instanceEntry.driverKind}
              models={instanceEntry.models}
              model={member.model}
              prompt=""
              onPromptChange={() => {}}
              modelOptions={memberOptionSelections(member.options)}
              allowPromptInjectedEffort={false}
              planModeEnabled={settings.planModeEnabled}
              triggerVariant="outline"
              triggerClassName={SETTINGS_PICKER_TRIGGER_CLASSNAME}
              onModelOptionsChange={(nextOptions) => {
                onPatch({ options: memberOptionsFromSelections(nextOptions) });
              }}
            />
          ) : null}
        </div>
        {instanceEntry ? null : (
          <p className="pt-1 text-xs text-muted-foreground">
            实例 {member.instanceId} 不存在，保留当前值。
          </p>
        )}
      </td>
      <td className="px-2 py-2.5 align-top">
        <Select
          value={member.runtimeMode}
          onValueChange={(value) => {
            if (!value) return;
            onPatch({ runtimeMode: value as RuntimeMode });
          }}
        >
          <SelectTrigger
            size="sm"
            className="w-full min-w-40"
            aria-label={`成员 ${member.id} 的权限`}
          >
            <SelectValue>{runtimeModeLabel}</SelectValue>
          </SelectTrigger>
          <SelectPopup alignItemWithTrigger={false}>
            {RUNTIME_MODE_OPTIONS.map((option) => (
              <SelectItem key={option.value} hideIndicator value={option.value}>
                {option.label}
              </SelectItem>
            ))}
          </SelectPopup>
        </Select>
      </td>
      <td className="px-2 py-2.5 align-top">
        <Checkbox
          className="mt-1"
          checked={member.enabled !== false}
          onCheckedChange={(checked) => onPatch({ enabled: checked === true })}
          aria-label={`启用成员 ${member.id}`}
        />
      </td>
      <td className="px-2 py-2.5 align-top">
        <Button
          size="icon-xs"
          variant="ghost"
          aria-label={`删除成员 ${member.id}`}
          onClick={onRemove}
        >
          <Trash2Icon className="size-3.5" />
        </Button>
      </td>
    </tr>
  );
}
