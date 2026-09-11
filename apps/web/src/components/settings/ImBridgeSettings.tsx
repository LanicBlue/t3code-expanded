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
import * as Option from "effect/Option";
import { PlusIcon, ScrollTextIcon, Trash2Icon } from "lucide-react";
import { lazy, Suspense, useEffect, useMemo, useState } from "react";
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
import { useEnvironmentQuery } from "../../state/query";
import { environmentShell } from "../../state/shell";
import { primaryServerProvidersAtom } from "../../state/server";
import { usePrimaryEnvironmentId } from "../../state/environments";
import { ProviderModelPicker } from "../chat/ProviderModelPicker";
import { TraitsPicker } from "../chat/TraitsPicker";
import { Button } from "../ui/button";
import { Checkbox } from "../ui/checkbox";
import {
  Dialog,
  DialogDescription,
  DialogHeader,
  DialogPanel,
  DialogPopup,
  DialogTitle,
} from "../ui/dialog";
import { DraftInput } from "../ui/draft-input";
import { Select, SelectItem, SelectPopup, SelectTrigger, SelectValue } from "../ui/select";
import { Textarea } from "../ui/textarea";
import { toastManager } from "../ui/toast";
import {
  appendMember,
  appendMemberFromTemplate,
  type ImBridgeMember,
  type ImBridgeMembers,
  type ImBridgeMemberPatch,
  type ImBridgeThreadLike,
  liveImBridgeThreadCount,
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

// 2.6 MB of bundled template data sits behind this lazy chunk + a lazy fetch.
const ImBridgeMarketplaceDrawer = lazy(() =>
  import("./ImBridgeMarketplaceDrawer").then((module) => ({
    default: module.ImBridgeMarketplaceDrawer,
  })),
);

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

  // The roster commits as one whole array; the draft clears once the
  // persisted roster catches up with a committed write.
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
    updateSettings({ imBridge: { members: next } });
  };

  // The delete confirmation's in-flight count: the primary environment's
  // shell threads, filtered to this member's unsettled mission threads.
  const environmentId = usePrimaryEnvironmentId();
  const shellQuery = useEnvironmentQuery(
    environmentId ? environmentShell.stateAtom(environmentId) : null,
  );
  const shellSnapshot = shellQuery.data?.snapshot;
  const shellThreads: ReadonlyArray<ImBridgeThreadLike> =
    shellSnapshot !== undefined && Option.isSome(shellSnapshot) ? shellSnapshot.value.threads : [];

  const [pendingRemoval, setPendingRemoval] = useState<{
    readonly index: number;
    readonly member: ImBridgeMember;
  } | null>(null);

  const canAppend = entries.some((entry) => entry.models.length > 0);
  const [marketplaceOpen, setMarketplaceOpen] = useState(false);

  return (
    <SettingsSection {...searchableSetting("im-bridge")} title="IM 成员">
      <div
        className={cn("px-3 pb-4 pt-1 sm:px-4", !primarySettingsAvailable && "opacity-50")}
        inert={!primarySettingsAvailable}
      >
        <p className="px-1 pb-2 text-[12px] text-muted-foreground/80">
          这些成员由外部 IM 桥以 T3 线程执行，改动在桥的下一个 reconcile tick（≤60s）生效。id
          自动生成、不可改；名称随便改。桥只 join
          勾选「启用」的成员：新行（含模板市场导入）默认未启用，起好名、选好实例 /
          模型后再勾选。人设由 T3 服务端注入该成员线程的每一轮，不经桥。
        </p>
        {members.length === 0 ? (
          <p className="py-6 text-center text-[13px] text-muted-foreground/80">
            尚未声明成员——加一行，桥会在每个 IM 工作区 join 该成员。
          </p>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-left text-[13px]">
              <thead className="border-b border-border/60 text-[11px] uppercase tracking-[0.08em] text-muted-foreground/70">
                <tr>
                  <th className="px-2 py-2.5 font-semibold">成员（名称 · id）</th>
                  <th className="px-2 py-2.5 font-semibold">运行时（实例 / 模型 / 权限）</th>
                  <th className="w-px px-2 py-2.5 font-semibold">人设</th>
                  <th className="w-px px-2 py-2.5 font-semibold">启用</th>
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
                    onPatch={(patch) => commitMembers(patchMember(members, index, patch))}
                    onRemove={() => setPendingRemoval({ index, member })}
                  />
                ))}
              </tbody>
            </table>
          </div>
        )}
        <div className="flex items-center justify-between gap-4 pt-3">
          <div className="flex items-center gap-2">
            <Button
              size="xs"
              variant="outline"
              disabled={!canAppend}
              onClick={() => commitMembers(appendMember(members, entries))}
            >
              <PlusIcon className="size-3" aria-hidden />
              加成员
            </Button>
            <Button
              size="xs"
              variant="outline"
              disabled={!canAppend}
              onClick={() => setMarketplaceOpen(true)}
            >
              <ScrollTextIcon className="size-3" aria-hidden />
              从模板市场加
            </Button>
          </div>
          {!primarySettingsAvailable ? (
            <p className="text-xs text-muted-foreground">{PRIMARY_SETTINGS_UNAVAILABLE_MESSAGE}</p>
          ) : null}
        </div>
        <Suspense fallback={null}>
          <ImBridgeMarketplaceDrawer
            open={marketplaceOpen}
            onOpenChange={setMarketplaceOpen}
            onImport={(template) =>
              commitMembers(appendMemberFromTemplate(members, entries, template))
            }
          />
        </Suspense>
        {pendingRemoval !== null ? (
          <RemoveMemberDialog
            member={pendingRemoval.member}
            liveThreadCount={liveImBridgeThreadCount(shellThreads, pendingRemoval.member.id)}
            onClose={() => setPendingRemoval(null)}
            onConfirm={() => {
              const { index, member } = pendingRemoval;
              const settled = liveImBridgeThreadCount(shellThreads, member.id);
              commitMembers(removeMember(members, index));
              setPendingRemoval(null);
              toastManager.add({
                type: "success",
                title: `已删除成员 ${member.displayName?.trim() || member.id}`,
                description:
                  settled > 0
                    ? `已 settle ${settled} 个进行中的会话；桥将在下个 tick（≤60s）归档其 IM 身份并归还工位。`
                    : "桥将在下个 tick（≤60s）归档其 IM 身份并归还工位；停驻的 mission 原地不动。",
              });
            }}
          />
        ) : null}
      </div>
    </SettingsSection>
  );
}

/**
 * Delete confirmation — the one destructive action on a member row. The
 * consequences span both sides: T3 settles the member's in-flight threads
 * immediately (server-side, inside the settings save), and the bridge
 * archives the member in every IM workspace on its next tick.
 */
function RemoveMemberDialog({
  member,
  liveThreadCount,
  onClose,
  onConfirm,
}: {
  readonly member: ImBridgeMember;
  readonly liveThreadCount: number;
  readonly onClose: () => void;
  readonly onConfirm: () => void;
}) {
  const label = member.displayName?.trim() || member.id;
  return (
    <Dialog open onOpenChange={(open) => (open ? undefined : onClose())}>
      <DialogPopup className="max-w-md">
        <DialogHeader>
          <DialogTitle>删除成员 {label}？</DialogTitle>
          <DialogDescription>
            该成员将从成员表移除，桥在下个 reconcile tick（≤60s）会对每个 IM 工作区执行
            leave：身份归档、值守工位归还给你，停驻的 mission 原地不动。
            {liveThreadCount > 0
              ? ` 该成员名下 ${liveThreadCount} 个进行中的会话将立即 settle（在途轮次无法再提交）。`
              : " 该成员当前没有进行中的会话。"}
          </DialogDescription>
        </DialogHeader>
        <DialogPanel>
          <div className="flex justify-end gap-2">
            <Button size="xs" variant="ghost" onClick={onClose}>
              取消
            </Button>
            <Button size="xs" variant="destructive" onClick={onConfirm}>
              删除
            </Button>
          </div>
        </DialogPanel>
      </DialogPopup>
    </Dialog>
  );
}

function ImBridgeMemberRow({
  member,
  entries,
  settings,
  providers,
  onPatch,
  onRemove,
}: {
  readonly member: ImBridgeMember;
  readonly entries: ReadonlyArray<ProviderInstanceEntry>;
  readonly settings: UnifiedSettings;
  readonly providers: ReadonlyArray<ServerProvider>;
  readonly onPatch: (patch: ImBridgeMemberPatch) => void;
  readonly onRemove: () => void;
}) {
  const navigate = useNavigate();
  const environmentId = usePrimaryEnvironmentId();
  const [personaOpen, setPersonaOpen] = useState(false);
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
        <div className="flex w-full items-center gap-2">
          <DraftInput
            size="sm"
            className="min-w-0 flex-1"
            value={member.displayName ?? ""}
            placeholder="成员名称"
            aria-label={`成员 ${member.displayName ?? member.id} 的名称`}
            onCommit={(next) => onPatch({ displayName: next.trim() || undefined })}
          />
          {/* The identity key: generated once, immutable, select-to-copy when
              wiring im commands (`im missions <id>`, set-executor …). */}
          <span
            className="shrink-0 font-mono text-[10.5px] text-muted-foreground/70"
            title="成员 id（自动生成，不可改）"
          >
            {member.id}
          </span>
        </div>
      </td>
      <td className="px-2 py-2.5 align-top">
        <div className="flex items-center gap-1.5">
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
          <Select
            value={member.runtimeMode}
            onValueChange={(value) => {
              if (!value) return;
              onPatch({ runtimeMode: value as RuntimeMode });
            }}
          >
            <SelectTrigger
              size="sm"
              className="w-fit min-w-28 max-w-36 text-xs"
              aria-label={`成员 ${member.id} 的权限`}
            >
              <SelectValue className="truncate">{runtimeModeLabel}</SelectValue>
            </SelectTrigger>
            <SelectPopup alignItemWithTrigger={false}>
              {RUNTIME_MODE_OPTIONS.map((option) => (
                <SelectItem key={option.value} hideIndicator value={option.value}>
                  {option.label}
                </SelectItem>
              ))}
            </SelectPopup>
          </Select>
        </div>
        {instanceEntry ? null : (
          <p className="pt-1 text-xs text-muted-foreground">
            实例 {member.instanceId} 不存在，保留当前值。
          </p>
        )}
      </td>
      <td className="px-2 py-2.5 align-top">
        <Button
          size="xs"
          variant="outline"
          aria-label={`编辑成员 ${member.id} 的人设`}
          onClick={() => setPersonaOpen(true)}
        >
          <ScrollTextIcon className="size-3" aria-hidden />
          {member.persona ? "已设" : "未设"}
        </Button>
        <PersonaDialog
          member={member}
          open={personaOpen}
          onOpenChange={setPersonaOpen}
          onPatch={onPatch}
        />
      </td>
      <td className="px-2 py-2.5 align-top">
        <div className="flex items-center gap-1">
          <Checkbox
            checked={member.enabled !== false}
            onCheckedChange={(checked) => onPatch({ enabled: checked === true })}
            aria-label={`启用成员 ${member.id}`}
          />
          <Button
            size="icon-xs"
            variant="ghost"
            aria-label={`删除成员 ${member.id}`}
            onClick={onRemove}
          >
            <Trash2Icon className="size-3.5" />
          </Button>
        </div>
      </td>
    </tr>
  );
}

/**
 * Edit one member's persona — the prompt T3's server prepends to every turn
 * on that member's threads. Saving an empty box clears the field (patch
 * `undefined` drops the key); the textarea seeds from the persisted value on
 * each open so a canceled edit never half-applies.
 */
function PersonaDialog({
  member,
  open,
  onOpenChange,
  onPatch,
}: {
  readonly member: ImBridgeMember;
  readonly open: boolean;
  readonly onOpenChange: (open: boolean) => void;
  readonly onPatch: (patch: ImBridgeMemberPatch) => void;
}) {
  const [text, setText] = useState("");
  useEffect(() => {
    if (open) setText(member.persona ?? "");
  }, [open, member.persona]);

  const commit = () => {
    const trimmed = text.trim();
    onPatch({ persona: trimmed.length > 0 ? text : undefined });
    onOpenChange(false);
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogPopup className="max-w-2xl">
        <DialogHeader>
          <DialogTitle>成员 {member.displayName ?? member.id} 的人设</DialogTitle>
          <DialogDescription>
            由 T3 服务端注入该成员线程（im-{member.id}
            -ms_*）的每一轮消息开头，不经桥；模板市场导入的人设也存这里。
          </DialogDescription>
        </DialogHeader>
        <DialogPanel>
          <Textarea
            className="min-h-56 font-mono text-xs leading-relaxed"
            placeholder="你是……（留空保存即清除人设）"
            value={text}
            onChange={(event) => setText(event.target.value)}
            aria-label={`成员 ${member.id} 的人设文本`}
          />
          <div className="flex justify-end gap-2 pt-3">
            <Button size="xs" variant="ghost" onClick={() => onOpenChange(false)}>
              取消
            </Button>
            <Button size="xs" onClick={commit}>
              保存
            </Button>
          </div>
        </DialogPanel>
      </DialogPopup>
    </Dialog>
  );
}
