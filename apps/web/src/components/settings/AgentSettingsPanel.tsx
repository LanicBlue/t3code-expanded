/**
 * Agent settings page — the full editor for one logical agent.
 *
 * The Services settings list keeps only each agent's enable switch; every
 * other field (name, instance, model override, think level, persona, delete)
 * lives here on a dedicated page, mirroring the /projects/$projectKey detail
 * pattern: full-page layout, breadcrumb with an agent switcher, Escape goes
 * back.
 *
 * @module AgentSettingsPanel
 */
import { useAtomValue } from "@effect/atom-react";
import {
  type LogicalAgentConfig,
  ProviderInstanceId,
  type ServerSettingsPatch,
} from "@t3tools/contracts";
import { useCanGoBack, useNavigate } from "@tanstack/react-router";
import { ChevronDownIcon, Trash2Icon } from "lucide-react";
import { useCallback, useEffect, useMemo } from "react";

import { useCommitOnBlur } from "../../hooks/useCommitOnBlur";
import { usePrimarySettings, useUpdatePrimarySettings } from "../../hooks/useSettings";
import { cn } from "../../lib/utils";
import {
  applyProviderInstanceSettings,
  deriveProviderInstanceEntries,
  sortProviderInstanceEntries,
} from "../../providerInstances";
import { primaryServerProvidersAtom } from "../../state/server";
import { isElectron } from "../../env";
import { COLLAPSED_SIDEBAR_TITLEBAR_INSET_CLASS } from "../../workspaceTitlebar";
import { Button } from "../ui/button";
import { Input } from "../ui/input";
import { Textarea } from "../ui/textarea";
import { Select, SelectItem, SelectPopup, SelectTrigger, SelectValue } from "../ui/select";
import { SidebarInset } from "../ui/sidebar";
import { Switch } from "../ui/switch";
import { SettingsPageContainer, SettingsRow, SettingsSection } from "./settingsLayout";
import { nextAgentMapWithAgent, nextAgentMapWithoutAgent } from "./ServicesSettings.logic";

/**
 * Effort levels offered for an agent's thinkLevel. Free-form in the schema
 * on purpose (drivers drop values their model does not support); the UI
 * offers the levels the bundled drivers currently honor.
 */
const THINK_LEVEL_OPTIONS: ReadonlyArray<string> = ["", "low", "medium", "high", "xhigh", "max"];

export function AgentSettingsPage({ agentId }: { agentId: string }) {
  const navigate = useNavigate();
  const canGoBack = useCanGoBack();
  const navigateBackWithinApp = useCallback(() => {
    if (canGoBack) {
      window.history.back();
      return;
    }
    void navigate({ to: "/settings/services" });
  }, [canGoBack, navigate]);

  useEffect(() => {
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.defaultPrevented) return;
      if (event.key !== "Escape") return;
      event.preventDefault();
      const activeElement = document.activeElement;
      if (activeElement instanceof HTMLElement) {
        activeElement.blur();
      }
      navigateBackWithinApp();
    };
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [navigateBackWithinApp]);

  return (
    <SidebarInset className="h-dvh min-h-0 overflow-hidden overscroll-y-none bg-background text-foreground isolate">
      <div className="flex min-h-0 min-w-0 flex-1 flex-col bg-background text-foreground">
        {!isElectron && (
          <header
            className={cn(
              "flex h-[var(--workspace-topbar-height)] min-h-[var(--workspace-topbar-height)] shrink-0 items-center px-3 transition-[padding-left] duration-200 ease-linear motion-reduce:transition-none sm:px-5",
              COLLAPSED_SIDEBAR_TITLEBAR_INSET_CLASS,
            )}
          >
            <AgentSettingsBreadcrumb agentId={agentId} />
          </header>
        )}
        {isElectron && (
          <div
            className={cn(
              "drag-region flex h-[52px] shrink-0 items-center px-5 transition-[padding-left] duration-200 ease-linear motion-reduce:transition-none wco:h-[env(titlebar-area-height)] wco:pr-[calc(100vw-env(titlebar-area-width)-env(titlebar-area-x)+1em)]",
              COLLAPSED_SIDEBAR_TITLEBAR_INSET_CLASS,
            )}
          >
            <AgentSettingsBreadcrumb agentId={agentId} />
          </div>
        )}
        <AgentSettingsPanel agentId={agentId} />
      </div>
    </SidebarInset>
  );
}

function AgentSettingsBreadcrumb({ agentId }: { agentId: string }) {
  const agentMap = usePrimarySettings((settings) => settings.logicalAgents);
  const navigate = useNavigate();
  const entries = Object.entries(agentMap).sort(([, a], [, b]) =>
    a.agentName.localeCompare(b.agentName),
  );
  // Route params are plain strings; the map keys are branded ids, so look
  // the agent up by equality instead of indexing.
  const selected = entries.find(([id]) => id === agentId)?.[1] ?? null;

  return (
    <div className="flex min-w-0 items-center gap-2 text-sm">
      <button
        type="button"
        className="text-muted-foreground hover:text-foreground"
        onClick={() => void navigate({ to: "/settings/services" })}
      >
        Agents
      </button>
      <span className="text-muted-foreground">/</span>
      {selected !== null && entries.length > 1 ? (
        <Select
          value={agentId}
          onValueChange={(value) => {
            if (value !== null) {
              void navigate({
                to: "/agents/$agentId",
                params: { agentId: value },
                replace: true,
              });
            }
          }}
        >
          <SelectTrigger
            size="sm"
            aria-label="Switch agent"
            className="min-w-0 max-w-64 font-normal"
          >
            <SelectValue>
              <span className="truncate">{selected.agentName}</span>
              <ChevronDownIcon aria-hidden className="size-3.5 shrink-0 opacity-60" />
            </SelectValue>
          </SelectTrigger>
          <SelectPopup align="start">
            {entries.map(([id, agent]) => (
              <SelectItem key={id} hideIndicator value={id}>
                {agent.agentName}
              </SelectItem>
            ))}
          </SelectPopup>
        </Select>
      ) : (
        <span className="min-w-0 max-w-64 truncate">
          {selected?.agentName ?? "Unavailable agent"}
        </span>
      )}
    </div>
  );
}

export function AgentSettingsPanel({ agentId }: { agentId: string }) {
  const settings = usePrimarySettings();
  const agentMap = settings.logicalAgents;
  const updateSettings = useUpdatePrimarySettings();
  const serverProviders = useAtomValue(primaryServerProvidersAtom);

  const providerEntries = useMemo(
    () =>
      sortProviderInstanceEntries(
        applyProviderInstanceSettings(deriveProviderInstanceEntries(serverProviders), settings),
      ),
    [serverProviders, settings],
  );
  const instanceOptions = providerEntries.map((entry) => ({
    id: entry.instanceId,
    label: entry.displayName,
  }));
  const modelOptionsByInstance = new Map(
    providerEntries.map(
      (entry) =>
        [
          entry.instanceId,
          entry.models.map((model) => ({ slug: model.slug, name: model.name })),
        ] as const,
    ),
  );

  // Route params are plain strings; the map keys are branded ids, so look
  // the agent up by equality instead of indexing.
  const agent = Object.entries(agentMap).find(([id]) => id === agentId)?.[1] ?? null;

  if (agent === null) {
    return (
      <div className="flex flex-1 items-center justify-center p-8 text-sm text-muted-foreground">
        This agent is no longer available.
      </div>
    );
  }

  return (
    <AgentDetail
      agentId={agentId}
      agent={agent}
      agentMap={agentMap}
      instanceOptions={instanceOptions}
      modelOptionsByInstance={modelOptionsByInstance}
      updateSettings={updateSettings}
    />
  );
}

function AgentDetail({
  agentId,
  agent,
  agentMap,
  instanceOptions,
  modelOptionsByInstance,
  updateSettings,
}: {
  readonly agentId: string;
  readonly agent: LogicalAgentConfig;
  readonly agentMap: Readonly<Record<string, LogicalAgentConfig>>;
  readonly instanceOptions: ReadonlyArray<{
    readonly id: ProviderInstanceId;
    readonly label: string;
  }>;
  /** Catalog models per instance, for the agent's model-override picker. */
  readonly modelOptionsByInstance: ReadonlyMap<
    ProviderInstanceId,
    ReadonlyArray<{ readonly slug: string; readonly name: string }>
  >;
  readonly updateSettings: (patch: ServerSettingsPatch) => void;
}) {
  const navigate = useNavigate();

  const patchAgent = (next: LogicalAgentConfig) =>
    updateSettings({ logicalAgents: nextAgentMapWithAgent(agentMap, agentId, next) });

  const nameInput = useCommitOnBlur(agent.agentName, (agentName) => {
    if (agentName.trim().length > 0) {
      patchAgent({ ...agent, agentName });
    }
  });
  const personaInput = useCommitOnBlur<HTMLTextAreaElement>(agent.persona, (persona) => {
    patchAgent({ ...agent, persona });
  });

  const removeAgent = () => {
    updateSettings({ logicalAgents: nextAgentMapWithoutAgent(agentMap, agentId) });
    void navigate({ to: "/settings/services" });
  };

  return (
    <SettingsPageContainer>
      <SettingsSection title="Agent">
        <SettingsRow
          title="Name"
          description={<span className="font-mono text-[11px]">{agentId}</span>}
          control={<Input {...nameInput} aria-label="Agent name" className="w-full sm:w-64" />}
        />
        <SettingsRow
          title="Provider instance"
          description="Which provider instance this agent runs through. A model override is instance-scoped; moving the agent to another instance clears it rather than leaving it pointing at a model the new instance may not serve."
          control={
            <Select
              value={agent.providerInstanceId}
              onValueChange={(providerInstanceId) => {
                if (providerInstanceId !== null) {
                  patchAgent({
                    ...agent,
                    providerInstanceId: ProviderInstanceId.make(providerInstanceId),
                    modelOverride: null,
                  });
                }
              }}
            >
              <SelectTrigger size="sm" className="w-56" aria-label="Provider instance">
                <SelectValue>
                  {instanceOptions.find((instance) => instance.id === agent.providerInstanceId)
                    ?.label ?? agent.providerInstanceId}
                </SelectValue>
              </SelectTrigger>
              <SelectPopup align="end" alignItemWithTrigger={false}>
                {instanceOptions.map((instance) => (
                  <SelectItem key={instance.id} value={instance.id}>
                    {instance.label}
                  </SelectItem>
                ))}
              </SelectPopup>
            </Select>
          }
        />
        <SettingsRow
          title="Project work"
          description="Whether this agent accepts Project Service work notices routed by workspace directory."
          control={
            <label className="flex items-center gap-2 text-xs text-muted-foreground">
              <Switch
                checked={agent.project.enabled}
                onCheckedChange={(checked) =>
                  patchAgent({ ...agent, project: { enabled: Boolean(checked) } })
                }
                aria-label="Project work enabled"
              />
              Project work enabled
            </label>
          }
        />
        <SettingsRow
          title="Model"
          description="Default model for this agent's sessions on its provider instance."
          control={
            <Select
              value={
                agent.modelOverride !== null &&
                agent.modelOverride.instanceId === agent.providerInstanceId
                  ? agent.modelOverride.model
                  : ""
              }
              onValueChange={(model) => {
                patchAgent({
                  ...agent,
                  modelOverride:
                    model === null || model === ""
                      ? null
                      : {
                          instanceId: agent.providerInstanceId,
                          model,
                        },
                });
              }}
            >
              <SelectTrigger size="sm" className="w-56" aria-label="Model override">
                <SelectValue>
                  {agent.modelOverride === null
                    ? "Instance default"
                    : agent.modelOverride.instanceId === agent.providerInstanceId
                      ? agent.modelOverride.model
                      : "Instance default"}
                </SelectValue>
              </SelectTrigger>
              <SelectPopup align="end" alignItemWithTrigger={false}>
                <SelectItem value="">Instance default</SelectItem>
                {(modelOptionsByInstance.get(agent.providerInstanceId) ?? []).map((model) => (
                  <SelectItem key={model.slug} value={model.slug}>
                    {model.name}
                  </SelectItem>
                ))}
              </SelectPopup>
            </Select>
          }
        />
        <SettingsRow
          title="Think level"
          description="Effort level the agent's driver reads (drivers drop values their model does not support). Default = follow the model."
          control={
            <Select
              value={agent.thinkLevel ?? ""}
              onValueChange={(thinkLevel) => {
                patchAgent({
                  ...agent,
                  thinkLevel: thinkLevel === null || thinkLevel === "" ? null : thinkLevel,
                });
              }}
            >
              <SelectTrigger size="sm" className="w-36" aria-label="Think level">
                <SelectValue>
                  {agent.thinkLevel === null ? "Default" : agent.thinkLevel}
                </SelectValue>
              </SelectTrigger>
              <SelectPopup align="end" alignItemWithTrigger={false}>
                {THINK_LEVEL_OPTIONS.map((level) => (
                  <SelectItem key={level} value={level}>
                    {level === "" ? "Default" : level}
                  </SelectItem>
                ))}
              </SelectPopup>
            </Select>
          }
        />
      </SettingsSection>

      <SettingsSection title="Persona">
        <SettingsRow
          title="Persona"
          description="Who this agent is and how it works. Rides the system prompt for every session, never the wake message."
        >
          <Textarea
            {...personaInput}
            aria-label={`Persona for ${agent.agentName}`}
            placeholder="Persona（角色人设）：这个 agent 是谁、以什么方式工作。留空 = 无。"
            className="min-h-[96px] w-full font-mono text-[11px]"
          />
        </SettingsRow>
      </SettingsSection>

      <SettingsSection title="Danger">
        <SettingsRow
          title="Remove agent"
          description="Deletes the logical agent. Its stable ag_ id never comes back; sessions it already created are unaffected."
          control={
            <Button variant="destructive-outline" onClick={removeAgent}>
              <Trash2Icon />
              Remove agent
            </Button>
          }
        />
      </SettingsSection>
    </SettingsPageContainer>
  );
}
