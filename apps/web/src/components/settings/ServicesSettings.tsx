/**
 * Services settings — external services a T3 environment integrates with.
 *
 * First surface: the Project Service client (connection + credential) and the
 * logical agents that act through provider instances. Credentials are
 * write-only here: the paste box never echoes a stored value, and everything
 * the server reports back is redacted state (keyId hint + set flag).
 *
 * @module ServicesSettings
 */
import { useAtomValue } from "@effect/atom-react";
import {
  type LogicalAgentConfig,
  type ProjectServiceConnectionTestResult,
  ProviderInstanceId,
  type ServerSettingsPatch,
} from "@t3tools/contracts";
import {
  isAtomCommandInterrupted,
  squashAtomCommandFailure,
} from "@t3tools/client-runtime/state/runtime";
import { PlugZapIcon, PlusIcon, Trash2Icon } from "lucide-react";
import { useState } from "react";

import { useCommitOnBlur } from "../../hooks/useCommitOnBlur";
import { usePrimarySettings, useUpdatePrimarySettings } from "../../hooks/useSettings";
import { cn, randomUUID } from "../../lib/utils";
import {
  applyProviderInstanceSettings,
  deriveProviderInstanceEntries,
  sortProviderInstanceEntries,
} from "../../providerInstances";
import { usePrimaryEnvironmentId } from "../../state/environments";
import { primaryServerProvidersAtom, serverEnvironment } from "../../state/server";
import { useAtomCommand } from "../../state/use-atom-command";
import { Button } from "../ui/button";
import { Input } from "../ui/input";
import { Select, SelectItem, SelectPopup, SelectTrigger, SelectValue } from "../ui/select";
import { Switch } from "../ui/switch";
import { SettingsPageContainer, SettingsRow, SettingsSection } from "./settingsLayout";
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

function ProjectServiceClientSection() {
  const client = usePrimarySettings((settings) => settings.projectServiceClient);
  const updateSettings = useUpdatePrimarySettings();
  const primaryEnvironmentId = usePrimaryEnvironmentId();
  const testConnection = useAtomCommand(serverEnvironment.testProjectServiceConnection, {
    reportFailure: false,
  });
  const [credentialDraft, setCredentialDraft] = useState("");
  const [testResult, setTestResult] = useState<ProjectServiceConnectionTestResult | null>(null);
  const [testPending, setTestPending] = useState(false);
  const [testError, setTestError] = useState<string | null>(null);

  const baseUrlInput = useCommitOnBlur(client.baseUrl, (baseUrl) =>
    updateSettings({ projectServiceClient: { baseUrl } }),
  );
  const credentialInvalid = !isValidCredentialPaste(credentialDraft);
  const credentialDirty = credentialDraft.length > 0 && !credentialInvalid;

  const runTest = async () => {
    if (primaryEnvironmentId === null) return;
    setTestPending(true);
    setTestError(null);
    const result = await testConnection({ environmentId: primaryEnvironmentId, input: {} });
    setTestPending(false);
    if (result._tag === "Success") {
      setTestResult(result.value);
      return;
    }
    if (!isAtomCommandInterrupted(result)) {
      const error = squashAtomCommandFailure(result);
      setTestError(error instanceof Error ? error.message : "Connection test failed.");
    }
  };

  return (
    <SettingsSection title="Project Service">
      <SettingsRow
        title="Enabled"
        description="Master switch for the Project Service integration. Nothing consumes it until the agent runtime lands; the connection and credential stay stored either way."
        control={
          <Switch
            checked={client.enabled}
            onCheckedChange={(checked) =>
              updateSettings({ projectServiceClient: { enabled: Boolean(checked) } })
            }
            aria-label="Project Service enabled"
          />
        }
      />
      <SettingsRow
        title="Base URL"
        description="Where this environment's Project Service listens."
        control={
          <Input
            {...baseUrlInput}
            className="w-full sm:w-72"
            aria-label="Project Service base URL"
          />
        }
      />
      <SettingsRow
        title="Client credential"
        description="Paste the client credential created at the service (psk_<keyId>.<secret>). Stored only in the server's secret store; never displayed again."
        status={credentialStatusLabel(client)}
        control={
          <div className="flex w-full flex-wrap items-center gap-2 sm:w-auto sm:justify-end">
            <Input
              type="password"
              value={credentialDraft}
              onChange={(event) => setCredentialDraft(event.target.value)}
              placeholder="psk_…"
              aria-invalid={credentialInvalid}
              className={cn("w-full sm:w-64", credentialInvalid && "border-destructive/60")}
              aria-label="New Project Service credential"
            />
            {client.credentialSet ? (
              <Button
                variant="ghost-muted"
                size="sm"
                disabled={credentialDirty}
                onClick={() => {
                  setCredentialDraft("");
                  updateSettings({ projectServiceClient: { clearCredential: true } });
                }}
              >
                Remove
              </Button>
            ) : null}
            <Button
              size="sm"
              disabled={!credentialDirty}
              onClick={() => {
                updateSettings({ projectServiceClient: { newCredential: credentialDraft } });
                setCredentialDraft("");
              }}
            >
              {client.credentialSet ? "Replace" : "Save"}
            </Button>
          </div>
        }
      >
        {credentialInvalid ? (
          <p className="px-3 pb-2 text-xs text-destructive sm:px-4">
            Credentials look like psk_&lt;keyId&gt;.&lt;secret&gt;.
          </p>
        ) : null}
      </SettingsRow>
      <SettingsRow
        title="Connection"
        description="Authenticates against the configured service with the stored credential. A rejected credential only fails this test — sessions and threads are untouched."
        control={
          <Button size="sm" variant="outline" disabled={testPending} onClick={() => void runTest()}>
            <PlugZapIcon className="size-3.5" />
            {testPending ? "Testing…" : "Test connection"}
          </Button>
        }
      >
        {testError !== null ? (
          <p className="px-3 pb-2 text-xs text-muted-foreground sm:px-4">{testError}</p>
        ) : testResult !== null ? (
          <p className="px-3 pb-2 text-xs text-muted-foreground sm:px-4">
            <span className="font-medium">
              {
                CONNECTION_TEST_STATUS_LABELS[
                  connectionTestStatus({ result: testResult, credentialSet: client.credentialSet })
                ]
              }
            </span>
            {" — "}
            {testResult.detail}
          </p>
        ) : null}
      </SettingsRow>
    </SettingsSection>
  );
}

function AgentRow({
  agentId,
  agent,
  agentMap,
  instanceOptions,
  updateSettings,
}: {
  readonly agentId: string;
  readonly agent: LogicalAgentConfig;
  readonly agentMap: Readonly<Record<string, LogicalAgentConfig>>;
  readonly instanceOptions: ReadonlyArray<{
    readonly id: ProviderInstanceId;
    readonly label: string;
  }>;
  readonly updateSettings: (patch: ServerSettingsPatch) => void;
}) {
  const nameInput = useCommitOnBlur(agent.agentName, (agentName) => {
    if (agentName.trim().length > 0) {
      updateSettings({
        logicalAgents: nextAgentMapWithAgent(agentMap, agentId, { ...agent, agentName }),
      });
    }
  });
  const patchAgent = (next: LogicalAgentConfig) =>
    updateSettings({ logicalAgents: nextAgentMapWithAgent(agentMap, agentId, next) });

  return (
    <SettingsRow
      title={agent.agentName}
      description={<span className="font-mono text-[11px]">{agentId}</span>}
      control={
        <Button
          variant="ghost-muted"
          size="icon-sm"
          aria-label={`Remove agent ${agent.agentName}`}
          onClick={() =>
            updateSettings({ logicalAgents: nextAgentMapWithoutAgent(agentMap, agentId) })
          }
        >
          <Trash2Icon className="size-3.5" />
        </Button>
      }
    >
      <div className="mb-3 space-y-2 rounded-lg border border-border/60 bg-muted/20 p-3">
        <div className="flex flex-wrap items-center gap-2">
          <Input {...nameInput} aria-label="Agent name" className="w-48" />
          <Select
            value={agent.providerInstanceId}
            onValueChange={(providerInstanceId) => {
              if (providerInstanceId !== null) {
                patchAgent({
                  ...agent,
                  providerInstanceId: ProviderInstanceId.make(providerInstanceId),
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
            <SelectPopup align="start">
              {instanceOptions.map((instance) => (
                <SelectItem key={instance.id} value={instance.id}>
                  {instance.label}
                </SelectItem>
              ))}
            </SelectPopup>
          </Select>
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
        </div>
      </div>
    </SettingsRow>
  );
}

function LogicalAgentsSection() {
  const settings = usePrimarySettings();
  const agentMap = settings.logicalAgents;
  const updateSettings = useUpdatePrimarySettings();
  const serverProviders = useAtomValue(primaryServerProvidersAtom);

  const instanceOptions = sortProviderInstanceEntries(
    applyProviderInstanceSettings(deriveProviderInstanceEntries(serverProviders), settings),
  ).map((entry) => ({ id: entry.instanceId, label: entry.displayName }));
  const defaultInstance = instanceOptions[0]?.id ?? null;

  return (
    <SettingsSection title="Logical Agents">
      <SettingsRow
        title="Agents"
        description="Each agent is a stable identity (the ag_ id never changes) routed through one provider instance. Work notices route by workspace directory — no per-project configuration is needed here."
      >
        {Object.keys(agentMap).length === 0 ? (
          <p className="px-3 pb-3 text-xs text-muted-foreground sm:px-4">No agents yet.</p>
        ) : (
          Object.entries(agentMap).map(([agentId, agent]) => (
            <AgentRow
              key={agentId}
              agentId={agentId}
              agent={agent}
              agentMap={agentMap}
              instanceOptions={instanceOptions}
              updateSettings={updateSettings}
            />
          ))
        )}
        <div className="px-3 pb-3 sm:px-4">
          <Button
            variant="outline"
            size="sm"
            disabled={defaultInstance === null}
            onClick={() => {
              if (defaultInstance === null) return;
              updateSettings({
                logicalAgents: nextAgentMapWithAgent(
                  agentMap,
                  makeLogicalAgentId(randomUUID()),
                  makeEmptyLogicalAgentConfig(defaultInstance),
                ),
              });
            }}
          >
            <PlusIcon className="size-3.5" />
            Add agent
          </Button>
        </div>
      </SettingsRow>
    </SettingsSection>
  );
}

export function ServicesSettingsPanel() {
  return (
    <SettingsPageContainer>
      <ProjectServiceClientSection />
      <LogicalAgentsSection />
    </SettingsPageContainer>
  );
}
