import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  DEFAULT_SERVER_SETTINGS,
  LogicalAgentId,
  ProviderDriverKind,
  ProviderInstanceId,
  ServerSettings,
  ServerSettingsPatch,
} from "@t3tools/contracts";
import { createModelSelection } from "@t3tools/shared/model";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Duration from "effect/Duration";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as PlatformError from "effect/PlatformError";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as ServerSecretStore from "./auth/ServerSecretStore.ts";
import * as ServerConfig from "./config.ts";
import * as ServerSettingsModule from "./serverSettings.ts";

const decodeSettingsPatch = Schema.decodeUnknownEffect(ServerSettingsPatch);
const decodeServerSettings = Schema.decodeUnknownEffect(ServerSettings);

const makeServerSettingsLayer = () =>
  ServerSettingsModule.layer.pipe(
    Layer.provide(ServerSecretStore.layer),
    Layer.provideMerge(
      Layer.fresh(
        ServerConfig.layerTest(process.cwd(), {
          prefix: "t3code-server-settings-test-",
        }),
      ),
    ),
  );

const makeFailingSecretStoreLayer = (cause: ServerSecretStore.SecretStoreError) =>
  Layer.succeed(
    ServerSecretStore.ServerSecretStore,
    ServerSecretStore.ServerSecretStore.of({
      get: () => Effect.fail(cause),
      set: () => Effect.void,
      create: () => Effect.void,
      getOrCreateRandom: () => Effect.succeed(new Uint8Array()),
      remove: () => Effect.void,
    }),
  );

it.layer(NodeServices.layer)("server settings", (it) => {
  it.effect("preserves context when reading a provider environment secret fails", () => {
    const platformCause = PlatformError.systemError({
      _tag: "PermissionDenied",
      module: "FileSystem",
      method: "readFile",
      pathOrDescriptor: "provider environment secret",
      description: "Secret backend unavailable.",
    });
    const cause = new ServerSecretStore.SecretStoreReadError({
      resource: "provider environment secret",
      cause: platformCause,
    });
    const configLayer = Layer.fresh(
      ServerConfig.layerTest(process.cwd(), {
        prefix: "t3code-server-settings-secret-failure-test-",
      }),
    );
    const settingsLayer = ServerSettingsModule.layer.pipe(
      Layer.provide(makeFailingSecretStoreLayer(cause)),
      Layer.provideMerge(configLayer),
    );

    return Effect.gen(function* () {
      const serverConfig = yield* ServerConfig.ServerConfig;
      const fileSystem = yield* FileSystem.FileSystem;
      const serverSettings = yield* ServerSettingsModule.ServerSettingsService;
      yield* fileSystem.writeFileString(
        serverConfig.settingsPath,
        '{"providerInstances":{"codex_personal":{"driver":"codex","environment":[{"name":"OPENROUTER_API_KEY","value":"","sensitive":true,"valueRedacted":true}],"config":{}}}}',
      );

      const error = yield* Effect.flip(serverSettings.getSettings);

      assert.deepInclude(error, {
        _tag: "ServerSettingsError",
        operation: "read-secret",
        providerInstanceId: "codex_personal",
        environmentVariable: "OPENROUTER_API_KEY",
      });
      assert.strictEqual(error.cause, cause);
      assert.notInclude(error.message, cause.message);
    }).pipe(Effect.provide(settingsLayer));
  });

  it.effect("decodes nested settings patches", () =>
    Effect.gen(function* () {
      assert.deepEqual(
        yield* decodeSettingsPatch({ providers: { codex: { binaryPath: "/tmp/codex" } } }),
        {
          providers: { codex: { binaryPath: "/tmp/codex" } },
        },
      );

      assert.deepEqual(
        yield* decodeSettingsPatch({
          textGenerationModelSelection: {
            options: [{ id: "fastMode", value: false }],
          },
        }),
        {
          textGenerationModelSelection: {
            options: [{ id: "fastMode", value: false }],
          },
        },
      );
    }),
  );

  it.effect(
    "decodes legacy object-shaped textGenerationModelSelection.options from settings.json",
    () =>
      Effect.gen(function* () {
        const decoded = yield* decodeServerSettings({
          textGenerationModelSelection: {
            provider: ProviderDriverKind.make("codex"),
            model: "gpt-5.4-mini",
            options: { reasoningEffort: "low" },
          },
        });

        assert.deepEqual(decoded.textGenerationModelSelection, {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5.4-mini",
          options: [{ id: "reasoningEffort", value: "low" }],
        });
      }),
  );

  it.effect("deep merges nested settings updates without dropping siblings", () =>
    Effect.gen(function* () {
      const serverSettings = yield* ServerSettingsModule.ServerSettingsService;

      yield* serverSettings.updateSettings({
        providers: {
          codex: {
            binaryPath: "/usr/local/bin/codex",
            homePath: "/Users/julius/.codex",
          },
          claudeAgent: {
            binaryPath: "/usr/local/bin/claude",
            customModels: ["claude-custom"],
          },
        },
        textGenerationModelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: DEFAULT_SERVER_SETTINGS.textGenerationModelSelection.model,
          options: createModelSelection(
            ProviderInstanceId.make("codex"),
            DEFAULT_SERVER_SETTINGS.textGenerationModelSelection.model,
            [
              { id: "reasoningEffort", value: "high" },
              { id: "fastMode", value: true },
            ],
          ).options!,
        },
      });

      const next = yield* serverSettings.updateSettings({
        providers: {
          codex: {
            binaryPath: "/opt/homebrew/bin/codex",
          },
        },
        textGenerationModelSelection: {
          options: [{ id: "fastMode", value: false }],
        },
      });

      assert.deepEqual(next.providers.codex, {
        enabled: true,
        binaryPath: "/opt/homebrew/bin/codex",
        homePath: "/Users/julius/.codex",
        shadowHomePath: "",
        launchArgs: "",
        customModels: [],
      });
      assert.deepEqual(next.providers.claudeAgent, {
        enabled: true,
        binaryPath: "/usr/local/bin/claude",
        homePath: "",
        customModels: ["claude-custom"],
        launchArgs: "",
        autoCompactWindow: null,
      });
      assert.deepEqual(
        next.textGenerationModelSelection,
        createModelSelection(
          ProviderInstanceId.make("codex"),
          DEFAULT_SERVER_SETTINGS.textGenerationModelSelection.model,
          [
            { id: "reasoningEffort", value: "high" },
            { id: "fastMode", value: false },
          ],
        ),
      );
    }).pipe(Effect.provide(makeServerSettingsLayer())),
  );

  it.effect("buffers changes after a subscription is acquired but before it is consumed", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const serverSettings = yield* ServerSettingsModule.ServerSettingsService;
        const changes = yield* serverSettings.subscribeChanges;

        yield* serverSettings.updateSettings({
          providers: {
            codex: {
              binaryPath: "/usr/local/bin/codex-next",
            },
          },
        });

        const firstChange = yield* changes.pipe(Stream.runHead, Effect.timeout("1 second"));
        assert.equal(
          Option.getOrUndefined(firstChange)?.providers.codex.binaryPath,
          "/usr/local/bin/codex-next",
        );
      }),
    ).pipe(Effect.provide(makeServerSettingsLayer())),
  );

  it.effect("preserves model when switching providers via textGenerationModelSelection", () =>
    Effect.gen(function* () {
      const serverSettings = yield* ServerSettingsModule.ServerSettingsService;

      // Start with Claude text generation selection
      yield* serverSettings.updateSettings({
        textGenerationModelSelection: {
          instanceId: ProviderInstanceId.make("claudeAgent"),
          model: "claude-sonnet-4-6",
          options: createModelSelection(
            ProviderInstanceId.make("claudeAgent"),
            "claude-sonnet-4-6",
            [{ id: "effort", value: "high" }],
          ).options!,
        },
      });

      // Switch to Codex — the stale Claude "effort" in options must not
      // cause the update to lose the selected model.
      const next = yield* serverSettings.updateSettings({
        textGenerationModelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5.4",
          options: createModelSelection(ProviderInstanceId.make("codex"), "gpt-5.4", [
            { id: "reasoningEffort", value: "high" },
          ]).options!,
        },
      });

      assert.deepEqual(
        next.textGenerationModelSelection,
        createModelSelection(ProviderInstanceId.make("codex"), "gpt-5.4", [
          { id: "reasoningEffort", value: "high" },
        ]),
      );
    }).pipe(Effect.provide(makeServerSettingsLayer())),
  );

  it.effect("preserves custom provider instance text generation selections", () =>
    Effect.gen(function* () {
      const serverSettings = yield* ServerSettingsModule.ServerSettingsService;

      const next = yield* serverSettings.updateSettings({
        providerInstances: {
          [ProviderInstanceId.make("claude_openrouter")]: {
            driver: ProviderDriverKind.make("claudeAgent"),
            enabled: true,
            config: { customModels: ["openai/gpt-5.5"] },
          },
        },
        textGenerationModelSelection: {
          instanceId: ProviderInstanceId.make("claude_openrouter"),
          model: "openai/gpt-5.5",
        },
      });

      assert.deepEqual(next.textGenerationModelSelection, {
        instanceId: ProviderInstanceId.make("claude_openrouter"),
        model: "openai/gpt-5.5",
      });
    }).pipe(Effect.provide(makeServerSettingsLayer())),
  );

  it.effect(
    "uses explicit provider instance enabled state over legacy provider enabled state",
    () =>
      Effect.gen(function* () {
        const serverSettings = yield* ServerSettingsModule.ServerSettingsService;
        const instanceId = ProviderInstanceId.make("claude_openrouter");

        const next = yield* serverSettings.updateSettings({
          providers: {
            claudeAgent: {
              enabled: false,
            },
          },
          providerInstances: {
            [instanceId]: {
              driver: ProviderDriverKind.make("claudeAgent"),
              enabled: true,
              config: { customModels: ["openai/gpt-5.5"] },
            },
          },
          textGenerationModelSelection: {
            instanceId,
            model: "openai/gpt-5.5",
          },
        });

        assert.deepEqual(next.textGenerationModelSelection, {
          instanceId,
          model: "openai/gpt-5.5",
        });
      }).pipe(Effect.provide(makeServerSettingsLayer())),
  );

  it.effect("preserves enabled text generation selections for non-built-in drivers", () =>
    Effect.gen(function* () {
      const serverSettings = yield* ServerSettingsModule.ServerSettingsService;
      const instanceId = ProviderInstanceId.make("openrouter_text");

      const next = yield* serverSettings.updateSettings({
        providerInstances: {
          [instanceId]: {
            driver: ProviderDriverKind.make("openrouter"),
            enabled: true,
            config: { customModels: ["openai/gpt-5.5"] },
          },
        },
        textGenerationModelSelection: {
          instanceId,
          model: "openai/gpt-5.5",
        },
      });

      assert.deepEqual(next.textGenerationModelSelection, {
        instanceId,
        model: "openai/gpt-5.5",
      });
    }).pipe(Effect.provide(makeServerSettingsLayer())),
  );

  it.effect(
    "preserves the source control writer selection when its provider instance is disabled",
    () =>
      Effect.gen(function* () {
        const serverSettings = yield* ServerSettingsModule.ServerSettingsService;
        const serverConfig = yield* ServerConfig.ServerConfig;
        const fileSystem = yield* FileSystem.FileSystem;
        const instanceId = ProviderInstanceId.make("codex_writer");
        const sourceControlWriterModelSelection = {
          instanceId,
          model: "gpt-5.4-mini",
        };

        yield* serverSettings.updateSettings({
          providerInstances: {
            [instanceId]: {
              driver: ProviderDriverKind.make("codex"),
              enabled: true,
              config: {},
            },
          },
          sourceControlWriterModelSelection,
        });

        const next = yield* serverSettings.updateSettings({
          providerInstances: {
            [instanceId]: {
              driver: ProviderDriverKind.make("codex"),
              enabled: false,
              config: {},
            },
          },
        });

        assert.deepEqual(next.sourceControlWriterModelSelection, sourceControlWriterModelSelection);
        assert.deepEqual(
          ServerSettingsModule.resolveSourceControlWriterModelSelection(next),
          next.textGenerationModelSelection,
        );
        assert.deepEqual(
          (yield* serverSettings.getSettings).sourceControlWriterModelSelection,
          sourceControlWriterModelSelection,
        );

        const raw = yield* fileSystem.readFileString(serverConfig.settingsPath);
        assert.deepEqual(
          // @effect-diagnostics-next-line preferSchemaOverJson:off
          JSON.parse(raw).sourceControlWriterModelSelection,
          sourceControlWriterModelSelection,
        );

        const restored = yield* serverSettings.updateSettings({
          providerInstances: {
            [instanceId]: {
              driver: ProviderDriverKind.make("codex"),
              enabled: true,
              config: {},
            },
          },
        });
        assert.deepEqual(
          ServerSettingsModule.resolveSourceControlWriterModelSelection(restored),
          sourceControlWriterModelSelection,
        );
      }).pipe(Effect.provide(makeServerSettingsLayer())),
  );

  it.effect("drops stale text generation options when resetting model selection", () =>
    Effect.gen(function* () {
      const serverSettings = yield* ServerSettingsModule.ServerSettingsService;

      yield* serverSettings.updateSettings({
        textGenerationModelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: DEFAULT_SERVER_SETTINGS.textGenerationModelSelection.model,
          options: createModelSelection(
            ProviderInstanceId.make("codex"),
            DEFAULT_SERVER_SETTINGS.textGenerationModelSelection.model,
            [
              { id: "reasoningEffort", value: "high" },
              { id: "fastMode", value: true },
            ],
          ).options!,
        },
      });

      const next = yield* serverSettings.updateSettings({
        textGenerationModelSelection: {
          instanceId: DEFAULT_SERVER_SETTINGS.textGenerationModelSelection.instanceId,
          model: DEFAULT_SERVER_SETTINGS.textGenerationModelSelection.model,
        },
      });

      assert.deepEqual(next.textGenerationModelSelection, {
        instanceId: DEFAULT_SERVER_SETTINGS.textGenerationModelSelection.instanceId,
        model: DEFAULT_SERVER_SETTINGS.textGenerationModelSelection.model,
      });
    }).pipe(Effect.provide(makeServerSettingsLayer())),
  );

  it.effect("replaces provider instance maps when clearing optional fields", () =>
    Effect.gen(function* () {
      const serverSettings = yield* ServerSettingsModule.ServerSettingsService;
      const codexId = ProviderInstanceId.make("codex");

      yield* serverSettings.updateSettings({
        providerInstances: {
          [codexId]: {
            driver: ProviderDriverKind.make("codex"),
            displayName: "Codex Work",
            accentColor: "#7c3aed",
            enabled: true,
            config: { homePath: "~/.codex" },
          },
        },
      });

      const next = yield* serverSettings.updateSettings({
        providerInstances: {
          [codexId]: {
            driver: ProviderDriverKind.make("codex"),
            displayName: "Codex Work",
            enabled: true,
            config: { homePath: "~/.codex" },
          },
        },
      });

      assert.deepEqual(next.providerInstances[codexId], {
        driver: ProviderDriverKind.make("codex"),
        displayName: "Codex Work",
        enabled: true,
        config: { homePath: "~/.codex" },
      });
    }).pipe(Effect.provide(makeServerSettingsLayer())),
  );

  it.effect("trims provider path settings when updates are applied", () =>
    Effect.gen(function* () {
      const serverSettings = yield* ServerSettingsModule.ServerSettingsService;

      const next = yield* serverSettings.updateSettings({
        providers: {
          codex: {
            binaryPath: "  /opt/homebrew/bin/codex  ",
            homePath: "   ",
          },
          claudeAgent: {
            binaryPath: "  /opt/homebrew/bin/claude  ",
          },
          opencode: {
            binaryPath: "  /opt/homebrew/bin/opencode  ",
            serverUrl: "  http://127.0.0.1:4096  ",
            serverPassword: "  secret-password  ",
          },
        },
      });

      assert.deepEqual(next.providers.codex, {
        enabled: true,
        binaryPath: "/opt/homebrew/bin/codex",
        homePath: "",
        shadowHomePath: "",
        launchArgs: "",
        customModels: [],
      });
      assert.deepEqual(next.providers.claudeAgent, {
        enabled: true,
        binaryPath: "/opt/homebrew/bin/claude",
        homePath: "",
        customModels: [],
        launchArgs: "",
        autoCompactWindow: null,
      });
      assert.deepEqual(next.providers.opencode, {
        enabled: true,
        binaryPath: "/opt/homebrew/bin/opencode",
        serverUrl: "http://127.0.0.1:4096",
        serverPassword: "secret-password",
        customModels: [],
      });
    }).pipe(Effect.provide(makeServerSettingsLayer())),
  );

  it.effect("trims observability settings when updates are applied", () =>
    Effect.gen(function* () {
      const serverSettings = yield* ServerSettingsModule.ServerSettingsService;

      const next = yield* serverSettings.updateSettings({
        addProjectBaseDirectory: "  ~/Development  ",
        observability: {
          otlpTracesUrl: "  http://localhost:4318/v1/traces  ",
          otlpMetricsUrl: "  http://localhost:4318/v1/metrics  ",
        },
      });

      assert.equal(next.addProjectBaseDirectory, "~/Development");
      assert.deepEqual(next.observability, {
        otlpTracesUrl: "http://localhost:4318/v1/traces",
        otlpMetricsUrl: "http://localhost:4318/v1/metrics",
      });
    }).pipe(Effect.provide(makeServerSettingsLayer())),
  );

  it.effect("defaults blank binary paths to provider executables", () =>
    Effect.gen(function* () {
      const serverSettings = yield* ServerSettingsModule.ServerSettingsService;

      const next = yield* serverSettings.updateSettings({
        providers: {
          codex: {
            binaryPath: "   ",
          },
          claudeAgent: {
            binaryPath: "",
          },
        },
      });

      assert.equal(next.providers.codex.binaryPath, "codex");
      assert.equal(next.providers.claudeAgent.binaryPath, "claude");
    }).pipe(Effect.provide(makeServerSettingsLayer())),
  );

  it.effect("writes only non-default server settings to disk", () =>
    Effect.gen(function* () {
      const serverSettings = yield* ServerSettingsModule.ServerSettingsService;
      const serverConfig = yield* ServerConfig.ServerConfig;
      const fileSystem = yield* FileSystem.FileSystem;
      const next = yield* serverSettings.updateSettings({
        addProjectBaseDirectory: "~/Development",
        observability: {
          otlpTracesUrl: "http://localhost:4318/v1/traces",
          otlpMetricsUrl: "http://localhost:4318/v1/metrics",
        },
        providers: {
          codex: {
            binaryPath: "/opt/homebrew/bin/codex",
          },
          opencode: {
            serverUrl: "http://127.0.0.1:4096",
            serverPassword: "secret-password",
          },
        },
        automaticGitFetchInterval: Duration.seconds(10),
      });

      assert.equal(next.providers.codex.binaryPath, "/opt/homebrew/bin/codex");

      const raw = yield* fileSystem.readFileString(serverConfig.settingsPath);
      // @effect-diagnostics-next-line preferSchemaOverJson:off
      assert.deepEqual(JSON.parse(raw), {
        addProjectBaseDirectory: "~/Development",
        observability: {
          otlpTracesUrl: "http://localhost:4318/v1/traces",
          otlpMetricsUrl: "http://localhost:4318/v1/metrics",
        },
        providers: {
          codex: {
            binaryPath: "/opt/homebrew/bin/codex",
          },
          opencode: {
            serverUrl: "http://127.0.0.1:4096",
            serverPassword: "secret-password",
          },
        },
        backgroundActivity: {
          schemaVersion: 1,
          profile: "custom",
          baseProfile: "balanced",
          overrides: {
            automaticGitFetchInterval: 10_000,
          },
        },
        automaticGitFetchInterval: 10_000,
      });
    }).pipe(Effect.provide(makeServerSettingsLayer())),
  );

  it.effect("stores sensitive provider instance environment values outside settings.json", () =>
    Effect.gen(function* () {
      const serverSettings = yield* ServerSettingsModule.ServerSettingsService;
      const serverConfig = yield* ServerConfig.ServerConfig;
      const fileSystem = yield* FileSystem.FileSystem;
      const instanceId = ProviderInstanceId.make("codex_personal");

      const next = yield* serverSettings.updateSettings({
        providerInstances: {
          [instanceId]: {
            driver: ProviderDriverKind.make("codex"),
            environment: [
              { name: "OPENROUTER_API_KEY", value: "sk-or-secret", sensitive: true },
              { name: "ANTHROPIC_BASE_URL", value: "https://openrouter.ai/api", sensitive: false },
            ],
            config: {},
          },
        },
      });

      assert.deepEqual(next.providerInstances[instanceId]?.environment, [
        {
          name: "OPENROUTER_API_KEY",
          value: "sk-or-secret",
          sensitive: true,
          valueRedacted: true,
        },
        { name: "ANTHROPIC_BASE_URL", value: "https://openrouter.ai/api", sensitive: false },
      ]);

      const raw = yield* fileSystem.readFileString(serverConfig.settingsPath);
      assert.notInclude(raw, "sk-or-secret");
      // @effect-diagnostics-next-line preferSchemaOverJson:off
      assert.deepEqual(JSON.parse(raw).providerInstances.codex_personal.environment, [
        {
          name: "OPENROUTER_API_KEY",
          value: "",
          sensitive: true,
          valueRedacted: true,
        },
        { name: "ANTHROPIC_BASE_URL", value: "https://openrouter.ai/api", sensitive: false },
      ]);

      const roundTripped = yield* serverSettings.updateSettings({
        providerInstances: {
          [instanceId]: {
            driver: ProviderDriverKind.make("codex"),
            displayName: "Codex Personal",
            environment: [
              { name: "OPENROUTER_API_KEY", value: "", sensitive: true, valueRedacted: true },
              { name: "ANTHROPIC_BASE_URL", value: "https://openrouter.ai/api", sensitive: false },
            ],
            config: {},
          },
        },
      });

      assert.equal(
        roundTripped.providerInstances[instanceId]?.environment?.[0]?.value,
        "sk-or-secret",
      );
    }).pipe(Effect.provide(makeServerSettingsLayer())),
  );

  it.effect("stores a Project Service credential in the secret store and redacts settings", () =>
    Effect.gen(function* () {
      const serverConfig = yield* ServerConfig.ServerConfig;
      const fileSystem = yield* FileSystem.FileSystem;
      const serverSettings = yield* ServerSettingsModule.ServerSettingsService;

      const next = yield* serverSettings.updateSettings({
        projectServiceClient: { enabled: true, newCredential: "psk_key-1.s3cret" },
      });

      assert.deepEqual(next.projectServiceClient, {
        enabled: true,
        baseUrl: DEFAULT_SERVER_SETTINGS.projectServiceClient.baseUrl,
        keyIdHint: "key-1",
        credentialSet: true,
      });
      // @effect-diagnostics-next-line preferSchemaOverJson:off
      assert.notInclude(JSON.stringify(next), "s3cret");
      // @effect-diagnostics-next-line preferSchemaOverJson:off
      assert.notInclude(JSON.stringify(next), "newCredential");

      const raw = yield* fileSystem.readFileString(serverConfig.settingsPath);
      assert.notInclude(raw, "psk_");
      assert.notInclude(raw, "s3cret");

      const secretBytes = yield* fileSystem.readFile(
        `${serverConfig.secretsDir}/project-service-client-credential.bin`,
      );
      assert.equal(new TextDecoder().decode(secretBytes), "psk_key-1.s3cret");
    }).pipe(Effect.provide(makeServerSettingsLayer())),
  );

  it.effect("replaces the Project Service credential only when a new one is supplied", () =>
    Effect.gen(function* () {
      const serverConfig = yield* ServerConfig.ServerConfig;
      const fileSystem = yield* FileSystem.FileSystem;
      const serverSettings = yield* ServerSettingsModule.ServerSettingsService;
      const secretPath = `${serverConfig.secretsDir}/project-service-client-credential.bin`;

      yield* serverSettings.updateSettings({
        projectServiceClient: { newCredential: "psk_key-1.s3cret" },
      });

      // A credential-less write keeps the stored one.
      const kept = yield* serverSettings.updateSettings({
        projectServiceClient: { baseUrl: "http://127.0.0.1:7601" },
      });
      assert.equal(kept.projectServiceClient.keyIdHint, "key-1");
      assert.equal(kept.projectServiceClient.credentialSet, true);
      assert.equal(
        new TextDecoder().decode(yield* fileSystem.readFile(secretPath)),
        "psk_key-1.s3cret",
      );

      // A new credential replaces it wholesale.
      const replaced = yield* serverSettings.updateSettings({
        projectServiceClient: { newCredential: "psk_key-2.next-secret" },
      });
      assert.equal(replaced.projectServiceClient.keyIdHint, "key-2");
      const storedSecret = new TextDecoder().decode(yield* fileSystem.readFile(secretPath));
      assert.equal(storedSecret, "psk_key-2.next-secret");
      assert.notInclude(storedSecret, "s3cret");

      // clearCredential removes it.
      const cleared = yield* serverSettings.updateSettings({
        projectServiceClient: { clearCredential: true },
      });
      assert.equal(cleared.projectServiceClient.keyIdHint, "");
      assert.equal(cleared.projectServiceClient.credentialSet, false);
      const secretAfterClear = yield* fileSystem.exists(secretPath);
      assert.equal(secretAfterClear, false);
    }).pipe(Effect.provide(makeServerSettingsLayer())),
  );

  it.effect("rejects a malformed Project Service credential without persisting anything", () =>
    Effect.gen(function* () {
      const serverConfig = yield* ServerConfig.ServerConfig;
      const fileSystem = yield* FileSystem.FileSystem;
      const serverSettings = yield* ServerSettingsModule.ServerSettingsService;

      const error = yield* Effect.flip(
        serverSettings.updateSettings({
          projectServiceClient: { enabled: true, newCredential: "not-a-psk-credential" },
        }),
      );

      assert.deepInclude(error, {
        _tag: "ServerSettingsError",
        operation: "validate",
      });
      const settings = yield* serverSettings.getSettings;
      assert.equal(settings.projectServiceClient.enabled, false);
      assert.equal(
        yield* fileSystem.exists(
          `${serverConfig.secretsDir}/project-service-client-credential.bin`,
        ),
        false,
      );
    }).pipe(Effect.provide(makeServerSettingsLayer())),
  );

  it.effect("rejects logical agents referencing an unconfigured provider instance", () =>
    Effect.gen(function* () {
      const serverSettings = yield* ServerSettingsModule.ServerSettingsService;

      const error = yield* Effect.flip(
        serverSettings.updateSettings({
          logicalAgents: {
            [LogicalAgentId.make("ag_one")]: {
              agentName: "Ghost rider",
              providerInstanceId: ProviderInstanceId.make("ghost_instance"),
              persona: "",
              thinkLevel: null,
              modelOverride: null,
              project: { enabled: false },
            },
          },
        }),
      );
      assert.deepInclude(error, {
        _tag: "ServerSettingsError",
        operation: "validate",
        providerInstanceId: "ghost_instance",
      });

      // Driver defaults resolve without an explicit providerInstances entry.
      const next = yield* serverSettings.updateSettings({
        logicalAgents: {
          [LogicalAgentId.make("ag_one")]: {
            agentName: "Build agent",
            providerInstanceId: ProviderInstanceId.make("codex"),
            persona: "",
            thinkLevel: null,
            modelOverride: null,
            project: { enabled: false },
          },
        },
      });
      assert.equal(next.logicalAgents[LogicalAgentId.make("ag_one")]?.providerInstanceId, "codex");
    }).pipe(Effect.provide(makeServerSettingsLayer())),
  );

  it.effect("several Project-enabled agents may share one provider instance", () =>
    Effect.gen(function* () {
      const serverSettings = yield* ServerSettingsModule.ServerSettingsService;

      // The one-agent-per-instance guard is gone: wake threads stamp their
      // logical agent (thread.create -> logicalAgentId) and MCP credentials
      // carry it, so tool calls resolve identity from the session binding.
      // Only unbound sessions fall back to instance-level resolution, which
      // the tool layer gates (see resolveProjectToolContext).
      const next = yield* serverSettings.updateSettings({
        logicalAgents: {
          [LogicalAgentId.make("ag_build")]: {
            agentName: "Build agent",
            providerInstanceId: ProviderInstanceId.make("codex"),
            persona: "",
            thinkLevel: null,
            modelOverride: null,
            project: { enabled: true },
          },
          [LogicalAgentId.make("ag_review")]: {
            agentName: "Review agent",
            providerInstanceId: ProviderInstanceId.make("codex"),
            persona: "",
            thinkLevel: null,
            modelOverride: null,
            project: { enabled: true },
          },
          [LogicalAgentId.make("ag_other")]: {
            agentName: "Other agent",
            providerInstanceId: ProviderInstanceId.make("claudeAgent"),
            persona: "",
            thinkLevel: null,
            modelOverride: null,
            project: { enabled: true },
          },
        },
      });
      assert.equal(Object.keys(next.logicalAgents).length, 3);
      // The rejected-reference rule still stands on its own.
      const error = yield* Effect.flip(
        serverSettings.updateSettings({
          logicalAgents: {
            [LogicalAgentId.make("ag_ghost")]: {
              agentName: "Ghost",
              providerInstanceId: ProviderInstanceId.make("ghost_instance"),
              persona: "",
              thinkLevel: null,
              modelOverride: null,
              project: { enabled: false },
            },
          },
        }),
      );
      assert.deepInclude(error, {
        _tag: "ServerSettingsError",
        operation: "validate",
        providerInstanceId: "ghost_instance",
      });
    }).pipe(Effect.provide(makeServerSettingsLayer())),
  );

  it.effect("renaming a logical agent keeps its id and every other field", () =>
    Effect.gen(function* () {
      const serverSettings = yield* ServerSettingsModule.ServerSettingsService;

      const created = yield* serverSettings.updateSettings({
        logicalAgents: {
          [LogicalAgentId.make("ag_stable")]: {
            agentName: "Build agent",
            providerInstanceId: ProviderInstanceId.make("codex"),
            persona: "",
            thinkLevel: null,
            modelOverride: null,
            project: { enabled: true },
          },
        },
      });
      const createdAgent = created.logicalAgents[LogicalAgentId.make("ag_stable")]!;

      const renamed = yield* serverSettings.updateSettings({
        logicalAgents: {
          [LogicalAgentId.make("ag_stable")]: { ...createdAgent, agentName: "Review agent" },
        },
      });

      assert.deepEqual(Object.keys(renamed.logicalAgents), ["ag_stable"]);
      assert.deepEqual(renamed.logicalAgents[LogicalAgentId.make("ag_stable")], {
        ...createdAgent,
        agentName: "Review agent",
      });
    }).pipe(Effect.provide(makeServerSettingsLayer())),
  );

  it.effect("deleting a provider instance succeeds while a stored agent references it", () =>
    Effect.gen(function* () {
      const serverSettings = yield* ServerSettingsModule.ServerSettingsService;
      const instance = {
        [ProviderInstanceId.make("codex_personal")]: {
          driver: ProviderDriverKind.make("codex"),
          config: {},
        },
      };
      yield* serverSettings.updateSettings({
        providerInstances: instance,
        logicalAgents: {
          [LogicalAgentId.make("ag_one")]: {
            agentName: "Build agent",
            providerInstanceId: ProviderInstanceId.make("codex_personal"),
            persona: "",
            thinkLevel: null,
            modelOverride: null,
            project: { enabled: false },
          },
        },
      });

      // Whole-map replacement without the instance must not be blocked by
      // the stored agent still pointing at it.
      const next = yield* serverSettings.updateSettings({ providerInstances: {} });
      assert.deepEqual(next.providerInstances, {});
      const stored = yield* serverSettings.getSettings;
      assert.equal(
        stored.logicalAgents[LogicalAgentId.make("ag_one")]?.providerInstanceId,
        "codex_personal",
      );
    }).pipe(Effect.provide(makeServerSettingsLayer())),
  );

  it.effect(
    "a services-only patch succeeds while a stored agent has a stale provider reference",
    () =>
      Effect.gen(function* () {
        const serverConfig = yield* ServerConfig.ServerConfig;
        const fileSystem = yield* FileSystem.FileSystem;
        // An out-of-band settings edit leaves an agent referencing an
        // instance that no longer resolves.
        yield* fileSystem.writeFileString(
          serverConfig.settingsPath,
          '{"logicalAgents":{"ag_ghost":{"agentName":"Ghost","providerInstanceId":"ghost_instance"}}}',
        );

        const serverSettings = yield* ServerSettingsModule.ServerSettingsService;
        const next = yield* serverSettings.updateSettings({
          projectServiceClient: { enabled: true },
        });

        assert.equal(next.projectServiceClient.enabled, true);
      }).pipe(Effect.provide(makeServerSettingsLayer())),
  );

  it.effect("accepts a logical agent patch that still carries a stale projectBindings key", () =>
    Effect.gen(function* () {
      const serverSettings = yield* ServerSettingsModule.ServerSettingsService;

      // Issue #6 deleted the bindings surface; a client built before it may
      // echo the key back. The patch decodes with the unknown key stripped
      // (pinned in @t3tools/contracts) and the write applies.
      const agents = {
        [LogicalAgentId.make("ag_one")]: {
          agentName: "Build agent",
          providerInstanceId: ProviderInstanceId.make("codex"),
          persona: "",
          thinkLevel: null,
          modelOverride: null,
          project: { enabled: true },
          projectBindings: [{ projectId: "proj_9", projectName: "Wiki", t3ProjectId: "local-1" }],
        },
      };
      const next = yield* serverSettings.updateSettings({
        logicalAgents: agents as never,
      });

      assert.deepEqual(next.logicalAgents[LogicalAgentId.make("ag_one")], {
        agentName: "Build agent",
        providerInstanceId: ProviderInstanceId.make("codex"),
        persona: "",
        thinkLevel: null,
        modelOverride: null,
        project: { enabled: true },
      });
    }).pipe(Effect.provide(makeServerSettingsLayer())),
  );

  it.effect("rejects a non-local Project Service base URL before touching the secret store", () =>
    Effect.gen(function* () {
      const serverConfig = yield* ServerConfig.ServerConfig;
      const fileSystem = yield* FileSystem.FileSystem;
      const serverSettings = yield* ServerSettingsModule.ServerSettingsService;

      const error = yield* Effect.flip(
        serverSettings.updateSettings({
          projectServiceClient: {
            baseUrl: "https://ps.example.com",
            newCredential: "psk_key-1.s3cret",
          },
        }),
      );

      assert.deepInclude(error, { _tag: "ServerSettingsError", operation: "validate" });
      assert.equal(
        yield* fileSystem.exists(
          `${serverConfig.secretsDir}/project-service-client-credential.bin`,
        ),
        false,
      );
      const settings = yield* serverSettings.getSettings;
      assert.notEqual(settings.projectServiceClient.baseUrl, "https://ps.example.com");
    }).pipe(Effect.provide(makeServerSettingsLayer())),
  );

  it.effect("accepts a private-network Project Service base URL", () =>
    Effect.gen(function* () {
      const serverSettings = yield* ServerSettingsModule.ServerSettingsService;

      const next = yield* serverSettings.updateSettings({
        projectServiceClient: { baseUrl: "http://10.1.2.3:7600" },
      });

      assert.equal(next.projectServiceClient.baseUrl, "http://10.1.2.3:7600");
    }).pipe(Effect.provide(makeServerSettingsLayer())),
  );

  it.effect("restores the previous credential when the settings write fails", () =>
    Effect.gen(function* () {
      const serverConfig = yield* ServerConfig.ServerConfig;
      const fileSystem = yield* FileSystem.FileSystem;
      const serverSettings = yield* ServerSettingsModule.ServerSettingsService;
      const secretPath = `${serverConfig.secretsDir}/project-service-client-credential.bin`;

      yield* serverSettings.updateSettings({
        projectServiceClient: { newCredential: "psk_key-1.s3cret" },
      });

      // Break the settings write seam: the atomic rename cannot land on a
      // directory, so the next save fails after the secret was replaced.
      yield* fileSystem.remove(serverConfig.settingsPath);
      yield* fileSystem.makeDirectory(serverConfig.settingsPath);

      const error = yield* Effect.flip(
        serverSettings.updateSettings({
          projectServiceClient: { newCredential: "psk_key-2.next-secret" },
        }),
      );
      assert.deepInclude(error, { _tag: "ServerSettingsError", operation: "write-file" });

      // The previous secret is back and the visible state never moved.
      assert.equal(
        new TextDecoder().decode(yield* fileSystem.readFile(secretPath)),
        "psk_key-1.s3cret",
      );
      const settings = yield* serverSettings.getSettings;
      assert.equal(settings.projectServiceClient.keyIdHint, "key-1");
      assert.equal(settings.projectServiceClient.credentialSet, true);
    }).pipe(Effect.provide(makeServerSettingsLayer())),
  );

  it.effect(
    "removes a just-written credential when the settings write fails and none was stored",
    () =>
      Effect.gen(function* () {
        const serverConfig = yield* ServerConfig.ServerConfig;
        const fileSystem = yield* FileSystem.FileSystem;
        const serverSettings = yield* ServerSettingsModule.ServerSettingsService;
        const secretPath = `${serverConfig.secretsDir}/project-service-client-credential.bin`;

        // Warm the settings file and cache before breaking the write seam.
        yield* serverSettings.updateSettings({});
        yield* fileSystem.remove(serverConfig.settingsPath).pipe(Effect.ignore);
        yield* fileSystem.makeDirectory(serverConfig.settingsPath);

        const error = yield* Effect.flip(
          serverSettings.updateSettings({
            projectServiceClient: { newCredential: "psk_key-1.s3cret" },
          }),
        );
        assert.deepInclude(error, { _tag: "ServerSettingsError", operation: "write-file" });
        assert.equal(yield* fileSystem.exists(secretPath), false);
      }).pipe(Effect.provide(makeServerSettingsLayer())),
  );
});
