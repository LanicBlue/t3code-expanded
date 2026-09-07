/**
 * ZCodeProvider — status probe + model catalog for ZCode provider snapshots.
 *
 * The probe spawns a short-lived zcode app-server, verifies the runtime with
 * `session/list`, and reads the model catalog from a scratch `session/create`
 * (probe-verified: there is no lighter model-list method — every full-snapshot
 * response carries `settings.model.available`, and create is the cheapest way
 * to obtain one). The scratch session is closed immediately. CLI version comes
 * from a separate `zcode --version` spawn; auth state from the presence of a
 * provider entry in `~/.zcode/cli/config.json` (zcode reads its credentials
 * from there — T3 never handles ZCode OAuth).
 *
 * @module ZCodeProvider
 */
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as ChildProcess from "effect/unstable/process/ChildProcess";
import * as ChildProcessSpawner from "effect/unstable/process/ChildProcessSpawner";
import * as NodeOS from "node:os";

import type { ServerProvider, ServerProviderModel, ZCodeSettings } from "@t3tools/contracts";

import {
  AUTH_PROBE_TIMEOUT_MS,
  buildServerProvider,
  COMPACT_SLASH_COMMAND,
  DEFAULT_TIMEOUT_MS,
  type ServerProviderDraft,
  spawnAndCollect,
} from "../providerSnapshot.ts";
import {
  makeZcodeProtocolClient,
  readZcodeSessionSnapshot,
  ZcodeProtocolSpawnError,
  zcodeModelRefToSlug,
  type ZcodeModelDescriptor,
  type ZcodeModelRef,
} from "../zcode/ZcodeProtocolClient.ts";
import { resolveZcodeLaunchArgs, zcodeLaunchArgv } from "./zcodeLaunchArgs.ts";

const isZcodeProtocolSpawnError = Schema.is(ZcodeProtocolSpawnError);

const ZCODE_PRESENTATION = {
  displayName: "ZCode",
} as const;

const decodeUnknownJson = Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Unknown));

interface ZcodeAppServerProbeSnapshot {
  readonly models: ReadonlyArray<ServerProviderModel>;
}

export function zcodeModelCatalogToServerModels(
  available: ReadonlyArray<ZcodeModelDescriptor>,
  current: ZcodeModelRef | undefined,
  customModels: ReadonlyArray<string>,
): ReadonlyArray<ServerProviderModel> {
  const currentSlug = current !== undefined ? zcodeModelRefToSlug(current) : undefined;
  const models: Array<ServerProviderModel> = available.map((descriptor) => ({
    slug: zcodeModelRefToSlug(descriptor.ref),
    name: descriptor.label,
    isCustom: false,
    ...(currentSlug === zcodeModelRefToSlug(descriptor.ref) ? { isDefault: true } : {}),
    capabilities: null,
  }));

  const seen = new Set(models.map((model) => model.slug));
  for (const rawModel of customModels) {
    const slug = rawModel.trim();
    if (!slug || seen.has(slug)) {
      continue;
    }
    seen.add(slug);
    models.push({ slug, name: slug, isCustom: true, capabilities: null });
  }
  return models;
}

/** Reads `~/.zcode/cli/config.json` and reports whether a provider is configured. */
export const probeZcodeAuthConfig = Effect.gen(function* () {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const configPath = path.join(NodeOS.homedir(), ".zcode", "cli", "config.json");
  const content = yield* fileSystem.exists(configPath).pipe(
    Effect.flatMap((exists) =>
      exists ? fileSystem.readFileString(configPath) : Effect.succeed(""),
    ),
    Effect.orElseSucceed(() => ""),
  );
  const parsed = yield* decodeUnknownJson(content).pipe(Effect.option);
  if (Option.isNone(parsed)) {
    return false;
  }
  const provider =
    typeof parsed.value === "object" && parsed.value !== null
      ? (parsed.value as { readonly provider?: unknown }).provider
      : undefined;
  return (
    typeof provider === "object" &&
    provider !== null &&
    Object.keys(provider as Record<string, unknown>).length > 0
  );
});

const probeZcodeVersion = Effect.fn("probeZcodeVersion")(function* (input: {
  readonly binaryPath: string;
  readonly environment?: NodeJS.ProcessEnv;
}) {
  return yield* spawnAndCollect(
    input.binaryPath,
    ChildProcess.make(process.execPath, [input.binaryPath, "--version"], {
      cwd: process.cwd(),
      ...(input.environment ? { env: input.environment } : {}),
    }),
  ).pipe(
    Effect.timeoutOption(Duration.millis(DEFAULT_TIMEOUT_MS)),
    Effect.map(
      Option.match({
        onNone: () => ({ version: undefined as string | undefined, missing: true }),
        onSome: (result) => ({
          version: result.stdout.match(/(\d+\.\d+\.\d+)/)?.[1],
          missing:
            /Cannot find module|MODULE_NOT_FOUND/i.test(result.stderr) ||
            (result.code !== 0 && result.stdout.trim().length === 0),
        }),
      }),
    ),
    Effect.orElseSucceed(() => ({ version: undefined as string | undefined, missing: true })),
  );
});

const probeZcodeAppServer = Effect.fn("probeZcodeAppServer")(function* (input: {
  readonly binaryPath: string;
  readonly launchArgs?: ReadonlyArray<string> | undefined;
  readonly cwd: string;
  readonly environment?: NodeJS.ProcessEnv | undefined;
  readonly customModels: ReadonlyArray<string>;
}) {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  return yield* Effect.gen(function* () {
    const client = yield* Effect.acquireRelease(
      makeZcodeProtocolClient({
        binaryPath: input.binaryPath,
        ...(input.launchArgs ? { launchArgs: input.launchArgs } : {}),
        cwd: input.cwd,
        ...(input.environment ? { environment: input.environment } : {}),
      }).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner)),
      (shape) => shape.close.pipe(Effect.ignore),
    );

    // Cheap runtime check first: session/list reads the shared store and
    // requires a working app-server.
    yield* client.request("session/list", {});

    // The catalog only rides on full-snapshot responses; create a scratch
    // session, read it, close it.
    const created = yield* client.request("session/create", {
      workspace: { workspacePath: input.cwd, workspaceKey: input.cwd },
      mode: "build",
    });
    const snapshot = readZcodeSessionSnapshot(created);
    const models =
      snapshot?.settings?.model !== undefined
        ? zcodeModelCatalogToServerModels(
            snapshot.settings.model.available ?? [],
            snapshot.settings.model.current,
            input.customModels,
          )
        : zcodeModelCatalogToServerModels([], undefined, input.customModels);
    if (snapshot !== undefined) {
      yield* client
        .request("session/close", { sessionId: snapshot.session.sessionId })
        .pipe(Effect.ignore);
    }
    return { models } satisfies ZcodeAppServerProbeSnapshot;
  }).pipe(Effect.scoped);
});

export const makePendingZcodeProvider = (
  zcodeSettings: ZCodeSettings,
): Effect.Effect<ServerProviderDraft> =>
  Effect.gen(function* () {
    const checkedAt = yield* Effect.map(DateTime.now, DateTime.formatIso);
    const models = zcodeModelCatalogToServerModels([], undefined, zcodeSettings.customModels);

    if (!zcodeSettings.enabled) {
      return buildServerProvider({
        presentation: ZCODE_PRESENTATION,
        enabled: false,
        checkedAt,
        models,
        skills: [],
        probe: {
          installed: false,
          version: null,
          status: "warning",
          auth: { status: "unknown" },
          message: "ZCode is disabled in T3 Code settings.",
        },
      });
    }

    return buildServerProvider({
      presentation: ZCODE_PRESENTATION,
      enabled: true,
      checkedAt,
      models,
      skills: [],
      probe: {
        installed: false,
        version: null,
        status: "warning",
        auth: { status: "unknown" },
        message: "ZCode provider status has not been checked in this session yet.",
      },
    });
  });

export const checkZcodeProviderStatus = Effect.fn("checkZcodeProviderStatus")(function* (
  zcodeSettings: ZCodeSettings,
  environment?: NodeJS.ProcessEnv,
): Effect.fn.Return<
  ServerProviderDraft,
  never,
  ChildProcessSpawner.ChildProcessSpawner | FileSystem.FileSystem | Path.Path
> {
  const resolvedEnvironment = environment ?? process.env;
  const checkedAt = DateTime.formatIso(yield* DateTime.now);
  const emptyModels = zcodeModelCatalogToServerModels([], undefined, zcodeSettings.customModels);

  if (!zcodeSettings.enabled) {
    return buildServerProvider({
      presentation: ZCODE_PRESENTATION,
      enabled: false,
      checkedAt,
      models: emptyModels,
      skills: [],
      probe: {
        installed: false,
        version: null,
        status: "warning",
        auth: { status: "unknown" },
        message: "ZCode is disabled in T3 Code settings.",
      },
    });
  }

  const launchArgs = zcodeLaunchArgv(
    resolveZcodeLaunchArgs(zcodeSettings.launchArgs, resolvedEnvironment),
  );

  const [versionProbe, appServerProbe, authenticated] = yield* Effect.all(
    [
      probeZcodeVersion({
        binaryPath: zcodeSettings.binaryPath,
        environment: resolvedEnvironment,
      }),
      probeZcodeAppServer({
        binaryPath: zcodeSettings.binaryPath,
        ...(launchArgs.length > 0 ? { launchArgs } : {}),
        cwd: process.cwd(),
        ...(resolvedEnvironment ? { environment: resolvedEnvironment } : {}),
        customModels: zcodeSettings.customModels,
      }).pipe(Effect.timeoutOption(Duration.millis(AUTH_PROBE_TIMEOUT_MS)), Effect.result),
      probeZcodeAuthConfig,
    ],
    { concurrency: "unbounded" },
  );

  if (versionProbe.missing) {
    return buildServerProvider({
      presentation: ZCODE_PRESENTATION,
      enabled: true,
      checkedAt,
      models: emptyModels,
      skills: [],
      probe: {
        installed: false,
        version: null,
        status: "error",
        auth: { status: "unknown" },
        message: `ZCode CLI was not found at '${zcodeSettings.binaryPath}'.`,
      },
    });
  }

  if (Result.isFailure(appServerProbe)) {
    const error = appServerProbe.failure;
    const installed = !isZcodeProtocolSpawnError(error);
    return buildServerProvider({
      presentation: ZCODE_PRESENTATION,
      enabled: true,
      checkedAt,
      models: emptyModels,
      skills: [],
      probe: {
        installed,
        version: versionProbe.version ?? null,
        status: "error",
        auth: { status: "unknown" },
        message: installed
          ? `ZCode app-server probe failed: ${error.message}`
          : "ZCode app-server failed to start.",
      },
    });
  }

  if (Option.isNone(appServerProbe.success)) {
    return buildServerProvider({
      presentation: ZCODE_PRESENTATION,
      enabled: true,
      checkedAt,
      models: emptyModels,
      skills: [],
      probe: {
        installed: true,
        version: versionProbe.version ?? null,
        status: "error",
        auth: { status: "unknown" },
        message: "Timed out while checking ZCode app-server status.",
      },
    });
  }

  const snapshot = appServerProbe.success.value;
  const auth: ServerProvider["auth"] = authenticated
    ? { status: "authenticated" }
    : { status: "unauthenticated" };

  return buildServerProvider({
    presentation: ZCODE_PRESENTATION,
    enabled: true,
    checkedAt,
    models: snapshot.models,
    skills: [],
    slashCommands: [COMPACT_SLASH_COMMAND],
    probe: {
      installed: true,
      version: versionProbe.version ?? null,
      status: "ready",
      auth,
      ...(!authenticated
        ? { message: "ZCode is not signed in. Run `zcode login` and try again." }
        : {}),
    },
  });
});
