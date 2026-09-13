/**
 * ZCodeDriver — `ProviderDriver` for the ZCode CLI runtime.
 *
 * Mirrors `GrokDriver`: a plain value whose `create()` returns one
 * `ProviderInstance` bundling `snapshot` / `adapter` / `textGeneration`
 * closures captured over the per-instance `ZCodeSettings`. The adapter owns
 * ONE shared zcode app-server process for all of the instance's sessions
 * (see `Layers/ZCodeAdapter` for the sharing rationale).
 *
 * @module provider/Drivers/ZCodeDriver
 */
import { ProviderDriverKind, ZCodeSettings } from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import { ChildProcessSpawner } from "effect/unstable/process";

import * as BackgroundPolicy from "../../background/BackgroundPolicy.ts";
import { ServerSettingsService } from "../../serverSettings.ts";
import { makeZcodeTextGeneration } from "../../textGeneration/ZCodeTextGeneration.ts";
import { ProviderDriverError } from "../Errors.ts";
import { makeZcodeAdapter } from "../Layers/ZCodeAdapter.ts";
import { checkZcodeProviderStatus, makePendingZcodeProvider } from "../Layers/ZCodeProvider.ts";
import { makeManagedServerProvider } from "../makeManagedServerProvider.ts";
import {
  defaultProviderContinuationIdentity,
  type ProviderDriver,
  type ProviderInstance,
} from "../ProviderDriver.ts";
import { withInstanceIdentity } from "./instanceIdentity.ts";
import { mergeProviderInstanceEnvironment } from "../ProviderInstanceEnvironment.ts";
import {
  materializeZcodeShadowHome,
  resolveZcodeShadowHomePath,
  zcodeShadowHomeEnvironment,
} from "./ZcodeShadowHome.ts";
import {
  makeCachedProviderMaintenanceResolution,
  makeManualOnlyProviderMaintenanceCapabilities,
  type ProviderMaintenanceCapabilitiesResolver,
  resolveProviderMaintenanceCapabilitiesEffect,
} from "../providerMaintenance.ts";
import {
  haveProviderSnapshotSettingsChanged,
  makeProviderSnapshotSettingsSource,
  type ProviderSnapshotSettings,
} from "../providerUpdateSettings.ts";

const decodeZcodeSettings = Schema.decodeSync(ZCodeSettings);

const DRIVER_KIND = ProviderDriverKind.make("zcode");
// The ZCode desktop app updates itself; from T3's side updates are manual-only.
const UPDATE: ProviderMaintenanceCapabilitiesResolver = {
  resolve: () =>
    Effect.succeed(
      makeManualOnlyProviderMaintenanceCapabilities({
        provider: DRIVER_KIND,
        packageName: null,
      }),
    ),
};

export type ZCodeDriverEnv =
  | BackgroundPolicy.BackgroundPolicy
  | ChildProcessSpawner.ChildProcessSpawner
  | Crypto.Crypto
  | FileSystem.FileSystem
  | Path.Path
  | ServerSettingsService;

export const ZCodeDriver: ProviderDriver<ZCodeSettings, ZCodeDriverEnv> = {
  driverKind: DRIVER_KIND,
  metadata: {
    displayName: "ZCode",
    supportsMultipleInstances: true,
  },
  configSchema: ZCodeSettings,
  defaultConfig: (): ZCodeSettings => decodeZcodeSettings({}),
  create: ({ instanceId, displayName, accentColor, environment, enabled, config }) =>
    Effect.gen(function* () {
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const serverSettings = yield* ServerSettingsService;
      const processEnv = mergeProviderInstanceEnvironment(environment);
      const continuationIdentity = defaultProviderContinuationIdentity({
        driverKind: DRIVER_KIND,
        instanceId,
      });
      const stampIdentity = withInstanceIdentity({
        instanceId,
        driverKind: DRIVER_KIND,
        displayName,
        accentColor,
        continuationGroupKey: continuationIdentity.continuationKey,
      });
      const effectiveConfig = { ...config, enabled } satisfies ZCodeSettings;
      // Shadow home: without it, T3's zcode sessions land in the desktop
      // app's shared ~/.zcode store (listed there, and opening them in the
      // desktop steals session ownership). Every zcode process this driver
      // spawns — adapter app-server, probes, text generation — runs with
      // HOME pointed at the shadow directory instead.
      const shadowHomePath = resolveZcodeShadowHomePath(config);
      if (shadowHomePath instanceof Error) {
        return yield* new ProviderDriverError({
          driver: DRIVER_KIND,
          instanceId,
          detail: shadowHomePath.message,
          cause: shadowHomePath,
        });
      }
      if (shadowHomePath !== undefined) {
        yield* materializeZcodeShadowHome({ shadowHomePath }).pipe(
          Effect.mapError(
            (cause) =>
              new ProviderDriverError({
                driver: DRIVER_KIND,
                instanceId,
                detail: cause.message,
                cause,
              }),
          ),
        );
      }
      const zcodeProcessEnv =
        shadowHomePath !== undefined
          ? zcodeShadowHomeEnvironment(shadowHomePath, processEnv)
          : processEnv;
      const resolveMaintenance = yield* makeCachedProviderMaintenanceResolution(
        resolveProviderMaintenanceCapabilitiesEffect(UPDATE, {
          binaryPath: effectiveConfig.binaryPath,
          env: zcodeProcessEnv,
        }).pipe(
          Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
          Effect.provideService(FileSystem.FileSystem, fileSystem),
          Effect.provideService(Path.Path, path),
        ),
      );

      const adapter = yield* makeZcodeAdapter(effectiveConfig, {
        instanceId,
        environment: zcodeProcessEnv,
      });
      const textGeneration = yield* makeZcodeTextGeneration(effectiveConfig, zcodeProcessEnv);

      const checkProvider = checkZcodeProviderStatus(effectiveConfig, zcodeProcessEnv).pipe(
        Effect.map(stampIdentity),
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
        Effect.provideService(FileSystem.FileSystem, fileSystem),
        Effect.provideService(Path.Path, path),
      );

      const snapshotSettings = makeProviderSnapshotSettingsSource(effectiveConfig, serverSettings);
      const snapshot = yield* makeManagedServerProvider<ProviderSnapshotSettings<ZCodeSettings>>({
        resolveMaintenance,
        getSettings: snapshotSettings.getSettings,
        streamSettings: snapshotSettings.streamSettings,
        haveSettingsChanged: haveProviderSnapshotSettingsChanged,
        initialSnapshot: (settings) =>
          makePendingZcodeProvider(settings.provider).pipe(Effect.map(stampIdentity)),
        checkProvider,
      }).pipe(
        Effect.mapError(
          (cause) =>
            new ProviderDriverError({
              driver: DRIVER_KIND,
              instanceId,
              detail: `Failed to build ZCode snapshot: ${cause.message ?? String(cause)}`,
              cause,
            }),
        ),
      );

      return {
        instanceId,
        driverKind: DRIVER_KIND,
        continuationIdentity,
        displayName,
        accentColor,
        enabled,
        snapshot,
        adapter,
        textGeneration,
      } satisfies ProviderInstance;
    }),
};
