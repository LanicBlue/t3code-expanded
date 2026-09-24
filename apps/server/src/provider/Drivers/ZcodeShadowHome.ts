/**
 * ZcodeShadowHome — isolate T3's zcode app-server from the desktop's shared
 * `~/.zcode` store.
 *
 * zcode has no `CODEX_HOME`-style env var: every store path (session db,
 * rollout, logs, exec) resolves from `os.homedir() + ".zcode"`, and that one
 * store is also what the ZCode desktop app lists — so T3-created duty
 * sessions surface (and can be hijacked) in the desktop UI. Isolation =
 * run every T3-spawned zcode process with `HOME` pointed at a shadow
 * directory:
 *   - `cli/config.json` (+ `v2/config.json`) are copied from the real home
 *     on every materialization — auth and model config travel along and
 *     stay fresh;
 *   - a personal provider-config pair is materialized under
 *     `v2/t3-provider-config/` and exported through the
 *     `ZCODE_{BUILTIN,PERSONAL}_PROVIDER_CONFIG_FILE` env pair (see
 *     `zcodeProviderConfigEnvironment`): ZCode ≥3.12.3 resolves the
 *     headless provider registry from the builtin store plus a personal
 *     provider config file ONLY, so the API-key providers the user keeps in
 *     the legacy `v2/config.json` provider map would otherwise vanish from
 *     every T3-spawned app-server (turns fail with "Model creation failed");
 *   - `cli/{agents,plugins,memories}` and `skills` are symlinked — static
 *     user-level content stays shared;
 *   - everything else (db, rollout, log, exec, artifacts) is created fresh
 *     inside the shadow and never reaches the desktop's store;
 *   - `.zshenv`/`.bash_profile` stubs flip `HOME` back to the real home, so
 *     shell-snapshot capture and every tool shell keep the user's PATH and
 *     dotfiles. The app-server resolves its store from its own process env
 *     before any shell runs, so this does not reintroduce sharing.
 *
 * Materialization is idempotent — safe to run on every driver create.
 *
 * @module provider/Drivers/ZcodeShadowHome
 */
// @effect-diagnostics nodeBuiltinImport:off
import type { ZCodeSettings } from "@t3tools/contracts";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

import { expandHomePath } from "../../pathExpansion.ts";
import { fromJsonStringPretty } from "@t3tools/shared/schemaJson";

/** Copied (not linked): auth/model config that must live in the shadow home. */
const COPY_CONFIG_FILES = ["cli/config.json", "v2/config.json"] as const;
/** Symlinked: static user-level content shared with the real home. */
const LINKED_ENTRIES = ["cli/agents", "cli/plugins", "cli/memories", "skills"] as const;

/**
 * Env pair the zcode CLI documents for pointing a headless process at its
 * provider registry inputs. Both must be provided together — the CLI errors
 * out when only one is set.
 */
export const ZCODE_BUILTIN_PROVIDER_CONFIG_FILE_ENV = "ZCODE_BUILTIN_PROVIDER_CONFIG_FILE";
export const ZCODE_PERSONAL_PROVIDER_CONFIG_FILE_ENV = "ZCODE_PERSONAL_PROVIDER_CONFIG_FILE";

const PROVIDER_CONFIG_DIR = ["v2", "t3-provider-config"] as const;
const BUILTIN_STORE_FILENAME = "zcode-builtin.json";
const PERSONAL_CONFIG_FILENAME = "provider-personal.json";

export class ZcodeShadowHomePathConflictError extends Schema.TaggedError<ZcodeShadowHomePathConflictError>()(
  "ZcodeShadowHomePathConflictError",
  {
    shadowHomePath: Schema.String,
    realHomeDir: Schema.String,
  },
) {
  override get message(): string {
    return `zcode shadow home '${this.shadowHomePath}' must live outside the shared zcode store '${this.realHomeDir}' — pointing it at (or into) ~/.zcode would share the desktop store instead of isolating it.`;
  }
}

export class ZcodeShadowHomeFileSystemError extends Schema.TaggedError<ZcodeShadowHomeFileSystemError>()(
  "ZcodeShadowHomeFileSystemError",
  {
    operation: Schema.Literals([
      "makeDirectory",
      "copyFile",
      "symlink",
      "lstat",
      "remove",
      "writeFile",
    ]),
    path: Schema.String,
    cause: Schema.Defect(),
  },
) {
  override get message(): string {
    return `zcode shadow home operation '${this.operation}' failed for '${this.path}'.`;
  }
}

export class ZcodeShadowHomeEntryConflictError extends Schema.TaggedError<ZcodeShadowHomeEntryConflictError>()(
  "ZcodeShadowHomeEntryConflictError",
  {
    entry: Schema.String,
    path: Schema.String,
  },
) {
  override get message(): string {
    return `zcode shadow home entry '${this.entry}' already exists at '${this.path}' and is not a symlink.`;
  }
}

export const ZcodeShadowHomeError = Schema.Union([
  ZcodeShadowHomePathConflictError,
  ZcodeShadowHomeFileSystemError,
  ZcodeShadowHomeEntryConflictError,
]);
export type ZcodeShadowHomeError = typeof ZcodeShadowHomeError.Type;

type LinkState =
  | { readonly kind: "missing" }
  | { readonly kind: "real" }
  | { readonly kind: "symlink"; readonly target: string };

/**
 * Absolute shadow home path for the settings value, or undefined when the
 * setting is empty (shared-home mode, the previous behavior). A path at (or
 * inside) the real `~/.zcode` store is rejected — it would silently keep the
 * desktop store shared. A sibling elsewhere under the home (e.g.
 * `~/.t3/zcode-home`) is fine: only the `.zcode` tree is what zcode and the
 * desktop share.
 */
export function resolveZcodeShadowHomePath(
  config: Pick<ZCodeSettings, "shadowHomePath">,
  homedir: string = NodeOS.homedir(),
): string | undefined | ZcodeShadowHomePathConflictError {
  const raw = config.shadowHomePath.trim();
  if (raw.length === 0) return undefined;
  const shadowHomePath = NodePath.resolve(expandHomePath(raw));
  const realZcodeStore = NodePath.join(NodePath.resolve(homedir), ".zcode");
  if (
    shadowHomePath === realZcodeStore ||
    shadowHomePath.startsWith(realZcodeStore + NodePath.sep)
  ) {
    return new ZcodeShadowHomePathConflictError({ shadowHomePath, realHomeDir: realZcodeStore });
  }
  return shadowHomePath;
}

const fsError =
  (operation: (typeof ZcodeShadowHomeFileSystemError.prototype)["operation"], path: string) =>
  (cause: unknown) =>
    new ZcodeShadowHomeFileSystemError({ operation, path, cause });

// ── Personal provider config (ZCode ≥3.12.3 registry bridge) ─────────────

type LegacyProviderEntry = {
  readonly name?: unknown;
  readonly kind?: unknown;
  readonly enabled?: unknown;
  readonly options?: unknown;
  readonly models?: unknown;
};

/** One entry of the personal `providerRules` the current CLI accepts. */
export interface ZcodePersonalProviderRule {
  readonly providerId: string;
  readonly providerName: string;
  readonly enabled: boolean;
  readonly config: {
    readonly group: "standard-personal";
    readonly access: { readonly type: "api-key"; readonly apiKey: string };
    readonly api: {
      readonly type: "anthropic-messages" | "openai-chat-completions";
      readonly baseUrl: string;
    };
    readonly personalModelIds: ReadonlyArray<string>;
  };
}

const LEGACY_PROVIDER_ID_PREFIX = "builtin:";

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null;

/**
 * Convert the legacy `v2/config.json` `provider` map (API-key providers the
 * desktop no longer feeds into the headless registry) into personal
 * `providerRules`. Entries without an API key + base URL (e.g. OAuth-backed
 * official entries) are skipped — they carry no migratable credentials.
 */
export function buildZcodePersonalProviderRules(
  providerMap: unknown,
): ReadonlyArray<ZcodePersonalProviderRule> {
  if (!isRecord(providerMap)) return [];
  const rules: Array<ZcodePersonalProviderRule> = [];
  for (const [rawId, rawEntry] of Object.entries(providerMap)) {
    if (!isRecord(rawEntry)) continue;
    const entry = rawEntry as LegacyProviderEntry;
    const options = isRecord(entry.options) ? entry.options : undefined;
    const apiKey = typeof options?.apiKey === "string" ? options.apiKey.trim() : "";
    const baseURL = typeof options?.baseURL === "string" ? options.baseURL.trim() : "";
    if (apiKey.length === 0 || !/^https?:\/\//.test(baseURL)) continue;
    const modelIds = isRecord(entry.models) ? Object.keys(entry.models) : [];
    rules.push({
      providerId: rawId.startsWith(LEGACY_PROVIDER_ID_PREFIX)
        ? rawId.slice(LEGACY_PROVIDER_ID_PREFIX.length)
        : rawId,
      providerName:
        typeof entry.name === "string" && entry.name.trim().length > 0 ? entry.name : rawId,
      enabled: typeof entry.enabled === "boolean" ? entry.enabled : true,
      config: {
        group: "standard-personal",
        access: { type: "api-key", apiKey },
        api: {
          // The legacy `kind` is the wire protocol; the registry wants the
          // API flavor. Both values the CLI ships (anthropic-messages,
          // openai-chat-completions) map from the two legacy kinds.
          type: entry.kind === "anthropic" ? "anthropic-messages" : "openai-chat-completions",
          baseUrl: baseURL,
        },
        personalModelIds: modelIds,
      },
    });
  }
  return rules;
}

/**
 * The complete personal provider-config file, or undefined when the legacy
 * config has nothing migratable (no provider rules to contribute).
 */
export function buildZcodePersonalProviderConfigFile(
  providerMap: unknown,
): { readonly schemaVersion: 1; readonly config: unknown } | undefined {
  const rules = buildZcodePersonalProviderRules(providerMap);
  if (rules.length === 0) return undefined;
  return {
    schemaVersion: 1,
    config: {
      providerConfigRules: { providerRules: rules },
      modelConfigRules: { providerModelRules: [], manualProviderModelRules: [] },
    },
  };
}

/**
 * Locate a zcode-builtin store to copy for the env pair: the copy shipped
 * next to the CLI (self-built CLIs place one at `<cli-dir>/provider/`, the
 * CLI's own first lookup candidate — usable even with no desktop app), else
 * the app-bundle copy, else the newest runtime-store cache the desktop keeps
 * under the real home.
 */
function resolveZcodeBuiltinStoreSource(
  binaryPath: string | undefined,
  realZcode: string,
): string | undefined {
  if (binaryPath !== undefined && binaryPath.trim().length > 0) {
    const alongside = NodePath.join(
      NodePath.dirname(binaryPath),
      "provider",
      BUILTIN_STORE_FILENAME,
    );
    if (NodeFS.existsSync(alongside)) return alongside;
    const bundled = NodePath.resolve(
      NodePath.dirname(binaryPath),
      "..",
      "config",
      "provider",
      BUILTIN_STORE_FILENAME,
    );
    if (NodeFS.existsSync(bundled)) return bundled;
  }
  const runtimeRoot = NodePath.join(realZcode, "v2", "runtime", "provider");
  let best: { readonly path: string; readonly mtime: number } | undefined;
  const walk = (directory: string): void => {
    let entries;
    try {
      entries = NodeFS.readdirSync(directory, { withFileTypes: true });
    } catch {
      return;
    }
    for (const item of entries) {
      const itemPath = NodePath.join(directory, item.name);
      if (item.isDirectory()) {
        walk(itemPath);
        continue;
      }
      if (item.name !== BUILTIN_STORE_FILENAME) continue;
      const mtime = NodeFS.statSync(itemPath).mtimeMs;
      if (best === undefined || mtime > best.mtime) best = { path: itemPath, mtime };
    }
  };
  walk(runtimeRoot);
  return best?.path;
}

/**
 * The runtime-store-layout mirror target: standalone (one-shot `--prompt`)
 * CLIs resolve their "active" builtin release from
 * `.zcode/v2/runtime/provider/<platform>/<version>/endpoint-<hash>/` — not
 * from the env pair — and without that layout they lean on a network refresh
 * that flakes in windows. The desktop writes a directory per app version; the
 * running CLI looks under its own version, so the mirror must track the
 * HIGHEST version directory (not the newest file: a CDN refresh can rewrite
 * an old version's file). Resolved independently of the env-pair source — a
 * bundle source must not stop the mirror from refreshing after updates.
 */
interface ZcodeRuntimeStoreMirror {
  readonly path: string;
  /** Path under `<home>/.zcode/v2`, e.g. `runtime/provider/<plat>/<ver>/endpoint-<h>/zcode-builtin.json`. */
  readonly relPath: string;
}

const parseVersionSegments = (raw: string): ReadonlyArray<number> | undefined => {
  const parts = raw.split(".");
  if (parts.length === 0 || parts.some((part) => !/^\d+$/.test(part))) return undefined;
  return parts.map((part) => Number.parseInt(part, 10));
};

const compareVersionSegments = (a: ReadonlyArray<number>, b: ReadonlyArray<number>): number => {
  for (let i = 0; i < Math.max(a.length, b.length); i++) {
    const left = a[i] ?? 0;
    const right = b[i] ?? 0;
    if (left !== right) return left - right;
  }
  return 0;
};

function resolveZcodeRuntimeStoreMirror(realZcode: string): ZcodeRuntimeStoreMirror | undefined {
  const runtimeRoot = NodePath.join(realZcode, "v2", "runtime", "provider");
  let platformDir;
  try {
    platformDir = NodeFS.readdirSync(runtimeRoot, { withFileTypes: true }).find((entry) =>
      entry.isDirectory(),
    );
  } catch {
    return undefined;
  }
  if (platformDir === undefined) return undefined;
  let best:
    | {
        readonly path: string;
        readonly relPath: string;
        readonly version: ReadonlyArray<number>;
        readonly mtime: number;
      }
    | undefined;
  const platformRoot = NodePath.join(runtimeRoot, platformDir.name);
  let versionDirs;
  try {
    versionDirs = NodeFS.readdirSync(platformRoot, { withFileTypes: true });
  } catch {
    return undefined;
  }
  for (const versionDir of versionDirs) {
    if (!versionDir.isDirectory()) continue;
    const version = parseVersionSegments(versionDir.name);
    if (version === undefined) continue;
    let endpointDirs;
    try {
      endpointDirs = NodeFS.readdirSync(NodePath.join(platformRoot, versionDir.name), {
        withFileTypes: true,
      });
    } catch {
      continue;
    }
    for (const endpointDir of endpointDirs) {
      if (!endpointDir.isDirectory()) continue;
      const storePath = NodePath.join(
        platformRoot,
        versionDir.name,
        endpointDir.name,
        BUILTIN_STORE_FILENAME,
      );
      if (!NodeFS.existsSync(storePath)) continue;
      const mtime = NodeFS.statSync(storePath).mtimeMs;
      if (
        best === undefined ||
        compareVersionSegments(version, best.version) > 0 ||
        (compareVersionSegments(version, best.version) === 0 && mtime > best.mtime)
      ) {
        best = {
          path: storePath,
          relPath: NodePath.relative(NodePath.join(realZcode, "v2"), storePath),
          version,
          mtime,
        };
      }
    }
  }
  if (best === undefined || best.relPath.startsWith("..")) return undefined;
  return { path: best.path, relPath: best.relPath };
}

const realHomeStub = (realHomeDir: string) =>
  [
    "# T3 zcode shadow home: flip shells back to the real user home so PATH,",
    "# tools and dotfiles behave exactly like a normal session. The zcode",
    "# app-server resolved its isolated store from its own process env",
    "# before any shell ran, so this does not reintroduce store sharing.",
    `export HOME='${realHomeDir}'`,
    "",
  ].join("\n");

const readLinkState = (link: string): Effect.Effect<LinkState, ZcodeShadowHomeFileSystemError> =>
  Effect.tryPromise({
    try: async (): Promise<LinkState> => {
      try {
        const stat = await NodeFS.promises.lstat(link);
        if (!stat.isSymbolicLink()) return { kind: "real" };
        const raw = await NodeFS.promises.readlink(link);
        return { kind: "symlink", target: NodePath.resolve(NodePath.dirname(link), raw) };
      } catch (cause) {
        if (
          typeof cause === "object" &&
          cause !== null &&
          "code" in cause &&
          cause.code === "ENOENT"
        ) {
          return { kind: "missing" };
        }
        throw cause;
      }
    },
    catch: fsError("lstat", link),
  });

/**
 * Materialize the shadow home: config copies, static symlinks, rc stubs.
 * Idempotent — an already-correct entry is left alone.
 */
export const materializeZcodeShadowHome = Effect.fn("materializeZcodeShadowHome")(
  function* (input: {
    readonly shadowHomePath: string;
    readonly realHomeDir?: string;
    /** zcode CLI path — used to locate the app-bundle builtin provider store. */
    readonly binaryPath?: string | undefined;
  }): Effect.fn.Return<void, ZcodeShadowHomeError, FileSystem.FileSystem> {
    const fileSystem = yield* FileSystem.FileSystem;
    const realHomeDir = NodePath.resolve(input.realHomeDir ?? NodeOS.homedir());
    const realZcode = NodePath.join(realHomeDir, ".zcode");
    const shadowZcode = NodePath.join(input.shadowHomePath, ".zcode");

    const makeDirectory = (directoryPath: string) =>
      fileSystem
        .makeDirectory(directoryPath, { recursive: true })
        .pipe(Effect.mapError(fsError("makeDirectory", directoryPath)));

    yield* makeDirectory(NodePath.join(shadowZcode, "cli"));

    // Fresh copies on every materialization keep auth/model config current.
    for (const configEntry of COPY_CONFIG_FILES) {
      const from = NodePath.join(realZcode, configEntry);
      const to = NodePath.join(shadowZcode, configEntry);
      if (!NodeFS.existsSync(from)) continue;
      yield* makeDirectory(NodePath.dirname(to));
      yield* Effect.tryPromise({
        try: () => NodeFS.promises.copyFile(from, to),
        catch: fsError("copyFile", to),
      });
    }

    // Personal provider-config pair (see module doc): without it the current
    // CLI drops the user's API-key providers from the headless registry.
    // Generated from the legacy provider map in the just-copied v2 config so
    // credentials never leave the shadow home; the builtin store is copied
    // (not linked) so CDN refresh writes by the CLI stay inside the shadow.
    const providerConfigDirPath = NodePath.join(shadowZcode, ...PROVIDER_CONFIG_DIR);
    const builtinStorePath = NodePath.join(providerConfigDirPath, BUILTIN_STORE_FILENAME);
    const personalConfigPath = NodePath.join(providerConfigDirPath, PERSONAL_CONFIG_FILENAME);
    const builtinStoreSource = resolveZcodeBuiltinStoreSource(input.binaryPath, realZcode);
    // Mirror target tracked independently of the env-pair source: the bundle
    // usually wins for the pair, but the mirror must follow the runtime
    // tree's newest app version or one-shot CLIs lose their active release
    // after every desktop update.
    const runtimeMirror = resolveZcodeRuntimeStoreMirror(realZcode);
    const shadowRuntimeProviderRoot = NodePath.join(shadowZcode, "v2", "runtime", "provider");
    const encodePersonalConfigJson = Schema.encodeUnknownEffect(
      fromJsonStringPretty(Schema.Unknown),
    );
    const decodeLegacyProviderMap = Schema.decodeUnknownEffect(
      Schema.fromJsonString(Schema.Struct({ provider: Schema.optional(Schema.Unknown) })),
    );
    // An unreadable/unparsable legacy config keeps the previous behavior (no
    // personal providers) rather than failing the whole driver.
    const personalConfig = yield* fileSystem
      .readFileString(NodePath.join(shadowZcode, "v2", "config.json"))
      .pipe(
        Effect.flatMap((raw) => decodeLegacyProviderMap(raw)),
        Effect.flatMap((legacy) => {
          const file = buildZcodePersonalProviderConfigFile(legacy.provider);
          return file === undefined ? Effect.succeedNone : Effect.succeedSome(file);
        }),
        Effect.catch(() => Effect.succeedNone),
      );
    if (builtinStoreSource !== undefined && Option.isSome(personalConfig)) {
      yield* makeDirectory(providerConfigDirPath);
      yield* Effect.tryPromise({
        try: () => NodeFS.promises.copyFile(builtinStoreSource, builtinStorePath),
        catch: fsError("copyFile", builtinStorePath),
      });
      yield* encodePersonalConfigJson(personalConfig.value).pipe(
        Effect.mapError(
          (cause) =>
            new ZcodeShadowHomeFileSystemError({
              operation: "writeFile",
              path: personalConfigPath,
              cause,
            }),
        ),
        Effect.flatMap((json) =>
          Effect.tryPromise({
            try: () => NodeFS.promises.writeFile(personalConfigPath, `${json}\n`),
            catch: fsError("writeFile", personalConfigPath),
          }),
        ),
      );
      // Runtime-store mirror: rebuilt from scratch so a desktop update (new
      // version directory) replaces any aged mirror instead of accumulating.
      if (runtimeMirror !== undefined) {
        if (NodeFS.existsSync(shadowRuntimeProviderRoot)) {
          yield* Effect.tryPromise({
            try: () =>
              NodeFS.promises.rm(shadowRuntimeProviderRoot, { recursive: true, force: true }),
            catch: fsError("remove", shadowRuntimeProviderRoot),
          });
        }
        const mirrorPath = NodePath.join(shadowZcode, "v2", runtimeMirror.relPath);
        yield* makeDirectory(NodePath.dirname(mirrorPath));
        yield* Effect.tryPromise({
          try: () => NodeFS.promises.copyFile(runtimeMirror.path, mirrorPath),
          catch: fsError("copyFile", mirrorPath),
        });
      }
    } else {
      // The pair is no longer derivable — drop a stale pair instead of
      // pointing the env at outdated providers.
      if (NodeFS.existsSync(providerConfigDirPath)) {
        yield* Effect.tryPromise({
          try: () => NodeFS.promises.rm(providerConfigDirPath, { recursive: true, force: true }),
          catch: fsError("remove", providerConfigDirPath),
        });
      }
      if (NodeFS.existsSync(shadowRuntimeProviderRoot)) {
        yield* Effect.tryPromise({
          try: () =>
            NodeFS.promises.rm(shadowRuntimeProviderRoot, { recursive: true, force: true }),
          catch: fsError("remove", shadowRuntimeProviderRoot),
        });
      }
    }

    // Static shared entries: symlink once; fix a stale link, never overwrite a
    // real file the operator placed.
    for (const entry of LINKED_ENTRIES) {
      const target = NodePath.join(realZcode, entry);
      if (!NodeFS.existsSync(target)) continue;
      const link = NodePath.join(shadowZcode, entry);
      const state = yield* readLinkState(link);
      if (state.kind === "real") {
        return yield* new ZcodeShadowHomeEntryConflictError({ entry, path: link });
      }
      if (state.kind === "symlink" && state.target === target) continue;
      if (state.kind === "symlink") {
        yield* Effect.tryPromise({
          try: () => NodeFS.promises.rm(link, { recursive: true, force: true }),
          catch: fsError("remove", link),
        });
      }
      yield* makeDirectory(NodePath.dirname(link));
      yield* Effect.tryPromise({
        try: () => NodeFS.promises.symlink(target, link, "dir"),
        catch: fsError("symlink", link),
      });
    }

    // Shell stubs: flip HOME back to the real home in every shell the
    // app-server spawns (snapshot capture and tool shells alike). zsh reads
    // .zshenv unconditionally, so later startup files resolve to the real home
    // on their own; bash login shells get .bash_profile.
    for (const [stubName, contents] of [
      [".zshenv", realHomeStub(realHomeDir)],
      [".bash_profile", realHomeStub(realHomeDir)],
    ] as const) {
      const stubPath = NodePath.join(input.shadowHomePath, stubName);
      if (NodeFS.existsSync(stubPath)) continue;
      yield* Effect.tryPromise({
        try: () => NodeFS.promises.writeFile(stubPath, contents, { flag: "wx" }),
        catch: fsError("writeFile", stubPath),
      });
    }
  },
);

/**
 * The `ZCODE_*_PROVIDER_CONFIG_FILE` env pair pointing at the materialized
 * provider-config files inside the shadow home — empty until both files
 * exist, and always both-or-neither (the CLI rejects a lone value).
 */
export function zcodeProviderConfigEnvironment(shadowHomePath: string): NodeJS.ProcessEnv {
  const providerConfigDirPath = NodePath.join(shadowHomePath, ".zcode", ...PROVIDER_CONFIG_DIR);
  const builtinStorePath = NodePath.join(providerConfigDirPath, BUILTIN_STORE_FILENAME);
  const personalConfigPath = NodePath.join(providerConfigDirPath, PERSONAL_CONFIG_FILENAME);
  if (!NodeFS.existsSync(builtinStorePath) || !NodeFS.existsSync(personalConfigPath)) return {};
  return {
    [ZCODE_BUILTIN_PROVIDER_CONFIG_FILE_ENV]: builtinStorePath,
    [ZCODE_PERSONAL_PROVIDER_CONFIG_FILE_ENV]: personalConfigPath,
  };
}

/**
 * The environment for every T3-spawned zcode process (app-server, probes,
 * text-generation helpers): `HOME` (and Windows `USERPROFILE`) pointed at the
 * shadow directory, which is where zcode resolves its entire store from, plus
 * the provider-config env pair once its files are materialized.
 */
export function zcodeShadowHomeEnvironment(
  shadowHomePath: string,
  base: NodeJS.ProcessEnv = {},
): NodeJS.ProcessEnv {
  return {
    ...base,
    HOME: shadowHomePath,
    USERPROFILE: shadowHomePath,
    ...zcodeProviderConfigEnvironment(shadowHomePath),
  };
}
