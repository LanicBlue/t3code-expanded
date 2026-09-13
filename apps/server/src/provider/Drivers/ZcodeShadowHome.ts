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
import * as Schema from "effect/Schema";

import { expandHomePath } from "../../pathExpansion.ts";

/** Copied (not linked): auth/model config that must live in the shadow home. */
const COPY_CONFIG_FILES = ["cli/config.json", "v2/config.json"] as const;
/** Symlinked: static user-level content shared with the real home. */
const LINKED_ENTRIES = ["cli/agents", "cli/plugins", "cli/memories", "skills"] as const;

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
 * The environment for every T3-spawned zcode process (app-server, probes,
 * text-generation helpers): `HOME` (and Windows `USERPROFILE`) pointed at the
 * shadow directory, which is where zcode resolves its entire store from.
 */
export function zcodeShadowHomeEnvironment(
  shadowHomePath: string,
  base: NodeJS.ProcessEnv = {},
): NodeJS.ProcessEnv {
  return { ...base, HOME: shadowHomePath, USERPROFILE: shadowHomePath };
}
