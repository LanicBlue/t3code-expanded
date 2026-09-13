import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";

import {
  materializeZcodeShadowHome,
  resolveZcodeShadowHomePath,
  zcodeShadowHomeEnvironment,
  ZcodeShadowHomeEntryConflictError,
  ZcodeShadowHomePathConflictError,
} from "./ZcodeShadowHome.ts";

const makeTempDir = Effect.fn("ZcodeShadowHome.test.makeTempDir")(function* (prefix: string) {
  const fileSystem = yield* FileSystem.FileSystem;
  return yield* fileSystem.makeTempDirectoryScoped({ prefix });
});

const writeTextFile = (filePath: string, contents: string) =>
  Effect.tryPromise({
    try: async () => {
      await NodeFS.promises.mkdir(NodePath.dirname(filePath), { recursive: true });
      await NodeFS.promises.writeFile(filePath, contents);
    },
    catch: (cause) => new Error(`writeTextFile ${filePath} failed: ${String(cause)}`),
  });

const fixtureHome = Effect.fn("ZcodeShadowHome.test.fixtureHome")(function* () {
  const realHome = yield* makeTempDir("t3code-zcode-real-");
  const zcode = NodePath.join(realHome, ".zcode");
  yield* writeTextFile(NodePath.join(zcode, "cli", "config.json"), '{"model":{"main":"GLM-5.3"}}');
  yield* writeTextFile(NodePath.join(zcode, "v2", "config.json"), '{"desktop":true}');
  yield* writeTextFile(NodePath.join(zcode, "cli", "agents", "coder.md"), "# coder");
  yield* writeTextFile(NodePath.join(zcode, "skills", "probe", "SKILL.md"), "# probe");
  return { realHome, zcode };
});

describe("resolveZcodeShadowHomePath (pure)", () => {
  it("empty setting keeps the shared home (undefined)", () => {
    expect(resolveZcodeShadowHomePath({ shadowHomePath: "" }, "/Users/tester")).toBeUndefined();
    expect(resolveZcodeShadowHomePath({ shadowHomePath: "   " }, "/Users/tester")).toBeUndefined();
  });

  it("resolves an absolute shadow path", () => {
    expect(resolveZcodeShadowHomePath({ shadowHomePath: "/srv/zcode-home" }, "/Users/tester")).toBe(
      "/srv/zcode-home",
    );
  });

  it("expands ~ against the process home to an absolute path", () => {
    const tilde = resolveZcodeShadowHomePath(
      { shadowHomePath: "~/.t3/zcode-home" },
      "/Users/tester",
    );
    expect(typeof tilde).toBe("string");
    expect(NodePath.isAbsolute(tilde as string)).toBe(true);
  });

  it("rejects the shared .zcode store and anything inside it, but allows siblings under the home", () => {
    const store = resolveZcodeShadowHomePath(
      { shadowHomePath: "/Users/tester/.zcode" },
      "/Users/tester",
    );
    expect(store).toBeInstanceOf(ZcodeShadowHomePathConflictError);
    const inside = resolveZcodeShadowHomePath(
      { shadowHomePath: "/Users/tester/.zcode/shadow" },
      "/Users/tester",
    );
    expect(inside).toBeInstanceOf(ZcodeShadowHomePathConflictError);
    const sibling = resolveZcodeShadowHomePath(
      { shadowHomePath: "/Users/tester/.t3/zcode-home" },
      "/Users/tester",
    );
    expect(sibling).toBe("/Users/tester/.t3/zcode-home");
  });
});

describe("zcodeShadowHomeEnvironment (pure)", () => {
  it("overrides HOME/USERPROFILE while preserving the base env", () => {
    const env = zcodeShadowHomeEnvironment("/srv/zcode-shadow", {
      PATH: "/usr/bin",
      HOME: "/Users/tester",
    });
    expect(env).toEqual({
      PATH: "/usr/bin",
      HOME: "/srv/zcode-shadow",
      USERPROFILE: "/srv/zcode-shadow",
    });
  });
});

it.layer(NodeServices.layer)("ZcodeShadowHome", (it) => {
  describe("materializeZcodeShadowHome", () => {
    it.effect("copies config, links static entries, writes HOME-flip stubs", () =>
      Effect.gen(function* () {
        const { realHome } = yield* fixtureHome();
        const shadow = yield* makeTempDir("t3code-zcode-shadow-");
        yield* materializeZcodeShadowHome({ shadowHomePath: shadow, realHomeDir: realHome });

        expect(
          NodeFS.readFileSync(NodePath.join(shadow, ".zcode", "cli", "config.json"), "utf8"),
        ).toBe('{"model":{"main":"GLM-5.3"}}');
        expect(
          NodeFS.readFileSync(NodePath.join(shadow, ".zcode", "v2", "config.json"), "utf8"),
        ).toBe('{"desktop":true}');
        // Static entries resolve back into the real home.
        expect(
          NodeFS.readFileSync(NodePath.join(shadow, ".zcode", "cli", "agents", "coder.md"), "utf8"),
        ).toBe("# coder");
        expect(
          NodeFS.readFileSync(
            NodePath.join(shadow, ".zcode", "skills", "probe", "SKILL.md"),
            "utf8",
          ),
        ).toBe("# probe");
        // Volatile store dirs are NOT pre-created links — they must stay local.
        expect(NodeFS.existsSync(NodePath.join(shadow, ".zcode", "cli", "db"))).toBe(false);
        const zshenv = NodeFS.readFileSync(NodePath.join(shadow, ".zshenv"), "utf8");
        expect(zshenv).toContain(`export HOME='${realHome}'`);
        expect(NodeFS.readFileSync(NodePath.join(shadow, ".bash_profile"), "utf8")).toContain(
          realHome,
        );
      }),
    );

    it.effect("is idempotent and refreshes config copies", () =>
      Effect.gen(function* () {
        const { realHome, zcode } = yield* fixtureHome();
        const shadow = yield* makeTempDir("t3code-zcode-shadow-");
        yield* materializeZcodeShadowHome({ shadowHomePath: shadow, realHomeDir: realHome });
        yield* writeTextFile(
          NodePath.join(zcode, "cli", "config.json"),
          '{"model":{"main":"GLM-5.4"}}',
        );

        yield* materializeZcodeShadowHome({ shadowHomePath: shadow, realHomeDir: realHome });

        expect(
          NodeFS.readFileSync(NodePath.join(shadow, ".zcode", "cli", "config.json"), "utf8"),
        ).toBe('{"model":{"main":"GLM-5.4"}}');
        // The app-server may have created its local store between runs —
        // materialization must not touch it.
        NodeFS.mkdirSync(NodePath.join(shadow, ".zcode", "cli", "db"), { recursive: true });
        yield* materializeZcodeShadowHome({ shadowHomePath: shadow, realHomeDir: realHome });
        expect(NodeFS.statSync(NodePath.join(shadow, ".zcode", "cli", "db")).isDirectory()).toBe(
          true,
        );
      }),
    );

    it.effect("replaces a stale symlink but refuses a real file in a linked slot", () =>
      Effect.gen(function* () {
        const realHome = yield* makeTempDir("t3code-zcode-real-");
        const elsewhere = yield* makeTempDir("t3code-zcode-elsewhere-");
        const shadow = yield* makeTempDir("t3code-zcode-shadow-");
        const zcode = NodePath.join(realHome, ".zcode");
        yield* writeTextFile(NodePath.join(zcode, "cli", "agents", "coder.md"), "# coder");

        // Stale link → replaced with the correct target.
        NodeFS.mkdirSync(NodePath.join(shadow, ".zcode", "cli"), { recursive: true });
        NodeFS.symlinkSync(elsewhere, NodePath.join(shadow, ".zcode", "cli", "agents"), "dir");
        yield* materializeZcodeShadowHome({ shadowHomePath: shadow, realHomeDir: realHome });
        expect(
          NodeFS.readFileSync(NodePath.join(shadow, ".zcode", "cli", "agents", "coder.md"), "utf8"),
        ).toBe("# coder");

        // A real file in a linked slot is an operator conflict, not silently
        // clobbered.
        NodeFS.rmSync(NodePath.join(shadow, ".zcode", "cli", "agents"));
        NodeFS.mkdirSync(NodePath.join(shadow, ".zcode", "cli", "agents"));
        const failure = yield* Effect.flip(
          materializeZcodeShadowHome({ shadowHomePath: shadow, realHomeDir: realHome }),
        );
        expect(failure).toBeInstanceOf(ZcodeShadowHomeEntryConflictError);
      }),
    );

    it.effect("skips entries missing from the real home", () =>
      Effect.gen(function* () {
        const realHome = yield* makeTempDir("t3code-zcode-real-"); // bare home, no .zcode
        const shadow = yield* makeTempDir("t3code-zcode-shadow-");
        yield* materializeZcodeShadowHome({ shadowHomePath: shadow, realHomeDir: realHome });
        expect(NodeFS.existsSync(NodePath.join(shadow, ".zcode", "cli"))).toBe(true);
        expect(NodeFS.existsSync(NodePath.join(shadow, ".zcode", "skills"))).toBe(false);
      }),
    );
  });
});
