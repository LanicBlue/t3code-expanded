// @effect-diagnostics nodeBuiltinImport:off
import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Schema from "effect/Schema";

const decodeUnknownJson = Schema.decodeUnknownSync(Schema.fromJsonString(Schema.Unknown));

import {
  buildZcodePersonalProviderRules,
  materializeZcodeShadowHome,
  resolveZcodeShadowHomePath,
  zcodeProviderConfigEnvironment,
  zcodeShadowHomeEnvironment,
  ZCODE_BUILTIN_PROVIDER_CONFIG_FILE_ENV,
  ZCODE_PERSONAL_PROVIDER_CONFIG_FILE_ENV,
  ZcodeShadowHomeEntryConflictError,
  ZcodeShadowHomePathConflictError,
} from "./ZcodeShadowHome.ts";

const makeTempDir = Effect.fn("ZcodeShadowHome.test.makeTempDir")(function* (prefix: string) {
  const fileSystem = yield* FileSystem.FileSystem;
  return yield* fileSystem.makeTempDirectoryScoped({ prefix });
});

class WriteTextFileError extends Schema.TaggedError<WriteTextFileError>()("WriteTextFileError", {
  filePath: Schema.String,
  cause: Schema.Defect(),
}) {}

const writeTextFile = (filePath: string, contents: string) =>
  Effect.tryPromise({
    try: async () => {
      await NodeFS.promises.mkdir(NodePath.dirname(filePath), { recursive: true });
      await NodeFS.promises.writeFile(filePath, contents);
    },
    catch: (cause) => new WriteTextFileError({ filePath, cause }),
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

  it("adds the provider-config env pair once both files are materialized", () => {
    const env = zcodeShadowHomeEnvironment("/srv/zcode-shadow", {});
    expect(ZCODE_BUILTIN_PROVIDER_CONFIG_FILE_ENV in env).toBe(false);
    expect(zcodeProviderConfigEnvironment("/srv/zcode-shadow")).toEqual({});
  });
});

describe("buildZcodePersonalProviderRules (pure)", () => {
  const legacyMap = {
    "builtin:bigmodel-coding-plan": {
      name: "BigModel - Coding Plan",
      kind: "anthropic",
      options: { apiKey: "sk-test", baseURL: "https://open.bigmodel.cn/api/anthropic" },
      enabled: true,
      models: { "GLM-5.3-Flash": {}, "GLM-5.3": {} },
    },
    "builtin:zai": {
      name: "Z.ai - API Key",
      kind: "anthropic",
      options: { apiKey: "sk-zai", baseURL: "https://api.z.ai/api" },
      enabled: false,
      models: { "GLM-5.3": {} },
    },
    "builtin:oauth-entry": { name: "OAuth", kind: "anthropic", options: {}, models: {} },
    "custom-plain": {
      kind: "openai",
      options: { apiKey: "sk-openai", baseURL: "https://api.example.com/v1" },
      models: { "gpt-x": {} },
    },
  };

  it("migrates API-key entries, strips the builtin: prefix, skips credential-less ones", () => {
    const rules = buildZcodePersonalProviderRules(legacyMap);
    expect(rules.map((rule) => rule.providerId)).toEqual([
      "bigmodel-coding-plan",
      "zai",
      "custom-plain",
    ]);
    expect(rules[0]).toEqual({
      providerId: "bigmodel-coding-plan",
      providerName: "BigModel - Coding Plan",
      enabled: true,
      config: {
        group: "standard-personal",
        access: { type: "api-key", apiKey: "sk-test" },
        api: {
          type: "anthropic-messages",
          baseUrl: "https://open.bigmodel.cn/api/anthropic",
        },
        personalModelIds: ["GLM-5.3-Flash", "GLM-5.3"],
      },
    });
    expect(rules[1]!.enabled).toBe(false);
    expect(rules[2]!.config.api.type).toBe("openai-chat-completions");
  });

  it("returns empty for non-object input", () => {
    expect(buildZcodePersonalProviderRules(undefined)).toEqual([]);
    expect(buildZcodePersonalProviderRules("nope")).toEqual([]);
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

    it.effect("materializes the provider-config pair from legacy providers + bundle store", () =>
      Effect.gen(function* () {
        const realHome = yield* makeTempDir("t3code-zcode-real-");
        const shadow = yield* makeTempDir("t3code-zcode-shadow-");
        const bundle = yield* makeTempDir("t3code-zcode-bundle-");
        // App-bundle layout: <bundle>/glm/zcode.cjs + <bundle>/config/provider/zcode-builtin.json
        const binaryPath = NodePath.join(bundle, "glm", "zcode.cjs");
        yield* writeTextFile(
          NodePath.join(realHome, ".zcode", "v2", "config.json"),
          '{"provider":{"builtin:bigmodel-coding-plan":{"name":"BigModel - Coding Plan","kind":"anthropic","options":{"apiKey":"sk-test","baseURL":"https://open.bigmodel.cn/api/anthropic"},"models":{"GLM-5.3":{}}}}}',
        );
        yield* writeTextFile(
          NodePath.join(bundle, "config", "provider", "zcode-builtin.json"),
          '{"revision":28}',
        );

        yield* materializeZcodeShadowHome({
          shadowHomePath: shadow,
          realHomeDir: realHome,
          binaryPath,
        });

        const dir = NodePath.join(shadow, ".zcode", "v2", "t3-provider-config");
        expect(NodeFS.readFileSync(NodePath.join(dir, "zcode-builtin.json"), "utf8")).toBe(
          '{"revision":28}',
        );
        const personal = decodeUnknownJson(
          NodeFS.readFileSync(NodePath.join(dir, "provider-personal.json"), "utf8"),
        ) as {
          schemaVersion: number;
          config: { providerConfigRules: { providerRules: ReadonlyArray<{ providerId: string }> } };
        };
        expect(personal.schemaVersion).toBe(1);
        expect(
          personal.config.providerConfigRules.providerRules.map((rule) => rule.providerId),
        ).toEqual(["bigmodel-coding-plan"]);
        const env = zcodeShadowHomeEnvironment(shadow, {});
        expect(env[ZCODE_BUILTIN_PROVIDER_CONFIG_FILE_ENV]).toBe(
          NodePath.join(dir, "zcode-builtin.json"),
        );
        expect(env[ZCODE_PERSONAL_PROVIDER_CONFIG_FILE_ENV]).toBe(
          NodePath.join(dir, "provider-personal.json"),
        );
      }),
    );

    it.effect("falls back to the newest runtime store when no bundle store exists", () =>
      Effect.gen(function* () {
        const realHome = yield* makeTempDir("t3code-zcode-real-");
        const shadow = yield* makeTempDir("t3code-zcode-shadow-");
        yield* writeTextFile(
          NodePath.join(realHome, ".zcode", "v2", "config.json"),
          '{"provider":{"builtin:p":{"kind":"openai","options":{"apiKey":"k","baseURL":"https://x.example"},"models":{"m":{}}}}}',
        );
        yield* writeTextFile(
          NodePath.join(
            realHome,
            ".zcode",
            "v2",
            "runtime",
            "provider",
            "darwin-arm64",
            "3.12.3",
            "endpoint-abc",
            "zcode-builtin.json",
          ),
          '{"revision":28}',
        );

        yield* materializeZcodeShadowHome({ shadowHomePath: shadow, realHomeDir: realHome });

        expect(
          NodeFS.readFileSync(
            NodePath.join(shadow, ".zcode", "v2", "t3-provider-config", "zcode-builtin.json"),
            "utf8",
          ),
        ).toBe('{"revision":28}');
      }),
    );

    it.effect("drops a stale provider-config pair when it is no longer derivable", () =>
      Effect.gen(function* () {
        const realHome = yield* makeTempDir("t3code-zcode-real-");
        const shadow = yield* makeTempDir("t3code-zcode-shadow-");
        const bundle = yield* makeTempDir("t3code-zcode-bundle-");
        const binaryPath = NodePath.join(bundle, "glm", "zcode.cjs");
        const legacyConfig = NodePath.join(realHome, ".zcode", "v2", "config.json");
        yield* writeTextFile(
          legacyConfig,
          '{"provider":{"builtin:p":{"kind":"openai","options":{"apiKey":"k","baseURL":"https://x.example"},"models":{"m":{}}}}}',
        );
        yield* writeTextFile(
          NodePath.join(bundle, "config", "provider", "zcode-builtin.json"),
          '{"revision":28}',
        );
        yield* materializeZcodeShadowHome({
          shadowHomePath: shadow,
          realHomeDir: realHome,
          binaryPath,
        });
        expect(NodeFS.existsSync(NodePath.join(shadow, ".zcode", "v2", "t3-provider-config"))).toBe(
          true,
        );

        // The provider map disappears → the stale pair must not survive.
        yield* writeTextFile(legacyConfig, "{}");
        yield* materializeZcodeShadowHome({
          shadowHomePath: shadow,
          realHomeDir: realHome,
          binaryPath,
        });
        expect(NodeFS.existsSync(NodePath.join(shadow, ".zcode", "v2", "t3-provider-config"))).toBe(
          false,
        );
        expect(zcodeProviderConfigEnvironment(shadow)).toEqual({});
      }),
    );
  });
});
