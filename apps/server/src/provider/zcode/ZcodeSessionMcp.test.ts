// @effect-diagnostics nodeBuiltinImport:off
// @effect-diagnostics preferSchemaOverJson:off
import { describe, expect, it } from "@effect/vitest";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as Effect from "effect/Effect";

import { convertDesktopMcpServers, resolveSessionMcpServers } from "./ZcodeSessionMcp.ts";

const httpServer = {
  type: "http",
  url: "https://mcp.example.com/mcp",
  headers: { Authorization: "Bearer token" },
  timeoutMs: 86400000,
};

const stdioServer = {
  type: "stdio",
  command: "/opt/homebrew/bin/node",
  args: ["server.js", "--flag"],
  env: { NODE_ENV: "production", badValue: 42 },
  cwd: "/tmp/server",
  timeoutMs: 60000,
};

describe("convertDesktopMcpServers", () => {
  it("converts an http entry with headers and timeoutMs", () => {
    expect(convertDesktopMcpServers({ "ECS-MCP": httpServer })).toEqual([
      {
        name: "ECS-MCP",
        type: "http",
        url: "https://mcp.example.com/mcp",
        headers: [{ name: "Authorization", value: "Bearer token" }],
        timeoutMs: 86400000,
      },
    ]);
  });

  it("converts a stdio entry, dropping non-string env values and unknown fields", () => {
    expect(convertDesktopMcpServers({ local: stdioServer })).toEqual([
      {
        name: "local",
        command: "/opt/homebrew/bin/node",
        args: ["server.js", "--flag"],
        env: [{ name: "NODE_ENV", value: "production" }],
        timeoutMs: 60000,
      },
    ]);
  });

  it("drops enabled:false entries instead of forwarding them (pure mirror)", () => {
    const servers = {
      "ECS-MCP": { ...httpServer, enabled: false },
      healthy: httpServer,
    };
    const converted = convertDesktopMcpServers(servers);
    expect(converted).toHaveLength(1);
    expect(converted?.[0]?.name).toBe("healthy");
  });

  it("falls back to the legacy http_headers key when headers is absent", () => {
    const converted = convertDesktopMcpServers({
      legacy: {
        type: "http",
        url: "https://mcp.example.com/mcp",
        http_headers: { "X-Key": "v" },
      },
    });
    expect(converted?.[0]).toMatchObject({
      name: "legacy",
      headers: [{ name: "X-Key", value: "v" }],
    });
  });

  it("keeps an explicitly empty headers object as empty (no legacy fallback)", () => {
    const converted = convertDesktopMcpServers({
      anon: { type: "http", url: "https://mcp.example.com/mcp", headers: {} },
    });
    expect(converted?.[0]).toMatchObject({ headers: [] });
  });

  it("strips invalid timeoutMs, isolation, and protocolVersion instead of failing", () => {
    const converted = convertDesktopMcpServers({
      weird: {
        ...httpServer,
        timeoutMs: 0,
        isolation: "bogus",
        protocolVersion: "1999-01-01",
      },
    });
    expect(converted?.[0]).toEqual({
      name: "weird",
      type: "http",
      url: "https://mcp.example.com/mcp",
      headers: [{ name: "Authorization", value: "Bearer token" }],
    });
  });

  it("keeps valid isolation and protocolVersion passthrough fields", () => {
    const converted = convertDesktopMcpServers({
      tuned: {
        ...httpServer,
        isolation: "workspace",
        protocolVersion: "auto",
      },
    });
    expect(converted?.[0]).toMatchObject({ isolation: "workspace", protocolVersion: "auto" });
  });

  it("forwards a well-formed oauth object but drops a malformed one", () => {
    const good = convertDesktopMcpServers({
      oauthed: {
        type: "http",
        url: "https://mcp.example.com/mcp",
        headers: {},
        oauth: { type: "client_credentials", clientId: "a", clientSecret: "b" },
      },
    });
    expect(good?.[0]).toMatchObject({
      oauth: { type: "client_credentials", clientId: "a", clientSecret: "b" },
    });

    const bad = convertDesktopMcpServers({
      oauthed: {
        type: "http",
        url: "https://mcp.example.com/mcp",
        headers: {},
        oauth: { type: "made_up_flow" },
      },
    });
    expect("oauth" in (bad?.[0] ?? {})).toBe(false);
  });

  it("skips entries with no recognizable transport and returns undefined when nothing survives", () => {
    expect(
      convertDesktopMcpServers({ broken: { type: "carrier-pigeon" }, no: {} }),
    ).toBeUndefined();
    expect(
      convertDesktopMcpServers({ disabled: { ...httpServer, enabled: false } }),
    ).toBeUndefined();
    expect(convertDesktopMcpServers({})).toBeUndefined();
    expect(convertDesktopMcpServers("nope")).toBeUndefined();
  });
});

describe("resolveSessionMcpServers", () => {
  const writeConfig = (homeDir: string, contents: string) => {
    const dir = NodePath.join(homeDir, ".zcode", "cli");
    NodeFS.mkdirSync(dir, { recursive: true });
    NodeFS.writeFileSync(NodePath.join(dir, "config.json"), contents);
  };

  it.effect("reads and mirrors the real home config", () =>
    Effect.gen(function* () {
      const home = NodeFS.mkdtempSync(`${NodeOS.tmpdir()}/t3-zcode-mcp-home-`);
      writeConfig(
        home,
        JSON.stringify({
          mcp: { servers: { "ECS-MCP": { ...httpServer, enabled: false }, keep: httpServer } },
        }),
      );
      const mirrored = yield* resolveSessionMcpServers(home);
      expect(mirrored).toHaveLength(1);
      expect(mirrored?.[0]?.name).toBe("keep");
    }),
  );

  it.effect("returns undefined for a missing config file", () =>
    Effect.gen(function* () {
      const home = NodeFS.mkdtempSync(`${NodeOS.tmpdir()}/t3-zcode-mcp-home-`);
      expect(yield* resolveSessionMcpServers(home)).toBeUndefined();
    }),
  );

  it.effect("returns undefined for unreadable JSON without failing", () =>
    Effect.gen(function* () {
      const home = NodeFS.mkdtempSync(`${NodeOS.tmpdir()}/t3-zcode-mcp-home-`);
      writeConfig(home, "{ not json");
      expect(yield* resolveSessionMcpServers(home)).toBeUndefined();
    }),
  );

  it.effect("returns undefined when the config has no mcp.servers section", () =>
    Effect.gen(function* () {
      const home = NodeFS.mkdtempSync(`${NodeOS.tmpdir()}/t3-zcode-mcp-home-`);
      writeConfig(home, JSON.stringify({ model: { main: "x" } }));
      expect(yield* resolveSessionMcpServers(home)).toBeUndefined();
    }),
  );
});
