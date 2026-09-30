/**
 * ZcodeSessionMcp — mirror the desktop home's user MCP servers into every
 * fresh `session/create`.
 *
 * The zcode protocol accepts `params.mcpServers` on `session/create` as the
 * session-scoped user configuration layer; it is merged ON TOP of plugin MCP
 * servers and built-ins, so injecting it never removes the plugin fleet. T3's
 * shadow home keeps `cli/config.json` only as a materialize-time copy, which
 * goes stale while the shared app-server lives on — reading the REAL home at
 * session-create time sidesteps that staleness and needs no app-server
 * restart, so running sessions are never interrupted.
 *
 * Pure-mirror semantics (user decision): the desktop config is the single
 * source of truth. An entry marked `enabled: false` is NOT injected — the
 * protocol shape has no `enabled` field and the receiving side defaults
 * missing entries to enabled, so forwarding a disabled entry would silently
 * ENABLE it. Not injecting it is what "mirrored as disabled" means here.
 *
 * The emitted shape must stay strictly aligned with zcode's
 * `zcodeProtocolMcpServerSchema` (a strict union — unknown fields are
 * rejected and would fail the whole `session/create`): stdio entries need
 * `command`/`args`/`env`, http/sse entries need `type`/`url`/`headers`.
 *
 * @module provider/zcode/ZcodeSessionMcp
 */
// @effect-diagnostics nodeBuiltinImport:off
// @effect-diagnostics preferSchemaOverJson:off
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as Effect from "effect/Effect";

export type ZcodeSessionMcpEntry = {
  readonly name: string;
  readonly value: string;
};

export type ZcodeSessionMcpServer =
  | {
      readonly name: string;
      readonly command: string;
      readonly args: ReadonlyArray<string>;
      readonly env: ReadonlyArray<ZcodeSessionMcpEntry>;
      readonly isolation?: "session" | "workspace";
      readonly protocolVersion?: "legacy" | "auto" | "2026-07-28";
      readonly timeoutMs?: number;
    }
  | {
      readonly name: string;
      readonly type: "http" | "sse";
      readonly url: string;
      readonly headers: ReadonlyArray<ZcodeSessionMcpEntry>;
      readonly oauth?: unknown;
      readonly isolation?: "session" | "workspace";
      readonly protocolVersion?: "legacy" | "auto" | "2026-07-28";
      readonly timeoutMs?: number;
    };

const ISOLATION_VALUES = ["session", "workspace"] as const;
const PROTOCOL_VERSION_VALUES = ["legacy", "auto", "2026-07-28"] as const;
const OAUTH_TYPES = ["client_credentials", "authorization_code"] as const;

type UnknownRecord = Record<string, unknown>;

const isRecord = (value: unknown): value is UnknownRecord =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const pickEnumValue = <T extends readonly string[]>(
  values: T,
  value: unknown,
): T[number] | undefined =>
  typeof value === "string" && (values as ReadonlyArray<string>).includes(value)
    ? (value as T[number])
    : undefined;

/** Schema: positive safe integer. Anything else is dropped, not fatal. */
const pickTimeoutMs = (value: unknown): number | undefined =>
  typeof value === "number" && Number.isSafeInteger(value) && value > 0 ? value : undefined;

const toEntries = (input: unknown): ReadonlyArray<ZcodeSessionMcpEntry> =>
  isRecord(input)
    ? Object.entries(input)
        .filter((entry): entry is [string, string] => typeof entry[1] === "string")
        .map(([name, value]) => ({ name, value }))
    : [];

/**
 * Convert the parsed `mcp.servers` record from the desktop `config.json`
 * (keyed by server name) into the protocol array shape. Entries that are
 * disabled, unrecognizable, or missing required fields are skipped — a
 * malformed entry must not fail the whole session create.
 */
export const convertDesktopMcpServers = (
  servers: unknown,
): ReadonlyArray<ZcodeSessionMcpServer> | undefined => {
  if (!isRecord(servers)) return undefined;
  const converted: Array<ZcodeSessionMcpServer> = [];
  for (const [name, raw] of Object.entries(servers)) {
    if (name.trim().length === 0 || !isRecord(raw)) continue;
    // Pure mirror: a desktop-disabled entry is not forwarded at all. The
    // protocol shape cannot carry `enabled: false` (unknown field → schema
    // reject), and omitting the field would enable it on the zcode side.
    if (raw.enabled === false) continue;

    const timeoutMs = pickTimeoutMs(raw.timeoutMs);
    const isolation = pickEnumValue(ISOLATION_VALUES, raw.isolation);
    const protocolVersion = pickEnumValue(PROTOCOL_VERSION_VALUES, raw.protocolVersion);

    if (typeof raw.command === "string" && raw.command.trim().length > 0) {
      converted.push({
        name,
        command: raw.command,
        args: Array.isArray(raw.args)
          ? raw.args.filter((arg): arg is string => typeof arg === "string")
          : [],
        env: toEntries(raw.env),
        ...(isolation !== undefined ? { isolation } : {}),
        ...(protocolVersion !== undefined ? { protocolVersion } : {}),
        ...(timeoutMs !== undefined ? { timeoutMs } : {}),
      });
      continue;
    }

    if (
      (raw.type === "http" || raw.type === "sse") &&
      typeof raw.url === "string" &&
      raw.url.trim().length > 0
    ) {
      // `http_headers` is the legacy config key the desktop still accepts;
      // mirror it when the modern `headers` key is absent.
      const headers = isRecord(raw.headers) ? toEntries(raw.headers) : toEntries(raw.http_headers);
      // A malformed oauth object is dropped rather than forwarded: the strict
      // protocol schema would reject it and take the whole create down.
      const oauth =
        isRecord(raw.oauth) && OAUTH_TYPES.includes(raw.oauth.type as never)
          ? raw.oauth
          : undefined;
      converted.push({
        name,
        type: raw.type,
        url: raw.url,
        headers,
        ...(oauth !== undefined ? { oauth } : {}),
        ...(isolation !== undefined ? { isolation } : {}),
        ...(protocolVersion !== undefined ? { protocolVersion } : {}),
        ...(timeoutMs !== undefined ? { timeoutMs } : {}),
      });
    }
  }
  return converted.length > 0 ? converted : undefined;
};

/**
 * Read the real (desktop) home's `cli/config.json` and return the mirrored
 * MCP server list for `session/create`, or `undefined` when there is nothing
 * to inject (no file, unreadable JSON, no enabled servers). `undefined` means
 * "send no field" — zcode then falls back to the shadow home's config, which
 * is the pre-injection behavior.
 */
export const resolveSessionMcpServers = (
  realHomeDir: string = NodeOS.homedir(),
): Effect.Effect<ReadonlyArray<ZcodeSessionMcpServer> | undefined, never> =>
  Effect.tryPromise({
    // Read + parse + convert in one try block: any failure (missing file,
    // unreadable JSON, wrong shape) funnels into the catch below.
    try: async () => {
      const text = await NodeFS.promises.readFile(
        NodePath.join(realHomeDir, ".zcode", "cli", "config.json"),
        "utf8",
      );
      const parsed: unknown = JSON.parse(text);
      const servers = isRecord(parsed) && isRecord(parsed.mcp) ? parsed.mcp.servers : undefined;
      return convertDesktopMcpServers(servers);
    },
    catch: () => "desktop-config-unreadable" as const,
  }).pipe(Effect.orElseSucceed(() => undefined));
