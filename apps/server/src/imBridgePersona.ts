/**
 * IM Bridge persona injection — the T3 side of the bundled template
 * marketplace. A member's `persona` (Settings → IM 成员, seeded by the
 * marketplace drawer) is prepended to every turn started on that member's
 * mission threads, right where the bridge's duty preamble already lives.
 *
 * Deliberately server-side: the bridge (an external process over the
 * orchestration HTTP API) stays persona-agnostic — it keeps delivering bare
 * duty preambles, and this fork feature recognizes its threads by the
 * deterministic id shape `im-<memberId>-ms_<hex>` (mission ids are `ms_` plus
 * hex, so `-ms_` cannot appear inside a member id; suffixed fallback ids such
 * as `im-<member>-<mission>-3` keep the same infix).
 *
 * @module imBridgePersona
 */
import * as Effect from "effect/Effect";
import type { OrchestrationCommand, ServerSettingsError } from "@t3tools/contracts";
import { imBridgeMemberIdOfThreadId } from "@t3tools/contracts/settings";
import type { ServerSettings } from "@t3tools/contracts/settings";

import type { OrchestrationDispatchError } from "./orchestration/Errors.ts";
import type { OrchestrationEngineShape } from "./orchestration/Services/OrchestrationEngine.ts";

const PERSONA_HEADER = [
  "The persona below is configured for this member in T3's settings.",
  "Stay in character throughout the mission work; it supplements, never",
  "overrides, the duty rules that follow.",
  "",
  "",
].join("\n");

/**
 * Pure transform: prepend the member's persona to a turn.start message.
 * Anything else — other commands, non-bridge threads, members without a
 * persona — passes through untouched.
 */
export function injectImBridgePersona(
  command: OrchestrationCommand,
  settings: ServerSettings,
): OrchestrationCommand {
  if (command.type !== "thread.turn.start") return command;
  const memberId = imBridgeMemberIdOfThreadId(command.threadId);
  if (memberId === null) return command;
  const persona = settings.imBridge.members
    .find((member) => member.id === memberId)
    ?.persona?.trim();
  if (persona === undefined || persona.length === 0) return command;
  return {
    ...command,
    message: { ...command.message, text: `${PERSONA_HEADER}${persona}\n\n${command.message.text}` },
  };
}

/**
 * Wrap an engine dispatch with persona injection. A settings read failure must
 * never block the dispatch — the command goes through raw.
 */
export function dispatchWithImBridgePersona(
  engine: OrchestrationEngineShape,
  getSettings: Effect.Effect<ServerSettings, ServerSettingsError>,
): (
  command: OrchestrationCommand,
  options?: Parameters<OrchestrationEngineShape["dispatch"]>[1],
) => Effect.Effect<{ sequence: number }, OrchestrationDispatchError> {
  return (command, options) =>
    Effect.flatMap(
      Effect.orElseSucceed(getSettings, () => null),
      (settings) =>
        engine.dispatch(
          settings === null ? command : injectImBridgePersona(command, settings),
          options,
        ),
    );
}
