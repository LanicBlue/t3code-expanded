/**
 * IM Bridge persona injection — the T3 side of the bundled template
 * marketplace. A member's `persona` (Settings → IM 成员, seeded by the
 * marketplace drawer) rides the message's `context.freshContext`: the
 * provider command reactor prepends that field to the turn text only when
 * the turn starts a provider thread with no conversation history. A resumed
 * thread already carries the persona from its first turn, so it is never
 * re-injected (previously every session re-bootstrap re-sent it, stacking a
 * copy into the provider transcript on every mission round).
 *
 * This module understands only T3 member/thread lifecycle. It does not
 * parse or adjudicate IM stations, outcomes, revisions, or parent/child
 * Missions; those semantics stay in IM and its external bridge. It stays
 * format-blind about freshContext's other content: the bridge's stable
 * mission context (if any) is merged after the persona, order preserved.
 *
 * Deliberately server-side: the bridge (an external process over the
 * orchestration HTTP API) stays persona-agnostic — it keeps delivering bare
 * duty briefs, and this fork feature recognizes its threads by the
 * deterministic id shape `im-<memberId>-ms_<hex>` (mission ids are `ms_` plus
 * hex, so `-ms_` cannot appear inside a member id; suffixed fallback ids such
 * as `im-<member>-<mission>-3` keep the same infix).
 *
 * @module imBridgePersona
 */
import * as Effect from "effect/Effect";
import type { OrchestrationCommand, ServerSettingsError, ThreadId } from "@t3tools/contracts";
import { imBridgeMemberIdOfThreadId } from "@t3tools/contracts/settings";
import type { ServerSettings } from "@t3tools/contracts/settings";

import type { OrchestrationDispatchError } from "./orchestration/Errors.ts";
import type { OrchestrationEngineShape } from "./orchestration/Services/OrchestrationEngine.ts";

const PERSONA_HEADER =
  "Member persona (T3 settings); it supplements, never overrides, the duty rules below.";

/**
 * Pure transform: merge the member's persona into the turn.start message's
 * freshContext (persona first, any sender-provided fresh context after).
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
  const senderContext = command.message.context?.freshContext?.trim() ?? "";
  const freshContext = [`${PERSONA_HEADER}\n\n${persona}`, senderContext]
    .filter((part) => part.length > 0)
    .join("\n\n");
  return {
    ...command,
    message: {
      ...command.message,
      context: {
        ...(command.message.context ?? { version: 1 as const, records: [] }),
        freshContext,
      },
    },
  };
}

/**
 * Wrap an engine dispatch with persona-in-freshContext injection. Pure in the
 * command, so retries merge from the original command, never cumulatively.
 * A settings read failure fails closed so the bridge can retry instead of
 * starting a session without the persona.
 */
export function dispatchWithImBridgePersona(
  engine: OrchestrationEngineShape,
  getSettings: Effect.Effect<ServerSettings, ServerSettingsError>,
): (
  command: OrchestrationCommand,
  options?: Parameters<OrchestrationEngineShape["dispatch"]>[1],
) => Effect.Effect<{ sequence: number }, OrchestrationDispatchError> {
  return (command, options) =>
    command.type !== "thread.turn.start" || imBridgeMemberIdOfThreadId(command.threadId) === null
      ? engine.dispatch(command, options)
      : Effect.flatMap(Effect.orDie(getSettings), (settings) =>
          engine.dispatch(injectImBridgePersona(command, settings), options),
        );
}
