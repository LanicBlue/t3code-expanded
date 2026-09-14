/**
 * IM Bridge persona injection — the T3 side of the bundled template
 * marketplace. A member's `persona` (Settings → IM 成员, seeded by the
 * marketplace drawer) is prepended to the first turn of each newly started
 * member mission session; the session transcript carries it from there,
 * so later turns go through raw. A stopped/errored session gets the persona
 * again on its replacement turn even when the durable T3 thread survives.
 *
 * This module understands only T3 member/thread/session lifecycle. It does
 * not parse or adjudicate IM stations, outcomes, revisions, or parent/child
 * Missions; those semantics stay in IM and its external bridge.
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
import type {
  OrchestrationCommand,
  OrchestrationShellSnapshot,
  ServerSettingsError,
  ThreadId,
} from "@t3tools/contracts";
import { imBridgeMemberIdOfThreadId } from "@t3tools/contracts/settings";
import type { ServerSettings } from "@t3tools/contracts/settings";

import type { OrchestrationDispatchError } from "./orchestration/Errors.ts";
import type { ProjectionRepositoryError } from "./persistence/Errors.ts";
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
 * Whether a thread's next turn starts a fresh T3 session context. Unknown or
 * never-run threads are fresh; stopped/errored sessions are replaced by the
 * provider reactor and therefore need the bootstrap persona again. Snapshot
 * failure fails open to fresh, which may duplicate persona but cannot omit it.
 */
export function firstTurnForBridgeThread(
  getShellSnapshot: () => Effect.Effect<OrchestrationShellSnapshot, ProjectionRepositoryError>,
): (threadId: ThreadId) => Effect.Effect<boolean> {
  return (threadId) =>
    Effect.orElseSucceed(
      Effect.map(getShellSnapshot(), (snapshot) => {
        const thread = snapshot.threads.find((candidate) => candidate.id === threadId);
        return (
          thread === undefined ||
          thread.latestTurn === null ||
          thread.session == null ||
          thread.session.status === "error" ||
          thread.session.status === "stopped"
        );
      }),
      () => true,
    );
}

/**
 * Wrap an engine dispatch with session-bootstrap persona injection. A bridge
 * turn never goes through raw after a settings read failure: fail closed so
 * the bridge can retry instead of starting a session with the wrong persona.
 */
export function dispatchWithImBridgePersona(
  engine: OrchestrationEngineShape,
  getSettings: Effect.Effect<ServerSettings, ServerSettingsError>,
  isFirstTurn: (threadId: ThreadId) => Effect.Effect<boolean> = () => Effect.succeed(true),
): (
  command: OrchestrationCommand,
  options?: Parameters<OrchestrationEngineShape["dispatch"]>[1],
) => Effect.Effect<{ sequence: number }, OrchestrationDispatchError> {
  return (command, options) =>
    command.type !== "thread.turn.start" || imBridgeMemberIdOfThreadId(command.threadId) === null
      ? engine.dispatch(command, options)
      : Effect.flatMap(Effect.orDie(getSettings), (settings) => {
          const withPersona = injectImBridgePersona(command, settings);
          // Member without a persona: nothing to gate.
          if (withPersona === command) return engine.dispatch(command, options);
          return Effect.flatMap(isFirstTurn(command.threadId), (first) =>
            engine.dispatch(first ? withPersona : command, options),
          );
        });
}
