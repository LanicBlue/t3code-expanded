/**
 * IM Bridge persona injection — the T3 side of the bundled template
 * marketplace. A member's `persona` (Settings → IM 成员, seeded by the
 * marketplace drawer) is prepended to the FIRST turn started on that
 * member's mission thread; the session transcript carries it from there,
 * so later turns (result submissions, follow-ups) go through raw and the
 * persona never repeats in the context window.
 *
 * "First" is `latestTurn === null` in the projection shell at dispatch
 * time: bridge threads are created bare (`thread.create` carries no
 * message), so a null latest turn means no turn ever committed. A
 * turn-error reopen deletes and recreates the thread, which re-enters
 * the first-turn branch — the new session gets the persona again.
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
 * Whether a thread's next turn.start is its first, from the projection
 * shell: true while the thread is unknown or has no committed turn. A
 * snapshot read failure fails open to "first" — a mission without its
 * persona is worse than one duplicated header.
 */
export function firstTurnForBridgeThread(
  getShellSnapshot: () => Effect.Effect<OrchestrationShellSnapshot, ProjectionRepositoryError>,
): (threadId: ThreadId) => Effect.Effect<boolean> {
  return (threadId) =>
    Effect.orElseSucceed(
      Effect.map(getShellSnapshot(), (snapshot) => {
        const thread = snapshot.threads.find((candidate) => candidate.id === threadId);
        return thread === undefined || thread.latestTurn === null;
      }),
      () => true,
    );
}

/**
 * Wrap an engine dispatch with first-turn-only persona injection. Settings
 * failures never block the dispatch — the command goes through raw.
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
    Effect.flatMap(
      Effect.orElseSucceed(getSettings, () => null),
      (settings) => {
        if (
          command.type !== "thread.turn.start" ||
          imBridgeMemberIdOfThreadId(command.threadId) === null
        ) {
          return engine.dispatch(command, options);
        }
        const withPersona = settings === null ? command : injectImBridgePersona(command, settings);
        // Member without a persona: nothing to gate.
        if (withPersona === command) return engine.dispatch(command, options);
        return Effect.flatMap(isFirstTurn(command.threadId), (first) =>
          engine.dispatch(first ? withPersona : command, options),
        );
      },
    );
}
