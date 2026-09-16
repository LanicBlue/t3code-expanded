/**
 * IM Bridge turn finalization — the T3 side of the bundled template
 * marketplace. For a bridge thread's FIRST user turn — the thread has no
 * prior turns by T3's own facts; no provider state is consulted — the
 * member's `persona` (Settings → IM 成员) and the sender's
 * `context.freshContext` (the mission's stable block) are folded into the
 * message text as one complete input, and the context field is cleared.
 * The stored message, the thread UI, and the provider input are the same
 * bytes. Later turns arrive round-form and pass through untouched: the
 * provider conversation already carries the first turn's full text, so
 * nothing is re-sent (previously every session re-bootstrap re-sent the
 * persona, stacking a copy into the provider transcript on every mission
 * round).
 *
 * This module understands only T3 member/thread lifecycle. It does not
 * parse or adjudicate IM stations, outcomes, revisions, or parent/child
 * Missions; those semantics stay in IM and its external bridge. It is
 * format-blind about freshContext's content beyond folding it verbatim.
 *
 * Deliberately server-side at the dispatch seam, before the command is
 * persisted: the bridge (an external process over the orchestration HTTP
 * API) stays persona-agnostic, and the fold lands once — there is no
 * second composition point at the provider layer. It recognizes bridge
 * threads by the deterministic id shape `im-<memberId>-ms_<hex>` (mission
 * ids are `ms_` plus hex, so `-ms_` cannot appear inside a member id;
 * suffixed fallback ids such as `im-<member>-<mission>-3` keep the same
 * infix).
 *
 * @module imBridgePersona
 */
import * as Effect from "effect/Effect";
import type {
  OrchestrationCommand,
  OrchestrationShellSnapshot,
  ProjectionRepositoryError,
  ServerSettingsError,
  ThreadId,
} from "@t3tools/contracts";
import { imBridgeMemberIdOfThreadId } from "@t3tools/contracts/settings";
import type { ServerSettings } from "@t3tools/contracts/settings";

import type { OrchestrationDispatchError } from "./orchestration/Errors.ts";
import type { OrchestrationEngineShape } from "./orchestration/Services/OrchestrationEngine.ts";

const PERSONA_HEADER =
  "Member persona (T3 settings); it supplements, never overrides, the duty rules below.";

type TurnStartCommand = Extract<OrchestrationCommand, { type: "thread.turn.start" }>;

/** Drop the consumed freshContext, keeping any other context fields. */
const withoutFreshContext = (command: TurnStartCommand): TurnStartCommand => {
  const context = command.message.context;
  if (context === undefined || context.freshContext === undefined) {
    return command;
  }
  const { freshContext: _consumed, ...rest } = context;
  const message = {
    ...command.message,
    ...("version" in rest || "records" in rest ? { context: rest } : {}),
  };
  return { ...command, message };
};

/**
 * Pure transform: fold the persona and the sender's freshContext into the
 * turn's message text when the turn starts the thread's conversation.
 * Anything else — other commands, non-bridge threads, later rounds,
 * members without a persona or senders without a stable block — passes
 * through with the freshContext consumed (stripped, never re-applied).
 */
export function foldImBridgeFreshContext(
  command: OrchestrationCommand,
  settings: ServerSettings,
  startsConversation: boolean,
): OrchestrationCommand {
  if (command.type !== "thread.turn.start") return command;
  const memberId = imBridgeMemberIdOfThreadId(command.threadId);
  if (memberId === null) return command;
  const senderContext = command.message.context?.freshContext?.trim() ?? "";
  if (!startsConversation) {
    return withoutFreshContext(command);
  }
  const persona = settings.imBridge.members
    .find((member) => member.id === memberId)
    ?.persona?.trim();
  const prefix = [
    ...(persona !== undefined && persona.length > 0 ? [`${PERSONA_HEADER}\n\n${persona}`] : []),
    ...(senderContext.length > 0 ? [senderContext] : []),
  ].join("\n\n");
  if (prefix.length === 0) {
    return withoutFreshContext(command);
  }
  const folded = withoutFreshContext(command);
  return {
    ...folded,
    message: {
      ...folded.message,
      text: `${prefix}\n\n${command.message.text}`,
    },
  };
}

/**
 * Whether the thread starts its conversation with this turn, judged from
 * T3's own facts alone: the thread is unknown (about to be created) or has
 * no turns yet. Provider-side history is deliberately not consulted. An
 * unreadable snapshot fails toward "starts" — redundancy on the first turn
 * beats silently dropping the stable context.
 */
export function imBridgeThreadStartsConversation(
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
 * Wrap an engine dispatch with the bridge-turn fold. Pure in the command,
 * so retries fold from the original command, never cumulatively. A
 * settings read failure fails closed so the bridge can retry instead of
 * starting a conversation with the wrong persona.
 */
export function dispatchWithImBridgePersona(
  engine: OrchestrationEngineShape,
  getSettings: Effect.Effect<ServerSettings, ServerSettingsError>,
  startsConversation: (threadId: ThreadId) => Effect.Effect<boolean> = () => Effect.succeed(true),
): (
  command: OrchestrationCommand,
  options?: Parameters<OrchestrationEngineShape["dispatch"]>[1],
) => Effect.Effect<{ sequence: number }, OrchestrationDispatchError> {
  return (command, options) =>
    command.type !== "thread.turn.start" || imBridgeMemberIdOfThreadId(command.threadId) === null
      ? engine.dispatch(command, options)
      : Effect.flatMap(
          Effect.all([Effect.orDie(getSettings), startsConversation(command.threadId)]),
          ([settings, isNew]) =>
            engine.dispatch(foldImBridgeFreshContext(command, settings, isNew), options),
        );
}
