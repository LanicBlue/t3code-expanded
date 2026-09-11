/**
 * IM Bridge departure handling — the T3 side of deleting a member row in
 * Settings → IM 成员. The bridge archives the member in every IM workspace on
 * its next reconcile tick (`im leave` — stations handed back, parked missions
 * untouched), after which the member's in-flight mission threads can never
 * close their rounds: every `im mission submit` fails for an archived member.
 * T3 settles those threads at the moment of removal, inside the settings save
 * itself, so the delete confirmation the user just saw and the settle are one
 * user action rather than a reconcile-tick surprise.
 *
 * @module imBridgeDeparture
 */
import { CommandId, ThreadId } from "@t3tools/contracts";
import { imBridgeMemberIdOfThreadId } from "@t3tools/contracts/settings";
import type { ServerSettings } from "@t3tools/contracts/settings";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Result from "effect/Result";

import type { OrchestrationEngineShape } from "./orchestration/Services/OrchestrationEngine.ts";
import type { ProjectionSnapshotQueryShape } from "./orchestration/Services/ProjectionSnapshotQuery.ts";

/**
 * Member ids present in `previous` but gone from `next`. Removal only — a
 * disabled member stays in the table, and the bridge's leave on disable is a
 * different (reversible) flow this module deliberately does not touch.
 */
export function removedImBridgeMemberIds(
  previous: Pick<ServerSettings, "imBridge">,
  next: Pick<ServerSettings, "imBridge">,
): ReadonlyArray<string> {
  const nextIds = new Set(next.imBridge.members.map((member) => member.id));
  const removed = previous.imBridge.members
    .map((member) => member.id)
    .filter((id) => !nextIds.has(id));
  return removed;
}

/** The shell fields the liveness rule needs; satisfied by `OrchestrationThreadShell`. */
export interface DepartureThreadRow {
  readonly id: ThreadId;
  readonly settledAt: string | null;
  readonly settledOverride: string | null;
}

/**
 * The member's mission threads that are still live: ids shaped
 * `im-<memberId>-ms_<hex>…` that are neither settled nor settle-overridden.
 * The shell snapshot lists non-deleted threads only, so no deletedAt filter
 * is needed here.
 */
export function liveImBridgeThreads<T extends DepartureThreadRow>(
  threads: ReadonlyArray<T>,
  memberId: string,
): ReadonlyArray<T> {
  return threads.filter(
    (thread) =>
      imBridgeMemberIdOfThreadId(thread.id) === memberId &&
      thread.settledAt === null &&
      thread.settledOverride !== "settled",
  );
}

/**
 * Settle every live thread of the removed members. Never fails: a snapshot
 * outage skips the sweep with a warning and one bad thread never blocks the
 * others — a departure settle is best-effort cosmetics, never worth failing a
 * settings save over. Returns the thread ids that were dispatched for settle.
 */
export const settleDepartedImBridgeThreads = Effect.fn(
  "imBridgeDeparture.settleDepartedImBridgeThreads",
)(
  (
    engine: OrchestrationEngineShape,
    snapshots: Pick<ProjectionSnapshotQueryShape, "getShellSnapshot">,
    memberIds: ReadonlyArray<string>,
  ): Effect.Effect<ReadonlyArray<ThreadId>, never, Crypto.Crypto> =>
    Effect.gen(function* () {
      if (memberIds.length === 0) return [];
      const crypto = yield* Crypto.Crypto;
      const snapshot = yield* Effect.result(snapshots.getShellSnapshot());
      if (Result.isFailure(snapshot)) {
        yield* Effect.logWarning(
          "IM bridge departure settle skipped — shell snapshot unavailable",
          {
            memberIds: [...memberIds],
            reason:
              snapshot.failure instanceof Error
                ? snapshot.failure.message
                : String(snapshot.failure),
          },
        );
        return [];
      }
      const threads: ReadonlyArray<DepartureThreadRow> = snapshot.success.threads;
      const targets = memberIds.flatMap((memberId) => liveImBridgeThreads(threads, memberId));
      const settled: Array<ThreadId> = [];
      for (const thread of targets) {
        const dispatched = yield* Effect.result(
          Effect.flatMap(crypto.randomUUIDv4, (uuid) =>
            engine.dispatch({
              type: "thread.settle",
              commandId: CommandId.make(`server:im-bridge-departure:${thread.id}:${uuid}`),
              threadId: thread.id,
            }),
          ),
        );
        if (Result.isFailure(dispatched)) {
          yield* Effect.logWarning("IM bridge departure settle failed for one thread", {
            threadId: thread.id,
            reason:
              dispatched.failure instanceof Error
                ? dispatched.failure.message
                : String(dispatched.failure),
          });
          continue;
        }
        settled.push(thread.id);
      }
      if (settled.length > 0) {
        yield* Effect.logInfo("IM bridge member removal settled in-flight threads", {
          memberIds: [...memberIds],
          settledThreads: settled,
        });
      }
      return settled;
    }),
);
