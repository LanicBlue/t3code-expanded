/**
 * MissionSessionSettlement — the T3 side of the Project Service
 * `mission.ended` push frame (work-mission-v5 design §6.1, SDK 0.14).
 *
 * When a mission reaches a terminal state the Project Service delivers a
 * MissionEndedNotice carrying (noticeId, missionId, group, disposition, …) —
 * idempotent by noticeId, durable-ledger ACK semantics like every pushed
 * terminal frame. T3 answers by settling the sessions that ran the mission's
 * visits: the notice's `group` IS the workspace grouping key the sessions
 * live under (missionId), so the settlement finds them through the
 * association facts — the live routing registry plus the durable group→thread
 * rows the routing path recorded while the visits were still open — and then
 * drives the router's settle drive (safe-idle checks, settle or
 * settle-then-delete per the agent's retention, project-scope rebind), which
 * is why this module stays thin: it owns only the notice idempotency and the
 * ACK ledger.
 *
 * Durability: the ledger file IS the commit point. A notice is recorded —
 * with its full settlement plan — BEFORE any side effect runs, so a restart
 * resumes an unfinished settlement from disk even if the notice that started
 * it was never ACKed (and a redelivered notice for an acked record resolves
 * without repeating any side effect). A settlement blocked on a session's
 * safe-idle state keeps the record pending and the notice un-ACKed; the
 * thread-event hook and the periodic sweep retry.
 *
 * Gating: the handler itself is gate-free — it only ever runs behind a
 * mission frame, and the runtime service attaches the frame intake only
 * while the `mission.v1` capability is declared (settings gate
 * projectServiceClient.missionsEnabled, default ON since work-mission-v5
 * Phase 7 — the mission line is THE line; an explicit false is the emergency
 * off-switch). work-mission-v5 Phase 7 deleted the flow terminal path
 * (FlowSessionFinalization): this settlement is the only terminal-path
 * driver left.
 *
 * @module MissionSessionSettlement
 */
import {
  MissionEndedNoticeFacts,
  type OrchestrationThreadShell,
  type ServerSettings,
  ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";

import { writeFileStringAtomically } from "../atomicWrite.ts";
import type {
  FlowInstanceFinalizationInput,
  FlowInstanceFinalizationOutcome,
} from "./ProjectWorkNoticeRouting.ts";
import { ProjectWorkRoutingError } from "./ProjectWorkNoticeRouting.ts";
import type { ProjectWorkSessionSnapshot } from "./ProjectWorkNoticeRouting.ts";

// ── Ledger ───────────────────────────────────────────────────────

/** One mission-ended notice's durable target: whose session to settle. */
const SettlementTarget = Schema.Struct({
  /** The logical agent whose session ran the mission's visits. */
  agentId: Schema.String,
  /** The Project Service project the mission belongs to. */
  psProjectId: Schema.String,
  threadId: ThreadId,
});
export type SettlementTarget = typeof SettlementTarget.Type;

/** One mission-ended notice's durable lifecycle: planned, progressing, done. */
export const MissionEndedLedgerRecord = Schema.Struct({
  /** The Project Service notice identity — the intake idempotency key. */
  noticeId: Schema.String,
  missionId: Schema.String,
  /** The workspace grouping key the settled sessions live under. */
  group: Schema.String,
  disposition: MissionEndedNoticeFacts.fields.disposition,
  receivedAt: Schema.String,
  updatedAt: Schema.String,
  status: Schema.Literals(["pending", "acked"]),
  /** Sessions not yet settled (unprocessed OR blocked on a safe idle state). */
  remaining: Schema.Array(SettlementTarget),
  /** Sessions already settled (or proven gone) under this notice. */
  settled: Schema.Array(SettlementTarget),
});
export type MissionEndedLedgerRecord = typeof MissionEndedLedgerRecord.Type;

const MissionEndedLedger = Schema.Struct({
  version: Schema.Literal(1),
  records: Schema.Array(MissionEndedLedgerRecord),
});

export class MissionEndedLedgerError extends Schema.TaggedErrorClass<MissionEndedLedgerError>()(
  "MissionEndedLedgerError",
  { operation: Schema.Literals(["read", "write", "decode"]), ledgerPath: Schema.String },
) {
  override get message(): string {
    return `Failed to ${this.operation} the Project Service mission-ended ledger at ${this.ledgerPath}.`;
  }
}

const decodeLedger = Schema.decodeUnknownEffect(Schema.fromJsonString(MissionEndedLedger));

export interface MissionEndedLedgerStore {
  readonly load: Effect.Effect<ReadonlyArray<MissionEndedLedgerRecord>, MissionEndedLedgerError>;
  readonly store: (
    records: ReadonlyArray<MissionEndedLedgerRecord>,
  ) => Effect.Effect<void, MissionEndedLedgerError>;
}

/**
 * The production ledger: one JSON file, atomically replaced on every write.
 * A missing file is an empty ledger; an undecodable one is logged and treated
 * as empty — re-planning a settlement from current state is idempotent (an
 * already-settled session completes the drive immediately), so self-healing
 * beats wedging every future notice on a corrupt file. Requires the
 * FileSystem/Path services once, at construction, so the returned store is
 * R-free.
 */
export const makeFileMissionEndedLedgerStore = (
  ledgerPath: string,
): Effect.Effect<MissionEndedLedgerStore, never, FileSystem.FileSystem | Path.Path> =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const load: MissionEndedLedgerStore["load"] = Effect.gen(function* () {
      const raw = yield* fs
        .readFileString(ledgerPath)
        .pipe(
          Effect.catch((cause) =>
            cause.reason._tag === "NotFound"
              ? Effect.succeed(null)
              : Effect.fail(new MissionEndedLedgerError({ operation: "read", ledgerPath })),
          ),
        );
      if (raw === null || raw.trim().length === 0) {
        return [];
      }
      const decoded = yield* decodeLedger(raw).pipe(Effect.result);
      if (decoded._tag === "Failure") {
        yield* Effect.logWarning(
          "Project Service mission-ended ledger could not be decoded; replanning from current state",
          { ledgerPath },
        );
        return [];
      }
      return decoded.success.records;
    });
    const store: MissionEndedLedgerStore["store"] = (records) =>
      writeFileStringAtomically({
        filePath: ledgerPath,
        contents: `${JSON.stringify({ version: 1, records }, null, 2)}\n`,
      }).pipe(
        Effect.provideService(FileSystem.FileSystem, fs),
        Effect.provideService(Path.Path, path),
        Effect.mapError(() => new MissionEndedLedgerError({ operation: "write", ledgerPath })),
      );
    return { load, store };
  });

// ── Handler ──────────────────────────────────────────────────────

/** What a `mission.ended` notice carries; the noticeId pair is the contract. */
export type MissionEndedNoticeInput = MissionEndedNoticeFacts;

/**
 * `acked` — every target session settled (a redelivery of an acked notice
 * returns this with no new side effects). `waiting` — at least one session has
 * not reached a safe idle state; the record stays pending and the notice must
 * NOT be ACKed yet.
 */
export type MissionEndedOutcome =
  | { readonly status: "acked"; readonly settledThreadIds: ReadonlyArray<ThreadId> }
  | { readonly status: "waiting"; readonly blockedThreadIds: ReadonlyArray<ThreadId> };

/** Seams the settlement needs from its host; failures carry the wire codes. */
export interface MissionSessionSettlementDeps {
  readonly loadLedger: Effect.Effect<
    ReadonlyArray<MissionEndedLedgerRecord>,
    MissionEndedLedgerError
  >;
  readonly storeLedger: (
    records: ReadonlyArray<MissionEndedLedgerRecord>,
  ) => Effect.Effect<void, MissionEndedLedgerError>;
  readonly readSettings: Effect.Effect<ServerSettings, ProjectWorkRoutingError>;
  /**
   * The live routing registry's current sessions — the fast path: a session
   * whose grouping key equals the notice's group ran the mission's visits.
   */
  readonly routingSessions: Effect.Effect<
    ReadonlyArray<ProjectWorkSessionSnapshot>,
    ProjectWorkRoutingError
  >;
  /**
   * The thread-shell read used to correct persisted association rows against
   * CURRENT facts: a deleted thread drops out of the plan (nothing to settle),
   * and a thread no longer carrying the recorded agent's stamp is not trusted.
   * Active-only, like the finalization drive's own read — an archived session
   * is finished by definition.
   */
  readonly readThreadShell: (
    threadId: ThreadId,
  ) => Effect.Effect<Option.Option<OrchestrationThreadShell>, ProjectWorkRoutingError>;
  /**
   * The durable group→thread association the routing path recorded while the
   * mission's visits were still open (the restart-safe half of plan building —
   * open-run APIs cannot answer after the mission ended, which is exactly when
   * this notice arrives).
   */
  readonly resolveSessionRoute: (input: {
    readonly instanceId: string;
    readonly agentId: string;
  }) => Effect.Effect<
    { readonly threadId: string; readonly psProjectId: string } | null,
    ProjectWorkRoutingError
  >;
  /**
   * The settlement drive itself: the router's existing finalization (safe-idle
   * checks, settle / settle-then-delete per retention, project-scope rebind),
   * keyed by the group. Never fails — a drive that cannot finish yet resolves
   * "waiting" and the caller retries.
   */
  readonly settleWorkGroupSession: (
    input: FlowInstanceFinalizationInput,
  ) => Effect.Effect<FlowInstanceFinalizationOutcome, never>;
  readonly nowIso: Effect.Effect<string>;
}

export interface MissionSessionSettlementHandler {
  /**
   * Intake for one `mission.ended` notice. Idempotent by noticeId: an ACKed
   * record resolves immediately, a pending one continues its settlement.
   */
  readonly handleMissionEndedNotice: (
    input: MissionEndedNoticeInput,
  ) => Effect.Effect<MissionEndedOutcome, ProjectWorkRoutingError>;
  /**
   * Continue every pending settlement (restart recovery, a blocked session
   * that turned safe, or a crash between plan and completion). Never fails;
   * per-record failures are logged and retried on the next pass.
   */
  readonly resumePending: Effect.Effect<void>;
}

const projectEnabledAgents = (settings: ServerSettings): ReadonlyArray<string> =>
  Object.entries(settings.logicalAgents)
    .filter(([, agent]) => agent.project.enabled)
    .map(([agentId]) => agentId);

export const makeMissionSessionSettlementHandler = Effect.fn("makeMissionSessionSettlement")(
  function* (
    deps: MissionSessionSettlementDeps,
  ): Effect.fn.Return<MissionSessionSettlementHandler> {
    // The ledger is loaded once and kept in memory; every mutation persists
    // first and updates the Ref second, so a crash between the two re-reads the
    // persisted truth at the next start.
    const ledgerRef = yield* Ref.make<ReadonlyArray<MissionEndedLedgerRecord> | null>(null);
    // Notices, resume passes, and thread-event retries all funnel through one
    // permit: the ledger is a single-file resource and a record's lifecycle
    // must never interleave with itself.
    const permit = Semaphore.makeUnsafe(1);
    const serialized = <A, E>(self: Effect.Effect<A, E>): Effect.Effect<A, E> =>
      permit.withPermits(1)(self);

    const ledgerFailure = (error: MissionEndedLedgerError) =>
      new ProjectWorkRoutingError({ code: "CONSUMER_INTERNAL", detail: error.message });

    const ensureLedger: Effect.Effect<
      ReadonlyArray<MissionEndedLedgerRecord>,
      ProjectWorkRoutingError
    > = Ref.get(ledgerRef).pipe(
      Effect.flatMap((cached) =>
        cached !== null
          ? Effect.succeed(cached)
          : deps.loadLedger.pipe(
              Effect.mapError(ledgerFailure),
              Effect.tap((records) => Ref.set(ledgerRef, records)),
            ),
      ),
    );

    const persist = (
      records: ReadonlyArray<MissionEndedLedgerRecord>,
    ): Effect.Effect<void, ProjectWorkRoutingError> =>
      deps.storeLedger(records).pipe(
        Effect.mapError(ledgerFailure),
        Effect.tap(() => Ref.set(ledgerRef, records)),
      );

    const modifyRecord = (
      noticeId: string,
      mutate: (record: MissionEndedLedgerRecord) => MissionEndedLedgerRecord,
    ): Effect.Effect<void, ProjectWorkRoutingError> =>
      Effect.flatMap(ensureLedger, (ledger) =>
        persist(ledger.map((record) => (record.noticeId === noticeId ? mutate(record) : record))),
      );

    /**
     * Plan one settlement: whose sessions settle under this notice. The union
     * of the routing registry's group-keyed sessions (live routing state) and
     * the persisted group→thread rows corrected against current thread facts
     * (the durable catch-up a restart needs). A mission ended without any
     * associated session records an empty plan — an immediate no-op ACK, the
     * honest answer for a mission this consumer never worked.
     */
    const buildRecord = Effect.fn("MissionSessionSettlement.buildRecord")(function* (
      input: MissionEndedNoticeInput,
    ): Effect.fn.Return<MissionEndedLedgerRecord, ProjectWorkRoutingError> {
      const targets = new Map<string, SettlementTarget>();
      const addTarget = (target: SettlementTarget): void => {
        targets.set(`${target.agentId}\n${target.threadId}`, target);
      };
      // Live registry: any session whose grouping key is the mission's group.
      const registry = yield* deps.routingSessions.pipe(Effect.result);
      if (registry._tag === "Success") {
        for (const session of registry.success) {
          if (session.flowInstanceKey === input.group) {
            addTarget({
              agentId: session.agentId,
              psProjectId: session.projectId,
              threadId: session.threadId,
            });
          }
        }
      } else {
        // The registry is a fast path only; a failed read reads as "no
        // registry" (the persisted association still associates).
        yield* Effect.logDebug(
          "mission-ended settlement could not read the routing registry; falling back to persisted associations",
          { group: input.group, code: registry.failure.code },
        );
      }
      // Persisted association rows, one per project-enabled agent — the fact
      // that survives the visits' closure and a registry-emptying restart.
      const settings = yield* deps.readSettings.pipe(Effect.result);
      if (settings._tag === "Success") {
        for (const agentId of projectEnabledAgents(settings.success)) {
          const route = yield* deps
            .resolveSessionRoute({ instanceId: input.group, agentId })
            .pipe(Effect.result);
          if (route._tag === "Failure") {
            // An unreadable ledger skips the agent this pass — a missing row
            // and a failed read must not blur (the record stays pending
            // rebuildable only before the commit point, so fail the plan).
            return yield* route.failure;
          }
          if (route.success === null) {
            continue;
          }
          const shell = yield* deps
            .readThreadShell(ThreadId.make(route.success.threadId))
            .pipe(Effect.result);
          if (shell._tag === "Failure") {
            return yield* shell.failure;
          }
          if (Option.isNone(shell.success) || shell.success.value.logicalAgentId !== agentId) {
            // Gone or re-stamped: no longer this agent's session to settle.
            continue;
          }
          addTarget({
            agentId,
            psProjectId: route.success.psProjectId,
            threadId: ThreadId.make(route.success.threadId),
          });
        }
      } else {
        return yield* settings.failure;
      }
      const now = yield* deps.nowIso;
      return {
        noticeId: input.noticeId,
        missionId: input.missionId,
        group: input.group,
        disposition: input.disposition,
        receivedAt: now,
        updatedAt: now,
        status: "pending",
        remaining: [...targets.values()].toSorted((left, right) =>
          `${left.agentId}\n${left.threadId}`.localeCompare(`${right.agentId}\n${right.threadId}`),
        ),
        settled: [],
      };
    });

    /**
     * Execute one record's settlement plan. Settles only sessions the drive
     * could finish; blocked ones keep the record pending. Persists after every
     * state change so a crash resumes mid-plan without repeating a settle
     * (the drive itself is idempotent — a settled session completes at once).
     */
    const processRecord = Effect.fn("MissionSessionSettlement.processRecord")(function* (
      record: MissionEndedLedgerRecord,
    ): Effect.fn.Return<MissionEndedOutcome, ProjectWorkRoutingError> {
      const blocked: SettlementTarget[] = [];
      let queue = [...record.remaining];
      const settled = [...record.settled];
      while (queue.length > 0) {
        const target = queue[0];
        if (target === undefined) {
          queue = [];
          break;
        }
        const outcome = yield* deps.settleWorkGroupSession({
          agentId: target.agentId,
          projectId: target.psProjectId,
          instanceKey: record.group,
          threadId: target.threadId,
        });
        queue = queue.slice(1);
        if (outcome.kind === "waiting") {
          // Blocked on a safe idle state (or transiently unreadable facts):
          // the session stays until the drive's own conditions hold.
          blocked.push(target);
          continue;
        }
        settled.push(target);
        yield* modifyRecord(record.noticeId, (current) => ({
          ...current,
          remaining: [...blocked, ...queue],
          settled: [...settled],
        }));
      }
      const remaining = [...blocked, ...queue];
      if (remaining.length === 0) {
        const now = yield* deps.nowIso;
        yield* modifyRecord(record.noticeId, (current) => ({
          ...current,
          status: "acked" as const,
          updatedAt: now,
        }));
        return { status: "acked" as const, settledThreadIds: settled.map((t) => t.threadId) };
      }
      return { status: "waiting" as const, blockedThreadIds: remaining.map((t) => t.threadId) };
    });

    const handleMissionEndedNotice = Effect.fn("MissionSessionSettlement.handleMissionEndedNotice")(
      function* (
        input: MissionEndedNoticeInput,
      ): Effect.fn.Return<MissionEndedOutcome, ProjectWorkRoutingError> {
        return yield* serialized(
          Effect.gen(function* () {
            const ledger = yield* ensureLedger;
            const existing = ledger.find((record) => record.noticeId === input.noticeId);
            if (existing !== undefined) {
              if (existing.status === "acked") {
                // Redelivery of a finished notice: the recorded settlements are
                // the answer, and no side effect runs again.
                return {
                  status: "acked" as const,
                  settledThreadIds: existing.settled.map((t) => t.threadId),
                };
              }
              return yield* processRecord(existing);
            }
            const record = yield* buildRecord(input);
            // COMMIT POINT: the plan is durable before the first side effect,
            // so a crash from here on resumes from the file without the notice.
            yield* persist([...ledger, record]);
            return yield* processRecord(record);
          }),
        );
      },
    );

    const resumePending: MissionSessionSettlementHandler["resumePending"] = serialized(
      Effect.gen(function* () {
        const ledger = yield* ensureLedger.pipe(Effect.result);
        if (ledger._tag === "Failure") {
          yield* Effect.logWarning(
            "mission-ended settlement resume pass could not read the ledger",
            { code: ledger.failure.code, detail: ledger.failure.detail },
          );
          return;
        }
        for (const record of ledger.success) {
          if (record.status !== "pending") {
            continue;
          }
          const outcome = yield* processRecord(record).pipe(Effect.result);
          if (outcome._tag === "Failure") {
            yield* Effect.logWarning(
              "mission-ended settlement could not finish; retrying on the next pass",
              {
                group: record.group,
                code: outcome.failure.code,
                detail: outcome.failure.detail,
              },
            );
          }
        }
      }),
    );

    return { handleMissionEndedNotice, resumePending };
  },
);
