/**
 * Project retirement cleanup — the T3 side of the Project Service
 * `project.retired` notice.
 *
 * When the Project Service retires a project it delivers a notice carrying
 * (noticeId, projectId) — idempotent by noticeId and redeliverable until
 * ACKed. T3 answers by deleting every Project Work Session routed for that
 * Project Service project and wiping the session routing, so Work that
 * arrives for the project later starts from a fresh session (the router's
 * missing-session path). The local T3 project and the workspace directory
 * are never touched.
 *
 * Durability: the ledger file IS the commit point. A notice is recorded —
 * with its full deletion plan — BEFORE any side effect runs, so a restart
 * resumes an unfinished cleanup from disk even if the notice that started it
 * was never ACKed (and therefore never re-fires). The record flips to
 * `acked` only after the last session is deleted; the adapter call resolving
 * is the ACK. A redelivered notice for an `acked` record resolves without
 * repeating any side effect.
 *
 * Safety: a session is deleted only once it reached a safe idle state — the
 * server twin of the client's settle guard (`canSettle` in client-runtime
 * threadSettled.ts): no pending approvals or user input, no starting/running
 * provider session, no running turn, and no queued turn start inside the
 * adoption grace window. Blocked sessions keep the record `pending` and the
 * notice un-ACKed; the thread-event hook and the periodic sweep retry, and
 * the Project Service redelivers.
 *
 * @module ProjectRetirement
 */
import {
  CommandId,
  type OrchestrationCommand,
  type OrchestrationProject,
  type OrchestrationThreadShell,
  ProjectId,
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
import { ProjectWorkRoutingError } from "./ProjectWorkNoticeRouting.ts";
import type { ProjectWorkSessionSnapshot } from "./ProjectWorkNoticeRouting.ts";

// ── Safe idle ────────────────────────────────────────────────────

/**
 * A user message no turn has picked up yet counts as queued work for this
 * long. Mirrors QUEUED_TURN_START_GRACE_MS in client-runtime threadSettled.ts
 * (and the decider's own twin): session adoption takes seconds, so a message
 * still unadopted after the window is a failed or stale start.
 */
export const RETIREMENT_QUEUED_TURN_START_GRACE_MS = 2 * 60 * 1_000;

type SafeIdleShell = Pick<
  OrchestrationThreadShell,
  "hasPendingApprovals" | "hasPendingUserInput" | "session" | "latestTurn" | "latestUserMessageAt"
>;

/** The queued-turn-start detection, ported from the client's threadSettled. */
const hasQueuedTurnStart = (shell: SafeIdleShell, options: { readonly now: string }): boolean => {
  if (shell.latestUserMessageAt === null) return false;
  // A failed session start clears the queued state: the failure is already
  // visible as a status edge.
  if (shell.session?.status === "error") return false;
  const messageAt = Date.parse(shell.latestUserMessageAt);
  if (Number.isNaN(messageAt)) return false;
  const nowMs = Date.parse(options.now);
  if (Number.isNaN(nowMs)) return false;
  // Bounded on both sides: a message timestamp from a clock ahead of this
  // one must not hold the queued state for the whole skew.
  if (Math.abs(nowMs - messageAt) > RETIREMENT_QUEUED_TURN_START_GRACE_MS) return false;
  const turn = shell.latestTurn;
  if (turn === null) return true;
  return [turn.requestedAt, turn.startedAt, turn.completedAt].every(
    (candidate) => candidate === null || Date.parse(candidate) < messageAt,
  );
};

/**
 * Whether a Project Work Session reached the state where deleting it is
 * safe: the client's `canSettle` condition list plus the running-turn check
 * the wake router's own busy derivation uses. Anything the client refuses to
 * CLASSIFY as settled is also refused as a deletion target — a session mid
 * turn or blocked on a human answer is left in place and the retirement
 * waits for it instead.
 */
export const isWorkSessionSafeToRetire = (
  shell: SafeIdleShell,
  options: { readonly now: string },
): boolean => {
  if (shell.hasPendingApprovals || shell.hasPendingUserInput) return false;
  if (
    shell.session !== null &&
    (shell.session.status === "starting" || shell.session.status === "running")
  ) {
    return false;
  }
  if (shell.latestTurn?.state === "running") return false;
  return !hasQueuedTurnStart(shell, options);
};

// ── Ledger ───────────────────────────────────────────────────────

/** One retirement notice's durable lifecycle: planned, progressing, done. */
export const RetirementLedgerRecord = Schema.Struct({
  /** The Project Service notice identity — the intake idempotency key. */
  noticeId: Schema.String,
  projectId: Schema.String,
  receivedAt: Schema.String,
  updatedAt: Schema.String,
  status: Schema.Literals(["pending", "acked"]),
  /** The workspace root the plan resolved (null = unresolvable at intake). */
  resolvedWorkspaceRoot: Schema.NullOr(Schema.String),
  /** The T3 project the plan resolved (null = none; only registry ids apply). */
  t3ProjectId: Schema.NullOr(ProjectId),
  /** Sessions not yet deleted (unprocessed OR blocked on a safe idle state). */
  remainingThreadIds: Schema.Array(ThreadId),
  /** Sessions already deleted (or proven gone) under this notice. */
  deletedThreadIds: Schema.Array(ThreadId),
});
export type RetirementLedgerRecord = typeof RetirementLedgerRecord.Type;

const RetirementLedger = Schema.Struct({
  version: Schema.Literal(1),
  records: Schema.Array(RetirementLedgerRecord),
});

export class ProjectRetirementLedgerError extends Schema.TaggedErrorClass<ProjectRetirementLedgerError>()(
  "ProjectRetirementLedgerError",
  { operation: Schema.Literals(["read", "write", "decode"]), ledgerPath: Schema.String },
) {
  override get message(): string {
    return `Failed to ${this.operation} the Project Service retirement ledger at ${this.ledgerPath}.`;
  }
}

const decodeLedger = Schema.decodeUnknownEffect(Schema.fromJsonString(RetirementLedger));

export interface ProjectRetirementLedgerStore {
  readonly load: Effect.Effect<ReadonlyArray<RetirementLedgerRecord>, ProjectRetirementLedgerError>;
  readonly store: (
    records: ReadonlyArray<RetirementLedgerRecord>,
  ) => Effect.Effect<void, ProjectRetirementLedgerError>;
}

/**
 * The production ledger: one JSON file, atomically replaced on every write.
 * A missing file is an empty ledger; an undecodable one is logged and treated
 * as empty — re-planning a retirement from current state is idempotent
 * (already-deleted sessions read as absent), so self-healing beats wedging
 * every future notice on a corrupt file. Requires the FileSystem/Path
 * services once, at construction, so the returned store is R-free.
 */
export const makeFileProjectRetirementLedgerStore = (
  ledgerPath: string,
): Effect.Effect<ProjectRetirementLedgerStore, never, FileSystem.FileSystem | Path.Path> =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const load: ProjectRetirementLedgerStore["load"] = Effect.gen(function* () {
      const raw = yield* fs
        .readFileString(ledgerPath)
        .pipe(
          Effect.catch((cause) =>
            cause.reason._tag === "NotFound"
              ? Effect.succeed(null)
              : Effect.fail(new ProjectRetirementLedgerError({ operation: "read", ledgerPath })),
          ),
        );
      if (raw === null || raw.trim().length === 0) {
        return [];
      }
      const decoded = yield* decodeLedger(raw).pipe(Effect.result);
      if (decoded._tag === "Failure") {
        yield* Effect.logWarning(
          "Project Service retirement ledger could not be decoded; replanning from current state",
          { ledgerPath },
        );
        return [];
      }
      return decoded.success.records;
    });
    const store: ProjectRetirementLedgerStore["store"] = (records) =>
      writeFileStringAtomically({
        filePath: ledgerPath,
        contents: `${JSON.stringify({ version: 1, records }, null, 2)}\n`,
      }).pipe(
        Effect.provideService(FileSystem.FileSystem, fs),
        Effect.provideService(Path.Path, path),
        Effect.mapError(() => new ProjectRetirementLedgerError({ operation: "write", ledgerPath })),
      );
    return { load, store };
  });

// ── Handler ──────────────────────────────────────────────────────

/** What a `project.retired` notice carries; the id pair is the contract. */
export interface ProjectRetiredNoticeInput {
  readonly noticeId: string;
  readonly projectId: string;
  readonly projectName?: string;
  readonly workspaceDir?: string;
}

/**
 * `acked` — every session is gone and the notice may be ACKed (a redelivery
 * of an acked notice returns this with no new side effects). `waiting` — at
 * least one session has not reached a safe idle state; the record stays
 * pending and the notice must NOT be ACKed yet.
 */
export type ProjectRetirementOutcome =
  | { readonly status: "acked"; readonly deletedThreadIds: ReadonlyArray<ThreadId> }
  | { readonly status: "waiting"; readonly blockedThreadIds: ReadonlyArray<ThreadId> };

/** Seams the handler needs from its host; failures carry the wire codes. */
export interface ProjectRetirementHandlerDeps {
  readonly loadLedger: Effect.Effect<
    ReadonlyArray<RetirementLedgerRecord>,
    ProjectRetirementLedgerError
  >;
  readonly storeLedger: (
    records: ReadonlyArray<RetirementLedgerRecord>,
  ) => Effect.Effect<void, ProjectRetirementLedgerError>;
  /**
   * The executor's shell read must see ARCHIVED shells — the plan includes
   * them, and a None is treated as "proven gone" (see processRecord). A
   * seam that filters archived threads would ACK archived sessions as
   * deleted without ever dispatching their thread.delete.
   */
  readonly readThreadShell: (
    threadId: ThreadId,
  ) => Effect.Effect<Option.Option<OrchestrationThreadShell>, ProjectWorkRoutingError>;
  /**
   * Active AND archived thread shells of one T3 project — the durable half
   * of session discovery (the in-memory routing registry does not survive a
   * restart, these do).
   */
  readonly listProjectThreadShells: (
    projectId: ProjectId,
  ) => Effect.Effect<ReadonlyArray<OrchestrationThreadShell>, ProjectWorkRoutingError>;
  /** The in-memory Project Work routing registry's current sessions. */
  readonly routingSessions: Effect.Effect<ReadonlyArray<ProjectWorkSessionSnapshot>>;
  /** Clear every routing entry for one Project Service project. Never fails. */
  readonly dropProjectSessions: (projectId: string) => Effect.Effect<void>;
  /** The Project Service project list — the workspace-dir fallback source. */
  readonly listProjectServiceProjects: Effect.Effect<
    ReadonlyArray<{ readonly projectId: string; readonly workspaceDir: string }>,
    ProjectWorkRoutingError
  >;
  readonly normalizeWorkspaceDir: (
    workspaceDir: string,
  ) => Effect.Effect<string, ProjectWorkRoutingError>;
  readonly getActiveProjectByWorkspaceRoot: (
    workspaceRoot: string,
  ) => Effect.Effect<Option.Option<OrchestrationProject>, ProjectWorkRoutingError>;
  readonly canonicalizeWorkspaceRoot: (workspaceRoot: string) => Effect.Effect<string>;
  readonly listActiveProjectRoots: () => Effect.Effect<
    ReadonlyArray<{ readonly projectId: ProjectId; readonly workspaceRoot: string }>,
    ProjectWorkRoutingError
  >;
  readonly dispatchCommand: (
    command: OrchestrationCommand,
  ) => Effect.Effect<void, ProjectWorkRoutingError>;
  readonly nowIso: Effect.Effect<string>;
  readonly newId: Effect.Effect<string>;
}

export interface ProjectRetirementHandler {
  /**
   * Intake for one `project.retired` notice. Idempotent by noticeId: an
   * ACKed record resolves immediately, a pending one continues its cleanup.
   */
  readonly handleRetiredNotice: (
    input: ProjectRetiredNoticeInput,
  ) => Effect.Effect<ProjectRetirementOutcome, ProjectWorkRoutingError>;
  /**
   * Continue every pending retirement (restart recovery, a blocked session
   * that turned safe, or a crash between plan and completion). Never fails;
   * per-record failures are logged and retried on the next pass.
   */
  readonly resumePending: Effect.Effect<void>;
}

/** Only threads the wake router stamped carry the logical agent binding. */
const isRoutedSessionShell = (shell: OrchestrationThreadShell): boolean =>
  shell.logicalAgentId != null;

export const makeProjectRetirementHandler = Effect.fn("makeProjectRetirementHandler")(function* (
  deps: ProjectRetirementHandlerDeps,
): Effect.fn.Return<ProjectRetirementHandler> {
  // The ledger is loaded once and kept in memory; every mutation persists
  // first and updates the Ref second, so a crash between the two re-reads the
  // persisted truth at the next start.
  const ledgerRef = yield* Ref.make<ReadonlyArray<RetirementLedgerRecord> | null>(null);
  // Notices, resume passes, and thread-event retries all funnel through one
  // permit: the ledger is a single-file resource and a record's lifecycle
  // must never interleave with itself.
  const permit = Semaphore.makeUnsafe(1);
  const serialized = <A, E>(self: Effect.Effect<A, E>): Effect.Effect<A, E> =>
    permit.withPermits(1)(self);

  const ledgerFailure = (error: ProjectRetirementLedgerError) =>
    new ProjectWorkRoutingError({ code: "CONSUMER_INTERNAL", detail: error.message });

  const ensureLedger: Effect.Effect<
    ReadonlyArray<RetirementLedgerRecord>,
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
    records: ReadonlyArray<RetirementLedgerRecord>,
  ): Effect.Effect<void, ProjectWorkRoutingError> =>
    deps.storeLedger(records).pipe(
      Effect.mapError(ledgerFailure),
      Effect.tap(() => Ref.set(ledgerRef, records)),
    );

  const modifyRecord = (
    noticeId: string,
    mutate: (record: RetirementLedgerRecord) => RetirementLedgerRecord,
  ): Effect.Effect<void, ProjectWorkRoutingError> =>
    Effect.flatMap(ensureLedger, (ledger) =>
      persist(ledger.map((record) => (record.noticeId === noticeId ? mutate(record) : record))),
    );

  /**
   * Resolve the workspace root for the retired project, best-effort: the
   * notice's own directory, then the Project Service list, then any session
   * the routing registry holds. Retiring may already have removed the
   * project upstream and the directory locally, so every source may fail —
   * null then means only registry-known sessions can be deleted.
   */
  const resolveWorkspaceRoot = Effect.fn("ProjectRetirement.resolveWorkspaceRoot")(function* (
    input: ProjectRetiredNoticeInput,
    registrySessions: ReadonlyArray<ProjectWorkSessionSnapshot>,
  ): Effect.fn.Return<string | null, ProjectWorkRoutingError> {
    const candidates: string[] = [];
    const noticeDir = input.workspaceDir?.trim() ?? "";
    if (noticeDir.length > 0) {
      candidates.push(noticeDir);
    }
    const listed = yield* deps.listProjectServiceProjects.pipe(Effect.result);
    if (listed._tag === "Success") {
      const hit = listed.success.find((project) => project.projectId === input.projectId);
      if (hit !== undefined) {
        candidates.push(hit.workspaceDir);
      }
    }
    for (const session of registrySessions) {
      if (session.workspaceDir !== null) {
        candidates.push(session.workspaceDir);
      }
    }
    for (const candidate of candidates) {
      const resolved = yield* deps.normalizeWorkspaceDir(candidate).pipe(Effect.result);
      if (resolved._tag === "Success") {
        return resolved.success;
      }
    }
    return null;
  });

  /** Canonical-key reuse scan, mirroring the wake router's root resolution. */
  const findProjectByCanonicalRoot = Effect.fn("ProjectRetirement.findProjectByCanonicalRoot")(
    function* (
      workspaceRoot: string,
    ): Effect.fn.Return<Option.Option<ProjectId>, ProjectWorkRoutingError> {
      const actives = yield* deps.listActiveProjectRoots();
      for (const active of actives) {
        const key = yield* deps.canonicalizeWorkspaceRoot(active.workspaceRoot);
        if (key === workspaceRoot) {
          return Option.some(active.projectId);
        }
      }
      return Option.none();
    },
  );

  /**
   * Plan one retirement: which sessions die under this notice. The union of
   * the routing registry's thread ids for the Project Service project (live
   * routing state) and every routed shell in the resolved T3 project (the
   * durable catch-up a restart needs — human threads in the same project are
   * left alone).
   */
  const buildRecord = Effect.fn("ProjectRetirement.buildRecord")(function* (
    input: ProjectRetiredNoticeInput,
  ): Effect.fn.Return<RetirementLedgerRecord, ProjectWorkRoutingError> {
    const registry = yield* deps.routingSessions.pipe(Effect.result);
    const registrySessions =
      registry._tag === "Success"
        ? registry.success.filter((session) => session.projectId === input.projectId)
        : [];
    const workspaceRoot = yield* resolveWorkspaceRoot(input, registrySessions);
    let t3ProjectId: ProjectId | null = null;
    if (workspaceRoot !== null) {
      const exact = yield* deps.getActiveProjectByWorkspaceRoot(workspaceRoot);
      if (Option.isSome(exact)) {
        t3ProjectId = exact.value.id;
      } else {
        const canonical = yield* findProjectByCanonicalRoot(workspaceRoot);
        if (Option.isSome(canonical)) {
          t3ProjectId = canonical.value;
        }
      }
    }
    const threadIds = new Set<ThreadId>(registrySessions.map((session) => session.threadId));
    if (t3ProjectId !== null) {
      const shells = yield* deps.listProjectThreadShells(t3ProjectId);
      for (const shell of shells) {
        if (isRoutedSessionShell(shell)) {
          threadIds.add(shell.id);
        }
      }
    }
    const now = yield* deps.nowIso;
    return {
      noticeId: input.noticeId,
      projectId: input.projectId,
      receivedAt: now,
      updatedAt: now,
      status: "pending",
      resolvedWorkspaceRoot: workspaceRoot,
      t3ProjectId,
      remainingThreadIds: [...threadIds].toSorted((left, right) => left.localeCompare(right)),
      deletedThreadIds: [],
    };
  });

  /**
   * Execute one record's deletion plan. Deletes only sessions that reached
   * a safe idle state; blocked ones keep the record pending. Persists after
   * every state change so a crash resumes mid-plan without repeating a
   * deletion (an already-deleted session reads as absent and just moves to
   * the done list).
   */
  const processRecord = Effect.fn("ProjectRetirement.processRecord")(function* (
    record: RetirementLedgerRecord,
  ): Effect.fn.Return<ProjectRetirementOutcome, ProjectWorkRoutingError> {
    // Clear the routing first — idempotent, and it also catches entries a
    // racing wake recreated after the plan was written.
    yield* deps.dropProjectSessions(record.projectId);
    // The queue of not-yet-decided sessions plus the ones already blocked on
    // a safe idle state: together they stay persisted as `remaining` after
    // every step, so a crash mid-plan resumes exactly where it stopped.
    const blocked: ThreadId[] = [];
    let queue = [...record.remainingThreadIds];
    const deleted = [...record.deletedThreadIds];
    while (queue.length > 0) {
      const threadId = queue[0];
      if (threadId === undefined) {
        queue = [];
        break;
      }
      const shell = yield* deps.readThreadShell(threadId);
      if (Option.isNone(shell)) {
        // Gone (deleted under this or an earlier pass): the deletion is done.
        queue = queue.slice(1);
        deleted.push(threadId);
        yield* modifyRecord(record.noticeId, (current) => ({
          ...current,
          remainingThreadIds: [...blocked, ...queue],
          deletedThreadIds: [...deleted],
        }));
        continue;
      }
      const now = yield* deps.nowIso;
      if (!isWorkSessionSafeToRetire(shell.value, { now })) {
        // Blocked on a safe idle state: the session stays until it settles.
        queue = queue.slice(1);
        blocked.push(threadId);
        continue;
      }
      yield* deps.dispatchCommand({
        type: "thread.delete",
        commandId: CommandId.make(yield* deps.newId),
        threadId,
      });
      queue = queue.slice(1);
      deleted.push(threadId);
      yield* modifyRecord(record.noticeId, (current) => ({
        ...current,
        remainingThreadIds: [...blocked, ...queue],
        deletedThreadIds: [...deleted],
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
      return { status: "acked", deletedThreadIds: deleted };
    }
    return { status: "waiting", blockedThreadIds: remaining };
  });

  const handleRetiredNotice = Effect.fn("ProjectRetirement.handleRetiredNotice")(function* (
    input: ProjectRetiredNoticeInput,
  ): Effect.fn.Return<ProjectRetirementOutcome, ProjectWorkRoutingError> {
    return yield* serialized(
      Effect.gen(function* () {
        const ledger = yield* ensureLedger;
        const existing = ledger.find((record) => record.noticeId === input.noticeId);
        if (existing !== undefined) {
          if (existing.status === "acked") {
            // Redelivery of a finished notice: the recorded deletions are the
            // answer, and no side effect runs again.
            return { status: "acked" as const, deletedThreadIds: existing.deletedThreadIds };
          }
          return yield* processRecord(existing);
        }
        const record = yield* buildRecord(input);
        // COMMIT POINT: the plan is durable before the first side effect, so
        // a crash from here on resumes from the file without the notice.
        yield* persist([...ledger, record]);
        return yield* processRecord(record);
      }),
    );
  });

  const resumePending: ProjectRetirementHandler["resumePending"] = serialized(
    Effect.gen(function* () {
      const ledger = yield* ensureLedger.pipe(Effect.result);
      if (ledger._tag === "Failure") {
        yield* Effect.logWarning("Project retirement resume pass could not read the ledger", {
          code: ledger.failure.code,
          detail: ledger.failure.detail,
        });
        return;
      }
      for (const record of ledger.success) {
        if (record.status !== "pending") {
          continue;
        }
        const outcome = yield* processRecord(record).pipe(Effect.result);
        if (outcome._tag === "Failure") {
          yield* Effect.logWarning(
            "Project retirement cleanup could not finish; retrying on the next pass",
            {
              projectId: record.projectId,
              code: outcome.failure.code,
              detail: outcome.failure.detail,
            },
          );
        }
      }
    }),
  );

  return { handleRetiredNotice, resumePending };
});
