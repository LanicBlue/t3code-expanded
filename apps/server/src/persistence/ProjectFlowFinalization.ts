/**
 * ProjectFlowFinalizationStore — durable ledgers for Project Service
 * flow-instance finalization.
 *
 * The finalization table keys one row per (eventId, agentId): the derived
 * terminal event identity (`flow-ended:<instanceId>:<completedByEventId>`)
 * plus the consuming agent — that composite primary key IS the eventId
 * idempotency the design requires (a duplicate terminal observation never
 * creates a second finalization for the same agent; the agent segment exists
 * because one terminal event fans out to every enabled agent's session). A
 * DIFFERENT terminal eventId for the same instance inserts its own row and
 * supersedes the older one — the newest `completedByEventId` wins and the
 * superseded row never drives again. Pending rows are the restart-surviving
 * record, replayed at startup until the session reaches its retention
 * end-state. A recorded no-op (null `threadId`, no session at intake) lands
 * done but stays overturnable: a later association fact for the same event
 * upgrades it back to pending and re-drives it.
 *
 * The session-route table is the instance→thread association ledger: the
 * routing path records which thread served an instance's work while the work
 * was still open (open-run APIs cannot answer after closure — exactly when
 * finalization needs the association), and the finalization intake reads it
 * after a restart, then corrects it against current thread facts.
 *
 * @module ProjectFlowFinalizationStore
 */
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as SqlSchema from "effect/unstable/sql/SqlSchema";

import {
  type PersistenceErrorCorrelation,
  PersistenceSqlError,
  PersistenceDecodeError,
} from "./Errors.ts";

/** A finalization row: `threadId` is null on no-session no-op records. */
export const ProjectFlowFinalizationRecord = Schema.Struct({
  instanceId: Schema.String,
  agentId: Schema.String,
  psProjectId: Schema.String,
  eventId: Schema.String,
  threadId: Schema.NullOr(Schema.String),
  state: Schema.Literals(["pending", "done", "superseded"]),
  createdAt: Schema.String,
  resolvedAt: Schema.NullOr(Schema.String),
});
export type ProjectFlowFinalizationRecord = typeof ProjectFlowFinalizationRecord.Type;

export const RecordProjectFlowFinalizationInput = Schema.Struct({
  instanceId: Schema.String,
  agentId: Schema.String,
  psProjectId: Schema.String,
  eventId: Schema.String,
  threadId: Schema.NullOr(Schema.String),
  createdAt: Schema.String,
});
export type RecordProjectFlowFinalizationInput = typeof RecordProjectFlowFinalizationInput.Type;

export const MarkProjectFlowFinalizationDoneInput = Schema.Struct({
  eventId: Schema.String,
  agentId: Schema.String,
  resolvedAt: Schema.String,
});
export type MarkProjectFlowFinalizationDoneInput = typeof MarkProjectFlowFinalizationDoneInput.Type;

/** One instance→thread association row (the restart-safe routing ledger). */
export const ProjectFlowSessionRouteRecord = Schema.Struct({
  instanceId: Schema.String,
  agentId: Schema.String,
  psProjectId: Schema.String,
  threadId: Schema.String,
  updatedAt: Schema.String,
});
export type ProjectFlowSessionRouteRecord = typeof ProjectFlowSessionRouteRecord.Type;

export const RecordProjectFlowSessionRouteInput = Schema.Struct({
  instanceId: Schema.String,
  agentId: Schema.String,
  psProjectId: Schema.String,
  threadId: Schema.String,
  updatedAt: Schema.String,
});
export type RecordProjectFlowSessionRouteInput = typeof RecordProjectFlowSessionRouteInput.Type;

export const FindProjectFlowSessionRouteInput = Schema.Struct({
  instanceId: Schema.String,
  agentId: Schema.String,
});
export type FindProjectFlowSessionRouteInput = typeof FindProjectFlowSessionRouteInput.Type;

export type ProjectFlowFinalizationStoreError = PersistenceSqlError | PersistenceDecodeError;

function toPersistenceSqlOrDecodeError(
  sqlOperation: string,
  decodeOperation: string,
  correlation?: PersistenceErrorCorrelation,
) {
  return (cause: unknown): ProjectFlowFinalizationStoreError =>
    Schema.isSchemaError(cause)
      ? PersistenceDecodeError.fromSchemaError(decodeOperation, cause, correlation)
      : new PersistenceSqlError({
          operation: sqlOperation,
          ...(correlation === undefined ? {} : { correlation }),
          cause,
        });
}

/**
 * ProjectFlowFinalizationStore — service tag for the flow finalization
 * ledger.
 */
export class ProjectFlowFinalizationStore extends Context.Service<
  ProjectFlowFinalizationStore,
  {
    /**
     * Insert one terminal-observation row idempotently. Resolves:
     * - "recorded" — this (eventId, agentId) was new (older terminal events
     *   of the same instance+agent are superseded by it);
     * - "exists" — a prior intake already owns this exact event;
     * - "upgraded" — the prior intake was a premature no-op (null threadId,
     *   done) and this observation carries a session: the no-op is overturned
     *   back to pending so the late association re-drives.
     */
    readonly record: (
      input: RecordProjectFlowFinalizationInput,
    ) => Effect.Effect<"recorded" | "exists" | "upgraded", ProjectFlowFinalizationStoreError>;

    /** Every unfinished finalization, oldest first (the startup replay set). */
    readonly listPending: () => Effect.Effect<
      ReadonlyArray<ProjectFlowFinalizationRecord>,
      ProjectFlowFinalizationStoreError
    >;

    /** Mark one finalization finished; idempotent (a done row stays done). */
    readonly markDone: (
      input: MarkProjectFlowFinalizationDoneInput,
    ) => Effect.Effect<void, ProjectFlowFinalizationStoreError>;
  }
>()("t3/persistence/ProjectFlowFinalization/ProjectFlowFinalizationStore") {}

/**
 * ProjectFlowSessionRouteStore — the persistent instance→thread association
 * the routing path records while an instance's work is open and the
 * finalization intake reads after the runs have closed (or a restart emptied
 * the live registry).
 */
export class ProjectFlowSessionRouteStore extends Context.Service<
  ProjectFlowSessionRouteStore,
  {
    /** Upsert one association: the newest recorded thread wins. */
    readonly record: (
      input: RecordProjectFlowSessionRouteInput,
    ) => Effect.Effect<void, ProjectFlowFinalizationStoreError>;

    /** The recorded association for one (instance, agent), or null. */
    readonly find: (
      input: FindProjectFlowSessionRouteInput,
    ) => Effect.Effect<ProjectFlowSessionRouteRecord | null, ProjectFlowFinalizationStoreError>;
  }
>()("t3/persistence/ProjectFlowFinalization/ProjectFlowSessionRouteStore") {}

const FinalizationRowSelection = Schema.Struct({
  eventId: Schema.String,
  agentId: Schema.String,
});

export const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  const insertFinalizationRow = SqlSchema.findAll({
    Request: RecordProjectFlowFinalizationInput,
    Result: FinalizationRowSelection,
    execute: (input) => sql`
      INSERT INTO project_flow_finalizations (
        instance_id,
        agent_id,
        ps_project_id,
        event_id,
        thread_id,
        state,
        created_at,
        resolved_at
      )
      VALUES (
        ${input.instanceId},
        ${input.agentId},
        ${input.psProjectId},
        ${input.eventId},
        ${input.threadId},
        ${input.threadId === null ? "done" : "pending"},
        ${input.createdAt},
        ${input.threadId === null ? input.createdAt : null}
      )
      ON CONFLICT (event_id, agent_id)
      DO NOTHING
      RETURNING
        event_id AS "eventId",
        agent_id AS "agentId"
    `,
  });

  // A NEW terminal event identity for an (instance, agent) — the theoretical
  // reopen — supersedes every older finalization of the same pair: the newest
  // completedByEventId wins, and a superseded row never drives again.
  const supersedeOlderRows = SqlSchema.void({
    Request: RecordProjectFlowFinalizationInput,
    execute: (input) => sql`
      UPDATE project_flow_finalizations
      SET state = 'superseded'
      WHERE instance_id = ${input.instanceId}
        AND agent_id = ${input.agentId}
        AND event_id <> ${input.eventId}
        AND state <> 'superseded'
    `,
  });

  // The no-op overturning: an intake that found no session recorded a done
  // row with a null thread; a LATER association fact for the same event
  // upgrades it back to pending so the finalization drives after all.
  const upgradeNoopRow = SqlSchema.findAll({
    Request: RecordProjectFlowFinalizationInput,
    Result: FinalizationRowSelection,
    execute: (input) => sql`
      UPDATE project_flow_finalizations
      SET thread_id = ${input.threadId},
        state = 'pending',
        resolved_at = null
      WHERE event_id = ${input.eventId}
        AND agent_id = ${input.agentId}
        AND thread_id IS NULL
        AND state = 'done'
      RETURNING
        event_id AS "eventId",
        agent_id AS "agentId"
    `,
  });

  const listPendingRows = SqlSchema.findAll({
    Request: Schema.Struct({}),
    Result: ProjectFlowFinalizationRecord,
    execute: () => sql`
      SELECT
        instance_id AS "instanceId",
        agent_id AS "agentId",
        ps_project_id AS "psProjectId",
        event_id AS "eventId",
        thread_id AS "threadId",
        state AS "state",
        created_at AS "createdAt",
        resolved_at AS "resolvedAt"
      FROM project_flow_finalizations
      WHERE state = 'pending'
      ORDER BY created_at ASC, instance_id ASC
    `,
  });

  const markDoneRow = SqlSchema.void({
    Request: MarkProjectFlowFinalizationDoneInput,
    execute: (input) => sql`
      UPDATE project_flow_finalizations
      SET state = 'done',
        resolved_at = ${input.resolvedAt}
      WHERE event_id = ${input.eventId}
        AND agent_id = ${input.agentId}
        AND state = 'pending'
    `,
  });

  const recordFinalization = Effect.fn("ProjectFlowFinalizationStore.record")(function* (
    input: RecordProjectFlowFinalizationInput,
  ) {
    const inserted = yield* insertFinalizationRow(input);
    if (inserted.length > 0) {
      yield* supersedeOlderRows(input);
      return "recorded" as const;
    }
    if (input.threadId === null) {
      return "exists" as const;
    }
    const upgraded = yield* upgradeNoopRow(input);
    return upgraded.length > 0 ? ("upgraded" as const) : ("exists" as const);
  });

  const record = Effect.fn("ProjectFlowFinalizationStore.record")(function* (
    input: RecordProjectFlowFinalizationInput,
  ) {
    return yield* recordFinalization(input).pipe(
      Effect.mapError(
        toPersistenceSqlOrDecodeError(
          "project_flow_finalizations.record",
          "project_flow_finalizations.record.encode",
        ),
      ),
    );
  });

  const listPending = Effect.fn("ProjectFlowFinalizationStore.listPending")(function* () {
    return yield* listPendingRows({}).pipe(
      Effect.mapError(
        toPersistenceSqlOrDecodeError(
          "project_flow_finalizations.listPending",
          "project_flow_finalizations.listPending.decode",
        ),
      ),
    );
  });

  const markDone = Effect.fn("ProjectFlowFinalizationStore.markDone")(function* (
    input: MarkProjectFlowFinalizationDoneInput,
  ) {
    return yield* markDoneRow(input).pipe(
      Effect.mapError(
        toPersistenceSqlOrDecodeError(
          "project_flow_finalizations.markDone",
          "project_flow_finalizations.markDone.encode",
        ),
      ),
    );
  });

  const upsertRouteRow = SqlSchema.void({
    Request: RecordProjectFlowSessionRouteInput,
    execute: (input) => sql`
      INSERT INTO project_flow_session_routes (
        instance_id,
        agent_id,
        ps_project_id,
        thread_id,
        updated_at
      )
      VALUES (
        ${input.instanceId},
        ${input.agentId},
        ${input.psProjectId},
        ${input.threadId},
        ${input.updatedAt}
      )
      ON CONFLICT (instance_id, agent_id)
      DO UPDATE SET
        ps_project_id = excluded.ps_project_id,
        thread_id = excluded.thread_id,
        updated_at = excluded.updated_at
    `,
  });

  const findRouteRows = SqlSchema.findAll({
    Request: FindProjectFlowSessionRouteInput,
    Result: ProjectFlowSessionRouteRecord,
    execute: (input) => sql`
      SELECT
        instance_id AS "instanceId",
        agent_id AS "agentId",
        ps_project_id AS "psProjectId",
        thread_id AS "threadId",
        updated_at AS "updatedAt"
      FROM project_flow_session_routes
      WHERE instance_id = ${input.instanceId}
        AND agent_id = ${input.agentId}
    `,
  });

  const recordRoute = Effect.fn("ProjectFlowSessionRouteStore.record")(function* (
    input: RecordProjectFlowSessionRouteInput,
  ) {
    return yield* upsertRouteRow(input).pipe(
      Effect.mapError(
        toPersistenceSqlOrDecodeError(
          "project_flow_session_routes.record",
          "project_flow_session_routes.record.encode",
        ),
      ),
    );
  });

  const findRoute = Effect.fn("ProjectFlowSessionRouteStore.find")(function* (
    input: FindProjectFlowSessionRouteInput,
  ) {
    const rows = yield* findRouteRows(input).pipe(
      Effect.mapError(
        toPersistenceSqlOrDecodeError(
          "project_flow_session_routes.find",
          "project_flow_session_routes.find.decode",
        ),
      ),
    );
    return rows[0] ?? null;
  });

  return {
    finalization: ProjectFlowFinalizationStore.of({ record, listPending, markDone }),
    sessionRoute: ProjectFlowSessionRouteStore.of({ record: recordRoute, find: findRoute }),
  };
});

export const layer = Layer.effect(
  ProjectFlowFinalizationStore,
  Effect.map(make, (stores) => stores.finalization),
);

export const sessionRouteLayer = Layer.effect(
  ProjectFlowSessionRouteStore,
  Effect.map(make, (stores) => stores.sessionRoute),
);
