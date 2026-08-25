/**
 * ProjectFlowFinalizationStore - durable ledger for Project Service
 * flow-instance finalizations.
 *
 * One row per (instanceId, agentId) intake. The composite primary key IS the
 * eventId idempotency: a terminal instance re-observed by a later sweep (or a
 * duplicate notification) never creates a second finalization. Pending rows
 * are the restart-surviving record — they are replayed at startup and driven
 * until the session reaches its retention end-state, so an ACKed finalization
 * is never lost to a process exit.
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
  state: Schema.Literals(["pending", "done"]),
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
  instanceId: Schema.String,
  agentId: Schema.String,
  resolvedAt: Schema.String,
});
export type MarkProjectFlowFinalizationDoneInput = typeof MarkProjectFlowFinalizationDoneInput.Type;

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
 * ProjectFlowFinalizationStore - service tag for the flow finalization
 * ledger.
 */
export class ProjectFlowFinalizationStore extends Context.Service<
  ProjectFlowFinalizationStore,
  {
    /**
     * Insert one intake row idempotently: resolves "recorded" when this
     * (instanceId, agentId) pair was new, "exists" when a prior intake
     * already owns it (duplicate terminal observation — never a second
     * finalization). A null `threadId` records the no-session no-op case
     * directly as done.
     */
    readonly record: (
      input: RecordProjectFlowFinalizationInput,
    ) => Effect.Effect<"recorded" | "exists", ProjectFlowFinalizationStoreError>;

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

const FinalizationRowSelection = Schema.Struct({
  instanceId: Schema.String,
  agentId: Schema.String,
});

export const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  const recordFinalizationRow = SqlSchema.findAll({
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
      ON CONFLICT (instance_id, agent_id)
      DO NOTHING
      RETURNING
        instance_id AS "instanceId",
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
      WHERE instance_id = ${input.instanceId}
        AND agent_id = ${input.agentId}
        AND state = 'pending'
    `,
  });

  const record = Effect.fn("ProjectFlowFinalizationStore.record")(function* (
    input: RecordProjectFlowFinalizationInput,
  ) {
    const rows = yield* recordFinalizationRow(input).pipe(
      Effect.mapError(
        toPersistenceSqlOrDecodeError(
          "project_flow_finalizations.insert",
          "project_flow_finalizations.insert.encode",
        ),
      ),
    );
    return rows.length > 0 ? ("recorded" as const) : ("exists" as const);
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

  return ProjectFlowFinalizationStore.of({ record, listPending, markDone });
});

export const layer = Layer.effect(ProjectFlowFinalizationStore, make);
