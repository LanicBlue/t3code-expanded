/**
 * ProjectWorkSessionRouteStore — the durable work-group→thread association
 * ledger (`project_flow_session_routes`).
 *
 * The routing path records which session thread served a work group's runs
 * while that work was still open, because the Project Service's run lists
 * only ever answer OPEN runs — after the runs close (exactly when a terminal
 * notice arrives) no live API can recover the association. The
 * `mission.ended` settlement reads this table as its restart-safe recovery
 * path (a restart empties the in-memory registry; these rows survive).
 *
 * work-mission-v5 Phase 7: the flow population's drivers are deleted — the
 * flow-finalization ledger (`project_flow_finalizations`) is RETAINED IN THE
 * DATABASE as a read-only audit artifact (historical rows reference it; no
 * code reads or writes it anymore), and this store is the surviving half of
 * the old ProjectFlowFinalization module, serving the mission line's
 * mission-id-keyed associations. Rows recorded before the drain (flow
 * instance ids) stay untouched in the same table.
 *
 * @module ProjectWorkSessionRouteStore
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

/** One work-group→thread association row (the restart-safe routing ledger). */
export const ProjectWorkSessionRouteRecord = Schema.Struct({
  /** The work-group key the sessions live under (the mission id). */
  instanceId: Schema.String,
  agentId: Schema.String,
  psProjectId: Schema.String,
  threadId: Schema.String,
  updatedAt: Schema.String,
});
export type ProjectWorkSessionRouteRecord = typeof ProjectWorkSessionRouteRecord.Type;

export const RecordProjectWorkSessionRouteInput = Schema.Struct({
  instanceId: Schema.String,
  agentId: Schema.String,
  psProjectId: Schema.String,
  threadId: Schema.String,
  updatedAt: Schema.String,
});
export type RecordProjectWorkSessionRouteInput = typeof RecordProjectWorkSessionRouteInput.Type;

export const FindProjectWorkSessionRouteInput = Schema.Struct({
  instanceId: Schema.String,
  agentId: Schema.String,
});
export type FindProjectWorkSessionRouteInput = typeof FindProjectWorkSessionRouteInput.Type;

export type ProjectWorkSessionRouteStoreError = PersistenceSqlError | PersistenceDecodeError;

function toPersistenceSqlOrDecodeError(
  sqlOperation: string,
  decodeOperation: string,
  correlation?: PersistenceErrorCorrelation,
) {
  return (cause: unknown): ProjectWorkSessionRouteStoreError =>
    Schema.isSchemaError(cause)
      ? PersistenceDecodeError.fromSchemaError(decodeOperation, cause, correlation)
      : new PersistenceSqlError({
          operation: sqlOperation,
          ...(correlation === undefined ? {} : { correlation }),
          cause,
        });
}

/**
 * ProjectFlowSessionRouteStore — the persistent work-group→thread association
 * the routing path records while a group's work is open and the
 * `mission.ended` settlement reads after the runs have closed (or a restart
 * emptied the live registry).
 */
export class ProjectFlowSessionRouteStore extends Context.Service<
  ProjectFlowSessionRouteStore,
  {
    /** Upsert one association: the newest recorded thread wins. */
    readonly record: (
      input: RecordProjectWorkSessionRouteInput,
    ) => Effect.Effect<void, ProjectWorkSessionRouteStoreError>;

    /** The recorded association for one (group, agent), or null. */
    readonly find: (
      input: FindProjectWorkSessionRouteInput,
    ) => Effect.Effect<ProjectWorkSessionRouteRecord | null, ProjectWorkSessionRouteStoreError>;
  }
>()("t3/persistence/ProjectWorkSessionRoute/ProjectFlowSessionRouteStore") {}

export const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  const upsertRouteRow = SqlSchema.void({
    Request: RecordProjectWorkSessionRouteInput,
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
    Request: FindProjectWorkSessionRouteInput,
    Result: ProjectWorkSessionRouteRecord,
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
    input: RecordProjectWorkSessionRouteInput,
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
    input: FindProjectWorkSessionRouteInput,
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

  return ProjectFlowSessionRouteStore.of({ record: recordRoute, find: findRoute });
});

export const sessionRouteLayer = Layer.effect(ProjectFlowSessionRouteStore, make);
