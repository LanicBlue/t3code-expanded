import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

/**
 * Durable ledger for Project Service flow-instance finalizations: one row per
 * (instance, agent) intake. The composite primary key IS the eventId
 * idempotency — a re-observed terminal instance never creates a second
 * finalization. Pending rows survive restarts and are replayed at startup;
 * `thread_id` is null on the no-session no-op records.
 */
export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  yield* sql`
    CREATE TABLE IF NOT EXISTS project_flow_finalizations (
      instance_id TEXT NOT NULL,
      agent_id TEXT NOT NULL,
      ps_project_id TEXT NOT NULL,
      event_id TEXT NOT NULL,
      thread_id TEXT,
      state TEXT NOT NULL CHECK (state IN ('pending', 'done')),
      created_at TEXT NOT NULL,
      resolved_at TEXT,
      PRIMARY KEY (instance_id, agent_id)
    )
  `;

  yield* sql`
    CREATE INDEX IF NOT EXISTS idx_project_flow_finalizations_state
    ON project_flow_finalizations(state, created_at)
  `;
});
