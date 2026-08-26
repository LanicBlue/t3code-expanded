import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

/**
 * Durable ledgers for Project Service flow-instance finalization.
 *
 * `project_flow_finalizations` keys one finalization by (event_id, agent_id):
 * the derived terminal event identity
 * (`flow-ended:<instanceId>:<completedByEventId>`) plus the consuming agent —
 * the primary key IS the eventId idempotency, so a re-observed terminal
 * instance (duplicate sweeps, restarts) never creates a second finalization
 * for the same agent. A DIFFERENT terminal eventId for the same instance (a
 * theoretical reopen) inserts its own row and supersedes the older one.
 *
 * `project_flow_session_routes` is the restart-safe instance→thread
 * association: the routing path records which session thread served each
 * instance's work while that work was still open, because the Project
 * Service's run lists only ever answer OPEN runs — after the run closes
 * (exactly when finalization needs the association) no live API can recover
 * it. The finalization intake reads this table first, then corrects it
 * against current facts.
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
      state TEXT NOT NULL CHECK (state IN ('pending', 'done', 'superseded')),
      created_at TEXT NOT NULL,
      resolved_at TEXT,
      PRIMARY KEY (event_id, agent_id)
    )
  `;

  yield* sql`
    CREATE INDEX IF NOT EXISTS idx_project_flow_finalizations_state
    ON project_flow_finalizations(state, created_at)
  `;

  yield* sql`
    CREATE INDEX IF NOT EXISTS idx_project_flow_finalizations_instance
    ON project_flow_finalizations(instance_id, agent_id, state)
  `;

  yield* sql`
    CREATE TABLE IF NOT EXISTS project_flow_session_routes (
      instance_id TEXT NOT NULL,
      agent_id TEXT NOT NULL,
      ps_project_id TEXT NOT NULL,
      thread_id TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      PRIMARY KEY (instance_id, agent_id)
    )
  `;
});
