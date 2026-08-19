import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

/**
 * Threads woken by Project Service work routing carry the logical agent they
 * serve, so MCP tool calls resolve agent identity from the session binding
 * instead of "the one Project-enabled agent on the provider instance".
 * Nullable: human-created threads and pre-migration rows have none.
 */
export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const columns = yield* sql<{ readonly name: string }>`
    PRAGMA table_info(projection_threads)
  `;

  if (!columns.some((column) => column.name === "logical_agent_id")) {
    yield* sql`
      ALTER TABLE projection_threads
      ADD COLUMN logical_agent_id TEXT
    `;
  }
});
