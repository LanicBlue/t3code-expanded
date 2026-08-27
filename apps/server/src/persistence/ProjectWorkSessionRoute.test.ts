import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import { runMigrations } from "./Migrations.ts";
import * as NodeSqliteClient from "./NodeSqliteClient.ts";
import * as ProjectWorkSessionRoute from "./ProjectWorkSessionRoute.ts";

const layer = it.layer(
  ProjectWorkSessionRoute.sessionRouteLayer.pipe(
    Layer.provideMerge(NodeSqliteClient.layerMemory()),
  ),
);

layer("ProjectFlowSessionRouteStore", (it) => {
  it.effect("upserts the association and finds it back after the runs close", () =>
    Effect.gen(function* () {
      yield* runMigrations();
      const store = yield* ProjectWorkSessionRoute.ProjectFlowSessionRouteStore;

      yield* store.record({
        instanceId: "ms_1",
        agentId: "ag_primary",
        psProjectId: "ps_proj_1",
        threadId: "thread_1",
        updatedAt: "2026-08-25T09:00:00.000Z",
      });
      assert.deepEqual(yield* store.find({ instanceId: "ms_1", agentId: "ag_primary" }), {
        instanceId: "ms_1",
        agentId: "ag_primary",
        psProjectId: "ps_proj_1",
        threadId: "thread_1",
        updatedAt: "2026-08-25T09:00:00.000Z",
      });

      // A newer delivering thread REPLACES the association (last writer
      // wins — the newest session that served the group's work).
      yield* store.record({
        instanceId: "ms_1",
        agentId: "ag_primary",
        psProjectId: "ps_proj_1",
        threadId: "thread_2",
        updatedAt: "2026-08-25T09:30:00.000Z",
      });
      assert.strictEqual(
        (yield* store.find({ instanceId: "ms_1", agentId: "ag_primary" }))?.threadId,
        "thread_2",
      );

      // Unknown pairs read as null (no association), never as an error.
      assert.isNull(yield* store.find({ instanceId: "ms_missing", agentId: "ag_primary" }));
      // Other agents' associations never bleed across the agent segment.
      assert.isNull(yield* store.find({ instanceId: "ms_1", agentId: "ag_other" }));
    }),
  );
});
