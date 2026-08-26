import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import { runMigrations } from "./Migrations.ts";
import * as NodeSqliteClient from "./NodeSqliteClient.ts";
import * as ProjectFlowFinalization from "./ProjectFlowFinalization.ts";

const layer = it.layer(
  Layer.merge(ProjectFlowFinalization.layer, ProjectFlowFinalization.sessionRouteLayer).pipe(
    Layer.provideMerge(NodeSqliteClient.layerMemory()),
  ),
);

const ROW = {
  instanceId: "fi_1",
  agentId: "ag_primary",
  psProjectId: "ps_proj_1",
  eventId: "flow-ended:fi_1:evt_term_1",
  threadId: "thread_1",
  createdAt: "2026-08-25T10:00:00.000Z",
} as const;

layer("ProjectFlowFinalizationStore", (it) => {
  it.effect("records one row per event id: duplicate observations are no-ops", () =>
    Effect.gen(function* () {
      yield* runMigrations();
      const store = yield* ProjectFlowFinalization.ProjectFlowFinalizationStore;

      assert.strictEqual(yield* store.record(ROW), "recorded");
      // The same ended instance observed again (a duplicate notification or
      // a later sweep) must not create a second finalization: the
      // (event_id, agent_id) key IS the idempotency.
      assert.strictEqual(yield* store.record(ROW), "exists");
      // A second agent observing the SAME terminal event gets its OWN row
      // (the event identity fans out per agent; a bare event_id key would
      // silently drop the second agent's finalization).
      assert.strictEqual(yield* store.record({ ...ROW, agentId: "ag_other" }), "recorded");

      const pending = yield* store.listPending();
      assert.lengthOf(pending, 2);
      assert.strictEqual(pending.find((row) => row.agentId === "ag_primary")?.threadId, "thread_1");
      assert.strictEqual(pending.find((row) => row.agentId === "ag_other")?.threadId, "thread_1");
    }),
  );

  it.effect("a different terminal event supersedes the older finalization", () =>
    Effect.gen(function* () {
      yield* runMigrations();
      const store = yield* ProjectFlowFinalization.ProjectFlowFinalizationStore;
      const agentId = "ag_reopen";
      const mine = () =>
        Effect.map(store.listPending(), (rows) =>
          rows.filter((row) => row.agentId === agentId).map((row) => row.eventId),
        );

      yield* store.record({ ...ROW, agentId });
      // The theoretical reopen: the same instance ends AGAIN under a new
      // completedByEventId. The newest event wins; the older row is marked
      // superseded and never drives again.
      assert.strictEqual(
        yield* store.record({
          ...ROW,
          agentId,
          eventId: "flow-ended:fi_1:evt_term_2",
          threadId: "thread_2",
          createdAt: "2026-08-25T11:00:00.000Z",
        }),
        "recorded",
      );

      assert.deepEqual(yield* mine(), ["flow-ended:fi_1:evt_term_2"]);
    }),
  );

  it.effect("a recorded no-op is overturned by a late association for the same event", () =>
    Effect.gen(function* () {
      yield* runMigrations();
      const store = yield* ProjectFlowFinalization.ProjectFlowFinalizationStore;
      const noopRow = {
        ...ROW,
        instanceId: "fi_noop",
        agentId: "ag_noop",
        eventId: "flow-ended:fi_noop:evt_noop",
      };
      const mine = () =>
        Effect.map(store.listPending(), (rows) =>
          rows.filter((row) => row.agentId === noopRow.agentId),
        );

      // No association fact at intake: the no-op lands done and never drives.
      assert.strictEqual(yield* store.record({ ...noopRow, threadId: null }), "recorded");
      assert.lengthOf(yield* mine(), 0);

      // The association arrives LATER (a delivery racing the sweep, or the
      // persisted route row landing after the read): the done no-op flips
      // back to pending and drives.
      assert.strictEqual(yield* store.record({ ...noopRow, threadId: "thread_late" }), "upgraded");
      const pending = yield* mine();
      assert.lengthOf(pending, 1);
      assert.strictEqual(pending[0]?.threadId, "thread_late");
      assert.strictEqual(pending[0]?.state, "pending");

      // A third observation with no NEW fact changes nothing.
      assert.strictEqual(yield* store.record({ ...noopRow, threadId: null }), "exists");
      assert.lengthOf(yield* mine(), 1);
    }),
  );

  it.effect("markDone finishes a pending row, oldest-first ordering, idempotently", () =>
    Effect.gen(function* () {
      yield* runMigrations();
      const store = yield* ProjectFlowFinalization.ProjectFlowFinalizationStore;

      const agentId = "ag_order";
      const mine = () =>
        Effect.map(store.listPending(), (rows) =>
          rows.filter((row) => row.agentId === agentId).map((row) => row.instanceId),
        );
      yield* store.record({
        ...ROW,
        agentId,
        instanceId: "fi_newer",
        eventId: "flow-ended:fi_newer:evt_1",
        createdAt: "2026-08-25T10:00:02.000Z",
      });
      yield* store.record({
        ...ROW,
        agentId,
        instanceId: "fi_older",
        eventId: "flow-ended:fi_older:evt_1",
        threadId: "thread_0",
        createdAt: "2026-08-25T10:00:01.000Z",
      });

      assert.deepEqual(yield* mine(), ["fi_older", "fi_newer"]);

      yield* store.markDone({
        eventId: "flow-ended:fi_newer:evt_1",
        agentId,
        resolvedAt: "2026-08-25T10:00:03.000Z",
      });
      assert.deepEqual(yield* mine(), ["fi_older"]);
      // Idempotent: a done row stays done.
      yield* store.markDone({
        eventId: "flow-ended:fi_newer:evt_1",
        agentId,
        resolvedAt: "2026-08-25T10:00:04.000Z",
      });
      assert.deepEqual(yield* mine(), ["fi_older"]);
    }),
  );
});

layer("ProjectFlowSessionRouteStore", (it) => {
  it.effect("upserts the association and finds it back after the runs close", () =>
    Effect.gen(function* () {
      yield* runMigrations();
      const store = yield* ProjectFlowFinalization.ProjectFlowSessionRouteStore;

      yield* store.record({
        instanceId: "fi_1",
        agentId: "ag_primary",
        psProjectId: "ps_proj_1",
        threadId: "thread_1",
        updatedAt: "2026-08-25T09:00:00.000Z",
      });
      assert.deepEqual(yield* store.find({ instanceId: "fi_1", agentId: "ag_primary" }), {
        instanceId: "fi_1",
        agentId: "ag_primary",
        psProjectId: "ps_proj_1",
        threadId: "thread_1",
        updatedAt: "2026-08-25T09:00:00.000Z",
      });

      // A newer delivering thread REPLACES the association (last writer
      // wins — the newest session that served the instance's work).
      yield* store.record({
        instanceId: "fi_1",
        agentId: "ag_primary",
        psProjectId: "ps_proj_1",
        threadId: "thread_2",
        updatedAt: "2026-08-25T09:30:00.000Z",
      });
      assert.strictEqual(
        (yield* store.find({ instanceId: "fi_1", agentId: "ag_primary" }))?.threadId,
        "thread_2",
      );

      // Unknown pairs read as null (no association), never as an error.
      assert.isNull(yield* store.find({ instanceId: "fi_missing", agentId: "ag_primary" }));
      // Other agents' associations never bleed across the agent segment.
      assert.isNull(yield* store.find({ instanceId: "fi_1", agentId: "ag_other" }));
    }),
  );
});
