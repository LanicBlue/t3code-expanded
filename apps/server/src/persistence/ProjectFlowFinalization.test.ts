import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import { runMigrations } from "./Migrations.ts";
import * as NodeSqliteClient from "./NodeSqliteClient.ts";
import * as ProjectFlowFinalization from "./ProjectFlowFinalization.ts";

const layer = it.layer(
  ProjectFlowFinalization.layer.pipe(Layer.provideMerge(NodeSqliteClient.layerMemory())),
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
  it.effect("records a pending row once; a duplicate terminal observation is a no-op", () =>
    Effect.gen(function* () {
      yield* runMigrations();
      const store = yield* ProjectFlowFinalization.ProjectFlowFinalizationStore;

      assert.strictEqual(yield* store.record(ROW), "recorded");
      // The same ended instance observed again (a duplicate notification or
      // a later sweep) must not create a second finalization.
      assert.strictEqual(yield* store.record(ROW), "exists");
      // Even a different eventId for the same (instance, agent) — a terminal
      // marker rewritten upstream — keeps the first finalization.
      assert.strictEqual(
        yield* store.record({ ...ROW, eventId: "flow-ended:fi_1:evt_term_2" }),
        "exists",
      );

      const pending = yield* store.listPending();
      assert.lengthOf(pending, 1);
      assert.strictEqual(pending[0]?.instanceId, "fi_1");
      assert.strictEqual(pending[0]?.threadId, "thread_1");
      assert.strictEqual(pending[0]?.state, "pending");
    }),
  );

  it.effect("a no-session record lands directly as done and never drives", () =>
    Effect.gen(function* () {
      yield* runMigrations();
      const store = yield* ProjectFlowFinalization.ProjectFlowFinalizationStore;

      assert.strictEqual(
        yield* store.record({
          ...ROW,
          instanceId: "fi_noop",
          agentId: "ag_noop",
          threadId: null,
        }),
        "recorded",
      );
      const pending = yield* store.listPending();
      assert.isUndefined(pending.find((row) => row.instanceId === "fi_noop"));
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
        createdAt: "2026-08-25T10:00:02.000Z",
      });
      yield* store.record({
        ...ROW,
        agentId,
        instanceId: "fi_older",
        threadId: "thread_0",
        createdAt: "2026-08-25T10:00:01.000Z",
      });

      assert.deepEqual(yield* mine(), ["fi_older", "fi_newer"]);

      yield* store.markDone({
        instanceId: "fi_newer",
        agentId,
        resolvedAt: "2026-08-25T10:00:03.000Z",
      });
      assert.deepEqual(yield* mine(), ["fi_older"]);
      // Idempotent: a done row stays done.
      yield* store.markDone({
        instanceId: "fi_newer",
        agentId,
        resolvedAt: "2026-08-25T10:00:04.000Z",
      });
      assert.deepEqual(yield* mine(), ["fi_older"]);
    }),
  );
});
