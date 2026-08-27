import * as NodeAssert from "node:assert/strict";

import { describe, it } from "@effect/vitest";

import type { AssignedWorkQueueEntry } from "./AssignedWorkQueue.ts";
import {
  assignedWorkTaskSummary,
  assignedWorkWakeMessage,
  currentAssignedWork,
  flowInstanceKeyOf,
  flowInstanceNameOf,
  missionKeyOf,
  missionNameOf,
  orderAssignedWorkQueue,
  partitionOpenWork,
  runsForWorkGroup,
  workGroupKeyOf,
  workGroupNameOf,
} from "./AssignedWorkQueue.ts";

const run = (overrides: Partial<AssignedWorkQueueEntry>): AssignedWorkQueueEntry => ({
  runId: "run_a",
  positionId: "pos_1",
  runRevision: "run:1",
  state: "open",
  task: { prompt: "do the thing" },
  createdAt: "2026-08-21T00:00:00.000Z",
  ...overrides,
});

describe("AssignedWorkQueue", () => {
  it("orders open runs oldest-first with the runId as tie-break and drops non-open runs", () => {
    const ordered = orderAssignedWorkQueue([
      run({ runId: "run_new", createdAt: "2026-08-21T00:00:09.000Z" }),
      run({ runId: "run_b", runRevision: "run:1", createdAt: "2026-08-21T00:00:02.000Z" }),
      run({ runId: "run_a", runRevision: "run:1", createdAt: "2026-08-21T00:00:02.000Z" }),
      run({ runId: "run_old_done", state: "completed", createdAt: "2026-08-21T00:00:01.000Z" }),
      run({ runId: "run_old_gone", state: "superseded", createdAt: "2026-08-21T00:00:00.000Z" }),
    ]);
    NodeAssert.deepEqual(
      ordered.map((entry) => entry.runId),
      ["run_a", "run_b", "run_new"],
    );
    NodeAssert.equal(currentAssignedWork(ordered)?.runId, "run_a");
    NodeAssert.equal(currentAssignedWork([]), null);
    // All non-open: the queue is empty even though runs exist.
    NodeAssert.equal(
      currentAssignedWork([run({ state: "cancelled" }), run({ state: "completed" })]),
      null,
    );
  });

  it("the task summary prefers the prompt and never leads with snapshot identifiers", () => {
    // The wire `task` is the run's full snapshot record, not a prompt record:
    // workDefinitionId and a 64-hex semanticFingerprint ride alongside the
    // prompt. The summary must lead with the prompt, not the metadata.
    const snapshotTask = {
      workDefinitionId: "release-notes.write-draft",
      owner: { type: "agent" },
      prompt: "Write the draft release notes for v2.3",
      semanticFingerprint: "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1b2",
      documentRights: { read: [], write: [] },
    };
    NodeAssert.equal(
      assignedWorkTaskSummary(snapshotTask),
      "Write the draft release notes for v2.3",
    );
    // No prompt: fall back to remaining string values, joined in key order.
    NodeAssert.equal(
      assignedWorkTaskSummary({ id: "x", note: "handle the flake" }),
      "x — handle the flake",
    );
    // No string values at all: trimmed JSON, and an empty payload degrades to
    // a stable placeholder instead of empty brackets.
    NodeAssert.equal(assignedWorkTaskSummary({ count: 3 }), '"count":3');
    NodeAssert.equal(assignedWorkTaskSummary({}), "no task text");
    NodeAssert.equal(assignedWorkTaskSummary({ prompt: "   " }), "no task text");
    // Whitespace collapses; the prompt passes through IN FULL — its tail
    // carries the completion rules, so nothing is truncated.
    NodeAssert.equal(
      assignedWorkTaskSummary({ prompt: "line one\n  line two" }),
      "line one line two",
    );
    const long = "x".repeat(250);
    NodeAssert.equal(assignedWorkTaskSummary({ prompt: long }), long);
    NodeAssert.equal(assignedWorkTaskSummary({ prompt: long }).includes("…"), false);
  });

  it("the wake message names the current work and the queue depth behind it", () => {
    const current = run({ task: { prompt: "分诊：修复登录" } });
    NodeAssert.equal(
      assignedWorkWakeMessage({ current, queued: 0 }),
      "Your current work: 分诊：修复登录. Use the t3-code Agent Project tools (project_work_list, project_doc_read, project_doc_write, project_doc_edit, and project_work_submit) to inspect and complete the current work first. Do not use human PS Control tools for Agent Work.",
    );
    NodeAssert.equal(
      assignedWorkWakeMessage({ current, queued: 1 }),
      "Your current work: 分诊：修复登录. 1 more item waiting behind it. Use the t3-code Agent Project tools (project_work_list, project_doc_read, project_doc_write, project_doc_edit, and project_work_submit) to inspect and complete the current work first. Do not use human PS Control tools for Agent Work.",
    );
    NodeAssert.equal(
      assignedWorkWakeMessage({ current, queued: 3 }),
      "Your current work: 分诊：修复登录. 3 more items waiting behind it. Use the t3-code Agent Project tools (project_work_list, project_doc_read, project_doc_write, project_doc_edit, and project_work_submit) to inspect and complete the current work first. Do not use human PS Control tools for Agent Work.",
    );
  });

  it("flow-instance keys read task.instance.instanceId structurally, blank and absent facts degrade to the legacy bucket", () => {
    const instanceRun = run({
      task: {
        prompt: "x",
        instance: { instanceId: "inst_1", name: "  Release  , v2  ", iteration: 1 },
      },
    });
    NodeAssert.equal(flowInstanceKeyOf(instanceRun), "inst_1");
    NodeAssert.equal(flowInstanceNameOf(instanceRun), "Release  , v2");
    // Absent, non-object, non-string, and blank facts all degrade.
    NodeAssert.equal(flowInstanceKeyOf(run({ task: { prompt: "x" } })), "");
    NodeAssert.equal(flowInstanceNameOf(run({ task: { prompt: "x" } })), null);
    NodeAssert.equal(flowInstanceKeyOf(run({ task: { prompt: "x", instance: null } })), "");
    NodeAssert.equal(flowInstanceNameOf(run({ task: { prompt: "x", instance: "inst_1" } })), null);
    NodeAssert.equal(
      flowInstanceKeyOf(run({ task: { prompt: "x", instance: { instanceId: "   " } } })),
      "",
    );
    NodeAssert.equal(
      flowInstanceNameOf(run({ task: { prompt: "x", instance: { instanceId: "i", name: "  " } } })),
      null,
    );
    NodeAssert.equal(
      flowInstanceNameOf(run({ task: { prompt: "x", instance: { instanceId: "i" } } })),
      null,
    );
  });

  it("partitionOpenWork groups OPEN runs by instance key in first-appearance order", () => {
    const partitions = partitionOpenWork([
      run({
        runId: "run_b1",
        task: { prompt: "x", instance: { instanceId: "inst_b", name: "b", iteration: 1 } },
      }),
      run({ runId: "run_legacy", task: { prompt: "x" } }),
      run({
        runId: "run_a1",
        task: { prompt: "x", instance: { instanceId: "inst_a", name: "a", iteration: 1 } },
      }),
      run({
        runId: "run_b2",
        task: { prompt: "x", instance: { instanceId: "inst_b", name: "b", iteration: 1 } },
      }),
      // Settled runs never form or join a partition.
      run({
        runId: "run_c_done",
        state: "completed",
        task: { prompt: "x", instance: { instanceId: "inst_c", name: "c", iteration: 1 } },
      }),
    ]);
    NodeAssert.deepEqual(
      [...partitions.entries()].map(([key, entries]) => [key, entries.map((entry) => entry.runId)]),
      [
        ["inst_b", ["run_b1", "run_b2"]],
        ["", ["run_legacy"]],
        ["inst_a", ["run_a1"]],
      ],
    );
  });

  it("runsForWorkGroup: null owns every run, a string owns that group's runs", () => {
    const aRun = run({
      runId: "run_a",
      task: { prompt: "x", instance: { instanceId: "inst_a", name: "a", iteration: 1 } },
    });
    const legacyRun = run({ runId: "run_legacy", task: { prompt: "x" } });
    const all = [aRun, legacyRun];
    // Project scope: identity (every run, whatever its state).
    NodeAssert.deepEqual(
      runsForWorkGroup(all, null).map((entry) => entry.runId),
      ["run_a", "run_legacy"],
    );
    NodeAssert.deepEqual(
      runsForWorkGroup(all, "inst_a").map((entry) => entry.runId),
      ["run_a"],
    );
    NodeAssert.deepEqual(
      runsForWorkGroup(all, "").map((entry) => entry.runId),
      ["run_legacy"],
    );
    NodeAssert.deepEqual(runsForWorkGroup([], "inst_a"), []);
  });

  // ── Drain-period dual population (work-mission-v5 design §6.1) ──────

  /**
   * A visit-view task per the pinned §6.1 contract: task.mission replaces
   * task.instance, task.work carries the group, task.action is the visit
   * completion contract.
   */
  const visitTask = (missionId: string, missionName: string): Record<string, unknown> => ({
    prompt: "以 design.md 为唯一需求 authority 实现",
    mission: { id: missionId, name: missionName, objective: "Ship the release" },
    work: { group: missionId, workKey: "implement", iteration: 1 },
    executor: { type: "agent", executorRef: "client-1:ag_one" },
    action: { kind: "visit", outcomes: ["implementation-ready"], candidates: ["validation"] },
  });

  it("mission keys read task.mission.id structurally, blank and absent facts degrade", () => {
    const visitRun = run({ runId: "run_v", task: visitTask("ms_a", "  Release v2  ") });
    NodeAssert.equal(missionKeyOf(visitRun), "ms_a");
    NodeAssert.equal(missionNameOf(visitRun), "Release v2");
    NodeAssert.equal(workGroupKeyOf(visitRun), "ms_a");
    NodeAssert.equal(workGroupNameOf(visitRun), "Release v2");
    // Absent, non-object, non-string, and blank mission facts degrade —
    // the queue discipline never throws on a task shape.
    NodeAssert.equal(missionKeyOf(run({ runId: "r", task: { prompt: "x" } })), "");
    NodeAssert.equal(missionNameOf(run({ runId: "r", task: { prompt: "x" } })), null);
    NodeAssert.equal(missionKeyOf(run({ runId: "r", task: { prompt: "x", mission: null } })), "");
    NodeAssert.equal(
      missionKeyOf(run({ runId: "r", task: { prompt: "x", mission: "ms_a" } })),
      "",
    );
    NodeAssert.equal(
      missionNameOf(run({ runId: "r", task: { prompt: "x", mission: { id: "m", name: 7 } } })),
      null,
    );
    NodeAssert.equal(
      missionKeyOf(run({ runId: "r", task: { prompt: "x", mission: { id: "   " } } })),
      "",
    );
    NodeAssert.equal(
      missionNameOf(
        run({ runId: "r", task: { prompt: "x", mission: { id: "m", name: "  " } } }),
      ),
      null,
    );
  });

  it("the dual-population group key dispatches: mission runs by mission id, flow runs by instance id", () => {
    // A mission-block task with a blank id still counts as the visit
    // population: workGroupKeyOf must NOT fall through to task.instance
    // (§6.1 pins task.mission presence as the discriminator).
    const blankMissionRun = run({
      runId: "run_blank_mission",
      task: {
        prompt: "x",
        mission: { id: "   ", name: "n", objective: "o" },
        instance: { instanceId: "inst_shadowed", name: "shadow", iteration: 1 },
      },
    });
    NodeAssert.equal(workGroupKeyOf(blankMissionRun), "");
    // The flow population keys by instance id as before.
    const flowRun = run({
      runId: "run_flow",
      task: { prompt: "x", instance: { instanceId: "inst_1", name: "f", iteration: 2 } },
    });
    NodeAssert.equal(workGroupKeyOf(flowRun), "inst_1");
    NodeAssert.equal(workGroupNameOf(flowRun), "f");
    // Standalone work keeps the legacy bucket.
    NodeAssert.equal(workGroupKeyOf(run({ runId: "run_legacy", task: { prompt: "x" } })), "");
  });

  it("partitionOpenWork groups OPEN runs of BOTH populations side by side", () => {
    const partitions = partitionOpenWork([
      run({ runId: "run_m1", task: visitTask("ms_a", "Mission A") }),
      run({ runId: "run_f1", task: { prompt: "x", instance: { instanceId: "inst_f", name: "f", iteration: 1 } } }),
      run({ runId: "run_m2", task: visitTask("ms_a", "Mission A") }),
      run({ runId: "run_m_other", task: visitTask("ms_b", "Mission B") }),
      run({ runId: "run_legacy", task: { prompt: "x" } }),
      // Settled runs never form or join a partition — either population.
      run({ runId: "run_m_done", state: "completed", task: visitTask("ms_c", "C") }),
      run({
        runId: "run_f_done",
        state: "cancelled",
        task: { prompt: "x", instance: { instanceId: "inst_g", name: "g", iteration: 1 } },
      }),
    ]);
    NodeAssert.deepEqual(
      [...partitions.entries()].map(([key, entries]) => [key, entries.map((entry) => entry.runId)]),
      [
        ["ms_a", ["run_m1", "run_m2"]],
        ["inst_f", ["run_f1"]],
        ["ms_b", ["run_m_other"]],
        ["", ["run_legacy"]],
      ],
    );
    // A group's run universe sees only its own population's runs.
    const all = [
      run({ runId: "run_m", task: visitTask("ms_a", "A") }),
      run({
        runId: "run_f",
        task: { prompt: "x", instance: { instanceId: "ms_a", name: "collision-spelled", iteration: 1 } },
      }),
    ];
    // An instance id SPELLED exactly like a mission id compares textually
    // equal — the key is the contract's plain ternary, and the populations'
    // disjoint id namespaces (mission ids are ms_-prefixed content hashes)
    // are what keeps the groups apart, not extra tagging here.
    NodeAssert.deepEqual(
      runsForWorkGroup(all, "ms_a").map((entry) => entry.runId),
      ["run_m", "run_f"],
    );
  });
});
