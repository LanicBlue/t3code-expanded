import {
  CONSUMER_CAPABILITIES,
  CONSUMER_CAPABILITY_MISSION_V1,
  CONSUMER_CLIENT_LATEST,
  type MissionEndedNotice,
  type MissionVisitSubmitResult,
  type WorkRunMissionTaskView,
  type WorkRunView,
  type WorkRunWorkTaskView,
} from "@lanicblue/project-consumer";
import type {
  MissionEndedNoticeFacts,
  ProjectWorkMissionRecord,
  ProjectWorkVisitSubmitResult,
  ProjectWorkVisitWorkRecord,
} from "@t3tools/contracts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import * as NodeCrypto from "node:crypto";
import * as NodeURL from "node:url";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";

// apps/server/src/projectService/ → repository root's vendored SDK directory.
const vendorDir = NodeURL.fileURLToPath(
  new URL("../../../../vendor/consumer-sdk/", import.meta.url),
);

it.effect("vendored Project Consumer SDK matches its sha256 sidecar", () =>
  Effect.gen(function* () {
    // The sidecar is the drift check beyond pnpm-lock's own sha512 pinning: a
    // tampered tarball together with an edited lockfile still trips this.
    // Mirrors scripts/verify-vendored-sdk.mjs (shasum-format sidecars), so the
    // invariant is enforced wherever the test suite runs. A missing sidecar
    // fails via the read; a mismatch fails via the digest comparison.
    const fileSystem = yield* FileSystem.FileSystem;
    const entries = yield* fileSystem.readDirectory(vendorDir);
    const tarballs = entries
      .map((entry) => entry.split("/").pop() ?? entry)
      .filter((name) => name.endsWith(".tgz"));
    expect(tarballs.length).toBeGreaterThan(0);

    for (const tarball of tarballs) {
      const sidecar = yield* fileSystem.readFileString(`${vendorDir}${tarball}.sha256`);
      const expected = sidecar.trim().split(/\s+/)[0];
      const actual = NodeCrypto.createHash("sha256")
        .update(yield* fileSystem.readFile(`${vendorDir}${tarball}`))
        .digest("hex");
      expect(actual).toBe(expected);
    }
  }).pipe(Effect.provide(NodeServices.layer)),
);

// ── work-mission-v5 Phase 5: the vendored generation carries the mission line ──
// The install must be the 0.14 artifact (mission.ended frames + the visit-view
// types) — the runtime service's capability gate filters mission.v1 OUT of
// CONSUMER_CAPABILITIES while the settings gate is off, which only works when
// the vendored registry actually carries the token.
it("vendored SDK is the 0.14 mission.v1 generation", () => {
  expect(CONSUMER_CLIENT_LATEST).toBe("0.14.0");
  expect(CONSUMER_CAPABILITY_MISSION_V1).toBe("mission.v1");
  expect(CONSUMER_CAPABILITIES).toContain(CONSUMER_CAPABILITY_MISSION_V1);
});

// Compile-time drift alarms: the Effect schemas in packages/contracts decode
// at T3's trust boundaries, but their SHAPES are pinned by the SDK's exported
// types (single source of truth). Each annotated literal fails to compile when
// the vendored SDK changes a block contracts still decodes with its own schema
// — the two must be updated together, deliberately.
it("contracts' mission vocabulary stays shape-compatible with the vendored SDK", () => {
  // A fully-decoded SDK MissionEndedNotice feeds the settlement intake.
  const notice: MissionEndedNotice = {
    noticeId: "mne_" + "0".repeat(32),
    missionId: "ms_" + "0".repeat(32),
    group: "ms_" + "0".repeat(32),
    disposition: "completed",
    outcome: null,
    workspacePolicy: "managed-worktree",
  };
  const facts: MissionEndedNoticeFacts = notice;
  expect(facts.group).toBe(facts.missionId);

  // The visit-view blocks: SDK block types are assignable to the decoded records.
  const mission: ProjectWorkMissionRecord = {
    id: notice.missionId,
    name: "Ship v5",
    objective: "Land the work-mission model",
  } satisfies WorkRunMissionTaskView;
  const work: ProjectWorkVisitWorkRecord = {
    group: notice.group,
    workKey: "implement",
    iteration: 1,
  } satisfies WorkRunWorkTaskView;
  const submit: ProjectWorkVisitSubmitResult = {
    outcome: "implementation-ready",
  } satisfies MissionVisitSubmitResult;
  expect([mission.id, work.group, submit.outcome]).toEqual([
    notice.missionId,
    notice.group,
    "implementation-ready",
  ]);

  // The top-level CAS echo the visit submit fences on (§6.1: the run's
  // assignmentRevision is the station's, echoed on every read). The SDK brands
  // its wire ids/revisions — cross the brands exactly once, the way the work
  // client's call sites do for trusted local values.
  const run: WorkRunView = {
    runId: "vs_" + "0".repeat(31) + "1",
    projectId: "p1" as WorkRunView["projectId"],
    projectGeneration: 1 as WorkRunView["projectGeneration"],
    positionId: "implement",
    workspaceRef: ("ew_" + "0".repeat(31) + "1") as WorkRunView["workspaceRef"],
    state: "open",
    runRevision: "run:1" as WorkRunView["runRevision"],
    assignmentRevision: "position:3" as NonNullable<WorkRunView["assignmentRevision"]>,
    task: {},
    createdAt: "2026-08-27T00:00:00.000Z",
    resolvedAt: null,
  };
  expect(run.assignmentRevision).toBe("position:3");
});
