/**
 * The consumer-side work-queue discipline.
 *
 * A logical agent works strictly in arrival order: the OLDEST open assigned
 * run is "the current work"; everything behind it waits until the current
 * work is submitted. The queue is never stored — it is derived from the
 * Project Service's authoritative answer on every query, so T3 keeps no work
 * lifecycle state of its own (the Project Service stays the only ledger).
 *
 * The point of the discipline is cognitive, not scheduling: the agent never
 * chooses among open works, so it can never submit a result to the wrong
 * one. Its entire contract is "take the current work, do it, submit it" —
 * no workflow position, instance, or state knowledge required.
 *
 * @module AssignedWorkQueue
 */
import type * as ProjectServiceWorkClient from "./ProjectServiceWorkClient.ts";

/**
 * A Work run as the queue sees it. The discipline only READS
 * `state`/`createdAt`/`runId`, but the whole record flows through so callers
 * (wake message, MCP handlers) keep the fields they need.
 */
export type AssignedWorkQueueEntry = ProjectServiceWorkClient.ProjectWorkRunRecord;

/**
 * The open runs in queue order: oldest `createdAt` first, `runId` as the
 * deterministic tie-break. `createdAt` is the service's RFC 3339 timestamp,
 * so lexicographic comparison is chronological comparison. Non-open runs are
 * dropped — a completed/superseded/cancelled run is never in the queue.
 */
export const orderAssignedWorkQueue = (
  runs: ReadonlyArray<AssignedWorkQueueEntry>,
): Array<AssignedWorkQueueEntry> =>
  runs
    .filter((run) => run.state === "open")
    .toSorted(
      (left, right) =>
        left.createdAt.localeCompare(right.createdAt) || left.runId.localeCompare(right.runId),
    );

/** The head of the queue — the one run the agent may currently act on. */
export const currentAssignedWork = (
  runs: ReadonlyArray<AssignedWorkQueueEntry>,
): AssignedWorkQueueEntry | null => orderAssignedWorkQueue(runs).at(0) ?? null;

// ── Work-group scoping (work-mission-v5: mission-only population) ──

/**
 * The run's `task.mission` block when present — the visit population's
 * discriminator. Structural read only; the decoded contract view lives on
 * the run record's `visit` field (ProjectServiceWorkClient).
 *
 * work-mission-v5 Phase 7: the flow population's `task.instance` arm is
 * DELETED — the Project Service stopped delivering task.instance runs when
 * it removed the flow stack, structurally and not by convention. A run whose
 * task carries no mission block (standalone work, or a pre-drain frame that
 * can no longer exist) keys to the legacy "" bucket: read as ungrouped,
 * never decoded, never crashed.
 */
const missionBlockOf = (run: AssignedWorkQueueEntry): Record<string, unknown> | null => {
  const mission = run.task.mission;
  return typeof mission === "object" && mission !== null
    ? (mission as Record<string, unknown>)
    : null;
};

/** The run's `task.mission.id` when a non-blank string; "" without one. */
export const missionKeyOf = (run: AssignedWorkQueueEntry): string => {
  const mission = missionBlockOf(run);
  if (mission === null) return "";
  const id = mission.id;
  return typeof id === "string" && id.trim().length > 0 ? id : "";
};

/** The run's `task.mission.name` trimmed; null when absent or blank. */
export const missionNameOf = (run: AssignedWorkQueueEntry): string | null => {
  const mission = missionBlockOf(run);
  if (mission === null) return null;
  const name = mission.name;
  if (typeof name !== "string") return null;
  const trimmed = name.trim();
  return trimmed.length > 0 ? trimmed : null;
};

/**
 * Open runs grouped by the mission key (same open-only discipline as
 * `orderAssignedWorkQueue`, so a partition of only settled runs never exists
 * and no key is ever minted for a dead group). Map iteration order is the
 * keys' first appearance in the input.
 */
export const partitionOpenWork = (
  runs: ReadonlyArray<AssignedWorkQueueEntry>,
): Map<string, Array<AssignedWorkQueueEntry>> => {
  const partitions = new Map<string, Array<AssignedWorkQueueEntry>>();
  for (const run of runs) {
    if (run.state !== "open") continue;
    const key = missionKeyOf(run);
    const partition = partitions.get(key);
    if (partition === undefined) {
      partitions.set(key, [run]);
    } else {
      partition.push(run);
    }
  }
  return partitions;
};

/**
 * A session's run universe: null owns every run (project scope); a string
 * owns that mission's runs, with "" as the legacy no-mission bucket.
 */
export const runsForWorkGroup = (
  runs: ReadonlyArray<AssignedWorkQueueEntry>,
  workGroupKey: string | null,
): Array<AssignedWorkQueueEntry> =>
  workGroupKey === null ? [...runs] : runs.filter((run) => missionKeyOf(run) === workGroupKey);

/**
 * A compact one-line summary of a work task payload for wake messages. The
 * task is the run's free-form snapshot record; prefer its `prompt` value,
 * falling back to the remaining string values, then trimmed JSON. Collapse
 * whitespace; the prompt is passed IN FULL — its tail carries the completion
 * rules the woken agent needs before its first tool call.
 */
export const assignedWorkTaskSummary = (task: Readonly<Record<string, unknown>>): string => {
  // The wire `task` is the run's full snapshot (workDefinitionId, owner,
  // prompt, semanticFingerprint, …) — NOT a prompt record. Prefer the task
  // text explicitly: joining every string value would lead with opaque
  // identifiers and crowd the summary out with the 64-hex fingerprint.
  const prompt =
    typeof task.prompt === "string" && task.prompt.trim().length > 0 ? task.prompt : undefined;
  const otherStrings = Object.entries(task)
    .filter(([key, value]) => key !== "prompt")
    .filter(([, value]) => typeof value === "string" && value.trim().length > 0)
    .map(([, value]) => value as string);
  // JSON fallback only over entries worth showing: blank strings are dropped
  // so a whitespace-only prompt degrades to the placeholder, not `{"prompt":" "}`.
  const fallbackEntries = Object.entries(task ?? {}).filter(
    ([, value]) => !(typeof value === "string" && value.trim().length === 0),
  );
  const raw =
    prompt !== undefined
      ? prompt
      : otherStrings.length > 0
        ? otherStrings.join(" — ")
        : fallbackEntries.length > 0
          ? JSON.stringify(Object.fromEntries(fallbackEntries)).replace(/^\{|\}$/gu, "")
          : undefined;
  const flat = (raw ?? "").replace(/\s+/gu, " ").trim();
  if (flat.length === 0) {
    return "no task text";
  }
  // NO length cap: the work prompt's tail carries the completion rules
  // (which verdicts to submit, what to write) — truncating it mid-sentence
  // sent agents to work half-instructed. Whitespace is still collapsed so
  // the message stays one line.
  return flat;
};

/** What a wake message says: the current work, plus how deep the queue is. */
export const assignedWorkWakeMessage = (input: {
  readonly current: AssignedWorkQueueEntry;
  readonly queued: number;
}): string => {
  const summary = assignedWorkTaskSummary(input.current.task);
  const waiting =
    input.queued > 0
      ? ` ${input.queued} more item${input.queued === 1 ? "" : "s"} waiting behind it.`
      : "";
  return `Your current work: ${summary}.${waiting} Use the t3-code Agent Project tools (project_work_list, project_doc_read, project_doc_write, project_doc_edit, and project_work_submit) to inspect and complete the current work first. Do not use human PS Control tools for Agent Work.`;
};
