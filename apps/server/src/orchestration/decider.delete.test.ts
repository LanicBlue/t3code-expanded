import {
  CommandId,
  DEFAULT_PROVIDER_INTERACTION_MODE,
  EventId,
  MessageId,
  ProjectId,
  ThreadId,
  type OrchestrationCommand,
  type OrchestrationEvent,
  type OrchestrationReadModel,
  type OrchestrationThread,
  ProviderInstanceId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";

import { decideOrchestrationCommand } from "./decider.ts";
import { createEmptyReadModel, projectEvent } from "./projector.ts";

const asCommandId = (value: string): CommandId => CommandId.make(value);
const asEventId = (value: string): EventId => EventId.make(value);
const asProjectId = (value: string): ProjectId => ProjectId.make(value);
const asThreadId = (value: string): ThreadId => ThreadId.make(value);

const seedReadModel = Effect.gen(function* () {
  const now = "2026-01-01T00:00:00.000Z";
  const initial = createEmptyReadModel(now);
  const withProject = yield* projectEvent(initial, {
    sequence: 1,
    eventId: asEventId("evt-project-create"),
    aggregateKind: "project",
    aggregateId: asProjectId("project-delete"),
    type: "project.created",
    occurredAt: now,
    commandId: asCommandId("cmd-project-create"),
    causationEventId: null,
    correlationId: asCommandId("cmd-project-create"),
    metadata: {},
    payload: {
      projectId: asProjectId("project-delete"),
      title: "Project Delete",
      workspaceRoot: "/tmp/project-delete",
      defaultModelSelection: null,
      scripts: [],
      createdAt: now,
      updatedAt: now,
    },
  });

  const withFirstThread = yield* projectEvent(withProject, {
    sequence: 2,
    eventId: asEventId("evt-thread-create-1"),
    aggregateKind: "thread",
    aggregateId: asThreadId("thread-delete-1"),
    type: "thread.created",
    occurredAt: now,
    commandId: asCommandId("cmd-thread-create-1"),
    causationEventId: null,
    correlationId: asCommandId("cmd-thread-create-1"),
    metadata: {},
    payload: {
      threadId: asThreadId("thread-delete-1"),
      projectId: asProjectId("project-delete"),
      title: "Thread Delete 1",
      modelSelection: {
        instanceId: ProviderInstanceId.make("codex"),
        model: "gpt-5-codex",
      },
      interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
      runtimeMode: "approval-required",
      branch: null,
      worktreePath: null,
      createdAt: now,
      updatedAt: now,
    },
  });

  return yield* projectEvent(withFirstThread, {
    sequence: 3,
    eventId: asEventId("evt-thread-create-2"),
    aggregateKind: "thread",
    aggregateId: asThreadId("thread-delete-2"),
    type: "thread.created",
    occurredAt: now,
    commandId: asCommandId("cmd-thread-create-2"),
    causationEventId: null,
    correlationId: asCommandId("cmd-thread-create-2"),
    metadata: {},
    payload: {
      threadId: asThreadId("thread-delete-2"),
      projectId: asProjectId("project-delete"),
      title: "Thread Delete 2",
      modelSelection: {
        instanceId: ProviderInstanceId.make("codex"),
        model: "gpt-5-codex",
      },
      interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
      runtimeMode: "approval-required",
      branch: null,
      worktreePath: null,
      createdAt: now,
      updatedAt: now,
    },
  });
});

type PlannedEvent = Omit<OrchestrationEvent, "sequence">;

function normalizeDeleteEvent(event: PlannedEvent | ReadonlyArray<PlannedEvent>) {
  const events = Array.isArray(event) ? event : [event];
  return events.map((entry) => {
    switch (entry.type) {
      case "thread.deleted":
        return {
          type: entry.type,
          aggregateKind: entry.aggregateKind,
          aggregateId: entry.aggregateId,
          commandId: entry.commandId,
          correlationId: entry.correlationId,
          payload: {
            threadId: entry.payload.threadId,
          },
        };
      case "project.deleted":
        return {
          type: entry.type,
          aggregateKind: entry.aggregateKind,
          aggregateId: entry.aggregateId,
          commandId: entry.commandId,
          correlationId: entry.correlationId,
          payload: {
            projectId: entry.payload.projectId,
          },
        };
      default:
        return entry;
    }
  });
}

it.layer(NodeServices.layer)("decider deletion flows", (it) => {
  it.effect("rejects deleting a non-empty project without force", () =>
    Effect.gen(function* () {
      const readModel = yield* seedReadModel;
      const error = yield* Effect.flip(
        decideOrchestrationCommand({
          command: {
            type: "project.delete",
            commandId: asCommandId("cmd-project-delete-no-force"),
            projectId: asProjectId("project-delete"),
          },
          readModel,
        }),
      );
      expect(error.message).toContain("cannot be deleted without force=true");
    }),
  );

  it.effect("reuses thread.delete semantics when force-deleting a non-empty project", () =>
    Effect.gen(function* () {
      const readModel = yield* seedReadModel;
      const projectDeleteCommand: Extract<OrchestrationCommand, { type: "project.delete" }> = {
        type: "project.delete",
        commandId: asCommandId("cmd-project-delete-force"),
        projectId: asProjectId("project-delete"),
        force: true,
      };

      const forcedResult = yield* decideOrchestrationCommand({
        command: projectDeleteCommand,
        readModel,
      });
      const forcedEvents = Array.isArray(forcedResult) ? forcedResult : [forcedResult];

      expect(forcedEvents.map((event) => event.type)).toEqual([
        "thread.deleted",
        "thread.deleted",
        "project.deleted",
      ]);

      let sequentialReadModel = readModel;
      let nextSequence = readModel.snapshotSequence;
      const sequentialEvents: PlannedEvent[] = [];
      for (const nextCommand of [
        {
          type: "thread.delete",
          commandId: projectDeleteCommand.commandId,
          threadId: asThreadId("thread-delete-1"),
        },
        {
          type: "thread.delete",
          commandId: projectDeleteCommand.commandId,
          threadId: asThreadId("thread-delete-2"),
        },
        {
          type: "project.delete",
          commandId: projectDeleteCommand.commandId,
          projectId: asProjectId("project-delete"),
        },
      ] satisfies ReadonlyArray<OrchestrationCommand>) {
        const decided = yield* decideOrchestrationCommand({
          command: nextCommand,
          readModel: sequentialReadModel,
        });
        const nextEvents = Array.isArray(decided) ? decided : [decided];
        sequentialEvents.push(...nextEvents);
        for (const nextEvent of nextEvents) {
          nextSequence += 1;
          sequentialReadModel = yield* projectEvent(sequentialReadModel, {
            ...nextEvent,
            sequence: nextSequence,
          });
        }
      }

      expect(normalizeDeleteEvent(forcedResult)).toEqual(normalizeDeleteEvent(sequentialEvents));
    }),
  );
});

// ── Idle-required delete (flow-instance retention) ───────────────

const IDLE_NOW = "2026-01-01T00:00:00.000Z";

function makeIdleGuardReadModel(input: {
  readonly sessionStatus?: "starting" | "running" | "stopped" | null;
  readonly activities?: OrchestrationThread["activities"];
  readonly messages?: OrchestrationThread["messages"];
}): OrchestrationReadModel {
  return {
    snapshotSequence: 0,
    projects: [],
    threads: [
      {
        id: asThreadId("thread-idle-guard"),
        projectId: asProjectId("project-delete"),
        title: "Flow Instance Session",
        modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5.4" },
        runtimeMode: "full-access",
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        branch: null,
        worktreePath: "/wt/instance",
        latestTurn: null,
        createdAt: IDLE_NOW,
        updatedAt: IDLE_NOW,
        archivedAt: null,
        settledOverride: null,
        settledAt: null,
        snoozedUntil: null,
        snoozedAt: null,
        pinnedAt: null,
        deletedAt: null,
        messages: input.messages ?? [],
        proposedPlans: [],
        activities: input.activities ?? [],
        checkpoints: [],
        session:
          input.sessionStatus === null || input.sessionStatus === undefined
            ? null
            : {
                threadId: asThreadId("thread-idle-guard"),
                status: input.sessionStatus,
                providerName: "Codex",
                runtimeMode: "full-access",
                activeTurnId: null,
                lastError: null,
                updatedAt: IDLE_NOW,
              },
      },
    ],
    updatedAt: IDLE_NOW,
  };
}

const guardActivity = (
  kind: string,
  requestId: string,
): OrchestrationThread["activities"][number] =>
  ({
    id: asEventId(`activity-${requestId}-${kind}`),
    tone: "approval" as const,
    kind,
    summary: kind,
    payload: { requestId },
    turnId: null,
    createdAt: IDLE_NOW,
  }) as OrchestrationThread["activities"][number];

const guardUserMessage = (createdAt: string): OrchestrationThread["messages"][number] =>
  ({
    id: MessageId.make(`message-${createdAt}`),
    role: "user",
    text: "one more thing",
    turnId: null,
    streaming: false,
    createdAt,
    updatedAt: createdAt,
  }) as OrchestrationThread["messages"][number];

it.layer(NodeServices.layer)("decider idle-required thread delete", (it) => {
  it.effect("rejects an idle-required delete into a running or starting session", () =>
    Effect.gen(function* () {
      for (const status of ["starting", "running"] as const) {
        const error = yield* Effect.flip(
          decideOrchestrationCommand({
            command: {
              type: "thread.delete",
              commandId: asCommandId(`cmd-idle-delete-${status}`),
              threadId: asThreadId("thread-idle-guard"),
              requireIdle: true,
            },
            readModel: makeIdleGuardReadModel({ sessionStatus: status }),
          }),
        );
        expect(error._tag).toBe("OrchestrationCommandInvariantError");
        expect(error.message).toContain("cannot be deleted while idle-required");
      }
    }),
  );

  it.effect("rejects an idle-required delete behind an open human request or queued turn", () =>
    Effect.gen(function* () {
      // Open approval request.
      const approvalError = yield* Effect.flip(
        decideOrchestrationCommand({
          command: {
            type: "thread.delete",
            commandId: asCommandId("cmd-idle-delete-approval"),
            threadId: asThreadId("thread-idle-guard"),
            requireIdle: true,
          },
          readModel: makeIdleGuardReadModel({
            activities: [guardActivity("approval.requested", "req-1")],
          }),
        }),
      );
      expect(approvalError._tag).toBe("OrchestrationCommandInvariantError");

      // A queued turn start: a user message no turn has picked up yet, inside
      // the adoption grace window (the decider's clock here is the Effect
      // test clock, pinned to the epoch).
      const queuedError = yield* Effect.flip(
        decideOrchestrationCommand({
          command: {
            type: "thread.delete",
            commandId: asCommandId("cmd-idle-delete-queued"),
            threadId: asThreadId("thread-idle-guard"),
            requireIdle: true,
          },
          readModel: makeIdleGuardReadModel({
            messages: [guardUserMessage("1969-12-31T23:59:30.000Z")],
          }),
        }),
      );
      expect(queuedError._tag).toBe("OrchestrationCommandInvariantError");
    }),
  );

  it.effect(
    "deletes an idle thread when the precondition passes, and keeps plain deletes unconditional",
    () =>
      Effect.gen(function* () {
        // Idle: a stopped session, no open requests, no queued turn — the
        // precondition passes and the delete lands.
        const decided = yield* decideOrchestrationCommand({
          command: {
            type: "thread.delete",
            commandId: asCommandId("cmd-idle-delete-ok"),
            threadId: asThreadId("thread-idle-guard"),
            requireIdle: true,
          },
          readModel: makeIdleGuardReadModel({ sessionStatus: "stopped" }),
        });
        const decidedEvents = Array.isArray(decided) ? decided : [decided];
        expect(decidedEvents[0]?.type).toBe("thread.deleted");

        // A plain (user-driven) delete stays UNCONDITIONAL: deleting a stuck
        // running thread remains the user's escape hatch.
        const userDelete = yield* decideOrchestrationCommand({
          command: {
            type: "thread.delete",
            commandId: asCommandId("cmd-user-delete"),
            threadId: asThreadId("thread-idle-guard"),
          },
          readModel: makeIdleGuardReadModel({ sessionStatus: "running" }),
        });
        const userDeleteEvents = Array.isArray(userDelete) ? userDelete : [userDelete];
        expect(userDeleteEvents[0]?.type).toBe("thread.deleted");
      }),
  );
});
