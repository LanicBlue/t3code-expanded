/**
 * Session routing for Project Service Work notices (issue #4).
 *
 * Pure routing policy plus an in-memory per-(agentId, projectId) session
 * state machine. A notice is a TRIGGER, never a verbatim payload: every
 * delivered message is derived from an authoritative Work query at delivery
 * time (see `aggregateWorkNotificationMessage`). The local pending set is
 * debounce/dedup state only — Work lifecycle is never mirrored into T3.
 *
 * The routing key is `logicalAgentId + Project Service projectId`; names in
 * either direction are display metadata and never participate in the mapping.
 * The vendored SDK runtime deduplicates noticeIds (same-notice replays never
 * re-wake), so this module coalesces at the routing-key level only: many
 * distinct notices for one key collapse into at most one aggregate
 * notification per busy period.
 *
 * @module ProjectWorkNoticeRouting
 */
import {
  CommandId,
  DEFAULT_MODEL,
  DEFAULT_MODEL_BY_PROVIDER,
  type LogicalAgentId,
  MessageId,
  type ModelSelection,
  type OrchestrationCommand,
  type OrchestrationProjectShell,
  type OrchestrationThreadShell,
  type ProjectId,
  type ProviderDriverKind,
  type ServerSettings,
  ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";

const DEFAULT_ROUTING_RUNTIME_MODE = "full-access" as const;
const DEFAULT_ROUTING_INTERACTION_MODE = "default" as const;

// ── Errors ───────────────────────────────────────────────────────

/** Routing failures carry the Consumer wire's failure-code vocabulary. */
export class ProjectWorkRoutingError extends Schema.TaggedErrorClass<ProjectWorkRoutingError>()(
  "ProjectWorkRoutingError",
  {
    code: Schema.Literals(["AGENT_NOT_FOUND", "AGENT_NOT_DISPATCHABLE", "CONSUMER_INTERNAL"]),
    detail: Schema.String,
  },
) {
  override get message(): string {
    return `${this.code}: ${this.detail}`;
  }
}

// ── Settings-level routing resolution ────────────────────────────

export interface ProjectWorkRoutingTarget {
  readonly logicalAgentId: LogicalAgentId;
  readonly agentName: string;
  readonly providerInstanceId: string;
  /** The binding whose Project Service projectId matched the notice. */
  readonly projectServiceProjectId: string;
  readonly projectName: string;
  readonly t3ProjectId: ProjectId;
}

export type ProjectWorkRoutingResolution =
  | { readonly ok: true; readonly target: ProjectWorkRoutingTarget }
  | { readonly ok: false; readonly detail: string };

/**
 * Map `agentId + projectId` onto the configured agent and T3 project.
 * Reads stay structural (duplicates and unknowns resolve to failures, never
 * crashes) because settings.json can be hand-edited between validations.
 */
export const resolveProjectWorkRouting = (
  settings: ServerSettings,
  agentId: string,
  projectId: string,
): ProjectWorkRoutingResolution => {
  if (!settings.projectServiceClient.enabled) {
    return { ok: false, detail: "the Project Service integration is disabled" };
  }
  const agent = Object.entries(settings.logicalAgents).find(([id]) => id === agentId);
  if (agent === undefined) {
    return { ok: false, detail: `agent ${agentId} is not configured` };
  }
  const [logicalAgentId, agentConfig] = agent;
  if (!agentConfig.project.enabled) {
    return { ok: false, detail: `project work is disabled for agent ${agentId}` };
  }
  const matches = agentConfig.projectBindings.filter((binding) => binding.projectId === projectId);
  const binding = matches.at(0);
  if (binding === undefined || matches.length > 1) {
    return {
      ok: false,
      detail:
        matches.length === 0
          ? `agent ${agentId} does not bind Project Service project ${projectId}`
          : `agent ${agentId} binds Project Service project ${projectId} more than once`,
    };
  }
  return {
    ok: true,
    target: {
      logicalAgentId: logicalAgentId as LogicalAgentId,
      agentName: agentConfig.agentName,
      providerInstanceId: agentConfig.providerInstanceId,
      projectServiceProjectId: binding.projectId,
      projectName: binding.projectName,
      t3ProjectId: binding.t3ProjectId,
    },
  };
};

// ── Thread classification ────────────────────────────────────────

/**
 * A session is busy while a turn runs or a provider session is starting/
 * running. This mirrors the client's own activity derivation: leaving the
 * "running" session status is the authoritative turn-end signal.
 */
export const isWorkThreadBusy = (shell: OrchestrationThreadShell): boolean =>
  (shell.session !== null &&
    (shell.session.status === "starting" || shell.session.status === "running")) ||
  shell.latestTurn?.state === "running";

/** Session statuses that mean a notification turn can never still engage. */
const TERMINAL_SESSION_STATUSES: ReadonlySet<string> = new Set(["stopped", "error", "interrupted"]);

/** Archived sessions are no longer current (deleted ones read as absent). */
export const isWorkThreadCurrent = (shell: OrchestrationThreadShell): boolean =>
  shell.archivedAt === null;

// ── Aggregate notification ───────────────────────────────────────

/** The single compact notification, with a count refreshed at delivery time. */
export const aggregateWorkNotificationMessage = (openWorkCount: number): string =>
  openWorkCount === 1
    ? "There is 1 assigned Work item waiting. Use the Project tools to inspect it."
    : `There are ${openWorkCount} assigned Work items waiting. Use the Project tools to inspect them.`;

/** Title for sessions this integration creates; display-only. */
export const workSessionThreadTitle = (
  projectName: string,
  projectServiceProjectId: string,
): string => {
  const name = projectName.trim();
  return `Project Work — ${name.length > 0 ? name : projectServiceProjectId}`;
};

// ── Router ───────────────────────────────────────────────────────

/** What a wake carries into the router; SDK notice facts minus transport detail. */
export interface ProjectWorkWakeInput {
  readonly agentId: string;
  readonly projectId: string;
}

/** Seams the router needs from its host; failures carry the wire failure code. */
export interface ProjectWorkSessionRouterDeps {
  readonly readSettings: Effect.Effect<ServerSettings, ProjectWorkRoutingError>;
  readonly readThreadShell: (
    threadId: ThreadId,
  ) => Effect.Effect<Option.Option<OrchestrationThreadShell>, ProjectWorkRoutingError>;
  readonly readProjectShell: (
    projectId: ProjectId,
  ) => Effect.Effect<Option.Option<OrchestrationProjectShell>, ProjectWorkRoutingError>;
  /** `None` when the instance is unknown to the provider registry. */
  readonly resolveProviderDriver: (
    instanceId: string,
  ) => Effect.Effect<Option.Option<ProviderDriverKind>, ProjectWorkRoutingError>;
  readonly dispatchCommand: (
    command: OrchestrationCommand,
  ) => Effect.Effect<void, ProjectWorkRoutingError>;
  /** Authoritative assigned/open Work count for the agent+project, queried now. */
  readonly countOpenAssignedWork: (
    input: ProjectWorkWakeInput,
  ) => Effect.Effect<number, ProjectWorkRoutingError>;
  readonly nowIso: Effect.Effect<string>;
  readonly newId: Effect.Effect<string>;
}

export interface ProjectWorkSessionSnapshot {
  readonly agentId: string;
  readonly projectId: string;
  readonly threadId: ThreadId;
  readonly phase: "idle" | "notifying";
  readonly pendingWork: boolean;
}

export interface ProjectWorkSessionRouter {
  /**
   * Route one notice by agentId + projectId. Resolving means T3 accepted and
   * routed the notice (session created, notification delivered, or work
   * recorded against a busy session) — never that the agent ran the work.
   */
  readonly routeWake: (input: ProjectWorkWakeInput) => Effect.Effect<void, ProjectWorkRoutingError>;
  /**
   * Re-evaluate a thread after an orchestration event: a finished turn with
   * recorded work delivers at most one aggregate notification. Never fails.
   */
  readonly onThreadEvent: (threadId: ThreadId) => Effect.Effect<void>;
  /** Test/observability view of the current-session registry. */
  readonly snapshotSessions: Effect.Effect<ReadonlyArray<ProjectWorkSessionSnapshot>>;
}

/**
 * One current session per `agentId + projectId`. `notifying` spans the window
 * from dispatching the aggregate turn until its turn is observed finished, so
 * the queued-turn gap (command committed, provider session not yet started)
 * still counts as busy and no second notification can be dispatched into it.
 */
interface RoutedSession {
  readonly threadId: ThreadId;
  phase: "idle" | "notifying";
  pendingWork: boolean;
  /**
   * Whether the current notification turn was observed busy. The aggregate's
   * turn only counts as finished after the thread engaged (or its session
   * proved idle without it), never during the queued-turn window between
   * dispatch and session start — that gap would otherwise let a second
   * notification dispatch on top of the queued first one.
   */
  seenBusy: boolean;
}

const routingKeyOf = (agentId: string, projectId: string): string => `${agentId}::${projectId}`;

const routingKeyParts = (key: string): { readonly agentId: string; readonly projectId: string } => {
  const separator = key.indexOf("::");
  return {
    agentId: separator === -1 ? key : key.slice(0, separator),
    projectId: separator === -1 ? "" : key.slice(separator + 2),
  };
};

export const makeProjectWorkSessionRouter = Effect.fn("makeProjectWorkSessionRouter")(function* (
  deps: ProjectWorkSessionRouterDeps,
): Effect.fn.Return<ProjectWorkSessionRouter> {
  const sessionsRef = yield* Ref.make(new Map<string, RoutedSession>());

  // One in-flight routing pass per key. The SDK may deliver notices
  // concurrently (replay bursts deliver several back-to-back), and every pass
  // spans async seams — the authoritative Work query and orchestration
  // dispatch. Without serialization, two passes for one key interleave at
  // those seams and create duplicate sessions or stack duplicate aggregates.
  // A late pass simply observes the post-dispatch state instead of racing it.
  const keySemaphoresRef = yield* Ref.make(new Map<string, Semaphore.Semaphore>());
  const withKeySerialization = <A, E, R>(
    key: string,
    self: Effect.Effect<A, E, R>,
  ): Effect.Effect<A, E, R> =>
    Effect.flatMap(
      Ref.modify(keySemaphoresRef, (semaphores) => {
        const existing = semaphores.get(key);
        if (existing !== undefined) {
          return [existing, semaphores];
        }
        const created = Semaphore.makeUnsafe(1);
        semaphores.set(key, created);
        return [created, semaphores];
      }),
      (semaphore) => semaphore.withPermits(1)(self),
    );

  const getSession = (key: string): Effect.Effect<RoutedSession | undefined> =>
    Ref.get(sessionsRef).pipe(Effect.map((sessions) => sessions.get(key)));

  const mutateSession = (
    key: string,
    mutate: (session: RoutedSession) => RoutedSession,
  ): Effect.Effect<void> =>
    Ref.modify(sessionsRef, (sessions) => {
      const session = sessions.get(key);
      if (session !== undefined) {
        sessions.set(key, mutate(session));
      }
      return [undefined, sessions];
    });

  const putSession = (key: string, session: RoutedSession): Effect.Effect<void> =>
    Ref.modify(sessionsRef, (sessions) => {
      sessions.set(key, session);
      return [undefined, sessions];
    });

  const dropSession = (key: string): Effect.Effect<void> =>
    Ref.modify(sessionsRef, (sessions) => {
      sessions.delete(key);
      return [undefined, sessions];
    });

  // The model a created session runs on: the agent's provider instance is
  // fixed by routing; the model falls back from the project's default
  // (when it targets the same instance) to the driver default.
  const resolveModelSelection = Effect.fn("ProjectWorkSessionRouter.resolveModelSelection")(
    function* (
      target: ProjectWorkRoutingTarget,
      project: OrchestrationProjectShell,
    ): Effect.fn.Return<ModelSelection, ProjectWorkRoutingError> {
      const driverKind = yield* deps.resolveProviderDriver(target.providerInstanceId);
      if (Option.isNone(driverKind)) {
        return yield* new ProjectWorkRoutingError({
          code: "AGENT_NOT_DISPATCHABLE",
          detail: `provider instance ${target.providerInstanceId} for agent ${target.logicalAgentId} is not available`,
        });
      }
      const projectDefault = project.defaultModelSelection;
      const model =
        projectDefault !== null && projectDefault.instanceId === target.providerInstanceId
          ? projectDefault.model
          : (DEFAULT_MODEL_BY_PROVIDER[driverKind.value] ?? DEFAULT_MODEL);
      return {
        instanceId: target.providerInstanceId as ModelSelection["instanceId"],
        model,
      } satisfies ModelSelection;
    },
  );

  const resolveRoutingOr = Effect.fn("ProjectWorkSessionRouter.resolveRouting")(function* (
    input: ProjectWorkWakeInput,
  ): Effect.fn.Return<ProjectWorkRoutingTarget, ProjectWorkRoutingError> {
    const settings = yield* deps.readSettings;
    const resolution = resolveProjectWorkRouting(settings, input.agentId, input.projectId);
    if (!resolution.ok) {
      return yield* new ProjectWorkRoutingError({
        code: "AGENT_NOT_FOUND",
        detail: resolution.detail,
      });
    }
    return resolution.target;
  });

  // Deliver the aggregate. The open-work count always comes from the
  // authoritative query at delivery time; a synchronous create path may
  // pass the count it just queried as `knownCount`. Resolves to whether a
  // notification turn was actually dispatched.
  const deliverAggregate = Effect.fn("ProjectWorkSessionRouter.deliverAggregate")(function* (
    target: ProjectWorkRoutingTarget,
    threadId: ThreadId,
    knownCount?: number,
  ): Effect.fn.Return<boolean, ProjectWorkRoutingError> {
    const count =
      knownCount ??
      (yield* deps.countOpenAssignedWork({
        agentId: target.logicalAgentId,
        projectId: target.projectServiceProjectId,
      }));
    if (count === 0) {
      // The authoritative query no longer shows open work: nothing to
      // say. Accept silently instead of notifying about zero items.
      return false;
    }
    yield* deps.dispatchCommand({
      type: "thread.turn.start",
      commandId: CommandId.make(yield* deps.newId),
      threadId,
      message: {
        messageId: MessageId.make(yield* deps.newId),
        role: "user",
        text: aggregateWorkNotificationMessage(count),
        attachments: [],
      },
      runtimeMode: DEFAULT_ROUTING_RUNTIME_MODE,
      interactionMode: DEFAULT_ROUTING_INTERACTION_MODE,
      createdAt: yield* deps.nowIso,
    });
    return true;
  });

  // Creates the next current session and delivers the aggregate as its
  // first turn. Used for missing sessions and archived ones (an archived
  // session is no longer current, so a NEW session becomes it).
  const createCurrentSession = Effect.fn("ProjectWorkSessionRouter.createCurrentSession")(
    function* (
      key: string,
      target: ProjectWorkRoutingTarget,
    ): Effect.fn.Return<void, ProjectWorkRoutingError> {
      const project = yield* deps.readProjectShell(target.t3ProjectId);
      if (Option.isNone(project)) {
        return yield* new ProjectWorkRoutingError({
          code: "AGENT_NOT_DISPATCHABLE",
          detail: `T3 project ${target.t3ProjectId} bound to Project Service project ${target.projectServiceProjectId} does not exist`,
        });
      }
      const count = yield* deps.countOpenAssignedWork({
        agentId: target.logicalAgentId,
        projectId: target.projectServiceProjectId,
      });
      if (count === 0) {
        // No session is needed for work that no longer exists.
        return;
      }
      const modelSelection = yield* resolveModelSelection(target, project.value);
      const threadId = ThreadId.make(yield* deps.newId);
      const createdAt = yield* deps.nowIso;
      yield* deps.dispatchCommand({
        type: "thread.create",
        commandId: CommandId.make(yield* deps.newId),
        threadId,
        projectId: target.t3ProjectId,
        title: workSessionThreadTitle(target.projectName, target.projectServiceProjectId),
        modelSelection,
        runtimeMode: DEFAULT_ROUTING_RUNTIME_MODE,
        interactionMode: DEFAULT_ROUTING_INTERACTION_MODE,
        branch: null,
        worktreePath: null,
        createdAt,
      });
      // The created session is current from the moment the thread exists;
      // an aggregate delivery failure leaves it idle so a redelivered
      // notice resumes on this session instead of creating another.
      yield* putSession(key, {
        threadId,
        phase: "idle",
        pendingWork: false,
        seenBusy: false,
      });
      const delivered = yield* deliverAggregate(target, threadId, count);
      yield* mutateSession(key, (session) => ({
        ...session,
        phase: delivered ? "notifying" : "idle",
        seenBusy: false,
      }));
    },
  );

  // Deliver at most one aggregate for recorded work. Called with the
  // pending flag already set; the deferred variant (after a turn) has no
  // ACK path, so its failures are contained here.
  const flushRecordedWork = Effect.fn("ProjectWorkSessionRouter.flushRecordedWork")(function* (
    key: string,
    target: ProjectWorkRoutingTarget,
    deferred: boolean,
  ): Effect.fn.Return<void, ProjectWorkRoutingError> {
    const session = yield* getSession(key);
    if (session === undefined) {
      return;
    }
    if (!deferred) {
      // Synchronous wake path: a delivery failure must reach the wire
      // ACK so Project Service can repair and redeliver.
      const dispatched = yield* deliverAggregate(target, session.threadId);
      yield* mutateSession(key, (current) => ({
        ...current,
        pendingWork: false,
        phase: dispatched ? "notifying" : "idle",
        seenBusy: false,
      }));
      return;
    }
    // Deferred post-turn path: no ACK exists, and the notices behind this
    // pending flag were already ACKed — Project Service will not redeliver
    // them. A delivery failure therefore KEEPS the work recorded (the next
    // thread event or wake retries); clearing it would lose the only
    // notification the agent would ever get.
    const delivered = yield* deliverAggregate(target, session.threadId).pipe(Effect.result);
    if (delivered._tag === "Failure") {
      yield* mutateSession(key, (current) => ({ ...current, phase: "idle", seenBusy: false }));
      yield* Effect.logWarning(
        "deferred Project Work notification could not be delivered; it stays pending for the next event",
        {
          agentId: target.logicalAgentId,
          projectId: target.projectServiceProjectId,
          code: delivered.failure.code,
        },
      );
      return;
    }
    yield* mutateSession(key, (current) => ({
      ...current,
      pendingWork: false,
      phase: delivered.success ? "notifying" : "idle",
      seenBusy: false,
    }));
  });

  const routeWake = Effect.fn("ProjectWorkSessionRouter.routeWake")(function* (
    input: ProjectWorkWakeInput,
  ): Effect.fn.Return<void, ProjectWorkRoutingError> {
    const target = yield* resolveRoutingOr(input);
    const key = routingKeyOf(input.agentId, input.projectId);
    return yield* withKeySerialization(
      key,
      Effect.gen(function* () {
        const session = yield* getSession(key);

        if (session === undefined) {
          return yield* createCurrentSession(key, target);
        }

        const shell = yield* deps.readThreadShell(session.threadId);
        if (Option.isNone(shell) || !isWorkThreadCurrent(shell.value)) {
          // Archived or gone: no longer current; a new session takes over.
          yield* dropSession(key);
          return yield* createCurrentSession(key, target);
        }
        // Busy sessions (a turn running, or our aggregate still in flight) get
        // the work recorded — never an interrupt.
        if (session.phase === "notifying" || isWorkThreadBusy(shell.value)) {
          yield* mutateSession(key, (current) => ({ ...current, pendingWork: true }));
          return;
        }
        yield* mutateSession(key, (current) => ({ ...current, pendingWork: true }));
        return yield* flushRecordedWork(key, target, false);
      }),
    );
  });

  // One thread event's work for a single routing key. Re-reads the session:
  // state may have advanced while this pass waited on the key's semaphore.
  const processThreadEvent = Effect.fn("ProjectWorkSessionRouter.processThreadEvent")(function* (
    key: string,
  ): Effect.fn.Return<void> {
    const session = yield* getSession(key);
    if (session === undefined) {
      return;
    }
    if (session.phase !== "notifying" && !session.pendingWork) {
      return;
    }
    const shell = yield* deps.readThreadShell(session.threadId).pipe(Effect.result);
    if (shell._tag === "Failure") {
      return;
    }
    if (Option.isSome(shell.success) && isWorkThreadBusy(shell.success.value)) {
      // The notification turn engaged; remember it so the FIRST
      // not-busy observation afterwards is the real turn end.
      yield* mutateSession(key, (current) => ({ ...current, seenBusy: true }));
      return;
    }
    if (
      session.phase === "notifying" &&
      !session.seenBusy &&
      (Option.isNone(shell.success) ||
        shell.success.value.session === null ||
        !TERMINAL_SESSION_STATUSES.has(shell.success.value.session.status))
    ) {
      // Queued-turn window: the aggregate was dispatched but its turn has
      // not engaged yet (no session, a resting/ready session, or a turn
      // still pending) — the turn has not finished, so recorded work must
      // keep waiting (dispatching again would stack a second notification
      // on the queued first one). Only a session that DIED without ever
      // running the turn (stopped/error/interrupted) counts as lapsed.
      return;
    }
    const { agentId, projectId } = routingKeyParts(key);
    const target = yield* resolveRoutingOr({ agentId, projectId }).pipe(Effect.result);
    if (target._tag === "Failure") {
      // Routing went away (agent unbound, integration disabled): the
      // session simply stops being driven; nothing is deleted.
      yield* mutateSession(key, (current) => ({ ...current, phase: "idle" }));
      return;
    }
    if (session.pendingWork) {
      yield* flushRecordedWork(key, target.success, true).pipe(Effect.ignore);
    } else {
      yield* mutateSession(key, (current) => ({ ...current, phase: "idle" }));
    }
  });

  const onThreadEvent = Effect.fn("ProjectWorkSessionRouter.onThreadEvent")(function* (
    threadId: ThreadId,
  ): Effect.fn.Return<void> {
    const sessions = yield* Ref.get(sessionsRef);
    for (const [key, session] of sessions.entries()) {
      if (session.threadId !== threadId) {
        continue;
      }
      if (session.phase !== "notifying" && !session.pendingWork) {
        continue;
      }
      yield* withKeySerialization(key, processThreadEvent(key)).pipe(Effect.ignore);
    }
  });

  const snapshotSessions: ProjectWorkSessionRouter["snapshotSessions"] = Ref.get(sessionsRef).pipe(
    Effect.map((sessions) =>
      [...sessions.entries()].map(([key, session]) => ({
        ...routingKeyParts(key),
        threadId: session.threadId,
        phase: session.phase,
        pendingWork: session.pendingWork,
      })),
    ),
  );

  return { routeWake, onThreadEvent, snapshotSessions };
});
