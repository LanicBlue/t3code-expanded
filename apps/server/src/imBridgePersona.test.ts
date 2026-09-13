import { imBridgeMemberIdOfThreadId } from "@t3tools/contracts/settings";
import type {
  OrchestrationCommand,
  OrchestrationShellSnapshot,
  ThreadId,
} from "@t3tools/contracts";
import type { ServerSettings } from "@t3tools/contracts/settings";
import * as Effect from "effect/Effect";
import { describe, expect, it } from "vite-plus/test";

import {
  dispatchWithImBridgePersona,
  firstTurnForBridgeThread,
  injectImBridgePersona,
} from "./imBridgePersona.ts";

const turnStart = (threadId: string, text = "brief"): OrchestrationCommand =>
  ({
    type: "thread.turn.start",
    commandId: "cmd-1",
    threadId,
    message: { messageId: "m-1", role: "user", text, attachments: [] },
    runtimeMode: "full-access",
    interactionMode: "default",
    createdAt: "2026-09-10T00:00:00.000Z",
  }) as unknown as OrchestrationCommand;

const settingsWith = (members: Array<{ id: string; persona?: string }>): ServerSettings =>
  ({ imBridge: { members } }) as unknown as ServerSettings;

describe("imBridgeMemberIdOfThreadId", () => {
  it("extracts the member id from deterministic and suffixed bridge thread ids", () => {
    expect(imBridgeMemberIdOfThreadId("im-t3-glm-ms_aaaabbbbccccdddd")).toBe("t3-glm");
    expect(imBridgeMemberIdOfThreadId("im-t3-glm-flash-ms_aaaabbbbccccdddd-3")).toBe(
      "t3-glm-flash",
    );
    expect(imBridgeMemberIdOfThreadId("im-t3-codex-ms_1770a068-d41d8c")).toBe("t3-codex");
  });

  it("rejects non-bridge and malformed ids", () => {
    expect(imBridgeMemberIdOfThreadId("chat-thread-1")).toBeNull();
    expect(imBridgeMemberIdOfThreadId("im-ms_aaaabbbbccccdddd")).toBeNull(); // empty member id
    expect(imBridgeMemberIdOfThreadId("im-t3-glm-ms_xyz")).toBeNull(); // mission not ms_+hex
    expect(imBridgeMemberIdOfThreadId("t3-glm-ms_aaaabbbbccccdddd")).toBeNull(); // missing im- prefix
  });
});

describe("injectImBridgePersona", () => {
  it("prepends the member persona to a turn.start on that member's thread", () => {
    const command = turnStart("im-t3-glm-ms_aaaabbbbccccdddd", "duty brief");
    const injected = injectImBridgePersona(
      command,
      settingsWith([{ id: "t3-glm", persona: "你是软件架构师" }]),
    );
    if (injected.type !== "thread.turn.start") throw new Error("expected turn.start");
    expect(injected.message.text).toContain("你是软件架构师");
    expect(injected.message.text.endsWith("duty brief")).toBe(true);
    expect(injected.message.text.indexOf("你是软件架构师")).toBeLessThan(
      injected.message.text.indexOf("duty brief"),
    );
  });

  it("passes through untouched for members without a persona, other threads, and other commands", () => {
    const bare = settingsWith([{ id: "t3-glm" }]);
    const blank = settingsWith([{ id: "t3-glm", persona: "   " }]);
    const command = turnStart("im-t3-glm-ms_aaaabbbbccccdddd");
    expect(injectImBridgePersona(command, bare)).toBe(command);
    expect(injectImBridgePersona(command, blank)).toBe(command);
    // Unrelated member in the table
    expect(injectImBridgePersona(command, settingsWith([{ id: "t3-codex", persona: "x" }]))).toBe(
      command,
    );

    const otherThread = turnStart("some-other-thread", "hello");
    expect(injectImBridgePersona(otherThread, settingsWith([{ id: "t3-glm", persona: "x" }]))).toBe(
      otherThread,
    );

    const threadCreate = { type: "thread.create", threadId: "im-t3-glm-ms_aaaabbbbccccdddd" };
    expect(
      injectImBridgePersona(
        threadCreate as unknown as OrchestrationCommand,
        settingsWith([{ id: "t3-glm", persona: "x" }]),
      ),
    ).toBe(threadCreate);
  });
});

const threadId = (value: string): ThreadId => value as unknown as ThreadId;

const shellWithThreads = (
  threads: Array<{ id: string; latestTurn: unknown }>,
): OrchestrationShellSnapshot =>
  ({
    snapshotSequence: 1,
    projects: [],
    threads,
    updatedAt: "2026-09-13T00:00:00.000Z",
  }) as unknown as OrchestrationShellSnapshot;

describe("firstTurnForBridgeThread", () => {
  it("treats unknown threads and threads without a committed turn as first", () => {
    const first = firstTurnForBridgeThread(() =>
      Effect.succeed(shellWithThreads([{ id: "im-t3-glm-ms_aaaabbbbccccdddd", latestTurn: null }])),
    );
    expect(Effect.runSync(first(threadId("im-t3-glm-ms_aaaabbbbccccdddd")))).toBe(true);
    expect(Effect.runSync(first(threadId("im-t3-glm-ms_0000000000000001")))).toBe(true);
  });

  it("treats threads with any committed turn as not first", () => {
    const first = firstTurnForBridgeThread(() =>
      Effect.succeed(
        shellWithThreads([
          { id: "im-t3-glm-ms_aaaabbbbccccdddd", latestTurn: { state: "completed" } },
        ]),
      ),
    );
    expect(Effect.runSync(first(threadId("im-t3-glm-ms_aaaabbbbccccdddd")))).toBe(false);
  });

  it("fails open to first when the shell snapshot read fails", () => {
    const first = firstTurnForBridgeThread(() => Effect.fail("projection down" as never));
    expect(Effect.runSync(first(threadId("im-t3-glm-ms_aaaabbbbccccdddd")))).toBe(true);
  });
});

describe("dispatchWithImBridgePersona", () => {
  const personaSettings = settingsWith([{ id: "t3-glm", persona: "你是软件架构师" }]);
  const bridgeThread = "im-t3-glm-ms_aaaabbbbccccdddd";

  const makeEngine = () => {
    const dispatched: Array<OrchestrationCommand> = [];
    const engine = {
      dispatch: (command: OrchestrationCommand) => {
        dispatched.push(command);
        return Effect.succeed({ sequence: dispatched.length });
      },
    };
    return { engine, dispatched };
  };

  it("injects the persona on the thread's first turn", () => {
    const { engine, dispatched } = makeEngine();
    const dispatch = dispatchWithImBridgePersona(
      engine as never,
      Effect.succeed(personaSettings),
      () => Effect.succeed(true),
    );
    Effect.runSync(dispatch(turnStart(bridgeThread)));
    expect(dispatched).toHaveLength(1);
    expect((dispatched[0] as { message: { text: string } }).message.text).toContain(
      "你是软件架构师",
    );
  });

  it("passes later turns through raw — the transcript already carries the persona", () => {
    const { engine, dispatched } = makeEngine();
    const command = turnStart(bridgeThread, "result submission");
    const dispatch = dispatchWithImBridgePersona(
      engine as never,
      Effect.succeed(personaSettings),
      () => Effect.succeed(false),
    );
    Effect.runSync(dispatch(command));
    expect(dispatched).toEqual([command]);
  });

  it("never consults the first-turn predicate for non-bridge commands or persona-less members", () => {
    const { engine, dispatched } = makeEngine();
    let predicateCalls = 0;
    const dispatch = dispatchWithImBridgePersona(
      engine as never,
      Effect.succeed(personaSettings),
      () => {
        predicateCalls += 1;
        return Effect.succeed(false);
      },
    );
    const plain = turnStart("some-other-thread");
    const noPersona = turnStart("im-t3-codex-ms_aaaabbbbccccdddd");
    Effect.runSync(dispatch(plain));
    Effect.runSync(dispatch(noPersona));
    expect(dispatched).toEqual([plain, noPersona]);
    expect(predicateCalls).toBe(0);
  });

  it("defaults to injecting when no predicate is supplied", () => {
    const { engine, dispatched } = makeEngine();
    const dispatch = dispatchWithImBridgePersona(engine as never, Effect.succeed(personaSettings));
    Effect.runSync(dispatch(turnStart(bridgeThread)));
    expect((dispatched[0] as { message: { text: string } }).message.text).toContain(
      "你是软件架构师",
    );
  });
});
