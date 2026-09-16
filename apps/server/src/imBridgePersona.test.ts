import { imBridgeMemberIdOfThreadId } from "@t3tools/contracts/settings";
import type { OrchestrationCommand, ThreadId } from "@t3tools/contracts";
import type { ServerSettings } from "@t3tools/contracts/settings";
import * as Effect from "effect/Effect";
import { describe, expect, it } from "vite-plus/test";

import {
  dispatchWithImBridgePersona,
  foldImBridgeFreshContext,
  imBridgeThreadStartsConversation,
} from "./imBridgePersona.ts";

const turnStart = (threadId: string, text = "brief", freshContext?: string): OrchestrationCommand =>
  ({
    type: "thread.turn.start",
    commandId: "cmd-1",
    threadId,
    message: {
      messageId: "m-1",
      role: "user",
      text,
      attachments: [],
      ...(freshContext !== undefined ? { context: { version: 1, records: [], freshContext } } : {}),
    },
    runtimeMode: "full-access",
    interactionMode: "default",
    createdAt: "2026-09-10T00:00:00.000Z",
  }) as unknown as OrchestrationCommand;

const settingsWith = (members: Array<{ id: string; persona?: string }>): ServerSettings =>
  ({ imBridge: { members } }) as unknown as ServerSettings;

const asTurnStart = (command: OrchestrationCommand) => {
  if (command.type !== "thread.turn.start") throw new Error("expected turn.start");
  return command;
};

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

describe("foldImBridgeFreshContext", () => {
  const bridgeThread = "im-t3-glm-ms_aaaabbbbccccdddd";

  it("folds persona and stable context into the text when the turn starts the conversation", () => {
    const folded = foldImBridgeFreshContext(
      turnStart(bridgeThread, "duty brief", "stable mission ctx"),
      settingsWith([{ id: "t3-glm", persona: "你是软件架构师" }]),
      true,
    );
    const message = asTurnStart(folded).message;
    // One complete input: persona block, then stable context, then the duty text.
    expect(message.text.startsWith("Member persona (T3 settings);")).toBe(true);
    expect(message.text).toContain("你是软件架构师\n\nstable mission ctx\n\nduty brief");
    // The consumed field is stripped — nothing downstream re-applies it.
    expect(message.context?.freshContext).toBeUndefined();
    expect(message.context?.records).toEqual([]);
  });

  it("folds the stable context alone for members without a persona", () => {
    const folded = foldImBridgeFreshContext(
      turnStart(bridgeThread, "duty brief", "stable mission ctx"),
      settingsWith([{ id: "t3-glm" }]),
      true,
    );
    expect(asTurnStart(folded).message.text).toBe("stable mission ctx\n\nduty brief");
    expect(asTurnStart(folded).message.context?.freshContext).toBeUndefined();
  });

  it("folds the persona alone when the sender carried no stable context", () => {
    const folded = foldImBridgeFreshContext(
      turnStart(bridgeThread, "duty brief"),
      settingsWith([{ id: "t3-glm", persona: "你是软件架构师" }]),
      true,
    );
    expect(asTurnStart(folded).message.text).toBe(
      "Member persona (T3 settings); it supplements, never overrides, the duty rules below.\n\n你是软件架构师\n\nduty brief",
    );
  });

  it("leaves later rounds round-form and strips the unused context", () => {
    const command = turnStart(bridgeThread, "round brief", "stable mission ctx");
    const folded = foldImBridgeFreshContext(
      command,
      settingsWith([{ id: "t3-glm", persona: "你是软件架构师" }]),
      false,
    );
    const message = asTurnStart(folded).message;
    expect(message.text).toBe("round brief");
    expect(message.context?.freshContext).toBeUndefined();
    expect(message.context?.records).toEqual([]);
  });

  it("passes through untouched for persona-less first turns without stable context, other threads, and other commands", () => {
    const bare = settingsWith([{ id: "t3-glm" }]);
    const blank = settingsWith([{ id: "t3-glm", persona: "   " }]);
    const command = turnStart(bridgeThread);
    expect(foldImBridgeFreshContext(command, bare, true)).toBe(command);
    expect(foldImBridgeFreshContext(command, blank, true)).toBe(command);
    // Unrelated member in the table
    expect(
      foldImBridgeFreshContext(command, settingsWith([{ id: "t3-codex", persona: "x" }]), true),
    ).toBe(command);

    const otherThread = turnStart("some-other-thread", "hello");
    expect(
      foldImBridgeFreshContext(otherThread, settingsWith([{ id: "t3-glm", persona: "x" }]), true),
    ).toBe(otherThread);

    const threadCreate = { type: "thread.create", threadId: bridgeThread };
    expect(
      foldImBridgeFreshContext(
        threadCreate as unknown as OrchestrationCommand,
        settingsWith([{ id: "t3-glm", persona: "x" }]),
        true,
      ),
    ).toBe(threadCreate);
  });
});

describe("imBridgeThreadStartsConversation", () => {
  const shellWith = (threads: Array<{ id: string; latestTurn: unknown }>) =>
    (() =>
      Effect.succeed({ threads }) as unknown as ReturnType<
        Parameters<typeof imBridgeThreadStartsConversation>[0]
      >) as Parameters<typeof imBridgeThreadStartsConversation>[0];

  it("marks unknown threads and threads without turns as starting the conversation", () => {
    const starts = imBridgeThreadStartsConversation(
      shellWith([{ id: "im-t3-glm-ms_aaaabbbbccccdddd", latestTurn: null }]),
    );
    expect(Effect.runSync(starts("im-t3-glm-ms_aaaabbbbccccdddd" as never as ThreadId))).toBe(true);
    expect(Effect.runSync(starts("im-t3-glm-ms_ffffffffffffffff" as never as ThreadId))).toBe(true);
  });

  it("marks threads with any prior turn as continuing — regardless of session state", () => {
    const starts = imBridgeThreadStartsConversation(
      shellWith([{ id: "im-t3-glm-ms_aaaabbbbccccdddd", latestTurn: { turnId: "t-1" } }]),
    );
    expect(Effect.runSync(starts("im-t3-glm-ms_aaaabbbbccccdddd" as never as ThreadId))).toBe(
      false,
    );
  });

  it("fails toward starting when the snapshot cannot be read", () => {
    const starts = imBridgeThreadStartsConversation((() =>
      Effect.fail(new Error("projection unavailable"))) as never);
    expect(Effect.runSync(starts("im-t3-glm-ms_aaaabbbbccccdddd" as never as ThreadId))).toBe(true);
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

  it("folds the full context into the first turn's text and leaves later turns round-form", () => {
    const { engine, dispatched } = makeEngine();
    const dispatch = dispatchWithImBridgePersona(
      engine as never,
      Effect.succeed(personaSettings),
      () => Effect.succeed(true),
    );
    Effect.runSync(dispatch(turnStart(bridgeThread, "round 1", "stable mission ctx")));
    expect(asTurnStart(dispatched[0]!).message.text).toContain(
      "你是软件架构师\n\nstable mission ctx\n\nround 1",
    );

    const later = dispatchWithImBridgePersona(
      engine as never,
      Effect.succeed(personaSettings),
      () => Effect.succeed(false),
    );
    Effect.runSync(later(turnStart(bridgeThread, "round 2", "stable mission ctx")));
    expect(asTurnStart(dispatched[1]!).message.text).toBe("round 2");
    expect(asTurnStart(dispatched[1]!).message.context?.freshContext).toBeUndefined();
  });

  it("passes non-bridge commands and persona-less members through raw", () => {
    const { engine, dispatched } = makeEngine();
    const dispatch = dispatchWithImBridgePersona(
      engine as never,
      Effect.succeed(personaSettings),
      () => Effect.succeed(true),
    );
    const plain = turnStart("some-other-thread");
    const noPersona = turnStart("im-t3-codex-ms_aaaabbbbccccdddd");
    Effect.runSync(dispatch(plain));
    Effect.runSync(dispatch(noPersona));
    expect(dispatched).toEqual([plain, noPersona]);
  });

  it("is pure in the command — retrying a dispatch never stacks context", () => {
    const { engine, dispatched } = makeEngine();
    const dispatch = dispatchWithImBridgePersona(
      engine as never,
      Effect.succeed(personaSettings),
      () => Effect.succeed(true),
    );
    const command = turnStart(bridgeThread, "round 1", "stable mission ctx");
    Effect.runSync(dispatch(command));
    const first = dispatched[0]!;
    Effect.runSync(dispatch(command));
    const second = dispatched[1]!;
    expect(second).toEqual(first);
    expect(asTurnStart(first).message.text.match(/你是软件架构师/g)).toHaveLength(1);
  });

  it("fails closed when bridge member settings cannot be read", () => {
    const { engine, dispatched } = makeEngine();
    const dispatch = dispatchWithImBridgePersona(
      engine as never,
      Effect.fail(new Error("settings unavailable") as never),
      () => Effect.succeed(true),
    );
    expect(() => Effect.runSync(dispatch(turnStart(bridgeThread)))).toThrow();
    expect(dispatched).toHaveLength(0);
  });
});
