import { imBridgeMemberIdOfThreadId } from "@t3tools/contracts/settings";
import type { OrchestrationCommand } from "@t3tools/contracts";
import type { ServerSettings } from "@t3tools/contracts/settings";
import * as Effect from "effect/Effect";
import { describe, expect, it } from "vite-plus/test";

import { dispatchWithImBridgePersona, injectImBridgePersona } from "./imBridgePersona.ts";

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
  it("merges the persona into the turn's freshContext, before any sender context, text untouched", () => {
    const command = turnStart("im-t3-glm-ms_aaaabbbbccccdddd", "duty brief", "stable mission ctx");
    const injected = injectImBridgePersona(
      command,
      settingsWith([{ id: "t3-glm", persona: "你是软件架构师" }]),
    );
    if (injected.type !== "thread.turn.start") throw new Error("expected turn.start");
    // The message text is delivered untouched; the persona rides freshContext.
    expect(injected.message.text).toBe("duty brief");
    const freshContext = injected.message.context?.freshContext ?? "";
    expect(freshContext).toContain("你是软件架构师");
    expect(freshContext.indexOf("你是软件架构师")).toBeLessThan(
      freshContext.indexOf("stable mission ctx"),
    );
    expect(injected.message.context?.records).toEqual([]);
  });

  it("seeds a context for messages that carried none", () => {
    const injected = injectImBridgePersona(
      turnStart("im-t3-glm-ms_aaaabbbbccccdddd"),
      settingsWith([{ id: "t3-glm", persona: "你是软件架构师" }]),
    );
    if (injected.type !== "thread.turn.start") throw new Error("expected turn.start");
    expect(injected.message.context?.freshContext).toContain("你是软件架构师");
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

  it("puts the persona in freshContext on every dispatched bridge turn — the reactor decides when it rides", () => {
    const { engine, dispatched } = makeEngine();
    const dispatch = dispatchWithImBridgePersona(engine as never, Effect.succeed(personaSettings));
    Effect.runSync(dispatch(turnStart(bridgeThread, "round 1")));
    Effect.runSync(dispatch(turnStart(bridgeThread, "round 2")));
    for (const command of dispatched) {
      const message = (command as { message: { context?: { freshContext?: string } } }).message;
      expect(message.context?.freshContext).toContain("你是软件架构师");
    }
  });

  it("passes non-bridge commands and persona-less members through raw", () => {
    const { engine, dispatched } = makeEngine();
    const dispatch = dispatchWithImBridgePersona(engine as never, Effect.succeed(personaSettings));
    const plain = turnStart("some-other-thread");
    const noPersona = turnStart("im-t3-codex-ms_aaaabbbbccccdddd");
    Effect.runSync(dispatch(plain));
    Effect.runSync(dispatch(noPersona));
    expect(dispatched).toEqual([plain, noPersona]);
  });

  it("is pure in the command — retrying a dispatch never stacks personas", () => {
    const { engine, dispatched } = makeEngine();
    const dispatch = dispatchWithImBridgePersona(engine as never, Effect.succeed(personaSettings));
    const command = turnStart(bridgeThread);
    Effect.runSync(dispatch(command));
    const first = dispatched[0] as { message: { context?: { freshContext?: string } } };
    Effect.runSync(dispatch(command));
    const second = dispatched[1] as { message: { context?: { freshContext?: string } } };
    expect(second).toEqual(first);
    expect((first.message.context?.freshContext ?? "").match(/你是软件架构师/g)).toHaveLength(1);
  });

  it("fails closed when bridge member settings cannot be read", () => {
    const { engine, dispatched } = makeEngine();
    const dispatch = dispatchWithImBridgePersona(
      engine as never,
      Effect.fail(new Error("settings unavailable") as never),
    );
    expect(() => Effect.runSync(dispatch(turnStart(bridgeThread)))).toThrow();
    expect(dispatched).toHaveLength(0);
  });
});
