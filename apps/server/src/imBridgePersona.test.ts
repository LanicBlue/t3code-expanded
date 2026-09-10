import type { OrchestrationCommand } from "@t3tools/contracts";
import type { ServerSettings } from "@t3tools/contracts/settings";
import { describe, expect, it } from "vite-plus/test";

import { imBridgeMemberIdOfThread, injectImBridgePersona } from "./imBridgePersona.ts";

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

describe("imBridgeMemberIdOfThread", () => {
  it("extracts the member id from deterministic and suffixed bridge thread ids", () => {
    expect(imBridgeMemberIdOfThread("im-t3-glm-ms_aaaabbbbccccdddd")).toBe("t3-glm");
    expect(imBridgeMemberIdOfThread("im-t3-glm-flash-ms_aaaabbbbccccdddd-3")).toBe("t3-glm-flash");
    expect(imBridgeMemberIdOfThread("im-t3-codex-ms_1770a068-d41d8c")).toBe("t3-codex");
  });

  it("rejects non-bridge and malformed ids", () => {
    expect(imBridgeMemberIdOfThread("chat-thread-1")).toBeNull();
    expect(imBridgeMemberIdOfThread("im-ms_aaaabbbbccccdddd")).toBeNull(); // empty member id
    expect(imBridgeMemberIdOfThread("im-t3-glm-ms_xyz")).toBeNull(); // mission not ms_+hex
    expect(imBridgeMemberIdOfThread("t3-glm-ms_aaaabbbbccccdddd")).toBeNull(); // missing im- prefix
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
