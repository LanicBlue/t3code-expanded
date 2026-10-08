import { ThreadId, type OrchestrationV2Command } from "@t3tools/contracts";
import type { ServerSettings } from "@t3tools/contracts/settings";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import { describe, expect, it } from "vite-plus/test";

import {
  liveImBridgeThreads,
  removedImBridgeMemberIds,
  settleDepartedImBridgeThreads,
  type DepartureThreadRow,
} from "./imBridgeDeparture.ts";

const settingsWith = (ids: ReadonlyArray<string>): Pick<ServerSettings, "imBridge"> =>
  ({ imBridge: { members: ids.map((id) => ({ id })) } }) as unknown as Pick<
    ServerSettings,
    "imBridge"
  >;

const thread = (
  id: string,
  fields: Partial<Pick<DepartureThreadRow, "settledAt" | "settledOverride">> = {},
): DepartureThreadRow => ({
  id: ThreadId.make(id),
  settledAt: null,
  settledOverride: null,
  ...fields,
});

describe("removedImBridgeMemberIds", () => {
  it("returns ids that disappeared from the member table", () => {
    expect(removedImBridgeMemberIds(settingsWith(["m-a", "m-b"]), settingsWith(["m-b"]))).toEqual([
      "m-a",
    ]);
  });

  it("returns nothing for unchanged tables, added members, or disabled members", () => {
    expect(removedImBridgeMemberIds(settingsWith(["m-a"]), settingsWith(["m-a"]))).toEqual([]);
    expect(removedImBridgeMemberIds(settingsWith(["m-a"]), settingsWith(["m-a", "m-b"]))).toEqual(
      [],
    );
    // A disabled member stays in the table — that is a leave, not a removal.
    expect(removedImBridgeMemberIds(settingsWith(["m-a"]), settingsWith(["m-a"]))).toEqual([]);
  });

  it("treats an id swap as removal of the old id", () => {
    expect(removedImBridgeMemberIds(settingsWith(["m-old"]), settingsWith(["m-new"]))).toEqual([
      "m-old",
    ]);
  });
});

describe("liveImBridgeThreads", () => {
  const threads = [
    thread("im-m-a-ms_aaaabbbbccccdddd"),
    thread("im-m-a-ms_aaaabbbbccccdddd-3"), // create-race suffix
    thread("im-m-a-ms_1111222233334444", { settledAt: "2026-09-11T00:00:00.000Z" as never }),
    thread("im-m-a-ms_5555666677778888", { settledOverride: "settled" }),
    thread("im-m-b-ms_9999aaaabbbbcccc"),
    thread("ordinary-chat-thread"),
  ];

  it("keeps the member's unsettled mission threads, suffixed ids included", () => {
    expect(liveImBridgeThreads(threads, "m-a").map((t) => t.id)).toEqual([
      ThreadId.make("im-m-a-ms_aaaabbbbccccdddd"),
      ThreadId.make("im-m-a-ms_aaaabbbbccccdddd-3"),
    ]);
  });

  it("excludes other members and non-bridge threads", () => {
    expect(liveImBridgeThreads(threads, "m-b").map((t) => t.id)).toEqual([
      ThreadId.make("im-m-b-ms_9999aaaabbbbcccc"),
    ]);
    expect(liveImBridgeThreads(threads, "m-c")).toEqual([]);
  });
});

function fakeDispatch(failFor: ReadonlyArray<string> = []) {
  const dispatched: Array<{ type: string; threadId: string; commandId: string }> = [];
  const dispatchCommand = (command: OrchestrationV2Command) => {
    const record = command as unknown as { type: string; threadId: string; commandId: string };
    dispatched.push(record);
    return failFor.includes(record.threadId)
      ? Effect.fail("settle-blocked" as never)
      : Effect.succeed({ sequence: dispatched.length });
  };
  return { dispatchCommand, dispatched };
}

const snapshotsWith = (threads: ReadonlyArray<DepartureThreadRow>) => {
  let queried = 0;
  const getShellSnapshot = () => {
    queried += 1;
    return Effect.succeed({ threads }) as never;
  };
  return { getShellSnapshot: getShellSnapshot as never, queried: () => queried };
};

const failingSnapshots = () => () => Effect.fail("db down" as never);

const cryptoStub = { randomUUIDv4: Effect.succeed("uuid-fixed") } as unknown as Crypto.Crypto;

const run = <A>(effect: Effect.Effect<A, never, Crypto.Crypto>) =>
  Effect.runPromise(effect.pipe(Effect.provideService(Crypto.Crypto, cryptoStub)));

describe("settleDepartedImBridgeThreads", () => {
  it("dispatches thread.settle for each removed member's live threads", async () => {
    const { dispatchCommand, dispatched } = fakeDispatch();
    const { getShellSnapshot } = snapshotsWith([
      thread("im-m-a-ms_aaaabbbbccccdddd"),
      thread("im-m-b-ms_1111222233334444"),
      thread("im-m-a-ms_5555666677778888", { settledAt: "2026-09-11T00:00:00.000Z" as never }),
    ]);
    const settled = await run(
      settleDepartedImBridgeThreads(dispatchCommand, getShellSnapshot, ["m-a"]),
    );
    expect(dispatched).toEqual([
      {
        type: "thread.settle",
        threadId: "im-m-a-ms_aaaabbbbccccdddd",
        commandId: "server:im-bridge-departure:im-m-a-ms_aaaabbbbccccdddd:uuid-fixed",
      },
    ]);
    expect(settled).toEqual([ThreadId.make("im-m-a-ms_aaaabbbbccccdddd")]);
  });

  it("skips the snapshot query entirely when no member was removed", async () => {
    const { dispatchCommand, dispatched } = fakeDispatch();
    const { getShellSnapshot, queried } = snapshotsWith([]);
    const settled = await run(settleDepartedImBridgeThreads(dispatchCommand, getShellSnapshot, []));
    expect(settled).toEqual([]);
    expect(dispatched).toEqual([]);
    expect(queried()).toBe(0);
  });

  it("returns empty (never throws) when the shell snapshot fails", async () => {
    const { dispatchCommand, dispatched } = fakeDispatch();
    const settled = await run(
      settleDepartedImBridgeThreads(dispatchCommand, failingSnapshots(), ["m-a"]),
    );
    expect(settled).toEqual([]);
    expect(dispatched).toEqual([]);
  });

  it("keeps settling the remaining threads when one dispatch fails", async () => {
    const { dispatchCommand, dispatched } = fakeDispatch(["im-m-a-ms_1111222233334444"]);
    const { getShellSnapshot } = snapshotsWith([
      thread("im-m-a-ms_aaaabbbbccccdddd"),
      thread("im-m-a-ms_1111222233334444"),
    ]);
    const settled = await run(
      settleDepartedImBridgeThreads(dispatchCommand, getShellSnapshot, ["m-a"]),
    );
    expect(dispatched.map((command) => command.threadId)).toEqual([
      "im-m-a-ms_aaaabbbbccccdddd",
      "im-m-a-ms_1111222233334444",
    ]);
    expect(settled).toEqual([ThreadId.make("im-m-a-ms_aaaabbbbccccdddd")]);
  });
});
