import { ThreadId } from "@t3tools/contracts";
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
import { OrchestrationThreadSettleBlockedError } from "./orchestration/Errors.ts";
import { PersistenceSqlError } from "./persistence/Errors.ts";
import type { OrchestrationEngineShape } from "./orchestration/Services/OrchestrationEngine.ts";
import type { ProjectionSnapshotQueryShape } from "./orchestration/Services/ProjectionSnapshotQuery.ts";

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
    thread("im-m-a-ms_1111222233334444", { settledAt: "2026-09-11T00:00:00.000Z" }),
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

function fakeEngine(failFor: ReadonlyArray<string> = []) {
  const dispatched: Array<{ type: string; threadId: string; commandId: string }> = [];
  const engine = {
    dispatch: (command: { type: string; threadId: string; commandId: string }) => {
      dispatched.push(command);
      return failFor.includes(command.threadId)
        ? Effect.fail(
            new OrchestrationThreadSettleBlockedError({
              threadId: ThreadId.make(command.threadId),
            }),
          )
        : Effect.succeed({ sequence: dispatched.length });
    },
  } as unknown as OrchestrationEngineShape;
  return { engine, dispatched };
}

const snapshotsWith = (threads: ReadonlyArray<DepartureThreadRow>) => {
  let queried = 0;
  const snapshots = {
    getShellSnapshot: () => {
      queried += 1;
      return Effect.succeed({ threads });
    },
  } as unknown as Pick<ProjectionSnapshotQueryShape, "getShellSnapshot">;
  return { snapshots, queried: () => queried };
};

const failingSnapshots = () =>
  ({
    getShellSnapshot: () =>
      Effect.fail(new PersistenceSqlError({ operation: "getShellSnapshot", detail: "db down" })),
  }) as unknown as Pick<ProjectionSnapshotQueryShape, "getShellSnapshot">;

const cryptoStub = { randomUUIDv4: Effect.succeed("uuid-fixed") } as unknown as Crypto.Crypto;

const run = <A>(effect: Effect.Effect<A, never, Crypto.Crypto>) =>
  Effect.runPromise(effect.pipe(Effect.provideService(Crypto.Crypto, cryptoStub)));

describe("settleDepartedImBridgeThreads", () => {
  it("dispatches thread.settle for each removed member's live threads", async () => {
    const { engine, dispatched } = fakeEngine();
    const { snapshots } = snapshotsWith([
      thread("im-m-a-ms_aaaabbbbccccdddd"),
      thread("im-m-b-ms_1111222233334444"),
      thread("im-m-a-ms_5555666677778888", { settledAt: "2026-09-11T00:00:00.000Z" }),
    ]);
    const settled = await run(settleDepartedImBridgeThreads(engine, snapshots, ["m-a"]));
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
    const { engine, dispatched } = fakeEngine();
    const { snapshots, queried } = snapshotsWith([]);
    const settled = await run(settleDepartedImBridgeThreads(engine, snapshots, []));
    expect(settled).toEqual([]);
    expect(dispatched).toEqual([]);
    expect(queried()).toBe(0);
  });

  it("returns empty (never throws) when the shell snapshot fails", async () => {
    const { engine, dispatched } = fakeEngine();
    const snapshots = failingSnapshots();
    const settled = await run(settleDepartedImBridgeThreads(engine, snapshots, ["m-a"]));
    expect(settled).toEqual([]);
    expect(dispatched).toEqual([]);
  });

  it("keeps settling the remaining threads when one dispatch fails", async () => {
    const { engine, dispatched } = fakeEngine(["im-m-a-ms_1111222233334444"]);
    const { snapshots } = snapshotsWith([
      thread("im-m-a-ms_aaaabbbbccccdddd"),
      thread("im-m-a-ms_1111222233334444"),
    ]);
    const settled = await run(settleDepartedImBridgeThreads(engine, snapshots, ["m-a"]));
    expect(dispatched.map((command) => command.threadId)).toEqual([
      "im-m-a-ms_aaaabbbbccccdddd",
      "im-m-a-ms_1111222233334444",
    ]);
    expect(settled).toEqual([ThreadId.make("im-m-a-ms_aaaabbbbccccdddd")]);
  });
});
