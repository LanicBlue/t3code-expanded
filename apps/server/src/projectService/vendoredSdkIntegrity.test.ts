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
