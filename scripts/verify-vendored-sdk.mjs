/**
 * Verifies every vendored consumer-sdk tarball against its sha256 sidecar.
 *
 * The sidecars are written from the upstream release manifest
 * (project-service releases/consumer-sdk/manifest.json) at vendoring time;
 * a mismatch means the tarball drifted from what was reviewed — exit non-zero.
 *
 * Usage: node scripts/verify-vendored-sdk.mjs
 */
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";

const vendorDir = NodePath.join(
  NodePath.dirname(NodeURL.fileURLToPath(import.meta.url)),
  "..",
  "vendor",
  "consumer-sdk",
);

let failures = 0;
let checked = 0;

for (const entry of NodeFS.readdirSync(vendorDir, { withFileTypes: true }).sort((a, b) =>
  a.name.localeCompare(b.name),
)) {
  if (!entry.isFile() || !entry.name.endsWith(".sha256")) continue;
  const sidecar = NodePath.join(vendorDir, entry.name);
  const tarballName = entry.name.slice(0, -".sha256".length);
  const tarball = NodePath.join(vendorDir, tarballName);

  const expected = NodeFS.readFileSync(sidecar, "utf8").split(/\r?\n/)[0]?.split(/\s+/)[0];
  if (!expected || !/^[0-9a-f]{64}$/.test(expected)) {
    console.error(`✗ ${tarballName}: sidecar ${entry.name} does not start with a sha256 digest`);
    failures++;
    continue;
  }

  let actual;
  try {
    actual = NodeCrypto.createHash("sha256").update(NodeFS.readFileSync(tarball)).digest("hex");
  } catch {
    console.error(`✗ ${tarballName}: tarball missing or unreadable`);
    failures++;
    continue;
  }

  checked++;
  if (actual === expected) {
    console.log(`✓ ${tarballName}: sha256 ok (${actual})`);
  } else {
    console.error(`✗ ${tarballName}: sha256 mismatch`);
    console.error(`  expected ${expected}`);
    console.error(`  actual   ${actual}`);
    failures++;
  }
}

if (checked === 0) {
  console.error(`✗ no .sha256 sidecars found under ${vendorDir}`);
  failures++;
}

if (failures > 0) {
  console.error(`${failures} vendored sdk check(s) failed`);
  process.exit(1);
}
console.log(`${checked} vendored sdk tarball(s) verified`);
