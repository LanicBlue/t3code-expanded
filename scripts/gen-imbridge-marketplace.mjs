#!/usr/bin/env node
// Generate the IM Bridge template-marketplace asset bundled with the web app:
// reads a hive checkout's vendored marketplace snapshot (zh) and emits one
// JSON file under apps/web/public for the settings drawer to lazy-fetch.
//
//   node scripts/gen-imbridge-marketplace.mjs [path-to-hive-repo]
//
// Content is MIT-licensed (jnMetaCode/agency-agents-zh, a Chinese localization
// of Michael Sitarzewski's agency-agents); provenance is carried in the
// output's `source` block. Re-run after bumping the hive snapshot.

import fs from "node:fs";
import os from "node:os";
import path from "node:path";

const hiveRoot = process.argv[2] ?? path.join(os.homedir(), "projects", "hive");
const lang = "zh";
const srcDir = path.join(hiveRoot, "vendor", "marketplace", lang);
const outFile = path.join(
  import.meta.dirname,
  "..",
  "apps",
  "web",
  "public",
  "imbridge-marketplace.zh.json",
);

const manifest = JSON.parse(fs.readFileSync(path.join(srcDir, "manifest.json"), "utf8"));

/** Strip a leading YAML frontmatter block; keep the prompt body verbatim. */
const bodyOf = (raw) => {
  if (!raw.startsWith("---")) return raw.trim();
  const end = raw.indexOf("\n---", 3);
  if (end === -1) return raw.trim();
  return raw.slice(raw.indexOf("\n", end + 1) + 1).trim();
};

const agents = [];
for (const entry of manifest.agents) {
  const raw = fs.readFileSync(path.join(srcDir, entry.path), "utf8");
  agents.push({
    path: entry.path,
    category: entry.category,
    name: entry.name,
    description: entry.description,
    emoji: entry.emoji ?? null,
    color: entry.color ?? null,
    vibe: entry.vibe ?? null,
    prompt: bodyOf(raw),
  });
}

const out = {
  source: {
    repo: manifest.source.repo,
    commit: manifest.source.commit,
    fetched_at: manifest.source.fetched_at,
    license: "MIT (agency-agents-zh, Chinese localization of agency-agents)",
  },
  categories: manifest.categories,
  agents,
};

fs.mkdirSync(path.dirname(outFile), { recursive: true });
fs.writeFileSync(outFile, `${JSON.stringify(out, null, 1)}\n`);
console.log(
  `wrote ${outFile}: ${agents.length} agents, ${manifest.categories.length} categories, ` +
    `${(fs.statSync(outFile).size / 1024).toFixed(0)} KB`,
);
