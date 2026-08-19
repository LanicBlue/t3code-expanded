/**
 * ProjectDirectoryKey - canonical workspace-directory identity.
 *
 * The Project Service persists workspaceDir as its canonical root key
 * (resolve + realpathSync.native + Windows case-fold, control/canonical-root-key.ts),
 * and V4 Work notices carry that canonical spelling. Every directory this
 * integration compares against — the notice's workspaceDir at routing time,
 * a STORED local workspaceRoot whose spelling may predate canonical storage
 * or traverse a symlink (/tmp vs /private/tmp) — must pass the same
 * canonicalization, or equivalent spellings fork duplicate projects and
 * directory-keyed tool matching fails permanently (issue #6 review finding).
 *
 * @module ProjectDirectoryKey
 */
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";

/**
 * Canonicalize a directory to the key the Project Service keys projects on:
 * resolve, then best-effort realpath, then case-fold on Windows only.
 *
 * Never fails: realpath of an already-validated directory can still lose to
 * permissions or a removal race, and a lookup KEY must not take the wake down
 * with it — the resolved spelling is the fallback, exactly one canonical step
 * short rather than wrong.
 */
export const canonicalWorkspaceDirectory = Effect.fn(
  "ProjectDirectoryKey.canonicalWorkspaceDirectory",
)(function* (directory: string) {
  const path = yield* Path.Path;
  const resolved = path.resolve(directory);
  const fileSystem = yield* FileSystem.FileSystem;
  const canonical = yield* fileSystem.realPath(resolved).pipe(Effect.orElseSucceed(() => resolved));
  // Mirror the Project Service key: NTFS/FAT/SMB are case-insensitive, POSIX
  // preserves case (canonical-root-key step 5).
  return process.platform === "win32" ? canonical.toLowerCase() : canonical;
});
