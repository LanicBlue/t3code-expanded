/**
 * ZCodeTextGeneration – Text generation layer using the zcode CLI one-shot mode.
 *
 * Runs `node <zcode.cjs> --cwd <dir> --json --prompt <text> --mode yolo` and
 * parses the single JSON document zcode prints on stdout (`{sessionId,
 * response, usage, eventCount, …}`; leading log lines are tolerated by
 * scanning for the outermost JSON object). The model answers the structured
 * prompt inside `response`, which is then decoded through the operation's
 * JSON schema — same two-step contract as `GrokTextGeneration`.
 *
 * Limitations:
 *   - the one-shot CLI has no `--model` flag, so `modelSelection.model` is
 *     advisory only — generation always uses the CLI's configured default;
 *   - the prompt travels as an argv value (no stdin mode), so oversized
 *     prompts fail fast with a clear error instead of tripping ARG_MAX.
 *
 * @module ZCodeTextGeneration
 */
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";

import { type ModelSelection, TextGenerationError, type ZCodeSettings } from "@t3tools/contracts";
import { sanitizeBranchFragment, sanitizeFeatureBranchName } from "@t3tools/shared/git";
import { extractJsonObject } from "@t3tools/shared/schemaJson";
import { resolveSpawnCommand } from "@t3tools/shared/shell";

import * as TextGeneration from "./TextGeneration.ts";
import {
  buildBranchNamePrompt,
  buildCommitMessagePrompt,
  buildPrContentPrompt,
  buildThreadTitlePrompt,
} from "./TextGenerationPrompts.ts";
import {
  normalizeCliError,
  sanitizeCommitSubject,
  sanitizePrTitle,
  sanitizeThreadTitle,
} from "./TextGenerationUtils.ts";
import { resolveZcodeLaunchArgs, zcodeLaunchArgv } from "../provider/Layers/zcodeLaunchArgs.ts";

const ZCODE_TIMEOUT_MS = 180_000;
/** macOS ARG_MAX is ~1 MiB shared with the environment; stay well below it. */
const ZCODE_MAX_PROMPT_ARG_BYTES = 256 * 1024;

const ZcodeOneShotOutput = Schema.Struct({
  response: Schema.String,
});
const decodeZcodeOneShotEnvelope = Schema.decodeEffect(Schema.fromJsonString(ZcodeOneShotOutput));

/** Extract the outermost JSON object from mixed log/JSON stdout. */
function extractOutermostJson(stdout: string): string | undefined {
  const start = stdout.indexOf("{");
  const end = stdout.lastIndexOf("}");
  if (start === -1 || end === -1 || end <= start) {
    return undefined;
  }
  return stdout.slice(start, end + 1);
}

export const makeZcodeTextGeneration = Effect.fn("makeZcodeTextGeneration")(function* (
  zcodeSettings: ZCodeSettings,
  environment: NodeJS.ProcessEnv = process.env,
) {
  const commandSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;

  const readStreamAsString = <E>(
    operation: TextGenerationOperation,
    stream: Stream.Stream<Uint8Array, E>,
  ): Effect.Effect<string, TextGenerationError> =>
    stream.pipe(
      Stream.decodeText(),
      Stream.runFold(
        () => "",
        (acc, chunk) => acc + chunk,
      ),
      Effect.mapError((cause) =>
        normalizeCliError("zcode", operation, cause, "Failed to collect process output"),
      ),
    );

  const runZcodeJson = Effect.fn("ZCodeTextGeneration.runZcodeJson")(function* <
    S extends Schema.Top,
  >({
    operation,
    cwd,
    prompt,
    outputSchemaJson,
    modelSelection,
  }: {
    operation: TextGenerationOperation;
    cwd: string;
    prompt: string;
    outputSchemaJson: S;
    modelSelection: ModelSelection;
  }): Effect.fn.Return<S["Type"], TextGenerationError, S["DecodingServices"]> {
    // Referenced for symmetry with the other text-generation layers; the
    // one-shot CLI cannot select a model.
    void modelSelection;
    if (Buffer.byteLength(prompt, "utf8") > ZCODE_MAX_PROMPT_ARG_BYTES) {
      return yield* new TextGenerationError({
        operation,
        detail: "Prompt is too large for the zcode one-shot CLI (argv limit).",
      });
    }

    const spawnCommand = yield* resolveSpawnCommand(
      process.execPath,
      [
        zcodeSettings.binaryPath,
        ...zcodeLaunchArgv(resolveZcodeLaunchArgs(zcodeSettings.launchArgs, environment)),
        "--cwd",
        cwd,
        "--json",
        "--mode",
        "yolo",
        "--prompt",
        prompt,
      ],
      { env: environment },
    );
    const command = ChildProcess.make(spawnCommand.command, spawnCommand.args, {
      env: environment,
      cwd,
      shell: spawnCommand.shell,
    });

    const rawStdout = yield* Effect.gen(function* () {
      const child = yield* commandSpawner
        .spawn(command)
        .pipe(
          Effect.mapError((cause) =>
            normalizeCliError("zcode", operation, cause, "Failed to spawn zcode CLI process"),
          ),
        );
      const [stdout, stderr, exitCode] = yield* Effect.all(
        [
          readStreamAsString(operation, child.stdout),
          readStreamAsString(operation, child.stderr),
          child.exitCode.pipe(
            Effect.mapError((cause) =>
              normalizeCliError("zcode", operation, cause, "Failed to read zcode CLI exit code"),
            ),
          ),
        ],
        { concurrency: "unbounded" },
      );
      if (exitCode !== 0) {
        const stderrDetail = stderr.trim();
        const stdoutDetail = stdout.trim();
        const detail = stderrDetail.length > 0 ? stderrDetail : stdoutDetail;
        return yield* new TextGenerationError({
          operation,
          detail:
            detail.length > 0
              ? `zcode CLI command failed: ${detail}`
              : `zcode CLI command failed with code ${exitCode}.`,
        });
      }
      return stdout;
    }).pipe(
      Effect.scoped,
      Effect.timeoutOption(ZCODE_TIMEOUT_MS),
      Effect.flatMap(
        Option.match({
          onNone: () =>
            Effect.fail(
              new TextGenerationError({ operation, detail: "zcode CLI request timed out." }),
            ),
          onSome: (value) => Effect.succeed(value),
        }),
      ),
    );

    const envelopeJson = extractOutermostJson(rawStdout);
    if (envelopeJson === undefined) {
      return yield* new TextGenerationError({
        operation,
        detail: "zcode CLI returned unexpected output format.",
      });
    }
    const envelope = yield* decodeZcodeOneShotEnvelope(envelopeJson).pipe(
      Effect.catchTags({
        SchemaError: (cause) =>
          Effect.fail(
            new TextGenerationError({
              operation,
              detail: "zcode CLI returned unexpected output format.",
              cause,
            }),
          ),
      }),
    );

    const trimmed = envelope.response.trim();
    if (!trimmed) {
      return yield* new TextGenerationError({
        operation,
        detail: "zcode CLI returned an empty response.",
      });
    }

    const decodeOutput = Schema.decodeEffect(Schema.fromJsonString(outputSchemaJson));
    return yield* decodeOutput(extractJsonObject(trimmed)).pipe(
      Effect.catchTags({
        SchemaError: (cause) =>
          Effect.fail(
            new TextGenerationError({
              operation,
              detail: "zcode returned invalid structured output.",
              cause,
            }),
          ),
      }),
    );
  });

  const generateCommitMessage: TextGeneration.TextGeneration["Service"]["generateCommitMessage"] =
    Effect.fn("ZCodeTextGeneration.generateCommitMessage")(function* (input) {
      const { prompt, outputSchema } = buildCommitMessagePrompt({
        branch: input.branch,
        stagedSummary: input.stagedSummary,
        stagedPatch: input.stagedPatch,
        includeBranch: input.includeBranch === true,
        policy: input.policy,
      });

      const generated = yield* runZcodeJson({
        operation: "generateCommitMessage",
        cwd: input.cwd,
        prompt,
        outputSchemaJson: outputSchema,
        modelSelection: input.modelSelection,
      });

      return {
        subject: sanitizeCommitSubject(generated.subject),
        body: generated.body.trim(),
        ...("branch" in generated && typeof generated.branch === "string"
          ? { branch: sanitizeFeatureBranchName(generated.branch) }
          : {}),
      };
    });

  const generatePrContent: TextGeneration.TextGeneration["Service"]["generatePrContent"] =
    Effect.fn("ZCodeTextGeneration.generatePrContent")(function* (input) {
      const { prompt, outputSchema } = buildPrContentPrompt({
        baseBranch: input.baseBranch,
        headBranch: input.headBranch,
        commitSummary: input.commitSummary,
        diffSummary: input.diffSummary,
        diffPatch: input.diffPatch,
        policy: input.policy,
        changeRequestTemplate: input.changeRequestTemplate,
      });

      const generated = yield* runZcodeJson({
        operation: "generatePrContent",
        cwd: input.cwd,
        prompt,
        outputSchemaJson: outputSchema,
        modelSelection: input.modelSelection,
      });

      return {
        title: sanitizePrTitle(generated.title),
        body: generated.body.trim(),
      };
    });

  const generateBranchName: TextGeneration.TextGeneration["Service"]["generateBranchName"] =
    Effect.fn("ZCodeTextGeneration.generateBranchName")(function* (input) {
      const { prompt, outputSchema } = buildBranchNamePrompt({
        message: input.message,
        attachments: input.attachments,
      });

      const generated = yield* runZcodeJson({
        operation: "generateBranchName",
        cwd: input.cwd,
        prompt,
        outputSchemaJson: outputSchema,
        modelSelection: input.modelSelection,
      });

      return {
        branch: sanitizeBranchFragment(generated.branch),
      };
    });

  const generateThreadTitle: TextGeneration.TextGeneration["Service"]["generateThreadTitle"] =
    Effect.fn("ZCodeTextGeneration.generateThreadTitle")(function* (input) {
      const { prompt, outputSchema } = buildThreadTitlePrompt({
        message: input.message,
        previousTitle: input.previousTitle,
        attachments: input.attachments,
      });

      const generated = yield* runZcodeJson({
        operation: "generateThreadTitle",
        cwd: input.cwd,
        prompt,
        outputSchemaJson: outputSchema,
        modelSelection: input.modelSelection,
      });

      return {
        title: sanitizeThreadTitle(generated.title),
      } satisfies TextGeneration.ThreadTitleGenerationResult;
    });

  return {
    generateCommitMessage,
    generatePrContent,
    generateBranchName,
    generateThreadTitle,
  } satisfies TextGeneration.TextGeneration["Service"];
});

type TextGenerationOperation =
  | "generateCommitMessage"
  | "generatePrContent"
  | "generateBranchName"
  | "generateThreadTitle";
