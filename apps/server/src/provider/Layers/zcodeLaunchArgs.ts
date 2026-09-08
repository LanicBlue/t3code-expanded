import { tokenizeCliArgs } from "@t3tools/shared/cliArgs";

export const T3CODE_ZCODE_LAUNCH_ARGS_ENV = "T3CODE_ZCODE_LAUNCH_ARGS";

export const resolveZcodeLaunchArgs = (
  launchArgs?: string,
  environment: NodeJS.ProcessEnv = process.env,
) => environment[T3CODE_ZCODE_LAUNCH_ARGS_ENV]?.trim() || launchArgs?.trim() || "";

export const zcodeLaunchArgv = (launchArgs?: string): ReadonlyArray<string> =>
  tokenizeCliArgs(launchArgs);
