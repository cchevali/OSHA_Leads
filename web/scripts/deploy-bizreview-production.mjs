/* eslint-disable no-console */
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

const args = new Set(process.argv.slice(2));
const scriptDirectory = dirname(fileURLToPath(import.meta.url));
const parityScript = join(scriptDirectory, "verify-bizreview-production-parity.mjs");
const repositoryRoot = join(scriptDirectory, "..", "..");
const npx = process.platform === "win32" ? "npx.cmd" : "npx";
const windowsCommandShell = process.env.ComSpec || "cmd.exe";

function printConfig() {
  console.log(
    JSON.stringify(
      {
        deploy:
          process.platform === "win32"
            ? [windowsCommandShell, "/d", "/s", "/c", "npx vercel --prod --yes"]
            : [npx, "vercel", "--prod", "--yes"],
        parity: [process.execPath, parityScript],
        vercelWorkingDirectory: repositoryRoot,
      },
      null,
      2,
    ),
  );
}

function run(command, commandArgs, { cwd = process.cwd(), shell = false } = {}) {
  const result = spawnSync(command, commandArgs, {
    cwd,
    stdio: "inherit",
    shell,
  });
  return result.status ?? 1;
}

function deployWithVercel() {
  if (process.platform === "win32") {
    return run(
      windowsCommandShell,
      ["/d", "/s", "/c", "npx vercel --prod --yes"],
      { cwd: repositoryRoot },
    );
  }
  return run(npx, ["vercel", "--prod", "--yes"], { cwd: repositoryRoot });
}

function main() {
  if (args.has("--print-config")) {
    printConfig();
    return;
  }

  if (args.has("--dry-run")) {
    console.log("DRY_RUN_BIZREVIEW_DEPLOY");
    printConfig();
    return;
  }

  const unknownArgs = [...args].filter((arg) => arg !== "--print-config" && arg !== "--dry-run");
  if (unknownArgs.length) {
    throw new Error(`ERR_BIZREVIEW_DEPLOY_ARGS unsupported=${unknownArgs.join(",")}`);
  }

  if (deployWithVercel() !== 0) {
    throw new Error("ERR_BIZREVIEW_DEPLOY_VERCEL");
  }

  if (run(process.execPath, [parityScript]) !== 0) {
    throw new Error("ERR_BIZREVIEW_DEPLOY_PARITY");
  }
}

try {
  main();
} catch (error) {
  console.error(error instanceof Error ? error.message : String(error));
  process.exit(1);
}
