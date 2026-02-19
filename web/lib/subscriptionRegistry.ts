import { spawnSync } from "node:child_process";
import path from "node:path";

type RegistryCommand = "onboarding-submit" | "stripe-ingest";

type RegistryResult = {
  ok: boolean;
  payload: Record<string, unknown>;
  exitCode: number;
  stderr: string;
};

function resolvePythonCommand(): { bin: string; args: string[] } {
  const bin = (process.env.WEB_PYTHON_BIN || "py").trim();
  const rawArgs = (process.env.WEB_PYTHON_ARGS || "-3").trim();
  const args = rawArgs ? rawArgs.split(/\s+/).filter(Boolean) : [];
  return { bin, args };
}

function parseJsonFromStdout(stdout: string): Record<string, unknown> {
  const lines = stdout
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter((line) => line.length > 0);
  if (lines.length === 0) {
    return { ok: false, err_code: "ERR_REGISTRY_EMPTY_STDOUT" };
  }
  const lastLine = lines[lines.length - 1];
  try {
    const parsed = JSON.parse(lastLine) as Record<string, unknown>;
    return parsed;
  } catch {
    return { ok: false, err_code: "ERR_REGISTRY_INVALID_JSON", raw: lastLine };
  }
}

export function runSubscriptionRegistryCommand(command: RegistryCommand, payload: Record<string, unknown>): RegistryResult {
  const scriptPath = path.join(process.cwd(), "..", "scripts", "subscription_registry_ops.py");
  const python = resolvePythonCommand();
  const result = spawnSync(python.bin, [...python.args, scriptPath, command, "--stdin-json"], {
    input: JSON.stringify(payload),
    encoding: "utf-8",
    timeout: 10000
  });

  if (result.error) {
    return {
      ok: false,
      payload: { ok: false, err_code: "ERR_REGISTRY_PROCESS_EXEC", detail: result.error.message },
      exitCode: -1,
      stderr: result.error.message
    };
  }

  const parsed = parseJsonFromStdout(result.stdout || "");
  return {
    ok: Boolean(parsed.ok),
    payload: parsed,
    exitCode: result.status ?? 1,
    stderr: (result.stderr || "").trim()
  };
}
