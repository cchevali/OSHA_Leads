import re
import subprocess
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent
SCRIPT = REPO_ROOT / "scripts" / "install_scheduled_tasks.ps1"
EXPECTED_GENERATION_TR = (
    "powershell.exe -NoProfile -ExecutionPolicy Bypass -File "
    r"C:\dev\OSHA_Leads\scripts\scheduled\run_prospect_generation.ps1"
)


def _run(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [
            "powershell",
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(SCRIPT),
            *args,
        ],
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
    )


def _parse_task_config(output: str) -> dict[int, dict[str, str]]:
    tasks: dict[int, dict[str, str]] = {}
    for line in (output or "").splitlines():
        match = re.match(r"^TASK_(\d+)_(NAME|TIME|RL|TR|TR_LENGTH)=(.*)$", line.strip())
        if not match:
            continue
        idx = int(match.group(1))
        key = match.group(2)
        value = match.group(3)
        tasks.setdefault(idx, {})[key] = value
    return tasks


class TestInstallScheduledTasks(unittest.TestCase):
    def test_print_config_includes_generation_task_and_exact_tr(self):
        self.assertTrue(SCRIPT.exists(), msg=f"missing script: {SCRIPT}")
        proc = _run("--print-config")
        out = (proc.stdout or "") + "\n" + (proc.stderr or "")
        self.assertEqual(proc.returncode, 0, msg=out)
        self.assertIn("INSTALL_SCHEDULED_TASKS_MODE=print-config", out)
        self.assertIn("PASS_INSTALL_SCHEDULED_TASKS_PRINT_CONFIG", out)

        tasks = _parse_task_config(out)
        self.assertEqual(len(tasks), 3, msg=out)

        generation = [t for t in tasks.values() if t.get("NAME") == "OSHA_Prospect_Generation"]
        self.assertEqual(len(generation), 1, msg=out)
        generation_task = generation[0]
        self.assertEqual(generation_task.get("TIME"), "07:15", msg=out)
        self.assertEqual(generation_task.get("RL"), "HIGHEST", msg=out)
        self.assertEqual(generation_task.get("TR"), EXPECTED_GENERATION_TR, msg=out)
        self.assertLess(len(EXPECTED_GENERATION_TR), 261)
        self.assertEqual(int(generation_task.get("TR_LENGTH", "0")), len(EXPECTED_GENERATION_TR), msg=out)

    def test_dry_run_outputs_commands_and_no_apply_token(self):
        proc = _run("--dry-run")
        out = (proc.stdout or "") + "\n" + (proc.stderr or "")
        self.assertEqual(proc.returncode, 0, msg=out)
        self.assertIn("INSTALL_SCHEDULED_TASKS_MODE=dry-run", out)
        self.assertIn("DRY_RUN_COMMAND_1=", out)
        self.assertIn("DRY_RUN_COMMAND_2=", out)
        self.assertIn("DRY_RUN_COMMAND_3=", out)
        self.assertIn("PASS_INSTALL_SCHEDULED_TASKS_DRY_RUN", out)
        self.assertNotIn("PASS_INSTALL_SCHEDULED_TASKS_APPLY", out)

    def test_invalid_args_emit_err_token(self):
        proc = _run("--dry-run", "--apply")
        out = (proc.stdout or "") + "\n" + (proc.stderr or "")
        self.assertNotEqual(proc.returncode, 0, msg=out)
        self.assertIn("ERR_INSTALL_SCHEDULED_TASKS_ARGS", out)


if __name__ == "__main__":
    unittest.main()
