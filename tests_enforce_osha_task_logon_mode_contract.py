import os
import subprocess
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent
SCRIPT = REPO_ROOT / "scripts" / "enforce_osha_task_logon_mode.ps1"


def _run(*args: str, extra_env: dict[str, str] | None = None) -> subprocess.CompletedProcess:
    env = os.environ.copy()
    if extra_env:
        env.update(extra_env)
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
        env=env,
        capture_output=True,
        text=True,
    )


class TestEnforceOshaTaskLogonModeContract(unittest.TestCase):
    def test_script_exists(self) -> None:
        self.assertTrue(SCRIPT.exists(), msg=f"missing script: {SCRIPT}")

    def test_script_contains_required_tokens(self) -> None:
        text = SCRIPT.read_text(encoding="utf-8")
        self.assertIn("--print-config", text)
        self.assertIn("--dry-run", text)
        self.assertIn("--apply", text)
        self.assertIn("--verify", text)
        self.assertIn("OSHA_WIP_Autosave_Logon", text)
        self.assertIn("ONLOGON_trigger_requires_interactive", text)
        self.assertIn("/Change", text)
        self.assertIn("/RU", text)
        self.assertIn("/RP", text)
        self.assertIn("PASS_ENFORCE_OSHA_TASK_LOGON_MODE_VERIFY", text)
        self.assertIn("ERR_ENFORCE_OSHA_TASK_LOGON_MODE_VERIFY", text)

    def test_print_config_runs_and_redacts_password_presence_only(self) -> None:
        proc = _run(
            "--print-config",
            extra_env={
                "TASK_SCHED_USER": r"DESKTOP-Q8QM4N9\lever",
                "TASK_SCHED_PASSWORD": "dont-print-me",
            },
        )
        out = (proc.stdout or "") + "\n" + (proc.stderr or "")
        self.assertEqual(proc.returncode, 0, msg=out)
        self.assertIn("PASS_ENFORCE_OSHA_TASK_LOGON_MODE_PRINT_CONFIG", out)
        self.assertIn("ENFORCE_OSHA_TASK_LOGON_MODE_USER=DESKTOP-Q8QM4N9\\lever", out)
        self.assertIn("ENFORCE_OSHA_TASK_LOGON_MODE_PASSWORD_PRESENT=YES", out)
        self.assertNotIn("dont-print-me", out)

    def test_apply_requires_task_sched_password(self) -> None:
        proc = _run("--apply", extra_env={"TASK_SCHED_PASSWORD": ""})
        out = (proc.stdout or "") + "\n" + (proc.stderr or "")
        self.assertNotEqual(proc.returncode, 0, msg=out)
        self.assertIn("ERR_ENFORCE_OSHA_TASK_LOGON_MODE_CONFIG missing TASK_SCHED_PASSWORD", out)


if __name__ == "__main__":
    unittest.main()
