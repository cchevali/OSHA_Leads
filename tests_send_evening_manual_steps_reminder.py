import os
import subprocess
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent
SCRIPT = REPO_ROOT / "scripts" / "scheduled" / "send_evening_manual_steps_reminder.py"


def _run(*args: str, extra_env: dict[str, str] | None = None) -> subprocess.CompletedProcess:
    env = os.environ.copy()
    if extra_env:
        env.update(extra_env)
    return subprocess.run(
        ["py", "-3", str(SCRIPT), *args],
        cwd=str(REPO_ROOT),
        env=env,
        capture_output=True,
        text=True,
    )


class TestSendEveningManualStepsReminder(unittest.TestCase):
    def test_print_config_uses_fallback_recipient(self):
        proc = _run(
            "--print-config",
            extra_env={
                "OSHA_EVENING_MANUAL_STEPS_TO": "",
                "OSHA_SMOKE_TO": "",
                "CHASE_EMAIL": "",
            },
        )
        out = (proc.stdout or "") + "\n" + (proc.stderr or "")
        self.assertEqual(proc.returncode, 0, msg=out)
        self.assertIn("EVENING_MANUAL_REMINDER_TO=cchevali+oshasmoke@gmail.com", out)
        self.assertIn("PASS_EVENING_REMINDER_SENT status=PRINT_CONFIG", out)

    def test_invalid_recipient_emits_error_token(self):
        proc = _run("--print-config", extra_env={"OSHA_EVENING_MANUAL_STEPS_TO": "not-an-email"})
        out = (proc.stdout or "") + "\n" + (proc.stderr or "")
        self.assertNotEqual(proc.returncode, 0, msg=out)
        self.assertIn("ERR_EVENING_REMINDER_CONFIG", out)

    def test_missing_smtp_config_emits_error_token(self):
        proc = _run(
            "--ingest-exit-code",
            "0",
            extra_env={
                "OSHA_EVENING_MANUAL_STEPS_TO": "cchevali+oshasmoke@gmail.com",
                "SMTP_HOST": "",
                "SMTP_PORT": "",
                "SMTP_USER": "",
                "SMTP_PASS": "",
            },
        )
        out = (proc.stdout or "") + "\n" + (proc.stderr or "")
        self.assertNotEqual(proc.returncode, 0, msg=out)
        self.assertIn("ERR_EVENING_REMINDER_SEND missing_smtp_config", out)


if __name__ == "__main__":
    unittest.main()
