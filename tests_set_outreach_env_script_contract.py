import unittest
import os
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent
SCRIPT_PATH = REPO_ROOT / "scripts" / "set_outreach_env.ps1"
INSTALL_SCRIPT_PATH = REPO_ROOT / "scripts" / "install_scheduled_tasks.ps1"
RUN_AUTO_SCRIPT_PATH = REPO_ROOT / "outreach" / "run_outreach_auto.py"


def _cached_env_sops() -> str:
    proc = subprocess.run(
        ["git", "-C", str(REPO_ROOT), "diff", "--cached", "--name-only", "--", ".env.sops"],
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        return "ERR"
    return (proc.stdout or "").strip()


class TestSetOutreachEnvScriptContract(unittest.TestCase):
    def test_script_exists(self):
        self.assertTrue(SCRIPT_PATH.exists(), msg=f"missing script: {SCRIPT_PATH}")

    def test_script_contains_required_params_tokens_and_guard(self):
        text = SCRIPT_PATH.read_text(encoding="utf-8")

        required_params = [
            "OutreachDailyLimit",
            "OutreachStates",
            "OshaSmokeTo",
            "OutreachSuppressionMaxAgeHours",
            "TrialSendsLimitDefault",
            "TrialExpiredBehaviorDefault",
            "TrialConversionUrl",
            "PrintConfig",
        ]
        required_tokens = [
            "ERR_ENV_SOPS_STAGED",
            "ERR_SET_OUTREACH_ENV_TOOLING",
            "ERR_SET_OUTREACH_ENV_DECRYPT",
            "ERR_SET_OUTREACH_ENV_ARGS",
            "ERR_SET_OUTREACH_ENV_ENCRYPT",
            "ERR_SET_OUTREACH_ENV_WRITE",
            "ERR_SET_OUTREACH_ENV_VERIFY",
            "ERR_SET_OUTREACH_ENV_PRINT_CONFIG",
            "PASS_SET_OUTREACH_ENV_APPLY",
            "PASS_SET_OUTREACH_ENV_VERIFY",
            "PASS_SET_OUTREACH_ENV_PRINT_CONFIG",
            "PASS_SET_OUTREACH_ENV_COMPLETE",
        ]

        for param in required_params:
            self.assertIn(param, text)
        for token in required_tokens:
            self.assertIn(token, text)

        self.assertIn("git -C $repoRoot diff --cached --name-only -- .env.sops", text)
        self.assertIn("ERR_ENV_SOPS_STAGED", text)

    def test_doctor_and_installer_flows_do_not_stage_env_sops(self):
        before = _cached_env_sops()
        self.assertEqual(before, "", msg=f".env.sops is already staged before test: {before}")

        env = os.environ.copy()
        env.setdefault("OUTREACH_STATES", "TX")
        env.setdefault("OUTREACH_DAILY_LIMIT", "10")
        env.setdefault("OSHA_SMOKE_TO", "audit@example.com")
        env.setdefault("OUTREACH_SUPPRESSION_MAX_AGE_HOURS", "240")

        doctor = subprocess.run(
            [sys.executable, str(RUN_AUTO_SCRIPT_PATH), "--doctor"],
            cwd=str(REPO_ROOT),
            env=env,
            capture_output=True,
            text=True,
        )
        self.assertIn(
            doctor.returncode,
            (0, 2),
            msg=(doctor.stdout or "") + "\n" + (doctor.stderr or ""),
        )

        install = subprocess.run(
            [
                "powershell",
                "-NoProfile",
                "-ExecutionPolicy",
                "Bypass",
                "-File",
                str(INSTALL_SCRIPT_PATH),
                "--dry-run",
            ],
            cwd=str(REPO_ROOT),
            capture_output=True,
            text=True,
        )
        self.assertEqual(install.returncode, 0, msg=(install.stdout or "") + "\n" + (install.stderr or ""))

        after = _cached_env_sops()
        self.assertEqual(after, "", msg=f".env.sops was staged by flow: {after}")


if __name__ == "__main__":
    unittest.main()
