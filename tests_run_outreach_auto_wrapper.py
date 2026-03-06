import subprocess
import sys
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent
SCRIPT = REPO_ROOT / "run_outreach_auto.py"
SCHEDULED_WRAPPER = REPO_ROOT / "scripts" / "scheduled" / "run_outreach_auto.ps1"


class TestRunOutreachAutoWrapper(unittest.TestCase):
    def test_help_lists_required_flags(self):
        self.assertTrue(SCRIPT.exists(), msg=f"missing script: {SCRIPT}")
        proc = subprocess.run(
            [sys.executable, str(SCRIPT), "--help"],
            cwd=str(REPO_ROOT),
            capture_output=True,
            text=True,
        )
        self.assertEqual(proc.returncode, 0, msg=(proc.stderr or "") + "\n" + (proc.stdout or ""))

        out = (proc.stdout or "") + "\n" + (proc.stderr or "")
        self.assertIn("--doctor", out)
        self.assertIn("--dry-run", out)
        self.assertIn("--plan", out)
        self.assertIn("--for-date", out)
        self.assertIn("--print-config", out)
        self.assertIn("--allow-repeat", out)
        self.assertIn("--allow-second-live-run-same-day", out)
        self.assertIn("--to", out)

    def test_wrapper_is_thin_shim(self):
        self.assertTrue(SCRIPT.exists(), msg=f"missing script: {SCRIPT}")
        text = SCRIPT.read_text(encoding="utf-8")
        self.assertIn("from outreach.run_outreach_auto import main", text)
        self.assertIn("raise SystemExit(main())", text)

    def test_scheduled_wrapper_contract_tokens(self):
        self.assertTrue(SCHEDULED_WRAPPER.exists(), msg=f"missing wrapper: {SCHEDULED_WRAPPER}")
        text = SCHEDULED_WRAPPER.read_text(encoding="utf-8")
        self.assertIn("run_outreach_auto.py", text)
        self.assertIn("runtime_guard.ps1", text)
        self.assertIn("runtime_run_summary.ps1", text)
        self.assertIn("TASK_LOG_PATH=", text)
        self.assertIn("OUTREACH_EXIT_CODE=", text)
        self.assertIn("RUN_SUMMARY_JSON_PATH=", text)


if __name__ == "__main__":
    unittest.main()
