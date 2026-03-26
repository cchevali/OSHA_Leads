import subprocess
import sys
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent
SCRIPT = REPO_ROOT / "run_outreach_skipped_unsent.py"
SCHEDULED_WRAPPER = REPO_ROOT / "scripts" / "scheduled" / "run_outreach_skipped_unsent.ps1"


class TestRunOutreachSkippedUnsentWrapper(unittest.TestCase):
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
        self.assertIn("--dry-run", out)
        self.assertIn("--print-config", out)
        self.assertIn("--for-date", out)
        self.assertIn("--manifest", out)
        self.assertIn("--states", out)
        self.assertIn("--limit", out)
        self.assertIn("--confirm-live-send", out)
        self.assertIn("--to", out)

    def test_wrapper_is_thin_shim(self):
        self.assertTrue(SCRIPT.exists(), msg=f"missing script: {SCRIPT}")
        text = SCRIPT.read_text(encoding="utf-8")
        self.assertIn("from outreach.run_outreach_skipped_unsent import main", text)
        self.assertIn("raise SystemExit(main())", text)

    def test_scheduled_wrapper_contract_tokens(self):
        self.assertTrue(SCHEDULED_WRAPPER.exists(), msg=f"missing wrapper: {SCHEDULED_WRAPPER}")
        text = SCHEDULED_WRAPPER.read_text(encoding="utf-8")
        self.assertIn("OSHA_Outreach_Skipped_Unsent_Extra", text)
        self.assertIn("run_outreach_skipped_unsent.py", text)
        self.assertIn("runtime_guard.ps1", text)
        self.assertIn("runtime_run_summary.ps1", text)
        self.assertIn("OUTREACH_SKIPPED_UNSENT_SCHEDULED_DISABLED=1", text)
        self.assertIn("TASK_LOG_PATH=", text)
        self.assertIn("OUTREACH_EXIT_CODE=", text)
        self.assertIn("RUN_SUMMARY_JSON_PATH=", text)


if __name__ == "__main__":
    unittest.main()
