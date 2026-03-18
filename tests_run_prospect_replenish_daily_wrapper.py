import subprocess
import sys
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent
SCRIPT = REPO_ROOT / "run_prospect_replenish_daily.py"
CANONICAL_SCRIPT = REPO_ROOT / "outreach" / "run_prospect_replenish_daily.py"
SCHEDULED_WRAPPER = REPO_ROOT / "scripts" / "scheduled" / "run_prospect_replenish_daily.ps1"
EXPECTED_WRAPPER_REL = Path("run_prospect_replenish_daily.py")
EXPECTED_CANONICAL_REL = Path("outreach") / "run_prospect_replenish_daily.py"


class TestRunProspectReplenishDailyWrapper(unittest.TestCase):
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
        self.assertIn("--print-config", out)
        self.assertIn("--doctor", out)
        self.assertIn("--dry-run", out)
        self.assertIn("--for-date", out)

    def test_wrapper_is_thin_shim(self):
        text = SCRIPT.read_text(encoding="utf-8")
        self.assertIn("from outreach.run_prospect_replenish_daily import main", text)
        self.assertIn("raise SystemExit(main())", text)

    def test_two_file_model_for_run_prospect_replenish_daily(self):
        self.assertTrue(CANONICAL_SCRIPT.exists(), msg=f"missing canonical implementation: {CANONICAL_SCRIPT}")
        discovered = {
            p.resolve().relative_to(REPO_ROOT.resolve())
            for p in REPO_ROOT.rglob("run_prospect_replenish_daily.py")
            if not p.resolve().relative_to(REPO_ROOT.resolve()).as_posix().startswith(".local/wip_autosave_worktree/")
            and not p.resolve().relative_to(REPO_ROOT.resolve()).as_posix().startswith(".claude/")
        }
        expected = {EXPECTED_WRAPPER_REL, EXPECTED_CANONICAL_REL}
        self.assertEqual(
            discovered,
            expected,
            msg="expected exactly these paths: .\\run_prospect_replenish_daily.py and .\\outreach\\run_prospect_replenish_daily.py",
        )

    def test_scheduled_wrapper_contract_tokens(self):
        self.assertTrue(SCHEDULED_WRAPPER.exists(), msg=f"missing wrapper: {SCHEDULED_WRAPPER}")
        text = SCHEDULED_WRAPPER.read_text(encoding="utf-8")
        self.assertIn("run_prospect_replenish_daily.py", text)
        self.assertIn("runtime_guard.ps1", text)
        self.assertIn("runtime_run_summary.ps1", text)
        self.assertIn("OSHA_Prospect_Replenish_SafetyNet", text)
        self.assertIn("TASK_LOG_PATH=", text)
        self.assertIn("PROSPECT_REPLENISH_EXIT_CODE=", text)
        self.assertIn("RUN_SUMMARY_JSON_PATH=", text)


if __name__ == "__main__":
    unittest.main()
