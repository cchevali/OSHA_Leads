import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent
WRAPPER = REPO_ROOT / "scripts" / "scheduled" / "run_trial_jl_safety_daily.ps1"


class TestRunTrialJLSafetyDailyWrapper(unittest.TestCase):
    def test_wrapper_exists_and_has_required_tokens(self):
        self.assertTrue(WRAPPER.exists(), msg=f"missing wrapper: {WRAPPER}")
        text = WRAPPER.read_text(encoding="utf-8")
        self.assertIn("run_trial_daily.py", text)
        self.assertIn("--subscriber-key", text)
        self.assertIn("jl_safety_trial", text)
        self.assertIn("--send-live", text)
        self.assertIn("runtime_guard.ps1", text)
        self.assertIn("runtime_run_summary.ps1", text)
        self.assertIn("TASK_LOG_PATH=", text)
        self.assertIn("TRIAL_SUBSCRIBER_KEY=", text)
        self.assertIn("TRIAL_EXIT_CODE=", text)
        self.assertIn("RUN_SUMMARY_JSON_PATH=", text)


if __name__ == "__main__":
    unittest.main()
