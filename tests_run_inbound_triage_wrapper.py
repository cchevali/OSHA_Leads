import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent
WRAPPER = REPO_ROOT / "scripts" / "scheduled" / "run_inbound_triage.ps1"


class TestRunInboundTriageWrapper(unittest.TestCase):
    def test_wrapper_exists_and_has_required_tokens(self):
        self.assertTrue(WRAPPER.exists(), msg=f"missing wrapper: {WRAPPER}")
        text = WRAPPER.read_text(encoding="utf-8")
        self.assertIn("runtime_guard.ps1", text)
        self.assertIn("runtime_run_summary.ps1", text)
        self.assertIn("Test-RuntimeTickIntervalSlotAlreadyHandled", text)
        self.assertIn("gmail_credentials_missing", text)
        self.assertIn("runtime_tick_same_slot", text)
        self.assertIn("TASK_LOG_PATH=", text)
        self.assertIn("INBOUND_TRIAGE_EXIT_CODE=", text)
        self.assertIn("RUN_SUMMARY_JSON_PATH=", text)
        self.assertIn("OSHA_Inbound_Triage", text)


if __name__ == "__main__":
    unittest.main()
