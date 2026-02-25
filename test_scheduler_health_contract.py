import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent
SCRIPT = REPO_ROOT / "scripts" / "scheduled" / "scheduler_health.ps1"


class TestSchedulerHealthContract(unittest.TestCase):
    def test_scheduler_health_script_exists_and_has_stable_tokens(self) -> None:
        self.assertTrue(SCRIPT.exists(), msg=f"missing script: {SCRIPT}")
        text = SCRIPT.read_text(encoding="utf-8")
        self.assertIn("TASK_HEALTH|", text)
        self.assertIn("MISSED_RUNS=", text)
        self.assertIn("LAST_TASK_RESULT_DEC=", text)
        self.assertIn("LAST_TASK_RESULT_HEX=", text)
        self.assertIn("TRIGGER_SUMMARY=", text)
        self.assertIn("ACTION_EXECUTE=", text)
        self.assertIn("TASK_EVENT|", text)
        self.assertIn("TASKSCHED_OPERATIONAL_LOG_ENABLED=", text)
        self.assertIn("EXPECTED_TASK|TASK_NAME=OSHA_Outreach_Auto", text)
        self.assertIn("EXPECTED_TASK|TASK_NAME=OSHA Wally Trial Daily", text)


if __name__ == "__main__":
    unittest.main()
