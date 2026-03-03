import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent
WRAPPER = REPO_ROOT / "scripts" / "scheduled" / "run_osha_ingest_evening.ps1"


class TestRunOshaIngestEveningWrapper(unittest.TestCase):
    def test_wrapper_exists_and_has_required_tokens(self):
        self.assertTrue(WRAPPER.exists(), msg=f"missing wrapper: {WRAPPER}")
        text = WRAPPER.read_text(encoding="utf-8")
        self.assertIn("run_osha_ingest_daily.py", text)
        self.assertIn("--states", text)
        self.assertIn("TX,CA,FL,OR,WA", text)
        self.assertIn("dump_signals_for_ai_review.ps1", text)
        self.assertIn("-SinceDays 14", text)
        self.assertIn("TASK_LOG_PATH=", text)
        self.assertIn("INGEST_EXIT_CODE=", text)
        self.assertIn("AI_REVIEW_DUMP_EXIT_CODE=", text)
        self.assertIn("AI_REVIEW_DUMP_OUTPUT_PATH=", text)
        self.assertIn("AI_REVIEW_DUMP_OUTREACH_MATCHED_TOTAL=", text)
        self.assertIn("AI_REVIEW_DUMP_SUBSCRIBERS_MATCHED_TOTAL=", text)


if __name__ == "__main__":
    unittest.main()
