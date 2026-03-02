import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent
WRAPPER = REPO_ROOT / "scripts" / "dump_signals_for_ai_review.ps1"


class TestDumpSignalsForAiReviewWrapper(unittest.TestCase):
    def test_wrapper_exists_and_uses_secrets_wrapper(self):
        self.assertTrue(WRAPPER.exists(), msg=f"missing wrapper: {WRAPPER}")
        text = WRAPPER.read_text(encoding="utf-8")
        self.assertIn("run_with_secrets.ps1", text)
        self.assertIn("--print-config", text.lower())
        self.assertIn("--dry-run", text.lower())
        self.assertIn("AI_REVIEW_DUMP_OUTPUT_PATH=", text)
        self.assertIn("--all-outreach", text.lower())

if __name__ == "__main__":
    unittest.main()
