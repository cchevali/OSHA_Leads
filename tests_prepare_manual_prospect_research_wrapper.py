import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent
WRAPPER = REPO_ROOT / "scripts" / "prepare_manual_prospect_research.ps1"


class TestPrepareManualProspectResearchWrapper(unittest.TestCase):
    def test_wrapper_exists_and_uses_secrets_wrapper(self):
        self.assertTrue(WRAPPER.exists(), msg=f"missing wrapper: {WRAPPER}")
        text = WRAPPER.read_text(encoding="utf-8")
        self.assertIn("run_with_secrets.ps1", text)
        self.assertIn("--print-config", text.lower())
        self.assertIn("--dry-run", text.lower())
        self.assertIn("$TargetFirms", text)
        self.assertIn("--target-firms", text.lower())
        self.assertIn("$States", text)
        self.assertIn("--states", text.lower())
        self.assertIn("MANUAL_PROSPECT_RESEARCH_SCOPE=STATES", text)
        self.assertIn("MANUAL_PROSPECT_RESEARCH_SKIP_LIST_PATH", text)
        self.assertIn("MANUAL_PROSPECT_RESEARCH_PROMPT_OUTPUT_PATH", text)


if __name__ == "__main__":
    unittest.main()
