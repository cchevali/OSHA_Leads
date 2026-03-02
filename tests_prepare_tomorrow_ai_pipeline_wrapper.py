import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent
SCRIPT_PATH = REPO_ROOT / "scripts" / "prepare_tomorrow_ai_pipeline.ps1"


class TestPrepareTomorrowAiPipelineWrapper(unittest.TestCase):
    def test_script_exists_and_contains_expected_contract(self):
        self.assertTrue(SCRIPT_PATH.exists(), msg=f"missing script: {SCRIPT_PATH}")
        text = SCRIPT_PATH.read_text(encoding="utf-8")

        self.assertIn("PrintConfig", text)
        self.assertIn("DryRun", text)
        self.assertIn("Apply", text)
        self.assertIn("$WrapperPath -- py -3", text)
        self.assertIn("tools\\import_ai_triage.py", text)
        self.assertIn("run_osha_ingest_daily.py", text)
        self.assertIn("run_prospect_generation.py", text)
        self.assertIn("run_prospect_discovery.py", text)
        self.assertIn("run_outreach_auto.py", text)
        self.assertIn("--doctor", text)
        self.assertIn("--dry-run", text)
        self.assertIn("run_wally_trial.py", text)
        self.assertIn("--test-send-daily", text)
        self.assertIn("PIPELINE_STEP_", text)
        self.assertIn("PIPELINE_READY_FOR_TOMORROW", text)
        self.assertIn("PIPELINE_IMPORT_INPUT", text)


if __name__ == "__main__":
    unittest.main()
