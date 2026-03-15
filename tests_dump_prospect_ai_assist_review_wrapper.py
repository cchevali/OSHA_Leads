import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent
WRAPPER = REPO_ROOT / "scripts" / "dump_prospect_ai_assist_review.ps1"


class TestDumpProspectAiAssistReviewWrapper(unittest.TestCase):
    def test_wrapper_exists_and_uses_secrets_wrapper(self):
        self.assertTrue(WRAPPER.exists(), msg=f"missing wrapper: {WRAPPER}")
        text = WRAPPER.read_text(encoding="utf-8")
        self.assertIn("run_with_secrets.ps1", text)
        self.assertIn("--print-config", text.lower())
        self.assertIn("--dry-run", text.lower())
        self.assertIn("$RawTarget", text)
        self.assertIn("$PacketSize", text)
        self.assertIn("--raw-target", text.lower())
        self.assertIn("--packet-size", text.lower())
        self.assertIn("AI_ASSIST_DUMP_OUTPUT_PATH", text)
        self.assertIn("AI_ASSIST_PACKET_DIR", text)
        self.assertIn("AI_ASSIST_PACKET_MANIFEST_PATH", text)
        self.assertIn("$States", text)
        self.assertIn("--states", text.lower())
        self.assertIn("AI_ASSIST_DUMP_SCOPE=STATES", text)


if __name__ == "__main__":
    unittest.main()
