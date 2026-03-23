import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent
WRAPPER = REPO_ROOT / "scripts" / "import_prospect_ai_assist_from_clipboard.ps1"


class TestImportProspectAiAssistFromClipboardWrapper(unittest.TestCase):
    def test_wrapper_exists_and_supports_clipboard_and_print_config(self):
        self.assertTrue(WRAPPER.exists(), msg=f"missing wrapper: {WRAPPER}")
        text = WRAPPER.read_text(encoding="utf-8")
        self.assertIn("run_with_secrets.ps1", text)
        self.assertIn("Get-Clipboard -Raw", text)
        self.assertIn("--print-config", text.lower())
        self.assertIn("--stdin", text.lower())
        self.assertIn("$Batch", text)
        self.assertIn("--batch", text.lower())
        self.assertIn("ERR_AI_ASSIST_CLIPBOARD_EMPTY", text)


if __name__ == "__main__":
    unittest.main()
