import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent
SCRIPT_PATH = REPO_ROOT / "scripts" / "autosave_wip.ps1"
INSTALLER_SCRIPT_PATH = REPO_ROOT / "scripts" / "install_wip_autosave_task.ps1"


class TestAutosaveWipScriptContract(unittest.TestCase):
    def test_script_exists(self):
        self.assertTrue(SCRIPT_PATH.exists(), msg=f"missing script: {SCRIPT_PATH}")
        self.assertTrue(INSTALLER_SCRIPT_PATH.exists(), msg=f"missing script: {INSTALLER_SCRIPT_PATH}")

    def test_script_contains_required_tokens(self):
        text = SCRIPT_PATH.read_text(encoding="utf-8")
        required_tokens = [
            "PASS_WIP_AUTOSAVE_CLEAN",
            "PASS_WIP_AUTOSAVE_PUSHED branch=",
            "ERR_WIP_AUTOSAVE_PUSH_FAILED branch=",
        ]
        for token in required_tokens:
            self.assertIn(token, text)


if __name__ == "__main__":
    unittest.main()
