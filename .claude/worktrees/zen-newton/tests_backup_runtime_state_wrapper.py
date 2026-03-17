import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent
WRAPPER = REPO_ROOT / "scripts" / "scheduled" / "backup_runtime_state.ps1"
SNAPSHOT_TOOL = REPO_ROOT / "tools" / "sqlite_snapshot_backup.py"


class TestBackupRuntimeStateWrapper(unittest.TestCase):
    def test_wrapper_and_tool_contract(self):
        self.assertTrue(WRAPPER.exists(), msg=f"missing wrapper: {WRAPPER}")
        self.assertTrue(SNAPSHOT_TOOL.exists(), msg=f"missing tool: {SNAPSHOT_TOOL}")
        text = WRAPPER.read_text(encoding="utf-8")
        self.assertIn("--print-config", text.lower())
        self.assertIn("$DryRun", text)
        self.assertIn("sqlite_snapshot_backup.py", text)
        self.assertIn("BACKUP_MANIFEST_PATH=", text)
        self.assertIn("PASS_BACKUP_RUNTIME_STATE", text)


if __name__ == "__main__":
    unittest.main()
