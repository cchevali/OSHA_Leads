import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent
SCRIPT_PATH = REPO_ROOT / "scripts" / "autosave_wip.ps1"
INSTALLER_SCRIPT_PATH = REPO_ROOT / "scripts" / "install_wip_autosave_task.ps1"
AGENTS_PATH = REPO_ROOT / "AGENTS.md"
RUNBOOK_PATH = REPO_ROOT / "docs" / "RUNBOOK.md"


class TestAutosaveWipScriptContract(unittest.TestCase):
    def test_scripts_exist_under_scripts_dir(self):
        self.assertTrue(SCRIPT_PATH.exists(), msg=f"missing script: {SCRIPT_PATH}")
        self.assertTrue(INSTALLER_SCRIPT_PATH.exists(), msg=f"missing script: {INSTALLER_SCRIPT_PATH}")
        self.assertEqual(SCRIPT_PATH.parent.name.lower(), "scripts")
        self.assertEqual(INSTALLER_SCRIPT_PATH.parent.name.lower(), "scripts")

    def test_script_contains_required_tokens(self):
        text = SCRIPT_PATH.read_text(encoding="utf-8")
        required_tokens = [
            "PASS_WIP_AUTOSAVE_CLEAN",
            "PASS_WIP_AUTOSAVE_PUSHED branch=",
            "ERR_WIP_AUTOSAVE_PUSH_FAILED branch=",
        ]
        for token in required_tokens:
            self.assertIn(token, text)
        self.assertIn("wip_autosave_worktree", text)
        self.assertIn("wip_autosave.lock", text)
        self.assertIn("'worktree', 'add'", text)
        self.assertNotIn("Invoke-GitRepo -GitArgs @('reset', '--hard'", text)
        self.assertNotIn("Invoke-GitRepo -GitArgs @('restore'", text)
        self.assertNotIn("git -C $repoRoot reset --hard", text)
        self.assertNotIn("git -C $repoRoot restore", text)

    def test_installer_contains_warn_contract_and_repo_root_invocations(self):
        text = INSTALLER_SCRIPT_PATH.read_text(encoding="utf-8")
        required_tokens = [
            "WARN_WIP_AUTOSAVE_LOGON_NOT_INSTALLED access_denied=1",
            "WIP_AUTOSAVE_LOGON_INSTALL_ELEVATED_CMD=",
            "WIP_AUTOSAVE_HOURLY_INSTALLED=",
            "WIP_AUTOSAVE_LOGON_INSTALLED=",
            "WIP_AUTOSAVE_EFFECTIVE=",
            "WIP_AUTOSAVE_MODE=WORKTREE",
            "WIP_AUTOSAVE_NEXT_ACTION=",
            "WIP_AUTOSAVE_RUN_FROM_REPO_ROOT=powershell -NoProfile -ExecutionPolicy Bypass -File .\\scripts\\autosave_wip.ps1",
            "WIP_AUTOSAVE_INSTALL_FROM_REPO_ROOT_APPLY=powershell -NoProfile -ExecutionPolicy Bypass -File .\\scripts\\install_wip_autosave_task.ps1 --apply",
        ]
        for token in required_tokens:
            self.assertIn(token, text)
        self.assertIn("MinuteInterval 15", text)

    def test_docs_reference_correct_scripts_paths(self):
        agents = AGENTS_PATH.read_text(encoding="utf-8")
        runbook = RUNBOOK_PATH.read_text(encoding="utf-8")
        self.assertIn(".\\scripts\\autosave_wip.ps1", agents)
        self.assertIn(".\\scripts\\install_wip_autosave_task.ps1 --apply", agents)
        self.assertIn(".\\scripts\\autosave_wip.ps1", runbook)
        self.assertIn(".\\scripts\\install_wip_autosave_task.ps1 --apply", runbook)


if __name__ == "__main__":
    unittest.main()
