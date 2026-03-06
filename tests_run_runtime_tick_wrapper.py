import subprocess
import sys
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent
SCRIPT = REPO_ROOT / "run_runtime_tick.py"
CANONICAL_SCRIPT = REPO_ROOT / "outreach" / "run_runtime_tick.py"
WORKFLOW = REPO_ROOT / ".github" / "workflows" / "runtime-tick-selfhosted.yml"
BUILD_ARGS_SCRIPT = REPO_ROOT / "scripts" / "build_runtime_tick_args.ps1"
RUN_WORKFLOW_SCRIPT = REPO_ROOT / "scripts" / "run_runtime_tick_workflow.ps1"


class TestRunRuntimeTickWrapper(unittest.TestCase):
    def test_help_lists_required_flags(self):
        self.assertTrue(SCRIPT.exists(), msg=f"missing script: {SCRIPT}")
        proc = subprocess.run(
            [sys.executable, str(SCRIPT), "--help"],
            cwd=str(REPO_ROOT),
            capture_output=True,
            text=True,
        )
        self.assertEqual(proc.returncode, 0, msg=(proc.stderr or "") + "\n" + (proc.stdout or ""))
        out = (proc.stdout or "") + "\n" + (proc.stderr or "")
        self.assertIn("--print-config", out)
        self.assertIn("--doctor", out)
        self.assertIn("--dry-run", out)
        self.assertIn("--job", out)
        self.assertIn("--now-local", out)
        self.assertIn("--force", out)

    def test_wrapper_is_thin_shim(self):
        text = SCRIPT.read_text(encoding="utf-8")
        self.assertIn("from outreach.run_runtime_tick import main", text)
        self.assertIn("raise SystemExit(main())", text)

    def test_canonical_impl_exists(self):
        self.assertTrue(CANONICAL_SCRIPT.exists(), msg=f"missing canonical implementation: {CANONICAL_SCRIPT}")

    def test_runtime_tick_workflow_exists(self):
        self.assertTrue(WORKFLOW.exists(), msg=f"missing workflow: {WORKFLOW}")

    def test_workflow_uses_repo_scripts_with_cmd_shell(self):
        text = WORKFLOW.read_text(encoding="utf-8")
        self.assertIn("shell: cmd", text)
        self.assertIn(".\\scripts\\build_runtime_tick_args.ps1", text)
        self.assertIn(".\\scripts\\run_runtime_tick_workflow.ps1", text)
        self.assertIn('-WorkspacePath "%GITHUB_WORKSPACE%"', text)
        self.assertNotIn("cd C:\\dev\\OSHA_Leads", text)

    def test_runtime_tick_workflow_scripts_exist(self):
        self.assertTrue(BUILD_ARGS_SCRIPT.exists(), msg=f"missing workflow helper: {BUILD_ARGS_SCRIPT}")
        self.assertTrue(RUN_WORKFLOW_SCRIPT.exists(), msg=f"missing workflow helper: {RUN_WORKFLOW_SCRIPT}")


if __name__ == "__main__":
    unittest.main()
