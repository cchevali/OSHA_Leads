import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent
SCRIPT = REPO_ROOT / "run_runtime_tick.py"
CANONICAL_SCRIPT = REPO_ROOT / "outreach" / "run_runtime_tick.py"
WORKFLOW = REPO_ROOT / ".github" / "workflows" / "runtime-tick-selfhosted.yml"
TRIAL_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "trial-facs-daily-selfhosted.yml"
INGEST_EVENING_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "ingest-evening-ai-review-selfhosted.yml"
MANUAL_WRAPPER_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "manual-wrapper-smoke-selfhosted.yml"
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

    def test_runtime_tick_artifact_paths_match_canonical_roots(self):
        text = WORKFLOW.read_text(encoding="utf-8")
        self.assertIn(r"C:\osha_data\out\task_logs\**", text)
        self.assertIn(r"C:\osha_data\out\run_summaries\**", text)
        self.assertIn(r"C:\osha_data\out\backups\**", text)
        self.assertIn(r"C:\osha_data\runtime\status\**", text)

    def test_break_glass_wrapper_workflows_are_dispatch_only(self):
        for path in [TRIAL_WORKFLOW, MANUAL_WRAPPER_WORKFLOW]:
            text = path.read_text(encoding="utf-8")
            self.assertIn("workflow_dispatch:", text, msg=f"expected dispatch-only workflow: {path}")
            self.assertNotIn("\nschedule:", text, msg=f"unexpected scheduled trigger in {path}")

    def test_ingest_evening_workflow_has_dst_safe_schedule_and_local_gate(self):
        text = INGEST_EVENING_WORKFLOW.read_text(encoding="utf-8")
        self.assertIn("workflow_dispatch:", text)
        self.assertIn("  schedule:", text)
        self.assertIn("cron: '45 0 * * *'", text)
        self.assertIn("cron: '45 1 * * *'", text)
        self.assertIn("Gate Exact 8:45 PM ET Daily", text)
        self.assertIn("$etNow.Hour -eq 20 -and $etNow.Minute -eq 45", text)

    def test_manual_wrapper_artifact_paths_match_canonical_out_roots(self):
        for path in [TRIAL_WORKFLOW, INGEST_EVENING_WORKFLOW, MANUAL_WRAPPER_WORKFLOW]:
            text = path.read_text(encoding="utf-8")
            self.assertIn(r"C:\osha_data\out\task_logs\**", text, msg=f"missing task log upload path in {path}")
            self.assertIn(r"C:\osha_data\out\run_summaries\**", text, msg=f"missing run summary upload path in {path}")
            self.assertIn(r"C:\osha_data\out\backups\**", text, msg=f"missing backup upload path in {path}")

    def test_no_non_runtime_tick_workflow_schedules_live_wrappers(self):
        workflow_paths = list((REPO_ROOT / ".github" / "workflows").glob("*.yml"))
        live_wrapper_markers = (
            r".\scripts\scheduled\run_trial_facs_daily.ps1",
            r".\scripts\scheduled\run_osha_ingest_evening.ps1",
            r".\scripts\scheduled\run_outreach_auto.ps1",
        )
        offenders: list[str] = []
        for path in workflow_paths:
            if path in {WORKFLOW, INGEST_EVENING_WORKFLOW}:
                continue
            text = path.read_text(encoding="utf-8")
            if "\nschedule:" not in text:
                continue
            if any(marker in text for marker in live_wrapper_markers):
                offenders.append(path.name)
        self.assertEqual(offenders, [], msg=f"overlapping scheduled wrapper workflows: {','.join(offenders)}")

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

    def test_build_runtime_tick_args_script_executes(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "github_output.txt"
            env = dict(**os.environ, GITHUB_OUTPUT=str(output_path))
            proc = subprocess.run(
                [
                    "powershell",
                    "-NoProfile",
                    "-ExecutionPolicy",
                    "Bypass",
                    "-File",
                    str(BUILD_ARGS_SCRIPT),
                    "-GithubEventName",
                    "workflow_dispatch",
                    "-Mode",
                    "doctor",
                    "-Job",
                    "all",
                    "-NowLocal",
                    "",
                    "-Force",
                    "false",
                ],
                cwd=str(REPO_ROOT),
                capture_output=True,
                text=True,
                env=env,
            )
            self.assertEqual(proc.returncode, 0, msg=(proc.stderr or "") + "\n" + (proc.stdout or ""))
            self.assertTrue(output_path.exists(), msg="expected GITHUB_OUTPUT file to be written")
            self.assertIn("cmd_args=run_runtime_tick.py --doctor --job all", output_path.read_text(encoding="utf-8"))

    def test_run_runtime_tick_workflow_script_errors_cleanly_when_args_missing(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            workspace = Path(tmpdir)
            runner = workspace / "run_with_secrets.ps1"
            runner.write_text(
                "param([Parameter(ValueFromRemainingArguments=$true)][string[]]$Args)\n"
                "Write-Output ('RUNNER_ARGS=' + ($Args -join ' '))\n"
                "exit 0\n",
                encoding="utf-8",
            )
            proc = subprocess.run(
                [
                    "powershell",
                    "-NoProfile",
                    "-ExecutionPolicy",
                    "Bypass",
                    "-File",
                    str(RUN_WORKFLOW_SCRIPT),
                    "-WorkspacePath",
                    str(workspace),
                ],
                cwd=str(REPO_ROOT),
                capture_output=True,
                text=True,
            )
            self.assertNotEqual(proc.returncode, 0, msg=proc.stdout)
            self.assertIn("ERR_RUNTIME_TICK_WORKFLOW_ARGS_MISSING", proc.stdout)


if __name__ == "__main__":
    unittest.main()
