import json
import subprocess
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent
SCRIPT = REPO_ROOT / "scripts" / "scheduled" / "runtime_run_summary.ps1"


class TestRuntimeRunSummary(unittest.TestCase):
    def test_default_roots_follow_mfo_data_dir_effective(self):
        self.assertTrue(SCRIPT.exists(), msg=f"missing script: {SCRIPT}")
        with tempfile.TemporaryDirectory() as d:
            data_dir = Path(d) / "osha_data"
            data_dir.mkdir(parents=True, exist_ok=True)
            cmd = (
                f"$env:MFO_DATA_DIR_EFFECTIVE='{data_dir}'; "
                f". '{SCRIPT}'; "
                f"$a = Resolve-DefaultTaskLogRoot -RepoRoot '{REPO_ROOT}'; "
                f"$b = Resolve-DefaultRunSummaryRoot -RepoRoot '{REPO_ROOT}'; "
                "Write-Output ('TASK=' + $a); "
                "Write-Output ('SUMMARY=' + $b);"
            )
            proc = subprocess.run(
                ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", cmd],
                cwd=str(REPO_ROOT),
                capture_output=True,
                text=True,
            )
            out = (proc.stdout or "") + "\n" + (proc.stderr or "")
            self.assertEqual(proc.returncode, 0, msg=out)
            expected_task = str((data_dir / "out" / "task_logs").resolve())
            expected_summary = str((data_dir / "out" / "run_summaries").resolve())
            self.assertIn(f"TASK={expected_task}", out)
            self.assertIn(f"SUMMARY={expected_summary}", out)

    def test_writes_summary_json_and_text_with_tokens(self):
        self.assertTrue(SCRIPT.exists(), msg=f"missing script: {SCRIPT}")
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            task_log_root = tmp / "task_logs"
            summary_root = tmp / "run_summaries"
            task_log_root.mkdir(parents=True, exist_ok=True)
            summary_root.mkdir(parents=True, exist_ok=True)
            task_log = task_log_root / "sample.log"
            task_log.write_text(
                "\n".join(
                    [
                        "PASS_SAMPLE_TOKEN detail=ok",
                        "ERR_SAMPLE_TOKEN detail=fail",
                        "ROWS_INSERTED=12",
                        "AI_REVIEW_DUMP_OUTPUT_PATH=C:\\\\tmp\\\\dump.json",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            cmd = (
                f". '{SCRIPT}'; "
                f"$startLocal = Get-Date; "
                f"$startUtc = [datetime]::UtcNow; "
                f"Write-RuntimeRunSummary "
                f"-RepoRoot '{REPO_ROOT}' "
                f"-WrapperName 'TEST_WRAPPER' "
                f"-CommandLine 'echo test' "
                f"-Mode 'scheduled' "
                f"-Intent 'write' "
                f"-DryRun:$false "
                f"-ExitCode 1 "
                f"-StartLocal $startLocal "
                f"-StartUtc $startUtc "
                f"-TaskLogPath '{task_log}' "
                f"-TaskLogRoot '{task_log_root}' "
                f"-RunSummaryRoot '{summary_root}' | Out-Null"
            )
            proc = subprocess.run(
                ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", cmd],
                cwd=str(REPO_ROOT),
                capture_output=True,
                text=True,
            )
            out = (proc.stdout or "") + "\n" + (proc.stderr or "")
            self.assertEqual(proc.returncode, 0, msg=out)
            summary_files = list(summary_root.glob("*.summary.json"))
            text_files = list(summary_root.glob("*.summary.txt"))
            self.assertEqual(len(summary_files), 1, msg=str(summary_files))
            self.assertEqual(len(text_files), 1, msg=str(text_files))

            payload = json.loads(summary_files[0].read_text(encoding="utf-8"))
            self.assertEqual(payload.get("schema"), "runtime_run_summary_v1")
            self.assertEqual(payload.get("wrapper"), "TEST_WRAPPER")
            self.assertEqual(int(payload.get("exit_code", 0)), 1)
            self.assertIn("PASS_SAMPLE_TOKEN", payload.get("tokens", {}).get("pass", []))
            self.assertIn("ERR_SAMPLE_TOKEN", payload.get("tokens", {}).get("err", []))
            self.assertEqual(int(payload.get("counts", {}).get("ROWS_INSERTED", 0)), 12)


if __name__ == "__main__":
    unittest.main()
