import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent
SCRIPT = REPO_ROOT / "scripts" / "scheduled" / "runtime_guard.ps1"


class TestRuntimeGuardPowerShell(unittest.TestCase):
    def test_resolve_python_command_prefers_absolute_python_exe_when_path_is_empty(self):
        self.assertTrue(SCRIPT.exists(), msg=f"missing script: {SCRIPT}")
        python_exe = Path(sys.executable).resolve()
        cmd = (
            "$env:PATH=''; "
            f"$env:PYTHON_EXE='{python_exe}'; "
            f". '{SCRIPT}'; "
            "$resolved = Resolve-PythonCommand; "
            "Write-Output ('EXE=' + $resolved.Exe); "
            "Write-Output ('ARGS=' + (@($resolved.ArgsPrefix) -join ' '));"
        )
        proc = subprocess.run(
            ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", cmd],
            cwd=str(REPO_ROOT),
            capture_output=True,
            text=True,
        )
        out = (proc.stdout or "") + "\n" + (proc.stderr or "")
        self.assertEqual(proc.returncode, 0, msg=out)
        self.assertIn(f"EXE={python_exe}", out)
        self.assertIn("ARGS=", out)

    def test_invoke_runtime_preflight_sets_mfo_data_dir_effective_with_absolute_python(self):
        self.assertTrue(SCRIPT.exists(), msg=f"missing script: {SCRIPT}")
        python_exe = Path(sys.executable).resolve()
        with tempfile.TemporaryDirectory() as d:
            data_dir = Path(d).resolve()
            legacy_paths = [
                (REPO_ROOT / "data" / "osha.sqlite").resolve(),
                (REPO_ROOT / "out" / "crm.sqlite").resolve(),
                (REPO_ROOT / "out" / "crm_light.sqlite").resolve(),
            ]
            original: dict[Path, bytes | None] = {}
            for path in legacy_paths:
                original[path] = path.read_bytes() if path.exists() else None
                path.unlink(missing_ok=True)
            cmd = (
                "$env:PATH=''; "
                f"$env:PYTHON_EXE='{python_exe}'; "
                "$env:RUNTIME_ROLE='canonical_scheduler'; "
                "$env:CANONICAL_HOSTNAME=$env:COMPUTERNAME.ToLowerInvariant(); "
                f"$env:DATA_DIR='{data_dir}'; "
                f". '{SCRIPT}'; "
                f"$result = Invoke-RuntimePreflight -RepoRoot '{REPO_ROOT}' -Mode 'scheduled' -Intent 'write' -DryRun:$false -EmitLine {{ param($line) $null = $line }}; "
                "Write-Output ('OK=' + $(if ($result.Ok) { 'YES' } else { 'NO' })); "
                "Write-Output ('MFO_DATA_DIR_EFFECTIVE=' + $env:MFO_DATA_DIR_EFFECTIVE); "
                "Write-Output ('PYTHON_EXE=' + $env:PYTHON_EXE);"
            )
            try:
                proc = subprocess.run(
                    ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", cmd],
                    cwd=str(REPO_ROOT),
                    capture_output=True,
                    text=True,
                )
            finally:
                for path, prior_bytes in original.items():
                    if prior_bytes is None:
                        path.unlink(missing_ok=True)
                    else:
                        path.parent.mkdir(parents=True, exist_ok=True)
                        path.write_bytes(prior_bytes)
        out = (proc.stdout or "") + "\n" + (proc.stderr or "")
        self.assertEqual(proc.returncode, 0, msg=out)
        self.assertIn("OK=YES", out)
        self.assertIn(f"MFO_DATA_DIR_EFFECTIVE={data_dir}", out)
        self.assertIn(f"PYTHON_EXE={python_exe}", out)

    def test_runtime_tick_daily_slot_skip_detects_same_day_success(self):
        self.assertTrue(SCRIPT.exists(), msg=f"missing script: {SCRIPT}")
        with tempfile.TemporaryDirectory() as d:
            repo_root = Path(d).resolve()
            data_dir = repo_root / "canonical_data"
            state_dir = data_dir / "runtime" / "status" / "jobs"
            state_dir.mkdir(parents=True, exist_ok=True)
            (state_dir / "outreach_auto.json").write_text(
                '{"last_slot_key":"2026-03-10","last_result":"ran","last_run_summary_json_path":"C:\\\\osha_data\\\\out\\\\run_summaries\\\\x.json"}',
                encoding="utf-8",
            )
            cmd = (
                f"$env:DATA_DIR='{data_dir}'; "
                f". '{SCRIPT}'; "
                f"$result = Test-RuntimeTickDailySlotAlreadyCompleted -RepoRoot '{repo_root}' -JobName 'outreach_auto' -NowLocal ([datetime]::Parse('2026-03-10T08:05:00')); "
                "Write-Output ('SKIP=' + $(if ($result.Skip) { 'YES' } else { 'NO' })); "
                "Write-Output ('SLOT=' + $result.SlotKey);"
            )
            proc = subprocess.run(
                ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", cmd],
                cwd=str(REPO_ROOT),
                capture_output=True,
                text=True,
            )
        out = (proc.stdout or "") + "\n" + (proc.stderr or "")
        self.assertEqual(proc.returncode, 0, msg=out)
        self.assertIn("SKIP=YES", out)
        self.assertIn("SLOT=2026-03-10", out)


if __name__ == "__main__":
    unittest.main()
