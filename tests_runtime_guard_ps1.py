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
            proc = subprocess.run(
                ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", cmd],
                cwd=str(REPO_ROOT),
                capture_output=True,
                text=True,
            )
        out = (proc.stdout or "") + "\n" + (proc.stderr or "")
        self.assertEqual(proc.returncode, 0, msg=out)
        self.assertIn("OK=YES", out)
        self.assertIn(f"MFO_DATA_DIR_EFFECTIVE={data_dir}", out)
        self.assertIn(f"PYTHON_EXE={python_exe}", out)


if __name__ == "__main__":
    unittest.main()
