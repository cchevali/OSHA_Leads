import subprocess
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent
SCRIPT_PATH = REPO_ROOT / "scripts" / "verify_travel_readiness.ps1"
RUNBOOK_PATH = REPO_ROOT / "docs" / "RUNBOOK.md"
TODO_PATH = REPO_ROOT / "docs" / "TODO.md"


class TestTravelReadinessScriptContract(unittest.TestCase):
    def test_script_exists_and_contains_required_tokens(self):
        self.assertTrue(SCRIPT_PATH.exists(), msg=f"missing script: {SCRIPT_PATH}")
        text = SCRIPT_PATH.read_text(encoding="utf-8")

        required_tokens = [
            "ERR_TRAVEL_PREFLIGHT_ARGS",
            "ERR_TRAVEL_PREFLIGHT_RUNNER_MISSING",
            "ERR_TRAVEL_PREFLIGHT_CHECK_FAILED",
            "PASS_TRAVEL_PREFLIGHT_PRINT_CONFIG",
            "PASS_TRAVEL_PREFLIGHT_DRY_RUN",
            "TRAVEL_PREFLIGHT_REMOTE_PRIMARY=WINDOWS_RDP",
            "TRAVEL_PREFLIGHT_REMOTE_FALLBACK=GOOGLE_REMOTE_DESKTOP",
            "TRAVEL_PREFLIGHT_LIVE_RECOVERY_PATH=CANONICAL_PC_ONLY",
            "TRAVEL_PREFLIGHT_LAPTOP_SCOPE=print-config,doctor,dry-run,artifact_review,development",
            "TRAVEL_PREFLIGHT_PC_SCOPE=live_rerun,live_send,break_glass_recovery",
            "--print-config",
            "--dry-run",
            "--target",
            "--skip-tests",
            "Runtime Tick (Self-Hosted)",
            ".\\run_with_secrets.ps1 -- py -3 run_runtime_tick.py --doctor",
            ".\\run_with_secrets.ps1 -- py -3 run_runtime_tick.py --dry-run",
            "Google Remote Desktop",
            "Windows-native RDP",
            "Do not expose raw RDP to the public internet",
        ]
        for token in required_tokens:
            self.assertIn(token, text)

    def test_print_config_emits_expected_contract(self):
        proc = subprocess.run(
            [
                "powershell",
                "-NoProfile",
                "-ExecutionPolicy",
                "Bypass",
                "-File",
                str(SCRIPT_PATH),
                "--print-config",
            ],
            cwd=str(REPO_ROOT),
            capture_output=True,
            text=True,
        )
        out = (proc.stdout or "") + "\n" + (proc.stderr or "")
        self.assertEqual(proc.returncode, 0, msg=out)
        self.assertIn("TRAVEL_PREFLIGHT_MODE=print-config", out)
        self.assertIn("TRAVEL_PREFLIGHT_TARGET=both", out)
        self.assertIn("TRAVEL_PREFLIGHT_STEP_1_ID=context_pack_check", out)
        self.assertIn('TRAVEL_PREFLIGHT_STEP_6_COMMAND=gh run list --workflow "Runtime Tick (Self-Hosted)" --limit 5', out)
        self.assertIn("TRAVEL_PREFLIGHT_MANUAL_CHECK_1=Use Windows-native RDP to the canonical PC as the primary travel path for live operations.", out)
        self.assertIn("PASS_TRAVEL_PREFLIGHT_PRINT_CONFIG status=OK", out)

    def test_docs_reference_travel_preflight_workflow(self):
        runbook = RUNBOOK_PATH.read_text(encoding="utf-8")
        todo = TODO_PATH.read_text(encoding="utf-8")

        self.assertIn(".\\scripts\\verify_travel_readiness.ps1 --print-config", runbook)
        self.assertIn(".\\scripts\\verify_travel_readiness.ps1 --dry-run", runbook)
        self.assertIn("Windows-native RDP", runbook)
        self.assertIn("Google Remote Desktop", runbook)
        self.assertIn("Do not expose raw RDP directly to the public internet", runbook)

        self.assertIn(".\\scripts\\verify_travel_readiness.ps1 --dry-run", todo)
        self.assertIn("Windows-native RDP path from the laptop to the canonical PC", todo)
        self.assertIn("disconnect, reconnect, and confirm usable resolution/performance", todo)


if __name__ == "__main__":
    unittest.main()
