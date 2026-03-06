import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent
SCRIPT = REPO_ROOT / "run_with_secrets.ps1"
TARGET_SCRIPT = REPO_ROOT / "scripts" / "run_with_secrets.ps1"
SECRETS_TOOLING = REPO_ROOT / "scripts" / "secrets_tooling.ps1"


class TestRunWithSecretsWrapperContract(unittest.TestCase):
    def test_wrapper_has_context_pack_soft_check_contract(self):
        self.assertTrue(SCRIPT.exists(), msg=f"missing script: {SCRIPT}")
        text = SCRIPT.read_text(encoding="utf-8")

        self.assertIn("py -3 $contextPackScript --check --soft", text)
        self.assertIn("MFO_CONTEXT_PACK_SOFT_CHECK_DONE", text)
        self.assertIn("WARN_CONTEXT_PACK_SCRIPT_MISSING", text)
        self.assertIn("WARN_CONTEXT_PACK_CHECK_FAILED", text)
        self.assertIn("PASS_CONTEXT_PACK_CHECK", text)

    def test_soft_check_runs_before_payload_execution(self):
        self.assertTrue(SCRIPT.exists(), msg=f"missing script: {SCRIPT}")
        text = SCRIPT.read_text(encoding="utf-8")

        check_call = "Invoke-ContextPackSoftCheck -RepoRoot $PSScriptRoot"
        payload_call = "& $targetPath @forwardArgs"
        self.assertIn(check_call, text)
        self.assertIn(payload_call, text)
        self.assertLess(text.index(check_call), text.index(payload_call))

    def test_target_wrapper_contains_data_dir_provenance_contract(self):
        self.assertTrue(TARGET_SCRIPT.exists(), msg=f"missing script: {TARGET_SCRIPT}")
        target_text = TARGET_SCRIPT.read_text(encoding="utf-8")
        self.assertTrue(SECRETS_TOOLING.exists(), msg=f"missing script: {SECRETS_TOOLING}")
        tooling_text = SECRETS_TOOLING.read_text(encoding="utf-8")

        self.assertIn("Resolve-MfoDataDirPolicy", target_text)
        self.assertIn("MFO_DATA_DIR_EFFECTIVE", target_text)
        self.assertIn("MFO_DATA_DIR_SOURCE", target_text)
        self.assertIn("PYTHONWARNINGS", target_text)
        self.assertIn("ignore:urllib3", target_text)
        self.assertIn("OSHA_Leads\\python\\python.exe", target_text)
        self.assertIn("OSHA_Leads\\Python313\\python.exe", target_text)
        self.assertLess(
            target_text.index("OSHA_Leads\\python\\python.exe"),
            target_text.index("Get-Command -Name 'python'")
        )
        self.assertIn("WARN_ENV_CONFLICT=1 key=DATA_DIR", tooling_text)
        self.assertIn("WARN_DATA_DIR_NOT_ABSOLUTE=1", tooling_text)
        self.assertIn("OSHA_Leads\\keys\\age\\keys.txt", tooling_text)
        self.assertIn("$env:ProgramData", tooling_text)

    def test_target_wrapper_check_decrypt_uses_direct_sops_invocation(self):
        self.assertTrue(TARGET_SCRIPT.exists(), msg=f"missing script: {TARGET_SCRIPT}")
        target_text = TARGET_SCRIPT.read_text(encoding="utf-8")
        self.assertIn("Invoke-NativeAllowStderr -FilePath $sopsExe", target_text)
        self.assertNotIn("-FilePath 'cmd' -ArgumentList @('/c', $cmdLine)", target_text)

    def test_wrapper_sets_context_pack_sentinel_before_payload(self):
        self.assertTrue(SCRIPT.exists(), msg=f"missing script: {SCRIPT}")
        text = SCRIPT.read_text(encoding="utf-8")
        sentinel_assignment = "$env:MFO_CONTEXT_PACK_SOFT_CHECK_DONE = '1'"
        payload_call = "& $targetPath @forwardArgs"
        self.assertIn(sentinel_assignment, text)
        self.assertIn(payload_call, text)
        self.assertLess(text.index(sentinel_assignment), text.index(payload_call))

    def test_wrapper_supports_double_dash_delimiter(self):
        self.assertTrue(SCRIPT.exists(), msg=f"missing script: {SCRIPT}")
        text = SCRIPT.read_text(encoding="utf-8")
        self.assertIn("if ($forwardArgs.Count -ge 1 -and $forwardArgs[0] -eq '--')", text)
        self.assertIn("& $targetPath @forwardArgs", text)


if __name__ == "__main__":
    unittest.main()
