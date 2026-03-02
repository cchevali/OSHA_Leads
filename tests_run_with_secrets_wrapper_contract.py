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
        self.assertIn("WARN_CONTEXT_PACK_SCRIPT_MISSING", text)
        self.assertIn("WARN_CONTEXT_PACK_CHECK_FAILED", text)
        self.assertIn("PASS_CONTEXT_PACK_CHECK", text)

    def test_soft_check_runs_before_payload_execution(self):
        self.assertTrue(SCRIPT.exists(), msg=f"missing script: {SCRIPT}")
        text = SCRIPT.read_text(encoding="utf-8")

        check_call = "Invoke-ContextPackSoftCheck -RepoRoot $PSScriptRoot"
        payload_call = "& $targetPath @args"
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
        self.assertIn("WARN_ENV_CONFLICT=1 key=DATA_DIR", tooling_text)
        self.assertIn("WARN_DATA_DIR_NOT_ABSOLUTE=1", tooling_text)


if __name__ == "__main__":
    unittest.main()
