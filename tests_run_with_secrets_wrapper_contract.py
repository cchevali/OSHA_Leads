import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent
SCRIPT = REPO_ROOT / "run_with_secrets.ps1"


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


if __name__ == "__main__":
    unittest.main()
