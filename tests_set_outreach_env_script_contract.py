import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent
SCRIPT_PATH = REPO_ROOT / "scripts" / "set_outreach_env.ps1"


class TestSetOutreachEnvScriptContract(unittest.TestCase):
    def test_script_exists(self):
        self.assertTrue(SCRIPT_PATH.exists(), msg=f"missing script: {SCRIPT_PATH}")

    def test_script_contains_required_params_tokens_and_guard(self):
        text = SCRIPT_PATH.read_text(encoding="utf-8")

        required_params = [
            "OutreachDailyLimit",
            "OutreachStates",
            "OshaSmokeTo",
            "OutreachSuppressionMaxAgeHours",
            "TrialSendsLimitDefault",
            "TrialExpiredBehaviorDefault",
            "TrialConversionUrl",
            "PrintConfig",
        ]
        required_tokens = [
            "ERR_ENV_SOPS_STAGED",
            "ERR_SET_OUTREACH_ENV_TOOLING",
            "ERR_SET_OUTREACH_ENV_DECRYPT",
            "ERR_SET_OUTREACH_ENV_ARGS",
            "ERR_SET_OUTREACH_ENV_ENCRYPT",
            "ERR_SET_OUTREACH_ENV_WRITE",
            "ERR_SET_OUTREACH_ENV_VERIFY",
            "ERR_SET_OUTREACH_ENV_PRINT_CONFIG",
            "PASS_SET_OUTREACH_ENV_APPLY",
            "PASS_SET_OUTREACH_ENV_VERIFY",
            "PASS_SET_OUTREACH_ENV_PRINT_CONFIG",
            "PASS_SET_OUTREACH_ENV_COMPLETE",
        ]

        for param in required_params:
            self.assertIn(param, text)
        for token in required_tokens:
            self.assertIn(token, text)

        self.assertIn("git -C $repoRoot diff --cached --name-only -- .env.sops", text)
        self.assertIn("ERR_ENV_SOPS_STAGED", text)


if __name__ == "__main__":
    unittest.main()
