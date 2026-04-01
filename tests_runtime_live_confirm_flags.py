import subprocess
import sys
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent


class TestRuntimeLiveConfirmFlags(unittest.TestCase):
    def _help_output(self, script: str, *args: str) -> str:
        proc = subprocess.run(
            [sys.executable, str(REPO_ROOT / script), *args, "--help"],
            cwd=str(REPO_ROOT),
            capture_output=True,
            text=True,
        )
        out = (proc.stdout or "") + "\n" + (proc.stderr or "")
        self.assertEqual(proc.returncode, 0, msg=out)
        return out

    def test_run_trial_daily_has_confirm_flag(self):
        out = self._help_output("run_trial_daily.py")
        self.assertIn("--confirm-live-send", out)
        self.assertIn("--allow-outside-send-window-live", out)

    def test_run_outreach_auto_has_confirm_flag(self):
        out = self._help_output("run_outreach_auto.py")
        self.assertIn("--confirm-live-send", out)

    def test_send_digest_email_has_confirm_flag(self):
        out = self._help_output("send_digest_email.py")
        self.assertIn("--confirm-live-send", out)
        self.assertIn("--allow-outside-send-window-live", out)

    def test_deliver_daily_has_confirm_flag(self):
        out = self._help_output("deliver_daily.py")
        self.assertIn("--confirm-live-send", out)
        self.assertIn("--allow-outside-send-window-live", out)

    def test_run_trial_admin_scope_enhancement_has_confirm_flag(self):
        out = self._help_output("run_trial_admin.py", "scope-enhancement")
        self.assertIn("--confirm-live-send", out)

    def test_run_wally_trial_has_confirm_flag(self):
        out = self._help_output("run_wally_trial.py")
        self.assertIn("--confirm-live-send", out)


if __name__ == "__main__":
    unittest.main()
