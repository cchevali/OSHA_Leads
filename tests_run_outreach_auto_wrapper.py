import subprocess
import sys
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent
SCRIPT = REPO_ROOT / "run_outreach_auto.py"


class TestRunOutreachAutoWrapper(unittest.TestCase):
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
        self.assertIn("--doctor", out)
        self.assertIn("--dry-run", out)
        self.assertIn("--print-config", out)
        self.assertIn("--allow-repeat", out)
        self.assertIn("--to", out)

    def test_wrapper_is_thin_shim(self):
        self.assertTrue(SCRIPT.exists(), msg=f"missing script: {SCRIPT}")
        text = SCRIPT.read_text(encoding="utf-8")
        self.assertIn("from outreach.run_outreach_auto import main", text)
        self.assertIn("raise SystemExit(main())", text)


if __name__ == "__main__":
    unittest.main()
