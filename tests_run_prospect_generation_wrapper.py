import subprocess
import sys
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent
SCRIPT = REPO_ROOT / "run_prospect_generation.py"
CANONICAL_SCRIPT = REPO_ROOT / "outreach" / "run_prospect_generation.py"
EXPECTED_WRAPPER_REL = Path("run_prospect_generation.py")
EXPECTED_CANONICAL_REL = Path("outreach") / "run_prospect_generation.py"


class TestRunProspectGenerationWrapper(unittest.TestCase):
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
        self.assertIn("--print-config", out)
        self.assertIn("--dry-run", out)

    def test_wrapper_is_thin_shim(self):
        self.assertTrue(SCRIPT.exists(), msg=f"missing script: {SCRIPT}")
        text = SCRIPT.read_text(encoding="utf-8")
        self.assertIn("from outreach.run_prospect_generation import main", text)
        self.assertIn("raise SystemExit(main())", text)

    def test_two_file_model_for_run_prospect_generation(self):
        self.assertTrue(CANONICAL_SCRIPT.exists(), msg=f"missing canonical implementation: {CANONICAL_SCRIPT}")
        discovered = {p.resolve().relative_to(REPO_ROOT.resolve()) for p in REPO_ROOT.rglob("run_prospect_generation.py")}
        expected = {EXPECTED_WRAPPER_REL, EXPECTED_CANONICAL_REL}
        self.assertEqual(
            discovered,
            expected,
            msg="expected exactly these paths: .\\run_prospect_generation.py and .\\outreach\\run_prospect_generation.py",
        )


if __name__ == "__main__":
    unittest.main()
