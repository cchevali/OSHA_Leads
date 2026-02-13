import io
import sys
import unittest
from contextlib import redirect_stderr
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent
SCRIPT = REPO_ROOT / "run_prospect_discovery.py"
CANONICAL_SCRIPT = REPO_ROOT / "outreach" / "run_prospect_discovery.py"
EXPECTED_WRAPPER_REL = Path("run_prospect_discovery.py")
EXPECTED_CANONICAL_REL = Path("outreach") / "run_prospect_discovery.py"


class TestRunProspectDiscoveryWrapper(unittest.TestCase):
    def test_repo_root_wrapper_exists(self):
        self.assertTrue(SCRIPT.exists(), msg=f"missing wrapper: {SCRIPT}")

    def test_two_file_model_for_run_prospect_discovery(self):
        self.assertTrue(CANONICAL_SCRIPT.exists(), msg=f"missing canonical implementation: {CANONICAL_SCRIPT}")
        discovered = {p.resolve().relative_to(REPO_ROOT.resolve()) for p in REPO_ROOT.rglob("run_prospect_discovery.py")}
        expected = {EXPECTED_WRAPPER_REL, EXPECTED_CANONICAL_REL}
        self.assertEqual(
            discovered,
            expected,
            msg="expected exactly these paths: .\\run_prospect_discovery.py and .\\outreach\\run_prospect_discovery.py",
        )

    def test_main_forwards_argv_and_exit_code(self):
        import run_prospect_discovery as wrapper

        captured = {"argv": None}
        orig_import = wrapper.importlib.import_module
        orig_argv = sys.argv[:]

        class _FakeModule:
            @staticmethod
            def main(argv=None):
                captured["argv"] = list(argv or [])
                return 7

        try:
            wrapper.importlib.import_module = lambda _name: _FakeModule()
            sys.argv = ["run_prospect_discovery.py", "--dry-run", "--print-config"]
            rc = wrapper.main()
        finally:
            wrapper.importlib.import_module = orig_import
            sys.argv = orig_argv

        self.assertEqual(rc, 7)
        self.assertEqual(captured["argv"], ["--dry-run", "--print-config"])

    def test_corrupted_install_missing_impl_emits_error(self):
        import run_prospect_discovery as wrapper

        orig_import = wrapper.importlib.import_module
        orig_argv = sys.argv[:]
        try:
            def _raise(_name):
                err = ModuleNotFoundError("missing module")
                err.name = "outreach.run_prospect_discovery"
                raise err

            wrapper.importlib.import_module = _raise
            sys.argv = ["run_prospect_discovery.py", "--dry-run"]
            buf = io.StringIO()
            with redirect_stderr(buf):
                rc = wrapper.main()
        finally:
            wrapper.importlib.import_module = orig_import
            sys.argv = orig_argv

        self.assertNotEqual(rc, 0)
        text = buf.getvalue()
        self.assertIn("ERR_DISCOVERY_IMPL_MISSING", text)


if __name__ == "__main__":
    unittest.main()
