import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent
SCRIPT = REPO_ROOT / "scripts" / "run_ops_console.ps1"


class TestRunOpsConsoleScriptContract(unittest.TestCase):
    def test_script_exists_and_enforces_localhost_module_launch(self):
        self.assertTrue(SCRIPT.exists(), msg=f"missing script: {SCRIPT}")
        text = SCRIPT.read_text(encoding="utf-8")
        self.assertIn("127.0.0.1", text)
        self.assertIn("MICROFLOWOPS_OPS_CONSOLE_URL=", text)
        self.assertIn("py -3 -m ops_console.app --host $hostIp --port $Port", text)


if __name__ == "__main__":
    unittest.main()
