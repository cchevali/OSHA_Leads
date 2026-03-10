import json
import os
import socket
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent
SCRIPT = REPO_ROOT / "runtime_guard.py"


class TestRuntimeGuard(unittest.TestCase):
    def _run(self, args: list[str], env_overrides: dict[str, str | None]) -> subprocess.CompletedProcess:
        env = os.environ.copy()
        for key, value in env_overrides.items():
            if value is None:
                env.pop(key, None)
            else:
                env[key] = value
        return subprocess.run(
            [sys.executable, str(SCRIPT)] + args,
            cwd=str(REPO_ROOT),
            env=env,
            capture_output=True,
            text=True,
        )

    def test_invalid_runtime_role_errors(self):
        proc = self._run(
            ["preflight", "--mode", "manual", "--intent", "read"],
            {"RUNTIME_ROLE": "bad_role"},
        )
        out = (proc.stdout or "") + "\n" + (proc.stderr or "")
        self.assertNotEqual(proc.returncode, 0, msg=out)
        self.assertIn("ERR_RUNTIME_ROLE_INVALID", out)

    def test_host_mismatch_errors_for_live_send(self):
        with tempfile.TemporaryDirectory() as d:
            proc = self._run(
                ["preflight", "--mode", "manual", "--intent", "send"],
                {
                    "RUNTIME_ROLE": "dev_client",
                    "CANONICAL_HOSTNAME": "host_that_should_not_match",
                    "DATA_DIR": str(Path(d).resolve()),
                },
            )
        out = (proc.stdout or "") + "\n" + (proc.stderr or "")
        self.assertNotEqual(proc.returncode, 0, msg=out)
        self.assertIn("ERR_RUNTIME_HOST_MISMATCH", out)

    def test_repo_fallback_data_dir_errors_for_live_write(self):
        hostname = socket.gethostname().strip().lower()
        proc = self._run(
            ["preflight", "--mode", "manual", "--intent", "write"],
            {
                "RUNTIME_ROLE": "dev_client",
                "CANONICAL_HOSTNAME": hostname,
                "DATA_DIR": None,
            },
        )
        out = (proc.stdout or "") + "\n" + (proc.stderr or "")
        self.assertNotEqual(proc.returncode, 0, msg=out)
        self.assertIn("ERR_RUNTIME_DATA_DIR_REPO_FALLBACK", out)

    def test_confirm_live_send_required_when_requested(self):
        with tempfile.TemporaryDirectory() as d:
            hostname = socket.gethostname().strip().lower()
            proc = self._run(
                [
                    "preflight",
                    "--mode",
                    "manual",
                    "--intent",
                    "send",
                    "--require-confirm-live-send",
                ],
                {
                    "RUNTIME_ROLE": "dev_client",
                    "CANONICAL_HOSTNAME": hostname,
                    "DATA_DIR": str(Path(d).resolve()),
                    "MFO_TRUSTED_SCHEDULED": "0",
                },
            )
        out = (proc.stdout or "") + "\n" + (proc.stderr or "")
        self.assertNotEqual(proc.returncode, 0, msg=out)
        self.assertIn("ERR_RUNTIME_LIVE_CONFIRM_REQUIRED", out)

    def test_print_context_json_shape(self):
        with tempfile.TemporaryDirectory() as d:
            proc = self._run(["print-context"], {"DATA_DIR": str(Path(d).resolve())})
        out = (proc.stdout or "").strip()
        self.assertEqual(proc.returncode, 0, msg=(proc.stderr or "") + "\n" + out)
        payload = json.loads(out)
        self.assertIn("hostname", payload)
        self.assertIn("runtime_role", payload)
        self.assertIn("repo_root", payload)
        self.assertIn("db_crm", payload)
        self.assertEqual(
            payload.get("db_osha"),
            str((Path(d).resolve() / "osha.sqlite").resolve(strict=False)),
        )
        self.assertEqual(payload.get("db_osha_source"), "data_dir")

    def test_split_osha_db_errors_for_live_write(self):
        with tempfile.TemporaryDirectory() as d:
            data_dir = Path(d).resolve()
            (data_dir / "osha.sqlite").write_text("split", encoding="utf-8")
            hostname = socket.gethostname().strip().lower()
            proc = self._run(
                ["preflight", "--mode", "manual", "--intent", "write"],
                {
                    "RUNTIME_ROLE": "dev_client",
                    "CANONICAL_HOSTNAME": hostname,
                    "DATA_DIR": str(data_dir),
                },
            )
        out = (proc.stdout or "") + "\n" + (proc.stderr or "")
        self.assertNotEqual(proc.returncode, 0, msg=out)
        self.assertIn("ERR_RUNTIME_DB_OSHA_SPLIT", out)


if __name__ == "__main__":
    unittest.main()
