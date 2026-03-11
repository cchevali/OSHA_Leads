import json
import os
import socket
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import runtime_guard

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
        self.assertIn("db_crm_legacy_exists", payload)
        self.assertIn("db_crm_light_legacy_exists", payload)

    def test_split_osha_db_errors_for_live_write(self):
        with tempfile.TemporaryDirectory() as d:
            data_dir = Path(d).resolve()
            (data_dir / "osha.sqlite").write_text("split", encoding="utf-8")
            legacy_osha = (REPO_ROOT / "data" / "osha.sqlite").resolve()
            original_bytes = legacy_osha.read_bytes() if legacy_osha.exists() else None
            legacy_osha.parent.mkdir(parents=True, exist_ok=True)
            legacy_osha.write_text("legacy", encoding="utf-8")
            hostname = socket.gethostname().strip().lower()
            try:
                proc = self._run(
                    ["preflight", "--mode", "manual", "--intent", "write"],
                    {
                        "RUNTIME_ROLE": "dev_client",
                        "CANONICAL_HOSTNAME": hostname,
                        "DATA_DIR": str(data_dir),
                    },
                )
            finally:
                if original_bytes is None:
                    legacy_osha.unlink(missing_ok=True)
                else:
                    legacy_osha.write_bytes(original_bytes)
        out = (proc.stdout or "") + "\n" + (proc.stderr or "")
        self.assertNotEqual(proc.returncode, 0, msg=out)
        self.assertIn("ERR_RUNTIME_DB_OSHA_LEGACY_PRESENT", out)
        self.assertIn("ERR_RUNTIME_DB_OSHA_SPLIT", out)

    def test_split_osha_db_errors_for_canonical_scheduler_on_canonical_host(self):
        with tempfile.TemporaryDirectory() as d:
            data_dir = Path(d).resolve()
            osha_db = (data_dir / "osha.sqlite").resolve()
            osha_db.write_text("split", encoding="utf-8")
            legacy_osha = (REPO_ROOT / "data" / "osha.sqlite").resolve()
            original_bytes = legacy_osha.read_bytes() if legacy_osha.exists() else None
            legacy_osha.parent.mkdir(parents=True, exist_ok=True)
            legacy_osha.write_text("legacy", encoding="utf-8")
            hostname = socket.gethostname().strip().lower()
            try:
                proc = self._run(
                    ["preflight", "--mode", "scheduled", "--intent", "write"],
                    {
                        "RUNTIME_ROLE": "canonical_scheduler",
                        "CANONICAL_HOSTNAME": hostname,
                        "DATA_DIR": str(data_dir),
                    },
                )
            finally:
                if original_bytes is None:
                    legacy_osha.unlink(missing_ok=True)
                else:
                    legacy_osha.write_bytes(original_bytes)
            out = (proc.stdout or "") + "\n" + (proc.stderr or "")
            self.assertNotEqual(proc.returncode, 0, msg=out)
            self.assertIn("ERR_RUNTIME_DB_OSHA_LEGACY_PRESENT", out)

    def test_legacy_repo_crm_db_errors_for_live_send(self):
        with tempfile.TemporaryDirectory() as d:
            data_dir = Path(d).resolve()
            (data_dir / "crm.sqlite").write_text("canonical", encoding="utf-8")
            legacy_crm = (REPO_ROOT / "out" / "crm.sqlite").resolve()
            original_bytes = legacy_crm.read_bytes() if legacy_crm.exists() else None
            legacy_crm.parent.mkdir(parents=True, exist_ok=True)
            legacy_crm.write_text("legacy", encoding="utf-8")
            hostname = socket.gethostname().strip().lower()
            try:
                proc = self._run(
                    ["preflight", "--mode", "manual", "--intent", "send", "--confirm-live-send"],
                    {
                        "RUNTIME_ROLE": "dev_client",
                        "CANONICAL_HOSTNAME": hostname,
                        "DATA_DIR": str(data_dir),
                        "MFO_TRUSTED_SCHEDULED": "0",
                    },
                )
            finally:
                if original_bytes is None:
                    legacy_crm.unlink(missing_ok=True)
                else:
                    legacy_crm.write_bytes(original_bytes)
        out = (proc.stdout or "") + "\n" + (proc.stderr or "")
        self.assertNotEqual(proc.returncode, 0, msg=out)
        self.assertIn("ERR_RUNTIME_DB_CRM_LEGACY_PRESENT", out)
        self.assertIn("ERR_RUNTIME_DB_CRM_SPLIT", out)

    def test_legacy_repo_crm_light_db_errors_for_live_send(self):
        with tempfile.TemporaryDirectory() as d:
            data_dir = Path(d).resolve()
            (data_dir / "crm_light.sqlite").write_text("canonical", encoding="utf-8")
            legacy_db = (REPO_ROOT / "out" / "crm_light.sqlite").resolve()
            original_bytes = legacy_db.read_bytes() if legacy_db.exists() else None
            legacy_db.parent.mkdir(parents=True, exist_ok=True)
            legacy_db.write_text("legacy", encoding="utf-8")
            hostname = socket.gethostname().strip().lower()
            try:
                proc = self._run(
                    ["preflight", "--mode", "manual", "--intent", "send", "--confirm-live-send"],
                    {
                        "RUNTIME_ROLE": "dev_client",
                        "CANONICAL_HOSTNAME": hostname,
                        "DATA_DIR": str(data_dir),
                        "MFO_TRUSTED_SCHEDULED": "0",
                    },
                )
            finally:
                if original_bytes is None:
                    legacy_db.unlink(missing_ok=True)
                else:
                    legacy_db.write_bytes(original_bytes)
        out = (proc.stdout or "") + "\n" + (proc.stderr or "")
        self.assertNotEqual(proc.returncode, 0, msg=out)
        self.assertIn("ERR_RUNTIME_DB_CRM_LIGHT_LEGACY_PRESENT", out)
        self.assertIn("ERR_RUNTIME_DB_CRM_LIGHT_SPLIT", out)


if __name__ == "__main__":
    unittest.main()
