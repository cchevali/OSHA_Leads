import io
import json
import os
import socket
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

import send_digest_email as sde


class TestDigestSameDayGuard(unittest.TestCase):
    def _base_config(self) -> dict:
        return {
            "customer_id": "wally_trial_tx_triangle_v1",
            "subscriber_key": "wally_trial",
            "states": ["TX"],
            "opened_window_days": 14,
            "new_only_days": 1,
            "territory_code": "TX_TRI",
            "allow_live_send": True,
            "send_time_local": "08:00",
            "timezone": "America/Chicago",
            "email_recipients": [
                "wgs@indigocompliance.com",
                "brandon@indigoenergyservices.com",
            ],
            "brand_name": "MicroFlowOps",
            "mailing_address": "11539 Links Dr, Reston, VA 20190",
        }

    def _base_profile(self) -> dict:
        return {
            "active": 1,
            "send_enabled": 1,
            "territory_code": "TX_TRI",
            "content_filter": "high_medium",
            "include_low_fallback": 1,
            "email": "wgs@indigocompliance.com",
            "recipients": [
                "wgs@indigocompliance.com",
                "brandon@indigoenergyservices.com",
            ],
            "last_sent_at": "2026-03-02T13:14:07+00:00",
        }

    def test_skip_same_day_live_send_emits_token_and_logs_rows(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            out_dir = Path(td) / "out"
            out_dir.mkdir(parents=True, exist_ok=True)
            argv = [
                "send_digest_email.py",
                "--db",
                str(Path(td) / "osha.sqlite"),
                "--customer",
                str(Path(td) / "customer.json"),
                "--mode",
                "daily",
                "--send-live",
                "--confirm-live-send",
                "--output-dir",
                str(out_dir),
            ]
            env_keys = ["SMTP_HOST", "SMTP_PORT", "SMTP_USER", "SMTP_PASS", "DATA_DIR", "CANONICAL_HOSTNAME"]
            old_env = {k: os.environ.get(k) for k in env_keys}
            os.environ["SMTP_HOST"] = "smtp.example.com"
            os.environ["SMTP_PORT"] = "587"
            os.environ["SMTP_USER"] = "user"
            os.environ["SMTP_PASS"] = "pass"
            os.environ["DATA_DIR"] = str(Path(td) / "runtime_data")
            os.environ["CANONICAL_HOSTNAME"] = socket.gethostname().strip().lower()
            try:
                with mock.patch.object(sde, "load_environment", return_value=None), mock.patch.object(
                    sde, "load_customer_config", return_value=self._base_config()
                ), mock.patch.object(
                    sde, "_load_subscriber_profile", return_value=self._base_profile()
                ), mock.patch.object(
                    sde, "resolve_timezone", return_value=timezone.utc
                ), mock.patch.object(
                    sde, "_load_subscriber_entitlement_and_allowlist", return_value=({}, [])
                ), mock.patch.object(
                    sde, "_enforce_zip_cbsa_dataset_gate", return_value=(True, "")
                ), mock.patch.object(
                    sde, "preflight_missing_vars", return_value=[]
                ), mock.patch.object(
                    sde, "collect_recipients",
                    return_value=["wgs@indigocompliance.com", "brandon@indigoenergyservices.com"],
                ), mock.patch.object(
                    sde,
                    "_within_send_window",
                    return_value=(True, "", datetime.now(timezone.utc), datetime.now(timezone.utc)),
                ), mock.patch.object(
                    sde, "_already_sent_today_local", return_value=True
                ), mock.patch.object(
                    sde, "log_email_attempt"
                ) as log_attempt:
                    out = io.StringIO()
                    err = io.StringIO()
                    with mock.patch("sys.argv", argv), redirect_stdout(out), redirect_stderr(err):
                        with self.assertRaises(SystemExit) as cm:
                            sde.main()
                    self.assertEqual(cm.exception.code, 0)
                    text = out.getvalue()
                    self.assertIn("TRIAL_SKIP_ALREADY_SENT_TODAY=1 subscriber_key=wally_trial", text)
                    self.assertIn("guard=ON", text)
                    self.assertEqual(log_attempt.call_count, 2)
            finally:
                for key, value in old_env.items():
                    if value is None:
                        os.environ.pop(key, None)
                    else:
                        os.environ[key] = value

    def test_override_flag_bypasses_same_day_skip_guard(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            argv = [
                "send_digest_email.py",
                "--db",
                str(Path(td) / "osha.sqlite"),
                "--customer",
                str(Path(td) / "customer.json"),
                "--mode",
                "daily",
                "--send-live",
                "--confirm-live-send",
                "--allow-second-live-send-same-day",
            ]
            env_keys = ["SMTP_HOST", "SMTP_PORT", "SMTP_USER", "SMTP_PASS", "DATA_DIR", "CANONICAL_HOSTNAME"]
            old_env = {k: os.environ.get(k) for k in env_keys}
            os.environ["SMTP_HOST"] = "smtp.example.com"
            os.environ["SMTP_PORT"] = "587"
            os.environ["SMTP_USER"] = "user"
            os.environ["SMTP_PASS"] = "pass"
            os.environ["DATA_DIR"] = str(Path(td) / "runtime_data")
            os.environ["CANONICAL_HOSTNAME"] = socket.gethostname().strip().lower()
            try:
                with mock.patch.object(sde, "load_environment", return_value=None), mock.patch.object(
                    sde, "load_customer_config", return_value=self._base_config()
                ), mock.patch.object(
                    sde, "_load_subscriber_profile", return_value=self._base_profile()
                ), mock.patch.object(
                    sde, "resolve_timezone", return_value=timezone.utc
                ), mock.patch.object(
                    sde, "_load_subscriber_entitlement_and_allowlist", return_value=({}, [])
                ), mock.patch.object(
                    sde, "_enforce_zip_cbsa_dataset_gate", return_value=(True, "")
                ), mock.patch.object(
                    sde, "preflight_missing_vars", return_value=[]
                ), mock.patch.object(
                    sde, "collect_recipients",
                    return_value=["wgs@indigocompliance.com", "brandon@indigoenergyservices.com"],
                ), mock.patch.object(
                    sde,
                    "_within_send_window",
                    return_value=(True, "", datetime.now(timezone.utc), datetime.now(timezone.utc)),
                ), mock.patch.object(
                    sde, "_already_sent_today_local", return_value=True
                ), mock.patch.object(
                    sde.sqlite3, "connect", side_effect=RuntimeError("after_same_day_guard")
                ):
                    with mock.patch("sys.argv", argv):
                        with self.assertRaisesRegex(RuntimeError, "after_same_day_guard"):
                            sde.main()
            finally:
                for key, value in old_env.items():
                    if value is None:
                        os.environ.pop(key, None)
                    else:
                        os.environ[key] = value


if __name__ == "__main__":
    unittest.main()
