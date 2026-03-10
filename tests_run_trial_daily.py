import io
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest import mock

import run_trial_daily as trial_daily


class _Preflight:
    def __init__(self, ok: bool = True):
        self.ok = ok


class TestRunTrialDaily(unittest.TestCase):
    def _policy(self) -> trial_daily.TrialPolicy:
        return trial_daily.TrialPolicy(
            subscriber_key="facs_trial",
            email="owner@example.com",
            territory_code="TX_TRI",
            tz="America/Chicago",
            start_date="2026-03-01",
            sends_limit=14,
            expired_behavior="notify_once",
            successful_sends=1,
            expired=False,
        )

    def test_main_rejects_invalid_doctor_combinations(self):
        rc = trial_daily.main(["--subscriber-key", "facs_trial", "--doctor", "--send-live"])
        self.assertEqual(rc, 1)

    def test_doctor_mode_emits_pass_token(self):
        with (
            mock.patch.object(trial_daily, "_resolve_policy", return_value=self._policy()),
            mock.patch.object(trial_daily, "run_runtime_preflight", return_value=_Preflight(True)),
            mock.patch.object(trial_daily, "render_runtime_lines", return_value=["PASS_RUNTIME_PREFLIGHT"]),
        ):
            buf = io.StringIO()
            with redirect_stdout(buf):
                rc = trial_daily.run_trial_daily(
                    subscriber_key="facs_trial",
                    leads_db=r"C:\osha_data\osha.sqlite",
                    crm_db=r"C:\osha_data\crm_light.sqlite",
                    customer_arg="",
                    send_live=False,
                    dry_run=False,
                    test_send_daily=False,
                    print_config=False,
                    doctor=True,
                )
        out = buf.getvalue()
        self.assertEqual(rc, 0, msg=out)
        self.assertIn("PASS_RUNTIME_PREFLIGHT", out)
        self.assertIn("PASS_TRIAL_DAILY_DOCTOR status=OK", out)

    def test_split_ledger_is_warning_only_in_live_mode(self):
        with (
            mock.patch.object(trial_daily, "_resolve_policy", return_value=self._policy()),
            mock.patch.object(trial_daily, "run_runtime_preflight", return_value=_Preflight(True)),
            mock.patch.object(trial_daily, "render_runtime_lines", return_value=["PASS_RUNTIME_PREFLIGHT"]),
            mock.patch.object(trial_daily, "validate_live_osha_db_path", return_value=""),
            mock.patch.object(
                trial_daily,
                "_detect_split_ledger_conflict",
                return_value={
                    "conflict": True,
                    "primary_db": "C:\\osha_data\\crm_light.sqlite",
                    "secondary_db": "C:\\dev\\OSHA_Leads\\out\\crm_light.sqlite",
                    "reason": "fingerprint_mismatch",
                },
            ),
            mock.patch.object(
                trial_daily,
                "_trial_local_day_context",
                return_value={
                    "timezone": "America/Chicago",
                    "local_date": "2026-03-07",
                    "weekday_idx": 5,
                    "weekday_name": "sat",
                    "is_weekend": True,
                },
            ),
        ):
            buf = io.StringIO()
            with redirect_stdout(buf):
                rc = trial_daily.run_trial_daily(
                    subscriber_key="facs_trial",
                    leads_db=r"C:\osha_data\osha.sqlite",
                    crm_db=r"C:\osha_data\crm_light.sqlite",
                    customer_arg="",
                    send_live=True,
                    dry_run=False,
                    test_send_daily=False,
                    print_config=False,
                    doctor=False,
                    allow_weekend_send=False,
                )
        out = buf.getvalue()
        self.assertEqual(rc, 0, msg=out)
        self.assertIn("WARN_TRIAL_LEDGER_SPLIT", out)
        self.assertIn("SKIP_NON_WEEKDAY", out)

    def test_main_defaults_leads_db_to_data_dir_osha_sqlite(self):
        with tempfile.TemporaryDirectory() as d:
            data_dir = Path(d).resolve()
            with (
                mock.patch.dict(trial_daily.os.environ, {"DATA_DIR": str(data_dir)}, clear=True),
                mock.patch.object(trial_daily.crm_light, "resolve_crm_db_path", return_value="C:\\osha_data\\crm_light.sqlite"),
                mock.patch.object(trial_daily, "run_trial_daily", return_value=0) as run_mock,
            ):
                rc = trial_daily.main(["--subscriber-key", "facs_trial", "--print-config"])
        self.assertEqual(rc, 0)
        self.assertEqual(run_mock.call_args.kwargs["leads_db"], str((data_dir / "osha.sqlite").resolve(strict=False)))


if __name__ == "__main__":
    unittest.main()
