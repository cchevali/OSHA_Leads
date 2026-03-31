import io
import sqlite3
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

    def test_split_ledger_blocks_live_mode(self):
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
        self.assertEqual(rc, 2, msg=out)
        self.assertIn("WARN_TRIAL_LEDGER_SPLIT", out)
        self.assertIn("run_runtime_state_migrate.py --apply", out)

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

    def test_run_deliver_daily_passes_confirm_live_send_flag(self):
        seen_cmds: list[list[str]] = []

        def _run(cmd, **_kwargs):  # type: ignore[no-untyped-def]
            seen_cmds.append([str(part) for part in cmd])
            return mock.Mock(returncode=0, stdout="", stderr="")

        with mock.patch.object(trial_daily.subprocess, "run", side_effect=_run):
            code, out = trial_daily._run_deliver_daily(
                r"C:\osha_data\osha.sqlite",
                Path(r"C:\osha_data\trials\jl_safety_trial\customer.runtime.json"),
                send_live=True,
                dry_run=False,
                confirm_live_send=True,
            )

        self.assertEqual(code, 0, msg=out)
        self.assertTrue(seen_cmds)
        joined = " ".join(seen_cmds[0])
        self.assertIn("deliver_daily.py", joined)
        self.assertIn("--send-live", joined)
        self.assertIn("--confirm-live-send", joined)

    def test_run_deliver_daily_passes_same_day_live_override_flag(self):
        seen_cmds: list[list[str]] = []

        def _run(cmd, **_kwargs):  # type: ignore[no-untyped-def]
            seen_cmds.append([str(part) for part in cmd])
            return mock.Mock(returncode=0, stdout="", stderr="")

        with mock.patch.object(trial_daily.subprocess, "run", side_effect=_run):
            code, out = trial_daily._run_deliver_daily(
                r"C:\osha_data\osha.sqlite",
                Path(r"C:\osha_data\trials\facs_trial\customer.runtime.json"),
                send_live=True,
                dry_run=False,
                confirm_live_send=True,
                allow_second_live_send_same_day=True,
            )

        self.assertEqual(code, 0, msg=out)
        self.assertTrue(seen_cmds)
        joined = " ".join(seen_cmds[0])
        self.assertIn("--confirm-live-send", joined)
        self.assertIn("--allow-second-live-send-same-day", joined)

    def test_generate_minimal_customer_config_sets_50_signal_caps(self):
        cfg = trial_daily._generate_minimal_customer_config(self._policy())
        self.assertEqual(cfg["top_k_overall"], 50)
        self.assertEqual(cfg["top_k_per_state"], 50)

    def test_send_live_success_records_sent_event_with_live_mode(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            crm_db = tmp / "crm_light.sqlite"
            leads_db = tmp / "osha.sqlite"
            data_dir = tmp / "data"
            data_dir.mkdir(parents=True, exist_ok=True)
            sqlite3.connect(str(leads_db)).close()
            crm_light_db = trial_daily.crm_light.ensure_database(crm_db)
            with trial_daily.crm_light.open_conn(crm_light_db) as conn:
                trial_daily.crm_light.init_schema(conn)
                trial_daily.crm_light.upsert_subscriber(
                    conn,
                    subscriber_key="facs_trial",
                    email="owner@example.com",
                    territory_code="TX_TRI",
                    tz="America/Chicago",
                    status="trial",
                )
                trial_daily.crm_light.upsert_trial_state(
                    conn,
                    subscriber_key="facs_trial",
                    start_date="2026-03-01",
                    sends_limit=14,
                )

            customer_runtime = data_dir / "trials" / "facs_trial" / "customer.runtime.json"
            customer_runtime.parent.mkdir(parents=True, exist_ok=True)
            customer_runtime.write_text('{"customer_id":"facs_trial"}\n', encoding="utf-8")

            with (
                mock.patch.dict(trial_daily.os.environ, {"DATA_DIR": str(data_dir)}, clear=False),
                mock.patch.object(trial_daily, "run_runtime_preflight", return_value=_Preflight(True)),
                mock.patch.object(trial_daily, "render_runtime_lines", return_value=["PASS_RUNTIME_PREFLIGHT"]),
                mock.patch.object(trial_daily, "validate_live_osha_db_path", return_value=""),
                mock.patch.object(trial_daily, "_detect_split_ledger_conflict", return_value={"conflict": False}),
                mock.patch.object(trial_daily, "_resolve_customer_config_path", return_value=customer_runtime),
                mock.patch.object(trial_daily, "_load_or_build_customer_config", return_value=customer_runtime),
                mock.patch.object(
                    trial_daily,
                    "_trial_local_day_context",
                    return_value={
                        "timezone": "America/Chicago",
                        "local_date": "2026-03-23",
                        "weekday_idx": 0,
                        "weekday_name": "mon",
                        "is_weekend": False,
                    },
                ),
                mock.patch.object(trial_daily, "_run_deliver_daily", return_value=(0, "SEND_START mode=SAFE\n")),
            ):
                buf = io.StringIO()
                with redirect_stdout(buf):
                    rc = trial_daily.run_trial_daily(
                        subscriber_key="facs_trial",
                        leads_db=str(leads_db),
                        crm_db=str(crm_db),
                        customer_arg="",
                        send_live=True,
                        dry_run=False,
                        test_send_daily=False,
                        print_config=False,
                        doctor=False,
                        confirm_live_send=False,
                    )
            out = buf.getvalue()
            self.assertEqual(rc, 0, msg=out)
            self.assertIn("TRIAL_EVENT status=SENT", out)

            with trial_daily.crm_light.open_conn(crm_db) as conn:
                event = conn.execute(
                    """
                    SELECT status, variant, meta_json
                    FROM send_events
                    WHERE subscriber_key = ?
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    ("facs_trial",),
                ).fetchone()
            self.assertIsNotNone(event)
            assert event is not None
            self.assertEqual(str(event["status"]), "SENT")
            self.assertEqual(str(event["variant"]), "daily")
            self.assertIn('"send_mode": "LIVE"', str(event["meta_json"]))


if __name__ == "__main__":
    unittest.main()
