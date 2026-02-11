import io
import json
import os
import tempfile
import unittest
import sys
from contextlib import redirect_stdout
from pathlib import Path

import run_wally_trial


class TestWallyTrialDoctor(unittest.TestCase):
    def test_run_test_send_daily_passes_dry_run_flag(self) -> None:
        cfg = {
            "customer_id": "fanout_test",
            "subscriber_key": "fanout_sub",
            "recipients": ["test@example.com"],
            "brand_name": "Test Brand",
            "mailing_address": "123 Test St, Test City, TS",
        }
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "customer.json"
            cfg_path.write_text(json.dumps(cfg), encoding="utf-8")

            captured: dict[str, list[str]] = {}
            orig_run = run_wally_trial.subprocess.run
            orig_last_sent = run_wally_trial._load_subscriber_last_sent_at
            run_wally_trial._load_subscriber_last_sent_at = lambda *_a, **_k: None  # type: ignore[assignment]

            def _fake_run(cmd, check=True):  # type: ignore[no-untyped-def]
                captured["cmd"] = list(cmd)
                class _Done:
                    returncode = 0
                return _Done()

            run_wally_trial.subprocess.run = _fake_run  # type: ignore[assignment]
            try:
                run_wally_trial.run_test_send_daily(db_path="unused.sqlite", customer_config=str(cfg_path), dry_run=True)
            finally:
                run_wally_trial.subprocess.run = orig_run  # type: ignore[assignment]
                run_wally_trial._load_subscriber_last_sent_at = orig_last_sent  # type: ignore[assignment]

            self.assertIn("--dry-run", captured.get("cmd", []))

    def test_doctor_succeeds_when_preflight_succeeds_and_does_not_send(self) -> None:
        # Minimal config that satisfies run_wally_trial.preflight().
        cfg = {
            "brand_name": "Test Brand",
            "mailing_address": "123 Test St, Test City, TS",
            "recipients": ["test@example.com"],
        }

        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "customer.json"
            cfg_path.write_text(json.dumps(cfg), encoding="utf-8")

            # Provide required SMTP env vars (preflight validates presence only; doctor must not connect/send).
            keys = ["SMTP_HOST", "SMTP_PORT", "SMTP_USER", "SMTP_PASS"]
            old_vals = {k: os.environ.get(k) for k in keys}
            os.environ["SMTP_HOST"] = "smtp.example.com"
            os.environ["SMTP_PORT"] = "587"
            os.environ["SMTP_USER"] = "user"
            os.environ["SMTP_PASS"] = "pass"

            # If these are invoked in doctor mode, that would imply sending behavior.
            orig_preview = run_wally_trial.run_preview_send
            orig_live = run_wally_trial.run_live_send
            orig_query = run_wally_trial.query_task_to_run

            run_wally_trial.run_preview_send = lambda *a, **k: (_ for _ in ()).throw(  # type: ignore[assignment]
                AssertionError("run_preview_send should not be called in --doctor mode")
            )
            run_wally_trial.run_live_send = lambda *a, **k: (_ for _ in ()).throw(  # type: ignore[assignment]
                AssertionError("run_live_send should not be called in --doctor mode")
            )

            # Plain --doctor must never call schtasks.
            run_wally_trial.query_task_to_run = lambda _task_name: (_ for _ in ()).throw(  # type: ignore[assignment]
                AssertionError("query_task_to_run should not be called without --doctor-check-scheduler")
            )

            try:
                buf = io.StringIO()
                argv0 = sys.argv[:]
                sys.argv = ["run_wally_trial.py", str(cfg_path), "--doctor"]
                with redirect_stdout(buf):
                    with self.assertRaises(SystemExit) as cm:
                        run_wally_trial.main()
                self.assertEqual(cm.exception.code, 0)
                out = buf.getvalue()
                self.assertIn("DOCTOR_OK", out)
                self.assertNotIn("DOCTOR_FAIL", out)
                self.assertIn("DOCTOR_NOTE scheduler_check=SKIPPED (opt-in)", out)
            finally:
                for k, v in old_vals.items():
                    if v is None:
                        os.environ.pop(k, None)
                    else:
                        os.environ[k] = v
                run_wally_trial.run_preview_send = orig_preview  # type: ignore[assignment]
                run_wally_trial.run_live_send = orig_live  # type: ignore[assignment]
                run_wally_trial.query_task_to_run = orig_query  # type: ignore[assignment]
                sys.argv = argv0

    def test_print_config_uses_defaults_when_keys_missing(self) -> None:
        cfg = {
            "customer_id": "wally_trial_tx_triangle_v1",
            "subscriber_key": "wally_trial",
            "recipients": ["test@example.com"],
            "brand_name": "Test Brand",
            "mailing_address": "123 Test St, Test City, TS",
        }

        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "customer.json"
            cfg_path.write_text(json.dumps(cfg), encoding="utf-8")
            buf = io.StringIO()
            argv0 = sys.argv[:]
            try:
                sys.argv = ["run_wally_trial.py", str(cfg_path), "--print-config"]
                with redirect_stdout(buf):
                    with self.assertRaises(SystemExit) as cm:
                        run_wally_trial.main()
                self.assertEqual(cm.exception.code, 0)
                out = buf.getvalue()
                self.assertIn("trial_target_local_hhmm=09:00", out)
                self.assertIn("trial_catchup_max_minutes=180", out)
            finally:
                sys.argv = argv0

    def test_print_config_uses_explicit_values(self) -> None:
        cfg = {
            "customer_id": "wally_trial_tx_triangle_v1",
            "subscriber_key": "wally_trial",
            "trial_target_local_hhmm": "10:15",
            "trial_catchup_max_minutes": 75,
            "recipients": ["test@example.com"],
            "brand_name": "Test Brand",
            "mailing_address": "123 Test St, Test City, TS",
        }

        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "customer.json"
            cfg_path.write_text(json.dumps(cfg), encoding="utf-8")
            buf = io.StringIO()
            argv0 = sys.argv[:]
            try:
                sys.argv = ["run_wally_trial.py", str(cfg_path), "--print-config"]
                with redirect_stdout(buf):
                    with self.assertRaises(SystemExit) as cm:
                        run_wally_trial.main()
                self.assertEqual(cm.exception.code, 0)
                out = buf.getvalue()
                self.assertIn("trial_target_local_hhmm=10:15", out)
                self.assertIn("trial_catchup_max_minutes=75", out)
            finally:
                sys.argv = argv0

    def test_doctor_calls_schtasks_only_with_opt_in_flag(self) -> None:
        cfg = {
            "brand_name": "Test Brand",
            "mailing_address": "123 Test St, Test City, TS",
            "recipients": ["test@example.com"],
        }

        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "customer.json"
            cfg_path.write_text(json.dumps(cfg), encoding="utf-8")

            keys = ["SMTP_HOST", "SMTP_PORT", "SMTP_USER", "SMTP_PASS"]
            old_vals = {k: os.environ.get(k) for k in keys}
            os.environ["SMTP_HOST"] = "smtp.example.com"
            os.environ["SMTP_PORT"] = "587"
            os.environ["SMTP_USER"] = "user"
            os.environ["SMTP_PASS"] = "pass"

            orig_preview = run_wally_trial.run_preview_send
            orig_live = run_wally_trial.run_live_send
            orig_query = run_wally_trial.query_task_to_run

            run_wally_trial.run_preview_send = lambda *a, **k: (_ for _ in ()).throw(  # type: ignore[assignment]
                AssertionError("run_preview_send should not be called in --doctor mode")
            )
            run_wally_trial.run_live_send = lambda *a, **k: (_ for _ in ()).throw(  # type: ignore[assignment]
                AssertionError("run_live_send should not be called in --doctor mode")
            )

            called = {"n": 0}
            batch_path = (Path(run_wally_trial.__file__).resolve().parent / "run_wally_trial_daily.bat").resolve()
            expected = run_wally_trial.build_task_action(run_wally_trial._sanitize_task_path(batch_path))

            def _fake_query(task_name: str) -> str | None:
                called["n"] += 1
                return expected

            run_wally_trial.query_task_to_run = _fake_query  # type: ignore[assignment]

            try:
                buf = io.StringIO()
                argv0 = sys.argv[:]
                sys.argv = ["run_wally_trial.py", str(cfg_path), "--doctor", "--doctor-check-scheduler"]
                with redirect_stdout(buf):
                    with self.assertRaises(SystemExit) as cm:
                        run_wally_trial.main()
                self.assertEqual(cm.exception.code, 0)
                self.assertEqual(called["n"], 1)
                out = buf.getvalue()
                self.assertIn("DOCTOR_OK", out)
                self.assertIn("DOCTOR_NOTE scheduler_check=OK", out)
            finally:
                for k, v in old_vals.items():
                    if v is None:
                        os.environ.pop(k, None)
                    else:
                        os.environ[k] = v
                run_wally_trial.run_preview_send = orig_preview  # type: ignore[assignment]
                run_wally_trial.run_live_send = orig_live  # type: ignore[assignment]
                run_wally_trial.query_task_to_run = orig_query  # type: ignore[assignment]
                sys.argv = argv0
