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
                self.assertIn("TRIAL_WEEKDAYS_ONLY=1", out)
                self.assertIn("TRIAL_SCHEDULE_WEEKDAYS=MON,TUE,WED,THU,FRI", out)
                self.assertIn("trial_effective_timezone=", out)
                self.assertIn("trial_effective_local_date=", out)
                self.assertIn("trial_effective_weekday=", out)
                self.assertIn("trial_allow_weekend_send=NO", out)
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
                self.assertIn("TRIAL_WEEKDAYS_ONLY=1", out)
                self.assertIn("TRIAL_SCHEDULE_WEEKDAYS=MON,TUE,WED,THU,FRI", out)
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
            orig_query_logon = run_wally_trial.query_task_logon_mode

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
            run_wally_trial.query_task_logon_mode = lambda _task_name: "Interactive/Background"  # type: ignore[assignment]

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
                run_wally_trial.query_task_logon_mode = orig_query_logon  # type: ignore[assignment]
                sys.argv = argv0

    def test_doctor_invokes_project_context_soft_check(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "customer.json"
            cfg_path.write_text(
                json.dumps(
                    {
                        "brand_name": "Test Brand",
                        "mailing_address": "123 Test St, Test City, TS",
                        "recipients": ["test@example.com"],
                    }
                ),
                encoding="utf-8",
            )

            keys = ["SMTP_HOST", "SMTP_PORT", "SMTP_USER", "SMTP_PASS"]
            old_vals = {k: os.environ.get(k) for k in keys}
            os.environ["SMTP_HOST"] = "smtp.example.com"
            os.environ["SMTP_PORT"] = "587"
            os.environ["SMTP_USER"] = "user"
            os.environ["SMTP_PASS"] = "pass"

            called = {"n": 0}
            orig_soft = run_wally_trial.run_project_context_soft_check

            def _fake_soft(_repo_root: Path) -> None:
                called["n"] += 1
                print("WARN_CONTEXT_PACK_MISSING missing file PROJECT_CONTEXT_PACK.md")

            run_wally_trial.run_project_context_soft_check = _fake_soft  # type: ignore[assignment]
            try:
                out = io.StringIO()
                with redirect_stdout(out):
                    code = run_wally_trial.run_doctor(
                        customer_path=cfg_path,
                        repo_root=Path(run_wally_trial.__file__).resolve().parent,
                        task_name="OSHA Wally Trial Daily",
                        check_scheduler=False,
                    )
                self.assertEqual(code, 0)
                self.assertEqual(called["n"], 1)
                text = out.getvalue()
                self.assertIn("WARN_CONTEXT_PACK_MISSING", text)
                self.assertIn("DOCTOR_OK", text)
            finally:
                run_wally_trial.run_project_context_soft_check = orig_soft  # type: ignore[assignment]
                for k, v in old_vals.items():
                    if v is None:
                        os.environ.pop(k, None)
                    else:
                        os.environ[k] = v

    def test_project_context_soft_check_skips_when_wrapper_already_checked(self) -> None:
        old_val = os.environ.get("MFO_CONTEXT_PACK_SOFT_CHECK_DONE")
        os.environ["MFO_CONTEXT_PACK_SOFT_CHECK_DONE"] = "1"
        orig_run = run_wally_trial.subprocess.run

        def _fail_run(*_args, **_kwargs):  # type: ignore[no-untyped-def]
            raise AssertionError("subprocess.run should not be called when soft check sentinel is set")

        run_wally_trial.subprocess.run = _fail_run  # type: ignore[assignment]
        try:
            run_wally_trial.run_project_context_soft_check(Path(run_wally_trial.__file__).resolve().parent)
        finally:
            run_wally_trial.subprocess.run = orig_run  # type: ignore[assignment]
            if old_val is None:
                os.environ.pop("MFO_CONTEXT_PACK_SOFT_CHECK_DONE", None)
            else:
                os.environ["MFO_CONTEXT_PACK_SOFT_CHECK_DONE"] = old_val

    def test_write_batch_runner_contains_trial_daily_tokens_and_no_manual_append(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            batch_path = root / "run_wally_trial_daily.bat"
            run_wally_trial.write_batch_runner(
                batch_path=batch_path,
                project_root=root,
                customer_config=str(root / "customers" / "wally_trial_tx_triangle_v1.json"),
                db_path=r"C:\osha_data\osha.sqlite",
                admin_email="support@microflowops.com",
            )
            text = batch_path.read_text(encoding="utf-8")

            self.assertIn(
                "python run_trial_daily.py --subscriber-key wally_trial --customer \"%~dp0customers\\wally_trial_tx_triangle_v1.json\" --send-live",
                text,
            )
            self.assertNotIn("--db", text)
            self.assertNotIn("run_trial_admin.py append-event --subscriber-key wally_trial", text)
            self.assertNotIn("WARN_TRIAL_LEDGER_APPEND_FAILED subscriber_key=wally_trial", text)

    def test_run_live_send_invokes_trial_daily_runner(self) -> None:
        captured: dict[str, object] = {}
        orig_run = run_wally_trial.subprocess.run

        def _fake_run(cmd, check=True):  # type: ignore[no-untyped-def]
            captured["cmd"] = list(cmd)
            class _Done:
                returncode = 0
            return _Done()

        run_wally_trial.subprocess.run = _fake_run  # type: ignore[assignment]
        try:
            run_wally_trial.run_live_send(
                db_path=r"C:\osha_data\osha.sqlite",
                customer_config="customers/wally_trial_tx_triangle_v1.json",
                admin_email="support@microflowops.com",
                send_live=True,
                allow_weekend_send=True,
            )
        finally:
            run_wally_trial.subprocess.run = orig_run  # type: ignore[assignment]

        cmd = captured.get("cmd", [])
        self.assertIn("run_trial_daily.py", cmd)
        self.assertIn("--subscriber-key", cmd)
        self.assertIn("wally_trial", cmd)
        self.assertIn("--send-live", cmd)
        self.assertNotIn("--db", cmd)

    def test_run_live_send_skips_on_weekend_without_override(self) -> None:
        calls = {"run": 0}
        orig_run = run_wally_trial.subprocess.run
        orig_day = run_wally_trial._wally_local_day_context

        def _fake_run(*_args, **_kwargs):  # type: ignore[no-untyped-def]
            calls["run"] += 1
            class _Done:
                returncode = 0
            return _Done()

        run_wally_trial.subprocess.run = _fake_run  # type: ignore[assignment]
        run_wally_trial._wally_local_day_context = lambda _cfg: {  # type: ignore[assignment]
            "subscriber_key": "wally_trial",
            "timezone": "America/Chicago",
            "local_date": "2026-02-22",
            "weekday_name": "sun",
            "is_weekend": True,
        }
        try:
            buf = io.StringIO()
            with redirect_stdout(buf):
                run_wally_trial.run_live_send(
                    db_path=r"C:\osha_data\osha.sqlite",
                    customer_config="customers/wally_trial_tx_triangle_v1.json",
                    admin_email="support@microflowops.com",
                    send_live=True,
                )
        finally:
            run_wally_trial.subprocess.run = orig_run  # type: ignore[assignment]
            run_wally_trial._wally_local_day_context = orig_day  # type: ignore[assignment]

        self.assertIn(
            "SKIP_NON_WEEKDAY subscriber_key=wally_trial local_date=2026-02-22 weekday=sun gate=trial_weekdays_only",
            buf.getvalue(),
        )
        self.assertEqual(calls["run"], 0)

    def test_enable_schedule_uses_weekly_weekday_trigger(self) -> None:
        captured: dict[str, object] = {}
        orig_run = run_wally_trial.subprocess.run
        old_user = os.environ.get("TASK_SCHED_USER")
        old_password = os.environ.get("TASK_SCHED_PASSWORD")
        os.environ["TASK_SCHED_USER"] = r"DESKTOP-Q8QM4N9\lever"
        os.environ["TASK_SCHED_PASSWORD"] = "test-password"

        def _fake_run(cmd, check=True):  # type: ignore[no-untyped-def]
            captured["cmd"] = list(cmd)
            captured["check"] = check
            class _Done:
                returncode = 0
            return _Done()

        run_wally_trial.subprocess.run = _fake_run  # type: ignore[assignment]
        try:
            run_wally_trial.enable_schedule("OSHA Wally Trial Daily", Path(r"C:\dev\OSHA_Leads\run_wally_trial_daily.bat"))
        finally:
            run_wally_trial.subprocess.run = orig_run  # type: ignore[assignment]
            if old_user is None:
                os.environ.pop("TASK_SCHED_USER", None)
            else:
                os.environ["TASK_SCHED_USER"] = old_user
            if old_password is None:
                os.environ.pop("TASK_SCHED_PASSWORD", None)
            else:
                os.environ["TASK_SCHED_PASSWORD"] = old_password

        cmd = [str(x) for x in (captured.get("cmd") or [])]
        self.assertIn("/SC", cmd)
        self.assertIn("WEEKLY", cmd)
        self.assertIn("/D", cmd)
        self.assertIn("MON,TUE,WED,THU,FRI", cmd)
        self.assertIn("/RU", cmd)
        self.assertIn(r"DESKTOP-Q8QM4N9\lever", cmd)
        self.assertIn("/RP", cmd)
        self.assertIn("test-password", cmd)
        self.assertTrue(bool(captured.get("check")))

    def test_enable_schedule_requires_task_sched_password(self) -> None:
        old_user = os.environ.get("TASK_SCHED_USER")
        old_password = os.environ.get("TASK_SCHED_PASSWORD")
        os.environ["TASK_SCHED_USER"] = r"DESKTOP-Q8QM4N9\lever"
        os.environ.pop("TASK_SCHED_PASSWORD", None)
        try:
            with self.assertRaises(SystemExit) as ctx:
                run_wally_trial.enable_schedule("OSHA Wally Trial Daily", Path(r"C:\dev\OSHA_Leads\run_wally_trial_daily.bat"))
            self.assertEqual(ctx.exception.code, 1)
        finally:
            if old_user is None:
                os.environ.pop("TASK_SCHED_USER", None)
            else:
                os.environ["TASK_SCHED_USER"] = old_user
            if old_password is None:
                os.environ.pop("TASK_SCHED_PASSWORD", None)
            else:
                os.environ["TASK_SCHED_PASSWORD"] = old_password

    def test_run_live_send_does_not_append_event_on_failure(self) -> None:
        orig_run = run_wally_trial.subprocess.run

        def _fake_run(_cmd, check=True):  # type: ignore[no-untyped-def]
            raise run_wally_trial.subprocess.CalledProcessError(returncode=1, cmd=["run_trial_daily.py"])

        run_wally_trial.subprocess.run = _fake_run  # type: ignore[assignment]
        try:
            with self.assertRaises(run_wally_trial.subprocess.CalledProcessError):
                run_wally_trial.run_live_send(
                    db_path=r"C:\osha_data\osha.sqlite",
                    customer_config="customers/wally_trial_tx_triangle_v1.json",
                    admin_email="support@microflowops.com",
                    send_live=True,
                    allow_weekend_send=True,
                )
        finally:
            run_wally_trial.subprocess.run = orig_run  # type: ignore[assignment]
