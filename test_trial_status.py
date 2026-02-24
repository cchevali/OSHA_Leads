from __future__ import annotations

import contextlib
import io
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
import re

import crm_light
import run_trial_admin
import run_trial_daily
import run_wally_trial

REQUIRED_KEYS = [
    "TRIAL_SUBSCRIBER_KEY",
    "TRIAL_START_DATE",
    "TRIAL_FIRST_SENT_UTC",
    "TRIAL_LAST_SENT_UTC",
    "TRIAL_DAYS_SINCE_START",
    "TRIAL_SENDS_USED",
    "TRIAL_SENDS_LIMIT",
    "TRIAL_EXPIRED_BY_SENDS",
    "TRIAL_14_DAY_ELAPSED",
    "TRIAL_NEXT_ACTION_HINT",
    "TRIAL_EXPIRED",
]


def _parse_stdout_block(text: str) -> tuple[list[str], dict[str, str]]:
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    keys: list[str] = []
    values: dict[str, str] = {}
    for line in lines:
        if "=" not in line:
            continue
        k, v = line.split("=", 1)
        keys.append(k)
        values[k] = v
    return keys, values


class TestTrialStatus(unittest.TestCase):
    def _seed_trial(
        self,
        db_path: Path,
        *,
        subscriber_key: str,
        start_date: str,
        sends_limit: int,
    ) -> None:
        crm_light.ensure_database(db_path)
        with crm_light.open_conn(db_path) as conn:
            crm_light.init_schema(conn)
            crm_light.upsert_subscriber(
                conn,
                subscriber_key=subscriber_key,
                email=f"{subscriber_key}@example.com",
                territory_code="TX_TRIANGLE_V1",
                tz="America/Chicago",
                status="trial",
            )
            crm_light.upsert_trial_state(
                conn,
                subscriber_key=subscriber_key,
                start_date=start_date,
                sends_limit=sends_limit,
            )

    def test_sends_based_expiry_and_elapsed_boundary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            db = base / "crm_light.sqlite"
            self._seed_trial(
                db,
                subscriber_key="trial_boundary",
                start_date="2026-02-01",
                sends_limit=14,
            )
            with crm_light.open_conn(db) as conn:
                crm_light.init_schema(conn)
                weekday_dates = [
                    "2026-02-02",
                    "2026-02-03",
                    "2026-02-04",
                    "2026-02-05",
                    "2026-02-06",
                    "2026-02-09",
                    "2026-02-10",
                    "2026-02-11",
                    "2026-02-12",
                    "2026-02-13",
                    "2026-02-16",
                    "2026-02-17",
                    "2026-02-18",
                ]
                for i, day in enumerate(weekday_dates):
                    crm_light.append_send_event(
                        conn,
                        subscriber_key="trial_boundary",
                        variant="daily",
                        status="SENT",
                        run_id=f"sent_{i}",
                        meta={},
                        ts_utc=f"{day}T15:00:00+00:00",
                    )

            buf = io.StringIO()
            with contextlib.redirect_stdout(buf):
                code = run_trial_admin.print_trial_status(
                    subscriber_key="trial_boundary",
                    crm_db_path=db,
                    as_of="2026-02-15",
                )
            self.assertEqual(code, 0)
            keys, values = _parse_stdout_block(buf.getvalue())
            self.assertEqual(keys, REQUIRED_KEYS)
            self.assertEqual(values["TRIAL_DAYS_SINCE_START"], "14")
            self.assertEqual(values["TRIAL_SENDS_USED"], "13")
            self.assertEqual(values["TRIAL_EXPIRED_BY_SENDS"], "0")
            self.assertEqual(values["TRIAL_14_DAY_ELAPSED"], "0")
            self.assertEqual(values["TRIAL_EXPIRED"], "0")

            with crm_light.open_conn(db) as conn:
                crm_light.init_schema(conn)
                crm_light.append_send_event(
                    conn,
                    subscriber_key="trial_boundary",
                    variant="daily",
                    status="SENT",
                    run_id="sent_14",
                    meta={},
                    ts_utc="2026-02-19T15:00:00+00:00",
                )

            buf = io.StringIO()
            with contextlib.redirect_stdout(buf):
                code = run_trial_admin.print_trial_status(
                    subscriber_key="trial_boundary",
                    crm_db_path=db,
                    as_of="2026-02-15",
                )
            self.assertEqual(code, 0)
            keys, values = _parse_stdout_block(buf.getvalue())
            self.assertEqual(keys, REQUIRED_KEYS)
            self.assertEqual(values["TRIAL_SENDS_USED"], "14")
            self.assertEqual(values["TRIAL_EXPIRED_BY_SENDS"], "1")
            self.assertEqual(values["TRIAL_14_DAY_ELAPSED"], "1")
            self.assertEqual(values["TRIAL_EXPIRED"], "1")

    def test_first_last_sent_selection_and_send_expiry(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            db = base / "crm_light.sqlite"
            self._seed_trial(
                db,
                subscriber_key="trial_events",
                start_date="2026-02-04",
                sends_limit=1,
            )
            with crm_light.open_conn(db) as conn:
                crm_light.init_schema(conn)
                crm_light.append_send_event(
                    conn,
                    subscriber_key="trial_events",
                    variant="daily",
                    status="SENT",
                    run_id="before_start",
                    meta={},
                    ts_utc="2026-02-03T15:00:00+00:00",
                )
                crm_light.append_send_event(
                    conn,
                    subscriber_key="trial_events",
                    variant="daily",
                    status="DRY_RUN",
                    run_id="dry",
                    meta={},
                    ts_utc="2026-02-04T12:00:00+00:00",
                )
                crm_light.append_send_event(
                    conn,
                    subscriber_key="trial_events",
                    variant="daily",
                    status="SENT",
                    run_id="first",
                    meta={},
                    ts_utc="2026-02-04T15:00:00+00:00",
                )
                crm_light.append_send_event(
                    conn,
                    subscriber_key="trial_events",
                    variant="daily",
                    status="SENT",
                    run_id="last",
                    meta={},
                    ts_utc="2026-02-07T15:00:00+00:00",
                )
            buf = io.StringIO()
            with contextlib.redirect_stdout(buf):
                code = run_trial_admin.print_trial_status(
                    subscriber_key="trial_events",
                    crm_db_path=db,
                    as_of="2026-02-08",
                )
            self.assertEqual(code, 0)
            keys, values = _parse_stdout_block(buf.getvalue())
            self.assertEqual(keys, REQUIRED_KEYS)
            self.assertEqual(values["TRIAL_FIRST_SENT_UTC"], "2026-02-04T15:00:00+00:00")
            self.assertEqual(values["TRIAL_LAST_SENT_UTC"], "2026-02-07T15:00:00+00:00")
            self.assertEqual(values["TRIAL_SENDS_USED"], "1")
            self.assertEqual(values["TRIAL_EXPIRED_BY_SENDS"], "1")
            self.assertEqual(values["TRIAL_EXPIRED"], "1")

    def test_status_counts_distinct_live_primary_weekdays(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            db = base / "crm_light.sqlite"
            self._seed_trial(
                db,
                subscriber_key="trial_meta",
                start_date="2026-02-01",
                sends_limit=2,
            )
            with crm_light.open_conn(db) as conn:
                crm_light.init_schema(conn)
                conn.execute(
                    "UPDATE subscribers SET email = ? WHERE subscriber_key = ?",
                    ("trial_meta@example.com", "trial_meta"),
                )
                conn.commit()
                crm_light.append_send_event(
                    conn,
                    subscriber_key="trial_meta",
                    variant="DAILY",
                    status="SENT",
                    run_id="live_a",
                    meta={"send_mode": "LIVE", "primary_recipient": "trial_meta@example.com"},
                    ts_utc="2026-02-02T15:00:00+00:00",
                )
                crm_light.append_send_event(
                    conn,
                    subscriber_key="trial_meta",
                    variant="DAILY",
                    status="SENT",
                    run_id="live_a_dup",
                    meta={"send_mode": "LIVE", "primary_recipient": "trial_meta@example.com"},
                    ts_utc="2026-02-02T17:00:00+00:00",
                )
                crm_light.append_send_event(
                    conn,
                    subscriber_key="trial_meta",
                    variant="DAILY",
                    status="SENT",
                    run_id="weekend_live",
                    meta={"send_mode": "LIVE", "primary_recipient": "trial_meta@example.com"},
                    ts_utc="2026-02-07T15:00:00+00:00",
                )
                crm_light.append_send_event(
                    conn,
                    subscriber_key="trial_meta",
                    variant="DAILY",
                    status="SENT",
                    run_id="test_mode",
                    meta={"send_mode": "TEST", "primary_recipient": "trial_meta@example.com"},
                    ts_utc="2026-02-03T15:00:00+00:00",
                )
                crm_light.append_send_event(
                    conn,
                    subscriber_key="trial_meta",
                    variant="DAILY",
                    status="SENT",
                    run_id="other_recipient",
                    meta={"send_mode": "LIVE", "primary_recipient": "other@example.com"},
                    ts_utc="2026-02-04T15:00:00+00:00",
                )

            buf = io.StringIO()
            with contextlib.redirect_stdout(buf):
                code = run_trial_admin.print_trial_status(
                    subscriber_key="trial_meta",
                    crm_db_path=db,
                    as_of="2026-02-05",
                )
            self.assertEqual(code, 0)
            keys, values = _parse_stdout_block(buf.getvalue())
            self.assertEqual(keys, REQUIRED_KEYS)
            self.assertEqual(values["TRIAL_SENDS_USED"], "1")
            self.assertEqual(values["TRIAL_EXPIRED"], "0")
            self.assertEqual(values["TRIAL_EXPIRED_BY_SENDS"], "0")

    def test_trial_not_expired_before_day14_below_hard_cap(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            db = base / "crm_light.sqlite"
            self._seed_trial(
                db,
                subscriber_key="trial_below_cap",
                start_date="2026-02-01",
                sends_limit=30,
            )
            with crm_light.open_conn(db) as conn:
                crm_light.init_schema(conn)
                weekday_dates = [
                    "2026-02-02",
                    "2026-02-03",
                    "2026-02-04",
                    "2026-02-05",
                    "2026-02-06",
                    "2026-02-09",
                    "2026-02-10",
                    "2026-02-11",
                    "2026-02-12",
                    "2026-02-13",
                    "2026-02-16",
                    "2026-02-17",
                    "2026-02-18",
                ]
                for i, day in enumerate(weekday_dates):
                    crm_light.append_send_event(
                        conn,
                        subscriber_key="trial_below_cap",
                        variant="daily",
                        status="SENT",
                        run_id=f"seed_{i}",
                        meta={},
                        ts_utc=f"{day}T15:00:00+00:00",
                    )

            buf = io.StringIO()
            with contextlib.redirect_stdout(buf):
                code = run_trial_admin.print_trial_status(
                    subscriber_key="trial_below_cap",
                    crm_db_path=db,
                    as_of="2026-02-13",
                )
            self.assertEqual(code, 0)
            keys, values = _parse_stdout_block(buf.getvalue())
            self.assertEqual(keys, REQUIRED_KEYS)
            self.assertEqual(values["TRIAL_SENDS_USED"], "13")
            self.assertEqual(values["TRIAL_EXPIRED_BY_SENDS"], "0")
            self.assertEqual(values["TRIAL_EXPIRED"], "0")

    def test_next_action_hint_mapping(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            data_dir = base / "data"
            os.environ["DATA_DIR"] = str(data_dir)
            db = data_dir / "crm_light.sqlite"

            self._seed_trial(
                db,
                subscriber_key="hint_continue",
                start_date="2026-02-01",
                sends_limit=10,
            )
            self._seed_trial(
                db,
                subscriber_key="hint_day15",
                start_date="2026-02-01",
                sends_limit=10,
            )
            self._seed_trial(
                db,
                subscriber_key="hint_expired_missing",
                start_date="2026-02-01",
                sends_limit=1,
            )
            self._seed_trial(
                db,
                subscriber_key="hint_expired_artifact",
                start_date="2026-02-01",
                sends_limit=1,
            )
            with crm_light.open_conn(db) as conn:
                crm_light.init_schema(conn)
                crm_light.append_send_event(
                    conn,
                    subscriber_key="hint_expired_missing",
                    variant="daily",
                    status="SENT",
                    run_id="seed1",
                    meta={},
                    ts_utc="2026-02-02T16:00:00+00:00",
                )
                crm_light.append_send_event(
                    conn,
                    subscriber_key="hint_expired_artifact",
                    variant="daily",
                    status="SENT",
                    run_id="seed2",
                    meta={},
                    ts_utc="2026-02-02T16:00:00+00:00",
                )

            artifact_dir = data_dir / "trials" / "hint_expired_artifact"
            artifact_dir.mkdir(parents=True, exist_ok=True)
            (artifact_dir / "conversion_email.txt").write_text("x", encoding="utf-8")

            cases = [
                ("hint_continue", "2026-02-13", "continue_trial"),
                ("hint_day15", "2026-02-16", "continue_trial"),
                ("hint_expired_missing", "2026-02-03", "send_conversion"),
                ("hint_expired_artifact", "2026-02-03", "manual_followup"),
            ]
            for sk, as_of, expected_hint in cases:
                buf = io.StringIO()
                with contextlib.redirect_stdout(buf):
                    code = run_trial_admin.print_trial_status(
                        subscriber_key=sk,
                        crm_db_path=db,
                        as_of=as_of,
                    )
                self.assertEqual(code, 0)
                _, values = _parse_stdout_block(buf.getvalue())
                self.assertEqual(values["TRIAL_NEXT_ACTION_HINT"], expected_hint)

            os.environ.pop("DATA_DIR", None)

    def test_status_read_only_no_new_send_events(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            db = base / "crm_light.sqlite"
            self._seed_trial(
                db,
                subscriber_key="readonly_trial",
                start_date="2026-02-01",
                sends_limit=10,
            )
            conn = sqlite3.connect(db)
            before = int(conn.execute("SELECT COUNT(1) FROM send_events").fetchone()[0])
            conn.close()
            buf = io.StringIO()
            with contextlib.redirect_stdout(buf):
                code = run_trial_admin.print_trial_status(
                    subscriber_key="readonly_trial",
                    crm_db_path=db,
                    as_of="2026-02-02",
                )
            self.assertEqual(code, 0)
            conn = sqlite3.connect(db)
            after = int(conn.execute("SELECT COUNT(1) FROM send_events").fetchone()[0])
            conn.close()
            self.assertEqual(before, after)

    def test_wally_alias_status_stdout_shape(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            data_dir = base / "data"
            os.environ["DATA_DIR"] = str(data_dir)
            db = data_dir / "crm_light.sqlite"
            self._seed_trial(
                db,
                subscriber_key="wally_trial",
                start_date="2026-02-04",
                sends_limit=10,
            )
            old_argv = sys.argv
            sys.argv = ["run_wally_trial.py", "--status", "--as-of", "2026-02-05"]
            buf = io.StringIO()
            try:
                with contextlib.redirect_stdout(buf):
                    with self.assertRaises(SystemExit) as cm:
                        run_wally_trial.main()
                self.assertEqual(cm.exception.code, 0)
            finally:
                sys.argv = old_argv
                os.environ.pop("DATA_DIR", None)

            out_text = buf.getvalue()
            self.assertIn("TRIAL_WEEKDAYS_ONLY=1", out_text)
            self.assertIn("TRIAL_SCHEDULE_WEEKDAYS=MON,TUE,WED,THU,FRI", out_text)
            keys, _values = _parse_stdout_block(out_text)
            for token in REQUIRED_KEYS:
                self.assertIn(token, keys)

    def test_wally_alias_status_is_read_only(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            data_dir = base / "data"
            os.environ["DATA_DIR"] = str(data_dir)
            db = data_dir / "crm_light.sqlite"
            self._seed_trial(
                db,
                subscriber_key="wally_trial",
                start_date="2026-02-04",
                sends_limit=10,
            )
            conn = sqlite3.connect(db)
            before = int(conn.execute("SELECT COUNT(1) FROM send_events").fetchone()[0])
            conn.close()

            old_argv = sys.argv
            sys.argv = ["run_wally_trial.py", "--status", "--as-of", "2026-02-05"]
            try:
                with contextlib.redirect_stdout(io.StringIO()):
                    with self.assertRaises(SystemExit) as cm:
                        run_wally_trial.main()
                self.assertEqual(cm.exception.code, 0)
            finally:
                sys.argv = old_argv
                os.environ.pop("DATA_DIR", None)

            conn = sqlite3.connect(db)
            after = int(conn.execute("SELECT COUNT(1) FROM send_events").fetchone()[0])
            conn.close()
            self.assertEqual(before, after)

    def test_run_trial_daily_help_tokens_contract(self) -> None:
        old_argv = sys.argv
        sys.argv = ["run_trial_daily.py", "--help"]
        out = io.StringIO()
        try:
            with contextlib.redirect_stdout(out):
                with self.assertRaises(SystemExit) as cm:
                    __import__("run_trial_daily").main()
            self.assertEqual(cm.exception.code, 0)
        finally:
            sys.argv = old_argv
        help_text = out.getvalue()
        required_tokens = [
            "--subscriber-key",
            "--db",
            "--crm-db",
            "--customer",
            "--send-live",
            "--dry-run",
            "--print-config",
            "--test-send-daily",
            "--allow-weekend-send",
        ]
        for token in required_tokens:
            self.assertIn(token, help_text)

    def test_weekend_live_run_skips_without_send_event_write(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            data_dir = base / "data"
            leads_db = base / "osha.sqlite"
            old_data_dir = os.environ.get("DATA_DIR")
            os.environ["DATA_DIR"] = str(data_dir)
            try:
                db = crm_light.resolve_crm_db_path()
                self._seed_trial(
                    db,
                    subscriber_key="weekend_live_trial",
                    start_date="2026-02-01",
                    sends_limit=14,
                )

                orig_datetime = run_trial_daily.datetime
                orig_deliver = run_trial_daily._run_deliver_daily

                class _WeekendDateTime(datetime):  # type: ignore[misc]
                    @classmethod
                    def now(cls, tz=None):  # type: ignore[override]
                        base_dt = datetime(2026, 2, 22, 15, 0, 0, tzinfo=timezone.utc)  # Sunday
                        if tz is None:
                            return base_dt.replace(tzinfo=None)
                        return base_dt.astimezone(tz)

                def _unexpected_deliver(*_args, **_kwargs):  # type: ignore[no-untyped-def]
                    raise AssertionError("_run_deliver_daily should not be called on weekend skip")

                run_trial_daily.datetime = _WeekendDateTime  # type: ignore[assignment]
                run_trial_daily._run_deliver_daily = _unexpected_deliver  # type: ignore[assignment]
                try:
                    out = io.StringIO()
                    with contextlib.redirect_stdout(out):
                        code = run_trial_daily.run_trial_daily(
                            subscriber_key="weekend_live_trial",
                            leads_db=str(leads_db),
                            crm_db=db,
                            customer_arg="",
                            send_live=True,
                            dry_run=False,
                            test_send_daily=False,
                            print_config=False,
                        )
                finally:
                    run_trial_daily.datetime = orig_datetime  # type: ignore[assignment]
                    run_trial_daily._run_deliver_daily = orig_deliver  # type: ignore[assignment]

                self.assertEqual(code, 0)
                text = out.getvalue()
                self.assertIn("SKIP_NON_WEEKDAY subscriber_key=weekend_live_trial", text)
                self.assertIn("weekday=sun", text)
                with crm_light.open_conn(db) as conn:
                    crm_light.init_schema(conn)
                    count = int(
                        conn.execute(
                            "SELECT COUNT(*) FROM send_events WHERE subscriber_key='weekend_live_trial'"
                        ).fetchone()[0]
                    )
                self.assertEqual(count, 0)
            finally:
                if old_data_dir is None:
                    os.environ.pop("DATA_DIR", None)
                else:
                    os.environ["DATA_DIR"] = old_data_dir

    def test_weekend_dry_run_skips_unless_override_enabled(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            data_dir = base / "data"
            leads_db = base / "osha.sqlite"
            old_data_dir = os.environ.get("DATA_DIR")
            os.environ["DATA_DIR"] = str(data_dir)
            try:
                db = crm_light.resolve_crm_db_path()
                self._seed_trial(
                    db,
                    subscriber_key="weekend_dry_trial",
                    start_date="2026-02-01",
                    sends_limit=14,
                )

                calls = {"deliver": 0}
                orig_datetime = run_trial_daily.datetime
                orig_deliver = run_trial_daily._run_deliver_daily

                class _WeekendDateTime(datetime):  # type: ignore[misc]
                    @classmethod
                    def now(cls, tz=None):  # type: ignore[override]
                        base_dt = datetime(2026, 2, 22, 15, 0, 0, tzinfo=timezone.utc)  # Sunday
                        if tz is None:
                            return base_dt.replace(tzinfo=None)
                        return base_dt.astimezone(tz)

                def _fake_deliver(*_args, **_kwargs):  # type: ignore[no-untyped-def]
                    calls["deliver"] += 1
                    return 0, "ok"

                run_trial_daily.datetime = _WeekendDateTime  # type: ignore[assignment]
                run_trial_daily._run_deliver_daily = _fake_deliver  # type: ignore[assignment]
                try:
                    out_skip = io.StringIO()
                    with contextlib.redirect_stdout(out_skip):
                        code_skip = run_trial_daily.run_trial_daily(
                            subscriber_key="weekend_dry_trial",
                            leads_db=str(leads_db),
                            crm_db=db,
                            customer_arg="",
                            send_live=False,
                            dry_run=True,
                            test_send_daily=False,
                            print_config=False,
                            allow_weekend_send=False,
                        )
                    out_allow = io.StringIO()
                    with contextlib.redirect_stdout(out_allow):
                        code_allow = run_trial_daily.run_trial_daily(
                            subscriber_key="weekend_dry_trial",
                            leads_db=str(leads_db),
                            crm_db=db,
                            customer_arg="",
                            send_live=False,
                            dry_run=True,
                            test_send_daily=False,
                            print_config=False,
                            allow_weekend_send=True,
                        )
                finally:
                    run_trial_daily.datetime = orig_datetime  # type: ignore[assignment]
                    run_trial_daily._run_deliver_daily = orig_deliver  # type: ignore[assignment]

                self.assertEqual(code_skip, 0)
                self.assertEqual(code_allow, 0)
                self.assertIn("SKIP_NON_WEEKDAY subscriber_key=weekend_dry_trial", out_skip.getvalue())
                self.assertNotIn("SKIP_NON_WEEKDAY", out_allow.getvalue())
                self.assertEqual(calls["deliver"], 1)
                with crm_light.open_conn(db) as conn:
                    crm_light.init_schema(conn)
                    statuses = [
                        str(row[0] or "")
                        for row in conn.execute(
                            "SELECT status FROM send_events WHERE subscriber_key='weekend_dry_trial' ORDER BY id"
                        ).fetchall()
                    ]
                self.assertEqual(statuses, ["DRY_RUN"])
            finally:
                if old_data_dir is None:
                    os.environ.pop("DATA_DIR", None)
                else:
                    os.environ["DATA_DIR"] = old_data_dir

    def test_one_send_per_local_date_guard_prevents_double_counting(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            data_dir = base / "data"
            leads_db = base / "osha.sqlite"
            old_data_dir = os.environ.get("DATA_DIR")
            os.environ["DATA_DIR"] = str(data_dir)
            try:
                db = crm_light.resolve_crm_db_path()
                self._seed_trial(
                    db,
                    subscriber_key="guard_trial",
                    start_date="2026-02-01",
                    sends_limit=14,
                )

                # Freeze "today" to a weekday so weekday-only semantics are deterministic.
                orig_datetime = run_trial_daily.datetime
                class _FixedDateTime(datetime):  # type: ignore[misc]
                    @classmethod
                    def now(cls, tz=None):  # type: ignore[override]
                        base = datetime(2026, 2, 20, 14, 0, 0, tzinfo=timezone.utc)
                        if tz is None:
                            return base.replace(tzinfo=None)
                        return base.astimezone(tz)

                with crm_light.open_conn(db) as conn:
                    crm_light.init_schema(conn)
                    run_trial_daily.datetime = _FixedDateTime  # type: ignore[assignment]
                    now_local = run_trial_daily.datetime.now(run_trial_daily._resolve_trial_timezone("America/Chicago"))
                    sent_ts = (
                        now_local.replace(hour=12, minute=0, second=0, microsecond=0)
                        .astimezone(timezone.utc)
                        .isoformat(timespec="seconds")
                    )
                    crm_light.append_send_event(
                        conn,
                        subscriber_key="guard_trial",
                        variant="daily",
                        status="SENT",
                        run_id="already_sent_today",
                        meta={},
                        ts_utc=sent_ts,
                    )

                calls = {"deliver": 0}
                orig_deliver = run_trial_daily._run_deliver_daily

                def _fake_deliver(*_args, **_kwargs):  # type: ignore[no-untyped-def]
                    calls["deliver"] += 1
                    return 0, "ok"

                run_trial_daily._run_deliver_daily = _fake_deliver  # type: ignore[assignment]
                try:
                    code = run_trial_daily.run_trial_daily(
                        subscriber_key="guard_trial",
                        leads_db=str(leads_db),
                        crm_db=db,
                        customer_arg="",
                        send_live=True,
                        dry_run=False,
                        test_send_daily=False,
                        print_config=False,
                    )
                finally:
                    run_trial_daily._run_deliver_daily = orig_deliver  # type: ignore[assignment]
                    run_trial_daily.datetime = orig_datetime  # type: ignore[assignment]

                self.assertEqual(code, 0)
                self.assertEqual(calls["deliver"], 0)
                with crm_light.open_conn(db) as conn:
                    crm_light.init_schema(conn)
                    sends_used = crm_light.count_trial_delivery_days(
                        conn,
                        "guard_trial",
                        "2026-02-01",
                        tz_name="America/Chicago",
                        primary_recipient="guard_trial@example.com",
                        weekdays_only=True,
                    )
                    statuses = conn.execute(
                        "SELECT status FROM send_events WHERE subscriber_key='guard_trial' ORDER BY id"
                    ).fetchall()
                self.assertEqual(sends_used, 1)
                self.assertIn("SKIP_ALREADY_SENT_LOCAL_DATE", [str(row[0] or "") for row in statuses])
            finally:
                if old_data_dir is None:
                    os.environ.pop("DATA_DIR", None)
                else:
                    os.environ["DATA_DIR"] = old_data_dir

    def test_backfill_writes_sent_and_status_counts_with_temp_data_dir(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            data_dir = base / "custom_data_dir"
            old_data_dir = os.environ.get("DATA_DIR")
            os.environ["DATA_DIR"] = str(data_dir)
            try:
                db = crm_light.resolve_crm_db_path()
                self._seed_trial(
                    db,
                    subscriber_key="wally_trial",
                    start_date="2026-02-01",
                    sends_limit=10,
                )
                log_path = base / "wally_trial_task.log"
                log_path.write_text(
                    "[Mon 02/17/2026  9:01:24.39] SUCCESS: Wally trial run completed\n"
                    "[Tue 02/18/2026  9:02:13.00] SUCCESS: Wally trial run completed\n",
                    encoding="utf-8",
                )

                proc = subprocess.run(
                    [sys.executable, "scripts/backfill_wally_trial_send_events.py", "--log-path", str(log_path)],
                    cwd=str(Path(__file__).resolve().parent),
                    capture_output=True,
                    text=True,
                    check=False,
                    env=dict(os.environ),
                )
                self.assertEqual(proc.returncode, 0, msg=f"stdout={proc.stdout}\nstderr={proc.stderr}")

                conn = sqlite3.connect(db)
                rows = conn.execute(
                    "SELECT status FROM send_events WHERE subscriber_key = 'wally_trial' ORDER BY id"
                ).fetchall()
                conn.close()
                self.assertEqual(len(rows), 2)
                self.assertTrue(all(str(row[0] or "") == "SENT" for row in rows))

                old_argv = sys.argv
                sys.argv = ["run_wally_trial.py", "--status", "--as-of", "2026-02-18"]
                buf = io.StringIO()
                try:
                    with contextlib.redirect_stdout(buf):
                        with self.assertRaises(SystemExit) as cm:
                            run_wally_trial.main()
                    self.assertEqual(cm.exception.code, 0)
                finally:
                    sys.argv = old_argv

                _keys, values = _parse_stdout_block(buf.getvalue())
                self.assertEqual(values["TRIAL_SENDS_USED"], "2")
            finally:
                if old_data_dir is None:
                    os.environ.pop("DATA_DIR", None)
                else:
                    os.environ["DATA_DIR"] = old_data_dir

    def test_conversion_draft_writes_to_resolved_data_dir(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            data_dir = base / "data_dir"
            old_data_dir = os.environ.get("DATA_DIR")
            old_conv = os.environ.get("TRIAL_CONVERSION_URL")
            os.environ["DATA_DIR"] = str(data_dir)
            os.environ["TRIAL_CONVERSION_URL"] = "https://example.com/activate"
            try:
                db = crm_light.resolve_crm_db_path()
                self._seed_trial(
                    db,
                    subscriber_key="wally_trial",
                    start_date="2026-02-04",
                    sends_limit=14,
                )
                code = run_trial_admin.main(["conversion-draft", "--subscriber-key", "wally_trial"])
                self.assertEqual(code, 0)
                artifact = data_dir / "trials" / "wally_trial" / "conversion_email.txt"
                self.assertTrue(artifact.exists())
            finally:
                if old_data_dir is None:
                    os.environ.pop("DATA_DIR", None)
                else:
                    os.environ["DATA_DIR"] = old_data_dir
                if old_conv is None:
                    os.environ.pop("TRIAL_CONVERSION_URL", None)
                else:
                    os.environ["TRIAL_CONVERSION_URL"] = old_conv

    def test_conversion_draft_is_db_read_only(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            data_dir = base / "data_dir"
            old_data_dir = os.environ.get("DATA_DIR")
            os.environ["DATA_DIR"] = str(data_dir)
            try:
                db = crm_light.resolve_crm_db_path()
                self._seed_trial(
                    db,
                    subscriber_key="wally_trial",
                    start_date="2026-02-04",
                    sends_limit=14,
                )
                with crm_light.open_conn(db) as conn:
                    crm_light.init_schema(conn)
                    before_send_rows = int(conn.execute("SELECT COUNT(1) FROM send_events").fetchone()[0])
                    before_sub = conn.execute(
                        "SELECT subscriber_key, email, territory_code, tz, status FROM subscribers WHERE subscriber_key=?",
                        ("wally_trial",),
                    ).fetchone()
                    before_sub_tuple = tuple(before_sub) if before_sub else tuple()

                code = run_trial_admin.main(["conversion-draft", "--subscriber-key", "wally_trial"])
                self.assertEqual(code, 0)

                with crm_light.open_conn(db) as conn:
                    crm_light.init_schema(conn)
                    after_send_rows = int(conn.execute("SELECT COUNT(1) FROM send_events").fetchone()[0])
                    after_sub = conn.execute(
                        "SELECT subscriber_key, email, territory_code, tz, status FROM subscribers WHERE subscriber_key=?",
                        ("wally_trial",),
                    ).fetchone()
                    after_sub_tuple = tuple(after_sub) if after_sub else tuple()
                self.assertEqual(before_send_rows, after_send_rows)
                self.assertEqual(before_sub_tuple, after_sub_tuple)
            finally:
                if old_data_dir is None:
                    os.environ.pop("DATA_DIR", None)
                else:
                    os.environ["DATA_DIR"] = old_data_dir

    def test_conversion_draft_template_plain_text_and_required_content(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            data_dir = base / "data_dir"
            old_data_dir = os.environ.get("DATA_DIR")
            old_conv = os.environ.get("TRIAL_CONVERSION_URL")
            os.environ["DATA_DIR"] = str(data_dir)
            os.environ["TRIAL_CONVERSION_URL"] = "https://example.com/activate"
            try:
                db = crm_light.resolve_crm_db_path()
                self._seed_trial(
                    db,
                    subscriber_key="wally_trial",
                    start_date="2026-02-04",
                    sends_limit=14,
                )
                code = run_trial_admin.main(["conversion-draft", "--subscriber-key", "wally_trial"])
                self.assertEqual(code, 0)
                artifact = data_dir / "trials" / "wally_trial" / "conversion_email.txt"
                text = artifact.read_text(encoding="utf-8")
                self.assertIn("To: wally_trial@example.com", text)
                self.assertIn("Subject: Keep your OSHA signal digest running -", text)
                self.assertIn("Quick note on \"0 new\":", text)
                self.assertIn('Reply "go" and confirm the metros/cities', text)
                self.assertIn("Or activate via Stripe here:", text)
                self.assertIn("If you'd rather confirm fit before paying", text)
                self.assertIn("Want any tweaks (add/remove metros, add recipients, different send time)?", text)
                self.assertIn('P.S. If it\'s not a fit, just reply "stop" and I\'ll close it out.', text)
                self.assertIn("https://example.com/activate", text)
                self.assertIn("Texas Triangle", text)
                self.assertNotRegex(text, re.compile(r"<(html|body|p|a|br)\\b", re.IGNORECASE))
            finally:
                if old_data_dir is None:
                    os.environ.pop("DATA_DIR", None)
                else:
                    os.environ["DATA_DIR"] = old_data_dir
                if old_conv is None:
                    os.environ.pop("TRIAL_CONVERSION_URL", None)
                else:
                    os.environ["TRIAL_CONVERSION_URL"] = old_conv

    def test_conversion_draft_placeholder_when_link_unknown(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            data_dir = base / "data_dir"
            old_data_dir = os.environ.get("DATA_DIR")
            old_conv = os.environ.get("TRIAL_CONVERSION_URL")
            os.environ["DATA_DIR"] = str(data_dir)
            os.environ.pop("TRIAL_CONVERSION_URL", None)
            try:
                db = crm_light.resolve_crm_db_path()
                self._seed_trial(
                    db,
                    subscriber_key="wally_trial",
                    start_date="2026-02-04",
                    sends_limit=14,
                )
                code = run_trial_admin.main(["conversion-draft", "--subscriber-key", "wally_trial"])
                self.assertEqual(code, 0)
                artifact = data_dir / "trials" / "wally_trial" / "conversion_email.txt"
                text = artifact.read_text(encoding="utf-8")
                self.assertIn("{stripe_link}", text)
            finally:
                if old_data_dir is None:
                    os.environ.pop("DATA_DIR", None)
                else:
                    os.environ["DATA_DIR"] = old_data_dir
                if old_conv is None:
                    os.environ.pop("TRIAL_CONVERSION_URL", None)
                else:
                    os.environ["TRIAL_CONVERSION_URL"] = old_conv

    def test_expiry_path_uses_same_conversion_template_as_conversion_draft(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            data_dir = base / "data_dir"
            leads_db = base / "osha.sqlite"
            old_data_dir = os.environ.get("DATA_DIR")
            old_conv = os.environ.get("TRIAL_CONVERSION_URL")
            os.environ["DATA_DIR"] = str(data_dir)
            os.environ["TRIAL_CONVERSION_URL"] = "https://example.com/activate"
            try:
                db = crm_light.resolve_crm_db_path()
                self._seed_trial(
                    db,
                    subscriber_key="wally_trial",
                    start_date="2026-02-04",
                    sends_limit=1,
                )
                with crm_light.open_conn(db) as conn:
                    crm_light.init_schema(conn)
                    crm_light.append_send_event(
                        conn,
                        subscriber_key="wally_trial",
                        variant="DAILY",
                        status="SENT",
                        run_id="seed_expired",
                        meta={"send_mode": "LIVE", "primary_recipient": "wally_trial@example.com"},
                        ts_utc="2026-02-04T15:00:00+00:00",
                    )

                code = run_trial_admin.main(["conversion-draft", "--subscriber-key", "wally_trial"])
                self.assertEqual(code, 0)
                artifact = data_dir / "trials" / "wally_trial" / "conversion_email.txt"
                draft_text = artifact.read_text(encoding="utf-8")

                run_code = run_trial_daily.run_trial_daily(
                    subscriber_key="wally_trial",
                    leads_db=str(leads_db),
                    crm_db=db,
                    customer_arg="",
                    send_live=False,
                    dry_run=False,
                    test_send_daily=False,
                    print_config=False,
                )
                self.assertEqual(run_code, 0)
                expiry_text = artifact.read_text(encoding="utf-8")
                self.assertEqual(draft_text, expiry_text)
            finally:
                if old_data_dir is None:
                    os.environ.pop("DATA_DIR", None)
                else:
                    os.environ["DATA_DIR"] = old_data_dir
                if old_conv is None:
                    os.environ.pop("TRIAL_CONVERSION_URL", None)
                else:
                    os.environ["TRIAL_CONVERSION_URL"] = old_conv

    def test_conversion_email_auto_sends_once_on_first_expired_live_run(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            data_dir = base / "data_dir"
            leads_db = base / "osha.sqlite"
            old_data_dir = os.environ.get("DATA_DIR")
            old_conv = os.environ.get("TRIAL_CONVERSION_URL")
            os.environ["DATA_DIR"] = str(data_dir)
            os.environ["TRIAL_CONVERSION_URL"] = "https://example.com/activate"
            try:
                db = crm_light.resolve_crm_db_path()
                self._seed_trial(
                    db,
                    subscriber_key="wally_trial",
                    start_date="2026-02-04",
                    sends_limit=1,
                )

                calls = {"deliver": 0, "conversion": 0}
                orig_deliver = run_trial_daily._run_deliver_daily
                orig_mode = run_trial_daily._try_extract_latest_send_start_mode
                orig_send_conversion = run_trial_daily._send_conversion_email_from_artifact
                orig_datetime = run_trial_daily.datetime
                orig_crm_datetime = crm_light.datetime

                class _FixedDateTime(datetime):  # type: ignore[misc]
                    @classmethod
                    def now(cls, tz=None):  # type: ignore[override]
                        base = datetime(2026, 2, 20, 14, 0, 0, tzinfo=timezone.utc)
                        if tz is None:
                            return base.replace(tzinfo=None)
                        return base.astimezone(tz)

                def _fake_deliver(*_args, **_kwargs):  # type: ignore[no-untyped-def]
                    calls["deliver"] += 1
                    return 0, "ok"

                def _fake_mode(*_args, **_kwargs):  # type: ignore[no-untyped-def]
                    return "LIVE"

                def _fake_send_conversion(*, artifact_path, subscriber_key, territory_code):  # type: ignore[no-untyped-def]
                    calls["conversion"] += 1
                    self.assertTrue(Path(artifact_path).exists())
                    self.assertEqual(subscriber_key, "wally_trial")
                    self.assertEqual(territory_code, "TX_TRIANGLE_V1")
                    return True, "<msg-1>", ""

                run_trial_daily._run_deliver_daily = _fake_deliver  # type: ignore[assignment]
                run_trial_daily._try_extract_latest_send_start_mode = _fake_mode  # type: ignore[assignment]
                run_trial_daily._send_conversion_email_from_artifact = _fake_send_conversion  # type: ignore[assignment]
                run_trial_daily.datetime = _FixedDateTime  # type: ignore[assignment]
                crm_light.datetime = _FixedDateTime  # type: ignore[assignment]
                try:
                    code_first = run_trial_daily.run_trial_daily(
                        subscriber_key="wally_trial",
                        leads_db=str(leads_db),
                        crm_db=db,
                        customer_arg="",
                        send_live=True,
                        dry_run=False,
                        test_send_daily=False,
                        print_config=False,
                    )
                    code_second = run_trial_daily.run_trial_daily(
                        subscriber_key="wally_trial",
                        leads_db=str(leads_db),
                        crm_db=db,
                        customer_arg="",
                        send_live=True,
                        dry_run=False,
                        test_send_daily=False,
                        print_config=False,
                    )
                    code_third = run_trial_daily.run_trial_daily(
                        subscriber_key="wally_trial",
                        leads_db=str(leads_db),
                        crm_db=db,
                        customer_arg="",
                        send_live=True,
                        dry_run=False,
                        test_send_daily=False,
                        print_config=False,
                    )
                finally:
                    run_trial_daily._run_deliver_daily = orig_deliver  # type: ignore[assignment]
                    run_trial_daily._try_extract_latest_send_start_mode = orig_mode  # type: ignore[assignment]
                    run_trial_daily._send_conversion_email_from_artifact = orig_send_conversion  # type: ignore[assignment]
                    run_trial_daily.datetime = orig_datetime  # type: ignore[assignment]
                    crm_light.datetime = orig_crm_datetime  # type: ignore[assignment]

                self.assertEqual(code_first, 0)
                self.assertEqual(code_second, 0)
                self.assertEqual(code_third, 0)
                self.assertEqual(calls["deliver"], 1)
                self.assertEqual(calls["conversion"], 1)

                artifact = data_dir / "trials" / "wally_trial" / "conversion_email.txt"
                self.assertTrue(artifact.exists())

                with crm_light.open_conn(db) as conn:
                    crm_light.init_schema(conn)
                    trial = crm_light.get_trial_state(conn, "wally_trial") or {}
                    statuses = [
                        str(row[0] or "")
                        for row in conn.execute(
                            "SELECT status FROM send_events WHERE subscriber_key='wally_trial' ORDER BY id"
                        ).fetchall()
                    ]
                self.assertTrue(str(trial.get("notified_at_utc") or "").strip())
                self.assertEqual(statuses.count("SENT"), 1)
                self.assertEqual(statuses.count("CONVERSION_SENT"), 1)
            finally:
                if old_data_dir is None:
                    os.environ.pop("DATA_DIR", None)
                else:
                    os.environ["DATA_DIR"] = old_data_dir
                if old_conv is None:
                    os.environ.pop("TRIAL_CONVERSION_URL", None)
                else:
                    os.environ["TRIAL_CONVERSION_URL"] = old_conv

    def test_expired_non_live_run_leaves_conversion_pending_until_live(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            data_dir = base / "data_dir"
            leads_db = base / "osha.sqlite"
            old_data_dir = os.environ.get("DATA_DIR")
            old_conv = os.environ.get("TRIAL_CONVERSION_URL")
            os.environ["DATA_DIR"] = str(data_dir)
            os.environ["TRIAL_CONVERSION_URL"] = "https://example.com/activate"
            try:
                db = crm_light.resolve_crm_db_path()
                self._seed_trial(
                    db,
                    subscriber_key="wally_trial",
                    start_date="2026-02-04",
                    sends_limit=1,
                )
                with crm_light.open_conn(db) as conn:
                    crm_light.init_schema(conn)
                    crm_light.append_send_event(
                        conn,
                        subscriber_key="wally_trial",
                        variant="daily",
                        status="SENT",
                        run_id="seed_expired",
                        meta={"send_mode": "LIVE", "primary_recipient": "wally_trial@example.com"},
                        ts_utc="2026-02-04T15:00:00+00:00",
                    )

                calls = {"conversion": 0}
                orig_send_conversion = run_trial_daily._send_conversion_email_from_artifact

                def _fake_send_conversion(*, artifact_path, subscriber_key, territory_code):  # type: ignore[no-untyped-def]
                    calls["conversion"] += 1
                    self.assertTrue(Path(artifact_path).exists())
                    self.assertEqual(subscriber_key, "wally_trial")
                    self.assertEqual(territory_code, "TX_TRIANGLE_V1")
                    return True, "<msg-2>", ""

                run_trial_daily._send_conversion_email_from_artifact = _fake_send_conversion  # type: ignore[assignment]
                try:
                    code_non_live = run_trial_daily.run_trial_daily(
                        subscriber_key="wally_trial",
                        leads_db=str(leads_db),
                        crm_db=db,
                        customer_arg="",
                        send_live=False,
                        dry_run=False,
                        test_send_daily=False,
                        print_config=False,
                        allow_weekend_send=True,
                    )
                    with crm_light.open_conn(db) as conn:
                        crm_light.init_schema(conn)
                        trial_after_non_live = crm_light.get_trial_state(conn, "wally_trial") or {}
                    code_live = run_trial_daily.run_trial_daily(
                        subscriber_key="wally_trial",
                        leads_db=str(leads_db),
                        crm_db=db,
                        customer_arg="",
                        send_live=True,
                        dry_run=False,
                        test_send_daily=False,
                        print_config=False,
                        allow_weekend_send=True,
                    )
                    with crm_light.open_conn(db) as conn:
                        crm_light.init_schema(conn)
                        trial_after_live = crm_light.get_trial_state(conn, "wally_trial") or {}
                finally:
                    run_trial_daily._send_conversion_email_from_artifact = orig_send_conversion  # type: ignore[assignment]

                self.assertEqual(code_non_live, 0)
                self.assertEqual(code_live, 0)
                self.assertEqual(calls["conversion"], 1)
                self.assertFalse(str(trial_after_non_live.get("notified_at_utc") or "").strip())
                self.assertTrue(str(trial_after_live.get("notified_at_utc") or "").strip())
            finally:
                if old_data_dir is None:
                    os.environ.pop("DATA_DIR", None)
                else:
                    os.environ["DATA_DIR"] = old_data_dir
                if old_conv is None:
                    os.environ.pop("TRIAL_CONVERSION_URL", None)
                else:
                    os.environ["TRIAL_CONVERSION_URL"] = old_conv

    def test_expired_live_with_placeholder_link_blocks_send_and_latch(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            data_dir = base / "data_dir"
            leads_db = base / "osha.sqlite"
            old_data_dir = os.environ.get("DATA_DIR")
            old_conv = os.environ.get("TRIAL_CONVERSION_URL")
            os.environ["DATA_DIR"] = str(data_dir)
            os.environ.pop("TRIAL_CONVERSION_URL", None)
            try:
                db = crm_light.resolve_crm_db_path()
                self._seed_trial(
                    db,
                    subscriber_key="wally_trial",
                    start_date="2026-02-04",
                    sends_limit=1,
                )
                with crm_light.open_conn(db) as conn:
                    crm_light.init_schema(conn)
                    crm_light.append_send_event(
                        conn,
                        subscriber_key="wally_trial",
                        variant="daily",
                        status="SENT",
                        run_id="seed_expired",
                        meta={"send_mode": "LIVE", "primary_recipient": "wally_trial@example.com"},
                        ts_utc="2026-02-04T15:00:00+00:00",
                    )

                calls = {"conversion": 0}
                orig_send_conversion = run_trial_daily._send_conversion_email_from_artifact

                def _fake_send_conversion(*, artifact_path, subscriber_key, territory_code):  # type: ignore[no-untyped-def]
                    calls["conversion"] += 1
                    return True, "<msg-should-not-send>", ""

                run_trial_daily._send_conversion_email_from_artifact = _fake_send_conversion  # type: ignore[assignment]
                buf = io.StringIO()
                try:
                    with contextlib.redirect_stdout(buf):
                        code = run_trial_daily.run_trial_daily(
                            subscriber_key="wally_trial",
                            leads_db=str(leads_db),
                            crm_db=db,
                            customer_arg="",
                            send_live=True,
                            dry_run=False,
                            test_send_daily=False,
                            print_config=False,
                            allow_weekend_send=True,
                        )
                finally:
                    run_trial_daily._send_conversion_email_from_artifact = orig_send_conversion  # type: ignore[assignment]

                self.assertEqual(code, 0)
                self.assertEqual(calls["conversion"], 0)
                self.assertIn("ERR_CONVERSION_LINK_MISSING subscriber_key=wally_trial", buf.getvalue())

                with crm_light.open_conn(db) as conn:
                    crm_light.init_schema(conn)
                    trial = crm_light.get_trial_state(conn, "wally_trial") or {}
                    statuses = [
                        str(row[0] or "")
                        for row in conn.execute(
                            "SELECT status FROM send_events WHERE subscriber_key='wally_trial' ORDER BY id"
                        ).fetchall()
                    ]
                self.assertFalse(str(trial.get("notified_at_utc") or "").strip())
                self.assertIn("CONVERSION_LINK_MISSING", statuses)
            finally:
                if old_data_dir is None:
                    os.environ.pop("DATA_DIR", None)
                else:
                    os.environ["DATA_DIR"] = old_data_dir
                if old_conv is None:
                    os.environ.pop("TRIAL_CONVERSION_URL", None)
                else:
                    os.environ["TRIAL_CONVERSION_URL"] = old_conv

    def test_expired_live_uses_existing_conversion_artifact_verbatim(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            data_dir = base / "data_dir"
            leads_db = base / "osha.sqlite"
            old_data_dir = os.environ.get("DATA_DIR")
            old_conv = os.environ.get("TRIAL_CONVERSION_URL")
            os.environ["DATA_DIR"] = str(data_dir)
            os.environ["TRIAL_CONVERSION_URL"] = "https://example.com/activate"
            try:
                db = crm_light.resolve_crm_db_path()
                self._seed_trial(
                    db,
                    subscriber_key="wally_trial",
                    start_date="2026-02-04",
                    sends_limit=1,
                )
                with crm_light.open_conn(db) as conn:
                    crm_light.init_schema(conn)
                    crm_light.append_send_event(
                        conn,
                        subscriber_key="wally_trial",
                        variant="daily",
                        status="SENT",
                        run_id="seed_expired",
                        meta={"send_mode": "LIVE", "primary_recipient": "wally_trial@example.com"},
                        ts_utc="2026-02-04T15:00:00+00:00",
                    )

                artifact = data_dir / "trials" / "wally_trial" / "conversion_email.txt"
                artifact.parent.mkdir(parents=True, exist_ok=True)
                custom_text = (
                    "To: custom@example.com\n\n"
                    "Subject: Custom Conversion Subject\n\n"
                    "Custom line one.\n"
                    "Custom CTA link: https://example.com/pay\n"
                )
                artifact.write_text(custom_text, encoding="utf-8")

                seen = {"calls": 0, "text": ""}
                orig_send_conversion = run_trial_daily._send_conversion_email_from_artifact

                def _fake_send_conversion(*, artifact_path, subscriber_key, territory_code):  # type: ignore[no-untyped-def]
                    seen["calls"] += 1
                    seen["text"] = Path(artifact_path).read_text(encoding="utf-8")
                    self.assertEqual(subscriber_key, "wally_trial")
                    self.assertEqual(territory_code, "TX_TRIANGLE_V1")
                    return True, "<msg-custom>", ""

                run_trial_daily._send_conversion_email_from_artifact = _fake_send_conversion  # type: ignore[assignment]
                try:
                    code = run_trial_daily.run_trial_daily(
                        subscriber_key="wally_trial",
                        leads_db=str(leads_db),
                        crm_db=db,
                        customer_arg="",
                        send_live=True,
                        dry_run=False,
                        test_send_daily=False,
                        print_config=False,
                        allow_weekend_send=True,
                    )
                finally:
                    run_trial_daily._send_conversion_email_from_artifact = orig_send_conversion  # type: ignore[assignment]

                self.assertEqual(code, 0)
                self.assertEqual(seen["calls"], 1)
                self.assertEqual(seen["text"], custom_text)
                self.assertEqual(artifact.read_text(encoding="utf-8"), custom_text)

                with crm_light.open_conn(db) as conn:
                    crm_light.init_schema(conn)
                    trial = crm_light.get_trial_state(conn, "wally_trial") or {}
                self.assertTrue(str(trial.get("notified_at_utc") or "").strip())
            finally:
                if old_data_dir is None:
                    os.environ.pop("DATA_DIR", None)
                else:
                    os.environ["DATA_DIR"] = old_data_dir
                if old_conv is None:
                    os.environ.pop("TRIAL_CONVERSION_URL", None)
                else:
                    os.environ["TRIAL_CONVERSION_URL"] = old_conv


if __name__ == "__main__":
    unittest.main()
