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
                for i in range(13):
                    crm_light.append_send_event(
                        conn,
                        subscriber_key="trial_boundary",
                        variant="daily",
                        status="SENT",
                        run_id=f"sent_{i}",
                        meta={},
                        ts_utc=f"2026-02-{i + 1:02d}T15:00:00+00:00",
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
                    ts_utc="2026-02-14T15:00:00+00:00",
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
            self.assertEqual(values["TRIAL_SENDS_USED"], "2")
            self.assertEqual(values["TRIAL_EXPIRED_BY_SENDS"], "1")
            self.assertEqual(values["TRIAL_EXPIRED"], "1")

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
                for i in range(29):
                    crm_light.append_send_event(
                        conn,
                        subscriber_key="trial_below_cap",
                        variant="daily",
                        status="SENT",
                        run_id=f"seed_{i}",
                        meta={},
                        ts_utc=f"2026-02-{(i % 12) + 1:02d}T15:00:00+00:00",
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
            self.assertEqual(values["TRIAL_SENDS_USED"], "29")
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
                    ts_utc="2026-02-01T16:00:00+00:00",
                )
                crm_light.append_send_event(
                    conn,
                    subscriber_key="hint_expired_artifact",
                    variant="daily",
                    status="SENT",
                    run_id="seed2",
                    meta={},
                    ts_utc="2026-02-01T16:00:00+00:00",
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

            keys, _values = _parse_stdout_block(buf.getvalue())
            self.assertEqual(keys, REQUIRED_KEYS)

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
        ]
        for token in required_tokens:
            self.assertIn(token, help_text)

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

                with crm_light.open_conn(db) as conn:
                    crm_light.init_schema(conn)
                    now_local = datetime.now(run_trial_daily._resolve_trial_timezone("America/Chicago"))
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

                self.assertEqual(code, 0)
                self.assertEqual(calls["deliver"], 0)
                with crm_light.open_conn(db) as conn:
                    crm_light.init_schema(conn)
                    sends_used = crm_light.count_successful_sends(conn, "guard_trial", "2026-02-01")
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


if __name__ == "__main__":
    unittest.main()
