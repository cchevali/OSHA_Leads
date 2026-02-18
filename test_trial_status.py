from __future__ import annotations

import contextlib
import io
import os
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

import crm_light
import run_trial_admin
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

    def test_days_since_and_14_day_boundary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            db = base / "crm_light.sqlite"
            self._seed_trial(
                db,
                subscriber_key="trial_boundary",
                start_date="2026-02-01",
                sends_limit=10,
            )
            for as_of, expected_days, expected_elapsed in [
                ("2026-02-14", "13", "0"),
                ("2026-02-15", "14", "1"),
                ("2026-02-16", "15", "1"),
            ]:
                buf = io.StringIO()
                with contextlib.redirect_stdout(buf):
                    code = run_trial_admin.print_trial_status(
                        subscriber_key="trial_boundary",
                        crm_db_path=db,
                        as_of=as_of,
                    )
                self.assertEqual(code, 0)
                keys, values = _parse_stdout_block(buf.getvalue())
                self.assertEqual(keys, REQUIRED_KEYS)
                self.assertEqual(values["TRIAL_DAYS_SINCE_START"], expected_days)
                self.assertEqual(values["TRIAL_14_DAY_ELAPSED"], expected_elapsed)

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
                subscriber_key="hint_day14",
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
                ("hint_day14", "2026-02-15", "send_conversion"),
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


if __name__ == "__main__":
    unittest.main()
