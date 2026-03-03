from __future__ import annotations

import io
import sqlite3
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path

import crm_light
import run_trial_admin
import run_trial_daily


def _build_v7_style_crm_db(path: Path) -> None:
    conn = sqlite3.connect(str(path))
    try:
        conn.executescript(
            """
            CREATE TABLE subscribers (
                subscriber_key TEXT PRIMARY KEY,
                email TEXT NOT NULL DEFAULT '',
                territory_code TEXT NOT NULL DEFAULT '',
                tz TEXT NOT NULL DEFAULT '',
                created_at_utc TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'trial'
            );

            CREATE TABLE send_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                subscriber_key TEXT NOT NULL,
                ts_utc TEXT NOT NULL,
                variant TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL,
                run_id TEXT NOT NULL DEFAULT '',
                meta_json TEXT NOT NULL DEFAULT '{}'
            );
            CREATE INDEX idx_send_events_sub_ts
                ON send_events (subscriber_key, ts_utc);
            CREATE INDEX idx_send_events_sub_status_ts
                ON send_events (subscriber_key, status, ts_utc);

            CREATE TABLE trial_state (
                subscriber_key TEXT PRIMARY KEY,
                start_date TEXT NOT NULL DEFAULT '',
                sends_limit INTEGER,
                notified_at_utc TEXT,
                ended_at_utc TEXT
            );

            CREATE TABLE schema_version (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                version INTEGER NOT NULL,
                updated_at_utc TEXT NOT NULL
            );
            """
        )
        conn.execute(
            """
            INSERT INTO subscribers (subscriber_key, email, territory_code, tz, created_at_utc, status)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                "facs_trial",
                "taylor.thomas@facs.com",
                "FACS_TRIAL_STATES",
                "America/New_York",
                "2026-03-02T00:00:00+00:00",
                "trial",
            ),
        )
        conn.execute(
            """
            INSERT INTO trial_state (subscriber_key, start_date, sends_limit)
            VALUES (?, ?, ?)
            """,
            ("facs_trial", "2026-03-03", 14),
        )
        conn.execute(
            "INSERT INTO schema_version (id, version, updated_at_utc) VALUES (1, 7, ?)",
            ("2026-03-02T00:00:00+00:00",),
        )
        conn.commit()
    finally:
        conn.close()


class TestCrmLightSendEventsMigration(unittest.TestCase):
    def test_v7_send_events_upgrades_to_v8_with_recipient_column_and_index(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "crm_light.sqlite"
            _build_v7_style_crm_db(db_path)

            crm_light.ensure_database(db_path)

            with crm_light.open_conn(db_path) as conn:
                self.assertEqual(crm_light.get_schema_version(conn), 8)
                columns = {
                    str(row["name"]).strip().lower()
                    for row in conn.execute("PRAGMA table_info(send_events)").fetchall()
                }
                self.assertIn("recipient_email", columns)
                indexes = {
                    str(row["name"]).strip()
                    for row in conn.execute("PRAGMA index_list(send_events)").fetchall()
                }
                self.assertIn("idx_send_events_sub_recipient_status_ts", indexes)

    def test_run_trial_admin_show_and_trial_daily_print_config_succeed_after_upgrade(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            crm_db_path = tmp_path / "crm_light.sqlite"
            leads_db_path = tmp_path / "osha.sqlite"
            _build_v7_style_crm_db(crm_db_path)
            sqlite3.connect(str(leads_db_path)).close()

            show_out = io.StringIO()
            with redirect_stdout(show_out):
                show_code = run_trial_admin.main(
                    [
                        "show",
                        "--subscriber-key",
                        "facs_trial",
                        "--recent",
                        "5",
                        "--crm-db",
                        str(crm_db_path),
                    ]
                )
            self.assertEqual(show_code, 0, msg=show_out.getvalue())
            self.assertIn("subscriber_key=facs_trial", show_out.getvalue())

            config_out = io.StringIO()
            with redirect_stdout(config_out):
                config_code = run_trial_daily.main(
                    [
                        "--subscriber-key",
                        "facs_trial",
                        "--crm-db",
                        str(crm_db_path),
                        "--db",
                        str(leads_db_path),
                        "--print-config",
                    ]
                )
            self.assertEqual(config_code, 0, msg=config_out.getvalue())
            text = config_out.getvalue()
            self.assertIn("subscriber_key=facs_trial", text)
            self.assertIn("trial_effective_timezone=America/New_York", text)

    def test_last_sent_at_ignores_non_daily_variants(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "crm_light.sqlite"
            crm_light.ensure_database(db_path)

            with crm_light.open_conn(db_path) as conn:
                crm_light.init_schema(conn)
                crm_light.upsert_subscriber(
                    conn,
                    subscriber_key="trial_sub",
                    email="trial@example.com",
                    territory_code="TX_TRI",
                    tz="America/Chicago",
                    status="trial",
                )
                crm_light.append_send_event(
                    conn,
                    subscriber_key="trial_sub",
                    recipient_email="trial@example.com",
                    variant="DAILY",
                    status="SENT",
                    run_id="run-daily",
                    meta={},
                    ts_utc="2026-03-01T12:00:00+00:00",
                )
                crm_light.append_send_event(
                    conn,
                    subscriber_key="trial_sub",
                    recipient_email="trial@example.com",
                    variant="test_send_daily",
                    status="SENT",
                    run_id="run-smoke",
                    meta={"send_mode": "TEST"},
                    ts_utc="2026-03-01T12:30:00+00:00",
                )

                last_for_recipient = crm_light.get_last_sent_at_for_recipient(
                    conn,
                    "trial_sub",
                    "trial@example.com",
                )
                last_overall = crm_light.get_last_sent_at(conn, "trial_sub")

            self.assertEqual(last_for_recipient, "2026-03-01T12:00:00+00:00")
            self.assertEqual(last_overall, "2026-03-01T12:00:00+00:00")


if __name__ == "__main__":
    unittest.main()
