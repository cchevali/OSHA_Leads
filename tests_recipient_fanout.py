import csv
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent
SEND_SCRIPT = REPO_ROOT / "send_digest_email.py"
SCHEMA_FILE = REPO_ROOT / "schema.sql"


def init_db(db_path: Path) -> None:
    conn = sqlite3.connect(db_path)
    conn.executescript(SCHEMA_FILE.read_text(encoding="utf-8"))

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn.execute(
        """
        INSERT INTO inspections (
            activity_nr, date_opened, inspection_type, scope, case_status,
            establishment_name, site_city, site_state, site_zip,
            lead_score, first_seen_at, last_seen_at, parse_invalid, source_url
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?)
        """,
        (
            "900000001",
            datetime.now().strftime("%Y-%m-%d"),
            "Complaint",
            "Partial",
            "OPEN",
            "Fanout Test Co",
            "Austin",
            "TX",
            "78701",
            7,
            now,
            now,
            "https://example.com/lead/900000001",
        ),
    )
    conn.commit()
    conn.close()


def write_config(
    path: Path,
    recipients: list[str],
    *,
    customer_id: str = "fanout_test",
    subscriber_key: str | None = None,
    send_time_local: str | None = None,
    timezone_name: str | None = None,
    send_window_minutes: int | None = None,
    trial_target_local_hhmm: str | None = None,
    trial_catchup_max_minutes: int | None = None,
    allow_live_send: bool = True,
    pilot_mode: bool = False,
) -> None:
    config = {
        "customer_id": customer_id,
        "states": ["TX"],
        "opened_window_days": 14,
        "new_only_days": 1,
        "territory_code": "TX_TRIANGLE_V1",
        "content_filter": "high_medium",
        "include_low_fallback": True,
        "recipients": recipients,
        "email_recipients": recipients,
        "brand_name": "Acme Safety",
        "mailing_address": "123 Main St, Austin, TX 78701",
        "pilot_mode": pilot_mode,
        "allow_live_send": allow_live_send,
    }
    if subscriber_key:
        config["subscriber_key"] = subscriber_key
    if send_time_local:
        config["send_time_local"] = send_time_local
    if timezone_name:
        config["timezone"] = timezone_name
    if send_window_minutes is not None:
        config["send_window_minutes"] = int(send_window_minutes)
    if trial_target_local_hhmm is not None:
        config["trial_target_local_hhmm"] = trial_target_local_hhmm
    if trial_catchup_max_minutes is not None:
        config["trial_catchup_max_minutes"] = int(trial_catchup_max_minutes)
    path.write_text(json.dumps(config, indent=2) + "\n", encoding="utf-8")


def run_send(db_path: Path, config_path: Path, out_dir: Path, data_dir: Path, send_live: bool = True) -> subprocess.CompletedProcess:
    env = os.environ.copy()
    env["UNSUB_ENDPOINT_BASE"] = "https://example.com/unsubscribe"
    env["UNSUB_SECRET"] = "fanout-test-secret"
    env["DATA_DIR"] = str(data_dir)
    env["OSHA_SMOKE_TO"] = "cchevali+oshasmoke@gmail.com"

    cmd = [
        sys.executable,
        str(SEND_SCRIPT),
        "--db",
        str(db_path),
        "--customer",
        str(config_path),
        "--dry-run",
        "--disable-pilot-guard",
        "--output-dir",
        str(out_dir),
    ]
    if send_live:
        cmd.append("--send-live")
    return subprocess.run(cmd, cwd=REPO_ROOT, env=env, capture_output=True, text=True)


def insert_subscriber(
    db_path: Path,
    subscriber_key: str,
    email: str,
    recipients: list[str],
    *,
    send_enabled: int = 1,
    active: int = 1,
    territory_code: str = "TX_TRIANGLE_V1",
    content_filter: str = "high_medium",
    include_low_fallback: int = 1,
    send_time_local: str = "08:00",
    timezone_name: str = "UTC",
    customer_id: str = "fanout_test",
) -> None:
    conn = sqlite3.connect(db_path)
    today = datetime.now().date().isoformat()
    conn.execute(
        """
        INSERT INTO subscribers (
            subscriber_key, display_name, email, recipients_json, territory_code,
            content_filter, include_low_fallback, trial_length_days, trial_started_at,
            trial_ends_at, active, send_enabled, send_time_local, timezone, customer_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            subscriber_key,
            "Fanout Test",
            email,
            json.dumps(recipients),
            territory_code,
            content_filter,
            include_low_fallback,
            14,
            today,
            None,
            active,
            send_enabled,
            send_time_local,
            timezone_name,
            customer_id,
        ),
    )
    conn.commit()
    conn.close()


def set_subscriber_last_sent_at(db_path: Path, subscriber_key: str, last_sent_at: str) -> None:
    conn = sqlite3.connect(db_path)
    conn.execute(
        "UPDATE subscribers SET last_sent_at = ? WHERE subscriber_key = ?",
        (last_sent_at, subscriber_key),
    )
    conn.commit()
    conn.close()


class TestRecipientFanout(unittest.TestCase):
    def test_multi_recipient_distinct_tokens_and_logs(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            db_path = tmp_path / "fanout.sqlite"
            config_path = tmp_path / "customer.json"
            out_dir = tmp_path / "out"
            data_dir = tmp_path / "data"
            out_dir.mkdir(parents=True, exist_ok=True)
            data_dir.mkdir(parents=True, exist_ok=True)

            recipients = ["wgs@indigocompliance.com", "brandon@indigoenergyservices.com"]
            init_db(db_path)
            write_config(config_path, recipients)

            result = run_send(db_path, config_path, out_dir, data_dir)
            self.assertEqual(result.returncode, 0, msg=result.stderr)

            with (out_dir / "email_log.csv").open("r", encoding="utf-8") as f:
                email_log = list(csv.DictReader(f))
            self.assertEqual(len(email_log), 2)
            self.assertEqual({row["recipient"] for row in email_log}, set(recipients))
            self.assertTrue(all(row["status"] == "dry_run" for row in email_log))

            with (data_dir / "unsub_tokens.csv").open("r", encoding="utf-8") as f:
                token_rows = list(csv.DictReader(f))
            self.assertEqual(len(token_rows), 2)
            self.assertEqual({row["email"] for row in token_rows}, set(recipients))
            token_ids = {row["token_id"] for row in token_rows}
            self.assertEqual(len(token_ids), 2)

    def test_daily_new_since_last_send_excludes_reobserved_same_lead(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            db_path = tmp_path / "fanout.sqlite"
            config_path = tmp_path / "customer.json"
            out_dir = tmp_path / "out"
            data_dir = tmp_path / "data"
            out_dir.mkdir(parents=True, exist_ok=True)
            data_dir.mkdir(parents=True, exist_ok=True)

            recipients = ["wgs@indigocompliance.com"]
            init_db(db_path)
            write_config(
                config_path,
                recipients,
                customer_id="fanout_test",
                subscriber_key="fanout_sub",
            )
            insert_subscriber(
                db_path,
                subscriber_key="fanout_sub",
                email=recipients[0],
                recipients=recipients,
                customer_id="fanout_test",
            )

            t1 = "2026-02-11T09:00:00+00:00"
            t2 = "2026-02-11T11:00:00+00:00"
            t0 = "2026-02-11T08:59:59+00:00"
            conn = sqlite3.connect(db_path)
            conn.execute(
                """
                UPDATE inspections
                SET first_seen_at = ?, last_seen_at = ?, changed_at = ?, establishment_name = ?, site_state = ?
                WHERE activity_nr = ?
                """,
                (t1, t1, t1, "Regression Co", "TX", "900000001"),
            )
            conn.commit()
            conn.close()

            set_subscriber_last_sent_at(db_path, "fanout_sub", t0)
            first_run = run_send(db_path, config_path, out_dir, data_dir, send_live=True)
            self.assertEqual(first_run.returncode, 0, msg=first_run.stderr)
            self.assertIn("Leads after filters:      1", first_run.stdout)
            self.assertIn("name=Regression Co", first_run.stdout)

            conn = sqlite3.connect(db_path)
            conn.execute(
                "UPDATE inspections SET last_seen_at = ?, changed_at = ? WHERE activity_nr = ?",
                (t2, t2, "900000001"),
            )
            conn.commit()
            conn.close()

            set_subscriber_last_sent_at(db_path, "fanout_sub", t1)
            second_run = run_send(db_path, config_path, out_dir, data_dir, send_live=True)
            self.assertEqual(second_run.returncode, 0, msg=second_run.stderr)
            self.assertIn("Leads after filters:      0", second_run.stdout)
            self.assertNotIn("name=Regression Co", second_run.stdout)

    def test_suppressed_recipient_does_not_block_other(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            db_path = tmp_path / "fanout.sqlite"
            config_path = tmp_path / "customer.json"
            out_dir = tmp_path / "out"
            data_dir = tmp_path / "data"
            out_dir.mkdir(parents=True, exist_ok=True)
            data_dir.mkdir(parents=True, exist_ok=True)

            recipients = ["wgs@indigocompliance.com", "brandon@indigoenergyservices.com"]
            init_db(db_path)
            write_config(config_path, recipients)

            conn = sqlite3.connect(db_path)
            conn.execute(
                "INSERT INTO suppression_list (email_or_domain, reason) VALUES (?, ?)",
                ("wgs@indigocompliance.com", "manual opt-out"),
            )
            conn.commit()
            conn.close()

            result = run_send(db_path, config_path, out_dir, data_dir)
            self.assertEqual(result.returncode, 0, msg=result.stderr)

            with (out_dir / "email_log.csv").open("r", encoding="utf-8") as f:
                email_log = list(csv.DictReader(f))
            self.assertEqual(len(email_log), 2)

            by_recipient = {row["recipient"]: row for row in email_log}
            self.assertEqual(by_recipient["wgs@indigocompliance.com"]["status"], "suppressed")
            self.assertEqual(by_recipient["brandon@indigoenergyservices.com"]["status"], "dry_run")

            with (data_dir / "unsub_tokens.csv").open("r", encoding="utf-8") as f:
                token_rows = list(csv.DictReader(f))
            self.assertEqual(len(token_rows), 1)
            self.assertEqual(token_rows[0]["email"], "brandon@indigoenergyservices.com")

            with (out_dir / "unsubscribe_events.csv").open("r", encoding="utf-8") as f:
                unsub_events = list(csv.DictReader(f))
            self.assertEqual(len(unsub_events), 1)
            self.assertEqual(unsub_events[0]["email"], "wgs@indigocompliance.com")

    def test_safe_mode_forces_admin_recipient(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            db_path = tmp_path / "fanout.sqlite"
            config_path = tmp_path / "customer.json"
            out_dir = tmp_path / "out"
            data_dir = tmp_path / "data"
            out_dir.mkdir(parents=True, exist_ok=True)
            data_dir.mkdir(parents=True, exist_ok=True)

            recipients = ["wgs@indigocompliance.com", "brandon@indigoenergyservices.com"]
            init_db(db_path)

            # allow_live_send omitted -> safe mode
            config = {
                "customer_id": "fanout_test",
                "states": ["TX"],
                "opened_window_days": 14,
                "new_only_days": 1,
                "territory_code": "TX_TRIANGLE_V1",
                "content_filter": "high_medium",
                "include_low_fallback": True,
                "recipients": recipients,
                "email_recipients": recipients,
                "brand_name": "Acme Safety",
                "mailing_address": "123 Main St, Austin, TX 78701",
                "pilot_mode": False,
            }
            config_path.write_text(json.dumps(config, indent=2) + "\n", encoding="utf-8")

            result = run_send(db_path, config_path, out_dir, data_dir, send_live=False)
            self.assertEqual(result.returncode, 0, msg=result.stderr)
            self.assertIn("[SAFE_MODE]", result.stdout)

            with (out_dir / "email_log.csv").open("r", encoding="utf-8") as f:
                email_log = list(csv.DictReader(f))
            self.assertEqual(len(email_log), 1)
            self.assertEqual(email_log[0]["recipient"], "cchevali+oshasmoke@gmail.com")

    def test_missing_send_live_and_live_with_window(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            db_path = tmp_path / "fanout.sqlite"
            config_path = tmp_path / "customer.json"
            out_safe = tmp_path / "out_safe"
            out_live = tmp_path / "out_live"
            data_safe = tmp_path / "data_safe"
            data_live = tmp_path / "data_live"
            out_safe.mkdir(parents=True, exist_ok=True)
            out_live.mkdir(parents=True, exist_ok=True)
            data_safe.mkdir(parents=True, exist_ok=True)
            data_live.mkdir(parents=True, exist_ok=True)

            recipients = ["wgs@indigocompliance.com", "brandon@indigoenergyservices.com"]
            init_db(db_path)

            now_utc = datetime.now(timezone.utc)
            send_time_local = now_utc.strftime("%H:%M")

            write_config(
                config_path,
                recipients,
                subscriber_key="fanout_sub",
                send_time_local=send_time_local,
                timezone_name="UTC",
                send_window_minutes=60,
                allow_live_send=True,
                pilot_mode=False,
            )
            insert_subscriber(
                db_path,
                subscriber_key="fanout_sub",
                email=recipients[0],
                recipients=recipients,
                send_enabled=1,
                active=1,
                send_time_local=send_time_local,
                timezone_name="UTC",
            )

            result_safe = run_send(db_path, config_path, out_safe, data_safe, send_live=False)
            self.assertEqual(result_safe.returncode, 0, msg=result_safe.stderr)
            self.assertIn("SEND_START mode=SAFE", result_safe.stdout)
            self.assertIn("gate=missing --send-live", result_safe.stdout)

            with (out_safe / "email_log.csv").open("r", encoding="utf-8") as f:
                email_log_safe = list(csv.DictReader(f))
            self.assertEqual(len(email_log_safe), 1)
            self.assertEqual(email_log_safe[0]["recipient"], "cchevali+oshasmoke@gmail.com")

            result_live = run_send(db_path, config_path, out_live, data_live, send_live=True)
            self.assertEqual(result_live.returncode, 0, msg=result_live.stderr)
            self.assertIn("SEND_START mode=LIVE", result_live.stdout)
            self.assertNotIn("[SAFE_MODE]", result_live.stdout)

            with (out_live / "email_log.csv").open("r", encoding="utf-8") as f:
                email_log_live = list(csv.DictReader(f))
            self.assertEqual(len(email_log_live), 2)
            self.assertEqual({row["recipient"] for row in email_log_live}, set(recipients))
            self.assertTrue(all(row["status"] == "dry_run" for row in email_log_live))

    def test_trial_catchup_allows_live_send_after_strict_window(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            db_path = tmp_path / "fanout.sqlite"
            config_path = tmp_path / "customer.json"
            out_dir = tmp_path / "out"
            data_dir = tmp_path / "data"
            out_dir.mkdir(parents=True, exist_ok=True)
            data_dir.mkdir(parents=True, exist_ok=True)

            recipients = ["wgs@indigocompliance.com", "brandon@indigoenergyservices.com"]
            init_db(db_path)

            now_utc = datetime.now(timezone.utc)
            strict_window_time = (now_utc + timedelta(hours=12)).strftime("%H:%M")
            trial_target = now_utc.strftime("%H:%M")

            write_config(
                config_path,
                recipients,
                customer_id="wally_trial_tx_triangle_v1",
                subscriber_key="wally_trial",
                send_time_local=strict_window_time,
                timezone_name="UTC",
                send_window_minutes=20,
                trial_target_local_hhmm=trial_target,
                trial_catchup_max_minutes=180,
                allow_live_send=True,
                pilot_mode=False,
            )
            insert_subscriber(
                db_path,
                subscriber_key="wally_trial",
                email=recipients[0],
                recipients=recipients,
                send_enabled=1,
                active=1,
                send_time_local=strict_window_time,
                timezone_name="UTC",
                customer_id="wally_trial_tx_triangle_v1",
            )

            result = run_send(db_path, config_path, out_dir, data_dir, send_live=True)
            self.assertEqual(result.returncode, 0, msg=result.stderr)
            self.assertIn("SAFE_MODE_CATCHUP_ALLOWED gate=outside send window", result.stdout)
            self.assertIn("SEND_START mode=LIVE", result.stdout)
            self.assertNotIn("[SAFE_MODE] forced admin recipient", result.stdout)

            with (out_dir / "email_log.csv").open("r", encoding="utf-8") as f:
                email_log = list(csv.DictReader(f))
            self.assertEqual(len(email_log), 2)
            self.assertEqual({row["recipient"] for row in email_log}, set(recipients))

    def test_trial_catchup_denied_when_already_sent_today(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            db_path = tmp_path / "fanout.sqlite"
            config_path = tmp_path / "customer.json"
            out_dir = tmp_path / "out"
            data_dir = tmp_path / "data"
            out_dir.mkdir(parents=True, exist_ok=True)
            data_dir.mkdir(parents=True, exist_ok=True)

            recipients = ["wgs@indigocompliance.com", "brandon@indigoenergyservices.com"]
            init_db(db_path)

            now_utc = datetime.now(timezone.utc)
            strict_window_time = (now_utc + timedelta(hours=12)).strftime("%H:%M")
            trial_target = now_utc.strftime("%H:%M")

            write_config(
                config_path,
                recipients,
                customer_id="wally_trial_tx_triangle_v1",
                subscriber_key="wally_trial",
                send_time_local=strict_window_time,
                timezone_name="UTC",
                send_window_minutes=20,
                trial_target_local_hhmm=trial_target,
                trial_catchup_max_minutes=180,
                allow_live_send=True,
                pilot_mode=False,
            )
            insert_subscriber(
                db_path,
                subscriber_key="wally_trial",
                email=recipients[0],
                recipients=recipients,
                send_enabled=1,
                active=1,
                send_time_local=strict_window_time,
                timezone_name="UTC",
                customer_id="wally_trial_tx_triangle_v1",
            )
            set_subscriber_last_sent_at(db_path, "wally_trial", now_utc.isoformat())

            result = run_send(db_path, config_path, out_dir, data_dir, send_live=True)
            self.assertEqual(result.returncode, 0, msg=result.stderr)
            self.assertNotIn("SAFE_MODE_CATCHUP_ALLOWED", result.stdout)
            self.assertIn("SEND_START mode=LIVE", result.stdout)

            with (out_dir / "email_log.csv").open("r", encoding="utf-8") as f:
                email_log = list(csv.DictReader(f))
            self.assertEqual(len(email_log), 2)
            self.assertEqual({row["recipient"] for row in email_log}, set(recipients))

    def test_trial_catchup_denied_after_catchup_end(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            db_path = tmp_path / "fanout.sqlite"
            config_path = tmp_path / "customer.json"
            out_dir = tmp_path / "out"
            data_dir = tmp_path / "data"
            out_dir.mkdir(parents=True, exist_ok=True)
            data_dir.mkdir(parents=True, exist_ok=True)

            recipients = ["wgs@indigocompliance.com", "brandon@indigoenergyservices.com"]
            init_db(db_path)

            now_utc = datetime.now(timezone.utc)
            strict_window_time = (now_utc + timedelta(hours=12)).strftime("%H:%M")
            trial_target = (now_utc - timedelta(minutes=1)).strftime("%H:%M")

            write_config(
                config_path,
                recipients,
                customer_id="wally_trial_tx_triangle_v1",
                subscriber_key="wally_trial",
                send_time_local=strict_window_time,
                timezone_name="UTC",
                send_window_minutes=20,
                trial_target_local_hhmm=trial_target,
                trial_catchup_max_minutes=0,
                allow_live_send=True,
                pilot_mode=False,
            )
            insert_subscriber(
                db_path,
                subscriber_key="wally_trial",
                email=recipients[0],
                recipients=recipients,
                send_enabled=1,
                active=1,
                send_time_local=strict_window_time,
                timezone_name="UTC",
                customer_id="wally_trial_tx_triangle_v1",
            )

            result = run_send(db_path, config_path, out_dir, data_dir, send_live=True)
            self.assertEqual(result.returncode, 0, msg=result.stderr)
            self.assertNotIn("SAFE_MODE_CATCHUP_ALLOWED", result.stdout)
            self.assertIn("SEND_START mode=LIVE", result.stdout)

            with (out_dir / "email_log.csv").open("r", encoding="utf-8") as f:
                email_log = list(csv.DictReader(f))
            self.assertEqual(len(email_log), 2)
            self.assertEqual({row["recipient"] for row in email_log}, set(recipients))

    def test_non_trial_stays_strict_window_even_with_trial_keys(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            db_path = tmp_path / "fanout.sqlite"
            config_path = tmp_path / "customer.json"
            out_dir = tmp_path / "out"
            data_dir = tmp_path / "data"
            out_dir.mkdir(parents=True, exist_ok=True)
            data_dir.mkdir(parents=True, exist_ok=True)

            recipients = ["wgs@indigocompliance.com", "brandon@indigoenergyservices.com"]
            init_db(db_path)

            now_utc = datetime.now(timezone.utc)
            strict_window_time = (now_utc + timedelta(hours=12)).strftime("%H:%M")
            trial_target = now_utc.strftime("%H:%M")

            write_config(
                config_path,
                recipients,
                customer_id="fanout_test",
                subscriber_key="fanout_sub",
                send_time_local=strict_window_time,
                timezone_name="UTC",
                send_window_minutes=20,
                trial_target_local_hhmm=trial_target,
                trial_catchup_max_minutes=180,
                allow_live_send=True,
                pilot_mode=False,
            )
            insert_subscriber(
                db_path,
                subscriber_key="fanout_sub",
                email=recipients[0],
                recipients=recipients,
                send_enabled=1,
                active=1,
                send_time_local=strict_window_time,
                timezone_name="UTC",
                customer_id="fanout_test",
            )

            result = run_send(db_path, config_path, out_dir, data_dir, send_live=True)
            self.assertEqual(result.returncode, 0, msg=result.stderr)
            self.assertNotIn("SAFE_MODE_CATCHUP_ALLOWED", result.stdout)
            self.assertIn("SEND_START mode=LIVE", result.stdout)

            with (out_dir / "email_log.csv").open("r", encoding="utf-8") as f:
                email_log = list(csv.DictReader(f))
            self.assertEqual(len(email_log), 2)
            self.assertEqual({row["recipient"] for row in email_log}, set(recipients))


if __name__ == "__main__":
    unittest.main()
