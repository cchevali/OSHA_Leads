import csv
import csv
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from datetime import datetime
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


def write_config(path: Path, recipients: list[str]) -> None:
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
        "allow_live_send": True,
    }
    path.write_text(json.dumps(config, indent=2) + "\n", encoding="utf-8")


def run_send(db_path: Path, config_path: Path, out_dir: Path, data_dir: Path, send_live: bool = True) -> subprocess.CompletedProcess:
    env = os.environ.copy()
    env["UNSUB_ENDPOINT_BASE"] = "https://example.com/unsubscribe"
    env["UNSUB_SECRET"] = "fanout-test-secret"
    env["DATA_DIR"] = str(data_dir)
    env["CHASE_EMAIL"] = "cchevali@gmail.com"

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
            self.assertEqual(email_log[0]["recipient"], "cchevali@gmail.com")


if __name__ == "__main__":
    unittest.main()
