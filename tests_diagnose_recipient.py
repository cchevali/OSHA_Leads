import io
import json
import os
import sqlite3
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path

import crm_light
from outreach import crm_store
from tools import diagnose_recipient as dr


def _parse_tokens(text: str) -> dict[str, str]:
    tokens: dict[str, str] = {}
    for line in text.splitlines():
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        tokens[key.strip()] = value.strip()
    return tokens


def _seed_crm_light_subscriber(crm_light_db: Path, subscriber_key: str, email: str) -> None:
    crm_light.ensure_database(crm_light_db)
    with crm_light.open_conn(crm_light_db) as conn:
        crm_light.init_schema(conn)
        conn.execute(
            """
            INSERT OR REPLACE INTO subscribers (
                subscriber_key, email, territory_code, tz, created_at_utc, status
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                crm_light.normalize_subscriber_key(subscriber_key),
                crm_light.normalize_email(email),
                "TX_TRI",
                "UTC",
                "2026-02-26T00:00:00Z",
                "trial",
            ),
        )
        conn.commit()


class TestDiagnoseRecipient(unittest.TestCase):
    def test_print_config_is_side_effect_free(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            crm_light_db = tmp / "crm_light.sqlite"
            crm_db = tmp / "crm.sqlite"
            data_dir = tmp / "out"

            out = io.StringIO()
            err = io.StringIO()
            with redirect_stdout(out), redirect_stderr(err):
                rc = dr.main(
                    [
                        "--email",
                        "brandon@example.com",
                        "--subscriber-key",
                        "wally_trial",
                        "--crm-light-db",
                        str(crm_light_db),
                        "--crm-db",
                        str(crm_db),
                        "--data-dir",
                        str(data_dir),
                        "--print-config",
                    ]
                )

            self.assertEqual(rc, 0, msg=err.getvalue())
            text = out.getvalue()
            tokens = _parse_tokens(text)
            self.assertEqual(tokens["DIAGNOSE_RECIPIENT_COMPLETE"], "status=PRINT_CONFIG")
            self.assertEqual(tokens["DIAG_EMAIL"], "brandon@example.com")
            self.assertEqual(tokens["DIAG_SUBSCRIBER_KEY"], "wally_trial")
            self.assertFalse(crm_light_db.exists())
            self.assertFalse(crm_db.exists())
            self.assertFalse(data_dir.exists())

    def test_entitlement_recipients_json_sets_in_entitlement_token(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            crm_light_db = tmp / "crm_light.sqlite"
            crm_db = tmp / "crm.sqlite"
            data_dir = tmp / "out"
            data_dir.mkdir(parents=True, exist_ok=True)

            _seed_crm_light_subscriber(crm_light_db, "wally_trial", "wgs@indigocompliance.com")
            with crm_light.open_conn(crm_light_db) as conn:
                crm_light.upsert_subscriber_entitlement(
                    conn,
                    subscriber_key="wally_trial",
                    email="wgs@indigocompliance.com",
                    plan_code="core",
                    max_metros=4,
                    active=True,
                    source="test",
                    recipients=[
                        {"email": "wgs@indigocompliance.com"},
                        {"email": "brandon@example.com"},
                    ],
                )

            out = io.StringIO()
            err = io.StringIO()
            with redirect_stdout(out), redirect_stderr(err):
                rc = dr.main(
                    [
                        "--email",
                        "brandon@example.com",
                        "--subscriber-key",
                        "wally_trial",
                        "--crm-light-db",
                        str(crm_light_db),
                        "--crm-db",
                        str(crm_db),
                        "--data-dir",
                        str(data_dir),
                    ]
                )

            self.assertEqual(rc, 0, msg=err.getvalue())
            tokens = _parse_tokens(out.getvalue())
            self.assertEqual(tokens["DIAG_RECIPIENT_IN_ENTITLEMENT"], "1")
            self.assertEqual(tokens["DIAG_RECIPIENT_LAST_SENT"], "NEVER")
            self.assertEqual(tokens["DIAG_RECIPIENT_LAST_SKIP_REASON"], "NONE")

    def test_send_events_meta_json_recipient_matching_without_recipient_email_column(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            crm_light_db = tmp / "crm_light.sqlite"
            crm_db = tmp / "crm.sqlite"
            data_dir = tmp / "out"
            data_dir.mkdir(parents=True, exist_ok=True)

            _seed_crm_light_subscriber(crm_light_db, "wally_trial", "wgs@indigocompliance.com")
            with crm_light.open_conn(crm_light_db) as conn:
                crm_light.append_send_event(
                    conn,
                    "wally_trial",
                    "DAILY",
                    "SKIP_SUPPRESSED",
                    "r1",
                    {"recipient": "brandon@example.com"},
                    "2026-02-24T15:01:00Z",
                )
                crm_light.append_send_event(
                    conn,
                    "wally_trial",
                    "DAILY",
                    "SENT",
                    "r2",
                    {"primary_recipient": "brandon@example.com"},
                    "2026-02-25T15:01:00Z",
                )
                crm_light.append_send_event(
                    conn,
                    "wally_trial",
                    "DAILY",
                    "SENT",
                    "r3",
                    {"to": "someoneelse@example.com"},
                    "2026-02-26T15:01:00Z",
                )

            out = io.StringIO()
            err = io.StringIO()
            with redirect_stdout(out), redirect_stderr(err):
                rc = dr.main(
                    [
                        "--email",
                        "brandon@example.com",
                        "--subscriber-key",
                        "wally_trial",
                        "--crm-light-db",
                        str(crm_light_db),
                        "--crm-db",
                        str(crm_db),
                        "--data-dir",
                        str(data_dir),
                    ]
                )

            self.assertEqual(rc, 0, msg=err.getvalue())
            tokens = _parse_tokens(out.getvalue())
            self.assertEqual(tokens["DIAG_RECIPIENT_LAST_SENT"], "2026-02-25T15:01:00Z")
            self.assertEqual(tokens["DIAG_RECIPIENT_LAST_SKIP_REASON"], "SKIP_SUPPRESSED")
            self.assertEqual(tokens["DIAG_RECIPIENT_IN_ENTITLEMENT"], "0")

    def test_suppression_bounce_and_unsubscribe_evidence_set_tokens(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            crm_light_db = tmp / "crm_light.sqlite"
            crm_db = tmp / "crm.sqlite"
            data_dir = tmp / "out"
            data_dir.mkdir(parents=True, exist_ok=True)

            target_email = "brandon@example.com"
            _seed_crm_light_subscriber(crm_light_db, "wally_trial", "wgs@indigocompliance.com")

            crm_store.ensure_database(crm_db)
            conn = sqlite3.connect(str(crm_db))
            try:
                conn.execute(
                    "INSERT INTO suppression (email, reason, ts) VALUES (?, ?, ?)",
                    (target_email, "manual", "2026-02-26T10:00:00Z"),
                )
                conn.execute(
                    """
                    INSERT INTO bounce_events (
                        created_at_utc, recipient_email, bounce_class, smtp_status, smtp_code,
                        diagnostic_code, final_recipient, original_to, source, subject,
                        source_message_id, source_uid_fingerprint, metadata_json, prospect_id
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "2026-02-26T10:01:00Z",
                        target_email,
                        "hard",
                        "5.1.1",
                        "550",
                        "user unknown",
                        target_email,
                        target_email,
                        "imap_dsn",
                        "Undeliverable",
                        "mid-1",
                        "uidfp-1",
                        "{}",
                        "",
                    ),
                )
                conn.commit()
            finally:
                conn.close()

            (data_dir / "suppression.csv").write_text(
                "email,reason,source\n"
                f"{target_email},one_click_unsubscribe,footer_link\n",
                encoding="utf-8",
            )
            (data_dir / "unsub_tokens.csv").write_text(
                "email,token_id,created_at_utc\n"
                f"{target_email},tok123,2026-02-25T00:00:00Z\n",
                encoding="utf-8",
            )
            (data_dir / "unsubscribe_events.csv").write_text(
                "email,reason,source,created_at_utc\n"
                f"{target_email},unsubscribe,one_click,2026-02-26T09:59:00Z\n",
                encoding="utf-8",
            )
            (data_dir / "bounce_import_state.json").write_text(
                json.dumps({"last_checked_utc": "2026-02-26T10:02:00Z"}),
                encoding="utf-8",
            )

            out = io.StringIO()
            err = io.StringIO()
            with redirect_stdout(out), redirect_stderr(err):
                rc = dr.main(
                    [
                        "--email",
                        target_email,
                        "--subscriber-key",
                        "wally_trial",
                        "--crm-light-db",
                        str(crm_light_db),
                        "--crm-db",
                        str(crm_db),
                        "--data-dir",
                        str(data_dir),
                    ]
                )

            self.assertEqual(rc, 0, msg=err.getvalue())
            tokens = _parse_tokens(out.getvalue())
            self.assertEqual(tokens["DIAG_RECIPIENT_SUPPRESSED"], "1")
            self.assertEqual(tokens["DIAG_RECIPIENT_BOUNCED"], "1")
            self.assertEqual(tokens["DIAG_RECIPIENT_UNSUBSCRIBED"], "1")
            self.assertEqual(tokens["DIAGNOSE_RECIPIENT_COMPLETE"], "status=OK")


if __name__ == "__main__":
    unittest.main()
