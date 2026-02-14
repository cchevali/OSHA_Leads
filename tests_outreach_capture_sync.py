import csv
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from unittest import mock
from datetime import datetime, timedelta, timezone
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent
CAPTURE_SCRIPT = REPO_ROOT / "outreach" / "capture_sync.py"
OPS_REPORT_SCRIPT = REPO_ROOT / "outreach" / "ops_report.py"

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from outreach import crm_store
from outreach import capture_sync


def _ts_hours_ago(hours: int) -> str:
    return (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()


def _seed_prospect(conn: sqlite3.Connection, prospect_id: str, email: str, state: str = "TX") -> None:
    conn.execute(
        """
        INSERT INTO prospects(
            prospect_id, firm, contact_name, email, title, city, state, website, source,
            score, status, created_at, last_contacted_at
        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            prospect_id,
            "Firm",
            "Contact",
            email,
            "Owner",
            "Austin",
            state,
            "https://example.com",
            "test",
            9,
            "new",
            _ts_hours_ago(48),
            None,
        ),
    )


def _seed_sent_event(conn: sqlite3.Connection, prospect_id: str, message_id: str, batch_id: str = "2026-02-13_TX") -> int:
    conn.execute(
        """
        INSERT INTO outreach_events(
            prospect_id,
            ts,
            event_type,
            batch_id,
            metadata_json,
            attributed_send_event_id,
            attributed_batch_id,
            attributed_state_at_send,
            attributed_model
        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            prospect_id,
            _ts_hours_ago(12),
            "sent",
            batch_id,
            json.dumps({"message_id": message_id, "email": "owner@example.com", "state": "TX"}, separators=(",", ":")),
            None,
            "",
            "",
            "",
        ),
    )
    row = conn.execute("SELECT last_insert_rowid()").fetchone()
    return int(row[0])


def _write_triage(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["timestamp", "message_id", "from_email", "subject", "category", "action"])
        w.writeheader()
        for row in rows:
            w.writerow(row)


def _write_suppression(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["email", "reason", "source", "timestamp", "evidence_msg_id"])
        w.writeheader()
        for row in rows:
            w.writerow(row)


class TestCaptureSync(unittest.TestCase):
    def _run(self, script: Path, args: list[str], data_dir: Path) -> subprocess.CompletedProcess:
        env = os.environ.copy()
        env["PYTHONPATH"] = str(REPO_ROOT)
        env["DATA_DIR"] = str(data_dir)
        return subprocess.run(
            [sys.executable, str(script)] + args,
            cwd=str(REPO_ROOT),
            env=env,
            capture_output=True,
            text=True,
        )

    def test_reply_message_id_links_to_sent_and_marks_replied(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            data_dir = tmp / "data"
            triage = tmp / "triage.csv"
            suppression = tmp / "suppression.csv"

            db_path = crm_store.ensure_database(data_dir / "crm.sqlite")
            conn = crm_store.connect(db_path)
            try:
                crm_store.init_schema(conn)
                _seed_prospect(conn, "p1", "owner@example.com")
                sent_event_id = _seed_sent_event(conn, "p1", "<msg-1>")
                conn.commit()
            finally:
                conn.close()

            _write_triage(
                triage,
                [
                    {
                        "timestamp": _ts_hours_ago(1),
                        "message_id": "<msg-1>",
                        "from_email": "owner@example.com",
                        "subject": "Re: hello",
                        "category": "hot_interest",
                        "action": "notified+draft",
                    }
                ],
            )
            _write_suppression(suppression, [])

            proc = self._run(
                CAPTURE_SCRIPT,
                ["--triage-log", str(triage), "--suppression-csv", str(suppression)],
                data_dir,
            )
            out = (proc.stdout or "") + "\n" + (proc.stderr or "")
            self.assertEqual(proc.returncode, 0, msg=out)
            self.assertIn("PASS_CAPTURE_SYNC_APPLY", out)

            conn = sqlite3.connect(str(db_path))
            conn.row_factory = sqlite3.Row
            try:
                status = conn.execute("SELECT status FROM prospects WHERE prospect_id = 'p1'").fetchone()[0]
                self.assertEqual(status, "replied")
                row = conn.execute(
                    """
                    SELECT event_type, attributed_send_event_id, attributed_batch_id, attributed_state_at_send, metadata_json
                    FROM outreach_events
                    WHERE prospect_id = 'p1' AND event_type = 'replied'
                    ORDER BY event_id DESC
                    LIMIT 1
                    """
                ).fetchone()
                self.assertIsNotNone(row)
                self.assertEqual(row["event_type"], "replied")
                self.assertEqual(int(row["attributed_send_event_id"] or 0), sent_event_id)
                self.assertEqual((row["attributed_batch_id"] or ""), "2026-02-13_TX")
                self.assertEqual((row["attributed_state_at_send"] or ""), "TX")
                meta = json.loads(row["metadata_json"])
                self.assertEqual(meta.get("source"), "capture_sync")
            finally:
                conn.close()

    def test_unsubscribe_marks_do_not_contact_and_upserts_suppression(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            data_dir = tmp / "data"
            triage = tmp / "triage.csv"
            suppression = tmp / "suppression.csv"

            db_path = crm_store.ensure_database(data_dir / "crm.sqlite")
            conn = crm_store.connect(db_path)
            try:
                crm_store.init_schema(conn)
                _seed_prospect(conn, "p1", "owner@example.com")
                _seed_sent_event(conn, "p1", "<msg-2>")
                conn.commit()
            finally:
                conn.close()

            _write_triage(
                triage,
                [
                    {
                        "timestamp": _ts_hours_ago(1),
                        "message_id": "<msg-2>",
                        "from_email": "support@microflowops.com",
                        "subject": "unsubscribe",
                        "category": "unsubscribe",
                        "action": "suppressed",
                    }
                ],
            )
            _write_suppression(
                suppression,
                [
                    {
                        "email": "owner@example.com",
                        "reason": "unsubscribe",
                        "source": "inbound_triage",
                        "timestamp": _ts_hours_ago(1),
                        "evidence_msg_id": "<msg-2>",
                    }
                ],
            )

            proc = self._run(
                CAPTURE_SCRIPT,
                ["--triage-log", str(triage), "--suppression-csv", str(suppression)],
                data_dir,
            )
            out = (proc.stdout or "") + "\n" + (proc.stderr or "")
            self.assertEqual(proc.returncode, 0, msg=out)
            self.assertIn("PASS_CAPTURE_SYNC_APPLY", out)

            conn = sqlite3.connect(str(db_path))
            conn.row_factory = sqlite3.Row
            try:
                status = conn.execute("SELECT status FROM prospects WHERE prospect_id = 'p1'").fetchone()[0]
                self.assertEqual(status, "do_not_contact")
                sup = conn.execute("SELECT reason FROM suppression WHERE email = 'owner@example.com'").fetchone()
                self.assertIsNotNone(sup)
                self.assertEqual((sup["reason"] or ""), "do_not_contact")
            finally:
                conn.close()

    def test_idempotent_capture_key_prevents_duplicate_writes(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            data_dir = tmp / "data"
            triage = tmp / "triage.csv"
            suppression = tmp / "suppression.csv"

            db_path = crm_store.ensure_database(data_dir / "crm.sqlite")
            conn = crm_store.connect(db_path)
            try:
                crm_store.init_schema(conn)
                _seed_prospect(conn, "p1", "owner@example.com")
                _seed_sent_event(conn, "p1", "<msg-3>")
                conn.commit()
            finally:
                conn.close()

            _write_triage(
                triage,
                [
                    {
                        "timestamp": _ts_hours_ago(1),
                        "message_id": "<msg-3>",
                        "from_email": "owner@example.com",
                        "subject": "Re: hello",
                        "category": "question",
                        "action": "notified+draft",
                    }
                ],
            )
            _write_suppression(suppression, [])

            first = self._run(
                CAPTURE_SCRIPT,
                ["--triage-log", str(triage), "--suppression-csv", str(suppression)],
                data_dir,
            )
            second = self._run(
                CAPTURE_SCRIPT,
                ["--triage-log", str(triage), "--suppression-csv", str(suppression)],
                data_dir,
            )
            out1 = (first.stdout or "") + "\n" + (first.stderr or "")
            out2 = (second.stdout or "") + "\n" + (second.stderr or "")
            self.assertEqual(first.returncode, 0, msg=out1)
            self.assertEqual(second.returncode, 0, msg=out2)

            conn = sqlite3.connect(str(db_path))
            try:
                count = int(
                    conn.execute(
                        """
                        SELECT COUNT(*)
                        FROM outreach_events
                        WHERE event_type = 'replied' AND metadata_json LIKE '%\"source\":\"capture_sync\"%'
                        """
                    ).fetchone()[0]
                )
                self.assertEqual(count, 1)
            finally:
                conn.close()

    def test_ops_report_reflects_nonzero_replied_after_capture_sync(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            data_dir = tmp / "data"
            triage = tmp / "triage.csv"
            suppression = tmp / "suppression.csv"
            data_dir.mkdir(parents=True, exist_ok=True)

            db_path = crm_store.ensure_database(data_dir / "crm.sqlite")
            conn = crm_store.connect(db_path)
            try:
                crm_store.init_schema(conn)
                _seed_prospect(conn, "p1", "owner@example.com")
                _seed_sent_event(conn, "p1", "<msg-4>")
                conn.commit()
            finally:
                conn.close()

            _write_triage(
                triage,
                [
                    {
                        "timestamp": _ts_hours_ago(1),
                        "message_id": "<msg-4>",
                        "from_email": "owner@example.com",
                        "subject": "Re: hello",
                        "category": "hot_interest",
                        "action": "notified+draft",
                    }
                ],
            )
            _write_suppression(suppression, [])
            _write_suppression(data_dir / "suppression.csv", [])

            sync_proc = self._run(
                CAPTURE_SCRIPT,
                ["--triage-log", str(triage), "--suppression-csv", str(suppression)],
                data_dir,
            )
            sync_out = (sync_proc.stdout or "") + "\n" + (sync_proc.stderr or "")
            self.assertEqual(sync_proc.returncode, 0, msg=sync_out)

            report_proc = self._run(OPS_REPORT_SCRIPT, ["--format", "json", "--no-write"], data_dir)
            report_out = (report_proc.stdout or "") + "\n" + (report_proc.stderr or "")
            self.assertEqual(report_proc.returncode, 0, msg=report_out)

            payload = json.loads(report_proc.stdout)
            cohorts = ((payload.get("windows") or {}).get("30d") or {}).get("cohorts") or []
            row = next(
                (
                    r
                    for r in cohorts
                    if (r.get("batch_id") or "") == "2026-02-13_TX" and (r.get("state_at_send") or "") == "TX"
                ),
                None,
            )
            self.assertIsNotNone(row, msg=report_proc.stdout)
            self.assertGreaterEqual(int(row.get("replied", 0)), 1)

    def test_capture_sync_no_outbound_calls(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            data_dir = tmp / "data"
            triage = tmp / "triage.csv"
            suppression = tmp / "suppression.csv"

            db_path = crm_store.ensure_database(data_dir / "crm.sqlite")
            conn = crm_store.connect(db_path)
            try:
                crm_store.init_schema(conn)
                _seed_prospect(conn, "p1", "owner@example.com")
                _seed_sent_event(conn, "p1", "<msg-5>")
                conn.commit()
            finally:
                conn.close()

            _write_triage(
                triage,
                [
                    {
                        "timestamp": _ts_hours_ago(1),
                        "message_id": "<msg-5>",
                        "from_email": "owner@example.com",
                        "subject": "Re: hello",
                        "category": "question",
                        "action": "notified+draft",
                    }
                ],
            )
            _write_suppression(suppression, [])

            with mock.patch("smtplib.SMTP", side_effect=AssertionError("unexpected SMTP call")), mock.patch(
                "smtplib.SMTP_SSL", side_effect=AssertionError("unexpected SMTP_SSL call")
            ):
                env = os.environ.copy()
                env["DATA_DIR"] = str(data_dir)
                with mock.patch.dict(os.environ, env, clear=False):
                    rc = capture_sync.main(["--triage-log", str(triage), "--suppression-csv", str(suppression)])
            self.assertEqual(rc, 0)

    def test_capture_sync_write_scope_status_events_and_suppression_only(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            data_dir = tmp / "data"
            triage = tmp / "triage.csv"
            suppression = tmp / "suppression.csv"

            db_path = crm_store.ensure_database(data_dir / "crm.sqlite")
            conn = crm_store.connect(db_path)
            sql_trace: list[str] = []
            conn.set_trace_callback(lambda sql: sql_trace.append((sql or "").strip().upper()))
            try:
                crm_store.init_schema(conn)
                _seed_prospect(conn, "p1", "owner@example.com")
                _seed_sent_event(conn, "p1", "<msg-6>")
                conn.commit()
            finally:
                conn.close()

            _write_triage(
                triage,
                [
                    {
                        "timestamp": _ts_hours_ago(1),
                        "message_id": "<msg-6>",
                        "from_email": "support@microflowops.com",
                        "subject": "unsubscribe",
                        "category": "unsubscribe",
                        "action": "suppressed",
                    }
                ],
            )
            _write_suppression(
                suppression,
                [
                    {
                        "email": "owner@example.com",
                        "reason": "unsubscribe",
                        "source": "inbound_triage",
                        "timestamp": _ts_hours_ago(1),
                        "evidence_msg_id": "<msg-6>",
                    }
                ],
            )

            original_connect = crm_store.connect

            def _connect_with_trace(path: Path):
                traced_conn = original_connect(path)
                traced_conn.set_trace_callback(lambda sql: sql_trace.append((sql or "").strip().upper()))
                return traced_conn

            env = os.environ.copy()
            env["DATA_DIR"] = str(data_dir)
            sql_trace.clear()
            with mock.patch.dict(os.environ, env, clear=False), mock.patch.object(
                capture_sync.crm_store, "connect", side_effect=_connect_with_trace
            ):
                rc = capture_sync.main(["--triage-log", str(triage), "--suppression-csv", str(suppression)])
            self.assertEqual(rc, 0)

            writes = [
                sql
                for sql in sql_trace
                if sql.startswith("INSERT ") or sql.startswith("UPDATE ") or sql.startswith("DELETE ")
            ]
            self.assertTrue(any("INSERT INTO OUTREACH_EVENTS" in sql for sql in writes))
            self.assertTrue(any("UPDATE PROSPECTS SET STATUS" in sql for sql in writes))
            self.assertTrue(any("INSERT INTO SUPPRESSION" in sql for sql in writes))
            disallowed = [
                sql
                for sql in writes
                if "OUTREACH_EVENTS" not in sql and "PROSPECTS SET STATUS" not in sql and "SUPPRESSION" not in sql
            ]
            self.assertEqual(disallowed, [], msg="\n".join(disallowed))


if __name__ == "__main__":
    unittest.main()
