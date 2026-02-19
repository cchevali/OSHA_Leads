import contextlib
import email
import io
import json
import os
import sqlite3
import tempfile
import unittest
from email.message import EmailMessage
from pathlib import Path
from unittest import mock

from outreach import crm_store
from outreach import import_bounces_imap as ib


def _make_message(subject: str, body: str, message_id: str, sender: str = "MAILER-DAEMON <mailer-daemon@example.com>") -> bytes:
    msg = EmailMessage()
    msg["From"] = sender
    msg["Subject"] = subject
    msg["Message-ID"] = message_id
    msg.set_content(body)
    return msg.as_bytes(policy=email.policy.default)


class _FakeImap:
    def __init__(self, messages: dict[int, bytes], uidvalidity: str = "1"):
        self.messages = {int(k): v for k, v in messages.items()}
        self.uidvalidity = str(uidvalidity)
        self.selected_folder = ""

    def login(self, user: str, password: str):  # pragma: no cover
        return ("OK", [b"logged_in"])

    def logout(self):  # pragma: no cover
        return ("BYE", [b"logout"])

    def select(self, folder: str, readonly: bool = True):
        self.selected_folder = folder
        return ("OK", [b"1"])

    def response(self, key: str):
        if str(key).upper() == "UIDVALIDITY":
            return ("UIDVALIDITY", [self.uidvalidity.encode("utf-8")])
        return ("", [])

    def uid(self, command: str, *args):
        cmd = str(command or "").lower()
        if cmd == "search":
            joined = " ".join("" if a is None else str(a) for a in args)
            start_uid = 1
            marker = "UID "
            idx = joined.find(marker)
            if idx >= 0:
                tail = joined[idx + len(marker) :]
                left = tail.split(":*", 1)[0].strip()
                try:
                    start_uid = int(left)
                except Exception:
                    start_uid = 1
            uids = [u for u in sorted(self.messages) if u >= max(1, start_uid)]
            payload = " ".join(str(u) for u in uids).encode("utf-8")
            return ("OK", [payload])

        if cmd == "fetch":
            uid = int(str(args[0]))
            raw = self.messages.get(uid)
            if raw is None:
                return ("NO", [b"missing"])
            return ("OK", [(b"RFC822", raw)])

        return ("BAD", [b"unsupported"])


class TestImportBouncesImap(unittest.TestCase):
    def _seed_prospect(self, data_dir: Path, prospect_id: str, email_addr: str) -> Path:
        db_path = crm_store.ensure_database(data_dir / "crm.sqlite")
        conn = crm_store.connect(db_path)
        try:
            crm_store.init_schema(conn)
            conn.execute(
                "INSERT INTO prospects(prospect_id, email, created_at) VALUES(?, ?, ?)",
                (prospect_id, email_addr, "2026-02-19T00:00:00+00:00"),
            )
            conn.commit()
        finally:
            conn.close()
        return db_path

    def test_parse_direct_dsn(self):
        subject = "Undelivered Mail Returned to Sender"
        body = (
            "Final-Recipient: rfc822; bad@example.com\n"
            "Status: 5.1.1\n"
            "Diagnostic-Code: smtp; 550 Invalid Recipient\n"
        )
        parsed = ib._parse_bounce(subject, "mailer-daemon@example.com", "From: mailer-daemon", body, "<m1>")
        self.assertIsNotNone(parsed)
        assert parsed is not None
        self.assertEqual(parsed.recipient_email, "bad@example.com")
        self.assertEqual(parsed.bounce_class, "hard")
        self.assertEqual(parsed.smtp_status, "5.1.1")
        self.assertEqual(parsed.smtp_code, "550")
        self.assertEqual(parsed.source, "dsn")

    def test_parse_moderation_forwarded_dsn(self):
        subject = "Email held for Moderation - alerts@microflowops.com"
        body = (
            "Forwarded message\n"
            "To: alerts@microflowops.com\n"
            "Final-Recipient: rfc822; bounce-me@example.com\n"
            "Status: 5.1.1\n"
            "Diagnostic-Code: smtp; 550 Invalid Recipient\n"
        )
        parsed = ib._parse_bounce(subject, "group-noreply@example.com", "From: group-noreply", body, "<m2>")
        self.assertIsNotNone(parsed)
        assert parsed is not None
        self.assertEqual(parsed.source, "moderation")
        self.assertEqual(parsed.original_to, "alerts@microflowops.com")
        self.assertEqual(parsed.recipient_email, "bounce-me@example.com")

    def test_print_config_is_side_effect_free(self):
        with tempfile.TemporaryDirectory() as d:
            data_dir = Path(d) / "runtime"
            env = {"DATA_DIR": str(data_dir)}
            out = io.StringIO()
            with mock.patch.dict(os.environ, env, clear=False), contextlib.redirect_stdout(out):
                rc = ib.main(["--print-config"])
            self.assertEqual(rc, 0)
            self.assertIn("PASS_BOUNCE_IMPORT_PRINT_CONFIG", out.getvalue())
            self.assertFalse((data_dir / "bounce_import_state.json").exists())

    def test_dry_run_parses_without_writes(self):
        with tempfile.TemporaryDirectory() as d:
            data_dir = Path(d) / "runtime"
            msg = _make_message(
                "Undelivered Mail Returned to Sender",
                "Final-Recipient: rfc822; dryrun@example.com\nStatus: 5.1.1\nDiagnostic-Code: smtp; 550 bad\n",
                "<dry1>",
            )
            fake_imap = _FakeImap({101: msg}, uidvalidity="77")
            env = {"DATA_DIR": str(data_dir), "BOUNCE_IMAP_PASS": "secret"}
            out = io.StringIO()
            with (
                mock.patch.dict(os.environ, env, clear=False),
                mock.patch.object(ib, "_imap_connect", return_value=fake_imap),
                contextlib.redirect_stdout(out),
            ):
                rc = ib.main(["--dry-run"])
            self.assertEqual(rc, 0)
            payload = out.getvalue()
            self.assertIn("PASS_BOUNCE_IMPORT_DRY_RUN", payload)
            self.assertFalse((data_dir / "bounce_import_state.json").exists())
            self.assertFalse((data_dir / "suppression.csv").exists())
            self.assertFalse((data_dir / "crm.sqlite").exists())

    def test_apply_hard_bounce_writes_events_status_and_suppression(self):
        with tempfile.TemporaryDirectory() as d:
            data_dir = Path(d) / "runtime"
            db_path = self._seed_prospect(data_dir, "p1", "hard@example.com")
            msg = _make_message(
                "Undelivered Mail Returned to Sender",
                "Final-Recipient: rfc822; hard@example.com\nStatus: 5.1.1\nDiagnostic-Code: smtp; 550 bad recipient\n",
                "<hard-1>",
            )
            fake_imap = _FakeImap({51: msg}, uidvalidity="11")
            env = {"DATA_DIR": str(data_dir), "BOUNCE_IMAP_PASS": "secret"}
            out = io.StringIO()
            with (
                mock.patch.dict(os.environ, env, clear=False),
                mock.patch.object(ib, "_imap_connect", return_value=fake_imap),
                contextlib.redirect_stdout(out),
            ):
                rc = ib.main([])
            self.assertEqual(rc, 0)
            self.assertIn("PASS_BOUNCE_IMPORT_APPLY", out.getvalue())
            self.assertTrue((data_dir / "bounce_import_state.json").exists())
            self.assertTrue((data_dir / "suppression.csv").exists())

            conn = sqlite3.connect(str(db_path))
            conn.row_factory = sqlite3.Row
            try:
                c1 = conn.execute("SELECT COUNT(*) AS c FROM bounce_events").fetchone()["c"]
                c2 = conn.execute("SELECT COUNT(*) AS c FROM outreach_events WHERE event_type='bounced'").fetchone()["c"]
                status = conn.execute("SELECT status FROM prospects WHERE prospect_id='p1'").fetchone()["status"]
                sup = conn.execute("SELECT reason FROM suppression WHERE email='hard@example.com'").fetchone()
            finally:
                conn.close()

            self.assertEqual(int(c1), 1)
            self.assertEqual(int(c2), 1)
            self.assertEqual(status, "bounced")
            self.assertIsNotNone(sup)
            with open(data_dir / "suppression.csv", "r", encoding="utf-8") as f:
                self.assertIn("hard@example.com", f.read().lower())

    def test_apply_soft_bounce_records_bounce_event_only(self):
        with tempfile.TemporaryDirectory() as d:
            data_dir = Path(d) / "runtime"
            db_path = self._seed_prospect(data_dir, "p1", "soft@example.com")
            msg = _make_message(
                "Delivery Status Notification",
                "Final-Recipient: rfc822; soft@example.com\nStatus: 4.2.0\nDiagnostic-Code: smtp; 450 mailbox busy\n",
                "<soft-1>",
            )
            fake_imap = _FakeImap({91: msg}, uidvalidity="12")
            env = {"DATA_DIR": str(data_dir), "BOUNCE_IMAP_PASS": "secret"}
            with mock.patch.dict(os.environ, env, clear=False), mock.patch.object(ib, "_imap_connect", return_value=fake_imap):
                rc = ib.main([])
            self.assertEqual(rc, 0)

            conn = sqlite3.connect(str(db_path))
            conn.row_factory = sqlite3.Row
            try:
                c1 = conn.execute("SELECT COUNT(*) AS c FROM bounce_events").fetchone()["c"]
                c2 = conn.execute("SELECT COUNT(*) AS c FROM outreach_events WHERE event_type='bounced'").fetchone()["c"]
                c3 = conn.execute("SELECT COUNT(*) AS c FROM suppression").fetchone()["c"]
                status = conn.execute("SELECT status FROM prospects WHERE prospect_id='p1'").fetchone()["status"]
            finally:
                conn.close()

            self.assertEqual(int(c1), 1)
            self.assertEqual(int(c2), 0)
            self.assertEqual(int(c3), 0)
            self.assertEqual(status, "new")

    def test_replay_idempotent_no_duplicate_writes(self):
        with tempfile.TemporaryDirectory() as d:
            data_dir = Path(d) / "runtime"
            db_path = self._seed_prospect(data_dir, "p1", "dup@example.com")
            msg = _make_message(
                "Undelivered Mail Returned to Sender",
                "Final-Recipient: rfc822; dup@example.com\nStatus: 5.1.1\nDiagnostic-Code: smtp; 550 bad\n",
                "<dup-1>",
            )
            env = {"DATA_DIR": str(data_dir), "BOUNCE_IMAP_PASS": "secret"}

            fake_imap = _FakeImap({201: msg}, uidvalidity="90")
            with mock.patch.dict(os.environ, env, clear=False), mock.patch.object(ib, "_imap_connect", return_value=fake_imap):
                rc1 = ib.main([])
            self.assertEqual(rc1, 0)

            # Force a replay by removing state; duplicate guard should still prevent duplicate writes.
            state_path = data_dir / "bounce_import_state.json"
            if state_path.exists():
                state_path.unlink()

            fake_imap2 = _FakeImap({201: msg}, uidvalidity="90")
            with mock.patch.dict(os.environ, env, clear=False), mock.patch.object(ib, "_imap_connect", return_value=fake_imap2):
                rc2 = ib.main([])
            self.assertEqual(rc2, 0)

            conn = sqlite3.connect(str(db_path))
            conn.row_factory = sqlite3.Row
            try:
                bounces = conn.execute("SELECT COUNT(*) AS c FROM bounce_events").fetchone()["c"]
                events = conn.execute("SELECT COUNT(*) AS c FROM outreach_events WHERE event_type='bounced'").fetchone()["c"]
            finally:
                conn.close()
            self.assertEqual(int(bounces), 1)
            self.assertEqual(int(events), 1)

    def test_uidvalidity_change_warns_and_stays_idempotent(self):
        with tempfile.TemporaryDirectory() as d:
            data_dir = Path(d) / "runtime"
            db_path = self._seed_prospect(data_dir, "p1", "uv@example.com")
            msg = _make_message(
                "Undelivered Mail Returned to Sender",
                "Final-Recipient: rfc822; uv@example.com\nStatus: 5.1.1\nDiagnostic-Code: smtp; 550 bad\n",
                "<uv-1>",
            )
            env = {"DATA_DIR": str(data_dir), "BOUNCE_IMAP_PASS": "secret"}
            with mock.patch.dict(os.environ, env, clear=False), mock.patch.object(
                ib, "_imap_connect", return_value=_FakeImap({301: msg}, uidvalidity="100")
            ):
                rc1 = ib.main([])
            self.assertEqual(rc1, 0)

            err = io.StringIO()
            with (
                mock.patch.dict(os.environ, env, clear=False),
                mock.patch.object(ib, "_imap_connect", return_value=_FakeImap({301: msg}, uidvalidity="101")),
                contextlib.redirect_stderr(err),
            ):
                rc2 = ib.main([])
            self.assertEqual(rc2, 0)
            self.assertIn("WARN_BOUNCE_UIDVALIDITY_CHANGED", err.getvalue())

            conn = sqlite3.connect(str(db_path))
            conn.row_factory = sqlite3.Row
            try:
                bounces = conn.execute("SELECT COUNT(*) AS c FROM bounce_events").fetchone()["c"]
                events = conn.execute("SELECT COUNT(*) AS c FROM outreach_events WHERE event_type='bounced'").fetchone()["c"]
            finally:
                conn.close()
            self.assertEqual(int(bounces), 1)
            self.assertEqual(int(events), 1)

    def test_lock_file_returns_err_token(self):
        with tempfile.TemporaryDirectory() as d:
            data_dir = Path(d) / "runtime"
            data_dir.mkdir(parents=True, exist_ok=True)
            lock_file = data_dir / "bounce_import.lock"
            lock_file.write_text("locked", encoding="utf-8")
            env = {"DATA_DIR": str(data_dir), "BOUNCE_IMAP_PASS": "secret"}
            err = io.StringIO()
            with mock.patch.dict(os.environ, env, clear=False), contextlib.redirect_stderr(err):
                rc = ib.main(["--dry-run"])
            self.assertEqual(rc, 2)
            self.assertIn("ERR_BOUNCE_IMPORT_LOCKED", err.getvalue())


if __name__ == "__main__":
    unittest.main()
