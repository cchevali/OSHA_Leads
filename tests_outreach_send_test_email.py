import csv
import os
import subprocess
import sys
import tempfile
import unittest
from unittest import mock
import io
import contextlib
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent
SCRIPT = REPO_ROOT / "outreach" / "send_test_cold_email.py"


def _write_outbox(path: Path, fieldnames: list[str], rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fieldnames})


class TestOutreachSendTestEmail(unittest.TestCase):
    def _run(self, *, outbox: Path, to: str | None = None, extra_args: list[str] | None = None, env: dict | None = None):
        env2 = os.environ.copy()
        env2["PYTHONPATH"] = str(REPO_ROOT)
        if env:
            for k, v in env.items():
                if v is None:
                    env2.pop(k, None)
                else:
                    env2[k] = v

        args = [sys.executable, str(SCRIPT), "--outbox", str(outbox)]
        if to is not None:
            args.extend(["--to", to])
        if extra_args:
            args.extend(extra_args)

        return subprocess.run(args, cwd=str(REPO_ROOT), env=env2, capture_output=True, text=True)

    def test_safety_gate_mismatch(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            outbox = tmp / "outbox.csv"
            _write_outbox(
                outbox,
                ["prospect_id", "subject", "body", "unsubscribe_url", "prefs_url", "email"],
                [
                    {
                        "prospect_id": "p1",
                        "subject": "Hello",
                        "body": "Body",
                        "unsubscribe_url": "https://example/unsub",
                        "prefs_url": "https://example/prefs",
                        "email": "prospect@example.com",
                    }
                ],
            )

            p = self._run(outbox=outbox, to="wrong@example.com", env={"OSHA_SMOKE_TO": "allow@example.com"})
            self.assertNotEqual(p.returncode, 0)
            self.assertIn("ERR_TEST_TO_MISMATCH", (p.stderr or "") + (p.stdout or ""))

    def test_missing_canonical_env_var_fails(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            outbox = tmp / "outbox.csv"
            _write_outbox(
                outbox,
                ["prospect_id", "subject", "body", "unsubscribe_url", "prefs_url", "email"],
                [
                    {
                        "prospect_id": "p1",
                        "subject": "Hello",
                        "body": "Body",
                        "unsubscribe_url": "https://example/unsub",
                        "prefs_url": "https://example/prefs",
                        "email": "prospect@example.com",
                    }
                ],
            )

            p = self._run(
                outbox=outbox,
                to=None,
                extra_args=["--dry-run"],
                env={"OSHA_SMOKE_TO": None, "CHASE_EMAIL": None, "OUTREACH_TEST_TO": None},
            )
            self.assertNotEqual(p.returncode, 0)
            self.assertIn("ERR_TEST_TO_MISSING", (p.stderr or "") + (p.stdout or ""))

    def test_schema_missing_required_columns(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            outbox = tmp / "outbox.csv"
            _write_outbox(
                outbox,
                ["prospect_id", "body", "unsubscribe_url"],
                [{"prospect_id": "p1", "body": "Body", "unsubscribe_url": "https://example/unsub"}],
            )

            p = self._run(outbox=outbox, to=None, extra_args=["--dry-run"], env={"OSHA_SMOKE_TO": "allow@example.com"})
            self.assertNotEqual(p.returncode, 0)
            self.assertIn("ERR_OUTBOX_SCHEMA", (p.stderr or "") + (p.stdout or ""))

    def test_default_selects_first_row(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            outbox = tmp / "outbox.csv"
            _write_outbox(
                outbox,
                ["prospect_id", "subject", "body", "unsubscribe_url", "prefs_url", "email"],
                [
                    {
                        "prospect_id": "p1",
                        "subject": "Sub1",
                        "body": "Body1",
                        "unsubscribe_url": "https://example/unsub1",
                        "prefs_url": "https://example/prefs1",
                        "email": "prospect1@example.com",
                    },
                    {
                        "prospect_id": "p2",
                        "subject": "Sub2",
                        "body": "Body2",
                        "unsubscribe_url": "https://example/unsub2",
                        "prefs_url": "https://example/prefs2",
                        "email": "prospect2@example.com",
                    },
                ],
            )

            p = self._run(outbox=outbox, to=None, extra_args=["--dry-run"], env={"OSHA_SMOKE_TO": "allow@example.com"})
            self.assertEqual(p.returncode, 0, msg=p.stderr + "\n" + p.stdout)
            self.assertIn("[TEST] Sub1", p.stdout)
            self.assertIn("PASS_TEST_SEND to=allow@example.com prospect_id=p1", p.stdout)
            self.assertNotIn("TEST SEND (outreach)", p.stdout)

    def test_selects_specific_prospect_id(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            outbox = tmp / "outbox.csv"
            _write_outbox(
                outbox,
                ["prospect_id", "subject", "body", "unsubscribe_url", "prefs_url", "email"],
                [
                    {
                        "prospect_id": "p1",
                        "subject": "Sub1",
                        "body": "Body1",
                        "unsubscribe_url": "https://example/unsub1",
                        "prefs_url": "https://example/prefs1",
                        "email": "prospect1@example.com",
                    },
                    {
                        "prospect_id": "p2",
                        "subject": "Sub2",
                        "body": "Body2",
                        "unsubscribe_url": "https://example/unsub2",
                        "prefs_url": "https://example/prefs2",
                        "email": "prospect2@example.com",
                    },
                ],
            )

            p = self._run(
                outbox=outbox,
                to=None,
                extra_args=["--dry-run", "--prospect-id", "p2"],
                env={"OSHA_SMOKE_TO": "allow@example.com"},
            )
            self.assertEqual(p.returncode, 0, msg=p.stderr + "\n" + p.stdout)
            self.assertIn("[TEST] Sub2", p.stdout)
            self.assertIn("PASS_TEST_SEND to=allow@example.com prospect_id=p2", p.stdout)
            self.assertNotIn("TEST SEND (outreach)", p.stdout)

    def test_legacy_outreach_test_to_alias_only_when_canonical_missing(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            outbox = tmp / "outbox.csv"
            _write_outbox(
                outbox,
                ["prospect_id", "subject", "body", "unsubscribe_url", "prefs_url", "email"],
                [
                    {
                        "prospect_id": "p1",
                        "subject": "Sub1",
                        "body": "Body1",
                        "unsubscribe_url": "https://example/unsub1",
                        "prefs_url": "",
                        "email": "prospect1@example.com",
                    }
                ],
            )

            p = self._run(
                outbox=outbox,
                to=None,
                extra_args=["--dry-run"],
                env={"OSHA_SMOKE_TO": None, "CHASE_EMAIL": None, "OUTREACH_TEST_TO": "allow@example.com"},
            )
            self.assertEqual(p.returncode, 0, msg=p.stderr + "\n" + p.stdout)
            self.assertIn("PASS_TEST_SEND to=allow@example.com prospect_id=p1", p.stdout)

    def test_legacy_chase_email_alias_only_when_canonical_missing(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            outbox = tmp / "outbox.csv"
            _write_outbox(
                outbox,
                ["prospect_id", "subject", "body", "unsubscribe_url", "prefs_url", "email"],
                [
                    {
                        "prospect_id": "p1",
                        "subject": "Sub1",
                        "body": "Body1",
                        "unsubscribe_url": "https://example/unsub1",
                        "prefs_url": "",
                        "email": "prospect1@example.com",
                    }
                ],
            )

            p = self._run(
                outbox=outbox,
                to=None,
                extra_args=["--dry-run"],
                env={"OSHA_SMOKE_TO": None, "CHASE_EMAIL": "allow@example.com", "OUTREACH_TEST_TO": None},
            )
            self.assertEqual(p.returncode, 0, msg=p.stderr + "\n" + p.stdout)
            self.assertIn("PASS_TEST_SEND to=allow@example.com prospect_id=p1", p.stdout)

    def test_canonical_precedence_over_legacy(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            outbox = tmp / "outbox.csv"
            _write_outbox(
                outbox,
                ["prospect_id", "subject", "body", "unsubscribe_url", "prefs_url", "email"],
                [
                    {
                        "prospect_id": "p1",
                        "subject": "Sub1",
                        "body": "Body1",
                        "unsubscribe_url": "https://example/unsub1",
                        "prefs_url": "",
                        "email": "prospect1@example.com",
                    }
                ],
            )

            p = self._run(
                outbox=outbox,
                to=None,
                extra_args=["--dry-run"],
                env={"OSHA_SMOKE_TO": "canonical@example.com", "CHASE_EMAIL": "legacy@example.com"},
            )
            self.assertEqual(p.returncode, 0, msg=p.stderr + "\n" + p.stdout)
            self.assertIn("PASS_TEST_SEND to=canonical@example.com prospect_id=p1", p.stdout)

    def test_send_email_label_constant_is_passed_and_valid(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            outbox = tmp / "outbox.csv"
            _write_outbox(
                outbox,
                ["prospect_id", "subject", "body", "unsubscribe_url", "prefs_url", "email"],
                [
                    {
                        "prospect_id": "p1",
                        "subject": "Sub1",
                        "body": "Body1",
                        "unsubscribe_url": "https://example/unsub1",
                        "prefs_url": "",
                        "email": "prospect1@example.com",
                    }
                ],
            )

            # In-process call so we can assert the send_email kwargs.
            sys.path.insert(0, str(REPO_ROOT))
            import outreach.send_test_cold_email as st

            with mock.patch.dict(os.environ, {"OSHA_SMOKE_TO": "allow@example.com"}, clear=False):
                with mock.patch(
                    "send_digest_email.resolve_branding",
                    return_value={
                        "brand_name": "MicroFlowOps",
                        "brand_legal_name": "",
                        "mailing_address": "X",
                        "from_email": "alerts@example.com",
                        "reply_to": "support@example.com",
                        "from_display_name": "MicroFlowOps",
                    },
                ):
                    with mock.patch("send_digest_email.send_email", return_value=(True, "dry", "")) as m_send:
                        buf_out = io.StringIO()
                        buf_err = io.StringIO()
                        with contextlib.redirect_stdout(buf_out), contextlib.redirect_stderr(buf_err):
                            rc = st.main(["--outbox", str(outbox), "--dry-run"])
                        self.assertEqual(rc, 0)
                        self.assertEqual(m_send.call_count, 1)
                        kwargs = m_send.call_args.kwargs

                        # Outreach test-send must use the explicit `label` kwarg (not `customer_id`).
                        self.assertEqual(kwargs.get("label"), st.SEND_LABEL)
                        self.assertTrue(kwargs.get("label"))
                        self.assertLessEqual(len(kwargs.get("label") or ""), 64)

                        # Caller must not rely on customer_id for the label.
                        self.assertEqual(kwargs.get("customer_id"), "")

                        self.assertEqual(kwargs.get("territory_code"), st.SEND_TERRITORY_CODE)
                        self.assertTrue(kwargs.get("territory_code"))
                        self.assertLessEqual(len(kwargs.get("territory_code") or ""), 64)

                        # Debug header must be OFF by default.
                        self.assertNotIn("TEST SEND (outreach)", kwargs.get("text_body") or "")

                        # When html_body is absent from outbox, send_test must still send a non-empty HTML body.
                        self.assertTrue((kwargs.get("html_body") or "").strip())

    def test_debug_header_flag_includes_preamble_and_html_is_preferred(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            outbox = tmp / "outbox.csv"
            _write_outbox(
                outbox,
                ["prospect_id", "subject", "body", "text_body", "html_body", "unsubscribe_url", "prefs_url", "email"],
                [
                    {
                        "prospect_id": "p1",
                        "subject": "Sub1",
                        "body": "Body1",
                        "text_body": "Text body line",
                        "html_body": "<div>HTML CARD MARKER</div>",
                        "unsubscribe_url": "https://example/unsub",
                        "prefs_url": "https://example/prefs",
                        "email": "prospect1@example.com",
                    }
                ],
            )

            sys.path.insert(0, str(REPO_ROOT))
            import outreach.send_test_cold_email as st

            with mock.patch.dict(os.environ, {"OSHA_SMOKE_TO": "allow@example.com"}, clear=False):
                with mock.patch(
                    "send_digest_email.resolve_branding",
                    return_value={
                        "brand_name": "MicroFlowOps",
                        "brand_legal_name": "",
                        "mailing_address": "X",
                        "from_email": "alerts@example.com",
                        "reply_to": "support@example.com",
                        "from_display_name": "MicroFlowOps",
                    },
                ):
                    with mock.patch("send_digest_email.send_email", return_value=(True, "dry", "")) as m_send:
                        buf_out = io.StringIO()
                        buf_err = io.StringIO()
                        with contextlib.redirect_stdout(buf_out), contextlib.redirect_stderr(buf_err):
                            rc = st.main(["--outbox", str(outbox), "--dry-run", "--debug-header"])
                        self.assertEqual(rc, 0)
                        kwargs = m_send.call_args.kwargs
                        self.assertIn("TEST SEND (outreach)", kwargs.get("text_body") or "")
                        self.assertIn("HTML CARD MARKER", kwargs.get("html_body") or "")


if __name__ == "__main__":
    unittest.main()
