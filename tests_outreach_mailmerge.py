import csv
import io
import json
import os
import re
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest import mock


REPO_ROOT = Path(__file__).resolve().parent
SCRIPT = REPO_ROOT / "outreach" / "generate_mailmerge.py"

REQUIRED_COLS = [
    "prospect_id",
    "first_name",
    "last_name",
    "firm",
    "title",
    "email",
    "state",
    "city",
    "territory_code",
    "source",
    "notes",
]


def _write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=REQUIRED_COLS)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in REQUIRED_COLS})


def _read_csv(path: Path) -> list[dict]:
    with open(path, "r", newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _csv_fieldnames(path: Path) -> list[str]:
    with open(path, "r", newline="", encoding="utf-8") as f:
        return list((csv.DictReader(f).fieldnames or []))


def _write_suppression(path: Path, emails: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["email"])
        w.writeheader()
        for e in emails:
            w.writerow({"email": e})


def _write_template(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("Hi FIRST_NAME\nPREFS_URL\n", encoding="utf-8")


class TestOutreachMailmerge(unittest.TestCase):
    def _run_export(
        self,
        tmp: Path,
        *,
        input_csv: Path,
        out_csv: Path,
        batch: str = "TX_W2",
        state: str = "TX",
        template: Path,
        html_template: Path | None = None,
        db_path: Path | None = None,
        env_overrides: dict[str, str] | None = None,
        extra_args: list[str] | None = None,
    ) -> subprocess.CompletedProcess:
        env = os.environ.copy()
        env["PYTHONPATH"] = str(REPO_ROOT)
        env["DATA_DIR"] = str(tmp)  # isolates suppression + token store for tests
        if env_overrides:
            for k, v in env_overrides.items():
                if v is None:
                    env.pop(k, None)
                else:
                    env[k] = v

        args = [
            sys.executable,
            str(SCRIPT),
            "--input",
            str(input_csv),
            "--batch",
            batch,
            "--state",
            state,
            "--out",
            str(out_csv),
            "--template",
            str(template),
            "--html-template",
            str(html_template or (REPO_ROOT / "outreach" / "outreach_card.html")),
            "--db",
            str(db_path or (tmp / "no_db.sqlite")),
        ]
        if extra_args:
            args.extend(extra_args)

        return subprocess.run(args, cwd=str(tmp), env=env, capture_output=True, text=True)

    def _run_preview(
        self,
        tmp: Path,
        *,
        state: str = "TX",
        input_csv: Path | None = None,
        db_path: Path | None = None,
        env_overrides: dict[str, str] | None = None,
        limit: int = 1,
    ) -> subprocess.CompletedProcess:
        env = os.environ.copy()
        env["PYTHONPATH"] = str(REPO_ROOT)
        env["DATA_DIR"] = str(tmp)
        if env_overrides:
            for k, v in env_overrides.items():
                if v is None:
                    env.pop(k, None)
                else:
                    env[k] = v

        args = [
            sys.executable,
            str(SCRIPT),
            "--render-preview",
            "--state",
            state,
            "--limit",
            str(limit),
            "--template",
            str(REPO_ROOT / "outreach" / "outreach_plain.txt"),
            "--html-template",
            str(REPO_ROOT / "outreach" / "outreach_card.html"),
            "--db",
            str(db_path or (tmp / "no_db.sqlite")),
        ]
        if input_csv is not None:
            args.extend(["--input", str(input_csv)])
        return subprocess.run(args, cwd=str(tmp), env=env, capture_output=True, text=True)

    def test_dedupe_case_insensitive_and_manifest_reason(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            sup = tmp / "suppression.csv"
            _write_suppression(sup, [])

            in_csv = tmp / "in.csv"
            out_csv = tmp / "outbox.csv"
            tpl = tmp / "tpl.txt"
            _write_template(tpl)

            _write_csv(
                in_csv,
                [
                    {
                        "prospect_id": "p1",
                        "first_name": "A",
                        "last_name": "One",
                        "firm": "Co",
                        "title": "Ops",
                        "email": "TEST@Example.com",
                        "state": "TX",
                        "city": "Austin",
                        "territory_code": "X",
                        "source": "s",
                        "notes": "",
                    },
                    {
                        "prospect_id": "p2",
                        "first_name": "B",
                        "last_name": "Two",
                        "firm": "Co",
                        "title": "Ops",
                        "email": "test@example.com",
                        "state": "TX",
                        "city": "Austin",
                        "territory_code": "X",
                        "source": "s",
                        "notes": "",
                    },
                ],
            )

            env = {"UNSUB_ENDPOINT_BASE": "https://unsub.example.internal/unsubscribe", "UNSUB_SECRET": "test_secret"}
            p1 = self._run_export(tmp, input_csv=in_csv, out_csv=out_csv, template=tpl, env_overrides=env)
            self.assertEqual(p1.returncode, 0, msg=p1.stderr + "\n" + p1.stdout)

            out_rows = _read_csv(out_csv)
            self.assertEqual(len(out_rows), 1)
            self.assertIn("unsubscribe_url", out_rows[0])
            self.assertIn("token=", out_rows[0]["unsubscribe_url"])

            manifest = out_csv.with_name(out_csv.stem + "_manifest.csv")
            man_rows = _read_csv(manifest)
            self.assertEqual(len(man_rows), 2)
            dropped = [r for r in man_rows if (r.get("status") or "") == "dropped"]
            self.assertEqual(len(dropped), 1)
            self.assertEqual((dropped[0].get("reason") or "").strip(), "deduped")

            # Deterministic URL: run twice yields identical unsubscribe_url for the exported row.
            p2 = self._run_export(
                tmp,
                input_csv=in_csv,
                out_csv=out_csv,
                template=tpl,
                env_overrides=env,
                extra_args=["--allow-repeat"],
            )
            self.assertEqual(p2.returncode, 0, msg=p2.stderr + "\n" + p2.stdout)
            out_rows2 = _read_csv(out_csv)
            self.assertEqual(out_rows2[0]["unsubscribe_url"], out_rows[0]["unsubscribe_url"])

    def test_ledger_drops_already_exported_prospect_id_by_default(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            _write_suppression(tmp / "suppression.csv", [])
            tpl = tmp / "tpl.txt"
            _write_template(tpl)

            in_csv = tmp / "in.csv"
            out_csv = tmp / "outbox.csv"
            _write_csv(
                in_csv,
                [
                    {
                        "prospect_id": "p1",
                        "first_name": "A",
                        "last_name": "One",
                        "firm": "Co",
                        "title": "Ops",
                        "email": "a@example.com",
                        "state": "TX",
                        "city": "Austin",
                        "territory_code": "X",
                        "source": "s",
                        "notes": "",
                    }
                ],
            )

            env = {"UNSUB_ENDPOINT_BASE": "https://unsub.example.internal/unsubscribe", "UNSUB_SECRET": "test_secret"}
            p1 = self._run_export(tmp, input_csv=in_csv, out_csv=out_csv, template=tpl, env_overrides=env)
            self.assertEqual(p1.returncode, 0, msg=p1.stderr + "\n" + p1.stdout)
            self.assertEqual(len(_read_csv(out_csv)), 1)

            p2 = self._run_export(tmp, input_csv=in_csv, out_csv=out_csv, template=tpl, env_overrides=env)
            self.assertEqual(p2.returncode, 0, msg=p2.stderr + "\n" + p2.stdout)
            self.assertEqual(len(_read_csv(out_csv)), 0)

            manifest = out_csv.with_name(out_csv.stem + "_manifest.csv")
            man_rows = _read_csv(manifest)
            self.assertEqual(len(man_rows), 1)
            self.assertEqual((man_rows[0].get("reason") or "").strip(), "already_exported")

    def test_suppression_drops_with_reason(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            sup = tmp / "suppression.csv"
            _write_suppression(sup, ["blocked@example.com"])

            in_csv = tmp / "in.csv"
            out_csv = tmp / "outbox.csv"
            tpl = tmp / "tpl.txt"
            _write_template(tpl)

            _write_csv(
                in_csv,
                [
                    {
                        "prospect_id": "p1",
                        "first_name": "A",
                        "last_name": "One",
                        "firm": "Co",
                        "title": "Ops",
                        "email": "blocked@example.com",
                        "state": "TX",
                        "city": "Austin",
                        "territory_code": "X",
                        "source": "s",
                        "notes": "",
                    },
                    {
                        "prospect_id": "p2",
                        "first_name": "B",
                        "last_name": "Two",
                        "firm": "Co",
                        "title": "Ops",
                        "email": "ok@example.com",
                        "state": "TX",
                        "city": "Austin",
                        "territory_code": "X",
                        "source": "s",
                        "notes": "",
                    },
                ],
            )

            env = {"UNSUB_ENDPOINT_BASE": "https://unsub.example.internal/unsubscribe", "UNSUB_SECRET": "test_secret"}
            p = self._run_export(tmp, input_csv=in_csv, out_csv=out_csv, template=tpl, env_overrides=env)
            self.assertEqual(p.returncode, 0, msg=p.stderr + "\n" + p.stdout)

            out_rows = _read_csv(out_csv)
            self.assertEqual(len(out_rows), 1)
            self.assertEqual(out_rows[0]["email"], "ok@example.com")

            manifest = out_csv.with_name(out_csv.stem + "_manifest.csv")
            man_rows = _read_csv(manifest)
            suppressed = [r for r in man_rows if (r.get("reason") or "").strip() == "suppressed"]
            self.assertEqual(len(suppressed), 1)
            self.assertEqual((suppressed[0].get("prospect_id") or "").strip(), "p1")

    def test_subscriber_key_deterministic_and_url_safe(self):
        from outreach import generate_mailmerge as gm

        k1 = gm._subscriber_key_from_prospect_id("prospect-123", "TX_W2")
        k2 = gm._subscriber_key_from_prospect_id("prospect-123", "TX_W2")
        self.assertEqual(k1, k2)
        self.assertLessEqual(len(k1), 80)
        self.assertRegex(k1, r"^[A-Za-z0-9_.-]{1,80}$")

    def test_build_outreach_subject_prefers_primary_type_when_not_mixed(self):
        from outreach import generate_mailmerge as gm

        subject = gm.build_outreach_subject(
            "CA",
            recent_leads=[
                {"date_opened": "2026-02-19", "inspection_type": "Complaint"},
                {"date_opened": "2026-02-18", "inspection_type": "Complaint"},
                {"date_opened": "2026-02-17", "inspection_type": "Accident"},
            ],
            segment_descriptor="defense team",
            state_full_name="California",
            signal_count=3,
        )
        self.assertIn("Quick heads up — 3 recent CA complaint inspections", subject)
        self.assertLess(len(subject), 65)

    def test_build_outreach_subject_omits_type_when_mixed(self):
        from outreach import generate_mailmerge as gm

        subject = gm.build_outreach_subject(
            "CA",
            recent_leads=[
                {"date_opened": "2026-02-19", "inspection_type": "Complaint"},
                {"date_opened": "2026-02-18", "inspection_type": "Accident"},
            ],
            segment_descriptor="",
            state_full_name="California",
            signal_count=2,
        )
        self.assertIn("Quick heads up — 2 recent CA inspections", subject)
        self.assertNotIn("complaint", subject.lower())
        self.assertNotIn("accident", subject.lower())
        self.assertLess(len(subject), 65)

    def test_build_outreach_subject_single_signal_uses_opened_date(self):
        from outreach import generate_mailmerge as gm

        subject = gm.build_outreach_subject(
            "CA",
            recent_leads=[{"date_opened": "2026-02-19"}],
            signal_count=1,
        )
        self.assertEqual(subject, "Quick heads up — CA inspection opened Feb 19")

    def test_missing_one_click_config_exits_nonzero_with_token(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            _write_suppression(tmp / "suppression.csv", [])
            tpl = tmp / "tpl.txt"
            _write_template(tpl)

            in_csv = tmp / "in.csv"
            out_csv = tmp / "outbox.csv"
            _write_csv(
                in_csv,
                [
                    {
                        "prospect_id": "p1",
                        "first_name": "A",
                        "last_name": "One",
                        "firm": "Co",
                        "title": "Ops",
                        "email": "a@example.com",
                        "state": "TX",
                        "city": "Austin",
                        "territory_code": "X",
                        "source": "s",
                        "notes": "",
                    }
                ],
            )

            env = {"UNSUB_ENDPOINT_BASE": "", "UNSUB_SECRET": ""}
            p = self._run_export(tmp, input_csv=in_csv, out_csv=out_csv, template=tpl, env_overrides=env)
            self.assertNotEqual(p.returncode, 0)
            self.assertIn("ERR_ONE_CLICK_REQUIRED", (p.stderr or "") + (p.stdout or ""))

    def test_allow_mailto_fallback_writes_outbox_and_manifest(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            _write_suppression(tmp / "suppression.csv", [])
            tpl = tmp / "tpl.txt"
            _write_template(tpl)

            in_csv = tmp / "in.csv"
            out_csv = tmp / "outbox.csv"
            _write_csv(
                in_csv,
                [
                    {
                        "prospect_id": "p1",
                        "first_name": "A",
                        "last_name": "One",
                        "firm": "Co",
                        "title": "Ops",
                        "email": "a@example.com",
                        "state": "TX",
                        "city": "Austin",
                        "territory_code": "X",
                        "source": "s",
                        "notes": "",
                    }
                ],
            )

            env = {"UNSUB_ENDPOINT_BASE": "", "UNSUB_SECRET": "", "REPLY_TO_EMAIL": "support@microflowops.com"}
            p = self._run_export(
                tmp,
                input_csv=in_csv,
                out_csv=out_csv,
                template=tpl,
                env_overrides=env,
                extra_args=["--allow-mailto-fallback"],
            )
            self.assertEqual(p.returncode, 0, msg=p.stderr + "\n" + p.stdout)

            out_rows = _read_csv(out_csv)
            self.assertEqual(len(out_rows), 1)
            self.assertTrue(out_rows[0]["unsubscribe_url"].startswith("mailto:"))

            manifest = out_csv.with_name(out_csv.stem + "_manifest.csv")
            self.assertTrue(manifest.exists())
            man_rows = _read_csv(manifest)
            self.assertEqual(len(man_rows), 1)
            self.assertEqual(man_rows[0]["status"], "exported")

    def test_export_writes_expected_artifacts(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            _write_suppression(tmp / "suppression.csv", [])
            tpl = tmp / "tpl.txt"
            _write_template(tpl)

            in_csv = tmp / "in.csv"
            out_csv = tmp / "outbox.csv"
            _write_csv(
                in_csv,
                [
                    {
                        "prospect_id": "p1",
                        "first_name": "A",
                        "last_name": "One",
                        "firm": "Co",
                        "title": "Ops",
                        "email": "a@example.com",
                        "state": "TX",
                        "city": "Austin",
                        "territory_code": "X",
                        "source": "s",
                        "notes": "",
                    }
                ],
            )

            env = {"UNSUB_ENDPOINT_BASE": "https://unsub.example.internal/unsubscribe", "UNSUB_SECRET": "test_secret"}
            p = self._run_export(tmp, input_csv=in_csv, out_csv=out_csv, template=tpl, env_overrides=env)
            self.assertEqual(p.returncode, 0, msg=p.stderr + "\n" + p.stdout)

            stdout = p.stdout or ""
            outbox_line = next((ln.strip() for ln in stdout.splitlines() if ln.strip().startswith("outbox=")), "")
            manifest_line = next((ln.strip() for ln in stdout.splitlines() if ln.strip().startswith("manifest=")), "")
            ledger_line = next((ln.strip() for ln in stdout.splitlines() if ln.strip().startswith("ledger=")), "")
            run_log_line = next((ln.strip() for ln in stdout.splitlines() if ln.strip().startswith("run_log=")), "")
            self.assertTrue(outbox_line, msg=stdout)
            self.assertTrue(manifest_line, msg=stdout)
            self.assertTrue(ledger_line, msg=stdout)
            self.assertTrue(run_log_line, msg=stdout)

            outbox_path = Path(outbox_line.split("=", 1)[1].strip())
            manifest_path = Path(manifest_line.split("=", 1)[1].strip())
            ledger_path = Path(ledger_line.split("=", 1)[1].strip())
            run_log_path = Path(run_log_line.split("=", 1)[1].strip())
            if not outbox_path.is_absolute():
                outbox_path = (tmp / outbox_path).resolve()
            if not manifest_path.is_absolute():
                manifest_path = (tmp / manifest_path).resolve()
            if not ledger_path.is_absolute():
                ledger_path = (tmp / ledger_path).resolve()
            if not run_log_path.is_absolute():
                run_log_path = (tmp / run_log_path).resolve()

            self.assertTrue(outbox_path.exists(), msg=f"missing outbox: {outbox_path}")
            self.assertTrue(manifest_path.exists(), msg=f"missing manifest: {manifest_path}")
            self.assertTrue(ledger_path.exists(), msg=f"missing ledger: {ledger_path}")
            self.assertTrue(run_log_path.exists(), msg=f"missing run log: {run_log_path}")

    def test_missing_suppression_file_exits_nonzero_and_no_outputs(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            # Intentionally do NOT create suppression.csv in DATA_DIR.
            tpl = tmp / "tpl.txt"
            _write_template(tpl)

            in_csv = tmp / "in.csv"
            out_csv = tmp / "outbox.csv"
            _write_csv(
                in_csv,
                [
                    {
                        "prospect_id": "p1",
                        "first_name": "A",
                        "last_name": "One",
                        "firm": "Co",
                        "title": "Ops",
                        "email": "a@example.com",
                        "state": "TX",
                        "city": "Austin",
                        "territory_code": "X",
                        "source": "s",
                        "notes": "",
                    }
                ],
            )

            env = {"UNSUB_ENDPOINT_BASE": "https://unsub.example.internal/unsubscribe", "UNSUB_SECRET": "test_secret"}
            p = self._run_export(tmp, input_csv=in_csv, out_csv=out_csv, template=tpl, env_overrides=env)
            self.assertNotEqual(p.returncode, 0)
            combined = (p.stderr or "") + (p.stdout or "")
            self.assertIn("ERR_SUPPRESSION_REQUIRED", combined)

            manifest = out_csv.with_name(out_csv.stem + "_manifest.csv")
            self.assertFalse(out_csv.exists())
            self.assertFalse(manifest.exists())
            self.assertFalse((tmp / "outreach" / "outreach_runs").exists())

    def test_recent_signals_and_last_refresh_are_populated_when_inspections_db_present(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            _write_suppression(tmp / "suppression.csv", [])

            # Minimal inspections DB that send_digest_email.get_leads_for_period can query.
            db_path = tmp / "db.sqlite"
            import sqlite3

            conn = sqlite3.connect(str(db_path))
            cur = conn.cursor()
            cur.execute(
                """
                CREATE TABLE inspections (
                    activity_nr TEXT,
                    date_opened TEXT,
                    inspection_type TEXT,
                    scope TEXT,
                    case_status TEXT,
                    establishment_name TEXT,
                    site_city TEXT,
                    site_state TEXT,
                    site_zip TEXT,
                    naics TEXT,
                    naics_desc TEXT,
                    violations_count INTEGER,
                    emphasis TEXT,
                    lead_score INTEGER,
                    first_seen_at TEXT,
                    last_seen_at TEXT,
                    source_url TEXT,
                    parse_invalid INTEGER
                )
                """
            )
            cur.execute(
                """
                INSERT INTO inspections (
                    activity_nr, date_opened, inspection_type, scope, case_status,
                    establishment_name, site_city, site_state, site_zip,
                    naics, naics_desc, violations_count, emphasis, lead_score,
                    first_seen_at, last_seen_at, source_url, parse_invalid
                ) VALUES (
                    '1001', '2026-02-01', 'Complaint', 'Partial', 'Open',
                    'Acme Safety Co', 'Austin', 'TX', '78701',
                    '000000', 'NA', 0, '', 10,
                    '2026-02-10T12:00:00Z', '2026-02-10T12:00:00Z', 'https://example', 0
                )
                """
            )
            conn.commit()
            conn.close()

            tpl = tmp / "tpl.txt"
            tpl.write_text(
                "Hi FIRST_NAME\nRecent signals:\nRECENT_SIGNALS_LINES\nLast refresh: LAST_REFRESH_ET\n"
                "Opt out anytime: Unsubscribe | Manage preferences\nUNSUBSCRIBE_URL\nPREFS_URL\n",
                encoding="utf-8",
            )

            in_csv = tmp / "in.csv"
            out_csv = tmp / "outbox.csv"
            _write_csv(
                in_csv,
                [
                    {
                        "prospect_id": "p1",
                        "first_name": "A",
                        "last_name": "One",
                        "firm": "Co",
                        "title": "Ops",
                        "email": "a@example.com",
                        "state": "TX",
                        "city": "Austin",
                        "territory_code": "X",
                        "source": "s",
                        "notes": "",
                    }
                ],
            )

            env = {"UNSUB_ENDPOINT_BASE": "https://unsub.example.internal/unsubscribe", "UNSUB_SECRET": "test_secret"}
            p = self._run_export(
                tmp,
                input_csv=in_csv,
                out_csv=out_csv,
                template=tpl,
                db_path=db_path,
                env_overrides=env,
                extra_args=[],
            )
            self.assertEqual(p.returncode, 0, msg=p.stderr + "\n" + p.stdout)

            out_rows = _read_csv(out_csv)
            self.assertEqual(len(out_rows), 1)
            body = out_rows[0].get("body") or ""
            text_body = out_rows[0].get("text_body") or ""
            html_body = out_rows[0].get("html_body") or ""
            subject = (out_rows[0].get("subject") or "").strip()
            self.assertIn("Recent signals:", body)
            self.assertIn("Last refresh:", body)
            self.assertIn(" ET", body)
            self.assertTrue(text_body.strip())
            self.assertEqual(body, text_body)
            self.assertTrue(html_body.strip())
            self.assertIn("I spotted a few recent OSHA inspections in Texas", html_body)
            self.assertIn("may be relevant to your team.", html_body)

            # Wally-style markers.
            self.assertIn("Chase", html_body)
            self.assertIn("11539 Links Dr, Reston, VA 20190", html_body)
            self.assertIn('href="https://microflowops.com"', html_body)

            # Single opt-out block in the footer (not duplicated elsewhere).
            self.assertEqual(html_body.count(">Unsubscribe</a>"), 1)
            self.assertEqual(html_body.count(">Manage preferences</a>"), 0)
            self.assertEqual(html_body.count("unsub.example.internal/unsubscribe?token="), 1)
            self.assertEqual(html_body.count("unsub.example.internal/prefs?token="), 0)

            # Ensure one-click links are only in the footer area (after the address line).
            addr_idx = html_body.find("11539 Links Dr, Reston, VA 20190")
            self.assertGreater(addr_idx, 0)
            pre_footer = html_body[:addr_idx]
            self.assertNotIn("unsub.example.internal/unsubscribe?token=", pre_footer)
            self.assertEqual(subject, "Quick heads up — TX inspection opened Feb 1")
            self.assertLess(len(subject), 65)

    def test_recent_window_empty_has_no_backfill_or_sample_copy(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            _write_suppression(tmp / "suppression.csv", [])

            db_path = tmp / "db.sqlite"
            import sqlite3

            conn = sqlite3.connect(str(db_path))
            cur = conn.cursor()
            cur.execute(
                """
                CREATE TABLE inspections (
                    activity_nr TEXT,
                    date_opened TEXT,
                    inspection_type TEXT,
                    establishment_name TEXT,
                    site_city TEXT,
                    site_state TEXT,
                    lead_score INTEGER,
                    first_seen_at TEXT,
                    last_seen_at TEXT,
                    source_url TEXT,
                    parse_invalid INTEGER
                )
                """
            )
            cur.executemany(
                """
                INSERT INTO inspections (
                    activity_nr, date_opened, inspection_type, establishment_name,
                    site_city, site_state, lead_score, first_seen_at, last_seen_at, source_url, parse_invalid
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
                """,
                [
                    (
                        "2001",
                        "2025-12-15",
                        "Complaint",
                        "Legacy FL Co",
                        "Tampa",
                        "FL",
                        8,
                        "2025-12-16T12:00:00Z",
                        "2025-12-16T12:00:00Z",
                        "https://www.osha.gov/ords/imis/establishment.inspection_detail?id=2001",
                    ),
                    (
                        "2002",
                        "2025-12-01",
                        "Referral",
                        "Older FL Co",
                        "Orlando",
                        "FL",
                        6,
                        "2025-12-02T12:00:00Z",
                        "2025-12-02T12:00:00Z",
                        "https://www.osha.gov/ords/imis/establishment.inspection_detail?id=2002",
                    ),
                ],
            )
            conn.commit()
            conn.close()

            in_csv = tmp / "in.csv"
            out_csv = tmp / "outbox.csv"
            _write_csv(
                in_csv,
                [
                    {
                        "prospect_id": "p1",
                        "first_name": "Alex",
                        "last_name": "One",
                        "firm": "Co",
                        "title": "Ops",
                        "email": "a@example.com",
                        "state": "FL",
                        "city": "Miami",
                        "territory_code": "X",
                        "source": "s",
                        "notes": "",
                    }
                ],
            )

            env = {"UNSUB_ENDPOINT_BASE": "https://unsub.example.internal/unsubscribe", "UNSUB_SECRET": "test_secret"}
            p = self._run_export(
                tmp,
                input_csv=in_csv,
                out_csv=out_csv,
                template=REPO_ROOT / "outreach" / "outreach_plain.txt",
                html_template=REPO_ROOT / "outreach" / "outreach_card.html",
                db_path=db_path,
                state="FL",
                batch="TEST_FL",
                env_overrides=env,
            )
            self.assertEqual(p.returncode, 0, msg=p.stderr + "\n" + p.stdout)
            out_rows = _read_csv(out_csv)
            self.assertEqual(len(out_rows), 1)
            body = out_rows[0].get("body") or ""
            html_body = out_rows[0].get("html_body") or ""
            self.assertNotIn("No recent signals in the last 14 days", body)
            self.assertNotIn("Most recent signals we have", body)
            self.assertNotIn("outside the 14-day window", body)
            self.assertNotIn("Example signals (sample, not state-specific)", body)
            self.assertNotIn("Sample Industrial Services", body)
            self.assertNotIn("Example signals (sample, not state-specific)", html_body)
            self.assertNotIn("Sample Industrial Services", html_body)
            self.assertEqual((out_rows[0].get("subject") or "").strip(), "Quick heads up — FL inspection opened Dec 15")
            self.assertLess(len((out_rows[0].get("subject") or "").strip()), 65)

    def test_state_with_no_history_has_no_sample_signals(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            _write_suppression(tmp / "suppression.csv", [])

            db_path = tmp / "db.sqlite"
            import sqlite3

            conn = sqlite3.connect(str(db_path))
            cur = conn.cursor()
            cur.execute(
                """
                CREATE TABLE inspections (
                    site_state TEXT,
                    date_opened TEXT,
                    parse_invalid INTEGER
                )
                """
            )
            conn.commit()
            conn.close()

            in_csv = tmp / "in.csv"
            out_csv = tmp / "outbox.csv"
            _write_csv(
                in_csv,
                [
                    {
                        "prospect_id": "p1",
                        "first_name": "Alex",
                        "last_name": "One",
                        "firm": "Co",
                        "title": "Ops",
                        "email": "a@example.com",
                        "state": "FL",
                        "city": "Miami",
                        "territory_code": "X",
                        "source": "s",
                        "notes": "",
                    }
                ],
            )

            env = {"UNSUB_ENDPOINT_BASE": "https://unsub.example.internal/unsubscribe", "UNSUB_SECRET": "test_secret"}
            p = self._run_export(
                tmp,
                input_csv=in_csv,
                out_csv=out_csv,
                template=REPO_ROOT / "outreach" / "outreach_plain.txt",
                html_template=REPO_ROOT / "outreach" / "outreach_card.html",
                db_path=db_path,
                state="FL",
                batch="TEST_FL",
                env_overrides=env,
            )
            self.assertEqual(p.returncode, 0, msg=p.stderr + "\n" + p.stdout)
            out_rows = _read_csv(out_csv)
            self.assertEqual(len(out_rows), 1)
            body = out_rows[0].get("body") or ""
            html_body = out_rows[0].get("html_body") or ""
            self.assertNotIn("No recent signals in the last 14 days", body)
            self.assertNotIn("Example signals (sample, not state-specific)", body)
            self.assertNotIn("Sample Industrial Services", body)
            self.assertNotIn("Example signals (sample, not state-specific)", html_body)
            self.assertNotIn("Sample Industrial Services", html_body)
            self.assertEqual((out_rows[0].get("subject") or "").strip(), "Quick heads up — FL inspection opened recently")
            self.assertLess(len((out_rows[0].get("subject") or "").strip()), 65)

    def test_clean_company_name_treats_suffix_only_values_as_missing(self):
        from outreach import generate_mailmerge as gm

        self.assertEqual(gm._clean_company_name("Inc."), "")
        self.assertEqual(gm._clean_company_name("Precision Environmental Inc."), "Precision Environmental Inc.")
        self.assertEqual(gm._clean_company_name("TEMPERATURE PRO WEST AUSTIN"), "Temperature Pro West Austin")
        self.assertEqual(gm._clean_first_name("ALONSO,"), "Alonso")

    def test_format_recent_signal_line_normalizes_shouty_establishment_names(self):
        from outreach import generate_mailmerge as gm

        shouty = gm._format_recent_signal_line(
            {
                "establishment_name": "TRI DAL, LLC",
                "site_city": "Frisco",
                "site_state": "TX",
                "inspection_type": "Accident",
                "date_opened": "2026-02-11",
                "first_seen_at": "2026-02-17T00:00:00Z",
            }
        )
        self.assertIn("Tri Dal, LLC", shouty)
        self.assertNotIn("TRI DAL, LLC", shouty)

        suffix_case = gm._format_recent_signal_line(
            {
                "establishment_name": "Tri Dal, Llc",
                "site_city": "Frisco",
                "site_state": "TX",
                "inspection_type": "Accident",
                "date_opened": "2026-02-11",
                "first_seen_at": "2026-02-17T00:00:00Z",
            }
        )
        self.assertIn("Tri Dal, LLC", suffix_case)

        mixed_case = gm._format_recent_signal_line(
            {
                "establishment_name": "McVey Mechanical LLC",
                "site_city": "Austin",
                "site_state": "TX",
                "inspection_type": "Complaint",
                "date_opened": "2026-02-13",
                "first_seen_at": "2026-02-19T00:00:00Z",
            }
        )
        self.assertIn("McVey Mechanical LLC", mixed_case)

    def test_outreach_overlay_flag_off_keeps_mailmerge_schema_without_ai_columns(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            _write_suppression(tmp / "suppression.csv", [])
            tpl = tmp / "tpl.txt"
            _write_template(tpl)
            in_csv = tmp / "in.csv"
            out_csv = tmp / "outbox.csv"
            _write_csv(
                in_csv,
                [
                    {
                        "prospect_id": "p1",
                        "first_name": "A",
                        "last_name": "One",
                        "firm": "Co",
                        "title": "Ops",
                        "email": "a@example.com",
                        "state": "TX",
                        "city": "Austin",
                        "territory_code": "X",
                        "source": "s",
                        "notes": "",
                    }
                ],
            )
            env = {
                "UNSUB_ENDPOINT_BASE": "https://unsub.example.internal/unsubscribe",
                "UNSUB_SECRET": "test_secret",
                "OUTREACH_TRIAGE_OVERLAY_ENABLED": "0",
            }
            p = self._run_export(tmp, input_csv=in_csv, out_csv=out_csv, template=tpl, env_overrides=env)
            self.assertEqual(p.returncode, 0, msg=p.stderr + "\n" + p.stdout)
            manifest = out_csv.with_name(out_csv.stem + "_manifest.csv")
            self.assertIn("ai_triage_action", _csv_fieldnames(out_csv))
            self.assertIn("ai_triage_action", _csv_fieldnames(manifest))
            out_rows = _read_csv(out_csv)
            self.assertEqual((out_rows[0].get("ai_triage_action") or "").strip(), "AI_DISABLED")
            self.assertEqual((out_rows[0].get("ai_triage_details_relpath") or "").strip(), "")
            self.assertFalse((tmp / "outreach" / "TX_W2" / "signals_triage_TX_W2_export.json").exists())

    def test_outreach_overlay_on_filters_examples_and_writes_triage_artifact(self):
        from outreach import generate_mailmerge as gm

        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            _write_suppression(tmp / "suppression.csv", [])
            tpl = tmp / "tpl.txt"
            tpl.write_text("Hi FIRST_NAME\nRECENT_SIGNALS_LINES\nPREFS_URL\n", encoding="utf-8")

            in_csv = tmp / "in.csv"
            out_csv = tmp / "outbox.csv"
            _write_csv(
                in_csv,
                [
                    {
                        "prospect_id": "p1",
                        "first_name": "A",
                        "last_name": "One",
                        "firm": "Co",
                        "title": "Ops",
                        "email": "a@example.com",
                        "state": "TX",
                        "city": "Austin",
                        "territory_code": "X",
                        "source": "s",
                        "notes": "",
                    }
                ],
            )

            db_path = tmp / "db.sqlite"
            conn = sqlite3.connect(str(db_path))
            conn.execute("CREATE TABLE inspections(site_state TEXT, date_opened TEXT, parse_invalid INTEGER)")
            conn.commit()
            conn.close()

            recent_leads = [
                {
                    "activity_nr": "1001",
                    "establishment_name": "Keep Signal Co",
                    "site_city": "Austin",
                    "site_state": "TX",
                    "inspection_type": "Complaint",
                    "date_opened": "2026-02-24",
                    "first_seen_at": "2026-02-24T00:00:00Z",
                },
                {
                    "activity_nr": "1002",
                    "establishment_name": "Remove Signal Co",
                    "site_city": "Dallas",
                    "site_state": "TX",
                    "inspection_type": "Referral",
                    "date_opened": "2026-02-23",
                    "first_seen_at": "2026-02-23T00:00:00Z",
                },
            ]
            triage_decisions = [
                {
                    "activity_nr": "1001",
                    "current_priority": "medium",
                    "action": "keep",
                    "confidence": 0.61,
                    "reasons": ["complaint"],
                    "provenance": {"source": "rules_cached_detail"},
                },
                {
                    "activity_nr": "1002",
                    "current_priority": "medium",
                    "action": "remove_from_customer_email",
                    "confidence": 0.93,
                    "reasons": ["referral", "stale"],
                    "provenance": {"source": "rules_cached_detail"},
                },
            ]

            argv = [
                "generate_mailmerge.py",
                "--input",
                str(in_csv),
                "--batch",
                "TEST_TX",
                "--state",
                "TX",
                "--out",
                str(out_csv),
                "--template",
                str(tpl),
                "--html-template",
                str(REPO_ROOT / "outreach" / "outreach_card.html"),
                "--db",
                str(db_path),
            ]
            env = os.environ.copy()
            env["DATA_DIR"] = str(tmp)
            env["UNSUB_ENDPOINT_BASE"] = "https://unsub.example.internal/unsubscribe"
            env["UNSUB_SECRET"] = "test_secret"
            env["OUTREACH_TRIAGE_OVERLAY_ENABLED"] = "1"
            out = io.StringIO()
            err = io.StringIO()
            with mock.patch.dict(os.environ, env, clear=False), mock.patch.object(
                gm, "_best_effort_recent_leads_and_refresh", return_value=(list(recent_leads), "2026-02-25 09:00 ET")
            ), mock.patch.object(
                gm.scoring_osha_detail_cache, "ensure_cached_for_activities", return_value={"fetched": 0, "skipped_cached": 2, "failed": 0}
            ), mock.patch.object(
                gm.scoring_osha_detail_cache, "load_detail_cache_rows", return_value={}
            ), mock.patch.object(
                gm.scoring_triage_overlay, "triage", return_value=list(triage_decisions)
            ), mock.patch.object(
                gm, "_load_local_suppression_set", return_value=set()
            ), mock.patch.object(
                gm, "_is_suppressed", return_value=False
            ), mock.patch.object(sys, "argv", argv):
                with redirect_stdout(out), redirect_stderr(err):
                    rc = gm.main()

            self.assertEqual(rc, 0, msg=err.getvalue() + "\n" + out.getvalue())
            out_rows = _read_csv(out_csv)
            self.assertEqual(len(out_rows), 1)
            self.assertIn("Keep Signal Co", out_rows[0]["body"])
            self.assertNotIn("Remove Signal Co", out_rows[0]["body"])
            self.assertEqual(out_rows[0]["ai_triage_action"], "REPLACED_SOME")
            self.assertTrue(out_rows[0]["ai_triage_details_relpath"])

            manifest = out_csv.with_name(out_csv.stem + "_manifest.csv")
            self.assertIn("ai_triage_action", _csv_fieldnames(out_csv))
            self.assertIn("ai_triage_details_relpath", _csv_fieldnames(manifest))

            relpath = out_rows[0]["ai_triage_details_relpath"]
            artifact = tmp / relpath
            self.assertTrue(artifact.exists(), msg=f"missing artifact: {artifact}")
            data = json.loads(artifact.read_text(encoding="utf-8"))
            self.assertGreaterEqual(len(data), 2)
            self.assertTrue(any(int(r.get("final_included", 0)) == 0 for r in data))
            self.assertIn("outreach_triage_details=", out.getvalue())

    def test_render_preview_outputs_updated_copy_and_no_artifacts(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            db_path = tmp / "db.sqlite"
            conn = sqlite3.connect(str(db_path))
            conn.execute(
                """
                CREATE TABLE inspections (
                    activity_nr TEXT,
                    date_opened TEXT,
                    inspection_type TEXT,
                    establishment_name TEXT,
                    site_city TEXT,
                    site_state TEXT,
                    lead_score INTEGER,
                    first_seen_at TEXT,
                    last_seen_at TEXT,
                    source_url TEXT,
                    parse_invalid INTEGER
                )
                """
            )
            conn.execute(
                """
                INSERT INTO inspections (
                    activity_nr, date_opened, inspection_type, establishment_name,
                    site_city, site_state, lead_score, first_seen_at, last_seen_at, source_url, parse_invalid
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
                """,
                (
                    "3001",
                    "2099-02-19",
                    "Complaint",
                    "Preview Safety Co",
                    "Los Angeles",
                    "CA",
                    8,
                    "2099-02-19T12:00:00Z",
                    "2099-02-19T12:00:00Z",
                    "https://www.osha.gov/ords/imis/establishment.inspection_detail?id=3001",
                ),
            )
            conn.commit()
            conn.close()

            preview_input = tmp / "preview.csv"
            _write_csv(
                preview_input,
                [
                    {
                        "prospect_id": "p_preview_1",
                        "first_name": "Casey",
                        "last_name": "Preview",
                        "firm": "Northwind Safety",
                        "title": "Managing Partner",
                        "email": "preview@example.com",
                        "state": "CA",
                        "city": "Los Angeles",
                        "territory_code": "X",
                        "source": "s",
                        "notes": "",
                    },
                    {
                        "prospect_id": "p_preview_2",
                        "first_name": "",
                        "last_name": "Preview",
                        "firm": "Acme Industrial",
                        "title": "Managing Partner",
                        "email": "preview2@example.com",
                        "state": "CA",
                        "city": "Los Angeles",
                        "territory_code": "X",
                        "source": "s",
                        "notes": "",
                    },
                    {
                        "prospect_id": "p_preview_3",
                        "first_name": "",
                        "last_name": "Preview",
                        "firm": "",
                        "title": "Managing Partner",
                        "email": "preview3@example.com",
                        "state": "CA",
                        "city": "Los Angeles",
                        "territory_code": "X",
                        "source": "s",
                        "notes": "",
                    },
                    {
                        "prospect_id": "p_preview_4",
                        "first_name": "Riley",
                        "last_name": "Preview",
                        "firm": "Inc.",
                        "title": "Managing Partner",
                        "email": "preview4@example.com",
                        "state": "CA",
                        "city": "Los Angeles",
                        "territory_code": "X",
                        "source": "s",
                        "notes": "",
                    },
                ],
            )
            snapshot_before = sorted(
                str(path.relative_to(tmp)) for path in tmp.rglob("*") if path.is_file()
            )

            p = self._run_preview(
                tmp,
                state="CA",
                input_csv=preview_input,
                db_path=db_path,
                limit=4,
            )
            self.assertEqual(p.returncode, 0, msg=p.stderr + "\n" + p.stdout)
            stdout = p.stdout or ""
            subject_lines = [
                ln.split("SUBJECT:", 1)[1].strip()
                for ln in stdout.splitlines()
                if ln.strip().startswith("SUBJECT:")
            ]
            self.assertGreaterEqual(len(subject_lines), 3, msg=stdout)
            self.assertTrue(all(len(s) < 65 for s in subject_lines), msg=subject_lines)
            self.assertIn("BODY_TEXT_PREVIEW:", stdout)
            self.assertIn("BODY_HTML_PREVIEW:", stdout)
            self.assertIn("COMPLIANCE_CHECKS ", stdout)
            self.assertIn("Hi Casey,", stdout)
            self.assertIn("Hi Riley,", stdout)
            self.assertIn("Hi - saw a few things Acme Industrial should probably have on their radar:", stdout)
            self.assertIn(
                "Hi - saw a few new OSHA inspections in California that might be relevant to your team:",
                stdout,
            )
            self.assertIn(
                "I spotted a few recent OSHA inspections in California that may be relevant to your team at Northwind Safety",
                stdout,
            )
            self.assertIn(
                "I spotted a few recent OSHA inspections in California that may be relevant to your team",
                stdout,
            )
            self.assertNotIn("that may be relevant to your team at Inc.", stdout)
            self.assertNotIn("You're getting this because", stdout)
            self.assertNotIn("Opened = inspection opened date; Observed =", stdout)
            self.assertNotIn("no commitment, no login required", stdout)
            self.assertNotIn("Every item links to the public OSHA record", stdout)
            self.assertIn("unsubscribe_link_count_exactly_one=true", stdout)
            self.assertIn("no_duplicate_unsubscribe_pre_footer=true", stdout)
            self.assertNotIn("Example signals (sample, not state-specific)", stdout)
            self.assertNotIn("Sample Industrial Services", stdout)

            p_missing = self._run_preview(
                tmp,
                state="CA",
                input_csv=None,
                db_path=db_path,
                limit=1,
            )
            self.assertEqual(p_missing.returncode, 0, msg=p_missing.stderr + "\n" + p_missing.stdout)
            self.assertIn(
                "Hi - saw a few new OSHA inspections in California that might be relevant to your team:",
                p_missing.stdout or "",
            )

            snapshot_after = sorted(
                str(path.relative_to(tmp)) for path in tmp.rglob("*") if path.is_file()
            )
            self.assertEqual(snapshot_after, snapshot_before)
            self.assertFalse((tmp / "outreach_export_ledger.jsonl").exists())
            self.assertFalse((tmp / "outbox.csv").exists())
            self.assertFalse((tmp / "outreach" / "outreach_runs").exists())


if __name__ == "__main__":
    unittest.main()
