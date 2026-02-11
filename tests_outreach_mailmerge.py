import csv
import os
import re
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


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
            self.assertRegex(body, r"\n- ")
            self.assertIn("Last refresh:", body)
            self.assertIn(" ET", body)
            self.assertTrue(text_body.strip())
            self.assertEqual(body, text_body)
            self.assertTrue(html_body.strip())
            self.assertIn("Recent signals:", html_body)
            self.assertIn("Last refresh:", html_body)

            # Wally-style markers.
            self.assertIn("Chase Chevalier", html_body)
            self.assertIn("11539 Links Dr, Reston, VA 20190", html_body)
            self.assertIn("Priority:", html_body)
            self.assertIn('href="https://www.osha.gov/', html_body)
            self.assertIn('href="https://microflowops.com"', html_body)

            # Single opt-out block in the footer (not duplicated elsewhere).
            self.assertEqual(html_body.count(">Unsubscribe</a>"), 1)
            self.assertEqual(html_body.count(">Manage preferences</a>"), 1)
            self.assertEqual(html_body.count("unsub.example.internal/unsubscribe?token="), 1)
            self.assertEqual(html_body.count("unsub.example.internal/prefs?token="), 1)

            # Ensure one-click links are only in the footer area (after the address line).
            addr_idx = html_body.find("11539 Links Dr, Reston, VA 20190")
            self.assertGreater(addr_idx, 0)
            pre_footer = html_body[:addr_idx]
            self.assertNotIn("unsub.example.internal/unsubscribe?token=", pre_footer)
            self.assertNotIn("unsub.example.internal/prefs?token=", pre_footer)
            self.assertEqual(subject, "TX OSHA activity signals - Co")


if __name__ == "__main__":
    unittest.main()
