import csv
import gc
import io
import json
import os
import re
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest import mock


REPO_ROOT = Path(__file__).resolve().parent
SCRIPT = REPO_ROOT / "outreach" / "run_outreach_auto.py"

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from outreach import crm_store
from outreach import run_outreach_auto as roa


def _write_suppression(path: Path, emails: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["email"])
        w.writeheader()
        for email in emails or []:
            w.writerow({"email": email})


def _seed_crm(path: Path, rows: list[dict]) -> None:
    conn = crm_store.connect(path)
    try:
        crm_store.init_schema(conn)
        cur = conn.cursor()
        for row in rows:
            cur.execute(
                """
                INSERT INTO prospects(
                    prospect_id, firm, contact_name, email, title, city, state, website, source,
                    score, status, created_at, last_contacted_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row["prospect_id"],
                    row.get("firm", ""),
                    row.get("contact_name", ""),
                    row["email"],
                    row.get("title", ""),
                    row.get("city", ""),
                    row.get("state", "TX"),
                    row.get("website", ""),
                    row.get("source", "test"),
                    int(row.get("score", 0)),
                    row.get("status", "new"),
                    row.get("created_at", "2026-01-01T00:00:00+00:00"),
                    row.get("last_contacted_at"),
                ),
            )
        conn.commit()
    finally:
        conn.close()


def _seed_signal_db(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS inspections (
                site_state TEXT,
                date_opened TEXT,
                parse_invalid INTEGER
            )
            """
        )
        conn.execute("DELETE FROM inspections")
        for row in rows:
            conn.execute(
                "INSERT INTO inspections(site_state, date_opened, parse_invalid) VALUES(?, ?, ?)",
                (
                    str(row.get("site_state", "")),
                    str(row.get("date_opened", "")),
                    int(row.get("parse_invalid", 0)),
                ),
            )
        conn.commit()
    finally:
        conn.close()


class TestOutreachRunAuto(unittest.TestCase):
    def _stdout_value(self, stdout: str, key: str) -> str:
        prefix = f"{key}="
        line = next((ln.strip() for ln in (stdout or "").splitlines() if ln.strip().startswith(prefix)), "")
        self.assertTrue(line, msg=f"missing {key} in stdout:\n{stdout}")
        return line.split("=", 1)[1].strip()

    def _run(
        self,
        args: list[str],
        env_overrides: dict[str, str | None],
        base_env: dict[str, str] | None = None,
    ) -> subprocess.CompletedProcess:
        env = dict(base_env) if base_env is not None else os.environ.copy()
        env["PYTHONPATH"] = str(REPO_ROOT)
        for k, v in env_overrides.items():
            if v is None:
                env.pop(k, None)
            else:
                env[k] = v
        return subprocess.run(
            [sys.executable, str(SCRIPT)] + args,
            cwd=str(REPO_ROOT),
            env=env,
            capture_output=True,
            text=True,
        )

    def test_dry_run_prints_selected_ids_and_writes_no_db_changes(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            data_dir = tmp / "data"
            crm_db = data_dir / "crm.sqlite"
            _seed_crm(
                crm_db,
                [
                    {
                        "prospect_id": "p_new",
                        "contact_name": "Alice New",
                        "firm": "ACME",
                        "email": "alice@example.com",
                        "title": "Owner",
                        "state": "TX",
                        "score": 2,
                    },
                    {
                        "prospect_id": "p_old",
                        "contact_name": "Bob Old",
                        "firm": "ACME",
                        "email": "bob@example.com",
                        "title": "Safety Manager",
                        "state": "TX",
                        "score": 2,
                        "status": "contacted",
                        "last_contacted_at": "2026-01-05T00:00:00+00:00",
                    },
                    {
                        "prospect_id": "p_sup",
                        "contact_name": "Cara Sup",
                        "firm": "ACME",
                        "email": "suppressed@example.com",
                        "title": "Founder",
                        "state": "TX",
                        "score": 1,
                    },
                ],
            )
            _write_suppression(data_dir / "suppression.csv", emails=["suppressed@example.com"])

            env = {
                "DATA_DIR": str(data_dir),
                "OUTREACH_STATES": "TX",
                "OUTREACH_DAILY_LIMIT": "10",
                "OSHA_SMOKE_TO": "allow@example.com",
                "UNSUB_ENDPOINT_BASE": None,
                "UNSUB_SECRET": None,
            }
            p = self._run(["--dry-run"], env)
            self.assertEqual(p.returncode, 0, msg=p.stderr + "\n" + p.stdout)
            out = p.stdout or ""
            self.assertIn("PASS_AUTO_DRY_RUN", out)
            self.assertIn("would_contact_prospect_ids=p_new", out)
            self.assertIn("skipped_count=2", out)

            conn = sqlite3.connect(str(crm_db))
            try:
                cnt = int(conn.execute("SELECT COUNT(*) FROM outreach_events").fetchone()[0])
                self.assertEqual(cnt, 0)
                last_contacted = conn.execute(
                    "SELECT COALESCE(last_contacted_at, '') FROM prospects WHERE prospect_id = 'p_new'"
                ).fetchone()[0]
                self.assertEqual(last_contacted, "")
            finally:
                conn.close()

    def test_no_repeat_gate_and_allow_repeat_override(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            data_dir = tmp / "data"
            crm_db = data_dir / "crm.sqlite"
            _seed_crm(
                crm_db,
                [
                    {
                        "prospect_id": "p1",
                        "contact_name": "A",
                        "firm": "F",
                        "email": "a@example.com",
                        "title": "Owner",
                        "state": "TX",
                        "score": 1,
                        "status": "contacted",
                        "last_contacted_at": "2026-01-05T00:00:00+00:00",
                    }
                ],
            )
            _write_suppression(data_dir / "suppression.csv")
            env = {
                "DATA_DIR": str(data_dir),
                "OUTREACH_STATES": "TX",
                "OUTREACH_DAILY_LIMIT": "10",
                "OSHA_SMOKE_TO": "allow@example.com",
            }

            p1 = self._run(["--dry-run"], env)
            self.assertEqual(p1.returncode, 0, msg=p1.stderr + "\n" + p1.stdout)
            self.assertIn("would_contact_prospect_ids=(none)", p1.stdout)

            p2 = self._run(["--dry-run", "--allow-repeat"], env)
            self.assertEqual(p2.returncode, 0, msg=p2.stderr + "\n" + p2.stdout)
            self.assertIn("would_contact_prospect_ids=p1", p2.stdout)

    def test_to_mismatch_fails(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            data_dir = tmp / "data"
            crm_db = data_dir / "crm.sqlite"
            _seed_crm(
                crm_db,
                [
                    {
                        "prospect_id": "p1",
                        "contact_name": "A",
                        "firm": "F",
                        "email": "a@example.com",
                        "title": "Owner",
                        "state": "TX",
                    }
                ],
            )
            _write_suppression(data_dir / "suppression.csv")

            env = {
                "DATA_DIR": str(data_dir),
                "OUTREACH_STATES": "TX",
                "OUTREACH_DAILY_LIMIT": "10",
                "OSHA_SMOKE_TO": "allow@example.com",
            }
            p = self._run(["--to", "wrong@example.com"], env)
            self.assertNotEqual(p.returncode, 0)
            self.assertIn("ERR_AUTO_SUMMARY_TO_MISMATCH", (p.stderr or "") + (p.stdout or ""))

    def test_print_config_outputs_resolved_fields(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            data_dir = tmp / "data"
            env = {
                "DATA_DIR": str(data_dir),
                "OUTREACH_STATES": "TX,CA",
                "OSHA_SMOKE_TO": "allow@example.com",
            }
            sub_env = os.environ.copy()
            sub_env.pop("OUTREACH_DAILY_LIMIT", None)
            sub_env.pop("TRIAL_CONVERSION_URL", None)
            p = self._run(["--print-config"], env, base_env=sub_env)
            self.assertEqual(p.returncode, 0, msg=p.stderr + "\n" + p.stdout)

            out = p.stdout or ""
            self.assertIn("PASS_AUTO_PRINT_CONFIG", out)
            self.assertIn(f"data_dir={data_dir.resolve()}", out)
            self.assertIn(f"crm_db={(data_dir / 'crm.sqlite').resolve()}", out)
            self.assertIn(f"suppression_csv={(data_dir / 'suppression.csv').resolve()}", out)
            self.assertIn("outreach_daily_limit=200 source=default", out)
            self.assertIn("outreach_states=TX,CA", out)
            self.assertIn("selected_state=", out)
            self.assertIn("batch_id=", out)
            self.assertIn("trial_conversion_url_present=NO", out)

    def test_print_config_outputs_limit_source_env_and_trial_conversion_present(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            data_dir = tmp / "data"
            env = {
                "DATA_DIR": str(data_dir),
                "OUTREACH_STATES": "TX,CA",
                "OUTREACH_DAILY_LIMIT": "17",
                "OSHA_SMOKE_TO": "allow@example.com",
                "TRIAL_CONVERSION_URL": "https://buy.stripe.com/test123",
            }
            p = self._run(["--print-config"], env)
            self.assertEqual(p.returncode, 0, msg=p.stderr + "\n" + p.stdout)

            out = p.stdout or ""
            self.assertIn("outreach_daily_limit=17 source=env", out)
            self.assertIn("trial_conversion_url_present=YES", out)

    def test_plan_is_deterministic_and_no_db_mutation(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            data_dir = tmp / "data"
            crm_db = data_dir / "crm.sqlite"
            _seed_crm(
                crm_db,
                [
                    {
                        "prospect_id": "p1",
                        "contact_name": "Alice Owner",
                        "firm": "Alpha",
                        "email": "alice@alpha.com",
                        "title": "Owner",
                        "state": "TX",
                        "score": 5,
                        "created_at": "2026-01-01T00:00:00+00:00",
                    },
                    {
                        "prospect_id": "p2",
                        "contact_name": "Bob Safety",
                        "firm": "Bravo",
                        "email": "bob@bravo.com",
                        "title": "Safety Manager",
                        "state": "TX",
                        "score": 8,
                        "created_at": "2026-01-02T00:00:00+00:00",
                    },
                ],
            )
            _write_suppression(data_dir / "suppression.csv")
            env = {
                "DATA_DIR": str(data_dir),
                "OUTREACH_STATES": "TX",
                "OUTREACH_DAILY_LIMIT": "10",
                "OSHA_SMOKE_TO": "allow@example.com",
            }

            p1 = self._run(["--plan", "--for-date", "2026-02-10"], env)
            p2 = self._run(["--plan", "--for-date", "2026-02-10"], env)
            self.assertEqual(p1.returncode, 0, msg=p1.stderr + "\n" + p1.stdout)
            self.assertEqual(p2.returncode, 0, msg=p2.stderr + "\n" + p2.stdout)
            self.assertEqual(p1.stdout, p2.stdout)
            out = p1.stdout or ""
            self.assertIn("OUTREACH_PLAN_DATE=2026-02-10", out)
            self.assertIn("OUTREACH_PLAN_STATE=TX", out)
            self.assertIn("OUTREACH_PLAN_BATCH=2026-02-10_TX", out)
            self.assertIn("OUTREACH_PLAN_SKIP_BREAKDOWN", out)
            self.assertIn("OUTREACH_PLAN_POOL_TOTAL=", out)
            self.assertIn("OUTREACH_PLAN_POOL_TOTAL_ALL_STATES=", out)
            self.assertIn("OUTREACH_PLAN_POOL_TOTAL_SELECTED_STATE=", out)
            self.assertIn("OUTREACH_PLAN_FILTER_BREAKDOWN=", out)
            self.assertIn("OUTREACH_PLAN_DIAGNOSTICS_PATH=", out)
            self.assertIn("prospect_id,email,domain,segment,role_or_title,state_pref,rank_reason", out)
            breakdown_raw = self._stdout_value(out, "OUTREACH_PLAN_FILTER_BREAKDOWN")
            breakdown = json.loads(breakdown_raw)
            self.assertIn("pool_total_all_states", breakdown)
            self.assertIn("pool_total_selected_state", breakdown)
            self.assertIn("eligible", breakdown)
            self.assertIn("selected", breakdown)
            self.assertIn("filters", breakdown)
            self.assertIn("gates", breakdown)
            diagnostics_path = Path(self._stdout_value(out, "OUTREACH_PLAN_DIAGNOSTICS_PATH"))
            self.assertTrue(diagnostics_path.exists(), msg=f"missing diagnostics sidecar: {diagnostics_path}")
            with open(diagnostics_path, "r", encoding="utf-8") as f:
                diagnostics = json.load(f)
            for key in [
                "plan_date",
                "state",
                "batch_id",
                "daily_limit",
                "will_send",
                "pool_total_all_states",
                "pool_total_selected_state",
                "skip_breakdown",
                "filter_breakdown",
                "generated_at_utc",
            ]:
                self.assertIn(key, diagnostics)

            conn = sqlite3.connect(str(crm_db))
            try:
                events_count = int(conn.execute("SELECT COUNT(*) FROM outreach_events").fetchone()[0])
                self.assertEqual(events_count, 0)
                last_contacted = conn.execute(
                    "SELECT COALESCE(last_contacted_at, '') FROM prospects WHERE prospect_id = 'p1'"
                ).fetchone()[0]
                self.assertEqual(last_contacted, "")
            finally:
                conn.close()

    def test_plan_will_send_zero_reports_pool_totals_and_state_mismatch(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            data_dir = tmp / "data"
            crm_db = data_dir / "crm.sqlite"
            _seed_crm(
                crm_db,
                [
                    {
                        "prospect_id": "p_tx1",
                        "contact_name": "Alice TX",
                        "firm": "TX Co",
                        "email": "alice.tx@example.com",
                        "title": "Owner",
                        "state": "TX",
                        "score": 7,
                    },
                    {
                        "prospect_id": "p_tx2",
                        "contact_name": "Bob TX",
                        "firm": "TX Co",
                        "email": "bob.tx@example.com",
                        "title": "Safety Manager",
                        "state": "TX",
                        "score": 6,
                    },
                ],
            )
            _write_suppression(data_dir / "suppression.csv")

            env = {
                "DATA_DIR": str(data_dir),
                "OUTREACH_STATES": "TX,CA",
                "OUTREACH_DAILY_LIMIT": "10",
                "OSHA_SMOKE_TO": "allow@example.com",
            }
            plan = self._run(["--plan", "--for-date", "2001-01-02"], env)
            self.assertEqual(plan.returncode, 0, msg=plan.stderr + "\n" + plan.stdout)
            out = plan.stdout or ""
            self.assertIn("OUTREACH_PLAN_STATE=CA", out)
            self.assertIn("OUTREACH_PLAN_WILL_SEND=0", out)
            self.assertIn(
                "OUTREACH_PLAN_SKIP_BREAKDOWN suppressed=0 invalid_email=0 do_not_contact=0 already_contacted=0 other=0",
                out,
            )
            pool_all = int(self._stdout_value(out, "OUTREACH_PLAN_POOL_TOTAL_ALL_STATES"))
            pool_selected = int(self._stdout_value(out, "OUTREACH_PLAN_POOL_TOTAL_SELECTED_STATE"))
            pool_alias = int(self._stdout_value(out, "OUTREACH_PLAN_POOL_TOTAL"))
            self.assertGreater(pool_all, 0)
            self.assertEqual(pool_selected, 0)
            self.assertEqual(pool_alias, 0)

            breakdown = json.loads(self._stdout_value(out, "OUTREACH_PLAN_FILTER_BREAKDOWN"))
            self.assertEqual(int(breakdown.get("selected", -1)), 0)
            self.assertEqual(int(breakdown.get("pool_total_selected_state", -1)), 0)
            self.assertGreater(int((breakdown.get("gates") or {}).get("state_mismatch", 0)), 0)
            self.assertIs((breakdown.get("gates") or {}).get("weekend_block"), False)

    def test_for_date_changes_state_for_no_send_and_blocks_live_non_today(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            data_dir = tmp / "data"
            crm_db = data_dir / "crm.sqlite"
            _seed_crm(
                crm_db,
                [
                    {
                        "prospect_id": "p_tx",
                        "contact_name": "Alice TX",
                        "firm": "TX Co",
                        "email": "tx@example.com",
                        "title": "Owner",
                        "state": "TX",
                        "score": 7,
                    },
                    {
                        "prospect_id": "p_ca",
                        "contact_name": "Bob CA",
                        "firm": "CA Co",
                        "email": "ca@example.com",
                        "title": "Owner",
                        "state": "CA",
                        "score": 7,
                    },
                ],
            )
            _write_suppression(data_dir / "suppression.csv")

            env = {
                "DATA_DIR": str(data_dir),
                "OUTREACH_STATES": "TX,CA",
                "OUTREACH_DAILY_LIMIT": "10",
                "OSHA_SMOKE_TO": "allow@example.com",
            }
            plan = self._run(["--plan", "--for-date", "2001-01-02"], env)
            self.assertEqual(plan.returncode, 0, msg=plan.stderr + "\n" + plan.stdout)
            self.assertIn("OUTREACH_PLAN_STATE=CA", plan.stdout or "")
            self.assertIn("OUTREACH_PLAN_BATCH=2001-01-02_CA", plan.stdout or "")

            dry_run = self._run(["--dry-run", "--for-date", "2001-01-02"], env)
            self.assertEqual(dry_run.returncode, 0, msg=dry_run.stderr + "\n" + dry_run.stdout)
            self.assertIn("state=CA", dry_run.stdout or "")
            self.assertIn("batch=2001-01-02_CA", dry_run.stdout or "")
            self.assertIn("would_contact_prospect_ids=p_ca", dry_run.stdout or "")

            live = self._run(["--for-date", "2001-01-02"], env)
            self.assertNotEqual(live.returncode, 0)
            self.assertIn("ERR_AUTO_FOR_DATE_LIVE_SEND_BLOCKED", (live.stderr or "") + (live.stdout or ""))

            conn = sqlite3.connect(str(crm_db))
            try:
                events_count = int(conn.execute("SELECT COUNT(*) FROM outreach_events").fetchone()[0])
                self.assertEqual(events_count, 0)
            finally:
                conn.close()

    def test_dry_run_ca_fl_rendered_content_has_no_texas_copy(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            data_dir = tmp / "data"
            crm_db = data_dir / "crm.sqlite"
            _seed_crm(
                crm_db,
                [
                    {
                        "prospect_id": "p_tx",
                        "contact_name": "Alice TX",
                        "firm": "Northwind Safety",
                        "email": "alice.tx@example.com",
                        "title": "Owner",
                        "city": "Dallas",
                        "state": "TX",
                        "score": 7,
                    },
                    {
                        "prospect_id": "p_ca",
                        "contact_name": "Casey CA",
                        "firm": "Pacific Compliance",
                        "email": "casey.ca@example.com",
                        "title": "Safety Director",
                        "city": "Irvine",
                        "state": "CA",
                        "score": 8,
                    },
                    {
                        "prospect_id": "p_fl",
                        "contact_name": "Fran FL",
                        "firm": "Coastal Risk",
                        "email": "fran.fl@example.com",
                        "title": "Managing Partner",
                        "city": "Tampa",
                        "state": "FL",
                        "score": 8,
                    },
                ],
            )
            _write_suppression(data_dir / "suppression.csv")
            env = {
                "DATA_DIR": str(data_dir),
                "OUTREACH_STATES": "TX,CA,FL",
                "OUTREACH_DAILY_LIMIT": "10",
                "OSHA_SMOKE_TO": "allow@example.com",
            }

            template_text = roa.gm._read_template_text(REPO_ROOT / "outreach" / "outreach_plain.txt")
            html_template_text = roa.gm._read_template_text(REPO_ROOT / "outreach" / "outreach_card.html")
            banned_patterns = [
                r"\btexas triangle\b",
                r"serve texas",
                r"opened in tx",
                r"houston,\s*dfw",
            ]

            with mock.patch.object(
                roa.gm,
                "_build_urls",
                return_value=("https://unsubscribe.example/u", "https://unsubscribe.example/prefs"),
            ):
                for run_date, expected_state in [("2026-02-17", "CA"), ("2026-02-18", "FL")]:
                    dry_run = self._run(["--dry-run", "--for-date", run_date], env)
                    gc.collect()
                    dry_run_stdout = dry_run.stdout or ""
                    dry_run_stderr = dry_run.stderr or ""
                    self.assertEqual(dry_run.returncode, 0, msg=dry_run_stderr + "\n" + dry_run_stdout)
                    self.assertIn(f"state={expected_state}", dry_run_stdout or "")
                    self.assertIn("outbox_path=", dry_run_stdout or "")

                    outbox_line = next(
                        (ln.strip() for ln in (dry_run_stdout or "").splitlines() if "outbox_path=" in ln),
                        "",
                    )
                    self.assertTrue(outbox_line, msg=dry_run_stdout)
                    outbox_path = Path(outbox_line.split("outbox_path=", 1)[1].strip())
                    self.assertTrue(outbox_path.exists(), msg=f"missing outbox: {outbox_path}")

                    with open(outbox_path, "r", newline="", encoding="utf-8") as f:
                        outbox_rows = list(csv.DictReader(f))
                    self.assertGreater(len(outbox_rows), 0, msg=f"empty outbox: {outbox_path}")

                    batch = f"{run_date}_{expected_state}"
                    conn = sqlite3.connect(str(crm_db))
                    try:
                        conn.row_factory = sqlite3.Row
                        for outbox_row in outbox_rows:
                            prospect_id = (outbox_row.get("prospect_id") or "").strip()
                            self.assertTrue(prospect_id, msg=f"missing prospect_id row: {outbox_row}")
                            row = conn.execute("SELECT * FROM prospects WHERE prospect_id = ?", (prospect_id,)).fetchone()
                            self.assertIsNotNone(row, msg=f"missing prospect in crm: {prospect_id}")

                            subject, text_body, html_body, _unsub = roa._render_outreach_payload(
                                row=row,
                                state=expected_state,
                                batch=batch,
                                template_text=template_text,
                                html_template_text=html_template_text,
                                recent_signals_lines="- Example Co (Miami, FL) | Programmed | Opened 2026-02-18 | Observed 2026-02-18",
                                recent_signals_html="<div>Example Co &middot; Observed 2026-02-18</div>",
                                last_refresh_et="2026-02-18 08:00 ET",
                                signal_tokens={
                                    "STATE_FULL_NAME": "California" if expected_state == "CA" else "Florida",
                                    "STATE_METRO_EXAMPLES": "Los Angeles, Inland Empire"
                                    if expected_state == "CA"
                                    else "Miami, Orlando",
                                    "SIGNALS_WINDOW_NOTE_TEXT": "Opened = inspection opened date; Observed = first day it appeared in our feed.",
                                    "SIGNALS_WINDOW_NOTE_HTML": "<span>Opened = inspection opened date; Observed = first day it appeared in our feed.</span>",
                                    "SIGNALS_FALLBACK_TEXT": "",
                                    "SIGNALS_FALLBACK_HTML": "",
                                },
                            )
                            self.assertEqual(subject, f"{expected_state} OSHA inspection signals (daily brief)")
                            self.assertIn(f"Recent OSHA inspections opened in {expected_state}:", text_body)
                            rendered = "\n".join([subject, text_body, html_body]).lower()
                            for pattern in banned_patterns:
                                self.assertIsNone(
                                    re.search(pattern, rendered),
                                    msg=f"unexpected texas-centric copy pattern={pattern} state={expected_state}",
                                )
                    finally:
                        conn.close()
                    gc.collect()
                    time.sleep(0.05)

    def test_domain_dedupe_and_role_inbox_penalty_ordering_is_deterministic(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            data_dir = tmp / "data"
            crm_db = data_dir / "crm.sqlite"
            _seed_crm(
                crm_db,
                [
                    {
                        "prospect_id": "p_dm_low",
                        "contact_name": "Low Owner",
                        "firm": "One",
                        "email": "owner@one.com",
                        "title": "Owner",
                        "state": "TX",
                        "score": 4,
                        "created_at": "2026-01-01T00:00:00+00:00",
                    },
                    {
                        "prospect_id": "p_ops_high",
                        "contact_name": "Ops High",
                        "firm": "Two",
                        "email": "ops@two.com",
                        "title": "Compliance Manager",
                        "state": "TX",
                        "score": 10,
                        "created_at": "2026-01-03T00:00:00+00:00",
                    },
                    {
                        "prospect_id": "p_domain_personal",
                        "contact_name": "Jane Owner",
                        "firm": "Gamma",
                        "email": "jane@gamma.com",
                        "title": "Owner",
                        "state": "TX",
                        "score": 4,
                        "created_at": "2026-01-02T00:00:00+00:00",
                    },
                    {
                        "prospect_id": "p_domain_role",
                        "contact_name": "Info Owner",
                        "firm": "Gamma",
                        "email": "info@gamma.com",
                        "title": "Owner",
                        "state": "TX",
                        "score": 9,
                        "created_at": "2026-01-04T00:00:00+00:00",
                    },
                ],
            )
            _write_suppression(data_dir / "suppression.csv")
            env = {
                "DATA_DIR": str(data_dir),
                "OUTREACH_STATES": "TX",
                "OUTREACH_DAILY_LIMIT": "10",
                "OSHA_SMOKE_TO": "allow@example.com",
            }
            plan_1 = self._run(["--plan", "--for-date", "2026-02-10"], env)
            plan_2 = self._run(["--plan", "--for-date", "2026-02-10"], env)
            self.assertEqual(plan_1.returncode, 0, msg=plan_1.stderr + "\n" + plan_1.stdout)
            self.assertEqual(plan_2.returncode, 0, msg=plan_2.stderr + "\n" + plan_2.stdout)
            self.assertEqual(plan_1.stdout, plan_2.stdout)

            lines = [ln.strip() for ln in (plan_1.stdout or "").splitlines() if ln.strip()]
            candidate_lines = [
                ln
                for ln in lines
                if ln.startswith("p_dm_low,")
                or ln.startswith("p_ops_high,")
                or ln.startswith("p_domain_personal,")
                or ln.startswith("p_domain_role,")
            ]
            joined = "\n".join(candidate_lines)
            self.assertIn("p_domain_personal,", joined)
            self.assertNotIn("p_domain_role,", joined)

            index_dm_low = joined.find("p_dm_low,")
            index_ops_high = joined.find("p_ops_high,")
            self.assertNotEqual(index_dm_low, -1, msg=joined)
            self.assertNotEqual(index_ops_high, -1, msg=joined)
            self.assertLess(index_dm_low, index_ops_high, msg=joined)

            dry_run = self._run(["--dry-run", "--for-date", "2026-02-10"], env)
            self.assertEqual(dry_run.returncode, 0, msg=dry_run.stderr + "\n" + dry_run.stdout)
            out = dry_run.stdout or ""
            self.assertIn("manifest_path=", out)
            manifest_line = next((ln for ln in out.splitlines() if "manifest_path=" in ln), "")
            manifest_path = Path(manifest_line.split("manifest_path=", 1)[1].strip())
            self.assertTrue(manifest_path.exists(), msg=f"missing manifest: {manifest_path}")
            with open(manifest_path, "r", newline="", encoding="utf-8") as f:
                rows = list(csv.DictReader(f))
            dropped_role = [r for r in rows if (r.get("prospect_id") or "") == "p_domain_role"]
            self.assertEqual(len(dropped_role), 1)
            self.assertEqual((dropped_role[0].get("reason") or ""), "domain_dedup")
            for field in ["domain", "segment", "role_or_title", "state_pref", "rank_reason"]:
                self.assertIn(field, rows[0], msg=f"missing manifest field {field}")

    def test_dry_run_writes_plan_diagnostics_sidecar_and_prints_path(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            data_dir = tmp / "data"
            crm_db = data_dir / "crm.sqlite"
            _seed_crm(
                crm_db,
                [
                    {
                        "prospect_id": "p1",
                        "contact_name": "Alice Owner",
                        "firm": "Alpha",
                        "email": "alice@alpha.com",
                        "title": "Owner",
                        "state": "TX",
                        "score": 5,
                    }
                ],
            )
            _write_suppression(data_dir / "suppression.csv")
            env = {
                "DATA_DIR": str(data_dir),
                "OUTREACH_STATES": "TX",
                "OUTREACH_DAILY_LIMIT": "10",
                "OSHA_SMOKE_TO": "allow@example.com",
            }
            dry_run = self._run(["--dry-run", "--for-date", "2026-02-10"], env)
            self.assertEqual(dry_run.returncode, 0, msg=dry_run.stderr + "\n" + dry_run.stdout)
            out = dry_run.stdout or ""
            diagnostics_path = Path(self._stdout_value(out, "OUTREACH_PLAN_DIAGNOSTICS_PATH"))
            self.assertTrue(diagnostics_path.exists(), msg=f"missing diagnostics sidecar: {diagnostics_path}")
            with open(diagnostics_path, "r", encoding="utf-8") as f:
                diagnostics = json.load(f)
            self.assertIn("filter_breakdown", diagnostics)
            self.assertIn("skip_breakdown", diagnostics)
            self.assertEqual((diagnostics.get("state") or "").strip(), "TX")

    def test_doctor_missing_env_returns_aggregated_err_and_remediation(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            data_dir = tmp / "data"
            crm_db = data_dir / "crm.sqlite"
            _seed_crm(
                crm_db,
                [
                    {
                        "prospect_id": "p1",
                        "contact_name": "A",
                        "firm": "F",
                        "email": "a@example.com",
                        "title": "Owner",
                        "state": "TX",
                    }
                ],
            )
            _write_suppression(data_dir / "suppression.csv")

            env = {
                "DATA_DIR": str(data_dir),
                "OUTREACH_STATES": None,
                "OUTREACH_DAILY_LIMIT": "10",
                "OSHA_SMOKE_TO": "allow@example.com",
                "OUTREACH_SUPPRESSION_MAX_AGE_HOURS": "240",
            }
            with mock.patch.dict(os.environ, {}, clear=False):
                for key, value in env.items():
                    if value is None:
                        os.environ.pop(key, None)
                    else:
                        os.environ[key] = value

                with mock.patch.object(roa, "_doctor_check_secrets_decrypt", return_value=(True, "")):
                    with mock.patch.object(sys, "argv", ["run_outreach_auto.py", "--doctor"]):
                        out = io.StringIO()
                        err = io.StringIO()
                        with redirect_stdout(out), redirect_stderr(err):
                            rc = roa.main()

            self.assertEqual(rc, 2)
            err_lines = [ln.strip() for ln in (err.getvalue() or "").splitlines() if ln.strip()]
            self.assertEqual(len(err_lines), 2, msg=err.getvalue())
            self.assertEqual(err_lines[0], "ERR_DOCTOR_ENV_MISSING keys=OUTREACH_STATES")
            self.assertEqual(
                err_lines[1],
                "Remediation: pwsh -NoProfile -ExecutionPolicy Bypass -File scripts\\set_outreach_env.ps1",
            )

    def test_doctor_missing_env_multiple_keys_preserves_order(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            data_dir = tmp / "data"
            crm_db = data_dir / "crm.sqlite"
            _seed_crm(
                crm_db,
                [
                    {
                        "prospect_id": "p1",
                        "contact_name": "A",
                        "firm": "F",
                        "email": "a@example.com",
                        "title": "Owner",
                        "state": "TX",
                    }
                ],
            )
            _write_suppression(data_dir / "suppression.csv")

            env = {
                "DATA_DIR": str(data_dir),
                "OUTREACH_STATES": None,
                "OUTREACH_DAILY_LIMIT": "10",
                "OSHA_SMOKE_TO": None,
                "OUTREACH_SUPPRESSION_MAX_AGE_HOURS": "240",
            }
            with mock.patch.dict(os.environ, {}, clear=False):
                for key, value in env.items():
                    if value is None:
                        os.environ.pop(key, None)
                    else:
                        os.environ[key] = value

                with mock.patch.object(roa, "_doctor_check_secrets_decrypt", return_value=(True, "")):
                    with mock.patch.object(sys, "argv", ["run_outreach_auto.py", "--doctor"]):
                        out = io.StringIO()
                        err = io.StringIO()
                        with redirect_stdout(out), redirect_stderr(err):
                            rc = roa.main()

            self.assertEqual(rc, 2)
            err_lines = [ln.strip() for ln in (err.getvalue() or "").splitlines() if ln.strip()]
            self.assertEqual(len(err_lines), 2, msg=err.getvalue())
            self.assertEqual(err_lines[0], "ERR_DOCTOR_ENV_MISSING keys=OUTREACH_STATES,OSHA_SMOKE_TO")

    def test_doctor_for_date_is_forwarded_to_dry_run_artifact_check(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            data_dir = tmp / "data"
            crm_db = data_dir / "crm.sqlite"
            _seed_crm(
                crm_db,
                [
                    {
                        "prospect_id": "p_new",
                        "contact_name": "Alice New",
                        "firm": "ACME",
                        "email": "alice@example.com",
                        "title": "Owner",
                        "state": "TX",
                        "score": 2,
                    }
                ],
            )
            _write_suppression(data_dir / "suppression.csv")

            env = {
                "DATA_DIR": str(data_dir),
                "OUTREACH_STATES": "TX",
                "OUTREACH_DAILY_LIMIT": "10",
                "OSHA_SMOKE_TO": "allow@example.com",
                "OUTREACH_SUPPRESSION_MAX_AGE_HOURS": "240",
            }
            captured: dict[str, str] = {}
            with mock.patch.dict(os.environ, env, clear=False):
                with mock.patch.object(roa, "_doctor_context_pack_soft_check") as m_context, mock.patch.object(
                    roa, "_doctor_check_secrets_decrypt"
                ) as m_secrets, mock.patch.object(roa, "_doctor_check_unsub") as m_unsub, mock.patch.object(
                    roa, "_doctor_check_provider"
                ) as m_provider, mock.patch.object(roa, "_doctor_check_signal_freshness") as m_signals, mock.patch.object(
                    roa, "_doctor_check_dry_run_artifact"
                ) as m_dry_run:
                    m_context.side_effect = lambda: None
                    m_secrets.side_effect = lambda: (print("PASS_DOCTOR_SECRETS_DECRYPT diagnostics=ok"), (True, ""))[1]
                    m_unsub.side_effect = lambda: (print("PASS_DOCTOR_UNSUB version_status=200 unsubscribe_status=400"), (True, ""))[1]
                    m_provider.side_effect = lambda: (print("PASS_DOCTOR_PROVIDER_CONFIG smtp_port=465"), (True, ""))[1]
                    m_signals.side_effect = lambda ctx, run_date: (print("DOCTOR_SIGNALS_STATE=TX recent_14d=1 max_date=2026-02-17 status=OK"), (True, ""))[1]

                    def _capture_dry_run(allow_repeat: bool = False, run_date=None):
                        captured["run_date"] = str(getattr(run_date, "isoformat", lambda: "")())
                        print("PASS_DOCTOR_DRY_RUN_ARTIFACT dry_run_token=PASS_AUTO_DRY_RUN")
                        return True, ""

                    m_dry_run.side_effect = _capture_dry_run

                    with mock.patch.object(sys, "argv", ["run_outreach_auto.py", "--doctor", "--for-date", "2001-01-02"]):
                        out = io.StringIO()
                        err = io.StringIO()
                        with redirect_stdout(out), redirect_stderr(err):
                            rc = roa.main()

            self.assertEqual(rc, 0, msg=err.getvalue() + "\n" + out.getvalue())
            self.assertEqual(captured.get("run_date"), "2001-01-02")

    def test_doctor_success_pass_tokens_only_and_no_db_mutation(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            data_dir = tmp / "data"
            crm_db = data_dir / "crm.sqlite"
            _seed_crm(
                crm_db,
                [
                    {
                        "prospect_id": "p_new",
                        "contact_name": "Alice New",
                        "firm": "ACME",
                        "email": "alice@example.com",
                        "title": "Owner",
                        "state": "TX",
                        "score": 2,
                    }
                ],
            )
            _write_suppression(data_dir / "suppression.csv")

            conn = sqlite3.connect(str(crm_db))
            try:
                before_events = int(conn.execute("SELECT COUNT(*) FROM outreach_events").fetchone()[0])
                before_last_contacted = conn.execute(
                    "SELECT COALESCE(last_contacted_at, '') FROM prospects WHERE prospect_id = 'p_new'"
                ).fetchone()[0]
            finally:
                conn.close()

            env = {
                "DATA_DIR": str(data_dir),
                "OUTREACH_STATES": "TX",
                "OUTREACH_DAILY_LIMIT": "10",
                "OSHA_SMOKE_TO": "allow@example.com",
                "OUTREACH_SUPPRESSION_MAX_AGE_HOURS": "240",
            }
            with mock.patch.dict(os.environ, env, clear=False):
                with mock.patch.object(roa, "_doctor_context_pack_soft_check") as m_context, mock.patch.object(
                    roa, "_doctor_check_secrets_decrypt"
                ) as m_secrets, mock.patch.object(roa, "_doctor_check_unsub") as m_unsub, mock.patch.object(
                    roa, "_doctor_check_provider"
                ) as m_provider, mock.patch.object(roa, "_doctor_check_signal_freshness") as m_signals, mock.patch.object(
                    roa, "_doctor_check_dry_run_artifact"
                ) as m_dry_run:
                    m_context.side_effect = lambda: None
                    m_secrets.side_effect = lambda: (print("PASS_DOCTOR_SECRETS_DECRYPT diagnostics=ok"), (True, ""))[1]
                    m_unsub.side_effect = lambda: (print("PASS_DOCTOR_UNSUB version_status=200 unsubscribe_status=400"), (True, ""))[1]
                    m_provider.side_effect = lambda: (print("PASS_DOCTOR_PROVIDER_CONFIG smtp_port=465"), (True, ""))[1]
                    m_signals.side_effect = lambda ctx, run_date: (print("DOCTOR_SIGNALS_STATE=TX recent_14d=1 max_date=2026-02-17 status=OK"), (True, ""))[1]
                    m_dry_run.side_effect = lambda allow_repeat=False, run_date=None: (
                        print("PASS_DOCTOR_DRY_RUN_ARTIFACT dry_run_token=PASS_AUTO_DRY_RUN"),
                        (True, ""),
                    )[1]

                    with mock.patch.object(sys, "argv", ["run_outreach_auto.py", "--doctor"]):
                        out = io.StringIO()
                        err = io.StringIO()
                        with redirect_stdout(out), redirect_stderr(err):
                            rc = roa.main()

            self.assertEqual(rc, 0, msg=err.getvalue() + "\n" + out.getvalue())
            self.assertEqual((err.getvalue() or "").strip(), "")
            out_lines = [ln.strip() for ln in (out.getvalue() or "").splitlines() if ln.strip()]
            self.assertGreater(len(out_lines), 0)
            for line in out_lines:
                self.assertFalse(line.startswith("ERR_DOCTOR_"), msg=line)
            self.assertTrue(any(line.startswith("PASS_DOCTOR_COMPLETE") for line in out_lines))
            self.assertTrue(any(line.startswith("DOCTOR_SIGNALS_STATE=") for line in out_lines))

            conn = sqlite3.connect(str(crm_db))
            try:
                after_events = int(conn.execute("SELECT COUNT(*) FROM outreach_events").fetchone()[0])
                after_last_contacted = conn.execute(
                    "SELECT COALESCE(last_contacted_at, '') FROM prospects WHERE prospect_id = 'p_new'"
                ).fetchone()[0]
            finally:
                conn.close()

            self.assertEqual(before_events, after_events)
            self.assertEqual(before_last_contacted, after_last_contacted)

    def test_doctor_context_pack_warn_lines_do_not_fail(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            data_dir = tmp / "data"
            crm_db = data_dir / "crm.sqlite"
            _seed_crm(
                crm_db,
                [
                    {
                        "prospect_id": "p_new",
                        "contact_name": "Alice New",
                        "firm": "ACME",
                        "email": "alice@example.com",
                        "title": "Owner",
                        "state": "TX",
                        "score": 2,
                    }
                ],
            )
            _write_suppression(data_dir / "suppression.csv")

            env = {
                "DATA_DIR": str(data_dir),
                "OUTREACH_STATES": "TX",
                "OUTREACH_DAILY_LIMIT": "10",
                "OSHA_SMOKE_TO": "allow@example.com",
                "OUTREACH_SUPPRESSION_MAX_AGE_HOURS": "240",
            }
            with mock.patch.dict(os.environ, env, clear=False):
                with mock.patch.object(roa, "_doctor_context_pack_soft_check") as m_context, mock.patch.object(
                    roa, "_doctor_check_secrets_decrypt"
                ) as m_secrets, mock.patch.object(roa, "_doctor_check_unsub") as m_unsub, mock.patch.object(
                    roa, "_doctor_check_provider"
                ) as m_provider, mock.patch.object(roa, "_doctor_check_signal_freshness") as m_signals, mock.patch.object(
                    roa, "_doctor_check_dry_run_artifact"
                ) as m_dry_run:

                    def _fake_context_warn() -> None:
                        print("WARN_CONTEXT_PACK_STALE SOURCE_HASHES mismatch")
                        print("Upload PROJECT_CONTEXT_PACK.md to ChatGPT Project Settings -> Files")
                        print("Then run: py -3 tools/project_context_pack.py --mark-uploaded")

                    m_context.side_effect = _fake_context_warn
                    m_secrets.side_effect = lambda: (print("PASS_DOCTOR_SECRETS_DECRYPT diagnostics=ok"), (True, ""))[1]
                    m_unsub.side_effect = lambda: (print("PASS_DOCTOR_UNSUB version_status=200 unsubscribe_status=400"), (True, ""))[1]
                    m_provider.side_effect = lambda: (print("PASS_DOCTOR_PROVIDER_CONFIG smtp_port=465"), (True, ""))[1]
                    m_signals.side_effect = lambda ctx, run_date: (print("DOCTOR_SIGNALS_STATE=TX recent_14d=1 max_date=2026-02-17 status=OK"), (True, ""))[1]
                    m_dry_run.side_effect = lambda allow_repeat=False, run_date=None: (
                        print("PASS_DOCTOR_DRY_RUN_ARTIFACT dry_run_token=PASS_AUTO_DRY_RUN"),
                        (True, ""),
                    )[1]

                    with mock.patch.object(sys, "argv", ["run_outreach_auto.py", "--doctor"]):
                        out = io.StringIO()
                        err = io.StringIO()
                        with redirect_stdout(out), redirect_stderr(err):
                            rc = roa.main()

            self.assertEqual(rc, 0, msg=err.getvalue() + "\n" + out.getvalue())
            self.assertEqual((err.getvalue() or "").strip(), "")
            text = out.getvalue()
            self.assertIn("WARN_CONTEXT_PACK_STALE", text)
            self.assertIn("Upload PROJECT_CONTEXT_PACK.md to ChatGPT Project Settings -> Files", text)
            self.assertIn("PASS_DOCTOR_COMPLETE", text)

    def test_doctor_signals_fresh_and_stale_emit_warn_only_and_exit_zero(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            data_dir = tmp / "data"
            crm_db = data_dir / "crm.sqlite"
            signal_db = tmp / "signals.sqlite"
            _seed_crm(
                crm_db,
                [
                    {
                        "prospect_id": "p_new",
                        "contact_name": "Alice New",
                        "firm": "ACME",
                        "email": "alice@example.com",
                        "title": "Owner",
                        "state": "TX",
                    }
                ],
            )
            _seed_signal_db(
                signal_db,
                [
                    {"site_state": "TX", "date_opened": "2026-02-17", "parse_invalid": 0},
                ],
            )
            _write_suppression(data_dir / "suppression.csv")
            env = {
                "DATA_DIR": str(data_dir),
                "OUTREACH_STATES": "TX,CA",
                "OUTREACH_DAILY_LIMIT": "10",
                "OSHA_SMOKE_TO": "allow@example.com",
                "OUTREACH_SUPPRESSION_MAX_AGE_HOURS": "240",
                "OUTREACH_SIGNAL_DB": str(signal_db),
            }
            with mock.patch.dict(os.environ, env, clear=False):
                with mock.patch.object(roa, "_doctor_context_pack_soft_check", return_value=None), mock.patch.object(
                    roa, "_doctor_check_secrets_decrypt", return_value=(True, "")
                ), mock.patch.object(roa, "_doctor_check_unsub", return_value=(True, "")), mock.patch.object(
                    roa, "_doctor_check_provider", return_value=(True, "")
                ), mock.patch.object(roa, "_doctor_check_dry_run_artifact", return_value=(True, "")):
                    with mock.patch.object(sys, "argv", ["run_outreach_auto.py", "--doctor", "--for-date", "2026-02-18"]):
                        out = io.StringIO()
                        err = io.StringIO()
                        with redirect_stdout(out), redirect_stderr(err):
                            rc = roa.main()

            self.assertEqual(rc, 0, msg=err.getvalue() + "\n" + out.getvalue())
            text = out.getvalue()
            self.assertIn("DOCTOR_SIGNALS_DB_PATH=", text)
            self.assertIn("DOCTOR_SIGNALS_LOOKBACK_DAYS=14", text)
            self.assertIn("DOCTOR_SIGNALS_STATE=TX recent_14d=1 max_date=2026-02-17 status=OK", text)
            self.assertIn("DOCTOR_SIGNALS_STATE=CA recent_14d=0 max_date=NONE status=STALE", text)
            self.assertIn("WARN_SIGNALS_STALE states=CA", text)
            self.assertIn("WARN_SIGNALS_REMEDIATION run_ingest=", text)
            self.assertIn("WARN_SIGNALS_REMEDIATION verify_task=schtasks.exe /Query /TN \\OSHA_Osha_Ingest_Daily /V /FO LIST", text)
            self.assertIn("PASS_DOCTOR_COMPLETE", text)

    def test_doctor_signals_missing_db_warn_only_and_exit_zero(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            data_dir = tmp / "data"
            crm_db = data_dir / "crm.sqlite"
            missing_db = tmp / "missing_signals.sqlite"
            _seed_crm(
                crm_db,
                [
                    {
                        "prospect_id": "p_new",
                        "contact_name": "Alice New",
                        "firm": "ACME",
                        "email": "alice@example.com",
                        "title": "Owner",
                        "state": "TX",
                    }
                ],
            )
            _write_suppression(data_dir / "suppression.csv")
            env = {
                "DATA_DIR": str(data_dir),
                "OUTREACH_STATES": "TX",
                "OUTREACH_DAILY_LIMIT": "10",
                "OSHA_SMOKE_TO": "allow@example.com",
                "OUTREACH_SUPPRESSION_MAX_AGE_HOURS": "240",
                "OUTREACH_SIGNAL_DB": str(missing_db),
            }
            with mock.patch.dict(os.environ, env, clear=False):
                with mock.patch.object(roa, "_doctor_context_pack_soft_check", return_value=None), mock.patch.object(
                    roa, "_doctor_check_secrets_decrypt", return_value=(True, "")
                ), mock.patch.object(roa, "_doctor_check_unsub", return_value=(True, "")), mock.patch.object(
                    roa, "_doctor_check_provider", return_value=(True, "")
                ), mock.patch.object(roa, "_doctor_check_dry_run_artifact", return_value=(True, "")):
                    with mock.patch.object(sys, "argv", ["run_outreach_auto.py", "--doctor"]):
                        out = io.StringIO()
                        err = io.StringIO()
                        with redirect_stdout(out), redirect_stderr(err):
                            rc = roa.main()

            self.assertEqual(rc, 0, msg=err.getvalue() + "\n" + out.getvalue())
            text = out.getvalue()
            self.assertIn("DOCTOR_SIGNALS_STATE=TX recent_14d=0 max_date=NONE status=MISSING_DB", text)
            self.assertIn("WARN_SIGNALS_STALE states=TX", text)
            self.assertIn("PASS_DOCTOR_COMPLETE", text)

    def test_doctor_signals_for_date_controls_window(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            data_dir = tmp / "data"
            crm_db = data_dir / "crm.sqlite"
            signal_db = tmp / "signals.sqlite"
            _seed_crm(
                crm_db,
                [
                    {
                        "prospect_id": "p_new",
                        "contact_name": "Alice New",
                        "firm": "ACME",
                        "email": "alice@example.com",
                        "title": "Owner",
                        "state": "TX",
                    }
                ],
            )
            _seed_signal_db(
                signal_db,
                [
                    {"site_state": "TX", "date_opened": "2001-01-02", "parse_invalid": 0},
                ],
            )
            _write_suppression(data_dir / "suppression.csv")
            env = {
                "DATA_DIR": str(data_dir),
                "OUTREACH_STATES": "TX",
                "OUTREACH_DAILY_LIMIT": "10",
                "OSHA_SMOKE_TO": "allow@example.com",
                "OUTREACH_SUPPRESSION_MAX_AGE_HOURS": "240",
                "OUTREACH_SIGNAL_DB": str(signal_db),
            }
            with mock.patch.dict(os.environ, env, clear=False):
                with mock.patch.object(roa, "_doctor_context_pack_soft_check", return_value=None), mock.patch.object(
                    roa, "_doctor_check_secrets_decrypt", return_value=(True, "")
                ), mock.patch.object(roa, "_doctor_check_unsub", return_value=(True, "")), mock.patch.object(
                    roa, "_doctor_check_provider", return_value=(True, "")
                ), mock.patch.object(roa, "_doctor_check_dry_run_artifact", return_value=(True, "")):
                    with mock.patch.object(sys, "argv", ["run_outreach_auto.py", "--doctor", "--for-date", "2001-01-10"]):
                        out_1 = io.StringIO()
                        err_1 = io.StringIO()
                        with redirect_stdout(out_1), redirect_stderr(err_1):
                            rc_1 = roa.main()
                    with mock.patch.object(sys, "argv", ["run_outreach_auto.py", "--doctor", "--for-date", "2001-02-10"]):
                        out_2 = io.StringIO()
                        err_2 = io.StringIO()
                        with redirect_stdout(out_2), redirect_stderr(err_2):
                            rc_2 = roa.main()

            self.assertEqual(rc_1, 0, msg=err_1.getvalue() + "\n" + out_1.getvalue())
            self.assertEqual(rc_2, 0, msg=err_2.getvalue() + "\n" + out_2.getvalue())
            self.assertIn("DOCTOR_SIGNALS_STATE=TX recent_14d=1 max_date=2001-01-02 status=OK", out_1.getvalue())
            self.assertIn("DOCTOR_SIGNALS_STATE=TX recent_14d=0 max_date=2001-01-02 status=STALE", out_2.getvalue())
            self.assertIn("WARN_SIGNALS_STALE states=TX", out_2.getvalue())


if __name__ == "__main__":
    unittest.main()
