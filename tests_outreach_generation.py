import csv
import io
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from datetime import date, datetime, timezone
from pathlib import Path
from unittest import mock


REPO_ROOT = Path(__file__).resolve().parent
SCRIPT = REPO_ROOT / "run_prospect_generation.py"


class TestProspectGeneration(unittest.TestCase):
    def _test_env(self, env_overrides: dict[str, str | None]) -> dict[str, str]:
        env = os.environ.copy()
        for key in list(env.keys()):
            if key.startswith("PROSPECT_AUTOGROW_"):
                env.pop(key, None)
        env["PYTHONPATH"] = str(REPO_ROOT)
        for k, v in env_overrides.items():
            if v is None:
                env.pop(k, None)
            else:
                env[k] = v
        return env

    def _run(self, args: list[str], env_overrides: dict[str, str | None]) -> subprocess.CompletedProcess:
        env = self._test_env(env_overrides)
        return subprocess.run(
            [sys.executable, str(SCRIPT)] + args,
            cwd=str(REPO_ROOT),
            env=env,
            capture_output=True,
            text=True,
        )

    def _run_discovery(self, args: list[str], env_overrides: dict[str, str | None]) -> subprocess.CompletedProcess:
        env = self._test_env(env_overrides)
        return subprocess.run(
            [sys.executable, str(REPO_ROOT / "run_prospect_discovery.py")] + args,
            cwd=str(REPO_ROOT),
            env=env,
            capture_output=True,
            text=True,
        )

    def test_module_importable_and_main_callable(self):
        from outreach import run_prospect_generation as generator

        self.assertTrue(callable(getattr(generator, "main", None)))

    def test_print_config_side_effect_free(self):
        with tempfile.TemporaryDirectory() as d:
            data_dir = Path(d) / "data"
            out_path = data_dir / "prospect_discovery" / "prospects_latest.csv"
            inbox_dir = data_dir / "prospect_generation" / "inbox"
            p = self._run(["--print-config", "--for-date", "2026-02-18"], {"DATA_DIR": str(data_dir), "OUTREACH_STATES": "TX,CA,FL"})
            self.assertEqual(p.returncode, 0, msg=p.stderr + "\n" + p.stdout)
            out = p.stdout or ""
            self.assertIn("PASS_GENERATOR_PRINT_CONFIG", out)
            self.assertIn(f"output_path={out_path.resolve()}", out)
            self.assertIn(f"inbox_dir={inbox_dir.resolve()}", out)
            self.assertIn("GENERATOR_AUTOGROW_SELECTED_STATE=FL", out)
            self.assertIn("GENERATOR_AUTOGROW_STATES=TX,CA,FL", out)
            self.assertFalse(out_path.exists(), msg="--print-config must not write output")

    def test_dry_run_no_writes(self):
        with tempfile.TemporaryDirectory() as d:
            data_dir = Path(d) / "data"
            out_path = data_dir / "prospect_discovery" / "prospects_latest.csv"
            p = self._run(["--dry-run"], {"DATA_DIR": str(data_dir)})
            self.assertEqual(p.returncode, 0, msg=p.stderr + "\n" + p.stdout)
            out = p.stdout or ""
            self.assertIn("GENERATOR_OUTPUT_PATH=", out)
            self.assertIn("GENERATOR_ROWS_READ=", out)
            self.assertIn("GENERATOR_ROWS_WRITTEN=", out)
            self.assertIn("GENERATOR_AUTOGROW_ENABLED=0", out)
            self.assertIn("GENERATOR_STATE_BACKLOG_BELOW_TARGET state=TX backlog_current=0 target=60 gap=60", out)
            self.assertIn("GENERATOR_AUTOGROW_DISABLED_BACKLOG_GAP=1 states=TX:60", out)
            self.assertIn("GENERATOR_COMPLETE status=DRY_RUN", out)
            self.assertFalse(out_path.exists(), msg="--dry-run must not write output")

    def test_inbox_rows_merge_into_discovery_output(self):
        with tempfile.TemporaryDirectory() as d:
            data_dir = Path(d) / "data"
            inbox_dir = data_dir / "prospect_generation" / "inbox"
            inbox_dir.mkdir(parents=True, exist_ok=True)
            inbox_csv = inbox_dir / "new.csv"
            with open(inbox_csv, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(
                    f,
                    fieldnames=[
                        "prospect_id",
                        "email",
                        "contact_email",
                        "firm",
                        "company_name",
                        "title",
                        "contact_role",
                        "contact_name",
                        "website",
                        "city",
                        "state",
                        "source",
                    ],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "prospect_id": "custom_inbox_001",
                        "email": "owner@inboxfirm-a.com",
                        "firm": "Inbox Firm A",
                        "title": "Owner",
                        "contact_name": "Owner A",
                        "website": "https://inbox-a.example",
                        "city": "Houston",
                        "state": "TX",
                        "source": "manual_upload",
                    }
                )
                writer.writerow(
                    {
                        "contact_email": "ops@inboxfirm-b.com",
                        "company_name": "Inbox Firm B",
                        "contact_role": "Safety Manager",
                        "city": "Austin",
                        "state": "TX",
                    }
                )

            p = self._run([], {"DATA_DIR": str(data_dir)})
            self.assertEqual(p.returncode, 0, msg=p.stderr + "\n" + p.stdout)
            out = p.stdout or ""
            self.assertIn("GENERATOR_INBOX_FILES_FOUND=1", out)
            self.assertIn("GENERATOR_INBOX_ROWS_READ=2", out)
            self.assertIn("GENERATOR_INBOX_ROWS_ACCEPTED=2", out)
            self.assertIn("GENERATOR_INBOX_FILES_ARCHIVED=1", out)

            out_path = data_dir / "prospect_discovery" / "prospects_latest.csv"
            self.assertTrue(out_path.exists(), msg=f"missing output: {out_path}")
            by_email: dict[str, dict[str, str]] = {}
            with open(out_path, "r", newline="", encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    by_email[(row.get("email") or "").strip().lower()] = row

            row_a = by_email.get("owner@inboxfirm-a.com")
            self.assertIsNotNone(row_a, msg="inbox row A missing from output")
            self.assertEqual((row_a or {}).get("prospect_id"), "custom_inbox_001")
            self.assertEqual((row_a or {}).get("firm"), "Inbox Firm A")
            self.assertEqual((row_a or {}).get("title"), "Owner")
            self.assertEqual((row_a or {}).get("source"), "manual_upload")
            self.assertEqual((row_a or {}).get("contact_name"), "Owner A")
            self.assertEqual((row_a or {}).get("website"), "https://inbox-a.example")

    def test_legacy_inbox_still_supported_with_warning(self):
        with tempfile.TemporaryDirectory() as d:
            data_dir = Path(d) / "data"
            old_inbox = data_dir / "prospect_discovery" / "inbox"
            old_inbox.mkdir(parents=True, exist_ok=True)
            old_csv = old_inbox / "old.csv"
            old_csv.write_text("email,firm,title,city,state\nlegacy@inbox.com,Legacy Firm,Owner,Dallas,TX\n", encoding="utf-8")

            p = self._run([], {"DATA_DIR": str(data_dir)})
            self.assertEqual(p.returncode, 0, msg=p.stderr + "\n" + p.stdout)
            out = p.stdout or ""
            self.assertIn("WARN_INBOX_PATH_DEPRECATED", out)
            self.assertIn("files=1", out)

            processed = old_inbox / "processed" / datetime.now().date().isoformat() / "old.csv"
            self.assertTrue(processed.exists(), msg=f"expected old inbox file moved to {processed}")

    def test_dual_inbox_precedence_new_first(self):
        with tempfile.TemporaryDirectory() as d:
            data_dir = Path(d) / "data"
            new_inbox = data_dir / "prospect_generation" / "inbox"
            old_inbox = data_dir / "prospect_discovery" / "inbox"
            new_inbox.mkdir(parents=True, exist_ok=True)
            old_inbox.mkdir(parents=True, exist_ok=True)

            new_inbox.joinpath("a.csv").write_text(
                "email,firm,title,city,state,source\nshared@example.com,New Priority Firm,Owner,Houston,TX,new_inbox\n",
                encoding="utf-8",
            )
            old_inbox.joinpath("b.csv").write_text(
                "email,firm,title,city,state,source\nshared@example.com,Legacy Firm,Owner,Houston,TX,legacy_inbox\n",
                encoding="utf-8",
            )

            p = self._run([], {"DATA_DIR": str(data_dir)})
            self.assertEqual(p.returncode, 0, msg=p.stderr + "\n" + p.stdout)

            out_path = data_dir / "prospect_discovery" / "prospects_latest.csv"
            row = None
            with open(out_path, "r", newline="", encoding="utf-8") as f:
                for candidate in csv.DictReader(f):
                    if (candidate.get("email") or "").strip().lower() == "shared@example.com":
                        row = candidate
                        break
            self.assertIsNotNone(row)
            self.assertEqual((row or {}).get("firm"), "New Priority Firm")
            self.assertEqual((row or {}).get("source"), "new_inbox")

    def test_dry_run_does_not_archive_or_write_output(self):
        with tempfile.TemporaryDirectory() as d:
            data_dir = Path(d) / "data"
            inbox_dir = data_dir / "prospect_generation" / "inbox"
            inbox_dir.mkdir(parents=True, exist_ok=True)
            inbox_csv = inbox_dir / "new.csv"
            inbox_csv.write_text(
                "email,firm,title,city,state\nowner@dry-run-check.com,Dry Run LLC,Owner,Dallas,TX\n",
                encoding="utf-8",
            )

            out_path = data_dir / "prospect_discovery" / "prospects_latest.csv"
            p = self._run(["--dry-run"], {"DATA_DIR": str(data_dir)})
            self.assertEqual(p.returncode, 0, msg=p.stderr + "\n" + p.stdout)
            out = p.stdout or ""
            self.assertIn("GENERATOR_INBOX_FILES_FOUND=1", out)
            self.assertIn("GENERATOR_INBOX_ROWS_READ=1", out)
            self.assertIn("GENERATOR_INBOX_ROWS_ACCEPTED=1", out)
            self.assertNotIn("GENERATOR_INBOX_FILES_ARCHIVED=", out)
            self.assertTrue(inbox_csv.exists(), msg="dry-run must not move inbox files")
            self.assertFalse(out_path.exists(), msg="dry-run must not write discovery feed")

    def test_inbox_row_overrides_seed_duplicate_email(self):
        import seed_recipients_pools as pools

        deduped = pools.dedupe_rows(pools.TX_POOL)
        cleaned, _stats = pools.apply_hygiene(deduped)
        self.assertGreater(len(cleaned), 0, msg="expected at least one cleaned TX seed row")
        seed_email = str(cleaned[0].get("contact_email") or "").strip().lower()
        self.assertIn("@", seed_email, msg="expected valid seed email")

        with tempfile.TemporaryDirectory() as d:
            data_dir = Path(d) / "data"
            inbox_dir = data_dir / "prospect_generation" / "inbox"
            inbox_dir.mkdir(parents=True, exist_ok=True)
            inbox_csv = inbox_dir / "new.csv"
            inbox_csv.write_text(
                "email,firm,title,city,state,source\n"
                f"{seed_email},Override Inbox Firm,Managing Partner,Austin,TX,inbox_override\n",
                encoding="utf-8",
            )

            p = self._run([], {"DATA_DIR": str(data_dir)})
            self.assertEqual(p.returncode, 0, msg=p.stderr + "\n" + p.stdout)

            out_path = data_dir / "prospect_discovery" / "prospects_latest.csv"
            self.assertTrue(out_path.exists(), msg=f"missing output: {out_path}")
            matched = None
            with open(out_path, "r", newline="", encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    if str(row.get("email") or "").strip().lower() == seed_email:
                        matched = row
                        break
            self.assertIsNotNone(matched, msg=f"missing expected email {seed_email}")
            self.assertEqual((matched or {}).get("firm"), "Override Inbox Firm")
            self.assertEqual((matched or {}).get("title"), "Managing Partner")
            self.assertEqual((matched or {}).get("source"), "inbox_override")

    def test_for_date_controls_selected_state(self):
        with tempfile.TemporaryDirectory() as d:
            data_dir = Path(d) / "data"
            p = self._run(
                ["--print-config", "--for-date", "2026-02-18"],
                {"DATA_DIR": str(data_dir), "OUTREACH_STATES": "TX,CA,FL"},
            )
            self.assertEqual(p.returncode, 0, msg=p.stderr + "\n" + p.stdout)
            self.assertIn("GENERATOR_AUTOGROW_SELECTED_STATE=FL", p.stdout or "")

    def test_autogrow_enabled_backlog_targeted_and_deterministic_slice(self):
        from outreach import crm_store
        from outreach import run_prospect_generation as generator

        with tempfile.TemporaryDirectory() as d:
            data_dir = Path(d) / "data"
            data_dir.mkdir(parents=True, exist_ok=True)

            db_path = data_dir / "crm.sqlite"
            crm_store.ensure_database(path=db_path)
            now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
            conn = crm_store.connect(db_path)
            try:
                for i in range(59):
                    conn.execute(
                        """
                        INSERT INTO prospects(
                          prospect_id, firm, contact_name, email, title, city, state, website, source,
                          score, status, created_at, last_contacted_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            f"fl_existing_{i}",
                            f"Existing {i}",
                            "",
                            f"existing{i}@examplefl.com",
                            "EHS Consultant",
                            "Miami",
                            "FL",
                            "",
                            "seed",
                            0,
                            "new",
                            now,
                            None,
                        ),
                    )
                conn.commit()
            finally:
                conn.close()

            (data_dir / "suppression.csv").write_text("email\n", encoding="utf-8")

            cache_path = data_dir / "prospect_generation" / "cache" / "aiha" / "state_FL.json"
            mocked_fetch_result = {
                "rows": [
                    {
                        "email": "zeta@examplefl.com",
                        "state": "FL",
                        "firm": "Zeta Firm",
                        "title": "EHS Consultant",
                        "city": "Orlando",
                        "source": "aiha_consultants_listing:26-27",
                    },
                    {
                        "email": "alpha@examplefl.com",
                        "state": "FL",
                        "firm": "Alpha Firm",
                        "title": "EHS Consultant",
                        "city": "Tampa",
                        "source": "aiha_consultants_listing:26-27",
                    },
                ],
                "cache_used": False,
                "cache_age_days": 0,
                "cache_path": cache_path,
                "pages_fetched": 2,
                "parse_mode": "TEXT_CONTAINER",
                "diagnostics_path": None,
            }

            env = {
                "DATA_DIR": str(data_dir),
                "OUTREACH_STATES": "FL",
                "PROSPECT_AUTOGROW_ENABLED": "1",
                "PROSPECT_AUTOGROW_SOURCES": "AIHA",
                "PROSPECT_AUTOGROW_BACKLOG_TARGET": "60",
                "PROSPECT_AUTOGROW_MAX_FETCH_PAGES_PER_RUN": "6",
                "PROSPECT_AUTOGROW_HTTP_SLEEP_MS": "800",
            }

            buf = io.StringIO()
            with mock.patch.dict(os.environ, env, clear=False):
                with mock.patch("outreach.run_prospect_generation.prospect_sources_aiha.fetch_aiha_state_rows", return_value=mocked_fetch_result) as mocked_fetch:
                    with redirect_stdout(buf):
                        rc = generator.main(["--for-date", "2026-02-18"])

            self.assertEqual(rc, 0)
            mocked_fetch.assert_called_once()
            kwargs = mocked_fetch.call_args.kwargs
            self.assertEqual(kwargs["state"], "FL")
            self.assertEqual(kwargs["max_pages"], 6)
            self.assertEqual(kwargs["sleep_ms"], 800)

            out = buf.getvalue()
            self.assertIn("GENERATOR_AUTOGROW_ENABLED=1", out)
            self.assertIn("GENERATOR_AUTOGROW_SELECTED_STATE=FL", out)
            self.assertIn("GENERATOR_AUTOGROW_BACKLOG_CURRENT=59", out)
            self.assertIn("GENERATOR_AUTOGROW_NEW_NEEDED=1", out)
            self.assertIn("GENERATOR_AIHA_ROWS_CANDIDATE=2", out)
            self.assertIn("GENERATOR_AIHA_ROWS_ACCEPTED=1", out)

            out_path = data_dir / "prospect_discovery" / "prospects_latest.csv"
            self.assertTrue(out_path.exists())
            emails = []
            with open(out_path, "r", newline="", encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    emails.append((row.get("email") or "").strip().lower())
            self.assertIn("alpha@examplefl.com", emails)
            self.assertNotIn("zeta@examplefl.com", emails)

    def test_autogrow_enabled_backlog_gap_targets_low_nonzero_backlog(self):
        from outreach import crm_store
        from outreach import run_prospect_generation as generator

        with tempfile.TemporaryDirectory() as d:
            data_dir = Path(d) / "data"
            data_dir.mkdir(parents=True, exist_ok=True)

            db_path = data_dir / "crm.sqlite"
            crm_store.ensure_database(path=db_path)
            now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
            conn = crm_store.connect(db_path)
            try:
                for i in range(3):
                    conn.execute(
                        """
                        INSERT INTO prospects(
                          prospect_id, firm, contact_name, email, title, city, state, website, source,
                          score, status, created_at, last_contacted_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            f"ca_existing_{i}",
                            f"Existing CA {i}",
                            "",
                            f"existingca{i}@exampleca.com",
                            "EHS Consultant",
                            "Irvine",
                            "CA",
                            "",
                            "seed",
                            0,
                            "new",
                            now,
                            None,
                        ),
                    )
                conn.commit()
            finally:
                conn.close()

            (data_dir / "suppression.csv").write_text("email\n", encoding="utf-8")

            cache_path = data_dir / "prospect_generation" / "cache" / "aiha" / "state_CA.json"
            aiha_rows = []
            for i in range(80):
                aiha_rows.append(
                    {
                        "email": f"cand{i:03d}@freshca.com",
                        "state": "CA",
                        "firm": f"CA Firm {i:03d}",
                        "title": "EHS Consultant",
                        "city": "San Diego",
                        "source": "aiha_consultants_listing:26-27",
                    }
                )
            mocked_fetch_result = {
                "rows": aiha_rows,
                "cache_used": False,
                "cache_age_days": 0,
                "cache_path": cache_path,
                "pages_fetched": 4,
                "parse_mode": "TEXT_CONTAINER",
                "diagnostics_path": None,
            }

            env = {
                "DATA_DIR": str(data_dir),
                "OUTREACH_STATES": "CA",
                "PROSPECT_AUTOGROW_ENABLED": "1",
                "PROSPECT_AUTOGROW_SOURCES": "AIHA",
                "PROSPECT_AUTOGROW_BACKLOG_TARGET": "60",
                "PROSPECT_AUTOGROW_MAX_FETCH_PAGES_PER_RUN": "10",
                "PROSPECT_AUTOGROW_HTTP_SLEEP_MS": "800",
            }

            buf = io.StringIO()
            with mock.patch.dict(os.environ, env, clear=False):
                with mock.patch(
                    "outreach.run_prospect_generation.prospect_sources_aiha.fetch_aiha_state_rows",
                    return_value=mocked_fetch_result,
                ) as mocked_fetch:
                    with redirect_stdout(buf):
                        rc = generator.main(["--for-date", "2026-02-24"])

            self.assertEqual(rc, 0)
            mocked_fetch.assert_called_once()
            self.assertEqual(mocked_fetch.call_args.kwargs["state"], "CA")

            out = buf.getvalue()
            self.assertIn("GENERATOR_AUTOGROW_SELECTED_STATE=CA", out)
            self.assertIn("GENERATOR_AUTOGROW_BACKLOG_CURRENT=3", out)
            self.assertIn("GENERATOR_AUTOGROW_NEW_NEEDED=57", out)
            self.assertIn("GENERATOR_AUTOGROW_STATE=CA backlog_current=3 new_needed=57", out)
            self.assertIn("GENERATOR_STATE_BACKLOG_BELOW_TARGET state=CA backlog_current=3 target=60 gap=57", out)
            self.assertIn("GENERATOR_AIHA_ROWS_ACCEPTED=57", out)
            self.assertIn("GENERATOR_AUTOGROW_TOTAL_ACCEPTED=57", out)
            self.assertIn("GENERATOR_AUTOGROW_DISABLED_BACKLOG_GAP=0 states=none", out)

    def test_autogrow_enabled_with_empty_sources_emits_explicit_skip_token(self):
        from outreach import run_prospect_generation as generator

        with tempfile.TemporaryDirectory() as d:
            data_dir = Path(d) / "data"
            data_dir.mkdir(parents=True, exist_ok=True)
            (data_dir / "suppression.csv").write_text("email\n", encoding="utf-8")

            env = {
                "DATA_DIR": str(data_dir),
                "OUTREACH_STATES": "CA",
                "PROSPECT_AUTOGROW_ENABLED": "1",
                "PROSPECT_AUTOGROW_SOURCES": "",
                "PROSPECT_AUTOGROW_BACKLOG_TARGET": "60",
            }

            buf = io.StringIO()
            with mock.patch.dict(os.environ, env, clear=False):
                with mock.patch("outreach.run_prospect_generation.prospect_sources_aiha.fetch_aiha_state_rows") as mocked_fetch:
                    with redirect_stdout(buf):
                        rc = generator.main(["--dry-run", "--for-date", "2026-02-24"])

            self.assertEqual(rc, 0)
            mocked_fetch.assert_not_called()

            out = buf.getvalue()
            self.assertIn("GENERATOR_AUTOGROW_ENABLED=1", out)
            self.assertIn("GENERATOR_AUTOGROW_SOURCES=", out)
            self.assertIn("GENERATOR_AUTOGROW_SOURCES_EMPTY=1", out)
            self.assertIn("GENERATOR_AIHA_PAGE_PARSE_MODE=SKIP_NO_SOURCES", out)

    def test_autogrow_multi_source_ohs_bg_emits_tokens_and_shared_reject_buckets(self):
        from outreach import crm_store
        from outreach import run_prospect_generation as generator

        with tempfile.TemporaryDirectory() as d:
            data_dir = Path(d) / "data"
            data_dir.mkdir(parents=True, exist_ok=True)
            (data_dir / "suppression.csv").write_text("email\n", encoding="utf-8")

            db_path = data_dir / "crm.sqlite"
            crm_store.ensure_database(path=db_path)
            conn = crm_store.connect(db_path)
            try:
                conn.execute(
                    """
                    INSERT INTO prospects(
                      prospect_id, firm, contact_name, email, title, city, state, website, source,
                      score, status, created_at, last_contacted_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "crm_fl_1",
                        "Existing CRM",
                        "",
                        "crmdup@examplefl.com",
                        "Owner",
                        "Tampa",
                        "FL",
                        "",
                        "seed",
                        0,
                        "new",
                        datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
                        None,
                    ),
                )
                conn.commit()
            finally:
                conn.close()

            aiha_cache = data_dir / "prospect_generation" / "cache" / "aiha" / "state_FL.json"
            ohs_cache = data_dir / "prospect_generation" / "cache" / "ohs_bg" / "state_FL.json"
            aiha_result = {
                "rows": [
                    {"email": "alpha@examplefl.com", "state": "FL", "firm": "Alpha", "source": "aiha_consultants_listing:1"},
                    {"email": "dup@examplefl.com", "state": "FL", "firm": "Dup AIHA", "source": "aiha_consultants_listing:1"},
                ],
                "cache_used": False,
                "cache_age_days": 0,
                "cache_path": aiha_cache,
                "pages_fetched": 1,
                "parse_mode": "TEXT_CONTAINER",
                "diagnostics_path": None,
            }
            ohs_result = {
                "rows": [
                    {"email": "dup@examplefl.com", "state": "FL", "firm": "Dup OHS", "source": "ohs_buyers_guide:1"},
                    {"email": "crmdup@examplefl.com", "state": "FL", "firm": "CRM Dup", "source": "ohs_buyers_guide:2"},
                    {"email": "bad-email", "state": "FL", "firm": "Bad", "source": "ohs_buyers_guide:3"},
                    {"email": "txperson@exampletx.com", "state": "TX", "firm": "TX Person", "source": "ohs_buyers_guide:4"},
                    {"email": "bravo@examplefl.com", "state": "FL", "firm": "Bravo", "source": "ohs_buyers_guide:5"},
                ],
                "cache_used": False,
                "cache_age_days": 0,
                "cache_path": ohs_cache,
                "pages_fetched": 2,
                "parse_mode": "TEXT",
                "diagnostics_path": None,
            }

            env = {
                "DATA_DIR": str(data_dir),
                "OUTREACH_STATES": "FL",
                "PROSPECT_AUTOGROW_ENABLED": "1",
                "PROSPECT_AUTOGROW_SOURCES": "AIHA,OHS_BG",
                "PROSPECT_AUTOGROW_BACKLOG_TARGET": "4",
                "PROSPECT_AUTOGROW_MAX_FETCH_PAGES_PER_RUN": "6",
                "PROSPECT_AUTOGROW_HTTP_SLEEP_MS": "0",
            }

            buf = io.StringIO()
            with mock.patch.dict(os.environ, env, clear=False):
                with mock.patch(
                    "outreach.run_prospect_generation.prospect_sources_aiha.fetch_aiha_state_rows",
                    return_value=aiha_result,
                ) as mocked_aiha:
                    with mock.patch(
                        "outreach.run_prospect_generation.prospect_sources_ohs_bg.fetch_ohs_bg_state_rows",
                        return_value=ohs_result,
                    ) as mocked_ohs:
                        with redirect_stdout(buf):
                            rc = generator.main(["--for-date", "2026-02-24"])

            self.assertEqual(rc, 0)
            mocked_aiha.assert_called_once()
            mocked_ohs.assert_called_once()
            out = buf.getvalue()
            self.assertIn("GENERATOR_AUTOGROW_SOURCES=AIHA,OHS_BG", out)
            self.assertIn("GENERATOR_AUTOGROW_STATES=FL", out)
            self.assertIn("GENERATOR_OHS_BG_ROWS_CANDIDATE=5", out)
            self.assertIn("GENERATOR_OHS_BG_ROWS_ACCEPTED=1", out)
            self.assertIn("GENERATOR_OHS_BG_REJECTED_INVALID_EMAIL=1", out)
            self.assertIn("GENERATOR_OHS_BG_REJECTED_ALREADY_IN_CRM=1", out)
            self.assertIn("GENERATOR_OHS_BG_REJECTED_STATE_MISMATCH=1", out)
            self.assertIn("GENERATOR_OHS_BG_REJECTED_DUPLICATE_IN_BATCH=1", out)
            self.assertIn(
                "GENERATOR_AUTOGROW_SOURCE_STATE source=AIHA state=FL rows_candidate=2 rows_accepted=2",
                out,
            )
            self.assertIn(
                "GENERATOR_AUTOGROW_SOURCE_STATE source=OHS_BG state=FL rows_candidate=5 rows_accepted=1",
                out,
            )
            self.assertIn("ohs_bg_candidate=5", out)
            self.assertIn("ohs_bg_accepted=1", out)
            self.assertIn("GENERATOR_AUTOGROW_TOTAL_ACCEPTED=3", out)

    def test_autogrow_states_decouple_inventory_build_from_send_rotation(self):
        from outreach import run_prospect_generation as generator

        with tempfile.TemporaryDirectory() as d:
            data_dir = Path(d) / "data"
            data_dir.mkdir(parents=True, exist_ok=True)
            (data_dir / "suppression.csv").write_text("email\n", encoding="utf-8")

            def _aiha_fetch(**kwargs):  # type: ignore[no-untyped-def]
                state = str(kwargs["state"]).upper()
                cache_path = data_dir / "prospect_generation" / "cache" / "aiha" / f"state_{state}.json"
                return {
                    "rows": [
                        {
                            "email": f"{state.lower()}person@example{state.lower()}.com",
                            "state": state,
                            "firm": f"{state} Firm",
                            "source": "aiha_consultants_listing:1",
                        }
                    ],
                    "cache_used": False,
                    "cache_age_days": 0,
                    "cache_path": cache_path,
                    "pages_fetched": 1,
                    "parse_mode": "TEXT_CONTAINER",
                    "diagnostics_path": None,
                }

            env = {
                "DATA_DIR": str(data_dir),
                "OUTREACH_STATES": "CA",
                "PROSPECT_AUTOGROW_STATES": "TX,FL",
                "PROSPECT_AUTOGROW_ENABLED": "1",
                "PROSPECT_AUTOGROW_SOURCES": "AIHA",
                "PROSPECT_AUTOGROW_BACKLOG_TARGET": "1",
                "PROSPECT_AUTOGROW_MAX_FETCH_PAGES_PER_RUN": "2",
                "PROSPECT_AUTOGROW_HTTP_SLEEP_MS": "0",
            }

            buf = io.StringIO()
            with mock.patch.dict(os.environ, env, clear=False):
                with mock.patch(
                    "outreach.run_prospect_generation.prospect_sources_aiha.fetch_aiha_state_rows",
                    side_effect=_aiha_fetch,
                ) as mocked_aiha:
                    with redirect_stdout(buf):
                        rc = generator.main(["--dry-run", "--for-date", "2026-02-24"])

            self.assertEqual(rc, 0)
            self.assertEqual([c.kwargs.get("state") for c in mocked_aiha.call_args_list], ["TX", "FL"])
            out = buf.getvalue()
            self.assertIn("GENERATOR_AUTOGROW_SELECTED_STATE=CA", out)
            self.assertIn("GENERATOR_AUTOGROW_STATES=TX,FL", out)
            self.assertIn("GENERATOR_AUTOGROW_SOURCE_STATE source=AIHA state=TX rows_candidate=1 rows_accepted=1", out)
            self.assertIn("GENERATOR_AUTOGROW_SOURCE_STATE source=AIHA state=FL rows_candidate=1 rows_accepted=1", out)

    def test_depleted_state_can_be_refilled_via_second_source_with_autogrow_states(self):
        from outreach import crm_store
        from outreach import run_prospect_generation as generator

        with tempfile.TemporaryDirectory() as d:
            data_dir = Path(d) / "data"
            data_dir.mkdir(parents=True, exist_ok=True)
            (data_dir / "suppression.csv").write_text("email\n", encoding="utf-8")

            db_path = data_dir / "crm.sqlite"
            crm_store.ensure_database(path=db_path)
            now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
            conn = crm_store.connect(db_path)
            try:
                conn.execute(
                    """
                    INSERT INTO prospects(
                      prospect_id, firm, contact_name, email, title, city, state, website, source,
                      score, status, created_at, last_contacted_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "crm_fl_contacted",
                        "Existing FL Contacted",
                        "",
                        "crmdup@examplefl.com",
                        "Owner",
                        "Tampa",
                        "FL",
                        "",
                        "seed",
                        0,
                        "new",
                        now,
                        now,
                    ),
                )
                conn.commit()
            finally:
                conn.close()

            aiha_cache = data_dir / "prospect_generation" / "cache" / "aiha" / "state_FL.json"
            ohs_cache = data_dir / "prospect_generation" / "cache" / "ohs_bg" / "state_FL.json"
            aiha_result = {
                "rows": [
                    {"email": "crmdup@examplefl.com", "state": "FL", "firm": "CRM Dup", "source": "aiha_consultants_listing:1"},
                    {"email": "bad-email", "state": "FL", "firm": "Bad", "source": "aiha_consultants_listing:2"},
                    {"email": "txperson@exampletx.com", "state": "TX", "firm": "TX", "source": "aiha_consultants_listing:3"},
                ],
                "cache_used": False,
                "cache_age_days": 0,
                "cache_path": aiha_cache,
                "pages_fetched": 1,
                "parse_mode": "TEXT_CONTAINER",
                "diagnostics_path": None,
            }
            ohs_result = {
                "rows": [
                    {"email": "bravo@examplefl.com", "state": "FL", "firm": "Bravo", "source": "ohs_buyers_guide:1"},
                    {"email": "crmdup@examplefl.com", "state": "FL", "firm": "CRM Dup", "source": "ohs_buyers_guide:2"},
                    {"email": "bad-email", "state": "FL", "firm": "Bad", "source": "ohs_buyers_guide:3"},
                    {"email": "txperson@exampletx.com", "state": "TX", "firm": "TX Person", "source": "ohs_buyers_guide:4"},
                    {"email": "bravo@examplefl.com", "state": "FL", "firm": "Bravo Dup", "source": "ohs_buyers_guide:5"},
                ],
                "cache_used": False,
                "cache_age_days": 0,
                "cache_path": ohs_cache,
                "pages_fetched": 2,
                "parse_mode": "TEXT",
                "diagnostics_path": None,
            }

            env = {
                "DATA_DIR": str(data_dir),
                "OUTREACH_STATES": "CA",
                "PROSPECT_AUTOGROW_STATES": "FL",
                "PROSPECT_AUTOGROW_ENABLED": "1",
                "PROSPECT_AUTOGROW_SOURCES": "AIHA,OHS_BG",
                "PROSPECT_AUTOGROW_BACKLOG_TARGET": "1",
                "PROSPECT_AUTOGROW_MAX_FETCH_PAGES_PER_RUN": "6",
                "PROSPECT_AUTOGROW_HTTP_SLEEP_MS": "0",
            }

            buf = io.StringIO()
            with mock.patch.dict(os.environ, env, clear=False):
                with mock.patch(
                    "outreach.run_prospect_generation.prospect_sources_aiha.fetch_aiha_state_rows",
                    return_value=aiha_result,
                ) as mocked_aiha:
                    with mock.patch(
                        "outreach.run_prospect_generation.prospect_sources_ohs_bg.fetch_ohs_bg_state_rows",
                        return_value=ohs_result,
                    ) as mocked_ohs:
                        with redirect_stdout(buf):
                            rc = generator.main(["--for-date", "2026-02-24"])

            self.assertEqual(rc, 0)
            mocked_aiha.assert_called_once()
            mocked_ohs.assert_called_once()
            self.assertEqual(mocked_aiha.call_args.kwargs["state"], "FL")
            self.assertEqual(mocked_ohs.call_args.kwargs["state"], "FL")

            out = buf.getvalue()
            self.assertIn("GENERATOR_AUTOGROW_SELECTED_STATE=CA", out)
            self.assertIn("GENERATOR_AUTOGROW_STATES=FL", out)
            self.assertIn("GENERATOR_AUTOGROW_SOURCE_STATE source=AIHA state=FL rows_candidate=3 rows_accepted=0", out)
            self.assertIn("rejected_invalid_email=1", out)
            self.assertIn("rejected_already_in_crm=1", out)
            self.assertIn("rejected_state_mismatch=1", out)
            self.assertIn("GENERATOR_AUTOGROW_SOURCE_STATE source=OHS_BG state=FL rows_candidate=5 rows_accepted=1", out)
            self.assertIn("rejected_duplicate_in_batch=1", out)

            out_path = data_dir / "prospect_discovery" / "prospects_latest.csv"
            self.assertTrue(out_path.exists(), msg=f"missing output: {out_path}")
            with open(out_path, "r", newline="", encoding="utf-8") as f:
                rows = list(csv.DictReader(f))
            emails = {(row.get("email") or "").strip().lower() for row in rows}
            self.assertIn("bravo@examplefl.com", emails)

    def test_invalid_autogrow_source_fails_fast(self):
        p = self._run(
            ["--dry-run", "--for-date", "2026-02-24"],
            {
                "OUTREACH_STATES": "FL",
                "PROSPECT_AUTOGROW_ENABLED": "1",
                "PROSPECT_AUTOGROW_SOURCES": "AIHA,NOPE",
            },
        )
        self.assertNotEqual(p.returncode, 0)
        self.assertIn("ERR_GENERATOR_FAILED stage=autogrow_config", (p.stderr or "") + (p.stdout or ""))

    def test_transform_mapping_and_invalid_email_exclusion(self):
        from outreach import run_prospect_generation as generator

        rows = [
            {
                "company_name": "Firm A",
                "domain": "f1.com",
                "contact_email": "User@F1.com",
                "contact_role": "Owner",
                "contact_name": "Jane Doe",
                "website": "https://f1.com",
                "city": "Houston",
                "state": "tx",
            },
            {
                "company_name": "Firm B",
                "domain": "f2.com",
                "contact_email": "bad-email",
                "contact_role": "Owner",
                "city": "Austin",
                "state": "TX",
            },
            {
                "company_name": "Firm C",
                "domain": "f3.com",
                "contact_email": "user@f1.com",
                "contact_role": "Partner",
                "city": "Dallas",
                "state": "TX",
            },
        ]
        out = generator._to_discovery_rows(rows)
        self.assertEqual(len(out), 1)
        row = out[0]
        self.assertTrue(str(row["prospect_id"]).startswith("gen_"))
        self.assertEqual(row["firm"], "Firm A")
        self.assertEqual(row["email"], "user@f1.com")
        self.assertEqual(row["title"], "Owner")
        self.assertEqual(row["city"], "Houston")
        self.assertEqual(row["state"], "TX")
        self.assertEqual(row["source"], "seed_recipients_pools")
        self.assertEqual(row["contact_name"], "Jane Doe")
        self.assertEqual(row["website"], "https://f1.com")

    def test_live_generator_then_discovery_then_plan_non_zero_pool(self):
        with tempfile.TemporaryDirectory() as d:
            data_dir = Path(d) / "data"
            suppression = data_dir / "suppression.csv"
            suppression.parent.mkdir(parents=True, exist_ok=True)
            suppression.write_text("email\n", encoding="utf-8")

            p_gen = self._run([], {"DATA_DIR": str(data_dir), "OUTREACH_STATES": "TX"})
            self.assertEqual(p_gen.returncode, 0, msg=p_gen.stderr + "\n" + p_gen.stdout)
            self.assertIn("GENERATOR_COMPLETE status=OK", p_gen.stdout or "")

            p_disc = self._run_discovery(
                [],
                {
                    "DATA_DIR": str(data_dir),
                    "OUTREACH_STATES": "TX",
                    "PROSPECT_DISCOVERY_INPUT": None,
                    "DISCOVERY_INPUT_CSV": None,
                },
            )
            self.assertEqual(p_disc.returncode, 0, msg=p_disc.stderr + "\n" + p_disc.stdout)
            self.assertIn("DISCOVERY_COMPLETE status=OK", p_disc.stdout or "")

            env = os.environ.copy()
            env["PYTHONPATH"] = str(REPO_ROOT)
            env["DATA_DIR"] = str(data_dir)
            env["OUTREACH_STATES"] = "TX"
            p_plan = subprocess.run(
                [sys.executable, str(REPO_ROOT / "run_outreach_auto.py"), "--plan", "--for-date", "2026-02-13"],
                cwd=str(REPO_ROOT),
                env=env,
                capture_output=True,
                text=True,
            )
            self.assertEqual(p_plan.returncode, 0, msg=p_plan.stderr + "\n" + p_plan.stdout)

            pool_total = None
            for line in (p_plan.stdout or "").splitlines():
                if line.startswith("OUTREACH_PLAN_POOL_TOTAL="):
                    pool_total = int((line.split("=", 1)[1] or "0").strip())
                    break
            self.assertIsNotNone(pool_total, msg=p_plan.stdout)
            self.assertGreater(pool_total, 0, msg=p_plan.stdout)

            out_path = data_dir / "prospect_discovery" / "prospects_latest.csv"
            self.assertTrue(out_path.exists(), msg=f"missing output: {out_path}")
            with open(out_path, "r", newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                self.assertIn("prospect_id", reader.fieldnames or [])
                self.assertIn("email", reader.fieldnames or [])
                self.assertIn("source", reader.fieldnames or [])
                self.assertIn("contact_name", reader.fieldnames or [])
                self.assertIn("website", reader.fieldnames or [])


if __name__ == "__main__":
    unittest.main()
