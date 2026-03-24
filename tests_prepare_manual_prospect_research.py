import csv
import io
import json
import os
import tempfile
import unittest
from contextlib import redirect_stdout
from datetime import datetime
from pathlib import Path
from unittest import mock

from outreach import crm_store
from tools import prepare_manual_prospect_research as prep_tool


class TestPrepareManualProspectResearch(unittest.TestCase):
    def _seed_crm_prospect(self, db_path: Path, *, firm: str, website: str, state: str = "TX", email: str = "") -> None:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        website_host = str(website or "").replace("https://", "").replace("http://", "").split("/", 1)[0].strip().lower()
        effective_email = email or f"{firm.replace(' ', '').lower()}@{website_host or 'seed-mail.test'}"
        conn = crm_store.connect(db_path)
        try:
            crm_store.init_schema(conn)
            conn.execute(
                """
                INSERT INTO prospects(
                    prospect_id, firm, contact_name, email, title, city, state, website, source,
                    source_fit_tier, default_send_eligible, email_status, enrichment_lane,
                    score, status, created_at
                ) VALUES (?, ?, '', ?, '', '', ?, ?, 'seed', 'recoverable_consultant', 1, '', '', 0, 'new', ?)
                """,
                (
                    f"seed_{firm}_{state}".replace(" ", "_").lower(),
                    firm,
                    effective_email,
                    state,
                    website,
                    "2026-03-06T00:00:00+00:00",
                ),
            )
            conn.commit()
        finally:
            conn.close()

    def test_print_config_uses_active_state_scope_and_canonical_paths(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            data_dir = tmp / "runtime"
            db_path = data_dir / "crm.sqlite"
            self._seed_crm_prospect(db_path, firm="Known Safety Group", website="https://knowncrm.test", state="TX")
            self._seed_crm_prospect(db_path, firm="Ohio Safety Lab", website="https://ohiosafety.test", state="OH")

            out = io.StringIO()
            env = dict(os.environ)
            env["DATA_DIR"] = str(data_dir)
            env["OUTREACH_STATES"] = "TX,CA,FL,PA,OH"
            with (
                mock.patch.dict(os.environ, env, clear=False),
                mock.patch.object(
                    prep_tool,
                    "_local_now",
                    return_value=datetime.fromisoformat("2026-03-23T09:15:00-04:00"),
                ),
                redirect_stdout(out),
            ):
                rc = prep_tool.main(["--print-config"])

            self.assertEqual(rc, 0, msg=out.getvalue())
            text = out.getvalue()
            self.assertIn("MANUAL_PROSPECT_RESEARCH_STATES_SCOPE=TX,CA,FL,PA,OH", text)
            self.assertIn("MANUAL_PROSPECT_RESEARCH_TARGET_FIRMS=50", text)
            self.assertIn("MANUAL_PROSPECT_RESEARCH_SKIP_LIST_ROWS=2", text)
            self.assertIn(
                f"MANUAL_PROSPECT_RESEARCH_SKIP_LIST_PATH={(data_dir / 'audits' / 'prospect_ai_assist' / 'crm_skip_list_for_ai.csv').resolve()}",
                text,
            )
            self.assertIn(
                f"MANUAL_PROSPECT_RESEARCH_PROMPT_OUTPUT_PATH={(data_dir / 'audits' / 'prospect_ai_assist' / 'manual_prospect_deep_research_20260323.txt').resolve()}",
                text,
            )
            self.assertIn("For PA/OH, do not rely on STATE_LIC.", text)
            self.assertIn("PASS_MANUAL_PROSPECT_RESEARCH_PRINT_CONFIG status=OK", text)
            self.assertFalse((data_dir / "audits" / "prospect_ai_assist" / "crm_skip_list_for_ai.csv").exists())
            self.assertFalse((data_dir / "audits" / "prospect_ai_assist" / "manual_prospect_deep_research_20260323.txt").exists())

    def test_dry_run_allows_state_override_and_no_writes(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            data_dir = tmp / "runtime"
            db_path = data_dir / "crm.sqlite"
            self._seed_crm_prospect(db_path, firm="Penn Safety", website="https://penn-safety.test", state="PA")

            out = io.StringIO()
            env = dict(os.environ)
            env["DATA_DIR"] = str(data_dir)
            env["OUTREACH_STATES"] = "TX,CA,FL,PA,OH"
            with (
                mock.patch.dict(os.environ, env, clear=False),
                mock.patch.object(
                    prep_tool,
                    "_local_now",
                    return_value=datetime.fromisoformat("2026-03-23T09:15:00-04:00"),
                ),
                redirect_stdout(out),
            ):
                rc = prep_tool.main(["--dry-run", "--states", "PA,OH", "--target-firms", "12"])

            self.assertEqual(rc, 0, msg=out.getvalue())
            text = out.getvalue()
            self.assertIn("MANUAL_PROSPECT_RESEARCH_STATES_SCOPE=PA,OH", text)
            self.assertIn("MANUAL_PROSPECT_RESEARCH_TARGET_FIRMS=12", text)
            self.assertIn("MANUAL_PROSPECT_RESEARCH_DRY_RUN=1", text)
            self.assertIn("PASS_MANUAL_PROSPECT_RESEARCH_DRY_RUN status=OK", text)
            self.assertFalse((data_dir / "audits" / "prospect_ai_assist" / "crm_skip_list_for_ai.csv").exists())
            self.assertFalse((data_dir / "audits" / "prospect_ai_assist" / "manual_prospect_deep_research_20260323.txt").exists())

    def test_live_run_refreshes_skip_list_and_prompt_artifact(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            data_dir = tmp / "runtime"
            db_path = data_dir / "crm.sqlite"
            self._seed_crm_prospect(db_path, firm="Known Safety Group", website="https://knowncrm.test", state="TX")
            self._seed_crm_prospect(db_path, firm="Ohio Safety Lab", website="https://ohiosafety.test", state="OH")

            out = io.StringIO()
            env = dict(os.environ)
            env["DATA_DIR"] = str(data_dir)
            env["OUTREACH_STATES"] = "TX,CA,FL,PA,OH"
            with (
                mock.patch.dict(os.environ, env, clear=False),
                mock.patch.object(
                    prep_tool,
                    "_local_now",
                    return_value=datetime.fromisoformat("2026-03-23T09:15:00-04:00"),
                ),
                redirect_stdout(out),
            ):
                rc = prep_tool.main([])

            self.assertEqual(rc, 0, msg=out.getvalue())
            skip_list_path = data_dir / "audits" / "prospect_ai_assist" / "crm_skip_list_for_ai.csv"
            prompt_path = data_dir / "audits" / "prospect_ai_assist" / "manual_prospect_deep_research_20260323.txt"
            self.assertTrue(skip_list_path.exists())
            self.assertTrue(prompt_path.exists())

            with open(skip_list_path, "r", newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(len(rows), 2)

            prompt_text = prompt_path.read_text(encoding="utf-8")
            self.assertIn("Active states: TX,CA,FL,PA,OH", prompt_text)
            self.assertIn("Target firms: 50", prompt_text)
            self.assertIn(str(skip_list_path.resolve()), prompt_text)
            self.assertIn(prep_tool.CSV_HEADER, prompt_text)
            self.assertIn("Important execution rule:", prompt_text)
            self.assertIn("Do not return an executive summary", prompt_text)
            self.assertIn("Return ONLY CSV.", prompt_text)
            self.assertIn("For PA/OH, do not rely on STATE_LIC.", prompt_text)
            self.assertIn("Research source guidance:", prompt_text)
            self.assertIn("PASS_MANUAL_PROSPECT_RESEARCH status=OK", out.getvalue())


if __name__ == "__main__":
    unittest.main()
