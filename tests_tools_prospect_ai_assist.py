import csv
import io
import os
import sqlite3
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest import mock

from outreach import crm_store
from tools import dump_prospect_ai_assist_review as dump_tool
from tools import import_prospect_ai_assist_review as import_tool


class TestProspectAiAssistTools(unittest.TestCase):
    def test_dump_prints_gap_prompt_without_writing_in_dry_run(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            data_dir = tmp / "runtime"
            db_path = data_dir / "crm.sqlite"
            conn = crm_store.connect(db_path)
            try:
                crm_store.init_schema(conn)
            finally:
                conn.close()

            out = io.StringIO()
            env = dict(os.environ)
            env["DATA_DIR"] = str(data_dir)
            env["OUTREACH_STATES"] = "TX"
            env["PROSPECT_AUTOGROW_BACKLOG_TARGET"] = "5"
            env["PROSPECT_AI_ASSIST_MAX_ROWS_PER_STATE"] = "3"
            with mock.patch.dict(os.environ, env, clear=False), redirect_stdout(out):
                rc = dump_tool.main(["--dry-run", "--for-date", "2026-03-07"])

            self.assertEqual(rc, 0)
            text = out.getvalue()
            self.assertIn("AI_ASSIST_DUMP_GAP_STATES=TX", text)
            self.assertIn("AI_ASSIST_DUMP_GAP_TOTAL=5", text)
            self.assertIn("AI_ASSIST_DUMP_CANDIDATES_REQUESTED_TOTAL=3", text)
            self.assertIn("MANUAL AI-ASSIST DISCOVERY AUGMENTATION", text)
            self.assertFalse((data_dir / "audits" / "ai_assist" / "prospect_ai_assist_review_20260307.txt").exists())

    def test_import_dry_run_does_not_create_db(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            data_dir = tmp / "runtime"
            input_path = tmp / "review.csv"
            with open(input_path, "w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(handle, fieldnames=list(import_tool.REQUIRED_COLUMNS))
                writer.writeheader()
                writer.writerow(
                    {
                        "state": "TX",
                        "decision": "accept",
                        "firm": "SafeCo",
                        "website": "https://safeco.example.com",
                        "contact_name": "Taylor Safe",
                        "title": "Owner",
                        "email": "taylor@safeco.example.com",
                        "source_urls": "https://safeco.example.com/contact",
                        "confidence": "92",
                        "evidence_snippet": "Owner listed on consulting firm site",
                    }
                )

            out = io.StringIO()
            env = dict(os.environ)
            env["DATA_DIR"] = str(data_dir)
            with mock.patch.dict(os.environ, env, clear=False), redirect_stdout(out):
                rc = import_tool.main(["--input", str(input_path), "--dry-run", "--batch", "2026-03-07_AIASSIST"])

            self.assertEqual(rc, 0, msg=out.getvalue())
            self.assertIn("AI_ASSIST_VERIFIED_TOTAL=1", out.getvalue())
            self.assertIn("PASS_AI_ASSIST_IMPORT status=DRY_RUN", out.getvalue())
            self.assertFalse((data_dir / "crm.sqlite").exists())

    def test_import_upserts_verified_rows_through_discovery_contract(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            data_dir = tmp / "runtime"
            db_path = data_dir / "crm.sqlite"
            conn = crm_store.connect(db_path)
            try:
                crm_store.init_schema(conn)
                conn.execute(
                    """
                    INSERT INTO prospects(
                        prospect_id, firm, contact_name, email, title, city, state, website, source,
                        source_fit_tier, default_send_eligible, email_status, enrichment_lane,
                        score, status, created_at
                    ) VALUES (
                        'dnc_1', 'Blocked Co', 'Blocked Contact', 'blocked@blockedco.com', 'Owner', '', 'TX', 'https://blockedco.com',
                        'seed', 'recoverable_consultant', 1, '', '', 0, 'do_not_contact', '2026-03-06T00:00:00+00:00'
                    )
                    """
                )
                conn.commit()
            finally:
                conn.close()

            input_path = tmp / "review.csv"
            with open(input_path, "w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(handle, fieldnames=list(import_tool.REQUIRED_COLUMNS))
                writer.writeheader()
                writer.writerow(
                    {
                        "state": "TX",
                        "decision": "accept",
                        "firm": "SafeCo",
                        "website": "https://safeco.example.com",
                        "contact_name": "Taylor Safe",
                        "title": "Owner",
                        "email": "taylor@safeco.example.com",
                        "source_urls": "https://safeco.example.com/contact|https://linkedin.com/company/safeco",
                        "confidence": "92",
                        "evidence_snippet": "Owner listed on consulting firm site",
                    }
                )
                writer.writerow(
                    {
                        "state": "TX",
                        "decision": "accept",
                        "firm": "Blocked Co",
                        "website": "https://blockedco.com",
                        "contact_name": "Blocked Contact",
                        "title": "Owner",
                        "email": "owner@blockedco.com",
                        "source_urls": "https://blockedco.com/contact",
                        "confidence": "80",
                        "evidence_snippet": "Owner on blocked domain",
                    }
                )
                writer.writerow(
                    {
                        "state": "TX",
                        "decision": "accept",
                        "firm": "Personal Mail Co",
                        "website": "https://personalmail.example.com",
                        "contact_name": "Pat Personal",
                        "title": "Owner",
                        "email": "pat@gmail.com",
                        "source_urls": "https://personalmail.example.com/contact",
                        "confidence": "70",
                        "evidence_snippet": "gmail should be rejected",
                    }
                )
                writer.writerow(
                    {
                        "state": "FL",
                        "decision": "reject",
                        "firm": "Skip Co",
                        "website": "https://skipco.example.com",
                        "contact_name": "Skip Person",
                        "title": "Owner",
                        "email": "skip@skipco.example.com",
                        "source_urls": "https://skipco.example.com/contact",
                        "confidence": "10",
                        "evidence_snippet": "operator rejected",
                    }
                )

            out = io.StringIO()
            env = dict(os.environ)
            env["DATA_DIR"] = str(data_dir)
            with mock.patch.dict(os.environ, env, clear=False), redirect_stdout(out):
                rc = import_tool.main(["--input", str(input_path), "--batch", "2026-03-07_AIASSIST"])

            output = out.getvalue()
            self.assertEqual(rc, 0, msg=output)
            self.assertIn("AI_ASSIST_CANDIDATES_TOTAL=4", output)
            self.assertIn("AI_ASSIST_ACCEPTED_TOTAL=3", output)
            self.assertIn("AI_ASSIST_REJECTED_TOTAL=1", output)
            self.assertIn("AI_ASSIST_VERIFIED_TOTAL=1", output)
            self.assertIn("DISCOVERY_SOURCE_COUNT_AI_ASSIST=1", output)
            self.assertIn("PASS_AI_ASSIST_IMPORT status=OK", output)

            conn = crm_store.connect(db_path)
            try:
                prospect = conn.execute(
                    """
                    SELECT email, source, enrichment_lane, source_fit_tier
                    FROM prospects
                    WHERE email = 'taylor@safeco.example.com'
                    """
                ).fetchone()
                self.assertIsNotNone(prospect)
                self.assertEqual(str(prospect[1] or ""), "ai_assist_manual")
                self.assertEqual(str(prospect[2] or ""), "ai_assist")
                self.assertEqual(str(prospect[3] or ""), "recoverable_consultant")

                audit_rows = conn.execute(
                    f"""
                    SELECT email, verification_status, rejection_reason
                    FROM {crm_store.AI_ASSIST_CANDIDATE_TABLE}
                    WHERE batch_id = '2026-03-07_AIASSIST'
                    ORDER BY email
                    """
                ).fetchall()
            finally:
                conn.close()

            self.assertEqual(len(audit_rows), 4)
            audit_map = {str(email or ""): (str(status or ""), str(reason or "")) for email, status, reason in audit_rows}
            self.assertEqual(audit_map["taylor@safeco.example.com"][0], "verified")
            self.assertEqual(audit_map["owner@blockedco.com"][1], "do_not_contact_domain")
            self.assertEqual(audit_map["pat@gmail.com"][1], "free_domain")
            self.assertEqual(audit_map["skip@skipco.example.com"][0], "review_rejected")


if __name__ == "__main__":
    unittest.main()
