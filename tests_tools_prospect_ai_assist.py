import csv
import io
import os
import sqlite3
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest import mock

from outreach import crm_store
from tools import dump_prospect_ai_assist_review as dump_tool
from tools import import_prospect_ai_assist_review as import_tool


class TestProspectAiAssistTools(unittest.TestCase):
    def _write_review_csv(self, path: Path, rows: list[dict[str, str]]) -> None:
        with open(path, "w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(import_tool.REQUIRED_COLUMNS))
            writer.writeheader()
            for row in rows:
                writer.writerow(row)

    def _write_review_lines(self, path: Path, lines: list[str]) -> None:
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")

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

    def test_dump_print_config_uses_default_max_rows_per_state_when_unset(self):
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
            env["PROSPECT_AUTOGROW_BACKLOG_TARGET"] = "45"
            env.pop("PROSPECT_AI_ASSIST_MAX_ROWS_PER_STATE", None)
            with mock.patch.dict(os.environ, env, clear=True), redirect_stdout(out):
                rc = dump_tool.main(["--print-config", "--for-date", "2026-03-07"])

            self.assertEqual(rc, 0)
            text = out.getvalue()
            self.assertIn("AI_ASSIST_DUMP_MAX_ROWS_PER_STATE=40", text)
            self.assertIn("AI_ASSIST_DUMP_GAP_TOTAL=45", text)
            self.assertIn("AI_ASSIST_DUMP_CANDIDATES_REQUESTED_TOTAL=40", text)

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

    def test_load_csv_rows_parses_markdown_review_exports(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            input_path = tmp / "prospect_ai_assist_review_20260309_reviewed.csv"
            self._write_review_lines(
                input_path,
                [
                    ",".join(import_tool.REQUIRED_COLUMNS),
                    "TX,accept,Safety Compliance Management, Inc.,[https://www.scm-safety.com,Paul](https://www.scm-safety.com,Paul) Gantt,President and Founder,[info@scm-safety.com](mailto:info@scm-safety.com),[https://www.scm-safety.com/about|https://www.scm-safety.com/contact,96,President](https://www.scm-safety.com/about|https://www.scm-safety.com/contact,96,President) and founder listed on site",
                ],
            )

            rows = import_tool._load_csv_rows(input_path)

            self.assertEqual(len(rows), 1)
            row = rows[0]
            self.assertEqual(str(row["state"] or ""), "TX")
            self.assertEqual(str(row["firm"] or ""), "Safety Compliance Management, Inc.")
            self.assertEqual(str(row["website"] or ""), "https://www.scm-safety.com")
            self.assertEqual(str(row["contact_name"] or ""), "Paul Gantt")
            self.assertEqual(str(row["title"] or ""), "President and Founder")
            self.assertEqual(str(row["email"] or ""), "info@scm-safety.com")
            self.assertEqual(
                str(row["source_urls"] or ""),
                "https://www.scm-safety.com/about|https://www.scm-safety.com/contact",
            )
            self.assertEqual(str(row["confidence"] or ""), "96")
            self.assertIn("President and founder listed on site", str(row["evidence_snippet"] or ""))

    def test_load_csv_rows_parses_compact_markdown_review_exports(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            input_path = tmp / "prospect_ai_assist_review_20260311_reviewed.csv"
            self._write_review_lines(
                input_path,
                [
                    ",".join(import_tool.REQUIRED_COLUMNS),
                    "CA,accept,Cal Safety Solution,[https://calsafetysolution.com,Jim,Founder,Safetyjim@calsafetysolution.com,https://calsafetysolution.com/|https://calsafetysolution.com/Blogs,90,Homepage](https://calsafetysolution.com,Jim,Founder,Safetyjim@calsafetysolution.com,https://calsafetysolution.com/|https://calsafetysolution.com/Blogs,90,Homepage) says Jim established Cal Safety Solution in 2018; site lists [Safetyjim@calsafetysolution.com](mailto:Safetyjim@calsafetysolution.com)",
                ],
            )

            rows = import_tool._load_csv_rows(input_path)

            self.assertEqual(len(rows), 1)
            row = rows[0]
            self.assertEqual(str(row["firm"] or ""), "Cal Safety Solution")
            self.assertEqual(str(row["website"] or ""), "https://calsafetysolution.com")
            self.assertEqual(str(row["contact_name"] or ""), "Jim")
            self.assertEqual(str(row["title"] or ""), "Founder")
            self.assertEqual(str(row["email"] or ""), "Safetyjim@calsafetysolution.com")
            self.assertEqual(
                str(row["source_urls"] or ""),
                "https://calsafetysolution.com/|https://calsafetysolution.com/Blogs",
            )
            self.assertEqual(str(row["confidence"] or ""), "90")
            self.assertIn("Homepage says Jim established Cal Safety Solution in 2018", str(row["evidence_snippet"] or ""))

    def test_pending_import_discovers_only_reviewed_csvs_oldest_first(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            data_dir = tmp / "runtime"
            review_dir = data_dir / "audits" / "ai_assist"
            review_dir.mkdir(parents=True, exist_ok=True)
            (review_dir / "prospect_ai_assist_review_20260310.txt").write_text("prompt", encoding="utf-8")
            (review_dir / "prospect_ai_assist_review_20260310_reviewed_cleaned.csv").write_text("x", encoding="utf-8")
            first = review_dir / "prospect_ai_assist_review_20260309_reviewed.csv"
            second = review_dir / "prospect_ai_assist_review_20260310_reviewed.csv"
            self._write_review_csv(
                first,
                [
                    {
                        "state": "TX",
                        "decision": "accept",
                        "firm": "One",
                        "website": "https://one.example.com",
                        "contact_name": "One",
                        "title": "Owner",
                        "email": "one@one.example.com",
                        "source_urls": "https://one.example.com/contact",
                        "confidence": "90",
                        "evidence_snippet": "One",
                    }
                ],
            )
            self._write_review_csv(
                second,
                [
                    {
                        "state": "CA",
                        "decision": "accept",
                        "firm": "Two",
                        "website": "https://two.example.com",
                        "contact_name": "Two",
                        "title": "Owner",
                        "email": "two@two.example.com",
                        "source_urls": "https://two.example.com/contact",
                        "confidence": "91",
                        "evidence_snippet": "Two",
                    }
                ],
            )

            calls: list[tuple[str, str, bool]] = []

            def _fake_import(*, input_path: Path, batch_id_override: str = "", dry_run: bool = False):  # type: ignore[no-untyped-def]
                calls.append((str(input_path.name), str(batch_id_override), bool(dry_run)))
                return 0, "DRY_RUN"

            env = dict(os.environ)
            env["DATA_DIR"] = str(data_dir)
            with mock.patch.dict(os.environ, env, clear=False), mock.patch.object(
                import_tool, "_import_review_file", side_effect=_fake_import
            ):
                rc = import_tool.run_pending_imports(dry_run=True)

            self.assertEqual(rc, 0)
            self.assertEqual(
                calls,
                [
                    ("prospect_ai_assist_review_20260309_reviewed.csv", "2026-03-09_AIASSIST", True),
                    ("prospect_ai_assist_review_20260310_reviewed.csv", "2026-03-10_AIASSIST", True),
                ],
            )

    def test_completed_batch_same_hash_is_skipped_cleanly(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            data_dir = tmp / "runtime"
            db_path = data_dir / "crm.sqlite"
            input_path = tmp / "prospect_ai_assist_review_20260309_reviewed.csv"
            self._write_review_csv(
                input_path,
                [
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
                ],
            )

            conn = crm_store.connect(db_path)
            try:
                crm_store.init_schema(conn)
                conn.execute(
                    f"""
                    INSERT INTO {crm_store.AI_ASSIST_IMPORT_BATCH_TABLE}(
                        batch_id, source_path, source_filename, source_file_hash, status, started_at,
                        completed_at, last_error, candidates_total, accepted_total, rejected_total,
                        verified_total, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, '', 1, 1, 0, 1, ?, ?)
                    """,
                    (
                        "2026-03-09_AIASSIST",
                        str(input_path),
                        input_path.name,
                        import_tool._sha256_file(input_path),
                        "completed",
                        "2026-03-09T00:00:00+00:00",
                        "2026-03-09T00:01:00+00:00",
                        "2026-03-09T00:00:00+00:00",
                        "2026-03-09T00:01:00+00:00",
                    ),
                )
                conn.commit()
            finally:
                conn.close()

            out = io.StringIO()
            env = dict(os.environ)
            env["DATA_DIR"] = str(data_dir)
            with mock.patch.dict(os.environ, env, clear=False), redirect_stdout(out):
                rc = import_tool.main(["--input", str(input_path)])

            self.assertEqual(rc, 0, msg=out.getvalue())
            self.assertIn("PASS_AI_ASSIST_IMPORT status=SKIPPED_ALREADY_COMPLETED", out.getvalue())

    def test_batch_hash_drift_fails_fast(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            data_dir = tmp / "runtime"
            db_path = data_dir / "crm.sqlite"
            input_path = tmp / "prospect_ai_assist_review_20260309_reviewed.csv"
            self._write_review_csv(
                input_path,
                [
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
                ],
            )

            conn = crm_store.connect(db_path)
            try:
                crm_store.init_schema(conn)
                conn.execute(
                    f"""
                    INSERT INTO {crm_store.AI_ASSIST_IMPORT_BATCH_TABLE}(
                        batch_id, source_path, source_filename, source_file_hash, status, started_at,
                        completed_at, last_error, candidates_total, accepted_total, rejected_total,
                        verified_total, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, '', 1, 1, 0, 1, ?, ?)
                    """,
                    (
                        "2026-03-09_AIASSIST",
                        str(input_path),
                        input_path.name,
                        "different-hash",
                        "completed",
                        "2026-03-09T00:00:00+00:00",
                        "2026-03-09T00:01:00+00:00",
                        "2026-03-09T00:00:00+00:00",
                        "2026-03-09T00:01:00+00:00",
                    ),
                )
                conn.commit()
            finally:
                conn.close()

            out = io.StringIO()
            err = io.StringIO()
            env = dict(os.environ)
            env["DATA_DIR"] = str(data_dir)
            with mock.patch.dict(os.environ, env, clear=False), redirect_stdout(out), redirect_stderr(err):
                rc = import_tool.main(["--input", str(input_path)])

            self.assertEqual(rc, 2, msg=out.getvalue() + err.getvalue())
            self.assertIn("ERR_AI_ASSIST_IMPORT_DRIFT", err.getvalue())

    def test_failed_batch_rerun_recovers_existing_same_batch_prospect_without_reseeding(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            data_dir = tmp / "runtime"
            db_path = data_dir / "crm.sqlite"
            input_path = tmp / "prospect_ai_assist_review_20260309_reviewed.csv"
            self._write_review_csv(
                input_path,
                [
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
                ],
            )

            env = dict(os.environ)
            env["DATA_DIR"] = str(data_dir)
            prospect_id = import_tool._prospect_id_for_email("taylor@safeco.example.com")

            def _seed_then_fail(_input_path, archive_dir=None, no_archive=False):  # type: ignore[no-untyped-def]
                conn = crm_store.connect(db_path)
                try:
                    crm_store.init_schema(conn)
                    conn.execute(
                        """
                        INSERT INTO prospects(
                            prospect_id, firm, contact_name, email, title, city, state, website, source,
                            source_fit_tier, default_send_eligible, email_status, enrichment_lane,
                            score, status, created_at
                        ) VALUES (?, ?, ?, ?, ?, '', ?, ?, 'ai_assist_manual', 'recoverable_consultant', 1, '', 'ai_assist', 0, 'new', ?)
                        ON CONFLICT(prospect_id) DO UPDATE SET email = excluded.email
                        """,
                        (
                            prospect_id,
                            "SafeCo",
                            "Taylor Safe",
                            "taylor@safeco.example.com",
                            "Owner",
                            "TX",
                            "https://safeco.example.com",
                            "2026-03-09T00:00:00+00:00",
                        ),
                    )
                    conn.commit()
                finally:
                    conn.close()
                return 1

            with mock.patch.dict(os.environ, env, clear=False), mock.patch.object(
                import_tool.crm_admin, "_seed_from_csv", side_effect=_seed_then_fail
            ):
                first_out = io.StringIO()
                with redirect_stdout(first_out):
                    first_rc = import_tool.main(["--input", str(input_path)])
            self.assertEqual(first_rc, 1, msg=first_out.getvalue())

            with mock.patch.dict(os.environ, env, clear=False), mock.patch.object(
                import_tool.crm_admin, "_seed_from_csv", side_effect=AssertionError("should not reseed")
            ):
                second_out = io.StringIO()
                with redirect_stdout(second_out):
                    second_rc = import_tool.main(["--input", str(input_path)])
            self.assertEqual(second_rc, 0, msg=second_out.getvalue())
            self.assertIn("PASS_AI_ASSIST_IMPORT status=OK", second_out.getvalue())

            conn = crm_store.connect(db_path)
            try:
                batch_row = conn.execute(
                    f"SELECT status, verified_total FROM {crm_store.AI_ASSIST_IMPORT_BATCH_TABLE} WHERE batch_id = ?",
                    ("2026-03-09_AIASSIST",),
                ).fetchone()
                audit_row = conn.execute(
                    f"""
                    SELECT verification_status, prospect_id
                    FROM {crm_store.AI_ASSIST_CANDIDATE_TABLE}
                    WHERE batch_id = ? AND email = ?
                    """,
                    ("2026-03-09_AIASSIST", "taylor@safeco.example.com"),
                ).fetchone()
            finally:
                conn.close()

            self.assertEqual(str(batch_row[0] or ""), "completed")
            self.assertEqual(int(batch_row[1] or 0), 1)
            self.assertEqual(str(audit_row[0] or ""), "verified")
            self.assertEqual(str(audit_row[1] or ""), prospect_id)

    def test_legacy_completed_batch_without_tracking_is_backfilled_and_skipped(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            data_dir = tmp / "runtime"
            db_path = data_dir / "crm.sqlite"
            input_path = tmp / "prospect_ai_assist_review_20260308_reviewed.csv"
            self._write_review_csv(
                input_path,
                [
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
                ],
            )

            conn = crm_store.connect(db_path)
            try:
                crm_store.init_schema(conn)
                candidate_key = import_tool._candidate_key(
                    {
                        "firm": "SafeCo",
                        "contact_name": "Taylor Safe",
                        "title": "Owner",
                    },
                    email="taylor@safeco.example.com",
                    domain="safeco.example.com",
                    state="TX",
                )
                conn.execute(
                    f"""
                    INSERT INTO {crm_store.AI_ASSIST_CANDIDATE_TABLE}(
                        batch_id, candidate_key, state, decision, firm, website, domain, contact_name, title,
                        email, source_urls_json, confidence, evidence_snippet, verification_status,
                        rejection_reason, prospect_id, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '', ?, ?, ?)
                    """,
                    (
                        "2026-03-08_AIASSIST",
                        candidate_key,
                        "TX",
                        "accept",
                        "SafeCo",
                        "https://safeco.example.com",
                        "safeco.example.com",
                        "Taylor Safe",
                        "Owner",
                        "taylor@safeco.example.com",
                        "[\"https://safeco.example.com/contact\"]",
                        92,
                        "Owner listed on consulting firm site",
                        "verified",
                        "ai_assist_a1",
                        "2026-03-08T00:00:00+00:00",
                        "2026-03-08T00:00:00+00:00",
                    ),
                )
                conn.commit()
            finally:
                conn.close()

            out = io.StringIO()
            env = dict(os.environ)
            env["DATA_DIR"] = str(data_dir)
            with mock.patch.dict(os.environ, env, clear=False), redirect_stdout(out):
                rc = import_tool.main(["--input", str(input_path), "--batch", "2026-03-08_AIASSIST"])

            self.assertEqual(rc, 0, msg=out.getvalue())
            self.assertIn("PASS_AI_ASSIST_IMPORT status=SKIPPED_ALREADY_COMPLETED", out.getvalue())

            conn = crm_store.connect(db_path)
            try:
                tracking = conn.execute(
                    f"""
                    SELECT status, candidates_total, accepted_total, rejected_total, verified_total
                    FROM {crm_store.AI_ASSIST_IMPORT_BATCH_TABLE}
                    WHERE batch_id = ?
                    """,
                    ("2026-03-08_AIASSIST",),
                ).fetchone()
                audit_count = conn.execute(
                    f"SELECT COUNT(*) FROM {crm_store.AI_ASSIST_CANDIDATE_TABLE} WHERE batch_id = ?",
                    ("2026-03-08_AIASSIST",),
                ).fetchone()
            finally:
                conn.close()

            self.assertIsNotNone(tracking)
            self.assertEqual(str(tracking[0] or ""), "completed")
            self.assertEqual(int(tracking[1] or 0), 1)
            self.assertEqual(int(tracking[2] or 0), 1)
            self.assertEqual(int(tracking[3] or 0), 0)
            self.assertEqual(int(tracking[4] or 0), 1)
            self.assertEqual(int(audit_count[0] or 0), 1)


if __name__ == "__main__":
    unittest.main()
