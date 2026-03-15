import csv
import io
import json
import os
import sqlite3
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime
from pathlib import Path
from unittest import mock

from outreach import crm_store
from tools import dump_prospect_ai_assist_review as dump_tool
from tools import import_prospect_ai_assist_review as import_tool


class TestProspectAiAssistTools(unittest.TestCase):
    def _seed_crm_prospect(self, db_path: Path, *, firm: str, website: str, state: str = "TX", email: str = "") -> None:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        effective_email = email or f"{firm.replace(' ', '').lower()}@seed-mail.test"
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

    def _write_cache_rows(self, data_dir: Path, source_token: str, state: str, rows: list[dict[str, str]]) -> None:
        cache_root = data_dir / "prospect_generation" / "cache"
        cache_path = dump_tool.generation._source_cache_path_for_state(cache_root, source_token, state)
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(
            json.dumps(
                {
                    "source": source_token,
                    "state": state,
                    "rows": rows,
                }
            ),
            encoding="utf-8",
        )

    def _write_review_csv(self, path: Path, rows: list[dict[str, str]]) -> None:
        with open(path, "w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(import_tool.REQUIRED_COLUMNS))
            writer.writeheader()
            for row in rows:
                writer.writerow(row)

    def _write_review_lines(self, path: Path, lines: list[str]) -> None:
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    def test_dump_packets_candidates_with_dedupe_exclusion_prompts_and_manifest(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            data_dir = tmp / "runtime"
            db_path = data_dir / "crm.sqlite"
            self._seed_crm_prospect(db_path, firm="Known Safety Group", website="https://knowncrm.test", state="TX")
            self._seed_crm_prospect(db_path, firm="Existing Firm LLC", website="https://other-existingcrm.test", state="CA")

            self._write_cache_rows(
                data_dir,
                "AIHA",
                "TX",
                [
                    {
                        "firm": "Alpha Safety LLC",
                        "website": "https://www.alpha-safety.co",
                        "state": "TX",
                        "source": "aiha_consultants_listing:10-11",
                    },
                    {
                        "firm": "Bravo Safety",
                        "website": "https://bravo-safety.co",
                        "state": "TX",
                        "source": "aiha_consultants_listing:12-13",
                    },
                    {
                        "firm": "Known Safety Group",
                        "website": "https://knowncrm.test",
                        "state": "TX",
                        "source": "aiha_consultants_listing:14-15",
                    },
                    {
                        "firm": "Existing Firm LLC",
                        "website": "https://brand-new-safety.co",
                        "state": "TX",
                        "source": "aiha_consultants_listing:16-17",
                    },
                ],
            )
            self._write_cache_rows(
                data_dir,
                "OHS_BG",
                "CA",
                [
                    {
                        "firm": "Alpha Safety",
                        "website": "https://alpha-safety.co",
                        "state": "CA",
                        "source": "ohs_buyers_guide:dup-alpha",
                        "source_url": "https://buyersguide.example.com/alpha",
                    },
                    {
                        "firm": "Charlie Safety",
                        "website": "https://charlie-safety.co",
                        "state": "CA",
                        "source": "ohs_buyers_guide:charlie",
                        "source_url": "https://buyersguide.example.com/charlie",
                    },
                    {
                        "firm": "Delta Safety",
                        "website": "https://delta-safety.co",
                        "state": "CA",
                        "source": "ohs_buyers_guide:delta",
                        "source_url": "https://buyersguide.example.com/delta",
                    },
                ],
            )

            out = io.StringIO()
            packet_dir = tmp / "packets"
            env = dict(os.environ)
            env["DATA_DIR"] = str(data_dir)
            env["PROSPECT_AUTOGROW_STATES"] = "TX,CA"
            env["OUTREACH_STATES"] = "FL"
            env["PROSPECT_AUTOGROW_BACKLOG_TARGET"] = "5"
            with (
                mock.patch.dict(os.environ, env, clear=False),
                mock.patch.object(
                    dump_tool,
                    "_current_run_started_at",
                    return_value=datetime.fromisoformat("2026-03-07T09:10:11.123456-05:00"),
                ),
                redirect_stdout(out),
            ):
                rc = dump_tool.main(
                    [
                        "--for-date",
                        "2026-03-07",
                        "--output-dir",
                        str(packet_dir),
                        "--raw-target",
                        "4",
                        "--packet-size",
                        "2",
                    ]
                )

            self.assertEqual(rc, 0)
            text = out.getvalue()
            self.assertIn("AI_ASSIST_DUMP_STATES_SCOPE=TX,CA", text)
            self.assertIn("AI_ASSIST_PACKET_RAW_TARGET=4", text)
            self.assertIn("AI_ASSIST_PACKET_SIZE=2", text)
            self.assertIn("AI_ASSIST_PACKET_CANDIDATES_TOTAL=4", text)
            self.assertIn("AI_ASSIST_PACKET_ROWS_WRITTEN=4", text)
            self.assertIn("AI_ASSIST_PACKET_FILES_WRITTEN=2", text)

            manifest_path = packet_dir / "manifest.json"
            self.assertTrue(manifest_path.exists())
            self.assertTrue((packet_dir / "prompt_research.txt").exists())
            self.assertTrue((packet_dir / "prompt_review.txt").exists())
            self.assertTrue((packet_dir / "seed_packet_001.csv").exists())
            self.assertTrue((packet_dir / "seed_packet_002.csv").exists())

            prompt_research = (packet_dir / "prompt_research.txt").read_text(encoding="utf-8")
            prompt_review = (packet_dir / "prompt_review.txt").read_text(encoding="utf-8")
            self.assertIn("Visit each firm's website before returning any row.", prompt_research)
            self.assertIn("Return CSV only with this exact header:", prompt_research)
            self.assertIn("Reject any row that uses an inferred contact", prompt_review)

            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(manifest["raw_target"], 4)
            self.assertEqual(manifest["packet_size"], 2)
            self.assertEqual(manifest["rows_written"], 4)
            self.assertEqual(manifest["packet_files_written"], 2)
            self.assertEqual(manifest["run_token"], "R091011123456")
            self.assertEqual(manifest["packets"][0]["suggested_reviewed_filename"], "seed_packet_001_reviewed.csv")
            self.assertEqual(manifest["packets"][0]["suggested_batch_id"], "2026-03-07_AIASSIST_R091011123456_P001")
            self.assertIsNone(manifest["packets"][0]["reviewed_rows"])

            with open(packet_dir / "seed_packet_001.csv", newline="", encoding="utf-8") as handle:
                packet_one_rows = list(csv.DictReader(handle))
            with open(packet_dir / "seed_packet_002.csv", newline="", encoding="utf-8") as handle:
                packet_two_rows = list(csv.DictReader(handle))

            packet_rows = packet_one_rows + packet_two_rows
            firms = [row["firm"] for row in packet_rows]
            self.assertEqual(firms, ["Alpha Safety LLC", "Bravo Safety", "Charlie Safety", "Delta Safety"])
            self.assertNotIn("Known Safety Group", firms)
            self.assertNotIn("Existing Firm LLC", firms)
            self.assertEqual(packet_one_rows[0]["seed_source"], "aiha_consultants_listing:10-11")
            self.assertEqual(
                packet_one_rows[0]["seed_source_url"],
                dump_tool.prospect_sources_aiha.PAGE_URL_TEMPLATE.format(page_id="10-11"),
            )
            self.assertEqual(packet_two_rows[0]["seed_source_url"], "https://buyersguide.example.com/charlie")

    def test_dump_dry_run_emits_shortfall_without_writing_artifacts(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            data_dir = tmp / "runtime"
            db_path = data_dir / "crm.sqlite"
            self._seed_crm_prospect(db_path, firm="Unrelated", website="https://unrelatedcrm.test")
            self._write_cache_rows(
                data_dir,
                "AIHA",
                "TX",
                [
                    {
                        "firm": "Solo Safety",
                        "website": "https://solo-safety.co",
                        "state": "TX",
                        "source": "aiha_consultants_listing:88",
                    }
                ],
            )

            out = io.StringIO()
            packet_dir = tmp / "dry_packets"
            env = dict(os.environ)
            env["DATA_DIR"] = str(data_dir)
            env["OUTREACH_STATES"] = "TX"
            env["PROSPECT_AUTOGROW_BACKLOG_TARGET"] = "5"
            with mock.patch.dict(os.environ, env, clear=False), redirect_stdout(out):
                rc = dump_tool.main(
                    [
                        "--dry-run",
                        "--for-date",
                        "2026-03-07",
                        "--output-dir",
                        str(packet_dir),
                        "--raw-target",
                        "3",
                        "--packet-size",
                        "2",
                    ]
                )

            self.assertEqual(rc, 0)
            text = out.getvalue()
            self.assertIn("AI_ASSIST_PACKET_CANDIDATES_TOTAL=1", text)
            self.assertIn("AI_ASSIST_PACKET_ROWS_WRITTEN=1", text)
            self.assertIn("AI_ASSIST_PACKET_FILES_WRITTEN=1", text)
            self.assertIn("WARN_AI_ASSIST_PACKET_SHORTFALL=1 requested=3 available=1 shortfall=2", text)
            self.assertFalse(packet_dir.exists())

    def test_dump_default_packet_dir_and_batch_ids_are_unique_per_run(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            data_dir = tmp / "runtime"
            self._write_cache_rows(
                data_dir,
                "AIHA",
                "TX",
                [
                    {
                        "firm": "Repeat Safe",
                        "website": "https://repeat-safe.example.com",
                        "state": "TX",
                        "source": "aiha_consultants_listing:55",
                    }
                ],
            )

            env = dict(os.environ)
            env["DATA_DIR"] = str(data_dir)
            env["OUTREACH_STATES"] = "TX"
            env["PROSPECT_AUTOGROW_BACKLOG_TARGET"] = "5"
            out_one = io.StringIO()
            out_two = io.StringIO()
            with mock.patch.dict(os.environ, env, clear=False):
                with (
                    mock.patch.object(
                        dump_tool,
                        "_current_run_started_at",
                        return_value=datetime.fromisoformat("2026-03-07T09:10:11.123456-05:00"),
                    ),
                    redirect_stdout(out_one),
                ):
                    rc_one = dump_tool.main(["--for-date", "2026-03-07", "--raw-target", "1", "--packet-size", "1"])
                with (
                    mock.patch.object(
                        dump_tool,
                        "_current_run_started_at",
                        return_value=datetime.fromisoformat("2026-03-07T09:10:12.654321-05:00"),
                    ),
                    redirect_stdout(out_two),
                ):
                    rc_two = dump_tool.main(["--for-date", "2026-03-07", "--raw-target", "1", "--packet-size", "1"])

            self.assertEqual(rc_one, 0, msg=out_one.getvalue())
            self.assertEqual(rc_two, 0, msg=out_two.getvalue())
            manifest_one = next(
                line.split("=", 1)[1]
                for line in out_one.getvalue().splitlines()
                if line.startswith("AI_ASSIST_DUMP_OUTPUT_PATH=")
            )
            manifest_two = next(
                line.split("=", 1)[1]
                for line in out_two.getvalue().splitlines()
                if line.startswith("AI_ASSIST_DUMP_OUTPUT_PATH=")
            )
            self.assertNotEqual(manifest_one, manifest_two)
            manifest_one_payload = json.loads(Path(manifest_one).read_text(encoding="utf-8"))
            manifest_two_payload = json.loads(Path(manifest_two).read_text(encoding="utf-8"))
            self.assertEqual(manifest_one_payload["run_token"], "R091011123456")
            self.assertEqual(manifest_two_payload["run_token"], "R091012654321")
            self.assertNotEqual(
                manifest_one_payload["packets"][0]["suggested_batch_id"],
                manifest_two_payload["packets"][0]["suggested_batch_id"],
            )
            self.assertTrue(Path(manifest_one_payload["packet_dir"]).exists())
            self.assertTrue(Path(manifest_two_payload["packet_dir"]).exists())

    def test_dump_respects_explicit_non_packet_source_posture_without_fallback(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            data_dir = tmp / "runtime"
            self._write_cache_rows(
                data_dir,
                "AIHA",
                "TX",
                [
                    {
                        "firm": "Should Not Leak In",
                        "website": "https://no-fallback.example.com",
                        "state": "TX",
                        "source": "aiha_consultants_listing:77",
                    }
                ],
            )

            out = io.StringIO()
            env = dict(os.environ)
            env["DATA_DIR"] = str(data_dir)
            env["OUTREACH_STATES"] = "TX"
            env["PROSPECT_AUTOGROW_STATES"] = "TX"
            env["PROSPECT_AUTOGROW_SOURCES"] = "APOLLO"
            env["PROSPECT_AUTOGROW_BACKLOG_TARGET"] = "5"
            with mock.patch.dict(os.environ, env, clear=False), redirect_stdout(out):
                rc = dump_tool.main(["--dry-run", "--for-date", "2026-03-07", "--raw-target", "1", "--packet-size", "1"])

            self.assertEqual(rc, 0, msg=out.getvalue())
            text = out.getvalue()
            self.assertIn("AI_ASSIST_PACKET_SOURCES=none", text)
            self.assertIn("AI_ASSIST_PACKET_CANDIDATES_TOTAL=0", text)
            self.assertIn("AI_ASSIST_PACKET_ROWS_WRITTEN=0", text)
            self.assertIn("WARN_AI_ASSIST_PACKET_NO_ELIGIBLE_SOURCES=1 configured=APOLLO", text)

    def test_dump_excludes_crm_root_domain_matches_from_subdomain_records(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            data_dir = tmp / "runtime"
            db_path = data_dir / "crm.sqlite"
            self._seed_crm_prospect(
                db_path,
                firm="Existing Root Domain Holder",
                website="https://team.rootmatch.example.com",
                state="TX",
            )
            self._write_cache_rows(
                data_dir,
                "AIHA",
                "TX",
                [
                    {
                        "firm": "Fresh Safety",
                        "website": "https://rootmatch.example.com",
                        "state": "TX",
                        "source": "aiha_consultants_listing:91",
                    }
                ],
            )

            out = io.StringIO()
            env = dict(os.environ)
            env["DATA_DIR"] = str(data_dir)
            env["OUTREACH_STATES"] = "TX"
            env["PROSPECT_AUTOGROW_BACKLOG_TARGET"] = "5"
            with mock.patch.dict(os.environ, env, clear=False), redirect_stdout(out):
                rc = dump_tool.main(["--dry-run", "--for-date", "2026-03-07", "--raw-target", "1", "--packet-size", "1"])

            self.assertEqual(rc, 0, msg=out.getvalue())
            text = out.getvalue()
            self.assertIn("AI_ASSIST_PACKET_CANDIDATES_TOTAL=0", text)
            self.assertIn("AI_ASSIST_PACKET_ROWS_WRITTEN=0", text)

    def test_dump_print_config_prefers_autogrow_states_and_packet_defaults(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            data_dir = tmp / "runtime"
            db_path = data_dir / "crm.sqlite"
            self._seed_crm_prospect(db_path, firm="Existing", website="https://existing.example.com")

            out = io.StringIO()
            env = dict(os.environ)
            env["DATA_DIR"] = str(data_dir)
            env["PROSPECT_AUTOGROW_STATES"] = "CA,TX"
            env["OUTREACH_STATES"] = "FL"
            env["PROSPECT_AUTOGROW_BACKLOG_TARGET"] = "45"
            with mock.patch.dict(os.environ, env, clear=True), redirect_stdout(out):
                rc = dump_tool.main(["--print-config", "--for-date", "2026-03-07"])

            self.assertEqual(rc, 0)
            text = out.getvalue()
            self.assertIn("AI_ASSIST_DUMP_STATES_SCOPE=CA,TX", text)
            self.assertIn("AI_ASSIST_PACKET_RAW_TARGET=30", text)
            self.assertIn("AI_ASSIST_PACKET_SIZE=10", text)
            self.assertIn("AI_ASSIST_DUMP_GAP_TOTAL=89", text)
            self.assertIn("AI_ASSIST_DUMP_CANDIDATES_REQUESTED_TOTAL=30", text)

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

    def test_normalize_review_rows_repairs_quoted_markdown_cells(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            input_path = tmp / "prospect_ai_assist_review_20260313_reviewed.csv"
            self._write_review_csv(
                input_path,
                [
                    {
                        "state": "TX",
                        "decision": "accept",
                        "firm": "Berg Compliance Solutions",
                        "website": "[https://bes-corp.com/",
                        "contact_name": "Russell](https://bes-corp.com/%22,%22Russell) Carr",
                        "title": "Founder & CEO",
                        "email": "[rcarr@bes-corp.com](mailto:rcarr@bes-corp.com)",
                        "source_urls": "[https://bes-corp.com/contact-us/our-team/|https://bes-corp.com/osha-texas-compliance-basics/](https://bes-corp.com/contact-us/our-team/|https://bes-corp.com/osha-texas-compliance-basics/)",
                        "confidence": "95",
                        "evidence_snippet": "Team page lists Russell Carr as Founder & CEO; business email shown.",
                    }
                ],
            )

            rows = import_tool._load_csv_rows(input_path)
            normalized_rows, normalized_row_total, normalized_field_total = import_tool._normalize_review_rows(rows)

            self.assertEqual(normalized_row_total, 1)
            self.assertEqual(normalized_field_total, 4)
            row = normalized_rows[0]
            self.assertEqual(str(row["website"] or ""), "https://bes-corp.com")
            self.assertEqual(str(row["contact_name"] or ""), "Russell Carr")
            self.assertEqual(str(row["email"] or ""), "rcarr@bes-corp.com")
            self.assertEqual(
                str(row["source_urls"] or ""),
                "https://bes-corp.com/contact-us/our-team/|https://bes-corp.com/osha-texas-compliance-basics/",
            )

    def test_import_rejects_unrepairable_markdown_email_rows(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            data_dir = tmp / "runtime"
            input_path = tmp / "prospect_ai_assist_review_20260313_reviewed.csv"
            self._write_review_csv(
                input_path,
                [
                    {
                        "state": "TX",
                        "decision": "accept",
                        "firm": "Broken Co",
                        "website": "https://broken.example.com",
                        "contact_name": "Broken Person",
                        "title": "Owner",
                        "email": "[owner@broken.example.com](mailto:other@broken.example.com)",
                        "source_urls": "https://broken.example.com/contact",
                        "confidence": "95",
                        "evidence_snippet": "Owner listed on site",
                    }
                ],
            )

            out = io.StringIO()
            err = io.StringIO()
            env = dict(os.environ)
            env["DATA_DIR"] = str(data_dir)
            with mock.patch.dict(os.environ, env, clear=False), redirect_stdout(out), redirect_stderr(err):
                rc = import_tool.main(["--input", str(input_path), "--dry-run", "--batch", "2026-03-13_AIASSIST"])

            self.assertEqual(rc, 2, msg=out.getvalue() + err.getvalue())
            self.assertIn("ERR_AI_ASSIST_IMPORT_INPUT detail=malformed_row row=2 field=email", err.getvalue())

    def test_import_normalizes_quoted_markdown_rows_before_seed_and_audit(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            data_dir = tmp / "runtime"
            db_path = data_dir / "crm.sqlite"
            input_path = tmp / "prospect_ai_assist_review_20260313_reviewed.csv"
            self._write_review_csv(
                input_path,
                [
                    {
                        "state": "TX",
                        "decision": "accept",
                        "firm": "Berg Compliance Solutions",
                        "website": "[https://bes-corp.com/",
                        "contact_name": "Russell](https://bes-corp.com/%22,%22Russell) Carr",
                        "title": "Founder & CEO",
                        "email": "[rcarr@bes-corp.com](mailto:rcarr@bes-corp.com)",
                        "source_urls": "[https://bes-corp.com/contact-us/our-team/|https://bes-corp.com/osha-texas-compliance-basics/](https://bes-corp.com/contact-us/our-team/|https://bes-corp.com/osha-texas-compliance-basics/)",
                        "confidence": "95",
                        "evidence_snippet": "Team page lists Russell Carr as Founder & CEO; business email shown.",
                    }
                ],
            )

            out = io.StringIO()
            env = dict(os.environ)
            env["DATA_DIR"] = str(data_dir)
            with mock.patch.dict(os.environ, env, clear=False), redirect_stdout(out):
                rc = import_tool.main(["--input", str(input_path), "--batch", "2026-03-13_AIASSIST"])

            output = out.getvalue()
            self.assertEqual(rc, 0, msg=output)
            self.assertIn("AI_ASSIST_IMPORT_NORMALIZED_ROWS=1", output)
            self.assertIn("AI_ASSIST_IMPORT_NORMALIZED_FIELDS=4", output)
            self.assertIn("PASS_AI_ASSIST_IMPORT status=OK", output)

            conn = crm_store.connect(db_path)
            try:
                prospect = conn.execute(
                    """
                    SELECT website, contact_name, email
                    FROM prospects
                    WHERE email = 'rcarr@bes-corp.com'
                    """
                ).fetchone()
                audit = conn.execute(
                    f"""
                    SELECT website, contact_name, email, source_urls_json
                    FROM {crm_store.AI_ASSIST_CANDIDATE_TABLE}
                    WHERE batch_id = '2026-03-13_AIASSIST'
                    """
                ).fetchone()
            finally:
                conn.close()

            self.assertEqual(str(prospect[0] or ""), "https://bes-corp.com")
            self.assertEqual(str(prospect[1] or ""), "Russell Carr")
            self.assertEqual(str(prospect[2] or ""), "rcarr@bes-corp.com")
            self.assertEqual(str(audit[0] or ""), "https://bes-corp.com")
            self.assertEqual(str(audit[1] or ""), "Russell Carr")
            self.assertEqual(str(audit[2] or ""), "rcarr@bes-corp.com")
            self.assertEqual(
                str(audit[3] or ""),
                "[\"https://bes-corp.com/contact-us/our-team/\", \"https://bes-corp.com/osha-texas-compliance-basics/\"]",
            )

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

    def test_completed_tracked_malformed_file_skips_before_normalization(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            data_dir = tmp / "runtime"
            db_path = data_dir / "crm.sqlite"
            input_path = tmp / "prospect_ai_assist_review_20260312_reviewed.csv"
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
                        "evidence_snippet": "Broken [http",
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
                        "2026-03-12_AIASSIST",
                        str(input_path),
                        input_path.name,
                        import_tool._sha256_file(input_path),
                        "completed",
                        "2026-03-12T00:00:00+00:00",
                        "2026-03-12T00:01:00+00:00",
                        "2026-03-12T00:00:00+00:00",
                        "2026-03-12T00:01:00+00:00",
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
                rc = import_tool.main(["--input", str(input_path), "--batch", "2026-03-12_AIASSIST"])

            self.assertEqual(rc, 0, msg=out.getvalue() + err.getvalue())
            self.assertIn("PASS_AI_ASSIST_IMPORT status=SKIPPED_ALREADY_COMPLETED", out.getvalue())
            self.assertNotIn("ERR_AI_ASSIST_IMPORT_INPUT", err.getvalue())

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
