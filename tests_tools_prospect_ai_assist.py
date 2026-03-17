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

    def _read_seed_rows(self, path: Path) -> list[dict[str, str]]:
        with open(path, "r", newline="", encoding="utf-8") as handle:
            return list(csv.DictReader(handle))

    def test_dump_writes_packetized_artifacts_with_seed_candidates(self):
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
            output_dir = tmp / "prospect_ai_assist"
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
                        str(output_dir),
                        "--raw-target",
                        "4",
                        "--packet-size",
                        "2",
                    ]
                )

            self.assertEqual(rc, 0)
            text = out.getvalue()
            self.assertIn("AI_ASSIST_DUMP_STATES_SCOPE=TX,CA", text)
            self.assertIn("AI_ASSIST_DUMP_RAW_TARGET=4", text)
            self.assertIn("AI_ASSIST_DUMP_CANDIDATES_TOTAL=5", text)
            self.assertIn("AI_ASSIST_DUMP_ROWS_WRITTEN=4", text)
            self.assertIn("AI_ASSIST_DUMP_PACKET_SIZE=2", text)
            self.assertIn("AI_ASSIST_DUMP_PACKET_COUNT=2", text)

            output_path = output_dir / "prospect_ai_assist_review_20260307.txt"
            self.assertTrue(output_path.exists())
            packet_dir = output_dir / "prospect_ai_assist_review_20260307_packets"
            manifest_path = packet_dir / "manifest.json"
            self.assertTrue(packet_dir.exists())
            self.assertTrue(manifest_path.exists())
            prompt_text = output_path.read_text(encoding="utf-8")
            self.assertIn("# SEED CANDIDATES CSV:", prompt_text)
            self.assertIn(
                "firm,website,state,city,phone,address,seed_source,seed_source_url,source_record_id,license_number",
                prompt_text,
            )
            self.assertIn("website may be blank", prompt_text)
            self.assertIn("Do not invent websites or emails", prompt_text)
            self.assertIn("Return reject if no named principal/contact can be verified", prompt_text)
            self.assertIn("Alpha Safety LLC", prompt_text)
            self.assertIn("Bravo Safety", prompt_text)
            self.assertIn("Charlie Safety", prompt_text)
            self.assertNotIn("Delta Safety", prompt_text)
            self.assertNotIn("Known Safety Group", prompt_text)
            self.assertNotIn("Existing Firm LLC", prompt_text)
            self.assertIn(
                dump_tool.prospect_sources_aiha.PAGE_URL_TEMPLATE.format(page_id="10-11"),
                prompt_text,
            )
            self.assertIn("https://buyersguide.example.com/charlie", prompt_text)
            self.assertIn(",".join(dump_tool.REVIEW_COLUMNS), prompt_text)

            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(manifest["schema_version"], "ai_assist_packet_manifest_v2")
            self.assertEqual(manifest["packet_count"], 2)
            self.assertEqual(manifest["packet_size"], 2)
            self.assertEqual(manifest["candidate_count_before_filters"], 7)
            self.assertEqual(manifest["candidate_count_after_filters"], 5)
            self.assertEqual(manifest["candidate_count"], 5)
            self.assertEqual(manifest["excluded_already_in_crm"], 2)
            self.assertEqual(manifest["excluded_duplicate_seed"], 0)
            self.assertEqual(manifest["included_without_website"], 0)
            self.assertEqual(manifest["source_breakdown"], {"AIHA": 2, "OHS_BG": 3})
            self.assertEqual(len(manifest["packets"]), 2)
            self.assertEqual(manifest["packets"][0]["reviewed_import_filename"], "prospect_ai_assist_review_20260307_packet_001_reviewed.csv")
            self.assertEqual(manifest["packets"][0]["suggested_batch_id"], "2026-03-07_AIASSIST_P001")

            seed_packet_one = (packet_dir / "seed_packet_001.csv").read_text(encoding="utf-8")
            review_packet_one = (packet_dir / "review_packet_001.txt").read_text(encoding="utf-8")
            packet_status = (packet_dir / "packet_status.txt").read_text(encoding="utf-8")
            self.assertIn("Alpha Safety LLC", seed_packet_one)
            self.assertIn("Bravo Safety", seed_packet_one)
            self.assertNotIn("Charlie Safety", seed_packet_one)
            self.assertIn(
                "firm,website,state,city,phone,address,seed_source,seed_source_url,source_record_id,license_number",
                seed_packet_one,
            )
            self.assertIn("REVIEWED IMPORT FILENAME: prospect_ai_assist_review_20260307_packet_001_reviewed.csv", review_packet_one)
            self.assertIn("SUGGESTED_BATCH_ID: 2026-03-07_AIASSIST_P001", review_packet_one)
            self.assertIn("website may be blank", review_packet_one)
            self.assertIn("Do not invent websites or emails", review_packet_one)
            self.assertIn("PACKETS READY: 2", packet_status)
            self.assertIn("ROWS WITH BLANK WEBSITE: 0", packet_status)

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
            output_dir = tmp / "dry_dump"
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
                        str(output_dir),
                        "--raw-target",
                        "3",
                        "--packet-size",
                        "2",
                    ]
                )

            self.assertEqual(rc, 0)
            text = out.getvalue()
            self.assertIn("AI_ASSIST_DUMP_CANDIDATES_TOTAL=1", text)
            self.assertIn("AI_ASSIST_DUMP_ROWS_WRITTEN=1", text)
            self.assertIn("AI_ASSIST_DUMP_PACKET_COUNT=1", text)
            self.assertIn("WARN_AI_ASSIST_DUMP_SHORTFALL=1 requested=3 available=1 shortfall=2", text)

    def test_dump_skips_cross_state_and_invalid_website_cache_rows(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            data_dir = tmp / "runtime"
            db_path = data_dir / "crm.sqlite"
            self._seed_crm_prospect(db_path, firm="Known", website="https://known.test", state="TX")
            self._write_cache_rows(
                data_dir,
                "AIHA",
                "FL",
                [
                    {
                        "firm": "Boundary Row",
                        "website": "https://boundary.example",
                        "state": "CT",
                        "source": "aiha_consultants_listing:26-27",
                    },
                    {
                        "firm": "Label Bleed Row",
                        "website": "https://Contact:RobertC.Klein,CIH",
                        "state": "FL",
                        "source": "aiha_consultants_listing:26-27",
                    },
                    {
                        "firm": "Valid Florida Row",
                        "website": "https://valid-fl.example",
                        "state": "FL",
                        "source": "aiha_consultants_listing:26-27",
                    },
                ],
            )

            out = io.StringIO()
            output_dir = tmp / "prospect_ai_assist"
            env = dict(os.environ)
            env["DATA_DIR"] = str(data_dir)
            env["PROSPECT_AUTOGROW_STATES"] = "FL"
            env["PROSPECT_AUTOGROW_BACKLOG_TARGET"] = "5"
            with mock.patch.dict(os.environ, env, clear=False), redirect_stdout(out):
                rc = dump_tool.main(
                    [
                        "--for-date",
                        "2026-03-15",
                        "--output-dir",
                        str(output_dir),
                        "--raw-target",
                        "5",
                        "--packet-size",
                        "2",
                    ]
                )

            self.assertEqual(rc, 0)
            text = out.getvalue()
            self.assertIn("AI_ASSIST_DUMP_CANDIDATES_TOTAL=2", text)
            self.assertIn("AI_ASSIST_DUMP_ROWS_WRITTEN=2", text)
            prompt_text = (output_dir / "prospect_ai_assist_review_20260315.txt").read_text(encoding="utf-8")
            self.assertIn("Valid Florida Row", prompt_text)
            self.assertIn("Label Bleed Row", prompt_text)
            self.assertNotIn("Boundary Row", prompt_text)
            self.assertTrue((output_dir / "prospect_ai_assist_review_20260315_packets" / "review_packet_001.txt").exists())

    def test_dump_allows_state_lic_blank_website_rows_with_city_or_phone(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            data_dir = tmp / "runtime"
            output_dir = tmp / "prospect_ai_assist"
            self._write_cache_rows(
                data_dir,
                "STATE_LIC",
                "TX",
                [
                    {
                        "business_name": "City Safety Compliance Co",
                        "state": "TX",
                        "business_city_state_zip": "Houston TX 77002",
                        "business_address_line1": "123 Main St",
                        "license_number": "EC-12345",
                        "source_detail": "tdlr:EC-12345",
                        "source": "STATE_LIC",
                    },
                    {
                        "business_name": "Phone Environmental Training Co",
                        "state": "TX",
                        "business_telephone": "(713) 555-9000",
                        "source": "STATE_LIC",
                    },
                ],
            )

            out = io.StringIO()
            env = dict(os.environ)
            env["DATA_DIR"] = str(data_dir)
            env["OUTREACH_STATES"] = "TX"
            env["PROSPECT_AUTOGROW_BACKLOG_TARGET"] = "5"
            with mock.patch.dict(os.environ, env, clear=False), redirect_stdout(out):
                rc = dump_tool.main(
                    [
                        "--for-date",
                        "2026-03-15",
                        "--output-dir",
                        str(output_dir),
                        "--raw-target",
                        "5",
                        "--packet-size",
                        "10",
                    ]
                )

            self.assertEqual(rc, 0, msg=out.getvalue())
            text = out.getvalue()
            self.assertIn("AI_ASSIST_DUMP_RAW_INVENTORY_TOTAL=2", text)
            self.assertIn("AI_ASSIST_DUMP_CANDIDATES_TOTAL=2", text)
            self.assertIn("AI_ASSIST_DUMP_ROWS_WRITTEN=2", text)
            self.assertIn("AI_ASSIST_DUMP_OBSERVED_STATE_LIC_FIT_MISMATCH=0", text)
            self.assertIn("AI_ASSIST_DUMP_SOURCE_STATE_LIC_RAW=2", text)
            self.assertIn("AI_ASSIST_DUMP_SOURCE_STATE_LIC_REVIEW_ELIGIBLE=2", text)
            self.assertIn("AI_ASSIST_DUMP_SOURCE_STATE_LIC_SELECTED=2", text)

            packet_dir = output_dir / "prospect_ai_assist_review_20260315_packets"
            seed_rows = self._read_seed_rows(packet_dir / "seed_packet_001.csv")
            review_text = (packet_dir / "review_packet_001.txt").read_text(encoding="utf-8")
            packet_status = (packet_dir / "packet_status.txt").read_text(encoding="utf-8")
            manifest = json.loads((packet_dir / "manifest.json").read_text(encoding="utf-8"))

            self.assertEqual(
                list(seed_rows[0].keys()),
                list(dump_tool.SEED_COLUMNS),
            )
            self.assertEqual(seed_rows[0]["firm"], "City Safety Compliance Co")
            self.assertEqual(seed_rows[0]["website"], "")
            self.assertEqual(seed_rows[0]["state"], "TX")
            self.assertEqual(seed_rows[0]["city"], "Houston")
            self.assertEqual(seed_rows[0]["address"], "123 Main St")
            self.assertEqual(seed_rows[0]["source_record_id"], "tdlr:EC-12345")
            self.assertEqual(seed_rows[0]["license_number"], "EC-12345")
            self.assertEqual(seed_rows[1]["firm"], "Phone Environmental Training Co")
            self.assertEqual(seed_rows[1]["website"], "")
            self.assertEqual(seed_rows[1]["phone"], "(713) 555-9000")
            self.assertIn("website may be blank", review_text)
            self.assertIn("Use city/phone/address/license/source URL context to identify the business", review_text)
            self.assertIn("Do not invent websites or emails", review_text)
            self.assertEqual(manifest["included_without_website"], 2)
            self.assertEqual(manifest["excluded_state_lic_fit_mismatch"], 0)
            self.assertEqual(manifest["observed_state_lic_fit_mismatch"], 0)
            self.assertEqual(manifest["source_breakdown"], {"STATE_LIC": 2})
            self.assertEqual(manifest["source_raw_breakdown"], {"AIHA": 0, "OHS_BG": 0, "STATE_LIC": 2})
            self.assertEqual(manifest["source_review_eligible_breakdown"], {"AIHA": 0, "OHS_BG": 0, "STATE_LIC": 2})
            self.assertEqual(manifest["selected_source_breakdown"], {"AIHA": 0, "OHS_BG": 0, "STATE_LIC": 2})
            self.assertEqual(
                manifest["stage_counts_by_source"]["STATE_LIC"],
                {
                    "raw": 2,
                    "identity_ready": 2,
                    "review_eligible": 2,
                    "safety_passed": 2,
                    "candidates": 2,
                    "selected": 2,
                },
            )
            self.assertEqual(manifest["top_exclusion_reasons"], [])
            self.assertEqual(manifest["state_lic_license_type_breakdown"], {"UNKNOWN": 2})
            self.assertIn("PACKETS READY: 1", packet_status)
            self.assertIn("ROWS WITH BLANK WEBSITE: 2", packet_status)

    def test_dump_default_path_is_stable_per_day(self):
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
                    rc_one = dump_tool.main(["--for-date", "2026-03-07", "--raw-target", "1"])
                with (
                    mock.patch.object(
                        dump_tool,
                        "_current_run_started_at",
                        return_value=datetime.fromisoformat("2026-03-07T09:10:12.654321-05:00"),
                    ),
                    redirect_stdout(out_two),
                ):
                    rc_two = dump_tool.main(["--for-date", "2026-03-07", "--raw-target", "1"])

            self.assertEqual(rc_one, 0, msg=out_one.getvalue())
            self.assertEqual(rc_two, 0, msg=out_two.getvalue())
            output_path_one = next(
                line.split("=", 1)[1]
                for line in out_one.getvalue().splitlines()
                if line.startswith("AI_ASSIST_DUMP_OUTPUT_PATH=")
            )
            output_path_two = next(
                line.split("=", 1)[1]
                for line in out_two.getvalue().splitlines()
                if line.startswith("AI_ASSIST_DUMP_OUTPUT_PATH=")
            )
            self.assertEqual(output_path_one, output_path_two)
            self.assertTrue(Path(output_path_one).exists())
            self.assertEqual(
                Path(output_path_one),
                data_dir / "audits" / "prospect_ai_assist" / "prospect_ai_assist_review_20260307.txt",
            )
            self.assertTrue((data_dir / "audits" / "prospect_ai_assist" / "prospect_ai_assist_review_20260307_packets").exists())

    def test_dump_reports_no_packets_with_exclusion_diagnostics(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            data_dir = tmp / "runtime"
            output_dir = tmp / "prospect_ai_assist"
            db_path = data_dir / "crm.sqlite"
            self._seed_crm_prospect(db_path, firm="Known Safety Group", website="https://knowncrm.test", state="TX")
            self._write_cache_rows(
                data_dir,
                "AIHA",
                "TX",
                [
                    {
                        "firm": "",
                        "website": "https://blank-firm.example.com",
                        "state": "TX",
                        "source": "aiha_consultants_listing:01",
                    },
                    {
                        "firm": "Known Safety Group",
                        "website": "https://new-known.example.com",
                        "state": "TX",
                        "source": "aiha_consultants_listing:04",
                    },
                ],
            )
            self._write_cache_rows(
                data_dir,
                "OHS_BG",
                "TX",
                [
                    {
                        "firm": "Wrong State Co",
                        "website": "https://wrong-state.example.com",
                        "state": "CA",
                        "source": "ohs_buyers_guide:02",
                    },
                    {
                        "firm": "No Locator Co",
                        "website": "",
                        "state": "TX",
                        "source": "ohs_buyers_guide:03",
                    },
                ],
            )

            out = io.StringIO()
            env = dict(os.environ)
            env["DATA_DIR"] = str(data_dir)
            env["OUTREACH_STATES"] = "TX"
            env["PROSPECT_AUTOGROW_BACKLOG_TARGET"] = "5"
            with mock.patch.dict(os.environ, env, clear=False), redirect_stdout(out):
                rc = dump_tool.main(
                    [
                        "--for-date",
                        "2026-03-15",
                        "--output-dir",
                        str(output_dir),
                        "--raw-target",
                        "5",
                        "--packet-size",
                        "2",
                    ]
                )

            self.assertEqual(rc, 0, msg=out.getvalue())
            text = out.getvalue()
            packet_dir = output_dir / "prospect_ai_assist_review_20260315_packets"
            manifest = json.loads((packet_dir / "manifest.json").read_text(encoding="utf-8"))
            packet_status = (packet_dir / "packet_status.txt").read_text(encoding="utf-8")
            self.assertEqual(manifest["packet_count"], 0)
            self.assertEqual(manifest["candidate_count_before_filters"], 4)
            self.assertEqual(manifest["candidate_count_after_filters"], 0)
            self.assertEqual(manifest["identity_ready_count"], 2)
            self.assertEqual(manifest["review_eligible_count"], 1)
            self.assertEqual(manifest["safety_passed_count"], 0)
            self.assertEqual(manifest["excluded_bad_firm"], 1)
            self.assertEqual(manifest["excluded_state_mismatch"], 1)
            self.assertEqual(manifest["excluded_missing_minimum_locator"], 1)
            self.assertEqual(manifest["excluded_already_in_crm"], 1)
            self.assertEqual(manifest["excluded_duplicate_seed"], 0)
            self.assertEqual(manifest["included_without_website"], 0)
            self.assertEqual(manifest["source_raw_breakdown"], {"AIHA": 2, "OHS_BG": 2, "STATE_LIC": 0})
            self.assertEqual(manifest["source_review_eligible_breakdown"], {"AIHA": 1, "OHS_BG": 0, "STATE_LIC": 0})
            self.assertEqual(manifest["selected_source_breakdown"], {"AIHA": 0, "OHS_BG": 0, "STATE_LIC": 0})
            self.assertEqual(
                manifest["stage_counts_by_source"],
                {
                    "AIHA": {
                        "raw": 2,
                        "identity_ready": 1,
                        "review_eligible": 1,
                        "safety_passed": 0,
                        "candidates": 0,
                        "selected": 0,
                    },
                    "OHS_BG": {
                        "raw": 2,
                        "identity_ready": 1,
                        "review_eligible": 0,
                        "safety_passed": 0,
                        "candidates": 0,
                        "selected": 0,
                    },
                    "STATE_LIC": {
                        "raw": 0,
                        "identity_ready": 0,
                        "review_eligible": 0,
                        "safety_passed": 0,
                        "candidates": 0,
                        "selected": 0,
                    },
                },
            )
            self.assertEqual(
                manifest["exclusion_counts_by_reason"],
                {
                    "excluded_already_in_crm": 1,
                    "excluded_bad_firm": 1,
                    "excluded_missing_minimum_locator": 1,
                    "excluded_state_mismatch": 1,
                },
            )
            self.assertEqual(
                manifest["exclusion_counts_by_source"],
                {
                    "AIHA": 2,
                    "OHS_BG": 2,
                    "STATE_LIC": 0,
                },
            )
            self.assertEqual(
                manifest["exclusion_counts_by_source_and_reason"],
                {
                    "AIHA": {
                        "excluded_already_in_crm": 1,
                        "excluded_bad_firm": 1,
                    },
                    "OHS_BG": {
                        "excluded_missing_minimum_locator": 1,
                        "excluded_state_mismatch": 1,
                    },
                    "STATE_LIC": {},
                },
            )
            self.assertEqual(
                manifest["top_exclusion_reasons"],
                [
                    {"reason": "excluded_already_in_crm", "count": 1},
                    {"reason": "excluded_bad_firm", "count": 1},
                    {"reason": "excluded_missing_minimum_locator", "count": 1},
                    {"reason": "excluded_state_mismatch", "count": 1},
                ],
            )
            self.assertIn("NO PACKETS TODAY", packet_status)
            self.assertIn("excluded_bad_firm=1", packet_status)
            self.assertIn("excluded_state_mismatch=1", packet_status)
            self.assertIn("excluded_missing_minimum_locator=1", packet_status)
            self.assertIn("excluded_already_in_crm=1", packet_status)
            self.assertIn(
                "WARN_AI_ASSIST_DUMP_SAFETY_FILTER_STARVATION=1 review_eligible=1 safety_passed=0 candidates=0",
                text,
            )
            self.assertIn("AI_ASSIST_DUMP_SOURCE_AIHA_EXCLUDED_BAD_FIRM=1", text)
            self.assertIn("AI_ASSIST_DUMP_SOURCE_AIHA_EXCLUDED_ALREADY_IN_CRM=1", text)
            self.assertIn("AI_ASSIST_DUMP_SOURCE_OHS_BG_EXCLUDED_STATE_MISMATCH=1", text)
            self.assertIn("AI_ASSIST_DUMP_SOURCE_OHS_BG_EXCLUDED_MISSING_MINIMUM_LOCATOR=1", text)

    def test_dump_observes_state_lic_fit_mismatch_without_excluding_review_seed(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            data_dir = tmp / "runtime"
            output_dir = tmp / "prospect_ai_assist"
            self._write_cache_rows(
                data_dir,
                "STATE_LIC",
                "TX",
                [
                    {
                        "business_name": "Metro HVAC Services LLC",
                        "state": "TX",
                        "business_city_state_zip": "Houston TX 77002",
                        "license_type": "A/C Contractor",
                        "license_number": "AC-999",
                        "source_detail": "tdlr:AC-999",
                        "source": "STATE_LIC",
                    }
                ],
            )

            out = io.StringIO()
            env = dict(os.environ)
            env["DATA_DIR"] = str(data_dir)
            env["OUTREACH_STATES"] = "TX"
            env["PROSPECT_AUTOGROW_BACKLOG_TARGET"] = "5"
            with mock.patch.dict(os.environ, env, clear=False), redirect_stdout(out):
                rc = dump_tool.main(
                    [
                        "--for-date",
                        "2026-03-15",
                        "--output-dir",
                        str(output_dir),
                        "--raw-target",
                        "5",
                        "--packet-size",
                        "2",
                    ]
                )

            self.assertEqual(rc, 0, msg=out.getvalue())
            text = out.getvalue()
            packet_dir = output_dir / "prospect_ai_assist_review_20260315_packets"
            seed_rows = self._read_seed_rows(packet_dir / "seed_packet_001.csv")
            manifest = json.loads((packet_dir / "manifest.json").read_text(encoding="utf-8"))
            packet_status = (packet_dir / "packet_status.txt").read_text(encoding="utf-8")
            self.assertEqual(manifest["packet_count"], 1)
            self.assertEqual(manifest["candidate_count_before_filters"], 1)
            self.assertEqual(manifest["candidate_count_after_filters"], 1)
            self.assertEqual(manifest["excluded_state_lic_fit_mismatch"], 0)
            self.assertEqual(manifest["observed_state_lic_fit_mismatch"], 1)
            self.assertEqual(manifest["source_breakdown"], {"STATE_LIC": 1})
            self.assertEqual(manifest["source_raw_breakdown"], {"AIHA": 0, "OHS_BG": 0, "STATE_LIC": 1})
            self.assertEqual(manifest["source_review_eligible_breakdown"], {"AIHA": 0, "OHS_BG": 0, "STATE_LIC": 1})
            self.assertEqual(manifest["selected_source_breakdown"], {"AIHA": 0, "OHS_BG": 0, "STATE_LIC": 1})
            self.assertEqual(
                manifest["stage_counts_by_source"]["STATE_LIC"],
                {
                    "raw": 1,
                    "identity_ready": 1,
                    "review_eligible": 1,
                    "safety_passed": 1,
                    "candidates": 1,
                    "selected": 1,
                },
            )
            self.assertEqual(manifest["state_lic_license_type_breakdown"], {"A/C Contractor": 1})
            self.assertEqual(seed_rows[0]["firm"], "Metro HVAC Services LLC")
            self.assertEqual(seed_rows[0]["website"], "")
            self.assertEqual(seed_rows[0]["city"], "Houston")
            self.assertEqual(seed_rows[0]["license_number"], "AC-999")
            self.assertIn("PACKETS READY: 1", packet_status)
            self.assertIn("ROWS WITH BLANK WEBSITE: 1", packet_status)
            self.assertIn("AI_ASSIST_DUMP_OBSERVED_STATE_LIC_FIT_MISMATCH=1", text)
            self.assertIn("AI_ASSIST_DUMP_SOURCE_STATE_LIC_REVIEW_ELIGIBLE=1", text)
            self.assertIn("AI_ASSIST_DUMP_SOURCE_STATE_LIC_EXCLUDED_STATE_LIC_FIT_MISMATCH=0", text)

    def test_dump_excludes_state_lic_rows_missing_identity_or_review_anchors(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            data_dir = tmp / "runtime"
            output_dir = tmp / "prospect_ai_assist"
            self._write_cache_rows(
                data_dir,
                "STATE_LIC",
                "TX",
                [
                    {
                        "business_name": "",
                        "state": "TX",
                        "license_number": "EC-100",
                        "source_detail": "tdlr:EC-100",
                        "source": "STATE_LIC",
                    },
                    {
                        "business_name": "Metro Safety Services",
                        "state": "",
                        "license_number": "EC-101",
                        "source_detail": "tdlr:EC-101",
                        "source": "STATE_LIC",
                    },
                    {
                        "business_name": "Metro Safety Systems",
                        "state": "TX",
                        "source": "STATE_LIC",
                    },
                ],
            )

            out = io.StringIO()
            env = dict(os.environ)
            env["DATA_DIR"] = str(data_dir)
            env["OUTREACH_STATES"] = "TX"
            env["PROSPECT_AUTOGROW_BACKLOG_TARGET"] = "5"
            with mock.patch.dict(os.environ, env, clear=False), redirect_stdout(out):
                rc = dump_tool.main(
                    [
                        "--for-date",
                        "2026-03-15",
                        "--output-dir",
                        str(output_dir),
                        "--raw-target",
                        "5",
                        "--packet-size",
                        "2",
                    ]
                )

            self.assertEqual(rc, 0, msg=out.getvalue())
            text = out.getvalue()
            packet_dir = output_dir / "prospect_ai_assist_review_20260315_packets"
            manifest = json.loads((packet_dir / "manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(manifest["packet_count"], 0)
            self.assertEqual(manifest["candidate_count_before_filters"], 3)
            self.assertEqual(manifest["candidate_count_after_filters"], 0)
            self.assertEqual(manifest["identity_ready_count"], 1)
            self.assertEqual(manifest["review_eligible_count"], 0)
            self.assertEqual(manifest["excluded_bad_firm"], 1)
            self.assertEqual(manifest["excluded_state_mismatch"], 1)
            self.assertEqual(manifest["excluded_missing_minimum_locator"], 1)
            self.assertEqual(manifest["excluded_state_lic_fit_mismatch"], 0)
            self.assertEqual(
                manifest["stage_counts_by_source"]["STATE_LIC"],
                {
                    "raw": 3,
                    "identity_ready": 1,
                    "review_eligible": 0,
                    "safety_passed": 0,
                    "candidates": 0,
                    "selected": 0,
                },
            )
            self.assertIn("WARN_AI_ASSIST_DUMP_REVIEW_FIT_STARVATION=1 identity_ready=1 review_eligible=0", text)
            self.assertIn("AI_ASSIST_DUMP_SOURCE_STATE_LIC_EXCLUDED_BAD_FIRM=1", text)
            self.assertIn("AI_ASSIST_DUMP_SOURCE_STATE_LIC_EXCLUDED_STATE_MISMATCH=1", text)
            self.assertIn("AI_ASSIST_DUMP_SOURCE_STATE_LIC_EXCLUDED_MISSING_MINIMUM_LOCATOR=1", text)

    def test_dump_deterministic_packet_slicing_with_mixed_blank_website_rows(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            data_dir = tmp / "runtime"
            output_dir = tmp / "prospect_ai_assist"
            self._write_cache_rows(
                data_dir,
                "AIHA",
                "TX",
                [
                    {
                        "firm": "Alpha Safety",
                        "website": "https://alpha.example.com",
                        "state": "TX",
                        "source": "aiha_consultants_listing:11",
                    }
                ],
            )
            self._write_cache_rows(
                data_dir,
                "OHS_BG",
                "TX",
                [
                    {
                        "firm": "Bravo Safety",
                        "website": "https://bravo.example.com",
                        "state": "TX",
                        "source": "ohs_buyers_guide:11",
                        "source_url": "https://buyersguide.example.com/bravo",
                    }
                ],
            )
            self._write_cache_rows(
                data_dir,
                "STATE_LIC",
                "TX",
                [
                    {
                        "business_name": "Charlie Safety Compliance Group",
                        "state": "TX",
                        "business_city_state_zip": "Austin TX 78701",
                        "license_number": "EC-777",
                        "source_detail": "tdlr:EC-777",
                        "source": "STATE_LIC",
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
                with redirect_stdout(out_one):
                    rc_one = dump_tool.main(
                        [
                            "--for-date",
                            "2026-03-15",
                            "--output-dir",
                            str(output_dir),
                            "--raw-target",
                            "3",
                            "--packet-size",
                            "2",
                        ]
                    )
                packet_dir = output_dir / "prospect_ai_assist_review_20260315_packets"
                first_seed_one = (packet_dir / "seed_packet_001.csv").read_text(encoding="utf-8")
                second_seed_one = (packet_dir / "seed_packet_002.csv").read_text(encoding="utf-8")
                with redirect_stdout(out_two):
                    rc_two = dump_tool.main(
                        [
                            "--for-date",
                            "2026-03-15",
                            "--output-dir",
                            str(output_dir),
                            "--raw-target",
                            "3",
                            "--packet-size",
                            "2",
                        ]
                    )
                first_seed_two = (packet_dir / "seed_packet_001.csv").read_text(encoding="utf-8")
                second_seed_two = (packet_dir / "seed_packet_002.csv").read_text(encoding="utf-8")

            self.assertEqual(rc_one, 0, msg=out_one.getvalue())
            self.assertEqual(rc_two, 0, msg=out_two.getvalue())
            self.assertEqual(first_seed_one, first_seed_two)
            self.assertEqual(second_seed_one, second_seed_two)
            self.assertIn("Alpha Safety", first_seed_one)
            self.assertIn("Bravo Safety", first_seed_one)
            self.assertIn("Charlie Safety Compliance Group", second_seed_one)
            self.assertIn(",,TX,Austin,,,STATE_LIC", second_seed_one)

    def test_dump_state_lic_fit_scoring_prioritizes_consultant_like_rows(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            data_dir = tmp / "runtime"
            output_dir = tmp / "prospect_ai_assist"
            self._write_cache_rows(
                data_dir,
                "STATE_LIC",
                "TX",
                [
                    {
                        "business_name": "Alpha Safety Consulting LLC",
                        "state": "TX",
                        "business_city_state_zip": "Austin TX 78701",
                        "license_number": "SC-100",
                        "source_detail": "tdlr:SC-100",
                        "source": "STATE_LIC",
                    },
                    {
                        "business_name": "Zulu Environmental Safety Compliance Group",
                        "state": "TX",
                        "business_city_state_zip": "Dallas TX 75001",
                        "license_number": "SC-200",
                        "source_detail": "tdlr:SC-200",
                        "source": "STATE_LIC",
                    },
                    {
                        "business_name": "Bravo Air Conditioning Services LLC",
                        "state": "TX",
                        "business_city_state_zip": "Houston TX 77002",
                        "license_type": "A/C Contractor",
                        "license_number": "AC-300",
                        "source_detail": "tdlr:AC-300",
                        "source": "STATE_LIC",
                    },
                ],
            )

            out = io.StringIO()
            env = dict(os.environ)
            env["DATA_DIR"] = str(data_dir)
            env["OUTREACH_STATES"] = "TX"
            env["PROSPECT_AUTOGROW_BACKLOG_TARGET"] = "5"
            with mock.patch.dict(os.environ, env, clear=False), redirect_stdout(out):
                rc = dump_tool.main(
                    [
                        "--for-date",
                        "2026-03-15",
                        "--output-dir",
                        str(output_dir),
                        "--raw-target",
                        "1",
                        "--packet-size",
                        "1",
                    ]
                )

            self.assertEqual(rc, 0, msg=out.getvalue())
            text = out.getvalue()
            packet_dir = output_dir / "prospect_ai_assist_review_20260315_packets"
            seed_one = (packet_dir / "seed_packet_001.csv").read_text(encoding="utf-8")
            manifest = json.loads((packet_dir / "manifest.json").read_text(encoding="utf-8"))
            self.assertIn("Zulu Environmental Safety Compliance Group", seed_one)
            self.assertNotIn("Alpha Safety Consulting LLC", seed_one)
            self.assertNotIn("Bravo Air Conditioning Services LLC", seed_one)
            self.assertEqual(manifest["candidate_count_after_filters"], 3)
            self.assertEqual(manifest["excluded_state_lic_fit_mismatch"], 0)
            self.assertEqual(manifest["observed_state_lic_fit_mismatch"], 1)
            self.assertEqual(manifest["source_breakdown"], {"STATE_LIC": 3})
            self.assertEqual(manifest["selected_source_breakdown"], {"AIHA": 0, "OHS_BG": 0, "STATE_LIC": 1})
            self.assertEqual(
                manifest["stage_counts_by_source"]["STATE_LIC"],
                {
                    "raw": 3,
                    "identity_ready": 3,
                    "review_eligible": 3,
                    "safety_passed": 3,
                    "candidates": 3,
                    "selected": 1,
                },
            )
            self.assertIn("AI_ASSIST_DUMP_OBSERVED_STATE_LIC_FIT_MISMATCH=1", text)
            self.assertIn("AI_ASSIST_DUMP_SOURCE_STATE_LIC_CANDIDATES=3", text)
            self.assertIn("AI_ASSIST_DUMP_SOURCE_STATE_LIC_SELECTED=1", text)

    def test_dump_respects_explicit_source_posture_without_fallback(self):
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
                rc = dump_tool.main(["--dry-run", "--for-date", "2026-03-07", "--raw-target", "1"])

            self.assertEqual(rc, 0, msg=out.getvalue())
            text = out.getvalue()
            self.assertIn("AI_ASSIST_DUMP_SOURCES=none", text)
            self.assertIn("AI_ASSIST_DUMP_CANDIDATES_TOTAL=0", text)
            self.assertIn("AI_ASSIST_DUMP_ROWS_WRITTEN=0", text)
            self.assertIn("WARN_AI_ASSIST_DUMP_NO_ELIGIBLE_SOURCES=1 configured=APOLLO", text)

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
                rc = dump_tool.main(["--dry-run", "--for-date", "2026-03-07", "--raw-target", "1"])

            self.assertEqual(rc, 0, msg=out.getvalue())
            text = out.getvalue()
            self.assertIn("AI_ASSIST_DUMP_CANDIDATES_TOTAL=0", text)
            self.assertIn("AI_ASSIST_DUMP_ROWS_WRITTEN=0", text)

    def test_dump_print_config_prefers_autogrow_states_and_canonical_defaults(self):
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
            self.assertIn("WARN_AI_ASSIST_DUMP_SCOPE_DRIFT=1 outreach_states=FL autogrow_states=CA,TX", text)
            self.assertIn("AI_ASSIST_DUMP_STATES_SCOPE=CA,TX", text)
            self.assertIn("AI_ASSIST_DUMP_RAW_TARGET=30", text)
            self.assertIn("AI_ASSIST_DUMP_GAP_TOTAL=89", text)
            self.assertIn("AI_ASSIST_DUMP_CANDIDATES_REQUESTED_TOTAL=30", text)
            self.assertIn("AI_ASSIST_DUMP_PACKET_SIZE=10", text)
            self.assertIn(
                f"AI_ASSIST_DUMP_OUTPUT_DIR={(data_dir / 'audits' / 'prospect_ai_assist').resolve()}",
                text,
            )

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
            review_dir = data_dir / "imports" / "prospect_ai_assist"
            review_dir.mkdir(parents=True, exist_ok=True)
            (review_dir / "prospect_ai_assist_review_20260310.txt").write_text("prompt", encoding="utf-8")
            (review_dir / "prospect_ai_assist_review_20260310_reviewed_cleaned.csv").write_text("x", encoding="utf-8")
            first = review_dir / "prospect_ai_assist_review_20260309_reviewed.csv"
            second = review_dir / "prospect_ai_assist_review_20260310_packet_001_reviewed.csv"
            third = review_dir / "prospect_ai_assist_review_20260310_packet_002_reviewed.csv"
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
            self._write_review_csv(
                third,
                [
                    {
                        "state": "CA",
                        "decision": "accept",
                        "firm": "Three",
                        "website": "https://three.example.com",
                        "contact_name": "Three",
                        "title": "Owner",
                        "email": "three@three.example.com",
                        "source_urls": "https://three.example.com/contact",
                        "confidence": "92",
                        "evidence_snippet": "Three",
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
                    ("prospect_ai_assist_review_20260310_packet_001_reviewed.csv", "2026-03-10_AIASSIST_P001", True),
                    ("prospect_ai_assist_review_20260310_packet_002_reviewed.csv", "2026-03-10_AIASSIST_P002", True),
                ],
            )

    def test_pending_import_accepts_legacy_audit_dir_with_warning(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            data_dir = tmp / "runtime"
            legacy_dir = data_dir / "audits" / "ai_assist"
            legacy_dir.mkdir(parents=True, exist_ok=True)
            input_path = legacy_dir / "prospect_ai_assist_review_20260309_reviewed.csv"
            self._write_review_csv(
                input_path,
                [
                    {
                        "state": "TX",
                        "decision": "accept",
                        "firm": "Legacy One",
                        "website": "https://legacy-one.example.com",
                        "contact_name": "Legacy One",
                        "title": "Owner",
                        "email": "legacy@legacy-one.example.com",
                        "source_urls": "https://legacy-one.example.com/contact",
                        "confidence": "90",
                        "evidence_snippet": "Legacy One",
                    }
                ],
            )

            calls: list[tuple[str, str, bool]] = []

            def _fake_import(*, input_path: Path, batch_id_override: str = "", dry_run: bool = False):  # type: ignore[no-untyped-def]
                calls.append((str(input_path.name), str(batch_id_override), bool(dry_run)))
                return 0, "DRY_RUN"

            out = io.StringIO()
            env = dict(os.environ)
            env["DATA_DIR"] = str(data_dir)
            with (
                mock.patch.dict(os.environ, env, clear=False),
                mock.patch.object(import_tool, "_import_review_file", side_effect=_fake_import),
                redirect_stdout(out),
            ):
                rc = import_tool.run_pending_imports(dry_run=True)

            self.assertEqual(rc, 0, msg=out.getvalue())
            self.assertEqual(
                calls,
                [("prospect_ai_assist_review_20260309_reviewed.csv", "2026-03-09_AIASSIST", True)],
            )
            self.assertIn("WARN_AI_ASSIST_PENDING_IMPORT_LEGACY_DIR=", out.getvalue())

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
