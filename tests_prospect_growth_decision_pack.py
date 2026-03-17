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
from tools import prospect_growth_decision_pack as growth_tool


class TestProspectGrowthDecisionPack(unittest.TestCase):
    def _seed_prospect(
        self,
        db_path: Path,
        *,
        prospect_id: str,
        state: str,
        source: str,
        status: str,
        created_at: str,
        default_send_eligible: int = 1,
    ) -> None:
        conn = crm_store.connect(db_path)
        try:
            crm_store.init_schema(conn)
            conn.execute(
                """
                INSERT INTO prospects(
                    prospect_id, firm, contact_name, email, title, city, state, website, source,
                    source_fit_tier, default_send_eligible, email_status, enrichment_lane,
                    score, status, created_at
                ) VALUES (?, ?, '', ?, '', '', ?, ?, ?, 'recoverable_consultant', ?, '', '', 0, ?, ?)
                """,
                (
                    prospect_id,
                    f"{prospect_id} Firm",
                    f"{prospect_id}@example.com",
                    state,
                    f"https://{prospect_id}.example.com",
                    source,
                    int(default_send_eligible),
                    status,
                    created_at,
                ),
            )
            conn.commit()
        finally:
            conn.close()

    def _write_cache(
        self,
        data_dir: Path,
        *,
        source_token: str,
        state: str,
        fetched_at_utc: str,
        cache_max_age_days: int,
        rows: list[dict[str, str]],
    ) -> None:
        cache_root = data_dir / "prospect_generation" / "cache"
        cache_path = growth_tool.generation._source_cache_path_for_state(cache_root, source_token, state)
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(
            json.dumps(
                {
                    "source": source_token,
                    "state": state,
                    "fetched_at_utc": fetched_at_utc,
                    "cache_max_age_days": cache_max_age_days,
                    "pages_fetched": 1,
                    "parse_mode": "TEST",
                    "rows": rows,
                }
            ),
            encoding="utf-8",
        )

    def _write_manifest(
        self,
        data_dir: Path,
        *,
        date_token: str,
        run_started_at: str,
        raw_target: int = 5,
        selected_row_count: int = 2,
        include_raw_target: bool = True,
    ) -> None:
        packet_dir = data_dir / "audits" / "prospect_ai_assist" / f"prospect_ai_assist_review_{date_token}_packets"
        packet_dir.mkdir(parents=True, exist_ok=True)
        manifest_path = packet_dir / "manifest.json"
        payload = {
            "schema_version": "ai_assist_packet_manifest_v2",
            "run_date": f"{date_token[:4]}-{date_token[4:6]}-{date_token[6:8]}",
            "run_started_at": run_started_at,
            "selected_row_count": selected_row_count,
            "packet_count": 1,
            "candidate_count_before_filters": 4,
            "candidate_count_after_filters": 2,
            "included_without_website": 1,
            "state_lic_cap_limited_count": 0,
            "top_exclusion_reasons": [{"reason": "excluded_already_in_crm", "count": 2}],
        }
        if include_raw_target:
            payload["raw_target"] = raw_target
        manifest_path.write_text(
            json.dumps(payload, indent=2) + "\n",
            encoding="utf-8",
        )
        (packet_dir / "packet_status.txt").write_text(
            f"PACKETS READY: 1\nSELECTED ROWS: {selected_row_count}\nROWS WITH BLANK WEBSITE: 1\n",
            encoding="utf-8",
        )

    def _write_review_file(self, data_dir: Path, *, date_token: str) -> None:
        audit_dir = data_dir / "audits" / "prospect_ai_assist"
        audit_dir.mkdir(parents=True, exist_ok=True)
        packet_dir = audit_dir / f"prospect_ai_assist_review_{date_token}_packets"
        packet_dir.mkdir(parents=True, exist_ok=True)
        seed_index_path = packet_dir / "seed_index.json"
        seed_index_path.write_text(
            json.dumps(
                {
                    "seeds": {
                        "seed_aiha_tx": {"seed_source_token": "AIHA"},
                        "seed_ohs_ca": {"seed_source_token": "OHS_BG"},
                    }
                },
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
        review_path = audit_dir / f"prospect_ai_assist_review_{date_token}_reviewed.csv"
        with open(review_path, "w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=[
                    "state",
                    "decision",
                    "firm",
                    "website",
                    "contact_name",
                    "title",
                    "email",
                    "source_urls",
                    "confidence",
                    "evidence_snippet",
                    "seed_id",
                ],
            )
            writer.writeheader()
            writer.writerow(
                {
                    "state": "TX",
                    "decision": "accept",
                    "firm": "AIHA Reviewed",
                    "website": "https://reviewed-aiha.example.com",
                    "contact_name": "Alex Owner",
                    "title": "Owner",
                    "email": "alex@reviewed-aiha.example.com",
                    "source_urls": "https://reviewed-aiha.example.com",
                    "confidence": "90",
                    "evidence_snippet": "owner on site",
                    "seed_id": "seed_aiha_tx",
                }
            )
            writer.writerow(
                {
                    "state": "CA",
                    "decision": "reject",
                    "firm": "OHS Reviewed",
                    "website": "https://reviewed-ohs.example.com",
                    "contact_name": "Chris Principal",
                    "title": "Principal",
                    "email": "chris@reviewed-ohs.example.com",
                    "source_urls": "https://reviewed-ohs.example.com",
                    "confidence": "40",
                    "evidence_snippet": "uncertain fit",
                    "seed_id": "seed_ohs_ca",
                }
            )
        fixed_ts = datetime(2026, 3, 15, 12, 0, 0).timestamp()
        os.utime(review_path, (fixed_ts, fixed_ts))

    def _test_env(self, data_dir: Path) -> dict[str, str]:
        return {
            "DATA_DIR": str(data_dir),
            "MFO_DATA_DIR_EFFECTIVE": "",
            "MFO_DATA_DIR_SOURCE": "",
            "OUTREACH_STATES": "TX,CA",
            "PROSPECT_AUTOGROW_STATES": "",
            "PROSPECT_AUTOGROW_SOURCES": "AIHA,OHS_BG,STATE_LIC",
            "PROSPECT_AUTOGROW_BACKLOG_TARGET": "10",
            "PROSPECT_AI_ASSIST_REVIEW_RAW_TARGET": "5",
            "PROSPECT_AI_ASSIST_REVIEW_PACKET_SIZE": "2",
            "PROSPECT_AI_ASSIST_REVIEW_ENABLED": "1",
            "PROSPECT_ENRICH_DOMAIN_ENABLED": "0",
            "PROSPECT_ENRICH_HUNTER_ENABLED": "0",
            "PROSPECT_ENRICH_MAX_SITES_PER_RUN": "8",
            "PROSPECT_ENRICH_HTTP_SLEEP_MS": "250",
        }

    def _build_sample_runtime(
        self,
        data_dir: Path,
        *,
        manifest_selected_row_count: int = 2,
        manifest_raw_target: int = 5,
        include_manifest: bool = True,
        include_manifest_raw_target: bool = True,
    ) -> None:
        db_path = data_dir / "crm.sqlite"
        self._seed_prospect(
            db_path,
            prospect_id="aiha_import_tx",
            state="TX",
            source="aiha_consultants_listing:10-11",
            status="new",
            created_at="2026-03-14T10:00:00+00:00",
            default_send_eligible=1,
        )
        self._seed_prospect(
            db_path,
            prospect_id="ohs_import_ca",
            state="CA",
            source="ohs_buyers_guide:company-1",
            status="contacted",
            created_at="2026-03-13T10:00:00+00:00",
            default_send_eligible=1,
        )
        self._write_cache(
            data_dir,
            source_token="AIHA",
            state="TX",
            fetched_at_utc="2026-03-15T12:00:00+00:00",
            cache_max_age_days=7,
            rows=[
                {
                    "firm": "Alpha Safety",
                    "website": "https://alpha-safety.example.com",
                    "state": "TX",
                    "source": "aiha_consultants_listing:10-11",
                }
            ],
        )
        self._write_cache(
            data_dir,
            source_token="OHS_BG",
            state="CA",
            fetched_at_utc="2026-03-15T12:30:00+00:00",
            cache_max_age_days=7,
            rows=[
                {
                    "firm": "Bravo Safety",
                    "website": "https://bravo-safety.example.com",
                    "state": "CA",
                    "source": "ohs_buyers_guide:company-2",
                    "source_url": "https://buyersguide.example.com/company-2",
                }
            ],
        )
        self._write_cache(
            data_dir,
            source_token="BCSP",
            state="TX",
            fetched_at_utc="2026-03-01T12:00:00+00:00",
            cache_max_age_days=7,
            rows=[],
        )
        if include_manifest:
            self._write_manifest(
                data_dir,
                date_token="20260315",
                run_started_at="2026-03-15T20:00:00-04:00",
                raw_target=manifest_raw_target,
                selected_row_count=manifest_selected_row_count,
                include_raw_target=include_manifest_raw_target,
            )
        self._write_review_file(data_dir, date_token="20260315")

    def test_print_config_and_dry_run_are_read_only(self):
        with tempfile.TemporaryDirectory() as d:
            data_dir = Path(d) / "runtime"
            self._build_sample_runtime(data_dir)
            env = self._test_env(data_dir)
            fixed_now = datetime.fromisoformat("2026-03-16T22:00:00-04:00")

            out = io.StringIO()
            with mock.patch.dict(os.environ, env, clear=False), mock.patch.object(
                growth_tool, "_now_local", return_value=fixed_now
            ), redirect_stdout(out):
                rc = growth_tool.main(["--print-config"])
            self.assertEqual(rc, 0)
            self.assertIn("PROSPECT_GROWTH_STATES=TX,CA", out.getvalue())
            self.assertIn("PROSPECT_GROWTH_SOURCES=AIHA,OHS_BG,STATE_LIC", out.getvalue())

            dry_run_out = io.StringIO()
            with mock.patch.dict(os.environ, env, clear=False), mock.patch.object(
                growth_tool, "_now_local", return_value=fixed_now
            ), redirect_stdout(dry_run_out):
                rc = growth_tool.main(["--days", "14", "--dry-run"])
            self.assertEqual(rc, 0)
            text = dry_run_out.getvalue()
            self.assertIn("PROSPECT GROWTH DECISION PACK", text)
            self.assertIn("SOURCE-BY-SOURCE FUNNEL BY STATE", text)
            self.assertIn("AI-ASSIST REVIEW OUTCOMES BY SOURCE/STATE", text)
            self.assertIn("TX / AIHA: reviewed_accepts_14d=1", text)
            self.assertIn("CA / OHS_BG: reviewed_accepts_14d=0 reviewed_rejects_14d=1", text)
            self.assertIn("STATE LIC SHADOW PACKET PROFILES", text)
            self.assertIn("STATE_LIC_SHADOW_COUNTS_ARE_DIAGNOSTIC_ONLY=1", text)
            self.assertIn("EXHAUST_AIHA_OHS_BG_EXISTING_INVENTORY: RECOMMEND", text)
            self.assertIn("REMOVE_STATE_LIC_AC_CONTRACTOR_FETCHES: RECOMMEND", text)
            self.assertIn("REPORT_STATE_LIC_SHADOW_PACKET_COUNTS: RECOMMEND", text)
            self.assertIn("NEW_SOURCE_REQUIRED: TRIGGER - cycle_selected=2 target=5", text)
            self.assertFalse((data_dir / "audits" / "prospect_growth").exists())

    def test_tool_writes_text_and_json_artifacts(self):
        with tempfile.TemporaryDirectory() as d:
            data_dir = Path(d) / "runtime"
            self._build_sample_runtime(data_dir)
            env = self._test_env(data_dir)
            fixed_now = datetime.fromisoformat("2026-03-16T22:00:00-04:00")

            out = io.StringIO()
            with mock.patch.dict(os.environ, env, clear=False), mock.patch.object(
                growth_tool, "_now_local", return_value=fixed_now
            ), redirect_stdout(out):
                rc = growth_tool.main(["--days", "14"])
            self.assertEqual(rc, 0)
            stdout_text = out.getvalue()
            self.assertIn("PROSPECT_GROWTH_WRITTEN=1", stdout_text)
            self.assertIn("PASS_PROSPECT_GROWTH=1", stdout_text)

            output_dir = data_dir / "audits" / "prospect_growth"
            text_path = output_dir / "prospect_growth_decision_pack_20260316_220000.txt"
            json_path = output_dir / "prospect_growth_decision_pack_20260316_220000.json"
            self.assertTrue(text_path.exists())
            self.assertTrue(json_path.exists())

            report_text = text_path.read_text(encoding="utf-8")
            self.assertIn("DETERMINISTIC RECOMMENDATIONS", report_text)
            self.assertIn("KEEP_CURRENT_ARCHITECTURE: RECOMMEND", report_text)
            self.assertIn("EXHAUST_AIHA_OHS_BG_EXISTING_INVENTORY: RECOMMEND", report_text)
            self.assertIn("NEW_SOURCE_REQUIRED: TRIGGER - cycle_selected=2 target=5", report_text)
            self.assertIn("STATE_LIC_SHADOW_COUNTS_ARE_DIAGNOSTIC_ONLY=1", report_text)

            payload = json.loads(json_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["window_days"], 14)
            self.assertEqual(payload["review_attribution_strategy"], "files")
            self.assertIn("recommendations", payload)
            self.assertEqual(payload["state_lic_shadow_counts_are_diagnostic_only"], 1)
            self.assertIn("state_lic_shadow_packet_profiles", payload)
            recommendation_index = {row["key"]: row for row in payload["recommendations"]}
            self.assertEqual(
                list(recommendation_index.keys()),
                [
                    "KEEP_CURRENT_ARCHITECTURE",
                    "EXHAUST_AIHA_OHS_BG_EXISTING_INVENTORY",
                    "REMOVE_STATE_LIC_AC_CONTRACTOR_FETCHES",
                    "REPORT_STATE_LIC_SHADOW_PACKET_COUNTS",
                    "NEW_SOURCE_REQUIRED",
                ],
            )
            self.assertEqual(recommendation_index["NEW_SOURCE_REQUIRED"]["status"], "TRIGGER")
            review_index = {
                (row["state"], row["source"]): row for row in payload["review_outcomes"]
            }
            self.assertEqual(review_index[("TX", "AIHA")]["reviewed_accepts"], 1)
            self.assertEqual(review_index[("CA", "OHS_BG")]["reviewed_rejects"], 1)

    def test_new_source_required_holds_when_cycle_meets_target(self):
        with tempfile.TemporaryDirectory() as d:
            data_dir = Path(d) / "runtime"
            self._build_sample_runtime(
                data_dir,
                manifest_selected_row_count=5,
                manifest_raw_target=5,
            )
            env = self._test_env(data_dir)
            fixed_now = datetime.fromisoformat("2026-03-16T22:00:00-04:00")

            dry_run_out = io.StringIO()
            with mock.patch.dict(os.environ, env, clear=False), mock.patch.object(
                growth_tool, "_now_local", return_value=fixed_now
            ), redirect_stdout(dry_run_out):
                rc = growth_tool.main(["--days", "14", "--dry-run"])
            self.assertEqual(rc, 0)
            self.assertIn("NEW_SOURCE_REQUIRED: HOLD - cycle_selected=5 target=5", dry_run_out.getvalue())

    def test_missing_manifest_emits_explicit_new_source_required_error_token(self):
        with tempfile.TemporaryDirectory() as d:
            data_dir = Path(d) / "runtime"
            self._build_sample_runtime(data_dir, include_manifest=False)
            env = self._test_env(data_dir)
            fixed_now = datetime.fromisoformat("2026-03-16T22:00:00-04:00")

            dry_run_out = io.StringIO()
            with mock.patch.dict(os.environ, env, clear=False), mock.patch.object(
                growth_tool, "_now_local", return_value=fixed_now
            ), redirect_stdout(dry_run_out):
                rc = growth_tool.main(["--days", "14", "--dry-run"])
            self.assertEqual(rc, 0)
            self.assertIn(
                "NEW_SOURCE_REQUIRED: ERR_NEW_SOURCE_REQUIRED_MANIFEST_MISSING=1 - cycle_manifest_missing",
                dry_run_out.getvalue(),
            )

    def test_wrapper_exists_and_uses_secrets_wrapper(self):
        wrapper = Path(__file__).resolve().parent / "scripts" / "prospect_growth_decision_pack.ps1"
        self.assertTrue(wrapper.exists(), msg=f"missing wrapper: {wrapper}")
        text = wrapper.read_text(encoding="utf-8")
        self.assertIn("run_with_secrets.ps1", text)
        self.assertIn("--print-config", text.lower())
        self.assertIn("--dry-run", text.lower())
        self.assertIn("$Days", text)
        self.assertIn("--days", text.lower())
        self.assertIn("PROSPECT_GROWTH_OUTPUT_DIR", text)
        self.assertIn("PROSPECT_GROWTH_OUTPUT_TEXT_PATH", text)
        self.assertIn("PROSPECT_GROWTH_OUTPUT_JSON_PATH", text)


if __name__ == "__main__":
    unittest.main()
