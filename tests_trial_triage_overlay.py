import io
import json
import os
import sqlite3
import sys
import tempfile
import unittest
from contextlib import ExitStack, redirect_stderr, redirect_stdout
from datetime import timezone
from pathlib import Path
from unittest import mock

import send_digest_email as sde


class TestTrialTriageOverlay(unittest.TestCase):
    def test_overlay_flag_off_returns_original_leads(self):
        leads = [{"activity_nr": "1001", "lead_score": 10}]
        with mock.patch.dict(os.environ, {"TRIAL_TRIAGE_OVERLAY_ENABLED": "0"}, clear=False):
            out_leads, stats, promoted, decisions, removed = sde._apply_trial_triage_overlay_if_enabled(
                subscriber_key="wally_trial",
                gen_date="2026-02-25",
                leads=leads,
                dry_run=True,
            )
        self.assertEqual(out_leads, leads)
        self.assertEqual(stats, {})
        self.assertEqual(promoted, [])
        self.assertEqual(decisions, [])
        self.assertEqual(removed, [])

    def test_overlay_enabled_is_render_only_and_reports_counts(self):
        leads = [
            {"activity_nr": "1001", "lead_score": 11, "establishment_name": "A", "inspection_type": "Referral"},
            {"activity_nr": "1002", "lead_score": 8, "establishment_name": "B", "inspection_type": "Referral"},
            {"activity_nr": "1003", "lead_score": 7, "establishment_name": "C", "inspection_type": "Accident"},
        ]
        decisions = [
            {"activity_nr": "1001", "current_priority": "high", "action": "downgrade_to_medium", "confidence": 0.9, "reasons": ["referral"], "provenance": {"source": "rules_cached_detail"}},
            {"activity_nr": "1002", "current_priority": "medium", "action": "remove_from_customer_email", "confidence": 0.92, "reasons": ["stale"], "provenance": {"source": "rules_cached_detail"}},
            {"activity_nr": "1003", "current_priority": "medium", "action": "promote_candidate", "confidence": 0.98, "reasons": ["accident"], "provenance": {"source": "rules_cached_detail"}},
        ]
        buf = io.StringIO()
        with mock.patch.dict(os.environ, {"TRIAL_TRIAGE_OVERLAY_ENABLED": "1"}, clear=False), mock.patch.object(
            sde.scoring_osha_detail_cache, "ensure_cached_for_activities", return_value={"fetched": 0, "skipped_cached": 3, "failed": 0}
        ), mock.patch.object(
            sde.scoring_osha_detail_cache, "load_detail_cache_rows", return_value={}
        ), mock.patch.object(
            sde.scoring_triage_overlay, "triage", return_value=list(decisions)
        ), mock.patch.object(
            sde, "_write_trial_triage_artifacts", return_value=("x.json", "y.txt")
        ):
            with redirect_stdout(buf):
                out_leads, stats, promoted, out_decisions, removed = sde._apply_trial_triage_overlay_if_enabled(
                    subscriber_key="wally_trial",
                    gen_date="2026-02-25",
                    leads=list(leads),
                    dry_run=True,
                )
        self.assertEqual(len(out_leads), 2)
        self.assertEqual(int(out_leads[0]["lead_score"]), 6)
        self.assertEqual(int(stats.get("removed", 0)), 1)
        self.assertEqual(int(stats.get("promote_candidates", 0)), 1)
        self.assertEqual(len(promoted), 1)
        self.assertEqual(len(out_decisions), 3)
        self.assertEqual(len(removed), 1)
        text = buf.getvalue()
        self.assertIn("TRIAL_TRIAGE_OVERLAY enabled=1", text)
        self.assertIn("before=3 after=2", text)
        self.assertIn("TRIAL_TRIAGE_ARTIFACT_JSON path=x.json", text)
        self.assertIn("TRIAL_TRIAGE_ARTIFACT_TXT path=y.txt", text)

    def test_trial_artifacts_written_under_data_dir_trials_subscriber_scoring(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            data_dir = tmp / "runtime"
            with mock.patch.dict(os.environ, {"DATA_DIR": str(data_dir)}, clear=False):
                json_path, txt_path = sde._write_trial_triage_artifacts(
                    subscriber_key="wally_trial",
                    gen_date="2026-02-25",
                    decisions=[],
                    overlay_stats={"removed": 0, "downgraded_to_medium": 0, "downgraded_to_low": 0, "promote_candidates": 0},
                    removed_rows=[],
                    promoted_rows=[],
                    before_count=2,
                    after_count=2,
                )
            self.assertIsNotNone(json_path)
            self.assertIsNotNone(txt_path)
            json_file = Path(str(json_path))
            txt_file = Path(str(txt_path))
            self.assertTrue(json_file.exists())
            self.assertTrue(txt_file.exists())
            self.assertIn(str(Path("trials") / "wally_trial" / "scoring"), str(json_file))
            payload = json.loads(json_file.read_text(encoding="utf-8"))
            self.assertEqual(payload["counts"]["before"], 2)
            self.assertEqual(payload["counts"]["after"], 2)

    def test_main_dry_run_trial_render_overlay_off_vs_on_produces_artifacts(self):
        leads_seed = [
            {
                "activity_nr": "1001",
                "establishment_name": "Alpha Roofing",
                "site_city": "Austin",
                "site_state": "TX",
                "area_office": "Austin Area Office",
                "inspection_type": "Referral",
                "date_opened": "2026-02-24",
                "first_seen_at": "2026-02-24T10:00:00Z",
                "last_seen_at": "2026-02-24T10:00:00Z",
                "changed_at": "2026-02-24T10:00:00Z",
                "lead_score": 10,
                "source_url": "https://www.osha.gov/ords/imis/establishment.inspection_detail?id=1001",
            },
            {
                "activity_nr": "1002",
                "establishment_name": "Bravo Industrial",
                "site_city": "Dallas",
                "site_state": "TX",
                "area_office": "Dallas Area Office",
                "inspection_type": "Referral",
                "date_opened": "2026-02-24",
                "first_seen_at": "2026-02-24T11:00:00Z",
                "last_seen_at": "2026-02-24T11:00:00Z",
                "changed_at": "2026-02-24T11:00:00Z",
                "lead_score": 8,
                "source_url": "https://www.osha.gov/ords/imis/establishment.inspection_detail?id=1002",
            },
        ]
        filter_stats_seed = {
            "total_candidates": 2,
            "after_time_window": 2,
            "after_territory": 2,
            "after_content_filter": 2,
            "after_dedupe": 2,
            "final_leads": 2,
            "excluded_by_time_window": 0,
            "excluded_by_new_only": 0,
            "excluded_by_territory": 0,
            "matched_by_cbsa": 0,
            "matched_by_office": 0,
            "matched_by_fallback": 0,
            "excluded_by_content_filter": 0,
            "dedupe_removed": 0,
            "low_fallback_count": 0,
            "priority_counts": {"high": 1, "medium": 1, "low": 0},
            "shown_priority_counts": {"high": 1, "medium": 1, "low": 0},
        }

        def _run_once(overlay_enabled: bool) -> tuple[str, str, dict]:
            with tempfile.TemporaryDirectory() as d:
                tmp = Path(d)
                data_dir = tmp / "data"
                out_dir = tmp / "out"
                db_path = tmp / "osha.sqlite"
                customer_path = tmp / "customer.json"
                db_path.parent.mkdir(parents=True, exist_ok=True)
                sqlite3.connect(str(db_path)).close()
                customer_path.write_text(
                    json.dumps(
                        {
                            "customer_id": "wally_trial_tx_triangle_v1",
                            "subscriber_key": "wally_trial",
                            "states": ["TX"],
                            "territory_code": "TX_TRIANGLE_V1",
                            "content_filter": "high_medium",
                            "include_low_fallback": False,
                            "opened_window_days": 14,
                            "new_only_days": 14,
                            "allow_live_send": False,
                            "send_time_local": "09:00",
                            "send_window_minutes": 20,
                            "email_recipients": ["trial@example.com"],
                            "brand_name": "Acme Safety",
                            "mailing_address": "123 Main St, Austin, TX 78701",
                            "reply_to_email": "support@example.com",
                            "top_k_overall": 25,
                            "top_k_per_state": 10,
                            "pilot_mode": False,
                        }
                    )
                    + "\n",
                    encoding="utf-8",
                )

                def _mock_get_leads_for_period(*_args, **kwargs):  # noqa: ANN001
                    leads = [dict(r) for r in leads_seed]
                    low_fallback: list[dict] = []
                    stats = dict(filter_stats_seed)
                    stats["priority_counts"] = dict(filter_stats_seed["priority_counts"])
                    stats["shown_priority_counts"] = dict(filter_stats_seed["shown_priority_counts"])
                    if kwargs.get("return_debug"):
                        return leads, low_fallback, stats, [], []
                    return leads, low_fallback, stats

                decisions = [
                    {
                        "activity_nr": "1001",
                        "lead_key": "",
                        "current_priority": "high",
                        "action": "downgrade_to_medium",
                        "confidence": 0.90,
                        "reasons": ["referral"],
                        "provenance": {"source": "rules_cached_detail"},
                    },
                    {
                        "activity_nr": "1002",
                        "lead_key": "",
                        "current_priority": "medium",
                        "action": "remove_from_customer_email",
                        "confidence": 0.92,
                        "reasons": ["stale"],
                        "provenance": {"source": "rules_cached_detail"},
                    },
                ]

                env = {
                    "DATA_DIR": str(data_dir),
                    "PREFS_LINKS_DISABLED": "1",
                    "TRIAL_TRIAGE_OVERLAY_ENABLED": "1" if overlay_enabled else "0",
                    "UNSUB_ENDPOINT_BASE": "",
                    "UNSUB_SECRET": "",
                }
                argv = [
                    "send_digest_email.py",
                    "--db",
                    str(db_path),
                    "--customer",
                    str(customer_path),
                    "--mode",
                    "daily",
                    "--output-dir",
                    str(out_dir),
                    "--dry-run",
                ]
                stdout_buf = io.StringIO()
                stderr_buf = io.StringIO()
                with ExitStack() as stack:
                    stack.enter_context(mock.patch.dict(os.environ, env, clear=False))
                    stack.enter_context(mock.patch.object(sys, "argv", argv))
                    stack.enter_context(mock.patch.object(sde, "setup_logging", return_value=None))
                    stack.enter_context(mock.patch.object(sde, "load_environment", return_value=None))
                    stack.enter_context(mock.patch.object(sde, "_load_subscriber_profile", return_value={
                        "subscriber_key": "wally_trial",
                        "email": "trial@example.com",
                        "active": 1,
                        "territory_code": "TX_TRIANGLE_V1",
                        "content_filter": "high_medium",
                        "include_low_fallback": False,
                        "last_sent_at": None,
                        "send_enabled": True,
                    }))
                    stack.enter_context(mock.patch.object(sde, "_load_subscriber_entitlement_and_allowlist", return_value=(None, [])))
                    stack.enter_context(mock.patch.object(sde, "_enforce_zip_cbsa_dataset_gate", return_value=(True, "ZIP_CBSA_DATASET_READY")))
                    stack.enter_context(mock.patch.object(sde, "load_territory_definitions", return_value={}))
                    stack.enter_context(mock.patch.object(sde, "resolve_territory_code", side_effect=lambda raw, _defs=None: raw))
                    stack.enter_context(mock.patch.object(sde, "resolve_timezone", return_value=timezone.utc))
                    stack.enter_context(mock.patch.object(sde, "collect_recipients", return_value=["trial@example.com"]))
                    stack.enter_context(mock.patch.object(sde, "resolve_admin_recipient", return_value="admin@example.com"))
                    stack.enter_context(mock.patch.object(sde, "get_leads_for_period", side_effect=_mock_get_leads_for_period))
                    stack.enter_context(mock.patch.object(sde, "compute_territory_health", return_value={"window_24": {"share": 0.5}, "window_14": {"share": 0.5}, "alerts": []}))
                    stack.enter_context(mock.patch.object(sde, "store_territory_health", return_value=None))
                    stack.enter_context(mock.patch.object(sde, "format_territory_health_summary", return_value=("", "")))
                    stack.enter_context(mock.patch.object(sde, "write_trial_territory_debug_artifact", return_value=None))
                    stack.enter_context(mock.patch.object(sde, "write_tier_audit_artifact", return_value=str(out_dir / "tier_audit.json")))
                    stack.enter_context(mock.patch.object(sde, "_is_trial_subscriber", return_value=True))
                    stack.enter_context(mock.patch.object(sde, "check_suppression", return_value=False))
                    stack.enter_context(mock.patch.object(sde, "prefs_links_reachable", return_value=(False, "env_disabled")))
                    stack.enter_context(mock.patch.object(sde, "fetch_lows_enabled_pref", return_value=False))
                    stack.enter_context(mock.patch.object(sde, "preflight_missing_vars", return_value=[]))
                    stack.enter_context(mock.patch.object(sde, "_load_latest_ingestion_counts", return_value={"ingested_total": 0, "new_inserted": 0, "existing_updated": 0}))
                    if overlay_enabled:
                        stack.enter_context(mock.patch.object(sde.scoring_osha_detail_cache, "ensure_cached_for_activities", return_value={"fetched": 0, "skipped_cached": 2, "failed": 0}))
                        stack.enter_context(mock.patch.object(sde.scoring_osha_detail_cache, "load_detail_cache_rows", return_value={}))
                        stack.enter_context(mock.patch.object(sde.scoring_triage_overlay, "triage", return_value=[dict(x) for x in decisions]))
                    with redirect_stdout(stdout_buf), redirect_stderr(stderr_buf):
                        sde.main()
                scoring_dir = data_dir / "trials" / "wally_trial" / "scoring"
                artifact_info = {
                    "scoring_dir_exists": scoring_dir.exists(),
                    "json_files": [str(p) for p in sorted(scoring_dir.glob("triage_*.json"))] if scoring_dir.exists() else [],
                    "txt_files": [str(p) for p in sorted(scoring_dir.glob("triage_report_*.txt"))] if scoring_dir.exists() else [],
                    "json_payload": None,
                    "txt_report": "",
                }
                if artifact_info["json_files"]:
                    artifact_info["json_payload"] = json.loads(Path(str(artifact_info["json_files"][0])).read_text(encoding="utf-8"))
                if artifact_info["txt_files"]:
                    artifact_info["txt_report"] = Path(str(artifact_info["txt_files"][0])).read_text(encoding="utf-8")
                return stdout_buf.getvalue(), stderr_buf.getvalue(), artifact_info

        stdout_off, stderr_off, off_info = _run_once(False)
        self.assertEqual("", stderr_off.strip())
        self.assertIn("EMAIL_HTML_BYTES recipient=admin@example.com", stdout_off)
        self.assertIn("DRYRUN_EMAIL_RECIPIENT admin@example.com", stdout_off)
        self.assertIn("RUN_DIAGNOSTICS", stdout_off)
        self.assertIn("selected_for_digest=2", stdout_off)
        self.assertNotIn("TRIAL_TRIAGE_OVERLAY enabled=1", stdout_off)
        self.assertFalse(bool(off_info["scoring_dir_exists"]))

        stdout_on, stderr_on, on_info = _run_once(True)
        self.assertEqual("", stderr_on.strip())
        self.assertIn("EMAIL_HTML_BYTES recipient=admin@example.com", stdout_on)
        self.assertIn("DRYRUN_EMAIL_RECIPIENT admin@example.com", stdout_on)
        self.assertIn("TRIAL_TRIAGE_OVERLAY enabled=1 before=2 after=1", stdout_on)
        self.assertIn("TRIAL_TRIAGE_ARTIFACT_JSON path=", stdout_on)
        self.assertIn("TRIAL_TRIAGE_ARTIFACT_TXT path=", stdout_on)
        self.assertIn("selected_for_digest=1", stdout_on)

        self.assertTrue(bool(on_info["scoring_dir_exists"]))
        self.assertEqual(1, len(on_info["json_files"]))
        self.assertEqual(1, len(on_info["txt_files"]))
        payload = on_info["json_payload"]
        self.assertIsInstance(payload, dict)
        payload = dict(payload or {})
        for key in ["generated_at_utc", "subscriber_key", "gen_date", "counts", "decisions", "removed_rows", "promote_candidates"]:
            self.assertIn(key, payload)
        for key in ["before", "after", "removed", "downgraded_to_medium", "downgraded_to_low", "promote_candidates"]:
            self.assertIn(key, payload["counts"])
        self.assertEqual(2, int(payload["counts"]["before"]))
        self.assertEqual(1, int(payload["counts"]["after"]))
        self.assertEqual(1, int(payload["counts"]["removed"]))
        report_text = str(on_info["txt_report"])
        self.assertIn("# Trial Triage Overlay Report", report_text)
        self.assertIn("## Promote Candidates (Appendix)", report_text)


if __name__ == "__main__":
    unittest.main()
