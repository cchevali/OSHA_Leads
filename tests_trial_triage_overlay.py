import io
import json
import os
import tempfile
import unittest
from contextlib import redirect_stdout
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


if __name__ == "__main__":
    unittest.main()
