import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import send_digest_email as sde


class TestTrialTriageOverlay(unittest.TestCase):
    def test_non_trial_subscriber_still_applies_digest_priority_overlay(self):
        leads = [{"activity_nr": "2001", "lead_score": 6, "date_opened": "2026-02-20"}]
        decisions = [
            {
                "activity_nr": "2001",
                "current_priority": "MEDIUM",
                "rules_priority": "MEDIUM",
                "final_priority": "HIGH",
                "ai_priority": "HIGH",
                "ai_applied": 1,
                "action": "promote_candidate",
                "confidence": 0.95,
                "reasons": ["ai_raise"],
                "provenance": {"source": "ai_cached"},
            }
        ]
        with mock.patch.dict(os.environ, {"TRIAL_TRIAGE_OVERLAY_ENABLED": "1"}, clear=False), mock.patch.object(
            sde.scoring_osha_detail_cache, "load_detail_cache_rows", return_value={}
        ), mock.patch.object(
            sde.scoring_triage_overlay, "triage", return_value=list(decisions)
        ), mock.patch.object(
            sde, "_write_trial_triage_artifacts", return_value=(None, None)
        ):
            out_leads, stats, _promoted, _out_decisions, _removed = sde._apply_trial_triage_overlay_if_enabled(
                subscriber_key="live_subscriber",
                gen_date="2026-02-25",
                leads=leads,
                dry_run=True,
            )
        self.assertEqual(len(out_leads), 1)
        self.assertEqual(str(out_leads[0].get("effective_priority") or ""), "HIGH")
        self.assertEqual(int(stats.get("raised", 0)), 1)

    def test_rules_apply_even_when_overlay_flag_off(self):
        leads = [{"activity_nr": "1001", "lead_score": 8, "date_opened": "2026-02-20"}]
        decisions = [
            {
                "activity_nr": "1001",
                "current_priority": "MEDIUM",
                "rules_priority": "MEDIUM",
                "final_priority": "MEDIUM",
                "ai_priority": "NONE",
                "ai_applied": 0,
                "action": "keep",
                "confidence": 0.9,
                "reasons": ["rules_default"],
                "provenance": {"source": "rules_deterministic"},
            }
        ]
        with mock.patch.dict(os.environ, {"TRIAL_TRIAGE_OVERLAY_ENABLED": "0"}, clear=False), mock.patch.object(
            sde.scoring_osha_detail_cache, "load_detail_cache_rows", return_value={}
        ), mock.patch.object(
            sde.scoring_triage_overlay, "triage", return_value=list(decisions)
        ) as triage_mock, mock.patch.object(
            sde, "_write_trial_triage_artifacts", return_value=(None, None)
        ):
            out_leads, stats, promoted, out_decisions, removed = sde._apply_trial_triage_overlay_if_enabled(
                subscriber_key="wally_trial",
                gen_date="2026-02-25",
                leads=leads,
                dry_run=True,
            )
        self.assertEqual(len(out_leads), 1)
        self.assertEqual(int(stats.get("kept", 0)), 1)
        self.assertEqual(len(promoted), 0)
        self.assertEqual(len(out_decisions), 1)
        self.assertEqual(len(removed), 0)
        self.assertTrue(triage_mock.called)
        kwargs = triage_mock.call_args.kwargs
        self.assertIn("allow_ai", kwargs)
        self.assertFalse(bool(kwargs["allow_ai"]))

    def test_overlay_flag_on_enables_ai_gate_and_priority_raise(self):
        leads = [{"activity_nr": "1001", "lead_score": 6, "date_opened": "2026-02-20"}]
        decisions = [
            {
                "activity_nr": "1001",
                "current_priority": "MEDIUM",
                "rules_priority": "MEDIUM",
                "final_priority": "HIGH",
                "ai_priority": "HIGH",
                "ai_applied": 1,
                "action": "promote_candidate",
                "confidence": 0.95,
                "reasons": ["ai_raise"],
                "provenance": {"source": "ai_cached"},
            }
        ]
        with mock.patch.dict(os.environ, {"TRIAL_TRIAGE_OVERLAY_ENABLED": "1"}, clear=False), mock.patch.object(
            sde.scoring_osha_detail_cache, "load_detail_cache_rows", return_value={}
        ), mock.patch.object(
            sde.scoring_triage_overlay, "triage", return_value=list(decisions)
        ) as triage_mock, mock.patch.object(
            sde, "_write_trial_triage_artifacts", return_value=(None, None)
        ):
            out_leads, stats, promoted, _out_decisions, _removed = sde._apply_trial_triage_overlay_if_enabled(
                subscriber_key="wally_trial",
                gen_date="2026-02-25",
                leads=leads,
                dry_run=True,
            )
        self.assertEqual(len(out_leads), 1)
        self.assertEqual(int(out_leads[0]["lead_score"]), 6)
        self.assertEqual(str(out_leads[0].get("effective_priority") or ""), "HIGH")
        self.assertEqual(int(stats.get("raised", 0)), 1)
        self.assertEqual(len(promoted), 1)
        kwargs = triage_mock.call_args.kwargs
        self.assertTrue(bool(kwargs.get("allow_ai")))

    def test_suppressed_rows_removed(self):
        leads = [{"activity_nr": "1002", "lead_score": 8, "date_opened": "2026-02-20"}]
        decisions = [
            {
                "activity_nr": "1002",
                "current_priority": "MEDIUM",
                "rules_priority": "SUPPRESS",
                "final_priority": "SUPPRESS",
                "ai_priority": "NONE",
                "ai_applied": 0,
                "action": "remove_from_customer_email",
                "confidence": 0.99,
                "reasons": ["stale"],
                "provenance": {"source": "rules_deterministic"},
            }
        ]
        with mock.patch.dict(os.environ, {"TRIAL_TRIAGE_OVERLAY_ENABLED": "0"}, clear=False), mock.patch.object(
            sde.scoring_osha_detail_cache, "load_detail_cache_rows", return_value={}
        ), mock.patch.object(
            sde.scoring_triage_overlay, "triage", return_value=list(decisions)
        ), mock.patch.object(
            sde, "_write_trial_triage_artifacts", return_value=(None, None)
        ):
            out_leads, stats, _promoted, _out_decisions, removed = sde._apply_trial_triage_overlay_if_enabled(
                subscriber_key="wally_trial",
                gen_date="2026-02-25",
                leads=leads,
                dry_run=True,
            )
        self.assertEqual(len(out_leads), 0)
        self.assertEqual(int(stats.get("removed", 0)), 1)
        self.assertEqual(int(stats.get("suppressed", 0)), 1)
        self.assertEqual(len(removed), 1)

    def test_trial_artifacts_written_under_data_dir_trials_subscriber_scoring(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            data_dir = tmp / "runtime"
            with mock.patch.dict(os.environ, {"DATA_DIR": str(data_dir)}, clear=False):
                json_path, txt_path = sde._write_trial_triage_artifacts(
                    subscriber_key="wally_trial",
                    gen_date="2026-02-25",
                    decisions=[],
                    overlay_stats={
                        "removed": 0,
                        "suppressed": 0,
                        "raised": 0,
                        "downgraded_to_medium": 0,
                        "downgraded_to_low": 0,
                        "promote_candidates": 0,
                    },
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
            self.assertIn("suppressed", payload["counts"])
            self.assertIn("raised", payload["counts"])


if __name__ == "__main__":
    unittest.main()
