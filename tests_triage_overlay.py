import io
import os
import unittest
from contextlib import redirect_stdout
from unittest import mock

from scoring import triage_overlay as to


class TestTriageOverlay(unittest.TestCase):
    def test_rules_promote_accident(self):
        items = [
            {
                "activity_nr": "1001",
                "lead_score": 6,
                "date_opened": "2026-02-24",
                "inspection_type": "Complaint",
            }
        ]
        detail_rows = {
            "1001": {
                "inspection_type": "Accident",
                "content_sha256": "abc",
                "emphasis_markers_json": "[]",
                "related_activity_markers_json": "[]",
            }
        }
        with mock.patch("scoring.ai_triage.enabled", return_value=False):
            decisions = to.triage(items, detail_rows, mode="trial_render")
        self.assertEqual(len(decisions), 1)
        self.assertEqual(decisions[0]["action"], "promote_candidate")
        self.assertIn("accident", decisions[0]["reasons"])

    def test_rules_demote_referral_without_emphasis(self):
        items = [
            {
                "activity_nr": "1002",
                "lead_score": 10,
                "date_opened": "2026-01-01",
            }
        ]
        detail_rows = {
            "1002": {
                "inspection_type": "Referral",
                "content_sha256": "def",
                "emphasis_markers_json": "[]",
                "related_activity_markers_json": "[]",
            }
        }
        with mock.patch("scoring.ai_triage.enabled", return_value=False):
            decisions = to.triage(items, detail_rows, mode="trial_render")
        self.assertIn(decisions[0]["action"], {"downgrade_to_medium", "downgrade_to_low"})
        self.assertIn("referral", decisions[0]["reasons"])

    def test_apply_trial_overlay_removes_and_downgrades(self):
        leads = [
            {"activity_nr": "a1", "lead_score": 11},
            {"activity_nr": "a2", "lead_score": 8},
            {"activity_nr": "a3", "lead_score": 7},
        ]
        decisions = [
            {"activity_nr": "a1", "current_priority": "high", "action": "downgrade_to_medium", "confidence": 0.8, "reasons": ["referral"], "provenance": {"source": "rules_cached_detail"}},
            {"activity_nr": "a2", "current_priority": "medium", "action": "remove_from_customer_email", "confidence": 0.9, "reasons": ["stale"], "provenance": {"source": "rules_cached_detail"}},
            {"activity_nr": "a3", "current_priority": "medium", "action": "promote_candidate", "confidence": 0.95, "reasons": ["accident"], "provenance": {"source": "rules_cached_detail"}},
        ]
        out, stats, promoted = to.apply_trial_overlay_to_leads(leads, decisions)
        self.assertEqual(len(out), 2)
        self.assertEqual(int(out[0]["lead_score"]), 6)
        self.assertEqual(stats["removed"], 1)
        self.assertEqual(len(promoted), 1)

    def test_ai_missing_key_prints_disabled_marker_and_falls_back(self):
        items = [{"activity_nr": "1003", "lead_score": 8, "date_opened": "2026-02-20"}]
        detail_rows = {
            "1003": {
                "inspection_type": "Referral",
                "content_sha256": "xyz",
                "emphasis_markers_json": "[]",
                "related_activity_markers_json": "[]",
            }
        }
        buf = io.StringIO()
        with mock.patch.dict(os.environ, {"AI_TRIAGE_ENABLED": "1", "OPENAI_API_KEY": ""}, clear=False):
            # reset module-level one-time emitter
            import scoring.ai_triage as ai_triage_mod

            ai_triage_mod._DISABLED_EMITTED = False
            with redirect_stdout(buf):
                decisions = to.triage(items, detail_rows, mode="outreach_examples")
        self.assertEqual(len(decisions), 1)
        self.assertIn("AI_FEATURES_DISABLED=1", buf.getvalue())
        self.assertIn("missing=OPENAI_API_KEY", buf.getvalue())

    def test_ai_high_remove_cap_requires_strict_confidence(self):
        rule_decision = {
            "activity_nr": "k",
            "lead_key": "",
            "current_priority": "high",
            "action": "keep",
            "confidence": 0.6,
            "reasons": ["keep_default"],
            "provenance": {"source": "rules_cached_detail"},
            "_rules_type": "referral",
            "_markers": [],
            "_item_key": "k",
        }
        ai_payload = {
            "decision": "remove",
            "confidence": 0.80,
            "reasons": ["stale"],
            "prompt_version": "x",
            "content_sha256": "y",
        }
        capped = to._apply_ai_caps(rule_decision, ai_payload, {})
        self.assertEqual(capped["action"], "keep")


if __name__ == "__main__":
    unittest.main()

