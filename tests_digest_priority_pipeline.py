import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import send_digest_email as sde
from scoring import digest_intelligence as scoring_digest_intelligence


class TestDigestPriorityPipeline(unittest.TestCase):
    def test_zero_state_coverage_renders_explicit_zero(self):
        config = {
            "states": ["CA", "OR", "WA"],
            "top_k_overall": 25,
            "top_k_per_state": 10,
        }
        branding = {
            "brand_name": "Acme Safety",
            "mailing_address": "123 Main St, Austin, TX 78701",
            "from_email": "alerts@acme.com",
            "reply_to": "support@acme.com",
            "from_display_name": "Acme Safety Alerts",
        }
        leads = [
            {
                "activity_nr": "ca1",
                "establishment_name": "CA Co",
                "site_city": "Los Angeles",
                "site_state": "CA",
                "inspection_type": "Complaint",
                "date_opened": "2026-03-01",
                "lead_score": 8,
                "rules_priority": "HIGH",
                "effective_priority": "HIGH",
            },
            {
                "activity_nr": "wa1",
                "establishment_name": "WA Co",
                "site_city": "Seattle",
                "site_state": "WA",
                "inspection_type": "Referral",
                "date_opened": "2026-03-01",
                "lead_score": 7,
                "rules_priority": "MEDIUM",
                "effective_priority": "MEDIUM",
            },
        ]
        tier_counts = {"high": 1, "medium": 1, "low": 0}
        presentation = scoring_digest_intelligence.build_digest_presentation(leads, section_kind="daily_new")

        html = sde.generate_digest_html(
            leads=leads,
            low_fallback=[],
            config=config,
            gen_date="2026-03-01",
            mode="daily",
            territory_code="FACS_TRIAL_STATES",
            content_filter="high_medium",
            include_low_fallback=False,
            branding=branding,
            tier_counts=tier_counts,
            include_lows=False,
            low_priority=[],
            summary_label="Newly observed today: 2 signals",
            state_summary_states=["CA", "OR", "WA"],
            render_rows=presentation["visible_rows"],
            intro_summary_html=presentation["intro_html"],
            top_pick_rows=presentation["top_picks"],
            top_pick_heading=presentation["top_pick_heading"],
        )
        text = sde.generate_digest_text(
            leads=leads,
            low_fallback=[],
            config=config,
            gen_date="2026-03-01",
            mode="daily",
            territory_code="FACS_TRIAL_STATES",
            content_filter="high_medium",
            include_low_fallback=False,
            branding=branding,
            tier_counts=tier_counts,
            include_lows=False,
            low_priority=[],
            summary_label="Newly observed today: 2 signals",
            state_summary_states=["CA", "OR", "WA"],
            render_rows=presentation["visible_rows"],
            intro_summary_text=presentation["intro_text"],
            top_pick_rows=presentation["top_picks"],
            top_pick_heading=presentation["top_pick_heading"],
        )

        self.assertIn("New signals today by state:</strong> CA 1 | OR 0 | WA 1", html)
        self.assertIn("No new signals in OR today", html)
        self.assertIn("New signals today by state: CA 1 | OR 0 | WA 1", text)
        self.assertIn("No new signals in OR today", text)
        self.assertIn("Top signals today", html)
        self.assertIn("Top signals today", text)
        self.assertIn("Recent activity is concentrated across CA and WA, mostly complaint and referral signals.", html)
        self.assertIn("Recent activity is concentrated across CA and WA, mostly complaint and referral signals.", text)
        self.assertNotIn("Priority tiers use OSHA signal rules plus AI review", html)
        self.assertNotIn("Priority tiers use OSHA signal rules plus AI review", text)

    def test_sorting_is_deterministic_and_event_class_sensitive(self):
        rows = [
            {
                "activity_nr": "3",
                "inspection_type": "Planned",
                "naics": "541620",
                "effective_priority": "MEDIUM",
                "date_opened": "2026-03-02",
            },
            {
                "activity_nr": "2",
                "inspection_type": "Complaint",
                "naics": "236220",
                "effective_priority": "MEDIUM",
                "date_opened": "2026-03-02",
            },
            {
                "activity_nr": "1",
                "inspection_type": "Referral",
                "naics": "236220",
                "effective_priority": "MEDIUM",
                "date_opened": "2026-03-02",
            },
        ]
        ordered_a = sde._sort_leads_for_digest(rows)
        ordered_b = sde._sort_leads_for_digest(rows)
        self.assertEqual([r["activity_nr"] for r in ordered_a], [r["activity_nr"] for r in ordered_b])
        self.assertEqual([r["activity_nr"] for r in ordered_a], ["2", "1", "3"])

    def test_low_hide_behavior_after_ai_lowering(self):
        rows = [
            {"activity_nr": "x1", "rules_priority": "HIGH", "effective_priority": "LOW"},
            {"activity_nr": "x2", "rules_priority": "MEDIUM", "effective_priority": "MEDIUM"},
        ]
        filtered, excluded = sde._filter_by_effective_priority(rows, "high_medium")
        self.assertEqual([str(r.get("activity_nr")) for r in filtered], ["x2"])
        self.assertEqual(excluded, 1)

    def test_audit_artifact_contains_required_provenance_and_presentation_fields(self):
        candidates = [
            {
                "activity_nr": "h1",
                "lead_key": "h1",
                "establishment_name": "Acme, Inc.",
                "site_city": "Austin",
                "site_state": "CA",
                "inspection_type": "Complaint",
                "date_opened": "2026-03-01",
                "rules_priority": "MEDIUM",
                "effective_priority": "HIGH",
                "ai_priority": "HIGH",
                "ai_reason": "urgent complaint",
                "delta_direction": "raised",
                "decision_source": "ai_overlay",
                "triage_overlay_reasons": ["referral_or_complaint"],
            },
            {
                "activity_nr": "h2",
                "lead_key": "h2",
                "establishment_name": "Acme LLC",
                "site_city": "Austin",
                "site_state": "CA",
                "inspection_type": "Complaint",
                "date_opened": "2026-03-01",
                "rules_priority": "MEDIUM",
                "effective_priority": "HIGH",
                "ai_priority": "",
                "ai_reason": "",
                "delta_direction": "no_ai",
                "decision_source": "rules_only",
                "triage_overlay_reasons": ["referral_or_complaint"],
            },
            {
                "activity_nr": "l1",
                "lead_key": "l1",
                "site_state": "OR",
                "inspection_type": "Planned",
                "rules_priority": "LOW",
                "effective_priority": "LOW",
                "ai_priority": "",
                "ai_reason": "",
                "delta_direction": "no_ai",
                "decision_source": "rules_only",
            },
        ]
        presentation = scoring_digest_intelligence.build_digest_presentation(candidates[:2], section_kind="daily_new")
        visible_meta, hidden_map = sde._presentation_metadata_maps({"main": presentation["visible_rows"]})
        with tempfile.TemporaryDirectory() as d:
            data_dir = Path(d) / "runtime"
            with mock.patch.dict(os.environ, {"DATA_DIR": str(data_dir)}, clear=False):
                path, total, shown, hidden_low = sde._write_digest_audit_artifact(
                    subscriber_key="facs_trial",
                    customer_id="facs_trial",
                    recipient_email="taylor@example.com",
                    territory_code="FACS_TRIAL_STATES",
                    run_id="DAILY:FACS:2026-03-01:abcd",
                    gen_date="2026-03-01",
                    trial_subscriber=True,
                    candidates=candidates,
                    shown_keys={"h1"},
                    include_lows_pref=False,
                    content_filter="high_medium",
                    presentation_visible_meta=visible_meta,
                    presentation_hidden_rep_map=hidden_map,
                )
            self.assertTrue(Path(path).exists())
            self.assertEqual(total, 3)
            self.assertEqual(shown, 1)
            self.assertEqual(hidden_low, 1)
            rows = [json.loads(line) for line in Path(path).read_text(encoding="utf-8").splitlines() if line.strip()]
            self.assertEqual(len(rows), 3)
            row = rows[0]
            for field in (
                "subscriber_key",
                "recipient_email",
                "territory_code",
                "run_id",
                "activity_nr",
                "site_state",
                "signal_type",
                "rules_priority",
                "ai_priority",
                "effective_priority",
                "delta_direction",
                "decision_source",
                "shown_in_email",
                "hidden_reason",
                "ai_reason",
                "presentation_representative_key",
                "presentation_collapsed_member_keys",
                "presentation_collapsed_hidden_count",
                "presentation_top_pick_rank",
                "presentation_reason_sentence",
            ):
                self.assertIn(field, row)
            hidden_row = next(item for item in rows if item["activity_nr"] == "h2")
            self.assertEqual("presentation_collapsed", hidden_row["hidden_reason"])
            self.assertEqual("h1", hidden_row["presentation_representative_key"])

    def test_digest_hash_changes_when_presentation_changes(self):
        rows = [
            {
                "activity_nr": "a1",
                "lead_key": "a1",
                "establishment_name": "Acme Inc.",
                "site_city": "Austin",
                "site_state": "TX",
                "inspection_type": "Complaint",
                "date_opened": "2026-03-01",
                "effective_priority": "HIGH",
                "triage_overlay_reasons": ["referral_or_complaint"],
            },
            {
                "activity_nr": "a2",
                "lead_key": "a2",
                "establishment_name": "Acme LLC",
                "site_city": "Austin",
                "site_state": "TX",
                "inspection_type": "Complaint",
                "date_opened": "2026-03-01",
                "effective_priority": "HIGH",
                "triage_overlay_reasons": ["referral_or_complaint"],
            },
        ]
        collapsed = scoring_digest_intelligence.build_digest_presentation(rows, section_kind="daily_new")
        distinct_rows = [dict(rows[0]), dict(rows[1], site_city="Dallas")]
        distinct = scoring_digest_intelligence.build_digest_presentation(distinct_rows, section_kind="daily_new")
        hash_a = sde.compute_digest_hash(
            leads=collapsed["visible_rows"],
            low_fallback=[],
            mode="daily",
            territory_code="TX_TRIANGLE_V1",
            content_filter="high_medium",
            include_low_fallback=False,
            render_sections={"main": collapsed["visible_rows"]},
        )
        hash_b = sde.compute_digest_hash(
            leads=distinct["visible_rows"],
            low_fallback=[],
            mode="daily",
            territory_code="TX_TRIANGLE_V1",
            content_filter="high_medium",
            include_low_fallback=False,
            render_sections={"main": distinct["visible_rows"]},
        )
        self.assertNotEqual(hash_a, hash_b)

    def test_selected_lead_keys_use_visible_representatives(self):
        rows = [
            {
                "activity_nr": "a1",
                "lead_key": "a1",
                "establishment_name": "Acme Inc.",
                "site_city": "Austin",
                "site_state": "TX",
                "inspection_type": "Complaint",
                "date_opened": "2026-03-01",
                "effective_priority": "HIGH",
                "triage_overlay_reasons": ["referral_or_complaint"],
            },
            {
                "activity_nr": "a2",
                "lead_key": "a2",
                "establishment_name": "Acme LLC",
                "site_city": "Austin",
                "site_state": "TX",
                "inspection_type": "Complaint",
                "date_opened": "2026-03-01",
                "effective_priority": "HIGH",
                "triage_overlay_reasons": ["referral_or_complaint"],
            },
        ]
        presentation = scoring_digest_intelligence.build_digest_presentation(rows, section_kind="daily_new")
        selected = sde._selected_lead_keys_for_payload(
            leads=presentation["visible_rows"],
            low_fallback=[],
            signals_limit=10,
            include_lows=False,
            low_priority_shown=[],
            snapshot_rows=[],
        )
        self.assertEqual(["a1"], selected)

    def test_no_new_without_snapshot_uses_single_concise_summary(self):
        config = {"states": ["TX"], "top_k_overall": 25, "top_k_per_state": 10}
        branding = {
            "brand_name": "Acme Safety",
            "mailing_address": "123 Main St, Austin, TX 78701",
            "from_email": "alerts@acme.com",
            "reply_to": "support@acme.com",
            "from_display_name": "Acme Safety Alerts",
        }
        presentation = scoring_digest_intelligence.build_digest_presentation([], section_kind="daily_new")
        html = sde.generate_digest_html(
            leads=[],
            low_fallback=[],
            config=config,
            gen_date="2026-03-01",
            mode="daily",
            territory_code="TX_TRIANGLE_V1",
            content_filter="high_medium",
            include_low_fallback=False,
            branding=branding,
            tier_counts={"high": 0, "medium": 0, "low": 0},
            summary_label="Newly observed today: 0 signals",
            intro_summary_html=presentation["intro_html"],
        )
        text = sde.generate_digest_text(
            leads=[],
            low_fallback=[],
            config=config,
            gen_date="2026-03-01",
            mode="daily",
            territory_code="TX_TRIANGLE_V1",
            content_filter="high_medium",
            include_low_fallback=False,
            branding=branding,
            tier_counts={"high": 0, "medium": 0, "low": 0},
            summary_label="Newly observed today: 0 signals",
            intro_summary_text=presentation["intro_text"],
        )
        self.assertIn("No new OSHA activity signals were rendered today.", html)
        self.assertIn("No new OSHA activity signals were rendered today.", text)
        self.assertNotIn("since last send", html)
        self.assertNotIn("since last send", text)


if __name__ == "__main__":
    unittest.main()
