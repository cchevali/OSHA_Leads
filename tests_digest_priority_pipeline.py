import json
import os
import tempfile
import unittest
from pathlib import Path

import send_digest_email as sde


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
        )

        self.assertIn("New signals today by state:</strong> CA 1 | OR 0 | WA 1", html)
        self.assertIn("No new signals in OR today", html)
        self.assertIn("New signals today by state: CA 1 | OR 0 | WA 1", text)
        self.assertIn("No new signals in OR today", text)
        self.assertNotIn("Top picks (best bets)", html)
        self.assertNotIn("Top picks (best bets)", text)
        self.assertNotIn("Priority tiers use OSHA signal rules plus AI review", html)
        self.assertNotIn("Priority tiers use OSHA signal rules plus AI review", text)

    def test_sorting_is_deterministic_and_priority_first(self):
        rows = [
            {
                "activity_nr": "3",
                "inspection_type": "Inspection",
                "naics": "541620",
                "effective_priority": "MEDIUM",
                "date_opened": "2026-03-02",
            },
            {
                "activity_nr": "2",
                "inspection_type": "Accident",
                "naics": "236220",
                "effective_priority": "HIGH",
                "date_opened": "2026-03-02",
            },
            {
                "activity_nr": "1",
                "inspection_type": "Complaint",
                "naics": "236220",
                "effective_priority": "HIGH",
                "date_opened": "2026-03-03",
            },
        ]
        ordered_a = sde._sort_leads_for_digest(rows)
        ordered_b = sde._sort_leads_for_digest(rows)
        self.assertEqual([r["activity_nr"] for r in ordered_a], [r["activity_nr"] for r in ordered_b])
        self.assertEqual([r["activity_nr"] for r in ordered_a], ["1", "2", "3"])

    def test_low_hide_behavior_after_ai_lowering(self):
        rows = [
            {"activity_nr": "x1", "rules_priority": "HIGH", "effective_priority": "LOW"},
            {"activity_nr": "x2", "rules_priority": "MEDIUM", "effective_priority": "MEDIUM"},
        ]
        filtered, excluded = sde._filter_by_effective_priority(rows, "high_medium")
        self.assertEqual([str(r.get("activity_nr")) for r in filtered], ["x2"])
        self.assertEqual(excluded, 1)

    def test_audit_artifact_contains_required_provenance_fields(self):
        candidates = [
            {
                "activity_nr": "h1",
                "lead_key": "h1",
                "site_state": "CA",
                "inspection_type": "Complaint",
                "rules_priority": "MEDIUM",
                "effective_priority": "HIGH",
                "ai_priority": "HIGH",
                "ai_reason": "urgent complaint",
                "delta_direction": "raised",
                "decision_source": "ai_overlay",
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
        with tempfile.TemporaryDirectory() as d:
            data_dir = Path(d) / "runtime"
            with unittest.mock.patch.dict(os.environ, {"DATA_DIR": str(data_dir)}, clear=False):
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
                )
            self.assertTrue(Path(path).exists())
            self.assertEqual(total, 2)
            self.assertEqual(shown, 1)
            self.assertEqual(hidden_low, 1)
            rows = [json.loads(line) for line in Path(path).read_text(encoding="utf-8").splitlines() if line.strip()]
            self.assertEqual(len(rows), 2)
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
            ):
                self.assertIn(field, row)


if __name__ == "__main__":
    unittest.main()
