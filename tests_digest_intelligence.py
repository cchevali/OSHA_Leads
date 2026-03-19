import unittest

from scoring import digest_intelligence as di


class TestDigestIntelligence(unittest.TestCase):
    def test_same_company_city_type_day_collapses(self):
        rows = [
            {
                "activity_nr": "a1",
                "lead_key": "a1",
                "establishment_name": "Acme, Inc.",
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
                "effective_priority": "MEDIUM",
                "triage_overlay_reasons": ["referral_or_complaint"],
            },
        ]
        presentation = di.build_digest_presentation(rows, section_kind="daily_new")
        self.assertEqual(1, presentation["visible_row_count"])
        visible = presentation["visible_rows"][0]
        self.assertEqual("a1", visible["presentation_representative_key"])
        self.assertEqual(["a2"], visible["presentation_collapsed_member_keys"])
        self.assertEqual(1, visible["presentation_collapsed_hidden_count"])

    def test_missing_grouping_field_does_not_collapse(self):
        rows = [
            {
                "activity_nr": "a1",
                "lead_key": "a1",
                "establishment_name": "Acme Inc.",
                "site_city": "",
                "site_state": "TX",
                "inspection_type": "Complaint",
                "date_opened": "2026-03-01",
                "effective_priority": "HIGH",
            },
            {
                "activity_nr": "a2",
                "lead_key": "a2",
                "establishment_name": "Acme LLC",
                "site_city": "",
                "site_state": "TX",
                "inspection_type": "Complaint",
                "date_opened": "2026-03-01",
                "effective_priority": "HIGH",
            },
        ]
        presentation = di.build_digest_presentation(rows, section_kind="daily_new")
        self.assertEqual(2, presentation["visible_row_count"])

    def test_representative_selection_is_deterministic(self):
        rows = [
            {
                "activity_nr": "b2",
                "lead_key": "b2",
                "establishment_name": "Bravo LLC",
                "site_city": "Dallas",
                "site_state": "TX",
                "inspection_type": "Planned",
                "date_opened": "2026-03-02",
                "effective_priority": "MEDIUM",
            },
            {
                "activity_nr": "b1",
                "lead_key": "b1",
                "establishment_name": "Bravo Inc.",
                "site_city": "Dallas",
                "site_state": "TX",
                "inspection_type": "Complaint",
                "date_opened": "2026-03-02",
                "effective_priority": "MEDIUM",
                "triage_overlay_reasons": ["referral_or_complaint"],
            },
        ]
        presentation = di.build_digest_presentation(rows, section_kind="daily_new")
        self.assertEqual("b1", presentation["visible_rows"][0]["presentation_representative_key"])

    def test_priority_order_puts_complaint_referral_accident_ahead_of_planned(self):
        rows = [
            {
                "activity_nr": "p1",
                "inspection_type": "Planned",
                "effective_priority": "MEDIUM",
                "date_opened": "2026-03-02",
            },
            {
                "activity_nr": "c1",
                "inspection_type": "Complaint",
                "effective_priority": "MEDIUM",
                "date_opened": "2026-03-01",
            },
            {
                "activity_nr": "r1",
                "inspection_type": "Referral",
                "effective_priority": "MEDIUM",
                "date_opened": "2026-03-01",
            },
        ]
        ordered = di.sort_rows_for_digest(rows)
        self.assertEqual(["c1", "r1", "p1"], [row["activity_nr"] for row in ordered])

    def test_intro_text_marks_snapshot_rows_not_new(self):
        rows = [
            {
                "activity_nr": "s1",
                "lead_key": "s1",
                "establishment_name": "Snapshot Co",
                "site_city": "Austin",
                "site_state": "TX",
                "inspection_type": "Accident",
                "date_opened": "2026-03-01",
                "effective_priority": "HIGH",
            }
        ]
        starter = di.build_digest_presentation(rows, section_kind="starter_snapshot")
        fallback = di.build_digest_presentation(rows, section_kind="snapshot_not_new")
        self.assertIn("Recent activity is concentrated in Texas, mostly accident signals.", starter["intro_text"])
        self.assertIn("Recent activity is concentrated in Texas, mostly accident signals.", fallback["intro_text"])
        self.assertEqual("Top signals", starter["top_pick_heading"])

    def test_reason_sentences_use_shorter_customer_facing_copy(self):
        rows = [
            {
                "activity_nr": "n1",
                "lead_key": "n1",
                "establishment_name": "Priority Roofing LLC",
                "site_city": "Austin",
                "site_state": "TX",
                "inspection_type": "Accident",
                "date_opened": "2026-03-01",
                "effective_priority": "HIGH",
                "triage_overlay_reasons": ["naics_emphasis"],
            }
        ]
        presentation = di.build_digest_presentation(rows, section_kind="starter_snapshot")
        self.assertEqual("Higher-attention industry.", presentation["top_picks"][0]["presentation_reason_sentence"])


if __name__ == "__main__":
    unittest.main()
