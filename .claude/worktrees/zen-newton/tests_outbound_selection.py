import unittest
from datetime import date

from outbound_cold_email import select_sample_leads, select_sample_leads_with_reason


class TestOutboundSelection(unittest.TestCase):
    def setUp(self):
        self.config = {
            "recency_days": 14,
            "sample_leads_min": 1,
            "sample_leads_max": 5,
        }
        self.as_of = date(2026, 2, 3)

    def test_high_medium_only_and_order(self):
        leads = [
            {
                "activity_nr": "H1",
                "establishment_name": "High Old",
                "site_state": "TX",
                "site_city": "Austin",
                "date_opened": "2026-01-30",
                "lead_score": 9,
                "case_status": "OPEN",
                "first_seen_at": "2026-02-01T08:00:00+00:00",
            },
            {
                "activity_nr": "M1",
                "establishment_name": "Med New",
                "site_state": "TX",
                "site_city": "Austin",
                "date_opened": "2026-01-31",
                "lead_score": 6,
                "case_status": "OPEN",
                "first_seen_at": "2026-02-02T08:00:00+00:00",
            },
            {
                "activity_nr": "L1",
                "establishment_name": "Low Newer",
                "site_state": "TX",
                "site_city": "Austin",
                "date_opened": "2026-01-31",
                "lead_score": 4,
                "case_status": "OPEN",
                "first_seen_at": "2026-02-02T12:00:00+00:00",
            },
        ]

        samples = select_sample_leads(
            leads, self.config, "test@example.com", "camp",
            as_of_date=self.as_of
        )

        self.assertEqual(len(samples), 2)
        self.assertTrue(all(s["lead_score"] >= 6 for s in samples))
        self.assertEqual([s["activity_nr"] for s in samples], ["M1", "H1"])

    def test_no_high_med_reason(self):
        leads = [
            {
                "activity_nr": "L1",
                "establishment_name": "Low Only",
                "site_state": "TX",
                "site_city": "Austin",
                "date_opened": "2026-01-31",
                "lead_score": 4,
                "case_status": "OPEN",
                "first_seen_at": "2026-02-02T12:00:00+00:00",
            }
        ]

        samples, reason = select_sample_leads_with_reason(
            leads, self.config, "test@example.com", "camp",
            as_of_date=self.as_of
        )

        self.assertEqual(samples, [])
        self.assertEqual(reason, "no_high_med_leads")


if __name__ == "__main__":
    unittest.main()
