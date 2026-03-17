import unittest

from outbound_cold_email import generate_email_body, generate_email_subject


class TestOutboundEmailContent(unittest.TestCase):
    def test_unsubscribe_and_links(self):
        lead = {
            "activity_nr": "123456789",
            "establishment_name": "Test Construction LLC",
            "site_city": "Arlington",
            "site_state": "VA",
            "date_opened": "2025-01-05",
            "inspection_type": "Complaint",
            "source_url": "https://www.osha.gov/ords/imis/establishment.inspection_detail?id=123456789",
            "lead_score": 9,
            "first_seen_at": "2025-01-06T12:00:00+00:00",
        }
        recipient = {
            "email": "test@example.com",
            "first_name": "Test",
            "firm_name": "Test Firm",
            "state_pref": "VA",
        }
        text_body, html_body = generate_email_body(recipient, [lead], "tok123", "11539 Links Dr, Reston, VA 20190")

        # Unsubscribe link presence
        self.assertIn("support@microflowops.com", text_body)
        self.assertTrue(
            ("mailto:support@microflowops.com?subject=unsubscribe" in html_body)
            or ("unsubscribe" in html_body.lower())
        )

        # OSHA link presence
        self.assertIn("OSHA: https://www.osha.gov/ords/imis/establishment.inspection_detail?id=123456789", text_body)
        self.assertIn("https://www.osha.gov/ords/imis/establishment.inspection_detail?id=123456789", html_body)
        
        # Priority + observed freshness
        self.assertIn("[High]", text_body)
        self.assertIn("Observed 2025-01-06", text_body)
        self.assertIn("High", html_body)
        self.assertIn("See a live sample feed (real public data) -> https://microflowops.com/sample", text_body)
        self.assertIn("reply with the cities you care about", text_body.lower())
        self.assertIn("https://microflowops.com/sample", html_body)
        self.assertNotIn('reply "yes"', text_body.lower())
        self.assertNotIn("7-day trial", text_body.lower())

        # Address only after footer separator
        addr = "11539 Links Dr, Reston, VA 20190"
        sep_index = text_body.find("\n---\n")
        self.assertNotEqual(sep_index, -1)
        self.assertGreater(text_body.find(addr), sep_index)

    def test_subject_uses_opened_date_and_observed_fallback(self):
        lead_opened = {
            "site_state": "VA",
            "date_opened": "2025-02-21",
            "first_seen_at": "2025-02-22T12:00:00Z",
        }
        self.assertEqual(
            generate_email_subject({"state_pref": "VA"}, [lead_opened]),
            "New OSHA inspection in VA — opened Feb 21",
        )

        lead_fallback = {
            "site_state": "TX",
            "date_opened": "",
            "first_seen_at": "2025-02-23T09:15:00Z",
        }
        self.assertEqual(
            generate_email_subject({"state_pref": ""}, [lead_fallback]),
            "New OSHA inspection in TX — opened Feb 23",
        )


if __name__ == "__main__":
    unittest.main()
