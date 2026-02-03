import unittest

from outbound_cold_email import generate_email_body


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
        self.assertIn("mailto:support@microflowops.com?subject=unsubscribe", html_body)

        # OSHA link presence
        self.assertIn("OSHA: https://www.osha.gov/ords/imis/establishment.inspection_detail?id=123456789", text_body)
        self.assertIn("https://www.osha.gov/ords/imis/establishment.inspection_detail?id=123456789", html_body)

        # Address only after footer separator
        addr = "11539 Links Dr, Reston, VA 20190"
        sep_index = text_body.find("\n---\n")
        self.assertNotEqual(sep_index, -1)
        self.assertGreater(text_body.find(addr), sep_index)


if __name__ == "__main__":
    unittest.main()
