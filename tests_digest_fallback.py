import os
import unittest

from send_digest_email import (
    build_email_message,
    generate_digest_html,
    generate_digest_text,
)


class TestDigestFallback(unittest.TestCase):
    def setUp(self):
        os.environ.pop("UNSUB_ENDPOINT_BASE", None)
        os.environ.pop("UNSUB_SECRET", None)
        self.config = {
            "states": ["TX"],
            "top_k_overall": 25,
            "top_k_per_state": 10,
        }
        self.branding = {
            "brand_name": "Acme Safety",
            "mailing_address": "123 Main St, Austin, TX 78701",
            "from_email": "alerts@acme.com",
            "reply_to": "support@acme.com",
            "from_display_name": "Acme Safety Alerts",
        }
        self.low_fallback = [
            {
                "establishment_name": "Low Lead One",
                "site_city": "Austin",
                "site_state": "TX",
                "area_office": "Austin Area Office",
                "inspection_type": "Planned",
                "date_opened": "2026-02-01",
                "lead_score": 4,
                "source_url": "https://example.com/1",
            }
        ]

    def test_empty_high_medium_with_fallback_on(self):
        html = generate_digest_html(
            leads=[],
            low_fallback=self.low_fallback,
            config=self.config,
            gen_date="2026-02-04",
            mode="daily",
            territory_code="TX_TRIANGLE_V1",
            content_filter="high_medium",
            include_low_fallback=True,
            branding=self.branding,
        )
        text = generate_digest_text(
            leads=[],
            low_fallback=self.low_fallback,
            config=self.config,
            gen_date="2026-02-04",
            mode="daily",
            territory_code="TX_TRIANGLE_V1",
            content_filter="high_medium",
            include_low_fallback=True,
            branding=self.branding,
        )

        self.assertIn("No High/Medium today", html)
        self.assertIn("Low Leads (Fallback)", html)
        self.assertIn("Low Lead One", html)
        self.assertIn(self.branding["brand_name"], html)
        self.assertIn(self.branding["mailing_address"], html)

        self.assertIn("No High/Medium today", text)
        self.assertIn("Low Leads (Fallback)", text)
        self.assertIn("Low Lead One", text)
        self.assertIn(self.branding["brand_name"], text)
        self.assertIn(self.branding["mailing_address"], text)

        msg = build_email_message(
            recipient="test@example.com",
            subject="Fallback On",
            html_body=html,
            text_body=text,
            customer_id="cust1",
            territory_code="TX_TRIANGLE_V1",
            branding=self.branding,
        )
        self.assertIn("mailto:support@acme.com?subject=unsubscribe", msg["List-Unsubscribe"])
        self.assertIsNone(msg.get("List-Unsubscribe-Post"))

    def test_empty_high_medium_with_fallback_off(self):
        html = generate_digest_html(
            leads=[],
            low_fallback=self.low_fallback,
            config=self.config,
            gen_date="2026-02-04",
            mode="daily",
            territory_code="TX_TRIANGLE_V1",
            content_filter="high_medium",
            include_low_fallback=False,
            branding=self.branding,
        )
        text = generate_digest_text(
            leads=[],
            low_fallback=self.low_fallback,
            config=self.config,
            gen_date="2026-02-04",
            mode="daily",
            territory_code="TX_TRIANGLE_V1",
            content_filter="high_medium",
            include_low_fallback=False,
            branding=self.branding,
        )

        self.assertIn("No High/Medium today", html)
        self.assertNotIn("Low Leads (Fallback)", html)
        self.assertIn(self.branding["brand_name"], html)
        self.assertIn(self.branding["mailing_address"], html)

        self.assertIn("No High/Medium today", text)
        self.assertNotIn("Low Leads (Fallback)", text)
        self.assertIn(self.branding["brand_name"], text)
        self.assertIn(self.branding["mailing_address"], text)

        msg = build_email_message(
            recipient="test@example.com",
            subject="Fallback Off",
            html_body=html,
            text_body=text,
            customer_id="cust2",
            territory_code="TX_TRIANGLE_V1",
            branding=self.branding,
        )
        self.assertIn("mailto:support@acme.com?subject=unsubscribe", msg["List-Unsubscribe"])
        self.assertIsNone(msg.get("List-Unsubscribe-Post"))


if __name__ == "__main__":
    unittest.main()
