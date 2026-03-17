import os
import unittest

from send_digest_email import (
    build_email_message,
    generate_digest_html,
    generate_digest_text,
)
from email_footer import build_footer_html, build_footer_text


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
        self.footer_text = build_footer_text(
            brand_name=self.branding["brand_name"],
            mailing_address=self.branding["mailing_address"],
            disclaimer="This report contains public OSHA inspection data for informational purposes only. Not legal advice.",
            reply_to=self.branding["reply_to"],
            unsub_url=None,
            include_separator=True,
        )
        self.footer_html = build_footer_html(
            brand_name=self.branding["brand_name"],
            mailing_address=self.branding["mailing_address"],
            disclaimer="This report contains public OSHA inspection data for informational purposes only. Not legal advice.",
            reply_to=self.branding["reply_to"],
            unsub_url=None,
        )

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
            footer_html=self.footer_html,
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
            footer_text=self.footer_text,
        )

        self.assertIn("No new OSHA activity signals since last send for Texas Triangle.", html)
        self.assertIn("Low Signals (Fallback)", html)
        self.assertIn("Low Lead One", html)
        self.assertIn(self.branding["brand_name"], html)
        self.assertIn(self.branding["mailing_address"], html)

        self.assertIn("No new OSHA activity signals since last send for Texas Triangle.", text)
        self.assertIn("Low Signals (Fallback)", text)
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
            list_unsub="<mailto:support@acme.com?subject=unsubscribe>",
            list_unsub_post=None,
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
            footer_html=self.footer_html,
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
            footer_text=self.footer_text,
        )

        self.assertIn("No new OSHA activity signals since last send for Texas Triangle.", html)
        self.assertNotIn("Low Signals (Fallback)", html)
        self.assertIn(self.branding["brand_name"], html)
        self.assertIn(self.branding["mailing_address"], html)

        self.assertIn("No new OSHA activity signals since last send for Texas Triangle.", text)
        self.assertNotIn("Low Signals (Fallback)", text)
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
            list_unsub="<mailto:support@acme.com?subject=unsubscribe>",
            list_unsub_post=None,
        )
        self.assertIn("mailto:support@acme.com?subject=unsubscribe", msg["List-Unsubscribe"])
        self.assertIsNone(msg.get("List-Unsubscribe-Post"))


if __name__ == "__main__":
    unittest.main()
