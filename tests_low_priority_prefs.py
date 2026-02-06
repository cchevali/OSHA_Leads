import tempfile
import unittest
from pathlib import Path

from email_footer import build_footer_html, build_footer_text
from send_digest_email import generate_digest_html, generate_digest_text
from unsubscribe_utils import get_include_lows_pref, set_include_lows_pref


class TestLowPriorityPrefs(unittest.TestCase):
    def setUp(self):
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

    def test_low_count_line_renders_when_high_medium_zero(self):
        tier_counts = {"high": 0, "medium": 0, "low": 3}
        enable_url = "https://example.com/prefs/enable_lows?TOKEN=abc&territory=TX_TRIANGLE_V1"

        html = generate_digest_html(
            leads=[],
            low_fallback=[],
            config=self.config,
            gen_date="2026-02-06",
            mode="daily",
            territory_code="TX_TRIANGLE_V1",
            content_filter="high_medium",
            include_low_fallback=False,
            branding=self.branding,
            tier_counts=tier_counts,
            enable_lows_url=enable_url,
            include_lows=False,
            low_priority=[],
            footer_html=self.footer_html,
            summary_label="Newly observed today: 0 signals",
        )
        text = generate_digest_text(
            leads=[],
            low_fallback=[],
            config=self.config,
            gen_date="2026-02-06",
            mode="daily",
            territory_code="TX_TRIANGLE_V1",
            content_filter="high_medium",
            include_low_fallback=False,
            branding=self.branding,
            tier_counts=tier_counts,
            enable_lows_url=enable_url,
            include_lows=False,
            low_priority=[],
            footer_text=self.footer_text,
            summary_label="Newly observed today: 0 signals",
        )

        self.assertIn("Tier summary: High 0, Medium 0, Low 3", html)
        self.assertIn("Low-priority signals available: 3 (not shown).", html)
        self.assertIn("Enable lows", html)
        self.assertIn(enable_url, html)

        self.assertIn("Tier summary: High 0, Medium 0, Low 3", text)
        self.assertIn("Low-priority signals available: 3 (not shown).", text)
        self.assertIn("Enable lows:", text)
        self.assertIn(enable_url, text)

    def test_enable_lows_link_absent_when_no_low_signals(self):
        tier_counts = {"high": 0, "medium": 0, "low": 0}
        html = generate_digest_html(
            leads=[],
            low_fallback=[],
            config=self.config,
            gen_date="2026-02-06",
            mode="daily",
            territory_code="TX_TRIANGLE_V1",
            content_filter="high_medium",
            include_low_fallback=False,
            branding=self.branding,
            tier_counts=tier_counts,
            enable_lows_url="https://example.com/should_not_render",
            include_lows=False,
            low_priority=[],
            footer_html=self.footer_html,
            summary_label="Newly observed today: 0 signals",
        )
        self.assertIn("Low-priority signals: 0.", html)
        self.assertNotIn("Enable lows", html)

    def test_include_lows_preference_changes_rendering(self):
        with tempfile.TemporaryDirectory() as td:
            prefs_path = Path(td) / "prefs.csv"
            email = "ops@example.com"
            territory = "TX_TRIANGLE_V1"
            set_include_lows_pref(
                email=email,
                territory=territory,
                include_lows=True,
                source="one_click",
                prefs_path=prefs_path,
            )
            include_lows = get_include_lows_pref(email=email, territory=territory, prefs_path=prefs_path)
            self.assertTrue(include_lows)

            tier_counts = {"high": 0, "medium": 0, "low": 1}
            low_priority = [
                {
                    "establishment_name": "LowCo",
                    "site_city": "Austin",
                    "site_state": "TX",
                    "inspection_type": "Planned",
                    "date_opened": "2026-02-06",
                    "lead_score": 1,
                    "source_url": "https://example.com/low",
                }
            ]
            html = generate_digest_html(
                leads=[],
                low_fallback=[],
                config=self.config,
                gen_date="2026-02-07",
                mode="daily",
                territory_code=territory,
                content_filter="high_medium",
                include_low_fallback=False,
                branding=self.branding,
                tier_counts=tier_counts,
                enable_lows_url="https://example.com/prefs/enable_lows?TOKEN=abc&territory=TX_TRIANGLE_V1",
                include_lows=include_lows,
                low_priority=low_priority,
                footer_html=self.footer_html,
                summary_label="Newly observed today: 0 signals",
            )

            self.assertIn("Low-priority signals: 1.", html)
            self.assertIn("Low priority (1)", html)
            self.assertIn("LowCo", html)
            self.assertNotIn("(not shown)", html)
            self.assertNotIn("Enable lows", html)


if __name__ == "__main__":
    unittest.main()

