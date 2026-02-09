import unittest

from email_footer import build_footer_html, build_footer_text
from send_digest_email import generate_digest_html, generate_digest_text


class TestDigestSnapshotSection(unittest.TestCase):
    def setUp(self) -> None:
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
            disclaimer="Informational only. Not legal advice.",
            reply_to=self.branding["reply_to"],
            unsub_url=None,
            include_separator=True,
        )
        self.footer_html = build_footer_html(
            brand_name=self.branding["brand_name"],
            mailing_address=self.branding["mailing_address"],
            disclaimer="Informational only. Not legal advice.",
            reply_to=self.branding["reply_to"],
            unsub_url=None,
        )

    def test_snapshot_section_renders_label_and_table(self) -> None:
        snap_rows = [
            {
                "establishment_name": "Example Priority Co",
                "site_city": "Austin",
                "site_state": "TX",
                "inspection_type": "Complaint",
                "date_opened": "2026-02-01",
                "lead_score": 7,
                "source_url": "https://example.com/x",
                "first_seen_at": "2026-02-02T12:00:00+00:00",
            }
        ]
        snap_tiers = {"high": 1, "medium": 2, "low": 0}

        html = generate_digest_html(
            leads=[],
            low_fallback=[],
            config=self.config,
            gen_date="2026-02-08",
            mode="daily",
            territory_code="TX_TRIANGLE_V1",
            content_filter="high_medium",
            include_low_fallback=False,
            branding=self.branding,
            tier_counts={"high": 0, "medium": 0, "low": 0},
            enable_lows_url=None,
            include_lows=False,
            low_priority=[],
            footer_html=self.footer_html,
            summary_label="Newly observed today: 0 signals",
            snapshot_label="Last 14 days snapshot (not new)",
            snapshot_days=14,
            snapshot_tier_counts=snap_tiers,
            snapshot_enable_lows_url=None,
            snapshot_rows=snap_rows,
            snapshot_total=10,
        )
        self.assertIn("Last 14 days snapshot (not new)", html)
        self.assertIn("Tier summary (not new): High 1, Medium 2, Low 0", html)
        self.assertIn("Example Priority Co", html)
        self.assertNotIn("Low-priority signals: 0.", html)
        self.assertNotIn("Also observed (not shown)", html)

        text = generate_digest_text(
            leads=[],
            low_fallback=[],
            config=self.config,
            gen_date="2026-02-08",
            mode="daily",
            territory_code="TX_TRIANGLE_V1",
            content_filter="high_medium",
            include_low_fallback=False,
            branding=self.branding,
            tier_counts={"high": 0, "medium": 0, "low": 0},
            enable_lows_url=None,
            include_lows=False,
            low_priority=[],
            footer_text=self.footer_text,
            summary_label="Newly observed today: 0 signals",
            snapshot_label="Last 14 days snapshot (not new)",
            snapshot_days=14,
            snapshot_tier_counts=snap_tiers,
            snapshot_enable_lows_url=None,
            snapshot_rows=snap_rows,
            snapshot_total=10,
        )
        self.assertIn("Last 14 days snapshot (not new)", text)
        self.assertIn("Tier summary (not new): High 1, Medium 2, Low 0", text)
        self.assertIn("Example Priority Co", text)
        self.assertNotIn("Low-priority signals: 0.", text)
        self.assertNotIn("Also observed (not shown)", text)

    def test_snapshot_section_with_lows_emits_single_enable_lows_cta(self) -> None:
        snap_rows = [
            {
                "establishment_name": "Example Priority Co",
                "site_city": "Austin",
                "site_state": "TX",
                "inspection_type": "Accident",
                "date_opened": "2026-02-01",
                "lead_score": 10,
                "source_url": "https://example.com/x",
                "first_seen_at": "2026-02-02T12:00:00+00:00",
            }
        ]
        snap_tiers = {"high": 1, "medium": 0, "low": 3}
        enable_url = "https://unsub.microflowops.com/prefs/enable_lows?t=abc.def"

        html = generate_digest_html(
            leads=[],
            low_fallback=[],
            config=self.config,
            gen_date="2026-02-08",
            mode="daily",
            territory_code="TX_TRIANGLE_V1",
            content_filter="high_medium",
            include_low_fallback=False,
            branding=self.branding,
            tier_counts={"high": 0, "medium": 0, "low": 0},
            enable_lows_url=None,
            include_lows=False,
            low_priority=[],
            footer_html=self.footer_html,
            summary_label="Newly observed today: 0 signals",
            snapshot_label="Last 14 days snapshot (not new)",
            snapshot_days=14,
            snapshot_tier_counts=snap_tiers,
            snapshot_enable_lows_url=enable_url,
            snapshot_rows=snap_rows,
            snapshot_total=1,
        )
        self.assertEqual(1, html.count("Low-priority signals available:"))
        self.assertEqual(1, html.count("Enable lows.</a>"))
        self.assertIn(enable_url, html)
        self.assertNotIn("Also observed (not shown)", html)

        text = generate_digest_text(
            leads=[],
            low_fallback=[],
            config=self.config,
            gen_date="2026-02-08",
            mode="daily",
            territory_code="TX_TRIANGLE_V1",
            content_filter="high_medium",
            include_low_fallback=False,
            branding=self.branding,
            tier_counts={"high": 0, "medium": 0, "low": 0},
            enable_lows_url=None,
            include_lows=False,
            low_priority=[],
            footer_text=self.footer_text,
            summary_label="Newly observed today: 0 signals",
            snapshot_label="Last 14 days snapshot (not new)",
            snapshot_days=14,
            snapshot_tier_counts=snap_tiers,
            snapshot_enable_lows_url=enable_url,
            snapshot_rows=snap_rows,
            snapshot_total=1,
        )
        self.assertEqual(1, text.count("Low-priority signals available:"))
        self.assertEqual(1, text.count("Enable lows:"))
        self.assertIn(enable_url, text)
        self.assertNotIn("Also observed (not shown)", text)


if __name__ == "__main__":
    unittest.main()
