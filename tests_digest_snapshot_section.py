import unittest

from email_footer import build_footer_html, build_footer_text
from send_digest_email import (
    _build_preheader,
    _select_snapshot_rows,
    build_digest_subject,
    build_email_message,
    generate_digest_html,
    generate_digest_text,
)
from scoring import digest_intelligence as scoring_digest_intelligence


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
        presentation = scoring_digest_intelligence.build_digest_presentation(snap_rows, section_kind="snapshot_not_new")

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
            intro_summary_html=presentation["intro_html"],
            top_pick_rows=presentation["top_picks"],
            top_pick_heading=presentation["top_pick_heading"],
        )
        self.assertIn("Last 14 days snapshot (not new)", html)
        self.assertIn("Tier summary (not new): High 1, Medium 2, Low 0", html)
        self.assertIn("Example Priority Co", html)
        self.assertIn("Top signals", html)
        self.assertIn("Recent activity is concentrated in Texas, mostly complaint signals.", html)
        self.assertIn("Coverage:</strong> Texas Triangle", html)
        self.assertNotIn("Signals in this snapshot", html)
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
            intro_summary_text=presentation["intro_text"],
            top_pick_rows=presentation["top_picks"],
            top_pick_heading=presentation["top_pick_heading"],
        )
        self.assertIn("Last 14 days snapshot (not new)", text)
        self.assertIn("Tier summary (not new): High 1, Medium 2, Low 0", text)
        self.assertIn("Example Priority Co", text)
        self.assertIn("Top signals", text)
        self.assertIn("Recent activity is concentrated in Texas, mostly complaint signals.", text)
        self.assertIn("Coverage: Texas Triangle", text)
        self.assertNotIn("Signals in this snapshot", text)
        self.assertNotIn("Low-priority signals: 0.", text)
        self.assertNotIn("Also observed (not shown)", text)

    def test_single_state_snapshot_uses_clean_subject_preheader_and_body_labels(self) -> None:
        config = {
            "states": ["FL"],
            "top_k_overall": 25,
            "top_k_per_state": 10,
        }
        snap_rows = [
            {
                "establishment_name": "Florida Signal Co",
                "site_city": "Orlando",
                "site_state": "FL",
                "inspection_type": "Complaint",
                "date_opened": "2026-03-16",
                "lead_score": 8,
                "source_url": "https://example.com/fl",
                "first_seen_at": "2026-03-17T12:00:00+00:00",
            }
        ]
        presentation = scoring_digest_intelligence.build_digest_presentation(snap_rows, section_kind="starter_snapshot")

        subject = build_digest_subject(
            config=config,
            territory_code="JL_SAFETY_TRIAL_STATES",
            gen_date="2026-03-18",
            states=config["states"],
        )
        self.assertEqual("Florida OSHA Signals — 2026-03-18", subject)

        preheader = _build_preheader(
            lead_count=len(snap_rows),
            summary_states=["FL"],
            display_label="Florida",
            snapshot_report_mode=True,
            mode="daily",
        )
        self.assertEqual("Starter snapshot: 1 recent OSHA signals in Florida.", preheader)

        html = generate_digest_html(
            leads=snap_rows,
            low_fallback=[],
            config=config,
            gen_date="2026-03-18",
            mode="daily",
            territory_code="JL_SAFETY_TRIAL_STATES",
            content_filter="high_medium",
            include_low_fallback=False,
            branding=self.branding,
            tier_counts={"high": 1, "medium": 0, "low": 0},
            include_lows=False,
            low_priority=[],
            footer_html=self.footer_html,
            report_label="Starter snapshot",
            summary_label="1 signal in last 14 days",
            intro_summary_html=presentation["intro_html"],
            top_pick_rows=presentation["top_picks"],
            top_pick_heading=presentation["top_pick_heading"],
        )
        text = generate_digest_text(
            leads=snap_rows,
            low_fallback=[],
            config=config,
            gen_date="2026-03-18",
            mode="daily",
            territory_code="JL_SAFETY_TRIAL_STATES",
            content_filter="high_medium",
            include_low_fallback=False,
            branding=self.branding,
            tier_counts={"high": 1, "medium": 0, "low": 0},
            include_lows=False,
            low_priority=[],
            footer_text=self.footer_text,
            report_label="Starter snapshot",
            summary_label="1 signal in last 14 days",
            intro_summary_text=presentation["intro_text"],
            top_pick_rows=presentation["top_picks"],
            top_pick_heading=presentation["top_pick_heading"],
        )

        self.assertIn("Starter snapshot: 1 recent OSHA signals in Florida.", html)
        self.assertIn("Coverage:</strong> Florida", html)
        self.assertIn("Coverage: Florida", text)
        self.assertNotIn("JL_SAFETY_TRIAL_STATES", html)
        self.assertNotIn("JL_SAFETY_TRIAL_STATES", text)
        self.assertNotIn("Territory:", html)
        self.assertNotIn("Territory:", text)

    def test_multi_state_snapshot_retains_state_breakdown(self) -> None:
        config = {
            "states": ["FL", "GA", "AL"],
            "top_k_overall": 25,
            "top_k_per_state": 10,
        }
        snap_rows = [
            {
                "establishment_name": "Florida Signal Co",
                "site_city": "Orlando",
                "site_state": "FL",
                "inspection_type": "Complaint",
                "date_opened": "2026-03-16",
                "lead_score": 8,
                "source_url": "https://example.com/fl",
                "first_seen_at": "2026-03-17T12:00:00+00:00",
            },
            {
                "establishment_name": "Georgia Signal Co",
                "site_city": "Atlanta",
                "site_state": "GA",
                "inspection_type": "Referral",
                "date_opened": "2026-03-15",
                "lead_score": 7,
                "source_url": "https://example.com/ga",
                "first_seen_at": "2026-03-17T11:00:00+00:00",
            },
        ]
        presentation = scoring_digest_intelligence.build_digest_presentation(snap_rows, section_kind="starter_snapshot")

        html = generate_digest_html(
            leads=snap_rows,
            low_fallback=[],
            config=config,
            gen_date="2026-03-18",
            mode="daily",
            territory_code="FACS_TRIAL_STATES",
            content_filter="high_medium",
            include_low_fallback=False,
            branding=self.branding,
            tier_counts={"high": 1, "medium": 1, "low": 0},
            include_lows=False,
            low_priority=[],
            footer_html=self.footer_html,
            report_label="Starter snapshot",
            summary_label="2 signals in last 14 days",
            state_summary_states=["FL", "GA", "AL"],
            intro_summary_html=presentation["intro_html"],
            top_pick_rows=presentation["top_picks"],
            top_pick_heading=presentation["top_pick_heading"],
        )
        text = generate_digest_text(
            leads=snap_rows,
            low_fallback=[],
            config=config,
            gen_date="2026-03-18",
            mode="daily",
            territory_code="FACS_TRIAL_STATES",
            content_filter="high_medium",
            include_low_fallback=False,
            branding=self.branding,
            tier_counts={"high": 1, "medium": 1, "low": 0},
            include_lows=False,
            low_priority=[],
            footer_text=self.footer_text,
            report_label="Starter snapshot",
            summary_label="2 signals in last 14 days",
            state_summary_states=["FL", "GA", "AL"],
            intro_summary_text=presentation["intro_text"],
            top_pick_rows=presentation["top_picks"],
            top_pick_heading=presentation["top_pick_heading"],
        )

        self.assertIn("Signals in this snapshot:</strong> FL 1 | GA 1 | AL 0", html)
        self.assertIn("Signals in this snapshot: FL 1 | GA 1 | AL 0", text)
        self.assertIn("Starter snapshot: 2 recent OSHA signals across FL, GA, and AL.", html)

    def test_raw_code_only_falls_back_to_safe_customer_facing_label(self) -> None:
        config = {
            "states": [],
            "top_k_overall": 25,
            "top_k_per_state": 10,
        }
        leads = [
            {
                "establishment_name": "Fallback Co",
                "site_city": "Austin",
                "site_state": "TX",
                "inspection_type": "Complaint",
                "date_opened": "2026-03-16",
                "lead_score": 8,
                "source_url": "https://example.com/fallback",
                "first_seen_at": "2026-03-17T12:00:00+00:00",
            }
        ]
        subject = build_digest_subject(
            config=config,
            territory_code="RAW_INTERNAL_CODE",
            gen_date="2026-03-18",
            states=config["states"],
        )
        self.assertEqual("Your coverage area OSHA Signals — 2026-03-18", subject)

        html = generate_digest_html(
            leads=leads,
            low_fallback=[],
            config=config,
            gen_date="2026-03-18",
            mode="daily",
            territory_code="RAW_INTERNAL_CODE",
            content_filter="high_medium",
            include_low_fallback=False,
            branding=self.branding,
            tier_counts={"high": 1, "medium": 0, "low": 0},
            include_lows=False,
            low_priority=[],
            footer_html=self.footer_html,
            summary_label="1 signal today",
        )
        text = generate_digest_text(
            leads=leads,
            low_fallback=[],
            config=config,
            gen_date="2026-03-18",
            mode="daily",
            territory_code="RAW_INTERNAL_CODE",
            content_filter="high_medium",
            include_low_fallback=False,
            branding=self.branding,
            tier_counts={"high": 1, "medium": 0, "low": 0},
            include_lows=False,
            low_priority=[],
            footer_text=self.footer_text,
            summary_label="1 signal today",
        )

        self.assertIn("Coverage:</strong> Your coverage area", html)
        self.assertIn("Coverage: Your coverage area", text)
        self.assertNotIn("RAW_INTERNAL_CODE", html)
        self.assertNotIn("RAW_INTERNAL_CODE", text)

    def test_footer_and_list_unsubscribe_headers_remain_unchanged(self) -> None:
        footer_html = build_footer_html(
            brand_name=self.branding["brand_name"],
            mailing_address=self.branding["mailing_address"],
            disclaimer="Informational only. Not legal advice.",
            reply_to=self.branding["reply_to"],
            unsub_url="https://unsub.example/oneclick",
        )
        footer_text = build_footer_text(
            brand_name=self.branding["brand_name"],
            mailing_address=self.branding["mailing_address"],
            disclaimer="Informational only. Not legal advice.",
            reply_to=self.branding["reply_to"],
            unsub_url="https://unsub.example/oneclick",
            include_separator=True,
        )
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
            include_lows=False,
            low_priority=[],
            footer_html=footer_html,
            summary_label="0 signals today",
        )
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
            include_lows=False,
            low_priority=[],
            footer_text=footer_text,
            summary_label="0 signals today",
        )
        msg = build_email_message(
            recipient="test@example.com",
            subject="Texas Triangle OSHA Signals — 2026-02-08",
            html_body=html,
            text_body=text,
            customer_id="cust1",
            territory_code="TX_TRIANGLE_V1",
            branding=self.branding,
            list_unsub="<mailto:support@acme.com?subject=unsubscribe>, <https://unsub.example/oneclick>",
            list_unsub_post="List-Unsubscribe=One-Click",
        )

        self.assertEqual(1, html.count("https://unsub.example/oneclick"))
        self.assertEqual(1, text.count("https://unsub.example/oneclick"))
        self.assertIn("mailto:support@acme.com?subject=unsubscribe", msg["List-Unsubscribe"])
        self.assertIn("https://unsub.example/oneclick", msg["List-Unsubscribe"])
        self.assertEqual("List-Unsubscribe=One-Click", msg["List-Unsubscribe-Post"])

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
        enable_url = (
            "https://unsub.microflowops.com/prefs/enable_lows?"
            "token=abc.def&subscriber_key=sub_tx_triangle_v1_0000000000&territory_code=TX_TRIANGLE_V1"
        )

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
            enable_lows_url=enable_url,
            include_lows=False,
            low_priority=[],
            footer_html=self.footer_html,
            summary_label="Newly observed today: 0 signals",
            snapshot_label="Last 14 days snapshot (not new)",
            snapshot_days=14,
            snapshot_tier_counts=snap_tiers,
            snapshot_enable_lows_url=None,
            snapshot_rows=snap_rows,
            snapshot_total=1,
        )
        self.assertEqual(1, html.count(">Enable lows</a>"))
        self.assertIn(enable_url, html)
        self.assertNotIn("Low-priority signals available:", html)
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
            enable_lows_url=enable_url,
            include_lows=False,
            low_priority=[],
            footer_text=self.footer_text,
            summary_label="Newly observed today: 0 signals",
            snapshot_label="Last 14 days snapshot (not new)",
            snapshot_days=14,
            snapshot_tier_counts=snap_tiers,
            snapshot_enable_lows_url=None,
            snapshot_rows=snap_rows,
            snapshot_total=1,
        )
        self.assertEqual(1, text.count("Enable lows:"))
        self.assertIn(enable_url, text)
        self.assertNotIn("Low-priority signals available:", text)
        self.assertNotIn("Also observed (not shown)", text)

    def test_snapshot_rows_include_lows_when_enabled(self) -> None:
        snapshot_all = [
            {
                "establishment_name": "Low Snapshot Co",
                "site_city": "Austin",
                "site_state": "TX",
                "inspection_type": "Planned",
                "date_opened": "2026-02-01",
                "lead_score": 2,
                "source_url": "https://example.com/low",
                "last_seen_at": "2026-02-08T12:00:00+00:00",
            },
            {
                "establishment_name": "High Snapshot Co",
                "site_city": "Austin",
                "site_state": "TX",
                "inspection_type": "Complaint",
                "date_opened": "2026-02-01",
                "lead_score": 9,
                "source_url": "https://example.com/high",
                "last_seen_at": "2026-02-07T12:00:00+00:00",
            },
        ]

        rows_on, total_on = _select_snapshot_rows(snapshot_all, include_lows=True, medium_min=6, limit=25)
        self.assertEqual(2, total_on)
        self.assertTrue(any(r.get("establishment_name") == "Low Snapshot Co" for r in rows_on))

        rows_off, total_off = _select_snapshot_rows(snapshot_all, include_lows=False, medium_min=6, limit=25)
        self.assertEqual(1, total_off)
        self.assertFalse(any(r.get("establishment_name") == "Low Snapshot Co" for r in rows_off))

    def test_snapshot_section_shows_low_rows_when_enabled(self) -> None:
        snap_rows = [
            {
                "establishment_name": "Example Low Co",
                "site_city": "Austin",
                "site_state": "TX",
                "inspection_type": "Planned",
                "date_opened": "2026-02-01",
                "lead_score": 2,
                "source_url": "https://example.com/low",
                "first_seen_at": "2026-02-02T12:00:00+00:00",
            }
        ]
        snap_tiers = {"high": 0, "medium": 0, "low": 1}

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
            disable_lows_url=None,
            include_lows=True,
            low_priority=[],
            footer_html=self.footer_html,
            summary_label="Newly observed today: 0 signals",
            snapshot_label="Last 14 days snapshot (not new)",
            snapshot_days=14,
            snapshot_tier_counts=snap_tiers,
            snapshot_enable_lows_url="https://example.invalid/should_not_render",
            snapshot_disable_lows_url="https://unsub.example/prefs/disable_lows?token=x.y",
            snapshot_rows=snap_rows,
            snapshot_total=1,
        )
        self.assertIn("Example Low Co", html)
        self.assertNotIn("(not shown)", html)
        self.assertNotIn("Enable lows", html)
        self.assertIn("Low signals: <strong>ON</strong> (showing 1 of 1 low signals)", html)
        self.assertNotIn("https://unsub.example/prefs/disable_lows?token=x.y", html)

    def test_daily_low_signals_cta_renders_after_signals_section(self) -> None:
        leads = [
            {
                "establishment_name": "Example Priority Co",
                "site_city": "Austin",
                "site_state": "TX",
                "inspection_type": "Complaint",
                "date_opened": "2026-02-01",
                "lead_score": 8,
                "source_url": "https://example.com/x",
                "first_seen_at": "2026-02-02T12:00:00+00:00",
            }
        ]
        html = generate_digest_html(
            leads=leads,
            low_fallback=[],
            config=self.config,
            gen_date="2026-02-08",
            mode="daily",
            territory_code="TX_TRIANGLE_V1",
            content_filter="high_medium",
            include_low_fallback=False,
            branding=self.branding,
            tier_counts={"high": 1, "medium": 0, "low": 2},
            low_available_today=2,
            enable_lows_url="https://example.com/enable",
            include_lows=False,
            low_priority=[],
            footer_html=self.footer_html,
            summary_label="1 signal today",
        )
        text = generate_digest_text(
            leads=leads,
            low_fallback=[],
            config=self.config,
            gen_date="2026-02-08",
            mode="daily",
            territory_code="TX_TRIANGLE_V1",
            content_filter="high_medium",
            include_low_fallback=False,
            branding=self.branding,
            tier_counts={"high": 1, "medium": 0, "low": 2},
            low_available_today=2,
            enable_lows_url="https://example.com/enable",
            include_lows=False,
            low_priority=[],
            footer_text=self.footer_text,
            summary_label="1 signal today",
        )

        self.assertGreater(html.index("Low signals:"), html.index("<h2>Signals</h2>"))
        self.assertGreater(text.index("Low signals:"), text.index("Signals:"))

    def test_trial_digest_copy_adds_outreach_labels_and_optional_contact_details(self) -> None:
        leads = [
            {
                "establishment_name": "Example Priority Co",
                "site_city": "Austin",
                "site_state": "TX",
                "inspection_type": "Complaint",
                "date_opened": "2026-02-01",
                "lead_score": 8,
                "source_url": "https://example.com/x",
                "first_seen_at": "2026-02-02T12:00:00+00:00",
                "presentation_reason_sentence": "Public complaint signal observed recently.",
                "website": "https://examplepriorityco.test",
                "phone": "(512) 555-0100",
                "contact_name": "Jordan Safety",
                "contact_email": "jordan@examplepriorityco.test",
            }
        ]

        html = generate_digest_html(
            leads=leads,
            low_fallback=[],
            config=self.config,
            gen_date="2026-02-08",
            mode="daily",
            territory_code="TX_TRIANGLE_V1",
            content_filter="high_medium",
            include_low_fallback=False,
            branding=self.branding,
            tier_counts={"high": 0, "medium": 1, "low": 0},
            include_lows=False,
            low_priority=[],
            footer_html=self.footer_html,
            summary_label="Newly observed today: 1 signal",
            top_pick_rows=leads,
            top_pick_heading="Best 3 outreach targets today",
            digest_title="Outreach-ready OSHA leads",
            summary_note="Meant to help business development teams spot employers who may need help now and verify the public record quickly.",
            bottom_cta="Reply with your state or metro if you want a tighter territory sample. If the fit looks right, ask about the 30-day Founding Pilot.",
            trial_copy=True,
        )
        text = generate_digest_text(
            leads=leads,
            low_fallback=[],
            config=self.config,
            gen_date="2026-02-08",
            mode="daily",
            territory_code="TX_TRIANGLE_V1",
            content_filter="high_medium",
            include_low_fallback=False,
            branding=self.branding,
            tier_counts={"high": 0, "medium": 1, "low": 0},
            include_lows=False,
            low_priority=[],
            footer_text=self.footer_text,
            summary_label="Newly observed today: 1 signal",
            top_pick_rows=leads,
            top_pick_heading="Best 3 outreach targets today",
            digest_title="Outreach-ready OSHA leads",
            summary_note="Meant to help business development teams spot employers who may need help now and verify the public record quickly.",
            bottom_cta="Reply with your state or metro if you want a tighter territory sample. If the fit looks right, ask about the 30-day Founding Pilot.",
            trial_copy=True,
        )

        self.assertIn("Outreach-ready OSHA leads", html)
        self.assertIn("Best 3 outreach targets today", html)
        self.assertIn("Why this may matter now", html)
        self.assertIn("Website:</strong> https://examplepriorityco.test", html)
        self.assertIn("Phone:</strong> (512) 555-0100", html)
        self.assertIn("Public contact:</strong> Jordan Safety &lt;jordan@examplepriorityco.test&gt;", html)
        self.assertIn("Reply with your state or metro", html)

        self.assertIn("Outreach-ready OSHA leads - 2026-02-08", text)
        self.assertIn("Best 3 outreach targets today:", text)
        self.assertIn("Why this may matter now", text)
        self.assertIn("Website: https://examplepriorityco.test", text)
        self.assertIn("Phone: (512) 555-0100", text)
        self.assertIn("Public contact: Jordan Safety <jordan@examplepriorityco.test>", text)
        self.assertIn("Reply with your state or metro", text)


if __name__ == "__main__":
    unittest.main()
