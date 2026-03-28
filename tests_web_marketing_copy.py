import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parent
LAYOUT_PAGE = ROOT / "web" / "app" / "layout.tsx"
HOME_PAGE = ROOT / "web" / "app" / "page.tsx"
HOW_IT_WORKS_PAGE = ROOT / "web" / "app" / "how-it-works" / "page.tsx"
PRICING_PAGE = ROOT / "web" / "app" / "pricing" / "page.tsx"
CONTACT_PAGE = ROOT / "web" / "app" / "contact" / "page.tsx"
FAQ_PAGE = ROOT / "web" / "app" / "faq" / "page.tsx"
ONBOARDING_PAGE = ROOT / "web" / "app" / "onboarding" / "page.tsx"
TRIAL_ROUTE = ROOT / "web" / "app" / "api" / "trial-request" / "route.ts"
LOCAL_PREVIEW_PAGE = ROOT / "web" / "app" / "local-preview" / "outreach-followups" / "page.tsx"
CTA_BUTTONS = ROOT / "web" / "components" / "CTAButtons.tsx"
COPY_PACKET = ROOT / "docs" / "PAID_PILOT_CONVERSION_COPY.md"


class TestWebMarketingCopy(unittest.TestCase):
    def test_metadata_uses_outreach_ready_positioning(self):
        layout_text = LAYOUT_PAGE.read_text(encoding="utf-8")
        self.assertIn("Outreach-Ready OSHA Leads", layout_text)
        self.assertIn("Outreach-ready OSHA leads for safety consulting firms.", layout_text)
        self.assertIn("state or region", layout_text)
        self.assertNotIn("territory-based (metro)", layout_text)

    def test_home_page_centers_sample_first_outbound_positioning(self):
        home_text = HOME_PAGE.read_text(encoding="utf-8")
        self.assertIn("See newly observed public OSHA activity before citations post.", home_text)
        self.assertIn("Founding Pilot: $149 for 30 days in one state.", home_text)
        self.assertIn("Sample = one example digest for your state or region.", home_text)
        self.assertIn("Need live proof? Ask about a 14-day trial.", home_text)
        self.assertIn("What &quot;usable&quot; means", home_text)
        self.assertIn("Best for safety consulting and training firms", home_text)
        self.assertIn("Less useful for teams looking for a full compliance workflow", home_text)
        self.assertIn("Request a sample for your state or region.", home_text)
        self.assertNotIn("We confirm mapping before billing", home_text)
        self.assertNotIn("Why Timing Matters", home_text)
        self.assertNotIn("What Is Included", home_text)
        self.assertNotIn("Founder note", home_text)

    def test_pricing_page_adds_founding_pilot_and_removes_public_free_trial_language(self):
        pricing_text = PRICING_PAGE.read_text(encoding="utf-8")
        self.assertIn("Founding Pilot", pricing_text)
        self.assertIn("Standard", pricing_text)
        self.assertIn("$149", pricing_text)
        self.assertIn("$299", pricing_text)
        self.assertIn("$499", pricing_text)
        self.assertIn("Request a sample", pricing_text)
        self.assertIn("Simple pricing.", pricing_text)
        self.assertIn("Want to preview lead quality first? Request a sample.", pricing_text)
        self.assertIn("Tell us your state or region. State, metro, counties, or OSHA area all work. We confirm fit", pricing_text)
        self.assertNotIn("What most buyers do first", pricing_text)
        self.assertNotIn("See a sample for your state or region first.", pricing_text)
        self.assertNotIn("Request a sample first if you want to see the lead quality before we activate the pilot.", pricing_text)
        self.assertNotIn("CoverageEstimator", pricing_text)
        self.assertNotIn("current coverage model", pricing_text)
        self.assertNotIn("Coverage Estimator", pricing_text)

    def test_key_pages_drop_old_coverage_translation_phrase(self):
        page_texts = [
            HOME_PAGE.read_text(encoding="utf-8"),
            HOW_IT_WORKS_PAGE.read_text(encoding="utf-8"),
            PRICING_PAGE.read_text(encoding="utf-8"),
            CONTACT_PAGE.read_text(encoding="utf-8"),
            FAQ_PAGE.read_text(encoding="utf-8"),
            ONBOARDING_PAGE.read_text(encoding="utf-8"),
        ]
        for text in page_texts:
            self.assertNotIn("we translate coverage for you", text)

    def test_shared_cta_and_onboarding_use_updated_territory_language(self):
        cta_text = CTA_BUTTONS.read_text(encoding="utf-8")
        onboarding_text = ONBOARDING_PAGE.read_text(encoding="utf-8")
        self.assertIn("Tell us your state or region", cta_text)
        self.assertNotIn("Reply with your state or metro", cta_text)
        self.assertIn("Tell us your state, metro, counties, or OSHA area. We confirm fit before activation.", onboarding_text)
        self.assertIn("Standard supports one primary state or region setup.", onboarding_text)
        self.assertNotIn("Set your coverage", onboarding_text)

    def test_contact_and_faq_pages_match_manual_qualification_flow(self):
        contact_text = CONTACT_PAGE.read_text(encoding="utf-8")
        faq_text = FAQ_PAGE.read_text(encoding="utf-8")
        self.assertIn("Request a sample or start a founding pilot.", contact_text)
        self.assertIn("Manual qualification required before activation.", contact_text)
        self.assertIn("Verify in 30 seconds", contact_text)
        self.assertIn("Sample = one example digest for your state or region.", contact_text)
        self.assertIn("Need live proof? Ask about a 14-day trial.", contact_text)
        self.assertIn("State, metro, counties, or OSHA area all work.", contact_text)
        self.assertNotIn("14 days and up to 4 metros", contact_text)
        self.assertIn("Who is this best for?", faq_text)
        self.assertIn("Who is this not ideal for?", faq_text)
        self.assertIn("What happens first?", faq_text)
        self.assertIn("How does the Founding Pilot work?", faq_text)
        self.assertIn("Verify in 30 seconds", faq_text)
        self.assertIn("not a full compliance workflow", faq_text)

    def test_trial_request_route_uses_intent_and_manual_qualification_copy(self):
        route_text = TRIAL_ROUTE.read_text(encoding="utf-8")
        self.assertIn("Request type:", route_text)
        self.assertIn("Manual qualification required:", route_text)
        self.assertIn("We received your MicroFlowOps request.", route_text)
        self.assertIn("Request a sample first, then confirm whether Founding Pilot, Standard, or Multi-Territory is the right next step.", route_text)
        self.assertIn("Best for safety consulting and training firms already doing outbound", route_text)
        self.assertIn("Sample = one example digest for your state or region.", route_text)
        self.assertIn("Need live proof? Ask about a 14-day trial.", route_text)
        self.assertIn("State, metro, counties, or OSHA area all work.", route_text)
        self.assertNotIn("14-day free trial", route_text)

    def test_local_followup_preview_route_is_dev_only_and_not_indexable(self):
        preview_text = LOCAL_PREVIEW_PAGE.read_text(encoding="utf-8")
        self.assertIn('title: "Local Outreach Follow-Up Preview"', preview_text)
        self.assertIn('robots: { index: false, follow: false, nocache: true }', preview_text)
        self.assertIn('if (process.env.NODE_ENV !== "development") {', preview_text)
        self.assertIn("notFound();", preview_text)
        self.assertIn("Why timing matters on these leads", preview_text)
        self.assertIn("30-day founding pilot", preview_text)

    def test_copy_packet_doc_exists_with_required_sections(self):
        packet_text = COPY_PACKET.read_text(encoding="utf-8")
        self.assertIn("## A. Revised Homepage Copy Sections", packet_text)
        self.assertIn("## B. Revised Pricing Page Copy", packet_text)
        self.assertIn("## C. 3-Email Cold Outbound Sequence", packet_text)
        self.assertIn("## H. Fastest / Highest-ROI Implementation Notes", packet_text)


if __name__ == "__main__":
    unittest.main()
