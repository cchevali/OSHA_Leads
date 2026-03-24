import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parent
LAYOUT_PAGE = ROOT / "web" / "app" / "layout.tsx"
HOME_PAGE = ROOT / "web" / "app" / "page.tsx"
HOW_IT_WORKS_PAGE = ROOT / "web" / "app" / "how-it-works" / "page.tsx"
PRICING_PAGE = ROOT / "web" / "app" / "pricing" / "page.tsx"
CONTACT_PAGE = ROOT / "web" / "app" / "contact" / "page.tsx"
FAQ_PAGE = ROOT / "web" / "app" / "faq" / "page.tsx"
TRIAL_ROUTE = ROOT / "web" / "app" / "api" / "trial-request" / "route.ts"


class TestWebMarketingCopy(unittest.TestCase):
    def test_metadata_drops_stale_metro_only_positioning(self):
        layout_text = LAYOUT_PAGE.read_text(encoding="utf-8")
        self.assertNotIn("territory-based (metro)", layout_text)
        self.assertIn("Counties, cities, metros, or OSHA areas work", layout_text)

    def test_home_page_uses_frozen_snapshot_proof(self):
        home_text = HOME_PAGE.read_text(encoding="utf-8")
        self.assertIn("Frozen public sample", home_text)
        self.assertIn("Verify in 30 seconds", home_text)
        self.assertIn("We confirm mapping before billing", home_text)
        self.assertNotIn("Example City, ST", home_text)
        self.assertIn("What a buyer can confirm", home_text)
        self.assertNotIn("Why this was actionable", home_text)

    def test_pricing_page_keeps_public_plan_promises(self):
        pricing_text = PRICING_PAGE.read_text(encoding="utf-8")
        self.assertIn("$299", pricing_text)
        self.assertIn("$499", pricing_text)
        self.assertIn("14 days", pricing_text)
        self.assertIn("Up to 4 metros", pricing_text)
        self.assertIn("We confirm mapping before billing", pricing_text)

    def test_key_pages_reduce_duplicate_coverage_phrase(self):
        page_texts = [
            HOME_PAGE.read_text(encoding="utf-8"),
            HOW_IT_WORKS_PAGE.read_text(encoding="utf-8"),
            PRICING_PAGE.read_text(encoding="utf-8"),
            CONTACT_PAGE.read_text(encoding="utf-8"),
            FAQ_PAGE.read_text(encoding="utf-8"),
        ]
        for text in page_texts:
            self.assertNotIn("we translate coverage for you", text)

    def test_public_pages_keep_contact_and_faq_trust_copy(self):
        contact_text = CONTACT_PAGE.read_text(encoding="utf-8")
        faq_text = FAQ_PAGE.read_text(encoding="utf-8")
        self.assertIn("14 days and up to 4 metros", contact_text)
        self.assertIn("Verify in 30 seconds", contact_text)
        self.assertIn("We confirm mapping before billing", contact_text)
        self.assertIn("Why is the public sample frozen instead of live?", faq_text)
        self.assertIn("Is onboarding handled over email only?", faq_text)
        self.assertIn("Verify in 30 seconds", faq_text)

    def test_trial_request_confirmation_uses_coverage_language(self):
        route_text = TRIAL_ROUTE.read_text(encoding="utf-8")
        self.assertIn("Coverage requested:", route_text)
        self.assertIn("We captured this coverage:", route_text)
        self.assertNotIn("`Metros: ${metros}`", route_text)


if __name__ == "__main__":
    unittest.main()
