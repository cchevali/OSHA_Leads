import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parent
LAYOUT_PAGE = ROOT / "web" / "app" / "layout.tsx"
HOME_PAGE = ROOT / "web" / "app" / "page.tsx"
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
        self.assertIn("Frozen recent public snapshot", home_text)
        self.assertNotIn("Example City, ST", home_text)
        self.assertIn("Why this was actionable", home_text)

    def test_public_pages_include_county_language_and_frozen_sample_faq(self):
        pricing_text = PRICING_PAGE.read_text(encoding="utf-8")
        contact_text = CONTACT_PAGE.read_text(encoding="utf-8")
        faq_text = FAQ_PAGE.read_text(encoding="utf-8")
        self.assertIn("Counties, cities, metros, or OSHA areas work", pricing_text)
        self.assertIn("Counties, cities, metros, or OSHA areas work", contact_text)
        self.assertIn("Why is the public sample frozen instead of live?", faq_text)

    def test_trial_request_confirmation_uses_coverage_language(self):
        route_text = TRIAL_ROUTE.read_text(encoding="utf-8")
        self.assertIn("Coverage requested:", route_text)
        self.assertIn("We captured this coverage:", route_text)
        self.assertNotIn("`Metros: ${metros}`", route_text)


if __name__ == "__main__":
    unittest.main()
