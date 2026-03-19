import os
import unittest
from unittest import mock

from outreach import scraper_engine


class TestScraperEngine(unittest.TestCase):
    def test_probe_crawl4ai_runtime_missing_package(self):
        with mock.patch("outreach.scraper_engine._lazy_import_crawl4ai", return_value=(None, "ImportError:no module")):
            result = scraper_engine.probe_crawl4ai_runtime()
        self.assertFalse(result["crawl4ai_installed"])
        self.assertFalse(result["playwright_browsers_installed"])
        self.assertEqual(result["warn_token"], scraper_engine.WARN_CRAWL4AI_NOT_INSTALLED)

    def test_extract_contacts_regex(self):
        result = scraper_engine.extract_contacts_regex("Call (555) 123-4567 or email Jane@Example.com")
        self.assertIn("jane@example.com", result["emails"])
        self.assertTrue(any("555" in p for p in result["phones"]))

    def test_probe_source_availability_bcsp_uses_state_search_probe(self):
        with mock.patch(
            "outreach.prospect_sources_bcsp.doctor_probe_bcsp",
            return_value={"ok": False, "reason": "unfiltered_global_results", "status": 200, "rows_found": 0},
        ):
            result = scraper_engine.probe_source_availability("BCSP")
        self.assertFalse(result["available"])
        self.assertEqual(result["reason"], "unfiltered_global_results")
        self.assertEqual(result["rows_found"], 0)

    def test_probe_source_availability_bluebook_uses_doctor_probe(self):
        with mock.patch(
            "outreach.prospect_sources_bluebook.doctor_probe_bluebook",
            return_value={"ok": True, "status": 200, "rows_found": 4, "parse_mode": "BLUEBOOK_SEARCH_RESULTS"},
        ):
            result = scraper_engine.probe_source_availability("BLUEBOOK")
        self.assertTrue(result["available"])
        self.assertEqual(result["reason"], "BLUEBOOK_SEARCH_RESULTS")
        self.assertEqual(result["rows_found"], 4)

    def test_extract_llm_optional_disabled_default(self):
        with mock.patch.dict(os.environ, {"PROSPECT_AUTOGROW_LLM_ENABLED": "0"}, clear=False):
            result = scraper_engine.extract_llm_optional("hello", instruction="extract rows")
        self.assertFalse(result["enabled"])
        self.assertEqual(result["mode"], "DISABLED")

    def test_email_waterfall_generates_pattern(self):
        rows = [
            {
                "first_name": "Jane",
                "last_name": "Doe",
                "domain": "example.com",
                "firm": "Example Co",
                "state": "TX",
            }
        ]
        out = scraper_engine.apply_email_resolution_waterfall(rows)
        self.assertEqual(out[0]["email_status"], "pattern_generated")
        self.assertIn("@example.com", out[0]["email"])

    def test_email_waterfall_canonical_mode_keeps_nonfree_source_email(self):
        rows = [{"email": "owner@example.com", "website": "https://example.com", "source": "bluebook:1"}]
        out = scraper_engine.apply_email_resolution_waterfall(
            rows,
            allow_pattern_email=False,
            require_nonfree_source_email=True,
            require_nonfree_site_email=True,
            unresolved_status="",
        )
        self.assertEqual(out[0]["email"], "owner@example.com")
        self.assertEqual(out[0]["email_status"], "scraped_from_source")

    def test_email_waterfall_canonical_mode_uses_public_site_email_without_pattern_guess(self):
        rows = [{"website": "https://example.com", "domain": "example.com", "source": "bluebook:2"}]
        with mock.patch(
            "outreach.scraper_engine.fetch_contact_pages_for_domain",
            return_value=[{"contacts": {"emails": ["info@example.com"]}}],
        ):
            out = scraper_engine.apply_email_resolution_waterfall(
                rows,
                allow_pattern_email=False,
                require_nonfree_source_email=True,
                require_nonfree_site_email=True,
                unresolved_status="",
            )
        self.assertEqual(out[0]["email"], "info@example.com")
        self.assertEqual(out[0]["email_status"], "scraped_from_site")

    def test_email_waterfall_canonical_mode_leaves_unresolved_blank(self):
        rows = [{"website": "https://example.com", "domain": "example.com", "source": "bluebook:3"}]
        with mock.patch("outreach.scraper_engine.fetch_contact_pages_for_domain", return_value=[]):
            out = scraper_engine.apply_email_resolution_waterfall(
                rows,
                allow_pattern_email=False,
                require_nonfree_source_email=True,
                require_nonfree_site_email=True,
                unresolved_status="",
            )
        self.assertEqual(out[0].get("email", ""), "")
        self.assertEqual(out[0].get("contact_email", ""), "")
        self.assertNotIn("email_status", out[0])

    def test_crawl_page_with_storage_state_requires_existing_file(self):
        result = scraper_engine.crawl_page_with_storage_state(
            "https://buyersguide.ohsonline.com/",
            storage_state_path="C:\\nonexistent\\storage_state.json",
        )
        self.assertFalse(result["ok"])
        self.assertEqual(result["error"], "missing_storage_state_file")


if __name__ == "__main__":
    unittest.main()
