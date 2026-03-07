import json
import tempfile
import unittest
from datetime import date, datetime, timezone
from pathlib import Path

from outreach import prospect_sources_ohs_bg as ohs_bg


REPO_ROOT = Path(__file__).resolve().parent
FIXTURES = REPO_ROOT / "tests" / "fixtures" / "ohs_bg"


class TestProspectSourcesOhsBg(unittest.TestCase):
    def _read(self, name: str) -> str:
        return (FIXTURES / name).read_text(encoding="utf-8")

    def test_parse_text_page_filters_product_only_and_extracts_rows(self):
        rows, mode, diag = ohs_bg.parse_ohs_bg_page(self._read("page_ca_1.html"), "p1")
        self.assertEqual(mode, "TEXT")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["email"], "jane@acmesafetyca.com")
        self.assertEqual(rows[0]["state"], "CA")
        self.assertEqual(rows[0]["source"], "ohs_buyers_guide:1001")
        self.assertEqual(int(diag.get("allowlist_rejected", 0)), 1)

    def test_parse_jsonld_page(self):
        rows, mode, _diag = ohs_bg.parse_ohs_bg_page(self._read("page_jsonld.html"), "p1")
        self.assertIn(mode, {"JSON_LD", "MIXED"})
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["email"], "hello@deltacompliance.com")
        self.assertEqual(rows[0]["city"], "Los Angeles")
        self.assertEqual(rows[0]["state"], "CA")

    def test_fetch_uses_cache_metadata_timestamp(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            cache_dir = root / "cache"
            diagnostics_dir = root / "diagnostics"
            cache_dir.mkdir(parents=True, exist_ok=True)
            fetched_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
            cache_payload = {
                "source": "ohs_buyers_guide",
                "state": "CA",
                "fetched_at_utc": fetched_at,
                "cache_schema_version": ohs_bg.CACHE_SCHEMA_VERSION,
                "pages_fetched": 1,
                "parse_mode": "TEXT",
                "parse_counters": {"fetched_pages": 1},
                "parse_reasons": {"selector_missing": 0},
                "rows": [{"email": "cache@example.com", "state": "CA"}],
            }
            cache_path = ohs_bg._cache_path(cache_dir, "CA")
            cache_path.write_text(json.dumps(cache_payload), encoding="utf-8")

            def failing_fetcher(_url: str):
                raise AssertionError("fetcher should not be called for fresh cache")

            result = ohs_bg.fetch_ohs_bg_state_rows(
                state="CA",
                run_date=date(2026, 2, 24),
                max_pages=3,
                sleep_ms=0,
                cache_dir=cache_dir,
                diagnostics_dir=diagnostics_dir,
                fetcher=failing_fetcher,
                allow_cache_write=False,
            )
            self.assertTrue(result["cache_used"])
            self.assertEqual(result["rows"][0]["email"], "cache@example.com")

    def test_fetch_ignores_cache_when_schema_is_incompatible(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            cache_dir = root / "cache"
            diagnostics_dir = root / "diagnostics"
            cache_dir.mkdir(parents=True, exist_ok=True)
            fetched_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
            cache_payload = {
                "source": "ohs_buyers_guide",
                "state": "TX",
                "fetched_at_utc": fetched_at,
                "cache_schema_version": 1,
                "rows": [{"email": "stale@example.com", "state": "TX"}],
            }
            cache_path = ohs_bg._cache_path(cache_dir, "TX")
            cache_path.write_text(json.dumps(cache_payload), encoding="utf-8")

            calls = {"count": 0}

            def browser_fetcher(_url: str) -> dict:
                calls["count"] += 1
                return {"ok": True, "status": 200, "html": self._read("buyersguide_category_selector_missing.html")}

            result = ohs_bg.fetch_ohs_bg_state_rows(
                state="TX",
                run_date=date(2026, 3, 7),
                max_pages=1,
                sleep_ms=0,
                cache_dir=cache_dir,
                diagnostics_dir=diagnostics_dir,
                allow_cache_write=False,
                browser_fetcher=browser_fetcher,
            )
            self.assertFalse(result["cache_used"])
            self.assertGreaterEqual(calls["count"], 1)

    def test_fetch_browser_primary_parses_company_and_recovers_email(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            cache_dir = root / "cache"
            diagnostics_dir = root / "diagnostics"

            category_url = ohs_bg.BROWSER_CATEGORY_URL
            company_101 = "https://buyersguide.ohsonline.com/company/101/alpha-safety-partners"
            company_202 = "https://buyersguide.ohsonline.com/company/202/bravo-risk-consulting"

            def browser_fetcher(url: str) -> dict:
                if url == category_url:
                    return {"ok": True, "status": 200, "html": self._read("buyersguide_category_page1.html")}
                if url == company_101:
                    return {"ok": True, "status": 200, "html": self._read("buyersguide_company_101.html")}
                if url == company_202:
                    return {"ok": True, "status": 200, "html": self._read("buyersguide_company_202_missing_firm.html")}
                return {"ok": False, "status": 404, "html": "", "error": "not_found"}

            def contact_fetcher(url: str):
                if url.endswith("/contact"):
                    return 200, "<html><body>Contact us: team@alpha-safety.example</body></html>"
                return 200, "<html><body>No contact email</body></html>"

            result = ohs_bg.fetch_ohs_bg_state_rows(
                state="TX",
                run_date=date(2026, 3, 6),
                max_pages=1,
                sleep_ms=0,
                cache_dir=cache_dir,
                diagnostics_dir=diagnostics_dir,
                allow_cache_write=False,
                browser_fetcher=browser_fetcher,
                contact_fetcher=contact_fetcher,
            )
            self.assertEqual(result["parse_mode"], "BROWSER")
            self.assertEqual(result.get("fetch_strategy"), "HYBRID_BROWSER_PRIMARY")
            rows = list(result.get("rows") or [])
            self.assertEqual(len(rows), 1)
            row = rows[0]
            self.assertEqual(row["firm"], "Alpha Safety Partners")
            self.assertEqual(row["state"], "TX")
            self.assertEqual(row["source"], "ohs_buyers_guide:company-101")
            self.assertEqual(row["email"], "team@alpha-safety.example")
            parse_counters = dict(result.get("parse_counters") or {})
            parse_reasons = dict(result.get("parse_reasons") or {})
            self.assertEqual(int(parse_counters.get("candidate_rows_seen") or 0), 2)
            self.assertEqual(int(parse_counters.get("parsed_rows_accepted") or 0), 1)
            self.assertEqual(int(parse_counters.get("parsed_rows_rejected") or 0), 1)
            self.assertGreaterEqual(int(parse_counters.get("non_profile_links_filtered") or 0), 1)
            self.assertGreaterEqual(int(parse_counters.get("auth_gated_pages") or 0), 1)
            self.assertEqual(int(parse_reasons.get("missing_firm") or 0), 1)
            self.assertTrue(Path(result["diagnostics_path"]).exists())

    def test_fetch_browser_failure_falls_back_to_legacy_http_parser(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            cache_dir = root / "cache"
            diagnostics_dir = root / "diagnostics"

            page1_url = "https://www.ohsonline.com/Directory/SearchResults.aspx?state=CA"
            page2_url = "https://www.ohsonline.com/Directory/SearchResults.aspx?state=CA&page=2"

            def browser_fetcher(_url: str) -> dict:
                return {"ok": False, "status": 503, "html": "", "error": "browser_unavailable"}

            def legacy_fetcher(url: str):
                if url == page1_url:
                    return 200, self._read("page_ca_1.html")
                if url == page2_url:
                    return 200, self._read("page_ca_2.html")
                return 404, "not found"

            result = ohs_bg.fetch_ohs_bg_state_rows(
                state="CA",
                run_date=date(2026, 3, 6),
                max_pages=3,
                sleep_ms=0,
                cache_dir=cache_dir,
                diagnostics_dir=diagnostics_dir,
                fetcher=legacy_fetcher,
                allow_cache_write=False,
                browser_fetcher=browser_fetcher,
            )
            self.assertTrue(str(result.get("parse_mode") or "").startswith("LEGACY_"))
            self.assertEqual(result.get("fetch_strategy"), "HYBRID_FALLBACK_LEGACY")
            emails = sorted([str(r.get("email") or "") for r in (result.get("rows") or [])])
            self.assertEqual(
                emails,
                ["jane@acmesafetyca.com", "ops@bravoehsca.com", "team@coastalrisk.example"],
            )

    def test_fetch_failed_emits_deterministic_reason_bucket(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            cache_dir = root / "cache"
            diagnostics_dir = root / "diagnostics"

            def browser_fetcher(_url: str) -> dict:
                return {"ok": True, "status": 200, "html": self._read("buyersguide_category_selector_missing.html")}

            def bad_legacy_fetcher(_url: str):
                return 500, "error"

            result = ohs_bg.fetch_ohs_bg_state_rows(
                state="TX",
                run_date=date(2026, 3, 6),
                max_pages=1,
                sleep_ms=0,
                cache_dir=cache_dir,
                diagnostics_dir=diagnostics_dir,
                fetcher=bad_legacy_fetcher,
                allow_cache_write=False,
                browser_fetcher=browser_fetcher,
            )
            self.assertEqual(result["parse_mode"], "FAILED")
            self.assertGreaterEqual(int((result.get("parse_reasons") or {}).get("selector_missing") or 0), 1)
            self.assertTrue(Path(result["diagnostics_path"]).exists())


if __name__ == "__main__":
    unittest.main()
