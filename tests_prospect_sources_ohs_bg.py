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
                "pages_fetched": 1,
                "parse_mode": "TEXT",
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

    def test_fetch_follows_next_page_and_returns_rows(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            cache_dir = root / "cache"
            diagnostics_dir = root / "diagnostics"

            page1_url = "https://www.ohsonline.com/Directory/SearchResults.aspx?state=CA"
            page2_url = "https://www.ohsonline.com/Directory/SearchResults.aspx?state=CA&page=2"

            def fetcher(url: str):
                if url == page1_url:
                    return 200, self._read("page_ca_1.html")
                if url == page2_url:
                    return 200, self._read("page_ca_2.html")
                return 404, "not found"

            result = ohs_bg.fetch_ohs_bg_state_rows(
                state="CA",
                run_date=date(2026, 2, 24),
                max_pages=3,
                sleep_ms=0,
                cache_dir=cache_dir,
                diagnostics_dir=diagnostics_dir,
                fetcher=fetcher,
                allow_cache_write=False,
            )
            self.assertFalse(result["cache_used"])
            self.assertEqual(int(result.get("pages_fetched") or 0), 2)
            self.assertIn(result.get("parse_mode"), {"MULTI", "TEXT"})
            emails = sorted([str(r.get("email") or "") for r in (result.get("rows") or [])])
            self.assertEqual(
                emails,
                ["jane@acmesafetyca.com", "ops@bravoehsca.com", "team@coastalrisk.example"],
            )

    def test_fetch_failed_writes_diagnostics(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            cache_dir = root / "cache"
            diagnostics_dir = root / "diagnostics"

            def bad_fetcher(_url: str):
                return 500, "error"

            result = ohs_bg.fetch_ohs_bg_state_rows(
                state="TX",
                run_date=date(2026, 2, 24),
                max_pages=2,
                sleep_ms=0,
                cache_dir=cache_dir,
                diagnostics_dir=diagnostics_dir,
                fetcher=bad_fetcher,
                allow_cache_write=False,
            )
            self.assertEqual(result["parse_mode"], "FAILED")
            self.assertTrue(Path(result["diagnostics_path"]).exists())


if __name__ == "__main__":
    unittest.main()
