import json
import tempfile
import unittest
from datetime import date, datetime, timezone
from pathlib import Path

from outreach import prospect_sources_osha_news as osha_news


REPO_ROOT = Path(__file__).resolve().parent
FIXTURES = REPO_ROOT / "tests" / "fixtures" / "osha_news"


class TestProspectSourcesOshaNews(unittest.TestCase):
    def _read(self, name: str) -> str:
        return (FIXTURES / name).read_text(encoding="utf-8")

    def test_parse_listing_and_release(self):
        links, mode = osha_news.parse_osha_news_listing(self._read("listing_tx.html"), state="TX")
        self.assertEqual(mode, "LINKS")
        self.assertTrue(any("tx1234" in url for url in links))
        row, row_mode = osha_news.parse_osha_news_release(self._read("release_tx.html"), url="https://www.osha.gov/news/newsreleases/region6/tx1234")
        self.assertEqual(row_mode, "ARTICLE")
        self.assertEqual((row or {})["state"], "TX")
        self.assertEqual((row or {})["source"], "OSHA_NEWS")
        self.assertIn("$125,500", (row or {})["penalty_amount"])

    def test_fetch_uses_cache(self):
        with tempfile.TemporaryDirectory() as d:
            cache_dir = Path(d) / "cache"
            cache_dir.mkdir(parents=True, exist_ok=True)
            payload = {
                "source": "OSHA_NEWS",
                "state": "TX",
                "fetched_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
                "pages_fetched": 2,
                "parse_mode": "ARTICLE",
                "rows": [{"firm": "Acme", "state": "TX"}],
            }
            osha_news._cache_path(cache_dir, "TX").write_text(json.dumps(payload), encoding="utf-8")
            result = osha_news.fetch_osha_news_state_rows(
                state="TX",
                run_date=date(2026, 2, 26),
                max_pages=2,
                sleep_ms=0,
                cache_dir=cache_dir,
                diagnostics_dir=Path(d) / "diag",
                allow_cache_write=False,
            )
        self.assertTrue(result["cache_used"])
        self.assertEqual(result["rows"][0]["firm"], "Acme")

    def test_fetch_listing_and_release_with_fetcher(self):
        listing_url = osha_news.LISTING_URL
        tx_url = "https://www.osha.gov/news/newsreleases/region6/tx1234"

        def fetcher(url: str):  # type: ignore[no-untyped-def]
            if url == listing_url:
                return 200, self._read("listing_tx.html")
            if url == tx_url:
                return 200, self._read("release_tx.html")
            return 404, "not found"

        with tempfile.TemporaryDirectory() as d:
            result = osha_news.fetch_osha_news_state_rows(
                state="TX",
                run_date=date(2026, 2, 26),
                max_pages=1,
                sleep_ms=0,
                cache_dir=Path(d) / "cache",
                diagnostics_dir=Path(d) / "diag",
                fetcher=fetcher,
                allow_cache_write=False,
            )
        self.assertEqual(result["parse_mode"], "ARTICLE")
        self.assertEqual(len(result["rows"]), 1)


if __name__ == "__main__":
    unittest.main()
