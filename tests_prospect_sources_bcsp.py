import json
import tempfile
import unittest
from datetime import date, datetime, timezone
from pathlib import Path
from unittest import mock

from outreach import prospect_sources_bcsp as bcsp


REPO_ROOT = Path(__file__).resolve().parent
FIXTURES = REPO_ROOT / "tests" / "fixtures" / "bcsp"


class TestProspectSourcesBcsp(unittest.TestCase):
    def _read(self, name: str) -> str:
        return (FIXTURES / name).read_text(encoding="utf-8")

    def test_parse_bcsp_page_filters_state(self):
        rows, mode = bcsp.parse_bcsp_page(self._read("page_tx_1.html"), state="TX", page_ref="page=1")
        self.assertEqual(mode, "BCSP_CARDS")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["state"], "TX")
        self.assertEqual(rows[0]["source"], "BCSP")
        self.assertEqual(rows[0]["email"], "jane@acmesafety.example")

    def test_fetch_uses_cache(self):
        with tempfile.TemporaryDirectory() as d:
            cache_dir = Path(d) / "cache"
            cache_dir.mkdir(parents=True, exist_ok=True)
            diagnostics_dir = Path(d) / "diag"
            payload = {
                "source": "BCSP",
                "state": "TX",
                "fetched_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
                "pages_fetched": 1,
                "parse_mode": "BCSP_CARDS",
                "rows": [{"email": "cache@example.com", "state": "TX"}],
            }
            bcsp._cache_path(cache_dir, "TX").write_text(json.dumps(payload), encoding="utf-8")

            result = bcsp.fetch_bcsp_state_rows(
                state="TX",
                run_date=date(2026, 2, 26),
                max_pages=2,
                sleep_ms=0,
                cache_dir=cache_dir,
                diagnostics_dir=diagnostics_dir,
                allow_cache_write=False,
            )
            self.assertTrue(result["cache_used"])
            self.assertEqual(result["rows"][0]["email"], "cache@example.com")

    def test_fetch_unavailable_returns_warning(self):
        with tempfile.TemporaryDirectory() as d:
            with mock.patch("outreach.prospect_sources_bcsp.scraper_engine.probe_source_availability", return_value={"available": False, "reason": "crawl4ai_not_installed", "warn_token": "WARN_CRAWL4AI_NOT_INSTALLED"}):
                result = bcsp.fetch_bcsp_state_rows(
                    state="TX",
                    run_date=date(2026, 2, 26),
                    max_pages=1,
                    sleep_ms=0,
                    cache_dir=Path(d) / "cache",
                    diagnostics_dir=Path(d) / "diag",
                    allow_cache_write=False,
                )
        self.assertEqual(result.get("warn_token"), "WARN_CRAWL4AI_NOT_INSTALLED")
        self.assertIn("unavailable", str(result.get("error") or ""))


if __name__ == "__main__":
    unittest.main()
