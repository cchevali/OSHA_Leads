import json
import tempfile
import unittest
from datetime import date, datetime, timezone
from pathlib import Path

from outreach import prospect_sources_state_lic as state_lic


REPO_ROOT = Path(__file__).resolve().parent
FIXTURES = REPO_ROOT / "tests" / "fixtures" / "state_lic_tdlr"


class TestProspectSourcesStateLic(unittest.TestCase):
    def _payload(self):
        return json.loads((FIXTURES / "page1.json").read_text(encoding="utf-8"))

    def test_build_query_contains_filters(self):
        url = state_lic._build_query_url("TX", ["Electrician", "Mold Assessor"], limit=1000, offset=0)
        self.assertTrue(("$where=" in url) or ("%24where=" in url))
        self.assertIn("Electrician", url)
        self.assertIn("Mold+Assessor", url)

    def test_fetch_maps_rows(self):
        payload = self._payload()
        calls = []

        def fetcher(url: str):  # type: ignore[no-untyped-def]
            calls.append(url)
            if len(calls) == 1:
                return 200, payload
            return 200, []

        with tempfile.TemporaryDirectory() as d:
            result = state_lic.fetch_state_lic_state_rows(
                state="TX",
                run_date=date(2026, 2, 26),
                max_pages=2,
                sleep_ms=0,
                cache_dir=Path(d) / "cache",
                diagnostics_dir=Path(d) / "diag",
                fetcher=fetcher,
                allow_cache_write=False,
            )
        self.assertEqual(result["parse_mode"], "SOCRATA")
        self.assertEqual(len(result["rows"]), 2)
        self.assertEqual(result["rows"][0]["source"], "STATE_LIC")
        self.assertTrue(str(result["rows"][0]["prospect_id"]).startswith("state_lic_"))

    def test_fetch_uses_cache(self):
        with tempfile.TemporaryDirectory() as d:
            cache_dir = Path(d) / "cache"
            cache_dir.mkdir(parents=True, exist_ok=True)
            payload = {
                "source": "STATE_LIC",
                "state": "TX",
                "fetched_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
                "pages_fetched": 1,
                "parse_mode": "SOCRATA",
                "rows": [{"firm": "Cached Co", "state": "TX"}],
            }
            state_lic._cache_path(cache_dir, "TX").write_text(json.dumps(payload), encoding="utf-8")
            result = state_lic.fetch_state_lic_state_rows(
                state="TX",
                run_date=date(2026, 2, 26),
                max_pages=1,
                sleep_ms=0,
                cache_dir=cache_dir,
                diagnostics_dir=Path(d) / "diag",
                allow_cache_write=False,
            )
        self.assertTrue(result["cache_used"])
        self.assertEqual(result["rows"][0]["firm"], "Cached Co")

    def test_non_tx_returns_unsupported(self):
        with tempfile.TemporaryDirectory() as d:
            result = state_lic.fetch_state_lic_state_rows(
                state="CA",
                run_date=date(2026, 2, 26),
                max_pages=1,
                sleep_ms=0,
                cache_dir=Path(d) / "cache",
                diagnostics_dir=Path(d) / "diag",
                allow_cache_write=False,
            )
        self.assertEqual(result["parse_mode"], "UNSUPPORTED_STATE")


if __name__ == "__main__":
    unittest.main()
