import json
import tempfile
import unittest
from datetime import date, datetime, timezone
from pathlib import Path

from outreach import prospect_sources_aiha as aiha


REPO_ROOT = Path(__file__).resolve().parent
FIXTURES = REPO_ROOT / "tests" / "fixtures" / "aiha"


class TestProspectSourcesAiha(unittest.TestCase):
    def _read(self, name: str) -> str:
        return (FIXTURES / name).read_text(encoding="utf-8")

    def test_parse_toc_state_starts(self):
        starts = aiha.parse_toc_state_starts(self._read("toc.html"))
        self.assertEqual(starts.get("CA"), 12)
        self.assertEqual(starts.get("FL"), 26)
        self.assertEqual(starts.get("TX"), 62)

    def test_parse_aiha_page_text_container(self):
        rows, mode = aiha.parse_aiha_page(self._read("page_12-13.html"), "12-13")
        self.assertEqual(mode, "TEXT_CONTAINER")
        self.assertEqual(len(rows), 2)
        emails = sorted([r["email"] for r in rows])
        self.assertEqual(emails, ["jane@acmesafety.com", "sam@bravoehs.com"])
        self.assertEqual(rows[0]["state"], "CA")

    def test_parse_aiha_page_fallback(self):
        rows, mode = aiha.parse_aiha_page(self._read("page_fallback.html"), "26-27")
        self.assertEqual(mode, "FALLBACK")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["email"], "dana@deltarisk.com")
        self.assertEqual(rows[0]["state"], "FL")

    def test_parse_aiha_page_failed(self):
        rows, mode = aiha.parse_aiha_page(self._read("page_failed.html"), "00-01")
        self.assertEqual(mode, "FAILED")
        self.assertEqual(rows, [])

    def test_fetch_uses_cache_metadata_timestamp(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            cache_dir = root / "cache"
            diagnostics_dir = root / "diagnostics"
            cache_dir.mkdir(parents=True, exist_ok=True)
            fetched_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
            cache_payload = {
                "source": "aiha_consultants_listing",
                "state": "CA",
                "fetched_at_utc": fetched_at,
                "pages_fetched": 2,
                "parse_mode": "TEXT_CONTAINER",
                "rows": [{"email": "cache@example.com", "state": "CA"}],
            }
            cache_path = aiha._cache_path(cache_dir, "CA")
            cache_path.write_text(json.dumps(cache_payload), encoding="utf-8")

            def failing_fetcher(_url: str):
                raise AssertionError("fetcher should not be called for fresh cache")

            result = aiha.fetch_aiha_state_rows(
                state="CA",
                run_date=date(2026, 2, 18),
                max_pages=6,
                sleep_ms=0,
                cache_dir=cache_dir,
                diagnostics_dir=diagnostics_dir,
                fetcher=failing_fetcher,
                allow_cache_write=False,
            )
            self.assertTrue(result["cache_used"])
            self.assertEqual(len(result["rows"]), 1)
            self.assertEqual(result["rows"][0]["email"], "cache@example.com")

    def test_fetch_failed_writes_diagnostics(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            cache_dir = root / "cache"
            diagnostics_dir = root / "diagnostics"

            def bad_fetcher(_url: str):
                return 500, "error"

            result = aiha.fetch_aiha_state_rows(
                state="TX",
                run_date=date(2026, 2, 18),
                max_pages=6,
                sleep_ms=0,
                cache_dir=cache_dir,
                diagnostics_dir=diagnostics_dir,
                fetcher=bad_fetcher,
                allow_cache_write=False,
            )

            self.assertEqual(result["parse_mode"], "FAILED")
            diag = result.get("diagnostics_path")
            self.assertIsNotNone(diag)
            self.assertTrue(Path(diag).exists())


if __name__ == "__main__":
    unittest.main()
