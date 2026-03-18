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

    def test_parse_aiha_page_missing_website_does_not_turn_contact_into_url(self):
        page_html = """
        <html><body>
        <div id="text-container">
        <p>Coastal Safety LLC Commercial 760 Montauk Avenue New London, CT 06320 USA Website: Contact: Robert C. Klein, CIH Contact Email: coastal.safety.llc@sbcglobal.net Specialty: 5 Consulting</p>
        </div>
        </body></html>
        """
        rows, mode = aiha.parse_aiha_page(page_html, "26-27")
        self.assertEqual(mode, "TEXT_CONTAINER")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["website"], "")
        self.assertEqual(rows[0]["state"], "CT")

    def test_parse_aiha_page_failed(self):
        rows, mode = aiha.parse_aiha_page(self._read("page_failed.html"), "00-01")
        self.assertEqual(mode, "FAILED")
        self.assertEqual(rows, [])

    def test_parse_aiha_page_with_diagnostics_emits_reject_tokens(self):
        page_html = """
        <html><body>
        <div id="text-container">
        <p>Inc. Commercial 100 Main St Los Angeles, CA 90001 USA Website: inc.example Contact: Alex One Contact Email: alex@inc.example Specialty: 5 Consulting</p>
        <p>Valid Safety Partners Commercial 200 Main St Los Angeles, CA 90001 USA Website: valid.example Contact: Sam One and Lee Two Contact Email: sam@valid.example Specialty: 5 Consulting</p>
        <p>Valid Safety Partners Commercial 300 Main St Los Angeles USA Website: valid.example Contact: Sam One Contact Email: sam2@valid.example Specialty: 5 Consulting</p>
        <p>Valid Safety Partners Commercial 400 Main St Los Angeles, CA 90001 USA Website: valid.example Contact: Sam One Specialty: 5 Consulting</p>
        <p>Valid Safety Partners Commercial 500 Main St Los Angeles, CA 90001 USA Website: valid.example Contact: Sam One Contact Email: sam@valid.example Contact Email: secondary@valid.example Specialty: 5 Consulting</p>
        </div>
        </body></html>
        """
        parsed = aiha.parse_aiha_page_with_diagnostics(page_html, "12-13")
        rows = list(parsed.get("rows") or [])
        reject_counts = dict(parsed.get("reject_counts") or {})
        row_diagnostics = list(parsed.get("row_diagnostics") or [])

        self.assertEqual(parsed.get("mode"), "TEXT_CONTAINER")
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["email"], "")
        self.assertEqual(rows[0]["website"], "https://valid.example")
        self.assertEqual(rows[1]["email"], "sam@valid.example")
        self.assertEqual(reject_counts.get("placeholder_firm"), 1)
        self.assertEqual(reject_counts.get("multi_person_contact"), 1)
        self.assertEqual(reject_counts.get("invalid_city_state"), 1)
        self.assertEqual(reject_counts.get("missing_email"), 0)
        self.assertGreaterEqual(len(row_diagnostics), 5)
        statuses = {str(item.get("status") or "") for item in row_diagnostics}
        self.assertIn("accepted", statuses)
        self.assertIn("rejected", statuses)

    def test_parse_aiha_page_with_diagnostics_keeps_website_only_rows_for_site_contact_resolution(self):
        page_html = """
        <html><body>
        <div id="text-container">
        <p>Indigo Compliance Commercial 100 Main St Houston, TX 77001 USA Website: indigocompliance.com Specialty: 5 Consulting</p>
        </div>
        </body></html>
        """
        parsed = aiha.parse_aiha_page_with_diagnostics(page_html, "62-63")
        rows = list(parsed.get("rows") or [])
        reject_counts = dict(parsed.get("reject_counts") or {})

        self.assertEqual(parsed.get("mode"), "TEXT_CONTAINER")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["firm"], "Indigo Compliance")
        self.assertEqual(rows[0]["website"], "https://indigocompliance.com")
        self.assertEqual(rows[0]["email"], "")
        self.assertEqual(rows[0]["contact_email"], "")
        self.assertEqual(rows[0]["contact_name"], "")
        self.assertEqual(reject_counts.get("multi_person_contact"), 0)
        self.assertEqual(reject_counts.get("missing_email"), 0)

    def test_fetch_collects_reject_counts_and_row_diagnostics(self):
        toc_html = self._read("toc.html")
        page_html = """
        <html><body>
        <div id="text-container">
        <p>Consulting Commercial 100 Main St Los Angeles, CA 90001 USA Website: bad.example Contact: Sam One Contact Email: sam@bad.example Specialty: 5 Consulting</p>
        <p>Acme Safety Consulting Commercial 123 Main St Los Angeles, CA 90001 USA Website: good.example Contact: Jane Owner Contact Email: jane@good.example Specialty: 5 Consulting</p>
        </div>
        </body></html>
        """

        def fake_fetcher(url: str):
            if "toc" in url:
                return 200, toc_html
            return 200, page_html

        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            result = aiha.fetch_aiha_state_rows(
                state="CA",
                run_date=date(2026, 2, 18),
                max_pages=1,
                sleep_ms=0,
                cache_dir=root / "cache",
                diagnostics_dir=root / "diagnostics",
                fetcher=fake_fetcher,
                allow_cache_write=False,
            )

        self.assertEqual(result["parse_mode"], "TEXT_CONTAINER")
        self.assertEqual(len(list(result.get("rows") or [])), 1)
        reject_counts = dict(result.get("reject_counts") or {})
        self.assertEqual(int(reject_counts.get("placeholder_firm") or 0), 1)
        self.assertEqual(int(reject_counts.get("missing_email") or 0), 0)
        self.assertGreaterEqual(len(list(result.get("row_diagnostics") or [])), 2)

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
                "reject_counts": {token: 0 for token in aiha.AIHA_REJECT_TOKENS},
                "row_diagnostics": [],
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
            self.assertIn("reject_counts", result)
            self.assertIn("row_diagnostics", result)

    def test_fetch_filters_cross_state_rows_from_fresh_cache(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            cache_dir = root / "cache"
            diagnostics_dir = root / "diagnostics"
            cache_dir.mkdir(parents=True, exist_ok=True)
            fetched_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
            cache_payload = {
                "source": "aiha_consultants_listing",
                "state": "FL",
                "fetched_at_utc": fetched_at,
                "pages_fetched": 1,
                "parse_mode": "TEXT_CONTAINER",
                "rows": [
                    {"email": "ct@example.com", "state": "CT", "firm": "Boundary CT"},
                    {"email": "fl@example.com", "state": "FL", "firm": "Florida Safe"},
                ],
                "reject_counts": {token: 0 for token in aiha.AIHA_REJECT_TOKENS},
                "row_diagnostics": [],
            }
            cache_path = aiha._cache_path(cache_dir, "FL")
            cache_path.write_text(json.dumps(cache_payload), encoding="utf-8")

            result = aiha.fetch_aiha_state_rows(
                state="FL",
                run_date=date(2026, 3, 15),
                max_pages=6,
                sleep_ms=0,
                cache_dir=cache_dir,
                diagnostics_dir=diagnostics_dir,
                allow_cache_write=False,
            )

            self.assertTrue(result["cache_used"])
            self.assertEqual(len(result["rows"]), 1)
            self.assertEqual(result["rows"][0]["state"], "FL")
            self.assertEqual(result["rows"][0]["email"], "fl@example.com")

    def test_fetch_filters_cross_state_rows_before_caching(self):
        toc_html = self._read("toc.html")
        page_html = """
        <html><body>
        <div id="text-container">
        <p>Coastal Safety LLC Commercial 760 Montauk Avenue New London, CT 06320 USA Website: Contact: Robert C. Klein, CIH Contact Email: coastal.safety.llc@sbcglobal.net Specialty: 5 Consulting</p>
        <p>Delta Risk Advisors Commercial 100 Main St Tampa, FL 33602 USA Website: delta-risk.example Contact: Dana Owner Contact Email: dana@delta-risk.example Specialty: 5 Consulting</p>
        </div>
        </body></html>
        """

        def fake_fetcher(url: str):
            if "toc" in url:
                return 200, toc_html
            return 200, page_html

        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            result = aiha.fetch_aiha_state_rows(
                state="FL",
                run_date=date(2026, 3, 15),
                max_pages=1,
                sleep_ms=0,
                cache_dir=root / "cache",
                diagnostics_dir=root / "diagnostics",
                fetcher=fake_fetcher,
                allow_cache_write=False,
            )

        rows = list(result.get("rows") or [])
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["firm"], "Delta Risk Advisors")
        self.assertEqual(rows[0]["state"], "FL")

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
