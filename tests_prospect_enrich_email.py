import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

from outreach import prospect_enrich_email as enrich


REPO_ROOT = Path(__file__).resolve().parent
FIXTURES = REPO_ROOT / "tests" / "fixtures" / "prospect_enrich_email"


class TestProspectEnrichEmail(unittest.TestCase):
    def _read_fixture(self, name: str) -> str:
        return (FIXTURES / name).read_text(encoding="utf-8")

    def test_candidate_domains_compact_first(self):
        self.assertEqual(enrich._candidate_domains_for_firm("BASSETT ELECTRIC LLC")[0], "bassettelectric.com")

    def test_head_success_resolves_domain_and_guesses_email(self):
        rows = [
            {"firm": "BASSETT ELECTRIC LLC", "contact_name": "JOHN BASSETT", "email": "", "state": "TX"},
        ]
        calls = []

        def head_fetcher(url: str):  # type: ignore[no-untyped-def]
            calls.append(url)
            return {"status": 200, "url": url, "headers": {}}

        with tempfile.TemporaryDirectory() as d:
            out = enrich.enrich_autogrow_rows(
                rows,
                domain_enabled=True,
                hunter_enabled=False,
                hunter_api_key="",
                sleep_ms=0,
                hunter_usage_path=Path(d) / "hunter_usage.json",
                head_fetcher=head_fetcher,
                website_fetcher=lambda _url: {"status": 404, "url": _url, "html": "", "error": ""},
                allow_cache_write=False,
            )
        self.assertEqual(len(calls), 1)
        row = out["rows"][0]
        self.assertEqual(row["website"], "https://bassettelectric.com")
        self.assertEqual(row["email"], "john@bassettelectric.com")
        self.assertEqual(row["email_source"], "pattern_guess")
        self.assertEqual(out["metrics"]["domain_resolved"], 1)
        self.assertEqual(out["metrics"]["email_guessed"], 1)

    def test_head_failure_leaves_blank(self):
        rows = [{"firm": "Acme LLC", "contact_name": "Jane Doe", "email": ""}]
        with tempfile.TemporaryDirectory() as d:
            out = enrich.enrich_autogrow_rows(
                rows,
                domain_enabled=True,
                hunter_enabled=False,
                hunter_api_key="",
                sleep_ms=0,
                hunter_usage_path=Path(d) / "hunter_usage.json",
                head_fetcher=lambda _url: {"status": 404, "url": "", "headers": {}},
                allow_cache_write=False,
            )
        self.assertEqual(out["rows"][0].get("website") or "", "")
        self.assertEqual(out["rows"][0].get("email") or "", "")
        self.assertEqual(out["metrics"]["domain_resolved"], 0)
        self.assertEqual(out["metrics"]["still_no_email"], 1)

    def test_company_name_owner_fallback_prefers_info(self):
        candidates = enrich._email_candidates("BRAVO INSTALLATIONS INC", "Bravo Installations Inc", "bravoinstallations.com")
        self.assertGreaterEqual(len(candidates), 1)
        self.assertEqual(candidates[0], "info@bravoinstallations.com")

    def test_existing_email_and_blank_firm_are_skipped(self):
        rows = [
            {"firm": "", "email": "", "contact_name": "No Firm"},
            {"firm": "Acme", "email": "already@example.com", "contact_name": "Jane"},
        ]
        with tempfile.TemporaryDirectory() as d:
            out = enrich.enrich_autogrow_rows(
                rows,
                domain_enabled=True,
                hunter_enabled=False,
                hunter_api_key="",
                sleep_ms=0,
                hunter_usage_path=Path(d) / "hunter_usage.json",
                head_fetcher=lambda _url: {"status": 200, "url": "", "headers": {}},
                allow_cache_write=False,
            )
        self.assertEqual(out["metrics"]["attempted"], 0)
        self.assertEqual(out["rows"][1]["email"], "already@example.com")

    def test_hunter_usage_file_resets_on_month_rollover_and_cap_skip(self):
        with tempfile.TemporaryDirectory() as d:
            usage_path = Path(d) / "hunter_usage.json"
            usage_path.write_text('{"month":"2026-01","calls":99}\n', encoding="utf-8")
            rows = [{"firm": "Bassett Electric LLC", "contact_name": "John Bassett", "email": ""}]
            out = enrich.enrich_autogrow_rows(
                rows,
                domain_enabled=False,
                hunter_enabled=True,
                hunter_api_key="hunter-key",
                sleep_ms=0,
                hunter_usage_path=usage_path,
                head_fetcher=lambda _url: {"status": 200, "url": "", "headers": {}},
                now_utc=datetime(2026, 2, 26, 12, 0, 0, tzinfo=timezone.utc),
                allow_cache_write=False,
            )
            self.assertEqual(out["metrics"]["hunter_skipped_cap"], 0)
            usage_after = enrich._read_hunter_usage(usage_path, now_utc=datetime(2026, 2, 26, tzinfo=timezone.utc))
            self.assertEqual(usage_after["month"], "2026-02")
            self.assertEqual(usage_after["calls"], 0)

            enrich._write_hunter_usage(usage_path, {"month": "2026-02", "calls": enrich.HUNTER_FREE_MONTHLY_CAP})
            out_cap = enrich.enrich_autogrow_rows(
                rows,
                domain_enabled=True,
                hunter_enabled=True,
                hunter_api_key="hunter-key",
                sleep_ms=0,
                hunter_usage_path=usage_path,
                head_fetcher=lambda _url: {"status": 200, "url": _url, "headers": {}},
                website_fetcher=lambda _url: {"status": 404, "url": _url, "html": "", "error": ""},
                now_utc=datetime(2026, 2, 26, 12, 0, 0, tzinfo=timezone.utc),
                allow_cache_write=False,
            )
            self.assertEqual(out_cap["metrics"]["hunter_skipped_cap"], 1)
            self.assertEqual(out_cap["metrics"]["hunter_verified"], 0)

    def test_person_email_beats_role_inbox_when_both_present(self):
        html = self._read_fixture("page_mailto_role.html") + "\n" + "<html><body>Direct email chase@example.com</body></html>"

        row = {
            "firm": "Example Safety LLC",
            "contact_name": "",
            "domain": "example.com",
            "website": "https://example.com",
            "email": "",
            "state": "TX",
        }
        with tempfile.TemporaryDirectory() as d:
            out = enrich.enrich_autogrow_rows(
                [row],
                domain_enabled=True,
                hunter_enabled=False,
                hunter_api_key="",
                sleep_ms=0,
                hunter_usage_path=Path(d) / "hunter_usage.json",
                website_fetcher=lambda _url: {"status": 200, "url": _url, "html": html, "error": ""},
                allow_cache_write=False,
            )
        self.assertEqual(out["rows"][0]["email"], "chase@example.com")
        self.assertEqual(out["rows"][0]["email_kind"], "person")
        self.assertEqual(out["rows"][0]["email_source"], "website_visible")

    def test_role_inbox_set_only_when_allow_role_inbox_enabled(self):
        role_html = self._read_fixture("page_mailto_role.html")
        base_row = {
            "firm": "Example Safety LLC",
            "contact_name": "",
            "domain": "example.com",
            "website": "https://example.com",
            "email": "",
            "state": "TX",
        }
        with tempfile.TemporaryDirectory() as d:
            usage = Path(d) / "hunter_usage.json"
            out_off = enrich.enrich_autogrow_rows(
                [dict(base_row)],
                domain_enabled=True,
                hunter_enabled=False,
                hunter_api_key="",
                sleep_ms=0,
                hunter_usage_path=usage,
                website_fetcher=lambda _url: {"status": 200, "url": _url, "html": role_html, "error": ""},
                allow_role_inbox=False,
                allow_cache_write=False,
            )
            out_on = enrich.enrich_autogrow_rows(
                [dict(base_row)],
                domain_enabled=True,
                hunter_enabled=False,
                hunter_api_key="",
                sleep_ms=0,
                hunter_usage_path=usage,
                website_fetcher=lambda _url: {"status": 200, "url": _url, "html": role_html, "error": ""},
                allow_role_inbox=True,
                allow_cache_write=False,
            )

        self.assertEqual(out_off["rows"][0].get("email") or "", "")
        self.assertEqual(out_on["rows"][0].get("email") or "", "info@example.com")
        self.assertEqual(out_on["rows"][0].get("email_kind") or "", "role_inbox")

    def test_role_inbox_always_recorded_in_email_candidates_json_when_flag_off(self):
        role_html = self._read_fixture("page_mailto_role.html")
        row = {
            "firm": "Example Safety LLC",
            "contact_name": "",
            "domain": "example.com",
            "website": "https://example.com",
            "email": "",
            "state": "TX",
        }
        with tempfile.TemporaryDirectory() as d:
            out = enrich.enrich_autogrow_rows(
                [row],
                domain_enabled=True,
                hunter_enabled=False,
                hunter_api_key="",
                sleep_ms=0,
                hunter_usage_path=Path(d) / "hunter_usage.json",
                website_fetcher=lambda _url: {"status": 200, "url": _url, "html": role_html, "error": ""},
                allow_role_inbox=False,
                allow_cache_write=False,
            )
        payload = json.loads(out["rows"][0].get("email_candidates_json") or "[]")
        emails = [str(item.get("email") or "") for item in payload]
        self.assertIn("info@example.com", emails)

    def test_403_produces_needs_review_row_and_telemetry(self):
        row = {
            "firm": "Example Safety LLC",
            "contact_name": "",
            "domain": "example.com",
            "website": "https://example.com",
            "email": "",
            "state": "TX",
        }
        with tempfile.TemporaryDirectory() as d:
            with mock.patch(
                "outreach.prospect_enrich_email.scraper_engine.probe_crawl4ai_runtime",
                return_value={"crawl4ai_installed": False, "playwright_browsers_installed": False},
            ):
                out = enrich.enrich_autogrow_rows(
                    [row],
                    domain_enabled=True,
                    hunter_enabled=False,
                    hunter_api_key="",
                    sleep_ms=0,
                    hunter_usage_path=Path(d) / "hunter_usage.json",
                    website_fetcher=lambda _url: {"status": 403, "url": _url, "html": "", "error": "HTTPError:403"},
                    allow_cache_write=False,
                )

        self.assertGreaterEqual(int(out["metrics"].get("website_enrich_blocked_403") or 0), 1)
        self.assertEqual(len(out.get("needs_review") or []), 1)
        self.assertEqual((out["needs_review"][0].get("reason") or "").strip(), "403")

    def test_cache_ttl_prevents_refetch_within_14_days(self):
        row = {
            "firm": "Example Safety LLC",
            "contact_name": "",
            "domain": "example.com",
            "website": "https://example.com",
            "email": "",
            "state": "TX",
        }
        html = self._read_fixture("page_visible_person.html")
        calls = {"n": 0}

        def website_fetcher(url: str):  # type: ignore[no-untyped-def]
            calls["n"] += 1
            return {"status": 200, "url": url, "html": html, "error": ""}

        with tempfile.TemporaryDirectory() as d:
            usage = Path(d) / "hunter_usage.json"
            cache_dir = Path(d) / "cache"
            out_one = enrich.enrich_autogrow_rows(
                [dict(row)],
                domain_enabled=True,
                hunter_enabled=False,
                hunter_api_key="",
                sleep_ms=0,
                hunter_usage_path=usage,
                website_fetcher=website_fetcher,
                website_cache_dir=cache_dir,
                allow_role_inbox=True,
                allow_cache_write=True,
                now_utc=datetime(2026, 2, 1, 12, 0, 0, tzinfo=timezone.utc),
            )
            first_call_count = calls["n"]
            out_two = enrich.enrich_autogrow_rows(
                [dict(row)],
                domain_enabled=True,
                hunter_enabled=False,
                hunter_api_key="",
                sleep_ms=0,
                hunter_usage_path=usage,
                website_fetcher=website_fetcher,
                website_cache_dir=cache_dir,
                allow_role_inbox=True,
                allow_cache_write=True,
                now_utc=datetime(2026, 2, 10, 12, 0, 0, tzinfo=timezone.utc),
            )

        self.assertGreater(first_call_count, 0)
        self.assertEqual(calls["n"], first_call_count)
        self.assertEqual(out_one["rows"][0].get("email") or "", "contact@example.com")
        self.assertEqual(out_two["rows"][0].get("email") or "", "contact@example.com")

    def test_timeout_then_recover_counts_timeout_without_needs_review(self):
        row = {
            "firm": "Example Safety LLC",
            "contact_name": "",
            "domain": "example.com",
            "website": "https://example.com",
            "email": "",
            "state": "TX",
        }

        def website_fetcher(url: str):  # type: ignore[no-untyped-def]
            if url.endswith("/contact"):
                return {"status": 200, "url": url, "html": "<html><body>jane@example.com</body></html>", "error": ""}
            return {"status": 0, "url": url, "html": "", "error": "TimeoutError:timed out"}

        with tempfile.TemporaryDirectory() as d:
            out = enrich.enrich_autogrow_rows(
                [row],
                domain_enabled=True,
                hunter_enabled=False,
                hunter_api_key="",
                sleep_ms=0,
                hunter_usage_path=Path(d) / "hunter_usage.json",
                website_fetcher=website_fetcher,
                allow_cache_write=False,
            )

        self.assertEqual(out["rows"][0].get("email") or "", "jane@example.com")
        self.assertEqual(int(out["metrics"].get("website_enrich_timeout") or 0), 1)
        self.assertEqual(len(out.get("needs_review") or []), 0)

    def test_all_timeout_adds_timeout_needs_review(self):
        row = {
            "firm": "Example Safety LLC",
            "contact_name": "",
            "domain": "example.com",
            "website": "https://example.com",
            "email": "",
            "state": "TX",
        }
        with tempfile.TemporaryDirectory() as d:
            out = enrich.enrich_autogrow_rows(
                [row],
                domain_enabled=True,
                hunter_enabled=False,
                hunter_api_key="",
                sleep_ms=0,
                hunter_usage_path=Path(d) / "hunter_usage.json",
                website_fetcher=lambda url: {"status": 0, "url": url, "html": "", "error": "TimeoutError:timed out"},
                allow_cache_write=False,
            )

        self.assertEqual(int(out["metrics"].get("website_enrich_timeout") or 0), 1)
        self.assertEqual(len(out.get("needs_review") or []), 1)
        self.assertEqual((out["needs_review"][0].get("reason") or "").strip(), "timeout")

    def test_dedup_by_domain_crawls_each_domain_once_per_run(self):
        rows = [
            {
                "firm": "Example Safety LLC",
                "contact_name": "",
                "domain": "example.com",
                "website": "https://example.com",
                "email": "",
                "state": "TX",
            },
            {
                "firm": "Example Safety West LLC",
                "contact_name": "",
                "domain": "www.example.com",
                "website": "https://www.example.com",
                "email": "",
                "state": "TX",
            },
        ]
        calls = {"n": 0}

        def website_fetcher(url: str):  # type: ignore[no-untyped-def]
            calls["n"] += 1
            return {"status": 200, "url": url, "html": "<html><body>jane@example.com</body></html>", "error": ""}

        with tempfile.TemporaryDirectory() as d:
            out = enrich.enrich_autogrow_rows(
                rows,
                domain_enabled=True,
                hunter_enabled=False,
                hunter_api_key="",
                sleep_ms=0,
                hunter_usage_path=Path(d) / "hunter_usage.json",
                website_fetcher=website_fetcher,
                allow_cache_write=False,
            )

        self.assertEqual(calls["n"], 1)
        self.assertEqual(int(out["metrics"].get("website_enrich_attempted") or 0), 1)
        self.assertEqual(out["rows"][0].get("email") or "", "jane@example.com")
        self.assertEqual(out["rows"][1].get("email") or "", "jane@example.com")


if __name__ == "__main__":
    unittest.main()
