import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from outreach import prospect_enrich_email as enrich


class TestProspectEnrichEmail(unittest.TestCase):
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
            )
        self.assertEqual(len(calls), 1)
        row = out["rows"][0]
        self.assertEqual(row["website"], "https://bassettelectric.com")
        self.assertEqual(row["email"], "john@bassettelectric.com")
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
                now_utc=datetime(2026, 2, 26, 12, 0, 0, tzinfo=timezone.utc),
            )
            self.assertEqual(out_cap["metrics"]["hunter_skipped_cap"], 1)
            self.assertEqual(out_cap["metrics"]["hunter_verified"], 0)


if __name__ == "__main__":
    unittest.main()
