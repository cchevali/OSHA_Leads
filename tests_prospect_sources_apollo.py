import tempfile
import unittest
from datetime import date
from pathlib import Path


class TestProspectSourcesApollo(unittest.TestCase):
    def test_search_has_email_true_gating(self):
        from outreach import prospect_sources_apollo as apollo

        calls = []

        def _fetch(url, payload, api_key):  # type: ignore[no-untyped-def]
            calls.append((url, payload))
            if "api_search" in url:
                return 200, {
                    "people": [
                        {"id": "p1", "has_email": True},
                        {"id": "p2", "has_email": False},
                    ]
                }
            return 200, {
                "matches": [
                    {"id": "p1", "email": "one@example.com", "title": "Owner", "organization": {"name": "One Co", "primary_domain": "example.com"}}
                ]
            }

        with tempfile.TemporaryDirectory() as d:
            result = apollo.fetch_apollo_state_rows(
                state="TX",
                run_date=date(2026, 2, 24),
                max_pages=2,
                sleep_ms=0,
                cache_dir=Path(d) / "cache",
                diagnostics_dir=Path(d) / "diag",
                api_key="k",
                enrich_enabled=True,
                enrich_limit=10,
                person_titles=["owner"],
                fetcher=_fetch,
                allow_cache_write=False,
            )

        self.assertEqual(result["search_rows_returned"], 2)
        self.assertEqual(result["search_rows_has_email_true"], 1)
        self.assertEqual(result["enrich_attempted"], 1)
        self.assertEqual(len(result["rows"]), 1)

    def test_enrichment_batches_of_ten(self):
        from outreach import prospect_sources_apollo as apollo

        enrich_batch_sizes: list[int] = []

        def _fetch(url, payload, api_key):  # type: ignore[no-untyped-def]
            if "api_search" in url:
                return 200, {"people": [{"id": f"p{i}", "has_email": True} for i in range(23)]}
            enrich_batch_sizes.append(len(list(payload.get("details") or [])))
            matches = []
            for detail in list(payload.get("details") or []):
                pid = detail.get("id")
                matches.append(
                    {"id": pid, "email": f"{pid}@example.com", "title": "Owner", "organization": {"name": pid, "primary_domain": "example.com"}}
                )
            return 200, {"matches": matches}

        with tempfile.TemporaryDirectory() as d:
            result = apollo.fetch_apollo_state_rows(
                state="TX",
                run_date=date(2026, 2, 24),
                max_pages=1,
                sleep_ms=0,
                cache_dir=Path(d) / "cache",
                diagnostics_dir=Path(d) / "diag",
                api_key="k",
                enrich_enabled=True,
                enrich_limit=23,
                person_titles=["owner"],
                fetcher=_fetch,
                allow_cache_write=False,
            )

        self.assertEqual(enrich_batch_sizes, [10, 10, 3])
        self.assertEqual(result["enrich_attempted"], 23)
        self.assertEqual(result["enriched"], 23)

    def test_credit_guard_cap_logic(self):
        from outreach import prospect_sources_apollo as apollo

        enrich_calls = 0

        def _fetch(url, payload, api_key):  # type: ignore[no-untyped-def]
            nonlocal enrich_calls
            if "api_search" in url:
                return 200, {"people": [{"id": f"p{i}", "has_email": True} for i in range(12)]}
            enrich_calls += 1
            matches = [
                {"id": d["id"], "email": f"{d['id']}@example.com", "title": "Owner", "organization": {"name": "Co"}}
                for d in list(payload.get("details") or [])
            ]
            return 200, {"matches": matches}

        with tempfile.TemporaryDirectory() as d:
            result = apollo.fetch_apollo_state_rows(
                state="TX",
                run_date=date(2026, 2, 24),
                max_pages=1,
                sleep_ms=0,
                cache_dir=Path(d) / "cache",
                diagnostics_dir=Path(d) / "diag",
                api_key="k",
                enrich_enabled=True,
                enrich_limit=5,
                person_titles=["owner"],
                fetcher=_fetch,
                allow_cache_write=False,
            )

        self.assertEqual(enrich_calls, 1)
        self.assertEqual(result["enrich_attempted"], 5)
        self.assertEqual(result["enrich_skipped_credit_cap"], 7)
        self.assertTrue(result["credit_cap_hit"])

    def test_no_match_and_missing_email_handling(self):
        from outreach import prospect_sources_apollo as apollo

        def _fetch(url, payload, api_key):  # type: ignore[no-untyped-def]
            if "api_search" in url:
                return 200, {"people": [{"id": "p1", "has_email": True}, {"id": "p2", "has_email": True}]}
            return 200, {"matches": [{"id": "p1"}, {"id": "p2", "email": "p2@example.com", "organization": {"name": "P2"}}]}

        with tempfile.TemporaryDirectory() as d:
            result = apollo.fetch_apollo_state_rows(
                state="FL",
                run_date=date(2026, 2, 24),
                max_pages=1,
                sleep_ms=0,
                cache_dir=Path(d) / "cache",
                diagnostics_dir=Path(d) / "diag",
                api_key="k",
                enrich_enabled=True,
                enrich_limit=2,
                person_titles=["owner"],
                fetcher=_fetch,
                allow_cache_write=False,
            )

        self.assertEqual(result["enrich_attempted"], 2)
        self.assertEqual(result["enriched"], 1)
        self.assertGreaterEqual(result["enrich_no_match"], 1)
        self.assertEqual(len(result["rows"]), 1)

    def test_canonical_mapping_normalization(self):
        from outreach import prospect_sources_apollo as apollo

        row = apollo._map_enriched_person_to_row(
            {
                "id": "abc",
                "email": "USER@Example.COM",
                "title": "Partner",
                "name": "Jane Doe",
                "city": "Houston",
                "state": "tx",
                "organization": {"name": "Example LLP", "primary_domain": "Example.com", "website_url": "https://example.com"},
            },
            target_state="TX",
        )
        self.assertIsNotNone(row)
        self.assertEqual((row or {})["contact_email"], "user@example.com")
        self.assertEqual((row or {})["state"], "TX")
        self.assertEqual((row or {})["domain"], "example.com")
        self.assertIn("apollo:bulk_match", (row or {})["source"])

    def test_runtime_error_returns_error_and_diagnostic(self):
        from outreach import prospect_sources_apollo as apollo

        def _fetch(url, payload, api_key):  # type: ignore[no-untyped-def]
            return 500, {"error": "boom"}

        with tempfile.TemporaryDirectory() as d:
            result = apollo.fetch_apollo_state_rows(
                state="TX",
                run_date=date(2026, 2, 24),
                max_pages=1,
                sleep_ms=0,
                cache_dir=Path(d) / "cache",
                diagnostics_dir=Path(d) / "diag",
                api_key="k",
                enrich_enabled=True,
                enrich_limit=1,
                person_titles=["owner"],
                fetcher=_fetch,
                allow_cache_write=False,
            )

        self.assertEqual(result["parse_mode"], "FAILED")
        self.assertIn("error", result)
        self.assertTrue(result.get("diagnostics_path"))


if __name__ == "__main__":
    unittest.main()
