import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest import mock
import json


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
        self.assertIn("apollo_search_request_failed", str(result.get("error") or ""))

    def test_forbidden_403_records_structured_diagnostic_and_flag(self):
        from outreach import prospect_sources_apollo as apollo

        def _fetch(url, payload, api_key):  # type: ignore[no-untyped-def]
            return 403, {"error": "Forbidden: master key required"}

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
            diag_path = Path(str(result.get("diagnostics_path")))
            diag_payload = __import__("json").loads(diag_path.read_text(encoding="utf-8"))

        self.assertTrue(result.get("forbidden"))
        self.assertEqual(result.get("error_status"), 403)
        self.assertEqual(result.get("error_endpoint"), "api/v1/mixed_people/api_search")
        self.assertIn("status=403 retryable=0", str(result.get("error") or ""))
        self.assertEqual(diag_payload.get("status"), 403)
        self.assertEqual(diag_payload.get("endpoint"), "api/v1/mixed_people/api_search")
        self.assertIn("master key", str(diag_payload.get("apollo_error") or "").lower())

    def test_retries_transient_429_then_success(self):
        from outreach import prospect_sources_apollo as apollo

        calls = {"search": 0, "enrich": 0}

        def _fetch(url, payload, api_key):  # type: ignore[no-untyped-def]
            if "api_search" in url:
                calls["search"] += 1
                if calls["search"] < 3:
                    return 429, {"error": "rate_limited"}
                return 200, {"people": [{"id": "p1", "has_email": True}]}
            calls["enrich"] += 1
            return 200, {"matches": [{"id": "p1", "email": "p1@example.com", "organization": {"name": "P1"}}]}

        with tempfile.TemporaryDirectory() as d:
            with mock.patch("outreach.prospect_sources_apollo.time.sleep") as sleep_mock:
                result = apollo.fetch_apollo_state_rows(
                    state="TX",
                    run_date=date(2026, 2, 24),
                    max_pages=1,
                    sleep_ms=1,
                    cache_dir=Path(d) / "cache",
                    diagnostics_dir=Path(d) / "diag",
                    api_key="k",
                    enrich_enabled=True,
                    enrich_limit=1,
                    person_titles=["owner"],
                    fetcher=_fetch,
                    allow_cache_write=False,
                )

        self.assertEqual(calls["search"], 3)
        self.assertEqual(calls["enrich"], 1)
        self.assertEqual(result["enriched"], 1)
        self.assertGreaterEqual(sleep_mock.call_count, 2)

    def test_parse_failure_returns_stable_error(self):
        from outreach import prospect_sources_apollo as apollo

        def _fetch(url, payload, api_key):  # type: ignore[no-untyped-def]
            return 200, []  # invalid parse shape for client contract

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
        self.assertIn("apollo_search_parse_failed", str(result.get("error") or ""))

    def test_cache_reuse_reports_cache_used_and_age(self):
        from outreach import prospect_sources_apollo as apollo

        calls = {"search": 0}

        def _fetch(url, payload, api_key):  # type: ignore[no-untyped-def]
            if "api_search" in url:
                calls["search"] += 1
                return 200, {"people": []}
            raise AssertionError("enrich should not be called")

        with tempfile.TemporaryDirectory() as d:
            cache_dir = Path(d) / "cache"
            diag_dir = Path(d) / "diag"
            first = apollo.fetch_apollo_state_rows(
                state="TX",
                run_date=date(2026, 2, 24),
                max_pages=1,
                sleep_ms=0,
                cache_dir=cache_dir,
                diagnostics_dir=diag_dir,
                api_key="k",
                enrich_enabled=False,
                enrich_limit=0,
                person_titles=["owner"],
                fetcher=_fetch,
                allow_cache_write=True,
            )
            second = apollo.fetch_apollo_state_rows(
                state="TX",
                run_date=date(2026, 2, 24),
                max_pages=1,
                sleep_ms=0,
                cache_dir=cache_dir,
                diagnostics_dir=diag_dir,
                api_key="k",
                enrich_enabled=False,
                enrich_limit=0,
                person_titles=["owner"],
                fetcher=_fetch,
                allow_cache_write=True,
            )

        self.assertFalse(first["cache_used"])
        self.assertTrue(second["cache_used"])
        self.assertEqual(calls["search"], 1)
        self.assertIsNotNone(second["cache_age_days"])

    def test_doctor_forbidden_returns_flag_and_hintable_state(self):
        from outreach import prospect_sources_apollo as apollo

        def _fetch(url, payload, api_key):  # type: ignore[no-untyped-def]
            self.assertIn("usage_stats/api_usage_stats", url)
            self.assertEqual(payload, {})
            return 403, {"error": "Forbidden"}

        result = apollo.doctor_apollo_api(api_key="k", fetcher=_fetch, sleep_ms=0)
        self.assertFalse(result["ok"])
        self.assertTrue(result["forbidden"])
        self.assertEqual(result["status"], 403)
        self.assertEqual(result["endpoint"], "api/v1/usage_stats/api_usage_stats")

    def test_doctor_post_success_path_returns_ok(self):
        from outreach import prospect_sources_apollo as apollo

        def _fetch(url, payload, api_key):  # type: ignore[no-untyped-def]
            self.assertIn("usage_stats/api_usage_stats", url)
            self.assertEqual(payload, {})
            return 200, {"ok": True}

        result = apollo.doctor_apollo_api(api_key="k", fetcher=_fetch, sleep_ms=0)
        self.assertTrue(result["ok"])
        self.assertFalse(result["forbidden"])
        self.assertEqual(result["status"], 200)

    def test_doctor_404_non_json_writes_diagnostic_and_does_not_throw(self):
        from outreach import prospect_sources_apollo as apollo

        def _fetch(url, payload, api_key):  # type: ignore[no-untyped-def]
            return {
                "status": 404,
                "content_type": "text/html; charset=utf-8",
                "body_preview": "<html>not found</html>",
                "json": None,
            }

        with tempfile.TemporaryDirectory() as d:
            result = apollo.doctor_apollo_api(
                api_key="k",
                fetcher=_fetch,
                sleep_ms=0,
                diagnostics_dir=Path(d) / "diag",
            )
            diag_path = Path(str(result.get("diagnostics_path")))
            diag = json.loads(diag_path.read_text(encoding="utf-8"))

        self.assertFalse(result["ok"])
        self.assertFalse(result["forbidden"])
        self.assertTrue(result["not_found"])
        self.assertEqual(result["status"], 404)
        self.assertEqual(result["content_type"], "text/html; charset=utf-8")
        self.assertEqual(diag.get("status"), 404)
        self.assertEqual(diag.get("endpoint"), "api/v1/usage_stats/api_usage_stats")
        self.assertIn("text/html", str(diag.get("content_type") or ""))


if __name__ == "__main__":
    unittest.main()
