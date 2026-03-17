import json
import os
import tempfile
import unittest
from datetime import date, datetime, timezone
from pathlib import Path
from unittest import mock

from outreach import prospect_sources_state_lic as state_lic


REPO_ROOT = Path(__file__).resolve().parent
FIXTURES = REPO_ROOT / "tests" / "fixtures" / "state_lic_tdlr"


class TestProspectSourcesStateLic(unittest.TestCase):
    def _payload(self):
        return json.loads((FIXTURES / "page1.json").read_text(encoding="utf-8"))

    def test_build_query_contains_filters(self):
        url = state_lic._build_query_url("TX", ["Electrical Contractor", "Elevator Contractor"], limit=1000, offset=0)
        self.assertTrue(("$where=" in url) or ("%24where=" in url))
        self.assertIn("owner_name", url)
        self.assertIn("business_city_state_zip", url)
        self.assertIn("Electrical+Contractor", url)
        self.assertIn("Elevator+Contractor", url)

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
        self.assertEqual(result["rows"][0]["firm"], "Acme Electrical LLC")
        self.assertEqual(result["rows"][0]["contact_name"], "Jane Owner")
        self.assertEqual(result["rows"][0]["title"], "Electrical Contractor")
        self.assertEqual(result["rows"][0]["city"], "Houston")
        self.assertEqual(result["rows"][0]["state"], "TX")
        self.assertIn("state_lic_fit_status", result["rows"][0])
        self.assertIn("state_lic_consultant_eligible", result["rows"][0])

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
        self.assertIn("effective_license_types", result)
        self.assertIn("license_type_breakdown", result)

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

    def test_default_tx_license_types_exclude_ac_contractor_and_env_override_still_wins(self):
        with mock.patch.dict(os.environ, {}, clear=False):
            self.assertEqual(
                state_lic._parse_license_types_env(),
                [
                    "Electrical Contractor",
                    "Elevator Contractor",
                    "Appliance Installation Contractor",
                ],
            )
        with mock.patch.dict(
            os.environ,
            {
                "PROSPECT_AUTOGROW_STATE_LIC_TX_LICENSE_TYPES": "A/C Contractor,Electrical Contractor",
            },
            clear=False,
        ):
            self.assertEqual(
                state_lic._parse_license_types_env(),
                ["A/C Contractor", "Electrical Contractor"],
            )

    def test_consultant_fit_rejects_generic_hvac_contractor(self):
        fit = state_lic.evaluate_state_lic_consultant_fit(
            firm="Bravo Air Conditioning Services LLC",
            owner_name="Jamie Bravo",
            license_type="A/C Contractor",
            license_subtype="Class A",
            city="Houston",
            source_detail="tdlr:AC-100",
        )
        self.assertFalse(fit["state_lic_consultant_eligible"])
        self.assertEqual(fit["state_lic_fit_status"], "fit_mismatch")
        self.assertIn("-air_conditioning", fit["state_lic_fit_reasons"])
        self.assertIn("-contractor", fit["state_lic_fit_reasons"])

    def test_consultant_fit_accepts_safety_environmental_firm(self):
        fit = state_lic.evaluate_state_lic_consultant_fit(
            firm="Texas Environmental Safety Compliance Group",
            owner_name="Pat Rivera",
            license_type="Electrical Contractor",
            license_subtype="Class B",
            city="Dallas",
            source_detail="tdlr:EC-200",
        )
        self.assertTrue(fit["state_lic_consultant_eligible"])
        self.assertEqual(fit["state_lic_fit_status"], "consultant_candidate")
        self.assertGreater(int(fit["state_lic_fit_score"]), 0)
        self.assertIn("+safety", fit["state_lic_fit_reasons"])
        self.assertIn("+compliance", fit["state_lic_fit_reasons"])

    def test_consultant_fit_is_deterministic_for_mixed_ambiguous_name(self):
        kwargs = {
            "firm": "Delta Safety HVAC Consulting LLC",
            "owner_name": "Taylor Delta",
            "license_type": "A/C Contractor",
            "license_subtype": "Class A",
            "city": "Austin",
            "source_detail": "tdlr:AC-300",
        }
        first = state_lic.evaluate_state_lic_consultant_fit(**kwargs)
        second = state_lic.evaluate_state_lic_consultant_fit(**kwargs)
        self.assertEqual(first, second)
        self.assertFalse(first["state_lic_consultant_eligible"])
        self.assertEqual(first["state_lic_fit_status"], "fit_mismatch")
        self.assertIn("+safety", first["state_lic_fit_reasons"])
        self.assertIn("-hvac", first["state_lic_fit_reasons"])

    def test_plain_environmental_is_neutral_without_other_positive_cues(self):
        fit = state_lic.evaluate_state_lic_consultant_fit(
            firm="Environmental Matters LLC",
            owner_name="Taylor Neutral",
            license_type="Electrical Contractor",
            license_subtype="Class B",
            city="Austin",
            source_detail="tdlr:EC-301",
        )
        self.assertFalse(fit["state_lic_consultant_eligible"])
        self.assertNotIn("+environmental", fit["state_lic_fit_reasons"])


if __name__ == "__main__":
    unittest.main()
