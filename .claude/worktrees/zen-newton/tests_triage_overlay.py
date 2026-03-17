import io
import os
import unittest
from contextlib import redirect_stdout
from datetime import datetime, timedelta, timezone
from unittest import mock

from scoring import rules_config
from scoring import triage_overlay as to


class TestTriageOverlay(unittest.TestCase):
    def _fixture_rows(self):
        return [
            {
                "activity_nr": "1876272",
                "lead_score": 3,
                "date_opened": "2026-02-20",
                "inspection_type": "Inspection",
                "scope": "Partial",
                "case_status": "OPEN",
                "naics": "237110",
                "establishment_name": "Ps Underground Llc",
                "site_address1": "1200 Hicks St.",
                "site_city": "Conroe",
                "site_state": "TX",
                "mail_state": "TX",
            },
            {
                "activity_nr": "1876218",
                "lead_score": 3,
                "date_opened": "2026-02-20",
                "inspection_type": "Inspection",
                "scope": "Partial",
                "case_status": "OPEN",
                "naics": "237110",
                "establishment_name": "Jss Construction Llc",
                "site_address1": "1200 Hicks St.",
                "site_city": "Conroe",
                "site_state": "TX",
                "mail_state": "TX",
            },
            {
                "activity_nr": "1876221",
                "lead_score": 3,
                "date_opened": "2026-02-20",
                "inspection_type": "Inspection",
                "scope": "Partial",
                "case_status": "OPEN",
                "naics": "236220",
                "establishment_name": "Cheyenne Construction Group Llc",
                "site_address1": "1200 Hicks St.",
                "site_city": "Conroe",
                "site_state": "TX",
                "mail_state": "TX",
            },
            {
                "activity_nr": "1876394",
                "lead_score": 6,
                "date_opened": "2026-02-20",
                "inspection_type": "Referral",
                "scope": "Partial",
                "case_status": "OPEN",
                "naics": "238160",
                "establishment_name": "Jeff Eubank Roofing Company, Inc.",
                "site_city": "Fort Worth",
                "site_state": "TX",
                "mail_state": "TX",
            },
            {
                "activity_nr": "1876545",
                "lead_score": 3,
                "date_opened": "2026-02-24",
                "inspection_type": "Referral",
                "scope": "Partial",
                "case_status": "OPEN",
                "naics": "811310",
                "establishment_name": "Whaley Steel Corp.",
                "site_city": "Houston",
                "site_state": "TX",
                "mail_state": "TX",
            },
            {
                "activity_nr": "1876463",
                "lead_score": 4,
                "date_opened": "2026-02-23",
                "inspection_type": "Planned",
                "scope": "Partial",
                "case_status": "OPEN",
                "naics": "238120",
                "establishment_name": "Oscar Metal Building Inc.",
                "site_city": "San Antonio",
                "site_state": "TX",
                "mail_state": "TX",
            },
            {
                "activity_nr": "1876197",
                "lead_score": 1,
                "date_opened": "2026-02-20",
                "inspection_type": "Planned",
                "scope": "No Insp/10 or Fewer Empe",
                "case_status": "CLOSED",
                "naics": "311999",
                "establishment_name": "Lulu Distributors Inc.",
                "site_city": "Houston",
                "site_state": "TX",
                "mail_state": "TX",
            },
            {
                "activity_nr": "1876259",
                "lead_score": 0,
                "date_opened": "2026-02-20",
                "inspection_type": "Inspection",
                "scope": "Partial",
                "case_status": "OPEN",
                "naics": "561720",
                "establishment_name": "Gdi Services, Inc.",
                "site_city": "Rosenberg",
                "site_state": "TX",
                "mail_state": "MI",
            },
            {
                "activity_nr": "1875646",
                "lead_score": 8,
                "date_opened": "2026-02-19",
                "inspection_type": "Accident",
                "scope": "Complete",
                "case_status": "OPEN",
                "naics": "713110",
                "establishment_name": "Delaware North Companies Parks & Resorts, Inc.",
                "site_city": "Merritt Island",
                "site_state": "FL",
                "mail_state": "NY",
            },
            {
                "activity_nr": "1867716",
                "lead_score": 8,
                "date_opened": "2026-01-08",
                "inspection_type": "Accident",
                "scope": "Partial",
                "case_status": "OPEN",
                "naics": "236220",
                "establishment_name": "Luna Development Corp.",
                "site_city": "Coconut Creek",
                "site_state": "FL",
                "mail_state": "FL",
            },
        ]

    def test_rules_fixture_expected_priorities(self):
        with mock.patch.dict(os.environ, {"SIGNAL_FRESHNESS_MAX_DAYS": "30", "AI_TRIAGE_ENABLED": "0"}, clear=False):
            decisions = to.triage(self._fixture_rows(), {}, mode="trial_render", allow_ai=False)
        by_id = {str(d.get("activity_nr")): d for d in decisions}

        self.assertEqual(by_id["1876272"]["rules_priority"], "HIGH")
        self.assertEqual(by_id["1876218"]["rules_priority"], "HIGH")
        self.assertEqual(by_id["1876394"]["rules_priority"], "HIGH")
        self.assertEqual(by_id["1876221"]["rules_priority"], "MEDIUM")
        self.assertEqual(by_id["1876545"]["rules_priority"], "MEDIUM")
        self.assertEqual(by_id["1876463"]["rules_priority"], "MEDIUM")
        self.assertEqual(by_id["1876197"]["rules_priority"], "SUPPRESS")
        self.assertEqual(by_id["1876259"]["rules_priority"], "SUPPRESS")
        self.assertEqual(by_id["1875646"]["rules_priority"], "SUPPRESS")
        self.assertEqual(by_id["1867716"]["rules_priority"], "SUPPRESS")

    def test_multi_employer_detection_three_signals(self):
        rows = [r for r in self._fixture_rows() if str(r.get("activity_nr")) in {"1876272", "1876218", "1876221"}]
        with mock.patch.dict(os.environ, {"AI_TRIAGE_ENABLED": "0"}, clear=False):
            decisions = to.triage(rows, {}, mode="trial_render", allow_ai=False)
        for d in decisions:
            if str(d.get("activity_nr")) in {"1876272", "1876218", "1876221"}:
                self.assertIn("multi_employer_site", [str(x) for x in d.get("reasons") or []])

    def test_freshness_boundary_30_kept_31_suppressed(self):
        today_utc = datetime.now(timezone.utc).date()
        opened_30 = (today_utc - timedelta(days=30)).isoformat()
        opened_31 = (today_utc - timedelta(days=31)).isoformat()
        rows = [
            {
                "activity_nr": "k30",
                "lead_score": 7,
                "date_opened": opened_30,
                "inspection_type": "Inspection",
                "scope": "Partial",
                "case_status": "OPEN",
                "naics": "236220",
            },
            {
                "activity_nr": "k31",
                "lead_score": 7,
                "date_opened": opened_31,
                "inspection_type": "Inspection",
                "scope": "Partial",
                "case_status": "OPEN",
                "naics": "236220",
            },
        ]
        with mock.patch.dict(os.environ, {"SIGNAL_FRESHNESS_MAX_DAYS": "30", "AI_TRIAGE_ENABLED": "0"}, clear=False):
            decisions = to.triage(rows, {}, mode="trial_render", allow_ai=False)
        by_id = {str(d.get("activity_nr")): d for d in decisions}
        self.assertNotEqual(by_id["k30"]["rules_priority"], "SUPPRESS")
        self.assertEqual(by_id["k31"]["rules_priority"], "SUPPRESS")

    def test_naics_suppress_allow_override(self):
        rule = rules_config.match_naics_suppress("561720")
        self.assertIsNotNone(rule)
        self.assertEqual((rule.reason or "").strip(), "NAICS_JANITORIAL")
        allow_rule = rules_config.match_naics_suppress("561621")
        self.assertIsNone(allow_rule)

    def test_ai_bidirectional_and_cannot_unsuppress(self):
        rows = [
            {
                "activity_nr": "m1",
                "lead_score": 6,
                "date_opened": "2026-02-20",
                "inspection_type": "Inspection",
                "scope": "Partial",
                "case_status": "OPEN",
                "naics": "236220",
                "establishment_name": "Alpha",
                "site_city": "Houston",
                "site_state": "TX",
                "mail_state": "TX",
            },
            {
                "activity_nr": "s1",
                "lead_score": 8,
                "date_opened": "2026-02-20",
                "inspection_type": "Planned",
                "scope": "No Insp/10 or Fewer Empe",
                "case_status": "CLOSED",
                "naics": "236220",
                "establishment_name": "Beta",
                "site_city": "Houston",
                "site_state": "TX",
                "mail_state": "TX",
            },
        ]

        def _fake_ai(**kwargs):  # noqa: ANN001
            if kwargs.get("item_key") == "m1":
                return {
                    "priority": "LOW",
                    "reason": "try lower",
                    "prompt_hash": "x",
                    "prompt_version": "v",
                    "model": "m",
                    "cached": 0,
                }
            return {
                "priority": "HIGH",
                "reason": "try unsuppress",
                "prompt_hash": "x",
                "prompt_version": "v",
                "model": "m",
                "cached": 0,
            }

        with mock.patch("scoring.ai_triage.enabled", return_value=True), mock.patch(
            "scoring.ai_triage.get_or_compute", side_effect=_fake_ai
        ) as mocked:
            decisions = to.triage(rows, {}, mode="trial_render", allow_ai=True)

        by_id = {str(d.get("activity_nr")): d for d in decisions}
        self.assertEqual(by_id["m1"]["rules_priority"], "MEDIUM")
        self.assertEqual(by_id["m1"]["final_priority"], "LOW")
        self.assertEqual(by_id["m1"]["delta_direction"], "lowered")
        self.assertEqual(by_id["s1"]["rules_priority"], "SUPPRESS")
        self.assertEqual(by_id["s1"]["final_priority"], "SUPPRESS")
        self.assertEqual(mocked.call_count, 1)

    def test_ai_unavailable_emits_warning_and_counter(self):
        rows = [
            {
                "activity_nr": "x1",
                "lead_score": 7,
                "date_opened": "2026-02-20",
                "inspection_type": "Inspection",
                "scope": "Partial",
                "case_status": "OPEN",
                "naics": "236220",
                "establishment_name": "Gamma",
                "site_city": "Austin",
                "site_state": "TX",
                "mail_state": "TX",
            }
        ]
        out = io.StringIO()
        with mock.patch("scoring.ai_triage.enabled", return_value=True), mock.patch(
            "scoring.ai_triage.get_or_compute", return_value=None
        ):
            with redirect_stdout(out):
                to.triage(rows, {}, mode="trial_render", allow_ai=True)
        text = out.getvalue()
        self.assertIn("WARN_AI_TRIAGE_UNAVAILABLE", text)
        self.assertIn("AI_TRIAGE_UNAVAILABLE=1", text)

    def test_ai_telemetry_reconciliation(self):
        rows = [
            {
                "activity_nr": "t1",
                "lead_score": 6,
                "date_opened": "2026-02-20",
                "inspection_type": "Inspection",
                "scope": "Partial",
                "case_status": "OPEN",
                "naics": "236220",
                "establishment_name": "Delta",
                "site_city": "Austin",
                "site_state": "TX",
                "mail_state": "TX",
            },
            {
                "activity_nr": "t2",
                "lead_score": 6,
                "date_opened": "2026-02-20",
                "inspection_type": "Inspection",
                "scope": "Partial",
                "case_status": "OPEN",
                "naics": "236220",
                "establishment_name": "Epsilon",
                "site_city": "Austin",
                "site_state": "TX",
                "mail_state": "TX",
            },
        ]

        def _fake_ai(**kwargs):  # noqa: ANN001
            if kwargs.get("item_key") == "t1":
                return {
                    "priority": "HIGH",
                    "reason": "raise",
                    "prompt_hash": "x",
                    "prompt_version": "v",
                    "model": "m",
                    "cached": 1,
                }
            return {
                "priority": "LOW",
                "reason": "lower",
                "prompt_hash": "x",
                "prompt_version": "v",
                "model": "m",
                "cached": 0,
            }

        out = io.StringIO()
        with mock.patch("scoring.ai_triage.enabled", return_value=True), mock.patch(
            "scoring.ai_triage.get_or_compute", side_effect=_fake_ai
        ):
            with redirect_stdout(out):
                to.triage(rows, {}, mode="trial_render", allow_ai=True)
        text = out.getvalue()
        self.assertIn("AI_TRIAGE_EVALUATED=2", text)
        self.assertIn("AI_TRIAGE_RAISED=1 LOWERED=1 UNCHANGED=0 NO_AI=0", text)


if __name__ == "__main__":
    unittest.main()
