import tempfile
import unittest
from datetime import datetime, timezone
from contextlib import redirect_stdout
import io
from pathlib import Path

from outreach import crm_store
from outreach import run_outreach_auto as roa


class TestOutreachSendabilityGate(unittest.TestCase):
    def _seed(self, conn) -> None:
        now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        rows = [
            (
                "p_core",
                "Core Safety",
                "Core Owner",
                "core@core-safety.com",
                "Owner",
                "Austin",
                "TX",
                "https://core-safety.com",
                "apollo_export_csv",
                "core_consultant",
                1,
            ),
            (
                "p_recover",
                "Recover Safety",
                "Recover Owner",
                "recover@recover-safety.com",
                "Consultant",
                "Austin",
                "TX",
                "https://recover-safety.com",
                "aiha_consultants_listing:12-13",
                "recoverable_consultant",
                1,
            ),
            (
                "p_adj",
                "Adj Contractor",
                "Adj Owner",
                "adj@adj-contractor.com",
                "Owner",
                "Austin",
                "TX",
                "https://adj-contractor.com",
                "STATE_LIC",
                "adjacent_contractor",
                0,
            ),
            (
                "p_adj_legacy_sendable",
                "Adj Legacy Contractor",
                "Adj Legacy Owner",
                "adjlegacy@adj-legacy-contractor.com",
                "Owner",
                "Austin",
                "TX",
                "https://adj-legacy-contractor.com",
                "STATE_LIC",
                "core_consultant",
                1,
            ),
            (
                "p_not_sendable_recover",
                "Recover Blocked",
                "Blocked Owner",
                "blocked@recover-blocked.com",
                "Owner",
                "Austin",
                "TX",
                "https://recover-blocked.com",
                "legacy_directory_export",
                "recoverable_consultant",
                0,
            ),
        ]
        for (
            prospect_id,
            firm,
            contact_name,
            email,
            title,
            city,
            state,
            website,
            source,
            source_fit_tier,
            default_send_eligible,
        ) in rows:
            conn.execute(
                """
                INSERT INTO prospects(
                    prospect_id, firm, contact_name, email, title, city, state, website, source,
                    source_fit_tier, default_send_eligible, score, status, created_at, last_contacted_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    prospect_id,
                    firm,
                    contact_name,
                    email,
                    title,
                    city,
                    state,
                    website,
                    source,
                    source_fit_tier,
                    int(default_send_eligible),
                    7,
                    "new",
                    now,
                    None,
                ),
            )
        conn.commit()

    def test_default_selection_includes_legacy_blocked_rows(self):
        with tempfile.TemporaryDirectory() as d:
            db_path = Path(d) / "crm.sqlite"
            crm_store.ensure_database(path=db_path)
            conn = crm_store.connect(db_path)
            try:
                self._seed(conn)
                selected, skipped, _manifest_rows, stats = roa._select_candidates(
                    conn=conn,
                    state="TX",
                    limit=10,
                    suppressed_emails=set(),
                    allow_repeat=True,
                    skip_role_inboxes=True,
                    include_adjacent_contractors=False,
                )
            finally:
                conn.close()

            selected_ids = {str(item.get("prospect_id") or "") for item in selected}
            self.assertEqual(selected_ids, {"p_core", "p_recover", "p_adj", "p_adj_legacy_sendable", "p_not_sendable_recover"})
            self.assertEqual(int(skipped.get("not_default_send_eligible", 0)), 0)
            self.assertEqual(int(stats.get("eligible", 0)), 5)
            eligible_by_tier = dict(stats.get("eligible_by_tier") or {})
            self.assertEqual(int(eligible_by_tier.get("core_consultant", 0)), 1)
            self.assertEqual(int(eligible_by_tier.get("recoverable_consultant", 0)), 2)
            self.assertEqual(int(eligible_by_tier.get("adjacent_contractor", 0)), 2)
            self.assertEqual(int(stats.get("excluded_adjacent_contractor_total", 0)), 0)

    def test_override_matches_default_once_legacy_skips_are_removed(self):
        with tempfile.TemporaryDirectory() as d:
            db_path = Path(d) / "crm.sqlite"
            crm_store.ensure_database(path=db_path)
            conn = crm_store.connect(db_path)
            try:
                self._seed(conn)
                selected, skipped, _manifest_rows, stats = roa._select_candidates(
                    conn=conn,
                    state="TX",
                    limit=10,
                    suppressed_emails=set(),
                    allow_repeat=True,
                    skip_role_inboxes=True,
                    include_adjacent_contractors=True,
                )
            finally:
                conn.close()

            selected_ids = {str(item.get("prospect_id") or "") for item in selected}
            self.assertEqual(selected_ids, {"p_core", "p_recover", "p_adj", "p_adj_legacy_sendable", "p_not_sendable_recover"})
            self.assertEqual(int(skipped.get("not_default_send_eligible", 0)), 0)
            self.assertEqual(int(stats.get("eligible", 0)), 5)
            eligible_by_tier = dict(stats.get("eligible_by_tier") or {})
            self.assertEqual(int(eligible_by_tier.get("adjacent_contractor", 0)), 2)
            self.assertEqual(int(stats.get("excluded_adjacent_contractor_total", 0)), 0)

    def test_plan_selection_counters_emit_adjacent_exclusion_tokens(self):
        buf = io.StringIO()
        with redirect_stdout(buf):
            roa._print_plan_selection_counters(
                {
                    "eligible": 3,
                    "eligible_by_tier": {
                        "core_consultant": 1,
                        "recoverable_consultant": 1,
                        "adjacent_contractor": 1,
                    },
                    "excluded_adjacent_contractor_total": 2,
                }
            )
        out = buf.getvalue()
        self.assertIn("OUTREACH_PLAN_ELIGIBLE_TOTAL=3", out)
        self.assertIn("OUTREACH_PLAN_ELIGIBLE_BY_TIER_CORE_CONSULTANT=1", out)
        self.assertIn("OUTREACH_PLAN_ELIGIBLE_BY_TIER_RECOVERABLE_CONSULTANT=1", out)
        self.assertIn("OUTREACH_PLAN_ELIGIBLE_BY_TIER_ADJACENT_CONTRACTOR=1", out)
        self.assertIn("OUTREACH_PLAN_EXCLUDED_ADJACENT_CONTRACTOR_TOTAL=2", out)


if __name__ == "__main__":
    unittest.main()
