import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import crm_light
from lead_filters import filter_by_territory
from send_digest_email import (
    _is_trial_subscriber,
    build_unsubscribe_payload,
    check_suppression,
    write_trial_territory_debug_artifact,
)


class TestNonTrialGuardrails(unittest.TestCase):
    def test_non_trial_reason_tokens_do_not_change_suppression_or_optout_behavior(self) -> None:
        lead = {
            "activity_nr": "x1",
            "site_state": "TX",
            "site_city": "Plano",
            "site_zip": "99999",
            "mail_zip": "",
            "area_office": "Dallas Area Office",
        }
        filtered, stats, debug_rows = filter_by_territory([lead], "TX_TRI", include_debug=True)
        self.assertEqual(len(filtered), 1)
        self.assertEqual(stats["matched_by_office"], 1)
        self.assertEqual(debug_rows[0]["match_reason"], "FALLBACK_USED|OFFICE_MATCH|ZIP_UNKNOWN")

        with tempfile.TemporaryDirectory() as d:
            db_path = Path(d) / "osha.sqlite"
            conn = sqlite3.connect(str(db_path))
            try:
                conn.execute("CREATE TABLE suppression_list (email_or_domain TEXT NOT NULL)")
                conn.execute("INSERT INTO suppression_list(email_or_domain) VALUES (?)", ("blocked@example.com",))
                conn.commit()
            finally:
                conn.close()

            self.assertTrue(check_suppression(str(db_path), "blocked@example.com"))
            self.assertFalse(check_suppression(str(db_path), "allowed@example.com"))

        with mock.patch.dict(os.environ, {"UNSUB_ENDPOINT_BASE": ""}, clear=False):
            header, one_click, _url, _token = build_unsubscribe_payload(
                recipient="allowed@example.com",
                campaign_id="campaign_001",
                reply_to_email="support@example.com",
                dry_run=True,
            )
        self.assertEqual(header, "<mailto:support@example.com?subject=unsubscribe>")
        self.assertIsNone(one_click)

    def test_territory_debug_artifacts_are_trial_scoped_only(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            data_dir = Path(d) / "data_dir"
            with mock.patch.dict(os.environ, {"DATA_DIR": str(data_dir)}, clear=False):
                crm_light.ensure_database(None)
                with crm_light.open_conn(None) as conn:
                    crm_light.upsert_subscriber(
                        conn,
                        subscriber_key="non_trial_sub",
                        email="nontrial@example.com",
                        territory_code="TX_TRI",
                        tz="America/Chicago",
                        status="trial",
                    )
                    self.assertFalse(_is_trial_subscriber("non_trial_sub"))
                    crm_light.upsert_trial_state(
                        conn,
                        subscriber_key="non_trial_sub",
                        start_date="2026-02-11",
                        sends_limit=14,
                    )
                    self.assertTrue(_is_trial_subscriber("non_trial_sub"))

                out_path = write_trial_territory_debug_artifact(
                    subscriber_key="non_trial_sub",
                    gen_date="2026-02-19",
                    territory_debug_rows=[
                        {
                            "inspection_nr": "1874533.015",
                            "lead_key": "lead-1",
                            "site_city": "Frisco",
                            "site_zip": "75035",
                            "resolved_cbsa": "19100",
                            "territory_code": "TX_TRI",
                            "matched": "Y",
                            "match_reason": "CBSA_MATCH",
                        }
                    ],
                )
                self.assertIsNotNone(out_path)
                resolved = Path(str(out_path)).resolve()
                self.assertTrue(resolved.exists())
                expected_parent = (data_dir / "trials" / "non_trial_sub").resolve()
                self.assertEqual(resolved.parent, expected_parent)
                self.assertTrue(resolved.name.startswith("territory_debug_20260219"))
                self.assertNotIn("outreach", str(resolved).lower())
                csv_text = resolved.read_text(encoding="utf-8")
                self.assertIn("dataset_incomplete", csv_text.splitlines()[0])


if __name__ == "__main__":
    unittest.main()
