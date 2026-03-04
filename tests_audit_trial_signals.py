import io
import json
import os
import sqlite3
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest import mock

from tools import audit_trial_signals as ats


def _seed_leads_db(path: Path) -> None:
    conn = sqlite3.connect(str(path))
    try:
        conn.execute(
            """
            CREATE TABLE inspections (
                lead_key TEXT,
                activity_nr TEXT,
                date_opened TEXT,
                inspection_type TEXT,
                scope TEXT,
                case_status TEXT,
                establishment_name TEXT,
                site_city TEXT,
                site_state TEXT,
                site_zip TEXT,
                mail_zip TEXT,
                site_county TEXT,
                area_office TEXT,
                lead_score INTEGER,
                first_seen_at TEXT,
                last_seen_at TEXT,
                changed_at TEXT,
                source_url TEXT,
                parse_invalid INTEGER DEFAULT 0
            )
            """
        )
        rows = [
            ("osha:activity:1001", "1001", "2026-02-22", "Referral", "", "", "Alpha", "Dallas", "TX", "75001", "", "", "", 11, "2026-02-22T10:00:00Z", "2026-02-22T10:00:00Z", "2026-02-22T10:00:00Z", "https://x?id=1001.001", 0),
            ("osha:activity:1002", "1002", "2026-02-23", "Planned", "", "", "Bravo", "Tyler", "TX", "75701", "", "", "", 5, "2026-02-23T10:00:00Z", "2026-02-23T10:00:00Z", "2026-02-23T10:00:00Z", "https://x?id=1002.001", 0),
            ("osha:activity:1003", "1003", "2026-02-24", "Complaint", "", "", "Charlie", "Austin", "TX", "78701", "", "", "", 8, "2026-02-24T10:00:00Z", "2026-02-24T10:00:00Z", "2026-02-24T10:00:00Z", "https://x?id=1003.001", 0),
        ]
        conn.executemany(
            """
            INSERT INTO inspections (
                lead_key, activity_nr, date_opened, inspection_type, scope, case_status,
                establishment_name, site_city, site_state, site_zip, mail_zip, site_county,
                area_office, lead_score, first_seen_at, last_seen_at, changed_at, source_url, parse_invalid
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        conn.commit()
    finally:
        conn.close()


class TestAuditTrialSignals(unittest.TestCase):
    def _cfg(self) -> dict:
        return {
            "customer_id": "wally_trial_tx_triangle_v1",
            "subscriber_key": "wally_trial",
            "territory_code": "TX_TRI",
            "states": ["TX"],
            "content_filter": "high_medium",
            "include_low_fallback": True,
            "opened_window_days": 14,
            "new_only_days": 1,
            "timezone": "America/Chicago",
            "recipients": ["wgs@indigocompliance.com", "brandon@example.com"],
        }

    def test_print_config_emits_and_writes_no_artifact(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            data_dir = tmp / "runtime"
            leads_db = tmp / "osha.sqlite"
            customer_path = tmp / "customer.json"
            _seed_leads_db(leads_db)
            customer_path.write_text(json.dumps(self._cfg()) + "\n", encoding="utf-8")
            out = io.StringIO()
            err = io.StringIO()
            env = {"DATA_DIR": str(data_dir)}
            with (
                mock.patch.dict(os.environ, env, clear=False),
                mock.patch.object(ats, "_read_customer_config", return_value=(customer_path, self._cfg())),
                redirect_stdout(out),
                redirect_stderr(err),
            ):
                rc = ats.main(
                    [
                        "--subscriber-key",
                        "wally_trial",
                        "--since-date",
                        "2026-02-22",
                        "--through-date",
                        "2026-02-26",
                        "--db",
                        str(leads_db),
                        "--print-config",
                    ]
                )
            self.assertEqual(rc, 0, msg=err.getvalue())
            text = out.getvalue()
            self.assertIn("SIGNAL_AUDIT_OUTPUT_JSON=", text)
            self.assertIn("SIGNAL_AUDIT_COMPLETE=status=PRINT_CONFIG", text)
            expected = data_dir / "trials" / "wally_trial" / "signal_audit_2026-02-22_2026-02-26.json"
            self.assertFalse(expected.exists())

    def test_writes_data_dir_artifact_and_maps_exclusions(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            data_dir = tmp / "runtime"
            leads_db = tmp / "osha.sqlite"
            customer_path = tmp / "customer.json"
            _seed_leads_db(leads_db)
            customer_path.write_text(json.dumps(self._cfg()) + "\n", encoding="utf-8")

            debug_map = {
                "osha:activity:1001": {"lead_key": "osha:activity:1001", "matched": "Y", "resolved_cbsa": "19100", "match_reason": "CBSA_MATCH", "unmatched_reason": ""},
                "osha:activity:1002": {"lead_key": "osha:activity:1002", "matched": "N", "resolved_cbsa": "", "match_reason": "CBSA_UNRESOLVED|ZIP_UNKNOWN", "unmatched_reason": "CBSA_UNRESOLVED|ZIP_UNKNOWN"},
                "osha:activity:1003": {"lead_key": "osha:activity:1003", "matched": "Y", "resolved_cbsa": "12420", "match_reason": "CBSA_MATCH", "unmatched_reason": ""},
            }
            out = io.StringIO()
            err = io.StringIO()
            env = {"DATA_DIR": str(data_dir)}
            with (
                mock.patch.dict(os.environ, env, clear=False),
                mock.patch.object(ats, "_read_customer_config", return_value=(customer_path, self._cfg())),
                mock.patch.object(ats, "_collect_territory_debug", return_value=(debug_map, {"osha:activity:1001", "osha:activity:1003"})),
                mock.patch.object(
                    ats,
                    "_load_delivered_rendered_sets",
                    return_value=(
                        {"osha:activity:1001"},
                        {"2026-02-24": {"osha:activity:1001"}},
                        [{"ts_utc": "2026-02-24T15:00:00+00:00", "local_date": "2026-02-24"}],
                    ),
                ),
                mock.patch.object(
                    ats,
                    "_reconstruct_filter_exclusions",
                    return_value=(
                        {"osha:activity:1003": "below threshold"},
                        {"2026-02-24": {"osha:activity:1001", "osha:activity:1003"}},
                        {},
                        {"2026-02-24": {"stats": {}, "exclusion_count": 1}},
                    ),
                ),
                redirect_stdout(out),
                redirect_stderr(err),
            ):
                rc = ats.main(
                    [
                        "--subscriber-key",
                        "wally_trial",
                        "--since-date",
                        "2026-02-22",
                        "--through-date",
                        "2026-02-26",
                        "--db",
                        str(leads_db),
                    ]
                )
            self.assertEqual(rc, 0, msg=err.getvalue())
            artifact = data_dir / "trials" / "wally_trial" / "signal_audit_2026-02-22_2026-02-26.json"
            self.assertTrue(artifact.exists())
            payload = json.loads(artifact.read_text(encoding="utf-8"))
            rows = {r["activity_nr"]: r for r in payload["rows"]}
            self.assertEqual(rows["1001"]["was_delivered"], "Y")
            self.assertEqual(rows["1001"]["excluded_reason"], "")
            self.assertEqual(rows["1002"]["excluded_reason"], "outside territory")
            self.assertEqual(rows["1003"]["excluded_reason"], "below threshold")
            self.assertEqual(rows["1001"]["resolved_cbsa"], "19100")
            self.assertIn("date_opened\tactivity_nr", out.getvalue())
            self.assertIn("SIGNAL_AUDIT_JSON_WRITTEN=", out.getvalue())

    def test_with_triage_overlay_adds_columns_and_flags(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            data_dir = tmp / "runtime"
            leads_db = tmp / "osha.sqlite"
            customer_path = tmp / "customer.json"
            _seed_leads_db(leads_db)
            customer_path.write_text(json.dumps(self._cfg()) + "\n", encoding="utf-8")

            debug_map = {
                "osha:activity:1001": {"lead_key": "osha:activity:1001", "matched": "Y", "resolved_cbsa": "19100", "match_reason": "CBSA_MATCH", "unmatched_reason": ""},
                "osha:activity:1002": {"lead_key": "osha:activity:1002", "matched": "N", "resolved_cbsa": "", "match_reason": "CBSA_NO_MATCH", "unmatched_reason": "CBSA_NO_MATCH"},
                "osha:activity:1003": {"lead_key": "osha:activity:1003", "matched": "Y", "resolved_cbsa": "12420", "match_reason": "CBSA_MATCH", "unmatched_reason": ""},
            }
            with (
                mock.patch.dict(os.environ, {"DATA_DIR": str(data_dir)}, clear=False),
                mock.patch.object(ats, "_read_customer_config", return_value=(customer_path, self._cfg())),
                mock.patch.object(ats, "_collect_territory_debug", return_value=(debug_map, {"osha:activity:1001", "osha:activity:1003"})),
                mock.patch.object(
                    ats,
                    "_load_delivered_rendered_sets",
                    return_value=(
                        set(),
                        {},
                        [],
                    ),
                ),
                mock.patch.object(
                    ats,
                    "_reconstruct_filter_exclusions",
                    return_value=({}, {}, {}, {}),
                ),
                mock.patch.object(
                    ats,
                    "_compute_triage_overlay_annotations",
                    return_value=(
                        {
                            "osha:activity:1001": {
                                "triage_decision": "downgrade_to_medium",
                                "triage_reason": "referral;stale",
                                "ai_triage_decision": "",
                            },
                            "osha:activity:1003": {
                                "triage_decision": "remove_from_customer_email",
                                "triage_reason": "stale",
                                "ai_triage_decision": "remove_from_customer_email",
                            },
                        },
                        {"osha:activity:1003"},
                    ),
                ),
            ):
                buf = io.StringIO()
                err = io.StringIO()
                with redirect_stdout(buf), redirect_stderr(err):
                    rc = ats.main(
                        [
                            "--subscriber-key",
                            "wally_trial",
                            "--since-date",
                            "2026-02-22",
                            "--through-date",
                            "2026-02-26",
                            "--db",
                            str(leads_db),
                            "--with-triage-overlay",
                            "--dry-run",
                        ]
                    )
                self.assertEqual(rc, 0, msg=err.getvalue())
                text = buf.getvalue()
                header_line = next((ln for ln in text.splitlines() if ln.startswith("date_opened\tactivity_nr\t")), "")
                self.assertIn("triage_decision", header_line)
                self.assertIn("would_have_changed_delivery", header_line)
                self.assertIn("downgrade_to_medium", text)
                self.assertIn("remove_from_customer_email", text)


if __name__ == "__main__":
    unittest.main()
