import csv
import gzip
import json
import os
import sqlite3
import tempfile
import time
import unittest
from pathlib import Path

import crm_light
import run_wally_trial
from geo import zip_cbsa


class TestWallyAuditCbsa(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self._tmp_path = Path(self._tmp.name)
        self._orig_data_dir = os.environ.get("DATA_DIR")
        self._orig_zip_to_cbsa = zip_cbsa.ZIP_TO_CBSA_PATH
        self._orig_cbsa_meta = zip_cbsa.CBSA_META_PATH
        self._orig_dataset_meta = zip_cbsa.ZIP_TO_CBSA_DATASET_META_PATH
        self._orig_sources_path = zip_cbsa.ZIP_TO_CBSA_SOURCES_PATH

        os.environ["DATA_DIR"] = str(self._tmp_path / "data_dir")
        self.crm_db_path = crm_light.ensure_database(None)
        with crm_light.open_conn(None) as conn:
            crm_light.upsert_subscriber(
                conn,
                subscriber_key="wally_trial",
                email="wally@example.com",
                territory_code="TX_TRI",
                tz="America/Chicago",
                status="trial",
            )
            crm_light.upsert_trial_state(
                conn,
                subscriber_key="wally_trial",
                start_date="2026-02-11",
                sends_limit=14,
            )
            crm_light.append_send_event(
                conn,
                subscriber_key="wally_trial",
                variant="DAILY",
                status="SENT",
                run_id="run_001",
                meta={"primary_recipient": "wally@example.com", "send_mode": "LIVE"},
                ts_utc="2026-02-12T14:05:00+00:00",
            )

        self.leads_db = self._tmp_path / "osha.sqlite"
        self._build_leads_db(self.leads_db)
        self.customer_path = self._tmp_path / "wally_fixture.customer.json"
        self.customer_path.write_text(
            json.dumps(
                {
                    "customer_id": "wally_trial_tx_triangle_v1",
                    "subscriber_key": "wally_trial",
                    "territory_code": "TX_TRI",
                    "states": ["TX"],
                    "content_filter": "high_medium",
                    "include_low_fallback": True,
                    "opened_window_days": 14,
                    "new_only_days": 1,
                    "send_time_local": "08:00",
                    "send_window_minutes": 60,
                },
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )

        zip_map_path = self._tmp_path / "zip_to_cbsa.csv.gz"
        with gzip.open(zip_map_path, "wt", encoding="utf-8", newline="") as fh:
            writer = csv.writer(fh)
            writer.writerow(["ZIP5", "CBSA"])
            writer.writerow(["75035", "19100"])

        meta_path = self._tmp_path / "cbsa_meta.csv"
        with open(meta_path, "w", encoding="utf-8", newline="") as fh:
            writer = csv.writer(fh)
            writer.writerow(["CBSA", "metro_label"])
            writer.writerow(["19100", "Dallas-Fort Worth-Arlington, TX"])
        dataset_meta_path = self._tmp_path / "zip_to_cbsa.meta.json"
        dataset_meta_path.write_text(
            json.dumps(
                {
                    "source_label": "HUD USPS ZIP-CBSA seed bootstrap (coverage incomplete)",
                    "dataset_incomplete": True,
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        sources_path = self._tmp_path / "SOURCES.md"
        sources_path.write_text("# test\n", encoding="utf-8")

        zip_cbsa.ZIP_TO_CBSA_PATH = zip_map_path
        zip_cbsa.CBSA_META_PATH = meta_path
        zip_cbsa.ZIP_TO_CBSA_DATASET_META_PATH = dataset_meta_path
        zip_cbsa.ZIP_TO_CBSA_SOURCES_PATH = sources_path
        zip_cbsa.clear_caches()

    def tearDown(self) -> None:
        if self._orig_data_dir is None:
            os.environ.pop("DATA_DIR", None)
        else:
            os.environ["DATA_DIR"] = self._orig_data_dir
        zip_cbsa.ZIP_TO_CBSA_PATH = self._orig_zip_to_cbsa
        zip_cbsa.CBSA_META_PATH = self._orig_cbsa_meta
        zip_cbsa.ZIP_TO_CBSA_DATASET_META_PATH = self._orig_dataset_meta
        zip_cbsa.ZIP_TO_CBSA_SOURCES_PATH = self._orig_sources_path
        zip_cbsa.clear_caches()
        self._tmp.cleanup()

    def _build_leads_db(self, path: Path) -> None:
        conn = sqlite3.connect(str(path))
        try:
            conn.execute(
                """
                CREATE TABLE inspections (
                    lead_id TEXT,
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
                    area_office TEXT,
                    naics TEXT,
                    naics_desc TEXT,
                    violations_count INTEGER,
                    emphasis TEXT,
                    lead_score INTEGER,
                    first_seen_at TEXT,
                    last_seen_at TEXT,
                    changed_at TEXT,
                    source_url TEXT,
                    parse_invalid INTEGER DEFAULT 0
                )
                """
            )
            conn.execute(
                """
                INSERT INTO inspections (
                    lead_id, lead_key, activity_nr, date_opened, inspection_type, scope, case_status,
                    establishment_name, site_city, site_state, site_zip, mail_zip, area_office, naics, naics_desc,
                    violations_count, emphasis, lead_score, first_seen_at, last_seen_at, changed_at, source_url, parse_invalid
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "lead-1874533",
                    "lead-1874533",
                    "1874533",
                    "2026-02-11",
                    "Planned",
                    "Partial",
                    "Open",
                    "Fat/Cat",
                    "Frisco",
                    "TX",
                    "75035",
                    "",
                    "",
                    "236220",
                    "Commercial Building Construction",
                    0,
                    "",
                    7,
                    "2026-02-11T10:00:00",
                    "2026-02-11T10:10:00",
                    "2026-02-11T10:10:00",
                    "https://www.osha.gov/ords/imis/establishment.inspection_detail?id=1874533.015",
                    0,
                ),
            )
            conn.execute(
                """
                INSERT INTO inspections (
                    lead_id, lead_key, activity_nr, date_opened, inspection_type, scope, case_status,
                    establishment_name, site_city, site_state, site_zip, mail_zip, area_office, naics, naics_desc,
                    violations_count, emphasis, lead_score, first_seen_at, last_seen_at, changed_at, source_url, parse_invalid
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "lead-9990001",
                    "lead-9990001",
                    "9990001",
                    "2026-02-12",
                    "Planned",
                    "Partial",
                    "Open",
                    "Fallback Office Co",
                    "Unknown City",
                    "TX",
                    "99999",
                    "",
                    "Dallas Area Office",
                    "236220",
                    "Commercial Building Construction",
                    0,
                    "",
                    7,
                    "2026-02-12T09:00:00",
                    "2026-02-12T09:10:00",
                    "2026-02-12T09:10:00",
                    "https://www.osha.gov/ords/imis/establishment.inspection_detail?id=9990001.001",
                    0,
                ),
            )
            conn.commit()
        finally:
            conn.close()

    def test_audit_writes_artifacts_and_check_inspection_tokens(self) -> None:
        t0 = time.perf_counter()
        code = run_wally_trial.run_wally_audit(
            db_path=str(self.leads_db),
            customer_path=self.customer_path,
            as_of="2026-02-13",
            check_inspection="1874533.015",
        )
        elapsed = time.perf_counter() - t0
        self.assertEqual(code, 0)
        self.assertLess(elapsed, 5.0)

        out_dir = Path(os.environ["DATA_DIR"]) / "trials" / "wally_trial"
        events_path = out_dir / "audit_events.json"
        exclusions_path = out_dir / "audit_exclusions.csv"
        report_path = out_dir / "audit_report.md"
        self.assertTrue(events_path.exists())
        self.assertTrue(exclusions_path.exists())
        self.assertTrue(report_path.exists())

        payload = json.loads(events_path.read_text(encoding="utf-8"))
        self.assertTrue(payload.get("dataset_incomplete"))
        check_payload = payload.get("check_inspection") or {}
        self.assertTrue(check_payload.get("present_in_data"))
        self.assertEqual(check_payload.get("inspection_nr"), "1874533.015")
        self.assertIn("CBSA_MATCH", str(check_payload.get("reason_token") or ""))
        self.assertEqual(str(check_payload.get("unmatched_reason") or ""), "")
        self.assertTrue(check_payload.get("dataset_incomplete"))
        exclusions_text = exclusions_path.read_text(encoding="utf-8")
        self.assertIn("dataset_incomplete", exclusions_text.splitlines()[0])

        report = report_path.read_text(encoding="utf-8")
        self.assertIn("## Check Inspection", report)
        self.assertIn("1874533.015", report)

    def test_audit_persists_fallback_reason_tokens(self) -> None:
        code = run_wally_trial.run_wally_audit(
            db_path=str(self.leads_db),
            customer_path=self.customer_path,
            as_of="2026-02-13",
            check_inspection="9990001.001",
        )
        self.assertEqual(code, 0)
        out_dir = Path(os.environ["DATA_DIR"]) / "trials" / "wally_trial"
        payload = json.loads((out_dir / "audit_events.json").read_text(encoding="utf-8"))
        self.assertTrue(payload.get("dataset_incomplete"))
        check_payload = payload.get("check_inspection") or {}
        reason = str(check_payload.get("reason_token") or "")
        self.assertEqual(reason, "CBSA_UNRESOLVED|ZIP_UNKNOWN")
        self.assertEqual(str(check_payload.get("unmatched_reason") or ""), "CBSA_UNRESOLVED|ZIP_UNKNOWN")
        self.assertEqual(str(check_payload.get("inspection_office") or ""), "Dallas Area Office")


if __name__ == "__main__":
    unittest.main()
