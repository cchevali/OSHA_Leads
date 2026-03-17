import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import crm_light


class TestSubscriptionRegistryAuditMatch(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self._tmp_path = Path(self._tmp.name)
        self.crm_db = self._tmp_path / "crm_light.sqlite"
        self.leads_db = self._tmp_path / "osha.sqlite"

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _seed_crm(self, cbsa_allowlist: list[str]) -> None:
        crm_light.ensure_database(self.crm_db)
        with crm_light.open_conn(self.crm_db) as conn:
            crm_light.init_schema(conn)
            crm_light.upsert_subscriber(
                conn,
                subscriber_key="sub_example",
                email="sub@example.com",
                territory_code="TX_TRI",
                tz="America/Chicago",
                status="active",
            )
            crm_light.upsert_subscriber_entitlement(
                conn,
                subscriber_key="sub_example",
                email="sub@example.com",
                plan_code="core",
                max_metros=4,
                active=True,
                source="unittest",
            )
            crm_light.replace_subscriber_cbsa_allowlist(
                conn,
                subscriber_key="sub_example",
                cbsa_codes=cbsa_allowlist,
            )

    def _run_audit_match(self, inspection: str, expect_nonzero: bool = False) -> dict:
        repo_root = Path(__file__).resolve().parent
        env = dict(os.environ)
        env["PYTHONPATH"] = str(repo_root)
        cmd = [
            sys.executable,
            "scripts/subscription_registry_ops.py",
            "audit-match",
            "--inspection",
            inspection,
            "--subscriber-key",
            "sub_example",
            "--db",
            str(self.leads_db),
            "--crm-db",
            str(self.crm_db),
        ]
        proc = subprocess.run(cmd, cwd=str(repo_root), env=env, capture_output=True, text=True, check=False)
        if expect_nonzero:
            self.assertNotEqual(proc.returncode, 0)
        else:
            self.assertEqual(proc.returncode, 0)
        lines = [line.strip() for line in str(proc.stdout or "").splitlines() if line.strip()]
        self.assertTrue(lines)
        return json.loads(lines[-1])

    def test_audit_match_outputs_reason_and_office_when_unmatched(self) -> None:
        self._seed_crm(["12420"])
        conn = sqlite3.connect(str(self.leads_db))
        try:
            conn.execute(
                """
                CREATE TABLE inspections (
                    activity_nr TEXT,
                    lead_key TEXT,
                    source_url TEXT,
                    site_city TEXT,
                    site_state TEXT,
                    site_zip TEXT,
                    mail_zip TEXT,
                    area_office TEXT
                )
                """
            )
            conn.execute(
                """
                INSERT INTO inspections (
                    activity_nr, lead_key, source_url, site_city, site_state, site_zip, mail_zip, area_office
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "9990001",
                    "lead-9990001",
                    "https://www.osha.gov/ords/imis/establishment.inspection_detail?id=9990001.001",
                    "Unknown City",
                    "TX",
                    "99999",
                    "",
                    "Dallas Area Office",
                ),
            )
            conn.commit()
        finally:
            conn.close()

        payload = self._run_audit_match("9990001.001")
        self.assertTrue(payload.get("ok"))
        self.assertTrue(payload.get("present_in_data"))
        self.assertFalse(payload.get("matched"))
        self.assertIn("match_reason", payload)
        self.assertIn("unmatched_reason", payload)
        self.assertIn("resolution_source", payload)
        self.assertEqual(payload.get("match_reason"), "CBSA_UNRESOLVED|ZIP_UNKNOWN")
        self.assertEqual(payload.get("reason_token"), "CBSA_UNRESOLVED|ZIP_UNKNOWN")
        self.assertEqual(payload.get("unmatched_reason"), "CBSA_UNRESOLVED|ZIP_UNKNOWN")
        self.assertEqual(payload.get("inspection_office"), "Dallas Area Office")
        self.assertEqual(payload.get("site_county"), "")
        self.assertEqual(payload.get("resolution_source"), "NONE")
        self.assertEqual(payload.get("resolved_cbsa"), "")

    def test_audit_match_outputs_county_resolution_fields_when_matched(self) -> None:
        self._seed_crm(["12420"])
        conn = sqlite3.connect(str(self.leads_db))
        try:
            conn.execute(
                """
                CREATE TABLE inspections (
                    activity_nr TEXT,
                    lead_key TEXT,
                    source_url TEXT,
                    site_city TEXT,
                    site_state TEXT,
                    site_zip TEXT,
                    mail_zip TEXT,
                    site_county TEXT,
                    area_office TEXT
                )
                """
            )
            conn.execute(
                """
                INSERT INTO inspections (
                    activity_nr, lead_key, source_url, site_city, site_state, site_zip, mail_zip, site_county, area_office
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "9000001",
                    "lead-9000001",
                    "https://www.osha.gov/ords/imis/establishment.inspection_detail?id=9000001.001",
                    "Taylor",
                    "TX",
                    "",
                    "",
                    "Williamson",
                    "Dallas Area Office",
                ),
            )
            conn.commit()
        finally:
            conn.close()

        payload = self._run_audit_match("9000001.001")
        self.assertTrue(payload.get("ok"))
        self.assertTrue(payload.get("present_in_data"))
        self.assertTrue(payload.get("matched"))
        self.assertIn("match_reason", payload)
        self.assertIn("unmatched_reason", payload)
        self.assertIn("resolution_source", payload)
        self.assertEqual(payload.get("resolved_cbsa"), "12420")
        self.assertEqual(payload.get("resolution_source"), "SITE_COUNTY")
        self.assertEqual(payload.get("match_reason"), "CBSA_MATCH")
        self.assertEqual(payload.get("reason_token"), "CBSA_MATCH")
        self.assertEqual(payload.get("unmatched_reason"), "")
        self.assertEqual(payload.get("site_city"), "Taylor")
        self.assertEqual(payload.get("site_county"), "Williamson")
        self.assertEqual(payload.get("inspection_office"), "Dallas Area Office")

    def test_audit_match_empty_allowlist_still_emits_contract_fields(self) -> None:
        self._seed_crm([])
        payload = self._run_audit_match("no-such-id", expect_nonzero=True)
        self.assertFalse(payload.get("ok"))
        self.assertEqual(payload.get("reason_token"), "CBSA_ALLOWLIST_EMPTY")
        self.assertIn("match_reason", payload)
        self.assertEqual(payload.get("match_reason"), "")
        self.assertIn("unmatched_reason", payload)
        self.assertEqual(payload.get("unmatched_reason"), "CBSA_ALLOWLIST_EMPTY")
        self.assertIn("resolution_source", payload)
        self.assertEqual(payload.get("resolution_source"), "NONE")


if __name__ == "__main__":
    unittest.main()
