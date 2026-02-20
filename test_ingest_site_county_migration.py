import sqlite3
import tempfile
import unittest
from pathlib import Path

import ingest_osha


class TestIngestSiteCountyMigration(unittest.TestCase):
    def test_ensure_columns_adds_site_county_without_wiping_rows(self) -> None:
        conn = sqlite3.connect(":memory:")
        try:
            conn.execute(
                """
                CREATE TABLE inspections (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    activity_nr TEXT,
                    establishment_name TEXT,
                    site_city TEXT,
                    site_state TEXT,
                    inspection_type TEXT,
                    date_opened TEXT
                )
                """
            )
            conn.execute(
                """
                INSERT INTO inspections (
                    activity_nr, establishment_name, site_city, site_state, inspection_type, date_opened
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("1001", "Example Co", "Austin", "TX", "Complaint", "2026-02-01"),
            )
            conn.commit()

            ingest_osha.ensure_inspection_columns(conn)

            columns = {row[1] for row in conn.execute("PRAGMA table_info(inspections)").fetchall()}
            self.assertIn("site_county", columns)
            count = conn.execute("SELECT COUNT(1) FROM inspections").fetchone()[0]
            self.assertEqual(int(count), 1)
        finally:
            conn.close()

    def test_upsert_updates_existing_row_when_county_changes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "osha.sqlite"
            conn = sqlite3.connect(str(db_path))
            try:
                schema_text = Path("schema.sql").read_text(encoding="utf-8")
                conn.executescript(schema_text)

                base = {
                    "activity_nr": "2001",
                    "date_opened": "2026-02-01",
                    "inspection_type": "Complaint",
                    "scope": "Partial",
                    "case_status": "OPEN",
                    "establishment_name": "County Test Co",
                    "site_address1": "1 Main St",
                    "site_city": "Austin",
                    "site_state": "TX",
                    "site_zip": "78701",
                    "mail_zip": "",
                    "area_office": "Austin Area Office",
                    "source_url": "https://www.osha.gov/ords/imis/establishment.inspection_detail?id=2001.001",
                }

                first = dict(base)
                first["site_county"] = ""
                is_new, is_updated = ingest_osha.upsert_inspection(conn, first)
                self.assertTrue(is_new)
                self.assertFalse(is_updated)

                second = dict(base)
                second["site_county"] = "Travis"
                is_new2, is_updated2 = ingest_osha.upsert_inspection(conn, second)
                self.assertFalse(is_new2)
                self.assertTrue(is_updated2)

                rows = conn.execute("SELECT COUNT(1) FROM inspections WHERE activity_nr = ?", ("2001",)).fetchone()[0]
                self.assertEqual(int(rows), 1)
                stored_county = conn.execute(
                    "SELECT site_county FROM inspections WHERE activity_nr = ?",
                    ("2001",),
                ).fetchone()[0]
                self.assertEqual(str(stored_county or ""), "Travis")
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
