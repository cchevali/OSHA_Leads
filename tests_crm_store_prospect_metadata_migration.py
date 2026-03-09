import sqlite3
import tempfile
import unittest
from pathlib import Path

from outreach import crm_store


class TestCrmStoreProspectMetadataMigration(unittest.TestCase):
    def test_adds_metadata_columns_with_defaults_on_legacy_schema(self):
        with tempfile.TemporaryDirectory() as d:
            db_path = Path(d) / "crm.sqlite"
            conn = sqlite3.connect(str(db_path))
            try:
                conn.executescript(
                    """
                    CREATE TABLE prospects (
                        prospect_id TEXT PRIMARY KEY,
                        firm TEXT NOT NULL DEFAULT '',
                        contact_name TEXT NOT NULL DEFAULT '',
                        email TEXT NOT NULL UNIQUE,
                        title TEXT NOT NULL DEFAULT '',
                        city TEXT NOT NULL DEFAULT '',
                        state TEXT NOT NULL DEFAULT '',
                        website TEXT NOT NULL DEFAULT '',
                        source TEXT NOT NULL DEFAULT '',
                        score INTEGER NOT NULL DEFAULT 0,
                        status TEXT NOT NULL DEFAULT 'new',
                        created_at TEXT NOT NULL,
                        last_contacted_at TEXT
                    );
                    INSERT INTO prospects(
                        prospect_id, firm, contact_name, email, title, city, state, website, source, score, status, created_at
                    ) VALUES(
                        'legacy_1', 'Legacy Co', 'Legacy Owner', 'legacy@example.com', 'Owner', 'Austin', 'TX', '', 'legacy_csv', 0, 'new', '2026-03-06T00:00:00+00:00'
                    );
                    """
                )
                conn.commit()
            finally:
                conn.close()

            crm_store.ensure_database(db_path)
            crm_store.ensure_database(db_path)

            conn = sqlite3.connect(str(db_path))
            try:
                cols = {
                    str(row[1]): {"notnull": int(row[3] or 0), "default": str(row[4] or "")}
                    for row in conn.execute("PRAGMA table_info(prospects)").fetchall()
                }
                self.assertIn("source_fit_tier", cols)
                self.assertIn("default_send_eligible", cols)
                self.assertEqual(cols["source_fit_tier"]["notnull"], 1)
                self.assertIn("recoverable_consultant", cols["source_fit_tier"]["default"])
                self.assertEqual(cols["default_send_eligible"]["notnull"], 1)
                self.assertIn("1", cols["default_send_eligible"]["default"])

                row = conn.execute(
                    """
                    SELECT source_fit_tier, default_send_eligible
                    FROM prospects
                    WHERE prospect_id='legacy_1'
                    """
                ).fetchone()
                self.assertEqual(str(row[0] or ""), "recoverable_consultant")
                self.assertEqual(int(row[1] or 0), 1)
            finally:
                conn.close()

    def test_metadata_backfill_and_repeated_migration_are_idempotent(self):
        with tempfile.TemporaryDirectory() as d:
            db_path = Path(d) / "crm.sqlite"
            conn = sqlite3.connect(str(db_path))
            try:
                conn.executescript(
                    """
                    CREATE TABLE prospects (
                        prospect_id TEXT PRIMARY KEY,
                        firm TEXT NOT NULL DEFAULT '',
                        contact_name TEXT NOT NULL DEFAULT '',
                        email TEXT NOT NULL UNIQUE,
                        title TEXT NOT NULL DEFAULT '',
                        city TEXT NOT NULL DEFAULT '',
                        state TEXT NOT NULL DEFAULT '',
                        website TEXT NOT NULL DEFAULT '',
                        source TEXT NOT NULL DEFAULT '',
                        source_fit_tier TEXT,
                        default_send_eligible TEXT,
                        score INTEGER NOT NULL DEFAULT 0,
                        status TEXT NOT NULL DEFAULT 'new',
                        created_at TEXT NOT NULL,
                        last_contacted_at TEXT
                    );
                    INSERT INTO prospects(
                        prospect_id, firm, contact_name, email, title, city, state, website, source,
                        source_fit_tier, default_send_eligible, score, status, created_at
                    ) VALUES
                    ('p_null', 'A', 'A', 'a@example.com', 'Owner', 'Austin', 'TX', '', 'legacy', NULL, NULL, 0, 'new', '2026-03-06T00:00:00+00:00'),
                    ('p_upper', 'B', 'B', 'b@example.com', 'Owner', 'Austin', 'TX', '', 'legacy', 'CORE_CONSULTANT', 'true', 0, 'new', '2026-03-06T00:00:00+00:00'),
                    ('p_invalid', 'C', 'C', 'c@example.com', 'Owner', 'Austin', 'TX', '', 'legacy', 'bad_tier', '2', 0, 'new', '2026-03-06T00:00:00+00:00'),
                    ('p_adj', 'D', 'D', 'd@example.com', 'Owner', 'Austin', 'TX', '', 'legacy', 'adjacent_contractor', 'false', 0, 'new', '2026-03-06T00:00:00+00:00');
                    """
                )
                conn.commit()
            finally:
                conn.close()

            crm_store.ensure_database(db_path)
            conn = sqlite3.connect(str(db_path))
            try:
                first_rows = conn.execute(
                    """
                    SELECT prospect_id, source_fit_tier, default_send_eligible
                    FROM prospects
                    ORDER BY prospect_id
                    """
                ).fetchall()
            finally:
                conn.close()

            crm_store.ensure_database(db_path)
            conn = sqlite3.connect(str(db_path))
            try:
                second_rows = conn.execute(
                    """
                    SELECT prospect_id, source_fit_tier, default_send_eligible
                    FROM prospects
                    ORDER BY prospect_id
                    """
                ).fetchall()
            finally:
                conn.close()

            self.assertEqual(first_rows, second_rows)
            normalized = {str(pid): (str(tier or ""), int(sendable or 0)) for pid, tier, sendable in second_rows}
            self.assertEqual(normalized["p_null"], ("recoverable_consultant", 1))
            self.assertEqual(normalized["p_upper"], ("core_consultant", 1))
            self.assertEqual(normalized["p_invalid"], ("recoverable_consultant", 1))
            self.assertEqual(normalized["p_adj"], ("adjacent_contractor", 0))

    def test_ai_assist_audit_table_is_created_idempotently(self):
        with tempfile.TemporaryDirectory() as d:
            db_path = Path(d) / "crm.sqlite"

            crm_store.ensure_database(db_path)
            crm_store.ensure_database(db_path)

            conn = sqlite3.connect(str(db_path))
            try:
                table_row = conn.execute(
                    """
                    SELECT 1
                    FROM sqlite_master
                    WHERE type='table' AND name=?
                    LIMIT 1
                    """,
                    (crm_store.AI_ASSIST_CANDIDATE_TABLE,),
                ).fetchone()
                self.assertIsNotNone(table_row)

                cols = {
                    str(row[1]): {"notnull": int(row[3] or 0), "default": str(row[4] or "")}
                    for row in conn.execute(f"PRAGMA table_info({crm_store.AI_ASSIST_CANDIDATE_TABLE})").fetchall()
                }
                self.assertIn("batch_id", cols)
                self.assertIn("candidate_key", cols)
                self.assertIn("verification_status", cols)
                self.assertEqual(cols["batch_id"]["notnull"], 1)
                self.assertEqual(cols["candidate_key"]["notnull"], 1)
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
