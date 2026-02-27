import tempfile
import unittest
from pathlib import Path

from outreach import inbox_csv_ingest


class TestInboxCsvIngest(unittest.TestCase):
    def test_apollo_column_mapping_happy_path(self):
        with tempfile.TemporaryDirectory() as d:
            inbox = Path(d)
            (inbox / "apollo.csv").write_text(
                "First Name,Last Name,Email,Company,Title,City,State,Phone,Website,LinkedIn Url,# Employees,Industry\n"
                "Jane,Doe,jane@example.com,Acme,Owner,Austin,TX,555-0100,https://acme.example,https://linkedin.com/in/jane,25,Consulting\n",
                encoding="utf-8",
            )
            result = inbox_csv_ingest.ingest_inbox_csv_files(inbox)
            rows = list(result.get("rows_accepted") or [])
            self.assertEqual(len(rows), 1)
            row = rows[0]
            self.assertEqual(row.get("first_name"), "Jane")
            self.assertEqual(row.get("last_name"), "Doe")
            self.assertEqual(row.get("email"), "jane@example.com")
            self.assertEqual(row.get("company"), "Acme")
            self.assertEqual(row.get("role_or_title"), "Owner")
            self.assertEqual(row.get("employee_count"), "25")
            self.assertEqual(row.get("industry"), "Consulting")

    def test_case_insensitive_header_matching(self):
        with tempfile.TemporaryDirectory() as d:
            inbox = Path(d)
            (inbox / "apollo.csv").write_text(
                " first NAME , LAST name , EMAIL , company , TITLE \n"
                "Ava,Smith,ava@example.com,Bravo,EHS Director\n",
                encoding="utf-8",
            )
            result = inbox_csv_ingest.ingest_inbox_csv_files(inbox)
            rows = list(result.get("rows_accepted") or [])
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0].get("first_name"), "Ava")
            self.assertEqual(rows[0].get("last_name"), "Smith")
            self.assertEqual(rows[0].get("role_or_title"), "EHS Director")

    def test_missing_email_skipped(self):
        with tempfile.TemporaryDirectory() as d:
            inbox = Path(d)
            (inbox / "apollo.csv").write_text(
                "First Name,Last Name,Email,Company\n"
                "Jane,Doe,,Acme\n",
                encoding="utf-8",
            )
            result = inbox_csv_ingest.ingest_inbox_csv_files(inbox)
            self.assertEqual(int(result.get("rows_skipped_no_email") or 0), 1)
            self.assertEqual(len(list(result.get("rows_accepted") or [])), 0)

    def test_duplicate_email_within_batch_skipped(self):
        with tempfile.TemporaryDirectory() as d:
            inbox = Path(d)
            (inbox / "apollo.csv").write_text(
                "Email,Company\n"
                "dup@example.com,Acme\n"
                "DUP@example.com,Acme Two\n",
                encoding="utf-8",
            )
            result = inbox_csv_ingest.ingest_inbox_csv_files(inbox)
            self.assertEqual(int(result.get("rows_skipped_dupe") or 0), 1)
            self.assertEqual(len(list(result.get("rows_accepted") or [])), 1)

    def test_unknown_columns_ignored(self):
        with tempfile.TemporaryDirectory() as d:
            inbox = Path(d)
            (inbox / "apollo.csv").write_text(
                "Email,Unknown Column,Another One\n"
                "ok@example.com,abc,xyz\n",
                encoding="utf-8",
            )
            result = inbox_csv_ingest.ingest_inbox_csv_files(inbox)
            rows = list(result.get("rows_accepted") or [])
            self.assertEqual(len(rows), 1)
            row = rows[0]
            self.assertEqual(row.get("email"), "ok@example.com")
            self.assertEqual(row.get("company"), "")
            self.assertNotIn("Unknown Column", row)

    def test_dry_run_move_does_not_move_files(self):
        with tempfile.TemporaryDirectory() as d:
            inbox = Path(d)
            src = inbox / "apollo.csv"
            src.write_text("Email\nok@example.com\n", encoding="utf-8")
            moved = inbox_csv_ingest.move_processed_files(inbox, [src], dry_run=True)
            self.assertEqual(moved, [])
            self.assertTrue(src.exists())


if __name__ == "__main__":
    unittest.main()
