import csv
import io
import sqlite3
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest import mock

from tools import dump_signals_for_review as dump_tool
from tools import import_ai_triage as import_tool


class TestSignalReviewTools(unittest.TestCase):
    def test_dump_signals_print_config_and_dry_run(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            db_path = tmp / "osha.sqlite"
            sqlite3.connect(str(db_path)).close()

            fake_leads = [
                {
                    "activity_nr": "1001",
                    "establishment_name": "Keep Co",
                    "site_city": "Austin",
                    "site_state": "TX",
                    "site_zip": "78701",
                    "naics": "237110",
                    "naics_desc": "Water and Sewer",
                    "inspection_type": "Inspection",
                    "scope": "Partial",
                    "case_status": "OPEN",
                    "date_opened": "2026-02-20",
                    "first_seen_at": "2026-02-20T10:00:00Z",
                    "lead_score": 7,
                }
            ]
            fake_decisions = [
                {
                    "activity_nr": "1001",
                    "rules_priority": "HIGH",
                    "reasons": ["multi_employer_site"],
                }
            ]

            out = io.StringIO()
            with mock.patch.object(dump_tool, "load_territory_definitions", return_value={"TX_TRI": {"states": ["TX"]}}), mock.patch.object(
                dump_tool, "resolve_territory_code", return_value="TX_TRI"
            ), mock.patch.object(
                dump_tool.sde, "get_leads_for_period", return_value=(fake_leads, [], {})
            ), mock.patch.object(
                dump_tool.triage_overlay, "triage", return_value=fake_decisions
            ), redirect_stdout(out):
                with mock.patch("sys.argv", ["dump_signals_for_review.py", "--territory", "TX_TRI", "--since", "2026-02-20", "--dry-run", "--db", str(db_path)]):
                    code = dump_tool.main()
            self.assertEqual(code, 0)
            text = out.getvalue()
            self.assertIn("SIGNAL 1001", text)
            self.assertIn("Rules priority: HIGH", text)
            self.assertNotIn("SIGNAL_REVIEW_OUT=", text)

    def test_import_ai_triage_raise_only_dry_run(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            db_path = tmp / "osha.sqlite"
            input_csv = tmp / "ai_review.csv"
            with open(input_csv, "w", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=["activity_nr", "ai_priority", "ai_reason"])
                writer.writeheader()
                writer.writerow({"activity_nr": "m1", "ai_priority": "LOW", "ai_reason": "not a raise"})
                writer.writerow({"activity_nr": "m2", "ai_priority": "MEDIUM", "ai_reason": "raise one tier"})

            conn = sqlite3.connect(str(db_path))
            conn.execute(
                """
                CREATE TABLE inspections(
                    activity_nr TEXT,
                    establishment_name TEXT,
                    site_city TEXT,
                    site_state TEXT,
                    site_zip TEXT,
                    inspection_type TEXT,
                    scope TEXT,
                    case_status TEXT,
                    naics TEXT,
                    naics_desc TEXT,
                    sic TEXT,
                    emphasis TEXT,
                    violations_count INTEGER,
                    serious_violations INTEGER,
                    willful_violations INTEGER,
                    repeat_violations INTEGER,
                    date_opened TEXT,
                    first_seen_at TEXT,
                    lead_score INTEGER,
                    mail_state TEXT
                )
                """
            )
            conn.execute(
                "INSERT INTO inspections VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    "m1",
                    "Alpha",
                    "Austin",
                    "TX",
                    "78701",
                    "Inspection",
                    "Partial",
                    "OPEN",
                    "236220",
                    "Construction",
                    "",
                    "",
                    0,
                    None,
                    None,
                    None,
                    "2026-02-20",
                    "2026-02-20T00:00:00Z",
                    6,
                    "TX",
                ),
            )
            conn.execute(
                "INSERT INTO inspections VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    "m2",
                    "Beta",
                    "Austin",
                    "TX",
                    "78701",
                    "Inspection",
                    "Partial",
                    "OPEN",
                    "236220",
                    "Construction",
                    "",
                    "",
                    0,
                    None,
                    None,
                    None,
                    "2026-02-20",
                    "2026-02-20T00:00:00Z",
                    3,
                    "TX",
                ),
            )
            conn.commit()
            conn.close()

            out = io.StringIO()
            with redirect_stdout(out), mock.patch("sys.argv", ["import_ai_triage.py", "--input", str(input_csv), "--db", str(db_path), "--dry-run"]):
                code = import_tool.main()
            self.assertEqual(code, 0)
            text = out.getvalue()
            self.assertIn("IMPORT_AI_TRIAGE total=2 accepted=1 rejected_lower=1", text)

    def test_import_ai_triage_print_config(self):
        out = io.StringIO()
        with redirect_stdout(out), mock.patch("sys.argv", ["import_ai_triage.py", "--print-config"]):
            code = import_tool.main()
        self.assertEqual(code, 0)
        text = out.getvalue()
        self.assertIn("ai_cache=", text)
        self.assertIn("prompt_hash=", text)


if __name__ == "__main__":
    unittest.main()
