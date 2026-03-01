import csv
import io
import os
import sqlite3
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
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

    def test_dump_signals_all_outreach_groups_by_state_and_territory(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            db_path = tmp / "osha.sqlite"
            sqlite3.connect(str(db_path)).close()

            def _fake_get_leads_for_period(*, states, territory_code=None, **kwargs):
                if territory_code == "TX_TRI":
                    return (
                        [
                            {
                                "activity_nr": "9003",
                                "establishment_name": "Trial Territory Co",
                                "site_city": "Houston",
                                "site_state": "TX",
                                "site_zip": "77001",
                                "naics": "236220",
                                "naics_desc": "Commercial Building",
                                "inspection_type": "Inspection",
                                "scope": "Partial",
                                "case_status": "OPEN",
                                "date_opened": "2026-02-20",
                                "first_seen_at": "2026-02-20T10:00:00Z",
                            }
                        ],
                        [],
                        {},
                    )
                if list(states or []) == ["TX"]:
                    return (
                        [
                            {
                                "activity_nr": "9001",
                                "establishment_name": "Texas Co",
                                "site_city": "Austin",
                                "site_state": "TX",
                                "site_zip": "78701",
                                "naics": "236220",
                                "naics_desc": "Commercial Building",
                                "inspection_type": "Inspection",
                                "scope": "Partial",
                                "case_status": "OPEN",
                                "date_opened": "2026-02-20",
                                "first_seen_at": "2026-02-20T10:00:00Z",
                            }
                        ],
                        [],
                        {},
                    )
                if list(states or []) == ["CA"]:
                    return (
                        [
                            {
                                "activity_nr": "9002",
                                "establishment_name": "California Co",
                                "site_city": "Los Angeles",
                                "site_state": "CA",
                                "site_zip": "90001",
                                "naics": "236220",
                                "naics_desc": "Commercial Building",
                                "inspection_type": "Inspection",
                                "scope": "Partial",
                                "case_status": "OPEN",
                                "date_opened": "2026-02-20",
                                "first_seen_at": "2026-02-20T10:00:00Z",
                            }
                        ],
                        [],
                        {},
                    )
                return ([], [], {})

            def _fake_triage(selected, *_args, **_kwargs):
                return [
                    {
                        "activity_nr": str(row.get("activity_nr") or ""),
                        "rules_priority": "MEDIUM",
                        "reasons": ["rules_default"],
                    }
                    for row in list(selected or [])
                ]

            definitions = {"TX_TRI": {"states": ["TX"]}}
            out = io.StringIO()
            env = dict(os.environ)
            env["OUTREACH_STATES"] = "TX,CA"
            with (
                mock.patch.dict(os.environ, env, clear=False),
                mock.patch.object(dump_tool, "load_territory_definitions", return_value=definitions),
                mock.patch.object(dump_tool, "_configured_trial_territory_codes", return_value=["TX_TRI"]),
                mock.patch.object(dump_tool.sde, "get_leads_for_period", side_effect=_fake_get_leads_for_period),
                mock.patch.object(dump_tool.triage_overlay, "triage", side_effect=_fake_triage),
                redirect_stdout(out),
            ):
                with mock.patch(
                    "sys.argv",
                    [
                        "dump_signals_for_review.py",
                        "--all-outreach",
                        "--since",
                        "2026-02-20",
                        "--dry-run",
                        "--db",
                        str(db_path),
                    ],
                ):
                    code = dump_tool.main()
            self.assertEqual(code, 0)
            text = out.getvalue()
            self.assertIn("===== STATE TX =====", text)
            self.assertIn("===== STATE CA =====", text)
            self.assertIn("===== TERRITORY TX_TRI =====", text)
            self.assertIn("SIGNAL 9001", text)
            self.assertIn("SIGNAL 9002", text)
            self.assertIn("SIGNAL 9003", text)
            self.assertNotIn("SIGNAL_REVIEW_OUT=", text)

    def test_dump_signals_requires_territory_without_all_outreach(self):
        err = io.StringIO()
        with redirect_stderr(err), mock.patch("sys.argv", ["dump_signals_for_review.py", "--since", "2026-02-20"]):
            code = dump_tool.main()
        self.assertEqual(code, 2)
        self.assertIn("ERR_SIGNAL_REVIEW_TERRITORY_REQUIRED", err.getvalue())

    def test_dump_signals_for_ai_review_wraps_prompt_and_marks_suppress(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            db_path = tmp / "osha.sqlite"
            sqlite3.connect(str(db_path)).close()

            fake_leads = [
                {
                    "activity_nr": "9101",
                    "establishment_name": "Suppressed Co",
                    "site_city": "Austin",
                    "site_state": "TX",
                    "site_zip": "78701",
                    "naics": "561720",
                    "naics_desc": "Janitorial Services",
                    "inspection_type": "Inspection",
                    "scope": "Partial",
                    "case_status": "CLOSED",
                    "date_opened": "2026-02-20",
                    "first_seen_at": "2026-02-20T10:00:00Z",
                },
                {
                    "activity_nr": "9102",
                    "establishment_name": "Active Co",
                    "site_city": "Austin",
                    "site_state": "TX",
                    "site_zip": "78701",
                    "naics": "236220",
                    "naics_desc": "Commercial Building",
                    "inspection_type": "Complaint",
                    "scope": "Partial",
                    "case_status": "OPEN",
                    "date_opened": "2026-02-20",
                    "first_seen_at": "2026-02-20T10:00:00Z",
                },
            ]
            fake_decisions = [
                {"activity_nr": "9101", "rules_priority": "SUPPRESS", "reasons": ["no_insp_closed"]},
                {"activity_nr": "9102", "rules_priority": "HIGH", "reasons": ["referral_or_complaint"]},
            ]

            out = io.StringIO()
            with (
                mock.patch.object(dump_tool, "load_territory_definitions", return_value={"TX_TRI": {"states": ["TX"]}}),
                mock.patch.object(dump_tool, "resolve_territory_code", return_value="TX_TRI"),
                mock.patch.object(dump_tool.sde, "get_leads_for_period", return_value=(fake_leads, [], {})),
                mock.patch.object(dump_tool.triage_overlay, "triage", return_value=fake_decisions),
                redirect_stdout(out),
                mock.patch(
                    "sys.argv",
                    [
                        "dump_signals_for_review.py",
                        "--territory",
                        "TX_TRI",
                        "--since",
                        "2026-02-20",
                        "--for-ai-review",
                        "--dry-run",
                        "--db",
                        str(db_path),
                    ],
                ),
            ):
                code = dump_tool.main()

            self.assertEqual(code, 0)
            text = out.getvalue()
            self.assertIn("# AI SIGNAL TRIAGE REVIEW", text)
            self.assertIn("# --- END OF SIGNALS ---", text)
            self.assertIn("SIGNAL 9101", text)
            self.assertIn("Rules priority: SUPPRESS", text)
            self.assertIn("SIGNAL 9102", text)
            self.assertIn("# Return CSV now. Headers: activity_nr,ai_priority,ai_reason", text)

    def test_dump_signals_for_ai_review_default_output_path(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            db_path = tmp / "osha.sqlite"
            sqlite3.connect(str(db_path)).close()
            data_root = tmp / "out"

            fake_leads = [
                {
                    "activity_nr": "9201",
                    "establishment_name": "Output Co",
                    "site_city": "Austin",
                    "site_state": "TX",
                    "site_zip": "78701",
                    "naics": "236220",
                    "naics_desc": "Commercial Building",
                    "inspection_type": "Inspection",
                    "scope": "Partial",
                    "case_status": "OPEN",
                    "date_opened": "2026-02-20",
                    "first_seen_at": "2026-02-20T10:00:00Z",
                }
            ]
            fake_decisions = [{"activity_nr": "9201", "rules_priority": "MEDIUM", "reasons": ["rules_default"]}]

            out = io.StringIO()
            with (
                mock.patch.object(dump_tool.scoring_paths, "data_root", return_value=data_root),
                mock.patch.object(dump_tool, "load_territory_definitions", return_value={"TX_TRI": {"states": ["TX"]}}),
                mock.patch.object(dump_tool, "resolve_territory_code", return_value="TX_TRI"),
                mock.patch.object(dump_tool.sde, "get_leads_for_period", return_value=(fake_leads, [], {})),
                mock.patch.object(dump_tool.triage_overlay, "triage", return_value=fake_decisions),
                redirect_stdout(out),
                mock.patch(
                    "sys.argv",
                    [
                        "dump_signals_for_review.py",
                        "--territory",
                        "TX_TRI",
                        "--since",
                        "today",
                        "--for-ai-review",
                        "--db",
                        str(db_path),
                    ],
                ),
            ):
                code = dump_tool.main()

            self.assertEqual(code, 0)
            text = out.getvalue()
            self.assertIn("SIGNAL_REVIEW_OUT=", text)
            self.assertIn("signals_for_ai_review_", text)
            out_file = next((line.split("=", 1)[1].strip() for line in text.splitlines() if line.startswith("SIGNAL_REVIEW_OUT=")), "")
            self.assertTrue(out_file.endswith(".txt"))
            self.assertIn("signals_for_ai_review_", out_file)
            self.assertTrue(Path(out_file).exists())


if __name__ == "__main__":
    unittest.main()
