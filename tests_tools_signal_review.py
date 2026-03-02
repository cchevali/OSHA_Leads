import csv
import io
import json
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
    @staticmethod
    def _create_inspections_table(conn: sqlite3.Connection) -> None:
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

    @staticmethod
    def _insert_inspection(
        conn: sqlite3.Connection,
        *,
        activity_nr: str,
        lead_score: int,
        case_status: str = "OPEN",
        scope: str = "Partial",
    ) -> None:
        conn.execute(
            "INSERT INTO inspections VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                activity_nr,
                f"Company {activity_nr}",
                "Austin",
                "TX",
                "78701",
                "Inspection",
                scope,
                case_status,
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
                int(lead_score),
                "TX",
            ),
        )

    @staticmethod
    def _parse_import_counts(text: str) -> dict[str, int]:
        line = next((ln for ln in text.splitlines() if ln.startswith("IMPORT_AI_TRIAGE ")), "")
        out: dict[str, int] = {}
        for part in line.split():
            if "=" not in part:
                continue
            key, value = part.split("=", 1)
            if key == "IMPORT_AI_TRIAGE":
                continue
            try:
                out[key] = int(value)
            except Exception:
                pass
        return out

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
            self.assertIn("AI_REVIEW_DUMP_OUTPUT_DIR=", text)
            self.assertIn("AI_REVIEW_DUMP_OUTPUT_PATH=", text)
            self.assertIn("SIGNAL 1001", text)
            self.assertIn("Rules priority: HIGH", text)
            self.assertNotIn("SIGNAL_REVIEW_OUT=", text)

    def test_import_ai_triage_accepts_lower_high_to_low(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            db_path = tmp / "osha.sqlite"
            input_csv = tmp / "ai_review.csv"
            with open(input_csv, "w", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=["activity_nr", "ai_priority", "ai_reason"])
                writer.writeheader()
                writer.writerow({"activity_nr": "m1", "ai_priority": "LOW", "ai_reason": "large enterprise context"})

            conn = sqlite3.connect(str(db_path))
            self._create_inspections_table(conn)
            self._insert_inspection(conn, activity_nr="m1", lead_score=10)
            conn.commit()
            conn.close()

            out = io.StringIO()
            with redirect_stdout(out), mock.patch("sys.argv", ["import_ai_triage.py", "--input", str(input_csv), "--db", str(db_path), "--dry-run"]):
                code = import_tool.main()
            self.assertEqual(code, 0)
            counts = self._parse_import_counts(out.getvalue())
            self.assertEqual(counts.get("total"), 1)
            self.assertEqual(counts.get("accepted"), 1)
            self.assertEqual(counts.get("lowered"), 1)
            self.assertEqual(counts.get("rejected_invalid"), 0)
            self.assertNotIn("rejected_lower", out.getvalue())

    def test_import_ai_triage_accepts_raise_low_to_high(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            db_path = tmp / "osha.sqlite"
            input_csv = tmp / "ai_review.csv"
            with open(input_csv, "w", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=["activity_nr", "ai_priority", "ai_reason"])
                writer.writeheader()
                writer.writerow({"activity_nr": "m2", "ai_priority": "HIGH", "ai_reason": "urgent complaint pattern"})

            conn = sqlite3.connect(str(db_path))
            self._create_inspections_table(conn)
            self._insert_inspection(conn, activity_nr="m2", lead_score=0)
            conn.commit()
            conn.close()

            out = io.StringIO()
            with redirect_stdout(out), mock.patch("sys.argv", ["import_ai_triage.py", "--input", str(input_csv), "--db", str(db_path), "--dry-run"]):
                code = import_tool.main()
            self.assertEqual(code, 0)
            counts = self._parse_import_counts(out.getvalue())
            self.assertEqual(counts.get("total"), 1)
            self.assertEqual(counts.get("accepted"), 1)
            self.assertEqual(counts.get("raised"), 1)

    def test_import_ai_triage_rejects_suppressed_signal(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            db_path = tmp / "osha.sqlite"
            input_csv = tmp / "ai_review.csv"
            with open(input_csv, "w", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=["activity_nr", "ai_priority", "ai_reason"])
                writer.writeheader()
                writer.writerow({"activity_nr": "s1", "ai_priority": "HIGH", "ai_reason": "should be blocked"})

            conn = sqlite3.connect(str(db_path))
            self._create_inspections_table(conn)
            self._insert_inspection(conn, activity_nr="s1", lead_score=6, case_status="CLOSED", scope="No Insp")
            conn.commit()
            conn.close()

            out = io.StringIO()
            with redirect_stdout(out), mock.patch("sys.argv", ["import_ai_triage.py", "--input", str(input_csv), "--db", str(db_path), "--dry-run"]):
                code = import_tool.main()
            self.assertEqual(code, 0)
            counts = self._parse_import_counts(out.getvalue())
            self.assertEqual(counts.get("total"), 1)
            self.assertEqual(counts.get("accepted"), 0)
            self.assertEqual(counts.get("rejected_suppress"), 1)

    def test_import_ai_triage_rejects_invalid_priority(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            db_path = tmp / "osha.sqlite"
            input_csv = tmp / "ai_review.csv"
            with open(input_csv, "w", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=["activity_nr", "ai_priority", "ai_reason"])
                writer.writeheader()
                writer.writerow({"activity_nr": "m3", "ai_priority": "URGENT", "ai_reason": "invalid priority"})

            conn = sqlite3.connect(str(db_path))
            self._create_inspections_table(conn)
            self._insert_inspection(conn, activity_nr="m3", lead_score=6)
            conn.commit()
            conn.close()

            out = io.StringIO()
            with redirect_stdout(out), mock.patch("sys.argv", ["import_ai_triage.py", "--input", str(input_csv), "--db", str(db_path), "--dry-run"]):
                code = import_tool.main()
            self.assertEqual(code, 0)
            counts = self._parse_import_counts(out.getvalue())
            self.assertEqual(counts.get("total"), 1)
            self.assertEqual(counts.get("accepted"), 0)
            self.assertEqual(counts.get("rejected_invalid"), 1)

    def test_import_ai_triage_count_reconciliation(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            db_path = tmp / "osha.sqlite"
            input_csv = tmp / "ai_review.csv"
            with open(input_csv, "w", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=["activity_nr", "ai_priority", "ai_reason"])
                writer.writeheader()
                writer.writerow({"activity_nr": "a_high", "ai_priority": "LOW", "ai_reason": "lower"})
                writer.writerow({"activity_nr": "a_low", "ai_priority": "HIGH", "ai_reason": "raise"})
                writer.writerow({"activity_nr": "a_mid", "ai_priority": "MEDIUM", "ai_reason": "same"})
                writer.writerow({"activity_nr": "a_sup", "ai_priority": "HIGH", "ai_reason": "should reject suppress"})
                writer.writerow({"activity_nr": "missing", "ai_priority": "HIGH", "ai_reason": "missing activity"})
                writer.writerow({"activity_nr": "a_bad", "ai_priority": "BLAH", "ai_reason": "bad value"})

            conn = sqlite3.connect(str(db_path))
            self._create_inspections_table(conn)
            self._insert_inspection(conn, activity_nr="a_high", lead_score=10)
            self._insert_inspection(conn, activity_nr="a_low", lead_score=0)
            self._insert_inspection(conn, activity_nr="a_mid", lead_score=6)
            self._insert_inspection(conn, activity_nr="a_sup", lead_score=6, case_status="CLOSED", scope="No Insp")
            self._insert_inspection(conn, activity_nr="a_bad", lead_score=6)
            conn.commit()
            conn.close()

            out = io.StringIO()
            with redirect_stdout(out), mock.patch("sys.argv", ["import_ai_triage.py", "--input", str(input_csv), "--db", str(db_path), "--dry-run"]):
                code = import_tool.main()
            self.assertEqual(code, 0)
            counts = self._parse_import_counts(out.getvalue())
            self.assertEqual(counts.get("total"), 6)
            self.assertEqual(
                counts.get("raised", 0)
                + counts.get("lowered", 0)
                + counts.get("unchanged", 0)
                + counts.get("rejected_suppress", 0)
                + counts.get("rejected_invalid", 0),
                counts.get("total", 0),
            )

    def test_import_ai_triage_parses_quoted_csv_fields(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            db_path = tmp / "osha.sqlite"
            input_csv = tmp / "ai_review.csv"
            with open(input_csv, "w", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=["activity_nr", "ai_priority", "ai_reason"])
                writer.writeheader()
                writer.writerow({"activity_nr": "q1", "ai_priority": "MEDIUM", "ai_reason": "name says industrial, still moderate"})

            conn = sqlite3.connect(str(db_path))
            self._create_inspections_table(conn)
            self._insert_inspection(conn, activity_nr="q1", lead_score=6)
            conn.commit()
            conn.close()

            out = io.StringIO()
            with redirect_stdout(out), mock.patch("sys.argv", ["import_ai_triage.py", "--input", str(input_csv), "--db", str(db_path), "--dry-run"]):
                code = import_tool.main()
            self.assertEqual(code, 0)
            counts = self._parse_import_counts(out.getvalue())
            self.assertEqual(counts.get("total"), 1)
            self.assertEqual(counts.get("accepted"), 1)
            self.assertEqual(counts.get("unchanged"), 1)
            self.assertEqual(counts.get("rejected_invalid"), 0)

    def test_import_ai_triage_duplicate_import_upserts(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            db_path = tmp / "osha.sqlite"
            data_dir = tmp / "data"
            input_one = tmp / "ai_review_one.csv"
            input_two = tmp / "ai_review_two.csv"
            cache_path = data_dir / "scoring" / "ai_triage_cache.sqlite"

            with open(input_one, "w", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=["activity_nr", "ai_priority", "ai_reason"])
                writer.writeheader()
                writer.writerow({"activity_nr": "dup1", "ai_priority": "HIGH", "ai_reason": "first import"})
            with open(input_two, "w", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=["activity_nr", "ai_priority", "ai_reason"])
                writer.writeheader()
                writer.writerow({"activity_nr": "dup1", "ai_priority": "LOW", "ai_reason": "second import overwrites"})

            conn = sqlite3.connect(str(db_path))
            self._create_inspections_table(conn)
            self._insert_inspection(conn, activity_nr="dup1", lead_score=6)
            conn.commit()
            conn.close()

            env = dict(os.environ)
            env["DATA_DIR"] = str(data_dir)
            with mock.patch.dict(os.environ, env, clear=False):
                code1 = import_tool.main(["--input", str(input_one), "--db", str(db_path)])
                code2 = import_tool.main(["--input", str(input_two), "--db", str(db_path)])
            self.assertEqual(code1, 0)
            self.assertEqual(code2, 0)
            self.assertTrue(cache_path.exists())

            cache = sqlite3.connect(str(cache_path))
            try:
                row_count = int(
                    cache.execute("SELECT COUNT(*) FROM ai_triage_cache WHERE item_key = ?", ("dup1",)).fetchone()[0] or 0
                )
                self.assertEqual(row_count, 1)
                payload_json = cache.execute(
                    "SELECT response_json FROM ai_triage_cache WHERE item_key = ? LIMIT 1", ("dup1",)
                ).fetchone()[0]
            finally:
                cache.close()
            payload = json.loads(str(payload_json or "{}"))
            self.assertEqual(payload.get("reason"), "second import overwrites")
            self.assertEqual(payload.get("priority"), "LOW")

    def test_import_ai_triage_print_config(self):
        out = io.StringIO()
        with redirect_stdout(out), mock.patch("sys.argv", ["import_ai_triage.py", "--print-config"]):
            code = import_tool.main()
        self.assertEqual(code, 0)
        text = out.getvalue()
        self.assertIn("ai_cache=", text)
        self.assertIn("prompt_hash=", text)
        self.assertIn("ai_cache_rows=", text)
        self.assertIn("ai_cache_rows_for_prompt=", text)

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
            self.assertIn("# MICROFLOWOPS — NIGHTLY SIGNAL TRIAGE REVIEW", text)
            self.assertIn("FULL AUTHORITY to raise or lower", text)
            self.assertIn("# --- END OF SIGNALS ---", text)
            self.assertNotIn("SIGNAL 9101", text)
            self.assertIn("SIGNAL 9102", text)
            self.assertIn("AI_REVIEW_DUMP_MATCHED_TOTAL=1", text)
            self.assertIn("# Return ONLY: activity_nr,ai_priority,ai_reason", text)
            self.assertIn("Do not use commas inside the ai_reason field", text)
            self.assertNotIn("You may only RAISE", text)
            self.assertNotIn("Never lower", text)

    def test_dump_signals_for_ai_review_empty_has_header_footer_only(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            db_path = tmp / "osha.sqlite"
            sqlite3.connect(str(db_path)).close()

            out = io.StringIO()
            with (
                mock.patch.object(dump_tool, "load_territory_definitions", return_value={"TX_TRI": {"states": ["TX"]}}),
                mock.patch.object(dump_tool, "resolve_territory_code", return_value="TX_TRI"),
                mock.patch.object(dump_tool.sde, "get_leads_for_period", return_value=([], [], {})),
                mock.patch.object(dump_tool.triage_overlay, "triage", return_value=[]),
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
                        "--dry-run",
                        "--db",
                        str(db_path),
                    ],
                ),
            ):
                code = dump_tool.main()
            self.assertEqual(code, 0)
            text = out.getvalue()
            self.assertIn("# MICROFLOWOPS — NIGHTLY SIGNAL TRIAGE REVIEW", text)
            self.assertIn("# --- END OF SIGNALS ---", text)
            self.assertNotIn("\nSIGNAL ", text)
            self.assertNotIn("NO_SIGNALS_FOR_REVIEW", text)
            self.assertIn("AI_REVIEW_DUMP_MATCHED_TOTAL=0", text)
            self.assertIn("WARN_AI_REVIEW_DUMP_EMPTY=1 reason=NO_MATCHES", text)
            self.assertIn("AI_REVIEW_DUMP_MAX_FIRST_SEEN=", text)
            self.assertIn("AI_REVIEW_DUMP_MAX_DATE_OPENED=", text)

    def test_dump_signals_for_ai_review_default_output_path_without_data_dir(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            db_path = tmp / "osha.sqlite"
            sqlite3.connect(str(db_path)).close()

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
            env = dict(os.environ)
            env["DATA_DIR"] = ""
            with (
                mock.patch.dict(os.environ, env, clear=False),
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
            expected_dir = str((dump_tool.REPO_ROOT / "out" / "audits").resolve(strict=False))
            self.assertIn(f"AI_REVIEW_DUMP_OUTPUT_DIR={expected_dir}", text)
            self.assertIn("AI_REVIEW_DUMP_OUTPUT_PATH=", text)

    def test_dump_signals_for_ai_review_output_path_uses_data_dir_when_set(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            db_path = tmp / "osha.sqlite"
            sqlite3.connect(str(db_path)).close()
            data_dir = tmp / "runtime_data"

            fake_leads = [
                {
                    "activity_nr": "9202",
                    "establishment_name": "Output DataDir Co",
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
            fake_decisions = [{"activity_nr": "9202", "rules_priority": "MEDIUM", "reasons": ["rules_default"]}]

            out = io.StringIO()
            env = dict(os.environ)
            env["DATA_DIR"] = str(data_dir)
            with (
                mock.patch.dict(os.environ, env, clear=False),
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
                        "--dry-run",
                        "--db",
                        str(db_path),
                    ],
                ),
            ):
                code = dump_tool.main()

            self.assertEqual(code, 0)
            text = out.getvalue()
            expected_dir = str((data_dir / "audits").resolve(strict=False))
            self.assertIn(f"AI_REVIEW_DUMP_OUTPUT_DIR={expected_dir}", text)
            self.assertIn("AI_REVIEW_DUMP_OUTPUT_PATH=", text)

    def test_dump_signals_print_config_is_no_write(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            db_path = tmp / "osha.sqlite"
            sqlite3.connect(str(db_path)).close()
            out_dir = tmp / "audits_output"

            out = io.StringIO()
            env = dict(os.environ)
            env.pop("DATA_DIR", None)
            env.pop("MFO_DATA_DIR_EFFECTIVE", None)
            env.pop("MFO_DATA_DIR_SOURCE", None)
            with (
                mock.patch.dict(os.environ, env, clear=True),
                mock.patch.object(dump_tool, "load_territory_definitions", return_value={"TX_TRI": {"states": ["TX"]}}),
                mock.patch.object(dump_tool, "resolve_territory_code", return_value="TX_TRI"),
                redirect_stdout(out),
                mock.patch(
                    "sys.argv",
                    [
                        "dump_signals_for_review.py",
                        "--territory",
                        "TX_TRI",
                        "--for-ai-review",
                        "--since",
                        "2026-02-20",
                        "--print-config",
                        "--output-dir",
                        str(out_dir),
                        "--db",
                        str(db_path),
                    ],
                ),
            ):
                code = dump_tool.main()

            self.assertEqual(code, 0)
            text = out.getvalue()
            self.assertIn("AI_REVIEW_DUMP_DATA_DIR=", text)
            self.assertIn("AI_REVIEW_DUMP_DATA_DIR_SOURCE=default", text)
            self.assertIn("AI_REVIEW_DUMP_OUTPUT_DIR=", text)
            self.assertIn("AI_REVIEW_DUMP_OUTPUT_PATH=", text)
            self.assertIn("AI_REVIEW_DUMP_SINCE=2026-02-20", text)
            self.assertIn("AI_REVIEW_DUMP_UNTIL=", text)
            self.assertIn("AI_REVIEW_DUMP_STATES=TX", text)
            self.assertIn("AI_REVIEW_DUMP_TERRITORIES=TX_TRI", text)
            self.assertFalse(out_dir.exists())

    def test_dump_signals_direct_run_invalid_data_dir_warns_and_falls_back(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            db_path = tmp / "osha.sqlite"
            sqlite3.connect(str(db_path)).close()

            out = io.StringIO()
            env = dict(os.environ)
            env["DATA_DIR"] = "out"
            env.pop("MFO_DATA_DIR_EFFECTIVE", None)
            env.pop("MFO_DATA_DIR_SOURCE", None)
            with (
                mock.patch.dict(os.environ, env, clear=True),
                mock.patch.object(dump_tool, "load_territory_definitions", return_value={"TX_TRI": {"states": ["TX"]}}),
                mock.patch.object(dump_tool, "resolve_territory_code", return_value="TX_TRI"),
                redirect_stdout(out),
                mock.patch(
                    "sys.argv",
                    [
                        "dump_signals_for_review.py",
                        "--territory",
                        "TX_TRI",
                        "--for-ai-review",
                        "--since",
                        "2026-02-20",
                        "--print-config",
                        "--db",
                        str(db_path),
                    ],
                ),
            ):
                code = dump_tool.main()

            self.assertEqual(code, 0)
            text = out.getvalue()
            expected_data_dir = str((dump_tool.REPO_ROOT / "out").resolve(strict=False))
            expected_audits = str((dump_tool.REPO_ROOT / "out" / "audits").resolve(strict=False))
            self.assertIn("WARN_DATA_DIR_NOT_ABSOLUTE=1 value=out behavior=UNSET_FOR_CHILD", text)
            self.assertIn(f"AI_REVIEW_DUMP_DATA_DIR={expected_data_dir}", text)
            self.assertIn("AI_REVIEW_DUMP_DATA_DIR_SOURCE=default", text)
            self.assertIn(f"AI_REVIEW_DUMP_OUTPUT_DIR={expected_audits}", text)

    def test_dump_signals_include_suppressed_adds_skip_section(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            db_path = tmp / "osha.sqlite"
            sqlite3.connect(str(db_path)).close()

            fake_leads = [
                {
                    "activity_nr": "9301",
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
                    "activity_nr": "9302",
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
                {"activity_nr": "9301", "rules_priority": "SUPPRESS", "reasons": ["no_insp_closed"]},
                {"activity_nr": "9302", "rules_priority": "HIGH", "reasons": ["referral_or_complaint"]},
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
                        "--include-suppressed",
                        "--dry-run",
                        "--db",
                        str(db_path),
                    ],
                ),
            ):
                code = dump_tool.main()

            self.assertEqual(code, 0)
            text = out.getvalue()
            self.assertIn("SUPPRESSED (skip)", text)
            self.assertIn("SIGNAL 9301", text)
            self.assertIn("SIGNAL 9302", text)

    def test_dump_signals_output_override_precedence(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            db_path = tmp / "osha.sqlite"
            sqlite3.connect(str(db_path)).close()
            output_dir = tmp / "dir_precedence"
            explicit_path = tmp / "explicit_dir" / "custom_dump.txt"

            fake_leads = [
                {
                    "activity_nr": "9401",
                    "establishment_name": "Precedence Co",
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
            fake_decisions = [{"activity_nr": "9401", "rules_priority": "MEDIUM", "reasons": ["rules_default"]}]

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
                        "--output-dir",
                        str(output_dir),
                        "--output",
                        str(explicit_path),
                        "--db",
                        str(db_path),
                    ],
                ),
            ):
                code = dump_tool.main()

            self.assertEqual(code, 0)
            text = out.getvalue()
            self.assertIn(f"AI_REVIEW_DUMP_OUTPUT_PATH={explicit_path.resolve(strict=False)}", text)
            self.assertTrue(explicit_path.exists())
            self.assertFalse(output_dir.exists())


if __name__ == "__main__":
    unittest.main()
