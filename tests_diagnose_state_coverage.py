import io
import sqlite3
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path

from tools import diagnose_state_coverage as tool


def _seed_inspections(path: Path, rows: list[dict]) -> None:
    conn = sqlite3.connect(str(path))
    try:
        conn.execute(
            """
            CREATE TABLE inspections (
                activity_nr TEXT,
                site_state TEXT,
                date_opened TEXT,
                first_seen_at TEXT,
                last_seen_at TEXT
            )
            """
        )
        for row in rows:
            conn.execute(
                """
                INSERT INTO inspections(activity_nr, site_state, date_opened, first_seen_at, last_seen_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    row.get("activity_nr", ""),
                    row.get("site_state", ""),
                    row.get("date_opened", ""),
                    row.get("first_seen_at", ""),
                    row.get("last_seen_at", ""),
                ),
            )
        conn.commit()
    finally:
        conn.close()


class TestDiagnoseStateCoverage(unittest.TestCase):
    def test_print_config_emits_tokens(self):
        out = io.StringIO()
        with redirect_stdout(out):
            rc = tool.main(["--print-config", "--states", "TX,WA"])
        text = out.getvalue()
        self.assertEqual(rc, 0, msg=text)
        self.assertIn("STATE_COVERAGE_STATES=TX,WA", text)
        self.assertIn("STATE_COVERAGE_SINCE_DAYS=", text)
        self.assertIn("PASS_STATE_COVERAGE_COMPLETE status=PRINT_CONFIG", text)

    def test_state_coverage_rows_and_activity_shape_sample(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            db_path = tmp / "osha.sqlite"
            _seed_inspections(
                db_path,
                [
                    {
                        "activity_nr": "1234567",
                        "site_state": "TX",
                        "date_opened": "2026-03-01",
                        "first_seen_at": "2026-03-01T12:00:00+00:00",
                        "last_seen_at": "2026-03-01T12:00:00+00:00",
                    },
                    {
                        "activity_nr": "stateplan:WA:CASE-1001",
                        "site_state": "WA",
                        "date_opened": "2026-03-01",
                        "first_seen_at": "2026-03-01T12:00:00+00:00",
                        "last_seen_at": "2026-03-01T12:00:00+00:00",
                    },
                ],
            )
            out = io.StringIO()
            with redirect_stdout(out):
                rc = tool.main(
                    [
                        "--db",
                        str(db_path),
                        "--states",
                        "TX,WA",
                        "--since-days",
                        "365",
                    ]
                )
            text = out.getvalue()
            self.assertEqual(rc, 0, msg=text)
            self.assertIn("STATE_COVERAGE state=TX total=1", text)
            self.assertIn("STATE_COVERAGE state=WA total=1", text)
            self.assertIn("STATE_ACTIVITY_SAMPLE state=WA sample=stateplan:WA:CASE-1001", text)
            self.assertIn("PASS_STATE_COVERAGE_COMPLETE status=OK", text)


if __name__ == "__main__":
    unittest.main()
