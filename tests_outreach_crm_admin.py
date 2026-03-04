import csv
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from outreach import crm_store


REPO_ROOT = Path(__file__).resolve().parent
SCRIPT = REPO_ROOT / "outreach" / "crm_admin.py"


def _write_prospects(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "prospect_id",
        "first_name",
        "last_name",
        "firm",
        "title",
        "email",
        "state",
        "city",
        "source",
    ]
    rows = [
        {
            "prospect_id": "p1",
            "first_name": "A",
            "last_name": "One",
            "firm": "Firm",
            "title": "Owner",
            "email": "a@example.com",
            "state": "TX",
            "city": "Austin",
            "source": "test",
        }
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in rows:
            w.writerow(r)


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)


def _seed_stats_rows(db_path: Path) -> None:
    crm_store.ensure_database(db_path)
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute(
            """
            INSERT INTO prospects(
                prospect_id, firm, contact_name, email, title, city, state, website, source,
                score, status, created_at, last_contacted_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "p_aiha_email",
                "Acme Safety",
                "Alice",
                "alice@acme.example",
                "Owner",
                "Austin",
                "TX",
                "https://acme.example",
                "aiha",
                10,
                "new",
                "2026-01-01T00:00:00+00:00",
                None,
            ),
        )
        conn.execute(
            """
            INSERT INTO prospects(
                prospect_id, firm, contact_name, email, title, city, state, website, source,
                score, status, created_at, last_contacted_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "p_aiha_blank_email",
                "Acme Safety",
                "No Email",
                "",
                "Coordinator",
                "Austin",
                "TX",
                "",
                "aiha",
                5,
                "new",
                "2026-01-01T00:00:00+00:00",
                None,
            ),
        )
        conn.execute(
            """
            INSERT INTO prospects(
                prospect_id, firm, contact_name, email, title, city, state, website, source,
                score, status, created_at, last_contacted_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "p_apollo_email",
                "Bravo Safety",
                "Bob",
                "bob@bravo.example",
                "Safety Manager",
                "Dallas",
                "TX",
                "",
                "apollo_export_csv",
                8,
                "new",
                "2026-01-01T00:00:00+00:00",
                None,
            ),
        )
        conn.execute(
            """
            INSERT INTO prospects(
                prospect_id, firm, contact_name, email, title, city, state, website, source,
                score, status, created_at, last_contacted_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "p_blank_source",
                "Charlie Safety",
                "Carol",
                "carol@charlie.example",
                "Consultant",
                "Houston",
                "TX",
                "https://charlie.example",
                "",
                6,
                "new",
                "2026-01-01T00:00:00+00:00",
                None,
            ),
        )
        conn.commit()
    finally:
        conn.close()


class TestOutreachCrmAdmin(unittest.TestCase):
    def _run(self, args: list[str], env_overrides: dict[str, str | None]) -> subprocess.CompletedProcess:
        env = os.environ.copy()
        env["PYTHONPATH"] = str(REPO_ROOT)
        for k, v in env_overrides.items():
            if v is None:
                env.pop(k, None)
            else:
                env[k] = v
        return subprocess.run(
            [sys.executable, str(SCRIPT)] + args,
            cwd=str(REPO_ROOT),
            env=env,
            capture_output=True,
            text=True,
        )

    def test_seed_inserts_rows(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            csv_path = tmp / "prospects.csv"
            data_dir = tmp / "data"
            _write_prospects(csv_path)

            p = self._run(
                ["seed", "--input", str(csv_path), "--no-archive"],
                {"DATA_DIR": str(data_dir)},
            )
            self.assertEqual(p.returncode, 0, msg=p.stderr + "\n" + p.stdout)
            self.assertIn("inserted_count=1", p.stdout)

            crm_db = data_dir / "crm.sqlite"
            self.assertTrue(crm_db.exists())
            conn = sqlite3.connect(str(crm_db))
            try:
                count = int(conn.execute("SELECT COUNT(*) FROM prospects").fetchone()[0])
                self.assertEqual(count, 1)
            finally:
                conn.close()

    def test_mark_trial_started_updates_status_and_trials(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            csv_path = tmp / "prospects.csv"
            data_dir = tmp / "data"
            _write_prospects(csv_path)

            seed = self._run(
                ["seed", "--input", str(csv_path), "--no-archive"],
                {"DATA_DIR": str(data_dir)},
            )
            self.assertEqual(seed.returncode, 0, msg=seed.stderr + "\n" + seed.stdout)

            mark = self._run(
                ["mark", "--prospect-id", "p1", "--event", "trial_started", "--territory-code", "TX_AUTO"],
                {"DATA_DIR": str(data_dir)},
            )
            self.assertEqual(mark.returncode, 0, msg=mark.stderr + "\n" + mark.stdout)
            self.assertIn("PASS_CRM_MARK", mark.stdout)

            conn = sqlite3.connect(str(data_dir / "crm.sqlite"))
            try:
                status = conn.execute("SELECT status FROM prospects WHERE prospect_id = 'p1'").fetchone()[0]
                self.assertEqual(status, "trial_started")
                trials = int(conn.execute("SELECT COUNT(*) FROM trials WHERE prospect_id = 'p1'").fetchone()[0])
                self.assertEqual(trials, 1)
            finally:
                conn.close()

    def test_stats_outputs_fixed_order_and_source_breakdowns(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            data_dir = tmp / "data"
            db_path = data_dir / "crm.sqlite"
            _seed_stats_rows(db_path)

            p = self._run(["stats"], {"DATA_DIR": str(data_dir)})
            self.assertEqual(p.returncode, 0, msg=p.stderr + "\n" + p.stdout)
            lines = [line.strip() for line in (p.stdout or "").splitlines() if line.strip()]
            self.assertGreaterEqual(len(lines), 6, msg=p.stdout)
            self.assertEqual(lines[0], f"CRM_DB_PATH={db_path.resolve()}")
            self.assertEqual(lines[1], "CRM_PROSPECTS_TOTAL=4")
            self.assertEqual(lines[2], "CRM_PROSPECTS_HAS_EMAIL=3")

            by_source = [line for line in lines if line.startswith("CRM_BY_SOURCE ")]
            empty_by_source = [line for line in lines if line.startswith("CRM_EMPTY_EMAIL_BY_SOURCE ")]

            self.assertEqual(
                by_source,
                [
                    "CRM_BY_SOURCE source=(blank) total=1 has_email=1 has_website=1",
                    "CRM_BY_SOURCE source=aiha total=2 has_email=1 has_website=1",
                    "CRM_BY_SOURCE source=apollo_export_csv total=1 has_email=1 has_website=0",
                ],
            )
            self.assertEqual(empty_by_source, ["CRM_EMPTY_EMAIL_BY_SOURCE source=aiha total=1"])

            first_empty_index = lines.index(empty_by_source[0])
            for line in by_source:
                self.assertLess(lines.index(line), first_empty_index)

    def test_stats_missing_db_returns_zero_without_creating_db(self):
        with tempfile.TemporaryDirectory() as d:
            data_dir = Path(d) / "data"
            db_path = data_dir / "crm.sqlite"
            self.assertFalse(db_path.exists())
            p = self._run(["stats"], {"DATA_DIR": str(data_dir)})
            self.assertEqual(p.returncode, 0, msg=p.stderr + "\n" + p.stdout)
            lines = [line.strip() for line in (p.stdout or "").splitlines() if line.strip()]
            self.assertEqual(lines, [f"CRM_DB_PATH={db_path.resolve()}", "CRM_PROSPECTS_TOTAL=0", "CRM_PROSPECTS_HAS_EMAIL=0"])
            self.assertFalse(db_path.exists(), msg="stats must not initialize/write crm.sqlite")

    def test_verify_import_reads_any_column_and_reports_match_rate(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            data_dir = tmp / "data"
            csv_path = tmp / "apollo_export.csv"
            seed_path = tmp / "seed.csv"
            _write_csv(
                csv_path,
                ["name", "notes"],
                [
                    {"name": "A", "notes": "reach me at Match.One@Example.com"},
                    {"name": "B", "notes": "no email here"},
                    {"name": "C", "notes": "secondary <miss@example.com>"},
                    {"name": "D", "notes": "owner match.two@example.com now"},
                ],
            )
            _write_csv(
                seed_path,
                ["prospect_id", "email", "firm", "title", "state", "city", "source"],
                [
                    {
                        "prospect_id": "p1",
                        "email": "match.one@example.com",
                        "firm": "One",
                        "title": "Owner",
                        "state": "TX",
                        "city": "Austin",
                        "source": "seed",
                    },
                    {
                        "prospect_id": "p2",
                        "email": "match.two@example.com",
                        "firm": "Two",
                        "title": "Owner",
                        "state": "TX",
                        "city": "Austin",
                        "source": "seed",
                    },
                ],
            )
            seed = self._run(["seed", "--input", str(seed_path), "--no-archive"], {"DATA_DIR": str(data_dir)})
            self.assertEqual(seed.returncode, 0, msg=seed.stderr + "\n" + seed.stdout)

            p = self._run(["verify-import", "--csv", str(csv_path)], {"DATA_DIR": str(data_dir)})
            self.assertEqual(p.returncode, 0, msg=p.stderr + "\n" + p.stdout)
            lines = [line.strip() for line in (p.stdout or "").splitlines() if line.strip()]
            self.assertEqual(lines[0], "CRM_VERIFY_IMPORT_SAMPLE_SIZE=3")
            self.assertEqual(lines[1], "CRM_VERIFY_IMPORT_MATCHES=2")
            self.assertEqual(lines[2], "CRM_VERIFY_IMPORT_MATCH_RATE=66.67")

    def test_verify_import_caps_sample_at_25(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            data_dir = tmp / "data"
            csv_path = tmp / "apollo_export.csv"
            rows = []
            for i in range(40):
                rows.append({"name": f"Person{i}", "notes": f"contact user{i}@example.com today"})
            _write_csv(csv_path, ["name", "notes"], rows)

            p = self._run(["verify-import", "--csv", str(csv_path)], {"DATA_DIR": str(data_dir)})
            self.assertEqual(p.returncode, 0, msg=p.stderr + "\n" + p.stdout)
            lines = [line.strip() for line in (p.stdout or "").splitlines() if line.strip()]
            self.assertEqual(lines[0], "CRM_VERIFY_IMPORT_SAMPLE_SIZE=25")
            self.assertEqual(lines[1], "CRM_VERIFY_IMPORT_MATCHES=0")
            self.assertEqual(lines[2], "CRM_VERIFY_IMPORT_MATCH_RATE=0.00")
            self.assertFalse((data_dir / "crm.sqlite").exists(), msg="verify-import must not initialize/write crm.sqlite")

    def test_verify_import_missing_csv_returns_err_token(self):
        with tempfile.TemporaryDirectory() as d:
            data_dir = Path(d) / "data"
            missing = Path(d) / "missing.csv"
            p = self._run(["verify-import", "--csv", str(missing)], {"DATA_DIR": str(data_dir)})
            self.assertEqual(p.returncode, 2, msg=p.stderr + "\n" + p.stdout)
            self.assertIn("ERR_CRM_VERIFY_INPUT_MISSING", p.stderr or "")


if __name__ == "__main__":
    unittest.main()
