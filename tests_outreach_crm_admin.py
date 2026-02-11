import csv
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


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


if __name__ == "__main__":
    unittest.main()
