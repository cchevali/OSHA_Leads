import csv
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent
SCRIPT = REPO_ROOT / "outreach" / "run_outreach_auto.py"

REQUIRED_COLS = [
    "prospect_id",
    "first_name",
    "last_name",
    "firm",
    "title",
    "email",
    "state",
    "city",
    "territory_code",
    "source",
    "notes",
]


def _write_prospects(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = [
        {
            "prospect_id": "p1",
            "first_name": "A",
            "last_name": "One",
            "firm": "Co",
            "title": "Ops",
            "email": "a@example.com",
            "state": "TX",
            "city": "Austin",
            "territory_code": "X",
            "source": "s",
            "notes": "",
        }
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=REQUIRED_COLS)
        w.writeheader()
        for r in rows:
            w.writerow(r)


def _write_suppression(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["email"])
        w.writeheader()


class TestOutreachRunAuto(unittest.TestCase):
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

    def test_dry_run_passes_and_writes_no_outputs(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            data_dir = tmp / "data"
            out_root = tmp / "outreach_out"
            prospects = tmp / "prospects.csv"
            _write_prospects(prospects)
            _write_suppression(data_dir / "suppression.csv")

            env = {
                "DATA_DIR": str(data_dir),
                "OUTREACH_STATES": "TX,CA",
                "OUTREACH_DAILY_LIMIT": "10",
                "OUTREACH_PROSPECTS_PATH": str(prospects),
                "OUTREACH_OUTPUT_ROOT": str(out_root),
                "OSHA_SMOKE_TO": "allow@example.com",
            }
            p = self._run(["--dry-run"], env)
            self.assertEqual(p.returncode, 0, msg=p.stderr + "\n" + p.stdout)
            self.assertIn("PASS_AUTO_DRY_RUN", (p.stdout or "") + (p.stderr or ""))
            self.assertFalse(out_root.exists())

    def test_to_mismatch_fails(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            data_dir = tmp / "data"
            prospects = tmp / "prospects.csv"
            _write_prospects(prospects)
            _write_suppression(data_dir / "suppression.csv")

            env = {
                "DATA_DIR": str(data_dir),
                "OUTREACH_STATES": "TX",
                "OUTREACH_PROSPECTS_PATH": str(prospects),
                "OUTREACH_OUTPUT_ROOT": str(tmp / "outreach_out"),
                "OSHA_SMOKE_TO": "allow@example.com",
            }
            p = self._run(["--to", "wrong@example.com"], env)
            self.assertNotEqual(p.returncode, 0)
            self.assertIn("ERR_AUTO_SUMMARY_TO_MISMATCH", (p.stderr or "") + (p.stdout or ""))


if __name__ == "__main__":
    unittest.main()
