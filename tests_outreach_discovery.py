import csv
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent
SCRIPT = REPO_ROOT / "run_prospect_discovery.py"
DISCOVERY_KEYS = [
    "DISCOVERY_INPUT_PATH=",
    "DISCOVERY_CRM_DB=",
    "DISCOVERY_ROWS_READ=",
    "DISCOVERY_PROSPECTS_UPSERTED=",
    "DISCOVERY_SKIPPED_INVALID_EMAIL=",
    "DISCOVERY_SKIPPED_DUPLICATE_EMAIL=",
    "DISCOVERY_COMPLETE status=",
]


def _write_rows(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["prospect_id", "email", "state", "firm", "title"])
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _rows(row_count: int = 2) -> list[dict[str, str]]:
    base = [
        {
            "prospect_id": "disc_1",
            "email": "disc1@example.com",
            "state": "TX",
            "firm": "Discovery One",
            "title": "Owner",
        },
        {
            "prospect_id": "disc_2",
            "email": "disc2@example.com",
            "state": "TX",
            "firm": "Discovery Two",
            "title": "Safety Manager",
        },
        {
            "prospect_id": "disc_3",
            "email": "disc3@example.com",
            "state": "TX",
            "firm": "Discovery Three",
            "title": "Operator",
        },
        {
            "prospect_id": "disc_4",
            "email": "disc4@example.com",
            "state": "TX",
            "firm": "Discovery Four",
            "title": "Operator",
        },
    ]
    return base[: max(0, row_count)]


class TestOutreachDiscovery(unittest.TestCase):
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

    def _assert_discovery_block(self, stdout: str, status: str) -> list[str]:
        lines = [line.strip() for line in (stdout or "").splitlines() if line.strip()]
        self.assertGreaterEqual(len(lines), 7, msg=stdout)
        block = lines[-7:]
        for idx, key in enumerate(DISCOVERY_KEYS):
            self.assertTrue(block[idx].startswith(key), msg="\n".join(lines))
        self.assertEqual(block[-1], f"DISCOVERY_COMPLETE status={status}", msg="\n".join(lines))

        discovery_indexes = [i for i, line in enumerate(lines) if line.startswith("DISCOVERY_")]
        self.assertEqual(discovery_indexes, list(range(len(lines) - 7, len(lines))), msg="\n".join(lines))
        return block

    def test_module_importable_and_main_callable(self):
        from outreach import run_prospect_discovery as discovery

        self.assertTrue(callable(getattr(discovery, "main", None)))

    def test_print_config_shows_resolved_paths(self):
        with tempfile.TemporaryDirectory() as d:
            data_dir = Path(d) / "data"
            p = self._run(["--print-config"], {"DATA_DIR": str(data_dir)})
            self.assertEqual(p.returncode, 0, msg=p.stderr + "\n" + p.stdout)
            out = p.stdout or ""
            self.assertIn("PASS_DISCOVERY_PRINT_CONFIG", out)
            self.assertIn(f"data_dir={data_dir.resolve()}", out)
            self.assertIn(f"crm_db={(data_dir / 'crm.sqlite').resolve()}", out)
            self.assertIn("input_path=(missing)", out)

    def test_no_arg_missing_input_warns_and_exits_zero(self):
        with tempfile.TemporaryDirectory() as d:
            data_dir = Path(d) / "data"
            p = self._run(
                [],
                {
                    "DATA_DIR": str(data_dir),
                    "PROSPECT_DISCOVERY_INPUT": None,
                    "DISCOVERY_INPUT_CSV": None,
                    "DISCOVERY_ALLOW_SAMPLE": None,
                },
            )
            self.assertEqual(p.returncode, 0, msg=p.stderr + "\n" + p.stdout)
            text = (p.stderr or "") + (p.stdout or "")
            self.assertIn("ERR_DISCOVERY_NO_INPUT_SOURCE", text)
            self.assertIn("WARN_DISCOVERY_NO_INPUT", p.stdout or "")
            self.assertIn("attempted=4", p.stdout or "")
            block = self._assert_discovery_block(p.stdout or "", "NO_INPUT")
            self.assertEqual(block[0], "DISCOVERY_INPUT_PATH=NONE")
            self.assertIn(f"DISCOVERY_CRM_DB={(data_dir / 'crm.sqlite').resolve()}", block[1])
            self.assertEqual(block[2], "DISCOVERY_ROWS_READ=0")
            self.assertEqual(block[3], "DISCOVERY_PROSPECTS_UPSERTED=0")
            self.assertEqual(block[4], "DISCOVERY_SKIPPED_INVALID_EMAIL=0")
            self.assertEqual(block[5], "DISCOVERY_SKIPPED_DUPLICATE_EMAIL=0")

    def test_preferred_env_wins_over_legacy_and_fallbacks(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            data_dir = tmp / "data"
            env_csv = tmp / "preferred.csv"
            legacy_csv = tmp / "legacy.csv"
            fallback = data_dir / "prospect_discovery" / "prospects_latest.csv"
            _write_rows(env_csv, _rows(3))
            _write_rows(legacy_csv, _rows(1))
            _write_rows(fallback, _rows(2))

            p = self._run(
                ["--dry-run"],
                {
                    "DATA_DIR": str(data_dir),
                    "PROSPECT_DISCOVERY_INPUT": str(env_csv),
                    "DISCOVERY_INPUT_CSV": str(legacy_csv),
                    "DISCOVERY_ALLOW_SAMPLE": None,
                },
            )
            self.assertEqual(p.returncode, 0, msg=p.stderr + "\n" + p.stdout)
            out = p.stdout or ""
            self.assertIn("PASS_DISCOVERY_DRY_RUN", out)
            self.assertIn(f"input_path={env_csv.resolve()}", out)
            block = self._assert_discovery_block(out, "DRY_RUN")
            self.assertEqual(block[0], f"DISCOVERY_INPUT_PATH={env_csv.resolve()}")
            self.assertEqual(block[2], "DISCOVERY_ROWS_READ=3")
            self.assertEqual(block[3], "DISCOVERY_PROSPECTS_UPSERTED=3")

    def test_nonexistent_preferred_env_falls_through_to_fallback(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            data_dir = tmp / "data"
            fallback = data_dir / "prospect_discovery" / "prospects_latest.csv"
            _write_rows(fallback, _rows(2))

            p = self._run(
                ["--dry-run"],
                {
                    "DATA_DIR": str(data_dir),
                    "PROSPECT_DISCOVERY_INPUT": str(tmp / "missing.csv"),
                    "DISCOVERY_INPUT_CSV": None,
                },
            )
            self.assertEqual(p.returncode, 0, msg=p.stderr + "\n" + p.stdout)
            out = p.stdout or ""
            self.assertIn(f"input_path={fallback.resolve()}", out)
            block = self._assert_discovery_block(out, "DRY_RUN")
            self.assertEqual(block[0], f"DISCOVERY_INPUT_PATH={fallback.resolve()}")

    def test_fallback_order_is_deterministic(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            data_dir = tmp / "data"
            p1 = data_dir / "prospect_discovery" / "prospects_latest.csv"
            p2 = data_dir / "prospect_discovery" / "prospects.csv"
            p3 = data_dir / "prospects_latest.csv"
            p4 = data_dir / "prospects.csv"
            _write_rows(p1, _rows(1))
            _write_rows(p2, _rows(2))
            _write_rows(p3, _rows(3))
            _write_rows(p4, _rows(4))

            env = {
                "DATA_DIR": str(data_dir),
                "PROSPECT_DISCOVERY_INPUT": None,
                "DISCOVERY_INPUT_CSV": None,
                "DISCOVERY_ALLOW_SAMPLE": None,
            }

            first = self._run(["--dry-run"], env)
            self.assertEqual(first.returncode, 0, msg=first.stderr + "\n" + first.stdout)
            self.assertIn(f"input_path={p1.resolve()}", first.stdout or "")

            p1.unlink()
            second = self._run(["--dry-run"], env)
            self.assertEqual(second.returncode, 0, msg=second.stderr + "\n" + second.stdout)
            self.assertIn(f"input_path={p2.resolve()}", second.stdout or "")

            p2.unlink()
            third = self._run(["--dry-run"], env)
            self.assertEqual(third.returncode, 0, msg=third.stderr + "\n" + third.stdout)
            self.assertIn(f"input_path={p3.resolve()}", third.stdout or "")

            p3.unlink()
            fourth = self._run(["--dry-run"], env)
            self.assertEqual(fourth.returncode, 0, msg=fourth.stderr + "\n" + fourth.stdout)
            self.assertIn(f"input_path={p4.resolve()}", fourth.stdout or "")

    def test_dry_run_reads_rows_without_creating_crm(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            data_dir = tmp / "data"
            input_csv = tmp / "input.csv"
            _write_rows(input_csv, _rows(2))

            p = self._run(["--dry-run", "--input", str(input_csv)], {"DATA_DIR": str(data_dir)})
            self.assertEqual(p.returncode, 0, msg=p.stderr + "\n" + p.stdout)
            out = p.stdout or ""
            self.assertIn("PASS_DISCOVERY_DRY_RUN", out)
            self.assertIn("rows_read=2", out)
            self._assert_discovery_block(out, "DRY_RUN")
            self.assertFalse((data_dir / "crm.sqlite").exists(), msg="dry-run should not create crm.sqlite")

    def test_live_run_seeds_crm_and_emits_legacy_plus_discovery_block(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            data_dir = tmp / "data"
            input_csv = tmp / "input.csv"
            _write_rows(
                input_csv,
                [
                    {
                        "prospect_id": "p1",
                        "email": "ok1@example.com",
                        "state": "TX",
                        "firm": "Firm 1",
                        "title": "Owner",
                    },
                    {
                        "prospect_id": "p2",
                        "email": "not-an-email",
                        "state": "TX",
                        "firm": "Firm 2",
                        "title": "Owner",
                    },
                    {
                        "prospect_id": "p3",
                        "email": "ok1@example.com",
                        "state": "TX",
                        "firm": "Firm 3",
                        "title": "Owner",
                    },
                ],
            )

            p = self._run(["--input", str(input_csv)], {"DATA_DIR": str(data_dir)})
            self.assertEqual(p.returncode, 0, msg=p.stderr + "\n" + p.stdout)
            out = p.stdout or ""
            self.assertIn("PASS_DISCOVERY_UPSERT", out)
            block = self._assert_discovery_block(out, "OK")
            self.assertEqual(block[2], "DISCOVERY_ROWS_READ=3")
            self.assertEqual(block[3], "DISCOVERY_PROSPECTS_UPSERTED=1")
            self.assertEqual(block[4], "DISCOVERY_SKIPPED_INVALID_EMAIL=1")
            self.assertEqual(block[5], "DISCOVERY_SKIPPED_DUPLICATE_EMAIL=1")

            crm_db = data_dir / "crm.sqlite"
            self.assertTrue(crm_db.exists(), msg=f"expected crm db at {crm_db}")
            conn = sqlite3.connect(str(crm_db))
            try:
                row = conn.execute("SELECT COUNT(*) FROM prospects").fetchone()
                self.assertEqual(int(row[0] or 0), 1)
            finally:
                conn.close()

    def test_smoke_no_arg_discovery_populates_crm_via_legacy_env(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            data_dir = tmp / "data"
            input_csv = tmp / "legacy_env_input.csv"
            _write_rows(input_csv, _rows(1))

            p = self._run(
                [],
                {
                    "DATA_DIR": str(data_dir),
                    "PROSPECT_DISCOVERY_INPUT": None,
                    "DISCOVERY_INPUT_CSV": str(input_csv),
                },
            )
            self.assertEqual(p.returncode, 0, msg=p.stderr + "\n" + p.stdout)

            crm_db = data_dir / "crm.sqlite"
            self.assertTrue(crm_db.exists(), msg=f"expected crm db at {crm_db}")
            conn = sqlite3.connect(str(crm_db))
            try:
                count = int(conn.execute("SELECT COUNT(*) FROM prospects").fetchone()[0])
                self.assertGreaterEqual(count, 1)
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
