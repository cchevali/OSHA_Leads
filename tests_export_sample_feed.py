import importlib.util
import json
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parent
SCRIPT = ROOT / "scripts" / "export_sample_feed.py"
CONFIG = ROOT / "web" / "app" / "sample" / "sample_feed_config.json"


def _load_module():
    spec = importlib.util.spec_from_file_location("export_sample_feed", SCRIPT)
    if spec is None or spec.loader is None:
        raise RuntimeError("Failed to load export_sample_feed module")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


def _seed_db(path: Path, rows: list[dict]) -> None:
    conn = sqlite3.connect(str(path))
    try:
        conn.execute(
            """
            CREATE TABLE inspections (
              activity_nr TEXT,
              inspection_type TEXT,
              establishment_name TEXT,
              site_city TEXT,
              site_state TEXT,
              date_opened TEXT,
              first_seen_at TEXT,
              changed_at TEXT,
              last_seen_at TEXT,
              source_url TEXT,
              parse_invalid INTEGER,
              case_status TEXT
            )
            """
        )
        for row in rows:
            conn.execute(
                """
                INSERT INTO inspections (
                  activity_nr, inspection_type, establishment_name, site_city, site_state,
                  date_opened, first_seen_at, changed_at, last_seen_at, source_url,
                  parse_invalid, case_status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row.get("activity_nr"),
                    row.get("inspection_type"),
                    row.get("establishment_name"),
                    row.get("site_city"),
                    row.get("site_state"),
                    row.get("date_opened"),
                    row.get("first_seen_at"),
                    row.get("changed_at"),
                    row.get("last_seen_at"),
                    row.get("source_url"),
                    row.get("parse_invalid", 0),
                    row.get("case_status", "OPEN"),
                ),
            )
        conn.commit()
    finally:
        conn.close()


class TestExportSampleFeed(unittest.TestCase):
    def test_print_config_side_effect_free(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            out_path = tmp / "sample.json"
            p = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    "--db",
                    str(tmp / "missing.sqlite"),
                    "--out",
                    str(out_path),
                    "--print-config",
                ],
                cwd=str(ROOT),
                capture_output=True,
                text=True,
            )
            self.assertEqual(p.returncode, 0, msg=p.stderr + "\n" + p.stdout)
            self.assertIn("PASS_SAMPLE_FEED_PRINT_CONFIG", p.stdout or "")
            self.assertFalse(out_path.exists(), msg="--print-config must not write output")

    def test_dry_run_no_write(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            db_path = tmp / "db.sqlite"
            out_path = tmp / "sample.json"
            now = datetime.now(timezone.utc).replace(microsecond=0)
            _seed_db(
                db_path,
                [
                    {
                        "activity_nr": "1",
                        "inspection_type": "Complaint",
                        "establishment_name": "Acme",
                        "site_city": "Houston",
                        "site_state": "TX",
                        "date_opened": now.date().isoformat(),
                        "first_seen_at": now.isoformat(),
                        "source_url": "https://www.osha.gov/ords/imis/establishment.inspection_detail?id=1",
                    }
                ],
            )
            p = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    "--db",
                    str(db_path),
                    "--out",
                    str(out_path),
                    "--dry-run",
                ],
                cwd=str(ROOT),
                capture_output=True,
                text=True,
            )
            self.assertEqual(p.returncode, 0, msg=p.stderr + "\n" + p.stdout)
            self.assertIn("PASS_SAMPLE_FEED_DRY_RUN", p.stdout or "")
            self.assertFalse(out_path.exists(), msg="--dry-run must not write output")

    def test_auto_selects_top_three_deterministically(self):
        mod = _load_module()
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            db_path = tmp / "db.sqlite"
            now = datetime.now(timezone.utc).replace(microsecond=0)
            rows = [
                {
                    "activity_nr": "tx1",
                    "inspection_type": "Complaint",
                    "establishment_name": "TX One",
                    "site_city": "Houston",
                    "site_state": "TX",
                    "date_opened": now.date().isoformat(),
                    "first_seen_at": (now - timedelta(hours=1)).isoformat(),
                    "source_url": "https://www.osha.gov/ords/imis/establishment.inspection_detail?id=tx1",
                },
                {
                    "activity_nr": "tx2",
                    "inspection_type": "Referral",
                    "establishment_name": "TX Two",
                    "site_city": "Dallas",
                    "site_state": "TX",
                    "date_opened": now.date().isoformat(),
                    "first_seen_at": (now - timedelta(hours=2)).isoformat(),
                    "source_url": "https://www.osha.gov/ords/imis/establishment.inspection_detail?id=tx2",
                },
                {
                    "activity_nr": "wa1",
                    "inspection_type": "Accident",
                    "establishment_name": "WA One",
                    "site_city": "Seattle",
                    "site_state": "WA",
                    "date_opened": now.date().isoformat(),
                    "first_seen_at": (now - timedelta(hours=3)).isoformat(),
                    "source_url": "https://www.osha.gov/ords/imis/establishment.inspection_detail?id=wa1",
                },
                {
                    "activity_nr": "nv1",
                    "inspection_type": "Programmed",
                    "establishment_name": "NV One",
                    "site_city": "Las Vegas",
                    "site_state": "NV",
                    "date_opened": now.date().isoformat(),
                    "first_seen_at": (now - timedelta(hours=4)).isoformat(),
                    "source_url": "https://www.osha.gov/ords/imis/establishment.inspection_detail?id=nv1",
                },
                {
                    "activity_nr": "or1",
                    "inspection_type": "Complaint",
                    "establishment_name": "OR One",
                    "site_city": "Portland",
                    "site_state": "OR",
                    "date_opened": now.date().isoformat(),
                    "first_seen_at": (now - timedelta(days=10)).isoformat(),
                    "source_url": "https://www.osha.gov/ords/imis/establishment.inspection_detail?id=or1",
                },
            ]
            _seed_db(db_path, rows)
            payload, stats = mod.build_sample_feed(
                db_path=db_path,
                config_path=CONFIG,
                territories_arg="AUTO",
                rows_per_territory=4,
                lookback_days=7,
            )
            self.assertEqual(stats["territories_selected"], ["TX", "WA", "NV"])
            self.assertEqual([t["territory_id"] for t in payload], ["TX", "WA", "NV"])

    def test_auto_skips_empty_fallback_territories(self):
        mod = _load_module()
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            db_path = tmp / "db.sqlite"
            now = datetime.now(timezone.utc).replace(microsecond=0)
            _seed_db(
                db_path,
                [
                    {
                        "activity_nr": "tx1",
                        "inspection_type": "Complaint",
                        "establishment_name": "TX One",
                        "site_city": "Austin",
                        "site_state": "TX",
                        "date_opened": now.date().isoformat(),
                        "first_seen_at": now.isoformat(),
                        "source_url": "https://www.osha.gov/ords/imis/establishment.inspection_detail?id=tx1",
                    }
                ],
            )
            payload, stats = mod.build_sample_feed(
                db_path=db_path,
                config_path=CONFIG,
                territories_arg="AUTO",
                rows_per_territory=4,
                lookback_days=7,
            )
            self.assertEqual(stats["territories_considered"], ["TX"])
            self.assertEqual(stats["territories_selected"], ["TX"])
            self.assertEqual(len(payload), 1)
            self.assertEqual(payload[0]["territory_id"], "TX")
            self.assertGreaterEqual(len(payload[0]["rows"]), 1)

    def test_output_schema_and_updated_at_derivation(self):
        mod = _load_module()
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            db_path = tmp / "db.sqlite"
            _seed_db(
                db_path,
                [
                    {
                        "activity_nr": "ca1",
                        "inspection_type": "Complaint",
                        "establishment_name": "CA One",
                        "site_city": "Irvine",
                        "site_state": "CA",
                        "date_opened": "2026-02-10",
                        "first_seen_at": None,
                        "changed_at": None,
                        "last_seen_at": None,
                        "source_url": "https://www.osha.gov/ords/imis/establishment.inspection_detail?id=ca1",
                    }
                ],
            )
            payload, _stats = mod.build_sample_feed(
                db_path=db_path,
                config_path=CONFIG,
                territories_arg="CA,FL,TX",
                rows_per_territory=4,
                lookback_days=7,
            )
            self.assertEqual(len(payload), 1)
            first = payload[0]
            self.assertEqual(first["territory_id"], "CA")
            self.assertIn("territory_name", first)
            self.assertIn("updated_at_utc", first)
            self.assertIn("rows", first)
            self.assertEqual(first["updated_at_utc"], "2026-02-10T00:00:00Z")
            self.assertEqual(len(first["rows"]), 1)
            row = first["rows"][0]
            self.assertEqual(
                set(row.keys()),
                {
                    "activity_nr",
                    "inspection_type",
                    "establishment_name",
                    "city",
                    "state",
                    "opened_date",
                    "observed_at_utc",
                    "source_url",
                },
            )
            self.assertEqual(row["opened_date"], "2026-02-10")
            self.assertEqual(row["observed_at_utc"], "2026-02-10T00:00:00Z")


if __name__ == "__main__":
    unittest.main()
