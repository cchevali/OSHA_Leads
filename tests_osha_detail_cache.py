import io
import os
import sqlite3
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest import mock

from scoring import osha_detail_cache as odc
from scoring import paths as scoring_paths
from tools import cache_osha_inspection_detail as cli


def _seed_leads_db(path: Path, rows: list[dict]) -> None:
    conn = sqlite3.connect(str(path))
    try:
        conn.execute(
            """
            CREATE TABLE inspections (
                activity_nr TEXT,
                source_url TEXT,
                first_seen_at TEXT,
                changed_at TEXT,
                last_seen_at TEXT,
                date_opened TEXT,
                parse_invalid INTEGER
            )
            """
        )
        for row in rows:
            conn.execute(
                """
                INSERT INTO inspections(activity_nr, source_url, first_seen_at, changed_at, last_seen_at, date_opened, parse_invalid)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row.get("activity_nr", ""),
                    row.get("source_url", ""),
                    row.get("first_seen_at", ""),
                    row.get("changed_at", ""),
                    row.get("last_seen_at", ""),
                    row.get("date_opened", ""),
                    int(row.get("parse_invalid", 0)),
                ),
            )
        conn.commit()
    finally:
        conn.close()


class _FakeResp:
    def __init__(self, text: str, url: str, status_code: int = 200):
        self.text = text
        self.content = text.encode("utf-8")
        self.url = url
        self.status_code = status_code


class _FakeSession:
    def __init__(self, text: str):
        self._text = text
        self.calls = 0

    def get(self, url, timeout=None, allow_redirects=True):  # noqa: ANN001
        self.calls += 1
        return _FakeResp(self._text, url, 200)


class TestOshaDetailCache(unittest.TestCase):
    def test_cli_dry_run_prints_markers_and_no_writes(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            leads_db = tmp / "osha.sqlite"
            _seed_leads_db(
                leads_db,
                [
                    {
                        "activity_nr": "1234",
                        "source_url": "",
                        "first_seen_at": "2026-02-20T00:00:00Z",
                        "changed_at": "2026-02-20T00:00:00Z",
                        "last_seen_at": "2026-02-20T00:00:00Z",
                        "date_opened": "2026-02-19",
                    }
                ],
            )
            data_dir = tmp / "runtime"
            out = io.StringIO()
            with mock.patch.dict(os.environ, {"DATA_DIR": str(data_dir)}, clear=False):
                with redirect_stdout(out):
                    code = cli.main(["--db", str(leads_db), "--dry-run"])
            self.assertEqual(code, 0)
            stdout = out.getvalue()
            self.assertIn("OSHA_DETAIL_CACHE_DB=", stdout)
            self.assertIn("OSHA_DETAIL_CANDIDATES=1", stdout)
            self.assertIn("OSHA_DETAIL_COMPLETE=status=DRY_RUN", stdout.replace(" ", ""))
            self.assertFalse((data_dir / "scoring" / "osha_detail_cache.sqlite").exists())
            self.assertFalse((data_dir / "scoring" / "cache_runs").exists())

    def test_since_days_max_60_enforced(self):
        out = io.StringIO()
        with redirect_stdout(out):
            code = cli.main(["--since-days", "61", "--dry-run"])
        self.assertEqual(code, 1)
        self.assertIn("ERR_OSHA_DETAIL_CACHE_CONFIG", out.getvalue())

    def test_cache_schema_created_and_idempotent_ttl_skip(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            leads_db = tmp / "osha.sqlite"
            _seed_leads_db(
                leads_db,
                [
                    {
                        "activity_nr": "5555",
                        "source_url": "https://www.osha.gov/ords/imis/establishment.inspection_detail?id=5555",
                        "first_seen_at": "2026-02-20T00:00:00Z",
                        "changed_at": "2026-02-20T00:00:00Z",
                        "last_seen_at": "2026-02-20T00:00:00Z",
                        "date_opened": "2026-02-19",
                    }
                ],
            )
            cache_db = tmp / "scoring" / "osha_detail_cache.sqlite"
            html = """
            <html><body>
              <div>Inspection Type: Referral</div>
              <div>Case Status: Open</div>
              <div>Date Opened: 02/19/2026</div>
              <div>Area Office: Dallas Area Office</div>
              <div>Emphasis: Fall Protection</div>
            </body></html>
            """
            fake_session = _FakeSession(html)
            with mock.patch.object(odc.ingest_osha, "get_session", return_value=fake_session), mock.patch.object(
                odc.ingest_osha,
                "parse_inspection_detail",
                return_value={
                    "activity_nr": "5555",
                    "inspection_type": "Referral",
                    "case_status": "Open",
                    "date_opened": "2026-02-19",
                    "area_office": "Dallas Area Office",
                    "emphasis": "Fall Protection",
                },
            ):
                res1 = odc.run_cache(
                    odc.CacheRunConfig(
                        leads_db_path=leads_db,
                        cache_db_path=cache_db,
                        since_days=14,
                        limit=10,
                        sleep_ms=0,
                        ttl_days=30,
                        dry_run=False,
                    )
                )
                self.assertEqual(res1["fetched"], 1)
                self.assertEqual(res1["skipped_cached"], 0)
                self.assertTrue(cache_db.exists())

                res2 = odc.run_cache(
                    odc.CacheRunConfig(
                        leads_db_path=leads_db,
                        cache_db_path=cache_db,
                        since_days=14,
                        limit=10,
                        sleep_ms=0,
                        ttl_days=30,
                        dry_run=False,
                    )
                )
                self.assertEqual(res2["fetched"], 0)
                self.assertEqual(res2["skipped_cached"], 1)

            conn = sqlite3.connect(str(cache_db))
            try:
                tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
                self.assertIn("inspection_detail_cache", tables)
                self.assertIn("cache_failures", tables)
                row = conn.execute(
                    "SELECT activity_nr, final_url, http_status, content_sha256, raw_html_gz, inspection_type FROM inspection_detail_cache"
                ).fetchone()
                self.assertEqual(row[0], "5555")
                self.assertEqual(row[2], 200)
                self.assertTrue(row[3])
                self.assertIsNotNone(row[4])
                self.assertEqual(row[5], "Referral")
            finally:
                conn.close()

    def test_limit_enforced_in_candidate_selection(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            leads_db = tmp / "osha.sqlite"
            rows = []
            for i in range(5):
                rows.append(
                    {
                        "activity_nr": str(7000 + i),
                        "source_url": "",
                        "first_seen_at": "2026-02-20T00:00:00Z",
                        "changed_at": "2026-02-20T00:00:00Z",
                        "last_seen_at": "2026-02-20T00:00:00Z",
                        "date_opened": "2026-02-19",
                    }
                )
            _seed_leads_db(leads_db, rows)
            selected = odc.select_candidates_from_leads_db(leads_db, since_days=14, limit=2)
            self.assertEqual(len(selected), 2)


if __name__ == "__main__":
    unittest.main()

