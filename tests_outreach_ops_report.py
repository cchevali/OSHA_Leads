import csv
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent
SCRIPT = REPO_ROOT / "outreach" / "ops_report.py"
NO_WRITE_PATH_SENTINEL = "(no-write)"

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from outreach import crm_store


def _ts_days_ago(days: int) -> str:
    return (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()


def _extract_json_path_token(stdout: str) -> str:
    for line in (stdout or "").splitlines():
        if line.startswith("OPS_REPORT_JSON_PATH="):
            return line.split("=", 1)[1].strip()
    return ""


def _seed_dataset(data_dir: Path) -> None:
    data_dir.mkdir(parents=True, exist_ok=True)
    db_path = data_dir / "crm.sqlite"
    conn = crm_store.connect(db_path)
    try:
        crm_store.init_schema(conn)
        cur = conn.cursor()

        prospects = [
            ("p_tx1", "Alpha Safety", "Owner A", "owner@alpha.com", "Owner", "Austin", "TX", "https://alpha.com", "seed", 10, "new", _ts_days_ago(2), None),
            ("p_tx2", "Dup Co", "Ops B", "info@dup.com", "Safety Manager", "Austin", "TX", "https://dup.com", "seed", 8, "new", _ts_days_ago(3), None),
            ("p_tx3", "Dup Co 2", "Ops C", "sales@dup.com", "Operations Manager", "Houston", "TX", "https://dup.com", "seed", 7, "new", _ts_days_ago(4), None),
            ("p_ca1", "Bad Email Co", "Ops D", "bad-email", "Director", "Los Angeles", "CA", "https://bad.example", "seed", 4, "new", _ts_days_ago(1), None),
            ("p_unknown", "Unknown Co", "Ops E", "contact@unknown.com", "Partner", "Miami", "FL", "https://unknown.com", "seed", 5, "new", _ts_days_ago(10), None),
        ]
        cur.executemany(
            """
            INSERT INTO prospects(
                prospect_id, firm, contact_name, email, title, city, state, website, source,
                score, status, created_at, last_contacted_at
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            prospects,
        )

        def insert_event(
            prospect_id: str,
            days_ago: int,
            event_type: str,
            batch_id: str,
            metadata: dict | None = None,
            attributed_send_event_id=None,
            attributed_batch_id: str = "",
            attributed_state_at_send: str = "",
            attributed_model: str = "",
        ) -> int:
            cur.execute(
                """
                INSERT INTO outreach_events(
                    prospect_id,
                    ts,
                    event_type,
                    batch_id,
                    metadata_json,
                    attributed_send_event_id,
                    attributed_batch_id,
                    attributed_state_at_send,
                    attributed_model
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    prospect_id,
                    _ts_days_ago(days_ago),
                    event_type,
                    batch_id,
                    json.dumps(metadata or {}, separators=(",", ":")),
                    attributed_send_event_id,
                    attributed_batch_id,
                    attributed_state_at_send,
                    attributed_model,
                ),
            )
            return int(cur.lastrowid)

        sent_tx1 = insert_event("p_tx1", 6, "sent", "2026-02-01_TX", {"message_id": "<m1>", "state": "TX", "email": "owner@alpha.com"})
        sent_tx2 = insert_event("p_tx2", 5, "sent", "2026-02-02_TX", {"message_id": "<m2>", "state": "TX", "email": "info@dup.com"})
        _sent_tx3 = insert_event("p_tx3", 8, "sent", "2026-02-03_TX", {"message_id": "<m3>", "state": "TX", "email": "sales@dup.com"})
        _sent_ca1 = insert_event("p_ca1", 20, "sent", "2026-01-20_CA", {"message_id": "<m4>", "state": "CA"})

        insert_event("p_tx1", 5, "delivered", "2026-02-01_TX", {})
        insert_event("p_tx2", 4, "bounce", "", {"message_id": "<m2>", "email": "info@dup.com"})
        insert_event(
            "p_tx1",
            4,
            "replied",
            "TX_AUTO",
            {},
            attributed_send_event_id=sent_tx1,
            attributed_batch_id="2026-02-01_TX",
            attributed_state_at_send="TX",
            attributed_model="direct_send_event_id",
        )
        insert_event(
            "p_tx2",
            3,
            "trial_started",
            "TX_AUTO",
            {"send_message_id": "<m2>"},
            attributed_send_event_id=sent_tx2,
            attributed_batch_id="2026-02-02_TX",
            attributed_state_at_send="TX",
            attributed_model="direct_send_event_id",
        )
        insert_event("p_unknown", 2, "converted", "OUTREACH_AUTO", {})
        insert_event("p_ca1", 19, "replied", "OUTREACH_AUTO", {})

        cur.execute(
            "INSERT INTO suppression(email, reason, ts) VALUES(?, ?, ?)",
            ("sales@dup.com", "hard_bounce", _ts_days_ago(2)),
        )

        conn.commit()
    finally:
        conn.close()

    sup_csv = data_dir / "suppression.csv"
    with open(sup_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["email", "reason", "timestamp"])
        w.writeheader()
        w.writerow({"email": "info@dup.com", "reason": "bounce_event", "timestamp": _ts_days_ago(1)})
        w.writerow({"email": "contact@unknown.com", "reason": "spam_complaint", "timestamp": _ts_days_ago(1)})


def _seed_attribution_stability_dataset(data_dir: Path) -> None:
    data_dir.mkdir(parents=True, exist_ok=True)
    db_path = data_dir / "crm.sqlite"
    conn = crm_store.connect(db_path)
    try:
        crm_store.init_schema(conn)
        cur = conn.cursor()

        cur.execute(
            """
            INSERT INTO prospects(
                prospect_id, firm, contact_name, email, title, city, state, website, source,
                score, status, created_at, last_contacted_at
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "p_stable",
                "Stable Co",
                "Pat Stable",
                "pat@stable.co",
                "Director",
                "Austin",
                "TX",
                "https://stable.co",
                "seed",
                9,
                "new",
                _ts_days_ago(6),
                None,
            ),
        )

        def insert_event(
            days_ago: int,
            event_type: str,
            batch_id: str,
            metadata: dict | None = None,
            attributed_send_event_id=None,
            attributed_batch_id: str = "",
            attributed_state_at_send: str = "",
            attributed_model: str = "",
        ) -> int:
            cur.execute(
                """
                INSERT INTO outreach_events(
                    prospect_id,
                    ts,
                    event_type,
                    batch_id,
                    metadata_json,
                    attributed_send_event_id,
                    attributed_batch_id,
                    attributed_state_at_send,
                    attributed_model
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "p_stable",
                    _ts_days_ago(days_ago),
                    event_type,
                    batch_id,
                    json.dumps(metadata or {}, separators=(",", ":")),
                    attributed_send_event_id,
                    attributed_batch_id,
                    attributed_state_at_send,
                    attributed_model,
                ),
            )
            return int(cur.lastrowid)

        send_a = insert_event(6, "sent", "2026-02-01_TX", {"message_id": "<mA>", "state": "TX"})
        insert_event(
            2,
            "replied",
            "OUTREACH_AUTO",
            {"note": "persisted attribution to send A"},
            attributed_send_event_id=send_a,
            attributed_batch_id="2026-02-01_TX",
            attributed_state_at_send="TX",
            attributed_model="direct_send_event_id",
        )
        insert_event(1, "sent", "2026-02-06_CA", {"message_id": "<mB>", "state": "CA"})

        conn.commit()
    finally:
        conn.close()

    with open(data_dir / "suppression.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["email", "reason", "timestamp"])
        w.writeheader()


def _find_cohort(rows: list[dict], batch_id: str, state: str) -> dict | None:
    for row in rows:
        if (row.get("batch_id") or "") == batch_id and (row.get("state_at_send") or "") == state:
            return row
    return None


class TestOutreachOpsReport(unittest.TestCase):
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

    def test_print_config_is_side_effect_free(self):
        with tempfile.TemporaryDirectory() as d:
            data_dir = Path(d) / "data"
            p = self._run(["--print-config"], {"DATA_DIR": str(data_dir)})
            self.assertEqual(p.returncode, 0, msg=p.stderr + "\n" + p.stdout)
            self.assertIn("ops_report_schema_version=v1", p.stdout)
            self.assertIn("artifact_path=", p.stdout)
            self.assertIn("dry_run=False", p.stdout)
            self.assertIn("no_write=False", p.stdout)
            self.assertFalse((data_dir / "outreach" / "ops_reports").exists())

    def test_init_schema_migrates_old_outreach_events_columns_idempotently(self):
        with tempfile.TemporaryDirectory() as d:
            data_dir = Path(d) / "data"
            db_path = data_dir / "crm.sqlite"
            data_dir.mkdir(parents=True, exist_ok=True)

            conn = sqlite3.connect(str(db_path))
            try:
                conn.executescript(
                    """
                    CREATE TABLE outreach_events (
                        event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        prospect_id TEXT NOT NULL,
                        ts TEXT NOT NULL,
                        event_type TEXT NOT NULL,
                        batch_id TEXT NOT NULL DEFAULT '',
                        metadata_json TEXT NOT NULL DEFAULT '{}'
                    );
                    """
                )
                conn.commit()
            finally:
                conn.close()

            crm_store.ensure_database(db_path)
            crm_store.ensure_database(db_path)

            conn = sqlite3.connect(str(db_path))
            try:
                cols = [str(r[1]) for r in conn.execute("PRAGMA table_info(outreach_events)").fetchall()]
            finally:
                conn.close()

            self.assertEqual(cols.count("attributed_send_event_id"), 1)
            self.assertEqual(cols.count("attributed_batch_id"), 1)
            self.assertEqual(cols.count("attributed_state_at_send"), 1)
            self.assertEqual(cols.count("attributed_model"), 1)

    def test_default_output_writes_artifact_latest_and_footer_contract(self):
        with tempfile.TemporaryDirectory() as d:
            data_dir = Path(d) / "data"
            _seed_dataset(data_dir)
            p = self._run([], {"DATA_DIR": str(data_dir)})
            self.assertEqual(p.returncode, 0, msg=p.stderr + "\n" + p.stdout)

            lines = (p.stdout or "").splitlines()
            self.assertGreaterEqual(len(lines), 3)
            self.assertTrue(lines[-3].startswith("OPS_REPORT_JSON_PATH="))
            self.assertEqual(lines[-2], "OPS_REPORT_SCHEMA_VERSION=v1")
            self.assertTrue(lines[-1].startswith("OPS_REPORT_GENERATED_AT_UTC="))

            json_path = lines[-3].split("=", 1)[1].strip()
            self.assertNotEqual(json_path, NO_WRITE_PATH_SENTINEL)
            artifact = Path(json_path)
            latest = data_dir / "outreach" / "ops_reports" / "latest.json"
            self.assertTrue(artifact.exists())
            self.assertTrue(latest.exists())

            generated_footer = lines[-1].split("=", 1)[1].strip()
            payload = json.loads(artifact.read_text(encoding="utf-8"))
            self.assertEqual(payload.get("schema_version"), "v1")
            self.assertEqual(payload.get("json_path"), str(artifact))
            self.assertEqual(payload.get("generated_at_utc"), generated_footer)

    def test_dry_run_writes_artifact_and_latest(self):
        with tempfile.TemporaryDirectory() as d:
            data_dir = Path(d) / "data"
            _seed_dataset(data_dir)
            p = self._run(["--dry-run"], {"DATA_DIR": str(data_dir)})
            self.assertEqual(p.returncode, 0, msg=p.stderr + "\n" + p.stdout)

            json_path = _extract_json_path_token(p.stdout)
            self.assertTrue(json_path, msg=p.stdout)
            self.assertNotEqual(json_path, NO_WRITE_PATH_SENTINEL)

            artifact = Path(json_path)
            latest = data_dir / "outreach" / "ops_reports" / "latest.json"
            self.assertTrue(artifact.exists())
            self.assertTrue(latest.exists())

            payload = json.loads(artifact.read_text(encoding="utf-8"))
            self.assertTrue(bool((payload.get("config") or {}).get("dry_run")))

    def test_no_write_skips_artifacts_and_uses_sentinel(self):
        with tempfile.TemporaryDirectory() as d:
            data_dir = Path(d) / "data"
            _seed_dataset(data_dir)

            p_text = self._run(["--dry-run", "--no-write"], {"DATA_DIR": str(data_dir)})
            self.assertEqual(p_text.returncode, 0, msg=p_text.stderr + "\n" + p_text.stdout)
            lines = (p_text.stdout or "").splitlines()
            self.assertGreaterEqual(len(lines), 3)
            self.assertEqual(lines[-3], f"OPS_REPORT_JSON_PATH={NO_WRITE_PATH_SENTINEL}")
            self.assertEqual(lines[-2], "OPS_REPORT_SCHEMA_VERSION=v1")
            self.assertTrue(lines[-1].startswith("OPS_REPORT_GENERATED_AT_UTC="))
            self.assertFalse((data_dir / "outreach" / "ops_reports").exists())

            p_json = self._run(["--format", "json", "--no-write"], {"DATA_DIR": str(data_dir)})
            self.assertEqual(p_json.returncode, 0, msg=p_json.stderr + "\n" + p_json.stdout)
            payload = json.loads(p_json.stdout)
            self.assertEqual(payload.get("json_path"), NO_WRITE_PATH_SENTINEL)
            self.assertFalse((data_dir / "outreach" / "ops_reports").exists())

    def test_json_output_contract_and_metrics(self):
        with tempfile.TemporaryDirectory() as d:
            data_dir = Path(d) / "data"
            _seed_dataset(data_dir)
            p = self._run(["--format", "json", "--dry-run"], {"DATA_DIR": str(data_dir)})
            self.assertEqual(p.returncode, 0, msg=p.stderr + "\n" + p.stdout)
            self.assertNotIn("OPS_REPORT_SCHEMA_VERSION=", p.stdout)
            self.assertNotIn("OPS_REPORT_JSON_PATH=", p.stdout)
            self.assertNotIn("OPS_REPORT_GENERATED_AT_UTC=", p.stdout)

            payload = json.loads(p.stdout)
            self.assertEqual(payload.get("schema_version"), "v1")
            self.assertIn("json_path", payload)
            artifact = Path(str(payload.get("json_path") or ""))
            self.assertTrue(artifact.exists())
            latest = data_dir / "outreach" / "ops_reports" / "latest.json"
            self.assertTrue(latest.exists())

            windows = payload.get("windows") or {}
            list_quality = payload.get("list_quality") or {}
            self.assertIn("7d", windows)
            self.assertIn("30d", windows)

            rows_7d = (windows["7d"] or {}).get("cohorts") or []
            tx1 = _find_cohort(rows_7d, "2026-02-01_TX", "TX")
            self.assertIsNotNone(tx1)
            self.assertEqual(int(tx1["sent"]), 1)
            self.assertEqual(int(tx1["delivered_proxy"]), 1)
            self.assertEqual(int(tx1["replied"]), 1)

            tx2 = _find_cohort(rows_7d, "2026-02-02_TX", "TX")
            self.assertIsNotNone(tx2)
            self.assertEqual(int(tx2["bounced_confirmed"]), 1)
            self.assertEqual(int(tx2["trial_started"]), 1)

            tx3 = _find_cohort(rows_7d, "2026-02-03_TX", "TX")
            self.assertIsNotNone(tx3)
            self.assertEqual(int(tx3["sent"]), 0)
            self.assertEqual(int(tx3["bounced_inferred"]), 1)

            unknown_7d = _find_cohort(rows_7d, "UNKNOWN", "UNKNOWN")
            self.assertIsNotNone(unknown_7d)
            self.assertEqual(int(unknown_7d["converted"]), 1)
            self.assertEqual(int(unknown_7d["bounced_inferred"]), 1)

            rows_30d = (windows["30d"] or {}).get("cohorts") or []
            ca = _find_cohort(rows_30d, "2026-01-20_CA", "CA")
            self.assertIsNotNone(ca)
            self.assertEqual(int(ca["replied"]), 0)

            unknown_30d = _find_cohort(rows_30d, "UNKNOWN", "UNKNOWN")
            self.assertIsNotNone(unknown_30d)
            self.assertEqual(int(unknown_30d["replied"]), 1)
            self.assertEqual(int(unknown_30d["converted"]), 1)

            q7 = list_quality.get("7d") or {}
            self.assertEqual(int(q7.get("new_prospects_count", -1)), 4)
            self.assertAlmostEqual(float(q7.get("valid_email_format_pct", -1)), 0.75, places=4)
            self.assertEqual(int(q7.get("duplicate_domain_rows", -1)), 1)
            self.assertAlmostEqual(float(q7.get("duplicate_domain_pct", -1)), 0.25, places=4)
            self.assertAlmostEqual(float(q7.get("role_based_inbox_share_pct", -1)), 0.5, places=4)

            q30 = list_quality.get("30d") or {}
            self.assertEqual(int(q30.get("new_prospects_count", -1)), 5)
            self.assertAlmostEqual(float(q30.get("valid_email_format_pct", -1)), 0.8, places=4)
            self.assertEqual(int(q30.get("duplicate_domain_rows", -1)), 1)
            self.assertAlmostEqual(float(q30.get("duplicate_domain_pct", -1)), 0.2, places=4)
            self.assertAlmostEqual(float(q30.get("role_based_inbox_share_pct", -1)), 0.6, places=4)

            notes = payload.get("notes") or []
            self.assertTrue(any(str(n).startswith("unattributed_replied_event_id=") for n in notes))

    def test_persisted_lifecycle_attribution_is_stable_with_later_send(self):
        with tempfile.TemporaryDirectory() as d:
            data_dir = Path(d) / "data"
            _seed_attribution_stability_dataset(data_dir)
            p = self._run(["--format", "json", "--no-write"], {"DATA_DIR": str(data_dir)})
            self.assertEqual(p.returncode, 0, msg=p.stderr + "\n" + p.stdout)

            payload = json.loads(p.stdout)
            rows_7d = ((payload.get("windows") or {}).get("7d") or {}).get("cohorts") or []

            cohort_x = _find_cohort(rows_7d, "2026-02-01_TX", "TX")
            self.assertIsNotNone(cohort_x)
            self.assertEqual(int(cohort_x["replied"]), 1)

            cohort_y = _find_cohort(rows_7d, "2026-02-06_CA", "CA")
            self.assertIsNotNone(cohort_y)
            self.assertEqual(int(cohort_y["replied"]), 0)

            notes = payload.get("notes") or []
            self.assertFalse(any(str(n).startswith("unattributed_replied_event_id=") for n in notes))


if __name__ == "__main__":
    unittest.main()
