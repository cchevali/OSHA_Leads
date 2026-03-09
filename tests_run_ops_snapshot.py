import csv
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent
SCRIPT = REPO_ROOT / "outreach" / "run_ops_snapshot.py"

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from outreach import crm_store


def _seed_snapshot_dataset(data_dir: Path) -> None:
    data_dir.mkdir(parents=True, exist_ok=True)
    db_path = data_dir / "crm.sqlite"
    conn = crm_store.connect(db_path)
    try:
        crm_store.init_schema(conn)
        now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        conn.execute(
            """
            INSERT INTO prospects(
                prospect_id, firm, contact_name, email, title, city, state, website, source,
                score, status, created_at, last_contacted_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("p1", "Alpha", "Owner", "owner@example.com", "Owner", "Austin", "TX", "", "seed", 10, "new", now, None),
        )
        conn.execute(
            """
            INSERT INTO outreach_events(
                prospect_id, ts, event_type, batch_id, metadata_json,
                attributed_send_event_id, attributed_batch_id, attributed_state_at_send, attributed_model
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("p1", now, "sent", "2026-03-01_TX", "{\"message_id\":\"<m1>\",\"state\":\"TX\"}", None, "", "", ""),
        )
        conn.execute(
            """
            INSERT INTO outreach_events(
                prospect_id, ts, event_type, batch_id, metadata_json,
                attributed_send_event_id, attributed_batch_id, attributed_state_at_send, attributed_model
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("p1", now, "replied", "TX_AUTO", "{}", 1, "2026-03-01_TX", "TX", "direct_send_event_id"),
        )
        conn.execute("INSERT INTO suppression(email, reason, ts) VALUES (?, ?, ?)", ("owner@example.com", "complaint", now))
        conn.execute("INSERT INTO trials(prospect_id, territory_code, started_at, status) VALUES (?, ?, ?, ?)", ("p1", "TX", now, "active"))
        conn.commit()
    finally:
        conn.close()

    with open(data_dir / "suppression.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["email", "reason", "timestamp"])
        writer.writeheader()
        writer.writerow({"email": "owner@example.com", "reason": "complaint", "timestamp": datetime.now(timezone.utc).isoformat()})

    status_root = data_dir / "runtime" / "status"
    (status_root / "jobs").mkdir(parents=True, exist_ok=True)
    (status_root / "runtime_latest.json").write_text(
        json.dumps(
            {
                "schema": "runtime_tick_v1",
                "finished_local": datetime.now(timezone.utc).isoformat(),
                "alerts": {"alerts_sent": 0, "alerts_skipped": 1},
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    (status_root / "jobs" / "outreach_auto.json").write_text(
        json.dumps(
            {
                "schema": "runtime_tick_job_state_v1",
                "job_name": "outreach_auto",
                "last_slot_key": "2026-03-09",
                "last_result": "ran",
                "last_result_detail": "reconciled",
                "last_reason": "external_wrapper_success_within_window",
                "last_external_scheduler_detected": 1,
                "last_reconciliation_status": "external_wrapper_success_within_window",
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    (data_dir / "bounce_import_state.json").write_text(
        json.dumps({"updated_at_utc": datetime.now(timezone.utc).isoformat(), "last_uid_processed": 12, "uidvalidity": "99"}, indent=2)
        + "\n",
        encoding="utf-8",
    )


class TestRunOpsSnapshot(unittest.TestCase):
    def _run(self, args: list[str], env_overrides: dict[str, str]) -> subprocess.CompletedProcess:
        env = os.environ.copy()
        env["PYTHONPATH"] = str(REPO_ROOT)
        env.update(env_overrides)
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
            self.assertIn("ops_snapshot_schema_version=v1", p.stdout)
            self.assertFalse((data_dir / "outreach" / "ops_snapshots").exists())

    def test_live_snapshot_writes_artifact_with_ops_and_readiness(self):
        with tempfile.TemporaryDirectory() as d:
            data_dir = Path(d) / "data"
            _seed_snapshot_dataset(data_dir)
            p = self._run([], {"DATA_DIR": str(data_dir), "OUTREACH_SUPPRESSION_MAX_AGE_HOURS": "240"})
            self.assertEqual(p.returncode, 0, msg=p.stderr + "\n" + p.stdout)
            lines = (p.stdout or "").splitlines()
            self.assertTrue(lines[-3].startswith("OPS_SNAPSHOT_JSON_PATH="))
            json_path = lines[-3].split("=", 1)[1].strip()
            payload = json.loads(Path(json_path).read_text(encoding="utf-8"))
            self.assertIn("ops_report", payload)
            self.assertIn("readiness", payload)
            self.assertTrue(bool((payload.get("readiness") or {}).get("parallel_scheduler_active")))
            self.assertEqual((payload.get("readiness") or {}).get("parallel_scheduler_jobs"), ["outreach_auto"])
            latest = data_dir / "outreach" / "ops_snapshots" / "latest.json"
            self.assertTrue(latest.exists())

    def test_dry_run_uses_no_write_sentinel(self):
        with tempfile.TemporaryDirectory() as d:
            data_dir = Path(d) / "data"
            _seed_snapshot_dataset(data_dir)
            p = self._run(["--dry-run"], {"DATA_DIR": str(data_dir)})
            self.assertEqual(p.returncode, 0, msg=p.stderr + "\n" + p.stdout)
            self.assertIn("OPS_SNAPSHOT_JSON_PATH=(no-write)", p.stdout)
            self.assertFalse((data_dir / "outreach" / "ops_snapshots").exists())


if __name__ == "__main__":
    unittest.main()
