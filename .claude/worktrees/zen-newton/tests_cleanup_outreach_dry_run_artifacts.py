import json
import os
import subprocess
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent
SCRIPT = REPO_ROOT / "outreach" / "cleanup_outreach_dry_run_artifacts.py"


def _touch(path: Path, *, age_days: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("x\n", encoding="utf-8")
    ts = (datetime.now(timezone.utc) - timedelta(days=age_days)).timestamp()
    os.utime(path, (ts, ts))


class TestCleanupOutreachDryRunArtifacts(unittest.TestCase):
    def _run(self, args: list[str]) -> subprocess.CompletedProcess:
        env = os.environ.copy()
        env["PYTHONPATH"] = str(REPO_ROOT)
        return subprocess.run(
            [sys.executable, str(SCRIPT)] + args,
            cwd=str(REPO_ROOT),
            env=env,
            capture_output=True,
            text=True,
        )

    def test_print_config_is_side_effect_free(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d) / "out" / "outreach"
            p = self._run(["--print-config", "--root", str(root)])
            self.assertEqual(p.returncode, 0, msg=p.stderr + "\n" + p.stdout)
            self.assertIn("retention_days=14", p.stdout)
            self.assertFalse(root.exists())

    def test_dry_run_reports_only_stale_dry_run_artifacts(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d) / "out" / "outreach"
            batch = root / "2026-03-01_TX"
            _touch(batch / "outbox_2026-03-01_TX_dry_run.csv", age_days=20)
            _touch(batch / "outbox_2026-03-01_TX_dry_run_manifest.csv", age_days=20)
            _touch(batch / "plan_diagnostics.json", age_days=20)
            _touch(batch / "live_send_manifest.csv", age_days=20)
            payload = json.loads(self._run(["--dry-run", "--root", str(root)]).stdout)
            paths = [str(item.get("path") or "") for item in payload.get("candidates") or []]
            self.assertEqual(payload.get("candidate_count"), 3)
            self.assertTrue(any(path.endswith("plan_diagnostics.json") for path in paths))
            self.assertFalse(any(path.endswith("live_send_manifest.csv") for path in paths))
            self.assertTrue((batch / "outbox_2026-03-01_TX_dry_run.csv").exists())

    def test_live_cleanup_removes_only_stale_dry_run_files_and_empty_dirs(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d) / "out" / "outreach"
            stale_batch = root / "2026-03-01_TX"
            fresh_batch = root / "2026-03-02_CA"
            live_batch = root / "2026-03-03_FL"
            _touch(stale_batch / "outbox_2026-03-01_TX_dry_run.csv", age_days=20)
            _touch(stale_batch / "outbox_2026-03-01_TX_dry_run_manifest.csv", age_days=20)
            _touch(stale_batch / "plan_diagnostics.json", age_days=20)
            _touch(fresh_batch / "outbox_2026-03-02_CA_dry_run.csv", age_days=2)
            _touch(live_batch / "live_send_manifest.csv", age_days=20)

            payload = json.loads(self._run(["--root", str(root)]).stdout)
            self.assertEqual(payload.get("removed_file_count"), 3)
            self.assertEqual(payload.get("removed_dir_count"), 1)
            self.assertFalse(stale_batch.exists())
            self.assertTrue((fresh_batch / "outbox_2026-03-02_CA_dry_run.csv").exists())
            self.assertTrue((live_batch / "live_send_manifest.csv").exists())


if __name__ == "__main__":
    unittest.main()
