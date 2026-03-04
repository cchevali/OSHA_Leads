import io
import json
import os
import sqlite3
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest import mock

import deliver_daily


def _seed_db(path: Path) -> None:
    conn = sqlite3.connect(str(path))
    try:
        conn.execute("CREATE TABLE suppression_list (email TEXT)")
        conn.commit()
    finally:
        conn.close()


class TestDeliverDailyIngestTokens(unittest.TestCase):
    def test_emit_pre_ingest_scope_tokens(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            db_path = root / "osha.sqlite"
            customer_path = root / "customer.json"
            _seed_db(db_path)
            customer_path.write_text(
                json.dumps(
                    {
                        "customer_id": "facs_trial",
                        "states": ["CA", "OR", "WA"],
                        "opened_window_days": 14,
                        "new_only_days": 1,
                        "email_recipients": ["taylor.thomas@facs.com"],
                    }
                )
                + "\n",
                encoding="utf-8",
            )

            argv = [
                "deliver_daily.py",
                "--customer",
                str(customer_path),
                "--db",
                str(db_path),
                "--mode",
                "daily",
                "--dry-run",
                "--max-details",
                "77",
            ]

            out = io.StringIO()
            old_cwd = os.getcwd()
            try:
                with mock.patch.object(deliver_daily, "get_script_dir", return_value=str(root)), mock.patch.object(
                    deliver_daily, "load_environment", return_value=None
                ), mock.patch.object(
                    deliver_daily, "run_command", return_value=0
                ), mock.patch.object(
                    sys, "argv", argv
                ):
                    with redirect_stdout(out), self.assertRaises(SystemExit) as exit_ctx:
                        deliver_daily.main()
            finally:
                os.chdir(old_cwd)

            text = out.getvalue()
            self.assertEqual(int(exit_ctx.exception.code or 0), 0, msg=text)
            self.assertIn("DELIVER_INGEST_SCOPE_STATES=CA,OR,WA source=customer_config", text)
            self.assertIn("DELIVER_INGEST_MAX_DETAILS=77", text)


if __name__ == "__main__":
    unittest.main()
