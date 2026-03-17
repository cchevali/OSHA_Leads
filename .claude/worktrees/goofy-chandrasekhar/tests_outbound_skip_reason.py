import csv
import os
import tempfile
import unittest
from pathlib import Path

import outbound_cold_email as oce


class TestOutboundSkipReason(unittest.TestCase):
    def test_skip_reason_logged(self):
        temp_dir = tempfile.mkdtemp()
        log_path = Path(temp_dir) / "cold_email_log.csv"
        orig_log_path = oce.LOG_PATH
        try:
            oce.LOG_PATH = log_path
            oce.log_send(
                "test@example.com",
                "",
                [],
                "",
                "2026-02-03",
                "skipped",
                "no_high_med_leads",
                "",
            )

            with open(log_path, "r", newline="", encoding="utf-8") as f:
                rows = list(csv.DictReader(f))
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["error"], "no_high_med_leads")
        finally:
            oce.LOG_PATH = orig_log_path
            if log_path.exists():
                os.unlink(log_path)
            os.rmdir(temp_dir)


if __name__ == "__main__":
    unittest.main()
