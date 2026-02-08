import io
import json
import os
import tempfile
import unittest
import sys
from contextlib import redirect_stdout
from pathlib import Path

import run_wally_trial


class TestWallyTrialDoctor(unittest.TestCase):
    def test_doctor_succeeds_when_preflight_succeeds_and_does_not_send(self) -> None:
        # Minimal config that satisfies run_wally_trial.preflight().
        cfg = {
            "brand_name": "Test Brand",
            "mailing_address": "123 Test St, Test City, TS",
            "recipients": ["test@example.com"],
        }

        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "customer.json"
            cfg_path.write_text(json.dumps(cfg), encoding="utf-8")

            # Provide required SMTP env vars (preflight validates presence only; doctor must not connect/send).
            keys = ["SMTP_HOST", "SMTP_PORT", "SMTP_USER", "SMTP_PASS"]
            old_vals = {k: os.environ.get(k) for k in keys}
            os.environ["SMTP_HOST"] = "smtp.example.com"
            os.environ["SMTP_PORT"] = "587"
            os.environ["SMTP_USER"] = "user"
            os.environ["SMTP_PASS"] = "pass"

            # If these are invoked in doctor mode, that would imply sending behavior.
            orig_preview = run_wally_trial.run_preview_send
            orig_live = run_wally_trial.run_live_send
            orig_query = run_wally_trial.query_task_to_run

            run_wally_trial.run_preview_send = lambda *a, **k: (_ for _ in ()).throw(  # type: ignore[assignment]
                AssertionError("run_preview_send should not be called in --doctor mode")
            )
            run_wally_trial.run_live_send = lambda *a, **k: (_ for _ in ()).throw(  # type: ignore[assignment]
                AssertionError("run_live_send should not be called in --doctor mode")
            )

            # Avoid hitting schtasks during test; doctor should treat this as a best-effort skip.
            run_wally_trial.query_task_to_run = lambda _task_name: None  # type: ignore[assignment]

            try:
                buf = io.StringIO()
                argv0 = sys.argv[:]
                sys.argv = ["run_wally_trial.py", str(cfg_path), "--doctor"]
                with redirect_stdout(buf):
                    with self.assertRaises(SystemExit) as cm:
                        run_wally_trial.main()
                self.assertEqual(cm.exception.code, 0)
                out = buf.getvalue()
                self.assertIn("DOCTOR_OK", out)
                self.assertNotIn("DOCTOR_FAIL", out)
            finally:
                for k, v in old_vals.items():
                    if v is None:
                        os.environ.pop(k, None)
                    else:
                        os.environ[k] = v
                run_wally_trial.run_preview_send = orig_preview  # type: ignore[assignment]
                run_wally_trial.run_live_send = orig_live  # type: ignore[assignment]
                run_wally_trial.query_task_to_run = orig_query  # type: ignore[assignment]
                sys.argv = argv0
