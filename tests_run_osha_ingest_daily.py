import io
import os
import unittest
from contextlib import redirect_stdout
from unittest import mock

import run_osha_ingest_daily as ingest_daily


class TestRunOshaIngestDaily(unittest.TestCase):
    def _run(self, argv: list[str], env: dict[str, str | None] | None = None) -> tuple[int, str]:
        base_env = dict(os.environ)
        for key in ("DATA_DIR", "MFO_DATA_DIR_EFFECTIVE", "MFO_DATA_DIR_SOURCE"):
            base_env.pop(key, None)
        for key, value in (env or {}).items():
            if value is None:
                base_env.pop(key, None)
            else:
                base_env[key] = value
        with mock.patch.dict(os.environ, base_env, clear=True):
            buf = io.StringIO()
            with redirect_stdout(buf):
                rc = ingest_daily.main(argv)
            return rc, buf.getvalue()

    def test_print_config_emits_required_tokens(self):
        rc, out = self._run(["--print-config"], {"OUTREACH_STATES": "TX,CA,FL,PA,OH"})
        self.assertEqual(rc, 0, msg=out)
        self.assertIn("INGEST_DB_PATH=", out)
        self.assertIn("INGEST_DB_SOURCE=data_dir", out)
        self.assertIn("INGEST_SCOPE_MODE=outreach", out)
        self.assertIn("INGEST_SCOPE_STATES=TX,CA,FL,PA,OH", out)
        self.assertIn("INGEST_SCOPE_SOURCE=outreach", out)
        self.assertIn("INGEST_STATES=TX,CA,FL,PA,OH", out)
        self.assertIn("INGEST_SINCE_DAYS=3", out)
        self.assertIn("INGEST_MAX_DETAILS=200", out)
        self.assertIn("INGEST_RUNTIME_ROLE=", out)
        self.assertIn("INGEST_CANONICAL_HOSTNAME=", out)
        self.assertIn("INGEST_ARTIFACT_SYNC_DIR=", out)
        self.assertIn("INGEST_TASK_LOG_ROOT=", out)
        self.assertIn("INGEST_RUN_SUMMARY_ROOT=", out)
        self.assertIn("PASS_INGEST_DAILY_COMPLETE status=PRINT_CONFIG", out)

    def test_dry_run_skips_ingestion_call(self):
        with mock.patch.object(ingest_daily.ingest_osha, "run_ingestion", side_effect=AssertionError("must not call")):
            rc, out = self._run(["--dry-run"], {"OUTREACH_STATES": "TX,CA,FL,PA,OH"})
        self.assertEqual(rc, 0, msg=out)
        self.assertIn("PASS_INGEST_DAILY_COMPLETE status=DRY_RUN", out)

    def test_doctor_emits_runtime_lines_and_pass_token(self):
        rc, out = self._run(["--doctor"], {"OUTREACH_STATES": "TX"})
        self.assertEqual(rc, 0, msg=out)
        self.assertIn("PASS_RUNTIME_PREFLIGHT", out)
        self.assertIn("PASS_INGEST_DAILY_DOCTOR status=OK", out)
        self.assertIn("PASS_INGEST_DAILY_COMPLETE status=DOCTOR", out)

    def test_default_state_resolution_from_outreach_states(self):
        rc, out = self._run(["--print-config"], {"OUTREACH_STATES": "FL,CA,TX"})
        self.assertEqual(rc, 0, msg=out)
        self.assertIn("INGEST_STATES=FL,CA,TX", out)
        self.assertIn("INGEST_STATES_SOURCE=env", out)
        self.assertIn("INGEST_STATES_FALLBACK_USED=NO", out)

    def test_states_override_takes_precedence(self):
        rc, out = self._run(["--print-config", "--states", "FL,CA,FL"], {"OUTREACH_STATES": "TX"})
        self.assertEqual(rc, 0, msg=out)
        self.assertIn("INGEST_STATES=FL,CA", out)
        self.assertIn("INGEST_STATES_SOURCE=cli", out)

    def test_missing_env_falls_back_to_default_live_scope(self):
        rc, out = self._run(["--print-config"], {"OUTREACH_STATES": None})
        self.assertEqual(rc, 0, msg=out)
        self.assertIn("INGEST_STATES=TX,CA,FL,PA,OH,IL,NJ,LA,MI,GA,AL,WI,TN", out)
        self.assertIn("INGEST_STATES_SOURCE=fallback", out)
        self.assertIn("INGEST_STATES_FALLBACK_USED=YES", out)

    def test_invalid_override_emits_err_token(self):
        rc, out = self._run(["--print-config", "--states", "TX,F1"], {"OUTREACH_STATES": "TX"})
        self.assertNotEqual(rc, 0, msg=out)
        self.assertIn("ERR_INGEST_DAILY_CONFIG", out)

    def test_modes_are_mutually_exclusive(self):
        rc, out = self._run(["--doctor", "--dry-run"], {"OUTREACH_STATES": "TX"})
        self.assertNotEqual(rc, 0, msg=out)
        self.assertIn("ERR_INGEST_DAILY_CONFIG", out)
        self.assertIn("modes_mutually_exclusive", out)

    def test_scope_mode_resolver_unions_outreach_and_trial_live_states(self):
        with mock.patch.object(ingest_daily, "_trial_live_states_from_crm", return_value=["OR", "WA", "FL"]):
            rc, out = self._run(
                ["--print-config", "--scope-mode", "outreach_plus_trial_live"],
                {"OUTREACH_STATES": "TX,CA,FL,PA,OH"},
            )
        self.assertEqual(rc, 0, msg=out)
        self.assertIn("INGEST_SCOPE_MODE=outreach_plus_trial_live", out)
        self.assertIn("INGEST_SCOPE_SOURCE=resolver", out)
        self.assertIn("INGEST_SCOPE_STATES=TX,CA,FL,PA,OH,OR,WA", out)
        self.assertIn("INGEST_STATES=TX,CA,FL,PA,OH,OR,WA", out)
        self.assertIn("INGEST_STATES_SOURCE=resolver", out)

    def test_print_config_uses_data_dir_backed_osha_db_when_configured(self):
        rc, out = self._run(["--print-config"], {"OUTREACH_STATES": "TX", "DATA_DIR": r"C:\osha_data"})
        self.assertEqual(rc, 0, msg=out)
        self.assertIn(r"INGEST_DB_PATH=C:\osha_data\osha.sqlite", out)
        self.assertIn("INGEST_DB_SOURCE=data_dir", out)


if __name__ == "__main__":
    unittest.main()
