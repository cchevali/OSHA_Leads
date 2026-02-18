import io
import os
import unittest
from contextlib import redirect_stdout
from unittest import mock

import run_osha_ingest_daily as ingest_daily


class TestRunOshaIngestDaily(unittest.TestCase):
    def _run(self, argv: list[str], env: dict[str, str | None] | None = None) -> tuple[int, str]:
        base_env = dict(os.environ)
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
        rc, out = self._run(["--print-config"], {"OUTREACH_STATES": "TX,CA,FL"})
        self.assertEqual(rc, 0, msg=out)
        self.assertIn("INGEST_DB_PATH=", out)
        self.assertIn("INGEST_STATES=TX,CA,FL", out)
        self.assertIn("INGEST_SINCE_DAYS=3", out)
        self.assertIn("INGEST_MAX_DETAILS=200", out)
        self.assertIn("PASS_INGEST_DAILY_COMPLETE status=PRINT_CONFIG", out)

    def test_dry_run_skips_ingestion_call(self):
        with mock.patch.object(ingest_daily.ingest_osha, "run_ingestion", side_effect=AssertionError("must not call")):
            rc, out = self._run(["--dry-run"], {"OUTREACH_STATES": "TX,CA,FL"})
        self.assertEqual(rc, 0, msg=out)
        self.assertIn("PASS_INGEST_DAILY_COMPLETE status=DRY_RUN", out)

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

    def test_missing_env_falls_back_to_tx(self):
        rc, out = self._run(["--print-config"], {"OUTREACH_STATES": None})
        self.assertEqual(rc, 0, msg=out)
        self.assertIn("INGEST_STATES=TX", out)
        self.assertIn("INGEST_STATES_SOURCE=fallback", out)
        self.assertIn("INGEST_STATES_FALLBACK_USED=YES", out)

    def test_invalid_override_emits_err_token(self):
        rc, out = self._run(["--print-config", "--states", "TX,F1"], {"OUTREACH_STATES": "TX"})
        self.assertNotEqual(rc, 0, msg=out)
        self.assertIn("ERR_INGEST_DAILY_CONFIG", out)


if __name__ == "__main__":
    unittest.main()
