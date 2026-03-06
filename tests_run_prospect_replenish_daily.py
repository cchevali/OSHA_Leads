import io
import os
import subprocess
import unittest
from contextlib import redirect_stderr, redirect_stdout
from unittest import mock

from outreach import run_prospect_replenish_daily as replenish


class TestRunProspectReplenishDaily(unittest.TestCase):
    def _proc(self, code: int, stdout: str = "", stderr: str = "") -> subprocess.CompletedProcess:
        return subprocess.CompletedProcess(args=["mock"], returncode=code, stdout=stdout, stderr=stderr)

    def test_print_config_emits_default_sources_and_commands(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            buf = io.StringIO()
            with redirect_stdout(buf):
                rc = replenish.main(["--print-config"])
        out = buf.getvalue()
        self.assertEqual(rc, 0, msg=out)
        self.assertIn("PROSPECT_REPLENISH_EFFECTIVE_AUTOGROW_ENABLED=1", out)
        self.assertIn("PROSPECT_REPLENISH_EFFECTIVE_AUTOGROW_SOURCES=AIHA,OHS_BG", out)
        self.assertIn("PROSPECT_REPLENISH_EFFECTIVE_SAFETY_NET_ENABLED=1", out)
        self.assertIn("run_prospect_generation.py --doctor", out)
        self.assertIn("run_prospect_generation.py", out)
        self.assertIn("run_prospect_discovery.py", out)
        self.assertIn("PASS_PROSPECT_REPLENISH_PRINT_CONFIG status=OK", out)
        self.assertIn("PASS_PROSPECT_REPLENISH_COMPLETE status=PRINT_CONFIG", out)

    def test_live_mode_runs_doctor_then_generation_then_discovery(self):
        calls: list[list[str]] = []

        def _run(cmd, **_kwargs):  # type: ignore[no-untyped-def]
            parts = [str(c) for c in cmd]
            calls.append(parts)
            if "run_prospect_generation.py" in parts and "--doctor" in parts:
                return self._proc(0, stdout="PASS_DOCTOR_CRAWL4AI crawl4ai_installed=YES playwright_browsers_installed=YES\n")
            if "run_prospect_generation.py" in parts:
                return self._proc(
                    0,
                    stdout=(
                        "GENERATOR_AUTOGROW_SELECTED_STATE=TX\n"
                        "GENERATOR_AUTOGROW_BACKLOG_CURRENT=11\n"
                        "GENERATOR_AUTOGROW_NEW_NEEDED=49\n"
                        "GENERATOR_ROWS_WRITTEN=17\n"
                    ),
                )
            return self._proc(0, stdout="DISCOVERY_ROWS_READ=17\nDISCOVERY_PROSPECTS_UPSERTED=14\n")

        with mock.patch.object(replenish.subprocess, "run", side_effect=_run):
            buf = io.StringIO()
            with redirect_stdout(buf):
                rc = replenish.main([])
        out = buf.getvalue()
        self.assertEqual(rc, 0, msg=out)
        self.assertEqual(len(calls), 3, msg=str(calls))
        self.assertIn("run_prospect_generation.py", calls[0])
        self.assertIn("--doctor", calls[0])
        self.assertIn("run_prospect_generation.py", calls[1])
        self.assertNotIn("--doctor", calls[1])
        self.assertIn("run_prospect_discovery.py", calls[2])
        self.assertIn("PROSPECT_REPLENISH_SELECTED_STATE=TX", out)
        self.assertIn("PROSPECT_REPLENISH_BACKLOG_CURRENT=11", out)
        self.assertIn("PROSPECT_REPLENISH_NEW_NEEDED=49", out)
        self.assertIn("PROSPECT_REPLENISH_GENERATOR_ROWS_WRITTEN=17", out)
        self.assertIn("PROSPECT_REPLENISH_DISCOVERY_ROWS_READ=17", out)
        self.assertIn("PROSPECT_REPLENISH_DISCOVERY_PROSPECTS_UPSERTED=14", out)
        self.assertIn("PASS_PROSPECT_REPLENISH_COMPLETE status=OK", out)

    def test_dry_run_skips_live_discovery_import_and_never_calls_outreach(self):
        calls: list[list[str]] = []

        def _run(cmd, **_kwargs):  # type: ignore[no-untyped-def]
            parts = [str(c) for c in cmd]
            calls.append(parts)
            if "run_prospect_generation.py" in parts:
                return self._proc(
                    0,
                    stdout=(
                        "GENERATOR_AUTOGROW_SELECTED_STATE=CA\n"
                        "GENERATOR_AUTOGROW_BACKLOG_CURRENT=3\n"
                        "GENERATOR_AUTOGROW_NEW_NEEDED=57\n"
                        "GENERATOR_ROWS_WRITTEN=57\n"
                    ),
                )
            return self._proc(0, stdout="PASS_DISCOVERY_PRINT_CONFIG data_dir=C:\\osha_data\n")

        with mock.patch.object(replenish.subprocess, "run", side_effect=_run):
            buf = io.StringIO()
            with redirect_stdout(buf):
                rc = replenish.main(["--dry-run", "--for-date", "2026-03-05"])
        out = buf.getvalue()
        self.assertEqual(rc, 0, msg=out)
        self.assertEqual(len(calls), 2, msg=str(calls))
        self.assertIn("run_prospect_generation.py", calls[0])
        self.assertIn("--dry-run", calls[0])
        self.assertIn("--for-date", calls[0])
        self.assertIn("2026-03-05", calls[0])
        self.assertIn("run_prospect_discovery.py", calls[1])
        self.assertIn("--print-config", calls[1])
        self.assertNotIn("run_outreach_auto.py", " ".join(" ".join(c) for c in calls))
        self.assertIn("PROSPECT_REPLENISH_DISCOVERY_ROWS_READ=0", out)
        self.assertIn("PROSPECT_REPLENISH_DISCOVERY_PROSPECTS_UPSERTED=0", out)
        self.assertIn("PASS_PROSPECT_REPLENISH_COMPLETE status=DRY_RUN", out)

    def test_live_mode_fails_fast_on_stage_error(self):
        def _run(cmd, **_kwargs):  # type: ignore[no-untyped-def]
            parts = [str(c) for c in cmd]
            if "run_prospect_generation.py" in parts and "--doctor" in parts:
                return self._proc(1, stdout="", stderr="ERR_GENERATOR_FAILED stage=config\n")
            raise AssertionError("unexpected stage after doctor failure")

        with mock.patch.object(replenish.subprocess, "run", side_effect=_run):
            out_buf = io.StringIO()
            err_buf = io.StringIO()
            with redirect_stdout(out_buf), redirect_stderr(err_buf):
                rc = replenish.main([])
        self.assertEqual(rc, 2)
        self.assertIn("ERR_PROSPECT_REPLENISH_STAGE stage=doctor_generation code=1", err_buf.getvalue())


if __name__ == "__main__":
    unittest.main()
