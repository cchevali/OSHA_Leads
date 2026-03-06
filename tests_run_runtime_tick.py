import io
import os
import subprocess
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest import mock

from outreach import run_runtime_tick as tick


class _Preflight:
    def __init__(self, ok: bool = True):
        self.ok = ok


class TestRunRuntimeTick(unittest.TestCase):
    def _proc(self, code: int, stdout: str = "", stderr: str = "") -> subprocess.CompletedProcess:
        return subprocess.CompletedProcess(args=["mock"], returncode=code, stdout=stdout, stderr=stderr)

    def test_print_config_has_required_tokens(self):
        with tempfile.TemporaryDirectory() as d, mock.patch.dict(os.environ, {"DATA_DIR": d}, clear=False):
            buf = io.StringIO()
            with redirect_stdout(buf):
                rc = tick.main(["--print-config", "--job", "ingest_daily"])
        out = buf.getvalue()
        self.assertEqual(rc, 0, msg=out)
        self.assertIn("RUNTIME_TICK_REPO_ROOT=", out)
        self.assertIn(f"RUNTIME_TICK_DATA_DIR={Path(d).resolve()}", out)
        self.assertIn("RUNTIME_TICK_SELECTED_JOBS=ingest_daily", out)
        self.assertIn("PASS_RUNTIME_TICK_PRINT_CONFIG status=OK", out)
        self.assertIn("PASS_RUNTIME_TICK_COMPLETE status=PRINT_CONFIG", out)

    def test_doctor_trial_job_uses_trial_daily_doctor_flag(self):
        calls: list[list[str]] = []

        def _run(cmd, **_kwargs):  # type: ignore[no-untyped-def]
            parts = [str(c) for c in cmd]
            calls.append(parts)
            return self._proc(0, stdout="PASS_TRIAL_DAILY_DOCTOR status=OK\n")

        with (
            tempfile.TemporaryDirectory() as d,
            mock.patch.dict(os.environ, {"DATA_DIR": d}, clear=False),
            mock.patch.object(tick, "run_runtime_preflight", return_value=_Preflight(True)),
            mock.patch.object(tick, "render_runtime_lines", return_value=["PASS_RUNTIME_PREFLIGHT"]),
            mock.patch.object(tick.subprocess, "run", side_effect=_run),
        ):
            out_buf = io.StringIO()
            err_buf = io.StringIO()
            with redirect_stdout(out_buf), redirect_stderr(err_buf):
                rc = tick.main(["--doctor", "--job", "trial_facs_daily"])
        out = out_buf.getvalue() + "\n" + err_buf.getvalue()
        self.assertEqual(rc, 0, msg=out)
        self.assertTrue(calls, msg=out)
        joined = " ".join(calls[0])
        self.assertIn("run_trial_daily.py", joined)
        self.assertIn("--doctor", joined)
        self.assertIn("PASS_RUNTIME_TICK_DOCTOR status=OK", out)

    def test_live_mode_writes_state_and_next_run_skips_same_slot(self):
        calls: list[list[str]] = []

        def _run(cmd, **_kwargs):  # type: ignore[no-untyped-def]
            calls.append([str(c) for c in cmd])
            return self._proc(0, stdout="PASS_INGEST_DAILY_COMPLETE status=OK\n")

        with (
            tempfile.TemporaryDirectory() as d,
            mock.patch.dict(os.environ, {"DATA_DIR": d}, clear=False),
            mock.patch.object(tick, "run_runtime_preflight", return_value=_Preflight(True)),
            mock.patch.object(tick, "render_runtime_lines", return_value=["PASS_RUNTIME_PREFLIGHT"]),
            mock.patch.object(tick.subprocess, "run", side_effect=_run),
        ):
            first_buf = io.StringIO()
            with redirect_stdout(first_buf):
                rc1 = tick.main(["--job", "ingest_daily", "--force", "--now-local", "2026-03-09T06:50", "--mode", "scheduled"])
            first_out = first_buf.getvalue()
            self.assertEqual(rc1, 0, msg=first_out)
            self.assertIn("RUNTIME_TICK_JOB_RESULT=name=ingest_daily result=ran exit_code=0", first_out)

            second_buf = io.StringIO()
            with redirect_stdout(second_buf):
                rc2 = tick.main(["--job", "ingest_daily", "--now-local", "2026-03-09T06:51", "--mode", "scheduled"])
            second_out = second_buf.getvalue()
            self.assertEqual(rc2, 0, msg=second_out)
            self.assertIn("RUNTIME_TICK_JOB_RESULT=name=ingest_daily result=skipped exit_code=0 reason=already_ran", second_out)

        ingest_invocations = [parts for parts in calls if "run_osha_ingest_daily.py" in parts]
        self.assertEqual(len(ingest_invocations), 1, msg="second run should not invoke ingest command for same slot")

    def test_dry_run_fails_on_stage_error(self):
        def _run(_cmd, **_kwargs):  # type: ignore[no-untyped-def]
            return self._proc(1, stdout="", stderr="ERR_STAGE_FAILED\n")

        with (
            tempfile.TemporaryDirectory() as d,
            mock.patch.dict(os.environ, {"DATA_DIR": d}, clear=False),
            mock.patch.object(tick, "run_runtime_preflight", return_value=_Preflight(True)),
            mock.patch.object(tick, "render_runtime_lines", return_value=["PASS_RUNTIME_PREFLIGHT"]),
            mock.patch.object(tick.subprocess, "run", side_effect=_run),
        ):
            out_buf = io.StringIO()
            err_buf = io.StringIO()
            with redirect_stdout(out_buf), redirect_stderr(err_buf):
                rc = tick.main(["--dry-run", "--job", "ingest_daily", "--force"])
        self.assertEqual(rc, 2)
        self.assertIn("ERR_RUNTIME_TICK_STAGE", err_buf.getvalue())


if __name__ == "__main__":
    unittest.main()
