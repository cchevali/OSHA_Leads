import io
import json
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

    def test_doctor_inbound_triage_skips_when_gmail_credentials_missing(self):
        with tempfile.TemporaryDirectory() as d:
            repo_root = Path(d)
            (repo_root / "run_with_secrets.ps1").write_text("", encoding="utf-8")
            data_dir = repo_root / "data"
            data_dir.mkdir(parents=True, exist_ok=True)
            out_buf = io.StringIO()
            with (
                mock.patch.dict(os.environ, {"DATA_DIR": str(data_dir)}, clear=False),
                mock.patch.object(tick, "_repo_root", return_value=repo_root),
                mock.patch.object(tick, "_git_sha", return_value="deadbeef"),
                mock.patch.object(tick, "run_runtime_preflight", return_value=_Preflight(True)),
                mock.patch.object(tick, "render_runtime_lines", return_value=["PASS_RUNTIME_PREFLIGHT"]),
                mock.patch.object(tick.subprocess, "run") as mocked_run,
                redirect_stdout(out_buf),
            ):
                rc = tick.main(["--doctor", "--job", "inbound_triage"])
        out = out_buf.getvalue()
        self.assertEqual(rc, 0, msg=out)
        mocked_run.assert_not_called()
        self.assertIn(
            "RUNTIME_TICK_JOB_RESULT=name=inbound_triage result=skipped exit_code=0 reason=gmail_credentials_missing",
            out,
        )
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

    def test_live_failure_alert_sends_once_and_dedupes(self):
        send_calls: list[dict[str, str]] = []

        def _run(_cmd, **_kwargs):  # type: ignore[no-untyped-def]
            return self._proc(1, stdout="", stderr="ERR_STAGE_FAILED\n")

        def _send_alert(*, recipient: str, subject: str, body: str, env):  # type: ignore[no-untyped-def]
            send_calls.append({"recipient": recipient, "subject": subject, "body": body})

        env = {
            "OSHA_SMOKE_TO": "ops@example.com",
            "SMTP_HOST": "smtp.example.com",
            "SMTP_PORT": "587",
            "SMTP_USER": "bot@example.com",
            "SMTP_PASS": "secret",
        }

        with (
            tempfile.TemporaryDirectory() as d,
            mock.patch.dict(os.environ, {"DATA_DIR": d, **env}, clear=False),
            mock.patch.object(tick, "run_runtime_preflight", return_value=_Preflight(True)),
            mock.patch.object(tick, "render_runtime_lines", return_value=["PASS_RUNTIME_PREFLIGHT"]),
            mock.patch.object(tick.subprocess, "run", side_effect=_run),
            mock.patch.object(tick, "send_plain_text_alert", side_effect=_send_alert),
        ):
            out_first = io.StringIO()
            err_first = io.StringIO()
            with redirect_stdout(out_first), redirect_stderr(err_first):
                rc1 = tick.main(["--job", "ingest_daily", "--force", "--now-local", "2026-03-09T06:50", "--mode", "scheduled"])
            self.assertEqual(rc1, 2, msg=out_first.getvalue() + "\n" + err_first.getvalue())
            self.assertIn("RUNTIME_TICK_ALERT_SENT=count=1 recipient=ops@example.com", out_first.getvalue())

            out_second = io.StringIO()
            err_second = io.StringIO()
            with redirect_stdout(out_second), redirect_stderr(err_second):
                rc2 = tick.main(["--job", "ingest_daily", "--force", "--now-local", "2026-03-09T06:50", "--mode", "scheduled"])
            self.assertEqual(rc2, 2, msg=out_second.getvalue() + "\n" + err_second.getvalue())
            self.assertIn("RUNTIME_TICK_ALERT_SKIPPED=name=ingest_daily category=job_failure reason=duplicate", out_second.getvalue())

            status_path = Path(d) / "runtime" / "status" / "runtime_latest.json"
            payload = json.loads(status_path.read_text(encoding="utf-8"))
            self.assertIn("alerts", payload)
            self.assertEqual(int(payload["alerts"]["alerts_sent"]), 0)
            self.assertGreaterEqual(int(payload["alerts"]["alerts_skipped"]), 1)

        self.assertEqual(len(send_calls), 1)

    def test_missed_window_alert_sends_for_critical_daily_job(self):
        sent: list[dict[str, str]] = []

        def _send_alert(*, recipient: str, subject: str, body: str, env):  # type: ignore[no-untyped-def]
            sent.append({"recipient": recipient, "subject": subject, "body": body})

        env = {
            "OSHA_SMOKE_TO": "ops@example.com",
            "SMTP_HOST": "smtp.example.com",
            "SMTP_PORT": "587",
            "SMTP_USER": "bot@example.com",
            "SMTP_PASS": "secret",
        }

        with (
            tempfile.TemporaryDirectory() as d,
            mock.patch.dict(os.environ, {"DATA_DIR": d, **env}, clear=False),
            mock.patch.object(tick, "run_runtime_preflight", return_value=_Preflight(True)),
            mock.patch.object(tick, "render_runtime_lines", return_value=["PASS_RUNTIME_PREFLIGHT"]),
            mock.patch.object(tick, "send_plain_text_alert", side_effect=_send_alert),
        ):
            out_buf = io.StringIO()
            err_buf = io.StringIO()
            with redirect_stdout(out_buf), redirect_stderr(err_buf):
                rc = tick.main(["--job", "outreach_auto", "--now-local", "2026-03-09T12:30", "--mode", "scheduled"])
        self.assertEqual(rc, 0, msg=out_buf.getvalue() + "\n" + err_buf.getvalue())
        out = out_buf.getvalue()
        self.assertIn("RUNTIME_TICK_JOB_RESULT=name=outreach_auto result=skipped exit_code=0 reason=window_closed_180m", out)
        self.assertIn("RUNTIME_TICK_ALERT_CANDIDATE=name=outreach_auto category=missed_window send=1 reason=ready_to_send", out)
        self.assertEqual(len(sent), 1)

    def test_dry_run_failure_alert_is_non_live_candidate_only(self):
        def _run(_cmd, **_kwargs):  # type: ignore[no-untyped-def]
            return self._proc(1, stdout="", stderr="ERR_STAGE_FAILED\n")

        with (
            tempfile.TemporaryDirectory() as d,
            mock.patch.dict(
                os.environ,
                {
                    "DATA_DIR": d,
                    "OSHA_SMOKE_TO": "ops@example.com",
                    "SMTP_HOST": "smtp.example.com",
                    "SMTP_PORT": "587",
                    "SMTP_USER": "bot@example.com",
                    "SMTP_PASS": "secret",
                },
                clear=False,
            ),
            mock.patch.object(tick, "run_runtime_preflight", return_value=_Preflight(True)),
            mock.patch.object(tick, "render_runtime_lines", return_value=["PASS_RUNTIME_PREFLIGHT"]),
            mock.patch.object(tick.subprocess, "run", side_effect=_run),
            mock.patch.object(tick, "send_plain_text_alert") as send_mock,
        ):
            out_buf = io.StringIO()
            err_buf = io.StringIO()
            with redirect_stdout(out_buf), redirect_stderr(err_buf):
                rc = tick.main(["--dry-run", "--job", "ingest_daily", "--force"])
        self.assertEqual(rc, 2)
        out = out_buf.getvalue()
        self.assertIn("RUNTIME_TICK_ALERT_CANDIDATE=name=ingest_daily category=job_failure send=0 reason=non_live_mode", out)
        self.assertIn("RUNTIME_TICK_ALERT_SKIPPED=name=ingest_daily category=job_failure reason=non_live_mode", out)
        send_mock.assert_not_called()


if __name__ == "__main__":
    unittest.main()
