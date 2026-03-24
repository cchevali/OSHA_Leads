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
from runtime_schedule_config import write_runtime_schedule


class _Preflight:
    def __init__(self, ok: bool = True, trusted_scheduled: bool = False):
        self.ok = ok
        self.fingerprint = type("Fingerprint", (), {"trusted_scheduled": bool(trusted_scheduled)})()


class TestRunRuntimeTick(unittest.TestCase):
    def _proc(self, code: int, stdout: str = "", stderr: str = "") -> subprocess.CompletedProcess:
        return subprocess.CompletedProcess(args=["mock"], returncode=code, stdout=stdout, stderr=stderr)

    def _write_wrapper_summary(
        self,
        *,
        data_dir: Path,
        wrapper_name: str,
        slot_token: str,
        start_local: str,
        end_local: str,
        exit_code: int,
    ) -> Path:
        root = data_dir / "out" / "run_summaries"
        root.mkdir(parents=True, exist_ok=True)
        summary_path = root / f"{wrapper_name}_{slot_token}_000001.summary.json"
        payload = {
            "wrapper": wrapper_name,
            "start_local": start_local,
            "end_local": end_local,
            "exit_code": int(exit_code),
            "artifacts": {
                "task_log": str(data_dir / "out" / "task_logs" / f"{wrapper_name}_{slot_token}.log"),
                "summary_json": str(summary_path),
                "summary_text": str(root / f"{wrapper_name}_{slot_token}_000001.summary.txt"),
            },
        }
        summary_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
        return summary_path

    def test_print_config_has_required_tokens(self):
        with tempfile.TemporaryDirectory() as d, mock.patch.dict(os.environ, {"DATA_DIR": d}, clear=False):
            buf = io.StringIO()
            with redirect_stdout(buf):
                rc = tick.main(["--print-config", "--job", "ingest_daily"])
        out = buf.getvalue()
        self.assertEqual(rc, 0, msg=out)
        self.assertIn("RUNTIME_TICK_REPO_ROOT=", out)
        self.assertIn(f"RUNTIME_TICK_DATA_DIR={Path(d).resolve()}", out)
        self.assertIn("RUNTIME_TICK_PRIMARY_SCHEDULER=runtime_tick_selfhosted", out)
        self.assertIn(f"RUNTIME_TICK_CANONICAL_RUN_SUMMARY_ROOT={(Path(d).resolve() / 'out' / 'run_summaries').resolve()}", out)
        self.assertIn("RUNTIME_TICK_SELECTED_JOBS=ingest_daily", out)
        self.assertIn("PASS_RUNTIME_TICK_PRINT_CONFIG status=OK", out)
        self.assertIn("PASS_RUNTIME_TICK_COMPLETE status=PRINT_CONFIG", out)

    def test_print_config_uses_schedule_override_times_for_timed_jobs(self):
        with tempfile.TemporaryDirectory() as d, mock.patch.dict(os.environ, {"DATA_DIR": d}, clear=False):
            write_runtime_schedule(
                d,
                outreach_send_local_hhmm="10:10",
                trial_default_send_local_hhmm="11:11",
                evening_prep_local_hhmm="21:20",
                updated_by="unit_test",
            )

            outreach_buf = io.StringIO()
            with redirect_stdout(outreach_buf):
                outreach_rc = tick.main(["--print-config", "--job", "outreach_auto"])
            outreach_out = outreach_buf.getvalue()
            self.assertEqual(outreach_rc, 0, msg=outreach_out)
            self.assertIn("RUNTIME_TICK_SCHEDULE_SOURCE=file", outreach_out)
            self.assertIn("RUNTIME_TICK_SCHEDULE_OUTREACH_SEND_LOCAL_HHMM=10:10", outreach_out)
            self.assertIn("RUNTIME_TICK_JOB_TIME=10:10", outreach_out)

            trial_buf = io.StringIO()
            with redirect_stdout(trial_buf):
                trial_rc = tick.main(["--print-config", "--job", "trial_facs_daily"])
            trial_out = trial_buf.getvalue()
            self.assertEqual(trial_rc, 0, msg=trial_out)
            self.assertIn("RUNTIME_TICK_SCHEDULE_TRIAL_DEFAULT_SEND_LOCAL_HHMM=11:11", trial_out)
            self.assertIn("RUNTIME_TICK_JOB_TIME=11:11", trial_out)

            evening_buf = io.StringIO()
            with redirect_stdout(evening_buf):
                evening_rc = tick.main(["--print-config", "--job", "ingest_evening"])
            evening_out = evening_buf.getvalue()
            self.assertEqual(evening_rc, 0, msg=evening_out)
            self.assertIn("RUNTIME_TICK_SCHEDULE_EVENING_PREP_LOCAL_HHMM=21:20", evening_out)
            self.assertIn("RUNTIME_TICK_JOB_TIME=21:20", evening_out)

    def test_print_config_includes_ops_snapshot_and_cleanup_support_jobs(self):
        with tempfile.TemporaryDirectory() as d, mock.patch.dict(os.environ, {"DATA_DIR": d}, clear=False):
            snapshot_buf = io.StringIO()
            with redirect_stdout(snapshot_buf):
                snapshot_rc = tick.main(["--print-config", "--job", "ops_snapshot_daily"])
            snapshot_out = snapshot_buf.getvalue()
            self.assertEqual(snapshot_rc, 0, msg=snapshot_out)
            self.assertIn("RUNTIME_TICK_SELECTED_JOBS=ops_snapshot_daily", snapshot_out)
            self.assertIn("RUNTIME_TICK_JOB_TIME=09:30", snapshot_out)

            cleanup_buf = io.StringIO()
            with redirect_stdout(cleanup_buf):
                cleanup_rc = tick.main(["--print-config", "--job", "outreach_cleanup_daily"])
            cleanup_out = cleanup_buf.getvalue()
            self.assertEqual(cleanup_rc, 0, msg=cleanup_out)
            self.assertIn("RUNTIME_TICK_SELECTED_JOBS=outreach_cleanup_daily", cleanup_out)
            self.assertIn("RUNTIME_TICK_JOB_TIME=09:45", cleanup_out)

    def test_support_jobs_are_not_critical_missed_window_candidates(self):
        self.assertNotIn("ops_snapshot_daily", tick.CRITICAL_WINDOW_JOBS)
        self.assertNotIn("outreach_cleanup_daily", tick.CRITICAL_WINDOW_JOBS)

    def test_doctor_ops_snapshot_job_uses_print_config_then_dry_run(self):
        calls: list[list[str]] = []

        def _run(cmd, **_kwargs):  # type: ignore[no-untyped-def]
            parts = [str(c) for c in cmd]
            calls.append(parts)
            return self._proc(0, stdout="PASS_OPS_SNAPSHOT status=OK\n")

        with (
            tempfile.TemporaryDirectory() as d,
            mock.patch.dict(os.environ, {"DATA_DIR": d}, clear=False),
            mock.patch.object(tick, "run_runtime_preflight", return_value=_Preflight(True, trusted_scheduled=True)),
            mock.patch.object(tick, "render_runtime_lines", return_value=["PASS_RUNTIME_PREFLIGHT"]),
            mock.patch.object(tick.subprocess, "run", side_effect=_run),
        ):
            out_buf = io.StringIO()
            err_buf = io.StringIO()
            with redirect_stdout(out_buf), redirect_stderr(err_buf):
                rc = tick.main(["--doctor", "--job", "ops_snapshot_daily"])
        out = out_buf.getvalue() + "\n" + err_buf.getvalue()
        self.assertEqual(rc, 0, msg=out)
        job_calls = [parts for parts in calls if "run_ops_snapshot.py" in " ".join(parts)]
        self.assertEqual(len(job_calls), 2, msg=out)
        self.assertIn("--print-config", " ".join(job_calls[0]))
        self.assertIn("--dry-run", " ".join(job_calls[1]))
        self.assertIn("PASS_RUNTIME_TICK_DOCTOR status=OK", out)

    def test_doctor_outreach_cleanup_job_uses_guarded_cleanup_commands(self):
        calls: list[list[str]] = []

        def _run(cmd, **_kwargs):  # type: ignore[no-untyped-def]
            parts = [str(c) for c in cmd]
            calls.append(parts)
            return self._proc(0, stdout="PASS_OUTREACH_CLEANUP status=OK\n")

        with (
            tempfile.TemporaryDirectory() as d,
            mock.patch.dict(os.environ, {"DATA_DIR": d}, clear=False),
            mock.patch.object(tick, "run_runtime_preflight", return_value=_Preflight(True, trusted_scheduled=True)),
            mock.patch.object(tick, "render_runtime_lines", return_value=["PASS_RUNTIME_PREFLIGHT"]),
            mock.patch.object(tick.subprocess, "run", side_effect=_run),
        ):
            out_buf = io.StringIO()
            err_buf = io.StringIO()
            with redirect_stdout(out_buf), redirect_stderr(err_buf):
                rc = tick.main(["--doctor", "--job", "outreach_cleanup_daily"])
        out = out_buf.getvalue() + "\n" + err_buf.getvalue()
        self.assertEqual(rc, 0, msg=out)
        job_calls = [parts for parts in calls if "cleanup_outreach_dry_run_artifacts.py" in " ".join(parts)]
        self.assertEqual(len(job_calls), 2, msg=out)
        self.assertIn("--print-config", " ".join(job_calls[0]))
        self.assertIn("--dry-run", " ".join(job_calls[1]))
        self.assertIn("--retention-days 14", " ".join(job_calls[1]))
        self.assertIn("PASS_RUNTIME_TICK_DOCTOR status=OK", out)

    def test_doctor_evening_job_uses_existing_guarded_commands(self):
        calls: list[list[str]] = []

        def _run(cmd, **_kwargs):  # type: ignore[no-untyped-def]
            parts = [str(c) for c in cmd]
            calls.append(parts)
            return self._proc(0, stdout="PASS_RUNTIME_TICK_STAGE status=OK\n")

        with (
            tempfile.TemporaryDirectory() as d,
            mock.patch.dict(os.environ, {"DATA_DIR": d}, clear=False),
            mock.patch.object(tick, "run_runtime_preflight", return_value=_Preflight(True, trusted_scheduled=True)),
            mock.patch.object(tick, "render_runtime_lines", return_value=["PASS_RUNTIME_PREFLIGHT"]),
            mock.patch.object(tick.subprocess, "run", side_effect=_run),
        ):
            out_buf = io.StringIO()
            err_buf = io.StringIO()
            with redirect_stdout(out_buf), redirect_stderr(err_buf):
                rc = tick.main(["--doctor", "--job", "ingest_evening"])
        out = out_buf.getvalue() + "\n" + err_buf.getvalue()
        job_calls = [
            parts for parts in calls
            if any(
                marker in " ".join(parts)
                for marker in (
                    "run_osha_ingest_daily.py",
                    "dump_signals_for_ai_review.ps1",
                    "prepare_manual_prospect_research.ps1",
                )
            )
        ]
        self.assertEqual(rc, 0, msg=out)
        self.assertEqual(len(job_calls), 3, msg=out)
        self.assertIn("run_osha_ingest_daily.py", " ".join(job_calls[0]))
        self.assertIn("--doctor", " ".join(job_calls[0]))
        self.assertIn("--scope-mode outreach_plus_trial_live", " ".join(job_calls[0]))
        self.assertIn("dump_signals_for_ai_review.ps1", " ".join(job_calls[1]))
        self.assertIn("-PrintConfig", " ".join(job_calls[1]))
        self.assertIn("prepare_manual_prospect_research.ps1", " ".join(job_calls[2]))
        self.assertIn("-PrintConfig", " ".join(job_calls[2]))
        self.assertIn("PASS_RUNTIME_TICK_DOCTOR status=OK", out)

    def test_doctor_trial_job_uses_trial_daily_doctor_flag(self):
        calls: list[list[str]] = []

        def _run(cmd, **_kwargs):  # type: ignore[no-untyped-def]
            parts = [str(c) for c in cmd]
            calls.append(parts)
            return self._proc(0, stdout="PASS_TRIAL_DAILY_DOCTOR status=OK\n")

        with (
            tempfile.TemporaryDirectory() as d,
            mock.patch.dict(os.environ, {"DATA_DIR": d}, clear=False),
            mock.patch.object(tick, "run_runtime_preflight", return_value=_Preflight(True, trusted_scheduled=True)),
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

    def test_doctor_jl_safety_trial_job_targets_jl_safety_subscriber(self):
        calls: list[list[str]] = []

        def _run(cmd, **_kwargs):  # type: ignore[no-untyped-def]
            parts = [str(c) for c in cmd]
            calls.append(parts)
            return self._proc(0, stdout="PASS_TRIAL_DAILY_DOCTOR status=OK\n")

        with (
            tempfile.TemporaryDirectory() as d,
            mock.patch.dict(os.environ, {"DATA_DIR": d}, clear=False),
            mock.patch.object(tick, "run_runtime_preflight", return_value=_Preflight(True, trusted_scheduled=True)),
            mock.patch.object(tick, "render_runtime_lines", return_value=["PASS_RUNTIME_PREFLIGHT"]),
            mock.patch.object(tick.subprocess, "run", side_effect=_run),
        ):
            out_buf = io.StringIO()
            err_buf = io.StringIO()
            with redirect_stdout(out_buf), redirect_stderr(err_buf):
                rc = tick.main(["--doctor", "--job", "trial_jl_safety_daily"])
        out = out_buf.getvalue() + "\n" + err_buf.getvalue()
        self.assertEqual(rc, 0, msg=out)
        self.assertTrue(calls, msg=out)
        joined = " ".join(calls[0])
        self.assertIn("run_trial_daily.py", joined)
        self.assertIn("--subscriber-key jl_safety_trial", joined)
        self.assertIn("--doctor", joined)
        self.assertIn("PASS_RUNTIME_TICK_DOCTOR status=OK", out)

    def test_doctor_roi_safety_trial_job_targets_roi_safety_subscriber(self):
        calls: list[list[str]] = []

        def _run(cmd, **_kwargs):  # type: ignore[no-untyped-def]
            parts = [str(c) for c in cmd]
            calls.append(parts)
            return self._proc(0, stdout="PASS_TRIAL_DAILY_DOCTOR status=OK\n")

        with (
            tempfile.TemporaryDirectory() as d,
            mock.patch.dict(os.environ, {"DATA_DIR": d}, clear=False),
            mock.patch.object(tick, "run_runtime_preflight", return_value=_Preflight(True, trusted_scheduled=True)),
            mock.patch.object(tick, "render_runtime_lines", return_value=["PASS_RUNTIME_PREFLIGHT"]),
            mock.patch.object(tick.subprocess, "run", side_effect=_run),
        ):
            out_buf = io.StringIO()
            err_buf = io.StringIO()
            with redirect_stdout(out_buf), redirect_stderr(err_buf):
                rc = tick.main(["--doctor", "--job", "trial_roi_safety_daily"])
        out = out_buf.getvalue() + "\n" + err_buf.getvalue()
        self.assertEqual(rc, 0, msg=out)
        self.assertTrue(calls, msg=out)
        joined = " ".join(calls[0])
        self.assertIn("run_trial_daily.py", joined)
        self.assertIn("--subscriber-key roi_safety_trial", joined)
        self.assertIn("--doctor", joined)
        self.assertIn("PASS_RUNTIME_TICK_DOCTOR status=OK", out)

    def test_live_mode_propagates_scheduled_env_to_child_commands(self):
        seen_envs: list[dict[str, str]] = []

        def _run(cmd, **kwargs):  # type: ignore[no-untyped-def]
            env = kwargs.get("env") or {}
            seen_envs.append(
                {
                    "command": " ".join(str(part) for part in cmd),
                    "MFO_RUNTIME_MODE": str(env.get("MFO_RUNTIME_MODE") or ""),
                    "MFO_TRUSTED_SCHEDULED": str(env.get("MFO_TRUSTED_SCHEDULED") or ""),
                }
            )
            return self._proc(0, stdout="PASS_TRIAL_DAILY_DOCTOR status=OK\n")

        with (
            tempfile.TemporaryDirectory() as d,
            mock.patch.dict(os.environ, {"DATA_DIR": d}, clear=False),
            mock.patch.object(tick, "run_runtime_preflight", return_value=_Preflight(True, trusted_scheduled=True)),
            mock.patch.object(tick, "render_runtime_lines", return_value=["PASS_RUNTIME_PREFLIGHT"]),
            mock.patch.object(tick.subprocess, "run", side_effect=_run),
        ):
            out_buf = io.StringIO()
            err_buf = io.StringIO()
            with redirect_stdout(out_buf), redirect_stderr(err_buf):
                rc = tick.main(["--job", "trial_facs_daily", "--force", "--mode", "scheduled"])
        out = out_buf.getvalue() + "\n" + err_buf.getvalue()
        self.assertEqual(rc, 0, msg=out)
        child_env = next((item for item in seen_envs if "run_trial_daily.py" in item["command"]), {})
        self.assertEqual(child_env.get("MFO_RUNTIME_MODE"), "scheduled")
        self.assertEqual(child_env.get("MFO_TRUSTED_SCHEDULED"), "1")

    def test_trusted_scheduled_reconciliation_ignores_repo_run_summary_fallback(self):
        with tempfile.TemporaryDirectory() as d, tempfile.TemporaryDirectory() as repo_tmp:
            data_dir = Path(d)
            repo_root = Path(repo_tmp)
            repo_root.mkdir(parents=True, exist_ok=True)
            (repo_root / "run_with_secrets.ps1").write_text("", encoding="utf-8")
            repo_summary_root = repo_root / "out" / "run_summaries"
            repo_summary_root.mkdir(parents=True, exist_ok=True)
            repo_summary_path = repo_summary_root / "OSHA_Outreach_Auto_20260309_000001.summary.json"
            repo_summary_path.write_text(
                json.dumps(
                    {
                        "wrapper": "OSHA_Outreach_Auto",
                        "start_local": "2026-03-09T10:30:00-04:00",
                        "end_local": "2026-03-09T10:35:00-04:00",
                        "exit_code": 0,
                        "artifacts": {
                            "task_log": str(repo_root / "out" / "task_logs" / "OSHA_Outreach_Auto_20260309.log"),
                            "summary_json": str(repo_summary_path),
                            "summary_text": str(repo_summary_root / "OSHA_Outreach_Auto_20260309_000001.summary.txt"),
                        },
                    },
                    indent=2,
                )
                + "\n",
                encoding="utf-8",
            )

            out_buf = io.StringIO()
            with (
                mock.patch.dict(
                    os.environ,
                    {
                        "DATA_DIR": str(data_dir),
                        "MFO_TRUSTED_SCHEDULED": "1",
                        "OSHA_SMOKE_TO": "ops@example.com",
                        "SMTP_HOST": "smtp.example.com",
                        "SMTP_PORT": "587",
                        "SMTP_USER": "bot@example.com",
                        "SMTP_PASS": "secret",
                    },
                    clear=False,
                ),
                mock.patch.object(tick, "_repo_root", return_value=repo_root),
                mock.patch.object(tick, "run_runtime_preflight", return_value=_Preflight(True, trusted_scheduled=True)),
                mock.patch.object(tick, "render_runtime_lines", return_value=["PASS_RUNTIME_PREFLIGHT"]),
                mock.patch.object(tick, "send_plain_text_alert") as send_mock,
                redirect_stdout(out_buf),
            ):
                rc = tick.main(["--job", "outreach_auto", "--now-local", "2026-03-09T12:30", "--mode", "scheduled"])
            out = out_buf.getvalue()
            self.assertEqual(rc, 0, msg=out)
            self.assertIn("RUNTIME_TICK_REPO_RUN_SUMMARY_FALLBACK_ALLOWED=0", out)
            self.assertIn("RUNTIME_TICK_ALERT_CANDIDATE=name=outreach_auto category=missed_window send=1 reason=ready_to_send", out)
            send_mock.assert_called_once()

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

    def test_doctor_inbound_triage_runs_when_imap_is_configured_from_saved_mailbox_env(self):
        calls: list[list[str]] = []

        def _run(cmd, **_kwargs):  # type: ignore[no-untyped-def]
            parts = [str(c) for c in cmd]
            calls.append(parts)
            return self._proc(0, stdout="PASS_INBOUND_TRIAGE status=OK\n")

        with tempfile.TemporaryDirectory() as d:
            repo_root = Path(d)
            (repo_root / "run_with_secrets.ps1").write_text("", encoding="utf-8")
            data_dir = repo_root / "data"
            data_dir.mkdir(parents=True, exist_ok=True)
            out_buf = io.StringIO()
            with (
                mock.patch.dict(
                    os.environ,
                    {
                        "DATA_DIR": str(data_dir),
                        "BOUNCE_IMAP_USER": "ops@example.com",
                        "BOUNCE_IMAP_PASS": "secret",
                    },
                    clear=True,
                ),
                mock.patch.object(tick, "_repo_root", return_value=repo_root),
                mock.patch.object(tick, "_git_sha", return_value="deadbeef"),
                mock.patch.object(tick, "run_runtime_preflight", return_value=_Preflight(True)),
                mock.patch.object(tick, "render_runtime_lines", return_value=["PASS_RUNTIME_PREFLIGHT"]),
                mock.patch.object(tick.subprocess, "run", side_effect=_run),
                redirect_stdout(out_buf),
            ):
                rc = tick.main(["--doctor", "--job", "inbound_triage"])
        out = out_buf.getvalue()
        self.assertEqual(rc, 0, msg=out)
        self.assertTrue(calls, msg=out)
        joined = " ".join(calls[0])
        self.assertIn("inbound_inbox_triage.py", joined)
        self.assertIn("--run-once", joined)
        self.assertIn("--dry-run", joined)
        self.assertNotIn("gmail_credentials_missing", out)
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
            mock.patch.object(tick, "run_runtime_preflight", return_value=_Preflight(True, trusted_scheduled=True)),
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

    def test_live_skipped_job_persists_state_json(self):
        with (
            tempfile.TemporaryDirectory() as d,
            mock.patch.dict(os.environ, {"DATA_DIR": d}, clear=False),
            mock.patch.object(tick, "run_runtime_preflight", return_value=_Preflight(True)),
            mock.patch.object(tick, "render_runtime_lines", return_value=["PASS_RUNTIME_PREFLIGHT"]),
        ):
            out_buf = io.StringIO()
            with redirect_stdout(out_buf):
                rc = tick.main(["--job", "outreach_auto", "--now-local", "2026-03-09T12:30", "--mode", "scheduled"])
            out = out_buf.getvalue()
            self.assertEqual(rc, 0, msg=out)
            state_path = Path(d) / "runtime" / "status" / "jobs" / "outreach_auto.json"
            self.assertTrue(state_path.exists(), msg=out)
            payload = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertEqual(payload.get("last_slot_key"), "2026-03-09")
            self.assertEqual(payload.get("last_result"), "skipped")
            self.assertEqual(payload.get("last_reason"), "window_closed_180m")
            self.assertIn("T08:00:00", str(payload.get("last_scheduled_local") or ""))

    def test_wrapper_success_within_window_reconciles_and_suppresses_missed_window(self):
        send_calls: list[dict[str, str]] = []

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
            mock.patch.object(tick, "send_plain_text_alert", side_effect=_send_alert),
        ):
            data_dir = Path(d)
            self._write_wrapper_summary(
                data_dir=data_dir,
                wrapper_name="OSHA_Outreach_Auto_SafetyNet",
                slot_token="20260309",
                start_local="2026-03-09T10:30:00-04:00",
                end_local="2026-03-09T10:35:00-04:00",
                exit_code=0,
            )
            out_buf = io.StringIO()
            with redirect_stdout(out_buf):
                rc = tick.main(["--job", "outreach_auto", "--now-local", "2026-03-09T12:30", "--mode", "scheduled"])
            out = out_buf.getvalue()
            self.assertEqual(rc, 0, msg=out)
            self.assertIn("WARN_RUNTIME_TICK_EXTERNAL_SCHEDULER=job=outreach_auto slot=2026-03-09 timing=within_window", out)
            self.assertIn("RUNTIME_TICK_ALERT_SKIPPED=reason=no_candidates", out)
            self.assertEqual(send_calls, [])
            payload = json.loads((Path(d) / "runtime" / "status" / "jobs" / "outreach_auto.json").read_text(encoding="utf-8"))
            self.assertEqual(payload.get("last_result"), "ran")
            self.assertEqual(payload.get("last_result_detail"), "reconciled")
            self.assertEqual(payload.get("last_reconciliation_status"), "external_wrapper_success_within_window")
            self.assertEqual(int(payload.get("last_external_scheduler_detected") or 0), 1)

    def test_wrapper_success_after_window_still_alerts_with_evidence(self):
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
            data_dir = Path(d)
            summary_path = self._write_wrapper_summary(
                data_dir=data_dir,
                wrapper_name="OSHA_Outreach_Auto_SafetyNet",
                slot_token="20260309",
                start_local="2026-03-09T11:30:00-04:00",
                end_local="2026-03-09T11:35:00-04:00",
                exit_code=0,
            )
            out_buf = io.StringIO()
            with redirect_stdout(out_buf):
                rc = tick.main(["--job", "outreach_auto", "--now-local", "2026-03-09T12:30", "--mode", "scheduled"])
            out = out_buf.getvalue()
            self.assertEqual(rc, 0, msg=out)
            self.assertEqual(len(sent), 1, msg=out)
            self.assertIn("reconciliation_status: external_wrapper_success_late", sent[0]["body"])
            self.assertIn(str(summary_path), sent[0]["body"])
            marker_files = list((Path(d) / "runtime" / "status" / "alerts").glob("*.json"))
            self.assertEqual(len(marker_files), 1)
            marker_payload = json.loads(marker_files[0].read_text(encoding="utf-8"))
            self.assertEqual(marker_payload.get("reconciliation_status"), "external_wrapper_success_late")
            self.assertTrue(str(marker_payload.get("run_summary_json_path") or "").endswith(".summary.json"))

    def test_failed_wrapper_evidence_does_not_suppress_missed_window(self):
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
            data_dir = Path(d)
            self._write_wrapper_summary(
                data_dir=data_dir,
                wrapper_name="OSHA_Outreach_Auto_SafetyNet",
                slot_token="20260309",
                start_local="2026-03-09T10:30:00-04:00",
                end_local="2026-03-09T10:31:00-04:00",
                exit_code=1,
            )
            out_buf = io.StringIO()
            with redirect_stdout(out_buf):
                rc = tick.main(["--job", "outreach_auto", "--now-local", "2026-03-09T12:30", "--mode", "scheduled"])
            out = out_buf.getvalue()
            self.assertEqual(rc, 0, msg=out)
            self.assertIn("RUNTIME_TICK_ALERT_CANDIDATE=name=outreach_auto category=job_failure send=1 reason=ready_to_send", out)
            self.assertEqual(len(sent), 1, msg=out)
            self.assertIn("[OSHA Runtime Failure]", sent[0]["subject"])
            self.assertIn("reason: external_wrapper_failed", sent[0]["body"])
            self.assertIn("reconciliation_status: external_wrapper_failed", sent[0]["body"])
            payload = json.loads((Path(d) / "runtime" / "status" / "jobs" / "outreach_auto.json").read_text(encoding="utf-8"))
            self.assertEqual(payload.get("last_result"), "skipped")
            self.assertEqual(payload.get("last_reconciliation_status"), "external_wrapper_failed")

    def test_legacy_wrapper_names_still_reconcile_for_backward_compatibility(self):
        with tempfile.TemporaryDirectory() as d:
            data_dir = Path(d)
            self._write_wrapper_summary(
                data_dir=data_dir,
                wrapper_name="OSHA_Prospect_Replenish_Daily",
                slot_token="20260309",
                start_local="2026-03-09T07:20:00-04:00",
                end_local="2026-03-09T07:25:00-04:00",
                exit_code=0,
            )
            evidence = tick._find_wrapper_run_evidence_for_slot(
                repo_root=data_dir,
                data_dir=data_dir,
                env={"DATA_DIR": str(data_dir), "MFO_TRUSTED_SCHEDULED": "1"},
                job_name="prospect_replenish_daily",
                slot_key="2026-03-09",
                scheduled_local="2026-03-09T07:15:00-04:00",
            )
        self.assertIsNotNone(evidence)
        assert evidence is not None
        self.assertEqual(evidence.wrapper_name, "OSHA_Prospect_Replenish_Daily")

    def test_weekday_only_skip_persists_state_without_alert_spam(self):
        with (
            tempfile.TemporaryDirectory() as d,
            mock.patch.dict(os.environ, {"DATA_DIR": d}, clear=False),
            mock.patch.object(tick, "run_runtime_preflight", return_value=_Preflight(True)),
            mock.patch.object(tick, "render_runtime_lines", return_value=["PASS_RUNTIME_PREFLIGHT"]),
            mock.patch.object(tick, "send_plain_text_alert") as send_mock,
        ):
            out_buf = io.StringIO()
            with redirect_stdout(out_buf):
                rc = tick.main(["--job", "outreach_auto", "--now-local", "2026-03-07T12:30", "--mode", "scheduled"])
            out = out_buf.getvalue()
            self.assertEqual(rc, 0, msg=out)
            send_mock.assert_not_called()
            self.assertIn("RUNTIME_TICK_ALERT_SKIPPED=reason=no_candidates", out)
            payload = json.loads((Path(d) / "runtime" / "status" / "jobs" / "outreach_auto.json").read_text(encoding="utf-8"))
            self.assertEqual(payload.get("last_reason"), "weekday_only")
            self.assertEqual(payload.get("last_result"), "skipped")

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
