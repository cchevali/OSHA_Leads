import re
import os
import subprocess
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent
SCRIPT = REPO_ROOT / "scripts" / "install_scheduled_tasks.ps1"
EXPECTED_REPLENISH_TR = (
    "powershell.exe -NoProfile -ExecutionPolicy Bypass -File "
    r"C:\dev\OSHA_Leads\scripts\scheduled\run_prospect_replenish_daily.ps1"
)
EXPECTED_INGEST_TR = (
    "powershell.exe -NoProfile -ExecutionPolicy Bypass -File "
    r"C:\dev\OSHA_Leads\scripts\scheduled\run_osha_ingest_daily.ps1"
)
EXPECTED_OUTREACH_TR = (
    "powershell.exe -NoProfile -ExecutionPolicy Bypass -File "
    r"C:\dev\OSHA_Leads\scripts\scheduled\run_outreach_auto.ps1"
)
EXPECTED_INBOUND_TR = (
    "powershell.exe -NoProfile -ExecutionPolicy Bypass -File "
    r"C:\dev\OSHA_Leads\scripts\scheduled\run_inbound_triage.ps1"
)
EXPECTED_FACS_TRIAL_TR = (
    "powershell.exe -NoProfile -ExecutionPolicy Bypass -File "
    r"C:\dev\OSHA_Leads\scripts\scheduled\run_trial_facs_daily.ps1"
)


def _run(*args: str, extra_env: dict[str, str] | None = None) -> subprocess.CompletedProcess:
    env = os.environ.copy()
    if extra_env:
        env.update(extra_env)
    return subprocess.run(
        [
            "powershell",
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(SCRIPT),
            *args,
        ],
        cwd=str(REPO_ROOT),
        env=env,
        capture_output=True,
        text=True,
    )


def _parse_task_config(output: str) -> dict[int, dict[str, str]]:
    tasks: dict[int, dict[str, str]] = {}
    for line in (output or "").splitlines():
        match = re.match(
            r"^TASK_(\d+)_(NAME|TIME|RL|TR|TR_LENGTH|SCHEDULE|START_DATE|START_TIME|START_BOUNDARY_LOCAL|MINUTE_INTERVAL|WEEKDAYS|RECOVERY_ONLY|EXPECTED_STATE)=(.*)$",
            line.strip(),
        )
        if not match:
            continue
        idx = int(match.group(1))
        key = match.group(2)
        value = match.group(3)
        tasks.setdefault(idx, {})[key] = value
    return tasks


class TestInstallScheduledTasks(unittest.TestCase):
    def _assert_future_boundary(self, value: str, out: str):
        self.assertRegex(value, r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$", msg=out)
        proc = subprocess.run(
            [
                "powershell",
                "-NoProfile",
                "-Command",
                "(Get-Date '" + value + "').ToUniversalTime().ToString('o')",
            ],
            cwd=str(REPO_ROOT),
            capture_output=True,
            text=True,
        )
        self.assertEqual(proc.returncode, 0, msg=proc.stderr + "\n" + proc.stdout)
        now_proc = subprocess.run(
            [
                "powershell",
                "-NoProfile",
                "-Command",
                "(Get-Date).ToUniversalTime().ToString('o')",
            ],
            cwd=str(REPO_ROOT),
            capture_output=True,
            text=True,
        )
        self.assertEqual(now_proc.returncode, 0, msg=now_proc.stderr + "\n" + now_proc.stdout)
        self.assertGreater(proc.stdout.strip(), now_proc.stdout.strip(), msg=out)

    def test_print_config_includes_replenish_task_and_exact_tr(self):
        self.assertTrue(SCRIPT.exists(), msg=f"missing script: {SCRIPT}")
        proc = _run("--print-config")
        out = (proc.stdout or "") + "\n" + (proc.stderr or "")
        self.assertEqual(proc.returncode, 0, msg=out)
        self.assertIn("INSTALL_SCHEDULED_TASKS_MODE=print-config", out)
        self.assertIn("INSTALL_SCHEDULED_TASKS_PRIMARY_SCHEDULER=runtime_tick_selfhosted", out)
        self.assertIn("INSTALL_SCHEDULED_TASKS_RECOVERY_ONLY_COUNT=4", out)
        self.assertIn("INSTALL_SCHEDULED_TASKS_WEEKDAYS_ONLY=0", out)
        self.assertIn("INSTALL_SCHEDULED_TASKS_WEEKDAY_SCHEDULE=MON,TUE,WED,THU,FRI", out)
        self.assertIn("PASS_INSTALL_SCHEDULED_TASKS_PRINT_CONFIG", out)

        tasks = _parse_task_config(out)
        self.assertEqual(len(tasks), 5, msg=out)

        ingest = [t for t in tasks.values() if t.get("NAME") == "OSHA_Osha_Ingest_Daily"]
        self.assertEqual(len(ingest), 1, msg=out)
        ingest_task = ingest[0]
        self.assertEqual(ingest_task.get("SCHEDULE"), "weekly", msg=out)
        self.assertEqual(ingest_task.get("RECOVERY_ONLY"), "YES", msg=out)
        self.assertEqual(ingest_task.get("EXPECTED_STATE"), "Disabled", msg=out)
        self.assertEqual(ingest_task.get("WEEKDAYS"), "MON,TUE,WED,THU,FRI", msg=out)
        self.assertEqual(ingest_task.get("TIME"), "06:45", msg=out)
        self.assertEqual(ingest_task.get("RL"), "HIGHEST", msg=out)
        self.assertEqual(ingest_task.get("TR"), EXPECTED_INGEST_TR, msg=out)
        self.assertLess(len(EXPECTED_INGEST_TR), 261)
        self.assertEqual(int(ingest_task.get("TR_LENGTH", "0")), len(EXPECTED_INGEST_TR), msg=out)
        self._assert_future_boundary(ingest_task.get("START_BOUNDARY_LOCAL", ""), out)

        replenish = [t for t in tasks.values() if t.get("NAME") == "OSHA_Prospect_Replenish_Daily"]
        self.assertEqual(len(replenish), 1, msg=out)
        replenish_task = replenish[0]
        self.assertEqual(replenish_task.get("SCHEDULE"), "weekly", msg=out)
        self.assertEqual(replenish_task.get("RECOVERY_ONLY"), "YES", msg=out)
        self.assertEqual(replenish_task.get("EXPECTED_STATE"), "Disabled", msg=out)
        self.assertEqual(replenish_task.get("WEEKDAYS"), "MON,TUE,WED,THU,FRI", msg=out)
        self.assertEqual(replenish_task.get("TIME"), "07:15", msg=out)
        self.assertEqual(replenish_task.get("RL"), "HIGHEST", msg=out)
        self.assertEqual(replenish_task.get("TR"), EXPECTED_REPLENISH_TR, msg=out)
        self.assertLess(len(EXPECTED_REPLENISH_TR), 261)
        self.assertEqual(int(replenish_task.get("TR_LENGTH", "0")), len(EXPECTED_REPLENISH_TR), msg=out)
        self._assert_future_boundary(replenish_task.get("START_BOUNDARY_LOCAL", ""), out)

        outreach = [t for t in tasks.values() if t.get("NAME") == "OSHA_Outreach_Auto"]
        self.assertEqual(len(outreach), 1, msg=out)
        self.assertEqual(outreach[0].get("SCHEDULE"), "weekly", msg=out)
        self.assertEqual(outreach[0].get("RECOVERY_ONLY"), "YES", msg=out)
        self.assertEqual(outreach[0].get("EXPECTED_STATE"), "Disabled", msg=out)
        self.assertEqual(outreach[0].get("WEEKDAYS"), "MON,TUE,WED,THU,FRI", msg=out)
        self.assertEqual(outreach[0].get("TR"), EXPECTED_OUTREACH_TR, msg=out)
        self._assert_future_boundary(outreach[0].get("START_BOUNDARY_LOCAL", ""), out)

        facs_trial = [t for t in tasks.values() if t.get("NAME") == "OSHA_Trial_FACS_Daily"]
        self.assertEqual(len(facs_trial), 1, msg=out)
        self.assertEqual(facs_trial[0].get("SCHEDULE"), "weekly", msg=out)
        self.assertEqual(facs_trial[0].get("RECOVERY_ONLY"), "YES", msg=out)
        self.assertEqual(facs_trial[0].get("EXPECTED_STATE"), "Disabled", msg=out)
        self.assertEqual(facs_trial[0].get("WEEKDAYS"), "MON,TUE,WED,THU,FRI", msg=out)
        self.assertEqual(facs_trial[0].get("TIME"), "09:00", msg=out)
        self.assertEqual(facs_trial[0].get("RL"), "HIGHEST", msg=out)
        self.assertEqual(facs_trial[0].get("TR"), EXPECTED_FACS_TRIAL_TR, msg=out)
        self.assertLess(len(EXPECTED_FACS_TRIAL_TR), 261)
        self.assertEqual(int(facs_trial[0].get("TR_LENGTH", "0")), len(EXPECTED_FACS_TRIAL_TR), msg=out)
        self._assert_future_boundary(facs_trial[0].get("START_BOUNDARY_LOCAL", ""), out)

        inbound = [t for t in tasks.values() if t.get("NAME") == "OSHA_Inbound_Triage"]
        self.assertEqual(len(inbound), 1, msg=out)
        self.assertEqual(inbound[0].get("SCHEDULE"), "minute", msg=out)
        self.assertEqual(inbound[0].get("RECOVERY_ONLY"), "NO", msg=out)
        self.assertEqual(inbound[0].get("EXPECTED_STATE"), "Enabled", msg=out)
        self.assertEqual(inbound[0].get("MINUTE_INTERVAL"), "15", msg=out)
        self.assertEqual(inbound[0].get("TR"), EXPECTED_INBOUND_TR, msg=out)
        self._assert_future_boundary(inbound[0].get("START_BOUNDARY_LOCAL", ""), out)

    def test_dry_run_outputs_commands_and_no_apply_token(self):
        proc = _run(
            "--dry-run",
            extra_env={
                "TASK_SCHED_USER": r"DESKTOP-Q8QM4N9\lever",
                "TASK_SCHED_PASSWORD": "dont-print-me",
            },
        )
        out = (proc.stdout or "") + "\n" + (proc.stderr or "")
        self.assertEqual(proc.returncode, 0, msg=out)
        self.assertIn("INSTALL_SCHEDULED_TASKS_MODE=dry-run", out)
        self.assertIn("INSTALL_SCHEDULED_TASKS_TASK_SCHED_USER=DESKTOP-Q8QM4N9\\lever", out)
        self.assertIn("INSTALL_SCHEDULED_TASKS_TASK_SCHED_PASSWORD_PRESENT=YES", out)
        self.assertIn("DRY_RUN_COMMAND_1=", out)
        self.assertIn("DRY_RUN_COMMAND_2=", out)
        self.assertIn("DRY_RUN_COMMAND_3=", out)
        self.assertIn("DRY_RUN_COMMAND_4=", out)
        self.assertIn("DRY_RUN_COMMAND_5=", out)
        self.assertIn("DRY_RUN_STATE_COMMAND_1=schtasks /Change /TN \"\\OSHA_Osha_Ingest_Daily\" /Disable", out)
        self.assertIn("/RU \"DESKTOP-Q8QM4N9\\lever\" /RP ***REDACTED***", out)
        self.assertNotIn("dont-print-me", out)
        self.assertIn("PASS_INSTALL_SCHEDULED_TASKS_DRY_RUN", out)
        self.assertNotIn("PASS_INSTALL_SCHEDULED_TASKS_APPLY", out)
        self.assertIn("/SC MINUTE /MO 15", out)
        self.assertIn("/SC WEEKLY /D MON,TUE,WED,THU,FRI", out)
        self.assertIn(EXPECTED_INBOUND_TR, out)
        self.assertIn(EXPECTED_INGEST_TR, out)
        self.assertIn(EXPECTED_REPLENISH_TR, out)
        self.assertIn(EXPECTED_FACS_TRIAL_TR, out)
        self.assertNotIn(r"C:\dev\OSHA_Leads\run_inbound_triage.ps1", out)

    def test_print_config_has_single_inbound_task(self):
        proc = _run("--print-config")
        out = (proc.stdout or "") + "\n" + (proc.stderr or "")
        self.assertEqual(proc.returncode, 0, msg=out)
        self.assertEqual(out.count("OSHA_Inbound_Triage"), 1, msg=out)
        self.assertNotIn(r"C:\dev\OSHA_Leads\run_inbound_triage.ps1", out)
        self.assertFalse((REPO_ROOT / "run_inbound_triage.ps1").exists(), msg="root runner must not exist")

    def test_repo_has_exactly_one_inbound_runner_path(self):
        matches = []
        for path in REPO_ROOT.rglob("run_inbound_triage.ps1"):
            rel = path.relative_to(REPO_ROOT).as_posix()
            if "/__pycache__/" in ("/" + rel + "/"):
                continue
            if rel.startswith(".local/wip_autosave_worktree/"):
                continue
            matches.append(rel)
        self.assertEqual(matches, ["scripts/scheduled/run_inbound_triage.ps1"], msg=str(matches))

    def test_verify_flag_is_accepted_by_arg_contract(self):
        proc = _run()
        out = (proc.stdout or "") + "\n" + (proc.stderr or "")
        self.assertNotEqual(proc.returncode, 0, msg=out)
        self.assertIn("ERR_INSTALL_SCHEDULED_TASKS_ARGS", out)

    def test_verify_contract_tokens_present(self):
        text = SCRIPT.read_text(encoding="utf-8")
        self.assertIn("WARN_SCHEDTASK_NEVER_RUN", text)
        self.assertIn("WARN_INSTALL_SCHEDULED_TASKS_APPLY_ACCESS_DENIED", text)
        self.assertIn("WARN_INSTALL_SCHEDULED_TASKS_REMEDIATION Re-run in elevated PowerShell to repair task permissions.", text)
        self.assertIn("PASS_SCHEDTASK_INSTALL_OK", text)
        self.assertIn("ERR_INSTALL_SCHEDULED_TASKS_VERIFY", text)
        self.assertIn("Task To Run", text)
        self.assertIn("TASK_TO_RUN=", text)
        self.assertIn("Schedule Type", text)
        self.assertIn("Start Time", text)
        self.assertIn("start_time_mismatch", text)
        self.assertIn("function Convert-StartTimeTo24Hour([string]$Raw)", text)
        self.assertIn("Scheduled Task State", text)
        self.assertIn("Logon Mode", text)
        self.assertIn("logon_mode_interactive_only", text)
        self.assertIn("action_mismatch", text)
        self.assertIn("WARN_SCHEDTASK_ACTION_MISMATCH", text)
        self.assertIn("ERR_INSTALL_SCHEDULED_TASKS_APPLY_ACTION_STUCK", text)
        self.assertIn("ERR_SCHED_TASK_TARGET_MISSING=1", text)
        self.assertIn("ERR_SCHEDTASK_RECOVERY_TASK_ENABLED=1", text)
        self.assertIn("INSTALL_SCHEDULED_TASKS_PRIMARY_SCHEDULER=runtime_tick_selfhosted", text)
        self.assertIn("PASS_INSTALL_SCHEDULED_TASKS_RUNNER_SERVICE", text)
        self.assertIn("PASS_INSTALL_SCHEDULED_TASKS_PYTHON_RESOLUTION", text)
        self.assertIn("Resolve-TaskRunTargetPath", text)
        self.assertIn("function Invoke-SchtasksCommand([string[]]$SchtasksArgs)", text)
        self.assertNotIn("function Invoke-SchtasksCommand([string[]]$Args)", text)
        self.assertIn("last_run_result_hex=0x41303", text)
        self.assertNotIn("last_result=0x41303", text)
        self.assertIn("OSHA_Prospect_Replenish_Daily", text)
        self.assertIn("run_prospect_replenish_daily.ps1", text)
        self.assertIn("TASK_REMOVED_LEGACY", text)
        self.assertIn("--verify", text)
        self.assertIn("--status", text)
        self.assertIn("$modeArg -eq '--verify' -or $modeArg -eq '--status'", text)

    def test_apply_requires_task_scheduler_password(self):
        proc = _run("--apply", extra_env={"TASK_SCHED_PASSWORD": ""})
        out = (proc.stdout or "") + "\n" + (proc.stderr or "")
        self.assertNotEqual(proc.returncode, 0, msg=out)
        self.assertIn("ERR_INSTALL_SCHEDULED_TASKS_CONFIG missing TASK_SCHED_PASSWORD", out)

    def test_invalid_args_emit_err_token(self):
        proc = _run("--dry-run", "--apply")
        out = (proc.stdout or "") + "\n" + (proc.stderr or "")
        self.assertNotEqual(proc.returncode, 0, msg=out)
        self.assertIn("ERR_INSTALL_SCHEDULED_TASKS_ARGS", out)

    def test_no_arg_error_lists_status_alias(self):
        proc = _run()
        out = (proc.stdout or "") + "\n" + (proc.stderr or "")
        self.assertNotEqual(proc.returncode, 0, msg=out)
        self.assertIn("ERR_INSTALL_SCHEDULED_TASKS_ARGS", out)
        self.assertIn("--status", out)


if __name__ == "__main__":
    unittest.main()
