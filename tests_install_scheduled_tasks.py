import re
import subprocess
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent
SCRIPT = REPO_ROOT / "scripts" / "install_scheduled_tasks.ps1"
EXPECTED_GENERATION_TR = (
    "powershell.exe -NoProfile -ExecutionPolicy Bypass -File "
    r"C:\dev\OSHA_Leads\scripts\scheduled\run_prospect_generation.ps1"
)
EXPECTED_DISCOVERY_TR = (
    "powershell.exe -NoProfile -ExecutionPolicy Bypass -File "
    r"C:\dev\OSHA_Leads\run_with_secrets.ps1 py -3 C:\dev\OSHA_Leads\run_prospect_discovery.py"
)
EXPECTED_OUTREACH_TR = (
    "powershell.exe -NoProfile -ExecutionPolicy Bypass -File "
    r"C:\dev\OSHA_Leads\run_with_secrets.ps1 py -3 C:\dev\OSHA_Leads\run_outreach_auto.py"
)
EXPECTED_INBOUND_TR = (
    "powershell.exe -NoProfile -ExecutionPolicy Bypass -File "
    r"C:\dev\OSHA_Leads\scripts\scheduled\run_inbound_triage.ps1"
)


def _run(*args: str) -> subprocess.CompletedProcess:
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
        capture_output=True,
        text=True,
    )


def _parse_task_config(output: str) -> dict[int, dict[str, str]]:
    tasks: dict[int, dict[str, str]] = {}
    for line in (output or "").splitlines():
        match = re.match(
            r"^TASK_(\d+)_(NAME|TIME|RL|TR|TR_LENGTH|SCHEDULE|START_DATE|START_TIME|START_BOUNDARY_LOCAL|MINUTE_INTERVAL)=(.*)$",
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

    def test_print_config_includes_generation_task_and_exact_tr(self):
        self.assertTrue(SCRIPT.exists(), msg=f"missing script: {SCRIPT}")
        proc = _run("--print-config")
        out = (proc.stdout or "") + "\n" + (proc.stderr or "")
        self.assertEqual(proc.returncode, 0, msg=out)
        self.assertIn("INSTALL_SCHEDULED_TASKS_MODE=print-config", out)
        self.assertIn("PASS_INSTALL_SCHEDULED_TASKS_PRINT_CONFIG", out)

        tasks = _parse_task_config(out)
        self.assertEqual(len(tasks), 4, msg=out)

        generation = [t for t in tasks.values() if t.get("NAME") == "OSHA_Prospect_Generation"]
        self.assertEqual(len(generation), 1, msg=out)
        generation_task = generation[0]
        self.assertEqual(generation_task.get("SCHEDULE"), "daily", msg=out)
        self.assertEqual(generation_task.get("TIME"), "07:15", msg=out)
        self.assertEqual(generation_task.get("RL"), "HIGHEST", msg=out)
        self.assertEqual(generation_task.get("TR"), EXPECTED_GENERATION_TR, msg=out)
        self.assertLess(len(EXPECTED_GENERATION_TR), 261)
        self.assertEqual(int(generation_task.get("TR_LENGTH", "0")), len(EXPECTED_GENERATION_TR), msg=out)
        self._assert_future_boundary(generation_task.get("START_BOUNDARY_LOCAL", ""), out)

        discovery = [t for t in tasks.values() if t.get("NAME") == "OSHA_Prospect_Discovery"]
        self.assertEqual(len(discovery), 1, msg=out)
        self.assertEqual(discovery[0].get("TR"), EXPECTED_DISCOVERY_TR, msg=out)
        self._assert_future_boundary(discovery[0].get("START_BOUNDARY_LOCAL", ""), out)

        outreach = [t for t in tasks.values() if t.get("NAME") == "OSHA_Outreach_Auto"]
        self.assertEqual(len(outreach), 1, msg=out)
        self.assertEqual(outreach[0].get("TR"), EXPECTED_OUTREACH_TR, msg=out)
        self._assert_future_boundary(outreach[0].get("START_BOUNDARY_LOCAL", ""), out)

        inbound = [t for t in tasks.values() if t.get("NAME") == "OSHA_Inbound_Triage"]
        self.assertEqual(len(inbound), 1, msg=out)
        self.assertEqual(inbound[0].get("SCHEDULE"), "minute", msg=out)
        self.assertEqual(inbound[0].get("MINUTE_INTERVAL"), "15", msg=out)
        self.assertEqual(inbound[0].get("TR"), EXPECTED_INBOUND_TR, msg=out)
        self._assert_future_boundary(inbound[0].get("START_BOUNDARY_LOCAL", ""), out)

    def test_dry_run_outputs_commands_and_no_apply_token(self):
        proc = _run("--dry-run")
        out = (proc.stdout or "") + "\n" + (proc.stderr or "")
        self.assertEqual(proc.returncode, 0, msg=out)
        self.assertIn("INSTALL_SCHEDULED_TASKS_MODE=dry-run", out)
        self.assertIn("DRY_RUN_COMMAND_1=", out)
        self.assertIn("DRY_RUN_COMMAND_2=", out)
        self.assertIn("DRY_RUN_COMMAND_3=", out)
        self.assertIn("DRY_RUN_COMMAND_4=", out)
        self.assertIn("PASS_INSTALL_SCHEDULED_TASKS_DRY_RUN", out)
        self.assertNotIn("PASS_INSTALL_SCHEDULED_TASKS_APPLY", out)
        self.assertIn("/SC MINUTE /MO 15", out)
        self.assertIn(EXPECTED_INBOUND_TR, out)
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
        self.assertIn("PASS_SCHEDTASK_INSTALL_OK", text)
        self.assertIn("ERR_INSTALL_SCHEDULED_TASKS_VERIFY", text)
        self.assertIn("Task To Run", text)
        self.assertIn("TASK_TO_RUN=", text)
        self.assertIn("Schedule Type", text)
        self.assertIn("Start Time", text)
        self.assertIn("Scheduled Task State", text)
        self.assertIn("action_mismatch", text)
        self.assertIn("WARN_SCHEDTASK_ACTION_MISMATCH", text)
        self.assertIn("ERR_INSTALL_SCHEDULED_TASKS_APPLY_ACTION_STUCK", text)
        self.assertIn("function Invoke-SchtasksCommand([string[]]$SchtasksArgs)", text)
        self.assertNotIn("function Invoke-SchtasksCommand([string[]]$Args)", text)
        self.assertIn("last_run_result_hex=0x41303", text)
        self.assertNotIn("last_result=0x41303", text)
        self.assertNotIn(" -- py -3 ", text)
        self.assertIn(" py -3 ", text)
        self.assertIn("--verify", text)

    def test_invalid_args_emit_err_token(self):
        proc = _run("--dry-run", "--apply")
        out = (proc.stdout or "") + "\n" + (proc.stderr or "")
        self.assertNotEqual(proc.returncode, 0, msg=out)
        self.assertIn("ERR_INSTALL_SCHEDULED_TASKS_ARGS", out)


if __name__ == "__main__":
    unittest.main()
