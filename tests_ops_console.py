from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from ops_console import app as ops_app
from runtime_schedule_config import schedule_config_path, write_runtime_schedule


class _StubRunner:
    def __init__(self, *, fail_task_sync: bool = False) -> None:
        self.fail_task_sync = fail_task_sync
        self.calls: list[dict[str, object]] = []

    def __call__(
        self,
        command: list[str],
        *,
        env: dict[str, str] | None = None,
        stdin_text: str = "",
        timeout_seconds: int = 120,
    ) -> ops_app.CommandResult:
        parts = [str(item) for item in command]
        record = {
            "command": parts,
            "env": dict(env or {}),
            "stdin_text": stdin_text,
            "timeout_seconds": timeout_seconds,
        }
        self.calls.append(record)
        joined = " ".join(parts)

        if "set_outreach_env.ps1" in joined and "-PrintConfig" in parts:
            env_source = ops_app.os.environ

            def _first(*keys: str, default: str = "") -> str:
                for key in keys:
                    candidate = str(env_source.get(key) or "").strip()
                    if candidate:
                        return candidate
                return str(default or "").strip()

            inbound_backend = _first("INBOUND_BACKEND").lower()
            backend_source = "explicit"
            if not inbound_backend:
                if any(
                    _first(*keys)
                    for keys in (
                        ("IMAP_HOST", "BOUNCE_IMAP_HOST"),
                        ("IMAP_USER", "BOUNCE_IMAP_USER"),
                        ("IMAP_PASS", "BOUNCE_IMAP_PASS"),
                    )
                ):
                    inbound_backend = "imap"
                    backend_source = "inferred_imap_saved"
                else:
                    inbound_backend = "gmail"
                    backend_source = "default"
            imap_source = "direct_inbound" if any(_first(key) for key in ("IMAP_HOST", "IMAP_USER", "IMAP_PASS")) else "bounce_fallback"
            imap_user = _first("IMAP_USER", "BOUNCE_IMAP_USER")
            imap_pass_present = "YES" if _first("IMAP_PASS", "BOUNCE_IMAP_PASS") else "NO"
            stdout = "\n".join(
                [
                    "outreach_daily_limit=10",
                    "outreach_states=TX,CA",
                    "outreach_fallback_on_empty_state=0",
                    "prospect_autogrow_enabled=1",
                    "prospect_autogrow_safety_net_enabled=1",
                    "prospect_ai_assist_review_enabled=1",
                    "ai_triage_enabled=0",
                    "prospect_autogrow_states=TX,CA",
                    f"inbound_backend={inbound_backend}",
                    f"inbound_backend_source={backend_source}",
                    f"imap_source={imap_source}",
                    f"imap_host={_first('IMAP_HOST', 'BOUNCE_IMAP_HOST', default='imappro.zoho.com')}",
                    f"imap_port={_first('IMAP_PORT', 'BOUNCE_IMAP_PORT', default='993')}",
                    f"imap_user={imap_user}",
                    f"imap_pass_present={imap_pass_present}",
                    f"imap_folder={_first('IMAP_FOLDER', 'BOUNCE_IMAP_FOLDER', default='INBOX')}",
                    f"imap_folder_unsub={_first('IMAP_FOLDER_UNSUB', default='Processed/Unsubscribe')}",
                    f"imap_folder_bounce={_first('IMAP_FOLDER_BOUNCE', default='Processed/Bounce')}",
                    "PASS_SET_OUTREACH_ENV_PRINT_CONFIG status=OK",
                ]
            )
            return ops_app.CommandResult(parts, 0, stdout + "\n", "")

        if "set_outreach_env.ps1" in joined:
            return ops_app.CommandResult(parts, 0, "PASS_SET_OUTREACH_ENV_APPLY status=OK\n", "")

        if "run_outreach_auto.py" in joined and "--plan" in parts:
            target_date = parts[parts.index("--for-date") + 1]
            states = str((env or {}).get("OUTREACH_STATES") or "TX").split(",")
            selected_state = states[0].strip() or "TX"
            stdout = "\n".join(
                [
                    f"OUTREACH_PLAN_DATE={target_date}",
                    f"OUTREACH_PLAN_STATE={selected_state}",
                    f"OUTREACH_PLAN_BATCH={target_date}_{selected_state}",
                    "OUTREACH_PLAN_DAILY_LIMIT=10",
                    "OUTREACH_PLAN_WILL_SEND=4",
                    f"OUTREACH_STATE_ROTATION_SELECTED={selected_state}",
                    f"OUTREACH_STATE_EFFECTIVE_SEND={selected_state}",
                    f"OUTREACH_STATE_SENDABLE_ESTIMATE state={selected_state} sendable=12",
                ]
            )
            return ops_app.CommandResult(parts, 0, stdout + "\n", "")

        if "run_outreach_auto.py" in joined and "--print-config" in parts:
            return ops_app.CommandResult(parts, 0, "PASS_OUTREACH_PRINT_CONFIG status=OK\n", "")

        if "run_outreach_auto.py" in joined and "--doctor" in parts:
            return ops_app.CommandResult(parts, 0, "PASS_OUTREACH_DOCTOR status=OK\n", "")

        if "run_outreach_auto.py" in joined and "--dry-run" in parts:
            return ops_app.CommandResult(parts, 0, "PASS_OUTREACH_DRY_RUN status=OK\n", "")

        if "install_scheduled_tasks.ps1" in joined:
            if self.fail_task_sync:
                return ops_app.CommandResult(parts, 1, "", "ERR_INSTALL_SCHEDULED_TASKS_SYNC")
            return ops_app.CommandResult(parts, 0, "PASS_INSTALL_SCHEDULED_TASKS_APPLY status=OK\n", "")

        if "run_trial_admin.py" in joined and "add-trial" in parts:
            return ops_app.CommandResult(parts, 0, "PASS_TRIAL_ADD status=OK\n", "")

        if "run_trial_admin.py" in joined and "conversion-draft" in parts:
            return ops_app.CommandResult(parts, 0, "PASS_TRIAL_CONVERSION_DRAFT status=OK\n", "")

        if "run_trial_daily.py" in joined:
            return ops_app.CommandResult(parts, 0, "PASS_TRIAL_DAILY_DRY_RUN status=OK\n", "")

        if "crm_admin.py" in joined and "mark" in parts:
            return ops_app.CommandResult(parts, 0, "PASS_CRM_MARK status=OK\n", "")

        if "tools/import_prospect_ai_assist_review.py" in joined:
            if "--pending" in parts:
                mode = "pending"
            elif "--stdin" in parts:
                mode = "stdin"
            else:
                mode = "file"
            status = "preview" if "--dry-run" in parts else "apply"
            return ops_app.CommandResult(parts, 0, f"PASS_AI_ASSIST_IMPORT status={status} mode={mode}\n", "")

        return ops_app.CommandResult(parts, 0, "PASS_GENERIC status=OK\n", "")


class TestOpsConsole(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self._tmp_path = Path(self._tmp.name)
        self.repo_root = self._tmp_path / "repo"
        self.data_dir = self._tmp_path / "data"
        (self.repo_root / "out").mkdir(parents=True, exist_ok=True)
        (self.repo_root / "secrets").mkdir(parents=True, exist_ok=True)
        (self.repo_root / "ops_console" / "static").mkdir(parents=True, exist_ok=True)
        (self.repo_root / "ops_console" / "static" / "ops_console.css").write_text("body { color: #123; }\n", encoding="utf-8")
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.runner = _StubRunner()
        self.service = ops_app.OpsConsoleService(
            repo_root=self.repo_root,
            data_dir=self.data_dir,
            command_runner=self.runner,
        )
        self.app = ops_app.OpsConsoleApp(self.service)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _body(self, response: ops_app.Response) -> str:
        return response.body.decode("utf-8")

    def _preview_rows(self) -> list[dict[str, object]]:
        rows: list[dict[str, object]] = []
        preview_root = self.data_dir / "ops_console" / "previews"
        for path in sorted(preview_root.glob("*.json")) if preview_root.exists() else []:
            rows.append(json.loads(path.read_text(encoding="utf-8")))
        return rows

    def _audit_rows(self) -> list[dict[str, object]]:
        rows: list[dict[str, object]] = []
        path = self.data_dir / "ops_console" / "audit" / "ops_console_audit.jsonl"
        if not path.exists():
            return rows
        for line in path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                rows.append(json.loads(line))
        return rows

    def test_dashboard_and_inbox_render_without_secrets_or_artifacts(self):
        with mock.patch.object(ops_app, "_python_module_available", return_value=True):
            dashboard = self._body(self.app.dispatch("GET", "/"))
            inbox = self._body(self.app.dispatch("GET", "/inbox"))
        css = self._body(self.app.dispatch("GET", "/static/ops_console.css"))
        self.assertIn("MicroFlowOps Ops Console", dashboard)
        self.assertIn("Needs Attention", dashboard)
        self.assertIn("Nothing urgent is queued.", dashboard)
        self.assertIn("Inbound Setup", inbox)
        self.assertIn("not configured", inbox)
        self.assertIn("Gmail is optional", inbox)
        self.assertIn("set_outreach_env.ps1 -InboundBackend imap -SyncInboundImapFromBounce", inbox)
        self.assertIn("py -3 -m pip install google-api-python-client google-auth-oauthlib", inbox)
        self.assertIn("py -3 inbound_inbox_triage.py --dry-run --since-hours 1", inbox)
        self.assertIn("py -3 inbound_inbox_triage.py --run-once", inbox)
        self.assertIn(str((self.repo_root / "secrets" / "gmail_credentials.json").resolve(strict=False)), inbox)
        self.assertIn(str((self.repo_root / "out" / "inbox_triage_log.csv").resolve(strict=False)), inbox)
        self.assertIn("No local trial-request artifact", inbox)
        self.assertIn("color: #123", css)

    def test_inbox_gmail_status_shows_oauth_bootstrap_when_credentials_exist_without_token(self):
        (self.repo_root / "secrets" / "gmail_credentials.json").write_text('{"installed":{}}\n', encoding="utf-8")
        with mock.patch.object(ops_app, "_python_module_available", return_value=True):
            inbox = self._body(self.app.dispatch("GET", "/inbox"))
        self.assertIn("ready for first OAuth bootstrap", inbox)
        self.assertIn("Run the dry-run triage bootstrap, then a single --run-once bootstrap on the canonical PC.", inbox)
        self.assertIn("gmail_client_deps", inbox)
        self.assertIn("installed", inbox)

    def test_inbox_gmail_status_shows_configured_when_credentials_and_token_exist(self):
        (self.repo_root / "secrets" / "gmail_credentials.json").write_text('{"installed":{}}\n', encoding="utf-8")
        (self.repo_root / "secrets" / "gmail_token.json").write_text('{"token":"abc"}\n', encoding="utf-8")
        with mock.patch.object(ops_app, "_python_module_available", return_value=True):
            inbox = self._body(self.app.dispatch("GET", "/inbox"))
        self.assertIn("configured", inbox)
        self.assertIn("Run the dry-run triage check when you want to verify the Gmail inbox path.", inbox)
        self.assertIn(str((self.repo_root / "secrets" / "gmail_token.json").resolve(strict=False)), inbox)

    def test_inbox_imap_status_shows_readiness_from_env_file(self):
        with mock.patch.dict(
            ops_app.os.environ,
            {
                "INBOUND_BACKEND": "imap",
                "IMAP_USER": "ops@example.com",
                "IMAP_PASS": "topsecret",
                "IMAP_HOST": "imap.example.com",
            },
            clear=False,
        ):
            inbox = self._body(self.app.dispatch("GET", "/inbox"))
        self.assertIn("<dd>imap</dd>", inbox)
        self.assertIn("configured", inbox)
        self.assertIn("imap.example.com", inbox)
        self.assertIn("ops@example.com", inbox)
        self.assertIn("set_outreach_env.ps1 -InboundBackend imap -SyncInboundImapFromBounce", inbox)
        self.assertIn(".\\run_with_secrets.ps1 -- py -3 inbound_inbox_triage.py --dry-run --since-hours 1", inbox)
        self.assertIn("Run the IMAP dry-run triage check when you want to verify the inbound path.", inbox)

    def test_inbox_imap_status_can_be_inferred_from_saved_bounce_mailbox_values(self):
        with mock.patch.dict(
            ops_app.os.environ,
            {
                "BOUNCE_IMAP_USER": "zoho-ops@example.com",
                "BOUNCE_IMAP_PASS": "topsecret",
                "BOUNCE_IMAP_HOST": "imappro.zoho.com",
            },
            clear=True,
        ):
            inbox = self._body(self.app.dispatch("GET", "/inbox"))
        self.assertIn("<dd>imap</dd>", inbox)
        self.assertIn("inferred_imap_saved", inbox)
        self.assertIn("bounce_fallback", inbox)
        self.assertIn("zoho-ops@example.com", inbox)
        self.assertIn("Run the IMAP dry-run triage check when you want to verify the inbound path.", inbox)

    def test_build_server_rejects_non_localhost_binding(self):
        with self.assertRaises(ValueError):
            ops_app.build_server(host="0.0.0.0", port=8420, app=self.app)
        server = ops_app.build_server(port=0, app=self.app)
        server.server_close()

    def test_outreach_preview_apply_and_hash_gate_routes(self):
        preview_response = self.app.dispatch(
            "POST",
            "/outreach",
            form={
                "action": "preview_outreach",
                "outreach_daily_limit": "12",
                "outreach_states": "TX,CA,FL",
                "outreach_fallback_on_empty_state": "1",
                "prospect_autogrow_enabled": "1",
                "prospect_autogrow_safety_net_enabled": "1",
                "prospect_ai_assist_review_enabled": "1",
                "ai_triage_enabled": "0",
            },
        )
        preview_body = self._body(preview_response)
        self.assertIn("Pending Preview", preview_body)
        previews = self._preview_rows()
        self.assertEqual(len(previews), 1)
        preview = previews[0]
        bad_response = self.app.dispatch(
            "POST",
            "/outreach",
            form={
                "action": "apply_preview",
                "preview_id": str(preview["preview_id"]),
                "payload_hash": "bad-hash",
            },
        )
        self.assertIn("preview payload hash mismatch", self._body(bad_response))

        good_response = self.app.dispatch(
            "POST",
            "/outreach",
            form={
                "action": "apply_preview",
                "preview_id": str(preview["preview_id"]),
                "payload_hash": str(preview["payload_hash"]),
            },
        )
        good_body = self._body(good_response)
        self.assertIn("Outreach change applied through scripts/set_outreach_env.ps1.", good_body)
        self.assertIn("PASS_SET_OUTREACH_ENV_APPLY", good_body)
        audit_actions = [str(row.get("action") or "") for row in self._audit_rows()]
        self.assertIn("preview_created", audit_actions)
        self.assertIn("apply_completed", audit_actions)
        applied_commands = [" ".join(call["command"]) for call in self.runner.calls if "set_outreach_env.ps1" in " ".join(call["command"])]
        self.assertTrue(any("-OutreachStates TX,CA,FL" in command for command in applied_commands))

    def test_manual_import_preview_and_apply_use_existing_importer_contract(self):
        csv_text = (
            "state,decision,firm,website,contact_name,title,email,source_urls,confidence,evidence_snippet\n"
            "TX,accept,Acme,https://acme.example,Jane Doe,Owner,jane@acme.example,https://acme.example,high,proof\n"
        )
        preview = self.service.build_manual_import_preview({"import_mode": "stdin", "csv_text": csv_text})
        self.assertEqual(preview["kind"], "manual_import")
        self.assertIn("--stdin --dry-run", str(preview["preview_data"]["command"]))
        preview_calls = [call for call in self.runner.calls if "tools/import_prospect_ai_assist_review.py" in " ".join(call["command"])]
        self.assertTrue(preview_calls)
        self.assertEqual(str(preview_calls[-1]["stdin_text"]), csv_text)
        _preview, result = self.service.apply_manual_import_preview(
            preview_id=str(preview["preview_id"]),
            payload_hash=str(preview["payload_hash"]),
        )
        self.assertEqual(result.exit_code, 0)
        apply_calls = [call for call in self.runner.calls if "tools/import_prospect_ai_assist_review.py" in " ".join(call["command"])]
        self.assertIn("--stdin", apply_calls[-1]["command"])
        self.assertNotIn("--dry-run", apply_calls[-1]["command"])
        self.assertEqual(str(apply_calls[-1]["stdin_text"]), csv_text)

    def test_schedule_apply_rolls_back_when_task_sync_fails(self):
        customer_root = self.repo_root / "customers"
        customer_root.mkdir(parents=True, exist_ok=True)
        managed_paths = [
            customer_root / "facs_trial.json",
            customer_root / "jl_safety_trial.json",
            customer_root / "roi_safety_trial.json",
        ]
        for path in managed_paths:
            path.write_text(json.dumps({"subscriber_key": path.stem, "send_time_local": "09:00"}, indent=2) + "\n", encoding="utf-8")

        failing_runner = _StubRunner(fail_task_sync=True)
        with mock.patch.object(ops_app, "MANAGED_TRIAL_CUSTOMER_PATHS", managed_paths):
            service = ops_app.OpsConsoleService(
                repo_root=self.repo_root,
                data_dir=self.data_dir,
                command_runner=failing_runner,
            )
            preview = service.build_schedule_preview(
                {
                    "outreach_send_local_hhmm": "10:10",
                    "trial_default_send_local_hhmm": "11:11",
                    "evening_prep_local_hhmm": "21:20",
                }
            )
            _preview, result = service.apply_schedule_preview(
                preview_id=str(preview["preview_id"]),
                payload_hash=str(preview["payload_hash"]),
            )

        self.assertEqual(result.exit_code, 1)
        self.assertFalse(schedule_config_path(self.data_dir).exists())
        for path in managed_paths:
            payload = json.loads(path.read_text(encoding="utf-8"))
            self.assertEqual(payload["send_time_local"], "09:00")
        audit_actions = [str(row.get("action") or "") for row in self._audit_rows()]
        self.assertIn("apply_failed", audit_actions)

    def test_trial_add_preview_defaults_to_shared_trial_schedule(self):
        write_runtime_schedule(
            self.data_dir,
            outreach_send_local_hhmm="08:00",
            trial_default_send_local_hhmm="11:30",
            evening_prep_local_hhmm="20:45",
            updated_by="unit_test",
        )
        preview = self.service.build_trial_add_preview(
            {
                "subscriber_key": "acme_trial",
                "email": "ops@example.com",
                "states": "TX,CA",
                "tz": "America/New_York",
                "start_date": "2026-03-24",
                "sends_limit": "14",
            }
        )
        self.assertEqual(preview["payload"]["send_time_local"], "11:30")
        _preview, result = self.service.apply_trial_add_preview(
            preview_id=str(preview["preview_id"]),
            payload_hash=str(preview["payload_hash"]),
        )
        self.assertEqual(result.exit_code, 0)
        add_calls = [call for call in self.runner.calls if "run_trial_admin.py" in " ".join(call["command"]) and "add-trial" in call["command"]]
        self.assertTrue(add_calls)
        add_command = [str(part) for part in add_calls[-1]["command"]]
        self.assertIn("--send-time-local", add_command)
        self.assertIn("11:30", add_command)
