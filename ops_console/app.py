from __future__ import annotations

import argparse
import csv
import hashlib
import html
import importlib.util
import json
import os
import re
import sqlite3
import subprocess
import uuid
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from string import Template
from typing import Any, Callable
from urllib.parse import parse_qs
from wsgiref.simple_server import WSGIRequestHandler, WSGIServer, make_server
from wsgiref.util import setup_testing_defaults

import ai_assist_paths
import crm_light
import run_trial_admin
from outreach import crm_store
from outreach import us_state
from runtime_data_dir import resolve_data_dir
from runtime_schedule_config import (
    RuntimeSchedule,
    load_runtime_schedule,
    schedule_config_path,
    validate_local_hhmm,
    write_runtime_schedule,
)
from tools import import_prospect_ai_assist_review as ai_assist_import

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore[assignment]


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8420
LAYOUT_TEMPLATE_PATH = (REPO_ROOT / "ops_console" / "templates" / "layout.html").resolve(strict=False)
DEFAULT_LAYOUT_TEMPLATE_TEXT = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>$title</title>
  <link rel="stylesheet" href="/static/ops_console.css">
</head>
<body>
  <header class="topbar">
    <div>
      <h1>MicroFlowOps Ops Console</h1>
      <p class="subhead">Local-only control plane on 127.0.0.1</p>
    </div>
    <div class="runtime-meta">
      <div><strong>Repo</strong> $repo_root</div>
      <div><strong>Data</strong> $data_dir</div>
      <div><strong>Schedule</strong> $schedule_path</div>
    </div>
  </header>
  <nav class="nav">$nav</nav>
  $message
  <main class="page">$content</main>
</body>
</html>
"""
LAYOUT_TEMPLATE = Template(
    LAYOUT_TEMPLATE_PATH.read_text(encoding="utf-8") if LAYOUT_TEMPLATE_PATH.exists() else DEFAULT_LAYOUT_TEMPLATE_TEXT
)
NAV_ITEMS = [
    ("/", "Dashboard"),
    ("/outreach", "Outreach Control"),
    ("/schedule", "Scheduling Control"),
    ("/state-scope", "State Scope"),
    ("/trials", "Trials"),
    ("/manual-imports", "Manual Research"),
    ("/inbox", "Inbox / Requests"),
    ("/audit", "Audit Log"),
]
MANAGED_TRIAL_CUSTOMER_PATHS = [
    REPO_ROOT / "customers" / "facs_trial.json",
    REPO_ROOT / "customers" / "jl_safety_trial.json",
    REPO_ROOT / "customers" / "roi_safety_trial.json",
]
OUTREACH_SAFE_FIELDS = {
    "outreach_daily_limit": ("OUTREACH_DAILY_LIMIT", "OutreachDailyLimit"),
    "outreach_states": ("OUTREACH_STATES", "OutreachStates"),
    "outreach_fallback_on_empty_state": ("OUTREACH_FALLBACK_ON_EMPTY_STATE", "OutreachFallbackOnEmptyState"),
    "outreach_state_spread_mode": ("OUTREACH_STATE_SPREAD_MODE", "OutreachStateSpreadMode"),
    "prospect_autogrow_enabled": ("PROSPECT_AUTOGROW_ENABLED", "ProspectAutoGrowEnabled"),
    "prospect_autogrow_safety_net_enabled": (
        "PROSPECT_AUTOGROW_SAFETY_NET_ENABLED",
        "ProspectAutoGrowSafetyNetEnabled",
    ),
    "prospect_ai_assist_review_enabled": (
        "PROSPECT_AI_ASSIST_REVIEW_ENABLED",
        "ProspectAiAssistReviewEnabled",
    ),
    "ai_triage_enabled": ("AI_TRIAGE_ENABLED", "AiTriageEnabled"),
}
TRIAL_MARK_EVENTS = ("replied", "trial_started", "converted", "do_not_contact")


@dataclass(frozen=True)
class CommandResult:
    command: list[str]
    exit_code: int
    stdout: str
    stderr: str

    @property
    def display_command(self) -> str:
        return subprocess.list2cmdline([str(item) for item in self.command])


@dataclass(frozen=True)
class Response:
    status: str
    headers: list[tuple[str, str]]
    body: bytes


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _utc_now_iso() -> str:
    return _utc_now().isoformat(timespec="seconds")


def _repo_relative(path: Path) -> str:
    try:
        return str(path.resolve(strict=False).relative_to(REPO_ROOT.resolve(strict=False)))
    except Exception:
        return str(path.resolve(strict=False))


def _html(value: Any) -> str:
    return html.escape(str(value or ""))


def _safe_json_load(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _safe_json_list(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    out: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        text = line.strip()
        if not text:
            continue
        try:
            row = json.loads(text)
        except Exception:
            continue
        if isinstance(row, dict):
            out.append(row)
    return out


def _read_env_file(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    out: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        out[key.strip()] = value.strip().strip('"').strip("'")
    return out


def _env_or_file_value(env_file: dict[str, str], key: str, *, default: str = "") -> str:
    candidate = str(os.environ.get(key) or env_file.get(key) or "").strip()
    if candidate:
        return candidate
    return str(default or "").strip()


def _python_module_available(module_name: str) -> bool:
    try:
        return importlib.util.find_spec(str(module_name or "").strip()) is not None
    except Exception:
        return False


def _path_with_presence(path: Path) -> str:
    return f"{_repo_relative(path)} ({'present' if path.exists() else 'missing'})"


def _parse_state_csv(raw: str) -> list[str]:
    states: list[str] = []
    seen: set[str] = set()
    for part in str(raw or "").split(","):
        state = part.strip().upper()
        if not state:
            continue
        if not re.fullmatch(r"[A-Z]{2}", state):
            raise ValueError(f"invalid state code {state}")
        if state in seen:
            continue
        seen.add(state)
        states.append(state)
    return states


def _normalize_state_csv(raw: str) -> str:
    states = _parse_state_csv(raw)
    if not states:
        raise ValueError("outreach_states required")
    return ",".join(states)


def _normalize_boolish_int(raw: str, *, field_name: str) -> str:
    text = str(raw or "").strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return "1"
    if text in {"0", "false", "no", "off"}:
        return "0"
    raise ValueError(f"{field_name} must be 0 or 1")


def _parse_inline_key_values(text: str) -> dict[str, str]:
    return {match.group(1): match.group(2) for match in re.finditer(r"([A-Za-z0-9_]+)=([^\s]+)", text)}


def _parse_selected_by_state_csv(text: str) -> dict[str, int]:
    out: dict[str, int] = {}
    for part in [segment.strip() for segment in str(text or "").split(",") if segment.strip()]:
        state, _sep, count_text = part.partition(":")
        state_norm = str(state or "").strip().upper()
        if not state_norm:
            continue
        try:
            out[state_norm] = max(0, int(count_text or 0))
        except Exception:
            out[state_norm] = 0
    return out


def _format_selected_by_state(value: Any) -> str:
    if not isinstance(value, dict):
        return ""
    parts: list[str] = []
    for raw_state, raw_count in value.items():
        state = str(raw_state or "").strip().upper()
        if not state:
            continue
        try:
            count = max(0, int(raw_count or 0))
        except Exception:
            count = 0
        parts.append(f"{state}:{count}")
    return ", ".join(parts)


def _project_weekday_end_date(start_date_text: str, sends_limit: int) -> str:
    try:
        cursor = date.fromisoformat(start_date_text)
    except Exception:
        return ""
    sent = 0
    while sent < int(max(1, sends_limit)):
        if cursor.weekday() < 5:
            sent += 1
        if sent >= int(max(1, sends_limit)):
            return cursor.isoformat()
        cursor += timedelta(days=1)
    return cursor.isoformat()


def _resolve_zone(tz_name: str) -> Any:
    if ZoneInfo is None:
        return None
    try:
        return ZoneInfo(str(tz_name or "").strip() or "America/New_York")
    except Exception:
        return None


def _compute_next_send_time(*, send_time_local: str, tz_name: str, now_utc: datetime) -> str:
    zone = _resolve_zone(tz_name)
    if zone is None:
        return ""
    try:
        hour, minute = [int(part) for part in send_time_local.split(":", 1)]
    except Exception:
        return ""
    local_now = now_utc.astimezone(zone)
    candidate_date = local_now.date()
    for _ in range(8):
        if candidate_date.weekday() < 5:
            candidate_dt = datetime.combine(candidate_date, time(hour=hour, minute=minute), tzinfo=zone)
            if candidate_dt > local_now:
                return candidate_dt.isoformat(timespec="minutes")
        candidate_date += timedelta(days=1)
    return ""


def _csv_rows(path: Path, *, limit: int = 25) -> list[dict[str, str]]:
    if not path.exists():
        return []
    try:
        with open(path, "r", encoding="utf-8", newline="") as handle:
            rows = list(csv.DictReader(handle))
    except Exception:
        return []
    trimmed = [dict((key, str(value or "").strip()) for key, value in row.items()) for row in rows]
    trimmed.reverse()
    return trimmed[:limit]


class OpsConsoleService:
    def __init__(
        self,
        *,
        repo_root: Path | None = None,
        data_dir: Path | None = None,
        command_runner: Callable[..., CommandResult] | None = None,
        now_provider: Callable[[], datetime] | None = None,
    ) -> None:
        self.repo_root = (repo_root or REPO_ROOT).resolve(strict=False)
        self.data_dir = (
            Path(data_dir).resolve(strict=False)
            if data_dir is not None
            else resolve_data_dir(self.repo_root).effective_path.resolve(strict=False)
        )
        self._command_runner = command_runner or self._run_command
        self._now_provider = now_provider or _utc_now

    def previews_root(self) -> Path:
        return (self.data_dir / "ops_console" / "previews").resolve(strict=False)

    def audit_log_path(self) -> Path:
        return (self.data_dir / "ops_console" / "audit" / "ops_console_audit.jsonl").resolve(strict=False)

    def schedule(self) -> RuntimeSchedule:
        return load_runtime_schedule(self.data_dir)

    def _run_command(
        self,
        command: list[str],
        *,
        env: dict[str, str] | None = None,
        stdin_text: str = "",
        timeout_seconds: int = 120,
    ) -> CommandResult:
        merged_env = os.environ.copy()
        merged_env.setdefault("PYTHONPATH", str(self.repo_root))
        merged_env["DATA_DIR"] = str(self.data_dir)
        if env:
            merged_env.update({str(key): str(value) for key, value in env.items()})
        proc = subprocess.run(
            [str(item) for item in command],
            cwd=str(self.repo_root),
            env=merged_env,
            input=stdin_text,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
            check=False,
        )
        return CommandResult(
            command=[str(item) for item in command],
            exit_code=int(proc.returncode),
            stdout=str(proc.stdout or ""),
            stderr=str(proc.stderr or ""),
        )

    def _powershell_file_command(self, relative_path: str, *args: str) -> list[str]:
        return [
            "powershell",
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str((self.repo_root / relative_path).resolve(strict=False)),
            *[str(item) for item in args],
        ]

    def _python_command(self, script_name: str, *args: str) -> list[str]:
        return ["py", "-3", script_name, *[str(item) for item in args]]

    def _secrets_python_command(self, script_name: str, *args: str) -> list[str]:
        return [
            "powershell",
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str((self.repo_root / "run_with_secrets.ps1").resolve(strict=False)),
            "--",
            "py",
            "-3",
            script_name,
            *[str(item) for item in args],
        ]

    def _payload_hash(self, payload: dict[str, Any]) -> str:
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    def _write_audit_event(self, *, kind: str, action: str, payload: dict[str, Any], extra: dict[str, Any] | None = None) -> None:
        record: dict[str, Any] = {
            "schema": "ops_console_audit_v1",
            "recorded_at_utc": _utc_now_iso(),
            "kind": kind,
            "action": action,
            "payload_hash": self._payload_hash(payload),
            "payload": payload,
        }
        if extra:
            record.update(extra)
        path = self.audit_log_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "a", encoding="utf-8", newline="") as handle:
            handle.write(json.dumps(record, sort_keys=True) + "\n")

    def recent_audit_entries(self, *, limit: int = 100) -> list[dict[str, Any]]:
        rows = _safe_json_list(self.audit_log_path())
        rows.reverse()
        return rows[:limit]

    def _create_preview(
        self,
        *,
        kind: str,
        title: str,
        payload: dict[str, Any],
        preview_data: dict[str, Any],
    ) -> dict[str, Any]:
        preview_id = uuid.uuid4().hex
        payload_hash = self._payload_hash(payload)
        record = {
            "schema": "ops_console_preview_v1",
            "preview_id": preview_id,
            "created_at_utc": _utc_now_iso(),
            "kind": kind,
            "title": title,
            "payload": payload,
            "payload_hash": payload_hash,
            "preview_data": preview_data,
        }
        path = self.previews_root() / f"{preview_id}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(record, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        self._write_audit_event(
            kind=kind,
            action="preview_created",
            payload=payload,
            extra={"preview_id": preview_id, "title": title},
        )
        return record

    def _load_preview(self, preview_id: str) -> dict[str, Any]:
        path = self.previews_root() / f"{str(preview_id or '').strip()}.json"
        if not path.exists():
            raise ValueError("preview not found")
        payload = _safe_json_load(path)
        if not payload:
            raise ValueError("preview unreadable")
        return payload

    def _assert_preview(self, *, preview_id: str, payload_hash: str, kind: str) -> dict[str, Any]:
        preview = self._load_preview(preview_id)
        if str(preview.get("kind") or "").strip() != kind:
            raise ValueError("preview kind mismatch")
        stored_hash = str(preview.get("payload_hash") or "").strip()
        if not stored_hash or stored_hash != str(payload_hash or "").strip():
            raise ValueError("preview payload hash mismatch")
        return preview

    def _current_outreach_defaults(self) -> dict[str, str]:
        return {
            "outreach_daily_limit": "10",
            "outreach_states": us_state.DEFAULT_OUTREACH_STATE_CSV,
            "outreach_fallback_on_empty_state": "0",
            "outreach_state_spread_mode": "round_robin",
            "prospect_autogrow_enabled": "1",
            "prospect_autogrow_safety_net_enabled": "1",
            "prospect_ai_assist_review_enabled": "1",
            "ai_triage_enabled": "0",
            "outreach_skip_role_inboxes": "1",
            "outreach_allow_free_domains": "0",
            "prospect_autogrow_states": "",
        }

    def _set_outreach_env_print_config(self) -> tuple[dict[str, str], CommandResult]:
        result = self._command_runner(
            self._powershell_file_command("scripts/set_outreach_env.ps1", "-PrintConfig"),
            timeout_seconds=180,
        )
        values: dict[str, str] = {}
        if result.exit_code == 0:
            for line in result.stdout.splitlines():
                if "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key_norm = str(key or "").strip().lower()
                if not key_norm:
                    continue
                values[key_norm] = str(value or "").strip()
        return values, result

    def outreach_env_snapshot(self) -> dict[str, Any]:
        values = self._current_outreach_defaults()
        warnings: list[str] = []
        print_values, result = self._set_outreach_env_print_config()
        if result.exit_code == 0:
            for key_norm, value in print_values.items():
                if key_norm in values:
                    values[key_norm] = value
            source = "set_outreach_env_print_config"
        else:
            warnings.append("Canonical env print-config unavailable; using shell/.env fallbacks instead.")
            env_file = _read_env_file(self.repo_root / ".env")
            source = "fallback"
            mapping = {
                "OUTREACH_DAILY_LIMIT": "outreach_daily_limit",
                "OUTREACH_STATES": "outreach_states",
                "OUTREACH_FALLBACK_ON_EMPTY_STATE": "outreach_fallback_on_empty_state",
                "OUTREACH_STATE_SPREAD_MODE": "outreach_state_spread_mode",
                "PROSPECT_AUTOGROW_ENABLED": "prospect_autogrow_enabled",
                "PROSPECT_AUTOGROW_SAFETY_NET_ENABLED": "prospect_autogrow_safety_net_enabled",
                "PROSPECT_AI_ASSIST_REVIEW_ENABLED": "prospect_ai_assist_review_enabled",
                "AI_TRIAGE_ENABLED": "ai_triage_enabled",
                "OUTREACH_SKIP_ROLE_INBOXES": "outreach_skip_role_inboxes",
                "OUTREACH_ALLOW_FREE_DOMAINS": "outreach_allow_free_domains",
                "PROSPECT_AUTOGROW_STATES": "prospect_autogrow_states",
            }
            for env_key, normalized_key in mapping.items():
                candidate = str(os.environ.get(env_key) or env_file.get(env_key) or "").strip()
                if candidate:
                    values[normalized_key] = candidate
        try:
            values["outreach_states"] = _normalize_state_csv(values["outreach_states"])
        except Exception as exc:
            warnings.append(f"Current outreach state scope is invalid: {exc}")
        return {
            **values,
            "source": source,
            "warnings": warnings,
            "command_exit_code": result.exit_code,
            "command_stdout": result.stdout,
            "command_stderr": result.stderr,
        }

    def runtime_health(self) -> dict[str, Any]:
        status_root = (self.data_dir / "runtime" / "status").resolve(strict=False)
        latest_path = status_root / "runtime_latest.json"
        latest = _safe_json_load(latest_path)
        jobs: list[dict[str, Any]] = []
        jobs_root = status_root / "jobs"
        for path in sorted(jobs_root.glob("*.json")) if jobs_root.exists() else []:
            payload = _safe_json_load(path)
            if payload:
                payload.setdefault("job_name", path.stem)
                jobs.append(payload)
        return {
            "status_root": status_root,
            "runtime_latest_path": latest_path,
            "runtime_latest": latest,
            "jobs": jobs,
            "alerts": dict(latest.get("alerts") or {}),
        }

    def latest_ops_snapshot(self) -> dict[str, Any]:
        path = (self.data_dir / "outreach" / "ops_snapshots" / "latest.json").resolve(strict=False)
        return {"path": path, "payload": _safe_json_load(path)}

    def _plan_env(self, env_values: dict[str, str]) -> dict[str, str]:
        return {
            "DATA_DIR": str(self.data_dir),
            "OUTREACH_STATES": str(env_values.get("outreach_states") or ""),
            "OUTREACH_DAILY_LIMIT": str(env_values.get("outreach_daily_limit") or ""),
            "OUTREACH_FALLBACK_ON_EMPTY_STATE": str(env_values.get("outreach_fallback_on_empty_state") or "0"),
            "OUTREACH_STATE_SPREAD_MODE": str(env_values.get("outreach_state_spread_mode") or "round_robin"),
            "OUTREACH_SKIP_ROLE_INBOXES": str(env_values.get("outreach_skip_role_inboxes") or "1"),
            "OUTREACH_ALLOW_FREE_DOMAINS": str(env_values.get("outreach_allow_free_domains") or "0"),
        }

    def outreach_plan_preview(self, *, for_date: str, env_values: dict[str, str]) -> dict[str, Any]:
        cmd = self._python_command("run_outreach_auto.py", "--plan", "--for-date", for_date)
        result = self._command_runner(cmd, env=self._plan_env(env_values), timeout_seconds=180)
        plan: dict[str, Any] = {
            "date": for_date,
            "command": result.display_command,
            "exit_code": result.exit_code,
            "stdout": result.stdout,
            "stderr": result.stderr,
            "sendable_by_state": {},
            "selected_by_state": {},
            "below_floor_states": [],
        }
        for raw_line in result.stdout.splitlines():
            line = raw_line.strip()
            if line.startswith("OUTREACH_PLAN_DATE="):
                plan["resolved_date"] = line.split("=", 1)[1].strip()
            elif line.startswith("OUTREACH_PLAN_STATE="):
                plan["state"] = line.split("=", 1)[1].strip()
            elif line.startswith("OUTREACH_PLAN_BATCH="):
                plan["batch"] = line.split("=", 1)[1].strip()
            elif line.startswith("OUTREACH_PLAN_DAILY_LIMIT="):
                plan["daily_limit"] = line.split("=", 1)[1].strip()
            elif line.startswith("OUTREACH_PLAN_WILL_SEND="):
                plan["will_send"] = line.split("=", 1)[1].strip()
            elif line.startswith("OUTREACH_STATE_ROTATION_SELECTED="):
                plan["rotation_selected_state"] = line.split("=", 1)[1].strip()
            elif line.startswith("OUTREACH_STATE_EFFECTIVE_SEND="):
                plan["effective_send_state"] = line.split("=", 1)[1].strip()
            elif line.startswith("OUTREACH_SELECTED_BY_STATE="):
                plan["selected_by_state"] = _parse_selected_by_state_csv(line.split("=", 1)[1].strip())
            elif line.startswith("OUTREACH_STATE_SENDABLE_ESTIMATE "):
                fields = _parse_inline_key_values(line)
                state = str(fields.get("state") or "").strip().upper()
                if state:
                    plan["sendable_by_state"][state] = int(fields.get("sendable") or 0)
            elif line.startswith("OUTREACH_STATE_BELOW_SEND_FLOOR "):
                fields = _parse_inline_key_values(line)
                state = str(fields.get("state") or "").strip().upper()
                if state:
                    plan["below_floor_states"].append(state)
            elif line.startswith("OUTREACH_FALLBACK_TRIGGERED="):
                fields = _parse_inline_key_values(line)
                plan["fallback_reason"] = fields.get("reason", "")
        if result.exit_code != 0:
            plan["error"] = (result.stderr or result.stdout or "Plan command failed").strip()
        return plan

    def next_week_outreach_preview(self, env_values: dict[str, str]) -> list[dict[str, Any]]:
        zone = _resolve_zone("America/New_York")
        base_today = self._now_provider().astimezone(zone).date() if zone is not None else self._now_provider().date()
        previews: list[dict[str, Any]] = []
        for offset in range(7):
            preview_date = base_today + timedelta(days=offset)
            plan = self.outreach_plan_preview(
                for_date=preview_date.isoformat(),
                env_values=env_values,
            )
            if preview_date.weekday() >= 5:
                plan["will_send"] = "0"
                plan["fallback_reason"] = "SKIP_WEEKEND"
            previews.append(plan)
        return previews

    def trials_data(self) -> dict[str, Any]:
        crm_db = (self.data_dir / "crm_light.sqlite").resolve(strict=False)
        if not crm_db.exists():
            return {"crm_db": crm_db, "rows": [], "warnings": ["crm_light.sqlite not found"]}
        rows: list[dict[str, Any]] = []
        warnings: list[str] = []
        now_utc = self._now_provider()
        conn = sqlite3.connect(str(crm_db))
        conn.row_factory = sqlite3.Row
        try:
            query_rows = conn.execute(
                """
                SELECT s.subscriber_key, s.email, s.territory_code, s.tz, s.status,
                       t.start_date, t.sends_limit, t.notified_at_utc, t.ended_at_utc
                FROM subscribers s
                JOIN trial_state t ON t.subscriber_key = s.subscriber_key
                ORDER BY s.subscriber_key ASC
                """
            ).fetchall()
            for row in query_rows:
                payload = dict(row)
                subscriber_key = str(payload.get("subscriber_key") or "").strip().lower()
                customer_config_result = run_trial_admin._resolve_customer_config_for_subscriber(subscriber_key)  # type: ignore[attr-defined]
                customer_config_path: Path | None = None
                customer_config: dict[str, Any] = {}
                if isinstance(customer_config_result, tuple):
                    if len(customer_config_result) >= 1 and isinstance(customer_config_result[0], Path):
                        customer_config_path = customer_config_result[0]
                    if len(customer_config_result) >= 2 and isinstance(customer_config_result[1], dict):
                        customer_config = dict(customer_config_result[1])
                elif isinstance(customer_config_result, dict):
                    customer_config = dict(customer_config_result)
                recipients = list(customer_config.get("email_recipients") or customer_config.get("recipients") or [])
                if not recipients:
                    recipients = [str(payload.get("email") or "").strip()]
                sends_limit = int(payload.get("sends_limit") or 0)
                sends_used = crm_light.count_trial_delivery_days(
                    conn,
                    subscriber_key=subscriber_key,
                    start_date=str(payload.get("start_date") or ""),
                    tz_name=str(payload.get("tz") or "America/New_York"),
                    primary_recipient=str(recipients[0] or ""),
                )
                projected_end = _project_weekday_end_date(str(payload.get("start_date") or ""), sends_limit)
                next_send_time = _compute_next_send_time(
                    send_time_local=str(customer_config.get("send_time_local") or self.schedule().trial_default_send_local_hhmm),
                    tz_name=str(customer_config.get("timezone") or payload.get("tz") or "America/New_York"),
                    now_utc=now_utc,
                )
                conversion_path = (self.data_dir / "trials" / subscriber_key / "conversion_email.txt").resolve(strict=False)
                expired = bool(str(payload.get("ended_at_utc") or "").strip()) or sends_used >= sends_limit
                pending = sends_used == 0 and str(payload.get("status") or "").strip().lower() not in {"expired", "ended"}
                conversion_due = expired and not str(payload.get("notified_at_utc") or "").strip()
                rows.append(
                    {
                        **payload,
                        "subscriber_key": subscriber_key,
                        "recipients": recipients,
                        "sends_used": sends_used,
                        "projected_end_date": projected_end,
                        "next_send_time": next_send_time,
                        "conversion_due": conversion_due,
                        "conversion_draft_exists": conversion_path.exists(),
                        "conversion_draft_path": conversion_path,
                        "customer_config_path": customer_config_path,
                        "state_scope": list(customer_config.get("states") or []),
                        "territory_scope": str(customer_config.get("territory_code") or payload.get("territory_code") or ""),
                        "trial_status_group": "expired" if expired else ("pending" if pending else "active"),
                    }
                )
        except sqlite3.Error as exc:
            warnings.append(f"crm_light read failed: {exc}")
        finally:
            conn.close()
        return {"crm_db": crm_db, "rows": rows, "warnings": warnings}

    def manual_import_data(self) -> dict[str, Any]:
        audit_dir = ai_assist_paths.prospect_audit_dir(self.data_dir, repo_root=self.repo_root)
        skip_candidates = sorted(
            audit_dir.glob("crm_skip_list_for_ai.csv"),
            key=lambda item: item.stat().st_mtime if item.exists() else 0,
            reverse=True,
        )
        prompt_candidates = sorted(
            audit_dir.glob("manual_prospect_deep_research_*.txt"),
            key=lambda item: item.stat().st_mtime if item.exists() else 0,
            reverse=True,
        )
        review_dirs = [
            ai_assist_paths.PathCandidate(
                path=ai_assist_paths.prospect_import_dir(self.data_dir, repo_root=self.repo_root),
                is_legacy=False,
                source="canonical",
            ),
            ai_assist_paths.PathCandidate(
                path=ai_assist_paths.legacy_prospect_audit_dir(self.data_dir, repo_root=self.repo_root),
                is_legacy=True,
                source="legacy",
            ),
        ]
        pending_files = ai_assist_import._discover_pending_review_files(review_dirs)  # type: ignore[attr-defined]
        import_batches: list[dict[str, Any]] = []
        crm_db = (self.data_dir / "crm.sqlite").resolve(strict=False)
        if crm_db.exists():
            conn = sqlite3.connect(str(crm_db))
            conn.row_factory = sqlite3.Row
            try:
                crm_store.init_schema(conn)
                import_batches = [
                    dict(row)
                    for row in conn.execute(
                        f"""
                        SELECT batch_id, source_filename, status, completed_at, last_error,
                               candidates_total, accepted_total, rejected_total, verified_total, updated_at
                        FROM {crm_store.AI_ASSIST_IMPORT_BATCH_TABLE}
                        ORDER BY COALESCE(updated_at, '') DESC, batch_id DESC
                        LIMIT 10
                        """
                    ).fetchall()
                ]
            except Exception:
                import_batches = []
            finally:
                conn.close()
        return {
            "audit_dir": audit_dir,
            "skip_list_path": skip_candidates[0] if skip_candidates else None,
            "prompt_path": prompt_candidates[0] if prompt_candidates else None,
            "pending_files": [{"path": path, "legacy": legacy} for path, legacy in pending_files],
            "review_dirs": review_dirs,
            "import_batches": import_batches,
        }

    def inbox_data(self) -> dict[str, Any]:
        out_root = (self.repo_root / "out").resolve(strict=False)
        onboarding_path = out_root / "onboarding_audit_log.csv"
        triage_log_path = out_root / "inbox_triage_log.csv"
        reply_drafts_dir = out_root / "reply_drafts"
        eng_tickets_dir = out_root / "eng_tickets"
        env_file = _read_env_file(self.repo_root / ".env")
        print_values, print_result = self._set_outreach_env_print_config()

        def _print_or_env(key: str, *fallback_keys: str, default: str = "") -> str:
            normalized_keys = [str(key or "").strip().lower(), *[str(item or "").strip().lower() for item in fallback_keys]]
            for normalized_key in normalized_keys:
                if normalized_key and normalized_key in print_values:
                    candidate = str(print_values.get(normalized_key) or "").strip()
                    if candidate:
                        return candidate
            env_keys = [key, *fallback_keys]
            for env_key in env_keys:
                candidate = _env_or_file_value(env_file, env_key, default="")
                if candidate:
                    return candidate
            return str(default or "").strip()

        backend = _print_or_env("INBOUND_BACKEND", default="").lower()
        if not backend:
            imap_markers = (
                _print_or_env("IMAP_USER", "BOUNCE_IMAP_USER"),
                _print_or_env("IMAP_PASS", "BOUNCE_IMAP_PASS"),
                _print_or_env("IMAP_HOST", "BOUNCE_IMAP_HOST"),
            )
            backend = "imap" if any(str(marker or "").strip() for marker in imap_markers) else "gmail"
        credentials_path = (self.repo_root / "secrets" / "gmail_credentials.json").resolve(strict=False)
        token_path = (self.repo_root / "secrets" / "gmail_token.json").resolve(strict=False)
        reply_drafts = sorted(reply_drafts_dir.glob("*"), key=lambda item: item.stat().st_mtime if item.exists() else 0, reverse=True)[:10]
        eng_tickets = sorted(eng_tickets_dir.glob("*"), key=lambda item: item.stat().st_mtime if item.exists() else 0, reverse=True)[:10]
        entitlement_rows: list[dict[str, Any]] = []
        crm_db = (self.data_dir / "crm_light.sqlite").resolve(strict=False)
        if crm_db.exists():
            conn = sqlite3.connect(str(crm_db))
            conn.row_factory = sqlite3.Row
            try:
                entitlement_rows = [
                    dict(row)
                    for row in conn.execute(
                        """
                        SELECT subscriber_key, email, plan_code, active, source, created_at_utc
                        FROM subscriber_entitlements
                        ORDER BY created_at_utc DESC, subscriber_key DESC
                        LIMIT 10
                        """
                    ).fetchall()
                ]
            except sqlite3.Error:
                entitlement_rows = []
            finally:
                conn.close()

        gmail_missing_modules = [
            module_name
            for module_name in ("googleapiclient.discovery", "google_auth_oauthlib.flow", "google.oauth2.credentials")
            if not _python_module_available(module_name)
        ]
        imap_user = _print_or_env("IMAP_USER", "BOUNCE_IMAP_USER")
        imap_pass_present = _print_or_env("imap_pass_present")
        if not imap_pass_present:
            imap_pass_present = "YES" if _print_or_env("IMAP_PASS", "BOUNCE_IMAP_PASS") else "NO"
        imap_host = _print_or_env("IMAP_HOST", "BOUNCE_IMAP_HOST", default="imappro.zoho.com") or "imappro.zoho.com"
        imap_port = _print_or_env("IMAP_PORT", "BOUNCE_IMAP_PORT", default="993") or "993"
        imap_folder = _print_or_env("IMAP_FOLDER", "BOUNCE_IMAP_FOLDER", default="INBOX") or "INBOX"
        imap_folder_unsub = _print_or_env("IMAP_FOLDER_UNSUB", default="Processed/Unsubscribe") or "Processed/Unsubscribe"
        imap_folder_bounce = _print_or_env("IMAP_FOLDER_BOUNCE", default="Processed/Bounce") or "Processed/Bounce"
        inbound_setup: dict[str, Any] = {
            "backend": backend,
            "backend_source": _print_or_env("inbound_backend_source", default="fallback" if print_result.exit_code else ""),
            "credentials_path": credentials_path,
            "credentials_present": credentials_path.exists(),
            "token_path": token_path,
            "token_present": token_path.exists(),
            "gmail_client_deps_installed": not gmail_missing_modules,
            "gmail_missing_modules": gmail_missing_modules,
            "imap_host": imap_host,
            "imap_port": imap_port,
            "imap_user": imap_user,
            "imap_user_present": bool(imap_user),
            "imap_pass_present": str(imap_pass_present).strip().upper() == "YES",
            "imap_source": _print_or_env("imap_source", default="fallback" if print_result.exit_code else ""),
            "imap_folder": imap_folder,
            "imap_folder_unsub": imap_folder_unsub,
            "imap_folder_bounce": imap_folder_bounce,
            "triage_log_path": triage_log_path,
            "reply_drafts_dir": reply_drafts_dir,
            "eng_tickets_dir": eng_tickets_dir,
            "commands": [],
            "recommended_next_step": "",
            "status": "",
        }
        if backend == "imap":
            inbound_setup["commands"] = [
                "powershell -NoProfile -ExecutionPolicy Bypass -File .\\scripts\\set_outreach_env.ps1 -InboundBackend imap -SyncInboundImapFromBounce",
                "powershell -NoProfile -ExecutionPolicy Bypass -File .\\scripts\\set_outreach_env.ps1 -PrintConfig",
                ".\\run_with_secrets.ps1 -- py -3 inbound_inbox_triage.py --dry-run --since-hours 1",
                ".\\run_with_secrets.ps1 -- py -3 inbound_inbox_triage.py --run-once",
            ]
            if bool(imap_user) and (str(imap_pass_present).strip().upper() == "YES"):
                inbound_setup["status"] = "configured"
                inbound_setup["recommended_next_step"] = "Run the IMAP dry-run triage check when you want to verify the inbound path."
            else:
                inbound_setup["status"] = "not configured"
                inbound_setup["recommended_next_step"] = "Run the safe IMAP sync command to copy the saved Zoho mailbox values into INBOUND_BACKEND and IMAP_* before rerunning the IMAP dry-run triage check."
        elif backend == "gmail":
            inbound_setup["commands"] = [
                "powershell -NoProfile -ExecutionPolicy Bypass -File .\\scripts\\set_outreach_env.ps1 -InboundBackend imap -SyncInboundImapFromBounce",
                "py -3 -m pip install google-api-python-client google-auth-oauthlib",
                "py -3 inbound_inbox_triage.py --dry-run --since-hours 1",
                "py -3 inbound_inbox_triage.py --run-once",
            ]
            if gmail_missing_modules:
                inbound_setup["status"] = "deps missing"
                inbound_setup["recommended_next_step"] = "Install the Gmail client packages, then rerun the dry-run triage bootstrap."
            elif not credentials_path.exists():
                inbound_setup["status"] = "not configured"
                inbound_setup["recommended_next_step"] = "Gmail is optional. For Zoho/IMAP use the safe IMAP sync command; only create secrets/gmail_credentials.json if you intentionally want the Gmail backend."
            elif not token_path.exists():
                inbound_setup["status"] = "ready for first OAuth bootstrap"
                inbound_setup["recommended_next_step"] = "Run the dry-run triage bootstrap, then a single --run-once bootstrap on the canonical PC."
            else:
                inbound_setup["status"] = "configured"
                inbound_setup["recommended_next_step"] = "Run the dry-run triage check when you want to verify the Gmail inbox path."
        else:
            inbound_setup["status"] = f"unsupported backend: {backend}"
            inbound_setup["recommended_next_step"] = "Set INBOUND_BACKEND to gmail or imap, then rerun the inbox check."
        return {
            "onboarding_path": onboarding_path,
            "onboarding_rows": _csv_rows(onboarding_path, limit=12),
            "triage_log_path": triage_log_path,
            "triage_rows": _csv_rows(triage_log_path, limit=12),
            "reply_drafts": reply_drafts,
            "eng_tickets": eng_tickets,
            "inbound_setup": inbound_setup,
            "entitlements": entitlement_rows,
            "trial_request_registry_status": "No local trial-request artifact",
        }

    def needs_attention_queue(self) -> list[str]:
        items: list[str] = []
        runtime = self.runtime_health()
        latest = runtime.get("runtime_latest") or {}
        for job in list(latest.get("jobs") or []):
            name = str(job.get("name") or "").strip()
            result = str(job.get("result") or "").strip().lower()
            reason = str(job.get("reason") or "").strip()
            if result == "failed":
                items.append(f"Runtime failure: {name} ({reason or 'failed'})")
            elif result == "skipped" and reason.startswith("window_closed_"):
                items.append(f"Missed window: {name} ({reason})")
        env_values = self.outreach_env_snapshot()
        today_plan = self.outreach_plan_preview(
            for_date=self._now_provider().date().isoformat(),
            env_values=env_values,
        )
        for state in list(today_plan.get("below_floor_states") or []):
            items.append(f"State backlog low: {state} is below send floor")
        for row in list((self.trials_data().get("rows") or [])):
            if bool(row.get("conversion_due")):
                items.append(f"Trial conversion due: {row.get('subscriber_key')}")
        pending_total = len(list((self.manual_import_data().get("pending_files") or [])))
        if pending_total:
            items.append(f"Pending reviewed CSV imports: {pending_total}")
        return items[:20]

    def dashboard_data(self) -> dict[str, Any]:
        env_values = self.outreach_env_snapshot()
        next_week = self.next_week_outreach_preview(env_values)
        backlog_by_state = dict((next_week[0].get("sendable_by_state") or {}) if next_week else {})
        return {
            "runtime": self.runtime_health(),
            "outreach_env": env_values,
            "next_week": next_week,
            "backlog_by_state": backlog_by_state,
            "trials": self.trials_data(),
            "manual": self.manual_import_data(),
            "ops_snapshot": self.latest_ops_snapshot(),
            "inbox": self.inbox_data(),
            "needs_attention": self.needs_attention_queue(),
        }

    def build_outreach_preview(self, form: dict[str, str], *, title: str) -> dict[str, Any]:
        current = self.outreach_env_snapshot()
        desired = {
            "outreach_daily_limit": str(int(str(form.get("outreach_daily_limit") or current["outreach_daily_limit"]).strip())),
            "outreach_states": _normalize_state_csv(str(form.get("outreach_states") or current["outreach_states"])),
            "outreach_fallback_on_empty_state": _normalize_boolish_int(
                str(form.get("outreach_fallback_on_empty_state") or current["outreach_fallback_on_empty_state"]),
                field_name="outreach_fallback_on_empty_state",
            ),
            "outreach_state_spread_mode": str(
                form.get("outreach_state_spread_mode") or current.get("outreach_state_spread_mode") or "round_robin"
            ).strip().lower(),
            "prospect_autogrow_enabled": _normalize_boolish_int(
                str(form.get("prospect_autogrow_enabled") or current["prospect_autogrow_enabled"]),
                field_name="prospect_autogrow_enabled",
            ),
            "prospect_autogrow_safety_net_enabled": _normalize_boolish_int(
                str(form.get("prospect_autogrow_safety_net_enabled") or current["prospect_autogrow_safety_net_enabled"]),
                field_name="prospect_autogrow_safety_net_enabled",
            ),
            "prospect_ai_assist_review_enabled": _normalize_boolish_int(
                str(form.get("prospect_ai_assist_review_enabled") or current["prospect_ai_assist_review_enabled"]),
                field_name="prospect_ai_assist_review_enabled",
            ),
            "ai_triage_enabled": _normalize_boolish_int(
                str(form.get("ai_triage_enabled") or current["ai_triage_enabled"]),
                field_name="ai_triage_enabled",
            ),
        }
        command = self._powershell_file_command(
            "scripts/set_outreach_env.ps1",
            "-OutreachDailyLimit",
            desired["outreach_daily_limit"],
            "-OutreachStates",
            desired["outreach_states"],
            "-OutreachFallbackOnEmptyState",
            desired["outreach_fallback_on_empty_state"],
            "-OutreachStateSpreadMode",
            desired["outreach_state_spread_mode"],
            "-ProspectAutoGrowEnabled",
            desired["prospect_autogrow_enabled"],
            "-ProspectAutoGrowSafetyNetEnabled",
            desired["prospect_autogrow_safety_net_enabled"],
            "-ProspectAiAssistReviewEnabled",
            desired["prospect_ai_assist_review_enabled"],
            "-AiTriageEnabled",
            desired["ai_triage_enabled"],
        )
        preview_data = {
            "current": current,
            "desired": desired,
            "command": subprocess.list2cmdline(command),
            "next_week_preview": self.next_week_outreach_preview({**current, **desired}),
        }
        return self._create_preview(kind="outreach_env", title=title, payload=desired, preview_data=preview_data)

    def apply_outreach_preview(self, *, preview_id: str, payload_hash: str) -> tuple[dict[str, Any], CommandResult]:
        preview = self._assert_preview(preview_id=preview_id, payload_hash=payload_hash, kind="outreach_env")
        payload = dict(preview.get("payload") or {})
        command = self._powershell_file_command(
            "scripts/set_outreach_env.ps1",
            "-OutreachDailyLimit",
            str(payload["outreach_daily_limit"]),
            "-OutreachStates",
            str(payload["outreach_states"]),
            "-OutreachFallbackOnEmptyState",
            str(payload["outreach_fallback_on_empty_state"]),
            "-OutreachStateSpreadMode",
            str(payload["outreach_state_spread_mode"]),
            "-ProspectAutoGrowEnabled",
            str(payload["prospect_autogrow_enabled"]),
            "-ProspectAutoGrowSafetyNetEnabled",
            str(payload["prospect_autogrow_safety_net_enabled"]),
            "-ProspectAiAssistReviewEnabled",
            str(payload["prospect_ai_assist_review_enabled"]),
            "-AiTriageEnabled",
            str(payload["ai_triage_enabled"]),
        )
        result = self._command_runner(command, timeout_seconds=300)
        self._write_audit_event(
            kind="outreach_env",
            action="apply_completed" if result.exit_code == 0 else "apply_failed",
            payload=payload,
            extra={"preview_id": preview_id, "command": result.display_command, "stdout": result.stdout, "stderr": result.stderr, "exit_code": result.exit_code},
        )
        return preview, result

    def build_state_scope_preview(self, *, action: str, state_code: str) -> dict[str, Any]:
        current = self.outreach_env_snapshot()
        states = _parse_state_csv(str(current.get("outreach_states") or ""))
        state = str(state_code or "").strip().upper()
        if not re.fullmatch(r"[A-Z]{2}", state):
            raise ValueError("state must be a 2-letter USPS code")
        if action == "add" and state not in states:
            states.append(state)
        elif action == "remove":
            states = [item for item in states if item != state]
        if not states:
            raise ValueError("at least one live outreach state is required")
        desired_states = ",".join(states)
        current_autogrow = _parse_state_csv(str(current.get("prospect_autogrow_states") or "")) if str(current.get("prospect_autogrow_states") or "").strip() else []
        desired = {**current, "outreach_states": desired_states}
        scope_effects = {
            "target_state": state,
            "desired_outreach_states": states,
            "current_autogrow_states": current_autogrow,
            "outreach_rotation_active": state in states,
            "replenishment_active": state in current_autogrow,
            "discovery_active": state in current_autogrow,
            "manual_prompt_scope_active": state in states,
            "state_lic_warning": "STATE_LIC remains TX-only in v1.",
            "scope_drift_warning": (
                f"Autogrow scope drift: outreach={desired_states} autogrow={','.join(current_autogrow)}"
                if current_autogrow and desired_states != ",".join(current_autogrow)
                else ""
            ),
            "ingest_effect": "Raw ingest is not directly state-scoped here; live scope changes affect downstream rotation and replenishment visibility.",
        }
        command = self._powershell_file_command(
            "scripts/set_outreach_env.ps1",
            "-OutreachDailyLimit",
            str(current["outreach_daily_limit"]),
            "-OutreachStates",
            desired_states,
            "-OutreachFallbackOnEmptyState",
            str(current["outreach_fallback_on_empty_state"]),
            "-ProspectAutoGrowEnabled",
            str(current["prospect_autogrow_enabled"]),
            "-ProspectAutoGrowSafetyNetEnabled",
            str(current["prospect_autogrow_safety_net_enabled"]),
            "-ProspectAiAssistReviewEnabled",
            str(current["prospect_ai_assist_review_enabled"]),
            "-AiTriageEnabled",
            str(current["ai_triage_enabled"]),
        )
        return self._create_preview(
            kind="state_scope",
            title="State Scope Preview",
            payload={"outreach_states": desired_states, "action": action, "state_code": state},
            preview_data={
                "command": subprocess.list2cmdline(command),
                "scope_effects": scope_effects,
                "next_week_preview": self.next_week_outreach_preview(desired),
            },
        )

    def apply_state_scope_preview(self, *, preview_id: str, payload_hash: str) -> tuple[dict[str, Any], CommandResult]:
        preview = self._assert_preview(preview_id=preview_id, payload_hash=payload_hash, kind="state_scope")
        payload = dict(preview.get("payload") or {})
        current = self.outreach_env_snapshot()
        command = self._powershell_file_command(
            "scripts/set_outreach_env.ps1",
            "-OutreachDailyLimit",
            str(current["outreach_daily_limit"]),
            "-OutreachStates",
            str(payload["outreach_states"]),
            "-OutreachFallbackOnEmptyState",
            str(current["outreach_fallback_on_empty_state"]),
            "-ProspectAutoGrowEnabled",
            str(current["prospect_autogrow_enabled"]),
            "-ProspectAutoGrowSafetyNetEnabled",
            str(current["prospect_autogrow_safety_net_enabled"]),
            "-ProspectAiAssistReviewEnabled",
            str(current["prospect_ai_assist_review_enabled"]),
            "-AiTriageEnabled",
            str(current["ai_triage_enabled"]),
        )
        result = self._command_runner(command, timeout_seconds=300)
        self._write_audit_event(
            kind="state_scope",
            action="apply_completed" if result.exit_code == 0 else "apply_failed",
            payload=payload,
            extra={"preview_id": preview_id, "command": result.display_command, "stdout": result.stdout, "stderr": result.stderr, "exit_code": result.exit_code},
        )
        return preview, result

    def build_schedule_preview(self, form: dict[str, str]) -> dict[str, Any]:
        current = self.schedule()
        payload = {
            "outreach_send_local_hhmm": validate_local_hhmm(str(form.get("outreach_send_local_hhmm") or current.outreach_send_local_hhmm), field_name="outreach_send_local_hhmm"),
            "trial_default_send_local_hhmm": validate_local_hhmm(str(form.get("trial_default_send_local_hhmm") or current.trial_default_send_local_hhmm), field_name="trial_default_send_local_hhmm"),
            "evening_prep_local_hhmm": validate_local_hhmm(str(form.get("evening_prep_local_hhmm") or current.evening_prep_local_hhmm), field_name="evening_prep_local_hhmm"),
        }
        mirrored_tasks = [
            {"name": "OSHA_Outreach_Auto_SafetyNet", "time": payload["outreach_send_local_hhmm"]},
            {"name": "OSHA_Trial_FACS_Daily", "time": payload["trial_default_send_local_hhmm"]},
            {"name": "OSHA_Trial_JL_Safety_Daily", "time": payload["trial_default_send_local_hhmm"]},
            {"name": "OSHA_Trial_ROI_Safety_Daily", "time": payload["trial_default_send_local_hhmm"]},
            {"name": "OSHA_Osha_Ingest_Evening", "time": payload["evening_prep_local_hhmm"]},
        ]
        managed_files = [{"path": str(path), "send_time_local": payload["trial_default_send_local_hhmm"]} for path in MANAGED_TRIAL_CUSTOMER_PATHS]
        return self._create_preview(
            kind="schedule",
            title="Scheduling Preview",
            payload=payload,
            preview_data={
                "current": current.as_dict(),
                "desired": payload,
                "schedule_config_path": str(schedule_config_path(self.data_dir)),
                "mirrored_tasks": mirrored_tasks,
                "managed_trial_customer_updates": managed_files,
                "sync_policy": "Sync Both",
                "command": subprocess.list2cmdline(self._powershell_file_command("scripts/install_scheduled_tasks.ps1", "--apply")),
            },
        )

    def _write_managed_trial_customer_time(self, *, send_time_local: str) -> None:
        for path in MANAGED_TRIAL_CUSTOMER_PATHS:
            if not path.exists():
                continue
            payload = json.loads(path.read_text(encoding="utf-8"))
            payload["send_time_local"] = send_time_local
            path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")

    def apply_schedule_preview(self, *, preview_id: str, payload_hash: str) -> tuple[dict[str, Any], CommandResult]:
        preview = self._assert_preview(preview_id=preview_id, payload_hash=payload_hash, kind="schedule")
        payload = dict(preview.get("payload") or {})
        schedule_path = schedule_config_path(self.data_dir)
        before_schedule_text = schedule_path.read_text(encoding="utf-8") if schedule_path.exists() else None
        customer_backups = {path: path.read_text(encoding="utf-8") for path in MANAGED_TRIAL_CUSTOMER_PATHS if path.exists()}
        try:
            write_runtime_schedule(
                self.data_dir,
                outreach_send_local_hhmm=str(payload["outreach_send_local_hhmm"]),
                trial_default_send_local_hhmm=str(payload["trial_default_send_local_hhmm"]),
                evening_prep_local_hhmm=str(payload["evening_prep_local_hhmm"]),
                updated_by="ops_console",
            )
            self._write_managed_trial_customer_time(send_time_local=str(payload["trial_default_send_local_hhmm"]))
            result = self._command_runner(
                self._powershell_file_command("scripts/install_scheduled_tasks.ps1", "--apply"),
                timeout_seconds=300,
            )
            if result.exit_code != 0:
                raise RuntimeError(result.stderr or result.stdout or "scheduled task sync failed")
        except Exception as exc:
            if before_schedule_text is None:
                try:
                    if schedule_path.exists():
                        schedule_path.unlink()
                except Exception:
                    pass
            else:
                schedule_path.parent.mkdir(parents=True, exist_ok=True)
                schedule_path.write_text(before_schedule_text, encoding="utf-8")
            for path, content in customer_backups.items():
                path.write_text(content, encoding="utf-8")
            failed_result = CommandResult(
                command=self._powershell_file_command("scripts/install_scheduled_tasks.ps1", "--apply"),
                exit_code=1,
                stdout="",
                stderr=str(exc),
            )
            self._write_audit_event(
                kind="schedule",
                action="apply_failed",
                payload=payload,
                extra={"preview_id": preview_id, "stderr": failed_result.stderr, "exit_code": failed_result.exit_code},
            )
            return preview, failed_result
        self._write_audit_event(
            kind="schedule",
            action="apply_completed",
            payload=payload,
            extra={"preview_id": preview_id, "command": result.display_command, "stdout": result.stdout, "stderr": result.stderr, "exit_code": result.exit_code},
        )
        return preview, result

    def build_trial_add_preview(self, form: dict[str, str]) -> dict[str, Any]:
        subscriber_key = str(form.get("subscriber_key") or "").strip()
        email = str(form.get("email") or "").strip()
        scope_states = str(form.get("states") or "").strip()
        territory = str(form.get("territory") or "").strip()
        if not subscriber_key or not email:
            raise ValueError("subscriber key and email are required")
        if not scope_states and not territory:
            raise ValueError("either states or territory is required")
        send_time_local = validate_local_hhmm(
            str(form.get("send_time_local") or self.schedule().trial_default_send_local_hhmm),
            field_name="send_time_local",
        )
        command = self._python_command(
            "run_trial_admin.py",
            "add-trial",
            "--subscriber-key",
            subscriber_key,
            "--email",
            email,
            *(["--states", _normalize_state_csv(scope_states)] if scope_states else ["--territory", territory]),
            "--tz",
            str(form.get("tz") or "America/New_York"),
            "--send-time-local",
            send_time_local,
            "--start-date",
            str(form.get("start_date") or ""),
            "--sends-limit",
            str(int(str(form.get("sends_limit") or "14"))),
        )
        payload = {
            "subscriber_key": subscriber_key,
            "email": email,
            "states": _normalize_state_csv(scope_states) if scope_states else "",
            "territory": territory,
            "tz": str(form.get("tz") or "America/New_York"),
            "send_time_local": send_time_local,
            "start_date": str(form.get("start_date") or ""),
            "sends_limit": str(int(str(form.get("sends_limit") or "14"))),
        }
        return self._create_preview(kind="trial_add", title="Add Trial Preview", payload=payload, preview_data={"command": subprocess.list2cmdline(command)})

    def apply_trial_add_preview(self, *, preview_id: str, payload_hash: str) -> tuple[dict[str, Any], CommandResult]:
        preview = self._assert_preview(preview_id=preview_id, payload_hash=payload_hash, kind="trial_add")
        payload = dict(preview.get("payload") or {})
        command = self._python_command(
            "run_trial_admin.py",
            "add-trial",
            "--subscriber-key",
            str(payload["subscriber_key"]),
            "--email",
            str(payload["email"]),
            *(["--states", str(payload["states"])] if str(payload.get("states") or "").strip() else ["--territory", str(payload["territory"])]),
            "--tz",
            str(payload["tz"]),
            "--send-time-local",
            str(payload["send_time_local"]),
            "--start-date",
            str(payload["start_date"]),
            "--sends-limit",
            str(payload["sends_limit"]),
        )
        result = self._command_runner(command, timeout_seconds=300)
        self._write_audit_event(
            kind="trial_add",
            action="apply_completed" if result.exit_code == 0 else "apply_failed",
            payload=payload,
            extra={"preview_id": preview_id, "command": result.display_command, "stdout": result.stdout, "stderr": result.stderr, "exit_code": result.exit_code},
        )
        return preview, result

    def build_trial_conversion_preview(self, subscriber_key: str) -> dict[str, Any]:
        payload = {"subscriber_key": str(subscriber_key or "").strip()}
        if not payload["subscriber_key"]:
            raise ValueError("subscriber_key required")
        command = self._python_command("run_trial_admin.py", "conversion-draft", "--subscriber-key", payload["subscriber_key"])
        return self._create_preview(kind="trial_conversion", title="Conversion Draft Preview", payload=payload, preview_data={"command": subprocess.list2cmdline(command)})

    def apply_trial_conversion_preview(self, *, preview_id: str, payload_hash: str) -> tuple[dict[str, Any], CommandResult]:
        preview = self._assert_preview(preview_id=preview_id, payload_hash=payload_hash, kind="trial_conversion")
        payload = dict(preview.get("payload") or {})
        result = self._command_runner(
            self._python_command("run_trial_admin.py", "conversion-draft", "--subscriber-key", str(payload["subscriber_key"])),
            timeout_seconds=300,
        )
        self._write_audit_event(
            kind="trial_conversion",
            action="apply_completed" if result.exit_code == 0 else "apply_failed",
            payload=payload,
            extra={"preview_id": preview_id, "command": result.display_command, "stdout": result.stdout, "stderr": result.stderr, "exit_code": result.exit_code},
        )
        return preview, result

    def build_trial_mark_preview(self, form: dict[str, str]) -> dict[str, Any]:
        payload = {
            "prospect_id": str(form.get("prospect_id") or "").strip(),
            "event": str(form.get("event") or "").strip(),
            "territory_code": str(form.get("territory_code") or "OUTREACH_AUTO").strip(),
            "note": str(form.get("note") or "").strip(),
        }
        if not payload["prospect_id"]:
            raise ValueError("prospect_id required")
        if payload["event"] not in TRIAL_MARK_EVENTS:
            raise ValueError("event must use an existing crm_admin mark event")
        command = self._python_command(
            "outreach/crm_admin.py",
            "mark",
            "--prospect-id",
            payload["prospect_id"],
            "--event",
            payload["event"],
            "--territory-code",
            payload["territory_code"],
            "--note",
            payload["note"],
        )
        return self._create_preview(kind="trial_mark", title="Trial Lifecycle Mark Preview", payload=payload, preview_data={"command": subprocess.list2cmdline(command)})

    def apply_trial_mark_preview(self, *, preview_id: str, payload_hash: str) -> tuple[dict[str, Any], CommandResult]:
        preview = self._assert_preview(preview_id=preview_id, payload_hash=payload_hash, kind="trial_mark")
        payload = dict(preview.get("payload") or {})
        result = self._command_runner(
            self._python_command(
                "outreach/crm_admin.py",
                "mark",
                "--prospect-id",
                str(payload["prospect_id"]),
                "--event",
                str(payload["event"]),
                "--territory-code",
                str(payload["territory_code"]),
                "--note",
                str(payload["note"]),
            ),
            timeout_seconds=300,
        )
        self._write_audit_event(
            kind="trial_mark",
            action="apply_completed" if result.exit_code == 0 else "apply_failed",
            payload=payload,
            extra={"preview_id": preview_id, "command": result.display_command, "stdout": result.stdout, "stderr": result.stderr, "exit_code": result.exit_code},
        )
        return preview, result

    def run_trial_send_preview(self, subscriber_key: str) -> CommandResult:
        return self._command_runner(
            self._python_command(
                "run_trial_daily.py",
                "--subscriber-key",
                str(subscriber_key or "").strip(),
                "--test-send-daily",
                "--dry-run",
            ),
            timeout_seconds=300,
        )

    def build_manual_import_preview(self, form: dict[str, str]) -> dict[str, Any]:
        mode = str(form.get("import_mode") or "").strip() or "file"
        if mode == "pending":
            payload = {"mode": "pending"}
            preview_command = self._python_command("tools/import_prospect_ai_assist_review.py", "--pending", "--dry-run")
            apply_command = self._python_command("tools/import_prospect_ai_assist_review.py", "--pending")
            preview_result = self._command_runner(preview_command, timeout_seconds=300)
            preview_data = {
                "command": subprocess.list2cmdline(preview_command),
                "apply_command": subprocess.list2cmdline(apply_command),
                "preview_stdout": preview_result.stdout,
                "preview_stderr": preview_result.stderr,
                "preview_exit_code": preview_result.exit_code,
            }
            return self._create_preview(kind="manual_import", title="Pending Import Preview", payload=payload, preview_data=preview_data)

        if mode == "stdin":
            csv_text = str(form.get("csv_text") or "")
            if not csv_text.strip():
                raise ValueError("CSV paste box is empty")
            payload = {"mode": "stdin", "csv_text": csv_text}
            preview_command = self._python_command("tools/import_prospect_ai_assist_review.py", "--stdin", "--dry-run")
            apply_command = self._python_command("tools/import_prospect_ai_assist_review.py", "--stdin")
            preview_result = self._command_runner(preview_command, stdin_text=csv_text, timeout_seconds=300)
            preview_data = {
                "command": subprocess.list2cmdline(preview_command),
                "apply_command": subprocess.list2cmdline(apply_command),
                "preview_stdout": preview_result.stdout,
                "preview_stderr": preview_result.stderr,
                "preview_exit_code": preview_result.exit_code,
            }
            return self._create_preview(kind="manual_import", title="Manual CSV Paste Preview", payload=payload, preview_data=preview_data)

        input_path = Path(str(form.get("input_path") or "").strip()).expanduser().resolve(strict=False)
        if not input_path.exists():
            raise ValueError("input file not found")
        payload = {"mode": "file", "input_path": str(input_path)}
        preview_command = self._python_command("tools/import_prospect_ai_assist_review.py", "--input", str(input_path), "--dry-run")
        apply_command = self._python_command("tools/import_prospect_ai_assist_review.py", "--input", str(input_path))
        preview_result = self._command_runner(preview_command, timeout_seconds=300)
        preview_data = {
            "command": subprocess.list2cmdline(preview_command),
            "apply_command": subprocess.list2cmdline(apply_command),
            "preview_stdout": preview_result.stdout,
            "preview_stderr": preview_result.stderr,
            "preview_exit_code": preview_result.exit_code,
        }
        return self._create_preview(kind="manual_import", title="Reviewed CSV Import Preview", payload=payload, preview_data=preview_data)

    def apply_manual_import_preview(self, *, preview_id: str, payload_hash: str) -> tuple[dict[str, Any], CommandResult]:
        preview = self._assert_preview(preview_id=preview_id, payload_hash=payload_hash, kind="manual_import")
        payload = dict(preview.get("payload") or {})
        mode = str(payload.get("mode") or "").strip()
        if mode == "pending":
            result = self._command_runner(self._python_command("tools/import_prospect_ai_assist_review.py", "--pending"), timeout_seconds=300)
        elif mode == "stdin":
            result = self._command_runner(
                self._python_command("tools/import_prospect_ai_assist_review.py", "--stdin"),
                stdin_text=str(payload.get("csv_text") or ""),
                timeout_seconds=300,
            )
        else:
            result = self._command_runner(
                self._python_command("tools/import_prospect_ai_assist_review.py", "--input", str(payload.get("input_path") or "")),
                timeout_seconds=300,
            )
        self._write_audit_event(
            kind="manual_import",
            action="apply_completed" if result.exit_code == 0 else "apply_failed",
            payload=payload,
            extra={"preview_id": preview_id, "command": result.display_command, "stdout": result.stdout, "stderr": result.stderr, "exit_code": result.exit_code},
        )
        return preview, result

    def run_outreach_quick_action(self, *, action: str, for_date: str = "") -> CommandResult:
        if action == "print_config":
            command = self._secrets_python_command("run_outreach_auto.py", "--print-config")
        elif action == "doctor":
            command = self._secrets_python_command("run_outreach_auto.py", "--doctor")
        elif action == "dry_run":
            command = self._secrets_python_command("run_outreach_auto.py", "--dry-run")
        elif action == "plan":
            target_date = str(for_date or self._now_provider().date().isoformat())
            command = self._python_command("run_outreach_auto.py", "--plan", "--for-date", target_date)
        else:
            raise ValueError("unknown outreach action")
        return self._command_runner(command, timeout_seconds=300)


class OpsConsoleApp:
    def __init__(self, service: OpsConsoleService | None = None) -> None:
        self.service = service or OpsConsoleService()

    def _response(self, status_code: int, body: str, *, content_type: str = "text/html; charset=utf-8") -> Response:
        status_map = {200: "200 OK", 400: "400 Bad Request", 404: "404 Not Found", 405: "405 Method Not Allowed"}
        return Response(
            status=status_map.get(status_code, f"{status_code} OK"),
            headers=[("Content-Type", content_type)],
            body=body.encode("utf-8"),
        )

    def _render_nav(self, path: str) -> str:
        links = []
        for href, label in NAV_ITEMS:
            cls = "nav-link active" if href == path else "nav-link"
            links.append(f'<a class="{cls}" href="{href}">{_html(label)}</a>')
        return "".join(links)

    def _wrap(self, *, title: str, path: str, content: str, message: str = "") -> Response:
        banner = f'<section class="banner">{message}</section>' if message else ""
        html_text = LAYOUT_TEMPLATE.safe_substitute(
            title=_html(title),
            repo_root=_html(self.service.repo_root),
            data_dir=_html(self.service.data_dir),
            schedule_path=_html(self.service.schedule().path),
            nav=self._render_nav(path),
            message=banner,
            content=content,
        )
        return self._response(200, html_text)

    def _card(self, title: str, body: str) -> str:
        return f'<section class="card"><h2>{_html(title)}</h2>{body}</section>'

    def _dl(self, values: dict[str, Any]) -> str:
        parts = ['<dl class="kv">']
        for key, value in values.items():
            parts.append(f"<dt>{_html(key)}</dt><dd>{_html(value)}</dd>")
        parts.append("</dl>")
        return "".join(parts)

    def _list(self, items: list[str], *, empty: str) -> str:
        if not items:
            return f'<p class="muted">{_html(empty)}</p>'
        return "<ul>" + "".join(f"<li>{_html(item)}</li>" for item in items) + "</ul>"

    def _pre(self, text: str, *, empty: str = "(no output)") -> str:
        value = text.strip() or empty
        return f"<pre>{_html(value)}</pre>"

    def _preview_panel(self, preview: dict[str, Any], apply_path: str) -> str:
        preview_id = str(preview.get("preview_id") or "")
        payload_hash = str(preview.get("payload_hash") or "")
        return "".join(
            [
                '<section class="card preview-card"><h2>Pending Preview</h2>',
                f"<p><strong>{_html(preview.get('title') or '')}</strong></p>",
                f"<p>Preview id: <code>{_html(preview_id)}</code></p>",
                self._pre(json.dumps(preview.get("preview_data") or {}, indent=2, sort_keys=True)),
                f'<form method="post" action="{_html(apply_path)}">',
                '<input type="hidden" name="action" value="apply_preview">',
                f'<input type="hidden" name="preview_id" value="{_html(preview_id)}">',
                f'<input type="hidden" name="payload_hash" value="{_html(payload_hash)}">',
                '<button type="submit">Apply Preview</button>',
                "</form></section>",
            ]
        )

    def _result_panel(self, title: str, result: CommandResult) -> str:
        return self._card(
            title,
            "".join(
                [
                    f"<p><strong>Command</strong> <code>{_html(result.display_command)}</code></p>",
                    f"<p><strong>Exit</strong> {_html(result.exit_code)}</p>",
                    self._pre(result.stdout, empty="(stdout empty)"),
                    self._pre(result.stderr, empty="(stderr empty)"),
                ]
            ),
        )

    def _render_dashboard(self, *, message: str = "") -> Response:
        data = self.service.dashboard_data()
        runtime = data["runtime"]
        latest = dict(runtime.get("runtime_latest") or {})
        latest_jobs = list(latest.get("jobs") or [])
        next_week = list(data.get("next_week") or [])
        manual = dict(data.get("manual") or {})
        trials = dict(data.get("trials") or {})
        snapshot = dict((data.get("ops_snapshot") or {}).get("payload") or {})
        failures: list[str] = []
        for job in latest_jobs:
            result = str(job.get("result") or "").strip().lower()
            reason = str(job.get("reason") or "").strip()
            if result == "failed":
                failures.append(f"{job.get('name')} failed ({reason or 'failed'})")
            elif result == "skipped" and reason.startswith("window_closed_"):
                failures.append(f"{job.get('name')} missed window ({reason})")
        content = "".join(
            [
                self._card("Needs Attention", self._list(list(data.get("needs_attention") or []), empty="Nothing urgent is queued.")),
                self._card(
                    "Runtime Health",
                    self._dl(
                        {
                            "runtime_latest": runtime["runtime_latest_path"],
                            "mode": latest.get("mode") or "(missing)",
                            "finished_local": latest.get("finished_local") or "(missing)",
                            "jobs_seen": len(list(runtime.get("jobs") or [])),
                            "alerts_sent_last_run": dict(latest.get("alerts") or {}).get("alerts_sent", 0),
                        }
                    ),
                ),
                self._card(
                    "Next 7 Days Outreach",
                    "".join(
                        [
                            '<table class="grid"><thead><tr><th>Date</th><th>Rotation</th><th>Effective</th><th>Selected Spread</th><th>Will Send</th><th>Fallback</th></tr></thead><tbody>',
                            "".join(
                                [
                                    "<tr>"
                                    f"<td>{_html(item.get('date'))}</td>"
                                    f"<td>{_html(item.get('rotation_selected_state') or item.get('state') or '')}</td>"
                                    f"<td>{_html(item.get('effective_send_state') or item.get('state') or '')}</td>"
                                    f"<td>{_html(_format_selected_by_state(item.get('selected_by_state') or {}))}</td>"
                                    f"<td>{_html(item.get('will_send') or '')}</td>"
                                    f"<td>{_html(item.get('fallback_reason') or '')}</td>"
                                    "</tr>"
                                    for item in next_week
                                ]
                            ),
                            "</tbody></table>",
                        ]
                    ),
                ),
                self._card(
                    "Current Sendable Backlog",
                    self._list(
                        [f"{state}: {count}" for state, count in sorted(dict(data.get("backlog_by_state") or {}).items())],
                        empty="No state backlog data available.",
                    ),
                ),
                self._card(
                    "Trials Expiring Soon",
                    self._list(
                        [
                            f"{row['subscriber_key']} ends {row['projected_end_date']} ({row['sends_used']}/{row['sends_limit']})"
                            for row in list(trials.get("rows") or [])
                            if str(row.get("projected_end_date") or "") and bool(row.get("conversion_due") or False)
                        ],
                        empty="No trial expiry or conversion warnings right now.",
                    ),
                ),
                self._card(
                    "Manual Research / Import Queue",
                    self._list(
                        [
                            f"skip list: {_repo_relative(Path(manual['skip_list_path']))}" if manual.get("skip_list_path") else "skip list missing",
                            f"deep research prompt: {_repo_relative(Path(manual['prompt_path']))}" if manual.get("prompt_path") else "deep research prompt missing",
                            f"pending reviewed CSV imports: {len(list(manual.get('pending_files') or []))}",
                        ],
                        empty="No manual research artifacts found.",
                    ),
                ),
                self._card("Latest Ops Snapshot", self._pre(json.dumps(snapshot, indent=2, sort_keys=True), empty="No ops snapshot found.")),
                self._card("Latest Failures / Alerts", self._list(failures, empty="No recent failures or missed windows surfaced.")),
            ]
        )
        return self._wrap(title="Dashboard", path="/", content=content, message=message)

    def _render_outreach(self, *, preview: dict[str, Any] | None = None, result: CommandResult | None = None, message: str = "") -> Response:
        snapshot = self.service.outreach_env_snapshot()
        form = [
            '<form method="post"><input type="hidden" name="action" value="preview_outreach"><div class="form-grid">',
            f'<label>Daily limit<input name="outreach_daily_limit" value="{_html(snapshot["outreach_daily_limit"])}"></label>',
            f'<label>States<input name="outreach_states" value="{_html(snapshot["outreach_states"])}"></label>',
            f'<label>Fallback on empty state (0/1)<input name="outreach_fallback_on_empty_state" value="{_html(snapshot["outreach_fallback_on_empty_state"])}"></label>',
            f'<label>State spread mode<input name="outreach_state_spread_mode" value="{_html(snapshot["outreach_state_spread_mode"])}"></label>',
            f'<label>Autogrow enabled (0/1)<input name="prospect_autogrow_enabled" value="{_html(snapshot["prospect_autogrow_enabled"])}"></label>',
            f'<label>Autogrow safety net (0/1)<input name="prospect_autogrow_safety_net_enabled" value="{_html(snapshot["prospect_autogrow_safety_net_enabled"])}"></label>',
            f'<label>AI assist review (0/1)<input name="prospect_ai_assist_review_enabled" value="{_html(snapshot["prospect_ai_assist_review_enabled"])}"></label>',
            f'<label>AI triage enabled (0/1)<input name="ai_triage_enabled" value="{_html(snapshot["ai_triage_enabled"])}"></label>',
            '</div><button type="submit">Preview Changes</button></form>',
        ]
        quick_actions = [
            '<section class="card"><h2>Quick Actions</h2>',
            '<form method="post" class="inline-form"><input type="hidden" name="action" value="quick_outreach"><input type="hidden" name="quick_action" value="print_config"><button type="submit">outreach --print-config</button></form>',
            '<form method="post" class="inline-form"><input type="hidden" name="action" value="quick_outreach"><input type="hidden" name="quick_action" value="doctor"><button type="submit">outreach --doctor</button></form>',
            '<form method="post" class="inline-form"><input type="hidden" name="action" value="quick_outreach"><input type="hidden" name="quick_action" value="dry_run"><button type="submit">outreach --dry-run</button></form>',
            '<form method="post" class="inline-form"><input type="hidden" name="action" value="quick_outreach"><input type="hidden" name="quick_action" value="plan"><input name="for_date" placeholder="YYYY-MM-DD"><button type="submit">outreach --plan --for-date</button></form>',
            "</section>",
        ]
        content = [
            self._card("Current Values", self._dl({key: snapshot.get(key) for key in OUTREACH_SAFE_FIELDS})),
            self._card("Editable Controls", "".join(form)),
            "".join(quick_actions),
        ]
        if preview:
            content.append(self._preview_panel(preview, "/outreach"))
        if result:
            content.append(self._result_panel("Outreach Command Result", result))
        if snapshot.get("warnings"):
            content.append(self._card("Warnings", self._list(list(snapshot["warnings"]), empty="")))
        return self._wrap(title="Outreach Control", path="/outreach", content="".join(content), message=message)

    def _render_schedule(self, *, preview: dict[str, Any] | None = None, result: CommandResult | None = None, message: str = "") -> Response:
        schedule = self.service.schedule()
        content = [
            self._card("Current Schedule", self._dl(schedule.as_dict())),
            self._card(
                "Schedule Editor",
                "".join(
                    [
                        '<form method="post"><input type="hidden" name="action" value="preview_schedule"><div class="form-grid">',
                        f'<label>Outreach send HH:MM<input name="outreach_send_local_hhmm" value="{_html(schedule.outreach_send_local_hhmm)}"></label>',
                        f'<label>Trial default HH:MM<input name="trial_default_send_local_hhmm" value="{_html(schedule.trial_default_send_local_hhmm)}"></label>',
                        f'<label>Evening prep HH:MM<input name="evening_prep_local_hhmm" value="{_html(schedule.evening_prep_local_hhmm)}"></label>',
                        '</div><button type="submit">Preview Schedule Update</button></form>',
                    ]
                ),
            ),
        ]
        if preview:
            content.append(self._preview_panel(preview, "/schedule"))
        if result:
            content.append(self._result_panel("Schedule Apply Result", result))
        return self._wrap(title="Scheduling Control", path="/schedule", content="".join(content), message=message)

    def _render_state_scope(self, *, preview: dict[str, Any] | None = None, result: CommandResult | None = None, message: str = "") -> Response:
        snapshot = self.service.outreach_env_snapshot()
        current_states = snapshot.get("outreach_states") or ""
        current_autogrow = snapshot.get("prospect_autogrow_states") or "(unset)"
        content = [
            self._card(
                "Current Live Scope",
                self._dl(
                    {
                        "live_outreach_scope": current_states,
                        "autogrow_scope": current_autogrow,
                        "manual_deep_research_default_scope": current_states,
                        "state_lic": "TX-only",
                    }
                ),
            ),
            self._card(
                "Add / Remove State",
                "".join(
                    [
                        '<form method="post" class="inline-form"><input type="hidden" name="action" value="preview_state_scope"><input type="hidden" name="scope_action" value="add"><label>Add state<input name="state_code" placeholder="CA"></label><button type="submit">Preview Add</button></form>',
                        '<form method="post" class="inline-form"><input type="hidden" name="action" value="preview_state_scope"><input type="hidden" name="scope_action" value="remove"><label>Remove state<input name="state_code" placeholder="CA"></label><button type="submit">Preview Remove</button></form>',
                    ]
                ),
            ),
        ]
        if preview:
            content.append(self._preview_panel(preview, "/state-scope"))
        if result:
            content.append(self._result_panel("State Scope Apply Result", result))
        return self._wrap(title="State Scope", path="/state-scope", content="".join(content), message=message)

    def _render_trials(self, *, preview: dict[str, Any] | None = None, result: CommandResult | None = None, message: str = "") -> Response:
        trials = self.service.trials_data()
        rows = list(trials.get("rows") or [])
        table_rows = [
            "<tr>"
            f"<td>{_html(row['subscriber_key'])}</td>"
            f"<td>{_html(row['trial_status_group'])}</td>"
            f"<td>{_html(str(row['sends_used']) + '/' + str(row['sends_limit']))}</td>"
            f"<td>{_html(row['projected_end_date'])}</td>"
            f"<td>{_html(row['next_send_time'])}</td>"
            f"<td>{_html('yes' if bool(row.get('conversion_due')) else 'no')}</td>"
            f"<td>{_html(', '.join(row['recipients']))}</td>"
            f"<td>{_html(row['territory_scope'])}</td>"
            f"<td>{_html(', '.join(row.get('state_scope') or []))}</td>"
            "</tr>"
            for row in rows
        ]
        content = [
            self._card(
                "Trial Queue",
                "".join(
                    [
                        '<table class="grid"><thead><tr><th>Subscriber</th><th>Status</th><th>Sends</th><th>Projected End</th><th>Next Send</th><th>Conversion Due</th><th>Recipients</th><th>Territory</th><th>States</th></tr></thead><tbody>',
                        "".join(table_rows) or '<tr><td colspan="9" class="muted">No trials found.</td></tr>',
                        "</tbody></table>",
                    ]
                ),
            ),
            self._card(
                "Add New Trial",
                "".join(
                    [
                        '<form method="post"><input type="hidden" name="action" value="preview_trial_add"><div class="form-grid">',
                        '<label>Subscriber key<input name="subscriber_key"></label>',
                        '<label>Email<input name="email"></label>',
                        '<label>States (CSV)<input name="states" placeholder="CA,OR,WA"></label>',
                        '<label>Territory<input name="territory" placeholder="TX_TRI"></label>',
                        '<label>Timezone<input name="tz" value="America/New_York"></label>',
                        f'<label>Send time<input name="send_time_local" value="{_html(self.service.schedule().trial_default_send_local_hhmm)}"></label>',
                        '<label>Start date<input name="start_date" placeholder="YYYY-MM-DD"></label>',
                        '<label>Sends limit<input name="sends_limit" value="14"></label>',
                        '</div><button type="submit">Preview Add Trial</button></form>',
                    ]
                ),
            ),
            self._card(
                "Trial Actions",
                "".join(
                    [
                        '<form method="post" class="inline-form"><input type="hidden" name="action" value="trial_preview_send"><label>Subscriber key<input name="subscriber_key"></label><button type="submit">Preview Daily Send</button></form>',
                        '<form method="post" class="inline-form"><input type="hidden" name="action" value="preview_trial_conversion"><label>Subscriber key<input name="subscriber_key"></label><button type="submit">Preview Conversion Draft</button></form>',
                        '<form method="post" class="inline-form"><input type="hidden" name="action" value="preview_trial_mark"><label>Prospect id<input name="prospect_id"></label><label>Event<select name="event"><option value="replied">replied</option><option value="trial_started">trial_started</option><option value="converted">converted</option><option value="do_not_contact">do_not_contact</option></select></label><label>Territory<input name="territory_code" value="OUTREACH_AUTO"></label><label>Note<input name="note"></label><button type="submit">Preview CRM Mark</button></form>',
                    ]
                ),
            ),
        ]
        if preview:
            content.append(self._preview_panel(preview, "/trials"))
        if result:
            content.append(self._result_panel("Trial Command Result", result))
        if trials.get("warnings"):
            content.append(self._card("Warnings", self._list(list(trials["warnings"]), empty="")))
        return self._wrap(title="Trials", path="/trials", content="".join(content), message=message)

    def _render_manual_imports(self, *, preview: dict[str, Any] | None = None, result: CommandResult | None = None, message: str = "") -> Response:
        data = self.service.manual_import_data()
        pending_items = [
            f"{_repo_relative(Path(item['path']))}{' (legacy)' if item['legacy'] else ''}"
            for item in list(data.get("pending_files") or [])
        ]
        batches = [
            f"{row.get('batch_id')}: {row.get('status')} verified={row.get('verified_total')} updated_at={row.get('updated_at')}"
            for row in list(data.get("import_batches") or [])
        ]
        content = [
            self._card(
                "Research Artifacts",
                self._dl(
                    {
                        "skip_list": _repo_relative(Path(data["skip_list_path"])) if data.get("skip_list_path") else "missing",
                        "manual_deep_research_prompt": _repo_relative(Path(data["prompt_path"])) if data.get("prompt_path") else "missing",
                    }
                ),
            ),
            self._card("Pending Reviewed CSV Imports", self._list(pending_items, empty="No pending reviewed CSV files found.")),
            self._card("Last Import Results", self._list(batches, empty="No import batch history found.")),
            self._card(
                "Import From File",
                '<form method="post"><input type="hidden" name="action" value="preview_manual_import"><input type="hidden" name="import_mode" value="file"><label>Reviewed CSV path<input name="input_path" placeholder="C:\\path\\reviewed.csv"></label><button type="submit">Preview File Import</button></form>',
            ),
            self._card(
                "Paste Reviewed CSV",
                '<form method="post"><input type="hidden" name="action" value="preview_manual_import"><input type="hidden" name="import_mode" value="stdin"><label>CSV text<textarea name="csv_text" rows="10"></textarea></label><button type="submit">Preview Pasted CSV Import</button></form>',
            ),
            self._card(
                "Pending Queue Import",
                '<form method="post"><input type="hidden" name="action" value="preview_manual_import"><input type="hidden" name="import_mode" value="pending"><button type="submit">Preview Pending Reviewed Imports</button></form>',
            ),
        ]
        if preview:
            content.append(self._preview_panel(preview, "/manual-imports"))
        if result:
            content.append(self._result_panel("Manual Import Result", result))
        return self._wrap(title="Manual Prospect Research / Import Queue", path="/manual-imports", content="".join(content), message=message)

    def _render_inbox(self, *, message: str = "") -> Response:
        data = self.service.inbox_data()
        setup = dict(data.get("inbound_setup") or {})
        backend = str(setup.get("backend") or "gmail").strip().lower() or "gmail"
        setup_details = {
            "backend": backend,
            "backend_source": str(setup.get("backend_source") or ""),
            "status": str(setup.get("status") or "unknown"),
            "recommended_next_step": str(setup.get("recommended_next_step") or ""),
            "triage_log": _path_with_presence(Path(setup["triage_log_path"])) if setup.get("triage_log_path") else "missing",
            "reply_drafts_dir": _path_with_presence(Path(setup["reply_drafts_dir"])) if setup.get("reply_drafts_dir") else "missing",
            "eng_tickets_dir": _path_with_presence(Path(setup["eng_tickets_dir"])) if setup.get("eng_tickets_dir") else "missing",
        }
        if backend == "imap":
            setup_details["imap_host"] = str(setup.get("imap_host") or "")
            setup_details["imap_port"] = str(setup.get("imap_port") or "")
            setup_details["imap_source"] = str(setup.get("imap_source") or "")
            setup_details["imap_user"] = str(setup.get("imap_user") or "") if bool(setup.get("imap_user_present")) else "missing"
            setup_details["imap_password"] = "present" if bool(setup.get("imap_pass_present")) else "missing"
            setup_details["imap_folder"] = str(setup.get("imap_folder") or "")
            setup_details["imap_folder_unsub"] = str(setup.get("imap_folder_unsub") or "")
            setup_details["imap_folder_bounce"] = str(setup.get("imap_folder_bounce") or "")
        else:
            credentials_path = Path(setup["credentials_path"]) if setup.get("credentials_path") else None
            token_path = Path(setup["token_path"]) if setup.get("token_path") else None
            setup_details["gmail_credentials_json"] = _path_with_presence(credentials_path) if credentials_path is not None else "missing"
            setup_details["gmail_token_json"] = _path_with_presence(token_path) if token_path is not None else "missing"
            missing_modules = list(setup.get("gmail_missing_modules") or [])
            setup_details["gmail_client_deps"] = (
                "installed"
                if bool(setup.get("gmail_client_deps_installed"))
                else ("missing: " + ", ".join(str(item) for item in missing_modules))
            )
        commands = [str(item) for item in list(setup.get("commands") or []) if str(item).strip()]
        commands_html = ""
        if commands:
            commands_html = (
                "<p><strong>Commands</strong></p><ul>"
                + "".join(f"<li><code>{_html(command)}</code></li>" for command in commands)
                + "</ul>"
            )
        content = [
            self._card("Inbound Setup", self._dl(setup_details) + commands_html),
            self._card("Trial / Onboarding Requests", self._list([json.dumps(row, sort_keys=True) for row in list(data.get("onboarding_rows") or [])], empty="No onboarding audit rows found.")),
            self._card("Subscriber Entitlements", self._list([json.dumps(row, sort_keys=True) for row in list(data.get("entitlements") or [])], empty="No entitlement rows found.")),
            self._card("Inbound Triage Queue", self._list([json.dumps(row, sort_keys=True) for row in list(data.get("triage_rows") or [])], empty="No inbox triage artifacts found.")),
            self._card("Reply Drafts", self._list([_repo_relative(path) for path in list(data.get("reply_drafts") or [])], empty="No reply drafts found.")),
            self._card("Engineering Tickets", self._list([_repo_relative(path) for path in list(data.get("eng_tickets") or [])], empty="No engineering tickets found.")),
            self._card("Trial Request Registry", f'<p class="muted">{_html(data.get("trial_request_registry_status"))}</p>'),
        ]
        return self._wrap(title="Inbox / Requests", path="/inbox", content="".join(content), message=message)

    def _render_audit(self, *, message: str = "") -> Response:
        rows = self.service.recent_audit_entries(limit=100)
        content = self._card("Config Change Audit Log", self._pre(json.dumps(rows, indent=2, sort_keys=True), empty="No audit log entries yet."))
        return self._wrap(title="Audit Log", path="/audit", content=content, message=message)

    def dispatch(self, method: str, path: str, *, query: dict[str, list[str]] | None = None, form: dict[str, str] | None = None) -> Response:
        query = query or {}
        form = form or {}
        method_norm = str(method or "GET").upper()
        if path == "/static/ops_console.css":
            css_path = self.service.repo_root / "ops_console" / "static" / "ops_console.css"
            if not css_path.exists():
                return self._response(404, "missing stylesheet", content_type="text/plain; charset=utf-8")
            return self._response(200, css_path.read_text(encoding="utf-8"), content_type="text/css; charset=utf-8")
        if method_norm not in {"GET", "POST"}:
            return self._response(405, "method not allowed", content_type="text/plain; charset=utf-8")
        try:
            if path == "/":
                return self._render_dashboard()
            if path == "/outreach":
                if method_norm == "POST":
                    action = str(form.get("action") or "").strip()
                    if action == "preview_outreach":
                        return self._render_outreach(preview=self.service.build_outreach_preview(form, title="Outreach Control Preview"), message="Preview created. Apply uses the stored payload hash.")
                    if action == "apply_preview":
                        _preview, result = self.service.apply_outreach_preview(preview_id=str(form.get("preview_id") or ""), payload_hash=str(form.get("payload_hash") or ""))
                        return self._render_outreach(result=result, message="Outreach change applied through scripts/set_outreach_env.ps1.")
                    if action == "quick_outreach":
                        result = self.service.run_outreach_quick_action(action=str(form.get("quick_action") or ""), for_date=str(form.get("for_date") or ""))
                        return self._render_outreach(result=result, message="Quick action completed.")
                return self._render_outreach()
            if path == "/schedule":
                if method_norm == "POST":
                    action = str(form.get("action") or "").strip()
                    if action == "preview_schedule":
                        return self._render_schedule(preview=self.service.build_schedule_preview(form), message="Schedule preview created. Apply will sync both the JSON seam and Task Scheduler.")
                    if action == "apply_preview":
                        _preview, result = self.service.apply_schedule_preview(preview_id=str(form.get("preview_id") or ""), payload_hash=str(form.get("payload_hash") or ""))
                        return self._render_schedule(result=result, message="Schedule apply finished.")
                return self._render_schedule()
            if path == "/state-scope":
                if method_norm == "POST":
                    action = str(form.get("action") or "").strip()
                    if action == "preview_state_scope":
                        return self._render_state_scope(preview=self.service.build_state_scope_preview(action=str(form.get("scope_action") or ""), state_code=str(form.get("state_code") or "")), message="State scope preview created. STATE_LIC remains TX-only.")
                    if action == "apply_preview":
                        _preview, result = self.service.apply_state_scope_preview(preview_id=str(form.get("preview_id") or ""), payload_hash=str(form.get("payload_hash") or ""))
                        return self._render_state_scope(result=result, message="State scope change applied through scripts/set_outreach_env.ps1.")
                return self._render_state_scope()
            if path == "/trials":
                if method_norm == "POST":
                    action = str(form.get("action") or "").strip()
                    if action == "preview_trial_add":
                        return self._render_trials(preview=self.service.build_trial_add_preview(form), message="Trial add preview created.")
                    if action == "preview_trial_conversion":
                        return self._render_trials(preview=self.service.build_trial_conversion_preview(str(form.get("subscriber_key") or "")), message="Conversion draft preview created.")
                    if action == "preview_trial_mark":
                        return self._render_trials(preview=self.service.build_trial_mark_preview(form), message="CRM lifecycle mark preview created.")
                    if action == "apply_preview":
                        preview = self.service._load_preview(str(form.get("preview_id") or ""))
                        kind = str(preview.get("kind") or "")
                        if kind == "trial_add":
                            _preview, result = self.service.apply_trial_add_preview(preview_id=str(form.get("preview_id") or ""), payload_hash=str(form.get("payload_hash") or ""))
                        elif kind == "trial_conversion":
                            _preview, result = self.service.apply_trial_conversion_preview(preview_id=str(form.get("preview_id") or ""), payload_hash=str(form.get("payload_hash") or ""))
                        else:
                            _preview, result = self.service.apply_trial_mark_preview(preview_id=str(form.get("preview_id") or ""), payload_hash=str(form.get("payload_hash") or ""))
                        return self._render_trials(result=result, message="Trial mutation applied.")
                    if action == "trial_preview_send":
                        result = self.service.run_trial_send_preview(str(form.get("subscriber_key") or ""))
                        return self._render_trials(result=result, message="Dry-run trial send completed.")
                return self._render_trials()
            if path == "/manual-imports":
                if method_norm == "POST":
                    action = str(form.get("action") or "").strip()
                    if action == "preview_manual_import":
                        return self._render_manual_imports(preview=self.service.build_manual_import_preview(form), message="Manual import preview created.")
                    if action == "apply_preview":
                        _preview, result = self.service.apply_manual_import_preview(preview_id=str(form.get("preview_id") or ""), payload_hash=str(form.get("payload_hash") or ""))
                        return self._render_manual_imports(result=result, message="Manual import apply finished.")
                return self._render_manual_imports()
            if path == "/inbox":
                return self._render_inbox()
            if path == "/audit":
                return self._render_audit()
            return self._response(404, "not found", content_type="text/plain; charset=utf-8")
        except Exception as exc:
            return self._wrap(title="Error", path=path if path in {item[0] for item in NAV_ITEMS} else "/", content=self._card("Request Error", self._pre(str(exc))), message="The console caught an error but stayed up.")

    def __call__(self, environ: dict[str, Any], start_response: Callable[[str, list[tuple[str, str]]], None]) -> list[bytes]:
        setup_testing_defaults(environ)
        method = str(environ.get("REQUEST_METHOD") or "GET")
        path = str(environ.get("PATH_INFO") or "/")
        query = parse_qs(str(environ.get("QUERY_STRING") or ""), keep_blank_values=True)
        form: dict[str, str] = {}
        if method.upper() == "POST":
            size = int(str(environ.get("CONTENT_LENGTH") or "0") or "0")
            body_bytes = environ["wsgi.input"].read(size) if size > 0 else b""
            form = {key: values[-1] for key, values in parse_qs(body_bytes.decode("utf-8"), keep_blank_values=True).items()}
        response = self.dispatch(method, path, query=query, form=form)
        headers = list(response.headers) + [("Content-Length", str(len(response.body)))]
        start_response(response.status, headers)
        return [response.body]


class _LocalOnlyRequestHandler(WSGIRequestHandler):
    def log_message(self, format: str, *args: Any) -> None:  # pragma: no cover
        return


def build_server(*, host: str = DEFAULT_HOST, port: int = DEFAULT_PORT, app: OpsConsoleApp | None = None) -> WSGIServer:
    if str(host or "").strip() != DEFAULT_HOST:
        raise ValueError("Ops console must bind to 127.0.0.1 only")
    return make_server(host, int(port), app or OpsConsoleApp(), handler_class=_LocalOnlyRequestHandler)


def serve(*, host: str = DEFAULT_HOST, port: int = DEFAULT_PORT) -> int:
    server = build_server(host=host, port=port)
    print(f"MICROFLOWOPS_OPS_CONSOLE_URL=http://{host}:{int(port)}/")
    try:
        server.serve_forever()
    except KeyboardInterrupt:  # pragma: no cover
        pass
    finally:
        server.server_close()
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="MicroFlowOps Ops Console (local-only, stdlib server).")
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    args = parser.parse_args(argv)
    return serve(host=str(args.host or DEFAULT_HOST), port=int(args.port or DEFAULT_PORT))


if __name__ == "__main__":
    raise SystemExit(main())
