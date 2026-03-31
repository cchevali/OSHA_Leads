from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from outreach.runtime_operator_alerts import resolve_alert_recipient, send_plain_text_alert, smtp_missing_key
from runtime_data_dir import resolve_data_dir
from runtime_guard import render_runtime_lines, run_runtime_preflight
from runtime_schedule_config import load_runtime_schedule

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore[assignment]


ERR_RUNTIME_TICK_CONFIG = "ERR_RUNTIME_TICK_CONFIG"
ERR_RUNTIME_TICK_LOCKED = "ERR_RUNTIME_TICK_LOCKED"
ERR_RUNTIME_TICK_STAGE = "ERR_RUNTIME_TICK_STAGE"
WARN_RUNTIME_TICK_EXTERNAL_SCHEDULER = "WARN_RUNTIME_TICK_EXTERNAL_SCHEDULER"
PASS_RUNTIME_TICK_PRINT_CONFIG = "PASS_RUNTIME_TICK_PRINT_CONFIG"
PASS_RUNTIME_TICK_DOCTOR = "PASS_RUNTIME_TICK_DOCTOR"
PASS_RUNTIME_TICK_COMPLETE = "PASS_RUNTIME_TICK_COMPLETE"

RUNTIME_TZ_NAME = "America/New_York"
RUNTIME_TZ_FALLBACK = "Eastern Standard Time"
LOCK_STALE_SECONDS = 4 * 60 * 60
RUNTIME_JOB_STATE_SCHEMA = "runtime_tick_job_state_v1"
ALERTS_SCHEMA = "runtime_tick_alert_v1"
ALERTS_SUMMARY_SCHEMA = "runtime_tick_alert_summary_v1"
CRITICAL_WINDOW_JOBS = frozenset(
    {
        "ingest_daily",
        "ingest_evening",
        "prospect_replenish_daily",
        "outreach_auto",
        "trial_facs_daily",
        "trial_jl_safety_daily",
        "trial_roi_safety_daily",
    }
)


@dataclass(frozen=True)
class JobSpec:
    name: str
    kind: str  # daily | interval
    weekday_only: bool
    target_hhmm: str = ""
    interval_minutes: int = 0
    catchup_minutes: int = 180
    max_attempts_per_slot: int = 3


@dataclass(frozen=True)
class AlertCandidate:
    name: str
    category: str
    slot_key: str
    scheduled_local: str
    result: str
    reason: str
    exit_code: int
    task_log_path: str
    run_summary_json_path: str
    run_summary_text_path: str
    reconciliation_status: str


@dataclass(frozen=True)
class WrapperRunEvidence:
    job_name: str
    wrapper_name: str
    slot_key: str
    start_local: str
    end_local: str
    exit_code: int
    task_log_path: str
    run_summary_json_path: str
    run_summary_text_path: str
    success: bool
    timing: str


JOBS: tuple[JobSpec, ...] = (
    JobSpec(name="inbound_triage", kind="interval", weekday_only=False, interval_minutes=15, max_attempts_per_slot=1),
    JobSpec(name="ingest_daily", kind="daily", weekday_only=True, target_hhmm="06:45", catchup_minutes=180),
    JobSpec(name="prospect_replenish_daily", kind="daily", weekday_only=True, target_hhmm="07:15", catchup_minutes=180),
    JobSpec(name="outreach_auto", kind="daily", weekday_only=True, target_hhmm="08:00", catchup_minutes=180),
    JobSpec(name="trial_facs_daily", kind="daily", weekday_only=True, target_hhmm="09:00", catchup_minutes=180),
    JobSpec(name="trial_jl_safety_daily", kind="daily", weekday_only=True, target_hhmm="09:00", catchup_minutes=180),
    JobSpec(name="trial_roi_safety_daily", kind="daily", weekday_only=True, target_hhmm="09:00", catchup_minutes=180),
    JobSpec(name="ops_snapshot_daily", kind="daily", weekday_only=True, target_hhmm="09:30", catchup_minutes=180),
    JobSpec(name="outreach_cleanup_daily", kind="daily", weekday_only=True, target_hhmm="09:45", catchup_minutes=180),
    JobSpec(name="ingest_evening", kind="daily", weekday_only=False, target_hhmm="20:45", catchup_minutes=180),
)
JOB_NAMES = tuple(job.name for job in JOBS)


def _jobs_for_data_dir(data_dir: Path) -> tuple[JobSpec, ...]:
    schedule = load_runtime_schedule(data_dir)
    overrides = {
        "outreach_auto": schedule.outreach_send_local_hhmm,
        "trial_facs_daily": schedule.trial_default_send_local_hhmm,
        "trial_jl_safety_daily": schedule.trial_default_send_local_hhmm,
        "trial_roi_safety_daily": schedule.trial_default_send_local_hhmm,
        "ingest_evening": schedule.evening_prep_local_hhmm,
    }
    resolved: list[JobSpec] = []
    for spec in JOBS:
        override_hhmm = overrides.get(spec.name)
        if not override_hhmm or spec.kind != "daily":
            resolved.append(spec)
            continue
        resolved.append(
            JobSpec(
                name=spec.name,
                kind=spec.kind,
                weekday_only=spec.weekday_only,
                target_hhmm=override_hhmm,
                interval_minutes=spec.interval_minutes,
                catchup_minutes=spec.catchup_minutes,
                max_attempts_per_slot=spec.max_attempts_per_slot,
            )
        )
    return tuple(resolved)


def _emit(key: str, value: str | int) -> None:
    print(f"{key}={value}")


def _error(detail: str) -> int:
    print(f"{ERR_RUNTIME_TICK_CONFIG} {detail}", file=sys.stderr)
    return 2


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _runtime_tz() -> Any:
    if ZoneInfo is not None:
        try:
            return ZoneInfo(RUNTIME_TZ_NAME)
        except Exception:
            pass
    if ZoneInfo is not None:
        try:
            return ZoneInfo(RUNTIME_TZ_FALLBACK)
        except Exception:
            pass
    return None


def _now_local(now_local_override: str) -> datetime:
    tz = _runtime_tz()
    if now_local_override:
        text = str(now_local_override).strip()
        try:
            parsed = datetime.fromisoformat(text)
        except Exception as exc:
            raise ValueError(f"invalid_now_local={text} detail={exc}") from exc
        if parsed.tzinfo is None and tz is not None:
            parsed = parsed.replace(tzinfo=tz)
        elif tz is not None:
            parsed = parsed.astimezone(tz)
        return parsed
    if tz is not None:
        return datetime.now(tz)
    return datetime.now()


def _status_root(data_dir: Path) -> Path:
    return (data_dir / "runtime" / "status").resolve(strict=False)


def _locks_root(data_dir: Path) -> Path:
    return (data_dir / "runtime" / "locks").resolve(strict=False)


def _job_state_path(data_dir: Path, job_name: str) -> Path:
    return (_status_root(data_dir) / "jobs" / f"{job_name}.json").resolve(strict=False)


def _alerts_root(data_dir: Path) -> Path:
    return (_status_root(data_dir) / "alerts").resolve(strict=False)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp_path.replace(path)


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(raw, dict):
            return raw
    except Exception:
        return {}
    return {}


def _git_sha(repo_root: Path) -> str:
    try:
        proc = subprocess.run(
            ["git", "-C", str(repo_root), "rev-parse", "HEAD"],
            cwd=str(repo_root),
            capture_output=True,
            text=True,
            check=False,
            timeout=4,
        )
        if proc.returncode == 0:
            value = (proc.stdout or "").strip()
            if value:
                return value
    except Exception:
        pass
    return "unknown"


def _hms_for_daily(target_hhmm: str, now_local: datetime) -> datetime:
    parts = str(target_hhmm or "").strip().split(":")
    if len(parts) != 2:
        raise ValueError(f"invalid_target_hhmm={target_hhmm}")
    hour = int(parts[0])
    minute = int(parts[1])
    return now_local.replace(hour=hour, minute=minute, second=0, microsecond=0)


def _slot_for_interval(now_local: datetime, interval_minutes: int) -> datetime:
    minute = (int(now_local.minute) // int(interval_minutes)) * int(interval_minutes)
    return now_local.replace(minute=minute, second=0, microsecond=0)


def _candidate_for_job(
    spec: JobSpec,
    now_local: datetime,
    *,
    force: bool,
    state: dict[str, Any],
) -> dict[str, Any]:
    if spec.kind == "interval":
        slot_dt = _slot_for_interval(now_local, spec.interval_minutes)
        slot_key = slot_dt.strftime("%Y-%m-%dT%H:%M")
        if force:
            return {"due": True, "reason": "forced", "slot_key": slot_key, "scheduled_local": slot_dt.isoformat()}
        attempted_slot = str(state.get("last_slot_key") or "").strip()
        if attempted_slot == slot_key:
            return {"due": False, "reason": "slot_already_attempted", "slot_key": slot_key, "scheduled_local": slot_dt.isoformat()}
        return {"due": True, "reason": "new_interval_slot", "slot_key": slot_key, "scheduled_local": slot_dt.isoformat()}

    if spec.weekday_only and now_local.weekday() >= 5 and not force:
        scheduled = _hms_for_daily(spec.target_hhmm, now_local)
        return {
            "due": False,
            "reason": "weekday_only",
            "slot_key": now_local.date().isoformat(),
            "scheduled_local": scheduled.isoformat(),
        }

    scheduled_dt = _hms_for_daily(spec.target_hhmm, now_local)
    slot_key = scheduled_dt.date().isoformat()
    if force:
        return {"due": True, "reason": "forced", "slot_key": slot_key, "scheduled_local": scheduled_dt.isoformat()}

    if now_local < scheduled_dt:
        return {"due": False, "reason": "not_due_yet", "slot_key": slot_key, "scheduled_local": scheduled_dt.isoformat()}

    prior_slot = str(state.get("last_slot_key") or "").strip()
    prior_result = str(state.get("last_result") or "").strip().lower()
    prior_attempts = int(state.get("last_attempt_count") or 0)
    deadline = scheduled_dt + timedelta(minutes=int(spec.catchup_minutes))
    if now_local > deadline:
        if prior_slot == slot_key and prior_result in {"ran", "reconciled"}:
            return {
                "due": False,
                "reason": "already_ran",
                "slot_key": slot_key,
                "scheduled_local": scheduled_dt.isoformat(),
            }
        return {
            "due": False,
            "reason": f"window_closed_{spec.catchup_minutes}m",
            "slot_key": slot_key,
            "scheduled_local": scheduled_dt.isoformat(),
        }

    if prior_slot == slot_key:
        if prior_result == "ran":
            return {
                "due": False,
                "reason": "already_ran",
                "slot_key": slot_key,
                "scheduled_local": scheduled_dt.isoformat(),
            }
        if prior_attempts >= int(spec.max_attempts_per_slot):
            return {
                "due": False,
                "reason": "attempts_exhausted",
                "slot_key": slot_key,
                "scheduled_local": scheduled_dt.isoformat(),
            }
        return {
            "due": True,
            "reason": "retry_after_failure",
            "slot_key": slot_key,
            "scheduled_local": scheduled_dt.isoformat(),
        }

    return {"due": True, "reason": "first_attempt", "slot_key": slot_key, "scheduled_local": scheduled_dt.isoformat()}


def _run_with_secrets_cmd(repo_root: Path, script: str, args: list[str] | None = None) -> list[str]:
    cmd: list[str] = [
        "powershell",
        "-NoProfile",
        "-ExecutionPolicy",
        "Bypass",
        "-File",
        str((repo_root / "run_with_secrets.ps1").resolve(strict=False)),
        "--",
        "py",
        "-3",
        script,
    ]
    cmd.extend(args or [])
    return cmd


def _powershell_file_cmd(path: Path, args: list[str] | None = None) -> list[str]:
    cmd: list[str] = [
        "powershell",
        "-NoProfile",
        "-ExecutionPolicy",
        "Bypass",
        "-File",
        str(path.resolve(strict=False)),
    ]
    cmd.extend(args or [])
    return cmd


def _read_local_env_file(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    values: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = str(raw_line or "").strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def _first_env_config_value(repo_root: Path, *keys: str, default: str = "") -> str:
    env_file = _read_local_env_file(repo_root / ".env")
    for key in keys:
        candidate = str(os.environ.get(key) or env_file.get(key) or "").strip()
        if candidate:
            return candidate
    return str(default or "").strip()


def _set_outreach_env_print_config(repo_root: Path) -> dict[str, str]:
    script_path = (repo_root / "scripts" / "set_outreach_env.ps1").resolve(strict=False)
    if not script_path.exists():
        return {}
    proc = subprocess.run(
        [
            "powershell",
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(script_path),
            "-PrintConfig",
        ],
        cwd=str(repo_root),
        capture_output=True,
        text=True,
        check=False,
        timeout=180,
    )
    if proc.returncode != 0:
        return {}
    values: dict[str, str] = {}
    for raw_line in str(proc.stdout or "").splitlines():
        if "=" not in raw_line:
            continue
        key, value = raw_line.split("=", 1)
        key_norm = str(key or "").strip().lower()
        if not key_norm:
            continue
        values[key_norm] = str(value or "").strip()
    return values


def _resolve_inbound_backend(repo_root: Path) -> str:
    print_values = _set_outreach_env_print_config(repo_root)
    backend = str(print_values.get("inbound_backend") or "").strip().lower()
    if backend:
        return backend
    explicit = _first_env_config_value(repo_root, "INBOUND_BACKEND").strip().lower()
    if explicit:
        return explicit
    imap_markers = (
        _first_env_config_value(repo_root, "IMAP_USER", "BOUNCE_IMAP_USER"),
        _first_env_config_value(repo_root, "IMAP_PASS", "BOUNCE_IMAP_PASS"),
        _first_env_config_value(repo_root, "IMAP_HOST", "BOUNCE_IMAP_HOST"),
    )
    return "imap" if any(str(marker or "").strip() for marker in imap_markers) else "gmail"


def _python_file_cmd(repo_root: Path, relative_path: str, args: list[str] | None = None) -> list[str]:
    cmd: list[str] = ["py", "-3", str((repo_root / relative_path).resolve(strict=False))]
    cmd.extend(args or [])
    return cmd


def _job_skip_reason(repo_root: Path, job_name: str) -> str:
    if job_name == "inbound_triage":
        backend = _resolve_inbound_backend(repo_root)
        if backend == "imap":
            print_values = _set_outreach_env_print_config(repo_root)
            imap_user = str(print_values.get("imap_user") or "").strip()
            imap_pass_present = str(print_values.get("imap_pass_present") or "").strip().upper()
            if not imap_user:
                imap_user = _first_env_config_value(repo_root, "IMAP_USER", "BOUNCE_IMAP_USER")
            if not imap_pass_present:
                imap_pass = _first_env_config_value(repo_root, "IMAP_PASS", "BOUNCE_IMAP_PASS")
                imap_pass_present = "YES" if imap_pass else "NO"
            if not imap_user or imap_pass_present != "YES":
                return "imap_credentials_missing"
            return ""
        if backend == "gmail":
            gmail_credentials = (repo_root / "secrets" / "gmail_credentials.json").resolve(strict=False)
            if not gmail_credentials.exists():
                return "gmail_credentials_missing"
            return ""
        return f"invalid_inbound_backend_{backend}"
    return ""


def _job_commands(repo_root: Path, job_name: str, mode: str) -> list[list[str]]:
    if job_name == "inbound_triage":
        if mode == "live":
            return [
                _run_with_secrets_cmd(repo_root, "inbound_inbox_triage.py", ["--run-once"]),
                _run_with_secrets_cmd(repo_root, "run_capture_sync.py"),
            ]
        return [
            _run_with_secrets_cmd(repo_root, "inbound_inbox_triage.py", ["--run-once", "--dry-run"]),
            _run_with_secrets_cmd(repo_root, "run_capture_sync.py", ["--dry-run"]),
        ]

    if job_name == "ingest_daily":
        if mode == "live":
            return [_run_with_secrets_cmd(repo_root, "run_osha_ingest_daily.py")]
        if mode == "doctor":
            return [_run_with_secrets_cmd(repo_root, "run_osha_ingest_daily.py", ["--doctor"])]
        return [_run_with_secrets_cmd(repo_root, "run_osha_ingest_daily.py", ["--dry-run"])]

    if job_name == "prospect_replenish_daily":
        if mode == "live":
            return [_run_with_secrets_cmd(repo_root, "run_prospect_replenish_daily.py")]
        if mode == "doctor":
            return [_run_with_secrets_cmd(repo_root, "run_prospect_replenish_daily.py", ["--doctor"])]
        return [_run_with_secrets_cmd(repo_root, "run_prospect_replenish_daily.py", ["--dry-run"])]

    if job_name == "outreach_auto":
        if mode == "live":
            return [_run_with_secrets_cmd(repo_root, "run_outreach_auto.py")]
        if mode == "doctor":
            return [_run_with_secrets_cmd(repo_root, "run_outreach_auto.py", ["--doctor"])]
        return [_run_with_secrets_cmd(repo_root, "run_outreach_auto.py", ["--dry-run"])]

    if job_name == "trial_facs_daily":
        base = ["--subscriber-key", "facs_trial"]
        if mode == "live":
            return [_run_with_secrets_cmd(repo_root, "run_trial_daily.py", [*base, "--send-live"])]
        if mode == "doctor":
            return [_run_with_secrets_cmd(repo_root, "run_trial_daily.py", [*base, "--doctor"])]
        return [_run_with_secrets_cmd(repo_root, "run_trial_daily.py", [*base, "--dry-run"])]

    if job_name == "trial_jl_safety_daily":
        base = ["--subscriber-key", "jl_safety_trial"]
        if mode == "live":
            return [_run_with_secrets_cmd(repo_root, "run_trial_daily.py", [*base, "--send-live"])]
        if mode == "doctor":
            return [_run_with_secrets_cmd(repo_root, "run_trial_daily.py", [*base, "--doctor"])]
        return [_run_with_secrets_cmd(repo_root, "run_trial_daily.py", [*base, "--dry-run"])]

    if job_name == "trial_roi_safety_daily":
        base = ["--subscriber-key", "roi_safety_trial"]
        if mode == "live":
            return [_run_with_secrets_cmd(repo_root, "run_trial_daily.py", [*base, "--send-live"])]
        if mode == "doctor":
            return [_run_with_secrets_cmd(repo_root, "run_trial_daily.py", [*base, "--doctor"])]
        return [_run_with_secrets_cmd(repo_root, "run_trial_daily.py", [*base, "--dry-run"])]

    if job_name == "ops_snapshot_daily":
        if mode == "live":
            return [_python_file_cmd(repo_root, "outreach/run_ops_snapshot.py")]
        if mode == "doctor":
            return [
                _python_file_cmd(repo_root, "outreach/run_ops_snapshot.py", ["--print-config"]),
                _python_file_cmd(repo_root, "outreach/run_ops_snapshot.py", ["--dry-run"]),
            ]
        return [_python_file_cmd(repo_root, "outreach/run_ops_snapshot.py", ["--dry-run"])]

    if job_name == "outreach_cleanup_daily":
        cleanup_args = ["--retention-days", "14"]
        if mode == "live":
            return [_python_file_cmd(repo_root, "outreach/cleanup_outreach_dry_run_artifacts.py", cleanup_args)]
        if mode == "doctor":
            return [
                _python_file_cmd(repo_root, "outreach/cleanup_outreach_dry_run_artifacts.py", ["--print-config"]),
                _python_file_cmd(
                    repo_root,
                    "outreach/cleanup_outreach_dry_run_artifacts.py",
                    ["--dry-run", *cleanup_args],
                ),
            ]
        return [
            _python_file_cmd(
                repo_root,
                "outreach/cleanup_outreach_dry_run_artifacts.py",
                ["--dry-run", *cleanup_args],
            )
        ]

    if job_name == "ingest_evening":
        evening_wrapper = (repo_root / "scripts" / "scheduled" / "run_osha_ingest_evening.ps1").resolve(strict=False)
        signals_dump = (repo_root / "scripts" / "dump_signals_for_ai_review.ps1").resolve(strict=False)
        manual_prep = (repo_root / "scripts" / "prepare_manual_prospect_research.ps1").resolve(strict=False)
        if mode == "live":
            return [_powershell_file_cmd(evening_wrapper)]
        if mode == "doctor":
            return [
                _run_with_secrets_cmd(
                    repo_root,
                    "run_osha_ingest_daily.py",
                    ["--doctor", "--scope-mode", "outreach_plus_trial_live"],
                ),
                _powershell_file_cmd(signals_dump, ["-SinceDays", "14", "-PrintConfig"]),
                _powershell_file_cmd(manual_prep, ["-PrintConfig"]),
            ]
        return [
            _run_with_secrets_cmd(
                repo_root,
                "run_osha_ingest_daily.py",
                ["--dry-run", "--scope-mode", "outreach_plus_trial_live"],
            ),
            _powershell_file_cmd(signals_dump, ["-SinceDays", "14", "-DryRun"]),
            _powershell_file_cmd(manual_prep, ["-DryRun"]),
        ]

    raise ValueError(f"unsupported_job={job_name}")


def _run_command(repo_root: Path, cmd: list[str], env: dict[str, str]) -> tuple[int, str]:
    proc = subprocess.run(
        cmd,
        cwd=str(repo_root),
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    combined = ((proc.stdout or "") + "\n" + (proc.stderr or "")).strip()
    if proc.stdout:
        print(proc.stdout, end="")
    if proc.stderr:
        print(proc.stderr, end="", file=sys.stderr)
    return int(proc.returncode), combined


def _extract_token(text: str, token: str) -> str:
    matches = re.findall(rf"(?m)^{re.escape(token)}=(.*)$", text or "")
    if not matches:
        return ""
    return str(matches[-1]).strip()


def _parse_iso_local(raw: str) -> datetime | None:
    text = str(raw or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text)
    except Exception:
        return None
    if parsed.tzinfo is None:
        tz = _runtime_tz()
        if tz is not None:
            parsed = parsed.replace(tzinfo=tz)
    return parsed


def _wrapper_names_for_job(job_name: str) -> tuple[str, ...]:
    mapping = {
        "ingest_daily": ("OSHA_Osha_Ingest_Daily",),
        "ingest_evening": ("OSHA_Osha_Ingest_Evening",),
        "prospect_replenish_daily": ("OSHA_Prospect_Replenish_SafetyNet", "OSHA_Prospect_Replenish_Daily"),
        "outreach_auto": ("OSHA_Outreach_Auto_SafetyNet", "OSHA_Outreach_Auto"),
        "trial_facs_daily": ("OSHA_Trial_FACS_Daily",),
        "trial_jl_safety_daily": ("OSHA_Trial_JL_Safety_Daily",),
        "trial_roi_safety_daily": ("OSHA_Trial_ROI_Safety_Daily",),
    }
    return tuple(mapping.get(str(job_name or "").strip(), ()) or ())


def _artifact_root_candidates(repo_root: Path, data_dir: Path, env: dict[str, str], env_key: str, tail: tuple[str, ...]) -> list[Path]:
    roots: list[Path] = []
    raw_env = str(env.get(env_key) or "").strip()
    if raw_env:
        candidate = Path(raw_env).expanduser()
        if candidate.is_absolute():
            roots.append(candidate.resolve(strict=False))
    roots.append(data_dir.joinpath(*tail).resolve(strict=False))
    trusted_scheduled = str(env.get("MFO_TRUSTED_SCHEDULED") or "").strip() == "1"
    if not trusted_scheduled:
        roots.append((repo_root / Path(*tail)).resolve(strict=False))

    unique: list[Path] = []
    seen: set[str] = set()
    for root in roots:
        key = str(root).lower()
        if key in seen:
            continue
        seen.add(key)
        unique.append(root)
    return unique


def _run_summary_roots(repo_root: Path, data_dir: Path, env: dict[str, str]) -> list[Path]:
    return _artifact_root_candidates(repo_root, data_dir, env, "RUN_SUMMARY_ROOT", ("out", "run_summaries"))


def _choose_wrapper_evidence(
    *,
    job_name: str,
    slot_key: str,
    scheduled_local: str,
    summary_payloads: list[dict[str, Any]],
) -> WrapperRunEvidence | None:
    scheduled_dt = _parse_iso_local(scheduled_local)
    if scheduled_dt is None:
        return None
    job_spec = next((item for item in JOBS if item.name == job_name), None)
    catchup_minutes = int(job_spec.catchup_minutes) if job_spec is not None else 0
    deadline = scheduled_dt + timedelta(minutes=catchup_minutes)

    successes: list[WrapperRunEvidence] = []
    failures: list[WrapperRunEvidence] = []
    for payload in summary_payloads:
        payload_wrapper = str(payload.get("wrapper") or "").strip()
        if not payload_wrapper:
            continue
        start_local = str(payload.get("start_local") or "").strip()
        end_local = str(payload.get("end_local") or "").strip()
        started_dt = _parse_iso_local(start_local)
        if started_dt is None:
            continue
        exit_code = int(payload.get("exit_code") or 0)
        artifacts = payload.get("artifacts") or {}
        evidence = WrapperRunEvidence(
            job_name=job_name,
            wrapper_name=payload_wrapper,
            slot_key=slot_key,
            start_local=start_local,
            end_local=end_local,
            exit_code=exit_code,
            task_log_path=str(artifacts.get("task_log") or "").strip(),
            run_summary_json_path=str(artifacts.get("summary_json") or "").strip(),
            run_summary_text_path=str(artifacts.get("summary_text") or "").strip(),
            success=(exit_code == 0),
            timing="within_window" if started_dt <= deadline else "late",
        )
        if evidence.success:
            successes.append(evidence)
        else:
            failures.append(evidence)

    if successes:
        within_window = [item for item in successes if item.timing == "within_window"]
        if within_window:
            return sorted(within_window, key=lambda item: item.start_local)[0]
        return sorted(successes, key=lambda item: item.start_local)[0]
    if failures:
        return sorted(failures, key=lambda item: item.start_local)[-1]
    return None


def _find_wrapper_run_evidence_for_slot(
    *,
    repo_root: Path,
    data_dir: Path,
    env: dict[str, str],
    job_name: str,
    slot_key: str,
    scheduled_local: str,
) -> WrapperRunEvidence | None:
    wrapper_names = _wrapper_names_for_job(job_name)
    if not wrapper_names:
        return None
    slot_token = str(slot_key or "").replace("-", "")
    if not slot_token:
        return None

    summary_payloads: list[dict[str, Any]] = []
    seen_paths: set[str] = set()
    for root in _run_summary_roots(repo_root, data_dir, env):
        if not root.exists():
            continue
        for wrapper_name in wrapper_names:
            for path in sorted(root.glob(f"{wrapper_name}_{slot_token}_*.summary.json")):
                key = str(path.resolve(strict=False)).lower()
                if key in seen_paths:
                    continue
                seen_paths.add(key)
                payload = _read_json(path)
                if str(payload.get("wrapper") or "").strip() not in wrapper_names:
                    continue
                summary_payloads.append(payload)
    return _choose_wrapper_evidence(
        job_name=job_name,
        slot_key=slot_key,
        scheduled_local=scheduled_local,
        summary_payloads=summary_payloads,
    )


def _emit_external_scheduler_warning(job_name: str, slot_key: str, evidence: WrapperRunEvidence) -> None:
    _emit(
        WARN_RUNTIME_TICK_EXTERNAL_SCHEDULER,
        "job="
        + job_name
        + " slot="
        + slot_key
        + " timing="
        + evidence.timing
        + " exit_code="
        + str(evidence.exit_code)
        + " summary_json="
        + (evidence.run_summary_json_path or ""),
    )


def _reconcile_job_results_from_wrapper_artifacts(
    *,
    repo_root: Path,
    data_dir: Path,
    env: dict[str, str],
    job_results: list[dict[str, Any]],
) -> None:
    for job in job_results:
        name = str(job.get("name") or "").strip()
        if name not in CRITICAL_WINDOW_JOBS:
            continue
        slot_key = str(job.get("slot_key") or "").strip()
        scheduled_local = str(job.get("scheduled_local") or "").strip()
        if not slot_key or not scheduled_local:
            continue

        evidence = _find_wrapper_run_evidence_for_slot(
            repo_root=repo_root,
            data_dir=data_dir,
            env=env,
            job_name=name,
            slot_key=slot_key,
            scheduled_local=scheduled_local,
        )
        if evidence is None:
            continue

        _emit_external_scheduler_warning(name, slot_key, evidence)
        job["external_scheduler_detected"] = 1
        job["reconciliation_status"] = (
            "external_wrapper_success_within_window"
            if evidence.success and evidence.timing == "within_window"
            else "external_wrapper_success_late"
            if evidence.success
            else "external_wrapper_failed"
        )
        if evidence.start_local:
            job["wrapper_start_local"] = evidence.start_local
        if evidence.end_local:
            job["wrapper_end_local"] = evidence.end_local
        if evidence.task_log_path and not str(job.get("task_log_path") or "").strip():
            job["task_log_path"] = evidence.task_log_path
        if evidence.run_summary_json_path and not str(job.get("run_summary_json_path") or "").strip():
            job["run_summary_json_path"] = evidence.run_summary_json_path
        if evidence.run_summary_text_path and not str(job.get("run_summary_text_path") or "").strip():
            job["run_summary_text_path"] = evidence.run_summary_text_path

        result = str(job.get("result") or "").strip().lower()
        reason = str(job.get("reason") or "").strip()
        if result == "skipped" and reason.startswith("window_closed_") and evidence.success and evidence.timing == "within_window":
            job["result"] = "reconciled"
            job["reason"] = "external_wrapper_success_within_window"
            job["exit_code"] = 0
        elif result == "skipped" and reason.startswith("window_closed_") and evidence.success:
            job["wrapper_success_after_window_closed"] = 1


def _persist_job_states(
    *,
    repo_root: Path,
    data_dir: Path,
    prior_states: dict[str, dict[str, Any]],
    job_results: list[dict[str, Any]],
) -> None:
    for job in job_results:
        job_name = str(job.get("name") or "").strip()
        if not job_name:
            continue
        state_path = _job_state_path(data_dir, job_name)
        prior_state = dict(prior_states.get(job_name) or {})
        slot_key = str(job.get("slot_key") or "").strip()
        result = str(job.get("result") or "").strip().lower()
        prior_slot = str(prior_state.get("last_slot_key") or "").strip()
        prior_attempts = int(prior_state.get("last_attempt_count") or 0)
        if result in {"ran", "failed", "doctor_ok", "dry_run_ok"}:
            attempts = prior_attempts + 1 if prior_slot == slot_key else 1
        elif result == "reconciled":
            attempts = prior_attempts if prior_slot == slot_key and prior_attempts > 0 else 1
        elif prior_slot == slot_key:
            attempts = prior_attempts
        else:
            attempts = 0

        last_result = "failed" if result == "failed" else "ran" if result in {"ran", "reconciled"} else "skipped"
        state_payload: dict[str, Any] = {
            "schema": RUNTIME_JOB_STATE_SCHEMA,
            "job_name": job_name,
            "last_slot_key": slot_key,
            "last_scheduled_local": str(job.get("scheduled_local") or ""),
            "last_started_local": str(job.get("started_local") or ""),
            "last_finished_local": str(job.get("finished_local") or ""),
            "last_result": last_result,
            "last_result_detail": result,
            "last_exit_code": int(job.get("exit_code") or 0),
            "last_reason": str(job.get("reason") or ""),
            "last_attempt_count": attempts,
            "last_task_log_path": str(job.get("task_log_path") or ""),
            "last_run_summary_json_path": str(job.get("run_summary_json_path") or ""),
            "last_run_summary_text_path": str(job.get("run_summary_text_path") or ""),
            "last_git_sha": _git_sha(repo_root),
        }
        if job.get("external_scheduler_detected"):
            state_payload["last_external_scheduler_detected"] = 1
            state_payload["last_reconciliation_status"] = str(job.get("reconciliation_status") or "")
            state_payload["last_wrapper_start_local"] = str(job.get("wrapper_start_local") or "")
            state_payload["last_wrapper_end_local"] = str(job.get("wrapper_end_local") or "")
        _write_json(state_path, state_payload)


def _acquire_lock(lock_path: Path) -> tuple[bool, str]:
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    now = time.time()
    if lock_path.exists():
        age = now - lock_path.stat().st_mtime
        if age > LOCK_STALE_SECONDS:
            try:
                lock_path.unlink()
            except Exception:
                return False, f"lock_stale_cleanup_failed age_seconds={int(age)} path={lock_path}"
        else:
            return False, f"lock_active age_seconds={int(age)} path={lock_path}"
    payload = {
        "pid": os.getpid(),
        "created_utc": datetime.now(timezone.utc).isoformat(),
    }
    lock_path.write_text(json.dumps(payload) + "\n", encoding="utf-8")
    return True, ""


def _release_lock(lock_path: Path) -> None:
    try:
        if lock_path.exists():
            lock_path.unlink()
    except Exception:
        pass


def _selected_jobs(job_arg: str, *, jobs: tuple[JobSpec, ...]) -> list[JobSpec]:
    if str(job_arg or "").strip().lower() == "all":
        return list(jobs)
    wanted = str(job_arg or "").strip().lower()
    for spec in jobs:
        if spec.name == wanted:
            return [spec]
    raise ValueError(f"invalid_job={job_arg}")


def _env_bool(name: str, *, default: bool, env: dict[str, str]) -> bool:
    raw = str(env.get(name) or "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


def _sanitize_for_filename(text: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", str(text or "").strip())
    return cleaned or "none"


def _alert_marker_path(data_dir: Path, candidate: AlertCandidate) -> Path:
    file_name = (
        f"{_sanitize_for_filename(candidate.name)}_"
        f"{_sanitize_for_filename(candidate.slot_key)}_"
        f"{_sanitize_for_filename(candidate.category)}.json"
    )
    return (_alerts_root(data_dir) / file_name).resolve(strict=False)


def _collect_alert_candidates(job_results: list[dict[str, Any]]) -> list[AlertCandidate]:
    candidates: list[AlertCandidate] = []
    for job in job_results:
        name = str(job.get("name") or "").strip()
        if not name:
            continue
        result = str(job.get("result") or "").strip().lower()
        reason = str(job.get("reason") or "").strip()
        slot_key = str(job.get("slot_key") or "").strip()
        scheduled_local = str(job.get("scheduled_local") or "").strip()
        task_log_path = str(job.get("task_log_path") or "").strip()
        run_summary_json_path = str(job.get("run_summary_json_path") or "").strip()
        run_summary_text_path = str(job.get("run_summary_text_path") or "").strip()
        reconciliation_status = str(job.get("reconciliation_status") or "").strip()
        exit_code = int(job.get("exit_code") or 0)
        actionable_external_wrapper_failure = (
            reconciliation_status == "external_wrapper_failed"
            and result == "skipped"
            and name in CRITICAL_WINDOW_JOBS
            and reason.startswith("window_closed_")
        )

        if result == "failed" or actionable_external_wrapper_failure:
            alert_reason = reason if result == "failed" else reconciliation_status
            alert_exit_code = exit_code if int(exit_code) != 0 else 1
            candidates.append(
                AlertCandidate(
                    name=name,
                    category="job_failure",
                    slot_key=slot_key,
                    scheduled_local=scheduled_local,
                    result=result,
                    reason=alert_reason,
                    exit_code=alert_exit_code,
                    task_log_path=task_log_path,
                    run_summary_json_path=run_summary_json_path,
                    run_summary_text_path=run_summary_text_path,
                    reconciliation_status=reconciliation_status,
                )
            )
            continue

        if (
            result == "skipped"
            and name in CRITICAL_WINDOW_JOBS
            and reason.startswith("window_closed_")
        ):
            candidates.append(
                AlertCandidate(
                    name=name,
                    category="missed_window",
                    slot_key=slot_key,
                    scheduled_local=scheduled_local,
                    result=result,
                    reason=reason,
                    exit_code=exit_code,
                    task_log_path=task_log_path,
                    run_summary_json_path=run_summary_json_path,
                    run_summary_text_path=run_summary_text_path,
                    reconciliation_status=reconciliation_status,
                )
            )
    return candidates


def _build_alert_subject(candidate: AlertCandidate) -> str:
    if candidate.category == "missed_window":
        return f"[OSHA Runtime Missed Window] {candidate.name} {candidate.slot_key or 'unslotted'}"
    return f"[OSHA Runtime Failure] {candidate.name} {candidate.slot_key or 'unslotted'}"


def _build_alert_body(
    *,
    candidate: AlertCandidate,
    repo_root: Path,
    data_dir: Path,
    git_sha: str,
    now_local: datetime,
) -> str:
    lines = [
        "OSHA Runtime Alert",
        "",
        f"category: {candidate.category}",
        f"job_name: {candidate.name}",
        f"slot_key: {candidate.slot_key}",
        f"scheduled_local: {candidate.scheduled_local}",
        f"result: {candidate.result}",
        f"reason: {candidate.reason}",
        f"exit_code: {candidate.exit_code}",
        f"triggered_local: {now_local.isoformat()}",
        f"git_sha: {git_sha}",
        f"repo_root: {repo_root}",
        f"data_dir: {data_dir}",
    ]
    if candidate.reconciliation_status:
        lines.append(f"reconciliation_status: {candidate.reconciliation_status}")
    if candidate.task_log_path:
        lines.append(f"task_log_path: {candidate.task_log_path}")
    if candidate.run_summary_json_path:
        lines.append(f"run_summary_json_path: {candidate.run_summary_json_path}")
    if candidate.run_summary_text_path:
        lines.append(f"run_summary_text_path: {candidate.run_summary_text_path}")
    return "\n".join(lines) + "\n"


def _evaluate_runtime_alerts(
    *,
    data_dir: Path,
    repo_root: Path,
    mode: str,
    env: dict[str, str],
    now_local: datetime,
    job_results: list[dict[str, Any]],
) -> dict[str, Any]:
    git_sha = _git_sha(repo_root)
    candidates = _collect_alert_candidates(job_results)
    recipient = resolve_alert_recipient(env)
    alerts_enabled = _env_bool(
        "RUNTIME_ALERTS_ENABLED",
        default=bool(recipient),
        env=env,
    )

    sent_records: list[dict[str, Any]] = []
    skipped_records: list[dict[str, Any]] = []
    errors: list[str] = []

    if not candidates:
        _emit("RUNTIME_TICK_ALERT_SKIPPED", "reason=no_candidates")

    for candidate in candidates:
        send_allowed = mode == "live" and alerts_enabled and bool(recipient)
        candidate_reason = "ready_to_send" if send_allowed else (
            "non_live_mode" if mode != "live" else (
                "disabled" if not alerts_enabled else "no_recipient"
            )
        )
        _emit(
            "RUNTIME_TICK_ALERT_CANDIDATE",
            f"name={candidate.name} category={candidate.category} send={1 if send_allowed else 0} reason={candidate_reason}",
        )
        if not send_allowed:
            skipped_records.append(
                {
                    "name": candidate.name,
                    "category": candidate.category,
                    "slot_key": candidate.slot_key,
                    "reason": candidate_reason,
                }
            )
            _emit(
                "RUNTIME_TICK_ALERT_SKIPPED",
                f"name={candidate.name} category={candidate.category} reason={candidate_reason}",
            )
            continue

        marker_path = _alert_marker_path(data_dir, candidate)
        if marker_path.exists():
            skipped_records.append(
                {
                    "name": candidate.name,
                    "category": candidate.category,
                    "slot_key": candidate.slot_key,
                    "reason": "duplicate",
                }
            )
            _emit(
                "RUNTIME_TICK_ALERT_SKIPPED",
                f"name={candidate.name} category={candidate.category} reason=duplicate",
            )
            continue

        missing = smtp_missing_key(env)
        if missing:
            skipped_records.append(
                {
                    "name": candidate.name,
                    "category": candidate.category,
                    "slot_key": candidate.slot_key,
                    "reason": f"smtp_unavailable_{missing}",
                }
            )
            _emit(
                "RUNTIME_TICK_ALERT_SKIPPED",
                f"name={candidate.name} category={candidate.category} reason=smtp_unavailable_{missing}",
            )
            continue

        subject = _build_alert_subject(candidate)
        body = _build_alert_body(
            candidate=candidate,
            repo_root=repo_root,
            data_dir=data_dir,
            git_sha=git_sha,
            now_local=now_local,
        )
        try:
            send_plain_text_alert(
                recipient=recipient,
                subject=subject,
                body=body,
                env=env,
            )
            marker_payload = {
                "schema": ALERTS_SCHEMA,
                "sent_local": now_local.isoformat(),
                "recipient": recipient,
                "subject": subject,
                "git_sha": git_sha,
                "job_name": candidate.name,
                "category": candidate.category,
                "slot_key": candidate.slot_key,
                "scheduled_local": candidate.scheduled_local,
                "result": candidate.result,
                "reason": candidate.reason,
                "exit_code": candidate.exit_code,
                "reconciliation_status": candidate.reconciliation_status,
                "task_log_path": candidate.task_log_path,
                "run_summary_json_path": candidate.run_summary_json_path,
                "run_summary_text_path": candidate.run_summary_text_path,
            }
            _write_json(marker_path, marker_payload)
            sent_records.append(marker_payload)
            _emit("RUNTIME_TICK_ALERT_SENT", f"count={len(sent_records)} recipient={recipient}")
        except Exception as exc:
            detail = f"name={candidate.name} category={candidate.category} detail={exc.__class__.__name__}:{exc}"
            errors.append(detail)
            _emit("RUNTIME_TICK_ALERT_ERROR", detail)
            skipped_records.append(
                {
                    "name": candidate.name,
                    "category": candidate.category,
                    "slot_key": candidate.slot_key,
                    "reason": "send_error",
                }
            )

    summary: dict[str, Any] = {
        "schema": ALERTS_SUMMARY_SCHEMA,
        "alerts_enabled": bool(alerts_enabled),
        "recipient": recipient,
        "alerts_evaluated": len(candidates),
        "alerts_sent": len(sent_records),
        "alerts_skipped": len(skipped_records),
        "last_alerts": sent_records,
        "skipped_alerts": skipped_records,
        "errors": errors,
    }
    return summary


def _update_runtime_latest(
    *,
    data_dir: Path,
    mode: str,
    started_local: datetime,
    now_local: datetime,
    repo_root: Path,
    job_results: list[dict[str, Any]],
    alert_summary: dict[str, Any] | None = None,
) -> None:
    root = _status_root(data_dir)
    root.mkdir(parents=True, exist_ok=True)
    payload: dict[str, Any] = {
        "schema": "runtime_tick_v1",
        "mode": mode,
        "started_local": started_local.isoformat(),
        "finished_local": now_local.isoformat(),
        "repo_root": str(repo_root),
        "data_dir": str(data_dir),
        "git_sha": _git_sha(repo_root),
        "jobs": job_results,
    }
    if alert_summary:
        payload["alerts"] = alert_summary
    latest_json = root / "runtime_latest.json"
    latest_md = root / "runtime_latest.md"
    _write_json(latest_json, payload)
    lines: list[str] = [
        "# Runtime Latest",
        "",
        f"- mode: `{mode}`",
        f"- started_local: `{payload['started_local']}`",
        f"- finished_local: `{payload['finished_local']}`",
        f"- git_sha: `{payload['git_sha']}`",
        "",
        "## Jobs",
    ]
    for job in job_results:
        lines.append(
            f"- `{job.get('name','')}` result=`{job.get('result','')}` "
            f"exit_code=`{job.get('exit_code','')}` reason=`{job.get('reason','')}`"
        )
    if alert_summary:
        lines.extend(
            [
                "",
                "## Alerts",
                f"- enabled: `{1 if bool(alert_summary.get('alerts_enabled')) else 0}`",
                f"- evaluated: `{alert_summary.get('alerts_evaluated', 0)}`",
                f"- sent: `{alert_summary.get('alerts_sent', 0)}`",
                f"- skipped: `{alert_summary.get('alerts_skipped', 0)}`",
                f"- recipient: `{alert_summary.get('recipient', '')}`",
            ]
        )
        sent_rows = alert_summary.get("last_alerts") or []
        for row in sent_rows:
            lines.append(
                f"- sent `{row.get('category','')}` for `{row.get('job_name','')}` "
                f"slot=`{row.get('slot_key','')}`"
            )
    latest_md.write_text("\n".join(lines) + "\n", encoding="utf-8")
    _emit("RUNTIME_TICK_STATUS_JSON_PATH", str(latest_json))
    _emit("RUNTIME_TICK_STATUS_TEXT_PATH", str(latest_md))


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="15-minute runtime orchestrator for canonical operations.")
    ap.add_argument("--print-config", action="store_true", help="Print resolved runtime config and exit.")
    ap.add_argument("--doctor", action="store_true", help="Run job doctor checks and exit.")
    ap.add_argument("--dry-run", action="store_true", help="Run due jobs in dry-run mode and exit.")
    ap.add_argument("--job", default="all", help=f"Job name or all. Choices: {','.join(JOB_NAMES)}")
    ap.add_argument("--now-local", default="", help="Optional local timestamp override (YYYY-MM-DDTHH:MM).")
    ap.add_argument("--force", action="store_true", help="Force selected job(s) regardless of schedule windows.")
    ap.add_argument("--mode", choices=["manual", "scheduled"], default="", help="Optional runtime mode override.")
    return ap


def _print_config(repo_root: Path, data_dir: Path, selected: list[JobSpec]) -> None:
    schedule = load_runtime_schedule(data_dir)
    _emit("RUNTIME_TICK_REPO_ROOT", str(repo_root))
    _emit("RUNTIME_TICK_DATA_DIR", str(data_dir))
    _emit("RUNTIME_TICK_STATUS_ROOT", str(_status_root(data_dir)))
    _emit("RUNTIME_TICK_LOCK_ROOT", str(_locks_root(data_dir)))
    _emit("RUNTIME_TICK_SCHEDULE_CONFIG_PATH", str(schedule.path))
    _emit("RUNTIME_TICK_SCHEDULE_SCHEMA", schedule.schema)
    _emit("RUNTIME_TICK_SCHEDULE_SOURCE", schedule.source)
    _emit("RUNTIME_TICK_SCHEDULE_OUTREACH_SEND_LOCAL_HHMM", schedule.outreach_send_local_hhmm)
    _emit("RUNTIME_TICK_SCHEDULE_TRIAL_DEFAULT_SEND_LOCAL_HHMM", schedule.trial_default_send_local_hhmm)
    _emit("RUNTIME_TICK_SCHEDULE_EVENING_PREP_LOCAL_HHMM", schedule.evening_prep_local_hhmm)
    _emit("RUNTIME_TICK_PRIMARY_SCHEDULER", "runtime_tick_selfhosted")
    _emit("RUNTIME_TICK_CANONICAL_RUN_SUMMARY_ROOT", str((data_dir / "out" / "run_summaries").resolve(strict=False)))
    _emit("RUNTIME_TICK_SELECTED_JOBS", ",".join(spec.name for spec in selected))
    for spec in selected:
        _emit("RUNTIME_TICK_JOB_NAME", spec.name)
        _emit("RUNTIME_TICK_JOB_KIND", spec.kind)
        _emit("RUNTIME_TICK_JOB_WEEKDAY_ONLY", 1 if spec.weekday_only else 0)
        if spec.kind == "daily":
            _emit("RUNTIME_TICK_JOB_TIME", spec.target_hhmm)
            _emit("RUNTIME_TICK_JOB_CATCHUP_MINUTES", spec.catchup_minutes)
        else:
            _emit("RUNTIME_TICK_JOB_INTERVAL_MINUTES", spec.interval_minutes)
    print(f"{PASS_RUNTIME_TICK_PRINT_CONFIG} status=OK")
    print(f"{PASS_RUNTIME_TICK_COMPLETE} status=PRINT_CONFIG")


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    mode_count = int(bool(args.print_config)) + int(bool(args.doctor)) + int(bool(args.dry_run))
    if mode_count > 1:
        return _error("modes_mutually_exclusive")

    repo_root = _repo_root()
    if not (repo_root / "run_with_secrets.ps1").exists():
        return _error("missing_run_with_secrets")

    resolution = resolve_data_dir(repo_root)
    data_dir = resolution.effective_path
    if not data_dir.is_absolute():
        return _error(f"data_dir_not_absolute path={data_dir}")

    jobs = _jobs_for_data_dir(data_dir)
    try:
        selected_jobs = _selected_jobs(str(args.job or "all"), jobs=jobs)
    except ValueError as exc:
        return _error(str(exc))

    try:
        now_local = _now_local(str(args.now_local or ""))
    except ValueError as exc:
        return _error(str(exc))

    if args.print_config:
        _print_config(repo_root=repo_root, data_dir=data_dir, selected=selected_jobs)
        return 0

    live_mode = not args.doctor and not args.dry_run
    runtime_mode = str(args.mode or "").strip().lower()
    if runtime_mode not in {"manual", "scheduled"}:
        runtime_mode = "scheduled" if live_mode else "manual"

    preflight_intent = "write" if live_mode else "read"
    preflight = run_runtime_preflight(
        mode=runtime_mode,
        intent=preflight_intent,
        dry_run=not live_mode,
        task_log_root=str(os.getenv("TASK_LOG_ROOT") or ""),
        run_summary_root=str(os.getenv("RUN_SUMMARY_ROOT") or ""),
    )
    for line in render_runtime_lines(preflight):
        print(line)
    if not preflight.ok:
        return 2

    _emit("RUNTIME_TICK_MODE", "doctor" if args.doctor else "dry_run" if args.dry_run else "live")
    _emit("RUNTIME_TICK_REPO_ROOT", str(repo_root))
    _emit("RUNTIME_TICK_DATA_DIR", str(data_dir))
    _emit("RUNTIME_TICK_PRIMARY_SCHEDULER", "runtime_tick_selfhosted")
    _emit("RUNTIME_TICK_CANONICAL_RUN_SUMMARY_ROOT", str((data_dir / "out" / "run_summaries").resolve(strict=False)))
    _emit("RUNTIME_TICK_NOW_LOCAL", now_local.isoformat())
    _emit("RUNTIME_TICK_SELECTED_JOBS", ",".join(job.name for job in selected_jobs))

    started_local = now_local
    status_fail = False
    job_results: list[dict[str, Any]] = []
    alert_summary: dict[str, Any] | None = None
    trusted_scheduled = bool(getattr(getattr(preflight, "fingerprint", None), "trusted_scheduled", False))
    env = dict(os.environ)
    env["MFO_RUNTIME_MODE"] = runtime_mode
    env["MFO_TRUSTED_SCHEDULED"] = "1" if trusted_scheduled else "0"
    _emit("RUNTIME_TICK_REPO_RUN_SUMMARY_FALLBACK_ALLOWED", 0 if env["MFO_TRUSTED_SCHEDULED"] == "1" else 1)
    prior_states: dict[str, dict[str, Any]] = {}

    lock_path = (_locks_root(data_dir) / "runtime_tick.lock").resolve(strict=False)
    lock_acquired = False
    if live_mode:
        ok_lock, lock_detail = _acquire_lock(lock_path)
        if not ok_lock:
            print(f"{ERR_RUNTIME_TICK_LOCKED} {lock_detail}", file=sys.stderr)
            return 2
        lock_acquired = True

    try:
        for spec in selected_jobs:
            prior_state = _read_json(_job_state_path(data_dir, spec.name)) if live_mode else {}
            prior_states[spec.name] = dict(prior_state)
            candidate = _candidate_for_job(
                spec,
                now_local,
                force=bool(args.force or args.doctor),
                state=prior_state,
            )
            due = bool(candidate.get("due"))
            reason = str(candidate.get("reason") or "")
            slot_key = str(candidate.get("slot_key") or "")
            scheduled_local = str(candidate.get("scheduled_local") or "")
            _emit(
                "RUNTIME_TICK_JOB_CANDIDATE",
                f"name={spec.name} due={1 if due else 0} reason={reason} slot={slot_key}",
            )

            mode_name = "doctor" if args.doctor else "dry_run" if args.dry_run else "live"
            skip_reason = _job_skip_reason(repo_root, spec.name)
            if skip_reason:
                job_results.append(
                    {
                        "name": spec.name,
                        "result": "skipped",
                        "reason": skip_reason,
                        "exit_code": 0,
                        "slot_key": slot_key,
                        "scheduled_local": scheduled_local,
                        "started_local": "",
                        "finished_local": "",
                        "task_log_path": "",
                        "run_summary_json_path": "",
                        "run_summary_text_path": "",
                        "reconciliation_status": "",
                    }
                )
                _emit(
                    "RUNTIME_TICK_JOB_RESULT",
                    f"name={spec.name} result=skipped exit_code=0 reason={skip_reason}",
                )
                continue
            if not due and not args.doctor:
                job_results.append(
                    {
                        "name": spec.name,
                        "result": "skipped",
                        "reason": reason,
                        "exit_code": 0,
                        "slot_key": slot_key,
                        "scheduled_local": scheduled_local,
                        "started_local": "",
                        "finished_local": "",
                        "task_log_path": "",
                        "run_summary_json_path": "",
                        "run_summary_text_path": "",
                        "reconciliation_status": "",
                    }
                )
                _emit("RUNTIME_TICK_JOB_RESULT", f"name={spec.name} result=skipped exit_code=0 reason={reason}")
                continue

            started_job = datetime.now(now_local.tzinfo)
            commands = _job_commands(repo_root=repo_root, job_name=spec.name, mode=mode_name)
            exit_code = 0
            merged_output: list[str] = []
            for idx, cmd in enumerate(commands, start=1):
                _emit("RUNTIME_TICK_JOB_STAGE", f"name={spec.name} index={idx} command={' '.join(cmd)}")
                code, output = _run_command(repo_root=repo_root, cmd=cmd, env=env)
                merged_output.append(output)
                if code != 0:
                    exit_code = code
                    print(
                        f"{ERR_RUNTIME_TICK_STAGE} job={spec.name} stage_index={idx} code={code}",
                        file=sys.stderr,
                    )
                    break

            finished_job = datetime.now(now_local.tzinfo)
            combined_text = "\n".join(merged_output).strip()
            task_log_path = _extract_token(combined_text, "TASK_LOG_PATH")
            run_summary_json = _extract_token(combined_text, "RUN_SUMMARY_JSON_PATH")
            run_summary_text = _extract_token(combined_text, "RUN_SUMMARY_TEXT_PATH")
            result = "ran" if exit_code == 0 else "failed"
            if args.doctor and exit_code == 0:
                result = "doctor_ok"
            elif args.dry_run and exit_code == 0:
                result = "dry_run_ok"

            _emit(
                "RUNTIME_TICK_JOB_RESULT",
                f"name={spec.name} result={result} exit_code={exit_code} slot={slot_key}",
            )
            job_results.append(
                {
                    "name": spec.name,
                    "result": result,
                    "reason": reason,
                    "exit_code": int(exit_code),
                    "slot_key": slot_key,
                    "scheduled_local": scheduled_local,
                    "started_local": started_job.isoformat(),
                    "finished_local": finished_job.isoformat(),
                    "task_log_path": task_log_path,
                    "run_summary_json_path": run_summary_json,
                    "run_summary_text_path": run_summary_text,
                    "reconciliation_status": "",
                }
            )

            if exit_code != 0:
                status_fail = True
                if args.doctor:
                    continue
                if not args.dry_run:
                    # In live mode fail fast to avoid compounding bad state.
                    break

        if live_mode:
            _reconcile_job_results_from_wrapper_artifacts(
                repo_root=repo_root,
                data_dir=data_dir,
                env=env,
                job_results=job_results,
            )
            _persist_job_states(
                repo_root=repo_root,
                data_dir=data_dir,
                prior_states=prior_states,
                job_results=job_results,
            )

        now_done = datetime.now(now_local.tzinfo)
        alert_summary = _evaluate_runtime_alerts(
            data_dir=data_dir,
            repo_root=repo_root,
            mode="doctor" if args.doctor else "dry_run" if args.dry_run else "live",
            env=env,
            now_local=now_done,
            job_results=job_results,
        )
        _update_runtime_latest(
            data_dir=data_dir,
            mode="doctor" if args.doctor else "dry_run" if args.dry_run else "live",
            started_local=started_local,
            now_local=now_done,
            repo_root=repo_root,
            job_results=job_results,
            alert_summary=alert_summary,
        )
    finally:
        if lock_acquired:
            _release_lock(lock_path)

    if args.doctor:
        if status_fail:
            return 2
        print(f"{PASS_RUNTIME_TICK_DOCTOR} status=OK")
        print(f"{PASS_RUNTIME_TICK_COMPLETE} status=DOCTOR")
        return 0

    if args.dry_run:
        if status_fail:
            return 2
        print(f"{PASS_RUNTIME_TICK_COMPLETE} status=DRY_RUN")
        return 0

    if status_fail:
        return 2
    print(f"{PASS_RUNTIME_TICK_COMPLETE} status=OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
