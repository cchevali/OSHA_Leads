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

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore[assignment]


ERR_RUNTIME_TICK_CONFIG = "ERR_RUNTIME_TICK_CONFIG"
ERR_RUNTIME_TICK_LOCKED = "ERR_RUNTIME_TICK_LOCKED"
ERR_RUNTIME_TICK_STAGE = "ERR_RUNTIME_TICK_STAGE"
PASS_RUNTIME_TICK_PRINT_CONFIG = "PASS_RUNTIME_TICK_PRINT_CONFIG"
PASS_RUNTIME_TICK_DOCTOR = "PASS_RUNTIME_TICK_DOCTOR"
PASS_RUNTIME_TICK_COMPLETE = "PASS_RUNTIME_TICK_COMPLETE"

RUNTIME_TZ_NAME = "America/New_York"
RUNTIME_TZ_FALLBACK = "Eastern Standard Time"
LOCK_STALE_SECONDS = 4 * 60 * 60
ALERTS_SCHEMA = "runtime_tick_alert_v1"
ALERTS_SUMMARY_SCHEMA = "runtime_tick_alert_summary_v1"
CRITICAL_WINDOW_JOBS = frozenset({"ingest_daily", "prospect_replenish_daily", "outreach_auto", "trial_facs_daily"})


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


JOBS: tuple[JobSpec, ...] = (
    JobSpec(name="inbound_triage", kind="interval", weekday_only=False, interval_minutes=15, max_attempts_per_slot=1),
    JobSpec(name="ai_review_dump", kind="daily", weekday_only=True, target_hhmm="05:00", catchup_minutes=180),
    JobSpec(name="ingest_daily", kind="daily", weekday_only=True, target_hhmm="06:45", catchup_minutes=180),
    JobSpec(name="prospect_replenish_daily", kind="daily", weekday_only=True, target_hhmm="07:15", catchup_minutes=180),
    JobSpec(name="outreach_auto", kind="daily", weekday_only=True, target_hhmm="08:00", catchup_minutes=180),
    JobSpec(name="trial_facs_daily", kind="daily", weekday_only=True, target_hhmm="09:00", catchup_minutes=180),
)
JOB_NAMES = tuple(job.name for job in JOBS)


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

    deadline = scheduled_dt + timedelta(minutes=int(spec.catchup_minutes))
    if now_local > deadline:
        return {
            "due": False,
            "reason": f"window_closed_{spec.catchup_minutes}m",
            "slot_key": slot_key,
            "scheduled_local": scheduled_dt.isoformat(),
        }

    prior_slot = str(state.get("last_slot_key") or "").strip()
    prior_result = str(state.get("last_result") or "").strip().lower()
    prior_attempts = int(state.get("last_attempt_count") or 0)
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


def _job_skip_reason(repo_root: Path, job_name: str) -> str:
    if job_name == "inbound_triage":
        gmail_credentials = (repo_root / "secrets" / "gmail_credentials.json").resolve(strict=False)
        if not gmail_credentials.exists():
            return "gmail_credentials_missing"
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

    if job_name == "ai_review_dump":
        dump_ps = repo_root / "scripts" / "dump_signals_for_ai_review.ps1"
        if mode == "live":
            return [
                _run_with_secrets_cmd(repo_root, "run_osha_ingest_daily.py", ["--scope-mode", "outreach_plus_trial_live"]),
                _powershell_file_cmd(dump_ps, ["-SinceDays", "14"]),
            ]
        return [
            _run_with_secrets_cmd(repo_root, "run_osha_ingest_daily.py", ["--doctor"]),
            _powershell_file_cmd(dump_ps, ["-SinceDays", "14", "-PrintConfig"]),
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


def _selected_jobs(job_arg: str) -> list[JobSpec]:
    if str(job_arg or "").strip().lower() == "all":
        return list(JOBS)
    wanted = str(job_arg or "").strip().lower()
    for spec in JOBS:
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
        exit_code = int(job.get("exit_code") or 0)

        if result == "failed":
            candidates.append(
                AlertCandidate(
                    name=name,
                    category="job_failure",
                    slot_key=slot_key,
                    scheduled_local=scheduled_local,
                    result=result,
                    reason=reason,
                    exit_code=exit_code,
                    task_log_path=task_log_path,
                    run_summary_json_path=run_summary_json_path,
                    run_summary_text_path=run_summary_text_path,
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
    _emit("RUNTIME_TICK_REPO_ROOT", str(repo_root))
    _emit("RUNTIME_TICK_DATA_DIR", str(data_dir))
    _emit("RUNTIME_TICK_STATUS_ROOT", str(_status_root(data_dir)))
    _emit("RUNTIME_TICK_LOCK_ROOT", str(_locks_root(data_dir)))
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

    try:
        selected_jobs = _selected_jobs(str(args.job or "all"))
    except ValueError as exc:
        return _error(str(exc))

    resolution = resolve_data_dir(repo_root)
    data_dir = resolution.effective_path
    if not data_dir.is_absolute():
        return _error(f"data_dir_not_absolute path={data_dir}")

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
    _emit("RUNTIME_TICK_NOW_LOCAL", now_local.isoformat())
    _emit("RUNTIME_TICK_SELECTED_JOBS", ",".join(job.name for job in selected_jobs))

    started_local = now_local
    status_fail = False
    job_results: list[dict[str, Any]] = []
    alert_summary: dict[str, Any] | None = None
    env = dict(os.environ)

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
            state_path = _job_state_path(data_dir, spec.name)
            prior_state = _read_json(state_path) if live_mode else {}
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
                }
            )

            if live_mode:
                prior_slot = str(prior_state.get("last_slot_key") or "")
                prior_attempts = int(prior_state.get("last_attempt_count") or 0)
                attempts = prior_attempts + 1 if prior_slot == slot_key else 1
                state_payload: dict[str, Any] = {
                    "schema": "runtime_tick_job_state_v1",
                    "job_name": spec.name,
                    "last_slot_key": slot_key,
                    "last_scheduled_local": scheduled_local,
                    "last_started_local": started_job.isoformat(),
                    "last_finished_local": finished_job.isoformat(),
                    "last_result": "ran" if exit_code == 0 else "failed",
                    "last_exit_code": int(exit_code),
                    "last_reason": reason,
                    "last_attempt_count": attempts,
                    "last_task_log_path": task_log_path,
                    "last_run_summary_json_path": run_summary_json,
                    "last_run_summary_text_path": run_summary_text,
                    "last_git_sha": _git_sha(repo_root),
                }
                _write_json(state_path, state_payload)

            if exit_code != 0:
                status_fail = True
                if args.doctor:
                    continue
                if not args.dry_run:
                    # In live mode fail fast to avoid compounding bad state.
                    break

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
