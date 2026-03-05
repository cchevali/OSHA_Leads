from __future__ import annotations

import argparse
import json
import os
import socket
import subprocess
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from runtime_data_dir import resolve_data_dir

VALID_RUNTIME_ROLES = {"canonical_scheduler", "dev_client"}
VALID_MODES = {"scheduled", "manual"}
VALID_INTENTS = {"send", "write", "read"}

ERR_RUNTIME_ROLE_INVALID = "ERR_RUNTIME_ROLE_INVALID"
ERR_RUNTIME_CANONICAL_HOSTNAME_REQUIRED = "ERR_RUNTIME_CANONICAL_HOSTNAME_REQUIRED"
ERR_RUNTIME_HOST_MISMATCH = "ERR_RUNTIME_HOST_MISMATCH"
ERR_RUNTIME_DATA_DIR_REPO_FALLBACK = "ERR_RUNTIME_DATA_DIR_REPO_FALLBACK"
ERR_RUNTIME_DIR_PREP_FAILED = "ERR_RUNTIME_DIR_PREP_FAILED"
ERR_RUNTIME_LIVE_CONFIRM_REQUIRED = "ERR_RUNTIME_LIVE_CONFIRM_REQUIRED"
PASS_RUNTIME_PREFLIGHT = "PASS_RUNTIME_PREFLIGHT"


@dataclass(frozen=True)
class RuntimeFingerprint:
    hostname: str
    username: str
    runtime_role: str
    canonical_hostname: str
    canonical_host_match: bool
    trusted_scheduled: bool
    mode: str
    intent: str
    dry_run: bool
    repo_root: str
    data_dir: str
    data_dir_source: str
    data_dir_warning: str
    db_osha: str
    db_crm: str
    db_crm_light: str
    timezone: str
    git_sha: str
    timestamp_utc: str


@dataclass(frozen=True)
class RuntimePreflightResult:
    ok: bool
    fingerprint: RuntimeFingerprint
    errors: list[str]
    prepared_dirs: list[str]



def _repo_root_default() -> Path:
    return Path(__file__).resolve().parent



def _safe_tz_name() -> str:
    now = datetime.now().astimezone()
    tz_name = str(getattr(now.tzinfo, "key", "") or now.tzname() or "")
    return tz_name.strip() or "unknown"



def _safe_git_sha(repo_root: Path) -> str:
    try:
        proc = subprocess.run(
            ["git", "-C", str(repo_root), "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            check=False,
            timeout=4,
        )
        if proc.returncode == 0:
            value = (proc.stdout or "").strip()
            return value or "unknown"
    except Exception:
        pass
    return "unknown"



def _normalize_hostname(value: str) -> str:
    return (value or "").strip().lower()



def _effective_runtime_role() -> str:
    raw = (os.getenv("RUNTIME_ROLE") or "").strip().lower()
    if not raw:
        return "dev_client"
    return raw



def _canonical_hostname() -> str:
    return _normalize_hostname(os.getenv("CANONICAL_HOSTNAME") or "")



def collect_runtime_fingerprint(
    *,
    mode: str,
    intent: str,
    dry_run: bool,
    repo_root: Path | None = None,
) -> RuntimeFingerprint:
    root = (repo_root or _repo_root_default()).resolve(strict=False)
    resolution = resolve_data_dir(root)
    hostname = _normalize_hostname(socket.gethostname())
    username = (os.getenv("USERNAME") or os.getenv("USER") or "").strip()
    runtime_role = _effective_runtime_role()
    canonical = _canonical_hostname()
    canonical_match = bool(canonical and hostname == canonical)
    trusted_scheduled = bool(mode == "scheduled" and runtime_role == "canonical_scheduler" and canonical_match)

    return RuntimeFingerprint(
        hostname=hostname,
        username=username,
        runtime_role=runtime_role,
        canonical_hostname=canonical,
        canonical_host_match=canonical_match,
        trusted_scheduled=trusted_scheduled,
        mode=mode,
        intent=intent,
        dry_run=bool(dry_run),
        repo_root=str(root),
        data_dir=str(resolution.effective_path),
        data_dir_source=str(resolution.source),
        data_dir_warning=str(resolution.warning_token or ""),
        db_osha=str((root / "data" / "osha.sqlite").resolve(strict=False)),
        db_crm=str((resolution.effective_path / "crm.sqlite").resolve(strict=False)),
        db_crm_light=str((resolution.effective_path / "crm_light.sqlite").resolve(strict=False)),
        timezone=_safe_tz_name(),
        git_sha=_safe_git_sha(root),
        timestamp_utc=datetime.now(timezone.utc).isoformat(),
    )



def _canonical_host_required(fingerprint: RuntimeFingerprint) -> bool:
    if fingerprint.mode == "scheduled":
        return True
    return bool((fingerprint.intent in {"send", "write"}) and (not fingerprint.dry_run))



def _running_under_unittest() -> bool:
    return "unittest" in sys.modules



def _uses_repo_fallback_data_dir(fingerprint: RuntimeFingerprint) -> bool:
    if fingerprint.data_dir_source != "default":
        return False
    default_path = (Path(fingerprint.repo_root) / "out").resolve(strict=False)
    actual = Path(fingerprint.data_dir).resolve(strict=False)
    return actual == default_path



def run_runtime_preflight(
    *,
    mode: str,
    intent: str,
    dry_run: bool,
    task_log_root: str = "",
    run_summary_root: str = "",
    require_confirm_live_send: bool = False,
    confirm_live_send: bool = False,
) -> RuntimePreflightResult:
    normalized_mode = (mode or "").strip().lower()
    normalized_intent = (intent or "").strip().lower()
    if normalized_mode not in VALID_MODES:
        normalized_mode = "manual"
    if normalized_intent not in VALID_INTENTS:
        normalized_intent = "read"

    fingerprint = collect_runtime_fingerprint(
        mode=normalized_mode,
        intent=normalized_intent,
        dry_run=bool(dry_run),
    )

    errors: list[str] = []
    prepared_dirs: list[str] = []

    if fingerprint.runtime_role not in VALID_RUNTIME_ROLES:
        errors.append(f"{ERR_RUNTIME_ROLE_INVALID} value={fingerprint.runtime_role}")

    if _canonical_host_required(fingerprint):
        if (not fingerprint.canonical_hostname) and (not _running_under_unittest()):
            errors.append(ERR_RUNTIME_CANONICAL_HOSTNAME_REQUIRED)
        elif not fingerprint.canonical_host_match:
            errors.append(
                f"{ERR_RUNTIME_HOST_MISMATCH} expected={fingerprint.canonical_hostname} actual={fingerprint.hostname}"
            )

    if (fingerprint.intent in {"send", "write"}) and (not fingerprint.dry_run) and _uses_repo_fallback_data_dir(fingerprint):
        errors.append(
            f"{ERR_RUNTIME_DATA_DIR_REPO_FALLBACK} data_dir={fingerprint.data_dir} source={fingerprint.data_dir_source}"
        )

    if require_confirm_live_send and (fingerprint.intent == "send") and (not fingerprint.dry_run):
        if (not fingerprint.trusted_scheduled) and (not bool(confirm_live_send)):
            errors.append(ERR_RUNTIME_LIVE_CONFIRM_REQUIRED)

    for raw in [task_log_root, run_summary_root]:
        value = (raw or "").strip()
        if not value:
            continue
        path = Path(value).expanduser()
        try:
            path.mkdir(parents=True, exist_ok=True)
            prepared_dirs.append(str(path.resolve(strict=False)))
        except Exception as exc:
            errors.append(
                f"{ERR_RUNTIME_DIR_PREP_FAILED} path={path} err={type(exc).__name__}"
            )

    return RuntimePreflightResult(
        ok=(len(errors) == 0),
        fingerprint=fingerprint,
        errors=errors,
        prepared_dirs=prepared_dirs,
    )



def render_runtime_lines(result: RuntimePreflightResult) -> list[str]:
    fp = result.fingerprint
    lines = [
        f"RUNTIME_HOSTNAME={fp.hostname}",
        f"RUNTIME_USERNAME={fp.username}",
        f"RUNTIME_ROLE={fp.runtime_role}",
        f"RUNTIME_CANONICAL_HOSTNAME={fp.canonical_hostname or '(unset)'}",
        f"RUNTIME_CANONICAL_HOST_MATCH={1 if fp.canonical_host_match else 0}",
        f"RUNTIME_TRUSTED_SCHEDULED={1 if fp.trusted_scheduled else 0}",
        f"RUNTIME_MODE={fp.mode}",
        f"RUNTIME_INTENT={fp.intent}",
        f"RUNTIME_DRY_RUN={1 if fp.dry_run else 0}",
        f"RUNTIME_REPO_ROOT={fp.repo_root}",
        f"RUNTIME_DATA_DIR={fp.data_dir}",
        f"RUNTIME_DATA_DIR_SOURCE={fp.data_dir_source}",
        f"RUNTIME_DB_OSHA={fp.db_osha}",
        f"RUNTIME_DB_CRM={fp.db_crm}",
        f"RUNTIME_DB_CRM_LIGHT={fp.db_crm_light}",
        f"RUNTIME_TIMEZONE={fp.timezone}",
        f"RUNTIME_GIT_SHA={fp.git_sha}",
        f"RUNTIME_TIMESTAMP_UTC={fp.timestamp_utc}",
        f"MFO_RUNTIME_MODE={fp.mode}",
        f"MFO_TRUSTED_SCHEDULED={1 if fp.trusted_scheduled else 0}",
    ]
    if fp.data_dir_warning:
        lines.append(fp.data_dir_warning)
    for path in sorted(set(result.prepared_dirs)):
        lines.append(f"RUNTIME_PREPARED_DIR={path}")
    if result.ok:
        lines.append(PASS_RUNTIME_PREFLIGHT)
    else:
        lines.extend(result.errors)
    return lines



def runtime_context_dict(mode: str = "manual", intent: str = "read", dry_run: bool = False) -> dict[str, Any]:
    fp = collect_runtime_fingerprint(mode=mode, intent=intent, dry_run=dry_run)
    payload = asdict(fp)
    payload["runtime_role_valid"] = fp.runtime_role in VALID_RUNTIME_ROLES
    return payload



def _cmd_preflight(args: argparse.Namespace) -> int:
    result = run_runtime_preflight(
        mode=str(args.mode or "manual"),
        intent=str(args.intent or "read"),
        dry_run=bool(args.dry_run),
        task_log_root=str(args.task_log_root or ""),
        run_summary_root=str(args.run_summary_root or ""),
        require_confirm_live_send=bool(args.require_confirm_live_send),
        confirm_live_send=bool(args.confirm_live_send),
    )
    for line in render_runtime_lines(result):
        print(line)
    return 0 if result.ok else 1



def _cmd_print_context(args: argparse.Namespace) -> int:
    payload = runtime_context_dict(
        mode=str(args.mode or "manual"),
        intent=str(args.intent or "read"),
        dry_run=bool(args.dry_run),
    )
    print(json.dumps(payload, sort_keys=True))
    return 0



def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="Runtime guard and fingerprint helper.")
    sub = ap.add_subparsers(dest="cmd", required=True)

    pre = sub.add_parser("preflight", help="Validate runtime guardrails and emit fingerprint lines.")
    pre.add_argument("--mode", default="manual", choices=sorted(VALID_MODES))
    pre.add_argument("--intent", default="read", choices=sorted(VALID_INTENTS))
    pre.add_argument("--dry-run", action="store_true")
    pre.add_argument("--task-log-root", default="")
    pre.add_argument("--run-summary-root", default="")
    pre.add_argument("--require-confirm-live-send", action="store_true")
    pre.add_argument("--confirm-live-send", action="store_true")
    pre.set_defaults(func=_cmd_preflight)

    ctx = sub.add_parser("print-context", help="Print runtime fingerprint context as JSON.")
    ctx.add_argument("--mode", default="manual", choices=sorted(VALID_MODES))
    ctx.add_argument("--intent", default="read", choices=sorted(VALID_INTENTS))
    ctx.add_argument("--dry-run", action="store_true")
    ctx.set_defaults(func=_cmd_print_context)
    return ap



def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())

