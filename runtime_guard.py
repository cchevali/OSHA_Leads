from __future__ import annotations

import argparse
import hashlib
import json
import os
import socket
import subprocess
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from runtime_data_dir import resolve_data_dir, resolve_osha_db_path

VALID_RUNTIME_ROLES = {"canonical_scheduler", "dev_client"}
VALID_MODES = {"scheduled", "manual"}
VALID_INTENTS = {"send", "write", "read"}

ERR_RUNTIME_ROLE_INVALID = "ERR_RUNTIME_ROLE_INVALID"
ERR_RUNTIME_CANONICAL_HOSTNAME_REQUIRED = "ERR_RUNTIME_CANONICAL_HOSTNAME_REQUIRED"
ERR_RUNTIME_HOST_MISMATCH = "ERR_RUNTIME_HOST_MISMATCH"
ERR_RUNTIME_DATA_DIR_REPO_FALLBACK = "ERR_RUNTIME_DATA_DIR_REPO_FALLBACK"
ERR_RUNTIME_DIR_PREP_FAILED = "ERR_RUNTIME_DIR_PREP_FAILED"
ERR_RUNTIME_LIVE_CONFIRM_REQUIRED = "ERR_RUNTIME_LIVE_CONFIRM_REQUIRED"
ERR_RUNTIME_DB_OSHA_OUTSIDE_DATA_DIR = "ERR_RUNTIME_DB_OSHA_OUTSIDE_DATA_DIR"
ERR_RUNTIME_DB_OSHA_SPLIT = "ERR_RUNTIME_DB_OSHA_SPLIT"
WARN_RUNTIME_DB_OSHA_SPLIT_IGNORED = "WARN_RUNTIME_DB_OSHA_SPLIT_IGNORED"
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
    db_osha_source: str
    db_osha_warning: str
    db_osha_legacy: str
    db_osha_legacy_exists: bool
    db_osha_split_conflict: bool
    db_osha_split_reason: str
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


def _legacy_repo_osha_db(root: Path) -> Path:
    return (root / "data" / "osha.sqlite").resolve(strict=False)


def _sha256_file(path: Path) -> str:
    if (not path.exists()) or (not path.is_file()):
        return ""
    h = hashlib.sha256()
    with path.open("rb") as fh:
        while True:
            block = fh.read(1024 * 1024)
            if not block:
                break
            h.update(block)
    return h.hexdigest()


def _detect_osha_db_split(root: Path, effective_db: Path) -> dict[str, Any]:
    legacy = _legacy_repo_osha_db(root)
    canonical = Path(effective_db).resolve(strict=False)
    result: dict[str, Any] = {
        "legacy_path": str(legacy),
        "legacy_exists": legacy.exists(),
        "conflict": False,
        "reason": "",
    }
    if legacy == canonical or (not legacy.exists()) or (not canonical.exists()):
        return result
    legacy_hash = _sha256_file(legacy)
    canonical_hash = _sha256_file(canonical)
    if legacy_hash and canonical_hash and legacy_hash != canonical_hash:
        result["conflict"] = True
        result["reason"] = "hash_mismatch"
    return result


def validate_live_osha_db_path(selected_db: str | Path, repo_root: Path | None = None) -> str:
    if _running_under_unittest():
        return ""
    root = (repo_root or _repo_root_default()).resolve(strict=False)
    resolution = resolve_osha_db_path(root)
    canonical = resolution.effective_path.resolve(strict=False)
    selected = Path(selected_db).expanduser().resolve(strict=False)
    if selected != canonical:
        return (
            f"{ERR_RUNTIME_DB_OSHA_OUTSIDE_DATA_DIR} "
            f"selected_db={selected} canonical_db={canonical}"
        )
    split = _detect_osha_db_split(root, canonical)
    if bool(split.get("conflict")):
        hostname = _normalize_hostname(socket.gethostname())
        runtime_role = _effective_runtime_role()
        canonical_host = _canonical_hostname()
        canonical_match = bool(canonical_host and hostname == canonical_host)
        if (
            runtime_role == "canonical_scheduler"
            and canonical_match
            and str(resolution.source) == "data_dir"
        ):
            return ""
        return (
            f"{ERR_RUNTIME_DB_OSHA_SPLIT} db_osha={canonical} "
            f"legacy_db={split.get('legacy_path')} reason={split.get('reason') or 'hash_mismatch'}"
        )
    return ""



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
    osha_db = resolve_osha_db_path(root)
    legacy_osha = _detect_osha_db_split(root, osha_db.effective_path)
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
        db_osha=str(osha_db.effective_path),
        db_osha_source=str(osha_db.source),
        db_osha_warning=str(osha_db.warning_token or ""),
        db_osha_legacy=str(legacy_osha.get("legacy_path") or ""),
        db_osha_legacy_exists=bool(legacy_osha.get("legacy_exists")),
        db_osha_split_conflict=bool(legacy_osha.get("conflict")),
        db_osha_split_reason=str(legacy_osha.get("reason") or ""),
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



def _allow_canonical_split_override(fingerprint: RuntimeFingerprint) -> bool:
    return bool(
        fingerprint.runtime_role == "canonical_scheduler"
        and fingerprint.canonical_host_match
        and fingerprint.db_osha_source == "data_dir"
        and (not _uses_repo_fallback_data_dir(fingerprint))
        and (not _osha_db_outside_data_dir(fingerprint))
    )


def _uses_repo_fallback_data_dir(fingerprint: RuntimeFingerprint) -> bool:
    if fingerprint.data_dir_source != "default":
        return False
    default_path = (Path(fingerprint.repo_root) / "out").resolve(strict=False)
    actual = Path(fingerprint.data_dir).resolve(strict=False)
    return actual == default_path


def _osha_db_outside_data_dir(fingerprint: RuntimeFingerprint) -> bool:
    data_dir = Path(fingerprint.data_dir).resolve(strict=False)
    db_path = Path(fingerprint.db_osha).resolve(strict=False)
    try:
        db_path.relative_to(data_dir)
        return False
    except ValueError:
        return True



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
        if not fingerprint.canonical_hostname:
            if not _running_under_unittest():
                errors.append(ERR_RUNTIME_CANONICAL_HOSTNAME_REQUIRED)
        elif not fingerprint.canonical_host_match:
            errors.append(
                f"{ERR_RUNTIME_HOST_MISMATCH} expected={fingerprint.canonical_hostname} actual={fingerprint.hostname}"
            )

    if (fingerprint.intent in {"send", "write"}) and (not fingerprint.dry_run) and _uses_repo_fallback_data_dir(fingerprint):
        errors.append(
            f"{ERR_RUNTIME_DATA_DIR_REPO_FALLBACK} data_dir={fingerprint.data_dir} source={fingerprint.data_dir_source}"
        )
    if (fingerprint.intent in {"send", "write"}) and (not fingerprint.dry_run) and _osha_db_outside_data_dir(fingerprint):
        errors.append(
            f"{ERR_RUNTIME_DB_OSHA_OUTSIDE_DATA_DIR} db_osha={fingerprint.db_osha} data_dir={fingerprint.data_dir}"
        )
    if (
        (fingerprint.intent in {"send", "write"})
        and (not fingerprint.dry_run)
        and fingerprint.db_osha_split_conflict
        and (not _allow_canonical_split_override(fingerprint))
    ):
        errors.append(
            f"{ERR_RUNTIME_DB_OSHA_SPLIT} db_osha={fingerprint.db_osha} "
            f"legacy_db={fingerprint.db_osha_legacy} reason={fingerprint.db_osha_split_reason}"
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
        f"RUNTIME_DB_OSHA_SOURCE={fp.db_osha_source}",
        f"RUNTIME_DB_OSHA_LEGACY={fp.db_osha_legacy}",
        f"RUNTIME_DB_OSHA_LEGACY_EXISTS={1 if fp.db_osha_legacy_exists else 0}",
        f"RUNTIME_DB_OSHA_SPLIT_CONFLICT={1 if fp.db_osha_split_conflict else 0}",
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
    if fp.db_osha_warning:
        lines.append(fp.db_osha_warning)
    if fp.db_osha_split_reason:
        lines.append(f"RUNTIME_DB_OSHA_SPLIT_REASON={fp.db_osha_split_reason}")
    if fp.db_osha_split_conflict and _allow_canonical_split_override(fp):
        lines.append(
            f"{WARN_RUNTIME_DB_OSHA_SPLIT_IGNORED} "
            f"db_osha={fp.db_osha} legacy_db={fp.db_osha_legacy} reason={fp.db_osha_split_reason or 'hash_mismatch'}"
        )
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
