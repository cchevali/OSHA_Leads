from __future__ import annotations

import argparse
import hashlib
import json
import os
import socket
import subprocess
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

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
ERR_RUNTIME_DB_OSHA_LEGACY_PRESENT = "ERR_RUNTIME_DB_OSHA_LEGACY_PRESENT"
ERR_RUNTIME_DB_CRM_LEGACY_PRESENT = "ERR_RUNTIME_DB_CRM_LEGACY_PRESENT"
ERR_RUNTIME_DB_CRM_LIGHT_LEGACY_PRESENT = "ERR_RUNTIME_DB_CRM_LIGHT_LEGACY_PRESENT"
ERR_RUNTIME_DB_CRM_SPLIT = "ERR_RUNTIME_DB_CRM_SPLIT"
ERR_RUNTIME_DB_CRM_LIGHT_SPLIT = "ERR_RUNTIME_DB_CRM_LIGHT_SPLIT"
PASS_RUNTIME_PREFLIGHT = "PASS_RUNTIME_PREFLIGHT"
DEFAULT_RUNTIME_LOCK_STALE_SECONDS = 6 * 60 * 60


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
    db_crm_legacy: str
    db_crm_legacy_exists: bool
    db_crm_split_conflict: bool
    db_crm_split_reason: str
    db_crm_light: str
    db_crm_light_legacy: str
    db_crm_light_legacy_exists: bool
    db_crm_light_split_conflict: bool
    db_crm_light_split_reason: str
    timezone: str
    git_sha: str
    timestamp_utc: str


@dataclass(frozen=True)
class RuntimePreflightResult:
    ok: bool
    fingerprint: RuntimeFingerprint
    errors: list[str]
    prepared_dirs: list[str]


@dataclass
class RuntimeLockHandle:
    acquired: bool
    path: str
    name: str
    token: str
    metadata: dict[str, Any]
    reason: str = ""
    stale_reclaimed: bool = False

    def release(self) -> None:
        if not self.acquired or not self.path:
            return
        lock_path = Path(self.path)
        try:
            current = _read_runtime_lock_metadata(lock_path)
            if current and str(current.get("token") or "").strip() != str(self.token or "").strip():
                return
        except Exception:
            pass
        try:
            lock_path.unlink(missing_ok=True)
        except Exception:
            pass



def _repo_root_default() -> Path:
    return Path(__file__).resolve().parent


def _legacy_repo_osha_db(root: Path) -> Path:
    return (root / "data" / "osha.sqlite").resolve(strict=False)


def _legacy_repo_crm_db(root: Path) -> Path:
    return (root / "out" / "crm.sqlite").resolve(strict=False)


def _legacy_repo_crm_light_db(root: Path) -> Path:
    return (root / "out" / "crm_light.sqlite").resolve(strict=False)


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


def _detect_legacy_db_state(root: Path, effective_db: Path, *, family: str) -> dict[str, Any]:
    legacy_lookup = {
        "osha": _legacy_repo_osha_db,
        "crm": _legacy_repo_crm_db,
        "crm_light": _legacy_repo_crm_light_db,
    }
    resolver = legacy_lookup[str(family)]
    legacy = resolver(root)
    canonical = Path(effective_db).resolve(strict=False)
    result: dict[str, Any] = {
        "legacy_path": str(legacy),
        "legacy_exists": legacy.exists(),
        "conflict": False,
        "reason": "",
    }
    if legacy == canonical or (not legacy.exists()):
        return result
    if not canonical.exists():
        result["conflict"] = True
        result["reason"] = "legacy_present_canonical_missing"
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
    split = _detect_legacy_db_state(root, canonical, family="osha")
    if bool(split.get("legacy_exists")):
        return (
            f"{ERR_RUNTIME_DB_OSHA_LEGACY_PRESENT} db_osha={canonical} "
            f"legacy_db={split.get('legacy_path')}"
        )
    if bool(split.get("conflict")):
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


def _safe_runtime_lock_name(value: str) -> str:
    raw = str(value or "").strip().lower()
    cleaned = "".join(ch if ch.isalnum() or ch in "._-" else "_" for ch in raw).strip("._-")
    return cleaned or "runtime_lock"


def _runtime_lock_dir(repo_root: Path | None = None) -> Path:
    root = (repo_root or _repo_root_default()).resolve(strict=False)
    return (resolve_data_dir(root).effective_path / "runtime" / "locks").resolve(strict=False)


def runtime_lock_path(lock_name: str, repo_root: Path | None = None) -> Path:
    return _runtime_lock_dir(repo_root) / f"{_safe_runtime_lock_name(lock_name)}.json"


def _read_runtime_lock_metadata(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _runtime_pid_exists(pid: int) -> bool:
    if int(pid) <= 0:
        return False
    if os.name == "nt":
        try:
            import ctypes

            process = ctypes.windll.kernel32.OpenProcess(0x1000, False, int(pid))
            if not process:
                return False
            ctypes.windll.kernel32.CloseHandle(process)
            return True
        except Exception:
            return False
    try:
        os.kill(int(pid), 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True


def _parse_runtime_lock_timestamp(value: str | None) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text)
    except Exception:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _runtime_lock_is_stale(
    path: Path,
    metadata: dict[str, Any],
    *,
    stale_after_seconds: int = DEFAULT_RUNTIME_LOCK_STALE_SECONDS,
) -> bool:
    holder_host = _normalize_hostname(str(metadata.get("hostname") or ""))
    current_host = _normalize_hostname(socket.gethostname())
    try:
        holder_pid = int(metadata.get("pid") or 0)
    except Exception:
        holder_pid = 0
    if holder_host and holder_host == current_host and holder_pid > 0 and (not _runtime_pid_exists(holder_pid)):
        return True

    acquired_at = _parse_runtime_lock_timestamp(str(metadata.get("acquired_at_utc") or ""))
    if acquired_at is not None:
        age = datetime.now(timezone.utc) - acquired_at
        if age >= timedelta(seconds=max(60, int(stale_after_seconds))):
            return True

    try:
        mtime_utc = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
    except Exception:
        mtime_utc = None
    if mtime_utc is not None:
        if datetime.now(timezone.utc) - mtime_utc >= timedelta(seconds=max(60, int(stale_after_seconds))):
            return True
    return False


def acquire_runtime_lock(
    lock_name: str,
    *,
    repo_root: Path | None = None,
    stale_after_seconds: int = DEFAULT_RUNTIME_LOCK_STALE_SECONDS,
    metadata: dict[str, Any] | None = None,
) -> RuntimeLockHandle:
    root = (repo_root or _repo_root_default()).resolve(strict=False)
    safe_name = _safe_runtime_lock_name(lock_name)
    lock_path = runtime_lock_path(safe_name, repo_root=root)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    token = uuid4().hex
    payload: dict[str, Any] = {
        "name": safe_name,
        "token": token,
        "pid": os.getpid(),
        "hostname": _normalize_hostname(socket.gethostname()),
        "acquired_at_utc": datetime.now(timezone.utc).isoformat(),
        "repo_root": str(root),
    }
    for key, value in dict(metadata or {}).items():
        if key not in payload:
            payload[str(key)] = value

    stale_reclaimed = False
    while True:
        try:
            fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            existing = _read_runtime_lock_metadata(lock_path)
            if _runtime_lock_is_stale(
                lock_path,
                existing,
                stale_after_seconds=stale_after_seconds,
            ):
                try:
                    lock_path.unlink()
                except FileNotFoundError:
                    continue
                except Exception:
                    return RuntimeLockHandle(
                        acquired=False,
                        path=str(lock_path),
                        name=safe_name,
                        token="",
                        metadata=existing,
                        reason="stale_reclaim_failed",
                        stale_reclaimed=stale_reclaimed,
                    )
                stale_reclaimed = True
                continue
            return RuntimeLockHandle(
                acquired=False,
                path=str(lock_path),
                name=safe_name,
                token="",
                metadata=existing,
                reason="locked",
                stale_reclaimed=stale_reclaimed,
            )
        except Exception as exc:
            return RuntimeLockHandle(
                acquired=False,
                path=str(lock_path),
                name=safe_name,
                token="",
                metadata={},
                reason=f"lock_create_failed:{type(exc).__name__}",
                stale_reclaimed=stale_reclaimed,
            )

        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(payload, handle, sort_keys=True)
                handle.write("\n")
        except Exception:
            try:
                lock_path.unlink(missing_ok=True)
            except Exception:
                pass
            raise
        return RuntimeLockHandle(
            acquired=True,
            path=str(lock_path),
            name=safe_name,
            token=token,
            metadata=payload,
            stale_reclaimed=stale_reclaimed,
        )



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
    legacy_osha = _detect_legacy_db_state(root, osha_db.effective_path, family="osha")
    legacy_crm = _detect_legacy_db_state(root, resolution.effective_path / "crm.sqlite", family="crm")
    legacy_crm_light = _detect_legacy_db_state(
        root,
        resolution.effective_path / "crm_light.sqlite",
        family="crm_light",
    )
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
        db_crm_legacy=str(legacy_crm.get("legacy_path") or ""),
        db_crm_legacy_exists=bool(legacy_crm.get("legacy_exists")),
        db_crm_split_conflict=bool(legacy_crm.get("conflict")),
        db_crm_split_reason=str(legacy_crm.get("reason") or ""),
        db_crm_light=str((resolution.effective_path / "crm_light.sqlite").resolve(strict=False)),
        db_crm_light_legacy=str(legacy_crm_light.get("legacy_path") or ""),
        db_crm_light_legacy_exists=bool(legacy_crm_light.get("legacy_exists")),
        db_crm_light_split_conflict=bool(legacy_crm_light.get("conflict")),
        db_crm_light_split_reason=str(legacy_crm_light.get("reason") or ""),
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


def _osha_db_outside_data_dir(fingerprint: RuntimeFingerprint) -> bool:
    data_dir = Path(fingerprint.data_dir).resolve(strict=False)
    db_path = Path(fingerprint.db_osha).resolve(strict=False)
    try:
        db_path.relative_to(data_dir)
        return False
    except ValueError:
        return True


def _live_send_or_write(fingerprint: RuntimeFingerprint) -> bool:
    return bool((fingerprint.intent in {"send", "write"}) and (not fingerprint.dry_run))



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

    if _live_send_or_write(fingerprint) and _uses_repo_fallback_data_dir(fingerprint):
        errors.append(
            f"{ERR_RUNTIME_DATA_DIR_REPO_FALLBACK} data_dir={fingerprint.data_dir} source={fingerprint.data_dir_source}"
        )
    if _live_send_or_write(fingerprint) and _osha_db_outside_data_dir(fingerprint):
        errors.append(
            f"{ERR_RUNTIME_DB_OSHA_OUTSIDE_DATA_DIR} db_osha={fingerprint.db_osha} data_dir={fingerprint.data_dir}"
        )
    if _live_send_or_write(fingerprint) and fingerprint.db_osha_legacy_exists:
        errors.append(
            f"{ERR_RUNTIME_DB_OSHA_LEGACY_PRESENT} db_osha={fingerprint.db_osha} "
            f"legacy_db={fingerprint.db_osha_legacy}"
        )
    if _live_send_or_write(fingerprint) and fingerprint.db_osha_split_conflict:
        errors.append(
            f"{ERR_RUNTIME_DB_OSHA_SPLIT} db_osha={fingerprint.db_osha} "
            f"legacy_db={fingerprint.db_osha_legacy} reason={fingerprint.db_osha_split_reason}"
        )
    if _live_send_or_write(fingerprint) and fingerprint.db_crm_legacy_exists:
        errors.append(
            f"{ERR_RUNTIME_DB_CRM_LEGACY_PRESENT} db_crm={fingerprint.db_crm} "
            f"legacy_db={fingerprint.db_crm_legacy}"
        )
    if _live_send_or_write(fingerprint) and fingerprint.db_crm_split_conflict:
        errors.append(
            f"{ERR_RUNTIME_DB_CRM_SPLIT} db_crm={fingerprint.db_crm} "
            f"legacy_db={fingerprint.db_crm_legacy} reason={fingerprint.db_crm_split_reason}"
        )
    if _live_send_or_write(fingerprint) and fingerprint.db_crm_light_legacy_exists:
        errors.append(
            f"{ERR_RUNTIME_DB_CRM_LIGHT_LEGACY_PRESENT} db_crm_light={fingerprint.db_crm_light} "
            f"legacy_db={fingerprint.db_crm_light_legacy}"
        )
    if _live_send_or_write(fingerprint) and fingerprint.db_crm_light_split_conflict:
        errors.append(
            f"{ERR_RUNTIME_DB_CRM_LIGHT_SPLIT} db_crm_light={fingerprint.db_crm_light} "
            f"legacy_db={fingerprint.db_crm_light_legacy} reason={fingerprint.db_crm_light_split_reason}"
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
        f"RUNTIME_DB_CRM_LEGACY={fp.db_crm_legacy}",
        f"RUNTIME_DB_CRM_LEGACY_EXISTS={1 if fp.db_crm_legacy_exists else 0}",
        f"RUNTIME_DB_CRM_SPLIT_CONFLICT={1 if fp.db_crm_split_conflict else 0}",
        f"RUNTIME_DB_CRM_LIGHT={fp.db_crm_light}",
        f"RUNTIME_DB_CRM_LIGHT_LEGACY={fp.db_crm_light_legacy}",
        f"RUNTIME_DB_CRM_LIGHT_LEGACY_EXISTS={1 if fp.db_crm_light_legacy_exists else 0}",
        f"RUNTIME_DB_CRM_LIGHT_SPLIT_CONFLICT={1 if fp.db_crm_light_split_conflict else 0}",
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
    if fp.db_crm_split_reason:
        lines.append(f"RUNTIME_DB_CRM_SPLIT_REASON={fp.db_crm_split_reason}")
    if fp.db_crm_light_split_reason:
        lines.append(f"RUNTIME_DB_CRM_LIGHT_SPLIT_REASON={fp.db_crm_light_split_reason}")
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
