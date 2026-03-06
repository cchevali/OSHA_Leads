from __future__ import annotations

import argparse
import hashlib
import os
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import run_trial_admin
from runtime_data_dir import resolve_data_dir
from runtime_guard import render_runtime_lines, run_runtime_preflight


ERR_RUNTIME_STATE_MIGRATE_CONFIG = "ERR_RUNTIME_STATE_MIGRATE_CONFIG"
ERR_RUNTIME_STATE_MIGRATE_APPLY = "ERR_RUNTIME_STATE_MIGRATE_APPLY"
PASS_RUNTIME_STATE_MIGRATE_PRINT_CONFIG = "PASS_RUNTIME_STATE_MIGRATE_PRINT_CONFIG"
PASS_RUNTIME_STATE_MIGRATE_DOCTOR = "PASS_RUNTIME_STATE_MIGRATE_DOCTOR"
PASS_RUNTIME_STATE_MIGRATE_COMPLETE = "PASS_RUNTIME_STATE_MIGRATE_COMPLETE"


def _emit(key: str, value: str | int) -> None:
    print(f"{key}={value}")


def _error(detail: str) -> int:
    print(f"{ERR_RUNTIME_STATE_MIGRATE_CONFIG} {detail}", file=sys.stderr)
    return 2


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _file_hash(path: Path) -> str:
    if not path.exists() or not path.is_file():
        return ""
    h = hashlib.sha256()
    with path.open("rb") as fh:
        while True:
            block = fh.read(1024 * 1024)
            if not block:
                break
            h.update(block)
    return h.hexdigest()


def _copy_with_backup(
    *,
    src: Path,
    dst: Path,
    backup_root: Path,
    allow_overwrite: bool,
) -> tuple[bool, str]:
    if not src.exists():
        return False, "source_missing"
    dst.parent.mkdir(parents=True, exist_ok=True)
    if dst.exists():
        if not allow_overwrite:
            return False, "target_exists_no_overwrite"
        backup_path = (backup_root / dst.name).resolve(strict=False)
        backup_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(str(dst), str(backup_path))
    shutil.copy2(str(src), str(dst))
    return True, "copied"


def _resolve_paths(repo_root: Path) -> dict[str, Path]:
    data_dir = resolve_data_dir(repo_root).effective_path
    return {
        "repo_root": repo_root,
        "data_dir": data_dir,
        "repo_osha_db": (repo_root / "data" / "osha.sqlite").resolve(strict=False),
        "legacy_repo_crm_light_db": (repo_root / "out" / "crm_light.sqlite").resolve(strict=False),
        "canonical_osha_db": (data_dir / "osha.sqlite").resolve(strict=False),
        "canonical_crm_db": (data_dir / "crm.sqlite").resolve(strict=False),
        "canonical_crm_light_db": (data_dir / "crm_light.sqlite").resolve(strict=False),
        "backup_root": (data_dir / "out" / "backups").resolve(strict=False),
    }


def _print_paths(paths: dict[str, Path]) -> None:
    _emit("RUNTIME_STATE_MIGRATE_REPO_ROOT", str(paths["repo_root"]))
    _emit("RUNTIME_STATE_MIGRATE_DATA_DIR", str(paths["data_dir"]))
    _emit("RUNTIME_STATE_MIGRATE_REPO_OSHA_DB", str(paths["repo_osha_db"]))
    _emit("RUNTIME_STATE_MIGRATE_LEGACY_REPO_CRM_LIGHT_DB", str(paths["legacy_repo_crm_light_db"]))
    _emit("RUNTIME_STATE_MIGRATE_CANONICAL_OSHA_DB", str(paths["canonical_osha_db"]))
    _emit("RUNTIME_STATE_MIGRATE_CANONICAL_CRM_DB", str(paths["canonical_crm_db"]))
    _emit("RUNTIME_STATE_MIGRATE_CANONICAL_CRM_LIGHT_DB", str(paths["canonical_crm_light_db"]))
    _emit("RUNTIME_STATE_MIGRATE_BACKUP_ROOT", str(paths["backup_root"]))


def _exists_flag(path: Path) -> str:
    return "YES" if path.exists() else "NO"


def _doctor(paths: dict[str, Path]) -> int:
    _print_paths(paths)
    _emit("RUNTIME_STATE_MIGRATE_EXISTS_REPO_OSHA_DB", _exists_flag(paths["repo_osha_db"]))
    _emit("RUNTIME_STATE_MIGRATE_EXISTS_CANONICAL_OSHA_DB", _exists_flag(paths["canonical_osha_db"]))
    _emit("RUNTIME_STATE_MIGRATE_EXISTS_CANONICAL_CRM_DB", _exists_flag(paths["canonical_crm_db"]))
    _emit("RUNTIME_STATE_MIGRATE_EXISTS_CANONICAL_CRM_LIGHT_DB", _exists_flag(paths["canonical_crm_light_db"]))
    _emit(
        "RUNTIME_STATE_MIGRATE_EXISTS_LEGACY_REPO_CRM_LIGHT_DB",
        _exists_flag(paths["legacy_repo_crm_light_db"]),
    )
    print(f"{PASS_RUNTIME_STATE_MIGRATE_DOCTOR} status=OK")
    print(f"{PASS_RUNTIME_STATE_MIGRATE_COMPLETE} status=DOCTOR")
    return 0


def _plan_actions(paths: dict[str, Path]) -> list[dict[str, Any]]:
    actions: list[dict[str, Any]] = []
    repo_osha = paths["repo_osha_db"]
    canonical_osha = paths["canonical_osha_db"]
    if repo_osha.exists():
        if not canonical_osha.exists():
            actions.append({"name": "copy_repo_osha_to_canonical", "src": repo_osha, "dst": canonical_osha})
        else:
            src_hash = _file_hash(repo_osha)
            dst_hash = _file_hash(canonical_osha)
            if src_hash and dst_hash and src_hash != dst_hash:
                actions.append(
                    {
                        "name": "conflict_repo_osha_vs_canonical",
                        "src": repo_osha,
                        "dst": canonical_osha,
                        "src_hash": src_hash,
                        "dst_hash": dst_hash,
                    }
                )

    legacy = paths["legacy_repo_crm_light_db"]
    canonical_trial = paths["canonical_crm_light_db"]
    if legacy.exists():
        if not canonical_trial.exists():
            actions.append({"name": "copy_legacy_trial_to_canonical", "src": legacy, "dst": canonical_trial})
        else:
            legacy_hash = _file_hash(legacy)
            canonical_hash = _file_hash(canonical_trial)
            if legacy_hash and canonical_hash and legacy_hash != canonical_hash:
                actions.append({"name": "reconcile_legacy_trial_into_canonical", "src": legacy, "dst": canonical_trial})
        actions.append({"name": "archive_legacy_trial_db", "src": legacy})
    return actions


def _apply_actions(paths: dict[str, Path], actions: list[dict[str, Any]]) -> tuple[int, int]:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    backup_dir = (paths["backup_root"] / f"runtime_state_migrate_{timestamp}").resolve(strict=False)
    backup_dir.mkdir(parents=True, exist_ok=True)
    copied = 0
    archived = 0

    for action in actions:
        name = str(action.get("name") or "")
        if name == "conflict_repo_osha_vs_canonical":
            print(
                f"{ERR_RUNTIME_STATE_MIGRATE_APPLY} action={name} src={action.get('src')} dst={action.get('dst')} "
                f"src_hash={action.get('src_hash')} dst_hash={action.get('dst_hash')}",
                file=sys.stderr,
            )
            return copied, archived

        if name in {"copy_repo_osha_to_canonical", "copy_legacy_trial_to_canonical"}:
            src = Path(action["src"]).resolve(strict=False)
            dst = Path(action["dst"]).resolve(strict=False)
            ok, detail = _copy_with_backup(src=src, dst=dst, backup_root=backup_dir, allow_overwrite=True)
            _emit("RUNTIME_STATE_MIGRATE_ACTION", f"name={name} status={'OK' if ok else 'ERR'} detail={detail}")
            if not ok:
                print(f"{ERR_RUNTIME_STATE_MIGRATE_APPLY} action={name} detail={detail}", file=sys.stderr)
                return copied, archived
            copied += 1
            continue

        if name == "reconcile_legacy_trial_into_canonical":
            src = Path(action["src"]).resolve(strict=False)
            dst = Path(action["dst"]).resolve(strict=False)
            try:
                rc = run_trial_admin.reconcile_ledgers(
                    source_crm_db_path=src,
                    target_crm_db_path=dst,
                    scope="all",
                    subscriber_keys=[],
                    apply=True,
                    trial_state_merge="source",
                    emit_tokens=True,
                )
            except Exception as exc:
                print(
                    f"{ERR_RUNTIME_STATE_MIGRATE_APPLY} action={name} detail={exc.__class__.__name__}:{exc}",
                    file=sys.stderr,
                )
                return copied, archived
            if int(rc) != 0:
                print(f"{ERR_RUNTIME_STATE_MIGRATE_APPLY} action={name} detail=reconcile_exit_code_{rc}", file=sys.stderr)
                return copied, archived
            _emit("RUNTIME_STATE_MIGRATE_ACTION", f"name={name} status=OK detail=reconciled")
            copied += 1
            continue

        if name == "archive_legacy_trial_db":
            src = Path(action["src"]).resolve(strict=False)
            if src.exists():
                archived_path = (backup_dir / "legacy_repo_crm_light.sqlite").resolve(strict=False)
                shutil.copy2(str(src), str(archived_path))
                src.unlink()
                archived += 1
                _emit("RUNTIME_STATE_MIGRATE_ACTION", f"name={name} status=OK archived_path={archived_path}")
            else:
                _emit("RUNTIME_STATE_MIGRATE_ACTION", f"name={name} status=SKIP detail=source_missing")
            continue

        _emit("RUNTIME_STATE_MIGRATE_ACTION", f"name={name} status=SKIP detail=unknown_action")

    _emit("RUNTIME_STATE_MIGRATE_BACKUP_DIR", str(backup_dir))
    return copied, archived


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="Migrate live runtime state into canonical DATA_DIR layout.")
    ap.add_argument("--print-config", action="store_true", help="Print resolved migration paths and exit.")
    ap.add_argument("--doctor", action="store_true", help="Run migration readiness checks and exit.")
    ap.add_argument("--dry-run", action="store_true", help="Plan migration actions without writing.")
    ap.add_argument("--apply", action="store_true", help="Apply migration actions.")
    return ap


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    selected = int(bool(args.print_config)) + int(bool(args.doctor)) + int(bool(args.dry_run)) + int(bool(args.apply))
    if selected != 1:
        return _error("choose_exactly_one_mode")

    repo_root = _repo_root()
    paths = _resolve_paths(repo_root)
    if not paths["data_dir"].is_absolute():
        return _error(f"data_dir_not_absolute path={paths['data_dir']}")

    runtime_mode = "manual"
    preflight = run_runtime_preflight(
        mode=runtime_mode,
        intent="write",
        dry_run=not bool(args.apply),
        task_log_root=str(os.getenv("TASK_LOG_ROOT") or ""),
        run_summary_root=str(os.getenv("RUN_SUMMARY_ROOT") or ""),
    )
    for line in render_runtime_lines(preflight):
        print(line)
    if not preflight.ok:
        return 2

    if args.print_config:
        _print_paths(paths)
        print(f"{PASS_RUNTIME_STATE_MIGRATE_PRINT_CONFIG} status=OK")
        print(f"{PASS_RUNTIME_STATE_MIGRATE_COMPLETE} status=PRINT_CONFIG")
        return 0

    if args.doctor:
        return _doctor(paths)

    actions = _plan_actions(paths)
    _print_paths(paths)
    _emit("RUNTIME_STATE_MIGRATE_ACTION_COUNT", len(actions))
    for action in actions:
        name = str(action.get("name") or "")
        _emit("RUNTIME_STATE_MIGRATE_ACTION_PLAN", name)

    if args.dry_run:
        print(f"{PASS_RUNTIME_STATE_MIGRATE_COMPLETE} status=DRY_RUN")
        return 0

    copied, archived = _apply_actions(paths, actions)
    _emit("RUNTIME_STATE_MIGRATE_COPIED_COUNT", copied)
    _emit("RUNTIME_STATE_MIGRATE_ARCHIVED_COUNT", archived)

    # Fail if any conflict action remained unresolved.
    for action in actions:
        if str(action.get("name") or "") == "conflict_repo_osha_vs_canonical":
            return 2

    print(f"{PASS_RUNTIME_STATE_MIGRATE_COMPLETE} status=OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
