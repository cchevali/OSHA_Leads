from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from shutil import copyfile


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from outreach import crm_store
from outreach import ops_report


SCHEMA_VERSION = "v1"
NO_WRITE_PATH_SENTINEL = "(no-write)"
ERR_OPS_SNAPSHOT_CRM_REQUIRED = "ERR_OPS_SNAPSHOT_CRM_REQUIRED"
ERR_OPS_SNAPSHOT_CRM_SCHEMA = "ERR_OPS_SNAPSHOT_CRM_SCHEMA"


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def _parse_ts(raw: str) -> datetime | None:
    text = str(raw or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except Exception:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _output_paths(data_dir: Path, now_utc: datetime) -> tuple[Path, Path]:
    root = data_dir / "outreach" / "ops_snapshots"
    day_dir = root / now_utc.strftime("%Y-%m-%d")
    artifact = day_dir / f"ops_snapshot_{now_utc.strftime('%H%M%SZ')}.json"
    latest = root / "latest.json"
    return artifact, latest


def _load_json(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _suppression_freshness(suppression_csv: Path) -> dict[str, object]:
    raw_hours = str(os.getenv("OUTREACH_SUPPRESSION_MAX_AGE_HOURS") or "").strip()
    try:
        max_age_hours = float(raw_hours) if raw_hours else 240.0
    except Exception:
        max_age_hours = 240.0

    exists = suppression_csv.exists()
    modified_utc = None
    age_hours = None
    stale = False
    if exists:
        modified_utc = datetime.fromtimestamp(suppression_csv.stat().st_mtime, tz=timezone.utc)
        age_hours = round(max(0.0, (_now_utc() - modified_utc).total_seconds() / 3600.0), 2)
        stale = bool(age_hours > max_age_hours)
    return {
        "path": str(suppression_csv),
        "exists": exists,
        "max_age_hours": max_age_hours,
        "last_modified_utc": _iso(modified_utc) if modified_utc is not None else "",
        "age_hours": age_hours,
        "stale": stale if exists else True,
    }


def _readiness_summary(data_dir: Path, runtime_status_root: Path, now_utc: datetime) -> dict[str, object]:
    latest_path = runtime_status_root / "runtime_latest.json"
    latest_payload = _load_json(latest_path)
    latest_finished = _parse_ts(str(latest_payload.get("finished_local") or ""))
    runtime_age_minutes = None
    if latest_finished is not None:
        runtime_age_minutes = round(max(0.0, (now_utc - latest_finished).total_seconds() / 60.0), 2)

    job_states: list[dict[str, object]] = []
    parallel_scheduler_jobs: list[str] = []
    jobs_root = runtime_status_root / "jobs"
    for path in sorted(jobs_root.glob("*.json")) if jobs_root.exists() else []:
        payload = _load_json(path)
        job_name = str(payload.get("job_name") or path.stem)
        item = {
            "job_name": job_name,
            "last_slot_key": str(payload.get("last_slot_key") or ""),
            "last_result": str(payload.get("last_result") or ""),
            "last_result_detail": str(payload.get("last_result_detail") or ""),
            "last_reason": str(payload.get("last_reason") or ""),
            "last_external_scheduler_detected": int(payload.get("last_external_scheduler_detected") or 0),
            "last_reconciliation_status": str(payload.get("last_reconciliation_status") or ""),
        }
        if int(item["last_external_scheduler_detected"]) == 1:
            parallel_scheduler_jobs.append(job_name)
        job_states.append(item)

    bounce_state = _load_json(data_dir / "bounce_import_state.json")
    return {
        "runtime_status_root": str(runtime_status_root),
        "runtime_latest_path": str(latest_path),
        "runtime_latest_exists": latest_path.exists(),
        "runtime_finished_local": str(latest_payload.get("finished_local") or ""),
        "runtime_age_minutes": runtime_age_minutes,
        "alerts_sent_last_run": int(((latest_payload.get("alerts") or {}).get("alerts_sent") or 0)),
        "alerts_skipped_last_run": int(((latest_payload.get("alerts") or {}).get("alerts_skipped") or 0)),
        "job_states": job_states,
        "parallel_scheduler_jobs": parallel_scheduler_jobs,
        "parallel_scheduler_active": bool(parallel_scheduler_jobs),
        "suppression": _suppression_freshness(data_dir / "suppression.csv"),
        "bounce_import": {
            "state_path": str(data_dir / "bounce_import_state.json"),
            "exists": bool(bounce_state),
            "updated_at_utc": str(bounce_state.get("updated_at_utc") or ""),
            "last_uid_processed": int(bounce_state.get("last_uid_processed") or 0),
            "uidvalidity": str(bounce_state.get("uidvalidity") or ""),
        },
    }


def _compute_ops_report(crm_db: Path, suppression_csv: Path, now_utc: datetime, attribution_window_days: int) -> dict[str, object]:
    conn = sqlite3.connect(str(crm_db))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        required = ["prospects", "outreach_events", "suppression", "trials"]
        missing = [name for name in required if not ops_report._table_exists(conn, name)]
        if missing:
            raise RuntimeError(f"{ERR_OPS_SNAPSHOT_CRM_SCHEMA} missing_tables={','.join(missing)}")
        crm_store.ensure_outreach_events_columns(conn)
        conn.commit()

        sent_index = ops_report._load_sent_index(conn)
        windows, notes = ops_report._load_windows_report(
            conn=conn,
            sent_index=sent_index,
            now_utc=now_utc,
            attribution_window_days=attribution_window_days,
            suppression_csv=suppression_csv,
        )
        list_quality = ops_report._load_list_quality(conn=conn, now_utc=now_utc)
    finally:
        conn.close()

    return {
        "schema_version": ops_report.SCHEMA_VERSION,
        "generated_at_utc": ops_report._iso(now_utc),
        "windows": windows,
        "list_quality": list_quality,
        "notes": notes,
    }


def _render_text(payload: dict[str, object], json_path: str) -> str:
    readiness = dict(payload.get("readiness") or {})
    suppression = dict(readiness.get("suppression") or {})
    lines = [
        "Ops Snapshot",
        f"generated_at_utc={payload.get('generated_at_utc')}",
        f"parallel_scheduler_active={1 if bool(readiness.get('parallel_scheduler_active')) else 0}",
        f"parallel_scheduler_jobs={','.join(readiness.get('parallel_scheduler_jobs') or []) or 'none'}",
        f"runtime_age_minutes={readiness.get('runtime_age_minutes') if readiness.get('runtime_age_minutes') is not None else 'unknown'}",
        f"suppression_stale={1 if bool(suppression.get('stale')) else 0}",
        f"OPS_SNAPSHOT_JSON_PATH={json_path}",
        f"OPS_SNAPSHOT_SCHEMA_VERSION={SCHEMA_VERSION}",
        f"OPS_SNAPSHOT_GENERATED_AT_UTC={payload.get('generated_at_utc')}",
    ]
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Persist weekly ops + readiness snapshot artifact.")
    ap.add_argument("--print-config", action="store_true", help="Print resolved config and exit.")
    ap.add_argument("--dry-run", action="store_true", help="Compute snapshot without writing artifacts.")
    ap.add_argument("--format", choices=["text", "json"], default="text", help="Output format.")
    ap.add_argument("--crm-db", default="", help="Optional override path to crm.sqlite.")
    ap.add_argument("--suppression-csv", default="", help="Optional override path to suppression.csv.")
    ap.add_argument("--runtime-status-root", default="", help="Optional override path to runtime status root.")
    ap.add_argument("--attribution-window-days", type=int, default=30, help="Last-touch attribution window in days.")
    args = ap.parse_args(argv)

    data_dir = crm_store.data_dir().resolve()
    now_utc = _now_utc()
    crm_db = Path(args.crm_db).resolve() if (args.crm_db or "").strip() else crm_store.crm_db_path().resolve()
    suppression_csv = (
        Path(args.suppression_csv).resolve()
        if (args.suppression_csv or "").strip()
        else (data_dir / "suppression.csv").resolve()
    )
    runtime_status_root = (
        Path(args.runtime_status_root).resolve()
        if (args.runtime_status_root or "").strip()
        else (data_dir / "runtime" / "status").resolve()
    )
    artifact_path, latest_path = _output_paths(data_dir, now_utc)

    if args.print_config:
        print(f"ops_snapshot_schema_version={SCHEMA_VERSION}")
        print(f"data_dir={data_dir}")
        print(f"crm_db={crm_db}")
        print(f"suppression_csv={suppression_csv}")
        print(f"runtime_status_root={runtime_status_root}")
        print(f"artifact_path={artifact_path}")
        print(f"latest_path={latest_path}")
        print(f"dry_run={bool(args.dry_run)}")
        print(f"format={args.format}")
        return 0

    if not crm_db.exists():
        print(f"{ERR_OPS_SNAPSHOT_CRM_REQUIRED} missing_crm_db path={crm_db}", file=sys.stderr)
        return 2

    try:
        ops_payload = _compute_ops_report(
            crm_db=crm_db,
            suppression_csv=suppression_csv,
            now_utc=now_utc,
            attribution_window_days=max(1, int(args.attribution_window_days or 30)),
        )
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    readiness = _readiness_summary(
        data_dir=data_dir,
        runtime_status_root=runtime_status_root,
        now_utc=now_utc,
    )
    payload = {
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": _iso(now_utc),
        "config": {
            "crm_db": str(crm_db),
            "suppression_csv": str(suppression_csv),
            "runtime_status_root": str(runtime_status_root),
            "attribution_window_days": max(1, int(args.attribution_window_days or 30)),
            "dry_run": bool(args.dry_run),
            "format": args.format,
        },
        "ops_report": ops_payload,
        "readiness": readiness,
    }
    json_path = NO_WRITE_PATH_SENTINEL if args.dry_run else str(artifact_path)
    payload["json_path"] = json_path

    if not args.dry_run:
        artifact_path.parent.mkdir(parents=True, exist_ok=True)
        latest_path.parent.mkdir(parents=True, exist_ok=True)
        artifact_path.write_text(json.dumps(payload, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
        copyfile(str(artifact_path), str(latest_path))

    if args.format == "json":
        print(json.dumps(payload, separators=(",", ":"), ensure_ascii=True))
        return 0

    print(_render_text(payload, json_path=json_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
