from __future__ import annotations

import argparse
import json
import shutil
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RETENTION_DAYS = 14
TARGET_PATTERNS = (
    "outbox_*_dry_run.csv",
    "outbox_*_dry_run_manifest.csv",
    "plan_diagnostics.json",
)


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _output_payload(
    *,
    root: Path,
    retention_days: int,
    dry_run: bool,
    scanned_dirs: int,
    candidate_files: list[dict[str, object]],
    removed_files: list[str],
    removed_dirs: list[str],
) -> dict[str, object]:
    return {
        "schema_version": "outreach_dry_run_cleanup_v1",
        "root": str(root),
        "retention_days": retention_days,
        "dry_run": dry_run,
        "scanned_dirs": scanned_dirs,
        "candidate_count": len(candidate_files),
        "removed_file_count": len(removed_files),
        "removed_dir_count": len(removed_dirs),
        "candidates": candidate_files,
        "removed_files": removed_files,
        "removed_dirs": removed_dirs,
    }


def _is_stale(path: Path, cutoff: datetime) -> bool:
    modified = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
    return modified < cutoff


def _candidate_files(root: Path, cutoff: datetime) -> tuple[int, list[dict[str, object]]]:
    scanned_dirs = 0
    candidates: list[dict[str, object]] = []
    if not root.exists():
        return scanned_dirs, candidates
    for batch_dir in sorted(path for path in root.iterdir() if path.is_dir()):
        scanned_dirs += 1
        for pattern in TARGET_PATTERNS:
            for path in sorted(batch_dir.glob(pattern)):
                if not path.is_file():
                    continue
                if not _is_stale(path, cutoff):
                    continue
                modified = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
                candidates.append(
                    {
                        "path": str(path),
                        "batch_dir": str(batch_dir),
                        "modified_utc": modified.isoformat(),
                        "size_bytes": int(path.stat().st_size),
                    }
                )
    return scanned_dirs, candidates


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Prune stale outreach dry-run artifacts only.")
    ap.add_argument("--print-config", action="store_true", help="Print resolved config and exit.")
    ap.add_argument("--dry-run", action="store_true", help="Show stale dry-run artifacts without deleting them.")
    ap.add_argument("--root", default="", help="Optional override for outreach artifact root.")
    ap.add_argument("--retention-days", type=int, default=DEFAULT_RETENTION_DAYS, help="Age threshold in whole days.")
    args = ap.parse_args(argv)

    root = Path(args.root).resolve() if (args.root or "").strip() else (REPO_ROOT / "out" / "outreach").resolve()
    retention_days = max(1, int(args.retention_days or DEFAULT_RETENTION_DAYS))
    cutoff = _now_utc() - timedelta(days=retention_days)

    if args.print_config:
        print(f"cleanup_schema_version=outreach_dry_run_cleanup_v1")
        print(f"root={root}")
        print(f"retention_days={retention_days}")
        print(f"dry_run={bool(args.dry_run)}")
        print("target_patterns=" + ",".join(TARGET_PATTERNS))
        return 0

    scanned_dirs, candidates = _candidate_files(root, cutoff)
    removed_files: list[str] = []
    removed_dirs: list[str] = []

    if not args.dry_run:
        by_dir: dict[Path, list[Path]] = {}
        for item in candidates:
            path = Path(str(item["path"]))
            by_dir.setdefault(path.parent, []).append(path)
            path.unlink(missing_ok=True)
            removed_files.append(str(path))
        for batch_dir in sorted(by_dir):
            remaining = [path for path in batch_dir.iterdir()]
            if remaining:
                continue
            shutil.rmtree(batch_dir)
            removed_dirs.append(str(batch_dir))

    payload = _output_payload(
        root=root,
        retention_days=retention_days,
        dry_run=bool(args.dry_run),
        scanned_dirs=scanned_dirs,
        candidate_files=candidates,
        removed_files=removed_files,
        removed_dirs=removed_dirs,
    )
    print(json.dumps(payload, separators=(",", ":"), ensure_ascii=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
