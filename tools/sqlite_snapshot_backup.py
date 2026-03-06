from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path


ERR_SQLITE_SNAPSHOT_ARGS = "ERR_SQLITE_SNAPSHOT_ARGS"
ERR_SQLITE_SNAPSHOT_SOURCE_MISSING = "ERR_SQLITE_SNAPSHOT_SOURCE_MISSING"
ERR_SQLITE_SNAPSHOT_FAILED = "ERR_SQLITE_SNAPSHOT_FAILED"
PASS_SQLITE_SNAPSHOT_COMPLETE = "PASS_SQLITE_SNAPSHOT_COMPLETE"


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _snapshot_sqlite(source_path: Path, dest_path: Path) -> None:
    source_uri = f"file:{source_path.as_posix()}?mode=ro"
    src = sqlite3.connect(source_uri, uri=True)
    dst = sqlite3.connect(str(dest_path))
    try:
        src.backup(dst)
        dst.commit()
    finally:
        try:
            dst.close()
        except Exception:
            pass
        try:
            src.close()
        except Exception:
            pass


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="Create a safe SQLite snapshot copy.")
    ap.add_argument("--source", required=True, help="Path to source SQLite DB.")
    ap.add_argument("--output-dir", required=True, help="Directory for snapshot output.")
    ap.add_argument("--label", default="", help="Optional snapshot label.")
    ap.add_argument("--dry-run", action="store_true", help="Resolve paths and exit without writing files.")
    ap.add_argument("--print-config", action="store_true", help="Print resolved config and exit.")
    ap.add_argument("--skip-hash", action="store_true", help="Skip SHA256 computation.")
    return ap


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    source = Path(str(args.source or "")).expanduser().resolve(strict=False)
    output_dir = Path(str(args.output_dir or "")).expanduser().resolve(strict=False)
    label = str(args.label or "").strip()
    if not label:
        label = source.stem

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    snapshot_name = f"{label}_{ts}.sqlite"
    snapshot_path = output_dir / snapshot_name

    print(f"SQLITE_SNAPSHOT_SOURCE={source}")
    print(f"SQLITE_SNAPSHOT_OUTPUT_DIR={output_dir}")
    print(f"SQLITE_SNAPSHOT_OUTPUT_PATH={snapshot_path}")
    print(f"SQLITE_SNAPSHOT_LABEL={label}")
    print(f"SQLITE_SNAPSHOT_DRY_RUN={1 if args.dry_run else 0}")

    if args.print_config:
        print("PASS_SQLITE_SNAPSHOT_PRINT_CONFIG")
        return 0

    if not source.exists():
        print(f"{ERR_SQLITE_SNAPSHOT_SOURCE_MISSING} path={source}")
        return 1

    if args.dry_run:
        print(f"{PASS_SQLITE_SNAPSHOT_COMPLETE} status=DRY_RUN")
        return 0

    try:
        output_dir.mkdir(parents=True, exist_ok=True)
        _snapshot_sqlite(source, snapshot_path)
        size_bytes = int(snapshot_path.stat().st_size)
        payload = {
            "source": str(source),
            "snapshot_path": str(snapshot_path),
            "size_bytes": size_bytes,
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        }
        if not args.skip_hash:
            payload["sha256"] = _sha256_file(snapshot_path)
        print("SQLITE_SNAPSHOT_RESULT=" + json.dumps(payload, sort_keys=True))
        print(f"{PASS_SQLITE_SNAPSHOT_COMPLETE} status=OK")
        return 0
    except Exception as exc:
        print(f"{ERR_SQLITE_SNAPSHOT_FAILED} detail={type(exc).__name__}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
