from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from scoring import osha_detail_cache
from scoring import paths as scoring_paths


ERR_OSHA_DETAIL_CACHE_CONFIG = "ERR_OSHA_DETAIL_CACHE_CONFIG"


def _emit(key: str, value: object) -> None:
    print(f"{key}={value}")


def _error(detail: str) -> int:
    print(f"{ERR_OSHA_DETAIL_CACHE_CONFIG} {detail}")
    return 1


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="Fetch and cache OSHA inspection detail pages into a DATA_DIR-aware SQLite cache.")
    ap.add_argument("--db", default="", help="Optional leads SQLite db path (default: data/osha.sqlite).")
    ap.add_argument("--since-days", type=int, default=14, help="Candidate lookback window in days (default: 14, max: 60).")
    ap.add_argument("--limit", type=int, default=500, help="Max candidate inspections to process (default: 500).")
    ap.add_argument("--sleep-ms", type=int, default=800, help="Sleep between detail fetch attempts in ms (default: 800).")
    ap.add_argument("--ttl-days", type=int, default=30, help="Cache TTL in days before refetch (default: 30).")
    ap.add_argument("--print-config", action="store_true", help="Print resolved config and exit.")
    ap.add_argument("--dry-run", action="store_true", help="Select candidates and print counts only; no network or writes.")
    return ap


def _write_run_summary(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    if args.since_days < 1:
        return _error(f"invalid_since_days={args.since_days}")
    if args.since_days > 60:
        return _error(f"since_days_max_exceeded={args.since_days}")
    if args.limit < 1:
        return _error(f"invalid_limit={args.limit}")
    if args.sleep_ms < 0:
        return _error(f"invalid_sleep_ms={args.sleep_ms}")
    if args.ttl_days < 0:
        return _error(f"invalid_ttl_days={args.ttl_days}")

    leads_db_path = scoring_paths.resolve_leads_db_path(args.db or None)
    cache_db_path = scoring_paths.detail_cache_db_path()

    _emit("OSHA_DETAIL_CACHE_DB", str(cache_db_path))
    _emit("OSHA_DETAIL_WINDOW_SINCE_DAYS", int(args.since_days))
    _emit("OSHA_DETAIL_LEADS_DB", str(leads_db_path))
    _emit("OSHA_DETAIL_LIMIT", int(args.limit))
    _emit("OSHA_DETAIL_SLEEP_MS", int(args.sleep_ms))
    _emit("OSHA_DETAIL_TTL_DAYS", int(args.ttl_days))

    candidates = osha_detail_cache.select_candidates_from_leads_db(
        leads_db_path=leads_db_path,
        since_days=int(args.since_days),
        limit=int(args.limit),
    )
    _emit("OSHA_DETAIL_CANDIDATES", len(candidates))

    if args.print_config:
        _emit("OSHA_DETAIL_COMPLETE", "status=PRINT_CONFIG")
        return 0

    if args.dry_run:
        _emit("OSHA_DETAIL_FETCHED", 0)
        _emit("OSHA_DETAIL_SKIPPED_CACHED", 0)
        _emit("OSHA_DETAIL_FAILED", 0)
        _emit("OSHA_DETAIL_COMPLETE", "status=DRY_RUN")
        return 0

    result = osha_detail_cache.run_cache(
        osha_detail_cache.CacheRunConfig(
            leads_db_path=leads_db_path,
            cache_db_path=cache_db_path,
            since_days=int(args.since_days),
            limit=int(args.limit),
            sleep_ms=int(args.sleep_ms),
            ttl_days=int(args.ttl_days),
            dry_run=False,
        )
    )

    _emit("OSHA_DETAIL_FETCHED", int(result.get("fetched", 0)))
    _emit("OSHA_DETAIL_SKIPPED_CACHED", int(result.get("skipped_cached", 0)))
    _emit("OSHA_DETAIL_FAILED", int(result.get("failed", 0)))

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    summary_path = scoring_paths.cache_runs_dir() / f"cache_run_{ts}.json"
    payload = {
        "ts_utc": datetime.now(timezone.utc).isoformat(),
        "cache_db_path": str(cache_db_path),
        "leads_db_path": str(leads_db_path),
        "config": {
            "since_days": int(args.since_days),
            "limit": int(args.limit),
            "sleep_ms": int(args.sleep_ms),
            "ttl_days": int(args.ttl_days),
        },
        "counts": {
            "candidates": int(result.get("candidates", 0)),
            "fetched": int(result.get("fetched", 0)),
            "skipped_cached": int(result.get("skipped_cached", 0)),
            "failed": int(result.get("failed", 0)),
        },
        "top_failure_reasons": dict(result.get("failed_reasons") or {}),
    }
    _write_run_summary(summary_path, payload)
    _emit("OSHA_DETAIL_RUN_SUMMARY", str(summary_path))
    _emit("OSHA_DETAIL_COMPLETE", "status=OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

