import argparse
import os
import re
import sys
from pathlib import Path

import ingest_osha


ERR_INGEST_DAILY_CONFIG = "ERR_INGEST_DAILY_CONFIG"
ERR_INGEST_DAILY_FAILED = "ERR_INGEST_DAILY_FAILED"
PASS_INGEST_DAILY_COMPLETE = "PASS_INGEST_DAILY_COMPLETE"


def _emit(key: str, value: str) -> None:
    print(f"{key}={value}")


def _error(token: str, detail: str) -> int:
    print(f"{token} {detail}")
    return 1


def _parse_states(raw: str) -> list[str]:
    states: list[str] = []
    for part in (raw or "").split(","):
        state = (part or "").strip().upper()
        if not state:
            continue
        if not re.fullmatch(r"[A-Z]{2,3}", state):
            raise ValueError(f"invalid_state={state}")
        if state not in states:
            states.append(state)
    return states


def _resolve_states(cli_states: str) -> tuple[list[str], str]:
    if (cli_states or "").strip():
        return _parse_states(cli_states), "cli"

    env_states_raw = (os.getenv("OUTREACH_STATES") or "").strip()
    env_states = _parse_states(env_states_raw)
    if env_states:
        return env_states, "env"
    return ["TX"], "fallback"


def _emit_common_tokens(db_path: Path, states: list[str], since_days: int, max_details: int, source: str) -> None:
    _emit("INGEST_DB_PATH", str(db_path))
    _emit("INGEST_STATES", ",".join(states))
    _emit("INGEST_SINCE_DAYS", str(since_days))
    _emit("INGEST_MAX_DETAILS", str(max_details))
    _emit("INGEST_STATES_SOURCE", source)
    _emit("INGEST_STATES_FALLBACK_USED", "YES" if source == "fallback" else "NO")


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        description="Daily OSHA ingest wrapper driven by OUTREACH_STATES for outreach signal freshness."
    )
    ap.add_argument("--print-config", action="store_true", help="Print resolved config and exit.")
    ap.add_argument("--dry-run", action="store_true", help="Print resolved config and skip ingest.")
    ap.add_argument("--states", default="", help="Optional comma-separated state override (e.g., TX,CA,FL).")
    ap.add_argument("--since-days", type=int, default=3, help="Lookback days for ingest search (default: 3).")
    ap.add_argument("--max-details", type=int, default=200, help="Max detail pages to fetch (default: 200).")
    return ap


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.since_days < 1:
        return _error(ERR_INGEST_DAILY_CONFIG, f"invalid_since_days={args.since_days}")
    if args.max_details < 1:
        return _error(ERR_INGEST_DAILY_CONFIG, f"invalid_max_details={args.max_details}")

    try:
        states, source = _resolve_states(args.states)
    except ValueError as exc:
        return _error(ERR_INGEST_DAILY_CONFIG, str(exc))
    if not states:
        return _error(ERR_INGEST_DAILY_CONFIG, "states_empty")

    repo_root = Path(__file__).resolve().parent
    db_path = (repo_root / "data" / "osha.sqlite").resolve()
    _emit_common_tokens(
        db_path=db_path,
        states=states,
        since_days=args.since_days,
        max_details=args.max_details,
        source=source,
    )

    if args.print_config:
        print(f"{PASS_INGEST_DAILY_COMPLETE} status=PRINT_CONFIG")
        return 0
    if args.dry_run:
        print(f"{PASS_INGEST_DAILY_COMPLETE} status=DRY_RUN")
        return 0

    try:
        ingest_osha.setup_logging("INFO")
    except Exception:
        pass

    try:
        stats = ingest_osha.run_ingestion(
            db_path=str(db_path),
            since_days=int(args.since_days),
            states=list(states),
            max_details=int(args.max_details),
        )
    except Exception as exc:
        detail = re.sub(r"\s+", " ", str(exc)).strip() or exc.__class__.__name__
        print(f"{ERR_INGEST_DAILY_FAILED} {detail}")
        return 1

    _emit("INGEST_RESULTS_FOUND", str(int(stats.get("results_found", 0))))
    _emit("INGEST_DETAILS_FETCHED", str(int(stats.get("details_fetched", 0))))
    _emit("INGEST_ROWS_INSERTED", str(int(stats.get("rows_inserted", 0))))
    _emit("INGEST_ROWS_UPDATED", str(int(stats.get("rows_updated", 0))))
    _emit("INGEST_ERRORS_COUNT", str(int(stats.get("errors_count", 0))))
    print(f"{PASS_INGEST_DAILY_COMPLETE} status=OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
