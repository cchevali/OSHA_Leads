#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scoring import paths as scoring_paths


DEFAULT_STATES = ["TX", "CA", "FL", "OR", "WA"]
ERR_STATE_COVERAGE_CONFIG = "ERR_STATE_COVERAGE_CONFIG"
ERR_STATE_COVERAGE_DB = "ERR_STATE_COVERAGE_DB"
PASS_STATE_COVERAGE_COMPLETE = "PASS_STATE_COVERAGE_COMPLETE"


def _emit(key: str, value: object) -> None:
    print(f"{key}={value}")


def _error(token: str, detail: str) -> int:
    print(f"{token} {detail}")
    return 1


def _parse_states(raw: str) -> list[str]:
    out: list[str] = []
    for part in str(raw or "").split(","):
        state = str(part or "").strip().upper()
        if not state:
            continue
        if not re.fullmatch(r"[A-Z]{2}", state):
            raise ValueError(f"invalid_state={state}")
        if state not in out:
            out.append(state)
    return out


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (str(table_name or "").strip(),),
    ).fetchone()
    return row is not None


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="Read-only state coverage diagnostics for inspections in data/osha.sqlite.")
    ap.add_argument(
        "--states",
        default=",".join(DEFAULT_STATES),
        help="Comma-separated state list (default: TX,CA,FL,OR,WA).",
    )
    ap.add_argument(
        "--since-days",
        type=int,
        default=60,
        help="Lookback window for total counts using first_seen/date_opened fallback (default: 60).",
    )
    ap.add_argument(
        "--db",
        default=str(scoring_paths.default_leads_db_path()),
        help="SQLite inspections DB path (default: data/osha.sqlite).",
    )
    ap.add_argument("--print-config", action="store_true", help="Print resolved config and exit.")
    ap.add_argument("--dry-run", action="store_true", help="Run read-only diagnostics and mark dry-run status.")
    return ap


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if int(args.since_days) < 1:
        return _error(ERR_STATE_COVERAGE_CONFIG, f"invalid_since_days={args.since_days}")

    try:
        states = _parse_states(args.states)
    except ValueError as exc:
        return _error(ERR_STATE_COVERAGE_CONFIG, str(exc))
    if not states:
        return _error(ERR_STATE_COVERAGE_CONFIG, "states_empty")

    db_path = Path(str(args.db or "")).expanduser().resolve(strict=False)
    now_utc = datetime.now(timezone.utc)
    since_cutoff_dt = now_utc - timedelta(days=int(args.since_days))
    since_cutoff_iso = since_cutoff_dt.isoformat()
    since_cutoff_date = since_cutoff_dt.date().isoformat()
    first_seen_30d_cutoff = (now_utc - timedelta(days=30)).isoformat()

    _emit("STATE_COVERAGE_DB", db_path)
    _emit("STATE_COVERAGE_STATES", ",".join(states))
    _emit("STATE_COVERAGE_SINCE_DAYS", int(args.since_days))
    _emit("STATE_COVERAGE_SINCE_CUTOFF", since_cutoff_date)

    if args.print_config:
        print(f"{PASS_STATE_COVERAGE_COMPLETE} status=PRINT_CONFIG")
        return 0

    if not db_path.exists():
        return _error(ERR_STATE_COVERAGE_DB, f"missing_db={db_path}")

    conn = sqlite3.connect(str(db_path))
    try:
        if not _table_exists(conn, "inspections"):
            return _error(ERR_STATE_COVERAGE_DB, "missing_table=inspections")

        for state in states:
            total = int(
                conn.execute(
                    """
                    SELECT COUNT(*)
                    FROM inspections
                    WHERE upper(trim(COALESCE(site_state, ''))) = ?
                      AND (
                        (trim(COALESCE(first_seen_at, '')) <> '' AND datetime(first_seen_at) >= datetime(?))
                        OR
                        (trim(COALESCE(date_opened, '')) <> '' AND date(date_opened) >= date(?))
                      )
                    """,
                    (state, since_cutoff_iso, since_cutoff_date),
                ).fetchone()[0]
                or 0
            )
            open_date_max = str(
                conn.execute(
                    """
                    SELECT COALESCE(MAX(date_opened), '')
                    FROM inspections
                    WHERE upper(trim(COALESCE(site_state, ''))) = ?
                    """,
                    (state,),
                ).fetchone()[0]
                or ""
            ).strip()
            first_seen_max = str(
                conn.execute(
                    """
                    SELECT COALESCE(MAX(first_seen_at), '')
                    FROM inspections
                    WHERE upper(trim(COALESCE(site_state, ''))) = ?
                    """,
                    (state,),
                ).fetchone()[0]
                or ""
            ).strip()
            first_seen_30d = int(
                conn.execute(
                    """
                    SELECT COUNT(*)
                    FROM inspections
                    WHERE upper(trim(COALESCE(site_state, ''))) = ?
                      AND trim(COALESCE(first_seen_at, '')) <> ''
                      AND datetime(first_seen_at) >= datetime(?)
                    """,
                    (state, first_seen_30d_cutoff),
                ).fetchone()[0]
                or 0
            )

            print(
                "STATE_COVERAGE "
                f"state={state} total={total} open_date_max={open_date_max or 'empty'} "
                f"first_seen_max={first_seen_max or 'empty'} first_seen_30d={first_seen_30d}"
            )

            sample_rows = conn.execute(
                """
                SELECT activity_nr
                FROM inspections
                WHERE upper(trim(COALESCE(site_state, ''))) = ?
                  AND trim(COALESCE(activity_nr, '')) <> ''
                ORDER BY COALESCE(datetime(first_seen_at), datetime(last_seen_at), date(date_opened), '') DESC
                LIMIT 5
                """,
                (state,),
            ).fetchall()
            sample = [str(row[0] or "").strip() for row in sample_rows if str(row[0] or "").strip()]
            print(f"STATE_ACTIVITY_SAMPLE state={state} sample={','.join(sample) if sample else 'empty'}")
    finally:
        conn.close()

    status = "DRY_RUN" if args.dry_run else "OK"
    print(f"{PASS_STATE_COVERAGE_COMPLETE} status={status}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
