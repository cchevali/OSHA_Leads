#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import crm_light

SUCCESS_PATTERN = re.compile(
    r"^\[(?P<dow>[A-Za-z]{3}) (?P<mdy>\d{2}/\d{2}/\d{4})\s+(?P<hms>\d{1,2}:\d{2}:\d{2}\.\d{2})\] SUCCESS: Wally trial run completed\s*$"
)


def _parse_success_utc(line: str, tz_name: str) -> datetime | None:
    m = SUCCESS_PATTERN.match(line.strip())
    if not m:
        return None
    local_naive = datetime.strptime(f"{m.group('mdy')} {m.group('hms')}", "%m/%d/%Y %H:%M:%S.%f")
    local_dt = local_naive.replace(tzinfo=ZoneInfo(tz_name))
    return local_dt.astimezone(timezone.utc)


def _load_existing_run_ids(conn: sqlite3.Connection, subscriber_key: str) -> set[str]:
    rows = conn.execute(
        """
        SELECT run_id
        FROM send_events
        WHERE subscriber_key = ?
        """,
        (subscriber_key,),
    ).fetchall()
    return {str(row[0] or "").strip() for row in rows if str(row[0] or "").strip()}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description="Backfill Wally trial SENT ledger rows from out/wally_trial_task.log success lines."
    )
    ap.add_argument("--subscriber-key", default="wally_trial")
    ap.add_argument("--log-path", default="out/wally_trial_task.log")
    ap.add_argument("--assume-tz", default="America/Chicago")
    ap.add_argument("--crm-db", default="", help="Optional override path for crm_light sqlite.")
    args = ap.parse_args(argv)

    subscriber_key = str(args.subscriber_key or "").strip().lower()
    if not subscriber_key:
        raise SystemExit("CONFIG_ERROR subscriber_key missing")

    log_path = Path(str(args.log_path or "").strip())
    if not log_path.exists():
        raise SystemExit(f"CONFIG_ERROR missing log_path={log_path}")

    crm_db: Path | None = None
    if str(args.crm_db or "").strip():
        crm_db = Path(str(args.crm_db)).expanduser()

    tz_name = str(args.assume_tz or "").strip() or "America/Chicago"
    try:
        ZoneInfo(tz_name)
    except Exception as exc:
        raise SystemExit(f"CONFIG_ERROR invalid assume_tz={tz_name} detail={exc}") from exc

    crm_light.ensure_database(crm_db)
    with crm_light.open_conn(crm_db) as conn:
        crm_light.init_schema(conn)
        if not crm_light.get_subscriber(conn, subscriber_key):
            raise SystemExit(f"CONFIG_ERROR subscriber not found subscriber_key={subscriber_key}")
        existing = _load_existing_run_ids(conn, subscriber_key)
        scanned = 0
        matched = 0
        appended = 0
        skipped_existing = 0

        for idx, raw_line in enumerate(log_path.read_text(encoding="utf-8", errors="replace").splitlines(), start=1):
            scanned += 1
            utc_dt = _parse_success_utc(raw_line, tz_name=tz_name)
            if utc_dt is None:
                continue
            matched += 1
            ts_utc = utc_dt.isoformat(timespec="seconds")
            run_id = f"scheduler_wally_trial_{utc_dt.strftime('%Y%m%dT%H%M%SZ')}"
            if run_id in existing:
                skipped_existing += 1
                continue
            crm_light.append_send_event(
                conn,
                subscriber_key=subscriber_key,
                variant="DAILY",
                status="SENT",
                run_id=run_id,
                meta={
                    "source": "wally_task_log_backfill",
                    "log_path": str(log_path),
                    "line_no": idx,
                },
                ts_utc=ts_utc,
            )
            existing.add(run_id)
            appended += 1

    print(f"BACKFILL_SUBSCRIBER_KEY={subscriber_key}")
    print(f"BACKFILL_LOG_PATH={log_path}")
    print(f"BACKFILL_ASSUME_TZ={tz_name}")
    print(f"BACKFILL_SCANNED_LINES={scanned}")
    print(f"BACKFILL_MATCHED_SUCCESS_LINES={matched}")
    print(f"BACKFILL_APPENDED={appended}")
    print(f"BACKFILL_SKIPPED_EXISTING={skipped_existing}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
