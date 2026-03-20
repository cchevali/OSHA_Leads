#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import io
import re
import sys
import zipfile
from datetime import date, datetime, timedelta
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import requests_warning_compat
import requests

DEFAULT_STATES = ["TX", "CA", "FL", "OR", "WA"]
BULK_FEED_URL_TEMPLATE = "https://enfxfr.dol.gov/data_catalog/OSHA/osha_inspection_{yyyymmdd}.csv.zip"
ERR_OSHA_BULK_FEED_CONFIG = "ERR_OSHA_BULK_FEED_CONFIG"
ERR_OSHA_BULK_FEED_UNAVAILABLE = "ERR_OSHA_BULK_FEED_UNAVAILABLE"
ERR_OSHA_BULK_FEED_PARSE = "ERR_OSHA_BULK_FEED_PARSE"
PASS_OSHA_BULK_FEED_COMPLETE = "PASS_OSHA_BULK_FEED_COMPLETE"


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


def _is_zip_payload(content: bytes) -> bool:
    return bytes(content or b"").startswith(b"PK")


def _normalize_header(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(name or "").strip().lower()).strip("_")


def _find_column_indexes(header: list[str]) -> tuple[int | None, int | None]:
    state_candidates = {
        "site_state",
        "state",
        "state_abbr",
        "state_code",
        "establishment_state",
    }
    open_date_candidates = {
        "date_opened",
        "open_date",
        "opened_date",
        "inspection_open_date",
    }
    state_idx: int | None = None
    opened_idx: int | None = None
    for idx, value in enumerate(list(header or [])):
        normalized = _normalize_header(value)
        if state_idx is None and normalized in state_candidates:
            state_idx = idx
        if opened_idx is None and normalized in open_date_candidates:
            opened_idx = idx
    if opened_idx is None:
        for idx, value in enumerate(list(header or [])):
            normalized = _normalize_header(value)
            if "open" in normalized and "date" in normalized:
                opened_idx = idx
                break
    if state_idx is None:
        for idx, value in enumerate(list(header or [])):
            normalized = _normalize_header(value)
            if normalized.endswith("state") or normalized == "state":
                state_idx = idx
                break
    return state_idx, opened_idx


def _parse_open_date(value: str) -> date | None:
    text = str(value or "").strip()
    if not text:
        return None
    if "T" in text:
        text = text.split("T", 1)[0]
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m-%d-%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    try:
        return date.fromisoformat(text)
    except ValueError:
        return None


def _probe_feed_payload(candidate_date: date) -> tuple[bytes | None, str, int | None]:
    yyyymmdd = candidate_date.strftime("%Y%m%d")
    url = BULK_FEED_URL_TEMPLATE.format(yyyymmdd=yyyymmdd)
    try:
        response = requests.get(url, timeout=30)
    except Exception as exc:
        print(f"WARN_OSHA_BULK_FEED_FETCH date={yyyymmdd} error={exc.__class__.__name__}")
        return None, url, None

    status = int(getattr(response, "status_code", 0) or 0)
    if status != 200:
        print(f"WARN_OSHA_BULK_FEED_HTTP date={yyyymmdd} status={status}")
        return None, url, status

    content = bytes(getattr(response, "content", b"") or b"")
    if not _is_zip_payload(content):
        content_type = str((response.headers or {}).get("Content-Type") or "").strip() or "unknown"
        print(
            "WARN_OSHA_BULK_FEED_NON_ZIP "
            f"date={yyyymmdd} status={status} content_type={content_type} bytes={len(content)}"
        )
        return None, url, status
    return content, url, status


def _parse_feed_counts(
    *,
    zip_payload: bytes,
    states: list[str],
    since_days: int,
) -> dict[str, dict[str, object]]:
    state_set = {s.upper() for s in states}
    today_local = datetime.now().astimezone().date()
    cutoff_date = today_local - timedelta(days=max(1, int(since_days)))
    per_state: dict[str, dict[str, object]] = {
        s: {"rows": 0, "open_date_max": None}
        for s in states
    }

    with zipfile.ZipFile(io.BytesIO(zip_payload), "r") as zf:
        csv_members = [name for name in zf.namelist() if str(name or "").lower().endswith(".csv")]
        if not csv_members:
            raise ValueError("missing_csv_member")
        member = sorted(csv_members)[0]
        with zf.open(member, "r") as raw:
            text_stream = io.TextIOWrapper(raw, encoding="utf-8-sig", newline="")
            reader = csv.reader(text_stream)
            try:
                header = next(reader)
            except StopIteration as exc:
                raise ValueError("empty_csv") from exc
            state_idx, opened_idx = _find_column_indexes(header)
            if state_idx is None:
                raise ValueError("missing_state_column")
            for row in reader:
                if state_idx >= len(row):
                    continue
                state = str(row[state_idx] or "").strip().upper()
                if state not in state_set:
                    continue
                opened = None
                if opened_idx is not None and opened_idx < len(row):
                    opened = _parse_open_date(row[opened_idx])
                    if opened is not None and opened < cutoff_date:
                        continue

                bucket = per_state[state]
                bucket["rows"] = int(bucket.get("rows", 0)) + 1
                current_max = bucket.get("open_date_max")
                if opened is not None and (current_max is None or opened > current_max):
                    bucket["open_date_max"] = opened
    return per_state


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="Probe OSHA bulk feed availability and state coverage (no DB writes).")
    ap.add_argument(
        "--states",
        default=",".join(DEFAULT_STATES),
        help="Comma-separated states to report (default: TX,CA,FL,OR,WA).",
    )
    ap.add_argument(
        "--since-days",
        type=int,
        default=60,
        help="Window for open-date filtering when feed has open-date field (default: 60).",
    )
    ap.add_argument("--print-config", action="store_true", help="Print resolved config and exit.")
    ap.add_argument("--dry-run", action="store_true", help="Skip network probing and exit.")
    return ap


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if int(args.since_days) < 1:
        return _error(ERR_OSHA_BULK_FEED_CONFIG, f"invalid_since_days={args.since_days}")
    try:
        states = _parse_states(args.states)
    except ValueError as exc:
        return _error(ERR_OSHA_BULK_FEED_CONFIG, str(exc))
    if not states:
        return _error(ERR_OSHA_BULK_FEED_CONFIG, "states_empty")

    today_local = datetime.now().astimezone().date()
    candidate_dates = [today_local, today_local - timedelta(days=1)]
    _emit("OSHA_BULK_FEED_STATES", ",".join(states))
    _emit("OSHA_BULK_FEED_SINCE_DAYS", int(args.since_days))
    _emit("OSHA_BULK_FEED_DATE_CANDIDATES", ",".join([d.strftime("%Y%m%d") for d in candidate_dates]))

    if args.print_config:
        print(f"{PASS_OSHA_BULK_FEED_COMPLETE} status=PRINT_CONFIG")
        return 0
    if args.dry_run:
        print(f"{PASS_OSHA_BULK_FEED_COMPLETE} status=DRY_RUN")
        return 0

    selected_payload = None
    selected_date = None
    selected_url = ""
    for d in candidate_dates:
        payload, url, _status = _probe_feed_payload(d)
        if payload is not None:
            selected_payload = payload
            selected_date = d
            selected_url = url
            break

    if selected_payload is None or selected_date is None:
        return _error(
            ERR_OSHA_BULK_FEED_UNAVAILABLE,
            f"tried_dates={','.join([d.strftime('%Y%m%d') for d in candidate_dates])}",
        )

    try:
        per_state = _parse_feed_counts(
            zip_payload=selected_payload,
            states=states,
            since_days=int(args.since_days),
        )
    except Exception as exc:
        return _error(ERR_OSHA_BULK_FEED_PARSE, f"detail={exc}")

    print(f"OSHA_BULK_FEED_OK date={selected_date.strftime('%Y%m%d')} url={selected_url}")
    rows_total = 0
    for state in states:
        bucket = per_state.get(state, {"rows": 0, "open_date_max": None})
        rows = int(bucket.get("rows", 0) or 0)
        rows_total += rows
        max_open_date = bucket.get("open_date_max")
        open_token = max_open_date.isoformat() if isinstance(max_open_date, date) else "empty"
        print(f"OSHA_BULK_FEED_STATE state={state} rows={rows} open_date_max={open_token}")
    print(f"{PASS_OSHA_BULK_FEED_COMPLETE} status=OK rows_total={rows_total}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
