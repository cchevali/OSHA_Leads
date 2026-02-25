#!/usr/bin/env python3
"""
Export a committed public sample feed JSON from OSHA inspections SQLite data.

Windows-first CLI contract:
  - supports --print-config (no writes)
  - supports --dry-run (no writes)
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


DEFAULT_OUT_PATH = Path("web/app/sample/sample_signals.json")
DEFAULT_CONFIG_PATH = Path("web/app/sample/sample_feed_config.json")

STATE_NAMES = {
    "AL": "Alabama",
    "AK": "Alaska",
    "AZ": "Arizona",
    "AR": "Arkansas",
    "CA": "California",
    "CO": "Colorado",
    "CT": "Connecticut",
    "DE": "Delaware",
    "FL": "Florida",
    "GA": "Georgia",
    "HI": "Hawaii",
    "ID": "Idaho",
    "IL": "Illinois",
    "IN": "Indiana",
    "IA": "Iowa",
    "KS": "Kansas",
    "KY": "Kentucky",
    "LA": "Louisiana",
    "ME": "Maine",
    "MD": "Maryland",
    "MA": "Massachusetts",
    "MI": "Michigan",
    "MN": "Minnesota",
    "MS": "Mississippi",
    "MO": "Missouri",
    "MT": "Montana",
    "NE": "Nebraska",
    "NV": "Nevada",
    "NH": "New Hampshire",
    "NJ": "New Jersey",
    "NM": "New Mexico",
    "NY": "New York",
    "NC": "North Carolina",
    "ND": "North Dakota",
    "OH": "Ohio",
    "OK": "Oklahoma",
    "OR": "Oregon",
    "PA": "Pennsylvania",
    "RI": "Rhode Island",
    "SC": "South Carolina",
    "SD": "South Dakota",
    "TN": "Tennessee",
    "TX": "Texas",
    "UT": "Utah",
    "VT": "Vermont",
    "VA": "Virginia",
    "WA": "Washington",
    "WV": "West Virginia",
    "WI": "Wisconsin",
    "WY": "Wyoming",
    "DC": "District of Columbia",
}


@dataclass(frozen=True)
class TerritoryConfig:
    territory_id: str
    territory_name: str


def _parse_dt(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    normalized = text.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(normalized)
    except ValueError:
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                dt = datetime.strptime(text, fmt)
                break
            except ValueError:
                continue
        else:
            return None

    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _to_iso_z(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _normalize_date(value: Any) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    if len(text) >= 10 and text[4] == "-" and text[7] == "-":
        return text[:10]
    dt = _parse_dt(text)
    if not dt:
        return None
    return dt.date().isoformat()


def _observed_dt(row: dict[str, Any]) -> datetime | None:
    for key in ("first_seen_at", "changed_at", "last_seen_at"):
        dt = _parse_dt(row.get(key))
        if dt:
            return dt
    opened = _normalize_date(row.get("date_opened"))
    if opened:
        return _parse_dt(opened + "T00:00:00+00:00")
    return None


def _row_sort_key(row: dict[str, Any]) -> tuple[Any, ...]:
    obs = _observed_dt(row)
    opened = _parse_dt(_normalize_date(row.get("date_opened")) or "")
    return (
        obs or datetime.min.replace(tzinfo=timezone.utc),
        opened or datetime.min.replace(tzinfo=timezone.utc),
        str(row.get("activity_nr") or ""),
    )


def _load_config(path: Path) -> list[TerritoryConfig]:
    data = json.loads(path.read_text(encoding="utf-8"))
    territories = data.get("fallback_territories") if isinstance(data, dict) else None
    if not isinstance(territories, list):
        raise ValueError("Invalid sample feed config: fallback_territories must be a list")
    out: list[TerritoryConfig] = []
    for item in territories:
        if not isinstance(item, dict):
            continue
        tid = str(item.get("territory_id") or "").strip().upper()
        tname = str(item.get("territory_name") or "").strip()
        if tid:
            out.append(TerritoryConfig(territory_id=tid, territory_name=tname or STATE_NAMES.get(tid, tid)))
    return out


def _table_columns(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute("PRAGMA table_info(inspections)").fetchall()
    return {str(r[1]) for r in rows if len(r) > 1}


def _query_inspection_rows(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    cols = _table_columns(conn)
    if not cols:
        return []

    wanted = [
        "activity_nr",
        "inspection_type",
        "establishment_name",
        "site_city",
        "site_state",
        "date_opened",
        "first_seen_at",
        "changed_at",
        "last_seen_at",
        "source_url",
        "parse_invalid",
        "case_status",
    ]
    select_cols = []
    for col in wanted:
        if col in cols:
            select_cols.append(col)
        else:
            select_cols.append(f"NULL AS {col}")

    where_parts = ["COALESCE(parse_invalid, 0) = 0"]
    if "case_status" in cols:
        where_parts.append("(case_status IS NULL OR UPPER(COALESCE(case_status, '')) = 'OPEN')")
    if "activity_nr" in cols:
        where_parts.append("COALESCE(activity_nr, '') <> ''")
    if "site_state" in cols:
        where_parts.append("LENGTH(TRIM(COALESCE(site_state, ''))) >= 2")

    sql = (
        "SELECT "
        + ", ".join(select_cols)
        + " FROM inspections WHERE "
        + " AND ".join(where_parts)
    )
    cur = conn.execute(sql)
    rows = [dict(zip([d[0] for d in cur.description], row)) for row in cur.fetchall()]
    cleaned: list[dict[str, Any]] = []
    for row in rows:
        state = str(row.get("site_state") or "").strip().upper()
        if not state:
            continue
        row["site_state"] = state
        cleaned.append(row)
    return cleaned


def _territory_name_for_id(territory_id: str, fallback_map: dict[str, str]) -> str:
    return fallback_map.get(territory_id, STATE_NAMES.get(territory_id, territory_id))


def _choose_auto_territories(
    rows: list[dict[str, Any]],
    lookback_days: int,
    fallback: list[TerritoryConfig],
) -> list[str]:
    now_utc = datetime.now(timezone.utc)
    cutoff = now_utc - timedelta(days=max(0, int(lookback_days)))
    by_state: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        by_state.setdefault(str(row.get("site_state") or ""), []).append(row)

    ranked: list[tuple[int, datetime, str]] = []
    for state, items in by_state.items():
        recent_items = [r for r in items if (_observed_dt(r) or datetime.min.replace(tzinfo=timezone.utc)) >= cutoff]
        if not recent_items:
            continue
        newest = max((_observed_dt(r) or datetime.min.replace(tzinfo=timezone.utc)) for r in recent_items)
        ranked.append((len(recent_items), newest, state))

    ranked.sort(key=lambda t: (-t[0], -(t[1].timestamp()), t[2]))
    chosen = [state for _, _, state in ranked[:3]]
    for item in fallback:
        if len(chosen) >= 3:
            break
        if item.territory_id not in chosen:
            chosen.append(item.territory_id)
    return chosen[:3]


def _explicit_territories(value: str, fallback: list[TerritoryConfig]) -> list[str]:
    parts = [p.strip().upper() for p in str(value or "").split(",")]
    out = [p for p in parts if p]
    if out:
        return out
    return [f.territory_id for f in fallback[:3]]


def _rows_for_territory(rows: list[dict[str, Any]], territory_id: str, limit: int) -> tuple[list[dict[str, Any]], str | None]:
    terr_rows = [r for r in rows if str(r.get("site_state") or "").upper() == territory_id.upper()]
    terr_rows.sort(key=_row_sort_key, reverse=True)
    selected = terr_rows[: max(0, int(limit))]
    updated_dt = max((_observed_dt(r) for r in selected if _observed_dt(r)), default=None)
    out_rows: list[dict[str, Any]] = []
    for row in selected:
        out_rows.append(
            {
                "activity_nr": str(row.get("activity_nr") or ""),
                "inspection_type": str(row.get("inspection_type") or "").strip() or "Inspection",
                "establishment_name": str(row.get("establishment_name") or "").strip() or "Unknown establishment",
                "city": str(row.get("site_city") or "").strip(),
                "state": str(row.get("site_state") or "").strip().upper(),
                "opened_date": _normalize_date(row.get("date_opened")),
                "observed_at_utc": _to_iso_z(_observed_dt(row)),
                "source_url": str(row.get("source_url") or "").strip(),
            }
        )
    return out_rows, _to_iso_z(updated_dt)


def build_sample_feed(
    db_path: Path,
    config_path: Path,
    territories_arg: str,
    rows_per_territory: int,
    lookback_days: int,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    fallback = _load_config(config_path)
    fallback_map = {t.territory_id: t.territory_name for t in fallback}

    conn = sqlite3.connect(str(db_path))
    try:
        rows = _query_inspection_rows(conn)
    finally:
        conn.close()

    if str(territories_arg).strip().upper() == "AUTO":
        chosen = _choose_auto_territories(rows, lookback_days=lookback_days, fallback=fallback)
        territory_mode = "AUTO"
    else:
        chosen = _explicit_territories(territories_arg, fallback=fallback)
        territory_mode = "EXPLICIT"

    payload: list[dict[str, Any]] = []
    total_rows = 0
    for territory_id in chosen:
        terr_rows, updated_at_utc = _rows_for_territory(rows, territory_id, rows_per_territory)
        total_rows += len(terr_rows)
        payload.append(
            {
                "territory_id": territory_id,
                "territory_name": _territory_name_for_id(territory_id, fallback_map),
                "updated_at_utc": updated_at_utc,
                "rows": terr_rows,
            }
        )

    stats = {
        "territory_mode": territory_mode,
        "territories_selected": chosen,
        "territory_count": len(payload),
        "row_count": total_rows,
        "rows_per_territory": rows_per_territory,
        "lookback_days": lookback_days,
        "fallback_territories": [t.territory_id for t in fallback],
    }
    return payload, stats


def _print_json(obj: dict[str, Any] | list[Any]) -> None:
    print(json.dumps(obj, indent=2, ensure_ascii=True))


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Export committed public sample feed JSON from inspections SQLite data.")
    ap.add_argument("--db", required=True, help="Path to SQLite database")
    ap.add_argument("--out", default=str(DEFAULT_OUT_PATH), help="Output JSON path")
    ap.add_argument("--config", default=str(DEFAULT_CONFIG_PATH), help="Sample feed config JSON path")
    ap.add_argument("--territories", default="AUTO", help="AUTO or comma-separated territory ids (state codes)")
    ap.add_argument("--rows-per-territory", type=int, default=4, help="Rows per territory section")
    ap.add_argument("--lookback-days", type=int, default=7, help="AUTO ranking lookback days")
    ap.add_argument("--print-config", action="store_true", help="Print resolved config and exit")
    ap.add_argument("--dry-run", action="store_true", help="Do not write output file")
    args = ap.parse_args(argv)

    db_path = Path(args.db)
    out_path = Path(args.out)
    config_path = Path(args.config)

    resolved = {
        "db_path": str(db_path.resolve(strict=False)),
        "db_exists": db_path.exists(),
        "out_path": str(out_path.resolve(strict=False)),
        "config_path": str(config_path.resolve(strict=False)),
        "config_exists": config_path.exists(),
        "territories": str(args.territories).strip() or "AUTO",
        "rows_per_territory": int(args.rows_per_territory),
        "lookback_days": int(args.lookback_days),
        "dry_run": bool(args.dry_run),
    }

    if args.print_config:
        print("PASS_SAMPLE_FEED_PRINT_CONFIG")
        _print_json(resolved)
        return 0

    if not db_path.exists():
        print(f"Database not found: {db_path}", file=sys.stderr)
        return 2
    if not config_path.exists():
        print(f"Config not found: {config_path}", file=sys.stderr)
        return 2

    payload, stats = build_sample_feed(
        db_path=db_path,
        config_path=config_path,
        territories_arg=args.territories,
        rows_per_territory=max(1, int(args.rows_per_territory)),
        lookback_days=max(0, int(args.lookback_days)),
    )

    if args.dry_run:
        print("PASS_SAMPLE_FEED_DRY_RUN")
        _print_json({"resolved": resolved, "stats": stats, "preview": payload})
        return 0

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
    print("PASS_SAMPLE_FEED_WRITTEN")
    _print_json({"out_path": str(out_path.resolve(strict=False)), "stats": stats})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
