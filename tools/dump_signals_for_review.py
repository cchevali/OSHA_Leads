#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sqlite3
import sys
from datetime import date, datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from lead_filters import load_territory_definitions, resolve_territory_code
from scoring import paths as scoring_paths
from scoring import triage_overlay

import send_digest_email as sde


def _parse_date(value: str) -> date:
    return datetime.strptime(str(value or "").strip(), "%Y-%m-%d").date()


def _format_signal_block(lead: dict, decision: dict) -> str:
    activity_nr = str(lead.get("activity_nr") or "").strip()
    name = str(lead.get("establishment_name") or "").strip() or "Unknown"
    city = str(lead.get("site_city") or "").strip()
    state = str(lead.get("site_state") or "").strip().upper()
    site_zip = str(lead.get("site_zip") or "").strip()
    naics = str(lead.get("naics") or "").strip()
    naics_desc = str(lead.get("naics_desc") or "").strip()
    inspection_type = str(lead.get("inspection_type") or "").strip()
    scope = str(lead.get("scope") or "").strip()
    case_status = str(lead.get("case_status") or "").strip()
    date_opened = str(lead.get("date_opened") or "").strip()
    first_seen = str(lead.get("first_seen_at") or "").strip()
    rules_priority = str(decision.get("rules_priority") or "").strip().upper() or "LOW"
    reasons = ",".join([str(x).strip() for x in (decision.get("reasons") or []) if str(x).strip()]) or "rules_default"

    return (
        f"SIGNAL {activity_nr} | {name} | {city} {state} {site_zip}\n"
        f"  NAICS {naics} {naics_desc} | {inspection_type} | {scope} | {case_status}\n"
        f"  Date opened: {date_opened} | First seen: {first_seen}\n"
        f"  Rules priority: {rules_priority} ({reasons})\n"
    )


def main() -> int:
    ap = argparse.ArgumentParser(description="Dump territory OSHA signals for manual priority review.")
    ap.add_argument("--territory", required=True, help="Territory code (for example TX_TRI).")
    ap.add_argument("--since", default=date.today().isoformat(), help="Inclusive start date (YYYY-MM-DD).")
    ap.add_argument("--until", default="", help="Inclusive end date (YYYY-MM-DD). Defaults to --since.")
    ap.add_argument(
        "--db",
        default=str(scoring_paths.default_leads_db_path()),
        help="SQLite inspections DB path (default: data/osha.sqlite).",
    )
    ap.add_argument("--print-config", action="store_true", help="Print resolved config and exit.")
    ap.add_argument("--dry-run", action="store_true", help="Do not write output file.")
    args = ap.parse_args()

    try:
        since_date = _parse_date(args.since)
        until_date = _parse_date(args.until) if str(args.until or "").strip() else since_date
    except Exception:
        print("ERR_SIGNAL_REVIEW_INVALID_DATE", file=sys.stderr)
        return 2
    if until_date < since_date:
        print("ERR_SIGNAL_REVIEW_INVALID_RANGE", file=sys.stderr)
        return 2

    definitions = load_territory_definitions()
    territory_code = resolve_territory_code(str(args.territory or ""), definitions)
    if territory_code not in definitions:
        print(f"ERR_SIGNAL_REVIEW_UNKNOWN_TERRITORY code={args.territory}", file=sys.stderr)
        return 2
    states = [str(s).strip().upper() for s in (definitions[territory_code].get("states") or []) if str(s).strip()]
    if not states:
        print(f"ERR_SIGNAL_REVIEW_TERRITORY_NO_STATES code={territory_code}", file=sys.stderr)
        return 2

    db_path = Path(str(args.db)).expanduser().resolve(strict=False)
    out_dir = scoring_paths.data_root() / "audits"
    out_path = out_dir / f"signals_for_review_{date.today().strftime('%Y%m%d')}.txt"

    if args.print_config:
        print(f"territory={territory_code}")
        print(f"states={','.join(states)}")
        print(f"since={since_date.isoformat()}")
        print(f"until={until_date.isoformat()}")
        print(f"db={db_path}")
        print(f"out={out_path}")
        return 0

    if not db_path.exists():
        print(f"ERR_SIGNAL_REVIEW_DB_MISSING path={db_path}", file=sys.stderr)
        return 2

    conn = sqlite3.connect(str(db_path))
    try:
        since_days = max(1, int((date.today() - since_date).days) + 1)
        leads, _low_fallback, _stats = sde.get_leads_for_period(
            conn=conn,
            states=states,
            since_days=since_days,
            new_only_days=36500,
            skip_first_seen_filter=True,
            territory_code=territory_code,
            content_filter="all",
            include_low_fallback=False,
            include_changed=True,
            use_opened_window=True,
        )
    finally:
        conn.close()

    selected: list[dict] = []
    for lead in list(leads or []):
        opened = str(lead.get("date_opened") or "").strip()
        try:
            opened_date = _parse_date(opened)
        except Exception:
            continue
        if since_date <= opened_date <= until_date:
            selected.append(dict(lead))

    decisions = triage_overlay.triage(selected, {}, mode="trial_render", allow_ai=False)
    by_key = triage_overlay.decisions_by_activity(decisions)

    blocks: list[str] = []
    for lead in sorted(selected, key=lambda r: (str(r.get("date_opened") or ""), str(r.get("activity_nr") or "")), reverse=True):
        key = str(lead.get("activity_nr") or lead.get("lead_key") or "").strip()
        decision = by_key.get(key, {})
        blocks.append(_format_signal_block(lead, decision))

    output_text = "\n".join(blocks).strip() + ("\n" if blocks else "")
    if output_text:
        print(output_text, end="")
    else:
        print("NO_SIGNALS_FOR_REVIEW")

    if not args.dry_run:
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path.write_text(output_text, encoding="utf-8")
        print(f"SIGNAL_REVIEW_OUT={out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
