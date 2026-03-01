#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import crm_light
from lead_filters import load_territory_definitions, resolve_territory_code
from scoring import paths as scoring_paths
from scoring import triage_overlay

import send_digest_email as sde

AI_REVIEW_HEADER_LINES = [
    "# AI SIGNAL TRIAGE REVIEW",
    "# Classify each non-suppressed signal below for relevance to independent",
    "# safety consultants and OSHA defense attorneys who serve small to mid-size",
    "# employers in construction, manufacturing, and industrial trades.",
    "#",
    "# For each signal, return a CSV row with:",
    "#   activity_nr, ai_priority, ai_reason",
    "#",
    "# Priority definitions:",
    "#   HIGH - Active inspection of a high-hazard trade employer, referral/complaint",
    "#          trigger, OSHA emphasis program NAICS, multi-employer site, or company",
    "#          clearly needing external safety help",
    "#   MEDIUM - Active inspection with moderate hazard profile, non-emphasis",
    "#            construction/industrial, or ambiguous company profile",
    "#   LOW - Routine planned inspection of low-hazard employer, or minimal",
    "#         information content",
    "#",
    "# Rules:",
    "#   - You may only RAISE priority above the rules_priority shown. Never lower it.",
    "#   - Do not classify SUPPRESS signals. Skip them entirely.",
    "#   - Consider: company name, NAICS description, inspection type, scope,",
    "#     multi-employer patterns (same address), and whether the company profile",
    "#     fits the target buyer audience.",
    "#",
    "# Return ONLY the CSV block. No commentary before or after.",
    "# Format:",
    "#   activity_nr,ai_priority,ai_reason",
]

AI_REVIEW_FOOTER_LINES = [
    "# --- END OF SIGNALS ---",
    "# Return CSV now. Headers: activity_nr,ai_priority,ai_reason",
]


def _parse_date(value: str) -> date:
    text = str(value or "").strip().lower()
    if text == "today":
        return date.today()
    return datetime.strptime(text, "%Y-%m-%d").date()


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


def _parse_outreach_states(raw: str) -> list[str]:
    parts = [str(x or "").strip().upper() for x in str(raw or "").split(",")]
    states: list[str] = []
    seen: set[str] = set()
    for value in parts:
        if not value:
            continue
        if len(value) != 2 or not value.isalpha():
            continue
        if value in seen:
            continue
        seen.add(value)
        states.append(value)
    return states


def _load_trial_territories_from_customers(definitions: dict[str, dict[str, Any]]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    customers_dir = REPO_ROOT / "customers"
    if not customers_dir.exists():
        return out
    for path in sorted(customers_dir.glob("*.json"), key=lambda p: str(p).lower()):
        if str(path.name).lower().endswith(".example.json"):
            continue
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(payload, dict):
            continue
        customer_id = str(payload.get("customer_id") or "").strip().lower()
        subscriber_key = str(payload.get("subscriber_key") or "").strip().lower()
        if "trial" not in customer_id and "trial" not in subscriber_key and "trial" not in path.stem.lower():
            continue
        raw_code = str(payload.get("territory_code") or "").strip()
        if not raw_code:
            continue
        canonical = resolve_territory_code(raw_code, definitions)
        if canonical not in definitions:
            continue
        if canonical in seen:
            continue
        seen.add(canonical)
        out.append(canonical)
    return out


def _load_trial_territories_from_crm_light(definitions: dict[str, dict[str, Any]]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    try:
        crm_db_path = crm_light.crm_light_db_path()
    except Exception:
        return out
    if not crm_db_path.exists():
        return out
    try:
        conn = sqlite3.connect(str(crm_db_path))
    except Exception:
        return out
    try:
        table_row = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='subscribers' LIMIT 1"
        ).fetchone()
        if not table_row:
            return out
        rows = conn.execute(
            """
            SELECT DISTINCT trim(territory_code) AS territory_code
            FROM subscribers
            WHERE territory_code IS NOT NULL
              AND trim(territory_code) <> ''
              AND lower(trim(status)) = 'trial'
            ORDER BY upper(trim(territory_code))
            """
        ).fetchall()
        for row in rows:
            raw_code = str((row[0] if row else "") or "").strip()
            if not raw_code:
                continue
            canonical = resolve_territory_code(raw_code, definitions)
            if canonical not in definitions:
                continue
            if canonical in seen:
                continue
            seen.add(canonical)
            out.append(canonical)
    except Exception:
        return out
    finally:
        try:
            conn.close()
        except Exception:
            pass
    return out


def _configured_trial_territory_codes(definitions: dict[str, dict[str, Any]]) -> list[str]:
    combined: list[str] = []
    seen: set[str] = set()
    for code in _load_trial_territories_from_customers(definitions):
        if code in seen:
            continue
        seen.add(code)
        combined.append(code)
    for code in _load_trial_territories_from_crm_light(definitions):
        if code in seen:
            continue
        seen.add(code)
        combined.append(code)
    return combined


def _group_for_state(state: str) -> dict[str, Any]:
    return {
        "kind": "state",
        "code": state,
        "states": [state],
        "territory_code": "",
        "header": f"===== STATE {state} =====",
    }


def _group_for_territory(code: str, definitions: dict[str, dict[str, Any]]) -> dict[str, Any]:
    states = [str(s).strip().upper() for s in (definitions.get(code, {}).get("states") or []) if str(s).strip()]
    return {
        "kind": "territory",
        "code": code,
        "states": states,
        "territory_code": code,
        "header": f"===== TERRITORY {code} =====",
    }


def _fetch_selected_for_group(
    conn: sqlite3.Connection,
    *,
    since_date: date,
    until_date: date,
    states: list[str],
    territory_code: str,
) -> list[dict[str, Any]]:
    since_days = max(1, int((date.today() - since_date).days) + 1)
    leads, _low_fallback, _stats = sde.get_leads_for_period(
        conn=conn,
        states=states,
        since_days=since_days,
        new_only_days=36500,
        skip_first_seen_filter=True,
        territory_code=territory_code or None,
        content_filter="all",
        include_low_fallback=False,
        include_changed=True,
        use_opened_window=True,
    )
    selected: list[dict[str, Any]] = []
    for lead in list(leads or []):
        opened = str(lead.get("date_opened") or "").strip()
        try:
            opened_date = _parse_date(opened)
        except Exception:
            continue
        if since_date <= opened_date <= until_date:
            selected.append(dict(lead))
    return selected


def _render_group_blocks(selected: list[dict[str, Any]]) -> list[str]:
    decisions = triage_overlay.triage(selected, {}, mode="trial_render", allow_ai=False)
    by_key = triage_overlay.decisions_by_activity(decisions)
    blocks: list[str] = []
    for lead in sorted(
        selected,
        key=lambda r: (str(r.get("date_opened") or ""), str(r.get("activity_nr") or "")),
        reverse=True,
    ):
        key = str(lead.get("activity_nr") or lead.get("lead_key") or "").strip()
        decision = by_key.get(key, {})
        blocks.append(_format_signal_block(lead, decision))
    return blocks


def main() -> int:
    ap = argparse.ArgumentParser(description="Dump territory OSHA signals for manual priority review.")
    ap.add_argument("--territory", default="", help="Territory code (for example TX_TRI).")
    ap.add_argument(
        "--all-outreach",
        action="store_true",
        help="Dump all OUTREACH_STATES plus configured trial territories in one grouped output.",
    )
    ap.add_argument(
        "--for-ai-review",
        action="store_true",
        help="Wrap output in a self-contained AI triage prompt header/footer.",
    )
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
    groups: list[dict[str, Any]] = []
    territory_code = ""
    states: list[str] = []
    trial_territories: list[str] = []

    if args.all_outreach:
        states = _parse_outreach_states(os.getenv("OUTREACH_STATES", ""))
        if not states:
            print("ERR_SIGNAL_REVIEW_OUTREACH_STATES_MISSING", file=sys.stderr)
            return 2
        for state in states:
            groups.append(_group_for_state(state))
        trial_territories = sorted(_configured_trial_territory_codes(definitions))
        for code in trial_territories:
            group = _group_for_territory(code, definitions)
            if not group["states"]:
                continue
            groups.append(group)
    else:
        if not str(args.territory or "").strip():
            print("ERR_SIGNAL_REVIEW_TERRITORY_REQUIRED", file=sys.stderr)
            return 2
        territory_code = resolve_territory_code(str(args.territory or ""), definitions)
        if territory_code not in definitions:
            print(f"ERR_SIGNAL_REVIEW_UNKNOWN_TERRITORY code={args.territory}", file=sys.stderr)
            return 2
        states = [str(s).strip().upper() for s in (definitions[territory_code].get("states") or []) if str(s).strip()]
        if not states:
            print(f"ERR_SIGNAL_REVIEW_TERRITORY_NO_STATES code={territory_code}", file=sys.stderr)
            return 2
        groups.append(_group_for_territory(territory_code, definitions))

    db_path = Path(str(args.db)).expanduser().resolve(strict=False)
    out_dir = scoring_paths.data_root() / "audits"
    if args.for_ai_review:
        out_path = out_dir / f"signals_for_ai_review_{date.today().strftime('%Y%m%d')}.txt"
    else:
        out_path = out_dir / f"signals_for_review_{date.today().strftime('%Y%m%d')}.txt"

    if args.print_config:
        if args.all_outreach:
            print("mode=all_outreach")
            print(f"outreach_states={','.join(states)}")
            print(f"trial_territories={','.join(trial_territories)}")
            print(f"group_count={len(groups)}")
        else:
            print("mode=single_territory")
            print(f"territory={territory_code}")
        print(f"for_ai_review={1 if args.for_ai_review else 0}")
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
        rendered_sections: list[str] = []
        for group in groups:
            selected = _fetch_selected_for_group(
                conn,
                since_date=since_date,
                until_date=until_date,
                states=list(group.get("states") or []),
                territory_code=str(group.get("territory_code") or ""),
            )
            if args.all_outreach:
                rendered_sections.append(str(group.get("header") or "").strip())
            blocks = _render_group_blocks(selected)
            if blocks:
                rendered_sections.extend(blocks)
            else:
                rendered_sections.append("NO_SIGNALS_FOR_REVIEW")
            if args.all_outreach:
                rendered_sections.append("")
    finally:
        conn.close()

    body_text = "\n".join([str(x) for x in rendered_sections]).strip()
    if args.for_ai_review:
        parts = ["\n".join(AI_REVIEW_HEADER_LINES).strip()]
        if body_text:
            parts.append(body_text)
        else:
            parts.append("NO_SIGNALS_FOR_REVIEW")
        parts.append("\n".join(AI_REVIEW_FOOTER_LINES).strip())
        output_text = "\n\n".join([str(p).strip() for p in parts if str(p).strip()]) + "\n"
    else:
        output_text = body_text + "\n" if body_text else ""
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
