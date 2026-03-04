#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import tempfile
import warnings
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

warnings.filterwarnings(
    "ignore",
    message=r"urllib3 .*doesn't match a supported version!",
    category=Warning,
    module=r"requests\.__init__",
)

import crm_light
from lead_filters import load_territory_definitions, resolve_territory_code
from scoring import paths as scoring_paths
from scoring import triage_overlay

import send_digest_email as sde
from runtime_data_dir import resolve_data_dir

AI_REVIEW_HEADER_LINES = [
    "# ============================================================",
    "# MICROFLOWOPS — NIGHTLY SIGNAL TRIAGE REVIEW",
    "# ============================================================",
    "#",
    "# CONTEXT:",
    "# MicroFlowOps delivers daily OSHA inspection alerts to two audiences:",
    "#   1. Trial/paid subscribers — safety consultants and OSHA defense",
    "#      attorneys who receive territory-filtered daily digests",
    "#   2. Cold outreach prospects — safety consultants and OSHA defense",
    "#      attorneys who receive cold emails containing recent high-value",
    "#      OSHA signal examples from their state",
    "#",
    "# Both audiences are employer-side professionals who help small to",
    "# mid-size companies (under 500 employees) in construction,",
    "# manufacturing, and industrial trades respond to OSHA enforcement",
    "# activity. They use these signals to identify businesses that may",
    "# need their services RIGHT NOW — before citations post publicly.",
    "#",
    "# THE BUSINESS VALUE OF A SIGNAL depends on:",
    "#   - Would a safety consultant or OSHA defense attorney want to",
    "#     contact this company based on this inspection?",
    "#   - Is this company the RIGHT SIZE for external help? (Solo shops",
    "#     to mid-size. NOT national enterprises with in-house EHS teams.)",
    "#   - Is the HAZARD PROFILE meaningful? (Construction, industrial,",
    "#     manufacturing >> janitorial, retail, food service)",
    "#   - Does the INSPECTION TYPE suggest urgency? (Referral/Complaint",
    "#     = someone reported them. Accident = injury occurred. These are",
    "#     far more urgent than routine Planned inspections.)",
    "#   - Is there a PATTERN? (Multiple inspections at the same address",
    "#     = multi-employer site enforcement action, very high value.)",
    "#   - Would this signal make our digest or cold email look credible",
    "#     and valuable, or would it make us look like we don't understand",
    "#     the industry?",
    "#",
    "# WHAT YOU ARE DOING:",
    "# Below are OSHA inspection signals with a rules-based priority",
    "# already assigned. Rules handle structural patterns well (NAICS",
    "# codes, inspection types, closed cases) but miss contextual",
    "# judgment calls about company type, hazard inference from company",
    "# names, and multi-signal patterns.",
    "#",
    "# You have FULL AUTHORITY to raise or lower any non-suppressed",
    "# signal's priority. If rules say LOW and you see a trenching",
    "# contractor that belongs at HIGH, raise it. If rules say HIGH",
    "# but the company is a massive national chain that would never",
    "# hire an independent safety consultant, lower it.",
    "#",
    "# PRIORITY DEFINITIONS:",
    "#   HIGH   — Clear, actionable signal. A safety consultant would",
    "#            want to call this company today. Referrals/complaints",
    "#            at construction or industrial employers, emphasis",
    "#            program NAICS, multi-employer sites, or any signal",
    "#            where the need for external safety help is obvious.",
    "#   MEDIUM — Moderate value. Worth including in a digest but not",
    "#            a top prospect. Active inspections at construction or",
    "#            industrial employers without strong urgency indicators.",
    "#   LOW    — Minimal value. Routine planned inspection, low-hazard",
    "#            industry, large enterprise, or insufficient information",
    "#            to assess relevance.",
    "#",
    "# RULES:",
    "#   - You may RAISE or LOWER priority vs rules_priority.",
    "#   - For any LOWERING, your reason must explain why this signal is",
    "#     less relevant to our audience than rules suggest.",
    "#   - SUPPRESS signals are already removed by deterministic rules",
    "#     (closed/no-inspection, stale >30 days, non-target industry).",
    "#     Do not classify them. Skip them entirely.",
    "#   - Return ONLY the CSV block. No commentary before or after.",
    "#",
    "# OUTPUT FORMAT (CSV):",
    "#   activity_nr,ai_priority,ai_reason",
    "#   Do not use commas inside the ai_reason field. Use semicolons or dashes instead.",
    "#",
    "# ============================================================",
]

AI_REVIEW_FOOTER_LINES = [
    "# --- END OF SIGNALS ---",
    "# Classify all non-suppressed signals above.",
    "# Return ONLY: activity_nr,ai_priority,ai_reason",
    "# One row per non-suppressed signal. No extra text.",
]


def _local_today_date() -> date:
    return datetime.now().astimezone().date()


def _parse_date(value: str) -> date:
    text = str(value or "").strip().lower()
    if text == "today":
        return _local_today_date()
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


def _parse_timestamp(value: str) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(text)
    except Exception:
        return None


def _parse_outreach_states(raw: str | list[str]) -> list[str]:
    parts: list[str] = []
    if isinstance(raw, list):
        for token in raw:
            parts.extend([str(x or "").strip().upper() for x in str(token or "").split(",")])
    else:
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


def _as_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    dt = value
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _fetch_selected_for_group(
    conn: sqlite3.Connection,
    *,
    since_date: date,
    until_date: date,
    states: list[str],
    territory_code: str,
    strict_after: datetime | None = None,
    subscriber_cbsa_allowlist: list[str] | None = None,
) -> tuple[list[dict[str, Any]], int, int]:
    since_days = max(1, int((_local_today_date() - since_date).days) + 1)
    strict_after_utc = _as_utc(strict_after)
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
        subscriber_cbsa_allowlist=list(subscriber_cbsa_allowlist or []),
    )
    selected: list[dict[str, Any]] = []
    matched_by_first_seen = 0
    matched_by_opened_fallback = 0
    for lead in list(leads or []):
        first_seen_dt = _parse_timestamp(str(lead.get("first_seen_at") or ""))
        if first_seen_dt is not None:
            first_seen_utc = _as_utc(first_seen_dt)
            if first_seen_utc is None:
                continue
            if strict_after_utc is not None and first_seen_utc <= strict_after_utc:
                continue
            first_seen_local = first_seen_utc.astimezone().date()
            if since_date <= first_seen_local <= until_date:
                selected.append(dict(lead))
                matched_by_first_seen += 1
            continue
        opened = str(lead.get("date_opened") or "").strip()
        try:
            opened_date = _parse_date(opened)
        except Exception:
            continue
        if strict_after_utc is not None and opened_date <= strict_after_utc.astimezone().date():
            continue
        if since_date <= opened_date <= until_date:
            selected.append(dict(lead))
            matched_by_opened_fallback += 1
    return selected, matched_by_first_seen, matched_by_opened_fallback


def _render_group_sections(
    selected: list[dict[str, Any]],
    *,
    include_suppressed: bool,
) -> tuple[list[str], list[str]]:
    decisions = triage_overlay.triage(selected, {}, mode="trial_render", allow_ai=False)
    by_key = triage_overlay.decisions_by_activity(decisions)
    action_blocks: list[str] = []
    suppressed_blocks: list[str] = []
    for lead in sorted(
        selected,
        key=lambda r: (str(r.get("date_opened") or ""), str(r.get("activity_nr") or "")),
        reverse=True,
    ):
        key = str(lead.get("activity_nr") or lead.get("lead_key") or "").strip()
        decision = by_key.get(key, {})
        rules_priority = str(decision.get("rules_priority") or "").strip().upper()
        if rules_priority == "SUPPRESS":
            if include_suppressed:
                suppressed_blocks.append(_format_signal_block(lead, decision))
            continue
        action_blocks.append(_format_signal_block(lead, decision))
    return action_blocks, suppressed_blocks


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM sqlite_master
        WHERE type = 'table' AND name = ?
        LIMIT 1
        """,
        (str(table or "").strip(),),
    ).fetchone()
    return row is not None


def _table_has_column(conn: sqlite3.Connection, table: str, column: str) -> bool:
    try:
        rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    except Exception:
        return False
    return any(str(row[1]).strip().lower() == str(column).strip().lower() for row in rows)


def _parse_recipient_json(raw: str) -> list[str]:
    text = str(raw or "").strip()
    if not text:
        return []
    try:
        payload = json.loads(text)
    except Exception:
        return []
    if not isinstance(payload, list):
        return []
    out: list[str] = []
    seen: set[str] = set()
    for item in payload:
        email = ""
        if isinstance(item, dict):
            email = crm_light.normalize_email(str(item.get("email") or ""))
        else:
            email = crm_light.normalize_email(str(item or ""))
        if not email or email in seen:
            continue
        seen.add(email)
        out.append(email)
    return out


def _subscriber_states_for_scope(territory_code: str, definitions: dict[str, dict[str, Any]]) -> tuple[str, list[str]]:
    raw = str(territory_code or "").strip()
    if not raw:
        return "", []
    canonical = resolve_territory_code(raw, definitions)
    if canonical in definitions:
        states = [str(s).strip().upper() for s in (definitions[canonical].get("states") or []) if str(s).strip()]
        return canonical, states
    fallback = raw.strip().upper()
    if len(fallback) == 2 and fallback.isalpha():
        return "", [fallback]
    return "", []


def _load_subscriber_review_targets(definitions: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    try:
        crm_db_path = crm_light.crm_light_db_path()
    except Exception:
        return []
    if not crm_db_path.exists():
        return []
    try:
        conn = sqlite3.connect(str(crm_db_path))
        conn.row_factory = sqlite3.Row
    except Exception:
        return []

    try:
        if not _table_exists(conn, "subscribers"):
            return []
        status_column = "status" if _table_has_column(conn, "subscribers", "status") else ""
        if not status_column:
            return []

        has_entitlements = _table_exists(conn, "subscriber_entitlements")
        has_recipients_json = has_entitlements and _table_has_column(conn, "subscriber_entitlements", "recipients_json")
        has_entitlement_email = has_entitlements and _table_has_column(conn, "subscriber_entitlements", "email")
        has_cbsa = _table_exists(conn, "subscriber_cbsa")

        ent_recipients_expr = "se.recipients_json AS entitlement_recipients_json" if has_recipients_json else "'' AS entitlement_recipients_json"
        ent_email_expr = "se.email AS entitlement_email" if has_entitlement_email else "'' AS entitlement_email"
        join_entitlements = "LEFT JOIN subscriber_entitlements se ON se.subscriber_key = s.subscriber_key" if has_entitlements else ""

        rows = conn.execute(
            f"""
            SELECT
                s.subscriber_key AS subscriber_key,
                s.email AS subscriber_email,
                s.territory_code AS territory_code,
                s.status AS subscriber_status,
                {ent_recipients_expr},
                {ent_email_expr}
            FROM subscribers s
            {join_entitlements}
            WHERE lower(trim(s.status)) IN ('trial', 'live', 'paid', 'active')
            ORDER BY lower(trim(s.status)), lower(trim(s.subscriber_key))
            """
        ).fetchall()

        targets: list[dict[str, Any]] = []
        for row in rows:
            sk = crm_light.normalize_subscriber_key(str(row["subscriber_key"] or ""))
            if not sk:
                continue
            status = str(row["subscriber_status"] or "").strip().lower()
            territory, states = _subscriber_states_for_scope(str(row["territory_code"] or ""), definitions)
            if not states:
                continue
            recipients = _parse_recipient_json(str(row["entitlement_recipients_json"] or ""))
            if not recipients:
                primary = crm_light.normalize_email(str(row["subscriber_email"] or ""))
                if primary:
                    recipients = [primary]
                else:
                    entitlement_email = crm_light.normalize_email(str(row["entitlement_email"] or ""))
                    if entitlement_email:
                        recipients = [entitlement_email]
            if not recipients:
                continue
            cbsa_allowlist: list[str] = []
            if has_cbsa:
                try:
                    cbsa_allowlist = crm_light.get_subscriber_cbsa_allowlist(conn, sk)
                except Exception:
                    cbsa_allowlist = []
            targets.append(
                {
                    "subscriber_key": sk,
                    "status": status,
                    "territory_code": territory,
                    "states": states,
                    "recipients": recipients,
                    "subscriber_email": crm_light.normalize_email(str(row["subscriber_email"] or "")),
                    "cbsa_allowlist": cbsa_allowlist,
                }
            )
        return targets
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _resolve_default_audits_dir() -> Path:
    return (resolve_data_dir(REPO_ROOT).effective_path / "audits").resolve(strict=False)


def _resolve_output_path(
    *,
    output: str,
    output_dir: str,
    for_ai_review: bool,
    today_local: date,
) -> tuple[Path, Path]:
    filename = (
        f"signals_for_ai_review_{today_local.strftime('%Y%m%d')}.txt"
        if for_ai_review
        else f"signals_for_review_{today_local.strftime('%Y%m%d')}.txt"
    )
    if str(output or "").strip():
        out_path = Path(str(output).strip()).expanduser().resolve(strict=False)
    else:
        if str(output_dir or "").strip():
            out_dir = Path(str(output_dir).strip()).expanduser().resolve(strict=False)
        else:
            out_dir = _resolve_default_audits_dir()
        out_path = (out_dir / filename).resolve(strict=False)
    return out_path.parent.resolve(strict=False), out_path.resolve(strict=False)


def _max_first_seen_iso(leads: list[dict[str, Any]]) -> str:
    max_dt: datetime | None = None
    for lead in list(leads or []):
        dt = _parse_timestamp(str(lead.get("first_seen_at") or ""))
        if not dt:
            continue
        if max_dt is None or dt > max_dt:
            max_dt = dt
    return max_dt.isoformat() if max_dt else ""


def _max_date_opened_iso(leads: list[dict[str, Any]]) -> str:
    max_date_opened: date | None = None
    for lead in list(leads or []):
        try:
            opened = _parse_date(str(lead.get("date_opened") or ""))
        except Exception:
            continue
        if max_date_opened is None or opened > max_date_opened:
            max_date_opened = opened
    return max_date_opened.isoformat() if max_date_opened else ""


def _atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_fd, tmp_name = tempfile.mkstemp(prefix=f"{path.name}.", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(tmp_fd, "w", encoding="utf-8", newline="") as handle:
            handle.write(text)
        os.replace(tmp_name, str(path))
    finally:
        try:
            if os.path.exists(tmp_name):
                os.unlink(tmp_name)
        except Exception:
            pass


def main() -> int:
    ap = argparse.ArgumentParser(description="Dump territory OSHA signals for manual priority review.")
    ap.add_argument("--territory", default="", help="Territory code (for example TX_TRI).")
    ap.add_argument(
        "--states",
        nargs="+",
        default=[],
        help="Explicit state scope (comma-separated or list form, for example: CA,OR,WA).",
    )
    ap.add_argument(
        "--all-outreach",
        action="store_true",
        help="Dump all OUTREACH_STATES in one grouped output.",
    )
    ap.add_argument(
        "--for-ai-review",
        action="store_true",
        help="Wrap output in a self-contained AI triage prompt header/footer.",
    )
    ap.add_argument("--since", default=_local_today_date().isoformat(), help="Inclusive start date (YYYY-MM-DD).")
    ap.add_argument("--until", default="", help="Inclusive end date (YYYY-MM-DD). Defaults to local today.")
    ap.add_argument(
        "--db",
        default=str(scoring_paths.default_leads_db_path()),
        help="SQLite inspections DB path (default: data/osha.sqlite).",
    )
    ap.add_argument("--print-config", action="store_true", help="Print resolved config and exit.")
    ap.add_argument("--dry-run", action="store_true", help="Do not write output file.")
    ap.add_argument("--output-dir", default="", help="Optional output directory override.")
    ap.add_argument("--output", default="", help="Optional full output file path override.")
    ap.add_argument("--include-suppressed", action="store_true", help="Include SUPPRESS signals under a skip section.")
    args = ap.parse_args()

    raw_until = str(args.until or "").strip()
    try:
        since_date = _parse_date(args.since)
        until_date = _parse_date(raw_until) if raw_until else _local_today_date()
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
    states_arg_present = bool(list(args.states or []))
    states_scope = _parse_outreach_states(list(args.states or []))
    territory_arg = str(args.territory or "").strip()
    if states_arg_present and not states_scope:
        print("ERR_SIGNAL_REVIEW_STATES_REQUIRED", file=sys.stderr)
        return 2
    scope_count = int(bool(args.all_outreach)) + int(bool(territory_arg)) + int(states_arg_present)
    if scope_count > 1:
        print("ERR_SIGNAL_REVIEW_SCOPE_CONFLICT", file=sys.stderr)
        return 2

    effective_all_outreach = bool(args.all_outreach)
    if args.print_config and scope_count == 0:
        effective_all_outreach = True

    if states_scope:
        states = list(states_scope)
        groups.append(
            {
                "kind": "states",
                "code": ",".join(states),
                "states": list(states),
                "territory_code": "",
                "header": f"===== STATES {','.join(states)} =====",
            }
        )
    elif effective_all_outreach:
        states = _parse_outreach_states(os.getenv("OUTREACH_STATES", ""))
        if not states:
            print("ERR_SIGNAL_REVIEW_OUTREACH_STATES_MISSING", file=sys.stderr)
            return 2
        for state in states:
            groups.append(_group_for_state(state))
    else:
        if not territory_arg:
            print("ERR_SIGNAL_REVIEW_TERRITORY_REQUIRED", file=sys.stderr)
            return 2
        territory_code = resolve_territory_code(territory_arg, definitions)
        if territory_code not in definitions:
            print(f"ERR_SIGNAL_REVIEW_UNKNOWN_TERRITORY code={args.territory}", file=sys.stderr)
            return 2
        states = [str(s).strip().upper() for s in (definitions[territory_code].get("states") or []) if str(s).strip()]
        if not states:
            print(f"ERR_SIGNAL_REVIEW_TERRITORY_NO_STATES code={territory_code}", file=sys.stderr)
            return 2
        groups.append(_group_for_territory(territory_code, definitions))

    db_path = Path(str(args.db)).expanduser().resolve(strict=False)
    today_local = _local_today_date()
    out_dir, out_path = _resolve_output_path(
        output=str(args.output or ""),
        output_dir=str(args.output_dir or ""),
        for_ai_review=bool(args.for_ai_review),
        today_local=today_local,
    )
    data_dir_resolution = resolve_data_dir(REPO_ROOT)
    effective_data_dir = str(data_dir_resolution.effective_path)
    data_dir_source = str(data_dir_resolution.source or "default")
    states_csv = ",".join(states)
    territories_csv = ",".join(([territory_code] if territory_code else []))
    if data_dir_resolution.warning_token:
        print(data_dir_resolution.warning_token)
    if states_scope:
        print(f"AI_REVIEW_DUMP_SCOPE=STATES states={states_csv}")
    print(f"AI_REVIEW_DUMP_OUTPUT_DIR={out_dir}")
    print(f"AI_REVIEW_DUMP_OUTPUT_PATH={out_path}")
    print("AI_REVIEW_DUMP_FILTER_BASIS=FIRST_SEEN_FALLBACK_OPENED")

    if args.print_config:
        print(f"AI_REVIEW_DUMP_DATA_DIR={effective_data_dir}")
        print(f"AI_REVIEW_DUMP_DATA_DIR_SOURCE={data_dir_source}")
        print(f"AI_REVIEW_DUMP_OUTPUT_DIR={out_dir}")
        print(f"AI_REVIEW_DUMP_OUTPUT_PATH={out_path}")
        print(f"AI_REVIEW_DUMP_SINCE={since_date.isoformat()}")
        print(f"AI_REVIEW_DUMP_UNTIL={until_date.isoformat()}")
        print(f"AI_REVIEW_DUMP_STATES={states_csv}")
        print(f"AI_REVIEW_DUMP_TERRITORIES={territories_csv}")
        return 0

    if not db_path.exists():
        print(f"ERR_SIGNAL_REVIEW_DB_MISSING path={db_path}", file=sys.stderr)
        return 2

    conn = sqlite3.connect(str(db_path))
    try:
        rendered_sections: list[str] = []
        outreach_actionable = 0
        subscribers_actionable = 0
        all_selected: list[dict[str, Any]] = []
        total_matched_by_first_seen = 0
        total_matched_by_opened_fallback = 0
        rolling_days = max(0, int((until_date - since_date).days))
        if effective_all_outreach:
            rendered_sections.append(f"OUTREACH STATES — rolling {rolling_days} days through {until_date.isoformat()}")
            rendered_sections.append("")
        for group in groups:
            selected, matched_by_first_seen, matched_by_opened_fallback = _fetch_selected_for_group(
                conn,
                since_date=since_date,
                until_date=until_date,
                states=list(group.get("states") or []),
                territory_code=str(group.get("territory_code") or ""),
            )
            total_matched_by_first_seen += int(matched_by_first_seen)
            total_matched_by_opened_fallback += int(matched_by_opened_fallback)
            all_selected.extend(list(selected or []))
            if effective_all_outreach:
                rendered_sections.append(str(group.get("header") or "").strip())
            action_blocks, suppressed_blocks = _render_group_sections(
                selected,
                include_suppressed=bool(args.include_suppressed),
            )
            outreach_actionable += len(action_blocks)
            if action_blocks:
                rendered_sections.extend(action_blocks)
            else:
                if not args.for_ai_review:
                    rendered_sections.append("NO_SIGNALS_FOR_REVIEW")
            if args.include_suppressed and suppressed_blocks:
                rendered_sections.append("SUPPRESSED (skip)")
                rendered_sections.extend(suppressed_blocks)
            if effective_all_outreach:
                rendered_sections.append("")

        if effective_all_outreach:
            rendered_sections.append(f"TRIAL/LIVE SUBSCRIBERS — since last successful send through {until_date.isoformat()}")
            rendered_sections.append("")
            subscriber_targets = _load_subscriber_review_targets(definitions)
            if not subscriber_targets:
                rendered_sections.append("NO_SUBSCRIBERS_FOR_REVIEW")
                rendered_sections.append("")
            else:
                crm_conn: sqlite3.Connection | None = None
                try:
                    crm_db_path = crm_light.crm_light_db_path()
                    if crm_db_path.exists():
                        crm_conn = sqlite3.connect(str(crm_db_path))
                        crm_conn.row_factory = sqlite3.Row
                    for target in subscriber_targets:
                        subscriber_key = str(target.get("subscriber_key") or "").strip().lower()
                        if not subscriber_key:
                            continue
                        subscriber_status = str(target.get("status") or "").strip().lower()
                        subscriber_territory = str(target.get("territory_code") or "").strip().upper()
                        subscriber_states = [str(s).strip().upper() for s in (target.get("states") or []) if str(s).strip()]
                        cbsa_allowlist = [str(x).strip() for x in (target.get("cbsa_allowlist") or []) if str(x).strip()]
                        recipients = [crm_light.normalize_email(str(x)) for x in (target.get("recipients") or []) if str(x).strip()]
                        rendered_sections.append(
                            f"===== SUBSCRIBER {subscriber_key} status={subscriber_status} territory={subscriber_territory} ====="
                        )
                        if not recipients:
                            rendered_sections.append("NO_RECIPIENTS_CONFIGURED")
                            rendered_sections.append("")
                            continue
                        for recipient in recipients:
                            last_sent_at = ""
                            if crm_conn is not None:
                                try:
                                    last_sent_at = str(
                                        crm_light.get_last_sent_at_for_recipient(
                                            crm_conn,
                                            subscriber_key,
                                            recipient,
                                        )
                                        or ""
                                    ).strip()
                                except Exception:
                                    last_sent_at = ""
                            cutoff_dt = _parse_timestamp(last_sent_at) if last_sent_at else None
                            cutoff_label = last_sent_at if last_sent_at else f"fallback_since={since_date.isoformat()}"
                            rendered_sections.append(f"---- recipient {recipient} since {cutoff_label} ----")
                            selected, _matched_by_first_seen, _matched_by_opened_fallback = _fetch_selected_for_group(
                                conn,
                                since_date=since_date,
                                until_date=until_date,
                                states=subscriber_states,
                                territory_code=subscriber_territory,
                                strict_after=cutoff_dt,
                                subscriber_cbsa_allowlist=cbsa_allowlist,
                            )
                            action_blocks, suppressed_blocks = _render_group_sections(
                                selected,
                                include_suppressed=bool(args.include_suppressed),
                            )
                            subscribers_actionable += len(action_blocks)
                            if action_blocks:
                                rendered_sections.extend(action_blocks)
                            else:
                                rendered_sections.append("NO_SIGNALS_SINCE_LAST_SEND")
                            if args.include_suppressed and suppressed_blocks:
                                rendered_sections.append("SUPPRESSED (skip)")
                                rendered_sections.extend(suppressed_blocks)
                            rendered_sections.append("")
                finally:
                    if crm_conn is not None:
                        try:
                            crm_conn.close()
                        except Exception:
                            pass
    finally:
        conn.close()

    max_first_seen = _max_first_seen_iso(all_selected)
    max_date_opened = _max_date_opened_iso(all_selected)
    total_actionable = outreach_actionable + subscribers_actionable
    print(f"AI_REVIEW_DUMP_MATCHED_TOTAL={outreach_actionable}")
    print(f"AI_REVIEW_DUMP_OUTREACH_MATCHED_TOTAL={outreach_actionable}")
    print(f"AI_REVIEW_DUMP_SUBSCRIBERS_MATCHED_TOTAL={subscribers_actionable}")
    print(f"AI_REVIEW_DUMP_MATCHED_BY_FIRST_SEEN={total_matched_by_first_seen}")
    print(f"AI_REVIEW_DUMP_MATCHED_BY_OPENED_FALLBACK={total_matched_by_opened_fallback}")
    print(f"AI_REVIEW_DUMP_MAX_FIRST_SEEN={max_first_seen}")
    print(f"AI_REVIEW_DUMP_MAX_DATE_OPENED={max_date_opened}")
    if total_actionable == 0:
        print(
            "WARN_AI_REVIEW_DUMP_EMPTY=1 "
            f"reason=NO_MATCHES since={since_date.isoformat()} until={until_date.isoformat()}"
        )

    body_text = "\n".join([str(x) for x in rendered_sections]).strip()
    if args.for_ai_review:
        parts = ["\n".join(AI_REVIEW_HEADER_LINES).strip()]
        if body_text:
            parts.append(body_text)
        parts.append("\n".join(AI_REVIEW_FOOTER_LINES).strip())
        output_text = "\n\n".join([str(p).strip() for p in parts if str(p).strip()]) + "\n"
    else:
        output_text = body_text + "\n" if body_text else ""
    if output_text:
        print(output_text, end="")
    else:
        print("NO_SIGNALS_FOR_REVIEW")

    if not args.dry_run:
        _atomic_write_text(out_path, output_text)
        print(f"AI_REVIEW_DUMP_OUTPUT_PATH={out_path}")
        print(f"SIGNAL_REVIEW_OUT={out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
