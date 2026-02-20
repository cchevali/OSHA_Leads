#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sqlite3
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from email_footer import build_footer_html, build_footer_text
from lead_filters import filter_by_territory, load_territory_definitions, resolve_territory_code
from send_digest_email import (
    build_unsubscribe_payload,
    generate_digest_html,
    generate_digest_text,
    resolve_branding,
    send_email,
    territory_display_name,
)


_RE_ISO_DATE = re.compile(r"\b(20\d{2}-\d{2}-\d{2})\b")


@dataclass(frozen=True)
class ResolvedConfig:
    leads_db: Path
    crm_db: Path
    email_log: Path
    customer_config: Path
    subscriber_key: str
    out_dir: Path
    min_score: int
    trial_start_date: str
    as_of_date: str
    customer_id: str
    states: list[str]
    territory_code_raw: str
    territory_code_canonical: str
    wally_recipients: list[str]


def _normalize_email(value: object) -> str:
    return str(value or "").strip().lower()


def _parse_email_list(values: object) -> list[str]:
    if not isinstance(values, list):
        return []
    out: list[str] = []
    seen = set()
    for item in values:
        email = _normalize_email(item)
        if email and email not in seen:
            seen.add(email)
            out.append(email)
    return out


def _parse_iso_dt(value: object) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    text = raw[:-1] + "+00:00" if raw.upper().endswith("Z") else raw
    try:
        dt = datetime.fromisoformat(text)
    except Exception:
        return None
    if dt.tzinfo is None or dt.utcoffset() is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _extract_gen_date_from_subject(subject: str) -> str | None:
    match = _RE_ISO_DATE.search(str(subject or ""))
    if not match:
        return None
    return match.group(1)


def _read_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise ValueError(f"ERR_INVALID_JSON path={path} detail={exc.__class__.__name__}") from exc


def _read_trial_start_date(crm_db: Path, subscriber_key: str) -> str:
    if not crm_db.exists():
        raise FileNotFoundError(f"ERR_CRM_DB_MISSING path={crm_db}")
    conn = sqlite3.connect(str(crm_db))
    try:
        row = conn.execute(
            "SELECT start_date FROM trial_state WHERE subscriber_key = ?",
            ((subscriber_key or "").strip().lower(),),
        ).fetchone()
    finally:
        conn.close()
    if not row or not str(row[0] or "").strip():
        raise ValueError(f"ERR_TRIAL_STATE_NOT_FOUND subscriber_key={subscriber_key}")
    start_date = str(row[0]).strip()
    try:
        date.fromisoformat(start_date)
    except Exception as exc:
        raise ValueError(
            f"ERR_TRIAL_START_DATE_INVALID subscriber_key={subscriber_key} value={start_date}"
        ) from exc
    return start_date


def _read_email_log_rows(email_log: Path) -> list[dict[str, str]]:
    if not email_log.exists():
        return []
    rows: list[dict[str, str]] = []
    with open(email_log, "r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            rows.append({str(k): str(v or "") for k, v in (row or {}).items()})
    return rows


def _resolve_last_completed_send_date(
    email_rows: list[dict[str, str]],
    *,
    customer_id: str,
    mode: str,
    recipients: list[str],
) -> str:
    recipients_set = {email.strip().lower() for email in recipients if email.strip()}
    candidates: list[str] = []
    for row in email_rows:
        if str(row.get("customer_id") or "").strip() != customer_id:
            continue
        if str(row.get("mode") or "").strip().lower() != mode:
            continue
        if str(row.get("status") or "").strip().lower() != "sent":
            continue
        recipient = str(row.get("recipient") or "").strip().lower()
        if recipients_set and recipient not in recipients_set:
            continue
        gen_date = _extract_gen_date_from_subject(str(row.get("subject") or ""))
        if gen_date:
            candidates.append(gen_date)
            continue
        ts = _parse_iso_dt(row.get("timestamp"))
        if ts:
            candidates.append(ts.date().isoformat())
    if not candidates:
        raise ValueError(
            f"ERR_AS_OF_DATE_NOT_FOUND customer_id={customer_id} mode={mode} recipients={len(recipients_set)}"
        )
    return max(candidates)


def _legacy_tx_tri_definitions(definitions: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    legacy = dict(definitions)
    legacy["TX_TRI_LEGACY_AUDIT"] = {
        "description": "Legacy regex matcher for Texas Triangle (pre-CBSA)",
        "kind": "LEGACY_REGEX",
        "states": ["TX"],
        "office_patterns": [
            r"\baustin\b",
            r"\bdallas\b",
            r"\bfort[\s-]*worth\b",
            r"\bdallas[\s/-]*fort[\s-]*worth\b",
            r"\bhouston\b",
            r"\bsan[\s-]*antonio\b",
        ],
        "fallback_city_patterns": [
            r"\baustin\b",
            r"\bdallas\b",
            r"\bfort[\s-]*worth\b",
            r"\bhouston\b",
            r"\bpasadena\b",
            r"\bpearland\b",
            r"\bsugar[\s-]*land\b",
            r"\bthe[\s-]*woodlands\b",
            r"\bkaty\b",
            r"\bbaytown\b",
            r"\bsan[\s-]*antonio\b",
        ],
    }
    return legacy


def _match_reason_for_lead(
    lead: dict[str, Any],
    territory_code: str,
    *,
    definitions: dict[str, dict[str, Any]] | None,
) -> tuple[bool, str]:
    filtered, _stats, debug_rows = filter_by_territory(
        [lead],
        territory_code,
        definitions=definitions,
        include_debug=True,
    )
    debug = debug_rows[0] if debug_rows else {}
    matched = len(filtered) == 1
    reason = str(debug.get("match_reason") or ("MATCH" if matched else "NO_MATCH"))
    return matched, reason


def _load_inspection_rows(
    leads_db: Path,
    *,
    states: list[str],
    min_score: int,
    trial_start_date: str,
    as_of_date: str,
) -> tuple[list[dict[str, Any]], list[str]]:
    if not leads_db.exists():
        raise FileNotFoundError(f"ERR_LEADS_DB_MISSING path={leads_db}")
    conn = sqlite3.connect(str(leads_db))
    conn.row_factory = sqlite3.Row
    try:
        cols = [row[1] for row in conn.execute("PRAGMA table_info(inspections)").fetchall()]
        if not cols:
            raise ValueError("ERR_INSPECTIONS_SCHEMA columns=0")
        select_cols = ", ".join(cols)
        placeholders = ", ".join(["?" for _ in states]) or "?"
        params: list[Any] = []
        params.extend(states if states else ["TX"])
        params.extend([int(min_score), trial_start_date, as_of_date])
        query = (
            f"SELECT {select_cols} FROM inspections "
            f"WHERE site_state IN ({placeholders}) "
            "  AND parse_invalid = 0 "
            "  AND lead_score >= ? "
            "  AND date(first_seen_at) >= date(?) "
            "  AND date(first_seen_at) <= date(?) "
            "ORDER BY lead_score DESC, date_opened DESC, first_seen_at ASC"
        )
        rows = [dict(row) for row in conn.execute(query, tuple(params)).fetchall()]
        return rows, cols
    finally:
        conn.close()


def _tier(score: int) -> str:
    if score >= 10:
        return "high"
    if score >= 6:
        return "medium"
    return "low"


def _normalize_csv_value(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, (int, float)):
        return str(value)
    return str(value)


def _linked_email_rows_for_digest_date(
    email_rows: list[dict[str, str]],
    *,
    customer_id: str,
    mode: str,
    digest_date: str,
    recipients: list[str],
) -> list[dict[str, str]]:
    recipients_set = {email.strip().lower() for email in recipients if email.strip()}
    out: list[dict[str, str]] = []
    for row in email_rows:
        if str(row.get("customer_id") or "").strip() != customer_id:
            continue
        if str(row.get("mode") or "").strip().lower() != mode:
            continue
        if str(row.get("status") or "").strip().lower() != "sent":
            continue
        recipient = str(row.get("recipient") or "").strip().lower()
        if recipients_set and recipient not in recipients_set:
            continue
        gen_date = _extract_gen_date_from_subject(str(row.get("subject") or "")) or ""
        if gen_date != digest_date:
            continue
        out.append(row)
    out.sort(key=lambda r: str(r.get("timestamp") or ""))
    return out


def _resolve_linked_digest_date(
    lead: dict[str, Any],
    email_rows: list[dict[str, str]],
    *,
    customer_id: str,
    mode: str,
    recipients: list[str],
) -> str:
    first_seen_dt = _parse_iso_dt(lead.get("first_seen_at")) or _parse_iso_dt(lead.get("changed_at"))
    if not first_seen_dt:
        return ""
    recipients_set = {email.strip().lower() for email in recipients if email.strip()}
    best: tuple[datetime, str] | None = None
    for row in email_rows:
        if str(row.get("customer_id") or "").strip() != customer_id:
            continue
        if str(row.get("mode") or "").strip().lower() != mode:
            continue
        if str(row.get("status") or "").strip().lower() != "sent":
            continue
        recipient = str(row.get("recipient") or "").strip().lower()
        if recipients_set and recipient not in recipients_set:
            continue
        sent_at = _parse_iso_dt(row.get("timestamp"))
        if not sent_at:
            continue
        if sent_at < first_seen_dt:
            continue
        gen_date = _extract_gen_date_from_subject(str(row.get("subject") or ""))
        if not gen_date:
            continue
        if best is None or sent_at < best[0]:
            best = (sent_at, gen_date)
    return best[1] if best else ""


def _write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            cleaned = {k: _normalize_csv_value(row.get(k)) for k in fieldnames}
            writer.writerow(cleaned)


def _write_md_report(path: Path, cfg: ResolvedConfig, rows: list[dict[str, Any]]) -> None:
    by_date: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_date[str(row.get("linked_digest_date") or "UNLINKED")].append(row)

    lines: list[str] = []
    lines.append("# Wally Trial — Missed Medium/High Signals (Legacy Territory Matcher)")
    lines.append("")
    lines.append(f"- Subscriber: `{cfg.subscriber_key}`")
    lines.append(f"- Customer: `{cfg.customer_id}`")
    lines.append(f"- Trial start: `{cfg.trial_start_date}`")
    lines.append(f"- As of: `{cfg.as_of_date}`")
    lines.append(f"- Territory (raw): `{cfg.territory_code_raw}`")
    lines.append(f"- Territory (canonical): `{cfg.territory_code_canonical}`")
    lines.append(f"- Min score: `{cfg.min_score}`")
    lines.append(f"- Total missed signals: `{len(rows)}`")
    lines.append("")

    for digest_date in sorted(by_date.keys()):
        group = by_date[digest_date]
        group.sort(key=lambda r: (-int(r.get("lead_score") or 0), str(r.get("first_seen_at") or "")))
        lines.append(f"## {digest_date}")
        lines.append("")
        for item in group:
            activity = str(item.get("activity_nr") or "").strip()
            score = int(item.get("lead_score") or 0)
            tier = str(item.get("tier") or "").strip()
            company = str(item.get("establishment_name") or "").strip()
            city = str(item.get("site_city") or "").strip()
            zip5 = str(item.get("site_zip") or "").strip()
            itype = str(item.get("inspection_type") or "").strip()
            opened = str(item.get("date_opened") or "").strip()
            first_seen = str(item.get("first_seen_at") or "").strip()
            url = str(item.get("source_url") or "").strip()
            cur_reason = str(item.get("current_match_reason") or "").strip()
            leg_reason = str(item.get("legacy_match_reason") or "").strip()
            subj = str(item.get("linked_email_subject") or "").strip()
            msg_ids = str(item.get("linked_email_message_ids") or "").strip()
            lines.append(f"- `{activity}` score={score} tier={tier} type={itype} opened={opened} observed={first_seen}")
            lines.append(f"  - Company: {company} | City/ZIP: {city} {zip5}")
            if url:
                lines.append(f"  - Link: {url}")
            if cur_reason or leg_reason:
                lines.append(f"  - Match reasons: current=`{cur_reason or '-'}` legacy=`{leg_reason or '-'}`")
            if subj or msg_ids:
                lines.append(f"  - Linked email: subject=`{subj or '-'}` message_ids=`{msg_ids or '-'}`")
        lines.append("")

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


def _send_or_write_preview(
    *,
    cfg: ResolvedConfig,
    digest_date: str,
    leads: list[dict[str, Any]],
    preview_to: str,
    send_live: bool,
    dry_run: bool,
) -> None:
    if not digest_date:
        return
    if not leads:
        return

    config = _read_json(cfg.customer_config)
    branding = resolve_branding(config)
    territory_code_for_render = cfg.territory_code_raw or cfg.territory_code_canonical
    territory_label = (
        territory_display_name(territory_code_for_render) or (territory_code_for_render or "").strip() or "Territory"
    )

    list_unsub, list_unsub_post, one_click_url, _token = build_unsubscribe_payload(
        recipient=preview_to,
        campaign_id=cfg.customer_id,
        reply_to_email=branding["reply_to"],
        dry_run=(dry_run or (not send_live)),
    )

    footer_disclaimer = "This report contains public OSHA inspection data for informational purposes only. Not legal advice."
    footer_text = build_footer_text(
        brand_name=branding.get("brand_legal_name") or branding.get("brand_name") or "",
        mailing_address=branding.get("mailing_address") or "",
        disclaimer=footer_disclaimer,
        reply_to=branding.get("reply_to") or "",
        unsub_url=one_click_url or None,
        include_separator=True,
    )
    footer_html = build_footer_html(
        brand_name=branding.get("brand_legal_name") or branding.get("brand_name") or "",
        mailing_address=branding.get("mailing_address") or "",
        disclaimer=footer_disclaimer,
        reply_to=branding.get("reply_to") or "",
        unsub_url=one_click_url or None,
    )

    report_label = "Backfill: missed medium/high signals (legacy territory matcher)"
    summary_label = f"Missed by legacy territory matcher: {len(leads)} signals"

    html_body = generate_digest_html(
        leads=leads,
        low_fallback=[],
        config=config,
        gen_date=digest_date,
        mode="daily",
        territory_code=territory_code_for_render,
        content_filter="high_medium",
        include_low_fallback=False,
        branding=branding,
        report_label=report_label,
        summary_label=summary_label,
        footer_html=footer_html,
    )
    text_body = generate_digest_text(
        leads=leads,
        low_fallback=[],
        config=config,
        gen_date=digest_date,
        mode="daily",
        territory_code=territory_code_for_render,
        content_filter="high_medium",
        include_low_fallback=False,
        branding=branding,
        report_label=report_label,
        summary_label=summary_label,
        footer_text=footer_text,
    )

    safe_date = digest_date.replace("/", "-")
    out_html = cfg.out_dir / f"preview_email_{safe_date}.html"
    out_txt = cfg.out_dir / f"preview_email_{safe_date}.txt"
    cfg.out_dir.mkdir(parents=True, exist_ok=True)
    out_html.write_text(html_body, encoding="utf-8")
    out_txt.write_text(text_body, encoding="utf-8")

    if not send_live or dry_run:
        print(f"PREVIEW_WRITTEN date={digest_date} html={out_html} txt={out_txt}")
        return

    subject = f"[BACKFILL] {territory_label} OSHA Signals - {digest_date} ({len(leads)} missed)"
    ok, message_id, err = send_email(
        recipient=preview_to,
        subject=subject,
        html_body=html_body,
        text_body=text_body,
        customer_id=cfg.customer_id,
        territory_code=territory_code_for_render,
        branding=branding,
        dry_run=False,
        list_unsub=list_unsub,
        list_unsub_post=list_unsub_post,
    )
    if ok:
        print(f"PREVIEW_SENT date={digest_date} to={preview_to} message_id={message_id}")
        return
    raise RuntimeError(f"ERR_PREVIEW_SEND_FAILED date={digest_date} to={preview_to} detail={err}")


def _resolve_config(args: argparse.Namespace) -> ResolvedConfig:
    leads_db = (REPO_ROOT / str(args.leads_db)).resolve() if not Path(args.leads_db).is_absolute() else Path(args.leads_db)
    crm_db = (REPO_ROOT / str(args.crm_db)).resolve() if not Path(args.crm_db).is_absolute() else Path(args.crm_db)
    email_log = (REPO_ROOT / str(args.email_log)).resolve() if not Path(args.email_log).is_absolute() else Path(args.email_log)
    customer_config = (
        (REPO_ROOT / str(args.customer_config)).resolve()
        if not Path(args.customer_config).is_absolute()
        else Path(args.customer_config)
    )
    out_dir = (REPO_ROOT / str(args.out_dir)).resolve() if not Path(args.out_dir).is_absolute() else Path(args.out_dir)
    subscriber_key = str(args.subscriber_key or "").strip().lower()
    if not subscriber_key:
        raise ValueError("ERR_SUBSCRIBER_KEY_MISSING")

    cfg = _read_json(customer_config)
    customer_id = str(cfg.get("customer_id") or "").strip()
    if not customer_id:
        raise ValueError(f"ERR_CUSTOMER_ID_MISSING path={customer_config}")

    states = [str(s).strip().upper() for s in (cfg.get("states") or []) if str(s).strip()]
    territory_code_raw = str(cfg.get("territory_code") or "").strip().upper()
    if not territory_code_raw:
        territory_code_raw = "TX_TRIANGLE_V1"

    territory_code_canonical = resolve_territory_code(territory_code_raw, load_territory_definitions())
    wally_recipients = _parse_email_list(cfg.get("recipients") or cfg.get("email_recipients") or [])

    trial_start_date = _read_trial_start_date(crm_db, subscriber_key)
    email_rows = _read_email_log_rows(email_log)
    as_of_date = _resolve_last_completed_send_date(
        email_rows,
        customer_id=customer_id,
        mode="daily",
        recipients=wally_recipients,
    )

    try:
        min_score = int(args.min_score)
    except Exception:
        min_score = 6
    min_score = max(0, min(100, int(min_score)))

    return ResolvedConfig(
        leads_db=leads_db,
        crm_db=crm_db,
        email_log=email_log,
        customer_config=customer_config,
        subscriber_key=subscriber_key,
        out_dir=out_dir,
        min_score=min_score,
        trial_start_date=trial_start_date,
        as_of_date=as_of_date,
        customer_id=customer_id,
        states=states or ["TX"],
        territory_code_raw=territory_code_raw,
        territory_code_canonical=territory_code_canonical,
        wally_recipients=wally_recipients,
    )


def _print_config(cfg: ResolvedConfig) -> None:
    print(f"subscriber_key={cfg.subscriber_key}")
    print(f"customer_id={cfg.customer_id}")
    print(f"trial_start_date={cfg.trial_start_date}")
    print(f"as_of_date={cfg.as_of_date}")
    print(f"min_score={cfg.min_score}")
    print(f"leads_db={cfg.leads_db}")
    print(f"crm_db={cfg.crm_db}")
    print(f"email_log={cfg.email_log}")
    print(f"customer_config={cfg.customer_config}")
    print(f"out_dir={cfg.out_dir}")
    print(f"states={','.join(cfg.states)}")
    print(f"territory_code_raw={cfg.territory_code_raw}")
    print(f"territory_code_canonical={cfg.territory_code_canonical}")
    print(f"wally_recipients={','.join(cfg.wally_recipients)}")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description="Report medium/high signals current TX_TRI would include but legacy regex matcher would miss."
    )
    ap.add_argument("--print-config", action="store_true")
    ap.add_argument("--dry-run", action="store_true", help="Do not send preview email; still writes report artifacts.")
    ap.add_argument("--leads-db", default="data/osha.sqlite")
    ap.add_argument("--crm-db", default="out/crm_light.sqlite")
    ap.add_argument("--email-log", default="out/email_log.csv")
    ap.add_argument("--customer-config", default="customers/wally_trial_tx_triangle_v1.json")
    ap.add_argument("--subscriber-key", default="wally_trial")
    ap.add_argument("--out-dir", default="out/trials/wally_trial")
    ap.add_argument("--min-score", default="6")
    ap.add_argument("--preview-to", default="", help="Optional email address to send/write preview digest.")
    ap.add_argument("--send-live", action="store_true", help="Actually send preview email (requires SMTP env + secrets).")
    args = ap.parse_args(argv)

    try:
        cfg = _resolve_config(args)
    except Exception as exc:
        print(f"ERR_CONFIG_RESOLVE {exc}")
        return 1

    if args.print_config:
        _print_config(cfg)
        return 0

    preview_to = _normalize_email(args.preview_to)
    if args.send_live and not preview_to:
        print("ERR_PREVIEW_TO_REQUIRED --send-live requires --preview-to")
        return 1
    if preview_to and ("@" not in preview_to or "." not in preview_to.split("@", 1)[-1]):
        print(f"ERR_PREVIEW_TO_INVALID value={preview_to!r}")
        return 1

    email_rows = _read_email_log_rows(cfg.email_log)
    try:
        inspections, inspection_cols = _load_inspection_rows(
            cfg.leads_db,
            states=cfg.states,
            min_score=cfg.min_score,
            trial_start_date=cfg.trial_start_date,
            as_of_date=cfg.as_of_date,
        )
    except Exception as exc:
        print(f"ERR_LOAD_INSPECTIONS {exc}")
        return 1

    definitions = load_territory_definitions()
    legacy_definitions = _legacy_tx_tri_definitions(definitions)

    missed: list[dict[str, Any]] = []
    for lead in inspections:
        try:
            current_ok, current_reason = _match_reason_for_lead(
                lead,
                cfg.territory_code_canonical,
                definitions=definitions,
            )
            legacy_ok, legacy_reason = _match_reason_for_lead(
                lead,
                "TX_TRI_LEGACY_AUDIT",
                definitions=legacy_definitions,
            )
        except Exception:
            continue
        if not current_ok:
            continue
        if legacy_ok:
            continue

        linked_digest_date = _resolve_linked_digest_date(
            lead,
            email_rows,
            customer_id=cfg.customer_id,
            mode="daily",
            recipients=cfg.wally_recipients,
        )
        linked_rows = (
            _linked_email_rows_for_digest_date(
                email_rows,
                customer_id=cfg.customer_id,
                mode="daily",
                digest_date=linked_digest_date,
                recipients=cfg.wally_recipients,
            )
            if linked_digest_date
            else []
        )
        subjects = sorted({str(r.get("subject") or "").strip() for r in linked_rows if str(r.get("subject") or "").strip()})
        msg_ids = sorted({str(r.get("message_id") or "").strip() for r in linked_rows if str(r.get("message_id") or "").strip()})
        timestamps = sorted({str(r.get("timestamp") or "").strip() for r in linked_rows if str(r.get("timestamp") or "").strip()})
        territory_for_artifacts = ""
        for r in linked_rows:
            t = str(r.get("territory_code") or "").strip()
            if t:
                territory_for_artifacts = t
                break
        if not territory_for_artifacts:
            territory_for_artifacts = cfg.territory_code_raw
        safe_terr = territory_for_artifacts.strip().replace(" ", "_") or "territory"

        run_log_path = REPO_ROOT / "out" / f"run_log_{linked_digest_date}.txt" if linked_digest_date else None
        tier_audit_path = (
            REPO_ROOT / "out" / f"tier_audit_{linked_digest_date}_{safe_terr}_daily.json"
            if linked_digest_date
            else None
        )

        row = dict(lead)
        score = int(row.get("lead_score") or 0)
        row.update(
            {
                "tier": _tier(score),
                "trial_start_date": cfg.trial_start_date,
                "as_of_date": cfg.as_of_date,
                "current_match_reason": current_reason,
                "legacy_match_reason": legacy_reason,
                "linked_digest_date": linked_digest_date,
                "linked_email_subject": subjects[0] if subjects else "",
                "linked_email_message_ids": json.dumps(msg_ids),
                "linked_email_timestamps": json.dumps(timestamps),
                "run_log_path": str(run_log_path) if run_log_path and run_log_path.exists() else "",
                "tier_audit_path": str(tier_audit_path) if tier_audit_path and tier_audit_path.exists() else "",
            }
        )
        missed.append(row)

    report_csv = cfg.out_dir / "missed_signals.csv"
    report_md = cfg.out_dir / "missed_signals_report.md"
    fieldnames = list(inspection_cols) + [
        "tier",
        "trial_start_date",
        "as_of_date",
        "current_match_reason",
        "legacy_match_reason",
        "linked_digest_date",
        "linked_email_subject",
        "linked_email_message_ids",
        "linked_email_timestamps",
        "run_log_path",
        "tier_audit_path",
    ]
    try:
        _write_csv(report_csv, missed, fieldnames)
        _write_md_report(report_md, cfg, missed)
    except Exception as exc:
        print(f"ERR_WRITE_REPORT {exc}")
        return 1

    print(f"OK_REPORT csv={report_csv} md={report_md} rows={len(missed)}")

    if preview_to:
        grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in missed:
            grouped[str(row.get('linked_digest_date') or '')].append(row)
        for digest_date, leads in sorted(grouped.items()):
            if not digest_date:
                continue
            leads.sort(key=lambda r: (-int(r.get("lead_score") or 0), str(r.get("first_seen_at") or "")))
            try:
                _send_or_write_preview(
                    cfg=cfg,
                    digest_date=digest_date,
                    leads=leads,
                    preview_to=preview_to,
                    send_live=bool(args.send_live),
                    dry_run=bool(args.dry_run),
                )
            except Exception as exc:
                print(str(exc))
                return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
