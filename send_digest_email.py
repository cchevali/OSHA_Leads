#!/usr/bin/env python3
"""
Send OSHA digest email to customer recipients.

Features:
- Territory-aware filtering (including TX_TRIANGLE_V1)
- High/medium content filters with low-lead fallback heartbeat
- Per-record dedupe by activity number
- Suppression enforcement
- Compliance footer and List-Unsubscribe headers
"""

import argparse
import csv
import json
import logging
import os
import smtplib
import sqlite3
import sys
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formataddr, formatdate, make_msgid
from pathlib import Path

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover
    load_dotenv = None

from lead_filters import (
    apply_content_filter,
    dedupe_by_activity_nr,
    filter_by_territory,
    normalize_content_filter,
)
from unsubscribe_utils import create_unsub_token

logger = logging.getLogger(__name__)

PILOT_MODE_DEFAULT = True
PILOT_WHITELIST_DEFAULT = ["cchevali@gmail.com"]

DEFAULT_REPLY_TO = "support@microflowops.com"
DEFAULT_FROM_LOCAL_PART = "alerts"
LOW_FALLBACK_LIMIT = 5


def load_environment(repo_root: Path) -> None:
    """Load .env for scheduler contexts where env vars are not inherited."""
    if load_dotenv is None:
        return

    dotenv_path = repo_root / ".env"
    if dotenv_path.exists():
        load_dotenv(dotenv_path=dotenv_path, override=False)


def preflight_missing_vars(config: dict, dry_run: bool) -> list[str]:
    """Return a concise list of missing required environment/config variables."""
    missing = []

    brand_name = (config.get("brand_name") or os.getenv("BRAND_NAME") or "").strip()
    mailing_address = (config.get("mailing_address") or os.getenv("MAILING_ADDRESS") or "").strip()

    if not brand_name:
        missing.append("BRAND_NAME")
    if not mailing_address:
        missing.append("MAILING_ADDRESS")

    if not dry_run:
        for key in ["SMTP_HOST", "SMTP_PORT", "SMTP_USER", "SMTP_PASS"]:
            if not os.getenv(key, "").strip():
                missing.append(key)

    return missing


def setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def log_email_attempt(log_path: str, row: dict) -> None:
    fieldnames = [
        "timestamp",
        "customer_id",
        "mode",
        "recipient",
        "subject",
        "status",
        "message_id",
        "error",
        "territory_code",
        "content_filter",
    ]
    file_exists = os.path.exists(log_path)
    with open(log_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerow({k: row.get(k, "") for k in fieldnames})


def log_suppression(log_path: str, row: dict) -> None:
    fieldnames = [
        "timestamp",
        "customer_id",
        "recipient",
        "reason",
        "territory_code",
    ]
    file_exists = os.path.exists(log_path)
    with open(log_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerow({k: row.get(k, "") for k in fieldnames})


def ensure_unsubscribe_events_table(conn: sqlite3.Connection) -> None:
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS unsubscribe_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT NOT NULL,
            event_type TEXT NOT NULL,
            reason TEXT,
            source TEXT NOT NULL,
            customer_id TEXT,
            territory_code TEXT,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.commit()


def append_unsubscribe_event(
    db_path: str,
    email: str,
    event_type: str,
    reason: str,
    source: str,
    customer_id: str,
    territory_code: str,
    output_dir: str,
) -> None:
    ts = datetime.now().isoformat()

    conn = sqlite3.connect(db_path)
    ensure_unsubscribe_events_table(conn)
    conn.execute(
        """
        INSERT INTO unsubscribe_events
        (email, event_type, reason, source, customer_id, territory_code, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (email.lower(), event_type, reason, source, customer_id, territory_code, ts),
    )
    conn.commit()
    conn.close()

    csv_path = Path(output_dir) / "unsubscribe_events.csv"
    csv_exists = csv_path.exists()
    with open(csv_path, "a", newline="", encoding="utf-8") as f:
        fieldnames = [
            "timestamp",
            "email",
            "event_type",
            "reason",
            "source",
            "customer_id",
            "territory_code",
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not csv_exists:
            writer.writeheader()
        writer.writerow(
            {
                "timestamp": ts,
                "email": email.lower(),
                "event_type": event_type,
                "reason": reason,
                "source": source,
                "customer_id": customer_id,
                "territory_code": territory_code,
            }
        )

def load_customer_config(config_path: str) -> dict:
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)


def _has_column(conn: sqlite3.Connection, table: str, column: str) -> bool:
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info({table})")
    return any(row[1] == column for row in cursor.fetchall())


def _load_subscriber_profile(db_path: str, subscriber_key: str | None) -> dict:
    if not subscriber_key:
        return {}

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    if not _has_column(conn, "subscribers", "include_low_fallback"):
        cursor.execute("ALTER TABLE subscribers ADD COLUMN include_low_fallback INTEGER NOT NULL DEFAULT 0")
        conn.commit()
    if not _has_column(conn, "subscribers", "recipients_json"):
        cursor.execute("ALTER TABLE subscribers ADD COLUMN recipients_json TEXT")
        conn.commit()

    cursor.execute(
        """
        SELECT subscriber_key, email, recipients_json, active, territory_code, content_filter, include_low_fallback
        FROM subscribers
        WHERE subscriber_key = ?
        LIMIT 1
        """,
        (subscriber_key,),
    )
    row = cursor.fetchone()
    conn.close()

    if not row:
        return {}

    recipients = []
    raw_recipients = row[2] if len(row) > 2 else None
    if raw_recipients:
        try:
            parsed = json.loads(raw_recipients)
            if isinstance(parsed, list):
                recipients = [str(email).strip().lower() for email in parsed if str(email).strip()]
        except Exception:
            recipients = []

    return {
        "subscriber_key": row[0],
        "email": (row[1] or "").strip().lower(),
        "recipients": recipients,
        "active": int(row[3] or 0),
        "territory_code": row[4],
        "content_filter": row[5],
        "include_low_fallback": bool(row[6]),
    }


def check_suppression(db_path: str, email: str) -> bool:
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute(
        "SELECT 1 FROM suppression_list WHERE lower(email_or_domain) = ? LIMIT 1",
        (email.lower(),),
    )
    if cursor.fetchone():
        conn.close()
        return True

    domain = email.split("@")[-1].lower()
    cursor.execute(
        "SELECT 1 FROM suppression_list WHERE lower(email_or_domain) = ? LIMIT 1",
        (domain,),
    )
    found = cursor.fetchone() is not None
    conn.close()
    return found


def get_leads_for_period(
    conn: sqlite3.Connection,
    states: list[str],
    since_days: int,
    new_only_days: int,
    skip_first_seen_filter: bool,
    territory_code: str | None,
    content_filter: str,
    include_low_fallback: bool,
) -> tuple[list[dict], list[dict], dict]:
    today = datetime.now()
    date_opened_cutoff = (today - timedelta(days=since_days)).strftime("%Y-%m-%d")
    first_seen_cutoff = (today - timedelta(days=new_only_days)).strftime("%Y-%m-%d %H:%M:%S")

    lead_id_expr = (
        "lead_id"
        if _has_column(conn, "inspections", "lead_id")
        else "('osha:inspection:' || activity_nr) AS lead_id"
    )
    area_office_expr = "area_office" if _has_column(conn, "inspections", "area_office") else "NULL AS area_office"
    placeholders = ",".join(["?" for _ in states])

    query = f"""
        SELECT
            {lead_id_expr},
            activity_nr,
            date_opened,
            inspection_type,
            scope,
            case_status,
            establishment_name,
            site_city,
            site_state,
            site_zip,
            {area_office_expr},
            naics,
            naics_desc,
            violations_count,
            emphasis,
            lead_score,
            first_seen_at,
            last_seen_at,
            source_url
        FROM inspections
        WHERE site_state IN ({placeholders})
          AND parse_invalid = 0
        ORDER BY lead_score DESC, date_opened DESC
    """

    cursor = conn.cursor()
    cursor.execute(query, tuple(states))

    columns = [desc[0] for desc in cursor.description]
    all_results = [dict(zip(columns, row)) for row in cursor.fetchall()]

    date_filtered = []
    excluded_by_date_opened = 0
    excluded_by_first_seen = 0

    for lead in all_results:
        date_opened = lead.get("date_opened")
        first_seen = lead.get("first_seen_at")

        if date_opened and date_opened < date_opened_cutoff:
            excluded_by_date_opened += 1
            continue

        if not skip_first_seen_filter and first_seen and first_seen < first_seen_cutoff:
            excluded_by_first_seen += 1
            continue

        date_filtered.append(lead)

    territory_filtered, territory_stats = filter_by_territory(date_filtered, territory_code)
    deduped, dedupe_removed = dedupe_by_activity_nr(territory_filtered)
    content_filtered, excluded_content = apply_content_filter(deduped, content_filter)

    low_fallback = []
    if (
        content_filter == "high_medium"
        and len(content_filtered) == 0
        and include_low_fallback
    ):
        low_candidates = [lead for lead in deduped if int(lead.get("lead_score") or 0) < 6]
        low_candidates.sort(
            key=lambda lead: (int(lead.get("lead_score") or 0), lead.get("date_opened") or ""),
            reverse=True,
        )
        low_fallback = low_candidates[:LOW_FALLBACK_LIMIT]

    stats = {
        "total_before_filter": len(all_results),
        "excluded_by_date_opened": excluded_by_date_opened,
        "excluded_by_first_seen": excluded_by_first_seen,
        "excluded_by_territory": territory_stats["excluded_state"] + territory_stats["excluded_territory"],
        "matched_by_office": territory_stats["matched_by_office"],
        "matched_by_fallback": territory_stats["matched_by_fallback"],
        "excluded_by_content_filter": excluded_content,
        "dedupe_removed": dedupe_removed,
        "low_fallback_count": len(low_fallback),
    }

    return content_filtered, low_fallback, stats


def resolve_branding(config: dict) -> dict:
    brand_name = (config.get("brand_name") or os.getenv("BRAND_NAME") or "").strip()
    brand_legal_name = (config.get("brand_legal_name") or os.getenv("BRAND_LEGAL_NAME") or "").strip()
    mailing_address = (config.get("mailing_address") or os.getenv("MAILING_ADDRESS") or "").strip()

    smtp_user = (os.getenv("SMTP_USER") or "").strip()
    from_email = (os.getenv("FROM_EMAIL") or smtp_user or f"{DEFAULT_FROM_LOCAL_PART}@localhost").strip()
    reply_to = (config.get("reply_to_email") or os.getenv("REPLY_TO_EMAIL") or DEFAULT_REPLY_TO).strip()
    from_display_name = (config.get("from_display_name") or os.getenv("FROM_NAME") or f"{brand_name} OSHA Alerts").strip()

    return {
        "brand_name": brand_name,
        "brand_legal_name": brand_legal_name,
        "mailing_address": mailing_address,
        "from_email": from_email,
        "reply_to": reply_to,
        "from_display_name": from_display_name,
    }


def build_unsubscribe_headers(
    recipient: str,
    campaign_id: str,
    reply_to_email: str,
) -> tuple[str, str | None]:
    mailto = f"mailto:{reply_to_email}?subject=unsubscribe"
    unsub_endpoint = os.getenv("UNSUB_ENDPOINT_BASE", "").strip()

    if not unsub_endpoint:
        return f"<{mailto}>", None

    signed_token = create_unsub_token(recipient, campaign_id)
    sep = "&" if "?" in unsub_endpoint else "?"
    one_click_url = f"{unsub_endpoint}{sep}token={signed_token}"
    return f"<{mailto}>, <{one_click_url}>", "List-Unsubscribe=One-Click"

def _lead_rows_html(rows: list[dict], max_rows: int) -> str:
    if not rows:
        return "<p><em>No leads match this section.</em></p>"

    parts = ['<table border="1" cellpadding="6" cellspacing="0" style="border-collapse: collapse; width: 100%;">']
    parts.append("<tr><th>Company</th><th>City</th><th>Area Office</th><th>Type</th><th>Date</th><th>Score</th><th>Link</th></tr>")
    for lead in rows[:max_rows]:
        company = (lead.get("establishment_name") or "Unknown")[:48]
        city = lead.get("site_city") or "-"
        state = lead.get("site_state") or "-"
        area_office = lead.get("area_office") or "-"
        itype = lead.get("inspection_type") or "-"
        date_opened = lead.get("date_opened") or "-"
        score = int(lead.get("lead_score") or 0)
        url = lead.get("source_url") or "#"
        parts.append(
            f"<tr><td>{company}</td><td>{city}, {state}</td><td>{area_office}</td><td>{itype}</td><td>{date_opened}</td><td><strong>{score}</strong></td><td><a href=\"{url}\">View</a></td></tr>"
        )
    parts.append("</table>")
    return "\n".join(parts)


def generate_digest_html(
    leads: list[dict],
    low_fallback: list[dict],
    config: dict,
    gen_date: str,
    mode: str,
    territory_code: str | None,
    content_filter: str,
    include_low_fallback: bool,
    branding: dict,
) -> str:
    states = config["states"]
    top_k_overall = config.get("top_k_overall", 25)
    top_k_per_state = config.get("top_k_per_state", 10)

    mode_label = "BASELINE" if mode == "baseline" else "DAILY"
    hi_count = sum(1 for lead in leads if int(lead.get("lead_score") or 0) >= 10)

    state_counts: dict[str, int] = {}
    for lead in leads:
        st = lead.get("site_state") or "UNK"
        state_counts[st] = state_counts.get(st, 0) + 1

    html: list[str] = []
    html.append("<!DOCTYPE html>")
    html.append("<html><head><meta charset=\"utf-8\"></head>")
    html.append('<body style="font-family: Arial, sans-serif; max-width: 900px; margin: 0 auto; padding: 20px; background-color: #f7f9fc;">')
    html.append('<div style="background-color: #ffffff; padding: 24px; border-radius: 8px;">')

    html.append(f"<h1 style=\"margin-top: 0; color: #1a1a2e;\">OSHA Lead Digest ({mode_label})</h1>")
    html.append(f"<p style=\"color: #555;\">{gen_date} | {'/'.join(states)}</p>")
    if territory_code:
        html.append(f"<p style=\"color: #555;\"><strong>Territory:</strong> {territory_code}</p>")
    html.append(f"<p style=\"color: #555;\"><strong>Content Filter:</strong> {content_filter}</p>")

    summary_label = "new leads today" if mode == "daily" else "leads in baseline"
    html.append('<div style="background-color: #eef5ff; padding: 14px; border-radius: 6px; margin: 16px 0;">')
    html.append(f"<p style=\"margin: 0;\"><strong>{len(leads)} {summary_label}</strong> | {hi_count} high-priority (score >= 10)</p>")
    html.append("</div>")

    if len(leads) == 0 and content_filter == "high_medium":
        html.append("<p><strong>No High/Medium today.</strong></p>")
        if include_low_fallback:
            if low_fallback:
                html.append(f"<h2>Low Leads (Fallback) - Top {len(low_fallback)}</h2>")
                html.append(_lead_rows_html(low_fallback, LOW_FALLBACK_LIMIT))
            else:
                html.append("<p><em>No fallback low leads available.</em></p>")
    else:
        html.append("<ul>")
        for state in states:
            html.append(f"<li>{state}: {state_counts.get(state, 0)} leads</li>")
        html.append("</ul>")

        html.append(f"<h2>Top {min(5, len(leads))} Leads</h2>")
        html.append(_lead_rows_html(leads, 5))

        html.append(f"<h2>All {len(leads)} Leads</h2>")
        html.append(_lead_rows_html(leads, top_k_overall))

        for state in states:
            state_leads = [lead for lead in leads if lead.get("site_state") == state]
            html.append(f"<h2>{state} Top {min(len(state_leads), top_k_per_state)}</h2>")
            html.append(_lead_rows_html(state_leads, top_k_per_state))

    html.append('<div style="margin-top: 24px; padding-top: 12px; border-top: 1px solid #ddd; font-size: 12px; color: #666;">')
    footer_brand = branding.get("brand_legal_name") or branding["brand_name"]
    html.append(f"<p><strong>{footer_brand}</strong><br>{branding['mailing_address']}</p>")
    html.append("<p>This report contains public OSHA inspection data for informational purposes only. Not legal advice.</p>")
    html.append(f"<p>Unsubscribe: reply 'opt out' or email {branding['reply_to']}.</p>")
    html.append("</div>")

    html.append("</div></body></html>")
    return "\n".join(html)


def generate_digest_text(
    leads: list[dict],
    low_fallback: list[dict],
    config: dict,
    gen_date: str,
    mode: str,
    territory_code: str | None,
    content_filter: str,
    include_low_fallback: bool,
    branding: dict,
) -> str:
    states = config["states"]
    mode_label = "BASELINE" if mode == "baseline" else "DAILY"
    hi_count = sum(1 for lead in leads if int(lead.get("lead_score") or 0) >= 10)

    lines = [
        f"OSHA Lead Digest ({mode_label}) - {gen_date}",
        f"Coverage: {'/'.join(states)}",
    ]
    if territory_code:
        lines.append(f"Territory: {territory_code}")
    lines.append(f"Content Filter: {content_filter}")
    lines.append("=" * 70)
    lines.append(f"Total Leads: {len(leads)}")
    lines.append(f"High Priority (>=10): {hi_count}")

    if len(leads) == 0 and content_filter == "high_medium":
        lines.append("")
        lines.append("No High/Medium today.")
        if include_low_fallback:
            if low_fallback:
                lines.append("")
                lines.append("Low Leads (Fallback):")
                for lead in low_fallback:
                    lines.append(
                        f"- {(lead.get('establishment_name') or 'Unknown')} | "
                        f"{(lead.get('site_city') or '-')}, {(lead.get('site_state') or '-')} | "
                        f"Score {int(lead.get('lead_score') or 0)}"
                    )
            else:
                lines.append("No fallback low leads available.")
    else:
        lines.append("")
        lines.append("Top Leads:")
        for lead in leads[:5]:
            lines.append("")
            lines.append(f"- {(lead.get('establishment_name') or 'Unknown')}")
            lines.append(
                f"  {(lead.get('site_city') or '-')}, {(lead.get('site_state') or '-')} | "
                f"Area Office: {(lead.get('area_office') or '-')}"
            )
            lines.append(
                f"  {(lead.get('inspection_type') or '-')} | "
                f"Date: {(lead.get('date_opened') or '-')} | "
                f"Score: {int(lead.get('lead_score') or 0)}"
            )
            lines.append(f"  {(lead.get('source_url') or '#')}")

    lines.append("")
    lines.append("-" * 70)
    lines.append(branding.get("brand_legal_name") or branding["brand_name"])
    lines.append(branding["mailing_address"])
    lines.append("This report contains public OSHA inspection data for informational purposes only. Not legal advice.")
    lines.append(f"To unsubscribe: reply 'opt out' or email {branding['reply_to']}")

    return "\n".join(lines)

def build_email_message(
    recipient: str,
    subject: str,
    html_body: str,
    text_body: str,
    customer_id: str,
    territory_code: str,
    branding: dict,
) -> MIMEMultipart:
    from_header = formataddr((branding["from_display_name"], branding["from_email"]))
    reply_to_header = formataddr((branding["from_display_name"], branding["reply_to"]))

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = from_header
    msg["To"] = recipient
    msg["Reply-To"] = reply_to_header
    msg["Date"] = formatdate(localtime=True)
    msg["Message-ID"] = make_msgid()
    msg["X-Customer-ID"] = customer_id
    msg["X-Territory-Code"] = territory_code or ""

    list_unsub, list_unsub_post = build_unsubscribe_headers(recipient, customer_id, branding["reply_to"])
    msg["List-Unsubscribe"] = list_unsub
    if list_unsub_post:
        msg["List-Unsubscribe-Post"] = list_unsub_post

    msg.attach(MIMEText(text_body, "plain", "utf-8"))
    msg.attach(MIMEText(html_body, "html", "utf-8"))
    return msg


def send_email(
    recipient: str,
    subject: str,
    html_body: str,
    text_body: str,
    customer_id: str,
    territory_code: str,
    branding: dict,
    dry_run: bool,
) -> tuple[bool, str, str]:
    smtp_host = os.environ.get("SMTP_HOST", "")
    smtp_port_text = os.environ.get("SMTP_PORT", "")
    smtp_user = os.environ.get("SMTP_USER", "")
    smtp_pass = os.environ.get("SMTP_PASS", "")

    msg = build_email_message(
        recipient=recipient,
        subject=subject,
        html_body=html_body,
        text_body=text_body,
        customer_id=customer_id,
        territory_code=territory_code,
        branding=branding,
    )

    if dry_run:
        logger.info("[DRY-RUN] Would send to %s | subject=%s", recipient, subject)
        return True, "dry-run-no-message-id", ""

    try:
        smtp_port = int(smtp_port_text)
    except ValueError:
        return False, "", "Invalid SMTP_PORT"

    try:
        if smtp_port == 465:
            with smtplib.SMTP_SSL(smtp_host, smtp_port) as server:
                server.login(smtp_user, smtp_pass)
                server.send_message(msg)
        else:
            with smtplib.SMTP(smtp_host, smtp_port) as server:
                server.starttls()
                server.login(smtp_user, smtp_pass)
                server.send_message(msg)

        return True, msg["Message-ID"], ""
    except Exception as exc:
        return False, "", str(exc)


def parse_recipients(value: str | None) -> list[str]:
    if not value:
        return []
    return [email.strip().lower() for email in value.split(",") if email.strip()]


def collect_recipients(config: dict, subscriber_profile: dict, override: str | None) -> list[str]:
    if override:
        return parse_recipients(override)

    recipients: list[str] = []

    if subscriber_profile.get("recipients"):
        recipients.extend(subscriber_profile["recipients"])
    elif subscriber_profile.get("email"):
        recipients.append(subscriber_profile["email"])

    config_recipients = config.get("recipients") or config.get("email_recipients") or []
    if isinstance(config_recipients, list):
        recipients.extend(str(email).strip().lower() for email in config_recipients if str(email).strip())

    # Preserve order while deduplicating.
    deduped = []
    seen = set()
    for email in recipients:
        if email not in seen:
            seen.add(email)
            deduped.append(email)
    return deduped


def main() -> None:
    parser = argparse.ArgumentParser(description="Send OSHA digest email")
    parser.add_argument("--db", required=True, help="Path to SQLite database")
    parser.add_argument("--customer", required=True, help="Path to customer config JSON")
    parser.add_argument("--mode", choices=["baseline", "daily"], default="daily")
    parser.add_argument("--output-dir", default="out", help="Output directory")
    parser.add_argument("--dry-run", action="store_true", help="Generate but do not send")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    parser.add_argument(
        "--recipient-override",
        default="",
        help="Comma-separated recipients to override config recipients (useful for preview sends)",
    )
    parser.add_argument(
        "--disable-pilot-guard",
        action="store_true",
        help="Disable pilot whitelist recipient guard",
    )

    args = parser.parse_args()
    setup_logging(args.log_level)

    repo_root = Path(__file__).resolve().parent
    load_environment(repo_root)

    gen_date = datetime.now().strftime("%Y-%m-%d")
    timestamp = datetime.now().isoformat()

    config = load_customer_config(args.customer)
    customer_id = config["customer_id"]
    states = [state.upper() for state in config.get("states", [])]

    subscriber_profile = _load_subscriber_profile(args.db, config.get("subscriber_key"))
    if subscriber_profile and not subscriber_profile.get("active", 0):
        print("CONFIG_ERROR subscriber inactive", file=sys.stderr)
        raise SystemExit(1)

    territory_code = subscriber_profile.get("territory_code") or config.get("territory_code")
    content_filter = normalize_content_filter(
        subscriber_profile.get("content_filter") or config.get("content_filter", "high_medium")
    )
    include_low_fallback = bool(
        subscriber_profile.get("include_low_fallback")
        if subscriber_profile
        else config.get("include_low_fallback", False)
    )

    missing = preflight_missing_vars(config, args.dry_run)
    if missing:
        print(f"CONFIG_ERROR missing variables: {', '.join(missing)}", file=sys.stderr)
        raise SystemExit(1)

    recipients = collect_recipients(config, subscriber_profile, args.recipient_override)

    if not recipients:
        raise ValueError("No recipients configured (email_recipients, subscriber email, or --recipient-override).")

    branding = resolve_branding(config)

    logger.info(
        "Generating %s digest for customer=%s territory=%s recipients=%d",
        args.mode,
        customer_id,
        territory_code or "(none)",
        len(recipients),
    )

    conn = sqlite3.connect(args.db)
    skip_first_seen_filter = args.mode == "baseline"
    leads, low_fallback, filter_stats = get_leads_for_period(
        conn=conn,
        states=states,
        since_days=int(config["opened_window_days"]),
        new_only_days=int(config["new_only_days"]),
        skip_first_seen_filter=skip_first_seen_filter,
        territory_code=territory_code,
        content_filter=content_filter,
        include_low_fallback=include_low_fallback,
    )
    conn.close()

    logger.info("Leads after filters: %d", len(leads))

    hi_count = sum(1 for lead in leads if int(lead.get("lead_score") or 0) >= 10)
    states_label = "/".join(states)
    territory_suffix = f" | {territory_code}" if territory_code else ""

    if args.mode == "daily":
        subject = f"{states_label}{territory_suffix} | {gen_date} | {len(leads)} new | {hi_count} high (>=10)"
    else:
        subject = f"{states_label}{territory_suffix} | {gen_date} | {len(leads)} leads | {hi_count} high (>=10)"

    html_body = generate_digest_html(
        leads=leads,
        low_fallback=low_fallback,
        config=config,
        gen_date=gen_date,
        mode=args.mode,
        territory_code=territory_code,
        content_filter=content_filter,
        include_low_fallback=include_low_fallback,
        branding=branding,
    )
    text_body = generate_digest_text(
        leads=leads,
        low_fallback=low_fallback,
        config=config,
        gen_date=gen_date,
        mode=args.mode,
        territory_code=territory_code,
        content_filter=content_filter,
        include_low_fallback=include_low_fallback,
        branding=branding,
    )

    Path(args.output_dir).mkdir(parents=True, exist_ok=True)
    email_log_path = os.path.join(args.output_dir, "email_log.csv")
    suppression_log_path = os.path.join(args.output_dir, "suppression_log.csv")

    pilot_mode = bool(config.get("pilot_mode", PILOT_MODE_DEFAULT)) and not args.disable_pilot_guard
    whitelist = [email.lower() for email in config.get("pilot_whitelist", PILOT_WHITELIST_DEFAULT)]
    failed_sends = 0
    sent_or_dry_run = 0
    suppressed_count = 0
    pilot_skipped_count = 0

    for recipient in recipients:
        if pilot_mode and recipient not in whitelist:
            logger.warning("PILOT MODE: skipping %s (not in whitelist)", recipient)
            pilot_skipped_count += 1
            log_email_attempt(
                email_log_path,
                {
                    "timestamp": timestamp,
                    "customer_id": customer_id,
                    "mode": args.mode,
                    "recipient": recipient,
                    "subject": subject,
                    "status": "skipped_pilot_mode",
                    "territory_code": territory_code or "",
                    "content_filter": content_filter,
                },
            )
            continue

        if check_suppression(args.db, recipient):
            logger.info("Suppressed recipient: %s", recipient)
            suppressed_count += 1
            log_suppression(
                suppression_log_path,
                {
                    "timestamp": timestamp,
                    "customer_id": customer_id,
                    "recipient": recipient,
                    "reason": "in_suppression_list",
                    "territory_code": territory_code or "",
                },
            )
            append_unsubscribe_event(
                db_path=args.db,
                email=recipient,
                event_type="suppressed_before_send",
                reason="suppression_list",
                source="send_digest_email",
                customer_id=customer_id,
                territory_code=territory_code or "",
                output_dir=args.output_dir,
            )
            log_email_attempt(
                email_log_path,
                {
                    "timestamp": timestamp,
                    "customer_id": customer_id,
                    "mode": args.mode,
                    "recipient": recipient,
                    "subject": subject,
                    "status": "suppressed",
                    "territory_code": territory_code or "",
                    "content_filter": content_filter,
                },
            )
            continue

        success, message_id, error = send_email(
            recipient=recipient,
            subject=subject,
            html_body=html_body,
            text_body=text_body,
            customer_id=customer_id,
            territory_code=territory_code or "",
            branding=branding,
            dry_run=args.dry_run,
        )

        status = "sent" if success else "failed"
        if args.dry_run and success:
            status = "dry_run"
        if success:
            sent_or_dry_run += 1
        else:
            failed_sends += 1

        log_email_attempt(
            email_log_path,
            {
                "timestamp": timestamp,
                "customer_id": customer_id,
                "mode": args.mode,
                "recipient": recipient,
                "subject": subject,
                "status": status,
                "message_id": message_id,
                "error": error,
                "territory_code": territory_code or "",
                "content_filter": content_filter,
            },
        )

    print("\n" + "=" * 72)
    print("EMAIL DIGEST SUMMARY")
    print("=" * 72)
    print(f"Customer:                 {customer_id}")
    print(f"Mode:                     {args.mode}")
    print(f"Territory:                {territory_code or '(none)'}")
    print(f"Content filter:           {content_filter}")
    print(f"Low fallback enabled:     {'YES' if include_low_fallback else 'NO'}")
    print(f"Low fallback leads:       {len(low_fallback)}")
    print(f"Leads after filters:      {len(leads)}")
    print(f"Recipients requested:     {len(recipients)}")
    print(f"Sent/Dry-run:             {sent_or_dry_run}")
    print(f"Suppressed:               {suppressed_count}")
    print(f"Pilot-skipped:            {pilot_skipped_count}")
    print(f"Failed sends:             {failed_sends}")
    print(f"Pilot mode:               {'ON' if pilot_mode else 'OFF'}")
    print(f"Dry run:                  {'YES' if args.dry_run else 'NO'}")
    print("")
    print("Filter stats:")
    print(f"  Total before filter:    {filter_stats['total_before_filter']}")
    print(f"  Excl. date_opened:      {filter_stats['excluded_by_date_opened']}")
    print(f"  Excl. first_seen:       {filter_stats['excluded_by_first_seen']}")
    print(f"  Excl. territory:        {filter_stats['excluded_by_territory']}")
    print(f"  Matched area_office:    {filter_stats['matched_by_office']}")
    print(f"  Matched fallback city:  {filter_stats['matched_by_fallback']}")
    print(f"  Excl. content filter:   {filter_stats['excluded_by_content_filter']}")
    print(f"  Dedupe removed:         {filter_stats['dedupe_removed']}")
    print(f"  Fallback lows used:     {filter_stats['low_fallback_count']}")
    print("=" * 72)

    if failed_sends > 0:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
