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
import hashlib
import logging
import os
import smtplib
import sqlite3
import sys
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
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
    load_territory_definitions,
    normalize_content_filter,
)
from unsubscribe_utils import create_unsub_token, sign_registration
from email_footer import build_footer_html, build_footer_text

logger = logging.getLogger(__name__)

PILOT_MODE_DEFAULT = True
PILOT_WHITELIST_DEFAULT = ["cchevali@gmail.com"]

DEFAULT_REPLY_TO = "support@microflowops.com"
DEFAULT_FROM_LOCAL_PART = "alerts"
LOW_FALLBACK_LIMIT = 5
HEALTH_MIN_SHARE_DEFAULT = 0.1
HEALTH_MIN_TOTAL_DEFAULT = 20
SEND_WINDOW_MINUTES_DEFAULT = 20
HEALTH_ANCHORS_BY_TERRITORY = {
    "TX_TRIANGLE_V1": ["Houston", "Dallas/Fort Worth", "Austin", "San Antonio"],
}


def content_filter_label(value: str) -> str:
    mapping = {
        "high_medium": "High + Medium",
        "high": "High Only",
        "medium": "Medium Only",
        "low": "Low Only",
        "all": "All",
    }
    normalized = (value or "").strip().lower()
    if normalized in mapping:
        return mapping[normalized]
    return normalized.replace("_", " ").title() if normalized else ""


def territory_display_name(territory_code: str | None) -> str:
    if not territory_code:
        return ""
    try:
        definitions = load_territory_definitions()
    except Exception:
        return territory_code
    territory = definitions.get(territory_code, {})
    display = (territory.get("display_name") or territory.get("name") or "").strip()
    if display:
        return display
    description = (territory.get("description") or "").strip()
    if description:
        for token in [" OSHA", " area offices"]:
            if token in description:
                return description.split(token, 1)[0].strip()
        return description
    return territory_code


def _parse_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    text = value.strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1]
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        pass
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def _to_naive(dt: datetime | None) -> datetime | None:
    if not dt:
        return None
    if dt.tzinfo is None:
        return dt
    return dt.astimezone(timezone.utc).replace(tzinfo=None)


def _observed_datetime(lead: dict) -> datetime | None:
    changed_dt = _parse_timestamp(lead.get("changed_at"))
    first_dt = _parse_timestamp(lead.get("first_seen_at"))
    last_dt = _parse_timestamp(lead.get("last_seen_at"))
    candidates = [dt for dt in (changed_dt, first_dt, last_dt) if dt]
    if not candidates:
        return None
    dt = max(candidates)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _observed_timestamp(lead: dict, tz: ZoneInfo) -> str:
    dt = _observed_datetime(lead)
    if not dt:
        return "-"
    local_dt = dt.astimezone(tz)
    tz_label = "CT" if getattr(tz, "key", "") in {"America/Chicago", "US/Central"} else local_dt.strftime("%Z")
    return f"{local_dt.strftime('%Y-%m-%d %H:%M')} {tz_label}"


def _priority_label(score: int) -> str:
    if score >= 10:
        return "High"
    if score >= 6:
        return "Medium"
    return "Low"


def _tier_counts(leads: list[dict]) -> dict[str, int]:
    counts = {"high": 0, "medium": 0, "low": 0}
    for lead in leads:
        score = int(lead.get("lead_score") or 0)
        if score >= 10:
            counts["high"] += 1
        elif score >= 6:
            counts["medium"] += 1
        else:
            counts["low"] += 1
    return counts


def _format_lead_row(lead: dict) -> str:
    score = int(lead.get("lead_score") or 0)
    activity = str(lead.get("activity_nr") or lead.get("lead_id") or "").strip()
    opened = str(lead.get("date_opened") or "").strip()
    itype = str(lead.get("inspection_type") or "").strip()
    name = " ".join(str(lead.get("establishment_name") or "").strip().split())
    city = str(lead.get("site_city") or "").strip()
    state = str(lead.get("site_state") or "").strip()
    loc = ", ".join([part for part in [city, state] if part])
    parts = [
        f"score={score}",
        f"activity={activity}" if activity else "",
        f"opened={opened}" if opened else "",
        f"type={itype}" if itype else "",
        f"name={name}" if name else "",
        f"loc={loc}" if loc else "",
    ]
    return " | ".join([p for p in parts if p])


def ensure_render_log_table(conn: sqlite3.Connection) -> None:
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS render_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            subscriber_key TEXT NOT NULL,
            mode TEXT NOT NULL,
            territory_code TEXT NOT NULL,
            territory_date TEXT NOT NULL,
            digest_hash TEXT NOT NULL,
            rendered_at DATETIME NOT NULL,
            UNIQUE (subscriber_key, mode, territory_code, territory_date, digest_hash)
        )
        """
    )
    conn.commit()


def has_duplicate_render(
    conn: sqlite3.Connection,
    subscriber_key: str,
    mode: str,
    territory_code: str,
    territory_date: str,
    digest_hash: str,
) -> bool:
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT 1 FROM render_log
        WHERE subscriber_key = ? AND mode = ? AND territory_code = ? AND territory_date = ? AND digest_hash = ?
        LIMIT 1
        """,
        (subscriber_key, mode, territory_code, territory_date, digest_hash),
    )
    return cursor.fetchone() is not None


def record_render_log(
    conn: sqlite3.Connection,
    subscriber_key: str,
    mode: str,
    territory_code: str,
    territory_date: str,
    digest_hash: str,
    rendered_at: str,
) -> None:
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT OR IGNORE INTO render_log
            (subscriber_key, mode, territory_code, territory_date, digest_hash, rendered_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (subscriber_key, mode, territory_code, territory_date, digest_hash, rendered_at),
    )
    conn.commit()



def build_coverage_line(total_counts: dict, shown_counts: dict) -> str:
    hidden_parts: list[str] = []
    for label, key in (("high-priority", "high"), ("medium-priority", "medium"), ("low-priority", "low")):
        total = int(total_counts.get(key, 0))
        shown = int(shown_counts.get(key, 0))
        hidden = max(0, total - shown)
        if hidden > 0:
            suffix = "signals" if hidden != 1 else "signal"
            hidden_parts.append(f"{hidden} {label} {suffix}")
    if not hidden_parts:
        return ""
    return "Also observed (not shown): " + ", ".join(hidden_parts) + "."

def _build_preheader(leads: list[dict]) -> str:
    if not leads:
        return "No new OSHA activity signals today."
    parts = []
    for lead in leads[:3]:
        company = (lead.get("establishment_name") or "Unknown").strip()
        signal = (lead.get("inspection_type") or "Signal").strip()
        parts.append(f"{company} ({signal})")
    return "Top signals: " + " | ".join(parts)


def compute_digest_hash(
    leads: list[dict],
    low_fallback: list[dict],
    mode: str,
    territory_code: str | None,
    content_filter: str,
    include_low_fallback: bool,
) -> str:
    """Stable digest hash over normalized lead identifiers and config flags."""
    def _lead_id(lead: dict) -> str:
        return str(lead.get("activity_nr") or lead.get("lead_id") or "").strip()

    main_ids = sorted([_lead_id(lead) for lead in leads if _lead_id(lead)])
    low_ids = sorted([_lead_id(lead) for lead in low_fallback if _lead_id(lead)])
    payload = {
        "mode": mode,
        "territory": territory_code or "",
        "content_filter": content_filter,
        "include_low_fallback": bool(include_low_fallback),
        "leads": main_ids,
        "low_fallback": low_ids,
    }
    blob = json.dumps(payload, sort_keys=True).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()


def ensure_send_log_table(conn: sqlite3.Connection) -> None:
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS send_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            subscriber_key TEXT NOT NULL,
            mode TEXT NOT NULL,
            territory_code TEXT NOT NULL,
            territory_date TEXT NOT NULL,
            digest_hash TEXT NOT NULL,
            sent_at TEXT NOT NULL,
            sent_count INTEGER NOT NULL DEFAULT 0
        )
        """
    )
    cursor.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_send_log_unique
        ON send_log (subscriber_key, mode, territory_code, territory_date, digest_hash)
        """
    )
    conn.commit()


def has_duplicate_send(
    conn: sqlite3.Connection,
    subscriber_key: str,
    mode: str,
    territory_code: str,
    territory_date: str,
    digest_hash: str,
) -> bool:
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT 1 FROM send_log
        WHERE subscriber_key = ?
          AND mode = ?
          AND territory_code = ?
          AND territory_date = ?
          AND digest_hash = ?
        LIMIT 1
        """,
        (subscriber_key, mode, territory_code, territory_date, digest_hash),
    )
    return cursor.fetchone() is not None


def record_send_log(
    conn: sqlite3.Connection,
    subscriber_key: str,
    mode: str,
    territory_code: str,
    territory_date: str,
    digest_hash: str,
    sent_at: str,
    sent_count: int,
) -> None:
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT OR IGNORE INTO send_log
            (subscriber_key, mode, territory_code, territory_date, digest_hash, sent_at, sent_count)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (subscriber_key, mode, territory_code, territory_date, digest_hash, sent_at, int(sent_count)),
    )
    conn.commit()


def resolve_timezone(config: dict, territory_code: str | None) -> ZoneInfo:
    tz_name = (config.get("timezone") or "").strip()
    if not tz_name and territory_code:
        try:
            territory = load_territory_definitions().get(territory_code, {})
            tz_name = (territory.get("timezone") or "").strip()
        except Exception:
            tz_name = ""
    if not tz_name:
        tz_name = "America/Chicago"
    try:
        return ZoneInfo(tz_name)
    except Exception:
        return ZoneInfo("America/Chicago")


def _parse_send_time_local(value: str | None) -> tuple[int, int] | None:
    if not value:
        return None
    text = value.strip()
    if not text:
        return None
    parts = text.split(":")
    if len(parts) != 2:
        return None
    try:
        hour = int(parts[0])
        minute = int(parts[1])
    except ValueError:
        return None
    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        return None
    return hour, minute


def _coerce_send_window_minutes(value: object) -> int:
    try:
        minutes = int(value)
    except (TypeError, ValueError):
        return SEND_WINDOW_MINUTES_DEFAULT
    if minutes <= 0:
        return SEND_WINDOW_MINUTES_DEFAULT
    return minutes


def _within_send_window(
    now_local: datetime,
    send_time_local: str | None,
    window_minutes: int,
) -> tuple[bool, str, datetime | None, datetime | None]:
    parsed = _parse_send_time_local(send_time_local)
    if not parsed:
        return False, "send_time_local missing/invalid", None, None
    hour, minute = parsed
    scheduled = now_local.replace(hour=hour, minute=minute, second=0, microsecond=0)
    window_start = scheduled - timedelta(minutes=window_minutes)
    window_end = scheduled + timedelta(minutes=window_minutes)
    if window_start <= now_local <= window_end:
        return True, "", window_start, window_end
    return False, "outside send window", window_start, window_end


def update_subscriber_last_sent_at(db_path: str, subscriber_key: str, timestamp: str) -> None:
    if not subscriber_key:
        return
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    if not _has_column(conn, "subscribers", "last_sent_at"):
        cursor.execute("ALTER TABLE subscribers ADD COLUMN last_sent_at DATETIME")
        conn.commit()
    cursor.execute(
        "UPDATE subscribers SET last_sent_at = ? WHERE subscriber_key = ?",
        (timestamp, subscriber_key),
    )
    conn.commit()
    conn.close()


def print_area_office_debug(conn: sqlite3.Connection) -> None:
    cutoff = datetime.now() - timedelta(days=30)
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT area_office, site_city, mail_city, site_address1, first_seen_at, last_seen_at
        FROM inspections
        WHERE site_state = 'TX'
        """
    )
    rows = cursor.fetchall()
    total = 0
    area_office_counts: dict[str, int] = {}
    site_city_counts: dict[str, int] = {}
    mail_city_counts: dict[str, int] = {}
    address_counts: dict[str, int] = {}

    for office, site_city, mail_city, site_address1, first_seen, last_seen in rows:
        first_dt = _parse_timestamp(first_seen)
        last_dt = _parse_timestamp(last_seen)
        if not ((first_dt and first_dt >= cutoff) or (last_dt and last_dt >= cutoff)):
            continue
        total += 1
        if office:
            area_office_counts[office] = area_office_counts.get(office, 0) + 1
        if site_city:
            site_city_counts[site_city] = site_city_counts.get(site_city, 0) + 1
        if mail_city:
            mail_city_counts[mail_city] = mail_city_counts.get(mail_city, 0) + 1
        if site_address1:
            address_counts[site_address1] = address_counts.get(site_address1, 0) + 1

    def _print_samples(label: str, counts: dict[str, int]) -> None:
        print(f"{label} non-null rate: {(len(counts) / total * 100) if total else 0:.1f}% ({len(counts)} distinct)")
        for value, count in sorted(counts.items(), key=lambda kv: kv[1], reverse=True)[:10]:
            print(f"  {value} ({count})")

    print("TX area_office/location debug (last 30 days):")
    print(f"Total TX records in window: {total}")
    _print_samples("area_office", area_office_counts)
    _print_samples("site_city", site_city_counts)
    _print_samples("mail_city", mail_city_counts)
    _print_samples("site_address1", address_counts)




def _summarize_health(leads: list[dict]) -> dict:
    priority_counts = Counter()
    type_counts = Counter()
    city_counts = Counter()
    for lead in leads:
        score = int(lead.get("lead_score") or 0)
        priority_counts[_priority_label(score).lower()] += 1
        itype = (lead.get("inspection_type") or "Unknown").strip() or "Unknown"
        type_counts[itype] += 1
        city = (lead.get("site_city") or "").strip()
        if city:
            city_counts[city] += 1
    top_cities = [{"city": city, "count": count} for city, count in city_counts.most_common(10)]
    return {
        "total": len(leads),
        "priority_counts": dict(priority_counts),
        "type_counts": dict(type_counts),
        "top_cities": top_cities,
    }


def compute_territory_health(
    conn: sqlite3.Connection,
    territory_code: str,
    states: list[str],
    now_utc: datetime | None = None,
    min_share: float = HEALTH_MIN_SHARE_DEFAULT,
    min_total: int = HEALTH_MIN_TOTAL_DEFAULT,
) -> dict:
    now_utc = now_utc or datetime.now(timezone.utc)
    anchors = HEALTH_ANCHORS_BY_TERRITORY.get(territory_code, [])
    placeholders = ",".join(["?" for _ in states])
    changed_at_expr = "changed_at" if _has_column(conn, "inspections", "changed_at") else "NULL AS changed_at"
    cursor = conn.cursor()
    cursor.execute(
        f"""
        SELECT
            activity_nr,
            site_state,
            site_city,
            area_office,
            inspection_type,
            lead_score,
            first_seen_at,
            last_seen_at,
            {changed_at_expr}
        FROM inspections
        WHERE site_state IN ({placeholders})
          AND parse_invalid = 0
        """,
        tuple(states),
    )
    columns = [desc[0] for desc in cursor.description]
    rows = [dict(zip(columns, row)) for row in cursor.fetchall()]

    window_24 = now_utc - timedelta(hours=24)
    window_14 = now_utc - timedelta(days=14)
    tx_24: list[dict] = []
    tx_14: list[dict] = []

    for row in rows:
        observed_dt = _observed_datetime(row)
        if not observed_dt:
            continue
        if observed_dt >= window_14:
            tx_14.append(row)
        if observed_dt >= window_24:
            tx_24.append(row)

    terr_24, _ = filter_by_territory(tx_24, territory_code)
    terr_14, _ = filter_by_territory(tx_14, territory_code)

    tx_summary_24 = _summarize_health(tx_24)
    tx_summary_14 = _summarize_health(tx_14)
    terr_summary_24 = _summarize_health(terr_24)
    terr_summary_14 = _summarize_health(terr_14)
    total_24 = tx_summary_24["total"]
    total_14 = tx_summary_14["total"]
    share_24 = (terr_summary_24["total"] / total_24) if total_24 else 0.0
    share_14 = (terr_summary_14["total"] / total_14) if total_14 else 0.0

    anchor_checks: dict[str, bool] = {}
    if anchors:
        anchor_leads = [
            {"activity_nr": f"anchor_{idx}", "site_state": "TX", "site_city": anchor, "area_office": ""}
            for idx, anchor in enumerate(anchors)
        ]
        matched, _ = filter_by_territory(anchor_leads, territory_code)
        matched_set = {lead.get("site_city") for lead in matched}
        anchor_checks = {anchor: anchor in matched_set for anchor in anchors}

    alerts: list[str] = []
    if anchors and not all(anchor_checks.values()):
        alerts.append("anchor_mismatch")
    if total_24 >= min_total and share_24 < min_share:
        alerts.append("share_low_24h")
    if total_14 >= min_total and share_14 < min_share:
        alerts.append("share_low_14d")

    return {
        "territory_code": territory_code,
        "run_at": now_utc.isoformat(),
        "window_24": {
            "tx_total": total_24,
            "territory_total": terr_summary_24["total"],
            "share": share_24,
            "tx_priority_counts": tx_summary_24["priority_counts"],
            "tx_type_counts": tx_summary_24["type_counts"],
            "tx_top_cities": tx_summary_24["top_cities"],
            "territory_priority_counts": terr_summary_24["priority_counts"],
            "territory_type_counts": terr_summary_24["type_counts"],
            "territory_top_cities": terr_summary_24["top_cities"],
        },
        "window_14": {
            "tx_total": total_14,
            "territory_total": terr_summary_14["total"],
            "share": share_14,
            "tx_priority_counts": tx_summary_14["priority_counts"],
            "tx_type_counts": tx_summary_14["type_counts"],
            "tx_top_cities": tx_summary_14["top_cities"],
            "territory_priority_counts": terr_summary_14["priority_counts"],
            "territory_type_counts": terr_summary_14["type_counts"],
            "territory_top_cities": terr_summary_14["top_cities"],
        },
        "anchor_checks": anchor_checks,
        "alerts": alerts,
    }


def store_territory_health(conn: sqlite3.Connection, health: dict) -> None:
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS territory_health (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_at TEXT,
            territory_code TEXT,
            window_hours INTEGER,
            tx_total INTEGER,
            territory_total INTEGER,
            share REAL,
            priority_counts TEXT,
            type_counts TEXT,
            top_cities TEXT,
            alerts TEXT,
            anchor_checks TEXT
        )
        """
    )
    run_at = health["run_at"]
    territory_code = health["territory_code"]
    alerts = json.dumps(health.get("alerts", []))
    anchor_checks = json.dumps(health.get("anchor_checks", {}))
    for window_hours, window in ((24, health["window_24"]), (336, health["window_14"])):
        cursor.execute(
            """
            INSERT INTO territory_health (
                run_at, territory_code, window_hours, tx_total, territory_total, share,
                priority_counts, type_counts, top_cities, alerts, anchor_checks
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_at,
                territory_code,
                window_hours,
                int(window["tx_total"]),
                int(window["territory_total"]),
                float(window["share"]),
                json.dumps({"tx": window.get("tx_priority_counts", {}), "territory": window.get("territory_priority_counts", {})}),
                json.dumps({"tx": window.get("tx_type_counts", {}), "territory": window.get("territory_type_counts", {})}),
                json.dumps({"tx": window.get("tx_top_cities", []), "territory": window.get("territory_top_cities", [])}),
                alerts,
                anchor_checks,
            ),
        )
    conn.commit()


def format_territory_health_summary(health: dict) -> tuple[str, str]:
    alerts = health.get("alerts") or []
    alert_text = "None" if not alerts else ", ".join(alerts)
    anchors = health.get("anchor_checks") or {}
    anchor_lines = [f"{name}: {'OK' if ok else 'FAIL'}" for name, ok in anchors.items()]
    anchor_text = "; ".join(anchor_lines) if anchor_lines else "No anchors configured"
    window_24 = health["window_24"]
    window_14 = health["window_14"]

    def _top_cities(window: dict) -> str:
        cities = window.get("territory_top_cities", [])
        if not cities:
            return "None"
        return ", ".join(f"{item['city']} ({item['count']})" for item in cities[:5])

    top_24 = _top_cities(window_24)
    top_14 = _top_cities(window_14)

    text = (
        "Territory health (admin only)\n"
        f"24h: TX total {window_24['tx_total']}, territory {window_24['territory_total']} "
        f"(share {window_24['share']:.2%})\n"
        f"14d: TX total {window_14['tx_total']}, territory {window_14['territory_total']} "
        f"(share {window_14['share']:.2%})\n"
        f"Top cities (24h territory): {top_24}\n"
        f"Top cities (14d territory): {top_14}\n"
        f"Anchors: {anchor_text}\n"
        f"Alerts: {alert_text}"
    )

    html = (
        '<h3 style="margin-top: 24px;">Territory health (admin only)</h3>'
        f"<p>24h: TX total {window_24['tx_total']}, territory {window_24['territory_total']} "
        f"(share {window_24['share']:.2%})<br>"
        f"14d: TX total {window_14['tx_total']}, territory {window_14['territory_total']} "
        f"(share {window_14['share']:.2%})<br>"
        f"Top cities (24h territory): {top_24}<br>"
        f"Top cities (14d territory): {top_14}<br>"
        f"Anchors: {anchor_text}<br>"
        f"Alerts: {alert_text}</p>"
    )
    return text, html


def resolve_admin_recipient(config: dict) -> str:
    return (
        (config.get("admin_email") or "").strip().lower()
        or (os.getenv("CHASE_EMAIL") or "").strip().lower()
        or (os.getenv("ADMIN_EMAIL") or "").strip().lower()
        or "cchevali@gmail.com"
    )


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
    if not _has_column(conn, "subscribers", "last_sent_at"):
        cursor.execute("ALTER TABLE subscribers ADD COLUMN last_sent_at DATETIME")
        conn.commit()
    if not _has_column(conn, "subscribers", "send_enabled"):
        cursor.execute("ALTER TABLE subscribers ADD COLUMN send_enabled INTEGER NOT NULL DEFAULT 0")
        conn.commit()

    cursor.execute(
        """
        SELECT subscriber_key, email, recipients_json, active, territory_code, content_filter, include_low_fallback, last_sent_at, send_enabled
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
        "last_sent_at": row[7] if len(row) > 7 else None,
        "send_enabled": bool(row[8]) if len(row) > 8 else False,
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
    window_start: datetime | None = None,
    new_only_cutoff: datetime | None = None,
    include_changed: bool = False,
    use_opened_window: bool = False,
) -> tuple[list[dict], list[dict], dict]:
    today = datetime.now()
    window_cutoff = window_start or (today - timedelta(days=since_days))
    effective_new_only = new_only_cutoff or (today - timedelta(days=new_only_days))
    window_cutoff = _to_naive(window_cutoff)
    effective_new_only = _to_naive(effective_new_only)

    lead_id_expr = (
        "lead_id"
        if _has_column(conn, "inspections", "lead_id")
        else "('osha:inspection:' || activity_nr) AS lead_id"
    )
    area_office_expr = "area_office" if _has_column(conn, "inspections", "area_office") else "NULL AS area_office"
    changed_at_expr = "changed_at" if _has_column(conn, "inspections", "changed_at") else "NULL AS changed_at"
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
            {changed_at_expr},
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

    time_filtered = []
    excluded_by_time_window = 0
    excluded_by_new_only = 0

    for lead in all_results:
        first_seen_dt = _to_naive(_parse_timestamp(lead.get("first_seen_at")))
        last_seen_dt = _to_naive(_parse_timestamp(lead.get("last_seen_at")))
        changed_dt = _to_naive(_parse_timestamp(lead.get("changed_at")))

        in_window = False
        if use_opened_window:
            date_opened = lead.get("date_opened")
            if date_opened:
                try:
                    opened_dt = datetime.strptime(date_opened, "%Y-%m-%d")
                    if opened_dt >= window_cutoff:
                        in_window = True
                except ValueError:
                    pass
            if not in_window:
                if first_seen_dt and first_seen_dt >= window_cutoff:
                    in_window = True
                if include_changed and changed_dt and changed_dt >= window_cutoff:
                    in_window = True
        else:
            if first_seen_dt and first_seen_dt >= window_cutoff:
                in_window = True
            if include_changed and changed_dt and changed_dt >= window_cutoff:
                in_window = True

        if not in_window:
            excluded_by_time_window += 1
            continue

        if not skip_first_seen_filter and effective_new_only:
            is_recent = False
            if first_seen_dt and first_seen_dt >= effective_new_only:
                is_recent = True
            if include_changed and changed_dt and changed_dt >= effective_new_only:
                is_recent = True
            if not is_recent:
                excluded_by_new_only += 1
                continue

        time_filtered.append(lead)

    territory_filtered, territory_stats = filter_by_territory(time_filtered, territory_code)
    content_filtered, excluded_content = apply_content_filter(territory_filtered, content_filter)
    deduped, dedupe_removed = dedupe_by_activity_nr(content_filtered)
    final_leads = deduped

    low_fallback = []
    if (
        content_filter == "high_medium"
        and len(final_leads) == 0
        and include_low_fallback
    ):
        fallback_base, _ = dedupe_by_activity_nr(territory_filtered)
        low_candidates = [lead for lead in fallback_base if int(lead.get("lead_score") or 0) < 6]
        low_candidates.sort(
            key=lambda lead: (int(lead.get("lead_score") or 0), lead.get("date_opened") or ""),
            reverse=True,
        )
        low_fallback = low_candidates[:LOW_FALLBACK_LIMIT]

    def _priority_counts(rows: list[dict]) -> dict:
        counts = {"high": 0, "medium": 0, "low": 0}
        for row in rows:
            score = int(row.get("lead_score") or 0)
            if score >= 10:
                counts["high"] += 1
            elif score >= 6:
                counts["medium"] += 1
            else:
                counts["low"] += 1
        return counts

    stats = {
        "total_candidates": len(all_results),
        "after_time_window": len(time_filtered),
        "after_territory": len(territory_filtered),
        "after_content_filter": len(content_filtered),
        "after_dedupe": len(final_leads),
        "final_leads": len(final_leads),
        "excluded_by_time_window": excluded_by_time_window,
        "excluded_by_new_only": excluded_by_new_only,
        "excluded_by_territory": territory_stats["excluded_state"] + territory_stats["excluded_territory"],
        "matched_by_office": territory_stats["matched_by_office"],
        "matched_by_fallback": territory_stats["matched_by_fallback"],
        "excluded_by_content_filter": excluded_content,
        "dedupe_removed": dedupe_removed,
        "low_fallback_count": len(low_fallback),
        "priority_counts": _priority_counts(territory_filtered),
        "shown_priority_counts": _priority_counts(final_leads),
    }

    return final_leads, low_fallback, stats


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


def register_unsub_token(
    unsub_token: str,
    recipient_email: str,
    campaign_id: str,
    dry_run: bool,
    retries: int = 2,
    timeout: int = 5,
) -> tuple[bool, int | None, str]:
    if dry_run:
        return True, None, ""
    unsub_endpoint = os.getenv("UNSUB_ENDPOINT_BASE", "").strip()
    secret = os.getenv("UNSUB_SECRET", "").strip()
    if not unsub_endpoint or not secret:
        return False, None, "missing_unsub_endpoint_or_secret"
    if not unsub_token or "." not in unsub_token:
        return False, None, "invalid_unsub_token"

    token_id = unsub_token.split(".", 1)[0]
    register_url = unsub_endpoint.rstrip("/") + "/register"
    auth = sign_registration(token_id, recipient_email, secret)

    last_error = ""
    last_status = None
    for attempt in range(1, retries + 2):
        try:
            import requests
            resp = requests.post(
                register_url,
                json={"token_id": token_id, "email": recipient_email, "campaign_id": campaign_id},
                headers={"X-Unsub-Auth": auth},
                timeout=timeout,
            )
            last_status = resp.status_code
            if resp.status_code in (200, 204):
                return True, resp.status_code, ""
            last_error = f"http_{resp.status_code}"
            if resp.status_code == 429:
                time.sleep(2 * attempt)
        except Exception as e:
            last_error = str(e)
        if attempt <= retries:
            time.sleep(0.5)
    return False, last_status, last_error


def build_unsubscribe_payload(
    recipient: str,
    campaign_id: str,
    reply_to_email: str,
    dry_run: bool,
) -> tuple[str, str | None, str]:
    mailto = f"mailto:{reply_to_email}?subject=unsubscribe"
    unsub_endpoint = os.getenv("UNSUB_ENDPOINT_BASE", "").strip()

    if not unsub_endpoint:
        return f"<{mailto}>", None, ""

    signed_token = create_unsub_token(recipient, campaign_id)
    sep = "&" if "?" in unsub_endpoint else "?"
    one_click_url = f"{unsub_endpoint}{sep}token={signed_token}"

    ok, status, err = register_unsub_token(
        signed_token,
        recipient,
        campaign_id,
        dry_run,
        retries=2,
        timeout=5,
    )
    if not ok:
        print(f"[WARN] one-click registration failed (status={status}, error={err})")
        return f"<{mailto}>", None, ""

    return f"<{mailto}>, <{one_click_url}>", "List-Unsubscribe=One-Click", one_click_url

def _lead_rows_html(rows: list[dict], max_rows: int, include_area_office: bool, tz: ZoneInfo) -> str:
    if not rows:
        return "<p><em>No leads match this section.</em></p>"

    parts = ['<table border="1" cellpadding="6" cellspacing="0" style="border-collapse: collapse; width: 100%;">']
    if include_area_office:
        parts.append("<tr><th>Priority</th><th>Company</th><th>City</th><th>Area Office</th><th>Signal</th><th>Observed</th><th>Opened</th></tr>")
    else:
        parts.append("<tr><th>Priority</th><th>Company</th><th>City</th><th>Signal</th><th>Observed</th><th>Opened</th></tr>")
    for lead in rows[:max_rows]:
        company = (lead.get("establishment_name") or "Unknown")[:48]
        city = lead.get("site_city") or "-"
        state = lead.get("site_state") or "-"
        itype = lead.get("inspection_type") or "-"
        date_opened = lead.get("date_opened") or "-"
        observed = _observed_timestamp(lead, tz)
        score = int(lead.get("lead_score") or 0)
        priority = _priority_label(score)
        url = lead.get("source_url") or "#"
        company_html = f'<a href="{url}">{company}</a>' if url and url != "#" else company
        if include_area_office:
            area_office = lead.get("area_office") or ""
            parts.append(
                f"<tr><td>{priority}</td><td>{company_html}</td><td>{city}, {state}</td><td>{area_office}</td><td>{itype}</td><td>{observed}</td><td>{date_opened}</td></tr>"
            )
        else:
            parts.append(
                f"<tr><td>{priority}</td><td>{company_html}</td><td>{city}, {state}</td><td>{itype}</td><td>{observed}</td><td>{date_opened}</td></tr>"
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
    report_label: str | None = None,
    footer_html: str | None = None,
    summary_label: str | None = None,
    coverage_line: str | None = None,
    health_summary_html: str | None = None,
    tz: ZoneInfo | None = None,
) -> str:
    states = config["states"]
    top_k_overall = config.get("top_k_overall", 25)
    top_k_per_state = config.get("top_k_per_state", 10)
    territory_label = territory_display_name(territory_code)

    mode_label = "BASELINE" if mode == "baseline" else "DAILY"
    state_counts: dict[str, int] = {}
    for lead in leads:
        st = (lead.get("site_state") or "UNK").upper()
        state_counts[st] = state_counts.get(st, 0) + 1
    unique_states = [state for state in state_counts.keys() if state]
    main_limit = min(10, top_k_overall)
    main_rows = leads[:main_limit]
    include_area_office_main = any((lead.get("area_office") or "").strip() for lead in main_rows)
    include_area_office_all = any((lead.get("area_office") or "").strip() for lead in leads)
    summary_line = summary_label or f"{len(leads)} signals"
    preheader = _build_preheader(leads)
    tz = tz or ZoneInfo("America/Chicago")

    html: list[str] = []
    html.append("<!DOCTYPE html>")
    html.append("<html><head><meta charset=\"utf-8\"></head>")
    html.append('<body style="font-family: Arial, sans-serif; max-width: 900px; margin: 0 auto; padding: 20px; background-color: #f7f9fc;">')
    html.append(
        f'<span style="display:none;visibility:hidden;opacity:0;color:transparent;height:0;width:0;max-height:0;max-width:0;overflow:hidden;">{preheader}</span>'
    )
    html.append('<div style="background-color: #ffffff; padding: 24px; border-radius: 8px;">')

    html.append(f"<h1 style=\"margin-top: 0; color: #1a1a2e;\">OSHA Lead Digest ({mode_label})</h1>")
    if report_label:
        html.append(f"<p style=\"color: #1a1a2e;\"><strong>{report_label}</strong></p>")
    html.append(f"<p style=\"color: #555;\">{gen_date} | {'/'.join(states)}</p>")
    if territory_label:
        html.append(f"<p style=\"color: #555;\"><strong>Territory:</strong> {territory_label}</p>")

    html.append('<div style="background-color: #eef5ff; padding: 14px; border-radius: 6px; margin: 16px 0;">')
    html.append(f"<p style=\"margin: 0;\"><strong>{summary_line}</strong></p>")
    html.append("</div>")
    if coverage_line:
        html.append(f"<p style=\"color: #555;\">{coverage_line}</p>")

    if len(leads) == 0 and mode == "daily":
        territory_text = territory_label or "/".join(states)
        if report_label:
            html.append(
                f"<p><strong>No OSHA activity signals found in the starter snapshot window for {territory_text}.</strong></p>"
            )
        else:
            html.append(
                f"<p><strong>No new OSHA activity signals since last send for {territory_text}.</strong></p>"
            )
        if include_low_fallback and low_fallback:
            html.append(f"<h2>Low Signals (Fallback) - Top {len(low_fallback)}</h2>")
            html.append(_lead_rows_html(low_fallback, LOW_FALLBACK_LIMIT, include_area_office_all, tz))
    else:
        if len(unique_states) > 1:
            html.append("<ul>")
            for state in sorted(unique_states):
                html.append(f"<li>{state}: {state_counts.get(state, 0)} signals</li>")
            html.append("</ul>")

        html.append("<h2>Signals</h2>")
        html.append(_lead_rows_html(main_rows, len(main_rows), include_area_office_main, tz))

        if len(leads) > main_limit:
            html.append(f"<h2>All Signals ({len(leads)})</h2>")
            html.append(_lead_rows_html(leads, len(leads), include_area_office_all, tz))

        if include_low_fallback and low_fallback:
            html.append(f"<h2>Low Signals (Fallback) - Top {len(low_fallback)}</h2>")
            html.append(_lead_rows_html(low_fallback, LOW_FALLBACK_LIMIT, include_area_office_all, tz))

    html.append(
        "<p style=\"color: #555; font-size: 12px;\">Accident, Complaint, and Referral describe OSHA activity signals (not citations).</p>"
    )

    if health_summary_html:
        html.append(health_summary_html)

    if footer_html:
        html.append(footer_html)

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
    report_label: str | None = None,
    footer_text: str | None = None,
    summary_label: str | None = None,
    coverage_line: str | None = None,
    health_summary_text: str | None = None,
    tz: ZoneInfo | None = None,
) -> str:
    states = config["states"]
    mode_label = "BASELINE" if mode == "baseline" else "DAILY"
    territory_label = territory_display_name(territory_code)
    state_counts: dict[str, int] = {}
    for lead in leads:
        st = (lead.get("site_state") or "UNK").upper()
        state_counts[st] = state_counts.get(st, 0) + 1
    unique_states = [state for state in state_counts.keys() if state]
    main_limit = min(10, config.get("top_k_overall", 25))
    main_rows = leads[:main_limit]
    include_area_office_main = any((lead.get("area_office") or "").strip() for lead in main_rows)
    include_area_office_all = any((lead.get("area_office") or "").strip() for lead in leads)
    summary_line = summary_label or f"{len(leads)} signals"
    tz = tz or ZoneInfo("America/Chicago")

    lines = [
        f"OSHA Lead Digest ({mode_label}) - {gen_date}",
        f"Coverage: {'/'.join(states)}",
    ]
    if report_label:
        lines.append(report_label)
    if territory_label:
        lines.append(f"Territory: {territory_label}")
    lines.append("=" * 70)
    lines.append(summary_line)
    if coverage_line:
        lines.append(coverage_line)

    if len(leads) == 0 and mode == "daily":
        territory_text = territory_label or "/".join(states)
        lines.append("")
        if report_label:
            lines.append(f"No OSHA activity signals found in the starter snapshot window for {territory_text}.")
        else:
            lines.append(f"No new OSHA activity signals since last send for {territory_text}.")
        if include_low_fallback and low_fallback:
            lines.append("")
            lines.append("Low Signals (Fallback):")
            for lead in low_fallback:
                lines.append(
                    f"- {(lead.get('establishment_name') or 'Unknown')} | "
                    f"{(lead.get('site_city') or '-')}, {(lead.get('site_state') or '-')} | "
                    f"Score {int(lead.get('lead_score') or 0)}"
                )
    else:
        if len(unique_states) > 1:
            lines.append("")
            lines.append("State breakdown:")
            for state in sorted(unique_states):
                lines.append(f"- {state}: {state_counts.get(state, 0)} signals")

        lines.append("")
        lines.append("Signals:")
        for lead in main_rows:
            lines.append("")
            lines.append(f"- {(lead.get('establishment_name') or 'Unknown')}")
            priority = _priority_label(int(lead.get("lead_score") or 0))
            location_line = f"  {(lead.get('site_city') or '-')}, {(lead.get('site_state') or '-')}"
            if include_area_office_main:
                location_line += f" | Area Office: {(lead.get('area_office') or '-')}"
            lines.append(location_line)
            lines.append(
                f"  Priority: {priority} | Signal: {(lead.get('inspection_type') or '-')}"
            )
            lines.append(
                f"  Observed: {_observed_timestamp(lead, tz)} | Opened: {(lead.get('date_opened') or '-')}"
            )
            lines.append(f"  {(lead.get('source_url') or '#')}")

        if len(leads) > main_limit:
            lines.append("")
            lines.append(f"All signals ({len(leads)}):")
            for lead in leads:
                priority = _priority_label(int(lead.get("lead_score") or 0))
                lines.append(
                    f"- {(lead.get('establishment_name') or 'Unknown')} | "
                    f"{(lead.get('site_city') or '-')}, {(lead.get('site_state') or '-')} | "
                    f"{priority}"
                )

        if include_low_fallback and low_fallback:
            lines.append("")
            lines.append("Low Signals (Fallback):")
            for lead in low_fallback:
                priority = _priority_label(int(lead.get("lead_score") or 0))
                lines.append(
                    f"- {(lead.get('establishment_name') or 'Unknown')} | "
                    f"{(lead.get('site_city') or '-')}, {(lead.get('site_state') or '-')} | "
                    f"{priority}"
                )

    lines.append("")
    lines.append("Accident, Complaint, and Referral describe OSHA activity signals (not citations).")

    if health_summary_text:
        lines.append("")
        lines.append(health_summary_text)

    if footer_text:
        lines.append("")
        lines.append(footer_text)

    return "\n".join(lines)

def build_email_message(
    recipient: str,
    subject: str,
    html_body: str,
    text_body: str,
    customer_id: str,
    territory_code: str,
    branding: dict,
    list_unsub: str,
    list_unsub_post: str | None,
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
    list_unsub: str,
    list_unsub_post: str | None,
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
        list_unsub=list_unsub,
        list_unsub_post=list_unsub_post,
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


def send_safe_mode_alert(subject: str, body: str, recipient: str) -> None:
    smtp_host = os.environ.get("SMTP_HOST", "")
    smtp_port_text = os.environ.get("SMTP_PORT", "")
    smtp_user = os.environ.get("SMTP_USER", "")
    smtp_pass = os.environ.get("SMTP_PASS", "")

    if not (smtp_host and smtp_port_text and smtp_user and smtp_pass):
        print("SAFE_MODE_ALERT_EMAIL_SKIPPED missing SMTP configuration")
        return

    try:
        smtp_port = int(smtp_port_text)
    except ValueError:
        print("SAFE_MODE_ALERT_EMAIL_SKIPPED invalid SMTP_PORT")
        return

    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = subject
    msg["From"] = smtp_user
    msg["To"] = recipient

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
    except Exception as exc:
        print(f"SAFE_MODE_ALERT_EMAIL_FAILED {exc}")


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
        "--send-live",
        action="store_true",
        help="Allow live sends to customer recipients (requires allow_live_send and send_enabled)",
    )
    parser.add_argument(
        "--debug-area-offices",
        action="store_true",
        help="Print distinct TX area_office values seen in last 30 days and exit",
    )
    parser.add_argument(
        "--health-summary",
        action="store_true",
        help="Include admin-only territory health summary in the email (safe-mode only)",
    )
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

    timestamp = datetime.now(timezone.utc).isoformat()

    if args.debug_area_offices:
        conn = sqlite3.connect(args.db)
        print_area_office_debug(conn)
        conn.close()
        raise SystemExit(0)

    config = load_customer_config(args.customer)
    customer_id = config["customer_id"]
    states = [state.upper() for state in config.get("states", [])]

    subscriber_profile = _load_subscriber_profile(args.db, config.get("subscriber_key"))
    if subscriber_profile and not subscriber_profile.get("active", 0):
        print("CONFIG_ERROR subscriber inactive", file=sys.stderr)
        raise SystemExit(1)

    territory_code = subscriber_profile.get("territory_code") or config.get("territory_code")
    tz = resolve_timezone(config, territory_code)
    now_local = datetime.now(tz)
    gen_date = now_local.strftime("%Y-%m-%d")
    content_filter = normalize_content_filter(
        subscriber_profile.get("content_filter") or config.get("content_filter", "high_medium")
    )
    include_low_fallback = bool(
        subscriber_profile.get("include_low_fallback")
        if subscriber_profile
        else config.get("include_low_fallback", False)
    )
    baseline_on_first_send = bool(config.get("baseline_on_first_send", True))
    last_sent_at = subscriber_profile.get("last_sent_at") if subscriber_profile else None
    allow_live_send = bool(config.get("allow_live_send", False))
    subscriber_key = config.get("subscriber_key") or ""
    send_enabled_ok = True
    if subscriber_key:
        send_enabled_ok = bool(subscriber_profile.get("send_enabled"))

    missing = preflight_missing_vars(config, args.dry_run)
    if missing:
        print(f"CONFIG_ERROR missing variables: {', '.join(missing)}", file=sys.stderr)
        raise SystemExit(1)

    recipients = collect_recipients(config, subscriber_profile, args.recipient_override)
    intended_recipients = list(recipients)

    send_time_local = (config.get("send_time_local") or "").strip()
    window_minutes = _coerce_send_window_minutes(config.get("send_window_minutes"))
    window_ok, window_reason, window_start, window_end = _within_send_window(
        now_local, send_time_local, window_minutes
    )
    window_start_text = window_start.isoformat() if window_start else "n/a"
    window_end_text = window_end.isoformat() if window_end else "n/a"
    send_time_text = send_time_local or "n/a"
    print(
        "WINDOW_CHECK "
        f"now_local={now_local.isoformat()} "
        f"send_time_local={send_time_text} "
        f"window_start={window_start_text} "
        f"window_end={window_end_text} "
        f"window_ok={'YES' if window_ok else 'NO'}"
    )

    live_allowed = bool(args.send_live and allow_live_send and send_enabled_ok and (args.dry_run or window_ok))
    safe_mode_reason = None
    if not live_allowed:
        if not args.send_live:
            safe_mode_reason = "missing --send-live"
        elif not allow_live_send:
            safe_mode_reason = "allow_live_send=false"
        elif not send_enabled_ok:
            safe_mode_reason = "send_enabled=0"
        elif not (args.dry_run or window_ok):
            safe_mode_reason = window_reason or "outside send window"
        else:
            safe_mode_reason = "unknown"

    run_log_path = (os.getenv("RUN_LOG_PATH") or "").strip() or "unknown"
    if live_allowed:
        print(f"SEND_START mode=LIVE intended_recipient_count={len(intended_recipients)}")
    else:
        print(
            f"SEND_START mode=SAFE intended_recipient_count={len(intended_recipients)} "
            f"gate={safe_mode_reason} run_log={run_log_path}"
        )
        if not args.dry_run:
            subject = f"[SAFE_MODE] {customer_id} {args.mode}"
            body = (
                f"SAFE_MODE triggered.\n"
                f"Gate: {safe_mode_reason}\n"
                f"Intended recipient count: {len(intended_recipients)}\n"
                f"Run log: {run_log_path}\n"
            )
            send_safe_mode_alert(subject, body, "cchevali@gmail.com")
    if not live_allowed and not args.dry_run:
        admin_recipient = resolve_admin_recipient(config)
        if not admin_recipient:
            raise RuntimeError("SAFE_MODE could not resolve admin recipient")
        if recipients != [admin_recipient]:
            print(
                f"[SAFE_MODE] forced admin recipient: {admin_recipient} | intended: {', '.join(intended_recipients)}"
            )
        recipients = [admin_recipient]

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
    ensure_send_log_table(conn)
    tz = resolve_timezone(config, territory_code)
    snapshot_mode = args.mode == "daily" and baseline_on_first_send and not last_sent_at
    report_label = None
    summary_label = None
    snapshot_days = int(config["opened_window_days"])
    window_start = None
    new_only_cutoff = None
    include_changed = False
    use_opened_window = False
    skip_first_seen_filter = args.mode == "baseline"

    if snapshot_mode:
        report_label = f"Starter Snapshot (last {snapshot_days} days)"
        use_opened_window = True
        skip_first_seen_filter = True
        window_start = None
        new_only_cutoff = None
    elif args.mode == "daily" and last_sent_at:
        last_sent_dt = _parse_timestamp(str(last_sent_at))
        if last_sent_dt:
            window_start = last_sent_dt
        include_changed = True
        skip_first_seen_filter = True
        new_only_cutoff = None
    elif args.mode == "daily":
        include_changed = True
    # summary_label set after leads computed

    leads, low_fallback, filter_stats = get_leads_for_period(
        conn=conn,
        states=states,
        since_days=int(config["opened_window_days"]),
        new_only_days=int(config["new_only_days"]),
        skip_first_seen_filter=skip_first_seen_filter,
        territory_code=territory_code,
        content_filter=content_filter,
        include_low_fallback=include_low_fallback,
        window_start=window_start,
        new_only_cutoff=new_only_cutoff,
        include_changed=include_changed,
        use_opened_window=use_opened_window,
    )

    health_summary_text = None
    health_summary_html = None
    health_alerts: list[str] = []
    health_enabled = bool(territory_code and states)
    if health_enabled:
        try:
            min_share = float(config.get("health_min_share", HEALTH_MIN_SHARE_DEFAULT))
            min_total = int(config.get("health_min_total", HEALTH_MIN_TOTAL_DEFAULT))
            health = compute_territory_health(
                conn=conn,
                territory_code=territory_code,
                states=states,
                min_share=min_share,
                min_total=min_total,
            )
            store_territory_health(conn, health)
            health_summary_text, health_summary_html = format_territory_health_summary(health)
            health_alerts = list(health.get("alerts", []))
            if health_alerts:
                logger.warning("Territory health alerts: %s", ", ".join(health_alerts))
            else:
                logger.info(
                    "Territory health OK: 24h share %.1f%%, 14d share %.1f%%",
                    health["window_24"]["share"] * 100,
                    health["window_14"]["share"] * 100,
                )
        except Exception as exc:
            logger.warning("Territory health diagnostics failed: %s", exc)

    conn.close()

    logger.info("Leads after filters: %d", len(leads))
    logger.info(
        "Filter stages: total=%d time_window=%d territory=%d content=%d dedupe=%d final=%d",
        filter_stats.get("total_candidates", 0),
        filter_stats.get("after_time_window", 0),
        filter_stats.get("after_territory", 0),
        filter_stats.get("after_content_filter", 0),
        filter_stats.get("after_dedupe", 0),
        filter_stats.get("final_leads", 0),
    )

    coverage_line = build_coverage_line(
        filter_stats.get("priority_counts", {}),
        filter_stats.get("shown_priority_counts", {}),
    )

    include_health_summary = bool(args.health_summary) and not live_allowed
    if not include_health_summary:
        health_summary_text = None
        health_summary_html = None

    if snapshot_mode:
        summary_label = f"Starter snapshot: {len(leads)} signals (last {snapshot_days} days)"
    elif args.mode == "daily":
        summary_label = f"Newly observed today: {len(leads)} signals"
    else:
        summary_label = f"{len(leads)} signals"

    hi_count = sum(1 for lead in leads if int(lead.get("lead_score") or 0) >= 10)
    states_label = "/".join(states)
    territory_label = territory_display_name(territory_code)
    location_label = territory_label or states_label

    if snapshot_mode:
        subject = f"{location_label} OSHA Signals - {gen_date} (Starter snapshot, {len(leads)} signals)"
    elif args.mode == "daily":
        subject = f"{location_label} OSHA Signals - {gen_date} ({len(leads)} new)"
    else:
        subject = f"{location_label} OSHA Signals - {gen_date} ({len(leads)} signals)"

    digest_hash = compute_digest_hash(
        leads=leads,
        low_fallback=low_fallback,
        mode=args.mode,
        territory_code=territory_code,
        content_filter=content_filter,
        include_low_fallback=include_low_fallback,
    )
    territory_date = gen_date
    duplicate_skip = False
    duplicate_render_skip = False

    Path(args.output_dir).mkdir(parents=True, exist_ok=True)
    email_log_path = os.path.join(args.output_dir, "email_log.csv")
    suppression_log_path = os.path.join(args.output_dir, "suppression_log.csv")

    if live_allowed and not args.dry_run:
        try:
            conn = sqlite3.connect(args.db)
            ensure_send_log_table(conn)
            key = subscriber_key or customer_id
            if has_duplicate_send(
                conn,
                key,
                args.mode,
                territory_code or "",
                territory_date,
                digest_hash,
            ):
                duplicate_skip = True
        finally:
            try:
                conn.close()
            except Exception:
                pass

    # Dry-run duplicate guard (does not affect live send idempotency).
    if args.dry_run:
        try:
            conn = sqlite3.connect(args.db)
            ensure_render_log_table(conn)
            key = subscriber_key or customer_id
            if key and has_duplicate_render(
                conn,
                key,
                args.mode,
                territory_code or "",
                territory_date,
                digest_hash,
            ):
                duplicate_render_skip = True
        finally:
            try:
                conn.close()
            except Exception:
                pass

    if duplicate_skip:
        print(
            f"[SKIP_DUPLICATE] Already sent identical digest for {territory_display_name(territory_code) or territory_code or 'territory'} "
            f"on {territory_date} (hash={digest_hash[:10]}...)"
        )
        for recipient in recipients:
            log_email_attempt(
                email_log_path,
                {
                    "timestamp": timestamp,
                    "customer_id": customer_id,
                    "mode": args.mode,
                    "recipient": recipient,
                    "subject": subject,
                    "status": "skipped_duplicate",
                    "territory_code": territory_code or "",
                    "content_filter": content_filter,
                },
            )
        raise SystemExit(0)

    if duplicate_render_skip:
        print(
            f"[SKIP_DUPLICATE_DRYRUN] Already rendered identical digest for {territory_display_name(territory_code) or territory_code or 'territory'} "
            f"on {territory_date} (hash={digest_hash[:10]}...)"
        )
        for recipient in recipients:
            log_email_attempt(
                email_log_path,
                {
                    "timestamp": timestamp,
                    "customer_id": customer_id,
                    "mode": args.mode,
                    "recipient": recipient,
                    "subject": subject,
                    "status": "skipped_duplicate_dry_run",
                    "territory_code": territory_code or "",
                    "content_filter": content_filter,
                },
            )
        raise SystemExit(0)

    if args.dry_run:
        tier = _tier_counts(leads)
        sample_pool = leads if leads else low_fallback
        sample_rows = sample_pool[:2]
        print(f"DRYRUN_RECIPIENTS intended={', '.join(intended_recipients)}")
        print(f"DRYRUN_TIER_COUNTS high={tier['high']} medium={tier['medium']} low={tier['low']}")
        if sample_rows:
            print("DRYRUN_SAMPLE_LEADS:")
            for row in sample_rows:
                print(f"  {_format_lead_row(row)}")
        else:
            print("DRYRUN_SAMPLE_LEADS: none")

    pilot_mode = bool(config.get("pilot_mode", PILOT_MODE_DEFAULT)) and not args.disable_pilot_guard
    whitelist = [email.lower() for email in config.get("pilot_whitelist", PILOT_WHITELIST_DEFAULT)]
    failed_sends = 0
    sent_or_dry_run = 0
    sent_success = 0
    suppressed_count = 0
    pilot_skipped_count = 0
    suppressed_emails: list[str] = []

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
            suppressed_emails.append(recipient)
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

        list_unsub, list_unsub_post, one_click_url = build_unsubscribe_payload(
            recipient=recipient,
            campaign_id=customer_id,
            reply_to_email=branding["reply_to"],
            dry_run=args.dry_run,
        )

        footer_disclaimer = "This report contains public OSHA inspection data for informational purposes only. Not legal advice."
        footer_text = build_footer_text(
            brand_name=branding.get("brand_legal_name") or branding["brand_name"],
            mailing_address=branding["mailing_address"],
            disclaimer=footer_disclaimer,
            reply_to=branding["reply_to"],
            unsub_url=one_click_url or None,
            include_separator=True,
        )
        footer_html = build_footer_html(
            brand_name=branding.get("brand_legal_name") or branding["brand_name"],
            mailing_address=branding["mailing_address"],
            disclaimer=footer_disclaimer,
            reply_to=branding["reply_to"],
            unsub_url=one_click_url or None,
        )

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
            report_label=report_label,
            footer_html=footer_html,
            summary_label=summary_label,
            coverage_line=coverage_line,
            health_summary_html=health_summary_html,
            tz=tz,
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
            report_label=report_label,
            footer_text=footer_text,
            summary_label=summary_label,
            coverage_line=coverage_line,
            health_summary_text=health_summary_text,
            tz=tz,
        )

        success, message_id, error = send_email(
            recipient=recipient,
            subject=subject,
            html_body=html_body,
            text_body=text_body,
            customer_id=customer_id,
            territory_code=territory_code or "",
            branding=branding,
            dry_run=args.dry_run,
            list_unsub=list_unsub,
            list_unsub_post=list_unsub_post,
        )

        status = "sent" if success else "failed"
        if args.dry_run and success:
            status = "dry_run"
        if success:
            sent_or_dry_run += 1
            if status == "sent":
                sent_success += 1
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
    print(f"Territory:                {territory_display_name(territory_code) or '(none)'}")
    print(f"Content filter:           {content_filter_label(content_filter)}")
    print(f"Low fallback enabled:     {'YES' if include_low_fallback else 'NO'}")
    print(f"Low fallback leads:       {len(low_fallback)}")
    print(f"Leads after filters:      {len(leads)}")
    print(f"Recipients requested:     {len(recipients)}")
    print(f"Live enabled:             {'YES' if live_allowed else 'NO'}")
    print(f"Sent/Dry-run:             {sent_or_dry_run}")
    print(f"Suppressed:               {suppressed_count}")
    print(f"Pilot-skipped:            {pilot_skipped_count}")
    print(f"Failed sends:             {failed_sends}")
    print(f"Pilot mode:               {'ON' if pilot_mode else 'OFF'}")
    print(f"Dry run:                  {'YES' if args.dry_run else 'NO'}")
    if args.dry_run:
        print(f"DRYRUN_SUPPRESSED         {', '.join(suppressed_emails) if suppressed_emails else '(none)'}")
    print("")
    print("Filter stats:")
    print(f"  Total candidates:       {filter_stats['total_candidates']}")
    print(f"  After time-window:      {filter_stats['after_time_window']}")
    print(f"  After territory:        {filter_stats['after_territory']}")
    print(f"  After content filter:   {filter_stats['after_content_filter']}")
    print(f"  After dedupe:           {filter_stats['after_dedupe']}")
    print(f"  Final leads:            {filter_stats['final_leads']}")
    print(f"  Excl. time-window:      {filter_stats['excluded_by_time_window']}")
    print(f"  Excl. new-only window:  {filter_stats['excluded_by_new_only']}")
    print(f"  Excl. territory:        {filter_stats['excluded_by_territory']}")
    print(f"  Matched area_office:    {filter_stats['matched_by_office']}")
    print(f"  Matched fallback city:  {filter_stats['matched_by_fallback']}")
    print(f"  Excl. content filter:   {filter_stats['excluded_by_content_filter']}")
    print(f"  Dedupe removed:         {filter_stats['dedupe_removed']}")
    print(f"  Fallback lows used:     {filter_stats['low_fallback_count']}")
    print("=" * 72)

    if args.dry_run:
        try:
            conn = sqlite3.connect(args.db)
            ensure_render_log_table(conn)
            key = subscriber_key or customer_id
            record_render_log(
                conn,
                key,
                args.mode,
                territory_code or "",
                territory_date,
                digest_hash,
                timestamp,
            )
        finally:
            try:
                conn.close()
            except Exception:
                pass

    if not args.dry_run and sent_success > 0:
        update_subscriber_last_sent_at(args.db, config.get("subscriber_key", ""), timestamp)
        if live_allowed:
            try:
                conn = sqlite3.connect(args.db)
                ensure_send_log_table(conn)
                record_send_log(
                    conn,
                    subscriber_key or customer_id,
                    args.mode,
                    territory_code or "",
                    territory_date,
                    digest_hash,
                    timestamp,
                    sent_success,
                )
            finally:
                try:
                    conn.close()
                except Exception:
                    pass

    if failed_sends > 0:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
