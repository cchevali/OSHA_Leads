#!/usr/bin/env python3
"""
Outbound Cold Email Script for OSHA Lead SaaS.

Sends daily cold email campaigns with fresh OSHA leads as samples.
Includes suppression checking, rate limiting, and comprehensive logging.

Usage:
    python outbound_cold_email.py --dry-run    # Preview without sending
    python outbound_cold_email.py              # Send emails
"""

import argparse
import csv
import hashlib
import json
import os
import random
import smtplib
import sys
import time
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formatdate, make_msgid
from pathlib import Path

# Load environment variables
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # dotenv not required if env vars set externally

# =============================================================================
# CONFIGURATION
# =============================================================================
SCRIPT_DIR = Path(__file__).parent.resolve()
CONFIG_PATH = SCRIPT_DIR / "cold_email_config.json"
LEADS_PATH = SCRIPT_DIR / "out" / "latest_leads.csv"
RECIPIENTS_PATH = SCRIPT_DIR / "out" / "recipients.csv"
SUPPRESSION_PATH = SCRIPT_DIR / "out" / "suppression.csv"
LOG_PATH = SCRIPT_DIR / "out" / "cold_email_log.csv"

# Default config (overridden by cold_email_config.json)
DEFAULT_CONFIG = {
    "daily_send_limit": 25,
    "min_delay_seconds": 4,
    "max_delay_seconds": 10,
    "sample_leads_min": 2,
    "sample_leads_max": 5,
    "score_thresholds": [8, 6, 4],
    "recency_days": 7
}

# Placeholder patterns to reject
PLACEHOLDER_PATTERNS = ["123 main street", "suite 100", "your address here", "example"]


def validate_environment() -> tuple:
    """
    Validate required environment variables for compliance.
    Returns (is_valid: bool, errors: list).
    """
    errors = []
    
    # FROM_EMAIL required
    from_email = os.getenv("FROM_EMAIL", "")
    if not from_email:
        errors.append("FROM_EMAIL is required (no default allowed)")
    
    # SMTP_USER required
    smtp_user = os.getenv("SMTP_USER", "")
    if not smtp_user:
        errors.append("SMTP_USER is required")
    
    # FROM_EMAIL must match SMTP_USER OR be in FROM_ALLOWED_ALIASES
    if from_email and smtp_user and from_email.lower() != smtp_user.lower():
        allowed_aliases = os.getenv("FROM_ALLOWED_ALIASES", "")
        alias_list = [a.strip().lower() for a in allowed_aliases.split(",") if a.strip()]
        
        if from_email.lower() in alias_list:
            pass  # Valid: FROM_EMAIL is a configured alias
        elif os.getenv("ALLOW_FROM_MISMATCH", "false").lower() == "true":
            pass  # Explicit override (not recommended)
        else:
            errors.append(
                f"FROM_EMAIL ({from_email}) must equal SMTP_USER ({smtp_user}) "
                "unless FROM_ALLOWED_ALIASES includes FROM_EMAIL (Zoho alias mode)."
            )
    
    # MAILING_ADDRESS required and no placeholders
    mailing_address = os.getenv("MAILING_ADDRESS", "")
    if not mailing_address:
        errors.append("MAILING_ADDRESS is required (CAN-SPAM compliance)")
    else:
        addr_lower = mailing_address.lower()
        for placeholder in PLACEHOLDER_PATTERNS:
            if placeholder in addr_lower:
                errors.append(
                    f"MAILING_ADDRESS contains placeholder text ('{placeholder}'). "
                    "Use a real physical mailing address."
                )
                break
    
    # REPLY_TO_EMAIL required (opt-out replies go here)
    reply_to = os.getenv("REPLY_TO_EMAIL", "")
    if not reply_to:
        errors.append("REPLY_TO_EMAIL is required (opt-out replies go here)")
    
    # REPLY_TO_EMAIL must be same domain as FROM_EMAIL
    if from_email and reply_to:
        from_domain = from_email.split("@")[-1].lower() if "@" in from_email else ""
        reply_domain = reply_to.split("@")[-1].lower() if "@" in reply_to else ""
        if from_domain and reply_domain and from_domain != reply_domain:
            if os.getenv("ALLOW_REPLYTO_MISMATCH", "false").lower() != "true":
                errors.append(
                    f"REPLY_TO_EMAIL domain ({reply_domain}) must match FROM_EMAIL domain ({from_domain}). "
                    "Set ALLOW_REPLYTO_MISMATCH=true to override."
                )
    
    # MAIL_FOOTER_ADDRESS required for cold outreach footer (physical address)
    footer_address = os.getenv("MAIL_FOOTER_ADDRESS", "")
    if not footer_address:
        errors.append(
            "MAIL_FOOTER_ADDRESS is required for cold outreach. "
            "Use a real physical mailing address."
        )
    
    return len(errors) == 0, errors


def validate_freshness() -> tuple:
    """
    Validate data freshness from out/latest_run.json.
    Returns (is_fresh: bool, report: dict, errors: list).
    """
    from datetime import timezone
    
    run_json_path = SCRIPT_DIR / "out" / "latest_run.json"
    max_pipeline_hours = float(os.getenv("MAX_PIPELINE_AGE_HOURS", "18"))
    max_signal_hours = float(os.getenv("MAX_SIGNAL_AGE_HOURS", "36"))
    
    report = {
        "generated_at_age_hours": None,
        "max_first_seen_age_hours": None,
        "csv_mtime_age_hours": None,
        "has_run_json": False
    }
    errors = []
    now = datetime.now(timezone.utc)
    
    # Check if run metadata exists
    if not run_json_path.exists():
        # Fallback: check CSV mtime
        csv_path = SCRIPT_DIR / "out" / "latest_leads.csv"
        if csv_path.exists():
            mtime = datetime.fromtimestamp(csv_path.stat().st_mtime, tz=timezone.utc)
            age_hours = (now - mtime).total_seconds() / 3600
            report["csv_mtime_age_hours"] = round(age_hours, 1)
            
            if age_hours > max_pipeline_hours:
                errors.append(
                    f"CSV file age ({age_hours:.1f}h) exceeds MAX_PIPELINE_AGE_HOURS ({max_pipeline_hours}h). "
                    "Run ingestion or python write_latest_run.py to update."
                )
        else:
            errors.append("No leads data found (out/latest_leads.csv missing)")
        
        return len(errors) == 0, report, errors
    
    # Parse run metadata
    report["has_run_json"] = True
    try:
        with open(run_json_path, "r", encoding="utf-8") as f:
            run_meta = json.load(f)
    except Exception as e:
        errors.append(f"Failed to parse latest_run.json: {e}")
        return False, report, errors
    
    # Check generated_at age
    generated_at_str = run_meta.get("generated_at", "")
    if generated_at_str:
        try:
            generated_at = datetime.fromisoformat(generated_at_str.replace("Z", "+00:00"))
            if generated_at.tzinfo is None:
                generated_at = generated_at.replace(tzinfo=timezone.utc)
            age_hours = (now - generated_at).total_seconds() / 3600
            report["generated_at_age_hours"] = round(age_hours, 1)
            
            if age_hours > max_pipeline_hours:
                errors.append(
                    f"Pipeline age ({age_hours:.1f}h) exceeds MAX_PIPELINE_AGE_HOURS ({max_pipeline_hours}h)"
                )
        except Exception:
            errors.append("Invalid generated_at timestamp in latest_run.json")
    
    # Check max_first_seen_at age
    max_first_seen_str = run_meta.get("max_first_seen_at", "")
    if max_first_seen_str:
        try:
            max_first_seen = datetime.fromisoformat(max_first_seen_str.replace("Z", "+00:00"))
            if max_first_seen.tzinfo is None:
                max_first_seen = max_first_seen.replace(tzinfo=timezone.utc)
            age_hours = (now - max_first_seen).total_seconds() / 3600
            report["max_first_seen_age_hours"] = round(age_hours, 1)
            
            if age_hours > max_signal_hours:
                errors.append(
                    f"Signal age ({age_hours:.1f}h) exceeds MAX_SIGNAL_AGE_HOURS ({max_signal_hours}h)"
                )
        except Exception:
            pass  # Optional field
    else:
        # Fallback to CSV mtime if first_seen_at not available
        csv_path = SCRIPT_DIR / "out" / "latest_leads.csv"
        if csv_path.exists():
            mtime = datetime.fromtimestamp(csv_path.stat().st_mtime, tz=timezone.utc)
            age_hours = (now - mtime).total_seconds() / 3600
            report["csv_mtime_age_hours"] = round(age_hours, 1)
    
    return len(errors) == 0, report, errors


def send_stale_data_alert(errors: list, report: dict):
    """Send email notification when data is stale."""
    notify_email = os.getenv("NOTIFY_EMAIL", "")
    if not notify_email:
        return
    
    smtp_host = os.getenv("SMTP_HOST", "smtppro.zoho.com")
    smtp_port = int(os.getenv("SMTP_PORT", "465"))
    smtp_user = os.getenv("SMTP_USER", "")
    smtp_pass = os.getenv("SMTP_PASS", "")
    from_email = os.getenv("FROM_EMAIL", smtp_user)
    
    if not smtp_user or not smtp_pass:
        return
    
    subject = "[OSHA Alerts] Outbound halted: stale data"
    body = f"""Outbound cold email was blocked due to stale data.

Errors:
{chr(10).join('- ' + e for e in errors)}

Freshness Report:
- generated_at age: {report.get('generated_at_age_hours', 'N/A')} hours
- max_first_seen age: {report.get('max_first_seen_age_hours', 'N/A')} hours  
- CSV mtime age: {report.get('csv_mtime_age_hours', 'N/A')} hours
- has latest_run.json: {report.get('has_run_json', False)}

Action Required:
Run the ingestion pipeline, then: python write_latest_run.py

Timestamp: {datetime.now().isoformat()}
"""
    
    try:
        msg = MIMEMultipart()
        msg["Subject"] = subject
        msg["From"] = from_email
        msg["To"] = notify_email
        msg.attach(MIMEText(body, "plain"))
        
        if smtp_port == 465:
            with smtplib.SMTP_SSL(smtp_host, smtp_port) as server:
                server.login(smtp_user, smtp_pass)
                server.send_message(msg)
        else:
            with smtplib.SMTP(smtp_host, smtp_port) as server:
                server.starttls()
                server.login(smtp_user, smtp_pass)
                server.send_message(msg)
        
        print(f"[INFO] Sent stale data alert to {notify_email}")
    except Exception as e:
        print(f"[WARN] Failed to send stale data alert: {e}")


def load_config() -> dict:
    """Load cold email configuration."""
    config = DEFAULT_CONFIG.copy()
    if CONFIG_PATH.exists():
        with open(CONFIG_PATH, "r") as f:
            config.update(json.load(f))
    return config


def get_campaign_id() -> str:
    """Generate campaign ID for today."""
    return datetime.now().strftime("%Y-%m-%d")


def compute_unsub_token(email: str, campaign_id: str = "", salt: str = "osha_cold_2026") -> str:
    """Generate unique unsubscribe token per recipient+campaign for tracking."""
    if not campaign_id:
        campaign_id = get_campaign_id()
    data = f"{email}:{campaign_id}:{salt}"
    return hashlib.sha256(data.encode()).hexdigest()[:16]


# =============================================================================
# SUPPRESSION MANAGEMENT
# =============================================================================
def load_suppression_list() -> set:
    """Load suppression list from CSV. Returns set of lowercase emails."""
    suppressed = set()
    if SUPPRESSION_PATH.exists():
        with open(SUPPRESSION_PATH, "r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                email = row.get("email", "").strip().lower()
                if email:
                    suppressed.add(email)
    return suppressed


def is_suppressed(email: str, suppression_list: set) -> bool:
    """Check if email is suppressed."""
    return email.strip().lower() in suppression_list


# =============================================================================
# RECIPIENT MANAGEMENT
# =============================================================================
def load_recipients() -> list:
    """Load recipients from CSV."""
    recipients = []
    if not RECIPIENTS_PATH.exists():
        print(f"[ERROR] Recipients file not found: {RECIPIENTS_PATH}")
        return recipients
    
    with open(RECIPIENTS_PATH, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            email = (row.get("email") or "").strip()
            if email and "@" in email:
                recipients.append({
                    "email": email,
                    "first_name": (row.get("first_name") or "").strip(),
                    "last_name": (row.get("last_name") or "").strip(),
                    "firm_name": (row.get("firm_name") or "").strip(),
                    "segment": (row.get("segment") or "").strip(),
                    "state_pref": (row.get("state_pref") or "").strip().upper()
                })
    return recipients


def get_already_sent_today(campaign_id: str) -> set:
    """Get emails already sent to today. Returns set of lowercase emails."""
    sent = set()
    if LOG_PATH.exists():
        with open(LOG_PATH, "r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get("campaign_id") == campaign_id:
                    email = row.get("recipient_email", "").strip().lower()
                    if email and row.get("status") in ("sent", "dry_run"):
                        sent.add(email)
    return sent


def get_throttle_stats() -> dict:
    """Get current throttle statistics from today's log."""
    from collections import Counter
    
    now = datetime.now()
    today_prefix = now.strftime("%Y-%m-%d")
    one_hour_ago = now - timedelta(hours=1)
    
    stats = {
        "daily_count": 0,
        "hourly_count": 0,
        "domain_counts": Counter()
    }
    
    if not LOG_PATH.exists():
        return stats
    
    with open(LOG_PATH, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            ts = row.get("timestamp", "")
            if not ts.startswith(today_prefix):
                continue
            if row.get("status") not in ("sent",):
                continue
            
            stats["daily_count"] += 1
            
            # Check hourly
            try:
                row_time = datetime.fromisoformat(ts)
                if row_time >= one_hour_ago:
                    stats["hourly_count"] += 1
            except (ValueError, TypeError):
                pass
            
            # Count by domain
            email = row.get("recipient_email", "")
            if "@" in email:
                domain = email.split("@")[-1].lower()
                stats["domain_counts"][domain] += 1
    
    return stats


def check_throttle_limits(recipient_email: str) -> tuple:
    """
    Check if sending is allowed by throttle limits.
    Returns (allowed: bool, reason: str).
    """
    daily_limit = int(os.getenv("DAILY_SEND_LIMIT", "10"))
    hourly_limit = int(os.getenv("PER_HOUR_LIMIT", "3"))
    domain_limit = int(os.getenv("PER_DOMAIN_LIMIT", "2"))
    
    stats = get_throttle_stats()
    
    if stats["daily_count"] >= daily_limit:
        return False, f"daily limit ({daily_limit}) reached"
    
    if stats["hourly_count"] >= hourly_limit:
        return False, f"hourly limit ({hourly_limit}) reached"
    
    if "@" in recipient_email:
        domain = recipient_email.split("@")[-1].lower()
        if stats["domain_counts"].get(domain, 0) >= domain_limit:
            return False, f"domain limit ({domain_limit}) reached for {domain}"
    
    return True, ""


# =============================================================================
# LEAD SELECTION
# =============================================================================
def load_leads(leads_path: Path = None) -> list:
    """Load leads from latest_leads.csv."""
    if leads_path is None:
        leads_path = LEADS_PATH
    leads = []
    if not leads_path.exists():
        print(f"[WARN] Leads file not found: {leads_path}")
        return leads
    
    with open(leads_path, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Parse lead_score
            try:
                score = int(row.get("lead_score", 0))
            except (ValueError, TypeError):
                score = 0
            
            leads.append({
                "lead_id": row.get("lead_id", ""),
                "activity_nr": row.get("activity_nr", ""),
                "establishment_name": row.get("establishment_name", ""),
                "site_city": row.get("site_city", ""),
                "site_state": row.get("site_state", ""),
                "date_opened": row.get("date_opened", ""),
                "inspection_type": row.get("inspection_type", ""),
                "naics_desc": row.get("naics_desc", ""),
                "lead_score": score
            })
    return leads


def select_sample_leads(leads: list, config: dict, recipient_email: str, 
                         campaign_id: str, state_pref: str = None) -> list:
    """
    Select 2-5 sample leads using deterministic rules:
    1. Prefer score >= 8, opened within 7 days
    2. Fallback to >= 6, then >= 4 if insufficient
    3. Use hash-based shuffle to vary selection across recipients
    """
    today = datetime.now().date()
    recency_cutoff = today - timedelta(days=config["recency_days"])
    
    # Parse date and filter by recency
    def is_recent(lead):
        try:
            opened = datetime.strptime(lead["date_opened"], "%Y-%m-%d").date()
            return opened >= recency_cutoff
        except (ValueError, TypeError):
            return False
    
    recent_leads = [l for l in leads if is_recent(l)]
    
    # If state preference, prioritize those leads
    if state_pref:
        state_leads = [l for l in recent_leads if l["site_state"] == state_pref]
        if len(state_leads) >= config["sample_leads_min"]:
            recent_leads = state_leads
    
    # Sort by freshness: first_seen_at desc, then score desc
    def sort_key(lead):
        # Parse first_seen_at for sorting (newest first)
        first_seen = lead.get("first_seen_at", "")
        if first_seen:
            try:
                fs_dt = datetime.fromisoformat(first_seen.replace("Z", "+00:00"))
                fs_ts = fs_dt.timestamp()
            except (ValueError, TypeError):
                fs_ts = 0
        else:
            # Fallback to date_opened
            try:
                opened_dt = datetime.strptime(lead.get("date_opened", ""), "%Y-%m-%d")
                fs_ts = opened_dt.timestamp()
            except (ValueError, TypeError):
                fs_ts = 0
        
        score = lead.get("lead_score", 0)
        return (-fs_ts, -score)  # Negative for descending
    
    recent_leads.sort(key=sort_key)
    
    # Tiered selection by score (from sorted list)
    selected = []
    for threshold in config["score_thresholds"]:
        candidates = [l for l in recent_leads if l["lead_score"] >= threshold]
        if len(candidates) >= config["sample_leads_min"]:
            # Take top N (already sorted by freshness)
            selected = candidates[:config["sample_leads_max"]]
            break
    
    # Fallback: use any recent leads (already sorted)
    if len(selected) < config["sample_leads_min"] and recent_leads:
        selected = recent_leads[:config["sample_leads_max"]]
    
    return selected


# =============================================================================
# EMAIL GENERATION
# =============================================================================
def generate_email_subject(recipient: dict, sample_leads: list, is_test: bool = False) -> str:
    """Generate polished subject line with em dash."""
    state = recipient.get("state_pref") or (sample_leads[0]["site_state"] if sample_leads else "")
    firm = recipient.get("firm_name", "").strip()
    
    territory = state if state else "OSHA"
    
    if is_test or not firm:
        return f"{territory} OSHA activity signals (sample)"
    return f"{territory} OSHA activity signals — {firm}"


def format_lead_for_text(lead: dict, index: int) -> str:
    """Format a single lead for plain text (numbered list)."""
    company = lead.get("establishment_name", "Unknown Company")
    city = lead.get("site_city", "")
    state = lead.get("site_state", "")
    location = f"{city}, {state}".strip(", ")
    opened = lead.get("date_opened", "")
    insp_type = lead.get("inspection_type", "")
    
    # Build compact line
    parts = [company]
    meta = []
    if location:
        meta.append(location)
    if insp_type:
        meta.append(insp_type)
    if opened:
        meta.append(f"Opened: {opened}")
    
    if meta:
        parts.append(" • ".join(meta))
    
    return f"{index}. {parts[0]}\n   {parts[1] if len(parts) > 1 else ''}"


def format_lead_for_html(lead: dict) -> str:
    """Format a single lead as HTML mini-card."""
    company = lead.get("establishment_name", "Unknown Company")
    city = lead.get("site_city", "")
    state = lead.get("site_state", "")
    location = f"{city}, {state}".strip(", ")
    opened = lead.get("date_opened", "")
    insp_type = lead.get("inspection_type", "")
    
    # Build meta line
    meta_parts = []
    if location:
        meta_parts.append(location)
    if insp_type:
        meta_parts.append(insp_type)
    if opened:
        meta_parts.append(f"Opened: {opened}")
    
    meta_line = " • ".join(meta_parts)
    
    return f'''<div style="margin-bottom: 12px;">
<div style="font-weight: 600; color: #1a1a1a;">{company}</div>
<div style="font-size: 13px; color: #666;">{meta_line}</div>
</div>'''


def generate_email_body(recipient: dict, sample_leads: list, 
                         unsub_token: str, mailing_address: str) -> tuple:
    """
    Generate polished email body (plain text and HTML).
    Returns (text_body, html_body).
    """
    first_name = recipient.get("first_name", "").strip()
    firm = recipient.get("firm_name", "").strip() or "your firm"
    state_pref = recipient.get("state_pref", "").strip()
    
    # Footer address (required for cold outreach - use PO Box/PMB)
    footer_address = os.getenv("MAIL_FOOTER_ADDRESS", mailing_address)
    
    # Greeting
    greeting = f"Hi {first_name}," if first_name else "Hi there,"
    
    # Territory
    territory = state_pref if state_pref else "your region"
    
    # Format sample leads for text (numbered)
    leads_text_lines = [format_lead_for_text(l, i+1) for i, l in enumerate(sample_leads[:5])]
    leads_text = "\n\n".join(leads_text_lines)
    
    # Format sample leads for HTML (mini-cards)
    leads_html = "\n".join([format_lead_for_html(l) for l in sample_leads[:5]])
    
    # Build unsubscribe link text for footer (only if endpoint exists)
    unsub_endpoint = os.getenv("UNSUB_ENDPOINT_BASE", "")
    if unsub_endpoint:
        unsub_link_text = f" or click: {unsub_endpoint}?token={unsub_token}"
        unsub_link_html = f' or <a href="{unsub_endpoint}?token={unsub_token}" style="color: #888;">click here</a>'
    else:
        unsub_link_text = ""
        unsub_link_html = ""
    
    # Build text body (no Ref line - clean for cold outreach)
    text_body = f"""{greeting}

I'm reaching out because {firm} appears active in safety/construction, and we track new OSHA activity signals in {territory}.

Here are a few recent signals:

{leads_text}

Some OSHA matters can be time-sensitive; deadlines vary by case. We include deadlines only when available.

If you'd like, I can send a short daily {territory} digest like this. Reply "yes" and I'll set it up.

Chase

---
Micro Flow Ops
{footer_address}
Opt out: reply "unsubscribe"{unsub_link_text}
"""
    
    # Build HTML body (600px centered, system-ui font stack, dark-mode safe)
    html_body = f'''<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<meta name="color-scheme" content="light dark">
</head>
<body style="margin: 0; padding: 0; background-color: #f5f5f5; font-family: system-ui, -apple-system, 'Segoe UI', Roboto, Arial, sans-serif;">
<div style="max-width: 600px; margin: 0 auto; padding: 20px;">
<div style="background-color: #ffffff; border-radius: 8px; padding: 24px; color: #1a1a1a;">

<p style="font-size: 14px; line-height: 1.6; margin: 0 0 16px 0;">{greeting}</p>

<p style="font-size: 14px; line-height: 1.6; margin: 0 0 20px 0;">
I'm reaching out because {firm} appears active in safety/construction, and we track new OSHA activity signals in {territory}.
</p>

<p style="font-size: 16px; font-weight: 600; margin: 0 0 12px 0; color: #1a1a1a;">Recent signals:</p>

<div style="margin-bottom: 20px;">
{leads_html}
</div>

<p style="font-size: 13px; color: #666; line-height: 1.5; margin: 0 0 20px 0;">
Some OSHA matters can be time-sensitive; deadlines vary by case. We include deadlines only when available.
</p>

<p style="font-size: 14px; line-height: 1.6; margin: 0 0 24px 0;">
If you'd like, I can send a short daily {territory} digest like this. Reply "yes" and I'll set it up.
</p>

<p style="font-size: 14px; margin: 0 0 16px 0; color: #1a1a1a;">Chase</p>

</div>

<div style="padding: 16px 24px; text-align: center;">
<p style="font-size: 12px; color: #666; margin: 0 0 4px 0;">Micro Flow Ops</p>
<p style="font-size: 12px; color: #666; margin: 0 0 12px 0;">{footer_address}</p>
<p style="font-size: 12px; color: #888; margin: 0;">Opt out: reply "unsubscribe"{unsub_link_html}</p>
</div>

</div>
</body>
</html>'''
    
    return text_body, html_body


# =============================================================================
# EMAIL SENDING
# =============================================================================
def send_email(recipient_email: str, subject: str, text_body: str, 
               html_body: str, unsub_token: str = "", campaign_id: str = "",
               sample_ids: list = None, dry_run: bool = False) -> tuple:
    """
    Send email via SMTP with deliverability and tracking headers.
    Returns (success: bool, message_id: str, error: str).
    """
    smtp_host = os.getenv("SMTP_HOST", "smtppro.zoho.com")
    smtp_port = int(os.getenv("SMTP_PORT", "465"))
    smtp_user = os.getenv("SMTP_USER", "")
    smtp_pass = os.getenv("SMTP_PASS", "")
    from_email = os.getenv("FROM_EMAIL", smtp_user)
    reply_to = os.getenv("REPLY_TO_EMAIL", from_email)
    from_display_name = os.getenv("FROM_DISPLAY_NAME", "MicroFlowOps OSHA Alerts")
    
    # Format From header with display name (RFC 5322)
    from_header = f"{from_display_name} <{from_email}>" if from_display_name else from_email
    reply_to_header = f"{from_display_name} <{reply_to}>" if from_display_name else reply_to
    
    if dry_run:
        print(f"\n{'='*60}")
        print(f"[DRY-RUN] Would send to: {recipient_email}")
        print(f"From: {from_header}")
        print(f"Subject: {subject}")
        print(f"{'='*60}")
        # Handle Unicode for Windows console
        preview = text_body[:500] + "..." if len(text_body) > 500 else text_body
        try:
            print(preview)
        except UnicodeEncodeError:
            print(preview.encode('ascii', 'replace').decode('ascii'))
        print(f"{'='*60}\n")
        return True, "dry-run-no-message-id", ""
    
    if not smtp_user or not smtp_pass:
        return False, "", "SMTP credentials not configured (SMTP_USER/SMTP_PASS)"
    
    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = from_header
        msg["To"] = recipient_email
        msg["Reply-To"] = reply_to_header
        
        # Add Date and Message-ID headers for deliverability
        msg["Date"] = formatdate(localtime=True)
        msg["Message-ID"] = make_msgid(domain=from_email.split("@")[-1] if "@" in from_email else "microflowops.com")
        
        # Add List-Unsubscribe headers (RFC 8058 One-Click only if endpoint exists)
        if unsub_token:
            unsub_mailto = f"mailto:{reply_to}?subject=unsubscribe"
            unsub_endpoint = os.getenv("UNSUB_ENDPOINT_BASE", "")
            
            if unsub_endpoint:
                # Endpoint exists: include both mailto and https, plus One-Click
                unsub_https = f"{unsub_endpoint}?token={unsub_token}"
                msg["List-Unsubscribe"] = f"<{unsub_mailto}>, <{unsub_https}>"
                msg["List-Unsubscribe-Post"] = "List-Unsubscribe=One-Click"
            else:
                # No endpoint: mailto only, no One-Click claim
                msg["List-Unsubscribe"] = f"<{unsub_mailto}>"
        
        # Add tracking headers
        if campaign_id:
            msg["X-Campaign-ID"] = campaign_id
        if unsub_token:
            msg["X-Unsub-Token"] = unsub_token
        if sample_ids:
            msg["X-Lead-Samples"] = ",".join(str(s) for s in sample_ids)
        
        # Attach plain text and HTML versions
        msg.attach(MIMEText(text_body, "plain"))
        msg.attach(MIMEText(html_body, "html"))
        
        # Use SSL for port 465, STARTTLS for port 587
        if smtp_port == 465:
            with smtplib.SMTP_SSL(smtp_host, smtp_port) as server:
                server.login(smtp_user, smtp_pass)
                server.send_message(msg)
        else:
            with smtplib.SMTP(smtp_host, smtp_port) as server:
                server.starttls()
                server.login(smtp_user, smtp_pass)
                server.send_message(msg)
        
        message_id = msg["Message-ID"]
        return True, message_id, ""
    
    except Exception as e:
        return False, "", str(e)


# =============================================================================
# LOGGING
# =============================================================================
def log_send(recipient_email: str, subject: str, samples_used: list,
             message_id: str, campaign_id: str, status: str, 
             error: str, unsub_token: str):
    """Log email send attempt to CSV."""
    # Create log file with headers if needed
    write_header = not LOG_PATH.exists()
    
    with open(LOG_PATH, "a", newline="", encoding="utf-8") as f:
        fieldnames = ["timestamp", "recipient_email", "subject", "samples_used",
                      "message_id", "campaign_id", "status", "error", "unsub_token"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        
        if write_header:
            writer.writeheader()
        
        # Format samples as pipe-separated list
        samples_str = "|".join([s.get("activity_nr", "") for s in samples_used])
        
        writer.writerow({
            "timestamp": datetime.now().isoformat(),
            "recipient_email": recipient_email,
            "subject": subject,
            "samples_used": samples_str,
            "message_id": message_id,
            "campaign_id": campaign_id,
            "status": status,
            "error": error,
            "unsub_token": unsub_token
        })


# =============================================================================
# MAIN
# =============================================================================
def main():
    parser = argparse.ArgumentParser(
        description="Send cold email campaign with OSHA leads",
        epilog="Example: python outbound_cold_email.py --dry-run"
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview emails without sending")
    parser.add_argument("--limit", type=int, default=None,
                        help="Override daily send limit")
    parser.add_argument("--leads-file", type=str, default=None,
                        help="Override leads CSV path")
    parser.add_argument("--smtp-test", action="store_true",
                        help="Test SMTP connectivity and exit")
    parser.add_argument("--to-override", type=str, default=None,
                        help="Override recipient (for testing)")
    parser.add_argument("--print-config", action="store_true",
                        help="Print resolved config (FROM_EMAIL, SMTP_USER, aliases) and exit")
    parser.add_argument("--preflight", action="store_true",
                        help="Run all checks (env, smtp, freshness, throttle) without sending")
    parser.add_argument("--render-preview", action="store_true",
                        help="Output preview_email.html and preview_email.txt to ./out/ (no send)")
    args = parser.parse_args()
    
    # Print config mode
    if args.print_config:
        from_email = os.getenv("FROM_EMAIL", "")
        smtp_user = os.getenv("SMTP_USER", "")
        allowed_aliases = os.getenv("FROM_ALLOWED_ALIASES", "")
        outbound_enabled = os.getenv("OUTBOUND_ENABLED", "false")
        from_display_name = os.getenv("FROM_DISPLAY_NAME", "MicroFlowOps OSHA Alerts")
        print("=" * 50)
        print("OUTBOUND CONFIG")
        print("=" * 50)
        print(f"  FROM_EMAIL:           {from_email}")
        print(f"  FROM_DISPLAY_NAME:    {from_display_name}")
        print(f"  SMTP_USER:            {smtp_user}")
        print(f"  FROM_ALLOWED_ALIASES: {allowed_aliases or '(none)'}")
        print(f"  OUTBOUND_ENABLED:     {outbound_enabled}")
        print(f"  SMTP_PASS:            {'***' if os.getenv('SMTP_PASS') else '(not set)'}")
        print("=" * 50)
        
        # Validate
        is_valid, errors = validate_environment()
        if is_valid:
            print("[OK] Configuration valid")
        else:
            print("[ERROR] Configuration errors:")
            for err in errors:
                print(f"  - {err}")
        sys.exit(0 if is_valid else 1)
    
    # SMTP connectivity test mode
    if args.smtp_test:
        smtp_host = os.getenv("SMTP_HOST", "smtppro.zoho.com")
        smtp_port = int(os.getenv("SMTP_PORT", "587"))
        smtp_user = os.getenv("SMTP_USER", "")
        smtp_pass = os.getenv("SMTP_PASS", "")
        
        if not smtp_user or not smtp_pass:
            print("[ERROR] SMTP credentials not configured (SMTP_USER/SMTP_PASS)")
            sys.exit(1)
        
        print(f"[INFO] Testing SMTP: {smtp_host}:{smtp_port}")
        try:
            if smtp_port == 465:
                with smtplib.SMTP_SSL(smtp_host, smtp_port) as server:
                    server.login(smtp_user, smtp_pass)
            else:
                with smtplib.SMTP(smtp_host, smtp_port) as server:
                    server.starttls()
                    server.login(smtp_user, smtp_pass)
            print("[OK] SMTP OK")
            sys.exit(0)
        except Exception as e:
            print(f"[ERROR] SMTP connection failed: {e}")
            sys.exit(1)
    
    # Render preview mode: output HTML and text files
    if args.render_preview:
        leads_path = Path(args.leads_file) if args.leads_file else LEADS_PATH
        leads = load_leads(leads_path)
        
        if not leads:
            print("[ERROR] No leads found for preview")
            sys.exit(1)
        
        # Use sample recipient
        recipient = {
            "email": "preview@example.com",
            "first_name": "Preview",
            "firm_name": "Sample Safety Consulting",
            "state_pref": leads[0].get("site_state", "TX") if leads else "TX"
        }
        
        mailing_address = os.getenv("MAILING_ADDRESS", "123 Main St, City, ST 12345")
        sample_leads = leads[:5]
        unsub_token = compute_unsub_token("preview@example.com", "preview")
        
        subject = generate_email_subject(recipient, sample_leads, is_test=True)
        text_body, html_body = generate_email_body(recipient, sample_leads, unsub_token, mailing_address)
        
        # Write preview files
        out_dir = SCRIPT_DIR / "out"
        out_dir.mkdir(exist_ok=True)
        
        html_path = out_dir / "preview_email.html"
        text_path = out_dir / "preview_email.txt"
        
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(html_body)
        with open(text_path, "w", encoding="utf-8") as f:
            f.write(f"Subject: {subject}\n\n{text_body}")
        
        print(f"[OK] Preview files written:")
        print(f"  HTML: {html_path}")
        print(f"  Text: {text_path}")
        print(f"  Subject: {subject}")
        sys.exit(0)
    
    # Preflight mode: validate all gates without sending
    if args.preflight:
        print("=" * 50)
        print("PREFLIGHT CHECK")
        print("=" * 50)
        all_pass = True
        
        # Env validation
        is_valid, errors = validate_environment()
        if is_valid:
            print("[OK] Environment validation passed")
        else:
            print("[FAIL] Environment validation:")
            for e in errors:
                print(f"  - {e}")
            all_pass = False
        
        # SMTP test
        smtp_host = os.getenv("SMTP_HOST", "smtppro.zoho.com")
        smtp_port = int(os.getenv("SMTP_PORT", "465"))
        smtp_user = os.getenv("SMTP_USER", "")
        smtp_pass = os.getenv("SMTP_PASS", "")
        try:
            if smtp_port == 465:
                with smtplib.SMTP_SSL(smtp_host, smtp_port) as server:
                    server.login(smtp_user, smtp_pass)
            else:
                with smtplib.SMTP(smtp_host, smtp_port) as server:
                    server.starttls()
                    server.login(smtp_user, smtp_pass)
            print("[OK] SMTP connectivity passed")
        except Exception as e:
            print(f"[FAIL] SMTP connectivity: {e}")
            all_pass = False
        
        # Freshness check
        is_fresh, report, freshness_errors = validate_freshness()
        if is_fresh:
            print(f"[OK] Freshness passed (pipeline age: {report.get('generated_at_age_hours', 'N/A')}h)")
        else:
            print("[FAIL] Freshness check:")
            for e in freshness_errors:
                print(f"  - {e}")
            all_pass = False
        
        # Throttle check
        stats = get_throttle_stats()
        daily_limit = int(os.getenv("DAILY_SEND_LIMIT", "10"))
        hourly_limit = int(os.getenv("PER_HOUR_LIMIT", "3"))
        remaining_daily = daily_limit - stats["daily_count"]
        remaining_hourly = hourly_limit - stats["hourly_count"]
        
        if remaining_daily > 0 and remaining_hourly > 0:
            print(f"[OK] Throttle OK (daily: {stats['daily_count']}/{daily_limit}, hourly: {stats['hourly_count']}/{hourly_limit})")
        else:
            print(f"[FAIL] Throttle limits reached (daily: {stats['daily_count']}/{daily_limit}, hourly: {stats['hourly_count']}/{hourly_limit})")
            all_pass = False
        
        # Kill switch
        outbound_enabled = os.getenv("OUTBOUND_ENABLED", "false").lower() == "true"
        if outbound_enabled:
            print("[OK] OUTBOUND_ENABLED=true")
        else:
            print("[WARN] OUTBOUND_ENABLED=false (no live sends)")
        
        # Suppression file presence
        if SUPPRESSION_PATH.exists():
            print("[OK] Suppression file present")
        else:
            print(f"[FAIL] Suppression file missing: {SUPPRESSION_PATH}")
            all_pass = False
        
        print("=" * 50)
        print(f"PREFLIGHT {'PASSED' if all_pass else 'FAILED'}")
        print("=" * 50)
        sys.exit(0 if all_pass else 1)
    
    print(f"[INFO] OSHA Cold Email Script")
    print(f"[INFO] Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"[INFO] Mode: {'DRY-RUN' if args.dry_run else 'LIVE'}")
    
    # Validate environment (compliance checks)
    is_valid, errors = validate_environment()
    if not is_valid:
        print("[ERROR] Environment validation failed:")
        for err in errors:
            print(f"  - {err}")
        sys.exit(1)
    
    # Kill switch - prevents accidental sends during warmup
    outbound_enabled = os.getenv("OUTBOUND_ENABLED", "false").lower() == "true"
    if not outbound_enabled and not args.dry_run:
        print("[BLOCKED] OUTBOUND_ENABLED is not set to 'true'.")
        print("  Set OUTBOUND_ENABLED=true in .env to enable live sending.")
        print("  Use --dry-run to preview without this check.")
        sys.exit(0)
    
    # Require suppression file for live sends (opt-out enforcement)
    if outbound_enabled and not args.dry_run and not SUPPRESSION_PATH.exists():
        print(f"[ERROR] Suppression file not found: {SUPPRESSION_PATH}")
        print("  Create out/suppression.csv before live sending.")
        sys.exit(1)
    
    # Validate data freshness
    is_fresh, freshness_report, freshness_errors = validate_freshness()
    
    # Print freshness report (always in dry-run, on error otherwise)
    if args.dry_run or not is_fresh:
        print(f"\n{'='*60}")
        print("FRESHNESS REPORT")
        print(f"{'='*60}")
        print(f"  generated_at age:    {freshness_report.get('generated_at_age_hours', 'N/A')} hours")
        print(f"  max_first_seen age:  {freshness_report.get('max_first_seen_age_hours', 'N/A')} hours")
        print(f"  CSV mtime age:       {freshness_report.get('csv_mtime_age_hours', 'N/A')} hours")
        print(f"  has latest_run.json: {freshness_report.get('has_run_json', False)}")
        print(f"{'='*60}\n")
    
    if not is_fresh:
        print("[ERROR] Data freshness check failed:")
        for err in freshness_errors:
            print(f"  - {err}")
        
        # Send notification (only for live runs, not dry-run)
        if not args.dry_run:
            send_stale_data_alert(freshness_errors, freshness_report)
        
        sys.exit(1)
    
    # Load configuration
    config = load_config()
    campaign_id = get_campaign_id()
    mailing_address = os.getenv("MAILING_ADDRESS")  # Required, validated above
    
    print(f"[INFO] Campaign ID: {campaign_id}")
    print(f"[INFO] Daily limit: {args.limit or config['daily_send_limit']}")
    
    # Load data
    suppression_list = load_suppression_list()
    print(f"[INFO] Suppression list: {len(suppression_list)} emails")
    
    recipients = load_recipients()
    
    # Override recipient for testing
    if args.to_override:
        recipients = [{
            "email": args.to_override,
            "first_name": "Test",
            "last_name": "User",
            "firm_name": "Test Firm",
            "segment": "",
            "state_pref": "TX"
        }]
        print(f"[INFO] Overriding recipient: {args.to_override}")
    
    print(f"[INFO] Recipients loaded: {len(recipients)}")
    
    leads_path = Path(args.leads_file) if args.leads_file else LEADS_PATH
    leads = load_leads(leads_path)
    print(f"[INFO] Leads loaded: {len(leads)}")
    
    if not recipients:
        print("[ERROR] No recipients found. Add recipients to out/recipients.csv")
        sys.exit(1)
    
    if not leads:
        print("[ERROR] No leads found. Ensure out/latest_leads.csv exists")
        sys.exit(1)
    
    # Filter out already sent and suppressed
    already_sent = get_already_sent_today(campaign_id)
    print(f"[INFO] Already sent today: {len(already_sent)}")
    
    eligible = []
    for r in recipients:
        email = r["email"].lower()
        if email in already_sent:
            print(f"  [SKIP] Already sent: {r['email']}")
            continue
        if is_suppressed(email, suppression_list):
            print(f"  [SKIP] Suppressed: {r['email']}")
            continue
        eligible.append(r)
    
    print(f"[INFO] Eligible recipients: {len(eligible)}")
    
    # Apply daily limit
    daily_limit = args.limit or config["daily_send_limit"]
    to_send = eligible[:daily_limit]
    
    if not to_send:
        print("[INFO] No emails to send today.")
        sys.exit(0)
    
    print(f"[INFO] Will send to {len(to_send)} recipients")
    
    # Send emails
    sent_count = 0
    failed_count = 0
    
    for i, recipient in enumerate(to_send):
        email = recipient["email"]
        
        # Check throttle limits before each send
        throttle_ok, throttle_reason = check_throttle_limits(email)
        if not throttle_ok:
            print(f"  [THROTTLE] {throttle_reason} - stopping sends")
            break
        
        # Select sample leads
        samples = select_sample_leads(
            leads, config, email, campaign_id, 
            recipient.get("state_pref")
        )
        
        if not samples:
            print(f"  [SKIP] No suitable leads for {email}")
            log_send(email, "", [], "", campaign_id, "skipped", 
                     "no_suitable_leads", "")
            continue
        
        # Generate email (unique token per recipient+campaign)
        unsub_token = compute_unsub_token(email, campaign_id)
        subject = generate_email_subject(recipient, samples)
        text_body, html_body = generate_email_body(
            recipient, samples, unsub_token, mailing_address
        )
        
        # Extract sample IDs for tracking
        sample_ids = [s.get("activity_nr", s.get("lead_id", "")) for s in samples]
        
        # Send with tracking headers
        success, message_id, error = send_email(
            email, subject, text_body, html_body, unsub_token, 
            campaign_id, sample_ids, args.dry_run
        )
        
        # Log
        status = "dry_run" if args.dry_run else ("sent" if success else "failed")
        log_send(email, subject, samples, message_id, campaign_id, 
                 status, error, unsub_token)
        
        if success:
            sent_count += 1
            print(f"  [{'DRY-RUN' if args.dry_run else 'SENT'}] {email} ({len(samples)} samples)")
        else:
            failed_count += 1
            print(f"  [FAILED] {email}: {error}")
        
        # Rate limiting (skip on last email)
        if i < len(to_send) - 1 and not args.dry_run:
            delay = random.uniform(
                config["min_delay_seconds"], 
                config["max_delay_seconds"]
            )
            print(f"  [WAIT] {delay:.1f}s")
            time.sleep(delay)
    
    # Summary
    print(f"\n{'='*60}")
    print(f"[SUMMARY] Campaign: {campaign_id}")
    print(f"  Sent: {sent_count}")
    print(f"  Failed: {failed_count}")
    print(f"  Log: {LOG_PATH}")
    print(f"{'='*60}")
    
    sys.exit(0 if failed_count == 0 else 1)


if __name__ == "__main__":
    main()
