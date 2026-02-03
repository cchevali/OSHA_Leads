#!/usr/bin/env python3
"""
Send OSHA digest email to customer recipients.

Generates digest, checks suppression list, and sends via SMTP.
Includes pilot mode guard to limit recipients during testing.
"""

import argparse
import csv
import json
import os
import smtplib
import sqlite3
import time
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

# =============================================================================
# PILOT MODE CONFIGURATION
# Set PILOT_MODE = False to allow sending to any recipient
# =============================================================================
PILOT_MODE = True
PILOT_WHITELIST = [
    "cchevali@gmail.com",
]

# Email sender configuration
DEFAULT_FROM = "MicroFlowOps OSHA Alerts <alerts@microflowops.com>"
DEFAULT_REPLY_TO = "support@microflowops.com"
DEFAULT_SENDER = "alerts@microflowops.com"
UNSUBSCRIBE_EMAIL = "support@microflowops.com"
UNSUBSCRIBE_URL = f"mailto:{UNSUBSCRIBE_EMAIL}?subject=Unsubscribe"

# Default branding (overridden by customer config)
DEFAULT_BRAND_NAME = "MicroFlowOps"


def setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def load_customer_config(config_path: str) -> dict:
    """Load customer configuration from JSON file."""
    with open(config_path, 'r') as f:
        return json.load(f)


def check_suppression(db_path: str, email: str) -> bool:
    """Check if email is in suppression list. Returns True if suppressed."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Check exact email match
    cursor.execute(
        "SELECT 1 FROM suppression_list WHERE email_or_domain = ? LIMIT 1",
        (email.lower(),)
    )
    if cursor.fetchone():
        conn.close()
        return True
    
    # Check domain match
    domain = email.split('@')[-1].lower()
    cursor.execute(
        "SELECT 1 FROM suppression_list WHERE email_or_domain = ? LIMIT 1",
        (domain,)
    )
    if cursor.fetchone():
        conn.close()
        return True
    
    conn.close()
    return False


def log_suppression(log_path: str, timestamp: str, customer_id: str, 
                    recipient: str, reason: str) -> None:
    """Log suppression event."""
    file_exists = os.path.exists(log_path)
    with open(log_path, 'a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["timestamp", "customer_id", "recipient", "reason"])
        writer.writerow([timestamp, customer_id, recipient, reason])


def log_email_attempt(log_path: str, timestamp: str, customer_id: str, mode: str,
                      recipient: str, subject: str, status: str, 
                      message_id: str = "", error: str = "") -> None:
    """Log email send attempt."""
    file_exists = os.path.exists(log_path)
    with open(log_path, 'a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["timestamp", "customer_id", "mode", "recipient", 
                           "subject", "status", "message_id", "error"])
        writer.writerow([timestamp, customer_id, mode, recipient, subject, 
                        status, message_id, error])


def get_leads_for_period(conn: sqlite3.Connection, states: list, 
                         since_days: int, new_only_days: int = 1,
                         skip_first_seen_filter: bool = False) -> list:
    """Get leads within the specified period and states."""
    today = datetime.now()
    date_opened_cutoff = (today - timedelta(days=since_days)).strftime("%Y-%m-%d")
    first_seen_cutoff = (today - timedelta(days=new_only_days)).strftime("%Y-%m-%d %H:%M:%S")
    
    placeholders = ",".join(["?" for _ in states])
    
    query = f"""
        SELECT 
            lead_id, activity_nr, date_opened, inspection_type, scope, 
            case_status, establishment_name, site_city, site_state, site_zip,
            naics, naics_desc, violations_count, emphasis, lead_score,
            first_seen_at, source_url
        FROM inspections 
        WHERE site_state IN ({placeholders})
          AND parse_invalid = 0
        ORDER BY lead_score DESC, date_opened DESC
    """
    
    cursor = conn.cursor()
    cursor.execute(query, tuple(states))
    
    columns = [desc[0] for desc in cursor.description]
    all_results = [dict(zip(columns, row)) for row in cursor.fetchall()]
    
    filtered = []
    for lead in all_results:
        date_opened = lead.get("date_opened")
        first_seen = lead.get("first_seen_at")
        
        if date_opened and date_opened < date_opened_cutoff:
            continue
        
        if not skip_first_seen_filter:
            if first_seen and first_seen < first_seen_cutoff:
                continue
        
        filtered.append(lead)
    
    return filtered


def generate_lead_table_html(leads: list, max_rows: int) -> str:
    """Generate HTML table for leads."""
    if not leads:
        return "<p><em>No leads match the filter criteria.</em></p>"
    
    html = ['<table border="1" cellpadding="8" cellspacing="0" style="border-collapse: collapse; width: 100%;">']
    html.append('<tr style="background-color: #f0f0f0;">')
    html.append('<th>Company</th><th>City</th><th>NAICS</th><th>Type</th><th>Date</th><th>Score</th><th>Link</th>')
    html.append('</tr>')
    
    for lead in leads[:max_rows]:
        company = (lead.get("establishment_name") or "Unknown")[:40]
        city = lead.get("site_city") or "-"
        state = lead.get("site_state") or "-"
        naics = lead.get("naics") or "-"
        itype = lead.get("inspection_type") or "-"
        date_opened = lead.get("date_opened") or "-"
        score = lead.get("lead_score") or 0
        url = lead.get("source_url") or "#"
        
        html.append('<tr>')
        html.append(f'<td>{company}</td>')
        html.append(f'<td>{city}, {state}</td>')
        html.append(f'<td>{naics}</td>')
        html.append(f'<td>{itype}</td>')
        html.append(f'<td>{date_opened}</td>')
        html.append(f'<td style="text-align: center;"><strong>{score}</strong></td>')
        html.append(f'<td><a href="{url}">View</a></td>')
        html.append('</tr>')
    
    html.append('</table>')
    return '\n'.join(html)


def generate_digest_html(leads: list, config: dict, gen_date: str, mode: str) -> str:
    """Generate HTML digest email body."""
    customer_id = config["customer_id"]
    states = config["states"]
    since_days = config["opened_window_days"]
    new_only_days = config["new_only_days"]
    top_k_overall = config.get("top_k_overall", 25)
    top_k_per_state = config.get("top_k_per_state", 10)
    
    mode_label = "BASELINE" if mode == "baseline" else "DAILY"
    
    # Per-state counts
    state_counts = {}
    for lead in leads:
        st = lead.get("site_state") or "UNK"
        state_counts[st] = state_counts.get(st, 0) + 1
    
    # Count high-scoring leads
    hi_count = sum(1 for l in leads if (l.get("lead_score") or 0) >= 10)
    
    # Build HTML
    html = []
    html.append('<!DOCTYPE html>')
    html.append('<html><head><meta charset="utf-8"></head>')
    html.append('<body style="font-family: Arial, sans-serif; max-width: 800px; margin: 0 auto; padding: 20px; background-color: #f9f9f9;">')
    
    # Main container
    html.append('<div style="background-color: #ffffff; padding: 30px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1);">')
    
    # Header
    html.append(f'<h1 style="color: #1a1a2e; margin-bottom: 10px;">OSHA Lead Digest ({mode_label})</h1>')
    html.append(f'<p style="color: #666; font-size: 14px; margin-top: 0;">{gen_date} • {"/".join(states)}</p>')
    
    # Above-the-fold summary box
    html.append('<div style="background-color: #e8f4f8; padding: 20px; border-radius: 6px; margin: 20px 0;">')
    if mode == "daily":
        html.append(f'<p style="margin: 0; font-size: 18px;"><strong>🔔 {len(leads)} new leads today</strong> • {hi_count} high-priority (score ≥10)</p>')
    else:
        html.append(f'<p style="margin: 0; font-size: 18px;"><strong>📊 {len(leads)} leads in baseline</strong> • {hi_count} high-priority (score ≥10)</p>')
    html.append('<p style="margin: 10px 0 0 0; font-size: 14px; color: #555;">')
    for state in states:
        html.append(f'{state}: {state_counts.get(state, 0)} • ')
    html.append('</p>')
    html.append('</div>')
    
    # Top Leads Quick Preview (above the fold)
    top_preview = leads[:5]
    if top_preview:
        html.append('<h2 style="color: #1a1a2e; border-bottom: 2px solid #0066cc; padding-bottom: 8px;">🏆 Top Leads</h2>')
        html.append('<ul style="list-style: none; padding: 0;">')
        for lead in top_preview:
            company = (lead.get("establishment_name") or "Unknown")[:50]
            city = lead.get("site_city") or "-"
            state = lead.get("site_state") or "-"
            itype = lead.get("inspection_type") or "-"
            date_opened = lead.get("date_opened") or "-"
            score = lead.get("lead_score") or 0
            url = lead.get("source_url") or "#"
            
            score_color = "#d32f2f" if score >= 10 else "#1976d2" if score >= 6 else "#666"
            html.append(f'<li style="margin-bottom: 12px; padding: 12px; background-color: #f5f5f5; border-radius: 4px; border-left: 4px solid {score_color};">')
            html.append(f'<strong>{company}</strong><br>')
            html.append(f'<span style="color: #555;">{city}, {state} • {itype} • {date_opened}</span><br>')
            html.append(f'<span style="font-weight: bold; color: {score_color};">Score: {score}</span> ')
            html.append(f'<a href="{url}" style="color: #0066cc;">View OSHA Record →</a>')
            html.append('</li>')
        html.append('</ul>')
    
    # Full table section
    html.append(f'<h2 style="color: #1a1a2e; margin-top: 30px;">All {len(leads)} Leads</h2>')
    html.append(generate_lead_table_html(leads, top_k_overall))
    
    # Per-state sections
    for state in states:
        state_leads = [l for l in leads if l.get("site_state") == state]
        if state_leads:
            html.append(f'<h2 style="color: #1a1a2e; margin-top: 30px;">Top {min(top_k_per_state, len(state_leads))} — {state}</h2>')
            html.append(generate_lead_table_html(state_leads, top_k_per_state))
    
    html.append('</div>')  # End main container
    
    # Get branding from config
    brand_name = config.get("brand_name", DEFAULT_BRAND_NAME)
    legal_name = config.get("legal_name", "")  # Optional
    mailing_address = config.get("mailing_address", "")  # Optional
    
    # Compliance Footer
    html.append('<div style="margin-top: 30px; padding: 20px; text-align: center; font-size: 12px; color: #888;">')
    
    # Build footer brand line
    if legal_name:
        html.append(f'<p><strong>{legal_name}</strong><br>')
    else:
        html.append(f'<p><strong>{brand_name}</strong><br>')
    
    if mailing_address:
        html.append(f'{mailing_address}<br><br>')
    
    html.append('This report contains public OSHA inspection data for informational purposes only.<br>')
    html.append('Not legal advice. Verify all information before taking action.</p>')
    html.append(f'<p style="margin-top: 15px;">Don\'t want these emails? <a href="{UNSUBSCRIBE_URL}" style="color: #0066cc;">Unsubscribe</a> or reply "opt out".</p>')
    html.append('</div>')
    
    html.append('</body></html>')
    return '\n'.join(html)


def generate_digest_text(leads: list, config: dict, gen_date: str, mode: str) -> str:
    """Generate plain text digest for email with full URLs."""
    customer_id = config["customer_id"]
    states = config["states"]
    mode_label = "BASELINE" if mode == "baseline" else "DAILY"
    hi_count = sum(1 for l in leads if (l.get("lead_score") or 0) >= 10)
    
    # Per-state counts
    state_counts = {}
    for lead in leads:
        st = lead.get("site_state") or "UNK"
        state_counts[st] = state_counts.get(st, 0) + 1
    
    lines = []
    lines.append(f"OSHA Lead Digest ({mode_label}) — {gen_date}")
    lines.append(f"Coverage: {'/'.join(states)}")
    lines.append("=" * 60)
    lines.append("")
    
    if mode == "daily":
        lines.append(f"🔔 {len(leads)} NEW LEADS TODAY • {hi_count} high-priority (score ≥10)")
    else:
        lines.append(f"📊 {len(leads)} LEADS IN BASELINE • {hi_count} high-priority (score ≥10)")
    lines.append("")
    
    # Per-state breakdown
    for state in states:
        lines.append(f"  {state}: {state_counts.get(state, 0)} leads")
    lines.append("")
    
    # Top Leads with full URLs
    lines.append("-" * 60)
    lines.append("TOP LEADS:")
    lines.append("-" * 60)
    
    for lead in leads[:5]:
        company = lead.get("establishment_name") or "Unknown"
        city = lead.get("site_city") or "-"
        state = lead.get("site_state") or "-"
        itype = lead.get("inspection_type") or "-"
        date_opened = lead.get("date_opened") or "-"
        score = lead.get("lead_score") or 0
        url = lead.get("source_url") or "#"
        
        lines.append("")
        lines.append(f"  ★ {company}")
        lines.append(f"    {city}, {state} • {itype} • {date_opened}")
        lines.append(f"    Score: {score}")
        lines.append(f"    View: {url}")
    
    lines.append("")
    lines.append("-" * 60)
    
    # Get branding from config
    brand_name = config.get("brand_name", DEFAULT_BRAND_NAME)
    legal_name = config.get("legal_name", "")  # Optional
    mailing_address = config.get("mailing_address", "")  # Optional
    
    # Compliance footer
    lines.append("")
    if legal_name:
        lines.append(legal_name)
    else:
        lines.append(brand_name)
    
    if mailing_address:
        lines.append(mailing_address)
        lines.append("")
    
    lines.append("This report contains public OSHA inspection data for informational purposes only.")
    lines.append("Not legal advice. Verify all information before taking action.")
    lines.append("")
    lines.append(f"To unsubscribe: reply 'opt out' or email {UNSUBSCRIBE_EMAIL}")
    
    return '\n'.join(lines)


def send_email(recipient: str, subject: str, html_body: str, text_body: str,
               customer_id: str, dry_run: bool = False) -> tuple[bool, str, str]:
    """
    Send email via SMTP.
    Returns (success, message_id, error).
    """
    # Get SMTP config from environment
    smtp_host = os.environ.get("SMTP_HOST", "smtp.zoho.com")
    smtp_port = int(os.environ.get("SMTP_PORT", "587"))
    smtp_user = os.environ.get("SMTP_USER", "")
    smtp_pass = os.environ.get("SMTP_PASS", "")
    
    if not smtp_user or not smtp_pass:
        return False, "", "SMTP credentials not configured (set SMTP_USER and SMTP_PASS)"
    
    # Create message
    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = DEFAULT_FROM
    msg['To'] = recipient
    msg['Reply-To'] = DEFAULT_REPLY_TO
    msg['Sender'] = DEFAULT_SENDER
    
    # List-Unsubscribe headers (RFC 8058 one-click)
    msg['List-Unsubscribe'] = f'<{UNSUBSCRIBE_URL}>'
    msg['List-Unsubscribe-Post'] = 'List-Unsubscribe=One-Click'
    
    # Add custom header for tracking
    msg['X-Customer-ID'] = customer_id
    
    # Attach parts
    part1 = MIMEText(text_body, 'plain')
    part2 = MIMEText(html_body, 'html')
    msg.attach(part1)
    msg.attach(part2)
    
    if dry_run:
        logger.info(f"[DRY-RUN] Would send to {recipient}: {subject}")
        return True, "dry-run-no-message-id", ""
    
    try:
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.starttls()
            server.login(smtp_user, smtp_pass)
            server.send_message(msg)
            message_id = msg.get('Message-ID', f'sent-{datetime.now().isoformat()}')
            logger.info(f"Email sent to {recipient}")
            return True, message_id, ""
    except Exception as e:
        error_msg = str(e)
        logger.error(f"Failed to send email to {recipient}: {error_msg}")
        return False, "", error_msg


def main():
    parser = argparse.ArgumentParser(description="Send OSHA digest email")
    parser.add_argument("--db", required=True, help="Path to SQLite database")
    parser.add_argument("--customer", required=True, help="Path to customer config JSON")
    parser.add_argument("--mode", choices=["baseline", "daily"], default="daily",
                        help="Output mode: 'baseline' or 'daily'")
    parser.add_argument("--output-dir", default="out", help="Output directory")
    parser.add_argument("--dry-run", action="store_true", help="Generate but don't send")
    parser.add_argument("--log-level", default="INFO", 
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    
    args = parser.parse_args()
    setup_logging(args.log_level)
    
    gen_date = datetime.now().strftime("%Y-%m-%d")
    timestamp = datetime.now().isoformat()
    
    # Load config
    config = load_customer_config(args.customer)
    customer_id = config["customer_id"]
    states = config["states"]
    recipients = config.get("email_recipients", [])
    
    if not recipients:
        logger.error("No email recipients configured in customer config")
        return
    
    logger.info(f"Generating {args.mode} digest for {customer_id}")
    
    # Get leads
    conn = sqlite3.connect(args.db)
    skip_first_seen = (args.mode == "baseline")
    leads = get_leads_for_period(
        conn, states, 
        config["opened_window_days"],
        config["new_only_days"],
        skip_first_seen_filter=skip_first_seen
    )
    conn.close()
    
    logger.info(f"Found {len(leads)} leads after filtering")
    
    # Calculate metrics for subject line
    hi_count = sum(1 for l in leads if (l.get("lead_score") or 0) >= 10)
    states_str = "/".join(states)
    
    # Generate email content
    html_body = generate_digest_html(leads, config, gen_date, args.mode)
    text_body = generate_digest_text(leads, config, gen_date, args.mode)
    
    # Value-signal subject line: {states} · {date} · {count} new · {hi_count} high (≥10)
    if args.mode == "daily":
        subject = f"{states_str} · {gen_date} · {len(leads)} new · {hi_count} high (≥10) (DAILY)"
    else:
        subject = f"{states_str} · {gen_date} · {len(leads)} leads · {hi_count} high (≥10) (BASELINE)"
    
    # Ensure output dir exists
    Path(args.output_dir).mkdir(parents=True, exist_ok=True)
    email_log_path = os.path.join(args.output_dir, "email_log.csv")
    suppression_log_path = os.path.join(args.output_dir, "suppression_log.csv")
    
    # Process each recipient
    for recipient in recipients:
        recipient = recipient.strip().lower()
        
        # Pilot mode check
        if PILOT_MODE and recipient not in [r.lower() for r in PILOT_WHITELIST]:
            logger.warning(f"PILOT MODE: Skipping {recipient} (not in whitelist)")
            log_email_attempt(
                email_log_path, timestamp, customer_id, args.mode,
                recipient, subject, "skipped_pilot_mode"
            )
            continue
        
        # Suppression check
        if check_suppression(args.db, recipient):
            logger.info(f"Suppressed: {recipient}")
            log_suppression(suppression_log_path, timestamp, customer_id, 
                          recipient, "in_suppression_list")
            log_email_attempt(
                email_log_path, timestamp, customer_id, args.mode,
                recipient, subject, "suppressed"
            )
            continue
        
        # Send email
        success, message_id, error = send_email(
            recipient, subject, html_body, text_body, 
            customer_id, dry_run=args.dry_run
        )
        
        status = "sent" if success else "failed"
        if args.dry_run and success:
            status = "dry_run"
        
        log_email_attempt(
            email_log_path, timestamp, customer_id, args.mode,
            recipient, subject, status, message_id, error
        )
    
    print(f"\n{'=' * 60}")
    print(f"EMAIL DIGEST SUMMARY")
    print(f"{'=' * 60}")
    print(f"Customer:        {customer_id}")
    print(f"Mode:            {args.mode}")
    print(f"Leads:           {len(leads)}")
    print(f"Recipients:      {len(recipients)}")
    print(f"Pilot Mode:      {'ON' if PILOT_MODE else 'OFF'}")
    print(f"Dry Run:         {'YES' if args.dry_run else 'NO'}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
