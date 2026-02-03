#!/usr/bin/env python3
"""
Inbound Inbox Triage Script for OSHA Lead SaaS.

Polls Gmail inbox, classifies messages, auto-updates suppression,
generates reply drafts and engineering tickets, sends notifications.

Usage:
    python inbound_inbox_triage.py --run-once           # Process new mail then exit
    python inbound_inbox_triage.py --since-hours 24     # Backfill last 24 hours
    python inbound_inbox_triage.py --daily-summary      # Send summary and exit
    python inbound_inbox_triage.py --dry-run            # Preview without changes
"""

import argparse
import base64
import csv
import json
import os
import re
import shutil
import smtplib
import sys
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from pathlib import Path

# Load environment variables
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# Gmail API imports
try:
    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import InstalledAppFlow
    from google.auth.transport.requests import Request
    from googleapiclient.discovery import build
    GMAIL_AVAILABLE = True
except ImportError:
    GMAIL_AVAILABLE = False

# =============================================================================
# CONFIGURATION
# =============================================================================
SCRIPT_DIR = Path(__file__).parent.resolve()
SECRETS_DIR = SCRIPT_DIR / "secrets"
OUT_DIR = SCRIPT_DIR / "out"

# Paths
CREDENTIALS_PATH = SECRETS_DIR / "gmail_credentials.json"
TOKEN_PATH = SECRETS_DIR / "gmail_token.json"
STATE_PATH = OUT_DIR / "inbox_state.json"
SUPPRESSION_PATH = OUT_DIR / "suppression.csv"
METRICS_PATH = OUT_DIR / "inbound_metrics.csv"
COLD_EMAIL_LOG_PATH = OUT_DIR / "cold_email_log.csv"
REPLY_DRAFTS_DIR = OUT_DIR / "reply_drafts"
ENG_TICKETS_DIR = OUT_DIR / "eng_tickets"
TRIAGE_LOG_PATH = OUT_DIR / "inbox_triage_log.csv"

# Gmail OAuth scopes
SCOPES = [
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/gmail.labels",
    "https://www.googleapis.com/auth/gmail.modify"
]

# Classification patterns (lowercase matching)
UNSUBSCRIBE_PATTERNS = [
    "unsubscribe", "remove me", "stop", "opt out", "do not contact",
    "take me off", "opt-out", "remove from list"
]

BOUNCE_SENDERS = ["mailer-daemon", "postmaster"]
BOUNCE_PATTERNS = [
    "undelivered", "delivery status notification", "returned mail",
    "delivery failed", "undeliverable", "mailbox not found",
    "550 ", "553 ", "bounced", "permanent failure", "user unknown"
]

HOT_INTEREST_PATTERNS = [
    "yes", "interested", "let's talk", "call", "meeting", "calendar",
    "schedule", "sign me up", "i want", "sounds good", "demo"
]

OUT_OF_OFFICE_PATTERNS = [
    "out of office", "automatic reply", "away from the office",
    "auto-reply", "on vacation", "currently unavailable", "ooo"
]

OBJECTION_PATTERNS = [
    "not interested", "no thanks", "don't email", "no thank you",
    "pass on this", "not for us"
]

QUESTION_PATTERNS = [
    "pricing", "cost", "how does this work", "sample", "territory",
    "coverage", "how much", "what is", "can you"
]

BUG_FEATURE_PATTERNS = [
    "wrong data", "duplicates", "missing", "error", "broken",
    "deliverability", "spam", "unsubscribe not working", "bug",
    "doesn't work", "issue"
]

# Gmail labels
LABEL_NAMES = {
    "unsubscribe": "OSHA_UNSUB",
    "bounce": "OSHA_BOUNCE",
    "hot_interest": "OSHA_HOT",
    "objection": "OSHA_UNSUB",  # Treat objections same as unsub
    "question": "OSHA_ACTION",
    "bug_feature": "OSHA_ACTION",
    "out_of_office": "OSHA_IGNORED",
    "other": "OSHA_ACTION"
}


# =============================================================================
# STATE MANAGEMENT
# =============================================================================
def load_state() -> dict:
    """Load inbox processing state."""
    default_state = {
        "last_message_id": None,
        "last_processed_time": None,
        "label_ids": {},  # Cache label IDs
        "processed_message_ids": []
    }
    if STATE_PATH.exists():
        with open(STATE_PATH, "r") as f:
            state = json.load(f)
            for key in default_state:
                if key not in state:
                    state[key] = default_state[key]
            return state
    return default_state


def save_state(state: dict):
    """Save inbox processing state."""
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(STATE_PATH, "w") as f:
        json.dump(state, f, indent=2)


# =============================================================================
# SUPPRESSION MANAGEMENT
# =============================================================================
def load_suppression_emails() -> set:
    """Load existing suppressed emails."""
    emails = set()
    if SUPPRESSION_PATH.exists():
        with open(SUPPRESSION_PATH, "r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                emails.add(row.get("email", "").strip().lower())
    return emails


def add_to_suppression(email: str, reason: str, source: str = "inbound_triage",
                       evidence_msg_id: str = "", dry_run: bool = False):
    """Add email to suppression list."""
    email = email.strip().lower()
    if not email or "@" not in email:
        return False
    
    # Skip mailer-daemon addresses
    if "mailer-daemon" in email or "postmaster" in email:
        return False
    
    existing = load_suppression_emails()
    if email in existing:
        print(f"    [SKIP] Already suppressed: {email}")
        return False
    
    if dry_run:
        print(f"    [DRY-RUN] Would suppress: {email} ({reason})")
        return True
    
    # Append to file
    write_header = not SUPPRESSION_PATH.exists()
    with open(SUPPRESSION_PATH, "a", newline="", encoding="utf-8") as f:
        fieldnames = ["email", "reason", "source", "timestamp", "evidence_msg_id"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        writer.writerow({
            "email": email,
            "reason": reason,
            "source": source,
            "timestamp": datetime.now().isoformat(),
            "evidence_msg_id": evidence_msg_id
        })
    print(f"    [SUPPRESSED] {email} ({reason})")
    return True


def backup_suppression_file():
    """Backup suppression.csv to a timestamped file."""
    if not SUPPRESSION_PATH.exists():
        return
    backup_dir = OUT_DIR / "suppression_backups"
    backup_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    backup_path = backup_dir / f"suppression_{ts}.csv"
    try:
        shutil.copy2(SUPPRESSION_PATH, backup_path)
        print(f"[INFO] Suppression backup: {backup_path}")
    except Exception as e:
        print(f"[WARN] Suppression backup failed: {e}")


# =============================================================================
# EMAIL CLASSIFICATION
# =============================================================================
def classify_email(subject: str, body: str, from_email: str) -> str:
    """
    Classify email into category.
    Priority order: unsubscribe > bounce > out_of_office > objection > hot_interest > question > bug_feature > other
    """
    text = f"{subject} {body}".lower()
    from_lower = from_email.lower()
    
    # Check for unsubscribe first
    for pattern in UNSUBSCRIBE_PATTERNS:
        if pattern in text:
            return "unsubscribe"
    
    # Check for bounce (from address or content)
    for sender in BOUNCE_SENDERS:
        if sender in from_lower:
            return "bounce"
    for pattern in BOUNCE_PATTERNS:
        if pattern in text:
            return "bounce"
    
    # Check for out of office
    for pattern in OUT_OF_OFFICE_PATTERNS:
        if pattern in text:
            return "out_of_office"
    
    # Check for objection (if unsubscribe not present, already checked)
    for pattern in OBJECTION_PATTERNS:
        if pattern in text:
            return "objection"
    
    # Check for hot interest
    for pattern in HOT_INTEREST_PATTERNS:
        if pattern in text:
            return "hot_interest"
    
    # Check for bug/feature (before question since it's more specific)
    for pattern in BUG_FEATURE_PATTERNS:
        if pattern in text:
            return "bug_feature"
    
    # Check for question (including '?')
    if "?" in text:
        return "question"
    for pattern in QUESTION_PATTERNS:
        if pattern in text:
            return "question"
    
    return "other"


def extract_sender_email(from_header: str) -> str:
    """Extract email address from From header."""
    match = re.search(r'<([^>]+)>', from_header)
    if match:
        return match.group(1).strip().lower()
    if "@" in from_header:
        return from_header.strip().lower()
    return ""


def extract_bounce_recipient(body: str, headers: dict) -> str:
    """
    Extract the original recipient from a bounce/DSN message.
    Looks for Final-Recipient, Original-Recipient, or rfc822; patterns.
    """
    # Check common DSN patterns
    patterns = [
        r'Final-Recipient:\s*rfc822;\s*([^\s\n<>]+)',
        r'Original-Recipient:\s*rfc822;\s*([^\s\n<>]+)',
        r'rfc822;\s*([^\s\n<>]+@[^\s\n<>]+)',
        r'<([^>]+@[^>]+)>\s*was not found',
        r'The email account.*?<([^>]+)>.*?does not exist',
        r'User\s+([^\s\n<>]+@[^\s\n<>]+)\s+not found',
    ]
    
    text = body.lower()
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            email = match.group(1).strip().lower()
            if "@" in email and "mailer-daemon" not in email:
                return email
    
    return ""


# =============================================================================
# GMAIL API
# =============================================================================
def get_gmail_service(dry_run: bool = False):
    """Connect to Gmail API. Returns service object or None."""
    if not GMAIL_AVAILABLE:
        print("[ERROR] Gmail API not available.")
        print("  Run: pip install google-api-python-client google-auth-oauthlib")
        return None
    
    SECRETS_DIR.mkdir(parents=True, exist_ok=True)
    
    creds = None
    
    # Load existing token
    if TOKEN_PATH.exists():
        creds = Credentials.from_authorized_user_file(str(TOKEN_PATH), SCOPES)
    
    # Refresh or get new token
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
            except Exception as e:
                print(f"[WARN] Token refresh failed: {e}")
                creds = None
        
        if not creds:
            if not CREDENTIALS_PATH.exists():
                print(f"[ERROR] Gmail credentials not found: {CREDENTIALS_PATH}")
                print("  1. Go to Google Cloud Console -> APIs -> Gmail API")
                print("  2. Create OAuth 2.0 Client ID (Desktop app)")
                print("  3. Download JSON and save as: secrets/gmail_credentials.json")
                return None
            
            if dry_run:
                print("[DRY-RUN] Would start OAuth flow")
                return None
            
            flow = InstalledAppFlow.from_client_secrets_file(str(CREDENTIALS_PATH), SCOPES)
            creds = flow.run_local_server(port=0)
        
        # Save token
        with open(TOKEN_PATH, "w") as f:
            f.write(creds.to_json())
    
    return build("gmail", "v1", credentials=creds)


def ensure_labels_exist(service, state: dict, dry_run: bool = False) -> dict:
    """Ensure all OSHA labels exist. Returns and caches label name -> ID mapping."""
    # Use cached labels if available
    if state.get("label_ids"):
        return state["label_ids"]
    
    label_map = {}
    
    # Get existing labels
    results = service.users().labels().list(userId="me").execute()
    existing = {l["name"]: l["id"] for l in results.get("labels", [])}
    
    # Create missing labels
    for label_name in set(LABEL_NAMES.values()):
        if label_name in existing:
            label_map[label_name] = existing[label_name]
        elif not dry_run:
            try:
                label_body = {
                    "name": label_name,
                    "labelListVisibility": "labelShow",
                    "messageListVisibility": "show"
                }
                result = service.users().labels().create(userId="me", body=label_body).execute()
                label_map[label_name] = result["id"]
                print(f"  [LABEL] Created: {label_name}")
            except Exception as e:
                print(f"  [WARN] Failed to create label {label_name}: {e}")
    
    # Cache in state
    state["label_ids"] = label_map
    return label_map


def get_new_messages(service, state: dict, since_hours: int = 24, max_results: int = 100) -> list:
    """Get new messages since last check or within time window."""
    # Build query
    query_parts = ["is:inbox"]
    if since_hours:
        query_parts.append(f"newer_than:{since_hours}h")
    
    query = " ".join(query_parts)
    
    try:
        results = service.users().messages().list(
            userId="me", q=query, maxResults=max_results
        ).execute()
        messages = results.get("messages", [])
        
        # Filter out already processed
        processed = set(state.get("processed_message_ids", []))
        new_messages = [m for m in messages if m["id"] not in processed]
        
        return new_messages
    except Exception as e:
        print(f"[ERROR] Failed to fetch messages: {e}")
        return []


def get_message_details(service, message_id: str) -> dict:
    """Get full message details."""
    try:
        msg = service.users().messages().get(
            userId="me", id=message_id, format="full"
        ).execute()
        
        # Extract headers
        headers = {h["name"].lower(): h["value"] for h in msg.get("payload", {}).get("headers", [])}
        
        # Extract body
        body = ""
        payload = msg.get("payload", {})
        
        # Simple text body
        if payload.get("body", {}).get("data"):
            body = base64.urlsafe_b64decode(payload["body"]["data"]).decode("utf-8", errors="ignore")
        
        # Multipart - look for text/plain first
        for part in payload.get("parts", []):
            if part.get("mimeType") == "text/plain":
                if part.get("body", {}).get("data"):
                    body = base64.urlsafe_b64decode(part["body"]["data"]).decode("utf-8", errors="ignore")
                    break
            # Check nested parts
            for subpart in part.get("parts", []):
                if subpart.get("mimeType") == "text/plain":
                    if subpart.get("body", {}).get("data"):
                        body = base64.urlsafe_b64decode(subpart["body"]["data"]).decode("utf-8", errors="ignore")
                        break
        
        return {
            "id": message_id,
            "subject": headers.get("subject", "(no subject)"),
            "from": headers.get("from", ""),
            "to": headers.get("to", ""),
            "date": headers.get("date", ""),
            "body": body[:5000],
            "snippet": msg.get("snippet", ""),
            "headers": headers
        }
    except Exception as e:
        print(f"[ERROR] Failed to get message {message_id}: {e}")
        return None


def apply_label(service, message_id: str, label_id: str, dry_run: bool = False):
    """Apply label to message."""
    if dry_run:
        return
    try:
        service.users().messages().modify(
            userId="me", id=message_id,
            body={"addLabelIds": [label_id]}
        ).execute()
    except Exception as e:
        print(f"  [WARN] Failed to apply label: {e}")


# =============================================================================
# REPLY DRAFTS & TICKETS
# =============================================================================
def create_reply_draft(from_email: str, subject: str, body: str, 
                       category: str, msg_id: str, dry_run: bool = False) -> str:
    """Create a draft reply for hot_interest or question."""
    if dry_run:
        print(f"    [DRY-RUN] Would create reply draft for {category}")
        return ""
    
    REPLY_DRAFTS_DIR.mkdir(parents=True, exist_ok=True)
    
    date_str = datetime.now().strftime("%Y-%m-%d")
    safe_id = re.sub(r'[^a-zA-Z0-9]', '', msg_id)[:20]
    filename = f"{date_str}_{safe_id}.md"
    filepath = REPLY_DRAFTS_DIR / filename
    
    if category == "hot_interest":
        template = f"""# Reply Draft - Hot Interest

**To**: {from_email}
**Re**: {subject}
**Generated**: {datetime.now().strftime("%Y-%m-%d %H:%M")}

---

Hi,

Thanks for your interest in OSHA inspection alerts!

I'd love to learn more about your territory and set you up with a daily digest of fresh leads.

Would you have 15 minutes this week for a quick call? Here's my calendar: [INSERT CALENDAR LINK]

Or just reply with your preferred states/regions and I'll get your digest started right away.

Best,
[YOUR NAME]

---
## Original Message
From: {from_email}
Subject: {subject}

{body[:500]}
"""
    else:  # question
        template = f"""# Reply Draft - Question

**To**: {from_email}
**Re**: {subject}
**Generated**: {datetime.now().strftime("%Y-%m-%d %H:%M")}

---

Hi,

Thanks for reaching out!

[ANSWER THEIR QUESTION HERE]

Let me know if you have any other questions.

Best,
[YOUR NAME]

---
## Original Message
From: {from_email}
Subject: {subject}

{body[:500]}
"""
    
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(template)
    
    print(f"    [DRAFT] Created: {filepath.name}")
    return str(filepath)


def create_eng_ticket(from_email: str, subject: str, body: str, 
                      msg_id: str, dry_run: bool = False) -> str:
    """Create engineering ticket for bug/feature request."""
    if dry_run:
        print(f"    [DRY-RUN] Would create eng ticket")
        return ""
    
    ENG_TICKETS_DIR.mkdir(parents=True, exist_ok=True)
    
    date_str = datetime.now().strftime("%Y-%m-%d")
    slug = re.sub(r'[^a-z0-9]+', '_', subject.lower())[:30].strip('_')
    filename = f"{date_str}_{slug}.md"
    filepath = ENG_TICKETS_DIR / filename
    
    # Avoid overwriting
    counter = 1
    while filepath.exists():
        filepath = ENG_TICKETS_DIR / f"{date_str}_{slug}_{counter}.md"
        counter += 1
    
    ticket = f"""# Engineering Ticket

**Date**: {datetime.now().strftime("%Y-%m-%d %H:%M")}
**Reporter**: {from_email}
**Subject**: {subject}
**Message ID**: {msg_id}

## Summary

{body[:500]}

## Repro Steps

1. [Extract from user report]
2. ...

## Expected Behavior

[What should happen]

## Actual Behavior

[What actually happens]

## User Impact

- Reporter: {from_email}
- Severity: TBD
- Affected users: TBD

## Suggested Fix

TBD - needs investigation

## Acceptance Criteria

- [ ] Issue resolved
- [ ] User notified
- [ ] No regression
"""
    
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(ticket)
    
    print(f"    [TICKET] Created: {filepath.name}")
    return str(filepath)


# =============================================================================
# NOTIFICATIONS
# =============================================================================
def send_smtp_email(to_email: str, subject: str, body: str, dry_run: bool = False) -> bool:
    """Send email via SMTP."""
    smtp_host = os.getenv("SMTP_HOST", "smtppro.zoho.com")
    smtp_port = int(os.getenv("SMTP_PORT", "465"))
    smtp_user = os.getenv("SMTP_USER", "")
    smtp_pass = os.getenv("SMTP_PASS", "")
    from_email = os.getenv("FROM_EMAIL", smtp_user)
    
    if dry_run:
        print(f"    [DRY-RUN] Would email: {to_email}")
        print(f"    Subject: {subject}")
        return True
    
    if not smtp_user or not smtp_pass:
        print("[WARN] SMTP not configured")
        return False
    
    try:
        msg = MIMEText(body)
        msg["Subject"] = subject
        msg["From"] = from_email
        msg["To"] = to_email
        
        if smtp_port == 465:
            with smtplib.SMTP_SSL(smtp_host, smtp_port) as server:
                server.login(smtp_user, smtp_pass)
                server.send_message(msg)
        else:
            with smtplib.SMTP(smtp_host, smtp_port) as server:
                server.starttls()
                server.login(smtp_user, smtp_pass)
                server.send_message(msg)
        
        print(f"    [NOTIFIED] {to_email}")
        return True
    except Exception as e:
        print(f"[ERROR] Failed to send notification: {e}")
        return False


def send_immediate_notification(from_email: str, subject: str, body: str,
                                 category: str, dry_run: bool = False):
    """Send immediate notification for high-priority items."""
    notify_email = os.getenv("NOTIFY_EMAIL")
    if not notify_email:
        return
    
    notif_subject = f"[OSHA {category.upper()}] {subject[:50]}"
    notif_body = f"""New {category.upper()} detected

From: {from_email}
Subject: {subject}

--- Message Preview ---
{body[:500]}
---

Action: Review and respond.
"""
    
    send_smtp_email(notify_email, notif_subject, notif_body, dry_run)


def send_bounce_spike_warning(bounce_count: int, sent_count: int, 
                               bounce_rate: float, dry_run: bool = False):
    """Send warning if bounce rate exceeds threshold."""
    notify_email = os.getenv("NOTIFY_EMAIL")
    if not notify_email:
        return
    
    subject = f"[OSHA WARNING] High bounce rate: {bounce_rate:.1%}"
    body = f"""BOUNCE RATE WARNING

Today's stats:
- Emails sent: {sent_count}
- Bounces: {bounce_count}
- Bounce rate: {bounce_rate:.1%}

This exceeds the 5% threshold. Please investigate:
1. Check suppression list is being used
2. Verify email list quality
3. Check for deliverability issues

Action required to protect sender reputation.
"""
    
    send_smtp_email(notify_email, subject, body, dry_run)


# =============================================================================
# METRICS
# =============================================================================
def get_today_sent_count() -> int:
    """Get count of emails sent today from cold_email_log.csv."""
    today = datetime.now().strftime("%Y-%m-%d")
    count = 0
    
    if COLD_EMAIL_LOG_PATH.exists():
        with open(COLD_EMAIL_LOG_PATH, "r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get("campaign_id") == today and row.get("status") == "sent":
                    count += 1
    
    return count


def log_metrics(processed: int, unsub: int, bounce: int, 
                hot: int, action: int, dry_run: bool = False):
    """Log run metrics to CSV."""
    if dry_run:
        return
    
    write_header = not METRICS_PATH.exists()
    with open(METRICS_PATH, "a", newline="", encoding="utf-8") as f:
        fieldnames = ["timestamp", "processed_count", "unsub_count", 
                      "bounce_count", "hot_count", "action_count"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        writer.writerow({
            "timestamp": datetime.now().isoformat(),
            "processed_count": processed,
            "unsub_count": unsub,
            "bounce_count": bounce,
            "hot_count": hot,
            "action_count": action
        })


def log_triage(msg_id: str, from_email: str, subject: str,
               category: str, action: str, dry_run: bool = False):
    """Log triage decision."""
    if dry_run:
        return
    
    write_header = not TRIAGE_LOG_PATH.exists()
    with open(TRIAGE_LOG_PATH, "a", newline="", encoding="utf-8") as f:
        fieldnames = ["timestamp", "message_id", "from_email", "subject", "category", "action"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        writer.writerow({
            "timestamp": datetime.now().isoformat(),
            "message_id": msg_id,
            "from_email": from_email,
            "subject": subject[:100],
            "category": category,
            "action": action
        })


# =============================================================================
# DAILY SUMMARY
# =============================================================================
def generate_daily_summary(dry_run: bool = False):
    """Generate and send daily summary from logs."""
    notify_email = os.getenv("NOTIFY_EMAIL")
    if not notify_email:
        print("[WARN] NOTIFY_EMAIL not set")
        return
    
    # Read last 24h from triage log
    cutoff = datetime.now() - timedelta(hours=24)
    
    counts = {"unsubscribe": 0, "bounce": 0, "hot_interest": 0, 
              "question": 0, "objection": 0, "out_of_office": 0, 
              "bug_feature": 0, "other": 0}
    hot_items = []
    questions = []
    
    if TRIAGE_LOG_PATH.exists():
        with open(TRIAGE_LOG_PATH, "r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    ts = datetime.fromisoformat(row.get("timestamp", ""))
                    if ts > cutoff:
                        cat = row.get("category", "other")
                        counts[cat] = counts.get(cat, 0) + 1
                        
                        if cat == "hot_interest":
                            hot_items.append(row)
                        elif cat == "question":
                            questions.append(row)
                except:
                    pass
    
    # Build summary
    date_str = datetime.now().strftime("%Y-%m-%d")
    total = sum(counts.values())
    
    body = f"""OSHA Inbox Daily Summary - {date_str}

Total processed (24h): {total}

COUNTS BY CATEGORY:
"""
    for cat, count in sorted(counts.items()):
        if count > 0:
            body += f"  {cat}: {count}\n"
    
    if hot_items:
        body += f"\nHOT INTEREST ({len(hot_items)}):\n"
        for item in hot_items[:10]:
            body += f"  - {item.get('from_email', 'Unknown')}: {item.get('subject', 'No subject')[:50]}\n"
    
    if questions:
        body += f"\nQUESTIONS NEEDING REPLY ({len(questions)}):\n"
        for item in questions[:10]:
            body += f"  - {item.get('from_email', 'Unknown')}: {item.get('subject', 'No subject')[:50]}\n"
    
    body += "\n---\nGenerated by OSHA Inbox Triage\n"
    
    subject = f"[OSHA Daily Summary] {date_str} - {total} items"
    send_smtp_email(notify_email, subject, body, dry_run)


# =============================================================================
# MAIN PROCESSING
# =============================================================================
def process_message(service, msg: dict, state: dict, label_map: dict,
                    dry_run: bool = False) -> dict:
    """Process a single message. Returns stats dict."""
    msg_id = msg["id"]
    
    details = get_message_details(service, msg_id)
    if not details:
        return {"category": None}
    
    from_email = extract_sender_email(details["from"])
    subject = details["subject"]
    body = details["body"]
    
    print(f"  [{msg_id[:8]}] {from_email}: {subject[:40]}...")
    
    # Classify
    category = classify_email(subject, body, from_email)
    print(f"    -> {category}")
    
    action = ""
    
    # Handle each category
    if category == "unsubscribe":
        add_to_suppression(from_email, "unsubscribe", "inbound_triage", msg_id, dry_run)
        action = "suppressed"
    
    elif category == "objection":
        add_to_suppression(from_email, "not_interested", "inbound_triage", msg_id, dry_run)
        action = "suppressed"
    
    elif category == "bounce":
        # Try to extract original recipient
        recipient = extract_bounce_recipient(body, details.get("headers", {}))
        if recipient:
            add_to_suppression(recipient, "bounce", "inbound_triage", msg_id, dry_run)
            action = "suppressed_recipient"
        else:
            # Log as unknown bounce
            print(f"    [WARN] Could not extract bounce recipient")
            action = "bounce_unknown"
    
    elif category == "hot_interest":
        create_reply_draft(from_email, subject, body, category, msg_id, dry_run)
        send_immediate_notification(from_email, subject, body, category, dry_run)
        action = "notified+draft"
    
    elif category == "question":
        create_reply_draft(from_email, subject, body, category, msg_id, dry_run)
        send_immediate_notification(from_email, subject, body, category, dry_run)
        action = "notified+draft"
    
    elif category == "bug_feature":
        create_eng_ticket(from_email, subject, body, msg_id, dry_run)
        action = "ticket_created"
    
    elif category == "out_of_office":
        action = "ignored"
    
    else:  # other
        action = "labeled"
    
    # Apply Gmail label
    label_name = LABEL_NAMES.get(category, "OSHA_ACTION")
    label_id = label_map.get(label_name)
    if label_id:
        apply_label(service, msg_id, label_id, dry_run)
    
    # Log triage
    log_triage(msg_id, from_email, subject, category, action, dry_run)
    
    # Mark processed
    if not dry_run:
        state["processed_message_ids"].append(msg_id)
        # Keep last 1000
        if len(state["processed_message_ids"]) > 1000:
            state["processed_message_ids"] = state["processed_message_ids"][-1000:]
    
    return {"category": category}


# =============================================================================
# MAIN
# =============================================================================
def main():
    parser = argparse.ArgumentParser(
        description="OSHA Inbox Triage - Gmail classification and auto-actions"
    )
    parser.add_argument("--run-once", action="store_true",
                        help="Process new mail then exit")
    parser.add_argument("--since-hours", type=int, default=24,
                        help="Backfill window in hours (default: 24)")
    parser.add_argument("--daily-summary", action="store_true",
                        help="Send daily summary email and exit")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview without changes")
    parser.add_argument("--max-messages", type=int, default=100,
                        help="Max messages to process")
    args = parser.parse_args()
    
    print(f"[INFO] OSHA Inbox Triage")
    print(f"[INFO] {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"[INFO] Mode: {'DRY-RUN' if args.dry_run else 'LIVE'}")
    
    # Daily summary mode
    if args.daily_summary:
        generate_daily_summary(args.dry_run)
        return
    
    # Connect to Gmail
    if not GMAIL_AVAILABLE:
        print("[ERROR] Gmail API not installed.")
        print("  pip install google-api-python-client google-auth-oauthlib")
        sys.exit(1)
    
    service = get_gmail_service(args.dry_run)
    if not service:
        sys.exit(1)
    
    # Load state
    state = load_state()
    
    # Ensure labels exist
    print("[INFO] Checking labels...")
    label_map = ensure_labels_exist(service, state, args.dry_run)
    
    # Get messages
    print(f"[INFO] Fetching messages (last {args.since_hours}h)...")
    messages = get_new_messages(service, state, args.since_hours, args.max_messages)
    print(f"[INFO] Found {len(messages)} new messages")
    
    if not messages:
        print("[INFO] No new messages to process")
        return
    
    # Process messages
    counts = {"unsubscribe": 0, "bounce": 0, "hot_interest": 0, 
              "question": 0, "objection": 0, "out_of_office": 0,
              "bug_feature": 0, "other": 0}
    
    for msg in messages:
        try:
            result = process_message(service, msg, state, label_map, args.dry_run)
            cat = result.get("category")
            if cat:
                counts[cat] = counts.get(cat, 0) + 1
        except Exception as e:
            print(f"[ERROR] Failed to process: {e}")
    
    # Check bounce rate
    bounce_count = counts.get("bounce", 0)
    sent_count = get_today_sent_count()
    if sent_count > 0 and bounce_count > 0:
        bounce_rate = bounce_count / sent_count
        if bounce_rate > 0.05:
            print(f"[WARNING] High bounce rate: {bounce_rate:.1%}")
            send_bounce_spike_warning(bounce_count, sent_count, bounce_rate, args.dry_run)
    
    # Log metrics
    log_metrics(
        processed=len(messages),
        unsub=counts.get("unsubscribe", 0) + counts.get("objection", 0),
        bounce=counts.get("bounce", 0),
        hot=counts.get("hot_interest", 0),
        action=counts.get("question", 0) + counts.get("bug_feature", 0) + counts.get("other", 0),
        dry_run=args.dry_run
    )
    
    # Save state
    if not args.dry_run:
        state["last_processed_time"] = datetime.now().isoformat()
        save_state(state)
        
        # Backup suppression list if it may have changed
        if (counts.get("unsubscribe", 0) > 0 or 
            counts.get("objection", 0) > 0 or 
            counts.get("bounce", 0) > 0):
            backup_suppression_file()
    
    # Summary
    print(f"\n{'='*50}")
    print(f"[SUMMARY] Processed: {len(messages)}")
    for cat, count in sorted(counts.items()):
        if count > 0:
            print(f"  {cat}: {count}")
    print(f"{'='*50}")


if __name__ == "__main__":
    main()
