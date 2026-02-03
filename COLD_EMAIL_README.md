# OSHA Cold Email & Inbox Triage

Automated outbound cold email and inbound inbox triage for OSHA Citation Lead SaaS.

## Quick Start (WSL2 Ubuntu)

```bash
# Navigate to project
cd /mnt/c/Users/lever/OneDrive/Desktop/OSHA_Leads

# Install dependencies
pip install -r requirements.txt

# Create latest_leads.csv symlink (or copy)
ln -sf out/daily_leads_2026-01-06.csv out/latest_leads.csv

# Add recipients
echo 'email,first_name,last_name,firm_name,segment,state_pref
test@example.com,Test,User,Test Firm,construction,TX' >> out/recipients.csv

# Test dry-run
python3 outbound_cold_email.py --dry-run
```

## Scripts

### outbound_cold_email.py

Sends daily cold email campaigns with OSHA lead samples.

```bash
# Dry-run (preview without sending)
python3 outbound_cold_email.py --dry-run

# Production run
python3 outbound_cold_email.py

# Override daily limit
python3 outbound_cold_email.py --limit 10
```

**Features:**
- Score-based lead selection (prefers >= 8, falls back to >= 6, >= 4)
- Suppression checking before sending
- Rate limiting (4-10 sec jitter)
- Comprehensive logging to `out/cold_email_log.csv`

### inbound_inbox_triage.py

Polls Gmail, classifies messages, and auto-manages suppression.

```bash
# First time: OAuth setup
python3 inbound_inbox_triage.py --setup-oauth

# Test classification
python3 inbound_inbox_triage.py --test-classify

# Dry-run
python3 inbound_inbox_triage.py --dry-run

# Production run
python3 inbound_inbox_triage.py

# Force daily digest
python3 inbound_inbox_triage.py --send-digest
```

**Features:**
- Gmail API with OAuth2
- Auto-classification: unsubscribe, bounce, interested, question, bug/feature, OOO
- Auto-suppression for unsubscribes/bounces
- Engineering tickets for bug/feature requests
- Immediate notifications for high-intent replies
- Daily digest summaries

## Configuration

### .env

```bash
# SMTP - use domain mailbox for production (see note below)
SMTP_HOST=smtppro.zoho.com
SMTP_PORT=465
SMTP_USER=alerts@microflowops.com
SMTP_PASS=your-app-specific-password

# Email Addresses - FROM_EMAIL must match SMTP_USER
FROM_EMAIL=alerts@microflowops.com
REPLY_TO_EMAIL=support@microflowops.com
NOTIFY_EMAIL=cchevali@gmail.com

# CAN-SPAM Compliance - REQUIRED, must be real address
MAILING_ADDRESS=11539 Links Dr, Reston, VA 20190

# Brand (no LLC unless registered)
BRAND_NAME=MicroFlowOps
# BRAND_LEGAL_NAME=MicroFlowOps LLC  # Only if registered

# Unsubscribe endpoint (only set if live)
# UNSUB_ENDPOINT_BASE=https://microflowops.com/unsubscribe

# Gmail OAuth
GMAIL_CREDENTIALS_PATH=./secrets/gmail_credentials.json
```

### Important: Zoho From Address Setup

**Do NOT send production campaigns from @zohomail.com addresses.**

To set up a domain-based sending address:
1. In Zoho Mail Admin → Users → Add User
2. Create `alerts@microflowops.com` mailbox
3. Enable IMAP/SMTP for that user
4. Generate an App-Specific Password (if 2FA enabled)
5. Update `.env` with those credentials

This ensures proper SPF/DKIM alignment for deliverability.

### cold_email_config.json

```json
{
  "daily_send_limit": 25,
  "min_delay_seconds": 4,
  "max_delay_seconds": 10,
  "sample_leads_min": 2,
  "sample_leads_max": 5,
  "score_thresholds": [8, 6, 4],
  "recency_days": 7
}
```

## File Formats

### out/recipients.csv
```csv
email,first_name,last_name,firm_name,segment,state_pref
```

### out/suppression.csv
```csv
email,reason,source,timestamp
```

### out/latest_leads.csv
Copy or symlink to latest daily export:
```bash
ln -sf daily_leads_2026-01-28.csv out/latest_leads.csv
```

## Gmail OAuth Setup

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create project → Enable Gmail API
3. Create OAuth 2.0 Client ID (Desktop app)
4. Download JSON → save as `credentials.json`
5. Run: `python3 inbound_inbox_triage.py --setup-oauth`
6. Complete browser auth flow

## Cron Examples (WSL2)

```bash
crontab -e

# Outbound at 8am ET daily
0 8 * * * cd /mnt/c/Users/lever/OneDrive/Desktop/OSHA_Leads && python3 outbound_cold_email.py >> out/cold_email_cron.log 2>&1

# Inbox triage every 15 min
*/15 * * * * cd /mnt/c/Users/lever/OneDrive/Desktop/OSHA_Leads && python3 inbound_inbox_triage.py >> out/inbox_triage_cron.log 2>&1
```

## Logs

- `out/cold_email_log.csv` - All outbound sends
- `out/inbox_triage_log.csv` - All triage decisions
- `out/inbox_state.json` - Processing cursor
- `out/suppression.csv` - Suppression list
- `out/eng_tickets/*.md` - Engineering tickets
