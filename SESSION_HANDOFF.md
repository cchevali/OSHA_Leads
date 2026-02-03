# SESSION HANDOFF — OSHA Concierge MVP
**Date:** 2026-01-13

---

## 1. Current Capabilities

| Feature | Status | Files |
|---------|--------|-------|
| Data Ingestion | ✅ Live | `ingest_osha.py` |
| Per-State Alerts | ✅ Live | `generate_alert.py` |
| Customer Bundles | ✅ Live | `generate_customer_alert.py` |
| Email Delivery | ✅ Live | `send_digest_email.py` |
| Daily Metrics | ✅ Tracking | `out/daily_metrics.csv` |

---

## 2. Database Status

| State | Records | 30-Day Leads |
|-------|---------|--------------|
| TX | 107 | 51+ |
| CA | 126 | 84 |
| FL | 92 | 53 |
| **Total** | **325** | **188+** |

---

## 3. Customer: sunbelt_ca_pilot

**Config:** `customers/sunbelt_ca_pilot.json`

| Setting | Value |
|---------|-------|
| States | TX, CA, FL |
| Opened Window | 30 days |
| New Only | 1 day |
| Top K Overall | 25 |
| Top K Per State | 10 |
| Email Recipients | cchevali@gmail.com |

---

## 4. Email System

**SMTP:** Zoho (`smtp.zoho.com:587`)

| Header | Value |
|--------|-------|
| From | `MicroFlowOps OSHA Alerts <alerts@microflowops.com>` |
| Reply-To | `support@microflowops.com` |
| Sender | `alerts@microflowops.com` |

**Features:**
- ✅ Pilot mode ON (whitelist only)
- ✅ Suppression check before send
- ✅ List-Unsubscribe + One-Click headers
- ✅ HTML + plain text multipart
- ✅ Configurable branding (`brand_name`, `legal_name`, `mailing_address`)
- ✅ Email logging → `out/email_log.csv`

**Subject Format:**
```
TX/CA/FL · 2026-01-13 · 22 new · 2 high (≥10) (DAILY)
```

---

## 5. Daily Commands

```powershell
# Set SMTP credentials via environment variables (DO NOT commit real values)
$env:SMTP_HOST='smtp.zoho.com'
$env:SMTP_PORT='587'
$env:SMTP_USER='<your-smtp-user>'      # Set via env vars
$env:SMTP_PASS='<your-smtp-password>'  # Set via env vars

# Run daily delivery
.\venv\Scripts\python deliver_daily.py --customer customers/sunbelt_ca_pilot.json
```

> **Note:** Store credentials securely. Never commit real passwords to version control.

---

## 6. Key Files

| File | Purpose |
|------|---------|
| `deliver_daily.py` | **Daily entrypoint** — runs ingest + email, validates config |
| `ingest_osha.py` | Scrape OSHA, parse, store with deduplication |
| `generate_alert.py` | Per-state CSV + digest generation |
| `generate_customer_alert.py` | Customer bundle with baseline/daily modes |
| `send_digest_email.py` | Email delivery via SMTP (PILOT_MODE here) |
| `schema.sql` | SQLite schema with suppression_list |
| `customers/*.json` | Customer configs |
| `out/daily_metrics.csv` | Append-only metrics log |
| `out/email_log.csv` | Email send history |
| `out/run_log_YYYY-MM-DD.txt` | Daily run logs |
| `TASK_SCHEDULER_RUNBOOK.md` | Windows Task Scheduler setup guide |
| `CUSTOMER_ONBOARDING.md` | 3-minute customer onboarding checklist |

---

## 7. Next Steps

1. **Rotate SMTP password** — Previous password was in repo; generate new app password in Zoho
2. **Add more customers** — See `CUSTOMER_ONBOARDING.md` for 3-minute process
3. **Schedule automation** — See `TASK_SCHEDULER_RUNBOOK.md`
4. **Disable pilot mode** — Edit `send_digest_email.py` line 28: `PILOT_MODE = False`
5. **Set legal entity** — Add `legal_name` and `mailing_address` to customer config when incorporated

