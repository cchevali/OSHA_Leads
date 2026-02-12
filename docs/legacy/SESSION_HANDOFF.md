> DEPRECATED - see `docs/V1_CUSTOMER_VALIDATED.md`, `docs/RUNBOOK.md`, and `docs/ARCHITECTURE.md`.
> Date: 2026-02-12
> Rationale: Archived during canonical spine adoption; retained only as a historical V1 artifact.

---
# SESSION HANDOFF â€” OSHA Concierge MVP
**Date:** 2026-01-13

---

## 1. Current Capabilities

| Feature | Status | Files |
|---------|--------|-------|
| Data Ingestion | âœ… Live | `ingest_osha.py` |
| Per-State Alerts | âœ… Live | `generate_alert.py` |
| Customer Bundles | âœ… Live | `generate_customer_alert.py` |
| Email Delivery | âœ… Live | `send_digest_email.py` |
| Daily Metrics | âœ… Tracking | `out/daily_metrics.csv` |

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
- âœ… Pilot mode ON (whitelist only)
- âœ… Suppression check before send
- âœ… List-Unsubscribe + One-Click headers
- âœ… HTML + plain text multipart
- âœ… Configurable branding (`brand_name`, `legal_name`, `mailing_address`)
- âœ… Email logging â†’ `out/email_log.csv`

**Subject Format:**
```
TX/CA/FL Â· 2026-01-13 Â· 22 new Â· 2 high (â‰¥10) (DAILY)
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
| `deliver_daily.py` | **Daily entrypoint** â€” runs ingest + email, validates config |
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
| `docs/legacy/CUSTOMER_ONBOARDING.md` | 3-minute customer onboarding checklist |

---

## 7. Next Steps

1. **Rotate SMTP password** â€” Previous password was in repo; generate new app password in Zoho
2. **Add more customers** â€” See `docs/legacy/CUSTOMER_ONBOARDING.md` for 3-minute process
3. **Schedule automation** â€” See `TASK_SCHEDULER_RUNBOOK.md`
4. **Disable pilot mode** â€” Edit `send_digest_email.py` line 28: `PILOT_MODE = False`
5. **Set legal entity** â€” Add `legal_name` and `mailing_address` to customer config when incorporated



