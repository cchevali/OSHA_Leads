# Onboard a New Customer in 3 Minutes

This guide walks through adding a new customer to the OSHA Concierge system. The process is 100% config-driven — no code changes required.

---

## Prerequisites

- SMTP credentials configured as environment variables
- Database initialized (`data/osha.sqlite`)
- Python virtual environment active

---

## Step 1: Create Customer Config (30 seconds)

Copy the template and customize:

```powershell
# Copy template
copy customers\sunbelt_ca_pilot.json customers\NEW_CUSTOMER.json
```

Edit `customers/NEW_CUSTOMER.json`:

```json
{
  "customer_id": "acme_corp",
  "states": ["TX", "CA"],
  "opened_window_days": 30,
  "new_only_days": 1,
  "top_k_overall": 25,
  "top_k_per_state": 10,
  "email_recipients": ["sales@acme.com", "ops@acme.com"],
  "brand_name": "MicroFlowOps",
  "legal_name": "",
  "mailing_address": ""
}
```

**Required fields:**
- `customer_id` — Unique identifier (used in filenames)
- `states` — List of 2-letter state codes
- `opened_window_days` — Include inspections opened in last N days
- `new_only_days` — Only include inspections first seen in last N days
- `email_recipients` — List of email addresses

---

## Step 2: Dry-Run Test (1 minute)

Validate everything without sending email:

```powershell
cd "C:\dev\OSHA_Leads"
.\venv\Scripts\python deliver_daily.py --customer customers/NEW_CUSTOMER.json --dry-run
```

Expected output:
```
[INFO] Customer: acme_corp
[INFO] ✓ Customer config valid
[INFO] ✓ Suppression list accessible
[INFO] ✓ Ingestion completed
[INFO] ✓ Email delivery completed
[SUCCESS] Daily delivery completed for acme_corp
```

**If errors appear:** Fix the config and re-run until all checks pass.

---

## Step 3: Send First Email (30 seconds)

### ⚠️ PILOT_MODE Check

By default, `PILOT_MODE = True` in `send_digest_email.py`. This restricts sending to the whitelist only.

**To add recipients to the pilot whitelist:**

Edit `send_digest_email.py` lines 29-31:
```python
PILOT_WHITELIST = [
    "cchevali@gmail.com",
    "sales@acme.com",  # Add new recipients here
]
```

**To disable pilot mode entirely (production):**

Edit `send_digest_email.py` line 28:
```python
PILOT_MODE = False  # WARNING: Enables sending to ALL recipients
```

### Send Live Email

```powershell
# Set SMTP credentials
$env:SMTP_HOST='smtp.zoho.com'
$env:SMTP_PORT='587'
$env:SMTP_USER='<your-user>'
$env:SMTP_PASS='<your-password>'

# Send baseline (first email for onboarding)
.\venv\Scripts\python deliver_daily.py --customer customers/NEW_CUSTOMER.json --mode baseline
```

---

## Step 4: Schedule Daily Sends (1 minute)

See `TASK_SCHEDULER_RUNBOOK.md` for Windows Task Scheduler setup.

Quick summary:
- **Program:** `C:\...\venv\Scripts\python.exe`
- **Arguments:** `deliver_daily.py --customer customers/NEW_CUSTOMER.json`
- **Start in:** `C:\dev\OSHA_Leads`
- **Schedule:** Daily at 6:00 AM

---

## Verification Checklist

After onboarding, verify:

- [ ] Config file exists in `customers/`
- [ ] Dry-run passes with no errors
- [ ] Test email received (check spam folder)
- [ ] Entry appears in `out/email_log.csv`
- [ ] Run log created: `out/run_log_YYYY-MM-DD.txt`
- [ ] (Optional) Task Scheduler job created

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Config validation error | Check required fields in JSON |
| Suppression check failed | Ensure `data/osha.sqlite` exists with schema |
| Email not received | Check SMTP env vars, verify recipient in whitelist |
| PILOT_MODE blocking | Add recipient to whitelist or disable pilot mode |

---

## Files Reference

| File | Purpose |
|------|---------|
| `customers/*.json` | Customer configurations |
| `deliver_daily.py` | Run this to deliver |
| `send_digest_email.py` | Email logic (PILOT_MODE here) |
| `out/email_log.csv` | Send history |
| `out/run_log_*.txt` | Execution logs |

