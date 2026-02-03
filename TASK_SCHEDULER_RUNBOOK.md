# Windows Task Scheduler Runbook — OSHA Daily Delivery

## Overview

This document describes how to schedule automated daily OSHA lead delivery using Windows Task Scheduler.

---

## Prerequisites

1. **Environment Variables** — Set system-wide or in a startup script:
   - `SMTP_HOST` = `smtp.zoho.com`
   - `SMTP_PORT` = `587`
   - `SMTP_USER` = `<your-zoho-username>`
   - `SMTP_PASS` = `<your-zoho-app-password>`

2. **Python Virtual Environment** — Located at `venv\` in project directory

3. **Customer Config** — Located at `customers\sunbelt_ca_pilot.json`

---

## Task Configuration

### General Tab

| Setting | Value |
|---------|-------|
| Name | `OSHA Daily Delivery` |
| Description | `Ingest OSHA data and send daily digest emails` |
| Security Options | `Run whether user is logged on or not` |
| Run with highest privileges | ☐ (not required) |

### Triggers Tab

| Setting | Value |
|---------|-------|
| Begin the task | `On a schedule` |
| Settings | `Daily` |
| Start | `6:00:00 AM` (recommended: before business hours) |
| Recur every | `1 days` |
| Enabled | ☑ |

### Actions Tab

| Setting | Value |
|---------|-------|
| Action | `Start a program` |
| Program/script | `C:\Users\lever\OneDrive\Desktop\OSHA Leads\venv\Scripts\python.exe` |
| Add arguments | `deliver_daily.py --customer customers/sunbelt_ca_pilot.json` |
| Start in | `C:\Users\lever\OneDrive\Desktop\OSHA Leads` |

### Conditions Tab

| Setting | Value |
|---------|-------|
| Start only if AC power | ☐ (uncheck for reliability) |
| Wake computer to run | ☑ (optional) |

### Settings Tab

| Setting | Value |
|---------|-------|
| Allow task to be run on demand | ☑ |
| Stop task if running longer than | `1 hour` |
| If task fails, restart every | `10 minutes` |
| Attempt to restart up to | `3 times` |

---

## Setting Environment Variables

### Option 1: System Environment Variables (Recommended)

1. Open: `Control Panel → System → Advanced system settings → Environment Variables`
2. Under "System variables", click "New" for each:
   - `SMTP_HOST` = `smtp.zoho.com`
   - `SMTP_PORT` = `587`
   - `SMTP_USER` = `<your-username>`
   - `SMTP_PASS` = `<your-password>`

### Option 2: Batch Wrapper Script

Create `run_daily.bat` in project directory:

```batch
@echo off
cd /d C:\Users\lever\OneDrive\Desktop\OSHA Leads

set SMTP_HOST=smtp.zoho.com
set SMTP_PORT=587
set SMTP_USER=<your-username>
set SMTP_PASS=<your-password>

.\venv\Scripts\python.exe deliver_daily.py --customer customers/sunbelt_ca_pilot.json

exit /b %ERRORLEVEL%
```

Then set the Task Scheduler action to run `run_daily.bat` instead.

---

## Log Files

| File | Location | Purpose |
|------|----------|---------|
| Run log | `out\run_log_YYYY-MM-DD.txt` | Full command output per run |
| Email log | `out\email_log.csv` | All email send attempts |
| Daily metrics | `out\daily_metrics.csv` | Per-state lead counts over time |

---

## Manual Test Run

```powershell
cd "C:\Users\lever\OneDrive\Desktop\OSHA Leads"

# Set env vars for this session
$env:SMTP_HOST='smtp.zoho.com'
$env:SMTP_PORT='587'
$env:SMTP_USER='<your-username>'
$env:SMTP_PASS='<your-password>'

# Dry-run (no actual email sent)
.\venv\Scripts\python deliver_daily.py --customer customers/sunbelt_ca_pilot.json --dry-run

# Live run
.\venv\Scripts\python deliver_daily.py --customer customers/sunbelt_ca_pilot.json
```

---

## Troubleshooting

| Issue | Check |
|-------|-------|
| Task not running | Verify credentials in Task Scheduler edit dialog |
| No emails sent | Check `out\email_log.csv` for status |
| SMTP errors | Verify env vars are set; check Zoho app password |
| Python not found | Use full path to `venv\Scripts\python.exe` |
| Run log missing | Ensure `out\` directory exists |

---

## Disabling Pilot Mode

Edit `send_digest_email.py` line 28:
```python
PILOT_MODE = False
```

This allows sending to all recipients in customer config, not just the whitelist.
