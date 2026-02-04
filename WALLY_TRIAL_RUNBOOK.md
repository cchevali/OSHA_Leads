# Wally Trial Runbook

## Supported Production Setup

Only one production setup is supported:
1. Place a real `.env` file in repo root (`C:\dev\OSHA_Leads\.env`) using `.env.template`.
2. Use `run_wally_trial_daily.bat` from Task Scheduler.
3. Ensure the batch starts with `cd /d C:\dev\OSHA_Leads` so `.env` is loaded reliably.

## 1) Create `.env` from Template

```powershell
copy .env.template .env
```

Required keys in `.env`:
- `SMTP_HOST`
- `SMTP_PORT`
- `SMTP_USER`
- `SMTP_PASS`
- `BRAND_NAME`
- `MAILING_ADDRESS`

Optional keys:
- `BRAND_LEGAL_NAME` (if used)
- `FROM_EMAIL` / `FROM_NAME` (if applicable)
- `UNSUB_ENDPOINT_BASE` (+ `UNSUB_SECRET` when one-click is enabled)

## 2) Seed Territory + Subscriber + Recipients

```powershell
python setup_wally_trial.py
```

This creates/updates:
- `territories.TX_TRIANGLE_V1`
- `subscribers.wally_trial` with:
  - `trial_length_days=14`
  - `active=1`
  - `send_time_local=08:00`
  - `timezone=America/Chicago`
  - `content_filter=high_medium`
  - `include_low_fallback=1`
  - `recipients_json=["wgs@indigocompliance.com","brandon@indigoenergyservices.com"]`
- `customers/wally_trial_tx_triangle_v1.json`

## 3) Preflight-Only Check (Required Before Live)

```powershell
python run_wally_trial.py wally_trial_tx_triangle_v1.json --preflight-only
```

Behavior:
- Exit `0` with `PREFLIGHT_OK` when config is complete.
- Exit nonzero with one concise error line:
  - `CONFIG_ERROR missing variables: ...`

## 4) Ingest Last 14 Days (TX)

```powershell
python ingest_osha.py --db data/osha.sqlite --states TX --since-days 14 --max-details 500
```

## 5) Dry-Run + Preview + Counts

```powershell
python run_wally_trial.py wally_trial_tx_triangle_v1.json --db data/osha.sqlite --lookback-days 14 --chase-email cchevali@gmail.com
```

Outputs:
- `out/wally_trial_daily_counts_<today>.csv`
- preview dry-run email to Chase (recipient override)

## 6) First Live Send

```powershell
python run_wally_trial.py wally_trial_tx_triangle_v1.json --send-live --chase-email cchevali@gmail.com
```

Operator verification (after first live send):
- Open `out/email_log.csv` and confirm two rows for the same run timestamp (one for `wgs@indigocompliance.com`, one for `brandon@indigoenergyservices.com`) with `status=sent`.
- Confirm both recipients received separate individually-addressed messages (no CC).

## 7) Enable 8:00 AM Schedule (CT)

```powershell
python run_wally_trial.py wally_trial_tx_triangle_v1.json --enable-schedule
```

Task name: `OSHA Wally Trial Daily`

Note: Windows Task Scheduler runs in host timezone. Ensure host timezone is Central Time for 8:00 AM CT delivery.

## Logging and Alerting

- Pipeline run logs: `out/run_log_YYYY-MM-DD.txt`
- Email attempts: `out/email_log.csv`
- Suppression actions: `out/suppression_log.csv`
- Append-only unsubscribe events: `out/unsubscribe_events.csv`
- Scheduled task log: `out/wally_trial_task.log`
- Failure alert email: sent by `deliver_daily.py --admin-email ...` on non-zero runs

Batch logging behavior:
- Task log captures full stdout/stderr for each run.
- Adds explicit `CONFIG_ERROR detected` line when preflight/config errors are found.
- Adds explicit success/failure line per run.

## Troubleshooting (Common Live Failures)

- `Zoho alias/from not authorized`: check SMTP/send error details in `out/email_log.csv`, full command output in `out/run_log_*.txt`, and scheduled runs in `out/wally_trial_task.log`.
- `Task Scheduler wrong working directory`: verify scheduler command and `cd /d C:\dev\OSHA_Leads` usage in `run_wally_trial_daily.bat`, then inspect `out/wally_trial_task.log` and `out/run_log_*.txt`.
- `SPF/DKIM/DMARC alignment warnings`: verify sender/domain settings, then correlate delivery outcomes in `out/email_log.csv`; config issues surface as `CONFIG_ERROR ...` in logs/output.
