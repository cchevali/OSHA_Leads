# Wally Trial Runbook

Do NOT click Task Scheduler "Run" - it will email Wally. Use `--preflight` for checks.

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

## 7) Enable 9:00 AM Schedule (ET)

```powershell
python run_wally_trial.py wally_trial_tx_triangle_v1.json --enable-schedule
```

Task name: `OSHA Wally Trial Daily`

Note: Windows Task Scheduler runs in host timezone. For an 8:00 AM CT delivery on a PC set to Eastern Time, schedule the task for 9:00 AM ET.
Note: Task Scheduler runs on the local PC. The machine must be on at the scheduled time (9:00 AM ET for Texas Triangle).



## Go-Live Minimum (Trial Scope Freeze)

Minimum safeguards required for the Wally trial:
- Preflight must pass (DB connectivity, subscriber gating, required send envs).
- Suppression/unsubscribe enforcement must be active (suppression list accessible).
- Idempotency guard enabled to prevent same-day duplicate sends (`send_log`).
- Single operator/admin failure alert on any preflight failure or send exception.
- Scheduler set to **9:00 AM ET** (8:00 AM CT) and PC is on at run time.

Anything beyond the above is optional/post-trial.

## Preflight (Scheduler Safety)

Run this once before enabling the schedule to ensure DB connectivity and live-send gating:

```powershell
python deliver_daily.py --preflight --db "C:\dev\OSHA_Leads\osha_leads.db" --customer "C:\dev\OSHA_Leads\customers\wally_trial_tx_triangle_v1.json" --mode daily --send-live
```

Expected output:
- `[PREFLIGHT_OK] DB connectivity, subscriber gating, and recipients validated`
- No `PREFLIGHT_ERROR` lines

Latest preflight output: [PREFLIGHT_OK] DB connectivity, subscriber gating, and recipients validated


## Run Artifacts

Each run writes artifacts to:
- `out/runs/<run_id>/preflight_result.json`
- `out/runs/<run_id>/send_result.json`
- `out/latest.json` (points to most recent run)

Digest hash definition (for idempotent send guard):
- SHA256 of JSON payload with sorted lead identifiers plus: `mode`, `territory_code`, `content_filter`, `include_low_fallback`.

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

Last verification: 2026-02-05 | python -m unittest -v (OK)

## Optional / Post-Trial

- Territory health diagnostics (admin-only).
- Extended analytics or dashboards.
- Additional gating or volume controls beyond the minimum safeguards above.

