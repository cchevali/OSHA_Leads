# Texas Triangle Daily Trial Status (Wally)

This document is the single source of truth for the current Texas Triangle daily trial setup (territory `TX_TRIANGLE_V1`, subscriber `wally_trial`).

## Operational Status

**Operational end-to-end: YES**, assuming the Windows Task Scheduler task exists on the operator machine and points to `run_wally_trial_daily.bat` exactly (see "Trigger Mechanism").

What the repo itself confirms:
- A production runner exists (`run_wally_trial_daily.bat`) and calls the live-send path.
- Territory filtering, content thresholds, dedupe, suppression checks, send idempotency, and run artifacts/logging are implemented.
- The SQLite DB (`data/osha.sqlite`) currently contains an enabled `wally_trial` subscriber with the intended recipients.

What the repo cannot confirm (machine state):
- Whether Task Scheduler is currently enabled/active on the operator machine and scheduled for the correct time.

## Trigger Mechanism and Timezone

**Trigger mechanism:** Windows Task Scheduler task (canonical task name: `OSHA Wally Trial Daily`) runs `run_wally_trial_daily.bat`.

**Timezone behavior:**
- Task Scheduler runs in the **host machine timezone**.
- The subscriber is configured to send at **08:00 America/Chicago** (Central Time) with a **60-minute send window**.
- If the operator PC is set to Eastern Time and you want an 08:00 CT delivery, schedule the task for **09:00 ET**.

**Safety rule:** Do NOT manually click Task Scheduler "Run" for this task. It will email recipients. Use `--preflight` to validate without sending.

## Scripts / Commands Used

### Scheduled runner (source of truth)

`run_wally_trial_daily.bat` runs this exact command:

```bat
python deliver_daily.py --db "data/osha.sqlite" --customer "%~dp0customers\wally_trial_tx_triangle_v1.json" --mode daily --since-days 14 --admin-email "support@microflowops.com" --send-live
```

Notes:
- `--send-live` is present in the scheduled action. This is why clicking "Run" emails recipients.
- The customer config `customers/wally_trial_tx_triangle_v1.json` is expected to exist locally and is intentionally untracked (real recipient emails).

### Scheduler setup/verification (operator tooling)

The repo also provides a canonical way to create/verify the Task Scheduler entry (recommended):

```powershell
python run_wally_trial.py wally_trial_tx_triangle_v1.json --enable-schedule
python run_wally_trial.py wally_trial_tx_triangle_v1.json --check-schedule
```

## Data Inputs

- SQLite DB: `data/osha.sqlite`
  - Primary lead table: `inspections` (includes `activity_nr`, `lead_score`, `first_seen_at`, `last_seen_at`, `date_opened`, `site_city`, `site_state`, optional `area_office`, etc.).
- Environment config: `.env` in repo root (SMTP + branding + optional unsubscribe endpoint config).
- Customer config (untracked): `customers/wally_trial_tx_triangle_v1.json`
  - Committed sanitized example: `customers/wally_trial_tx_triangle_v1.example.json`
- Public data ingestion: `deliver_daily.py` triggers ingestion via `ingest_osha.py` (OSHA/public sources), scoped to `states=["TX"]` and the configured lookback window (the scheduled runner uses `--since-days 14`).

## Territory Definition (Texas Triangle)

Territory code: `TX_TRIANGLE_V1` (defined in `territories.json` and upserted into the DB by `setup_wally_trial.py`).

Definition (current):
- States: `TX`
- Office patterns (preferred match against `area_office` / `office` / `osha_office`):
  - `austin`
  - `dallas`
  - `fort worth`
  - `dallas/fort worth`
  - `houston`
  - `san antonio`
- Fallback city patterns (used when office metadata is absent; checks `site_city`, `mail_city`, `site_address1`):
  - `austin`, `dallas`, `fort worth`
  - `houston` plus Houston-area suburbs (`pasadena`, `pearland`, `sugar land`, `the woodlands`, `katy`, `baytown`)
  - `san antonio`

## Scoring Thresholds and Content Filtering

Content filter modes:
- `high_medium`: keep leads with `lead_score >= 6`
- `high_only`: keep leads with `lead_score >= 10`

Email priority bands (for display):
- High: `lead_score >= 10`
- Medium: `6 <= lead_score < 10`
- Low: `< 6`

Low-score fallback behavior:
- Only when `content_filter == "high_medium"` AND the filtered lead list is empty AND `include_low_fallback == true`.
- Includes up to **5** low-priority items (`lead_score < 6`) as a fallback section.

## Dedupe / Idempotency

Record-level dedupe:
- Dedupe key: `activity_nr` (fallback `lead_id`).
- Chooses the "best" record when duplicates exist (highest score, newest timestamps).

Send-level idempotency:
- `send_digest_email.py` maintains a `send_log` table with a unique index on:
  - `(subscriber_key, mode, territory_code, territory_date, digest_hash)`
- If the same digest is attempted twice for the same day/territory/subscriber, the second attempt is skipped.

## Output Email Recipients (Current)

Current subscriber row in `data/osha.sqlite`:
- `subscriber_key`: `wally_trial`
- `territory_code`: `TX_TRIANGLE_V1`
- `send_time_local`: `08:00`
- `timezone`: `America/Chicago`
- `content_filter`: `high_medium`
- `include_low_fallback`: `true`
- `send_enabled`: `true`
- `recipients_json`:
  - `wgs@indigocompliance.com`
  - `brandon@indigoenergyservices.com`

## Logs and Artifacts

Scheduled-run wrapper log:
- `out/wally_trial_task.log` (includes header + full stdout/stderr from each run)
- `out/wally_trial_last_run.log` (the most recent run stdout/stderr capture before it is appended to the task log)

Per-day run log:
- `out/run_log_YYYY-MM-DD.txt`

Per-run artifacts:
- `out/runs/<run_id>/preflight_result.json`
- `out/runs/<run_id>/send_result.json`
- `out/latest.json` (pointer to the latest run)

Email/suppression/unsubscribe append-only logs:
- `out/email_log.csv`
- `out/suppression_log.csv`
- `out/unsubscribe_events.csv`

Trial counts output (when using `run_wally_trial.py` workflow modes):
- `out/wally_trial_daily_counts_<YYYY-MM-DD>.csv`

## YES Reply Onboarding (Email-Only Provisioning)

Provision a new subscriber (no manual DB edits) from a prospect's copy/paste reply block:

```powershell
# 1) Preflight (parse/validate only; no writes)
python onboard_subscriber.py --db data/osha.sqlite --preflight --reply-block-file out\\yes_reply.txt

# Option A: from a file containing the KEY=VALUE block
python onboard_subscriber.py --db data/osha.sqlite --dry-run --reply-block-file out\\yes_reply.txt

# 2) Delivery preflight (validates gating + SMTP env when --send-live is included)
python deliver_daily.py --db data/osha.sqlite --customer customers\\<subscriber_key>.json --mode daily --preflight --send-live

# 3) Delivery dry-run (renders digest, prints tier counts/recipients/sample leads; no emails sent)
python deliver_daily.py --db data/osha.sqlite --customer customers\\<subscriber_key>.json --mode daily --dry-run --skip-ingest

# 4) Live send (within the configured send window)
python deliver_daily.py --db data/osha.sqlite --customer customers\\<subscriber_key>.json --mode daily --send-live
```

What it does:
- Validates `TIMEZONE` and `SEND_TIME_LOCAL`.
- Validates the territory tag/code exists (and upserts it into `territories` for FK integrity).
- Upserts the subscriber into `subscribers` with `send_enabled=1` and recipient fanout.
- Writes an untracked customer config to `customers/<subscriber_key>.json` for use with `deliver_daily.py`.
- Sends a confirmation email to the configured recipient(s).

Onboarding audit log:
- `out/onboarding_audit_log.csv`

Operator artifacts (recommended):
- `logs/YYYY-MM-DD/onboard_subscriber_<subscriber_key>_{preflight|dry_run|live}.json`
- `logs/YYYY-MM-DD/deliver_daily_<subscriber_key>_<mode>_{preflight|dry_run}.json`

## Suppression / Unsubscribe Enforcement

Suppression enforcement:
- Before sending each recipient email, the pipeline checks `suppression_list` in `data/osha.sqlite` for:
  - exact email match (lowercased)
  - domain match (lowercased, e.g. `example.com`)
- If suppressed, the message is not sent and the suppression action is logged (CSV + run artifacts).

Unsubscribe events:
- Unsubscribe/suppression events are recorded to the DB (`unsubscribe_events`) and appended to `out/unsubscribe_events.csv`.
- One-click unsubscribe is supported when `UNSUB_ENDPOINT_BASE` (+ `UNSUB_SECRET`) are configured; otherwise, unsubscribe is handled by email reply and then enforced via `suppression_list`.

## Quick Operator Verification

Verify scheduler wiring (no changes):

```powershell
python run_wally_trial.py wally_trial_tx_triangle_v1.json --check-schedule
```

Verify preflight without sending:

```powershell
python run_wally_trial.py wally_trial_tx_triangle_v1.json --preflight-only
```

After a scheduled run, verify these artifacts exist/updated:
- `out/wally_trial_task.log`
- `out/run_log_YYYY-MM-DD.txt`
- `out/email_log.csv` (2 rows when live send succeeds and neither recipient is suppressed)
