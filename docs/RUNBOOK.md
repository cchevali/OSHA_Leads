# RUNBOOK

## Canonical Contract

`AGENTS.md` at repo root is the canonical operator + Codex instruction contract.
Use this runbook for executable commands, but resolve policy conflicts in favor of `AGENTS.md`.

## AGENTS Workflow + Re-Upload Guidance

1. Update `AGENTS.md` first when process or instruction policy changes.
2. Keep ChatGPT Project Instructions as a thin wrapper that points to `AGENTS.md`.
3. Rebuild and upload `PROJECT_CONTEXT_PACK.md` after each contract change.
4. Upload only `PROJECT_CONTEXT_PACK.md` to ChatGPT Project Files because it embeds `AGENTS.md`, `docs/V1_CUSTOMER_VALIDATED.md`, and the repo context spine docs.

## Project Context Pack (ChatGPT Project Files)

Use one generated upload file to keep Project Files current:

```powershell
cd C:\dev\OSHA_Leads
py -3 tools/project_context_pack.py --build
py -3 tools/project_context_pack.py --check
py -3 tools/project_context_pack.py --mark-uploaded
```

`PROJECT_CONTEXT_PACK.md` is the only upload artifact and includes:
- `AGENTS.md`
- `docs/V1_CUSTOMER_VALIDATED.md`
- spine docs (`docs/PROJECT_BRIEF.md`, `docs/ARCHITECTURE.md`, `docs/DECISIONS.md`, `docs/RUNBOOK.md`, `docs/TODO.md`)

Project Files are injected by the platform into ChatGPT context during chats.
The assistant cannot browse ChatGPT Project Settings -> Files UI to verify what is uploaded.
Verification is repo-side only: confirm `PACK_HASH` in `PROJECT_CONTEXT_PACK.md` and the upload marker in `.local/project_upload_state.json` via `--mark-uploaded` and `--check`.

Operator flow:

1. Run `--build`.
2. Upload only `PROJECT_CONTEXT_PACK.md` in ChatGPT Project Settings -> Files (replace prior file).
3. Run `--mark-uploaded`.
4. Run strict `--check` at session start to fail fast on stale context.

## Mandatory Session-Start Preflight (Strict)

Start every session/day with strict context-pack check:

```powershell
cd C:\dev\OSHA_Leads
py -3 tools/project_context_pack.py --check
```

Strict `--check` is a blocker. Resolve any `ERR_CONTEXT_PACK_*` output before operator work.

Doctor behavior:

- `.\run_with_secrets.ps1` runs `tools/project_context_pack.py --check --soft` before invoking wrapped commands.
- `run_wally_trial.py --doctor` runs `tools/project_context_pack.py --check --soft`.
- `run_outreach_auto.py --doctor` runs `tools/project_context_pack.py --check --soft`.
- Soft checks are reminder-only: silent on success, and they print `WARN_CONTEXT_PACK_*` plus remediation instructions when action is required.
- Soft checks do not fail wrapper/doctor by themselves.

## Switch machines: laptop -> PC

Commands:

- git fetch origin
- git checkout main
- git pull --ff-only
- run_with_secrets.ps1 --diagnostics --check-decrypt
- py -3 -m unittest -q

## Verify Prefs Service (Internal)

- curl -sS -H "X-MFO-Internal-Key: <key>" "https://unsub.microflowops.com/api/prefs_state?subscriber_key=<sk>&territory_code=<terr>"
  - expected: 200 JSON with `lows_enabled` and `updated_at_iso`
- curl -sS -H "X-MFO-Internal-Key: <key>" "https://unsub.microflowops.com/prefs_state?subscriber_key=<sk>&territory_code=<terr>"
  - expected: 200 JSON with `lows_enabled` and `updated_at_iso`

## Laptop Outreach Preflight + Export (Copy/Paste)

Assumptions:

- You are in repo root.
- You will run the real export via secrets wrapper so `UNSUB_ENDPOINT_BASE` and `UNSUB_SECRET` are present.

Suppression file location (required for all exports):

- Default: `out/suppression.csv`
- If `DATA_DIR` is set: `${env:DATA_DIR}/suppression.csv`

### DATA_DIR Note (Path Resolution)

Why this exists: operators sometimes run pipelines with a shared data/output directory; `DATA_DIR` moves runtime artifacts out of the repo.

- If `DATA_DIR` is **unset**, outreach exports read suppression from `.\out\suppression.csv` (repo-relative).
- If `DATA_DIR` is **set**, outreach exports read suppression from `${env:DATA_DIR}\suppression.csv` (and **do not** fall back to `.\out\suppression.csv`).

Concrete example (Windows):

- If you set `$env:DATA_DIR = "C:\\mfo\\runtime"` then the suppression file must be at `C:\mfo\runtime\suppression.csv`.

PowerShell (turnkey):

```powershell
cd C:\dev\OSHA_Leads

# (Optional) verify secrets tooling + decrypt works on this laptop
.\run_with_secrets.ps1 --diagnostics --check-decrypt

# Ensure suppression file exists (required). Create with header if missing.
if (-not (Test-Path -LiteralPath .\out\suppression.csv)) {
  New-Item -Force -ItemType Directory .\out | Out-Null
  "email" | Set-Content -Encoding utf8 .\out\suppression.csv
  Write-Output "BOOTSTRAP: created out/suppression.csv"
}

# Preflight (no outputs written). Prints PASS/FAIL tokens and exits 0/1.
.\run_with_secrets.ps1 -- py -3 outreach\preflight_outreach.py

# Preview export (mailto fallback allowed; still enforces suppression.csv presence).
py -3 outreach\generate_mailmerge.py `
  --input outreach\sample_prospects.csv `
  --batch TX_W2 `
  --state TX `
  --out outreach\outbox_TX_W2_preview.csv `
  --allow-mailto-fallback

# Send exactly one test email from the preview outbox (hard-gated to OSHA_SMOKE_TO).
# Canonical: set OSHA_SMOKE_TO=cchevali+oshasmoke@gmail.com in .env.sops; all test-sends use this.
# Legacy aliases (only if OSHA_SMOKE_TO is unset): CHASE_EMAIL, OUTREACH_TEST_TO.
# Note: test-send prefers `html_body` when present and sends multipart/alternative (text + HTML) to match the cold outreach card style.
.\run_with_secrets.ps1 -- py -3 outreach\send_test_cold_email.py `
  --outbox outreach\outbox_TX_W2_preview.csv

# Optional: include a diagnostic preamble in the email body (prospect_id + links).
.\run_with_secrets.ps1 -- py -3 outreach\send_test_cold_email.py `
  --outbox outreach\outbox_TX_W2_preview.csv `
  --debug-header

# Real export (requires one-click env; uses secrets wrapper).
.\run_with_secrets.ps1 -- py -3 outreach\generate_mailmerge.py `
  --input outreach\sample_prospects.csv `
  --batch TX_W2 `
  --state TX `
  --out outreach\outbox_TX_W2.csv

# Verify artifacts exist
Test-Path -LiteralPath .\outreach\outbox_TX_W2_preview.csv
Test-Path -LiteralPath .\outreach\outbox_TX_W2_preview_manifest.csv
Test-Path -LiteralPath .\outreach\outbox_TX_W2.csv
Test-Path -LiteralPath .\outreach\outbox_TX_W2_manifest.csv
Test-Path -LiteralPath .\outreach\outreach_runs
```

Outputs:

- Outbox CSV: path from `--out`
- Manifest CSV: `<outbox_stem>_manifest.csv` alongside the outbox export
- Run log: `outreach/outreach_runs/<YYYY-MM-DD>_<batch>.jsonl`

Failure tokens (no partial outputs):

- `ERR_SUPPRESSION_REQUIRED suppression.csv missing ...`: suppression file missing (create `out/suppression.csv` with header `email`).
- `ERR_ONE_CLICK_REQUIRED ...`: missing/invalid one-click config (run via `.\run_with_secrets.ps1 ...` or set `UNSUB_ENDPOINT_BASE` + `UNSUB_SECRET`).

## Ongoing Outreach Cadence

### Weekly Batch Naming Convention

- Manual weekly waves: use `STATE_W<sequence>`. Examples: `TX_W2`, `TX_W3`, `CA_W1`.
- Automated daily runs: use `<YYYY-MM-DD>_<STATE>`. Example: `2026-02-11_TX`.
- Keep one folder per batch under `out/outreach/<batch>/`.

### Daily Auto-Run Paths (DATA_DIR-aware)

- Suppression list:
`<DATA_DIR>\suppression.csv` when `DATA_DIR` is set, else `.\out\suppression.csv`
- CRM database:
`<DATA_DIR>\crm.sqlite` when `DATA_DIR` is set, else `.\out\crm.sqlite`
- Duplicate-prevention ledger:
`<DATA_DIR>\outreach_export_ledger.jsonl` when `DATA_DIR` is set, else `.\out\outreach_export_ledger.jsonl`

### Canonical Outreach Env Setup (Only Supported Method)

Do not edit `.env.sops` manually (no Notepad/editor workflow) for outreach keys.
Use only:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\set_outreach_env.ps1 `
  -OutreachDailyLimit 10 `
  -OutreachStates TX `
  -OshaSmokeTo cchevali+oshasmoke@gmail.com `
  -OutreachSuppressionMaxAgeHours 240 `
  -TrialSendsLimitDefault 10 `
  -TrialExpiredBehaviorDefault notify_once
```

This script:

- Ensures `DATA_DIR`, `OSHA_SMOKE_TO`, `OUTREACH_STATES`, and `OUTREACH_DAILY_LIMIT` exist in `.env.sops`
- Ensures `OUTREACH_SUPPRESSION_MAX_AGE_HOURS` is set to `240` when missing (or to your explicit parameter value)
- Ensures trial defaults `TRIAL_SENDS_LIMIT_DEFAULT`, `TRIAL_EXPIRED_BEHAVIOR_DEFAULT`, and optional `TRIAL_CONVERSION_URL` are managed in the same no-editor flow
- Re-encrypts `.env.sops` on save
- Refuses to run when `.env.sops` is staged (`ERR_ENV_SOPS_STAGED`)
- Verifies with `.\run_with_secrets.ps1 -- py -3 run_outreach_auto.py --print-config`

Expect clear `ERR_*` tokens on missing/invalid key states; treat them as hard blockers before live sends.

Use `-OutreachDailyLimit 10` as a safe first-live default. Increase only after deliverability/ops checks.

### One-Time Seed (CSV -> CRM)

```powershell
cd C:\dev\OSHA_Leads
.\run_with_secrets.ps1 -- py -3 outreach\crm_admin.py seed `
  --input C:\path\to\prospects.csv
```

CSV seed is optional bootstrap/debug only. Ongoing intake should run discovery, not CSV imports.

### Prospect Discovery (Scheduled First)

Run discovery before outreach each day:

```powershell
cd C:\dev\OSHA_Leads
.\run_with_secrets.ps1 -- py -3 run_prospect_discovery.py --input C:\path\to\prospects.csv
```

Dry-run discovery:

```powershell
.\run_with_secrets.ps1 -- py -3 run_prospect_discovery.py --input C:\path\to\prospects.csv --dry-run
```

Print resolved discovery config:

```powershell
.\run_with_secrets.ps1 -- py -3 run_prospect_discovery.py --print-config --input C:\path\to\prospects.csv
```

### Single Command (Scheduled Daily)

```powershell
cd C:\dev\OSHA_Leads
.\run_with_secrets.ps1 -- py -3 run_outreach_auto.py
```

### Doctor/Dry-Run/Live Sequence (Canonical)

Run this in order each day:

```powershell
cd C:\dev\OSHA_Leads
.\run_with_secrets.ps1 -- py -3 run_outreach_auto.py --doctor
.\run_with_secrets.ps1 -- py -3 run_outreach_auto.py --dry-run
.\run_with_secrets.ps1 -- py -3 run_outreach_auto.py
```

The `--doctor` command must exit `0` with `PASS_DOCTOR_*` lines only before unattended sends. The dry-run command must complete successfully before live send.

Dry-run (no sends, writes outbox + manifest artifacts):

```powershell
.\run_with_secrets.ps1 -- py -3 run_outreach_auto.py --dry-run
```

Repo-root wrapper (equivalent command path):

```powershell
.\run_with_secrets.ps1 -- py -3 run_outreach_auto.py --dry-run
```

Print resolved paths/state:

```powershell
.\run_with_secrets.ps1 -- py -3 run_outreach_auto.py --print-config
```

Required outreach env keys (managed by `scripts\set_outreach_env.ps1`):

- `OUTREACH_STATES=TX`
- `OUTREACH_DAILY_LIMIT=10`
- `OSHA_SMOKE_TO=cchevali+oshasmoke@gmail.com`
- `OUTREACH_SUPPRESSION_MAX_AGE_HOURS=240`
- `DATA_DIR=out` (or your runtime path)

`run_outreach_auto.py` deterministically picks today's state from `OUTREACH_STATES` by weekday index and uses batch id `<YYYY-MM-DD>_<STATE>`.
Normal runs select and prioritize prospects directly from `crm.sqlite`, send outreach emails, then record `outreach_events` and status updates.

Expected artifacts:

- `out/crm.sqlite` (or `${DATA_DIR}\crm.sqlite`)
- `out/outreach_export_ledger.jsonl` (optional compatibility ledger)

### Outreach Ops Report (7/30-Day KPI Snapshot)

```powershell
cd C:\dev\OSHA_Leads
.\run_with_secrets.ps1 -- py -3 outreach\ops_report.py --print-config
.\run_with_secrets.ps1 -- py -3 outreach\ops_report.py --dry-run
.\run_with_secrets.ps1 -- py -3 outreach\ops_report.py --dry-run --no-write
.\run_with_secrets.ps1 -- py -3 outreach\ops_report.py
.\run_with_secrets.ps1 -- py -3 outreach\ops_report.py --format json
```

Artifact behavior:

- Default and `--dry-run` both write:
- `out\outreach\ops_reports\<YYYY-MM-DD>\ops_report_<HHMMSSZ>.json`
- `out\outreach\ops_reports\latest.json`
- `--no-write` suppresses all report file writes (including `latest.json`).

Default text stdout always ends with these three lines (in order):

- `OPS_REPORT_JSON_PATH=<path>` (or `(no-write)` when `--no-write` is set)
- `OPS_REPORT_SCHEMA_VERSION=v1`
- `OPS_REPORT_GENERATED_AT_UTC=<iso>`

`--format json` rule:

- Prints only the JSON object to stdout (no footer lines).
- Still writes artifacts unless `--no-write` is provided.

Metric scope:

- Last 7 and 30 days by `(batch_id, state_at_send)` with `sent`, `delivered_proxy`, `bounced_confirmed`, `bounced_inferred`, `replied`, `trial_started`, and `converted`.
- List quality snapshot: `new_prospects_count`, `% valid email format`, duplicate-domain rows/share, and role-based inbox share.

### QA Checks (Before/After Daily Send)

```powershell
# Verify CRM + suppression paths
.\run_with_secrets.ps1 -- py -3 run_outreach_auto.py --print-config

# Dry-run candidate preview
.\run_with_secrets.ps1 -- py -3 run_outreach_auto.py --dry-run

# Verify dry-run artifacts exist and no-send marker was printed
Test-Path -LiteralPath .\out\outreach\*\outbox_*_dry_run.csv
Test-Path -LiteralPath .\out\outreach\*\outbox_*_dry_run_manifest.csv

# Ledger exists and is appending
Test-Path -LiteralPath .\out\outreach_export_ledger.jsonl

# Optional debug export path (not needed for operations)
.\run_with_secrets.ps1 -- py -3 outreach\generate_mailmerge.py `
  --input outreach\sample_prospects.csv `
  --batch TX_DEBUG `
  --state TX `
  --out outreach\outbox_TX_DEBUG.csv `
  --allow-mailto-fallback
```

### Doctor Failure Tokens (Troubleshooting)

- `ERR_DOCTOR_SECRETS_DECRYPT`: run `.\run_with_secrets.ps1 --diagnostics --check-decrypt`; fix `sops/age` install or key setup.
- `ERR_DOCTOR_ENV_MISSING_*` / `ERR_DOCTOR_ENV_INVALID_*`: set outreach keys via `scripts\set_outreach_env.ps1` only.
- `ERR_DOCTOR_CRM_REQUIRED` / `ERR_DOCTOR_CRM_SCHEMA`: ensure `crm.sqlite` exists and includes required outreach tables (`crm_admin.py seed` if needed).
- `ERR_DOCTOR_SUPPRESSION_REQUIRED` / `ERR_DOCTOR_SUPPRESSION_UNREADABLE`: ensure suppression CSV exists and is readable at resolved `DATA_DIR`.
- `ERR_DOCTOR_SUPPRESSION_STALE`: refresh/update suppression file; optionally tune `OUTREACH_SUPPRESSION_MAX_AGE_HOURS`.
- `ERR_DOCTOR_UNSUB_CONFIG`: set `UNSUB_ENDPOINT_BASE` + `UNSUB_SECRET`.
- `ERR_DOCTOR_UNSUB_UNREACHABLE`: verify unsubscribe host/network reachability (`/__version` and `/unsubscribe`).
- `ERR_DOCTOR_PROVIDER_CONFIG`: set `SMTP_HOST`, `SMTP_PORT`, `SMTP_USER`, `SMTP_PASS`.
- `ERR_DOCTOR_DRY_RUN_ARTIFACT`: run `--dry-run` directly and inspect template/unsub/suppression configuration.
- `ERR_DOCTOR_IDEMPOTENCY`: inspect `outreach_events` for duplicate `sent` rows in the same batch window and fix repeat-contact state.

### Daily Suppression Update Loop

```powershell
cd C:\dev\OSHA_Leads

# Ensure suppression file + header exist
if (-not (Test-Path -LiteralPath .\out\suppression.csv)) {
  New-Item -Force -ItemType Directory .\out | Out-Null
  "email" | Set-Content -Encoding utf8 .\out\suppression.csv
}

# Append new suppressions (one email per line in .\out\new_suppressions.txt)
if (Test-Path -LiteralPath .\out\new_suppressions.txt) {
  Get-Content .\out\new_suppressions.txt |
    Where-Object { $_ -and $_.Contains("@") } |
    ForEach-Object { $_.Trim().ToLowerInvariant() } |
    ForEach-Object { Add-Content -Encoding utf8 .\out\suppression.csv $_ }
}

# De-duplicate suppression list (keep header)
$rows = Import-Csv .\out\suppression.csv | Where-Object { $_.email }
$rows | Group-Object { $_.email.ToLowerInvariant().Trim() } | ForEach-Object { $_.Group[0] } |
  Export-Csv -NoTypeInformation -Encoding utf8 .\out\suppression.csv
```

### Task Scheduler (PC)

Create/update daily tasks (discovery first, outreach second):

```powershell
schtasks /Create /F /SC DAILY /ST 07:30 /TN "OSHA_Prospect_Discovery" `
  /TR "powershell.exe -NoProfile -ExecutionPolicy Bypass -File C:\dev\OSHA_Leads\run_with_secrets.ps1 -- py -3 C:\dev\OSHA_Leads\run_prospect_discovery.py" `
  /RL HIGHEST
```

```powershell
schtasks /Create /F /SC DAILY /ST 08:00 /TN "OSHA_Outreach_Auto" `
  /TR "powershell.exe -NoProfile -ExecutionPolicy Bypass -File C:\dev\OSHA_Leads\run_with_secrets.ps1 -- py -3 C:\dev\OSHA_Leads\run_outreach_auto.py" `
  /RL HIGHEST
```

Deterministic installer (preferred):

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\install_scheduled_tasks.ps1 --print-config
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\install_scheduled_tasks.ps1 --dry-run
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\install_scheduled_tasks.ps1 --apply
```

### Minimal Daily Ops Checklist

1. Update `suppression.csv` with yesterday's unsubscribes/bounces.
2. Confirm discovery run populated/updated prospects in `crm.sqlite`.
3. Confirm auto summary email arrived at `OSHA_SMOKE_TO` with contacted/skipped/new-replies-trials-conversions.
4. Use `outreach\crm_admin.py mark` to record `replied`, `trial_started`, `converted`, or `do_not_contact`.

## Wally Trial Missed 9:00 AM Catch-Up (SAFE_MODE)

Wally trial daily sends support a trial-only catch-up window for post-reboot or logged-out morning misses.
This does not change strict SAFE_MODE window behavior for outreach or any non-trial sender.

Trial config keys (customer JSON):

- `trial_target_local_hhmm` default: `09:00`
- `trial_catchup_max_minutes` default: `180`

Operator workflow:

1. Print resolved trial catch-up config:
`.\run_with_secrets.ps1 -- py -3 run_wally_trial.py --print-config`
2. If the 9:00 AM run was missed, run the scheduled command path once during the same-morning catch-up window.
3. Verify logs include:
`SAFE_MODE_CATCHUP_ALLOWED gate=outside send window target=<...> now=<...> max_minutes=<...>`
4. Verify logs also include:
`SEND_START mode=LIVE`

Important:

- Catch-up is allowed only for the Wally trial daily path and only when the subscriber has not already been sent that local day.
- Do not temporarily widen `send_window_minutes` for missed trial sends; use the trial catch-up keys/workflow above.

## Trial Framework (Subscriber-Keyed)

Trial daily sends are now subscriber-keyed and backed by a minimal SQLite CRM-light registry plus an append-only send ledger.

Source of truth:

- Subscriber registry + trial latches: `out/crm_light.sqlite` (or `${env:DATA_DIR}\crm_light.sqlite` when `DATA_DIR` is set)
- Send ledger: `send_events` (counts successful sends where `status=SENT`)

### Add a Trial Participant (No Secrets Required)

```powershell
cd C:\dev\OSHA_Leads
py -3 run_trial_admin.py add-trial --subscriber-key test_sub --email test@example.com --territory TX_TRI --start-date 2026-02-04 --sends-limit 10
```

### Single-Command Dry-Run Verification (PowerShell)

```powershell
cd C:\dev\OSHA_Leads
py -3 run_trial_admin.py add-trial --subscriber-key test_sub --email test@example.com --territory TX_TRI --start-date 2026-02-04 --sends-limit 10; .\run_with_secrets.ps1 -- py -3 run_trial_daily.py --subscriber-key test_sub --test-send-daily --dry-run
```

Expected markers:

- `dry_run=YES`
- `TRIAL_EVENT status=DRY_RUN`
- `send_events` appended with `status=DRY_RUN` (does not count toward expiry)

### Expiry QA (Limit=1)

```powershell
cd C:\dev\OSHA_Leads
py -3 run_trial_admin.py add-trial --subscriber-key test_sub --email test@example.com --territory TX_TRI --start-date 2026-02-04 --sends-limit 1
```

Unit test covers the expiry behavior:
- When a single `SENT` exists at/after `start_date` and `sends_limit=1`, the next run must emit `SKIP_TRIAL_EXPIRED` and generate exactly one conversion artifact at `out\trials\<subscriber_key>\conversion_email.txt` (notify_once).

### Backfill a Historical Send Event

```powershell
cd C:\dev\OSHA_Leads
py -3 run_trial_admin.py append-event --subscriber-key wally_trial --status SENT --ts-utc 2026-02-04T15:00:00Z --variant DAILY --run-id backfill_20260204
```

Verify backfill impact:

```powershell
py -3 run_trial_admin.py show --subscriber-key wally_trial --recent 5
```

## Duplicate Lead Prevention (`lead_key` + `first_seen_at`)

Root cause of repeats:

- A lead could be re-observed on a later run and appear "new" again when selection/rendering used mutable observation timestamps.

Current invariant:

- Stable lead identity is `lead_key` (prefer source id; fallback deterministic composite hash).
- `first_seen_at` is set once on insert and treated as immutable.
- `last_seen_at` is updated on re-observation.
- Daily "newly observed" is selected from `first_seen_at` (daily windowing uses `first_seen_at > last_sent_at`).
- The digest "Observed" column reflects first observation time semantics.

## Runtime Migration and Indexing

At ingestion startup, runtime migration logic ensures identity/dedupe shape:

- `ALTER TABLE inspections ADD COLUMN lead_key TEXT` (if missing).
- Deterministic backfill of missing `lead_key`.
- `CREATE UNIQUE INDEX IF NOT EXISTS idx_inspections_lead_key ON inspections(lead_key)`.

Troubleshooting migration/index failures:

- Watch for `UNIQUE constraint failed: inspections.lead_key`.
- This means existing rows collide on `lead_key` and must be reconciled before unique indexing can succeed.
- Find duplicates:

```powershell
cd C:\dev\OSHA_Leads
@'
import sqlite3
conn = sqlite3.connect("data/osha.sqlite")
cur = conn.cursor()
cur.execute("""
SELECT lead_key, COUNT(*) c
FROM inspections
WHERE lead_key IS NOT NULL AND trim(lead_key) <> ''
GROUP BY lead_key
HAVING c > 1
ORDER BY c DESC, lead_key
""")
for key, c in cur.fetchall():
    print(c, key)
conn.close()
'@ | py -3 -
```

- Reconcile duplicates, then rerun ingestion/startup to reattempt index creation.

## Diagnostics Counters (JSONL + stdout)

Per run, diagnostics are emitted as:

- Stdout line: `RUN_DIAGNOSTICS ...`
- JSONL artifact: `out/run_diagnostics.jsonl` (append-only).

Counters:

- `ingested_total`: latest ingestion inserts + updates.
- `new_inserted`: newly inserted leads in latest ingestion.
- `existing_updated`: existing leads updated (re-observed).
- `selected_for_digest`: leads selected for the current digest after filters.
- `dedupe_dropped_due_to_first_seen_before_window`: leads excluded from current window because first seen was before the active first-seen cutoff.

Healthy back-to-back dry-run pattern:

- Run 1 may show non-zero `selected_for_digest`.
- Run 2 on the same unchanged data should show `selected_for_digest=0` for previously-seen leads.
- Both dry-runs should complete with no live send.

## Operator Validation (Windows PowerShell)

```powershell
cd C:\dev\OSHA_Leads

# 1) Unit tests
py -3 -m unittest -q

# 2) Daily dry-run #1 (no send)
.\run_with_secrets.ps1 -- py -3 run_wally_trial.py --test-send-daily --dry-run

# 3) Daily dry-run #2 (no send; verify no repeats)
.\run_with_secrets.ps1 -- py -3 run_wally_trial.py --test-send-daily --dry-run
```

Operator checks:

- Confirm each run prints a `RUN_DIAGNOSTICS` line.
- Confirm dry-run output indicates no live send.
- On the second run, previously observed leads should not be counted as newly observed.
