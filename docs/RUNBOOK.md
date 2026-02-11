# RUNBOOK

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
.\run_with_secrets.ps1 py -3 outreach\preflight_outreach.py

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
.\run_with_secrets.ps1 py -3 outreach\send_test_cold_email.py `
  --outbox outreach\outbox_TX_W2_preview.csv

# Optional: include a diagnostic preamble in the email body (prospect_id + links).
.\run_with_secrets.ps1 py -3 outreach\send_test_cold_email.py `
  --outbox outreach\outbox_TX_W2_preview.csv `
  --debug-header

# Real export (requires one-click env; uses secrets wrapper).
.\run_with_secrets.ps1 py -3 outreach\generate_mailmerge.py `
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
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\set_outreach_env.ps1 -OutreachDailyLimit 10
```

This script:

- Ensures `DATA_DIR`, `OSHA_SMOKE_TO`, `OUTREACH_STATES`, and `OUTREACH_DAILY_LIMIT` exist in `.env.sops`
- Re-encrypts `.env.sops` on save
- Refuses to run when `.env.sops` is staged (`ERR_ENV_SOPS_STAGED`)
- Verifies with `.\run_with_secrets.ps1 -- py -3 outreach\run_outreach_auto.py --print-config`

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
.\run_with_secrets.ps1 -- py -3 outreach\run_prospect_discovery.py
```

Dry-run discovery:

```powershell
.\run_with_secrets.ps1 -- py -3 outreach\run_prospect_discovery.py --dry-run
```

### Single Command (Scheduled Daily)

```powershell
cd C:\dev\OSHA_Leads
.\run_with_secrets.ps1 -- py -3 outreach\run_outreach_auto.py
```

Dry-run (no outputs, no summary email):

```powershell
.\run_with_secrets.ps1 -- py -3 outreach\run_outreach_auto.py --dry-run
```

Repo-root wrapper (equivalent command path):

```powershell
.\run_with_secrets.ps1 -- py -3 run_outreach_auto.py --dry-run
```

Print resolved paths/state:

```powershell
.\run_with_secrets.ps1 -- py -3 outreach\run_outreach_auto.py --print-config
```

Required outreach env keys (managed by `scripts\set_outreach_env.ps1`):

- `OUTREACH_STATES=TX,CA,FL`
- `OUTREACH_DAILY_LIMIT=200`
- `OSHA_SMOKE_TO=cchevali+oshasmoke@gmail.com`
- `DATA_DIR=out` (or your runtime path)

`run_outreach_auto.py` deterministically picks today's state from `OUTREACH_STATES` by weekday index and uses batch id `<YYYY-MM-DD>_<STATE>`.
Normal runs select and prioritize prospects directly from `crm.sqlite`, send outreach emails, then record `outreach_events` and status updates.

Expected artifacts:

- `out/crm.sqlite` (or `${DATA_DIR}\crm.sqlite`)
- `out/outreach_export_ledger.jsonl` (optional compatibility ledger)

### QA Checks (Before/After Daily Send)

```powershell
# Verify CRM + suppression paths
.\run_with_secrets.ps1 -- py -3 outreach\run_outreach_auto.py --print-config

# Dry-run candidate preview
.\run_with_secrets.ps1 -- py -3 outreach\run_outreach_auto.py --dry-run

# Dry-run integrity/smoke lines to confirm:
# - template_fingerprint=<sha256>
# - template_golden=PASS
# - fixture_smoke_would_contact_count=1
# - fixture_smoke=PASS

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
  /TR "powershell -NoProfile -ExecutionPolicy Bypass -Command \"cd C:\dev\OSHA_Leads; .\run_with_secrets.ps1 -- py -3 outreach\run_prospect_discovery.py\"" `
  /RL HIGHEST
```

```powershell
schtasks /Create /F /SC DAILY /ST 08:00 /TN "OSHA_Outreach_Auto" `
  /TR "powershell -NoProfile -ExecutionPolicy Bypass -Command \"cd C:\dev\OSHA_Leads; .\run_with_secrets.ps1 -- py -3 outreach\run_outreach_auto.py\"" `
  /RL HIGHEST
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
