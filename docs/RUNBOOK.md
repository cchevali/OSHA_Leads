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
- Prospects input:
`OUTREACH_PROSPECTS_PATH` (required for production PC automation; local file)
- Outputs:
`OUTREACH_OUTPUT_ROOT\<YYYY-MM-DD_STATE>\outbox_<YYYY-MM-DD_STATE>.csv` and manifest/run log beside it
Default `OUTREACH_OUTPUT_ROOT` is `.\out\outreach`
- Duplicate-prevention ledger:
`<DATA_DIR>\outreach_export_ledger.jsonl` when `DATA_DIR` is set, else `.\out\outreach_export_ledger.jsonl`

### One-Command Batch Run

```powershell
cd C:\dev\OSHA_Leads
.\run_with_secrets.ps1 -- py -3 outreach\run_outreach_batch.py `
  --state TX `
  --batch TX_W2 `
  --input outreach\sample_prospects.csv
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

Recommended env on PC (set in `.env.sops`):

- `OUTREACH_STATES=TX,CA,FL`
- `OUTREACH_DAILY_LIMIT=200`
- `OUTREACH_PROSPECTS_PATH=C:\path\to\prospects.csv`
- `OUTREACH_OUTPUT_ROOT=C:\dev\OSHA_Leads\out\outreach`
- `OSHA_SMOKE_TO=cchevali+oshasmoke@gmail.com`

`run_outreach_auto.py` deterministically picks today's state from `OUTREACH_STATES` by weekday index and uses batch id `<YYYY-MM-DD>_<STATE>`.
If the day's outbox+manifest already exist, it exits cleanly with `PASS_AUTO_ALREADY_RAN`.

Expected artifacts:

- `out/outreach/TX_W2/outbox_TX_W2.csv`
- `out/outreach/TX_W2/outbox_TX_W2_manifest.csv`
- `out/outreach/TX_W2/run_log.jsonl`

### QA Checks (Before External Send)

```powershell
# Verify files exist
Test-Path -LiteralPath .\out\outreach\TX_W2\outbox_TX_W2.csv
Test-Path -LiteralPath .\out\outreach\TX_W2\outbox_TX_W2_manifest.csv
Test-Path -LiteralPath .\out\outreach\TX_W2\run_log.jsonl

# Quick counts
(Import-Csv .\out\outreach\TX_W2\outbox_TX_W2.csv).Count
(Import-Csv .\out\outreach\TX_W2\outbox_TX_W2_manifest.csv | Where-Object { $_.status -eq 'dropped' }).Count

# One-click links present in generated outbox
(Import-Csv .\out\outreach\TX_W2\outbox_TX_W2.csv | Select-Object -First 1).unsubscribe_url
(Import-Csv .\out\outreach\TX_W2\outbox_TX_W2.csv | Select-Object -First 1).prefs_url

# Ledger exists and is appending
Test-Path -LiteralPath .\out\outreach_export_ledger.jsonl

# Smoke test exactly one rendered outreach email
.\run_with_secrets.ps1 -- py -3 outreach\send_test_cold_email.py `
  --outbox .\out\outreach\TX_W2\outbox_TX_W2.csv
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

Create/update daily task (example runs at 08:00 local time):

```powershell
schtasks /Create /F /SC DAILY /ST 08:00 /TN "OSHA_Outreach_Auto" `
  /TR "powershell -NoProfile -ExecutionPolicy Bypass -Command \"cd C:\dev\OSHA_Leads; .\run_with_secrets.ps1 -- py -3 outreach\run_outreach_auto.py\"" `
  /RL HIGHEST
```

### Minimal Daily Ops Checklist

1. Update `suppression.csv` with yesterday's unsubscribes/bounces.
2. Confirm auto summary email arrived at `OSHA_SMOKE_TO` with exported/dropped counts.
3. QA outbox + manifest quickly, then upload outbox to your external mail-merge sender.
