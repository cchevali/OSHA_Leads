# RUNBOOK

## Canonical Contract

`AGENTS.md` at repo root is the canonical operator + Codex instruction contract.
Use this runbook for executable commands, but resolve policy conflicts in favor of `AGENTS.md`.

## WIP Autosave Discipline

Rules:

- Start-of-session: if the working tree is dirty, run `.\scripts\autosave_wip.ps1` before any scoped task work (or rely on scheduled autosave).
- Treat WIP branches as the safety net for drift so "only intended changes" remains enforceable.
- All nontrivial work happens on a task branch; `main` stays clean.

Manual autosave command:

```powershell
cd C:\dev\OSHA_Leads
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\autosave_wip.ps1
```

Install scheduled autosave tasks (logon + every 15 minutes):

```powershell
cd C:\dev\OSHA_Leads
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\install_wip_autosave_task.ps1 --print-config
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\install_wip_autosave_task.ps1 --dry-run
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\install_wip_autosave_task.ps1 --apply
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\install_wip_autosave_task.ps1 --status
```

Status output contract:

- `WIP_AUTOSAVE_HOURLY_INSTALLED=0|1`
- `WIP_AUTOSAVE_LOGON_INSTALLED=0|1`
- `WIP_AUTOSAVE_EFFECTIVE=0|1`
- `WIP_AUTOSAVE_MODE=WORKTREE`
- `WIP_AUTOSAVE_NEXT_ACTION=<none|run_elevated_cmd>`

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
py -3 tools/project_context_pack.py --fingerprint
# Upload PROJECT_CONTEXT_PACK.md to ChatGPT Project Settings -> Files (replace prior)
py -3 tools/project_context_pack.py --mark-uploaded
py -3 tools/project_context_pack.py --check
```

Automation/test-only build output override:

```powershell
py -3 tools/project_context_pack.py --build --output C:\temp\PROJECT_CONTEXT_PACK.md
```

Use `--output` for tests/automation to avoid mutating repo-root `PROJECT_CONTEXT_PACK.md`. Operator flow remains the default build-to-repo-root command above.

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

## Deliverability Preflight (DNS + Header Proof)

Run after any DNS, SMTP, or sender-identity change. No behavior change; verification only.

### DNS Record Checks

```powershell
# SPF — must include your SMTP provider
nslookup -type=TXT microflowops.com 8.8.8.8
# Expect: v=spf1 include:zoho.com ~all (or equivalent)

# DMARC — policy + reporting
nslookup -type=TXT _dmarc.microflowops.com 8.8.8.8
# Expect: v=DMARC1; p=none; rua=mailto:... (or p=quarantine/reject)

# DKIM — Zoho selector public key
nslookup -type=TXT zoho._domainkey.microflowops.com 8.8.8.8
# Expect: v=DKIM1; k=rsa; p=<public_key>
```

### Header Fields to Confirm (on a received test email)

Open a test email in Gmail → "Show original" (or equivalent) and verify:

| Header | Expected value |
|---|---|
| `Authentication-Results` | `spf=pass`, `dkim=pass`, `dmarc=pass` |
| `Received-SPF` | `pass` with `microflowops.com` |
| `DKIM-Signature` | `d=microflowops.com` |
| `From` / `Return-Path` | Both use `@microflowops.com` (domain alignment) |

If any field shows `fail` or `none`, re-check the DNS records above and the SMTP provider's domain verification panel before sending live outreach.

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
  -OutreachStates TX,CA,FL `
  -OshaSmokeTo cchevali+oshasmoke@gmail.com `
  -OutreachSuppressionMaxAgeHours 240 `
  -ProspectAutoGrowEnabled 1 `
  -ProspectAutoGrowSafetyNetEnabled 1 `
  -ProspectAutoGrowSources AIHA `
  -ProspectAutoGrowBacklogTarget 60 `
  -ProspectAutoGrowMaxFetchPagesPerRun 6 `
  -ProspectAutoGrowHttpSleepMs 800 `
  -TrialSendsLimitDefault 14 `
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

### Prospect Generation (Scheduled First)

Run canonical prospect generation first each day. This writes the discovery feed CSV that discovery imports into CRM:

```powershell
cd C:\dev\OSHA_Leads
.\run_with_secrets.ps1 -- py -3 run_prospect_generation.py
```

No-arg generation output path:

- `${DATA_DIR}\prospect_discovery\prospects_latest.csv`
- If `DATA_DIR` is unset: `.\out\prospect_discovery\prospects_latest.csv`
- Canonical optional inbox path: `${DATA_DIR}\prospect_generation\inbox\*.csv`
- If `DATA_DIR` is unset: `.\out\prospect_generation\inbox\*.csv`
- One-release compatibility path (deprecated): `${DATA_DIR}\prospect_discovery\inbox\*.csv`

Drop-folder behavior:

- Generator scans canonical inbox first, then deprecated inbox, each in deterministic filename order.
- Inbox rows are merged first, then seeded pool rows; inbox wins on duplicate email.
- On live runs, processed inbox files are moved to `<inbox_dir>\processed\YYYY-MM-DD\`.
- On `--dry-run`, inbox files are never moved.
- If deprecated inbox files are used, generator emits `WARN_INBOX_PATH_DEPRECATED`.

Auto-growth (env-gated, optional):

- Canonical keys (no aliases): `PROSPECT_AUTOGROW_ENABLED`, `PROSPECT_AUTOGROW_SAFETY_NET_ENABLED`, `PROSPECT_AUTOGROW_SOURCES`, `PROSPECT_AUTOGROW_BACKLOG_TARGET`, `PROSPECT_AUTOGROW_MAX_FETCH_PAGES_PER_RUN`, `PROSPECT_AUTOGROW_HTTP_SLEEP_MS`.
- Source scope v1: `AIHA` only.
- Cache path: `${DATA_DIR}\prospect_generation\cache\aiha\state_<STATE>.json`.
- Diagnostics path: `${DATA_DIR}\prospect_generation\diagnostics\...`.
- Backlog targeting is evaluated per configured state in `OUTREACH_STATES`.
- Safety net default (`PROSPECT_AUTOGROW_SAFETY_NET_ENABLED=1`): when `PROSPECT_AUTOGROW_ENABLED=0` and a configured state has a depleted CRM pool (`backlog_current=0` with existing pool rows), generator auto-forces AIHA autogrow for that depleted state.
- `--for-date YYYY-MM-DD` controls `selected_state` plus per-state backlog/new-needed previews in `--print-config` and `--dry-run`.

Dry-run generation (no writes):

```powershell
.\run_with_secrets.ps1 -- py -3 run_prospect_generation.py --dry-run
```

Print resolved generation config:

```powershell
.\run_with_secrets.ps1 -- py -3 run_prospect_generation.py --print-config
.\\run_with_secrets.ps1 -- py -3 run_prospect_generation.py --print-config --for-date 2026-02-18
```

Generator emits machine-readable lines:

- `GENERATOR_OUTPUT_PATH`
- `GENERATOR_ROWS_READ`
- `GENERATOR_ROWS_WRITTEN`
- `GENERATOR_INBOX_DIR`
- `GENERATOR_INBOX_FILES_FOUND`
- `GENERATOR_INBOX_ROWS_READ`
- `GENERATOR_INBOX_ROWS_ACCEPTED`
- `GENERATOR_INBOX_ROWS_MISSING_STATE`
- `GENERATOR_INBOX_FILES_ARCHIVED` (live runs only)
- `GENERATOR_AUTOGROW_*`
- `GENERATOR_AUTOGROW_SAFETY_NET_FORCED`, `GENERATOR_AUTOGROW_SAFETY_NET_STATES`
- `GENERATOR_AUTOGROW_TOTAL_STATES`, `GENERATOR_AUTOGROW_TOTAL_ACCEPTED`
- `GENERATOR_AUTOGROW_STATE=<STATE> backlog_current=<n> new_needed=<n> aiha_candidate=<n> aiha_accepted=<n>`
- `GENERATOR_AIHA_*`
- `GENERATOR_DIAGNOSTICS_PATH` (when generated)
- `GENERATOR_COMPLETE status=<OK|DRY_RUN>`

Artifact separation (do not mix these):

- Discovery feed CSV: `${DATA_DIR}\prospect_discovery\prospects_latest.csv`
- Send-time suppression list: `${DATA_DIR}\suppression.csv` (or `.\out\suppression.csv`)
- Optional campaign tracking logs: `out\campaign_tracking\...`

### Prospect Discovery (Scheduled First)

Run discovery before outreach each day:

```powershell
cd C:\dev\OSHA_Leads
.\run_with_secrets.ps1 -- py -3 run_prospect_discovery.py
```

No-arg discovery input resolution order:

1. `PROSPECT_DISCOVERY_INPUT` (preferred)
2. `DISCOVERY_INPUT_CSV` (legacy compatibility)
3. `${DATA_DIR}\prospect_discovery\prospects_latest.csv`
4. `${DATA_DIR}\prospect_discovery\prospects.csv`
5. `${DATA_DIR}\prospects_latest.csv`
6. `${DATA_DIR}\prospects.csv`

When `DATA_DIR` is unset, discovery resolves these fallback paths under repo `.\out\...`.

Set preferred discovery input via the canonical no-editor env helper:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\set_outreach_env.ps1 `
  -OutreachDailyLimit 10 `
  -OutreachStates TX,CA,FL `
  -OshaSmokeTo cchevali+oshasmoke@gmail.com `
  -OutreachSuppressionMaxAgeHours 240 `
  -TrialSendsLimitDefault 14 `
  -TrialExpiredBehaviorDefault notify_once `
  -ProspectDiscoveryInput C:\path\to\prospects.csv
```

Dry-run discovery:

```powershell
.\run_with_secrets.ps1 -- py -3 run_prospect_discovery.py --input C:\path\to\prospects.csv --dry-run
```

Print resolved discovery config:

```powershell
.\run_with_secrets.ps1 -- py -3 run_prospect_discovery.py --print-config --input C:\path\to\prospects.csv
```

Discovery emits a fixed-order `DISCOVERY_*` diagnostics block for operator parsing:

- `DISCOVERY_INPUT_PATH`
- `DISCOVERY_CRM_DB`
- `DISCOVERY_ROWS_READ`
- `DISCOVERY_PROSPECTS_UPSERTED`
- `DISCOVERY_SKIPPED_INVALID_EMAIL`
- `DISCOVERY_SKIPPED_DUPLICATE_EMAIL`
- `DISCOVERY_COMPLETE status=<OK|NO_INPUT|DRY_RUN>`

Legacy `PASS_DISCOVERY_*` / `ERR_DISCOVERY_*` tokens remain supported for compatibility.

Canonical daily sequence:

1. `.\run_with_secrets.ps1 -- py -3 run_osha_ingest_daily.py`
2. `.\run_with_secrets.ps1 -- py -3 run_prospect_generation.py`
3. `.\run_with_secrets.ps1 -- py -3 run_prospect_discovery.py`
4. `.\run_with_secrets.ps1 -- py -3 run_outreach_auto.py --plan --for-date YYYY-MM-DD` (or dry-run/live send flow)

Context pack hygiene (when docs/contracts changed or WARN_CONTEXT_PACK_STALE appears):

1. `py -3 tools/project_context_pack.py --build`
2. Upload `PROJECT_CONTEXT_PACK.md` in ChatGPT Project Settings -> Files
3. `py -3 tools/project_context_pack.py --mark-uploaded`
4. `py -3 tools/project_context_pack.py --check`

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

The `--doctor` command must exit `0` before unattended sends. Expected output includes `PASS_DOCTOR_*` plus signal freshness diagnostics (`DOCTOR_SIGNALS_*`, and optional `WARN_SIGNALS_STALE` when data is stale/missing). The dry-run command must complete successfully before live send.

Tomorrow confirmation (canonical no-send deterministic check):

```powershell
.\run_with_secrets.ps1 -- py -3 run_outreach_auto.py --plan --for-date 2026-02-14
```

`--plan` stdout contract includes:

- `OUTREACH_PLAN_POOL_TOTAL=<n>` (alias of selected-state pool before skip filters)
- `OUTREACH_PLAN_POOL_TOTAL_ALL_STATES=<n>`
- `OUTREACH_PLAN_POOL_TOTAL_SELECTED_STATE=<n>`
- `OUTREACH_PLAN_FILTER_BREAKDOWN=<minified_json>`
- `OUTREACH_PLAN_DIAGNOSTICS_PATH=<absolute_path>`

When `OUTREACH_PLAN_WILL_SEND=0`, root-cause must be interpreted from `OUTREACH_PLAN_POOL_TOTAL*`, `OUTREACH_PLAN_FILTER_BREAKDOWN`, and `OUTREACH_PLAN_DIAGNOSTICS_PATH` (instead of relying on skip totals alone).

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

- `OUTREACH_STATES=TX,CA,FL`
- `OUTREACH_DAILY_LIMIT=10`
- `OSHA_SMOKE_TO=cchevali+oshasmoke@gmail.com`
- `OUTREACH_SUPPRESSION_MAX_AGE_HOURS=240`
- `DATA_DIR=out` (or your runtime path)

`run_outreach_auto.py` deterministically picks today's state from `OUTREACH_STATES` by weekday index and uses batch id `<YYYY-MM-DD>_<STATE>`.
`--for-date YYYY-MM-DD` is allowed with `--print-config`, `--doctor`, `--dry-run`, and `--plan`.
If `--for-date` is not today and a live send is attempted, the command hard-fails with `ERR_AUTO_FOR_DATE_LIVE_SEND_BLOCKED` and no partial send effects.
Normal runs select and prioritize prospects directly from `crm.sqlite`, send outreach emails, then record `outreach_events` and status updates.

Expected artifacts:

- `out/crm.sqlite` (or `${DATA_DIR}\crm.sqlite`)
- `out/outreach_export_ledger.jsonl` (optional compatibility ledger)
- `out\outreach\<batch>\outbox_<batch>_dry_run.csv`
- `out\outreach\<batch>\outbox_<batch>_dry_run_manifest.csv` (includes `domain`, `segment`, `role_or_title`, `state_pref`, and `rank_reason` audit fields)
- `out\outreach\<batch>\plan_diagnostics.json` (run-level plan/dry-run diagnostics including pool totals and filter breakdown)

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
# Manifest rows include rank audit fields and dropped reasons for QA traceability.

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

### Bounce Import (IMAP Member Mailbox)

Use this importer to ingest DSN bounces (including Zoho moderation notifications) from the mailbox
that actually receives the moderation notices.

```powershell
cd C:\dev\OSHA_Leads
.\run_with_secrets.ps1 -- py -3 outreach\import_bounces_imap.py --print-config
.\run_with_secrets.ps1 -- py -3 outreach\import_bounces_imap.py --dry-run
.\run_with_secrets.ps1 -- py -3 outreach\import_bounces_imap.py
```

Notes:

- `--print-config` is side-effect free and can run without IMAP secrets.
- Real IMAP runs (`--dry-run` and live apply) should be executed via `.\run_with_secrets.ps1`.

Environment keys:

- `BOUNCE_IMAP_HOST` (default `imappro.zoho.com`)
- `BOUNCE_IMAP_PORT` (default `993`)
- `BOUNCE_IMAP_USER` (default `cchevali@zohomail.com`)
- `BOUNCE_IMAP_PASS` (required; falls back to `IMAP_PASS`)
- `BOUNCE_IMAP_FOLDER` (default `INBOX`)

Idempotency and state behavior:

- Uses a DATA_DIR-aware state file:
  `${DATA_DIR}\bounce_import_state.json` (or `.\out\bounce_import_state.json` when `DATA_DIR` is unset)
- Uses a lock file:
  `${DATA_DIR}\bounce_import.lock` (or `.\out\bounce_import.lock`)
- Does **not** mutate IMAP flags/folders (`Seen`/move is not used).
- Emits `BOUNCE_IMPORT_MODERATION_NOTICE_SEEN=1` when a moderation notice is parsed as a hard bounce.

### Task Scheduler (PC)

Create/update daily tasks (OSHA ingest first, then generation, discovery, outreach).
Operational expectation is America/New_York local time:

```powershell
schtasks /Create /F /SC DAILY /ST 06:45 /TN "OSHA_Osha_Ingest_Daily" `
  /TR "powershell.exe -NoProfile -ExecutionPolicy Bypass -File C:\dev\OSHA_Leads\scripts\scheduled\run_osha_ingest_daily.ps1" `
  /RL HIGHEST
```

```powershell
schtasks /Create /F /SC DAILY /ST 07:15 /TN "OSHA_Prospect_Generation" `
  /TR "powershell.exe -NoProfile -ExecutionPolicy Bypass -File C:\dev\OSHA_Leads\scripts\scheduled\run_prospect_generation.ps1" `
  /RL HIGHEST
```

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

### Recent Signals Troubleshooting

If cold outreach renders `(no recent signals found)`, outreach is working but `data\osha.sqlite` has no records in the state/last-14-day window used by `outreach\generate_mailmerge.py`.

One-time Florida catch-up:

```powershell
cd C:\dev\OSHA_Leads
py -3 ingest_osha.py --db data\osha.sqlite --since-days 45 --states FL
```

Confirm Florida freshness:

```powershell
@'
import sqlite3
conn = sqlite3.connect("data/osha.sqlite")
cur = conn.cursor()
cur.execute("SELECT MAX(date_opened) FROM inspections WHERE site_state='FL'")
print(cur.fetchone()[0])
conn.close()
'@ | py -3 -
```

Confirm ongoing automation:

```powershell
schtasks.exe /Query /TN \OSHA_Osha_Ingest_Daily /V /FO LIST
.\run_with_secrets.ps1 -- py -3 run_osha_ingest_daily.py --print-config
```

### Minimal Daily Ops Checklist

1. Update `suppression.csv` with yesterday's unsubscribes/bounces.
2. Confirm generation run produced `${DATA_DIR}\prospect_discovery\prospects_latest.csv` (or `.\out\prospect_discovery\prospects_latest.csv`).
3. Confirm discovery run populated/updated prospects in `crm.sqlite`.
4. Confirm auto summary email arrived at `OSHA_SMOKE_TO` with contacted/skipped/new-replies-trials-conversions.
5. Use `outreach\crm_admin.py mark` to record `replied`, `trial_started`, `converted`, or `do_not_contact`.

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
Trial policy is 14 weekday sends (Mon-Fri); send-limit is the trial target and weekend/holiday skips extend calendar duration naturally.

Source of truth:

- Subscriber registry + trial latches: `out/crm_light.sqlite` (or `${env:DATA_DIR}\crm_light.sqlite` when `DATA_DIR` is set)
- Send ledger: `send_events` (`TRIAL_SENDS_USED` counts distinct subscriber-local weekday dates for `status=SENT` daily LIVE events to the primary recipient; raw `status=SENT` row count remains telemetry via `TRIAL_EXPIRED_BY_SENDS`)
- Wally scheduled live runs now mirror successful sends into `send_events` automatically (best-effort, no send-path change)

Check trial days-since-start and sends-used (single command, no sends/no writes):

```powershell
cd C:\dev\OSHA_Leads
py -3 run_wally_trial.py --status
```

Preview the exact conversion text before expiry (writes draft artifact only; no sends, no DB writes):

```powershell
cd C:\dev\OSHA_Leads
py -3 run_trial_admin.py conversion-draft --subscriber-key wally_trial
```

The system auto-generates the conversion draft on the first scheduled run after the trial reaches the send target.
On live trial runs (`--send-live`, not `--dry-run`), that same first expired run also auto-sends the conversion email once and latches `notified_at_utc`.
Non-live or dry-run expiry checks keep the draft pending and do not consume the conversion latch.
Auto-send is hard-gated: if the draft still contains an unresolved Stripe placeholder (for example `{stripe_link}` or `<stripe_link>`), send is blocked (`ERR_CONVERSION_LINK_MISSING`) and `notified_at_utc` is not set.
If `conversion_email.txt` already exists, it is treated as review-locked and sent as-is (operator edits are preserved).

Status field note: `TRIAL_14_DAY_ELAPSED` is a compatibility key and now means "14 successful sends elapsed" (not calendar days).

One-time historical backfill for prior successful Wally scheduled runs from `out\wally_trial_task.log`:

```powershell
cd C:\dev\OSHA_Leads
py -3 backfill_wally_trial_send_events.py
```

Territory definition and deterministic audit commands:

```powershell
cd C:\dev\OSHA_Leads
py -3 tools\print_territory.py --code TX_TRI
py -3 run_wally_trial.py --audit --check-inspection 1874533.015
```

Notes:
- Canonical territory code is `TX_TRI` (`kind=CBSA_SET`, CBSAs `19100,26420,41700,12420`).
- Legacy aliases remain accepted and resolve to the same canonical matcher: `TX_TRIANGLE_V1`, `TX_TRIANGLE`, `TX_TRI_V1`.

Deterministic ZIP->CBSA rebuild command (from HUD USPS ZIP-CBSA CSV extract):

```powershell
cd C:\dev\OSHA_Leads
py -3 tools\build_zip_cbsa.py --input <hud_zip_cbsa_csv> --out data\geo\zip_to_cbsa.csv.gz --meta data\geo\cbsa_meta.csv --zip-meta-json data\geo\zip_to_cbsa.meta.json --sources data\geo\SOURCES.md --source-label "HUD USPS ZIP-CBSA <MONTH_OR_QUARTER>"
```

Operator note: if `data\geo\SOURCES.md` dataset label indicates `seed`/`incomplete`, rebuild from a full nationwide HUD USPS crosswalk file before relying on metro matching for new customers.

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

Unit tests cover expiry behavior:
- When a single `SENT` exists at/after `start_date` and `sends_limit=1`, the next live run emits `SKIP_TRIAL_EXPIRED`, writes `out\trials\<subscriber_key>\conversion_email.txt`, auto-sends conversion once, and latches notify_once.
- Non-live expiry runs still write the draft but keep conversion pending until the next live run.

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

