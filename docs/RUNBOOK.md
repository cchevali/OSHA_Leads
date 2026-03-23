# RUNBOOK

## Canonical Contract

`AGENTS.md` at repo root is the canonical operator + Codex instruction contract.
Use this runbook for executable commands, but resolve policy conflicts in favor of `AGENTS.md`.

## Centralized Runtime Operations

Runtime model:

- Canonical PC is the only live writer/sender for `osha.sqlite`, `crm.sqlite`, `crm_light.sqlite`, and live email sends.
- Laptop/dev clients are limited to `--print-config`, `--doctor`, `--dry-run`, and artifact review.
- GitHub Actions on the label-pinned self-hosted runner (`self-hosted`, `windows`, `osha-pc-canonical`) is the primary scheduled control plane.
- Windows Task Scheduler wrappers remain available as managed safety-net recovery tasks; runtime tick stays primary, and duplicate or legacy scheduler entries must be removed as drift.

Primary entrypoints:

- `run_runtime_tick.py`
- `run_runtime_state_migrate.py`
- `scripts\scheduled\run_trial_facs_daily.ps1`
- `scripts\scheduled\run_outreach_auto.ps1`
- `scripts\scheduled\run_osha_ingest_daily.ps1`
- `scripts\scheduled\run_osha_ingest_evening.ps1`
- `scripts\scheduled\run_prospect_replenish_daily.ps1`
- `scripts\scheduled\backup_runtime_state.ps1`

Artifact locations:

- Task logs: `${TASK_LOG_ROOT}` or `${DATA_DIR}\out\task_logs` or `.\out\task_logs`
- Run summaries: `${RUN_SUMMARY_ROOT}` or `${DATA_DIR}\out\run_summaries` or `.\out\run_summaries`
- Backup snapshots/manifests: `${BACKUP_ROOT}` or `${DATA_DIR}\out\backups` or `.\out\backups`
- Optional cloud mirror root: `${ARTIFACT_SYNC_DIR}` (artifacts/backups only; never live DB path)

Triage ladder:

1. Open run summary (`runtime_run_summary_v1` JSON + text).
2. Open referenced task log.
3. Validate runtime fingerprint block (`RUNTIME_HOSTNAME`, `RUNTIME_ROLE`, `RUNTIME_DATA_DIR`, `MFO_RUNTIME_MODE`, `MFO_TRUSTED_SCHEDULED`).
4. Resolve host/path mismatch (`ERR_RUNTIME_*`) before rerun.
5. Reconcile state if needed (for example trial ledger reconcile), then rerun wrapper.

Runner-offline behavior:

- If the canonical PC/self-hosted runner is offline, scheduled GitHub workflows will queue/fail visibly.
- Treat this as expected telemetry; do not reroute live writes/sends to laptop.
- Recovery is: restore runner availability on PC, validate guard/paths with `--print-config` and dry-run, then rerun wrapper/workflow.

## Runtime Tick (Primary Scheduler)

Primary scheduled workflow:

- `.github\workflows\runtime-tick-selfhosted.yml`
- Executes `run_runtime_tick.py` on the canonical self-hosted runner every 15 minutes
- Writes status artifacts under `${DATA_DIR}\runtime\status\`

Operator commands:

```powershell
cd C:\dev\OSHA_Leads
gh workflow view runtime-tick-selfhosted.yml
gh workflow enable runtime-tick-selfhosted.yml
gh workflow run runtime-tick-selfhosted.yml -f mode=doctor -f job=all
gh run list --workflow "Runtime Tick (Self-Hosted)" --limit 5
```

Direct wrapper commands:

```powershell
cd C:\dev\OSHA_Leads
.\run_with_secrets.ps1 -- py -3 run_runtime_tick.py --print-config
.\run_with_secrets.ps1 -- py -3 run_runtime_tick.py --doctor
.\run_with_secrets.ps1 -- py -3 run_runtime_tick.py --dry-run
```

Status artifacts:

- `${DATA_DIR}\runtime\status\runtime_latest.json`
- `${DATA_DIR}\runtime\status\runtime_latest.md`
- `${DATA_DIR}\runtime\status\jobs\<job>.json` (latest slot evaluation for ran, skipped, and reconciled jobs)
- `${DATA_DIR}\runtime\status\alerts\*.json` (dedupe markers for sent runtime alerts)

Runtime tick operator alerts:

- Recipient resolution: `RUNTIME_ALERT_RECIPIENT` -> `OSHA_SMOKE_TO`.
- Enablement: `RUNTIME_ALERTS_ENABLED` (`1|0`), default on when a recipient is resolvable.
- Alert categories:
  - `job_failure` for any failed runtime tick job.
  - `missed_window` for skipped `window_closed_*` on `ingest_daily`, `prospect_replenish_daily`, `outreach_auto`, and `trial_facs_daily`.
- Alerts are live-mode only; `--doctor` and `--dry-run` emit candidate/skipped tokens but do not send email.
- Runtime tick reconciles same-slot wrapper summaries before sending `missed_window`; successful break-glass wrapper evidence within the catchup window suppresses the alert and records a reconciled job state instead of leaving an empty missed-window marker.
- External wrapper evidence emits `WARN_RUNTIME_TICK_EXTERNAL_SCHEDULER` and records `last_external_scheduler_detected=1` plus `last_reconciliation_status` in `${DATA_DIR}\runtime\status\jobs\<job>.json`.

## Runtime State Migration

Use the migration entrypoint before cutting live runtime state fully into `${DATA_DIR}`:

```powershell
cd C:\dev\OSHA_Leads
.\run_with_secrets.ps1 -- py -3 run_runtime_state_migrate.py --print-config
.\run_with_secrets.ps1 -- py -3 run_runtime_state_migrate.py --doctor
.\run_with_secrets.ps1 -- py -3 run_runtime_state_migrate.py --dry-run
.\run_with_secrets.ps1 -- py -3 run_runtime_state_migrate.py --apply
```

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
`PACK_GIT_SHA` in the pack header is the doc-base commit (latest commit that touched canonical pack inputs), not repo `HEAD`. `PACK_BUILD_UTC` is that doc-base commit time in UTC.

Do not run `--build` unless you changed canonical pack inputs; for a re-upload of the same pack, just upload the existing `PROJECT_CONTEXT_PACK.md` and run `--mark-uploaded`.

Operator flow:

1. Run `--build` only when canonical pack inputs changed.
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

## Travel / Remote Operations

Primary travel operating model:

- Use the laptop local clone for development, tests, `--print-config`, `--doctor`, `--dry-run`, GitHub Actions review, and artifact inspection only.
- Use a Windows-native RDP session into the canonical PC for any live rerun, live send, or break-glass recovery.
- Treat Google Remote Desktop as fallback only; do not rely on it as the primary path if resolution/performance is already unreliable.
- Do not expose raw RDP directly to the public internet; use an existing secure access layer.

Travel preflight command (single entrypoint):

```powershell
cd C:\dev\OSHA_Leads
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\verify_travel_readiness.ps1 --print-config
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\verify_travel_readiness.ps1 --dry-run
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\verify_travel_readiness.ps1 --dry-run --target laptop
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\verify_travel_readiness.ps1 --dry-run --target pc
```

Script behavior:

- `--print-config` prints the exact laptop/PC preflight commands and manual checks without executing them.
- `--dry-run` executes the laptop-safe checks (`project_context_pack`, secrets decrypt diagnostics, runtime tick `--print-config`, `--doctor`, `--dry-run`, GitHub workflow list, and optional unit tests).
- `--target laptop` limits the run to laptop validation.
- `--target pc` limits the run to PC-side runtime/GitHub checks and emits the remote-session manual checks.
- `--skip-tests` omits `py -3 -m unittest -q` when you only want the shorter operational checks.

Required manual travel checks:

- From the laptop, open an RDP session to the canonical PC, disconnect, reconnect, and confirm the session is still usable at your normal working resolution.
- Confirm the canonical PC stays awake, network-connected, and reachable after disconnect/reconnect.
- From the remote PC session, inspect the latest runtime run summary/task log and confirm you can rerun the GitHub workflow if real recovery is needed.

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
  -OutreachStates TX,CA,FL,PA,OH `
  -OshaSmokeTo cchevali+oshasmoke@gmail.com `
  -OutreachSuppressionMaxAgeHours 240 `
  -SignalFreshnessMaxDays 30 `
  -AiTriageEnabled 0 `
  -AiTriageOpenAiModel gpt-4.1-mini `
  -OutreachFallbackOnEmptyState 0 `
  -OutreachSkipRoleInboxes 1 `
  -OutreachAllowFreeDomains 0 `
  -ProspectAutoGrowEnabled 1 `
  -ProspectAutoGrowSafetyNetEnabled 1 `
  -ProspectAiAssistReviewEnabled 1 `
  -ProspectAiAssistReviewRawTarget 30 `
  -ProspectAiAssistReviewPacketSize 10 `
  -ProspectAutoGrowSources AIHA `
  -ProspectAutoGrowBacklogTarget 60 `
  -ProspectAutoGrowMaxFetchPagesPerRun 6 `
  -ProspectAutoGrowHttpSleepMs 800 `
  -ProspectEnrichDomainEnabled 1 `
  -ProspectEnrichMaxSitesPerRun 25 `
  -ProspectEnrichHttpSleepMs 750 `
  -ApolloApiKey <your_apollo_api_key> `
  -ApolloEnrichEnabled 1 `
  -ApolloEnrichMaxPerRun 50 `
  -ApolloPersonLocationsMode state `
  -TrialSendsLimitDefault 14 `
  -TrialExpiredBehaviorDefault notify_once
```

This script:

- Ensures `DATA_DIR`, `OSHA_SMOKE_TO`, `OUTREACH_STATES`, and `OUTREACH_DAILY_LIMIT` exist in `.env.sops`
- Ensures `OUTREACH_SUPPRESSION_MAX_AGE_HOURS` is set to `240` when missing (or to your explicit parameter value)
- Ensures `SIGNAL_FRESHNESS_MAX_DAYS` is set to `30` when missing (or to your explicit parameter value)
- Ensures triage model defaults `AI_TRIAGE_ENABLED=0` and `AI_TRIAGE_OPENAI_MODEL=gpt-4.1-mini`
- Ensures `OUTREACH_FALLBACK_ON_EMPTY_STATE` default `0`, `OUTREACH_SKIP_ROLE_INBOXES` default `1`, and `OUTREACH_ALLOW_FREE_DOMAINS` default `0`
- Ensures prospect enrichment defaults include `PROSPECT_ENRICH_DOMAIN_ENABLED=0`, `PROSPECT_ENRICH_HUNTER_ENABLED=0`, `PROSPECT_ENRICH_MAX_SITES_PER_RUN=25`, and `PROSPECT_ENRICH_HTTP_SLEEP_MS=750`
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

CSV seed is optional bootstrap/debug only. Ongoing intake should run discovery, not CSV imports. Manual AI-assist review uses the controlled discovery augmentation flow below, not a generic side CSV workflow.

### CRM Diagnostics (read-only)

Use these commands instead of inline `py -3 -c "..."` one-liners. PowerShell quoting/escaping around embedded SQL/Python and `<`/`>` is brittle and can fail silently.

```powershell
.\run_with_secrets.ps1 -- py -3 outreach\crm_admin.py stats

.\run_with_secrets.ps1 -- py -3 outreach\crm_admin.py verify-import --csv .\apollo_export.csv
```

### Prospect Replenishment (Scheduled First)

`run_runtime_tick.py` runs replenishment automatically at the daily due window. Treat this as the automated background safety net, not the primary net-new prospect workflow. Use the canonical replenishment wrapper directly only for manual break-glass execution. It runs generation doctor -> generation -> discovery in order:

```powershell
cd C:\dev\OSHA_Leads
.\run_with_secrets.ps1 -- py -3 run_prospect_replenish_daily.py
```

Replenishment dry-run and print-config:

```powershell
.\run_with_secrets.ps1 -- py -3 run_prospect_replenish_daily.py --dry-run
.\run_with_secrets.ps1 -- py -3 run_prospect_replenish_daily.py --print-config
```

Direct generation/discovery commands remain available for troubleshooting:

```powershell
.\run_with_secrets.ps1 -- py -3 run_prospect_generation.py --doctor
.\run_with_secrets.ps1 -- py -3 run_prospect_generation.py
.\run_with_secrets.ps1 -- py -3 run_prospect_discovery.py
```

### Manual Deep Research Prospect Workflow

Manual Deep Research is the canonical net-new prospect lane. The evening ingest wrapper now refreshes the CRM skip list and writes a dated repo-managed prompt artifact under `${DATA_DIR}\audits\prospect_ai_assist\`.

Recommended operator commands:

```powershell
cd C:\dev\OSHA_Leads
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\prepare_manual_prospect_research.ps1 --print-config
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\prepare_manual_prospect_research.ps1 --dry-run
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\prepare_manual_prospect_research.ps1 -TargetFirms 50
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\import_prospect_ai_assist_from_clipboard.ps1 --print-config
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\import_prospect_ai_assist_from_clipboard.ps1 --dry-run
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\import_prospect_ai_assist_from_clipboard.ps1
.\run_with_secrets.ps1 -- py -3 tools\import_prospect_ai_assist_review.py --stdin --dry-run
.\run_with_secrets.ps1 -- py -3 tools\import_prospect_ai_assist_review.py --pending
.\run_with_secrets.ps1 -- py -3 tools\import_prospect_ai_assist_review.py --input C:\path\to\manual_deep_research_reviewed.csv --batch 2026-03-23_AIASSIST_MANUAL_203000
.\run_with_secrets.ps1 -- py -3 tools\prospect_growth_decision_pack.py --days 14
```

Operating rules:

- The prep tool refreshes `${DATA_DIR}\audits\prospect_ai_assist\crm_skip_list_for_ai.csv` and writes `${DATA_DIR}\audits\prospect_ai_assist\manual_prospect_deep_research_YYYYMMDD.txt`.
- The prompt template is repo-managed. External prompt files are no longer canonical.
- The prompt locks the active state scope, target-firm count, canonical CSV header, and the explicit `STATE_LIC` diagnostic that `STATE_LIC` remains TX-only while `PA` and `OH` are live through manual Deep Research and multi-state-capable sources.
- Canonical live scope is `TX,CA,FL,PA,OH`.
- Attach the current skip-list CSV to Deep Research and paste the generated prompt artifact. Deep Research must return only CSV, not prose, not a markdown table.
- Canonical Deep Research CSV header is `state,decision,firm,website,contact_name,title,email,source_urls,confidence,evidence_snippet`.
- `tools\import_prospect_ai_assist_review.py --stdin` accepts plain CSV or a single fenced `csv` block. Any extra commentary fails fast.
- The clipboard wrapper is the fastest operator path when you want to paste Deep Research output without creating a manual reviewed file yourself.
- Default stdin/clipboard batches use `YYYY-MM-DD_AIASSIST_MANUAL_HHMMSS`.
- `tools\import_prospect_ai_assist_review.py --pending` still scans `${DATA_DIR}\imports\prospect_ai_assist` oldest-first before live sends, so file-based reviewed CSVs remain valid for queued/manual backfill workflows.
- Import verifies domain/email shape, enforces suppression and `do_not_contact`, rejects accepted rows outside active `OUTREACH_STATES`, dedupes within the batch, and blocks CRM duplicates by email, root domain, and normalized firm key before any upsert. Free personal domains remain blocked by default, but you can opt in with `OUTREACH_ALLOW_FREE_DOMAINS=1`.
- This lane does not change outreach templates, cadence, scoring, suppression behavior, or sending rules.

### CRM Skip List Export (Low-Level Debug Path)

The prep wrapper above is the canonical operator command. Use the raw skip-list exporter only for low-level troubleshooting:

```powershell
.\run_with_secrets.ps1 -- py -3 tools\export_crm_ai_skip_list.py --print-config
.\run_with_secrets.ps1 -- py -3 tools\export_crm_ai_skip_list.py --dry-run
.\run_with_secrets.ps1 -- py -3 tools\export_crm_ai_skip_list.py
```

Operating notes:

- Default output path is `${DATA_DIR}\audits\prospect_ai_assist\crm_skip_list_for_ai.csv`.
- With canonical live runtime, that resolves to `C:\osha_data\audits\prospect_ai_assist\crm_skip_list_for_ai.csv`.
- Each row is a firm/domain-level skip record aggregated from `crm.sqlite`.

Canonical nightly schedule:

```powershell
cd C:\dev\OSHA_Leads
gh workflow view ingest-evening-ai-review-selfhosted.yml
gh run list --workflow "Ingest Evening + AI Review Dump (Self-Hosted)" --limit 5
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\scheduled\run_osha_ingest_evening.ps1
```

No-arg generation output path:

- `${DATA_DIR}\prospect_discovery\prospects_latest.csv`
- If `DATA_DIR` is unset: `.\out\prospect_discovery\prospects_latest.csv`
- Generator-side BYO CSV inbox paths are removed. Discovery input is now seed pools + autogrow sources only.

Auto-growth (env-gated, optional):

- Canonical keys (no aliases): `PROSPECT_AUTOGROW_ENABLED`, `PROSPECT_AUTOGROW_SAFETY_NET_ENABLED`, `PROSPECT_AUTOGROW_STATES`, `PROSPECT_AUTOGROW_SOURCES`, `PROSPECT_AUTOGROW_BACKLOG_TARGET`, `PROSPECT_AUTOGROW_MAX_FETCH_PAGES_PER_RUN`, `PROSPECT_AUTOGROW_HTTP_SLEEP_MS`, `PROSPECT_AI_ASSIST_REVIEW_ENABLED`, `PROSPECT_AI_ASSIST_REVIEW_RAW_TARGET`, `PROSPECT_AI_ASSIST_REVIEW_PACKET_SIZE`.
- Crawl4AI runtime keys (optional, default zero-cost): `PROSPECT_AUTOGROW_LLM_ENABLED` (default `0`), `PROSPECT_AUTOGROW_BCSP_CREDENTIALS`, `PROSPECT_AUTOGROW_BCSP_INDUSTRY`, `PROSPECT_AUTOGROW_STATE_LIC_TX_LICENSE_TYPES`.
- OHS optional auth key (only if buyersguide pagination is work-email gated): `OHS_BG_STORAGE_STATE_PATH` (Playwright storage state JSON path).
- Apollo keys: `APOLLO_API_KEY`, `APOLLO_ENRICH_ENABLED`, `APOLLO_ENRICH_MAX_PER_RUN`, `APOLLO_PERSON_TITLES`, `APOLLO_PERSON_LOCATIONS_MODE`.
- Generator enrichment keys: `PROSPECT_ENRICH_DOMAIN_ENABLED`, `PROSPECT_ENRICH_HUNTER_ENABLED`, `PROSPECT_ENRICH_MAX_SITES_PER_RUN` (default `25`), `PROSPECT_ENRICH_HTTP_SLEEP_MS` (canonical persisted default `750` via `scripts\set_outreach_env.ps1`; ad hoc runs fall back to `PROSPECT_AUTOGROW_HTTP_SLEEP_MS` when unset).
- Source scope: implemented tokens are `AIHA`, `BLUEBOOK`, `OHS_BG`, `APOLLO`, `BCSP`, `OSHA_NEWS`, and `STATE_LIC` (comma-separated via `PROSPECT_AUTOGROW_SOURCES`; canonical production list is `AIHA`, while `BLUEBOOK`, `OHS_BG`, `BCSP`, `OSHA_NEWS`, and `STATE_LIC` remain implemented nondefault lanes).
- Canonical discovery automation uses a directory-to-website public-contact path: valid non-free source email first, otherwise crawl the source-provided company website for a public business email. Guessed domains, guessed emails, and Hunter/provider lookups are not part of the canonical `AIHA` default lane.
- `BLUEBOOK` remains implemented for explicit/diagnostic runs, but public-mode listing access is currently captcha-blocked, so it is not part of canonical defaults until an approved access path exists.
- `STATE_LIC` remains implemented as a secondary lane. AI-assist packet eligibility allows strong-identity review seeds even when no website exists, while consultant-fit logic still governs consultant backlog credit and `STATE_LIC_WORK_EMAIL` promotion.
- Planned-but-unimplemented registry tokens such as `BBB`, `THOMASNET`, and `AGC` are rejected intentionally by `scripts\set_outreach_env.ps1` and `outreach\run_prospect_generation.py` until source modules land.
- Cache paths:
  - AIHA: `${DATA_DIR}\prospect_generation\cache\aiha\state_<STATE>.json`
  - BLUEBOOK: `${DATA_DIR}\prospect_generation\cache\bluebook\state_<STATE>.json`
  - OHS_BG: `${DATA_DIR}\prospect_generation\cache\ohs_bg\state_<STATE>.json`
  - APOLLO: `${DATA_DIR}\prospect_generation\cache\apollo\state_<STATE>.json`
  - BCSP: `${DATA_DIR}\prospect_generation\cache\bcsp\state_<STATE>.json`
  - OSHA_NEWS: `${DATA_DIR}\prospect_generation\cache\osha_news\state_<STATE>.json`
  - STATE_LIC: `${DATA_DIR}\prospect_generation\cache\state_lic\state_<STATE>.json`
  - Website enrichment: `${DATA_DIR}\prospect_generation\cache\website_email\<domain>.json` (TTL 14 days)
- Diagnostics path: `${DATA_DIR}\prospect_generation\diagnostics\...`.
- Backlog targeting is evaluated per configured state in `PROSPECT_AUTOGROW_STATES` (runtime default: `OUTREACH_STATES`).
- Canonical production posture is to leave `PROSPECT_AUTOGROW_STATES` unset so `OUTREACH_STATES` stays the single scope of truth; print-config/doctor paths emit a drift warning when both are set and differ.
- Safety net default (`PROSPECT_AUTOGROW_SAFETY_NET_ENABLED=1`): when `PROSPECT_AUTOGROW_ENABLED=0` and a configured state has a depleted CRM pool (`backlog_current=0` with existing pool rows), generator auto-forces AIHA autogrow for that depleted state.
- APOLLO v1 flow: People Search (`has_email=true` gating) + Bulk People Enrichment in batches of 10; no waterfall/webhook mode.
- APOLLO consumes enrichment credits. Search can be low/no-credit; enrichment is credit-capped by `APOLLO_ENRICH_MAX_PER_RUN`.
- Apollo free-tier credits are limited; validate refill volume against actual credit usage before increasing send limits.
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

Generation doctor (readiness checks only, warning-level for Crawl4AI/APOLLO free-tier blocks):

```powershell
.\run_with_secrets.ps1 -- py -3 run_prospect_generation.py --doctor
```

One-time Crawl4AI setup (human step, not auto-run by app code):

```powershell
pip install crawl4ai
crawl4ai-setup
```

Generator emits machine-readable lines:

- `GENERATOR_OUTPUT_PATH`
- `GENERATOR_ROWS_READ`
- `GENERATOR_ROWS_WRITTEN`
- `GENERATOR_AUTOGROW_*`
- `GENERATOR_AUTOGROW_SAFETY_NET_FORCED=1 reason=SENDABLE_BELOW_FLOOR states=<STATE:sendable,...>`, `GENERATOR_AUTOGROW_SAFETY_NET_STATES`
- `GENERATOR_AUTOGROW_TOTAL_STATES`, `GENERATOR_AUTOGROW_TOTAL_ACCEPTED`
- `GENERATOR_AUTOGROW_STATE=<STATE> backlog_current=<n> backlog_sendable_current=<n> new_needed=<n> aiha_candidate=<n> aiha_accepted=<n> bluebook_candidate=<n> bluebook_accepted=<n> ohs_bg_candidate=<n> ohs_bg_accepted=<n> apollo_candidate=<n> apollo_accepted=<n>`
- `GENERATOR_AUTOGROW_STATES`
- `GENERATOR_AUTOGROW_SOURCE_STATE source=<AIHA|BLUEBOOK|OHS_BG|APOLLO|BCSP|OSHA_NEWS|STATE_LIC> state=<STATE> ...`
- `GENERATOR_AIHA_*`
- `GENERATOR_BLUEBOOK_*`
- `GENERATOR_OHS_BG_*`
- `GENERATOR_APOLLO_*`
- `GENERATOR_BCSP_*`, `GENERATOR_OSHA_NEWS_*`, `GENERATOR_STATE_LIC_*` (including effective TX license types, candidate license-type breakdown, and `GENERATOR_STATE_LIC_REJECTED_FIT_MISMATCH`)
- `crawl4ai_installed`, `playwright_browsers_installed`, `<SOURCE>_available` (via `--print-config`)
- `GENERATOR_DIAGNOSTICS_PATH` (when generated)
- `GENERATOR_WEBSITE_ENRICH_*`, `GENERATOR_WEBSITE_ENRICH_NEEDS_REVIEW_PATH`
- `GENERATOR_COMPLETE status=<OK|DRY_RUN>`

APOLLO telemetry highlights:

- `GENERATOR_APOLLO_SEARCH_PAGES_FETCHED`
- `GENERATOR_APOLLO_SEARCH_ROWS_RETURNED`
- `GENERATOR_APOLLO_SEARCH_ROWS_HAS_EMAIL_TRUE`
- `GENERATOR_APOLLO_SEARCH_ROWS_DEDUPED_ID`
- `GENERATOR_APOLLO_ENRICH_ATTEMPTED`
- `GENERATOR_APOLLO_ENRICHED`
- `GENERATOR_APOLLO_ENRICH_NO_MATCH`
- `GENERATOR_APOLLO_ENRICH_SKIPPED_CREDIT_CAP`
- `GENERATOR_APOLLO_CREDIT_CAP_HIT`

Optional empty-state planner fallback:
- `OUTREACH_FALLBACK_ON_EMPTY_STATE=0` (default) preserves weekday rotation-selected state.
- Set `OUTREACH_FALLBACK_ON_EMPTY_STATE=1` to auto-switch plan/send to the configured state with the highest sendable estimate when the rotation-selected state is empty (or below floor).
- `OUTREACH_SKIP_ROLE_INBOXES=1` (default) skips role inbox local-parts (`info`, `contact`, `admin`, `office`, `support`, `sales`, `hello`, `help`, `billing`, `accounts`, `careers`, `jobs`, `hr`).
- `OUTREACH_ALLOW_FREE_DOMAINS=0` (default) keeps free personal domains out of reviewed AI-assist imports and generator sendability cohorts; set `OUTREACH_ALLOW_FREE_DOMAINS=1` to admit them.
- Stable state-selection tokens: `OUTREACH_STATE_ROTATION_SELECTED=<STATE>` and `OUTREACH_STATE_EFFECTIVE_SEND=<STATE>`
- Fallback token: `OUTREACH_FALLBACK_TRIGGERED=1 from=<STATE> to=<STATE> reason=<SENDABLE_BELOW_FLOOR>`
- No-signal token: `OUTREACH_SKIP_NO_SIGNALS state=<STATE> window_days=<N>`
- Empty-state no-send token: `OUTREACH_EMPTY_STATE_NO_SEND=1 state=<STATE>`
- Pre-send duplicate token: `OUTREACH_DUPLICATE_GUARD_DROPPED=<n>`
- Same-day live-run guard token: `OUTREACH_SKIP_ALREADY_SENT_TODAY=1 date=<YYYY-MM-DD> existing_batches=<csv|none> guard=ON`
- Floor readiness token (manual ramp remains operator-controlled): `OUTREACH_RAMP_READY=<0|1> desired_daily_limit=<N> states_ready=<k> states_total=<m> ready_states=<csv|none>`
- Emergency override (manual only): `.\run_with_secrets.ps1 -- py -3 run_outreach_auto.py --allow-second-live-run-same-day`

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

### Apollo export workflow

- Export Apollo contacts CSV from Apollo.
- Save the export locally (for example `apollo_export.csv`; filename is operator convenience only).
- From repo root, run:
  `.\run_with_secrets.ps1 -- py -3 tools\apollo_to_prospects_csv.py --input C:\path\to\apollo_export.csv`
- Converter default target (overwrite enabled, atomic replace): `${DATA_DIR}\imports\prospects_apollo.csv` (or `.\out\imports\prospects_apollo.csv` when `DATA_DIR` is unset).
- If you explicitly set `--output` to `prospects_latest.csv`, you are overwriting the canonical generator artifact.
- Optional overrides: `--output <path>` and `--diagnostics-out <path>`.
- Run discovery using the converter’s printed `output_path` value (recommended): `.\run_with_secrets.ps1 -- py -3 run_prospect_discovery.py --input <output_path_from_converter>`.
- If you are following `DATA_DIR` convention, use: `.\run_with_secrets.ps1 -- py -3 run_prospect_discovery.py --input "$env:DATA_DIR\imports\prospects_apollo.csv"`.
- Mismatch note: running discovery from repo root with a bare filename can target the wrong location (`C:\dev\OSHA_Leads\...`) instead of the converter output path.
- If `DATA_DIR` is set or defaults to `C:\osha_data`, discovery must be pointed at that location.
- Discovery no-arg still honors input overrides first: `PROSPECT_DISCOVERY_INPUT`, then `DISCOVERY_INPUT_CSV`.

Set preferred discovery input via the canonical no-editor env helper:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\set_outreach_env.ps1 `
  -OutreachDailyLimit 10 `
  -OutreachStates TX,CA,FL,PA,OH `
  -OshaSmokeTo cchevali+oshasmoke@gmail.com `
  -OutreachSuppressionMaxAgeHours 240 `
  -SignalFreshnessMaxDays 30 `
  -AiTriageEnabled 0 `
  -AiTriageOpenAiModel gpt-4.1-mini `
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
2. `.\run_with_secrets.ps1 -- py -3 run_prospect_replenish_daily.py`
3. `.\run_with_secrets.ps1 -- py -3 run_outreach_auto.py --plan --for-date YYYY-MM-DD` (or dry-run/live send flow)

### OSHA Ingest Scope Modes (Operator Truth)

- `run_osha_ingest_daily.py` defaults to `--scope-mode outreach`.
- `--scope-mode outreach` resolves states from `OUTREACH_STATES` (existing default behavior).
- `--scope-mode outreach_plus_trial_live` resolves the deterministic union:
  - outreach states from `OUTREACH_STATES`, plus
  - states from active subscriber territories (`trial/live/paid/active`) in CRM.
- Statement of truth:
  - Evening scheduler uses `outreach_plus_trial_live`.
  - Direct daily ingest invocations default to `outreach` unless explicitly overridden.
  - Trial send path (`run_trial_daily.py` -> `deliver_daily.py`) ingests from the trial customer config `states`.

Scope inspection commands:

```powershell
.\run_with_secrets.ps1 -- py -3 run_osha_ingest_daily.py --print-config
.\run_with_secrets.ps1 -- py -3 run_osha_ingest_daily.py --scope-mode outreach_plus_trial_live --print-config
```

Machine-readable ingest scope + planning tokens:

- `INGEST_SCOPE_MODE=<outreach|outreach_plus_trial_live>`
- `INGEST_SCOPE_STATES=<CSV>`
- `INGEST_SCOPE_SOURCE=<outreach|resolver>`
- `INGEST_CANDIDATES_BY_STATE state=<ST> count=<n>`
- `INGEST_FETCH_PLAN_BY_STATE state=<ST> planned=<n>`
- `DELIVER_INGEST_SCOPE_STATES=<CSV> source=customer_config`
- `DELIVER_INGEST_MAX_DETAILS=<n>`

Context pack hygiene (when docs/contracts changed or `WARN_CONTEXT_PACK_SOURCE_HASH_MISMATCH` appears):

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
Weekend live sends are blocked in code for outreach (`OUTREACH_SKIP_NON_WEEKDAY ... gate=outreach_weekdays_only`); `--plan`, `--dry-run`, `--doctor`, and `--print-config` remain available on weekends.

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
- Optional zero-send-day guard: set `OUTREACH_FALLBACK_ON_EMPTY_STATE=1` to allow auto-switching to the configured state with the highest sendable estimate; verify activation via `OUTREACH_FALLBACK_TRIGGERED=1 from=<STATE> to=<STATE> reason=<...>`.
- Recommended default remains `0` unless you explicitly want to prevent zero-send days by allowing state fallback.
- Track rotation vs effective send and floor readiness in stdout tokens: `OUTREACH_STATE_ROTATION_SELECTED`, `OUTREACH_STATE_EFFECTIVE_SEND`, and `OUTREACH_RAMP_READY`.
- Signal/selection guard tokens: `OUTREACH_SKIP_NO_SIGNALS`, `OUTREACH_EMPTY_STATE_NO_SEND`, and `OUTREACH_DUPLICATE_GUARD_DROPPED`.

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

- `OUTREACH_STATES=TX,CA,FL,PA,OH`
- `OUTREACH_DAILY_LIMIT=10`
- `OSHA_SMOKE_TO=cchevali+oshasmoke@gmail.com`
- `OUTREACH_SUPPRESSION_MAX_AGE_HOURS=240`
- `OUTREACH_FALLBACK_ON_EMPTY_STATE=0` (default)
- `OUTREACH_SKIP_ROLE_INBOXES=1` (default)
- `DATA_DIR=out` (or your runtime path)

`run_outreach_auto.py` deterministically picks today's rotation state from `OUTREACH_STATES` by weekday index, may optionally fallback to a different effective send state, and always uses the effective send-state batch id `<YYYY-MM-DD>_<STATE>`.
`--for-date YYYY-MM-DD` is allowed with `--print-config`, `--doctor`, `--dry-run`, and `--plan`.
If `--for-date` is not today and a live send is attempted, the command hard-fails with `ERR_AUTO_FOR_DATE_LIVE_SEND_BLOCKED` and no partial send effects.
Normal runs select and prioritize prospects directly from `crm.sqlite`, send outreach emails, then record `outreach_events` and status updates.

Expected artifacts:

- `out/crm.sqlite` (or `${DATA_DIR}\crm.sqlite`)
- `out/outreach_export_ledger.jsonl` (optional compatibility ledger)
- `out\outreach\<batch>\outbox_<batch>_dry_run.csv`
- `out\outreach\<batch>\outbox_<batch>_dry_run_manifest.csv` (includes `domain`, `segment`, `role_or_title`, `state_pref`, and `rank_reason` audit fields)
- `out\outreach\<batch>\plan_diagnostics.json` (run-level plan/dry-run diagnostics including pool totals and filter breakdown)

### OSHA Detail Cache + Triage Overlay (Preview Ops)

OSHA inspection-detail cache CLI:

- No secrets required (rules-only cache fetch; no AI calls).
- Uses DATA_DIR-aware cache path: `${DATA_DIR}\scoring\osha_detail_cache.sqlite` (fallback `.\out\scoring\osha_detail_cache.sqlite`).

```powershell
cd C:\dev\OSHA_Leads
py -3 tools\cache_osha_inspection_detail.py --print-config
```

```powershell
cd C:\dev\OSHA_Leads
py -3 tools\cache_osha_inspection_detail.py --since-days 14
```

Triage behavior contract:

- Rules layer is always on for trial/shared daily digest signal selection, render-time digest intelligence, and outreach signal examples.
- Shared daily subscriber/trial digest intelligence is rules-first and default-on in the renderer; this improves summary/top-pick presentation only and does not change outreach behavior.
- AI layer is optional. It is evaluated only when `AI_TRIAGE_ENABLED=1` and the path gate is on:
- Shared/trial daily digest AI overlay gate: `DIGEST_AI_OVERLAY_ENABLED=1` (with `TRIAL_TRIAGE_OVERLAY_ENABLED=1` as the default-on rules/render path gate)
- Outreach path gate: `OUTREACH_TRIAGE_OVERLAY_ENABLED=1`
- Cached/manual-reviewed AI overlay can raise or lower final digest priority, but it never unsuppresses a rules-suppressed signal.
- AI cache lookup is attempted before OpenAI API access; cached/manual-reviewed priorities can apply even when `OPENAI_API_KEY` is missing.
- Trial/outreach send paths auto-import the newest `ai_review_*.csv` once per process from `C:\osha_data\imports\signals_ai_review` (fallback `${DATA_DIR}\imports\signals_ai_review`, then legacy root `imports`) unless overridden.
- If AI is enabled but unavailable for uncached signals (missing key/network/API error), execution degrades to rules-only and emits `WARN_AI_TRIAGE_UNAVAILABLE` plus `AI_TRIAGE_UNAVAILABLE=1`.

Triage env keys:

- `SIGNAL_FRESHNESS_MAX_DAYS` (default `30`)
- `AI_TRIAGE_ENABLED` (default `0`)
- `AI_TRIAGE_OPENAI_MODEL` (default `gpt-4.1-mini`)
- `AI_REVIEW_AUTO_IMPORT_ENABLED` (default `1`)
- `AI_REVIEW_IMPORT_MAX_AGE_HOURS` (default `24`)
- `AI_REVIEW_IMPORT_DIR` (optional absolute override for `ai_review_*.csv`)

Rules-only trial dry run (no secrets required):

```powershell
cd C:\dev\OSHA_Leads
$env:TRIAL_TRIAGE_OVERLAY_ENABLED='1'
py -3 run_wally_trial.py --test-send-daily --dry-run
```

Rules + AI trial dry run (secrets required):

```powershell
cd C:\dev\OSHA_Leads
$env:TRIAL_TRIAGE_OVERLAY_ENABLED='1'
$env:AI_TRIAGE_ENABLED='1'
.\run_with_secrets.ps1 -- py -3 run_wally_trial.py --test-send-daily --dry-run
```

Outreach dry run with rules always-on and optional AI gate:

```powershell
cd C:\dev\OSHA_Leads
$env:OUTREACH_TRIAGE_OVERLAY_ENABLED='1'
$env:AI_TRIAGE_ENABLED='1'
.\run_with_secrets.ps1 -- py -3 run_outreach_auto.py --dry-run
```

Render preview behavior:

- Rules execute in render-preview.
- AI executes only when `AI_TRIAGE_ENABLED=1` and OpenAI key is available via secrets wrapper.
- Otherwise render-preview stays rules-only and emits warning telemetry.

Scoring config files (committed, operator-editable):

- `data/scoring/naics_emphasis_boost.csv` columns: `naics_prefix,label,boost_points`
- `data/scoring/naics_suppress.csv` columns: `naics_prefix,label,reason`
- `data/scoring/enterprise_names.csv` columns: `pattern,match_type,reason`

Config update procedure:

1. Edit CSV files in `data/scoring/`.
2. Keep header row and valid column names unchanged.
3. Use prefixes (`722*`), ranges (`44-45*`), and allow-overrides (`!561621`) as needed.
4. Run `py -3 -m unittest -q`.
5. Validate behavior with trial/outreach dry-run commands above before live sends.

Trial triage artifacts:

- `${DATA_DIR}\trials\<subscriber_key>\scoring\triage_<YYYY-MM-DD>.json`
- `${DATA_DIR}\trials\<subscriber_key>\scoring\triage_report_<YYYY-MM-DD>.txt`
- Fallback when `DATA_DIR` is unset: `.\out\trials\<subscriber_key>\scoring\...`

Outreach triage artifacts (only when `OUTREACH_TRIAGE_OVERLAY_ENABLED=1`):

- Dry-run outbox/manifest still write under `out\outreach\<batch>\...`
- Per-example triage JSON writes to `${DATA_DIR}\outreach\<batch>\signals_triage_<batch>_dry_run.json`
- Fallback when `DATA_DIR` is unset: `.\out\outreach\<batch>\signals_triage_<batch>_dry_run.json`

Weekly manual signal QA loop:

1. Dump rules-classified signals for external review:

```powershell
cd C:\dev\OSHA_Leads
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\dump_signals_for_ai_review.ps1 -PrintConfig
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\dump_signals_for_ai_review.ps1 -DryRun
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\dump_signals_for_ai_review.ps1
```

2. Review exported signals (for example in Claude) and produce CSV with `activity_nr,ai_priority,ai_reason`.
3. Validate and import reviewed raises into AI cache:

```powershell
cd C:\dev\OSHA_Leads
py -3 tools\import_ai_triage.py --print-config
py -3 tools\import_ai_triage.py --input .\out\imports\signals_ai_review\ai_review_20260315.csv --dry-run
py -3 tools\import_ai_triage.py --input .\out\imports\signals_ai_review\ai_review_20260315.csv
```

Auto-import notes:

- Runtime auto-import emits one of: `AI_REVIEW_AUTO_IMPORT_APPLIED`, `WARN_AI_REVIEW_AUTO_IMPORT_MISSING`, `WARN_AI_REVIEW_AUTO_IMPORT_STALE`, or `WARN_AI_REVIEW_AUTO_IMPORT_INVALID`.
- With defaults, only files modified within the last 24 hours are auto-imported.
- Manual `tools\import_ai_triage.py` remains supported for deterministic operator backfills/re-runs.

### Nightly AI Review Cycle

Canonical manual command path (always loads secrets/DATA_DIR via wrapper):

```powershell
cd C:\dev\OSHA_Leads
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\dump_signals_for_ai_review.ps1
```

Evening scheduler note:

- `.github\workflows\ingest-evening-ai-review-selfhosted.yml` and `scripts\scheduled\run_osha_ingest_evening.ps1` are the single canonical nightly AI-review generator at `20:45` America/New_York, all 7 days.
- `scripts\scheduled\run_osha_ingest_evening.ps1` runs ingest with `--scope-mode outreach_plus_trial_live`, then writes signals dumps to `${DATA_DIR}\audits\signals_ai_review\signals_for_ai_review_YYYYMMDD.txt`, then writes prospect dumps to `${DATA_DIR}\audits\prospect_ai_assist\prospect_ai_assist_review_YYYYMMDD.txt` plus packet slices under `${DATA_DIR}\audits\prospect_ai_assist\prospect_ai_assist_review_YYYYMMDD_packets\`.
- Reviewed signals CSV drops belong under `${DATA_DIR}\imports\signals_ai_review\ai_review_YYYYMMDD.csv`; auto-import prefers that folder and falls back to legacy root `imports` only during migration.
- WA/OR can still be zero on a given day when upstream data has no in-window records.

Common variants:

```powershell
# side-effect free resolved config
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\dump_signals_for_ai_review.ps1 -PrintConfig

# inspect output only (no file write)
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\dump_signals_for_ai_review.ps1 -DryRun

# explicit window / scope override
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\dump_signals_for_ai_review.ps1 -Since 2026-03-01 -Until 2026-03-01 -AllOutreach

# explicit state-scope override (manual/nightly include set)
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\dump_signals_for_ai_review.ps1 -Since 2026-03-03 -Until 2026-03-03 -States CA,OR,WA
```

Output location is DATA_DIR-aware:

- Effective precedence for wrapped commands (`.\run_with_secrets.ps1 -- ...`):
- Inherited process `DATA_DIR` (non-empty) wins.
- Else `.env.sops` `DATA_DIR` is used.
- Else fallback is repo `.\out`.
- Invalid values (`""`, `out`, non-rooted relative path) fall back to repo `.\out`.

AI review dump date-window basis:

- `--since/--until` are matched primarily on `first_seen_at` local date (when the signal first entered our DB).
- Fallback to `date_opened` is used only when `first_seen_at` is missing/unparseable.
- This keeps manual AI review aligned with what can actually be newly selected for daily sends.
- Late-posted OSHA rows are expected: `date_opened` may be older than `first_seen_at`.

Machine-readable path tokens:

- `MFO_DATA_DIR_EFFECTIVE=<abs_path|empty>`
- `MFO_DATA_DIR_SOURCE=inherited|dotenv|default`
- `WARN_ENV_CONFLICT=1 key=DATA_DIR inherited=<...> dotenv=<...> using=<...>`
- `WARN_DATA_DIR_NOT_ABSOLUTE=1 value=<...> behavior=UNSET_FOR_CHILD`
- `AI_REVIEW_DUMP_OUTPUT_DIR=<abs_path>`
- `AI_REVIEW_DUMP_OUTPUT_PATH=<abs_path>`
- `AI_REVIEW_DUMP_DATA_DIR=<effective_abs_path|empty>`
- `AI_REVIEW_DUMP_DATA_DIR_SOURCE=<inherited|dotenv|default>`
- `AI_REVIEW_DUMP_SCOPE=STATES states=<CSV>` (emitted when `-States` / `--states` scope override is used)
- `AI_REVIEW_DUMP_FILTER_BASIS=FIRST_SEEN_FALLBACK_OPENED`
- `AI_REVIEW_DUMP_MATCHED_BY_FIRST_SEEN=<n>`
- `AI_REVIEW_DUMP_MATCHED_BY_OPENED_FALLBACK=<n>`

Empty dump interpretation (file may contain only headers/section markers):

- `AI_REVIEW_DUMP_MATCHED_TOTAL=0`
- `WARN_AI_REVIEW_DUMP_EMPTY=1 reason=NO_MATCHES since=<...> until=<...>`
- `AI_REVIEW_DUMP_MAX_FIRST_SEEN=<iso|empty>`
- `AI_REVIEW_DUMP_MAX_DATE_OPENED=<iso|empty>`

Manual review timing note (Sun-Thu nights):

- Signals first seen Monday morning can still send in Monday 8:00 AM digest before manual review.
- This is expected with the current manual-only schedule and is not by itself evidence of ingest failure.

DATA_DIR persistence and edits:

- Use `scripts\set_outreach_env.ps1` as the only supported way to persist `.env.sops` keys.
- Do not manually edit `.env.sops`.
- Canonical persist command:

```powershell
cd C:\dev\OSHA_Leads
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\set_outreach_env.ps1 -DataDir "C:\osha_data" -OshaSmokeTo cchevali+oshasmoke@gmail.com
```

- Expected token: `PASS_SET_OUTREACH_ENV_DATA_DIR value=<...> source=<param|inherited|unchanged>`

### Tomorrow AI Prep (Non-Send)

Use one command to run the full readiness pipeline now (manual AI import + ingest + generation + discovery + doctor + outreach/trial dry-runs):

```powershell
cd C:\dev\OSHA_Leads
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\prepare_tomorrow_ai_pipeline.ps1 -Apply
```

Dry-run and config variants:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\prepare_tomorrow_ai_pipeline.ps1 -PrintConfig
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\prepare_tomorrow_ai_pipeline.ps1 -DryRun
```

Notes:

- The prep script auto-selects the newest `ai_review_*.csv` from `C:\osha_data\imports\signals_ai_review` (fallback: `${DATA_DIR}\imports\signals_ai_review`, then legacy root `imports`) unless `-AiReviewCsv` is passed.
- It auto-creates `${DATA_DIR}\suppression.csv` (or `.\out\suppression.csv`) with header `email` in `-Apply` mode when missing.
- Required AI gates for overlay behavior:
- `AI_TRIAGE_ENABLED=1`
- `OUTREACH_TRIAGE_OVERLAY_ENABLED=1`
- `TRIAL_TRIAGE_OVERLAY_ENABLED=1`
- Persist gates only through `scripts\set_outreach_env.ps1` (no manual `.env.sops` edits):

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\set_outreach_env.ps1 -AiTriageEnabled 1 -OutreachTriageOverlayEnabled 1 -TrialTriageOverlayEnabled 1 -OshaSmokeTo cchevali+oshasmoke@gmail.com
```

Readiness tokens:

- `PIPELINE_READY_FOR_TOMORROW=1|0`
- `PIPELINE_BLOCKERS=<csv|none>`

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

### Weekly Ops Snapshot (Persisted Ops + Readiness Artifact)

```powershell
cd C:\dev\OSHA_Leads
py -3 outreach\run_ops_snapshot.py --print-config
py -3 outreach\run_ops_snapshot.py --dry-run
py -3 outreach\run_ops_snapshot.py
py -3 outreach\run_ops_snapshot.py --format json
```

Snapshot behavior:

- `--print-config` is side-effect free and prints resolved artifact/config paths.
- `--dry-run` computes the snapshot without writing files and prints `OPS_SNAPSHOT_JSON_PATH=(no-write)`.
- Live mode writes:
- `${DATA_DIR}\outreach\ops_snapshots\<YYYY-MM-DD>\ops_snapshot_<HHMMSSZ>.json`
- `${DATA_DIR}\outreach\ops_snapshots\latest.json`
- Payload includes the existing ops-report windows plus readiness state for runtime age, per-job status, `parallel_scheduler_active`, suppression freshness, and bounce-import state.

### Dry-Run Artifact Retention Cleanup

```powershell
cd C:\dev\OSHA_Leads
py -3 outreach\cleanup_outreach_dry_run_artifacts.py --print-config
py -3 outreach\cleanup_outreach_dry_run_artifacts.py --dry-run --retention-days 14
py -3 outreach\cleanup_outreach_dry_run_artifacts.py --retention-days 14
```

Cleanup scope:

- Targets only stale dry-run artifacts under `out\outreach\<batch>\`:
- `outbox_*_dry_run.csv`
- `outbox_*_dry_run_manifest.csv`
- `plan_diagnostics.json`
- Does not touch live delivery artifacts, non-dry-run manifests, or other batch files.

### QA Checks (Before/After Daily Send)

```powershell
# Verify CRM + suppression paths
.\run_with_secrets.ps1 -- py -3 run_outreach_auto.py --print-config

# Dry-run candidate preview
.\run_with_secrets.ps1 -- py -3 run_outreach_auto.py --dry-run

# Rendered copy preview (no sends/no outbox artifacts)
.\run_with_secrets.ps1 -- py -3 outreach\generate_mailmerge.py --render-preview --state TX --limit 1

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

### State Of World (2026-02-27)

- PR #18 deployed outreach copy framing updates plus deterministic `--render-preview`; scope was copy/template/test/doc only.
- PR #19 added test-only hardening: export-writes-artifacts regression coverage + preview no-write guard.
- Canonical copy QA command: `py -3 -m outreach.generate_mailmerge --render-preview --state CA --limit 1`
- Canonical dry-run QA command: `$env:DATA_DIR='out'; py -3 -m outreach.run_outreach_auto --dry-run`
- Invariant: outreach dry-run remains candidate-only (outbox/manifest/diagnostics), not rendered-body output.
- Invariant: render-preview is side-effect free (no outbox/manifest/ledger/run-log writes).
- Invariant: compliance markers are regression-tested (single footer opt-out links; no pre-footer duplicate unsubscribe links).

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

Suppression + bounce alignment:

- `run_capture_sync.py` records downstream lifecycle outcomes such as `replied`, `do_not_contact`, and `bounced` back into CRM event/state.
- `outreach\import_bounces_imap.py` is idempotent by mailbox state and message fingerprint; hard bounces update CRM suppression and prospect status, while soft bounces remain event-only.
- `run_outreach_auto.py --doctor` enforces suppression freshness and now prints `WARN_DOCTOR_PARALLEL_SCHEDULER_ACTIVE jobs=<...>` when recent external scheduler drift is visible in runtime job state.
- `outreach\ops_report.py` and `outreach\run_ops_snapshot.py` surface reply, `trial_started`, `converted`, bounce, and suppression evidence for weekly review.
- Complaint/FBL intake is still a manual operator path until the provider exposes a deterministic machine-readable feed.

### Task Scheduler (Break-Glass Only)

Do not keep duplicate or legacy Windows Task Scheduler entries active once `runtime-tick-selfhosted.yml` is live on the canonical runner.
Use the managed Task Scheduler entries only as safety-net recovery rails when GitHub Actions on the canonical PC is unavailable or degraded.
If `run_outreach_auto.py --doctor` prints `WARN_DOCTOR_PARALLEL_SCHEDULER_ACTIVE`, treat it as a scheduler drift warning and remove the overlapping legacy or unmanaged tasks after recovery.

Break-glass installer commands:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\install_scheduled_tasks.ps1 --print-config
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\install_scheduled_tasks.ps1 --dry-run
.\run_with_secrets.ps1 -- powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\install_scheduled_tasks.ps1 --apply
```

Verification:

```powershell
.\run_with_secrets.ps1 -- powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\install_scheduled_tasks.ps1 --verify
```

### Logged-Off Execution Enforcement (Break-Glass Tasks Only)

Only required when using Task Scheduler as a temporary recovery path. Set scheduler credentials in secrets-managed env (password is never printed by `--print-config`):

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\set_outreach_env.ps1 -TaskSchedUser "DESKTOP-Q8QM4N9\lever" -TaskSchedPassword "<TASK_SCHED_PASSWORD>"
```

Apply break-glass installers via secrets wrapper so `TASK_SCHED_USER` / `TASK_SCHED_PASSWORD` are loaded:

```powershell
.\run_with_secrets.ps1 -- powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\install_scheduled_tasks.ps1 --apply
.\run_with_secrets.ps1 -- powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\install_wip_autosave_task.ps1 --apply
.\run_with_secrets.ps1 -- powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\enforce_osha_task_logon_mode.ps1 --apply
```

Verification commands:

```powershell
.\run_with_secrets.ps1 -- powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\install_scheduled_tasks.ps1 --verify
.\run_with_secrets.ps1 -- py -3 run_wally_trial.py --check-schedule
.\run_with_secrets.ps1 -- powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\enforce_osha_task_logon_mode.ps1 --verify
```

Intentional exception:
- `OSHA_WIP_Autosave_Logon` uses an `ONLOGON` trigger by design and cannot run while logged off.

### Recent Signals Troubleshooting

If cold outreach renders `(no recent signals found)`, outreach is working but `${DATA_DIR}\osha.sqlite` has no records in the state/last-14-day window used by `outreach\generate_mailmerge.py`.

One-time Florida catch-up:

```powershell
cd C:\dev\OSHA_Leads
.\run_with_secrets.ps1 -- py -3 run_osha_ingest_daily.py --since-days 45 --states FL
```

Confirm Florida freshness:

```powershell
@'
import sqlite3
conn = sqlite3.connect(r"${DATA_DIR}\osha.sqlite")
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
4. If the evening wrapper emitted a prospect AI-assist dump under `${DATA_DIR}\audits\prospect_ai_assist\`, place the reviewed CSV under `${DATA_DIR}\imports\prospect_ai_assist\` so the pending importer can apply it before the next business-day send pool.
5. Confirm auto summary email arrived at `OSHA_SMOKE_TO` with contacted/skipped/new-replies-trials-conversions.
6. Use `outreach\crm_admin.py mark` to record `replied`, `trial_started`, `converted`, or `do_not_contact`.

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
- Same-day live-send guard token: `TRIAL_SKIP_ALREADY_SENT_TODAY=1 subscriber_key=<key> local_date=<YYYY-MM-DD> guard=ON`
- Emergency override (manual only): pass `--allow-second-live-send-same-day` through `deliver_daily.py` / `send_digest_email.py` (Wally scheduler path remains `run_trial_daily.py`).
- Recipient fan-out stays unchanged: one trial send run still targets both configured recipients (Wally + Brandon).
- Do not temporarily widen `send_window_minutes` for missed trial sends; use the trial catch-up keys/workflow above.

## Trial Framework (Subscriber-Keyed)

Trial daily sends are now subscriber-keyed and backed by a minimal SQLite CRM-light registry plus an append-only send ledger.
Trial policy is 14 weekday sends (Mon-Fri); send-limit is the trial target and weekend/holiday skips extend calendar duration naturally.
Weekend live sends are blocked in code for both the trial sender and Wally manual live-send path by default (`SKIP_NON_WEEKDAY ... gate=trial_weekdays_only`); the emergency override flag is manual-only and scheduled tasks do not pass it.

Canonical semantics (enforced):

- Default trial = `14` weekday sends.
- Trial length is successful weekday digests only (Mon-Fri), not calendar days.
- `trial_state.sends_limit` is the effective max weekday sends allowed.
- `sent_count` is the count of distinct subscriber-local weekday dates with `status=SENT` and `variant=DAILY` (live delivery semantics).
- `expired = (sent_count >= sends_limit)`.
- `sent_rows_raw` is telemetry only and is not used for expiry decisions.
- A `7` calendar-day extension always equals `+5` weekday sends (any 7-day span has 5 weekdays).
- Holidays are not modeled; only weekends are excluded.

Source of truth:

- Subscriber registry + trial latches: `out/crm_light.sqlite` (or `${env:DATA_DIR}\crm_light.sqlite` when `DATA_DIR` is set)
- Send ledger: `send_events` (`TRIAL_SENDS_USED` counts distinct subscriber-local weekday dates for `status=SENT` daily LIVE events to the primary recipient)
- Wally manual/scheduled live sends run through `run_trial_daily.py --subscriber-key wally_trial --send-live`; no manual `append-event` step is required

Split-ledger safety gate:

- Live trial sends can fail with `ERR_TRIAL_LEDGER_SPLIT` when wrapper-resolved runtime points at a canonical CRM DB while repo-local `out\crm_light.sqlite` contains conflicting rows for the same subscriber.
- This guard blocks live sends only (`--send-live`, non-dry-run) and avoids post-expiry drift.
- Reconcile with dry-run then apply:
  - `.\run_with_secrets.ps1 -- py -3 run_trial_admin.py reconcile-ledgers --source-crm-db C:\dev\OSHA_Leads\out\crm_light.sqlite --crm-db C:\osha_data\crm_light.sqlite --scope all --dry-run`
  - `.\run_with_secrets.ps1 -- py -3 run_trial_admin.py reconcile-ledgers --source-crm-db C:\dev\OSHA_Leads\out\crm_light.sqlite --crm-db C:\osha_data\crm_light.sqlite --scope all --apply`

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

Deterministic ZIP->CBSA rebuild command (HUD USPS API token flow; no manual file download path required):

```powershell
cd C:\dev\OSHA_Leads
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\set_outreach_env.ps1 -HudApiToken "<HUD_API_TOKEN>"
py -3 tools\build_zip_cbsa.py --hud-api --hud-year 2026 --hud-quarter 1 --out data\geo\zip_to_cbsa.csv.gz --meta data\geo\cbsa_meta.csv --zip-meta-json data\geo\zip_to_cbsa.meta.json --sources data\geo\SOURCES.md --source-label "HUD USPS ZIP-CBSA 2026 Q1"
```

Operator note: HUD crosswalk file downloads are login-gated on HUD USER. Use the HUD API token flow above (type `3` / `zip-cbsa`) to rebuild deterministic ZIP->CBSA data.
Provenance note: API rebuilds record `HUD USPS ZIP Code Crosswalk Files API (type=3 zip-cbsa), year=<YYYY>, quarter=Q<N>` in `data\geo\SOURCES.md`.

County fallback provenance (`data\geo\county_to_cbsa.csv`):
- Origin/source: curated deterministic county->CBSA rows derived from official U.S. Census/OMB CBSA county delineation sources.
- Generation steps:
1. Select county entries from source delineation tables.
2. Normalize `state` to USPS 2-letter code.
3. Normalize `county` by removing `County` suffix and punctuation.
4. Write explicit `state,county,cbsa` rows to `data\geo\county_to_cbsa.csv`.
- Expected columns: `state`, `county`, `cbsa`.
- Runtime normalization: `state` upper alpha only, `county` upper + collapse spaces + strip `COUNTY`, `cbsa` digits only zero-padded to 5.

## Stripe + Metro Entitlements (CBSA)

Deterministic Stripe plan mapping uses Stripe **price IDs** (no heuristics):

- `STRIPE_PRICE_ID_CORE` -> `plan_code=core` -> `max_metros=4`
- `STRIPE_PRICE_ID_MULTI` -> `plan_code=multi` -> `max_metros=10`
- `STRIPE_PRICE_ID_PILOT` -> `plan_code=pilot` -> `max_metros=4`

Webhook/payload ingestion command (idempotent by Stripe `event_id`):

```powershell
cd C:\dev\OSHA_Leads
.\run_with_secrets.ps1 -- py -3 scripts\subscription_registry_ops.py stripe-ingest --print-config
.\run_with_secrets.ps1 -- py -3 scripts\subscription_registry_ops.py stripe-ingest --stdin-json --dry-run
```

Onboarding entitlement + CBSA allowlist persistence:

```powershell
cd C:\dev\OSHA_Leads
py -3 scripts\subscription_registry_ops.py onboarding-submit --print-config
py -3 scripts\subscription_registry_ops.py onboarding-submit --stdin-json --dry-run
```

Canonical onboarding payload fields (trial and paid onboarding web paths both normalize to this shape):

- `subscriber_key` (optional; derived from admin email when omitted)
- `email` (required admin/billing contact email)
- `plan_code` (`pilot|core|multi`)
- `cbsa_codes` (required list of 5-digit CBSA codes; plan-capped)
- `recipients` (required list of `{ email, name? }`; order preserved, index 0 = primary recipient)

Recipient caps (web validates and backend onboarding-submit enforces authoritatively):

- `pilot`: max `6` recipients
- `core`: max `6` recipients
- `multi`: max `15` recipients

Example dry-run onboarding payload (PowerShell):

```powershell
@'
{
  "subscriber_key": "sub_example",
  "email": "billing@example.com",
  "plan_code": "core",
  "cbsa_codes": ["19100","12420"],
  "recipients": [
    { "email": "ops@example.com", "name": "Ops Lead" },
    { "email": "safety@example.com", "name": "Safety Manager" }
  ]
}
'@ | py -3 scripts\subscription_registry_ops.py onboarding-submit --stdin-json --dry-run
```

Post-onboarding recipient changes:

- Current supported operator path: customer emails support with add/remove recipient names/emails.
- Support updates onboarding recipient configuration and re-runs onboarding-submit (dry-run first, then apply) with the updated `recipients[]`.
- Do not change outreach templates/cadence/scoring while processing recipient-only updates.

Metro match audit command ("present?", "matched?", "if not, why?"):

```powershell
cd C:\dev\OSHA_Leads
py -3 scripts\subscription_registry_ops.py audit-match --inspection 1874533.015 --subscriber-key sub_example --print-config
py -3 scripts\subscription_registry_ops.py audit-match --inspection 1874533.015 --subscriber-key sub_example
```

`audit-match` JSON now includes deterministic CBSA decision fields:
- Establishment geo: `site_city`, `site_zip`, `mail_zip`, `site_county`
- Informational office only: `inspection_office` (not used as a CBSA boundary matcher)
- CBSA resolution: `resolved_cbsa`, `resolution_source`
- Decision tokens: `reason_token`, `unmatched_reason` (empty when matched)

Safety gate:

- Trial subscribers: incomplete ZIP->CBSA dataset emits warning and continues.
- Paid entitlements (`core`/`multi`): send path hard-fails with `ERR_PAID_SEND_DATASET_INCOMPLETE`.

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
- Live smoke previews (`--test-send-daily` without `--dry-run`) append `status=TEST_SENT` and do not advance "since last successful send" subscriber cutoffs.

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
conn = sqlite3.connect(r"${DATA_DIR}\osha.sqlite")
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
