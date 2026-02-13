# PROJECT_CONTEXT_PACK

PACK_GIT_SHA=54c2a3c629aaee1412433f9545ce78f07c9e94a1
PACK_BUILD_UTC=2026-02-13T03:45:40Z
SOURCE_HASHES: AGENTS.md=d2938dfe71a714cb52b1e192a68e6da17e6f4a3c55c97f5d65f3c6cabb87b799 docs/ARCHITECTURE.md=00f51d1659f2245e395c6541d5c7c7d64fe22481b51f4f432b0fd1353b690510 docs/DECISIONS.md=5a1fbc744dc2dfb5e4f55a2e8dc8ab23e8d984c4d25d7c6438402a9c090b4e6f docs/PROJECT_BRIEF.md=a32d87e6fafaf55e584ed4dfc2d4d3d5aa0251c978e87323464c099cd453eb90 docs/RUNBOOK.md=da680afdb4d18c12358d816e810ed679fb61c1af3288c60f0e48e747a2e76ccf docs/TODO.md=85c1cd674e0cfe349d9239e88245d6e34e4f8410d59ddd6863262ffeb3adf0e2 docs/V1_CUSTOMER_VALIDATED.md=edc2cc03c980eb81ca9b72b827193904427468bdb13e7d945fa8a42c2be9ba03
PACK_HASH=7ab48ba601047a3ed1a2eeb24b6f8c0beba80d71f17aca87727f73dc28ddc32c

Generated from canonical repo docs. Upload this single file to ChatGPT Project Settings -> Files.

## AGENTS.md
```md
# AGENTS Contract (Canonical)

## Mission
Operate OSHA_Leads as a compliant, Windows-first outbound + trial operations system without changing product behavior unless explicitly requested in a Task Packet.

## Product
OSHA_Leads provides operational monitoring, alerting, and outreach support for business contacts. It does not provide legal advice.

## Buyers/Offer
Primary buyers are operators and safety-facing teams that need timely OSHA-related signals and disciplined outbound trial operations.

## Execution Strategy
- Use Task Packets as implementation source of truth.
- Prefer minimal, non-breaking changes.
- Keep durable context in repo docs, not chat history.
- Choose one highest-odds execution path; no forks/options in planner output.

## Guardrails
- No legal advice content.
- Enforce suppression, opt-out handling, and outreach logging.
- Preserve List-Unsubscribe headers and footer opt-out links.
- Never duplicate unsubscribe links in outbound content.
- Do not change outreach cadence, scoring, enrichment, templates, or sending logic unless explicitly required.
- Use PowerShell `Select-String` or Python for search in this repo; avoid non-default tooling such as `rg`.

## Operator CLI Contract (Windows-First)
- Every operator action is a single copy/pasteable PowerShell command from repo root (for example `C:\dev\OSHA_Leads`).
- If secrets are required, command pattern is:
  - `.\run_with_secrets.ps1 -- py -3 <script> [args]`
- Automation entrypoints must expose:
  - `--print-config` for side-effect-free resolved config output.
  - `--dry-run` for no-send/no-live-side-effect execution and no partial artifact behavior.

## Secrets/SOPS Guardrail
- Do not rely on interactive SOPS editor mode.
- Do not commit `.env.sops` unless explicitly instructed.
- Missing/invalid secret or config states must emit clear `ERR_*` tokens.
- Use the no-editor helper flow documented in `docs/RUNBOOK.md` (including `scripts\set_outreach_env.ps1`).

## Template Integrity Rule
- Do not modify outreach email copy/templates during docs/process-only tasks.
- Preserve compliance markers and unsubscribe behavior in all template-related changes.

## Repo Context Spine
- `docs/PROJECT_BRIEF.md`: mission, positioning, invariants, do-not-break list.
- `docs/ARCHITECTURE.md`: system boundaries and data flow.
- `docs/DECISIONS.md`: ADR history and policy decisions.
- `docs/RUNBOOK.md`: canonical operator commands and verification steps.

## Task Packet Standard
- Treat the current Task Packet as binding scope and acceptance source.
- Do not ask for extra context unless a referenced file is missing.
- Keep changes tightly scoped to stated goals/non-goals.

## Required Codex Output Format
- Changed files list
- Summary (<= 8 lines)
- Commands run (commands only)
- Remaining TODOs (<= 5 bullets)

## Acceptance Gates
- `py -3 -m unittest -q` must exit `0`.
- `git status --porcelain` must show only intended task-related changes (no stray artifacts).
- Docs integrity checks:
  - `AGENTS.md` exists at repo root.
  - Spine docs reference `AGENTS.md` as canonical instruction contract.
  - `docs/TODO.md` contains both human-only and Codex-owned sections.

## Single Source Of Truth Workflow
- `AGENTS.md` is the canonical instruction contract.
- ChatGPT Project Instructions should remain a thin wrapper that points to this file.
- When `AGENTS.md` changes, re-upload updated `AGENTS.md` to ChatGPT Project Files.
```

## docs/ARCHITECTURE.md
```md
# Architecture

## Instruction Authority

`AGENTS.md` at repo root is the canonical instruction contract for operator and Codex workflows.
Operator command procedures remain in `docs/RUNBOOK.md` under that contract.

## Modules (High Level)

- Ingest + data store: OSHA inspections -> `data/osha.sqlite`
- Digest delivery: build customer-facing alerts and send to subscribers
- Suppression/opt-out: local suppression list (`out/suppression.csv`) and optional one-click unsubscribe service
- Outreach operations (this repo): SQLite CRM-lite (`out/crm.sqlite`) for prospect selection, sending, and lifecycle tracking
- Outreach debug export: optional CSV outbox generation for QA/debug only

## Outreach CRM Auto-Run Data Flow

1. Seed/import: `outreach/crm_admin.py seed --input <prospects.csv>` loads initial prospects into `crm.sqlite`.
2. Daily run: `outreach/run_outreach_auto.py`
   - Resolves daily state from `OUTREACH_STATES` and batch id `<YYYY-MM-DD>_<STATE>`
   - Selects/prioritizes prospects from `prospects` table
   - Enforces suppression + one-click unsubscribe compliance gates
   - Supports a non-sending readiness gate via `--doctor` (secrets/env/config/provider/reachability/dry-run/idempotency checks)
   - Sends multipart outreach emails directly via `send_digest_email.send_email`
   - Records `outreach_events` and prospect status transitions atomically
   - Sends ops summary email to `OSHA_SMOKE_TO`
3. Lifecycle ops: `outreach/crm_admin.py mark` records replied/trial/converted/DNC outcomes.
4. Optional compatibility: append-only ledger at `out/outreach_export_ledger.jsonl`.

## Outreach Debug Export Data Flow

- `outreach/generate_mailmerge.py` remains available to generate outbox CSV + manifest for preview/debug workflows.
- This path is no longer required for normal daily operations.

## Operational Artifacts

- `out/crm.sqlite` (or `${DATA_DIR}/crm.sqlite`): prospects/outreach/trials/suppression source of truth
- `out/unsub_tokens.csv`: token store for one-click unsubscribe links (when enabled)
- `out/suppression.csv`: suppression list enforced by exports and sending paths
- `out/outreach_export_ledger.jsonl`: optional compatibility ledger for contacted records
- `out/outreach/<batch>/outbox_*_dry_run.csv` + manifest: non-sending artifact output from `run_outreach_auto.py --dry-run`

## V1 Preserved Invariants

- Suppression and opt-out controls are mandatory send/export gates.
- List-Unsubscribe headers and footer opt-out behavior are preserved compliance markers.
- Dry-run behavior remains no-send and side-effect-safe for live channels.
- Lead identity/dedupe semantics preserve first-observed behavior to avoid repeat "new" leads.
- Documentation consolidation (including legacy archival) does not change outreach behavior.
```

## docs/DECISIONS.md
```md
# Decisions (ADRs)

## ADR Template

Use this format for new entries:

- Date: YYYY-MM-DD
- Status: Proposed | Accepted | Superseded
- Context
- Decision
- Rationale
- Consequences

## ADR-0001: Outbound Via Mail-Merge Export (Not In-App Sending)

Date: 2026-02-10
Status: Superseded by ADR-0002

### Context

We need a fast, compliant outbound motion to validate demand by geography/batch, without building a full CRM or deliverability stack inside this repo.

### Decision (Historical)

Outbound outreach was initially executed via **mail-merge CSV exports**:

- `outreach/generate_mailmerge.py` produced an outbox CSV (subject/body + opt-out link fields)
- External sending was done outside this codebase

### Rationale

- Deliverability and sending ops are easier to iterate outside the product codebase
- Faster iteration on copy + targeting
- Keeps this repo focused on ingestion/alerts and compliance primitives (suppression + opt-out)

### Consequences (Superseded)

- We must log exports (counts + batch metadata) for auditing and measurement
- Suppression/opt-out enforcement becomes a hard gate for export generation

## ADR-0002: CRM-Lite SQLite As Outreach Source Of Truth

Date: 2026-02-11

### Context

CSV-driven outreach required repeated manual file handling and did not provide durable lifecycle state (contacted/replied/trial/converted) in one place.

### Decision

Daily outreach operations move to a SQLite CRM-lite database (`crm.sqlite`):

- `prospects`, `outreach_events`, `suppression`, and `trials` tables are the operational source of truth
- `run_outreach_auto.py` performs select -> prioritize -> send -> record directly from SQLite
- `crm_admin.py` handles initial CSV seed/import and lifecycle status marking
- CSV outbox generation remains only as a debug/export utility

### Rationale

- Deterministic, auditable no-repeat contact behavior by `prospect_id`
- Transactional event + status writes after each daily send cycle
- Lower operator overhead (no daily CSV dependency)
- Easier pipeline extension to future ingestion and analytics

### Consequences

- Daily runs require a seeded `crm.sqlite` and suppression file at startup
- Operator workflow now includes lifecycle updates via `crm_admin.py mark`
- Existing mail-merge export paths remain available but are non-operational by default

## ADR-0003: Outreach Doctor-First Operations Gate

Date: 2026-02-12

### Context

Operational readiness checks were spread across runbook steps and did not exist as a single machine-verifiable command.
This made it easy to miss env/config/dependency drift before scheduled sends.

### Decision

`run_outreach_auto.py` provides a single `--doctor` command that validates:

- secrets decrypt tooling
- required outreach env keys and value formats
- CRM presence/schema
- suppression presence/readability/freshness
- unsubscribe base URL configuration and reachability
- outbound provider configuration
- dry-run outbox/manifest artifact generation
- idempotency/no-repeat guard behavior

### Rationale

- One command gives a deterministic pass/fail gate before unattended daily operation.
- Stable `PASS_DOCTOR_*` and `ERR_DOCTOR_*` tokens make scheduling/ops checks scriptable.
- Keeps compliance controls centralized in the operational entrypoint.

### Consequences

- Operators can use `run_outreach_auto.py --doctor` as the first daily command and task-health probe.
- Misconfiguration now fails fast with explicit machine-readable tokens.

## ADR-0004: AGENTS.md As Canonical Instruction Contract

Date: 2026-02-12
Status: Accepted

### Context

Instruction and workflow expectations were spread across chat/project instruction surfaces and several docs.
This increased drift risk and made it harder to enforce a single operational contract.

### Decision

Adopt repo-root `AGENTS.md` as the canonical instruction contract for Codex and operator workflows.

### Rationale

- Centralizes execution and compliance guardrails in one repo-tracked document.
- Reduces ambiguity between chat instructions and durable repository context.
- Improves repeatability of Windows-first operator procedures.

### Consequences

- The docs spine (`PROJECT_BRIEF`, `ARCHITECTURE`, `DECISIONS`, `RUNBOOK`) references `AGENTS.md` as canonical.
- Task Packets are evaluated against `AGENTS.md` acceptance gates.
- ChatGPT Project Instructions remain a thin wrapper that points to `AGENTS.md`.

## ADR-0005: V1 Capsule Canonicalization and Legacy Doc Archival

Date: 2026-02-12
Status: Accepted

### Context

Customer-validated V1 knowledge was spread across multiple legacy markdown files at repo root.
Those files contained useful operational truths but created authority drift versus the spine docs.

### Decision

Adopt `docs/V1_CUSTOMER_VALIDATED.md` as the canonical V1 requirements capsule and archive the prior V1 markdown files under `docs/legacy/` with explicit deprecation headers that point to canonical docs.

### Rationale

- Preserve validated V1 operational truths without keeping multiple competing authorities.
- Keep current operator/system authority concentrated in `AGENTS.md` and the docs spine.
- Retain historical artifacts for auditability and traceability.

### Consequences

- Legacy V1 files remain available only as historical artifacts in `docs/legacy/`.
- `docs/V1_CUSTOMER_VALIDATED.md` becomes the canonical bridge between historical V1 behavior and current spine docs.
- `PROJECT_CONTEXT_PACK.md` generation includes the V1 capsule to keep single-file upload workflows complete.
```

## docs/PROJECT_BRIEF.md
```md
﻿# Project Brief

Canonical instruction authority: `AGENTS.md` at repo root.

## What This Is

OSHA_Leads is an intelligence + alerting system for operational teams.

- No legal advice. We provide monitoring, summaries, and operational heads-ups only.
- Business contacts only. No personal/sensitive enrichment.

## Current Priority: Outbound Concierge (Growth Engine)

The current growth engine is an "outbound concierge" motion with CRM auto-run as the operational default:

- Discover and prioritize prospects into `out/crm.sqlite`.
- Run daily outreach automation via `run_outreach_auto.py` (select -> prioritize -> send -> record).
- Use mail-merge CSV generation as a debug/compatibility path, not the default send path.
- Process replies manually and mark lifecycle events (`replied`, `trial_started`, `converted`, `do_not_contact`).

Weekly target (initial): **100-200 new prospects/week**.

Success metric (funnel): **reply -> call -> paid** (track conversion per batch).

## Compliance & Invariants

- All outreach exports/sends must include an opt-out mechanism.
- Suppression must be enforced for all exports and sends (email and, where available, domain).

## Do Not Break

- Windows-first operator flow: commands are single copy/paste PowerShell commands from repo root.
- Secrets-required commands run via `.\run_with_secrets.ps1 -- py -3 ...`.
- Automation scripts keep `--print-config` and `--dry-run` behaviors side-effect-safe.
- Preserve List-Unsubscribe + footer opt-out links; do not duplicate unsubscribe links.
- This repo provides operational monitoring and outreach tooling, not legal advice.
- Documentation/process alignment work must not alter product behavior.
```

## docs/RUNBOOK.md
```md
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
```

## docs/TODO.md
```md
# TODO

Policy: move completed items to `Done` with date (`YYYY-MM-DD`) and a short evidence note.

Durability rule: when Chase adds a new human-only setup step in chat, Codex must add it here instead of letting it live only in chat.

## Human-only (UI/credentials)

- [ ] After any doc/contract change: rebuild + upload `PROJECT_CONTEXT_PACK.md` + mark uploaded (`py -3 tools\project_context_pack.py --build`, upload in ChatGPT Project Settings -> Files, `py -3 tools\project_context_pack.py --mark-uploaded`).
- [ ] Create Stripe payment link URL and set it.
  Stripe Dashboard -> Payment Links -> Create payment link -> Select product/price -> Configure recurring monthly billing -> Collect customer email -> Copy Payment Link URL -> Set `TRIAL_CONVERSION_URL` via `scripts\set_outreach_env.ps1` -> Verify `trial_conversion_url_present=YES` via `run_wally_trial.py --print-config` -> After payment redirect: `https://microflowops.com/onboarding`.
- [ ] Complete outbound sender domain setup and verification (SPF, DKIM, DMARC, domain/DNS alignment, and `FROM_EMAIL`/`SMTP_USER` alignment).
- [ ] Ensure email provider account/sender credentials are configured for production and validated with daily doctor checks (`run_outreach_auto.py --doctor`).

## Codex-owned engineering backlog

- [ ] Wire landing page conversion CTA references to paid path after Stripe link is set.
  Reference points: `web/config/site.json`, `web/components/CTAButtons.tsx`, `web/app/pricing/page.tsx`, `web/app/contact/page.tsx`.
- [ ] Define trial -> paid email-only sequence using existing lifecycle states (`replied`, `trial_started`, `converted`) and conversion artifacts in `run_trial_daily.py`.
- [ ] Add operator KPI log for reply -> trial_started -> converted by batch id.
- [ ] Review suppression + bounce/complaint handling (data source, dedupe policy, freshness policy, and operator SOP alignment).
- [ ] Add periodic archive/retention cleanup for outreach dry-run artifacts under `out/outreach/<batch>/`.
- [ ] Add periodic readiness report snapshot generation for weekly operations review.

## Done

- [ ] (empty)
```

## docs/V1_CUSTOMER_VALIDATED.md
```md
# V1 Customer Validated (Canonical)

Canonical instruction authority remains `AGENTS.md` at repo root.

Purpose: preserve customer-validated V1 requirements and operator truths while deprecating legacy standalone docs.

## Source Snapshot (Legacy V1 Docs)

### `docs/legacy/COLD_EMAIL_README.md` (historical path: `COLD_EMAIL_README.md`)
- Last commit touching source: `704355f` (2026-02-02, Chase Chevali) "Cold email: require reply-to, enforce suppression, set footer address"
- What this doc asserts:
  - Outbound send path uses `outbound_cold_email.py` with dry-run/live modes, score-tier selection, and send logging in `out/cold_email_log.csv`.
  - Inbound reply handling uses `inbound_inbox_triage.py` with Gmail OAuth, message classification, suppression updates, and daily digest notification.
  - Outbound sends must enforce suppression checks before send.
  - Live outbound footer must include real mailing address and unsubscribe option.
  - Operator-visible artifacts include `out/cold_email_log.csv`, `out/inbox_triage_log.csv`, `out/inbox_state.json`, and `out/suppression.csv`.

### `docs/legacy/COLD_EMAIL_IMPLEMENTATION_PLAN.md` (historical path: `COLD_EMAIL_IMPLEMENTATION_PLAN.md`)
- Last commit touching source: `a3cb531` (2026-02-02, Chase Chevali) "Docs: switch paths to C:\\dev\\OSHA_Leads"
- What this doc asserts:
  - Outbound V1 selection logic is deterministic and score-tiered (`>=8`, then `>=6`, then `>=4`) with recency preference.
  - Recipient input contract includes `email`, `first_name`, `last_name`, `firm_name`, `segment`, `state_pref`.
  - V1 outbound content includes sample leads with urgency cues and compliance footer controls.
  - Inbound V1 triage classes include unsubscribe, bounce, interested, question, bug/feature, out-of-office, and other.
  - Reply classification drives concrete actions: suppression updates, notifications, drafts, and engineering tickets.

### `docs/legacy/CUSTOMER_ONBOARDING.md` (historical path: `CUSTOMER_ONBOARDING.md`)
- Last commit touching source: `a3cb531` (2026-02-02, Chase Chevali) "Docs: switch paths to C:\\dev\\OSHA_Leads"
- What this doc asserts:
  - New-customer onboarding is configuration-driven via `customers/*.json` and does not require code edits.
  - Onboarding sequence requires dry-run verification before first live send.
  - Early V1 used pilot mode controls to restrict recipients before full production rollout.
  - Operator verification includes log/artifact checks after first send.
  - Daily scheduling is an explicit operator responsibility after successful first send.

### `docs/legacy/TARGET_LIST_FACTORY_STATUS.md` (historical path: `TARGET_LIST_FACTORY_STATUS.md`)
- Last commit touching source: `a3cb531` (2026-02-02, Chase Chevali) "Docs: switch paths to C:\\dev\\OSHA_Leads"
- What this doc asserts:
  - Target list factory workflow is file-first and depends on CSV tracking plus dedupe normalization.
  - V1 sourcing relies on repeatable industry directories and explicit territory quotas.
  - Prospect quality controls include role normalization, duplicate-domain handling, and status lifecycle codes.
  - Operator output includes a prioritized outreach-ready subset and a deduped master list.
  - Dedupe script behavior is deterministic and no-external-dependency.

### `docs/legacy/lead_definition_v0_1.md` (historical path: `lead_definition_v0_1.md`)
- Last commit touching source: `251b30a` (2026-02-02, Chase Chevali) "Initial commit"
- What this doc asserts:
  - Canonical lead identity is inspection-level (`lead_id = osha:inspection:{activity_nr}`).
  - "New lead" status is tied to `first_seen_at` recency and required-field completeness.
  - Sendable lead minimum fields include inspection id, establishment, state, city/zip, open date, and source URL.
  - Re-ingest updates existing rows without creating duplicates and preserves existing non-null data.
  - Scoring is deterministic rule-based ranking with explicit point contributions.

### `docs/legacy/PROJECT_STATUS_REPORT.md` (historical path: `PROJECT_STATUS_REPORT.md`)
- Last commit touching source: `251b30a` (2026-02-02, Chase Chevali) "Initial commit"
- What this doc asserts:
  - V1 outbound and inbound automation were both operational as an MVP pipeline.
  - Compliance and deliverability gates include sender identity alignment, mailing-address validation, and unsubscribe support.
  - Freshness gates block outbound when pipeline/signal age thresholds are exceeded.
  - Outbound kill switch is an explicit runtime control and defaults safe.
  - Production readiness depends on identity, OAuth, and unsubscribe endpoint completion.

### `docs/legacy/PROSPECTING_SOP.md` (historical path: `PROSPECTING_SOP.md`)
- Last commit touching source: `251b30a` (2026-02-02, Chase Chevali) "Initial commit"
- What this doc asserts:
  - Territory prospecting targets a minimum of 30 qualified prospects with no duplicate domains.
  - V1 sourcing pipeline uses three repeatable source categories.
  - Dedupe/normalize is required after each batch and standardizes domain/state/role fields.
  - Contact priority is decision-maker first (owner/executive, then safety leaders, then operations/compliance).
  - Handoff requires a quality checklist before outreach execution.

### `docs/legacy/SESSION_HANDOFF.md` (historical path: `SESSION_HANDOFF.md`)
- Last commit touching source: `251b30a` (2026-02-02, Chase Chevali) "Initial commit"
- What this doc asserts:
  - V1 ingestion, bundle generation, delivery, and metrics tracking were active operator paths.
  - V1 trial/customer config was subscriber and territory scoped with daily operation expectations.
  - Email behavior included pilot gating, suppression checks, list-unsubscribe headers, and multipart delivery.
  - Daily operator workflow required explicit SMTP environment setup and daily run execution.
  - Expansion and production hardening were expected follow-on operations.

## What landed the first interested customer

- A territory-first target list process produced outreach-ready prospects from repeatable sources with domain dedupe and role prioritization.
- Outbound messaging sent recent OSHA signal samples with state context, urgency cues, and clear reply path.
- Compliance controls were present at send time: suppression enforcement, list-unsubscribe behavior, and physical-address footer.
- Inbound triage converted replies into actions quickly: immediate interested notifications, unsubscribe/bounce suppression updates, and operator digesting.
- Trial/onboarding path was config-driven and fast enough for same-session customer setup plus first-send validation.

## V1 workflow (targeting -> copy -> send -> handling replies -> trial -> conversion)

1. Targeting
   - Build territory prospect pools from repeatable directories.
   - Normalize/dedupe prospects by domain and prioritize decision-maker roles.
   - Require minimum prospect completeness before handoff.
2. Copy
   - Generate outreach with 2-5 recent OSHA lead examples.
   - Include sender identity, reply-to, and compliance footer markers.
3. Send
   - Enforce suppression before send.
   - Respect daily send caps/rate limits and log each send event.
   - Keep kill-switch and dry-run as first-class controls.
4. Handling replies
   - Poll inbox, classify response intent, and write suppression updates for unsubscribes/bounces.
   - Trigger immediate notification for interested replies.
   - Generate structured follow-up artifacts (digest/drafts/tickets) for operator action.
5. Trial
   - Onboard customer via config file with territory/state and recipient settings.
   - Run dry-run validation first, then perform controlled baseline/daily sends.
   - Verify logs/artifacts after initial delivery.
6. Conversion
   - Track lifecycle transitions (`replied`, `trial_started`, `converted`, `do_not_contact`) as explicit operational state.
   - Preserve opt-out/suppression handling throughout lifecycle transitions.

## V1 lead definition criteria and high-signal heuristics

### Lead definition criteria

- Canonical inspection identity is activity-number based and unique.
- "New" is first-observed recency (`first_seen_at` window), not repeated observations.
- Sendable lead requires: `activity_nr`, `establishment_name`, `site_state`, (`site_city` or `site_zip`), `date_opened`, `source_url`.
- Missing required fields force review path and exclusion from sendable output.
- Re-ingest updates observation metadata and fills nulls without duplicate row creation.

### High-signal heuristics

- Score-based prioritization is deterministic and rule-based.
- Signal tiers prioritize higher score bands first; fallback to lower bands only when needed for volume.
- Inspection-type weighting and recency are primary ordering factors.
- Additional weighting signals include construction NAICS, violations presence, and emphasis program markers.

## V1 onboarding steps and required operator actions

1. Create customer config from template and set required fields (customer id, geography, windows, recipients).
2. Execute dry-run delivery and require all validation checks to pass before live send.
3. Confirm send controls are set correctly (pilot restrictions for early trial phases; production controls when promoted).
4. Run first baseline/live send through the canonical entrypoint.
5. Verify send artifacts/logs and recipient receipt.
6. Schedule daily operation and monitor run outputs.
7. Record lifecycle outcomes and suppression/opt-out events as ongoing operator work.

## V1 invariants

- Suppression and opt-out handling are hard gates for all outreach sends.
- List-Unsubscribe headers and footer opt-out behavior must be preserved.
- No duplicate unsubscribe-link behavior is allowed in outbound content.
- Dry-run must remain side-effect-safe; live send requires explicit operator intent.
- Lead identity/dedupe must remain deterministic; first observation semantics must not regress.
- Freshness/readiness gates must block stale or misconfigured send operations.
- Operator flow must stay Windows-first with single copy/pasteable commands from repo root.
- Documentation/process changes must not change outreach behavior.

## Where this lives now

| V1 requirement/process | Canonical location(s) now | Notes |
|---|---|---|
| Windows-first operator execution and secrets wrapper contract | `docs/RUNBOOK.md`, `AGENTS.md` | Canonical command style and secrets flow are centralized there. |
| Outbound operations flow with suppression/one-click gates and doctor sequence | `docs/RUNBOOK.md`, `docs/ARCHITECTURE.md` | Runbook is operator procedure; Architecture is boundary/data-flow reference. |
| Compliance invariants (suppression, opt-out, no duplicate unsubscribe links) | `AGENTS.md`, `docs/PROJECT_BRIEF.md`, `docs/ARCHITECTURE.md` | Policy + invariant split across contract and architecture summary. |
| CRM-lite outreach source-of-truth and lifecycle recording | `docs/ARCHITECTURE.md`, `docs/DECISIONS.md` | ADR-0002 captures source-of-truth decision rationale. |
| Doctor-first readiness gating | `docs/RUNBOOK.md`, `docs/DECISIONS.md` | ADR-0003 plus daily operator command sequence. |
| Lead dedupe and first-seen semantics for digest/trial operations | `docs/RUNBOOK.md`, `docs/ARCHITECTURE.md` | Runbook details operator checks; architecture now summarizes invariant. |
| Project context single-file upload workflow | `docs/RUNBOOK.md`, `tools/project_context_pack.py` | Runbook is operator-facing; tooling enforces source inputs. |
| Canonical authority of `AGENTS.md` and spine alignment | `docs/PROJECT_BRIEF.md`, `docs/ARCHITECTURE.md`, `docs/DECISIONS.md`, `docs/RUNBOOK.md` | ADR-0004 defines contract authority decision. |

## Legacy -> Canonical pointers

| Legacy file (archived) | Canonical replacement | Use archived copy for |
|---|---|---|
| `docs/legacy/COLD_EMAIL_README.md` | `docs/V1_CUSTOMER_VALIDATED.md`, `docs/RUNBOOK.md`, `docs/ARCHITECTURE.md` | Historical setup examples and early environment notes. |
| `docs/legacy/COLD_EMAIL_IMPLEMENTATION_PLAN.md` | `docs/V1_CUSTOMER_VALIDATED.md`, `docs/ARCHITECTURE.md`, `docs/DECISIONS.md` | Historical implementation intent before later ADRs. |
| `docs/legacy/CUSTOMER_ONBOARDING.md` | `docs/V1_CUSTOMER_VALIDATED.md`, `docs/RUNBOOK.md` | Historical onboarding checklist wording. |
| `docs/legacy/TARGET_LIST_FACTORY_STATUS.md` | `docs/V1_CUSTOMER_VALIDATED.md`, `docs/ARCHITECTURE.md` | Historical rollout/status snapshot for target-list factory. |
| `docs/legacy/lead_definition_v0_1.md` | `docs/V1_CUSTOMER_VALIDATED.md`, `docs/RUNBOOK.md`, `docs/ARCHITECTURE.md` | Original scoring/lead-definition statement. |
| `docs/legacy/PROJECT_STATUS_REPORT.md` | `docs/V1_CUSTOMER_VALIDATED.md`, `docs/DECISIONS.md` | Historical readiness snapshot and blockers. |
| `docs/legacy/PROSPECTING_SOP.md` | `docs/V1_CUSTOMER_VALIDATED.md`, `docs/RUNBOOK.md` | Historical collection heuristics and role mapping detail. |
| `docs/legacy/SESSION_HANDOFF.md` | `docs/V1_CUSTOMER_VALIDATED.md`, `docs/RUNBOOK.md`, `docs/ARCHITECTURE.md` | Historical handoff context and dated operational state. |
```
