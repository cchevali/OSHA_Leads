# PROJECT_CONTEXT_PACK

PACK_GIT_SHA=d1682092cda74f79177c3c28a447ca81f3130d05
PACK_BUILD_UTC=2026-03-06T20:53:06Z
SOURCE_HASHES: AGENTS.md=44b9b5d2c9be79cae11ade5d73e294ded02880e9bd2846cbcbc86e77641b542d docs/ARCHITECTURE.md=26a00c72663c2703259bc704d1d2c06d7ddfc41455464cfa874ed37efd2ada53 docs/DECISIONS.md=57de77bd96d8df24800db9d1562536f087291c3699961da5058f8b490c50d1c8 docs/PROJECT_BRIEF.md=9568b8e30b88b2b8fcf0e0474a6121059f5cb677d65c78c959cf900e8fdd4bf7 docs/RUNBOOK.md=61e7966107bd946ef0d0a1b241334cb5cff4bd864a70aff5a1df9a5dff142b75 docs/TODO.md=81924f7ffb5e39f27384b64ccb9e3ef161f71bf1c5a7079ab64d6846d1ed0d31 docs/V1_CUSTOMER_VALIDATED.md=edc2cc03c980eb81ca9b72b827193904427468bdb13e7d945fa8a42c2be9ba03
PACK_HASH=8dc9949232d56a3b63f82060676a81a72a15374ee279e0cc34a3de719c1722b8

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
- Start-of-session rule: if the working tree is dirty, run `.\scripts\autosave_wip.ps1` before any scoped task work (or rely on scheduled autosave).
- Install scheduled autosave tasks from repo root with `.\scripts\install_wip_autosave_task.ps1 --apply`.
- Treat WIP branches as the safety net for drift so "only intended changes" remains enforceable.
- All nontrivial work happens on a task branch; `main` stays clean.

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

## Centralized Runtime Control Plane

- Single-writer rule: the canonical Windows PC is the only runtime that may perform live SQLite writes and live sends.
- Runtime guard layer (`runtime_guard.py` + `scripts/scheduled/runtime_guard.ps1`) enforces host/data-root policy before write/send paths.
- Primary scheduled control plane: GitHub Actions workflows run on a label-pinned self-hosted Windows runner (`self-hosted`, `windows`, `osha-pc-canonical`) on the canonical PC.
- Primary scheduler entrypoint: `run_runtime_tick.py`, invoked by `.github/workflows/runtime-tick-selfhosted.yml` every 15 minutes and fanning into due jobs by local time.
- Windows Task Scheduler wrappers are retained only as manual break-glass fallbacks and must not remain enabled in parallel with runtime tick once cutover is complete.
- Wrappers emit deterministic run summaries (`runtime_run_summary_v1`) plus task logs and optional backup manifests.
- Runtime tick emits operator alert candidates and sends live SMTP alerts (recipient `RUNTIME_ALERT_RECIPIENT` fallback `OSHA_SMOKE_TO`) for job failures and critical missed morning windows with per-slot dedupe markers under `${DATA_DIR}\runtime\status\alerts\`.
- Artifact roots:
  - Task logs: `${TASK_LOG_ROOT}` or `${DATA_DIR}\out\task_logs` or `<repo>\out\task_logs`
  - Run summaries: `${RUN_SUMMARY_ROOT}` or `${DATA_DIR}\out\run_summaries` or `<repo>\out\run_summaries`
  - Backup metadata/snapshots: `${BACKUP_ROOT}` or `${DATA_DIR}\out\backups` or `<repo>\out\backups`
  - Optional mirror: `${ARTIFACT_SYNC_DIR}` (artifacts/backups only; never live DB)
- Runtime tick status artifacts live under `${DATA_DIR}\runtime\status\` and include `runtime_latest.json`, `runtime_latest.md`, and per-job status JSON files.
- Laptop/dev clients are read-only operationally: print-config, doctor, dry-run, and artifact inspection.

## Outreach CRM Auto-Run Data Flow

1. Daily prospect replenishment: `run_prospect_replenish_daily.py` runs deterministic pipeline stages in order:
   - `run_prospect_generation.py --doctor`
   - `run_prospect_generation.py`
   - `run_prospect_discovery.py`
   - Wrapper default env posture is `PROSPECT_AUTOGROW_ENABLED=1`, `PROSPECT_AUTOGROW_SOURCES=AIHA,OHS_BG`, `PROSPECT_AUTOGROW_SAFETY_NET_ENABLED=1` when these keys are unset.
   - Optional env-gated auto-growth sources remain AIHA, OHS_BG, APOLLO, BCSP, OSHA_NEWS, and STATE_LIC (`PROSPECT_AUTOGROW_*` keys; `PROSPECT_AUTOGROW_SOURCES` is comma-separated and `PROSPECT_AUTOGROW_STATES` optionally decouples inventory replenishment targets from `OUTREACH_STATES`).
   - APOLLO source uses People Search (`has_email=true` gating) plus Bulk People Enrichment (batches of 10, no waterfall/webhook mode) and is credit-capped per run.
   - APOLLO remains opt-in/overflow and is not in default replenishment sources.
   - BCSP uses plain HTTP parsing (`search_results.php`) and is maintained as a future enrichment input (contact/location only; not directly sendable without employer/domain resolution).
   - OSHA_NEWS uses a lazy-loaded Crawl4AI wrapper (`outreach/scraper_engine.py`) with warning-level degradation when Crawl4AI/Playwright browsers are unavailable.
   - STATE_LIC Phase 1 uses the Texas TDLR public Socrata dataset (`7358-krk7`) and provides licensed-business metadata including address/phone/county fields.
   - Optional generator-stage email enrichment (default off) runs after source fetch and before autogrow filtering to populate existing `website`/`email` fields via domain resolution + pattern guesses.
   - Generation-owned cache/diagnostics live under `${DATA_DIR}/prospect_generation/`.
   - Generator-side BYO CSV inbox paths are removed (manual CSV seed remains available via `outreach/crm_admin.py seed --input ...`).
2. Prospect discovery import: `run_prospect_discovery.py` imports/upserts `${DATA_DIR}/prospect_discovery/prospects_latest.csv` into `crm.sqlite`.
3. Optional bootstrap/debug seed: `outreach/crm_admin.py seed --input <prospects.csv>` loads initial prospects into `crm.sqlite`.
4. Daily run: `outreach/run_outreach_auto.py`
   - Resolves weekday rotation-selected state from `OUTREACH_STATES`, emits `OUTREACH_STATE_ROTATION_SELECTED` / `OUTREACH_STATE_EFFECTIVE_SEND`, and uses effective send-state batch id `<YYYY-MM-DD>_<STATE>` (optional fallback override via `OUTREACH_FALLBACK_ON_EMPTY_STATE=1` when the rotation-selected state is depleted/below floor)
   - Emits `OUTREACH_RAMP_READY` readiness token (manual daily-limit ramping remains operator-controlled)
   - Selects/prioritizes prospects from `prospects` table
   - Enforces suppression + one-click unsubscribe compliance gates
   - Supports a non-sending readiness gate via `--doctor` (secrets/env/config/provider/reachability/dry-run/idempotency checks)
   - Sends multipart outreach emails directly via `send_digest_email.send_email`
   - Records `outreach_events` and prospect status transitions atomically
   - Sends ops summary email to `OSHA_SMOKE_TO`
5. Lifecycle ops: `outreach/crm_admin.py mark` records replied/trial/converted/DNC outcomes.
6. Optional compatibility: append-only ledger at `out/outreach_export_ledger.jsonl`.

## Outreach Debug Export Data Flow

- `outreach/generate_mailmerge.py` remains available to generate outbox CSV + manifest for preview/debug workflows.
- This path is no longer required for normal daily operations.

## Customer/Trial Onboarding Recipient Flow

1. Website forms:
   - Trial requests (`web/app/contact` -> `web/app/api/trial-request`) capture company/admin email, metros, and structured `recipients[]`.
   - Paid onboarding (`web/app/onboarding` -> `web/app/api/onboarding`) captures metros/CBSA selections plus structured `recipients[]`.
2. Web onboarding API invokes `scripts/subscription_registry_ops.py onboarding-submit` (CLI contract preserved with `--print-config` and `--dry-run`).
3. `subscription_registry_ops.py` writes onboarding state into `crm_light` (`subscriber_entitlements`, `subscriber_cbsa`) including canonical `recipients_json`.
4. `send_digest_email.py` loads `crm_light` entitlements/CBSA allowlist and prefers entitlement recipients for per-recipient fan-out delivery (one message per recipient; no To/CC batching).
5. Suppression and unsubscribe remain email-keyed and are enforced per recipient send attempt.

## Operational Artifacts

- `out/crm.sqlite` (or `${DATA_DIR}/crm.sqlite`): prospects/outreach/trials/suppression source of truth
- `out/prospect_discovery/prospects_latest.csv` (or `${DATA_DIR}/prospect_discovery/prospects_latest.csv`): canonical generated discovery feed input
- `out/unsub_tokens.csv`: token store for one-click unsubscribe links (when enabled)
- `out/suppression.csv`: suppression list enforced by exports and sending paths
- `out/crm_light.sqlite` (or `${DATA_DIR}/crm_light.sqlite`): onboarding entitlements + CBSA allowlists + canonical onboarding recipients
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

## ADR-0006: Prospect Generation Feed Standardized Before Discovery

Date: 2026-02-13
Status: Accepted

### Context

Discovery runs were deterministic but frequently had no input file, which left CRM empty and outreach plan pools at zero.
Legacy Wally-era prospecting existed as pool generators and hygiene scripts, but that output was not standardized into discovery's canonical no-arg input path.

### Decision

Adopt a single upstream feed path:

- `run_prospect_generation.py` generates `${DATA_DIR}/prospect_discovery/prospects_latest.csv` (or `./out/prospect_discovery/prospects_latest.csv` when `DATA_DIR` is unset).
- `run_prospect_discovery.py` continues to import/upsert from this feed into `crm.sqlite`.
- Outreach remains CRM-backed (`run_outreach_auto.py`) and unchanged in send/cadence/scoring/compliance behavior.

### Rationale

- Preserves deterministic discovery behavior while making no-arg scheduled runs operationally reliable.
- Keeps CRM as the authoritative pool and prevents drift back to legacy direct-send CSV workflows.
- Reuses existing Wally-era pool generation/hygiene logic without embedding scraping or generation into discovery.

### Consequences

- Daily scheduler flow becomes ingest -> prospect replenishment (doctor + generation + discovery) -> outreach.
- Operators now monitor both `GENERATOR_*` and `DISCOVERY_*` machine-readable outputs.
- Suppression and campaign tracking artifacts remain separate from the discovery feed.

## ADR-0007: Canonical Onboarding Recipients Schema And Recipient-Aware Fan-Out

Date: 2026-02-24
Status: Accepted

### Context

Pricing and onboarding copy already referenced multi-recipient delivery, but public trial and paid onboarding flows did not consistently capture structured recipient data end-to-end. Send fan-out and opt-out plumbing were already email-keyed and recipient-safe, but onboarding ingestion only persisted a single email in entitlement records.

### Decision

- Canonical onboarding recipients schema is `recipients: [{ email, name? }]` for trial request and paid onboarding submissions.
- Paid onboarding retains `email` as admin/billing contact (back-compat and entitlement lookup), while delivery recipients come from `recipients[]`.
- `crm_light.subscriber_entitlements` stores canonical onboarding recipients in `recipients_json` with forward-only migration and legacy `email` fallback.
- Send-time recipient precedence is:
  1. CLI override
  2. `crm_light` entitlement recipients
  3. Legacy subscriber profile recipients/email
  4. Config recipients fallback
- Delivery remains one message per recipient; unsubscribe/suppression semantics remain email-keyed per recipient.

### Rationale

- Aligns website capture and backend persistence with published pricing claims (`Up to 6` / `Up to 15` recipients).
- Preserves privacy (no recipient list exposure via To/CC batching) and existing compliance behavior.
- Minimizes product risk by changing only onboarding capture/registry/send recipient source selection, not outreach templates/cadence/scoring.

### Consequences

- Recipient cap enforcement is now duplicated intentionally (web UI/API + backend authoritative validation).
- Onboarding runbooks and operator docs must document canonical `recipients[]` schema and plan caps.
- Legacy entitlement rows without `recipients_json` continue to work via single-email fallback.

## ADR-0008: OSHA Inspection Detail Caching + Triage Overlay (Rules-First, AI Optional, Default Off)

Date: 2026-02-25
Status: Accepted

### Context

OSHA establishment detail pages expose structured inspection metadata (for example inspection type and case status) that is useful for conservative triage decisions, but the model cannot browse/click those pages directly during runtime. Trial and outreach example-signal presentation also needed a safe way to suppress obviously low-value examples without changing production ranking, cadence, or sending behavior by default.

### Decision

- Add a DATA_DIR-aware OSHA inspection-detail cache (`scoring/osha_detail_cache.py`, `tools/cache_osha_inspection_detail.py`) that fetches and stores raw page content + extracted fields keyed by `activity_nr`.
- Add a rules-first triage overlay (`scoring/triage_overlay.py`) that consumes cached detail fields and produces stable triage decisions.
- Keep AI triage optional, env-gated, and cache-backed (`scoring/ai_triage.py`) with hard caps so AI cannot override severity constraints.
- Integrate the overlay only in render/example selection paths:
  - trial daily digest render (optional, default off)
  - outreach recent-signal example selection/preview annotations (optional, default off)

### Rationale

- Caching makes downstream triage deterministic, auditable, and independent from live page fetches during repeated runs.
- Rules-first design guarantees a conservative baseline without secrets or AI availability.
- Default-off gating preserves existing production behavior and lowers rollout risk.
- Cache-backed AI results improve repeatability and operator trust when AI is explicitly enabled.

### Consequences

- New runtime artifacts are written under DATA_DIR-aware `scoring/`, `trials/<subscriber>/scoring/`, and `outreach/<batch>/` paths.
- Operators must populate OSHA detail cache (or allow programmatic fill) before expecting enriched triage decisions.
- AI triage enablement requires secrets workflow updates and key presence validation, but rules-only overlay remains fully functional.

## ADR-0009: Prospect Autogrow Crawl4AI Runtime Is Lazy And Warning-Level

Date: 2026-02-26
Status: Accepted

### Context

Prospect autogrow is expanding to browser-backed scraping sources (for example BCSP and OSHA_NEWS) using Crawl4AI + Playwright, but existing generator/discovery/outreach flows must not fail on machines that have not completed the one-time browser install step.

### Decision

- Add `outreach/scraper_engine.py` as a shared autogrow scraping wrapper with lazy Crawl4AI imports (no eager import in generator startup paths).
- Treat missing Crawl4AI package or Playwright browsers as warning-level conditions for Crawl4AI-backed sources (`WARN_CRAWL4AI_*`), not hard generator failures.
- Add `run_prospect_generation.py --doctor` as an aggregate readiness command; keep `--apollo-doctor` for backward compatibility.
- Keep `STATE_LIC` Phase 1 on Texas TDLR Socrata API (no browser dependency) so at least one new source remains available without Crawl4AI runtime setup.
- BCSP is implemented as plain HTTP parsing (no Crawl4AI dependency); OSHA_NEWS remains the Crawl4AI-gated autogrow source.
- Centralize zero-cost domain/email enrichment in the generator (default off) instead of source-specific waterfalls; keep Hunter.io as an env-gated stub/cap path until live integration is enabled.

### Rationale

- Preserves non-breaking daily operations on systems that only use legacy sources or API-based sources.
- Makes readiness issues explicit and machine-readable without blocking dry-run/print-config workflows.
- Reduces rollout risk while enabling incremental source expansion.

### Consequences

- Operators must perform a one-time `crawl4ai-setup` when enabling OSHA_NEWS in production.
- Generator output now includes additional readiness/availability tokens in `--print-config` and `--doctor` paths.

## ADR-0010: Centralized Runtime Writer + GitHub Actions Self-Hosted Control Plane

Date: 2026-03-05
Status: Accepted

### Context

Multiple machines were able to run write/send paths, creating split-brain risk for SQLite runtime state and send ledgers.
Operators also needed remote visibility into scheduled outcomes without touching live databases.

### Decision

- Enforce a canonical runtime model:
  - Canonical PC is the only live writer/sender.
  - Runtime preflight guard validates host, role, data-root, and required artifact directories before writes/sends.
- Add deterministic runtime fingerprints and summary artifacts to scheduled/manual wrappers.
- Use GitHub Actions only as control plane visibility, with workflows pinned to a self-hosted Windows runner label on the canonical PC.
- Publish logs/summaries/backups as artifacts for remote inspection.
- Keep backups as snapshot/copy artifacts only; no live DB synchronization.

### Rationale

- Removes split-brain live-write/send failure mode.
- Preserves existing outreach/compliance behavior while improving operability and incident triage.
- Gives laptop operators near-real-time observability through workflow/artifact telemetry without direct DB access.

### Consequences

- Non-canonical live attempts fail fast with deterministic `ERR_RUNTIME_*` tokens.
- Manual live sends now require explicit `--confirm-live-send` unless running in trusted scheduled context.
- Offline self-hosted runner state is visible as queued/failed workflow telemetry and treated as an ops signal.

## ADR-0011: Runtime Tick Workflow Is The Primary Scheduler; Task Scheduler Is Break-Glass Only

Date: 2026-03-06
Status: Accepted

### Context

Keeping both GitHub Actions and Windows Task Scheduler as active schedulers left the system vulnerable to drift in task actions, runtime accounts, Python resolution, and split job ownership.
The runtime hardening work introduced a single orchestrator (`run_runtime_tick.py`) and canonical status artifacts, but the docs still described GitHub Actions as visibility-only.

### Decision

- Make `.github/workflows/runtime-tick-selfhosted.yml` the primary scheduled control plane for live operations on the canonical self-hosted runner.
- Use `run_runtime_tick.py` as the only scheduled orchestrator for:
  - inbound triage
  - AI review dump
  - OSHA ingest
  - prospect replenishment
  - outreach auto-send
  - FACS daily trial send
- Keep Windows Task Scheduler wrappers and installers only for manual break-glass recovery on the canonical PC.
- Do not run Task Scheduler and runtime tick as parallel daily schedulers after cutover.

### Rationale

- One scheduler removes action drift and overlapping ownership of the same live jobs.
- The GitHub workflow gives remote visibility, durable artifacts, and explicit runner health while still executing on the canonical PC.
- Retaining local wrappers preserves a recovery path without keeping two live schedulers in competition.

### Consequences

- Operator docs and runbooks must treat runtime tick as the default daily path.
- Workflow enablement on the default branch becomes part of runtime cutover.
- Task Scheduler credentials and installers remain supported only for recovery scenarios, not as standard daily operations.

## ADR-0012: Runtime Tick Failure Alerts With Slot Dedupe

Date: 2026-03-06
Status: Accepted

### Context

Runtime tick centralized scheduling and status artifacts, but operators still had to poll logs to detect failures or missed morning windows. The system needed high-signal alerts without adding new infrastructure or introducing send loops.

### Decision

- Runtime tick evaluates alert candidates after each run.
- Live-mode SMTP alerts are sent to `RUNTIME_ALERT_RECIPIENT` or fallback `OSHA_SMOKE_TO`.
- Alert categories:
  - `job_failure`: any failed runtime job.
  - `missed_window`: `window_closed_*` skips for `ingest_daily`, `prospect_replenish_daily`, `outreach_auto`, and `trial_facs_daily`.
- Per-slot dedupe markers are written under `${DATA_DIR}\runtime\status\alerts\*.json`.
- Doctor/dry-run modes never send alerts.

### Rationale

- Reuses existing SMTP and operator mailbox setup.
- Avoids duplicate paging every 15 minutes after a window closes.
- Keeps runtime tick failures visible without changing send/prospect business behavior.

### Consequences

- Runtime status artifacts now include alert summary fields.
- Operators can monitor alert state from `runtime_latest.json` plus alert dedupe records.
- Missing SMTP or recipient configuration degrades to non-fatal skipped-alert tokens.
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
- Trial and paid onboarding must capture structured recipient data (`recipients[]`) so recipient-aware delivery is configured deterministically.
- Single-writer runtime invariant: the canonical PC is the only live writer/sender for SQLite state and outbound sends.
- Non-canonical clients (for example laptop) are limited to inspection, dry-run, and artifact review workflows.
- Scheduled execution visibility is provided by GitHub Actions using a self-hosted runner on the canonical PC; actions are control-plane only.

## Do Not Break

- Windows-first operator flow: commands are single copy/paste PowerShell commands from repo root.
- Secrets-required commands run via `.\run_with_secrets.ps1 -- py -3 ...`.
- Automation scripts keep `--print-config` and `--dry-run` behaviors side-effect-safe.
- Preserve List-Unsubscribe + footer opt-out links; do not duplicate unsubscribe links.
- Do not change outreach cadence/scoring/templates/sending behavior during onboarding/registry form work.
- This repo provides operational monitoring and outreach tooling, not legal advice.
- Documentation/process alignment work must not alter product behavior.
```

## docs/RUNBOOK.md
```md
# RUNBOOK

## Canonical Contract

`AGENTS.md` at repo root is the canonical operator + Codex instruction contract.
Use this runbook for executable commands, but resolve policy conflicts in favor of `AGENTS.md`.

## Centralized Runtime Operations

Runtime model:

- Canonical PC is the only live writer/sender for `osha.sqlite`, `crm.sqlite`, `crm_light.sqlite`, and live email sends.
- Laptop/dev clients are limited to `--print-config`, `--doctor`, `--dry-run`, and artifact review.
- GitHub Actions on the label-pinned self-hosted runner (`self-hosted`, `windows`, `osha-pc-canonical`) is the primary scheduled control plane.
- Windows Task Scheduler wrappers remain available for manual break-glass recovery only and must not stay enabled as a parallel daily scheduler once runtime tick is live.

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
- `${DATA_DIR}\runtime\status\jobs\<job>.json`
- `${DATA_DIR}\runtime\status\alerts\*.json` (dedupe markers for sent runtime alerts)

Runtime tick operator alerts:

- Recipient resolution: `RUNTIME_ALERT_RECIPIENT` -> `OSHA_SMOKE_TO`.
- Enablement: `RUNTIME_ALERTS_ENABLED` (`1|0`), default on when a recipient is resolvable.
- Alert categories:
  - `job_failure` for any failed runtime tick job.
  - `missed_window` for skipped `window_closed_*` on `ingest_daily`, `prospect_replenish_daily`, `outreach_auto`, and `trial_facs_daily`.
- Alerts are live-mode only; `--doctor` and `--dry-run` emit candidate/skipped tokens but do not send email.

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
  -SignalFreshnessMaxDays 30 `
  -AiTriageEnabled 0 `
  -AiTriageOpenAiModel gpt-4.1-mini `
  -OutreachFallbackOnEmptyState 0 `
  -OutreachSkipRoleInboxes 1 `
  -ProspectAutoGrowEnabled 1 `
  -ProspectAutoGrowSafetyNetEnabled 1 `
  -ProspectAutoGrowSources AIHA,OHS_BG `
  -ProspectAutoGrowBacklogTarget 60 `
  -ProspectAutoGrowMaxFetchPagesPerRun 6 `
  -ProspectAutoGrowHttpSleepMs 800 `
  -ProspectEnrichDomainEnabled 1 `
  -ProspectEnrichAllowRoleInbox 0 `
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
- Ensures `OUTREACH_FALLBACK_ON_EMPTY_STATE` default `0` and `OUTREACH_SKIP_ROLE_INBOXES` default `1`
- Ensures prospect enrichment defaults include `PROSPECT_ENRICH_DOMAIN_ENABLED=0`, `PROSPECT_ENRICH_HUNTER_ENABLED=0`, and `PROSPECT_ENRICH_ALLOW_ROLE_INBOX=0`
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

### CRM Diagnostics (read-only)

Use these commands instead of inline `py -3 -c "..."` one-liners. PowerShell quoting/escaping around embedded SQL/Python and `<`/`>` is brittle and can fail silently.

```powershell
.\run_with_secrets.ps1 -- py -3 outreach\crm_admin.py stats

.\run_with_secrets.ps1 -- py -3 outreach\crm_admin.py verify-import --csv .\apollo_export.csv
```

### Prospect Replenishment (Scheduled First)

`run_runtime_tick.py` runs replenishment automatically at the daily due window. Use the canonical replenishment wrapper directly only for manual break-glass execution. It runs generation doctor -> generation -> discovery in order:

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

No-arg generation output path:

- `${DATA_DIR}\prospect_discovery\prospects_latest.csv`
- If `DATA_DIR` is unset: `.\out\prospect_discovery\prospects_latest.csv`
- Generator-side BYO CSV inbox paths are removed. Discovery input is now seed pools + autogrow sources only.

Auto-growth (env-gated, optional):

- Canonical keys (no aliases): `PROSPECT_AUTOGROW_ENABLED`, `PROSPECT_AUTOGROW_SAFETY_NET_ENABLED`, `PROSPECT_AUTOGROW_STATES`, `PROSPECT_AUTOGROW_SOURCES`, `PROSPECT_AUTOGROW_BACKLOG_TARGET`, `PROSPECT_AUTOGROW_MAX_FETCH_PAGES_PER_RUN`, `PROSPECT_AUTOGROW_HTTP_SLEEP_MS`.
- Crawl4AI runtime keys (optional, default zero-cost): `PROSPECT_AUTOGROW_LLM_ENABLED` (default `0`), `PROSPECT_AUTOGROW_BCSP_CREDENTIALS`, `PROSPECT_AUTOGROW_BCSP_INDUSTRY`, `PROSPECT_AUTOGROW_STATE_LIC_TX_LICENSE_TYPES`.
- OHS optional auth key (only if buyersguide pagination is work-email gated): `OHS_BG_STORAGE_STATE_PATH` (Playwright storage state JSON path).
- Apollo keys: `APOLLO_API_KEY`, `APOLLO_ENRICH_ENABLED`, `APOLLO_ENRICH_MAX_PER_RUN`, `APOLLO_PERSON_TITLES`, `APOLLO_PERSON_LOCATIONS_MODE`.
- Generator enrichment keys: `PROSPECT_ENRICH_DOMAIN_ENABLED`, `PROSPECT_ENRICH_HUNTER_ENABLED`, `PROSPECT_ENRICH_ALLOW_ROLE_INBOX` (default `0`), `PROSPECT_ENRICH_MAX_SITES_PER_RUN` (default `25`), `PROSPECT_ENRICH_MAX_PAGES_PER_SITE` (default `5`), `PROSPECT_ENRICH_HTTP_SLEEP_MS` (default `750`; when unset, falls back to `PROSPECT_AUTOGROW_HTTP_SLEEP_MS`).
- Source scope: `AIHA`, `OHS_BG`, `APOLLO`, `BCSP`, `OSHA_NEWS`, `STATE_LIC` (comma-separated via `PROSPECT_AUTOGROW_SOURCES`, e.g. `AIHA,OHS_BG,BCSP,STATE_LIC`).
- Cache paths:
  - AIHA: `${DATA_DIR}\prospect_generation\cache\aiha\state_<STATE>.json`
  - OHS_BG: `${DATA_DIR}\prospect_generation\cache\ohs_bg\state_<STATE>.json`
  - APOLLO: `${DATA_DIR}\prospect_generation\cache\apollo\state_<STATE>.json`
  - BCSP: `${DATA_DIR}\prospect_generation\cache\bcsp\state_<STATE>.json`
  - OSHA_NEWS: `${DATA_DIR}\prospect_generation\cache\osha_news\state_<STATE>.json`
  - STATE_LIC: `${DATA_DIR}\prospect_generation\cache\state_lic\state_<STATE>.json`
  - Website enrichment: `${DATA_DIR}\prospect_generation\cache\website_email\<domain>.json` (TTL 14 days)
- Diagnostics path: `${DATA_DIR}\prospect_generation\diagnostics\...`.
- Backlog targeting is evaluated per configured state in `PROSPECT_AUTOGROW_STATES` (runtime default: `OUTREACH_STATES`).
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
- `GENERATOR_AUTOGROW_STATE=<STATE> backlog_current=<n> backlog_sendable_current=<n> new_needed=<n> aiha_candidate=<n> aiha_accepted=<n> ohs_bg_candidate=<n> ohs_bg_accepted=<n> apollo_candidate=<n> apollo_accepted=<n>`
- `GENERATOR_AUTOGROW_STATES`
- `GENERATOR_AUTOGROW_SOURCE_STATE source=<AIHA|OHS_BG|APOLLO|BCSP|OSHA_NEWS|STATE_LIC> state=<STATE> ...`
- `GENERATOR_AIHA_*`
- `GENERATOR_OHS_BG_*`
- `GENERATOR_APOLLO_*`
- `GENERATOR_BCSP_*`, `GENERATOR_OSHA_NEWS_*`, `GENERATOR_STATE_LIC_*`
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
  -OutreachStates TX,CA,FL `
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

- `OUTREACH_STATES=TX,CA,FL`
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

- Rules layer is always on for trial digest signal selection and outreach signal examples.
- AI layer is optional and raise-only. It is evaluated only when `AI_TRIAGE_ENABLED=1` and the path gate is on:
- Trial path gate: `TRIAL_TRIAGE_OVERLAY_ENABLED=1`
- Outreach path gate: `OUTREACH_TRIAGE_OVERLAY_ENABLED=1`
- AI never lowers rules priority and never unsuppresses a rules-suppressed signal.
- AI cache lookup is attempted before OpenAI API access; cached/manual-reviewed priorities can apply even when `OPENAI_API_KEY` is missing.
- Trial/outreach send paths auto-import the newest `ai_review_*.csv` once per process from `C:\osha_data\imports` (fallback `${DATA_DIR}\imports`) unless overridden.
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
py -3 tools\import_ai_triage.py --input .\out\audits\ai_triage_review.csv --dry-run
py -3 tools\import_ai_triage.py --input .\out\audits\ai_triage_review.csv
```

Auto-import notes:

- Runtime auto-import emits one of: `AI_REVIEW_AUTO_IMPORT_APPLIED`, `WARN_AI_REVIEW_AUTO_IMPORT_MISSING`, `WARN_AI_REVIEW_AUTO_IMPORT_STALE`, or `WARN_AI_REVIEW_AUTO_IMPORT_INVALID`.
- With defaults, only files modified within the last 24 hours are auto-imported.
- Manual `tools\import_ai_triage.py` remains supported for deterministic operator backfills/re-runs.

### Nightly AI triage dump (manual)

Canonical manual command path (always loads secrets/DATA_DIR via wrapper):

```powershell
cd C:\dev\OSHA_Leads
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\dump_signals_for_ai_review.ps1
```

Evening scheduler note:

- `scripts\scheduled\run_osha_ingest_evening.ps1` runs ingest with `--scope-mode outreach_plus_trial_live` before dumping AI review signals.
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

- The prep script auto-selects the newest `ai_review_*.csv` from `C:\osha_data\imports` (fallback: `${DATA_DIR}\imports`) unless `-AiReviewCsv` is passed.
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

### Task Scheduler (Break-Glass Only)

Do not keep Windows Task Scheduler as an active parallel scheduler once `runtime-tick-selfhosted.yml` is live on the canonical runner.
Use Task Scheduler only for temporary local recovery when GitHub Actions on the canonical PC is unavailable.

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

- [ ] After any PR/commit that changes docs/contracts/templates/workflow (or any time `WARN_CONTEXT_PACK_STALE` appears): run build + fingerprint + upload + mark-uploaded + check (in that order).
- [ ] Install the registered GitHub self-hosted runner on the canonical PC as a Windows service from an elevated shell so it survives reboot/logoff.
- [ ] Set runtime role keys on canonical PC via `scripts\set_outreach_env.ps1` (`RUNTIME_ROLE=canonical_scheduler`, `CANONICAL_HOSTNAME=<pc-hostname>`) and verify with wrapper `--print-config` paths.
- [ ] Set optional `ARTIFACT_SYNC_DIR` (for example OneDrive artifacts folder) via `scripts\set_outreach_env.ps1` and confirm mirrors for task logs/run summaries/backups.
- [ ] Provision Gmail OAuth client JSON for inbound triage: create `secrets/gmail_credentials.json` (Google Cloud Console -> APIs -> Gmail API -> OAuth 2.0 Client ID (Desktop app) -> Download JSON).
- [ ] Set outreach conversion URL for trial emails: set `TRIAL_CONVERSION_URL` via `scripts\set_outreach_env.ps1` and verify `trial_conversion_url_present=YES` via `run_wally_trial.py --print-config`.
- [ ] If enabling AI triage, set `AI_TRIAGE_ENABLED` / `AI_TRIAGE_OPENAI_MODEL` via `scripts\set_outreach_env.ps1` and load `OPENAI_API_KEY` in the shell first (no manual `.env` / `.env.sops` edits).
- [ ] If OHS buyersguide multi-page replenishment is needed, refresh a valid Playwright storage-state file and set `OHS_BG_STORAGE_STATE_PATH` via `scripts\set_outreach_env.ps1`.

- [ ] Ensure email provider account/sender credentials are configured for production and validated with daily doctor checks (`run_outreach_auto.py --doctor`).

## Codex-owned engineering backlog

- [ ] Add follow-on autogrow sources on top of `outreach/scraper_engine.py` foundation: `AGC`, `BLUEBOOK`, `THOMASNET`, `BBB` (source modules + fixtures + generator tests).
- [ ] Add integration test coverage that validates workflow artifact upload path patterns against generated wrapper outputs (`out/task_logs`, `out/run_summaries`, `out/backups`).
- [ ] Wire landing page conversion CTA references to paid path after Stripe link is set.
  Reference points: `web/config/site.json`, `web/components/CTAButtons.tsx`, `web/app/pricing/page.tsx`, `web/app/contact/page.tsx`.
- [ ] Define trial -> paid email-only sequence using existing lifecycle states (`replied`, `trial_started`, `converted`) and conversion artifacts in `run_trial_daily.py`.
- [ ] Add operator KPI log for reply -> trial_started -> converted by batch id.
- [ ] Review suppression + bounce/complaint handling (data source, dedupe policy, freshness policy, and operator SOP alignment).
- [ ] Add periodic archive/retention cleanup for outreach dry-run artifacts under `out/outreach/<batch>/`.
- [ ] Add periodic readiness report snapshot generation for weekly operations review.

## Done

- 2026-03-06: Registered and verified repo self-hosted runner `desktop-q8qm4n9-runtime` on the canonical PC with labels `self-hosted`, `Windows`, `X64`, `osha-pc-canonical`. Verified by successful job pickup from `Runtime Tick (Self-Hosted)` workflow dispatch on `main`.
- 2026-02-15: Completed outbound sender domain verification (SPF, DKIM, DMARC) for `microflowops.com`. DNS records published; test email confirmed `spf=pass`, `dkim=pass`, `dmarc=pass` with aligned domains. Verification commands added to `docs/RUNBOOK.md` under "Deliverability Preflight".
- 2026-02-12: Set website Stripe payment link in `web/config/site.json` (`stripePaymentLink`) and wire it into `web/app/pricing/page.tsx` + `web/app/contact/page.tsx` (commit `54c2a3c6`).

## Deliverability Verification Snippet (Regression Check)

```powershell
# SPF
nslookup -type=TXT microflowops.com 8.8.8.8
# Expect: v=spf1 include:zoho.com ~all (or equivalent)

# DMARC
nslookup -type=TXT _dmarc.microflowops.com 8.8.8.8
# Expect: v=DMARC1; p=none; ... (or p=quarantine/reject)

# DKIM (Zoho selector)
nslookup -type=TXT zoho._domainkey.microflowops.com 8.8.8.8
# Expect: v=DKIM1; k=rsa; p=<public_key>
```
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
