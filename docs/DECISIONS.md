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

- Daily scheduler flow becomes generation -> discovery -> outreach.
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
