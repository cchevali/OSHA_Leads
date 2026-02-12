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

