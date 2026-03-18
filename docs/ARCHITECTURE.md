# Architecture

## Instruction Authority

`AGENTS.md` at repo root is the canonical instruction contract for operator and Codex workflows.
Operator command procedures remain in `docs/RUNBOOK.md` under that contract.

## Modules (High Level)

- Ingest + data store: OSHA inspections -> `${DATA_DIR}\osha.sqlite`
- Digest delivery: build customer-facing alerts and send to subscribers
- Suppression/opt-out: local suppression list (`out/suppression.csv`) and optional one-click unsubscribe service
- Outreach operations (this repo): SQLite CRM-lite (`out/crm.sqlite`) for prospect selection, sending, and lifecycle tracking
- Outreach debug export: optional CSV outbox generation for QA/debug only

## Centralized Runtime Control Plane

- Single-writer rule: the canonical Windows PC is the only runtime that may perform live SQLite writes and live sends.
- Runtime guard layer (`runtime_guard.py` + `scripts/scheduled/runtime_guard.ps1`) enforces host/data-root policy before write/send paths.
- Primary scheduled control plane: GitHub Actions workflows run on a label-pinned self-hosted Windows runner (`self-hosted`, `windows`, `osha-pc-canonical`) on the canonical PC.
- Primary scheduler entrypoint: `run_runtime_tick.py`, invoked by `.github/workflows/runtime-tick-selfhosted.yml` every 15 minutes and fanning into due jobs by local time.
- Windows Task Scheduler wrappers remain as managed safety-net recovery tasks on the canonical PC; runtime tick stays primary, and duplicate or legacy scheduler entries are treated as drift.
- Wrappers emit deterministic run summaries (`runtime_run_summary_v1`) plus task logs and optional backup manifests.
- Runtime tick emits operator alert candidates and sends live SMTP alerts (recipient `RUNTIME_ALERT_RECIPIENT` fallback `OSHA_SMOKE_TO`) for job failures and critical missed morning windows with per-slot dedupe markers under `${DATA_DIR}\runtime\status\alerts\`.
- Runtime tick persists per-job status for the latest ran/skipped/reconciled slot under `${DATA_DIR}\runtime\status\jobs\*.json` and records external-wrapper reconciliation metadata when break-glass execution is detected.
- Artifact roots:
  - Task logs: `${TASK_LOG_ROOT}` or `${DATA_DIR}\out\task_logs` or `<repo>\out\task_logs`
  - Run summaries: `${RUN_SUMMARY_ROOT}` or `${DATA_DIR}\out\run_summaries` or `<repo>\out\run_summaries`
  - Backup metadata/snapshots: `${BACKUP_ROOT}` or `${DATA_DIR}\out\backups` or `<repo>\out\backups`
  - Optional mirror: `${ARTIFACT_SYNC_DIR}` (artifacts/backups only; never live DB)
- Runtime tick status artifacts live under `${DATA_DIR}\runtime\status\` and include `runtime_latest.json`, `runtime_latest.md`, and per-job status JSON files.
- Laptop/dev clients are read-only operationally: print-config, doctor, dry-run, and artifact inspection.

## Outreach CRM Auto-Run Data Flow

1. Prospect intake is manual-only.
   - Supported intake is a manually researched CSV or an AI-chat-prepared CSV reviewed by a human operator.
   - Supported import command is `outreach/crm_admin.py seed --input <prospects.csv>`.
   - Scraping, directory harvests, generated source feeds, and prospect AI-assist packet/import flows are retired.
2. Daily run: `outreach/run_outreach_auto.py`
   - Resolves weekday rotation-selected state from `OUTREACH_STATES`, emits `OUTREACH_STATE_ROTATION_SELECTED` / `OUTREACH_STATE_EFFECTIVE_SEND`, and uses effective send-state batch id `<YYYY-MM-DD>_<STATE>` (optional fallback override via `OUTREACH_FALLBACK_ON_EMPTY_STATE=1` when the rotation-selected state is depleted/below floor)
   - Emits `OUTREACH_RAMP_READY` readiness token (manual daily-limit ramping remains operator-controlled)
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
- `out/unsub_tokens.csv`: token store for one-click unsubscribe links (when enabled)
- `out/suppression.csv`: suppression list enforced by exports and sending paths
- `out/crm_light.sqlite` (or `${DATA_DIR}/crm_light.sqlite`): onboarding entitlements + CBSA allowlists + canonical onboarding recipients
- `out/outreach_export_ledger.jsonl`: optional compatibility ledger for contacted records
- `out/outreach/<batch>/outbox_*_dry_run.csv` + manifest: non-sending artifact output from `run_outreach_auto.py --dry-run`
- `${DATA_DIR}/outreach/ops_snapshots/*.json`: persisted operator artifact combining ops KPIs with runtime/suppression readiness state
- `${DATA_DIR}/runtime/status/jobs/*.json`: runtime scheduler state for latest slot evaluation and external scheduler drift visibility

## V1 Preserved Invariants

- Suppression and opt-out controls are mandatory send/export gates.
- List-Unsubscribe headers and footer opt-out behavior are preserved compliance markers.
- Dry-run behavior remains no-send and side-effect-safe for live channels.
- Lead identity/dedupe semantics preserve first-observed behavior to avoid repeat "new" leads.
- Documentation consolidation (including legacy archival) does not change outreach behavior.
- Prospect intake is manual-only as of 2026-03-18; manual CSV import via `outreach\crm_admin.py seed` remains the supported path.
