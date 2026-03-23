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

1. Daily prospect replenishment safety net: `run_prospect_replenish_daily.py` runs deterministic pipeline stages in order:
   - `run_prospect_generation.py --doctor`
   - `run_prospect_generation.py`
   - `run_prospect_discovery.py`
   - Replenishment remains enabled as the automated background safety net; manual Deep Research is the canonical operator lane for net-new prospects.
   - Replenishment owns generation plus discovery only; it does not generate manual research prep artifacts.
   - Wrapper default env posture is `PROSPECT_AUTOGROW_ENABLED=1`, `PROSPECT_AUTOGROW_SOURCES=AIHA`, `PROSPECT_AUTOGROW_SAFETY_NET_ENABLED=1`, `PROSPECT_AI_ASSIST_REVIEW_ENABLED=1`, and `PROSPECT_AI_ASSIST_REVIEW_RAW_TARGET=30` when these keys are unset.
   - Canonical live prospect/outreach scope is `TX,CA,FL,PA,OH`.
   - Auto-growth source support is registry-backed by `outreach/autogrow_source_registry.json`; implemented tokens currently remain AIHA, BLUEBOOK, OHS_BG, APOLLO, BCSP, OSHA_NEWS, and STATE_LIC (`PROSPECT_AUTOGROW_*` keys; `PROSPECT_AUTOGROW_SOURCES` is comma-separated and `PROSPECT_AUTOGROW_STATES` optionally decouples inventory replenishment targets from `OUTREACH_STATES`, though canonical production keeps it unset so `OUTREACH_STATES` remains the single scope of truth).
   - Planned tokens such as `BBB`, `THOMASNET`, and `AGC` are intentionally rejected by env/runtime validation until their source modules exist.
   - APOLLO source uses People Search (`has_email=true` gating) plus Bulk People Enrichment (batches of 10, no waterfall/webhook mode) and is credit-capped per run.
   - APOLLO remains opt-in/overflow and is not in default replenishment sources.
   - BCSP uses plain HTTP parsing (`search_results.php`) and remains implemented but outside the canonical production source list until state-scoped searches produce net-new accepted rows; doctor/probe output now reports state-search readiness instead of shallow base-page reachability.
   - OSHA_NEWS uses a lazy-loaded Crawl4AI wrapper (`outreach/scraper_engine.py`) with warning-level degradation when Crawl4AI/Playwright browsers are unavailable.
   - STATE_LIC Phase 1 uses the Texas TDLR public Socrata dataset (`7358-krk7`) and provides licensed-business metadata including address/phone/county fields.
   - STATE_LIC now uses one shared precision policy (`outreach/state_lic_precision.py`) with explicit `consultant_fit`, `packet_eligible`, and `send_eligible` modes so generator policy, cache annotation, and the AI-assist packet lane do not drift.
   - `STATE_LIC` remains explicitly TX-only; PA/OH are live states via manual Deep Research plus multistate-capable sources such as `AIHA`, `OHS_BG`, `BCSP`, and `OSHA_NEWS`.
   - Generator/backlog behavior still relies on `consultant_fit`; broad TX contractor inventory remains cached and observable, but only qualifying consultant-fit rows can flow through generator promotion/backlog credit.
   - Generator-stage enrichment can promote qualifying consultant-fit `STATE_LIC` rows to persisted `STATE_LIC_WORK_EMAIL`, which stays in the `STATE_LIC` source family but is the only STATE_LIC variant that defaults to send-eligible.
   - Canonical automated safety-net discovery currently runs on `AIHA` only. The directory-to-website public-contact path remains the discovery policy for `AIHA` and explicitly configured directory sources such as `BLUEBOOK`: keep a valid non-free source email when present, otherwise crawl the source-provided website (`/`, `/contact`, `/contact-us`, `/about`, `/about-us`, `/team`, `/our-team`) for a public business email, otherwise leave email blank.
   - `BLUEBOOK` remains implemented but is not part of canonical defaults while public-mode listing access is captcha-blocked; it should only return to defaults after an approved access path exists.
   - Guessed domains, guessed emails, and Hunter/provider lookups remain noncanonical/manual or secondary-lane tooling; they are not part of the canonical `AIHA` default lane.
   - Optional generator-stage email enrichment (default off) still exists for noncanonical/secondary sources and is bounded by `PROSPECT_ENRICH_MAX_SITES_PER_RUN` plus `PROSPECT_ENRICH_HTTP_SLEEP_MS` so large source pulls do not stall the full replenish run.
   - Generation-owned cache/diagnostics live under `${DATA_DIR}/prospect_generation/`.
   - Generator-side BYO CSV inbox paths are removed (manual CSV seed remains available via `outreach/crm_admin.py seed --input ...`).
2. Prospect discovery import: `run_prospect_discovery.py` imports/upserts `${DATA_DIR}/prospect_discovery/prospects_latest.csv` into `crm.sqlite`.
3. Nightly manual research prep: `scripts/scheduled/run_osha_ingest_evening.ps1` runs OSHA ingest, `tools/dump_signals_for_review.py`, and `tools/prepare_manual_prospect_research.py` at the shared 8:45 PM Eastern slot.
   - Signals dumps land under `${DATA_DIR}\audits\signals_ai_review\`.
   - Manual prospect prep lands under `${DATA_DIR}\audits\prospect_ai_assist\` as a refreshed `crm_skip_list_for_ai.csv` plus a dated repo-managed Deep Research prompt artifact.
   - The prompt artifact bakes in the active state scope, target firm count, canonical CSV header, and the explicit TX-only `STATE_LIC` diagnostic.
4. Controlled discovery augmentation: reviewed prospect CSVs belong under `${DATA_DIR}\imports\prospect_ai_assist\` and are imported oldest-first by `tools/import_prospect_ai_assist_review.py --pending` before live outreach sends; single-file/manual review remains valid via `--input`, and pasted Deep Research CSV is accepted via `--stdin` or the clipboard wrapper.
   - Stdin imports accept only raw CSV or a single fenced `csv` block with the canonical header `state,decision,firm,website,contact_name,title,email,source_urls,confidence,evidence_snippet` (optional trailing `seed_id` still allowed for legacy packet reviews).
   - The importer normalizes known markdown/mailto cell corruption, rejects ambiguous malformed rows, rejects accepted rows outside the active `OUTREACH_STATES` scope, and re-checks duplicates against CRM by email, root domain, and normalized firm key before any upsert.
   - Every row is audited in `crm.sqlite`, and verified accepts still upsert through the existing discovery/CRM contract with `source=ai_assist_manual` and `enrichment_lane=ai_assist`.
5. Optional bootstrap/debug seed: `outreach/crm_admin.py seed --input <prospects.csv>` loads initial prospects into `crm.sqlite`.
6. Daily run: `outreach/run_outreach_auto.py`
   - Resolves weekday rotation-selected state from `OUTREACH_STATES`, emits `OUTREACH_STATE_ROTATION_SELECTED` / `OUTREACH_STATE_EFFECTIVE_SEND`, and uses effective send-state batch id `<YYYY-MM-DD>_<STATE>` (optional fallback override via `OUTREACH_FALLBACK_ON_EMPTY_STATE=1` when the rotation-selected state is depleted/below floor)
   - Emits `OUTREACH_RAMP_READY` readiness token (manual daily-limit ramping remains operator-controlled)
   - Selects/prioritizes prospects from `prospects` table
   - Enforces suppression + one-click unsubscribe compliance gates
   - Supports a non-sending readiness gate via `--doctor` (secrets/env/config/provider/reachability/dry-run/idempotency checks)
   - Sends multipart outreach emails directly via `send_digest_email.send_email`
   - Records `outreach_events` and prospect status transitions atomically
   - Sends ops summary email to `OSHA_SMOKE_TO`
6. Lifecycle ops: `outreach/crm_admin.py mark` records replied/trial/converted/DNC outcomes.
7. Optional compatibility: append-only ledger at `out/outreach_export_ledger.jsonl`.

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
- `${DATA_DIR}/outreach/ops_snapshots/*.json`: persisted operator artifact combining ops KPIs with runtime/suppression readiness state
- `${DATA_DIR}/runtime/status/jobs/*.json`: runtime scheduler state for latest slot evaluation and external scheduler drift visibility
- `${DATA_DIR}/audits/prospect_ai_assist/crm_skip_list_for_ai.csv`: nightly/manual Deep Research skip list
- `${DATA_DIR}/audits/prospect_ai_assist/manual_prospect_deep_research_YYYYMMDD.txt`: dated repo-managed Deep Research prompt artifact

## V1 Preserved Invariants

- Suppression and opt-out controls are mandatory send/export gates.
- List-Unsubscribe headers and footer opt-out behavior are preserved compliance markers.
- Dry-run behavior remains no-send and side-effect-safe for live channels.
- Lead identity/dedupe semantics preserve first-observed behavior to avoid repeat "new" leads.
- Documentation consolidation (including legacy archival) does not change outreach behavior.
