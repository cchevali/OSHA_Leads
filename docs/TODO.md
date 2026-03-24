# TODO

Policy: move completed items to `Done` with date (`YYYY-MM-DD`) and a short evidence note.

Durability rule: when Chase adds a new human-only setup step in chat, Codex must add it here instead of letting it live only in chat.

## Human-only (UI/credentials)

- [ ] After any PR/commit that changes docs/contracts/templates/workflow (or any time `WARN_CONTEXT_PACK_STALE` appears): run build + fingerprint + upload + mark-uploaded + check (in that order).
- [ ] Set optional `ARTIFACT_SYNC_DIR` (for example OneDrive artifacts folder) via `scripts\set_outreach_env.ps1` and confirm mirrors for task logs/run summaries/backups.
- [ ] Provision Gmail OAuth client JSON for inbound triage: create `secrets/gmail_credentials.json` (Google Cloud Console -> APIs -> Gmail API -> OAuth 2.0 Client ID (Desktop app) -> Download JSON).
- [ ] Set outreach conversion URL for trial emails: set `TRIAL_CONVERSION_URL` via `scripts\set_outreach_env.ps1` and verify `trial_conversion_url_present=YES` via `run_wally_trial.py --print-config`.
- [ ] If enabling AI triage, set `AI_TRIAGE_ENABLED` / `AI_TRIAGE_OPENAI_MODEL` via `scripts\set_outreach_env.ps1` and load `OPENAI_API_KEY` in the shell first (no manual `.env` / `.env.sops` edits).
- [ ] If OHS buyersguide multi-page replenishment is needed, refresh a valid Playwright storage-state file and set `OHS_BG_STORAGE_STATE_PATH` via `scripts\set_outreach_env.ps1`.

- [ ] Before travel, verify the Windows-native RDP path from the laptop to the canonical PC over the existing secure access layer; disconnect, reconnect, and confirm usable resolution/performance.
- [ ] Before travel, confirm the canonical PC stays awake, network-connected, and reachable after disconnect/reconnect.
- [ ] Before travel, run `powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\verify_travel_readiness.ps1 --dry-run` on the laptop and resolve any failing step before leaving.

## Codex-owned engineering backlog

- [ ] Open the next source packet around an accessible second discovery lane that preserves the directory-to-website contact policy; `BLUEBOOK` stays nondefault until listing access is operationally approved.
- [ ] Add follow-on autogrow source modules on top of the registry-backed `outreach/scraper_engine.py` foundation: `BBB`, `THOMASNET`, `AGC` (source modules + fixtures + generator tests). Planned tokens now fail fast until implemented.
- [ ] Define trial -> paid email-only sequence using existing lifecycle states (`replied`, `trial_started`, `converted`) and conversion artifacts in `run_trial_daily.py`.
- [ ] Add an operator-triggered schedule for `outreach\run_ops_snapshot.py` and `outreach\cleanup_outreach_dry_run_artifacts.py` on the canonical PC or runner.
- [ ] Review complaint/FBL intake handling separately from the now-codified bounce + suppression path; provider complaint signals are still human/manual today.

## Done

- 2026-03-24: Unified evening AI-review scheduling so runtime tick now owns `ingest_evening`, the shared `${DATA_DIR}\runtime\config\schedule_overrides.json` seam controls the operator-visible HH:MM everywhere, the separate evening self-hosted workflow is dispatch-only break-glass/manual, and the evening wrapper skips the same slot when runtime tick already completed it. Evidence: `outreach/run_runtime_tick.py`, `scripts/scheduled/run_osha_ingest_evening.ps1`, `.github/workflows/ingest-evening-ai-review-selfhosted.yml`, `tests_run_runtime_tick.py`, `tests_run_runtime_tick_wrapper.py`, `tests_run_osha_ingest_evening_wrapper.py`, `docs/RUNBOOK.md`, `docs/ARCHITECTURE.md`.
- 2026-03-24: Added the local-only `MicroFlowOps Ops Console` in `ops_console/` with dashboard/outreach/schedule/state-scope/trials/manual-import/inbox/audit screens, preview-before-apply mutation gating, console audit artifacts, shared schedule override seam, `run_trial_admin.py --send-time-local` defaulting, and the repo-root launcher `scripts\run_ops_console.ps1`. Evidence: `ops_console/`, `runtime_schedule_config.py`, `run_trial_admin.py`, `outreach/run_runtime_tick.py`, `scripts/install_scheduled_tasks.ps1`, `scripts/set_outreach_env.ps1`, `tests_ops_console.py`, `tests_runtime_schedule_config.py`, `tests_run_runtime_tick.py`, `tests_install_scheduled_tasks.py`, `test_trial_audit_and_admin.py`, `docs/RUNBOOK.md`, `docs/ARCHITECTURE.md`.
- 2026-03-16: Added a shared `STATE_LIC` precision policy with aligned `consultant_fit` / `packet_eligible` / `send_eligible` modes, hard-negative TX HVAC class suppression, adaptive packet caps, and additive `seed_id` / `seed_index.json` provenance so AI-assist review outcomes can feed exact 7-day packet feedback without widening live send behavior. Evidence: `outreach/state_lic_precision.py`, `outreach/prospect_sources_state_lic.py`, `outreach/run_prospect_generation.py`, `outreach/crm_store.py`, `tools/dump_prospect_ai_assist_review.py`, `tools/import_prospect_ai_assist_review.py`, `tests_state_lic_precision.py`, `tests_prospect_sources_state_lic.py`, `tests_tools_prospect_ai_assist.py`, `docs/ARCHITECTURE.md`, `docs/RUNBOOK.md`.
- 2026-03-10: Cut over morning scheduling so the self-hosted `runtime-tick-selfhosted.yml` path remains the canonical live executor while managed Task Scheduler morning wrappers run as enabled safety nets with same-slot skip behavior and scheduler status emits runner/Python/topology health tokens. Evidence: `scripts/install_scheduled_tasks.ps1`, `scripts/scheduled/runtime_guard.ps1`, `scripts/scheduled/run_outreach_auto.ps1`, `scripts/scheduled/run_prospect_replenish_daily.ps1`, `scripts/scheduled/run_trial_facs_daily.ps1`, `outreach/run_runtime_tick.py`, `tests_install_scheduled_tasks.py`, `tests_runtime_guard_ps1.py`, `tests_run_runtime_tick.py`.
- 2026-03-10: Standardized scheduled wrapper Python resolution and canonical artifact roots under `${DATA_DIR}\out\...` so scheduled preflight, task logs, run summaries, and runtime-tick reconciliation agree in unattended runs. Evidence: `scripts/scheduled/runtime_guard.ps1`, `scripts/scheduled/runtime_run_summary.ps1`, `scripts/scheduled/run_osha_ingest_daily.ps1`, `scripts/scheduled/run_prospect_replenish_daily.ps1`, `scripts/scheduled/run_outreach_auto.ps1`, `scripts/scheduled/run_trial_facs_daily.ps1`, `tests_runtime_guard_ps1.py`, `tests_runtime_run_summary.py`, `tests_run_runtime_tick_wrapper.py`.
- 2026-03-10: Completed canonical DB cutover so live runtime state now uses only `C:\osha_data\osha.sqlite`, `C:\osha_data\crm.sqlite`, and `C:\osha_data\crm_light.sqlite`; repo-local live-like DB copies are quarantined under `out\backups\legacy_db_quarantine\...`, runtime preflight blocks new legacy drift, and trial ledger reconcile runs into canonical before quarantine. Evidence: `outreach/run_runtime_state_migrate.py`, `runtime_guard.py`, `run_trial_daily.py`, `tests_run_runtime_state_migrate.py`, `tests_runtime_guard.py`, `test_trial_status.py`.
- 2026-03-10: Verified the registered GitHub self-hosted runner is installed as a Windows service on the canonical PC and currently running as `actions.runner.cchevali-OSHA_Leads.desktop-q8qm4n9-runtime`. Evidence: `powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\install_scheduled_tasks.ps1 --status`.
- 2026-03-10: Verified canonical runtime-role keys on the canonical PC (`RUNTIME_ROLE=canonical_scheduler`, `CANONICAL_HOSTNAME=desktop-q8qm4n9`) via wrapper-backed config/doctor output. Evidence: `.\run_with_secrets.ps1 -- py -3 run_outreach_auto.py --print-config`, `.\run_with_secrets.ps1 -- py -3 run_runtime_tick.py --doctor`.
- 2026-03-10: Verified production outreach sender/provider configuration via doctor checks, including provider config, scheduler alignment, and no-send artifact coverage. Evidence: `.\run_with_secrets.ps1 -- py -3 run_outreach_auto.py --doctor`.
- 2026-03-09: Hardened `outreach/run_runtime_tick.py` so `${DATA_DIR}\runtime\status\jobs\*.json` persists latest slot evaluation for ran/skipped/reconciled jobs, reconciles same-slot wrapper summaries before `missed_window`, and emits `WARN_RUNTIME_TICK_EXTERNAL_SCHEDULER` plus reconciliation metadata when break-glass wrappers are detected. Covered by `tests_run_runtime_tick.py`.
- 2026-03-09: Added workflow contract coverage that keeps runtime tick as the only scheduled live workflow and validates artifact upload roots for canonical/runtime and manual wrapper paths. Evidence: `tests_run_runtime_tick_wrapper.py`.
- 2026-03-09: Added canonical autogrow source registry `outreach/autogrow_source_registry.json` and wired env/runtime validation so unknown tokens fail as `invalid_*` and planned-but-unimplemented tokens fail as `unimplemented_*`. Evidence: `outreach/source_policy.py`, `outreach/run_prospect_generation.py`, `scripts/set_outreach_env.ps1`, `tests_source_policy_registry.py`.
- 2026-03-09: Added persisted weekly-style ops/readiness snapshot generation via `outreach/run_ops_snapshot.py` and stale dry-run artifact retention cleanup via `outreach/cleanup_outreach_dry_run_artifacts.py`. Evidence: `tests_run_ops_snapshot.py`, `tests_cleanup_outreach_dry_run_artifacts.py`.
- 2026-03-09: Closed the old KPI-log backlog framing; reply -> `trial_started` -> `converted` by batch/state/source-family already exists in `outreach/ops_report.py`, and the durable exported artifact path is now `outreach/run_ops_snapshot.py`. Evidence: `tests_outreach_ops_report.py`.
- 2026-03-09: Codified bounce/suppression alignment in code and docs for hard-bounce suppression writes, soft-bounce event-only behavior, suppression freshness doctor checks, and operator snapshot visibility. Evidence: `outreach/import_bounces_imap.py`, `outreach/run_outreach_auto.py`, `outreach/run_ops_snapshot.py`, `tests_import_bounces_imap.py`.
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
