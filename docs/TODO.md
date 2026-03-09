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
- [ ] Before travel, verify the Windows-native RDP path from the laptop to the canonical PC over the existing secure access layer; disconnect, reconnect, and confirm usable resolution/performance.
- [ ] Before travel, confirm the canonical PC stays awake, network-connected, and reachable after disconnect/reconnect.
- [ ] Before travel, run `powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\verify_travel_readiness.ps1 --dry-run` on the laptop and resolve any failing step before leaving.

## Codex-owned engineering backlog

- [ ] Add follow-on autogrow source modules on top of the registry-backed `outreach/scraper_engine.py` foundation: `BBB`, `BLUEBOOK`, `THOMASNET`, `AGC` (source modules + fixtures + generator tests). Planned tokens now fail fast until implemented.
- [ ] Define trial -> paid email-only sequence using existing lifecycle states (`replied`, `trial_started`, `converted`) and conversion artifacts in `run_trial_daily.py`.
- [ ] Add an operator-triggered schedule for `outreach\run_ops_snapshot.py` and `outreach\cleanup_outreach_dry_run_artifacts.py` on the canonical PC or runner.
- [ ] Review complaint/FBL intake handling separately from the now-codified bounce + suppression path; provider complaint signals are still human/manual today.

## Done

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
