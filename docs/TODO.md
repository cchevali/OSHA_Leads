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
