# TODO

Policy: move completed items to `Done` with date (`YYYY-MM-DD`) and a short evidence note.

Durability rule: when Chase adds a new human-only setup step in chat, Codex must add it here instead of letting it live only in chat.

## Human-only (UI/credentials)

- [ ] After any PR/commit that changes docs/contracts/templates/workflow (or any time `WARN_CONTEXT_PACK_STALE` appears): run build + fingerprint + upload + mark-uploaded + check (in that order).
- [ ] Provision Gmail OAuth client JSON for inbound triage: create `secrets/gmail_credentials.json` (Google Cloud Console -> APIs -> Gmail API -> OAuth 2.0 Client ID (Desktop app) -> Download JSON).
- [ ] Set outreach conversion URL for trial emails: set `TRIAL_CONVERSION_URL` via `scripts\set_outreach_env.ps1` and verify `trial_conversion_url_present=YES` via `run_wally_trial.py --print-config`.

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

- 2026-02-19: Implemented WIP autosave hardening: 15-minute scheduled autosave, installer `--status` contract (`WIP_AUTOSAVE_*` tokens), and non-elevated logon self-heal reminder task/log path (`out/wip_autosave_logon_reminder.log`) with elevated remediation command output (commit `762b5856`).
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
