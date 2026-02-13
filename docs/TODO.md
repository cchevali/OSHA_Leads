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
