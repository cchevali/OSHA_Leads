# Deploy to Vercel (microflowops.com)

## Project settings

1. Import the Git repo in Vercel and select the production branch (recommended: `main`).
2. Framework preset: `Next.js`.
3. Root Directory: `web`.
4. Install/Build/Output: leave as Vercel defaults (no custom commands).
5. Production Overrides: clear/remove any overrides so Production uses the same settings as the project defaults.
6. Node.js version: use Vercel default (Node 18+).
7. Environment variables (Production only):
8. `NEXT_PUBLIC_PLAUSIBLE_ENABLED=true`
9. `NEXT_PUBLIC_PLAUSIBLE_DOMAIN=microflowops.com`
10. `NEXT_PUBLIC_SITE_HOST=microflowops.com`
11. `WEB_SMTP_HOST=<your_smtp_host>`
12. `WEB_SMTP_PORT=<your_smtp_port>`
13. `WEB_SMTP_USER=<your_smtp_user>`
14. `WEB_SMTP_PASS=<your_smtp_password>`
15. `WEB_SMTP_FROM=support@microflowops.com`
16. `WEB_TRIAL_TO=support@microflowops.com` (optional, defaults to support@microflowops.com)

Verification note:
- Successful deploy logs must show Next.js build output (routes/app output), and the Deployment URL must render `/` (not Vercel `NOT_FOUND`).

## DNS

1. In Vercel, add the domains `microflowops.com` and `www.microflowops.com` to the project.
2. In your DNS provider, set an `A` record for `@` to `76.76.21.21`.
3. Set a `CNAME` record for `www` to `cname.vercel-dns.com`.
4. Wait for Vercel domain verification to complete.
5. Confirm `www.microflowops.com` redirects (301) to `microflowops.com`.

## Post-deploy checks

1. Verify the homepage loads and CTAs render.
2. Submit `/contact` trial form and confirm the success message appears.
3. Confirm support notification and requester confirmation emails are delivered.
4. Confirm the "Email us directly" copy button works and mailto remains optional.
5. Confirm `/sitemap.xml` and `/robots.txt` resolve.
6. Validate the OpenGraph card with Vercel’s social preview.
7. Update `web/config/site.json` if the brand or email address changes.

## Go-Live Gate

Before touching DNS, run the local readiness gate:

```powershell
cd C:\dev\OSHA_Leads
cmd /c "cd web && npm.cmd run gate"
```

If it prints `PASS`, the remaining blocker is DNS + Vercel domain validation (use `DOMAIN_DOCTOR_RUNBOOK.md` + `domain_doctor.py`), then run `LAUNCH_CHECKLIST.md` on production.
