# Production Launch Checklist (microflowops.com)

## 1) Vercel Project Settings

1. Vercel project Root Directory: `web`
2. Install Command: `npm install`
3. Build Command: `npm run build`
4. Output Directory: leave blank

## 2) Environment Variables (Production Only)

Set these in Vercel -> Project -> Settings -> Environment Variables:

1. `NEXT_PUBLIC_PLAUSIBLE_ENABLED=true`
2. `NEXT_PUBLIC_PLAUSIBLE_DOMAIN=microflowops.com`
3. `NEXT_PUBLIC_SITE_HOST=microflowops.com,www.microflowops.com`

Notes:
1. Do not set `NEXT_PUBLIC_PLAUSIBLE_ENABLED=true` for Preview/Development environments.
2. Host gating prevents localhost and preview domains from emitting events.

## 3) Domain + DNS

1. Add `microflowops.com` and `www.microflowops.com` in Vercel -> Domains.
2. In DNS, set:
3. `A` record `@` -> `76.76.21.21`
4. `CNAME` record `www` -> `cname.vercel-dns.com`
5. Wait for Vercel to show both domains as verified.

## 4) Route Verification

Open each route on production:

1. `/`
2. `/how-it-works`
3. `/pricing`
4. `/sample`
5. `/faq`
6. `/contact`
7. `/privacy`
8. `/terms`
9. `/sitemap.xml`
10. `/robots.txt`

## 5) CTA Mailto Verification

On `/` and any page with CTAs:

1. Click `Request a sample`
2. Confirm your email client opens with:
3. Subject: `Requesting an OSHA Activity Signals sample`
4. Body matches `web/config/site.json` -> `ctaSampleBody` exactly

Then:

1. Click `Reply with your territory + firm name`
2. Confirm:
3. Subject: `Territory + firm name`
4. Body matches `web/config/site.json` -> `ctaTerritoryBody` exactly

## 6) Copy Button Verification

On `/contact` and `/pricing`:

1. Click `Copy` on Subject for both templates and paste into a text editor.
2. Click `Copy` on Body for both templates and paste into a text editor.
3. Confirm copied values exactly match the configured strings in `web/config/site.json`.

## 7) Analytics Verification (Plausible Realtime)

Prereqs:
1. Plausible site is configured for `microflowops.com`.
2. Production env vars are set (see section 2).

Open Plausible Realtime and generate these events from production:

1. Click `Request a sample` CTA
2. Click `Reply with your territory + firm name` CTA
3. Copy Subject for both templates
4. Copy Body for both templates

Expected event names:
1. `cta_mailto_request_sample`
2. `cta_mailto_territory_firm`
3. `copy_subject_request_sample`
4. `copy_body_request_sample`
5. `copy_subject_territory_firm`
6. `copy_body_territory_firm`

If events do not appear:
1. Confirm you are on `microflowops.com` or `www.microflowops.com` (not a preview domain).
2. Confirm `NEXT_PUBLIC_PLAUSIBLE_ENABLED=true` is set for Production.
3. Confirm `NEXT_PUBLIC_SITE_HOST` includes the hostname you are testing.

