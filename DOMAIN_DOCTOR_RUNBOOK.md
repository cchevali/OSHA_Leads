# Domain Doctor (microflowops.com)

Automated, local-only workflow to diagnose and fix common Vercel plus Cloudflare domain misconfigurations for:
- apex: `microflowops.com`
- `www`: `www.microflowops.com` (expected to redirect to apex from Vercel)

This workflow uses:
- Vercel REST API: project plus domain configuration and validation status
- Cloudflare REST API: DNS records, Page Rules, Workers routes, and (optionally) Redirect Rules (Rulesets)

## Security Note (Read First)

Store tokens in local environment variables only. Do not paste tokens into committed files, terminals with logging, or shared screenshots. Do not commit `.env` files containing tokens.

## What It Does

`domain_doctor.py` can:
1. Fetch Vercel project + domain status for `project_id=prj_4zNry4TmGzqQK1hVFMjqR0MMizT1`.
2. Fetch Vercel recommended DNS targets for apex and `www` via domain configuration endpoint.
3. Verify and enforce Cloudflare DNS:
   - remove conflicting `A/AAAA/CNAME` at apex and `www`
   - set apex `A` to Vercel recommended IPv4
   - set `www` `CNAME` to Vercel recommended CNAME
4. Optionally detect and remove likely Cloudflare redirect sources:
   - Page Rules forwarding URLs
   - Workers Routes matching the host
   - Redirect Rules in Rulesets entrypoint phases (disables matching rules)
5. Re-check Vercel domain validation after DNS changes.
6. Print a concise report and verification commands.

## Prereqs

- Python 3.10+
- `requests` installed (repo already includes it in `requirements.txt`)
- Vercel API token with access to the project
- Cloudflare API token with permissions:
  - Zone Read, DNS Write/Read
  - (Optional) Page Rules Read/Write
  - (Optional) Workers Routes Read/Write
  - (Optional) Rulesets Read/Write

## Environment Variables

Set these in your shell (recommended) or in a private `.env` that you do not commit.

Required:
- `VERCEL_TOKEN`
- `CLOUDFLARE_API_TOKEN`
- `CF_ZONE_NAME` (for example `microflowops.com`) OR `CF_ZONE_ID`

Optional (depends on your Vercel scope):
- `VERCEL_TEAM_ID`

Optional (override defaults):
- `VERCEL_PROJECT_ID` (default: `prj_4zNry4TmGzqQK1hVFMjqR0MMizT1`)
- `DOMAIN_APEX` (default: `microflowops.com`)
- `DOMAIN_WWW` (default: `www.microflowops.com`)

Example (PowerShell):

```powershell
$env:VERCEL_TOKEN="..."
$env:CLOUDFLARE_API_TOKEN="..."
$env:CF_ZONE_NAME="microflowops.com"
$env:VERCEL_TEAM_ID=""  # only if needed
```

## Run (Report-Only First)

```powershell
python domain_doctor.py
```

## Apply DNS Fixes

This will delete conflicting `A/AAAA/CNAME` records at apex and `www` and then upsert the Vercel-recommended apex `A` and `www` `CNAME`.

```powershell
python domain_doctor.py --apply-dns
```

## Optional: Remove Redirect Sources

Only run this if you see unexpected 30x behavior that is not coming from Vercel.

```powershell
python domain_doctor.py --apply-dns --apply-redirect-cleanup
```

## Verification Commands (Suggested)

DNS:
```powershell
Resolve-DnsName microflowops.com -Type A
Resolve-DnsName www.microflowops.com -Type CNAME
```

HTTP:
```powershell
curl -I https://microflowops.com/
curl -I https://www.microflowops.com/
```

Vercel status re-check (API):
```powershell
python domain_doctor.py
```
