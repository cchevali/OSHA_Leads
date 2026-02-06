# Customer Acquisition Ops

## Texas Triangle Prospect Target List

Texas Triangle coverage: Dallas-Fort Worth, Houston, Austin, San Antonio.

### OSHA Defense Attorneys

| Firm | City | Focus | Website | Source |
| --- | --- | --- | --- | --- |
| Hendershot Cowart P.C. | Houston, TX | OSHA citation defense | https://www.hchlawyers.com/osha/ | https://www.hchlawyers.com/osha/ |
| KRCL (Kane Russell Coleman Logan) | Dallas, TX / Houston, TX / Austin, TX | OSHA defense practice | https://www.krcl.com/practices/osha-defense | https://www.krcl.com/practices/osha-defense; https://www.krcl.com/contact |
| Andrews Myers | Houston, TX / Austin, TX | OSHA citations and proceedings | https://www.andrewsmyers.com/practice-areas/construction/osha-citations-and-proceedings/ | https://www.andrewsmyers.com/practice-areas/construction/osha-citations-and-proceedings/ |
| Pappas Grubbs Price PC | Dallas, TX / Houston, TX / Austin, TX / San Antonio, TX | OSHA practice | https://www.pappasgrubbs.com/practice/osha/ | https://www.pappasgrubbs.com/practice/osha/ |
| Seltzer, Chadwick, Seltzer and Ladik | Dallas, TX / Austin, TX / Houston, TX | OSHA law group | https://www.realclearcounsel.com/texas-osha-law-firm | https://www.realclearcounsel.com/texas-osha-law-firm |
| The Mullen Firm PLLC | Austin, TX | OSHA compliance and defense | https://themullenfirm.com/ | https://themullenfirm.com/; https://themullenfirm.com/contact-us |
| Ogletree Deakins | Dallas, TX | Workplace safety and health | https://ogletree.com/practices-industries/workplace-safety-and-health/ | https://ogletree.com/practices-industries/workplace-safety-and-health/; https://ogletree.com/locations/dallas |
| Fisher Phillips | Houston, TX | Workplace safety and catastrophe management | https://www.fisherphillips.com/en/services/practices/workplace-safety-and-catastrophe-management/index.html | https://www.fisherphillips.com/en/services/practices/workplace-safety-and-catastrophe-management/index.html; https://www.fisherphillips.com/en/offices/houston.html |
| Littler | Houston, TX | Occupational safety and health | https://www.littler.com/practices-industries/occupational-safety-and-health | https://www.littler.com/practices-industries/occupational-safety-and-health; https://www.littler.com/locations/houston |
| Jackson Lewis | Dallas, TX / Houston, TX | Workplace safety and health | https://www.jacksonlewis.com/services/workplace-safety-and-health | https://www.jacksonlewis.com/services/workplace-safety-and-health; https://www.jacksonlewis.com/locations/dallas; https://www.jacksonlewis.com/locations/houston |

### Safety Consultants

| Firm | City | Focus | Website | Source |
| --- | --- | --- | --- | --- |
| Safety First Consulting | Georgetown, TX (serves Austin, San Antonio, Dallas, Houston) | OSHA compliance consulting | https://safetyfirstconsulting.com/about-us/ | https://safetyfirstconsulting.com/about-us/; https://safetyfirstconsulting.com/contact |
| Greenberg Safety | Austin, TX | Safety consulting and training | https://greenbergsafety.com/ | https://greenbergsafety.com/ |
| Aggie Safety | Houston, TX | OSHA safety consulting | https://aggiesafety.com/ | https://aggiesafety.com/ |
| Costello Safety Consulting | Houston, TX | Safety consulting and training | https://www.costellohse.com/about | https://www.costellohse.com/about |
| Safety Consultants USA | Dallas, TX | Safety consulting and training | https://safetyconsultantsusa.com/contact/ | https://safetyconsultantsusa.com/contact/ |
| OSHA Safety Pro (Scott and Associates) | Houston, TX | OSHA compliance and safety consulting | https://www.oshasafetypro.com/safety | https://www.oshasafetypro.com/safety |
| Provisio EHS | San Antonio, TX | Safety consulting and industrial hygiene | https://www.provisioehs.com/locations/san-antonio-texas | https://www.provisioehs.com/locations/san-antonio-texas |
| Texas Safety Doc Consulting | San Antonio, TX | OSHA compliance support | https://txsafetydoc.com/ | https://txsafetydoc.com/ |
| SHORM Consulting | San Antonio, TX | Safety consulting and training | https://shormconsulting.com/contact-us | https://shormconsulting.com/contact-us |
| OccuPros | San Antonio, TX | Safety consulting and compliance | https://occupros.com/locations/san-antonio/ | https://occupros.com/locations/san-antonio/ |

## Outreach Messages (Cold Tone)

### Initial (Territory-First, Email-Only)

Subject: Texas Triangle OSHA Activity Signals (7-day sample)

Hi {First},

We send a short daily email with new OSHA activity signals filtered to the Texas Triangle (Austin, Dallas/Fort Worth, Houston, San Antonio), ranked by urgency so you can act while matters are still fresh.

Sample alert format: https://microflowops.com/sample

If you want a 7-day sample, reply with your territory + firm name (e.g., "Texas Triangle + {Firm}"). No calls; onboarding is email-only.

Chase Chevalier
MicroFlowOps
support@microflowops.com

Opt out anytime: reply "unsubscribe" (we maintain a suppression list and honor it)

### Follow-Up (3-4 days later)

Subject: Re: Texas Triangle OSHA Activity Signals

Hi {First},

Quick follow-up. If a daily Texas Triangle OSHA signal digest would help, reply with your territory + firm name and I will start a 7-day sample. Email-only; no calls.

Chase

## YES Reply Onboarding Checklist (Email-Only)

Collect and confirm via email (no calls):
- [ ] Territory (start with Texas Triangle, or specify alternatives)
- [ ] Firm name (as it should appear in the alert header/footer)
- [ ] Distribution list inbox (one or more recipient emails)
- [ ] Preferred send time + timezone (default: 08:00 America/Chicago)
- [ ] Severity threshold:
  - `high_medium` (lead_score >= 6) or `high_only` (lead_score >= 10)
  - whether to include a low-signal fallback section when no high/medium items exist
- [ ] Unsubscribe/suppression:
  - confirm they can reply "unsubscribe" any time
  - ask if any additional emails/domains should be suppressed

### Strict YES Reply Format (Copy/Paste Block)

Ask the prospect to reply with this exact block (edit values as needed). This enables zero-touch provisioning via CLI (no manual DB edits).

```text
TERRITORY=TX_TRIANGLE
SEND_TIME_LOCAL=08:00
TIMEZONE=America/Chicago
THRESHOLD=MEDIUM
RECIPIENTS=alerts@yourfirm.com, ops@yourfirm.com
FIRM_NAME=Your Firm Name
NOTES=Optional notes (routing, special handling, etc.)
```

Notes:
- `THRESHOLD` options: `MEDIUM` (lead_score >= 6) or `HIGH` (lead_score >= 10).
- `TERRITORY` options include `TX_TRIANGLE` (alias for `TX_TRIANGLE_V1`) or any territory code present in `territories.json`.
- `RECIPIENTS` must be a comma-separated list.

Operational notes:
- New subscriber configs live under `customers/` and are intentionally untracked when they contain real recipient emails.
- Suppression is enforced at send-time (email + domain), and events are logged for audit.

## Minimal Campaign Tracking (CSV)

Purpose: avoid double-sends, record replies, and ensure unsub/suppression is enforced consistently.

Committed templates (schema headers):
- `campaign_tracking/templates/tx_triangle_sent_template.csv.example`
- `campaign_tracking/templates/tx_triangle_replied_template.csv.example`
- `campaign_tracking/templates/tx_triangle_unsub_template.csv.example`

Where to write the working copies:
- Create per-campaign working logs under `out/campaign_tracking/` (gitignored), for example:
  - `out/campaign_tracking/tx_triangle_2026-02/sent.csv`
  - `out/campaign_tracking/tx_triangle_2026-02/replied.csv`
  - `out/campaign_tracking/tx_triangle_2026-02/unsub.csv`

Dedupe rule (ops-level):
- Before sending any cold email, check `sent.csv` for the normalized email (lowercase). If present with `status=sent`, do not send again.
