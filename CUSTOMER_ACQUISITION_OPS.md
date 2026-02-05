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

### Initial

Subject: TX OSHA activity signals (sample)

Hi {First},

I am reaching out because {Firm} supports employers on OSHA matters, and we track new OSHA activity signals across Texas.

Here are a few recent signals (sample):

1. Priority: High - Pedco Roofing, Inc. - Houston, TX - Accident - Opened: 2026-01-29 - Observed: 2026-02-03
2. Priority: Medium - Mmm Welders and Assemblers LLC - Mansfield, TX - Referral - Opened: 2026-01-22 - Observed: 2026-01-30
3. Priority: Medium - Pyramid Waterproofing, Inc. - Houston, TX - Complaint - Opened: 2026-01-27 - Observed: 2026-01-30

Priority is a heuristic based on severity, recency, and signal type. Not legal advice.

Some OSHA matters can be time sensitive; deadlines vary by case. We include deadlines only when the record supports them.

If you want a short daily TX digest like this, reply "yes" and I will set it up.

Not affiliated with OSHA; this is an independent alert service.

Chase Chevalier
Micro Flow Ops - OSHA Alerts
support@microflowops.com

---
Micro Flow Ops
11539 Links Dr, Reston, VA 20190
Opt out: reply with "unsubscribe" or email support@microflowops.com (subject: unsubscribe)

### Follow-Up (2-3 days later)

Subject: Re: TX OSHA activity signals

Hi {First},

Quick follow-up in case you missed the note. We send a short daily Texas Triangle brief with new OSHA activity, ranked by urgency.

If it would help your team, I can set up a no-commitment sample alert. Reply "yes" and I will get it live.

Thanks,
Chase

## New subscriber_key Onboarding Checklist

- [ ] Create a new customer config in `customers/` with `subscriber_key`, `territory_code`, and recipient emails.
- [ ] Ensure the subscriber exists in the `subscribers` table with the same `subscriber_key` and the chosen `territory_code`.
- [ ] Flip `send_enabled` to `1` for that subscriber after preflight passes.
- [ ] Confirm the territory selection is correct in `territories` and matches the customer config.
- [ ] Run a preflight-only check before any live send.
