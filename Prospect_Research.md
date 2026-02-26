# Free data sources for OSHA-targeted B2B contact enrichment

**The most effective zero-cost pipeline for turning OSHA inspection records into email-ready prospect data combines DOL enforcement bulk downloads, Apollo.io's free-tier title search, the BCSP credential directory, Google Places API, and state contractor licensing boards.** Together, these sources can produce **200–400 enriched contact records per month** at no cost — enough for a meaningful cold outreach operation targeting safety directors and EHS managers. The critical challenge isn't finding OSHA-exposed companies (the government gives you that universe for free), but bridging the "last mile" from company name to a named individual's verified email address. No single free source solves this; the winning approach stacks 4–5 complementary tools in sequence.

---

## The OSHA universe: 3 million inspections, zero email addresses

The DOL Open Data Portal at `dataportal.dol.gov` is the definitive starting point. It contains **every federal and state-plan OSHA inspection since 1972** — roughly 3 million records updated daily — available via REST API (v4) and bulk CSV download. Fields include establishment name, full site address, SIC/NAICS codes, inspection type, violation severity, penalty amounts, and abatement dates. An API key is free and grants reasonable throughput for automated ingestion.

Supplementary government datasets expand the target universe considerably. **EPA ECHO** (`echo.epa.gov`) covers 1.5 million+ facilities with environmental compliance histories and offers a well-documented REST API; its overlap with OSHA-exposed companies is strongest in manufacturing, chemical, and waste management sectors. **MSHA** data (bulk download only, updated weekly) adds ~14,000 active mines with operator and controller company names. The **ITA injury/illness data** from OSHA Form 300A submissions names individual establishments with 100+ employees alongside their injury rates — effectively letting you recreate OSHA's own Site-Specific Targeting lists. The **SVEP Public Log** at `osha.gov/enforcement/svep` identifies the most egregious violators by name and is freely downloadable.

**What none of these sources provide:** contact person names, email addresses, phone numbers, or job titles. Every government enforcement dataset stops at the company name and mailing address. This is the gap the rest of the pipeline must fill.

---

## Tier 1 enrichment: the four sources that actually produce email-ready records

### Apollo.io free tier — the single best starting point for contact discovery

Apollo.io's free plan offers **unlimited email credits** (subject to ~250/day fair-use cap), **5 mobile number credits**, and **10 export credits per month**. Critically, it allows searching by both company name and job title simultaneously — meaning you can input an OSHA-inspected company name and filter for "Safety Director," "EHS Manager," "VP of Environment Health and Safety," or "Operations Manager." Apollo's database spans **210 million+ contacts**, and coverage of mid-market US companies in construction, manufacturing, and logistics is strong. The free tier includes a Chrome extension but **no API access** (API requires the $119/month Organization plan). At 10 exports/month, the free tier supports targeted manual enrichment of your highest-priority prospects; rotating multiple free accounts is against TOS but commonly practiced.

### BCSP credential directory — a public goldmine of named safety professionals

The Board of Certified Safety Professionals maintains a **fully public, searchable directory** at `directory.bcsp.org` requiring no login. It covers **~24,000+ active CSP holders** and thousands more with ASP, CHST, OHST, and other credentials. The directory supports filtering by **company name, city, state, credential type, industry (50+ NAICS-based categories), and specialty (75+ options including Construction Safety, Fall Protection, Process Safety)**. Results return the individual's name, credentials, company, and location. This is the only free source identified that directly maps named safety professionals to their employers with industry and specialty context. The directory renders via JavaScript (requiring a headless browser for automated extraction), and while BCSP's TOS includes standard "informational purposes only" language, it does not contain explicit anti-scraping prohibitions.

### Google Places API — 5,000 free business lookups per month

Google's Places API (New) provides **5,000 free Pro-tier searches per month** (as of the March 2025 pricing restructure). A Text Search query with a company name and city returns the business phone number, website URL, formatted address, business hours, and Google Maps link. This is the most reliable free method for converting an OSHA record's company name into a working phone number and website domain — both essential for downstream enrichment. The website domain feeds directly into email pattern tools, and phone numbers enable direct outreach to smaller companies where the owner often answers.

### Hunter.io free tier — domain-to-email with API access

Hunter.io provides **25 email search credits and 50 email verification credits per month** on its free plan, and — uniquely among free tools — **includes API access**. Given a company's domain (obtained from Google Places), Hunter's Domain Search returns known email addresses with confidence scores and reveals the company's email pattern (e.g., `first.last@company.com`). The Email Finder endpoint combines a person's name with a domain to generate and verify a specific email address. At 25 searches/month the volume is limited, but the API access enables full automation within that cap.

---

## Tier 2 enrichment: high-value supplementary sources

**State contractor licensing boards** are an overlooked resource. California's CSLB alone has **~290,000 licensed contractors** with owner/qualifying individual names, addresses, license classifications, and disciplinary history — available via bulk data portal downloads. Florida's Sunbiz provides **free FTP bulk downloads** of 3.5 million business entities including officer and director names. Kentucky's SOS similarly offers bulk data with officer information. These registries are public records with no use restrictions and map directly to OSHA-exposed industries (roofing, electrical, demolition, asbestos abatement, general contracting).

**SAM.gov's Entity Management API** covers ~700,000+ active registrations and returns company name, address, UEI, NAICS codes, and — on the public tier — point-of-contact names (though email and phone are restricted to federal accounts). An estimated **15–30% of OSHA-inspected companies** (especially larger construction and manufacturing firms) are also registered as government contractors. The API is free but rate-limited: 10 requests/day for basic accounts, 10,000/day for system accounts.

**People Data Labs (PDL)** offers **100 free API records per month** with full programmatic access — the best free API option for automated company-to-person matching. The Person Search endpoint supports title filtering ("EHS Manager") and company name matching. PDL's database covers 3 billion+ person profiles, though email/phone field access may be limited on the free tier.

**OSHA VPP participant lists** at `osha.gov/vpp/bylocation` publicly name companies participating in OSHA's Voluntary Protection Programs, searchable by location and NAICS code. These companies have dedicated safety programs with named safety contacts — a high-value prospect segment.

The table below summarizes free-tier volumes across the key B2B enrichment tools:

| Tool | Free monthly allowance | Email data | Phone data | API access | Title search |
|------|----------------------|------------|------------|------------|--------------|
| **Apollo.io** | ~250 emails/day, 10 exports | ✅ Unlimited reveals | 5 mobile credits | ❌ | ✅ |
| **Hunter.io** | 25 searches, 50 verifications | ✅ | ❌ | ✅ | ❌ (domain-based) |
| **People Data Labs** | 100 records | Partial | Partial | ✅ | ✅ |
| **Lusha** | 40–70 credits | ✅ (1 credit each) | ✅ (5–10 credits each) | ❌ | Limited |
| **ContactOut** | ~150/month (5/day) | ✅ | ✅ | ❌ | ❌ (LinkedIn-dependent) |
| **Snov.io** | 50 credits | ✅ | ❌ | ❌ | Limited |
| **LeadIQ** | 50 emails, 5 phones | ✅ | ✅ | ❌ | Via LinkedIn |
| **Skrapp.io** | 100 searches, 200 verifications | ✅ | ❌ | ✅ | Via LinkedIn |
| **RocketReach** | 5 lookups | ✅ | ❌ | ❌ | ✅ |
| **ZoomInfo Lite** | 10 credits | ✅ | ✅ | ❌ | ✅ |

**Clearbit no longer has a free tier** — all free tools were shut down April 30, 2025, and Breeze Intelligence requires a paid HubSpot subscription plus credit packs starting at $45/month.

---

## Job posting signals identify companies actively hiring for safety roles

A company posting for "Safety Manager" or "EHS Director" is a high-intent signal: it confirms budget allocation for safety personnel, active compliance needs, and organizational receptiveness to safety-related outreach. **Google for Jobs via SerpApi** is the recommended access method — it aggregates listings from Indeed, LinkedIn, ZipRecruiter, Glassdoor, CareerBuilder, and company career pages into a single searchable endpoint. SerpApi's free tier provides **100 searches/month** (some accounts get 250), each returning job title, company name, location, description, and apply links. A query like `q=EHS+Director&location=Texas` returns dozens of actively hiring companies with full context.

Indeed discontinued its public Job Search API and aggressively blocks direct scraping. LinkedIn scraping carries the **highest legal risk** of any source — LinkedIn won a permanent injunction against Proxycurl in 2025, and account bans are routine for automation. The practical recommendation: use Google Jobs aggregation (which already includes both Indeed and LinkedIn postings) rather than scraping individual job boards directly.

---

## Industry directories that map directly to OSHA-exposed sectors

**The Blue Book** (`thebluebook.com`) lists **800,000+ commercial construction companies** across 560 trade categories with contact info and qualifications — the premier free-search directory for the construction industry. **ThomasNet** serves the same function for manufacturing, with company profiles including phone, website, certifications, employee counts, and detailed product/service classifications. Both are scrapable (no public API), and both have existing Apify actors and GitHub scrapers available.

**AGC chapter directories** are publicly accessible for many states (California, Massachusetts, Iowa, Alaska, San Diego, Florida East Coast, and others) and list member contractor companies with addresses, phones, websites, and trade classifications. The directories use common CMS platforms (GrowthZone) making scraping straightforward. **ABC's national "Find Contractors" tool** at `abc.org/membership/find-contractors` is also publicly searchable.

**BBB.org** is highly scrapable with multiple commercial scraping services available. Profiles include business name, address, phone, website, BBB rating, accreditation status, and category — useful for both contact enrichment and business validation.

---

## Creative sources that provide unique targeting advantages

**OSHA enforcement press releases** at `osha.gov/news/newsreleases/enforcement` name specific companies, their violations, locations, and penalty amounts in narrative form. These are easily scraped and convert directly into prospect lists. Companies appearing in press releases have both demonstrated compliance failures and public accountability — making them highly receptive to safety services. Setting up **Google News alerts** for "OSHA citation," "OSHA fine," and "workplace safety violation" creates an automated pipeline of new prospects.

**OSHRC decisions** at `oshrc.gov/decision-search/` provide rich case data on companies contesting OSHA citations, including employer name, docket number, violation details, and case narratives dating back to 1972. **CourtListener's RECAP archive** (`courtlistener.com/recap/`) offers free access to millions of PACER documents searchable for OSHA-related federal court filings — returning company names, officer/agent names, and attorney contacts.

**Texas workers' comp subscriber/non-subscriber lists** at `tdi.texas.gov/wc/data.html` are uniquely valuable — they identify which Texas employers carry workers' comp insurance and which don't. Non-subscribers likely have elevated risk management needs.

**Building permits** (public records at the county level) reveal active construction projects with contractor names, project values, and owner information. Many jurisdictions have searchable online databases, and cross-referencing with OSHA violation data identifies contractors with both active projects and safety issues.

For **email pattern discovery**, the most efficient free approach combines Hunter.io's domain search (to identify the email format) with name data from BCSP, state licensing boards, or LinkedIn. **Mailmeteor's email finder** (`mailmeteor.com/tools/email-finder`) is completely free with no signup required. **ZeroBounce** provides **100 free email verifications per month**, and **Reoon** offers the most generous free tier at **600 verifications/month**.

---

## Recommended end-to-end pipeline architecture

The optimal free pipeline processes OSHA records through five sequential stages:

**Stage 1 — Target identification (free, unlimited).** Bulk-download OSHA enforcement CSVs from the DOL data portal. Filter by violation severity (willful, repeat, serious), recency, industry (NAICS/SIC), and geography. Supplement with SVEP log, ITA injury data, fatality reports, and OSHRC decisions. This yields tens of thousands of company names with addresses and industry codes.

**Stage 2 — Company enrichment (free, ~5,000/month).** Pass company names + locations through Google Places API Text Search to obtain website domains, phone numbers, and verified addresses. Cross-reference with state contractor licensing boards (bulk download where available) and SAM.gov for additional firmographic data and officer names.

**Stage 3 — Contact discovery (free, ~300–500/month).** Search BCSP directory by company name, industry, and state to find named CSP/ASP holders at target companies. Use Apollo.io to search for "Safety Director," "EHS Manager," and "Operations Manager" titles at remaining companies. Supplement with state SOS officer data (Florida FTP bulk, Kentucky bulk) and SEC EDGAR for public companies.

**Stage 4 — Email generation (free, ~200/month).** Feed company domains from Stage 2 and contact names from Stage 3 into Hunter.io's Email Finder API. For companies where Hunter lacks data, use the discovered email pattern plus the contact's name to generate candidate addresses. Use Mailmeteor as a free backup.

**Stage 5 — Verification and outreach (free, ~600/month).** Verify all generated emails through Reoon (600 free/month) or ZeroBounce (100 free/month) before sending. Flag catch-all domains for cautious sending with reduced volume.

**Scaling beyond free tiers.** The most cost-effective paid upgrades are Apollo.io Basic ($49/month for 900 mobile credits and more exports), SerpApi ($75/month for 5,000 Google Jobs searches), and Hunter.io Starter ($34/month for 500 searches). At ~$160/month total, this roughly 10x the pipeline's monthly throughput.

---

## Conclusion: what makes this pipeline work

The strategy succeeds because it treats OSHA enforcement data as a **targeting filter**, not a contact source. Government datasets perfectly identify *which companies* to pursue and *why* (violation type, severity, industry, recency) — intelligence that powers highly relevant cold outreach messaging. The contact enrichment layer then converts company-level targeting into person-level outreach through a waterfall of complementary free tools.

Three sources deserve outsized engineering investment. **BCSP's credential directory** is the only free source that directly maps named safety professionals to specific employers with industry and specialty context — it should be the first lookup for every target company. **Google Places API** at 5,000 free searches/month is the most reliable company-to-domain resolver, and domain data unlocks the entire email-finding toolchain. **Apollo.io**, despite its export limits, is the only free tool that supports simultaneous company-name and job-title filtering across a 210-million-contact database.

The legal position is strong: OSHA enforcement data is public domain with no use restrictions, CAN-SPAM permits cold B2B email with opt-out mechanisms, and most data flows through legitimate APIs rather than TOS-violating scraping. The primary legal caution is **LinkedIn** — avoid automated scraping entirely given the 2025 Proxycurl injunction — and respect rate limits on government APIs. The realistic output ceiling with purely free tools is **200–400 verified, email-ready prospect records per month**, scaling to 2,000+ with modest paid upgrades.