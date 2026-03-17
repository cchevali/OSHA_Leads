> DEPRECATED - see `docs/V1_CUSTOMER_VALIDATED.md` and `docs/RUNBOOK.md`.
> Date: 2026-02-12
> Rationale: Archived during canonical spine adoption; retained only as a historical V1 artifact.

---
# Target List Factory â€“ Prospecting SOP

## Objective
Build 30 qualified prospects per territory, deduplicated by domain, with verified business email/contact.

---

## 1. Consistent Sources

| # | Source | URL | Best For |
|---|--------|-----|----------|
| 1 | **OH&S Buyers Guide â€“ OSHA Compliance** | ohsonline.com/buyersguide | Safety consultants, EHS service providers |
| 2 | **AIHA Consultants Listing** | aiha.org/consultants-list | Industrial hygiene / CIH consultants |
| 3 | **ASSP Chapter Consultant Lists** | assp.org/membership/chapters | Local safety professionals, regional coverage |

---

## 2. Step-by-Step Collection Process

### A. Gather Raw Leads (per territory)
1. Open each source and filter by target state/region.
2. Export or copy company name, website, and contact info into `out/prospect_tracking_template.csv`.
3. Aim for **10+ leads per source** â†’ 30 total per territory.

### B. Dedupe & Normalize
Run the dedupe script after adding new rows:

```powershell
python dedupe_prospects.py out/prospect_tracking_template.csv
```

The script will:
- **Normalize domains** (strip `www.`, lowercase, remove trailing slashes).
- **Flag duplicates** by domain â€“ keeps first occurrence, marks later rows `[DUP]`.
- **Standardize fields**:
  - `state` â†’ 2-letter uppercase (e.g., `California` â†’ `CA`).
  - `contact_role` â†’ mapped to standard titles (see table below).
  - `company_name` â†’ trim whitespace, title-case.
- Output saved to `out/prospect_tracking_deduped.csv`.

### C. Identify Best Contact
For each unique domain, locate the decision-maker:

| Priority | Role Keywords |
|----------|---------------|
| 1 | Owner, President, CEO, Founder |
| 2 | VP Safety, Director EHS, Safety Manager |
| 3 | Operations Manager, Compliance Officer |

**Email discovery tips:**
- Check company website "About/Team" page.
- Use LinkedIn â†’ filter by company + role.
- Pattern-guess: `first@domain.com`, `first.last@domain.com`.

---

## 3. Standard Contact Role Mappings

| Raw Input (case-insensitive) | Normalized Role |
|------------------------------|-----------------|
| owner, president, ceo, founder | Owner/Executive |
| vp safety, director ehs, safety director | Safety Director |
| safety manager, ehs manager | Safety Manager |
| operations manager, ops manager | Operations Manager |
| compliance officer, compliance manager | Compliance Officer |
| consultant, advisor | Consultant |
| *(other)* | Other |

---

## 4. Quality Checklist (before handoff)

- [ ] 30 prospects minimum per territory
- [ ] No duplicate domains
- [ ] Every row has `company_name`, `domain`, `state`, `source`
- [ ] 80%+ rows have `contact_email` populated
- [ ] Roles normalized to standard list

---

## Quick Reference

```
Template:  out/prospect_tracking_template.csv
Dedupe:    python dedupe_prospects.py out/prospect_tracking_template.csv
Output:    out/prospect_tracking_deduped.csv
```

