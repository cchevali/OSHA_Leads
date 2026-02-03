# Target List Factory – Implementation Status

**Date:** 2026-01-13  
**Status:** ✅ Complete  

---

## Summary

Implemented a "Target List Factory" workflow for building and managing prospect lists from consistent industry sources. No dashboard – pure file-based workflow with CSV tracking and a Python deduplication script.

---

## Deliverables

| File | Type | Purpose |
|------|------|---------|
| `out/prospect_tracking_template.csv` | CSV Template | Standard columns for prospect data entry |
| `out/prospect_status_codes.csv` | Reference | Status code definitions with next actions |
| `PROSPECTING_SOP.md` | Documentation | Step-by-step process for collecting 30 targets/territory |
| `dedupe_prospects.py` | Python Script | Deduplication and field normalization |

---

## What Was Built

### 1. Prospect Tracking Template
Standard CSV with 14 columns:
- `company_name`, `domain`, `state`, `territory`, `source`
- `contact_name`, `contact_role`, `contact_email`, `linkedin_url`, `phone`
- `status`, `last_contacted`, `notes`, `added_date`

### 2. Prospecting SOP
Documents the 3 consistent lead sources:
1. OH&S Buyers Guide – OSHA Compliance
2. AIHA Consultants Listing  
3. ASSP Chapter Consultant Lists

Includes step-by-step process for:
- Collecting 30 targets per territory (10+ per source)
- Running dedupe after each batch
- Identifying best contact role (Owner → VP Safety → Safety Manager)

### 3. Dedupe & Normalize Script
Lightweight Python script (no external dependencies) that:
- Normalizes domains (strips `www.`, lowercases, extracts from URLs)
- Flags duplicate domains with `[DUP]` prefix in notes
- Standardizes state codes (e.g., `California` → `CA`)
- Maps contact roles to 6 standard categories
- Title-cases company names, preserves acronyms (LLC, INC)

**Usage:**
```powershell
python dedupe_prospects.py out/prospect_tracking_template.csv
# Output: out/prospect_tracking_deduped.csv
```

### 4. Status Codes Reference
11 status codes covering full prospect lifecycle:
`NEW` → `RESEARCHING` → `OUTREACH_1/2/3` → `REPLIED` → `MEETING_SET` → `QUALIFIED`  
Plus: `NOT_FIT`, `COLD`, `DO_NOT_CONTACT`

---

## Prospecting Task Completed (2026-01-13)

### Summary Stats
| Metric | Value |
|--------|-------|
| **Total rows collected** | 31 |
| **Duplicates merged** | 9 |
| **Unique prospects (final)** | 22 |
| **With email** | 12 |
| **OUTREACH_1 ready** | 10 |

### Top 10 Highest-Priority Targets (OUTREACH_1)

| # | Company | Domain | Contact | Role | Email | Phone |
|---|---------|--------|---------|------|-------|-------|
| 1 | Jan Koehn M.S. CIH Inc | jkinc.biz | Jan Koehn | Owner/Executive | mail@jkinc.biz | (713) 664-1597 |
| 2 | Spear & Lancaster LLC | jespear.com | Jerome Spear | Owner/Executive | jerome.spear@jespear.com | (281) 252-0005 |
| 3 | Atlas Technical Consultants | oneatlas.com | Alex Peck | Consultant | alex.peck@oneatlas.com | (425) 273-3858 |
| 4 | Clean Environments Inc | cleanenvironments.com | Greg S | Consultant | gregs@cleanenvironments.com | (210) 349-7242 |
| 5 | EnviROSH Services Inc | envirosh.com | Lloyd Andrew | Consultant | lloyd.andrew@envirosh.com | (281) 290-8309 |
| 6 | Terracon Consultants Inc | terracon.com | Kevin Maloney | Consultant | Kevin.maloney@terracon.com | (713) 690-8989 |
| 7 | Bernardino LLC | bernardino-oehs.com | — | Consultant | contact@bernardino-oehs.com | (956) 605-2771 |
| 8 | Baer Engineering | baereng.com | — | Consultant | info@baereng.com | (512) 453-3733 |
| 9 | John A. Jurgiel & Associates | jurgiel.com | Dr. Romo | Consultant | dromo@jurgiel.com | (214) 735-8055 |
| 10 | CTEH | cteh.com | C. Ledbetter | Consultant | cledbetter@cteh.com | (501) 337-2900 |

### Priority Criteria
1. **Owner/Executive roles** ranked highest (direct decision-makers)
2. **Named contact + email** ranked next
3. **Generic email only** ranked last

---

## Next Steps (Future Work)

- [ ] Execute OUTREACH_1 emails to top 10 targets
- [ ] Research missing contacts for OH&S Buyers Guide prospects
- [ ] Add LinkedIn connection requests for ASSP chapter presidents
- [ ] Repeat process for additional territories (FL, CA, etc.)

---

## File Locations

All files created in: `C:\dev\OSHA_Leads\`

