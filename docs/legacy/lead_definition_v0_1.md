> DEPRECATED - see `docs/V1_CUSTOMER_VALIDATED.md`, `docs/RUNBOOK.md`, and `docs/ARCHITECTURE.md`.
> Date: 2026-02-12
> Rationale: Archived during canonical spine adoption; retained only as a historical V1 artifact.

---
# Lead Definition v0.1

## Entity: InspectionLead

An InspectionLead represents a single OSHA enforcement inspection at an establishment/site level.

## Canonical ID

```
lead_id = "osha:inspection:{activity_nr}"
```

Where `activity_nr` is the unique OSHA Activity Number assigned to the inspection.

## New Lead Criteria

A lead is considered "new" when:
1. `first_seen_at` is within the last 24 hours (rolling window)
2. All required fields are present

## Required Fields for "Sendable" Lead

| Field | Description | Source Priority |
|-------|-------------|-----------------|
| `activity_nr` | OSHA Activity Number | Results page / Detail page |
| `establishment_name` | Business name | Results page / Detail page |
| `site_state` | Two-letter state code | Detail page address block |
| `site_city` OR `site_zip` | At least one location identifier | Detail page address block |
| `date_opened` | Inspection open date | Results page / Detail page |
| `source_url` | URL of the detail page | Generated during ingestion |

If any required field is missing, the lead is stored but marked `needs_review = 1` and excluded from daily alerts.

## Dedupe Rules

### Primary Key
- `activity_nr` is unique in the `inspections` table
- No duplicate rows for the same inspection

### Re-Ingest Behavior
When an existing `activity_nr` is re-ingested:
1. Update `last_seen_at` to current timestamp
2. Fill any previously NULL fields with new values (do not overwrite non-NULL values)
3. Recalculate `raw_hash` for change detection
4. Do NOT create a duplicate row

## Re-Alert Rules

A lead triggers `re_alert = 1` only when a **material upgrade** occurs:

| Upgrade Type | Condition |
|--------------|-----------|
| Violations Posted | `violations_count` changes from NULL to non-NULL (>= 1) |
| Case Status Change | `case_status` changes from OPEN to CLOSED |
| Citations Posted | Citations section detected (future feature) |

Re-alert logic is implemented as flags; actual re-alerting behavior is not part of MVP.

## Scoring Algorithm

Deterministic, rule-based scoring for commercial intent ranking.

### Score Components

| Component | Condition | Points |
|-----------|-----------|--------|
| Inspection Type | Fatality/Catastrophe | +10 |
| Inspection Type | Accident | +8 |
| Inspection Type | Complaint | +4 |
| Inspection Type | Referral | +3 |
| Inspection Type | Planned | +1 |
| Scope | Complete | +2 |
| Violations | `violations_count >= 1` | +3 |
| Industry | NAICS starts with "23" (Construction) | +3 |
| Emphasis | Any emphasis program present | +2 |

### Sort Order
1. Score descending (highest first)
2. Date opened descending (most recent first)

## Data Sources

All data is from public OSHA sources:
- OSHA Establishment Search: `https://www.osha.gov/ords/imis/`
- Individual inspection detail pages

## Product Context

> **Important**: OSHA states that citation items are posted 30 days after the employer receives citations. Therefore:
> - **MVP Promise**: Early visibility into new inspections
> - **Future Phase**: Citation tracking once posted

This product provides informational alerts only. Users must verify all information independently.

## Compliance Notes

- Public data only
- No sensitive personal data collected
- Opt-out/suppression list maintained
- Source URLs logged for all records

