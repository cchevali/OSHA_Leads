# OSHA Concierge MVP

A reliable daily feed of newly observed OSHA enforcement inspections, ranked for commercial intent and exported as CSV for safety consultants.

## Overview

This tool monitors OSHA's public inspection database and:

1. **Ingests** new inspections from OSHA public pages
2. **Deduplicates** based on Activity Number
3. **Scores** leads for commercial intent
4. **Exports** daily CSV files of qualified leads

> **Important**: OSHA posts citation items 30 days after the employer receives them. This MVP provides early visibility into inspections; citations will be tracked in a future phase.

## Quick Start

### 1. Create Virtual Environment

```powershell
# Windows PowerShell
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

```bash
# Linux/Mac
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. Initialize Database

```bash
sqlite3 osha_leads.db < schema.sql
```

Or in Python:
```python
import sqlite3
with open('schema.sql') as f:
    sqlite3.connect('osha_leads.db').executescript(f.read())
```

### 3. Run Ingestion

```bash
# Default: last 2 days for VA, MD, DC
python ingest_osha.py --db osha_leads.db

# Custom states and lookback
python ingest_osha.py --db osha_leads.db --since-days 7 --states CA,TX,FL

# With verbose logging
python ingest_osha.py --db osha_leads.db --log-level DEBUG
```

### 4. Export Daily Leads

```bash
# Export to default ./out directory
python export_daily.py --db osha_leads.db

# Custom output directory
python export_daily.py --db osha_leads.db --outdir ./exports

# Export as of specific date
python export_daily.py --db osha_leads.db --as-of-date 2025-01-06
```

## CLI Reference

### ingest_osha.py

| Argument | Required | Default | Description |
|----------|----------|---------|-------------|
| `--db` | Yes | - | Path to SQLite database |
| `--since-days` | No | 2 | Look back this many days |
| `--states` | No | VA,MD,DC | Comma-separated state codes |
| `--max-details` | No | 500 | Max detail pages to fetch |
| `--log-level` | No | INFO | DEBUG, INFO, WARNING, ERROR |

### export_daily.py

| Argument | Required | Default | Description |
|----------|----------|---------|-------------|
| `--db` | Yes | - | Path to SQLite database |
| `--outdir` | No | ./out | Output directory for CSVs |
| `--as-of-date` | No | today | Export date (YYYY-MM-DD) |
| `--log-level` | No | INFO | DEBUG, INFO, WARNING, ERROR |

## Output Files

### daily_leads_{date}.csv

Qualified leads ready for delivery. Columns:

- `lead_id` - Canonical ID (osha:inspection:{activity_nr})
- `activity_nr` - OSHA Activity Number
- `date_opened` - Inspection open date
- `inspection_type` - Fat/Cat, Accident, Complaint, etc.
- `scope` - Complete or Partial
- `case_status` - Open, Closed, etc.
- `establishment_name` - Company name
- `site_city`, `site_state`, `site_zip` - Location
- `naics`, `naics_desc` - Industry code and description
- `violations_count` - Total violations if posted
- `emphasis` - Emphasis program if applicable
- `lead_score` - Commercial intent score
- `first_seen_at` - When we first observed this inspection
- `source_url` - OSHA detail page URL

### needs_review_{date}.csv

Leads missing required fields. Includes additional:

- `site_address1` - Street address if available
- `missing_fields` - Which fields are missing

## Lead Scoring

Leads are scored based on commercial intent:

| Factor | Points |
|--------|--------|
| Fatality/Catastrophe inspection | +10 |
| Accident inspection | +8 |
| Complaint inspection | +4 |
| Referral inspection | +3 |
| Planned/Programmed inspection | +1 |
| Complete scope | +2 |
| Has violations | +3 |
| Construction (NAICS 23*) | +3 |
| Has emphasis program | +2 |

## Database

SQLite database with tables:

- `inspections` - Primary lead data
- `citations` - Placeholder for future citation tracking
- `suppression_list` - Opt-out emails/domains
- `ingestion_log` - Run history and stats

## Running Tests

```bash
python -m pytest tests_smoke.py -v
```

Tests use local HTTP fixtures and do not make external network requests.

## Scheduling

For daily automated runs, set up a cron job or Windows Task Scheduler:

```bash
# Example cron (daily at 6 AM)
0 6 * * * cd /path/to/osha-leads && ./venv/bin/python ingest_osha.py --db osha_leads.db && ./venv/bin/python export_daily.py --db osha_leads.db
```

## Opt-Out Process

To add a domain or email to the suppression list:

```sql
INSERT INTO suppression_list (email_or_domain, reason) 
VALUES ('example.com', 'Requested opt-out via email');
```

## Disclaimer

⚠️ **This tool is for informational purposes only.**

- Data is sourced from public OSHA records
- Users must independently verify all information
- No legal advice is provided or implied
- Contest deadlines and legal matters require professional consultation
- Not affiliated with OSHA or any government agency

## Data Sources

All data is obtained from public OSHA sources:

- OSHA Establishment Search: https://www.osha.gov/ords/imis/establishment.html
- Inspection detail pages under the same domain

We use polite rate-limiting and identify ourselves in the User-Agent header.

## License

Internal use only. Not for redistribution.
