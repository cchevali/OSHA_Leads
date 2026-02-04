#!/usr/bin/env python3
"""
OSHA Daily Lead Export Script

Exports inspection leads to CSV files for delivery to clients.
Produces separate files for sendable leads and those needing review.

Usage:
    python export_daily.py --db osha_leads.db --outdir ./out
"""

import argparse
import csv
import logging
import os
import sys
from datetime import datetime, timedelta
from typing import Optional

import sqlite3
from lead_filters import (
    apply_content_filter,
    dedupe_by_activity_nr,
    filter_by_territory,
)

# Logging setup
logger = logging.getLogger(__name__)


def setup_logging(level: str) -> None:
    """Configure logging with timestamp and level."""
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


# CSV columns for daily leads export
DAILY_LEADS_COLUMNS = [
    "lead_id",
    "activity_nr",
    "date_opened",
    "inspection_type",
    "scope",
    "case_status",
    "establishment_name",
    "site_city",
    "site_state",
    "site_zip",
    "area_office",
    "naics",
    "naics_desc",
    "violations_count",
    "emphasis",
    "lead_score",
    "first_seen_at",
    "last_seen_at",
    "source_url",
]

# Additional columns for needs_review export
NEEDS_REVIEW_COLUMNS = DAILY_LEADS_COLUMNS + [
    "site_address1",
    "missing_fields",
]


def get_sendable_leads(
    conn: sqlite3.Connection,
    as_of_date: str,
) -> list[dict]:
    """
    Get leads that are sendable (meet all required field criteria).
    Returns OPEN leads opened in the last 14 days, sorted by score then date.
    """
    # Calculate 14-day opened window based on as_of_date
    as_of_dt = datetime.strptime(as_of_date, "%Y-%m-%d").date()
    start_date = (as_of_dt - timedelta(days=14)).isoformat()
    end_date = as_of_dt.isoformat()
    
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(inspections)")
    columns = {row[1] for row in cursor.fetchall()}
    area_office_expr = "area_office" if "area_office" in columns else "NULL AS area_office"

    query = f"""
        SELECT 
            lead_id,
            activity_nr,
            date_opened,
            inspection_type,
            scope,
            case_status,
            establishment_name,
            site_city,
            site_state,
            site_zip,
            {area_office_expr},
            naics,
            naics_desc,
            violations_count,
            emphasis,
            lead_score,
            first_seen_at,
            last_seen_at,
            source_url
        FROM inspections
        WHERE 
            needs_review = 0
            AND case_status = 'OPEN'
            AND date_opened IS NOT NULL
            AND date_opened != ''
            AND date_opened >= ?
            AND date_opened <= ?
        ORDER BY 
            lead_score DESC,
            date_opened DESC
    """

    cursor.execute(query, (start_date, end_date))
    
    columns = [desc[0] for desc in cursor.description]
    results = []
    
    for row in cursor.fetchall():
        results.append(dict(zip(columns, row)))
    
    return results


def get_needs_review_leads(
    conn: sqlite3.Connection,
    as_of_date: str,
) -> list[dict]:
    """
    Get leads that need review (missing required fields).
    Returns leads seen in last 24h.
    """
    # Calculate 24 hours ago from as_of_date
    as_of_dt = datetime.strptime(as_of_date, "%Y-%m-%d")
    cutoff = (as_of_dt - timedelta(hours=24)).isoformat()
    
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(inspections)")
    columns = {row[1] for row in cursor.fetchall()}
    area_office_expr = "area_office" if "area_office" in columns else "NULL AS area_office"

    query = f"""
        SELECT 
            lead_id,
            activity_nr,
            date_opened,
            inspection_type,
            scope,
            case_status,
            establishment_name,
            site_address1,
            site_city,
            site_state,
            site_zip,
            {area_office_expr},
            naics,
            naics_desc,
            violations_count,
            emphasis,
            lead_score,
            first_seen_at,
            last_seen_at,
            source_url
        FROM inspections
        WHERE 
            needs_review = 1
            AND first_seen_at >= ?
        ORDER BY 
            lead_score DESC,
            date_opened DESC
    """

    cursor.execute(query, (cutoff,))
    
    columns = [desc[0] for desc in cursor.description]
    results = []
    
    for row in cursor.fetchall():
        lead = dict(zip(columns, row))
        
        # Determine which fields are missing
        missing = []
        if not lead.get("activity_nr"):
            missing.append("activity_nr")
        if not lead.get("establishment_name"):
            missing.append("establishment_name")
        if not lead.get("site_state"):
            missing.append("site_state")
        if not lead.get("date_opened"):
            missing.append("date_opened")
        if not lead.get("site_city") and not lead.get("site_zip"):
            missing.append("site_city/zip")
        
        lead["missing_fields"] = "; ".join(missing) if missing else ""
        results.append(lead)
    
    return results


def get_suppressed_domains(conn: sqlite3.Connection) -> set[str]:
    """Get set of suppressed email domains."""
    cursor = conn.cursor()
    cursor.execute("SELECT email_or_domain FROM suppression_list")
    return {row[0].lower() for row in cursor.fetchall()}


def write_csv(
    filepath: str,
    leads: list[dict],
    columns: list[str],
) -> int:
    """Write leads to CSV file. Returns count written."""
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        
        for lead in leads:
            # Clean up None values for CSV
            cleaned = {k: (v if v is not None else "") for k, v in lead.items()}
            writer.writerow(cleaned)
    
    return len(leads)


def export_daily(
    db_path: str,
    outdir: str,
    as_of_date: str,
    territory_code: Optional[str] = None,
    content_filter: str = "all",
) -> dict:
    """
    Main export routine.
    Returns stats dict.
    """
    stats = {
        "sendable_leads": 0,
        "needs_review_leads": 0,
        "daily_leads_file": None,
        "needs_review_file": None,
        "territory_code": territory_code,
        "content_filter": content_filter,
        "excluded_by_territory": 0,
        "excluded_by_content_filter": 0,
        "deduped_records_removed": 0,
    }
    
    # Ensure output directory exists
    os.makedirs(outdir, exist_ok=True)
    
    # Connect to database
    conn = sqlite3.connect(db_path)
    
    try:
        # Get suppression list (for future use)
        suppressed = get_suppressed_domains(conn)
        if suppressed:
            logger.info(f"Loaded {len(suppressed)} suppressed domains")
        
        # Export sendable leads
        sendable = get_sendable_leads(conn, as_of_date)

        if territory_code:
            sendable, territory_stats = filter_by_territory(sendable, territory_code)
            stats["excluded_by_territory"] = (
                territory_stats["excluded_state"] + territory_stats["excluded_territory"]
            )

        sendable, excluded_content = apply_content_filter(sendable, content_filter)
        stats["excluded_by_content_filter"] = excluded_content

        sendable, deduped_removed = dedupe_by_activity_nr(sendable)
        stats["deduped_records_removed"] = deduped_removed
        
        if sendable:
            daily_file = os.path.join(
                outdir,
                f"daily_leads_{as_of_date}.csv"
            )
            count = write_csv(daily_file, sendable, DAILY_LEADS_COLUMNS)
            stats["sendable_leads"] = count
            stats["daily_leads_file"] = daily_file
            logger.info(f"Exported {count} sendable leads to {daily_file}")
        else:
            logger.info("No sendable leads found for today")
        
        # Export needs_review leads
        needs_review = get_needs_review_leads(conn, as_of_date)
        
        if needs_review:
            review_file = os.path.join(
                outdir,
                f"needs_review_{as_of_date}.csv"
            )
            count = write_csv(review_file, needs_review, NEEDS_REVIEW_COLUMNS)
            stats["needs_review_leads"] = count
            stats["needs_review_file"] = review_file
            logger.info(f"Exported {count} needs-review leads to {review_file}")
        else:
            logger.info("No needs-review leads found for today")
        
    finally:
        conn.close()
    
    return stats


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Export OSHA inspection leads to CSV"
    )
    parser.add_argument(
        "--db",
        required=True,
        help="Path to SQLite database file"
    )
    parser.add_argument(
        "--outdir",
        default="./out",
        help="Output directory for CSV files (default: ./out)"
    )
    parser.add_argument(
        "--as-of-date",
        default=None,
        help="Export as of date YYYY-MM-DD (default: today)"
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: INFO)"
    )
    parser.add_argument(
        "--territory-code",
        default=None,
        help="Optional territory code filter (e.g., TX_TRIANGLE_V1)"
    )
    parser.add_argument(
        "--content-filter",
        default="all",
        help="Content filter: all, high_medium (high+medium), or high_only"
    )
    
    args = parser.parse_args()
    
    setup_logging(args.log_level)
    
    # Default to today if not specified
    as_of_date = args.as_of_date or datetime.now().strftime("%Y-%m-%d")
    
    logger.info(f"Starting export: db={args.db}, outdir={args.outdir}, as_of={as_of_date}")
    
    if not os.path.exists(args.db):
        logger.error(f"Database not found: {args.db}")
        sys.exit(1)
    
    try:
        stats = export_daily(
            db_path=args.db,
            outdir=args.outdir,
            as_of_date=as_of_date,
            territory_code=args.territory_code,
            content_filter=args.content_filter,
        )
        
        logger.info(f"Export complete: {stats}")
        print(f"\nExport Summary:")
        print(f"  As of date:        {as_of_date}")
        print(f"  Sendable leads:    {stats['sendable_leads']}")
        print(f"  Needs review:      {stats['needs_review_leads']}")
        if args.territory_code:
            print(f"  Territory filter:  {args.territory_code}")
            print(f"  Excl. territory:   {stats['excluded_by_territory']}")
        print(f"  Content filter:    {stats['content_filter']}")
        print(f"  Excl. by score:    {stats['excluded_by_content_filter']}")
        print(f"  Dedupe removed:    {stats['deduped_records_removed']}")
        
        if stats["daily_leads_file"]:
            print(f"  Daily file:        {stats['daily_leads_file']}")
        if stats["needs_review_file"]:
            print(f"  Review file:       {stats['needs_review_file']}")
        
    except Exception as e:
        logger.error(f"Export failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
