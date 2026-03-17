#!/usr/bin/env python3
"""
Generate customer-ready alert pack (CSV + Markdown Digest).

Filters:
- since_days: Only include inspections where date_opened >= (today - N days)
- new_only_days: Only include inspections where first_seen_at >= (today - N days)
"""

import argparse
import csv
import sqlite3
import logging
import re
import time
from datetime import datetime, timedelta
from pathlib import Path

logger = logging.getLogger(__name__)


def setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def get_leads_for_period(
    conn: sqlite3.Connection, 
    states: list, 
    since_days: int,
    new_only_days: int = 1,
) -> tuple[list, dict]:
    """
    Get leads within the specified period and states.
    
    Args:
        states: List of state codes to filter
        since_days: Only include where date_opened >= (today - N days)
        new_only_days: Only include where first_seen_at >= (today - N days)
    
    Returns:
        Tuple of (filtered_leads, exclusion_stats)
    """
    # Calculate cutoffs
    today = datetime.now()
    date_opened_cutoff = (today - timedelta(days=since_days)).strftime("%Y-%m-%d")
    first_seen_cutoff = (today - timedelta(days=new_only_days)).strftime("%Y-%m-%d %H:%M:%S")
    
    placeholders = ",".join(["?" for _ in states])
    
    # Get all leads for states, then filter properly
    query = f"""
        SELECT 
            lead_id, activity_nr, date_opened, inspection_type, scope, 
            case_status, establishment_name, site_city, site_state, site_zip,
            naics, naics_desc, violations_count, emphasis, lead_score,
            first_seen_at, source_url
        FROM inspections 
        WHERE site_state IN ({placeholders})
          AND parse_invalid = 0
        ORDER BY lead_score DESC, date_opened DESC
    """
    
    cursor = conn.cursor()
    cursor.execute(query, tuple(states))
    
    columns = [desc[0] for desc in cursor.description]
    all_results = []
    for row in cursor.fetchall():
        all_results.append(dict(zip(columns, row)))
    
    # Apply filters and track exclusions
    filtered = []
    exclusion_stats = {
        "total_before_filter": len(all_results),
        "excluded_by_date_opened": 0,
        "excluded_by_first_seen": 0,
    }
    
    for lead in all_results:
        date_opened = lead.get("date_opened")
        first_seen = lead.get("first_seen_at")
        
        # Filter 1: date_opened must be >= cutoff
        if date_opened and date_opened < date_opened_cutoff:
            exclusion_stats["excluded_by_date_opened"] += 1
            continue
        
        # Filter 2: first_seen_at must be >= cutoff (newly observed)
        if first_seen and first_seen < first_seen_cutoff:
            exclusion_stats["excluded_by_first_seen"] += 1
            continue
        
        filtered.append(lead)
    
    return filtered, exclusion_stats


def generate_csv(leads: list, output_path: str) -> None:
    """Generate CSV output file."""
    if not leads:
        logger.warning("No leads to export to CSV")
        # Write empty file with headers
        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            f.write("lead_id,activity_nr,date_opened,inspection_type,scope,case_status,establishment_name,site_city,site_state,site_zip,naics,naics_desc,violations_count,emphasis,lead_score,first_seen_at,source_url\n")
        return
    
    fieldnames = list(leads[0].keys())
    
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(leads)
    
    logger.info(f"Exported {len(leads)} leads to {output_path}")


def generate_digest(
    leads: list, 
    states: list, 
    since_days: int,
    new_only_days: int,
    output_path: str, 
    gen_date: str,
    top_k: int = 15,
) -> None:
    """Generate markdown digest for email."""
    
    state_str = "/".join(states)
    
    lines = []
    lines.append(f"# {state_str} OSHA New Inspections — {gen_date}")
    lines.append("")
    lines.append(f"**Filters:** Opened in last {since_days} days; first observed in last {new_only_days} day(s).")
    lines.append("")
    
    # Summary stats
    lines.append("## Summary")
    lines.append("")
    lines.append(f"- **Total inspections:** {len(leads)}")
    
    if leads:
        dates = [l.get("date_opened") for l in leads if l.get("date_opened")]
        if dates:
            lines.append(f"- **Date range:** {min(dates)} to {max(dates)}")
    
    # Count by inspection type
    type_counts = {}
    for lead in leads:
        itype = lead.get("inspection_type") or "Unknown"
        type_counts[itype] = type_counts.get(itype, 0) + 1
    
    lines.append(f"- **By type:**")
    for itype, count in sorted(type_counts.items(), key=lambda x: -x[1]):
        lines.append(f"  - {itype}: {count}")
    lines.append("")
    
    # Top leads
    lines.append(f"## Top {top_k} Leads by Score")
    lines.append("")
    
    if not leads:
        lines.append("*No leads match the filter criteria.*")
    else:
        lines.append("| Company | City | NAICS | Type | Date Opened | Score | Link |")
        lines.append("|---------|------|-------|------|-------------|-------|------|")
        
        top_15 = leads[:top_k]
        for lead in top_15:
            company = (lead.get("establishment_name") or "Unknown")[:40]
            city = lead.get("site_city") or "-"
            state = lead.get("site_state") or "TX"
            naics = lead.get("naics") or "-"
            itype = lead.get("inspection_type") or "-"
            date_opened = lead.get("date_opened") or "-"
            score = lead.get("lead_score") or 0
            url = lead.get("source_url") or "#"
            
            # Escape pipe characters in company name
            company = company.replace("|", "\\|")
            
            lines.append(f"| {company} | {city}, {state} | {naics} | {itype} | {date_opened} | {score} | [View]({url}) |")
    
    lines.append("")
    
    # Score distribution
    if leads:
        lines.append("## Score Distribution")
        lines.append("")
        score_counts = {}
        for lead in leads:
            score = lead.get("lead_score") or 0
            score_counts[score] = score_counts.get(score, 0) + 1
        
        for score in sorted(score_counts.keys(), reverse=True):
            lines.append(f"- Score {score}: {score_counts[score]} leads")
        lines.append("")
    
    # Footer / Disclaimer
    lines.append("---")
    lines.append("")
    lines.append("*This report contains public OSHA inspection data and is for informational purposes only.*")
    lines.append("*This is not legal advice. Verify all information before taking action.*")
    lines.append("")
    lines.append("**To opt out:** Reply 'opt out' to be removed from future reports.")
    lines.append("")
    
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write("\n".join(lines))
    
    logger.info(f"Generated digest at {output_path}")


def validate_and_report(leads: list, exclusion_stats: dict) -> dict:
    """Validate data quality and return stats."""
    stats = {
        "total": len(leads),
        "valid_establishment": 0,
        "valid_city": 0,
        "type_counts": {},
        "score_counts": {},
        "min_date_opened": None,
        "max_date_opened": None,
    }
    
    dates = []
    
    for lead in leads:
        # Establishment validation
        name = lead.get("establishment_name")
        if name and re.search(r"[A-Za-z]", name) and not re.match(r"^\d+\.?\d*$", str(name)):
            stats["valid_establishment"] += 1
        
        # City validation
        city = lead.get("site_city")
        if city and re.match(r"^[A-Za-z\s]+$", city.strip()):
            stats["valid_city"] += 1
        
        # Type counts
        itype = lead.get("inspection_type") or "NULL"
        stats["type_counts"][itype] = stats["type_counts"].get(itype, 0) + 1
        
        # Score counts
        score = lead.get("lead_score") or 0
        stats["score_counts"][score] = stats["score_counts"].get(score, 0) + 1
        
        # Date tracking
        date_opened = lead.get("date_opened")
        if date_opened:
            dates.append(date_opened)
    
    if dates:
        stats["min_date_opened"] = min(dates)
        stats["max_date_opened"] = max(dates)
    
    # Merge exclusion stats
    stats.update(exclusion_stats)
    
    return stats


def main():
    parser = argparse.ArgumentParser(description="Generate OSHA alert pack (CSV + Digest)")
    parser.add_argument("--db", required=True, help="Path to SQLite database")
    parser.add_argument("--states", default="TX", help="Comma-separated state codes")
    parser.add_argument("--since-days", type=int, default=30, 
                        help="Only include inspections opened within N days (default: 30)")
    parser.add_argument("--new-only-days", type=int, default=1,
                        help="Only include inspections first seen within N days (default: 1)")
    parser.add_argument("--top-k", type=int, default=15,
                        help="Number of top leads to show in digest (default: 15)")
    parser.add_argument("--output-csv", required=True, help="Output CSV path")
    parser.add_argument("--output-digest", required=True, help="Output markdown digest path")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    
    args = parser.parse_args()
    setup_logging(args.log_level)
    
    states = [s.strip().upper() for s in args.states.split(",")]
    gen_date = datetime.now().strftime("%Y-%m-%d")
    start_time = time.time()
    
    logger.info(f"Generating alert pack: states={states}, since_days={args.since_days}, new_only_days={args.new_only_days}, top_k={args.top_k}")
    
    # Connect and get leads
    conn = sqlite3.connect(args.db)
    leads, exclusion_stats = get_leads_for_period(
        conn, states, args.since_days, args.new_only_days
    )
    conn.close()
    
    logger.info(f"Found {len(leads)} leads after filtering")
    
    # Generate outputs
    Path(args.output_csv).parent.mkdir(parents=True, exist_ok=True)
    Path(args.output_digest).parent.mkdir(parents=True, exist_ok=True)
    
    generate_csv(leads, args.output_csv)
    generate_digest(leads, states, args.since_days, args.new_only_days, args.output_digest, gen_date, args.top_k)
    
    # Validate and report
    stats = validate_and_report(leads, exclusion_stats)
    
    print("\n" + "=" * 70)
    print("QA SUMMARY")
    print("=" * 70)
    print(f"Total before filter:      {stats['total_before_filter']}")
    print(f"Excluded by date_opened:  {stats['excluded_by_date_opened']} (older than {args.since_days} days)")
    print(f"Excluded by first_seen:   {stats['excluded_by_first_seen']} (observed before {args.new_only_days} day(s) ago)")
    print(f"Total after filter:       {stats['total']}")
    print()
    if stats['min_date_opened'] and stats['max_date_opened']:
        print(f"date_opened range:        {stats['min_date_opened']} to {stats['max_date_opened']}")
    print(f"establishment_name valid: {stats['valid_establishment']}/{stats['total']} ({100*stats['valid_establishment']/max(1,stats['total']):.1f}%)")
    print(f"site_city valid:          {stats['valid_city']}/{stats['total']} ({100*stats['valid_city']/max(1,stats['total']):.1f}%)")
    print()
    print("inspection_type counts:")
    for itype, count in sorted(stats['type_counts'].items(), key=lambda x: -x[1])[:10]:
        print(f"  {itype}: {count}")
    print()
    print("lead_score distribution:")
    for score in sorted(stats['score_counts'].keys(), reverse=True):
        print(f"  Score {score}: {stats['score_counts'][score]}")
    print()
    elapsed = time.time() - start_time
    print(f"Total runtime:            {elapsed:.2f}s")
    print("Rate-limit/backoff:       N/A (tracked in ingestion)")
    print("=" * 70)


if __name__ == "__main__":
    main()
