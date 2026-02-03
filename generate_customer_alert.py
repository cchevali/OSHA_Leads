#!/usr/bin/env python3
"""
Generate customer-specific OSHA alert pack.

Loads customer config JSON and generates:
- Multi-section digest (Top K overall + Top N per state)
- Full leads CSV
- Appends to daily_metrics.csv
"""

import argparse
import csv
import json
import sqlite3
import logging
import os
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


def load_customer_config(config_path: str) -> dict:
    """Load customer configuration from JSON file."""
    with open(config_path, 'r') as f:
        return json.load(f)


def get_leads_for_period(
    conn: sqlite3.Connection, 
    states: list, 
    since_days: int,
    new_only_days: int = 1,
    skip_first_seen_filter: bool = False,
) -> tuple[list, dict]:
    """
    Get leads within the specified period and states.
    Returns tuple of (filtered_leads, exclusion_stats).
    
    If skip_first_seen_filter=True, no first_seen filter is applied (baseline mode).
    """
    today = datetime.now()
    date_opened_cutoff = (today - timedelta(days=since_days)).strftime("%Y-%m-%d")
    first_seen_cutoff = (today - timedelta(days=new_only_days)).strftime("%Y-%m-%d %H:%M:%S")
    
    placeholders = ",".join(["?" for _ in states])
    
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
    all_results = [dict(zip(columns, row)) for row in cursor.fetchall()]
    
    # Apply filters
    filtered = []
    exclusion_stats = {
        "total_before_filter": len(all_results),
        "excluded_by_date_opened": 0,
        "excluded_by_first_seen": 0,
    }
    
    for lead in all_results:
        date_opened = lead.get("date_opened")
        first_seen = lead.get("first_seen_at")
        
        if date_opened and date_opened < date_opened_cutoff:
            exclusion_stats["excluded_by_date_opened"] += 1
            continue
        
        # Skip first_seen filter in baseline mode
        if not skip_first_seen_filter:
            if first_seen and first_seen < first_seen_cutoff:
                exclusion_stats["excluded_by_first_seen"] += 1
                continue
        
        filtered.append(lead)
    
    return filtered, exclusion_stats


def generate_lead_table(leads: list, max_rows: int) -> list[str]:
    """Generate markdown table rows for leads."""
    lines = []
    lines.append("| Company | City | NAICS | Type | Date Opened | Score | Link |")
    lines.append("|---------|------|-------|------|-------------|-------|------|")
    
    for lead in leads[:max_rows]:
        company = (lead.get("establishment_name") or "Unknown")[:40]
        city = lead.get("site_city") or "-"
        state = lead.get("site_state") or "-"
        naics = lead.get("naics") or "-"
        itype = lead.get("inspection_type") or "-"
        date_opened = lead.get("date_opened") or "-"
        score = lead.get("lead_score") or 0
        url = lead.get("source_url") or "#"
        
        company = company.replace("|", "\\|")
        lines.append(f"| {company} | {city}, {state} | {naics} | {itype} | {date_opened} | {score} | [View]({url}) |")
    
    return lines


def generate_customer_digest(
    leads: list,
    config: dict,
    gen_date: str,
    output_path: str,
    mode: str = "daily",
) -> None:
    """Generate multi-section customer digest."""
    customer_id = config["customer_id"]
    states = config["states"]
    since_days = config["opened_window_days"]
    new_only_days = config["new_only_days"]
    top_k_overall = config["top_k_overall"]
    top_k_per_state = config["top_k_per_state"]
    
    lines = []
    mode_label = "BASELINE" if mode == "baseline" else "DAILY"
    lines.append(f"# {customer_id.upper()} — OSHA Lead Digest ({mode_label}) — {gen_date}")
    lines.append("")
    lines.append(f"**Coverage:** {', '.join(states)}")
    
    if mode == "baseline":
        lines.append(f"**Filters:** Opened in last {since_days} days (no first-seen filter — baseline snapshot).")
    else:
        lines.append(f"**Filters:** Opened in last {since_days} days; first observed in last {new_only_days} day(s).")
    lines.append("")
    
    # Summary
    lines.append("## Summary")
    lines.append("")
    lines.append(f"- **Total leads:** {len(leads)}")
    
    # Per-state counts
    state_counts = {}
    for lead in leads:
        st = lead.get("site_state") or "UNK"
        state_counts[st] = state_counts.get(st, 0) + 1
    
    for state in states:
        lines.append(f"- **{state}:** {state_counts.get(state, 0)} leads")
    lines.append("")
    
    # Date range
    dates = [l.get("date_opened") for l in leads if l.get("date_opened")]
    if dates:
        lines.append(f"- **Date range:** {min(dates)} to {max(dates)}")
        lines.append("")
    
    # Top K Overall
    lines.append(f"## Top {top_k_overall} Leads — All States")
    lines.append("")
    lines.extend(generate_lead_table(leads, top_k_overall))
    lines.append("")
    
    # Per-state sections
    for state in states:
        state_leads = [l for l in leads if l.get("site_state") == state]
        lines.append(f"## Top {top_k_per_state} Leads — {state}")
        lines.append("")
        if state_leads:
            lines.extend(generate_lead_table(state_leads, top_k_per_state))
        else:
            lines.append("*No leads for this state.*")
        lines.append("")
    
    # Score distribution
    lines.append("## Score Distribution")
    lines.append("")
    score_counts = {}
    for lead in leads:
        score = lead.get("lead_score") or 0
        score_counts[score] = score_counts.get(score, 0) + 1
    
    for score in sorted(score_counts.keys(), reverse=True):
        lines.append(f"- Score {score}: {score_counts[score]} leads")
    lines.append("")
    
    # Footer
    lines.append("---")
    lines.append("")
    lines.append("*This report contains public OSHA inspection data and is for informational purposes only.*")
    lines.append("*This is not legal advice. Verify all information before taking action.*")
    lines.append("")
    lines.append("**To opt out:** Reply 'opt out' to be removed from future reports.")
    lines.append("")
    
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write("\n".join(lines))
    
    logger.info(f"Generated customer digest at {output_path}")


def generate_csv(leads: list, output_path: str) -> None:
    """Generate CSV output file."""
    if not leads:
        logger.warning("No leads to export to CSV")
        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            f.write("lead_id,activity_nr,date_opened,inspection_type,scope,case_status,establishment_name,site_city,site_state,site_zip,naics,naics_desc,violations_count,emphasis,lead_score,first_seen_at,source_url\n")
        return
    
    fieldnames = list(leads[0].keys())
    
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(leads)
    
    logger.info(f"Exported {len(leads)} leads to {output_path}")


def append_daily_metrics(
    metrics_path: str,
    gen_date: str,
    customer_id: str,
    leads: list,
    states: list,
    mode: str = "daily",
) -> None:
    """Append per-state metrics to daily_metrics.csv."""
    file_exists = os.path.exists(metrics_path)
    
    with open(metrics_path, 'a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        
        if not file_exists:
            writer.writerow(["date", "customer_id", "mode", "state", "after_filter_count", "top_score", "count_score_gte_8"])
        
        for state in states:
            state_leads = [l for l in leads if l.get("site_state") == state]
            count = len(state_leads)
            top_score = max((l.get("lead_score") or 0 for l in state_leads), default=0)
            count_gte_8 = sum(1 for l in state_leads if (l.get("lead_score") or 0) >= 8)
            
            writer.writerow([gen_date, customer_id, mode, state, count, top_score, count_gte_8])
    
    logger.info(f"Appended {len(states)} rows to {metrics_path}")


def print_qa_summary(leads: list, config: dict, exclusion_stats: dict, elapsed: float, mode: str) -> None:
    """Print QA summary to console."""
    states = config["states"]
    
    print("\n" + "=" * 70)
    print(f"QA SUMMARY ({mode.upper()})")
    print("=" * 70)
    print(f"Customer:                 {config['customer_id']}")
    print(f"Mode:                     {mode}")
    print(f"States:                   {', '.join(states)}")
    print()
    print(f"Total before filter:      {exclusion_stats['total_before_filter']}")
    print(f"Excluded by date_opened:  {exclusion_stats['excluded_by_date_opened']}")
    print(f"Excluded by first_seen:   {exclusion_stats['excluded_by_first_seen']}")
    print(f"Total after filter:       {len(leads)}")
    print()
    
    # Per-state counts
    print("Per-state counts after filter:")
    for state in states:
        state_count = sum(1 for l in leads if l.get("site_state") == state)
        print(f"  {state}: {state_count}")
    print()
    
    # Date range
    dates = [l.get("date_opened") for l in leads if l.get("date_opened")]
    if dates:
        print(f"date_opened range:        {min(dates)} to {max(dates)}")
        print()
    
    # Inspection type counts
    print("inspection_type counts:")
    type_counts = {}
    for lead in leads:
        itype = lead.get("inspection_type") or "NULL"
        type_counts[itype] = type_counts.get(itype, 0) + 1
    for itype, count in sorted(type_counts.items(), key=lambda x: -x[1])[:10]:
        print(f"  {itype}: {count}")
    print()
    
    # Score distribution
    print("lead_score distribution:")
    score_counts = {}
    for lead in leads:
        score = lead.get("lead_score") or 0
        score_counts[score] = score_counts.get(score, 0) + 1
    for score in sorted(score_counts.keys(), reverse=True):
        print(f"  Score {score}: {score_counts[score]}")
    print()
    
    # High-value count
    count_gte_8 = sum(1 for l in leads if (l.get("lead_score") or 0) >= 8)
    print(f"Leads with score >= 8:    {count_gte_8}")
    print()
    print(f"Total runtime:            {elapsed:.2f}s")
    print("=" * 70)


def main():
    parser = argparse.ArgumentParser(description="Generate customer-specific OSHA alert pack")
    parser.add_argument("--db", required=True, help="Path to SQLite database")
    parser.add_argument("--customer", required=True, help="Path to customer config JSON")
    parser.add_argument("--mode", choices=["baseline", "daily"], default="daily",
                        help="Output mode: 'baseline' (day-1, no first-seen filter) or 'daily' (ongoing)")
    parser.add_argument("--output-dir", default="out", help="Output directory (default: out)")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    
    args = parser.parse_args()
    setup_logging(args.log_level)
    
    start_time = time.time()
    gen_date = datetime.now().strftime("%Y-%m-%d")
    
    # Load customer config
    config = load_customer_config(args.customer)
    customer_id = config["customer_id"]
    states = config["states"]
    mode = args.mode
    
    # Baseline mode: no first_seen filter
    skip_first_seen = (mode == "baseline")
    
    logger.info(f"Generating {mode} alert for customer={customer_id}, states={states}")
    
    # Connect and get leads
    conn = sqlite3.connect(args.db)
    leads, exclusion_stats = get_leads_for_period(
        conn, 
        states, 
        config["opened_window_days"], 
        config["new_only_days"],
        skip_first_seen_filter=skip_first_seen
    )
    conn.close()
    
    logger.info(f"Found {len(leads)} leads after filtering")
    
    # Ensure output directory exists
    Path(args.output_dir).mkdir(parents=True, exist_ok=True)
    
    # Generate outputs with mode-specific filenames
    digest_path = os.path.join(args.output_dir, f"{customer_id}_{mode}_{gen_date}.md")
    csv_path = os.path.join(args.output_dir, f"{customer_id}_{mode}_{gen_date}.csv")
    metrics_path = os.path.join(args.output_dir, "daily_metrics.csv")
    
    generate_customer_digest(leads, config, gen_date, digest_path, mode)
    generate_csv(leads, csv_path)
    append_daily_metrics(metrics_path, gen_date, customer_id, leads, states, mode)
    
    elapsed = time.time() - start_time
    print_qa_summary(leads, config, exclusion_stats, elapsed, mode)


if __name__ == "__main__":
    main()
