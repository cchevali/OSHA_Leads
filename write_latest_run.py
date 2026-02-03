#!/usr/bin/env python3
"""
Generate out/latest_run.json metadata from out/latest_leads.csv.

Run this after ingestion to create a single source of truth for data freshness.
The outbound_cold_email.py script uses this file to enforce freshness gates.

Usage:
    python write_latest_run.py
    python write_latest_run.py --csv out/daily_leads_2026-01-28.csv
"""

import argparse
import csv
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent.resolve()
DEFAULT_CSV = SCRIPT_DIR / "out" / "latest_leads.csv"
OUTPUT_JSON = SCRIPT_DIR / "out" / "latest_run.json"


def get_git_commit() -> str:
    """Get current git commit hash, or empty string if not in a git repo."""
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            cwd=SCRIPT_DIR
        )
        if result.returncode == 0:
            return result.stdout.strip()[:8]
    except Exception:
        pass
    return ""


def parse_iso_datetime(s: str) -> datetime | None:
    """Parse ISO datetime string, return None on failure."""
    if not s:
        return None
    try:
        # Handle various ISO formats
        s = s.replace("Z", "+00:00")
        if "T" in s:
            return datetime.fromisoformat(s)
        else:
            return datetime.strptime(s, "%Y-%m-%d")
    except (ValueError, TypeError):
        return None


def generate_run_metadata(csv_path: Path) -> dict:
    """Generate run metadata from a leads CSV file."""
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")
    
    # Read CSV and compute stats
    states = set()
    max_date_opened = None
    max_first_seen = None
    records_total = 0
    
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            records_total += 1
            
            # Track states
            state = row.get("site_state", "")
            if state:
                states.add(state)
            
            # Track max date_opened
            opened = row.get("date_opened", "")
            if opened:
                opened_dt = parse_iso_datetime(opened)
                if opened_dt:
                    if max_date_opened is None or opened_dt > max_date_opened:
                        max_date_opened = opened_dt
            
            # Track max first_seen_at
            first_seen = row.get("first_seen_at", "")
            if first_seen:
                first_seen_dt = parse_iso_datetime(first_seen)
                if first_seen_dt:
                    # Ensure timezone-aware for comparison
                    if first_seen_dt.tzinfo is None:
                        first_seen_dt = first_seen_dt.replace(tzinfo=timezone.utc)
                    if max_first_seen is None or first_seen_dt > max_first_seen:
                        max_first_seen = first_seen_dt
    
    # Build metadata
    now = datetime.now(timezone.utc)
    metadata = {
        "generated_at": now.isoformat(),
        "source": "OSHA Establishment Search",
        "states_included": sorted(list(states)),
        "records_total": records_total,
        "max_date_opened": max_date_opened.strftime("%Y-%m-%d") if max_date_opened else None,
        "max_first_seen_at": max_first_seen.isoformat() if max_first_seen else None,
        "csv_path": str(csv_path.relative_to(SCRIPT_DIR)) if csv_path.is_relative_to(SCRIPT_DIR) else str(csv_path),
        "schema_version": "1.0",
        "git_commit": get_git_commit() or None
    }
    
    return metadata


def write_metadata(metadata: dict, output_path: Path):
    """Write metadata to JSON file atomically (write temp, then rename)."""
    temp_path = output_path.with_suffix(".tmp")
    
    with open(temp_path, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2)
    
    # Atomic rename
    temp_path.replace(output_path)
    print(f"[OK] Wrote: {output_path}")


def main():
    parser = argparse.ArgumentParser(
        description="Generate out/latest_run.json metadata from leads CSV"
    )
    parser.add_argument(
        "--csv", type=str, default=None,
        help=f"Path to leads CSV (default: {DEFAULT_CSV})"
    )
    args = parser.parse_args()
    
    csv_path = Path(args.csv) if args.csv else DEFAULT_CSV
    
    try:
        metadata = generate_run_metadata(csv_path)
        write_metadata(metadata, OUTPUT_JSON)
        
        # Print summary
        print(f"  Source: {metadata['source']}")
        print(f"  Records: {metadata['records_total']}")
        print(f"  States: {', '.join(metadata['states_included'])}")
        print(f"  Max date_opened: {metadata['max_date_opened']}")
        print(f"  Max first_seen_at: {metadata['max_first_seen_at']}")
        
    except Exception as e:
        print(f"[ERROR] {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
