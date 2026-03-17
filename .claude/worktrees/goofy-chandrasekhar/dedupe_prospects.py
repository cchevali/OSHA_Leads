#!/usr/bin/env python3
"""
dedupe_prospects.py – Deduplicate and normalize prospect tracking CSV.

Usage:
    python dedupe_prospects.py out/prospect_tracking_template.csv

Output:
    out/prospect_tracking_deduped.csv
"""

import csv
import sys
import re
from pathlib import Path
from urllib.parse import urlparse

# ─────────────────────────────────────────────────────────────────────────────
# State Normalization
# ─────────────────────────────────────────────────────────────────────────────
STATE_MAP = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR",
    "california": "CA", "colorado": "CO", "connecticut": "CT", "delaware": "DE",
    "florida": "FL", "georgia": "GA", "hawaii": "HI", "idaho": "ID",
    "illinois": "IL", "indiana": "IN", "iowa": "IA", "kansas": "KS",
    "kentucky": "KY", "louisiana": "LA", "maine": "ME", "maryland": "MD",
    "massachusetts": "MA", "michigan": "MI", "minnesota": "MN", "mississippi": "MS",
    "missouri": "MO", "montana": "MT", "nebraska": "NE", "nevada": "NV",
    "new hampshire": "NH", "new jersey": "NJ", "new mexico": "NM", "new york": "NY",
    "north carolina": "NC", "north dakota": "ND", "ohio": "OH", "oklahoma": "OK",
    "oregon": "OR", "pennsylvania": "PA", "rhode island": "RI", "south carolina": "SC",
    "south dakota": "SD", "tennessee": "TN", "texas": "TX", "utah": "UT",
    "vermont": "VT", "virginia": "VA", "washington": "WA", "west virginia": "WV",
    "wisconsin": "WI", "wyoming": "WY", "district of columbia": "DC",
}

# ─────────────────────────────────────────────────────────────────────────────
# Contact Role Normalization
# ─────────────────────────────────────────────────────────────────────────────
ROLE_PATTERNS = [
    (r"\b(owner|president|ceo|founder)\b", "Owner/Executive"),
    (r"\b(vp safety|director.*ehs|safety director|director.*safety)\b", "Safety Director"),
    (r"\b(safety manager|ehs manager|manager.*safety)\b", "Safety Manager"),
    (r"\b(operations manager|ops manager)\b", "Operations Manager"),
    (r"\b(compliance officer|compliance manager)\b", "Compliance Officer"),
    (r"\b(consultant|advisor)\b", "Consultant"),
]


def normalize_domain(raw: str) -> str:
    """Extract and normalize domain from URL or raw domain string."""
    raw = raw.strip().lower()
    if not raw:
        return ""
    # Handle full URLs
    if raw.startswith(("http://", "https://")):
        parsed = urlparse(raw)
        raw = parsed.netloc or parsed.path
    # Strip www. prefix and trailing slashes/paths
    raw = re.sub(r"^www\.", "", raw)
    raw = raw.split("/")[0]
    return raw


def normalize_state(raw: str) -> str:
    """Convert state name to 2-letter abbreviation."""
    raw = raw.strip()
    if not raw:
        return ""
    # Already 2-letter?
    if len(raw) == 2 and raw.upper().isalpha():
        return raw.upper()
    # Lookup full name
    return STATE_MAP.get(raw.lower(), raw.upper()[:2] if raw else "")


def normalize_role(raw: str) -> str:
    """Map contact role to standard title."""
    raw = raw.strip().lower()
    if not raw:
        return ""
    for pattern, normalized in ROLE_PATTERNS:
        if re.search(pattern, raw, re.IGNORECASE):
            return normalized
    return "Other" if raw else ""


def normalize_company(raw: str) -> str:
    """Trim whitespace and apply title case."""
    raw = raw.strip()
    if not raw:
        return ""
    # Title case but preserve common acronyms
    words = raw.split()
    result = []
    for w in words:
        if w.upper() in ("LLC", "INC", "LTD", "CO", "LP", "LLP", "PC", "PA"):
            result.append(w.upper())
        else:
            result.append(w.title())
    return " ".join(result)


def merge_rows(existing: dict, new: dict) -> dict:
    """Merge two rows, preferring non-empty values. Keep existing if both have data."""
    merged = existing.copy()
    for key, val in new.items():
        existing_val = (existing.get(key) or "").strip()
        new_val = (val or "").strip()
        # Prefer non-empty value; if both have data, keep existing
        if not existing_val and new_val:
            merged[key] = new_val
        # For notes, concatenate if both have content
        elif key == "notes" and existing_val and new_val and new_val not in existing_val:
            merged[key] = f"{existing_val}; {new_val}"
    return merged


def dedupe_and_normalize(input_path: str) -> tuple[str, dict]:
    """
    Read CSV, dedupe by domain (merge to one row per domain), normalize fields, write output.
    Returns (output_path, stats_dict).
    """
    input_file = Path(input_path)
    if not input_file.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    output_file = input_file.parent / "prospect_tracking_deduped.csv"

    # domain -> merged row
    domain_rows: dict[str, dict] = {}
    # Rows without domains (keep all)
    no_domain_rows: list[dict] = []
    rows_in = 0
    duplicates = 0

    with open(input_file, "r", newline="", encoding="utf-8") as fin:
        reader = csv.DictReader(fin)
        fieldnames = reader.fieldnames or []

        if not fieldnames:
            raise ValueError("CSV has no headers")

        for row in reader:
            rows_in += 1

            # Normalize fields
            if "domain" in row:
                row["domain"] = normalize_domain(row["domain"])
            if "state" in row:
                row["state"] = normalize_state(row["state"])
            if "contact_role" in row:
                row["contact_role"] = normalize_role(row["contact_role"])
            if "company_name" in row:
                row["company_name"] = normalize_company(row["company_name"])

            domain = row.get("domain", "").lower()
            
            if not domain:
                # No domain - keep row as-is
                no_domain_rows.append(row)
            elif domain in domain_rows:
                # Duplicate domain - merge into existing row
                duplicates += 1
                domain_rows[domain] = merge_rows(domain_rows[domain], row)
            else:
                # First occurrence of this domain
                domain_rows[domain] = row

    # Combine unique domain rows + no-domain rows
    output_rows = list(domain_rows.values()) + no_domain_rows

    # Write output
    with open(output_file, "w", newline="", encoding="utf-8") as fout:
        writer = csv.DictWriter(fout, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(output_rows)

    stats = {
        "rows_in": rows_in,
        "rows_out": len(output_rows),
        "duplicates": duplicates,
        "unique_domains": len(domain_rows),
    }
    return str(output_file), stats


def main():
    if len(sys.argv) < 2:
        print("Usage: python dedupe_prospects.py <input_csv>")
        print("Example: python dedupe_prospects.py out/prospect_tracking_template.csv")
        sys.exit(1)

    input_path = sys.argv[1]

    try:
        output_path, stats = dedupe_and_normalize(input_path)
        print(f"[OK] Processed {stats['rows_in']} rows")
        print(f"  - Unique domains: {stats['unique_domains']}")
        print(f"  - Duplicates flagged: {stats['duplicates']}")
        print(f"  - Output: {output_path}")
    except Exception as e:
        print(f"[ERROR] {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
