#!/usr/bin/env python3
"""
OSHA Inspection Lead Ingestion Script

Fetches inspection data from OSHA public pages and stores in SQLite.
Uses polite rate-limiting and robust error handling.

Usage:
    python ingest_osha.py --db osha_leads.db --since-days 2 --states VA,MD,DC
"""

import argparse
import hashlib
import logging
import random
import re
import sqlite3
import sys
import time
from datetime import datetime, timedelta
from typing import Optional
from urllib.parse import urljoin, urlencode, urlparse

import requests
from bs4 import BeautifulSoup

# Constants
OSHA_BASE_URL = "https://www.osha.gov"
OSHA_SEARCH_URL = "https://www.osha.gov/ords/imis/establishment.html"
USER_AGENT = "OSHA-Lead-Monitor/0.1 (Educational/Research; +compliance)"
REQUEST_TIMEOUT = 30
MIN_DELAY = 0.3
MAX_DELAY = 0.7
BACKOFF_BASE = 2.0
MAX_RETRIES = 3

# Logging setup
logger = logging.getLogger(__name__)


def setup_logging(level: str) -> None:
    """Configure logging with timestamp and level."""
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def get_session() -> requests.Session:
    """Create a requests session with appropriate headers."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
    })
    return session


def polite_delay() -> None:
    """Sleep for a random interval between MIN_DELAY and MAX_DELAY."""
    time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))


def compute_hash(text: str) -> str:
    """Compute SHA256 hash of text for change detection."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:32]


def parse_date(date_str: Optional[str]) -> Optional[str]:
    """Parse various date formats to YYYY-MM-DD."""
    if not date_str:
        return None
    date_str = date_str.strip()
    
    # Try common formats
    formats = [
        "%m/%d/%Y",
        "%Y-%m-%d",
        "%m-%d-%Y",
        "%B %d, %Y",
        "%b %d, %Y",
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    
    return None


def clean_text(text: Optional[str]) -> Optional[str]:
    """Clean and normalize text."""
    if not text:
        return None
    # Normalize whitespace
    text = " ".join(text.split())
    return text.strip() if text else None


def extract_activity_nr(text: str) -> Optional[str]:
    """Extract OSHA activity number from text."""
    # Activity numbers are typically 9 digits
    match = re.search(r"\b(\d{9})\b", text)
    return match.group(1) if match else None


def calculate_lead_score(inspection: dict) -> int:
    """Calculate lead score based on scoring algorithm."""
    score = 0
    
    # Inspection type scoring
    insp_type = (inspection.get("inspection_type") or "").lower()
    if "fat" in insp_type or "cat" in insp_type:
        score += 10
    elif "accident" in insp_type:
        score += 8
    elif "complaint" in insp_type:
        score += 4
    elif "referral" in insp_type:
        score += 3
    elif "planned" in insp_type or "programmed" in insp_type:
        score += 1
    
    # Scope scoring
    scope = (inspection.get("scope") or "").lower()
    if "complete" in scope:
        score += 2
    
    # Violations scoring
    violations = inspection.get("violations_count")
    if violations is not None and violations >= 1:
        score += 3
    
    # NAICS construction scoring
    naics = inspection.get("naics") or ""
    if naics.startswith("23"):
        score += 3
    
    # Emphasis scoring
    emphasis = inspection.get("emphasis")
    if emphasis:
        score += 2
    
    return score


def check_needs_review(inspection: dict) -> bool:
    """Check if inspection is missing required fields."""
    required = ["activity_nr", "establishment_name", "site_state", "date_opened"]
    
    for field in required:
        if not inspection.get(field):
            return True
    
    # Need either city or zip
    if not inspection.get("site_city") and not inspection.get("site_zip"):
        return True
    
    return False


def fetch_with_retry(session: requests.Session, url: str, retries: int = MAX_RETRIES) -> Optional[requests.Response]:
    """Fetch URL with exponential backoff on errors."""
    for attempt in range(retries):
        try:
            polite_delay()
            response = session.get(url, timeout=REQUEST_TIMEOUT)
            
            if response.status_code == 200:
                return response
            elif response.status_code == 429:
                # Rate limited - back off significantly
                wait_time = BACKOFF_BASE ** (attempt + 2)
                logger.warning(f"Rate limited. Waiting {wait_time:.1f}s before retry...")
                time.sleep(wait_time)
            elif response.status_code >= 500:
                # Server error - back off
                wait_time = BACKOFF_BASE ** attempt
                logger.warning(f"Server error {response.status_code}. Waiting {wait_time:.1f}s...")
                time.sleep(wait_time)
            else:
                logger.warning(f"HTTP {response.status_code} for {url}")
                return None
                
        except requests.RequestException as e:
            wait_time = BACKOFF_BASE ** attempt
            logger.warning(f"Request error: {e}. Waiting {wait_time:.1f}s...")
            time.sleep(wait_time)
    
    logger.error(f"Failed to fetch {url} after {retries} retries")
    return None


def search_osha_inspections(
    session: requests.Session,
    state: str,
    since_date: str,
) -> list[dict]:
    """
    Search OSHA for inspections in a state since a date.
    Returns list of basic inspection info from results page.
    
    Note: OSHA requires a specific establishment search term, office, or zip.
    We search with common business name patterns to get broad coverage.
    """
    results = []
    seen_activity_nrs = set()
    
    # Parse since_date
    since_dt = datetime.strptime(since_date, "%Y-%m-%d")
    now = datetime.now()
    
    # Common business name patterns to search - covers most establishments
    # Each search returns up to ~500 results, so we use multiple terms
    search_terms = ["inc", "llc", "corp", "co", "services", "construction", "contractors"]
    
    for term in search_terms:
        # Build search URL
        params = {
            "p_logger": "1",
            "establishment": term,
            "State": state,
            "officetype": "all",
            "Office": "all",
            "sitezip": "",
            "p_case": "all",
            "p_violations_exist": "all",
            "startmonth": since_dt.strftime("%m"),
            "startday": since_dt.strftime("%d"),
            "startyear": since_dt.strftime("%Y"),
            "endmonth": now.strftime("%m"),
            "endday": now.strftime("%d"),
            "endyear": now.strftime("%Y"),
        }
        
        search_url = f"https://www.osha.gov/ords/imis/establishment.search?{urlencode(params)}"
        logger.info(f"Searching OSHA for state={state} term='{term}' since={since_date}")
        logger.debug(f"Search URL: {search_url}")
        
        response = fetch_with_retry(session, search_url)
        if not response:
            logger.warning(f"Failed to fetch search results for {state}/{term}")
            continue
        
        soup = BeautifulSoup(response.text, "html.parser")
        
        # Check for "no results" or redirect back to form
        page_text = soup.get_text().lower()
        if "enter an establishment" in page_text and "p_message" in response.url:
            logger.debug(f"No results for {state}/{term}")
            continue
        
        # Find results table - look for table with inspection detail links
        term_results = 0
        for table in soup.find_all("table"):
            rows = table.find_all("tr")
            
            for row in rows:
                cells = row.find_all(["td", "th"])
                if len(cells) < 5:
                    continue
                
                # Look for activity number links (format: establishment.inspection_detail?id=XXXXXXX.XXX)
                link = row.find("a", href=True)
                if not link:
                    continue
                
                href = link.get("href", "")
                if "inspection_detail" not in href.lower():
                    continue
                
                # Extract activity number from link text or URL
                link_text = link.get_text(strip=True)
                activity_nr = extract_activity_nr(link_text) or extract_activity_nr(href)
                
                # Also try extracting from the id parameter (format: id=XXXXXXX.XXX)
                if not activity_nr:
                    id_match = re.search(r"id=(\d+)", href)
                    if id_match:
                        activity_nr = id_match.group(1)
                
                if not activity_nr:
                    continue
                
                # Dedupe across search terms
                if activity_nr in seen_activity_nrs:
                    continue
                seen_activity_nrs.add(activity_nr)
                
                # Build detail URL - must include /ords/imis/ path
                if href.startswith("http"):
                    detail_url = href
                else:
                    # OSHA detail pages are under /ords/imis/
                    detail_url = f"https://www.osha.gov/ords/imis/{href}"
                
                inspection = {
                    "activity_nr": activity_nr,
                    "detail_url": detail_url,
                    "site_state": state,
                }
                
                # Try to extract fields from result row cells
                cell_texts = [clean_text(c.get_text()) for c in cells]
                
                for i, text in enumerate(cell_texts):
                    if not text:
                        continue
                    
                    # Look for date pattern (MM/DD/YYYY)
                    parsed_date = parse_date(text)
                    if parsed_date and not inspection.get("date_opened"):
                        inspection["date_opened"] = parsed_date
                        continue
                    
                    # Look for inspection type keywords
                    text_lower = text.lower()
                    if any(t in text_lower for t in ["complaint", "accident", "referral", "fat", "cat", "planned", "programmed"]):
                        if not inspection.get("inspection_type"):
                            inspection["inspection_type"] = text
                            continue
                    
                    # Look for scope
                    if text_lower in ["complete", "partial"]:
                        if not inspection.get("scope"):
                            inspection["scope"] = text
                            continue
                    
                    # Look for NAICS (6 digits starting with 2-9)
                    if re.match(r"^\d{6}$", text) and text[0] != "0":
                        if not inspection.get("naics"):
                            inspection["naics"] = text
                            continue
                    
                    # Look for violation count
                    if text.isdigit() and int(text) < 1000:
                        if not inspection.get("violations_count") and i > 5:
                            inspection["violations_count"] = int(text)
                            continue
                    
                    # Establishment name is usually the longest text, appears last
                    if len(text) > 10 and not text.isdigit() and "/" not in text:
                        if not inspection.get("establishment_name"):
                            inspection["establishment_name"] = text
                
                results.append(inspection)
                term_results += 1
                logger.debug(f"Found inspection: {activity_nr}")
        
        if term_results > 0:
            logger.info(f"Found {term_results} new inspections for {state}/{term}")
    
    logger.info(f"Total: Found {len(results)} unique inspections for {state}")
    return results


def parse_inspection_detail(html: str, url: str) -> dict:
    """
    Parse an OSHA inspection detail page.
    Returns dict with all available fields.
    """
    soup = BeautifulSoup(html, "html.parser")
    data = {"source_url": url, "raw_hash": compute_hash(html)}
    
    # Extract activity number from URL
    id_match = re.search(r"id=(\d+)", url)
    if id_match:
        data["activity_nr"] = id_match.group(1)
    else:
        # Some callers/tests use activity_nr= in the query string.
        q_match = re.search(r"(?:\\?|&)activity_nr=(\\d+)", url)
        if q_match:
            data["activity_nr"] = q_match.group(1)
    
    # Get page text for parsing
    page_text = soup.get_text()
    page_lines = [l.strip() for l in page_text.split("\n") if l.strip()]
    
    # === PRIMARY: Extract establishment name from "Inspection: XXXXXXX.XXX - Company Name" ===
    for line in page_lines:
        if line.startswith("Inspection:") and " - " in line:
            # Format: "Inspection: 1866601.015 - Miss Saigon Cafe, Inc."
            parts = line.split(" - ", 1)
            if len(parts) == 2:
                company_name = parts[1].strip()
                # Validate: must contain letters, not just numbers
                if company_name and re.search(r"[A-Za-z]", company_name):
                    data["establishment_name"] = company_name
                    logger.debug(f"Extracted establishment from header: {company_name}")
            break
    
    # === Parse Site Address block ===
    in_site_address = False
    site_lines = []
    for i, line in enumerate(page_lines):
        # Some variants omit the colon (e.g., "Site Address")
        if line.replace(":", "").strip() == "Site Address":
            in_site_address = True
            continue
        if in_site_address:
            # Stop at next section
            if "Mailing Address:" in line or "SIC:" in line or "NAICS:" in line:
                break
            if line and not line.startswith("Inspection"):
                site_lines.append(line)
            if len(site_lines) >= 3:  # Max 3 lines for address
                break
    
    # Parse site address lines
    if site_lines:
        # First line after "Site Address:" is often the company name (backup)
        if not data.get("establishment_name"):
            first_line = site_lines[0]
            if first_line and re.search(r"[A-Za-z]", first_line):
                # Make sure it's not an address (no numbers at start)
                if not re.match(r"^\d+\s+", first_line):
                    data["establishment_name"] = first_line
                    logger.debug(f"Extracted establishment from site address: {first_line}")
        
        # Parse address lines for street, city, state, zip
        for line in site_lines:
            # Look for City, ST ZZZZZ pattern (with possible street prefix concatenated)
            # Pattern: ...City, ST 12345 or ...City, ST 12345-6789
            csz_match = re.search(r"([A-Za-z][A-Za-z\s]{2,}),\s*([A-Z]{2})\s+(\d{5}(?:-\d{4})?)\s*$", line)
            if csz_match:
                raw_city = csz_match.group(1).strip()
                
                # Clean city - if it has lowercase followed by uppercase, it's concatenated
                # e.g., "Richmond AvenueHouston" -> "Houston"
                city_parts = re.split(r"(?<=[a-z])(?=[A-Z])", raw_city)
                if len(city_parts) > 1:
                    # Take the last part (the actual city)
                    data["site_city"] = city_parts[-1].strip()
                else:
                    data["site_city"] = raw_city
                
                data["site_state"] = csz_match.group(2)
                data["site_zip"] = csz_match.group(3)
                
                # Extract street address (everything before city)
                street_part = line[:csz_match.start()].strip()
                if street_part:
                    # Clean up if concatenated with city
                    if city_parts and len(city_parts) > 1:
                        # Street is everything before the last camelCase split
                        street_match = re.match(r"(.+?)([A-Z][a-z]+)$", street_part)
                        if street_match:
                            street_part = street_match.group(1).strip()
                    if street_part and not street_part.isdigit():
                        data["site_address1"] = street_part
                break
    
    # === Parse Mailing Address (similar logic) ===
    in_mail_address = False
    mail_lines = []
    for line in page_lines:
        if "Mailing Address:" in line:
            in_mail_address = True
            continue
        if in_mail_address:
            if "SIC:" in line or "NAICS:" in line or line.startswith("Inspection"):
                break
            if line:
                mail_lines.append(line)
            if len(mail_lines) >= 2:
                break
    
    for line in mail_lines:
        # Mailing format is often: "1421 Richmond Avenue, Houston, TX 77006"
        mail_match = re.search(r"(.+),\s*([A-Za-z\s]+),\s*([A-Z]{2})\s+(\d{5}(?:-\d{4})?)", line)
        if mail_match:
            data["mail_address1"] = mail_match.group(1).strip()
            data["mail_city"] = mail_match.group(2).strip()
            data["mail_state"] = mail_match.group(3)
            data["mail_zip"] = mail_match.group(4)
            break
    
    # === Parse other fields from page text ===
    for line in page_lines:
        line_lower = line.lower()
        
        # Case Status
        if line.startswith("Case Status:"):
            data["case_status"] = line.split(":", 1)[1].strip()
        
        # Date Opened
        elif line.startswith("Date Opened:"):
            date_str = line.split(":", 1)[1].strip()
            data["date_opened"] = parse_date(date_str)
        
        # Alternate label used on some pages/fixtures
        elif line.startswith("Open Date:"):
            date_str = line.split(":", 1)[1].strip()
            data["date_opened"] = parse_date(date_str)
        
        # Inspection Nr
        elif line.startswith("Inspection Nr:"):
            nr = line.split(":", 1)[1].strip()
            # Extract just the numeric part
            nr_match = re.match(r"(\d+)", nr)
            if nr_match:
                data["activity_nr"] = nr_match.group(1)
        
        # Alternate label used on some pages/fixtures
        elif line.startswith("Activity Nr:") and not data.get("activity_nr"):
            nr = line.split(":", 1)[1].strip()
            nr_match = re.search(r"(\\d+)", nr)
            if nr_match:
                data["activity_nr"] = nr_match.group(1)
        
        # Report ID
        elif line.startswith("Report ID:"):
            data["report_id"] = line.split(":", 1)[1].strip()

        # Area office
        elif line.startswith("Area Office:") and not data.get("area_office"):
            office = line.split(":", 1)[1].strip()
            if office:
                data["area_office"] = office

        # Alternate office label used on some pages/fixtures
        elif line.startswith("Office:") and not data.get("area_office"):
            office = line.split(":", 1)[1].strip()
            if office:
                data["area_office"] = office

        # Establishment Name (alternate path)
        elif line.startswith("Establishment Name:") and not data.get("establishment_name"):
            name = line.split(":", 1)[1].strip()
            if name and re.search(r"[A-Za-z]", name):
                data["establishment_name"] = name

        # Inspection Type (alternate path)
        elif line.startswith("Inspection Type:") and not data.get("inspection_type"):
            insp = line.split(":", 1)[1].strip()
            if insp:
                data["inspection_type"] = insp

        # Scope (alternate path)
        elif line.startswith("Scope:") and not data.get("scope"):
            scope_val = line.split(":", 1)[1].strip()
            if scope_val:
                data["scope"] = scope_val

        # Emphasis (alternate path)
        elif line.startswith("Emphasis:") and not data.get("emphasis"):
            emph = line.split(":", 1)[1].strip()
            if emph:
                data["emphasis"] = emph

        # Total violations (alternate path)
        elif ("violation" in line_lower) and line_lower.startswith("total"):
            try:
                value = line.split(":", 1)[1]
                m = re.search(r"\\d+", value)
                if m:
                    data["violations_count"] = int(m.group())
            except Exception:
                pass
        
        # NAICS
        elif line.startswith("NAICS:"):
            naics_val = line.split(":", 1)[1].strip()
            naics_match = re.match(r"(\d+)\s*[-/]?\s*(.*)", naics_val)
            if naics_match:
                data["naics"] = naics_match.group(1)
                if naics_match.group(2):
                    data["naics_desc"] = naics_match.group(2).strip()
            else:
                data["naics"] = naics_val
        
        # SIC
        elif line.startswith("SIC:"):
            data["sic"] = line.split(":", 1)[1].strip()
    
    # === Parse table data for inspection type, scope, violations ===
    # OSHA detail pages have a specific table structure:
    # - First table: Header row with ['Type', 'Activity Nr', 'Safety', 'Health']
    #                Data row with ['Accident', '2384224', '', '']
    tables = soup.find_all("table")
    
    if tables:
        first_table = tables[0]
        rows = first_table.find_all("tr")
        
        if len(rows) >= 2:
            # First row is header, second row is data
            header_cells = [c.get_text(strip=True).lower() for c in rows[0].find_all(["td", "th"])]
            data_cells = [c.get_text(strip=True) for c in rows[1].find_all(["td", "th"])]
            
            # Map header names to data values
            for i, header in enumerate(header_cells):
                if i < len(data_cells):
                    value = data_cells[i]
                    if not value:
                        continue
                    
                    if header == "type":
                        # Validate it's a real type, not a label like "Activity Nr"
                        valid_types = ["inspection", "accident", "complaint", "referral", 
                                       "planned", "programmed", "fat/cat", "follow-up", "other"]
                        if value.lower() in valid_types or any(t in value.lower() for t in valid_types):
                            data["inspection_type"] = value
                        else:
                            logger.debug(f"Skipping invalid inspection_type: {value}")
                    elif header == "scope":
                        if value.lower() in ["complete", "partial"]:
                            data["scope"] = value
    
    # Also check for violations table (second table)
    if len(tables) >= 2:
        viol_table = tables[1]
        rows = viol_table.find_all("tr")
        for row in rows:
            cells = row.find_all(["td", "th"])
            if len(cells) >= 2:
                label = clean_text(cells[0].get_text())
                value = clean_text(cells[1].get_text())
                
                if not label or not value:
                    continue
                
                label_lower = label.lower()
                
                if "total" in label_lower and "violation" in label_lower:
                    try:
                        data["violations_count"] = int(re.search(r"\d+", value).group())
                    except (AttributeError, ValueError):
                        pass
                elif "serious" in label_lower:
                    try:
                        data["serious_violations"] = int(re.search(r"\d+", value).group())
                    except (AttributeError, ValueError):
                        pass

    # Generic table label/value extraction (helps with alternate OSHA layouts and local fixtures)
    for table in tables:
        rows = table.find_all("tr")
        for row in rows:
            cells = row.find_all(["td", "th"])
            if len(cells) < 2:
                continue
            label = clean_text(cells[0].get_text())
            value = clean_text(cells[1].get_text())
            if not label or not value:
                continue

            label_norm = label.strip().rstrip(":").lower()
            if label_norm == "activity nr" and not data.get("activity_nr"):
                m = re.search(r"\d+", value)
                if m:
                    data["activity_nr"] = m.group()
            elif label_norm in ("date opened", "open date") and not data.get("date_opened"):
                data["date_opened"] = parse_date(value)
            elif label_norm in ("inspection type", "type") and not data.get("inspection_type"):
                data["inspection_type"] = value
            elif label_norm == "scope" and not data.get("scope"):
                data["scope"] = value
            elif label_norm == "case status" and not data.get("case_status"):
                data["case_status"] = value
            elif label_norm == "emphasis" and not data.get("emphasis"):
                data["emphasis"] = value
            elif label_norm == "safety/health" and not data.get("safety_health"):
                data["safety_health"] = value
            elif label_norm == "sic" and not data.get("sic"):
                data["sic"] = value
            elif label_norm == "naics" and not data.get("naics"):
                naics_match = re.match(r"(\d+)\s*[-/]?\s*(.*)", value)
                if naics_match:
                    data["naics"] = naics_match.group(1)
                    if naics_match.group(2):
                        data["naics_desc"] = naics_match.group(2).strip()
                else:
                    data["naics"] = value
            elif label_norm == "establishment name" and not data.get("establishment_name"):
                if re.search(r"[A-Za-z]", value):
                    data["establishment_name"] = value
            elif label_norm == "total violations" and data.get("violations_count") is None:
                m = re.search(r"\d+", value)
                if m:
                    data["violations_count"] = int(m.group())
            elif label_norm in ("area office", "office", "osha office") and not data.get("area_office"):
                data["area_office"] = value
    
    return data


def validate_establishment_name(name: Optional[str]) -> bool:
    """Check if establishment name is valid (contains letters, not just numbers/IDs)."""
    if not name:
        return False
    # Must contain at least one letter
    if not re.search(r"[A-Za-z]", name):
        return False
    # Should not be just a numeric ID like "1866601.015"
    if re.match(r"^\d+\.?\d*$", name.strip()):
        return False
    # Should have reasonable length
    if len(name.strip()) < 3:
        return False
    return True


def validate_city(city: Optional[str]) -> bool:
    """Check if city is valid (not concatenated with address)."""
    if not city:
        return False
    # Must contain only letters and spaces
    if not re.match(r"^[A-Za-z\s]+$", city.strip()):
        return False
    # Should have reasonable length
    if len(city.strip()) < 2 or len(city.strip()) > 50:
        return False
    return True


def upsert_inspection(conn: sqlite3.Connection, inspection: dict) -> tuple[bool, bool]:
    """
    Insert or update inspection record.
    Returns (is_new, is_updated).
    """
    cursor = conn.cursor()
    activity_nr = inspection.get("activity_nr")
    
    if not activity_nr:
        logger.warning("Cannot upsert inspection without activity_nr")
        return False, False
    
    # Validate key fields and set parse_invalid
    parse_invalid = 0
    invalid_reasons = []
    
    if not validate_establishment_name(inspection.get("establishment_name")):
        parse_invalid = 1
        invalid_reasons.append("establishment_name")
        logger.warning(f"Invalid establishment_name for {activity_nr}: '{inspection.get('establishment_name')}' - URL: {inspection.get('source_url')}")
    
    if not validate_city(inspection.get("site_city")):
        # Don't set parse_invalid for city, just clear it
        if inspection.get("site_city"):
            logger.debug(f"Invalid site_city for {activity_nr}: '{inspection.get('site_city')}' - clearing")
            inspection["site_city"] = None
    
    inspection["parse_invalid"] = parse_invalid
    
    # Check for existing record
    cursor.execute(
        "SELECT id, violations_count, case_status, raw_hash, parse_invalid FROM inspections WHERE activity_nr = ?",
        (activity_nr,)
    )
    existing = cursor.fetchone()
    
    # Calculate score and review status
    inspection["lead_score"] = calculate_lead_score(inspection)
    inspection["needs_review"] = 1 if check_needs_review(inspection) else 0
    
    now = datetime.utcnow().isoformat()
    
    if existing:
        # Update existing record
        existing_id, old_violations, old_status, old_hash, old_parse_invalid = existing
        
        # Check for material upgrade (re-alert)
        re_alert = 0
        new_violations = inspection.get("violations_count")
        new_status = inspection.get("case_status")
        
        if old_violations is None and new_violations is not None and new_violations >= 1:
            re_alert = 1
            logger.info(f"Material upgrade: {activity_nr} - violations posted")
        
        if old_status and new_status and old_status.upper() == "OPEN" and new_status.upper() == "CLOSED":
            re_alert = 1
            logger.info(f"Material upgrade: {activity_nr} - case closed")
        
        # Update only non-null fields (don't overwrite existing data with nulls)
        update_fields = []
        update_values = []
        
        for field in [
            "date_opened", "inspection_type", "scope", "case_status", "emphasis",
            "safety_health", "sic", "naics", "naics_desc", "violations_count",
            "serious_violations", "willful_violations", "repeat_violations", "other_violations",
            "establishment_name", "site_address1", "site_city", "site_state", "site_zip",
            "area_office", "mail_address1", "mail_city", "mail_state", "mail_zip",
            "report_id", "source_url", "raw_hash", "lead_score", "needs_review", "parse_invalid"
        ]:
            value = inspection.get(field)
            if value is not None:
                update_fields.append(f"{field} = ?")
                update_values.append(value)
        
        update_fields.append("last_seen_at = ?")
        update_values.append(now)
        
        if re_alert:
            update_fields.append("re_alert = ?")
            update_values.append(re_alert)
        
        update_values.append(existing_id)
        
        cursor.execute(
            f"UPDATE inspections SET {', '.join(update_fields)} WHERE id = ?",
            update_values
        )
        
        return False, True
    
    else:
        # Insert new record
        fields = [
            "activity_nr", "date_opened", "inspection_type", "scope", "case_status",
            "emphasis", "safety_health", "sic", "naics", "naics_desc",
            "violations_count", "serious_violations", "willful_violations",
            "repeat_violations", "other_violations", "establishment_name",
            "site_address1", "site_city", "site_state", "site_zip",
            "area_office", "mail_address1", "mail_city", "mail_state", "mail_zip",
            "report_id", "source_url", "raw_hash", "lead_score", "needs_review",
            "parse_invalid", "first_seen_at", "last_seen_at"
        ]
        
        inspection["first_seen_at"] = now
        inspection["last_seen_at"] = now
        
        values = [inspection.get(f) for f in fields]
        placeholders = ", ".join(["?" for _ in fields])
        field_names = ", ".join(fields)
        
        cursor.execute(
            f"INSERT INTO inspections ({field_names}) VALUES ({placeholders})",
            values
        )
        
        return True, False


def ensure_inspection_columns(conn: sqlite3.Connection) -> None:
    """Backfill optional columns for older databases."""
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(inspections)")
    existing = {row[1] for row in cursor.fetchall()}

    if "area_office" not in existing:
        cursor.execute("ALTER TABLE inspections ADD COLUMN area_office TEXT")
        conn.commit()
        logger.info("Added missing inspections.area_office column")


def run_ingestion(
    db_path: str,
    since_days: int,
    states: list[str],
    max_details: int,
) -> dict:
    """
    Main ingestion routine.
    Returns stats dict.
    """
    stats = {
        "results_found": 0,
        "details_fetched": 0,
        "rows_inserted": 0,
        "rows_updated": 0,
        "errors_count": 0,
    }
    
    # Calculate since date
    since_date = (datetime.now() - timedelta(days=since_days)).strftime("%Y-%m-%d")
    
    # Connect to database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    ensure_inspection_columns(conn)
    
    # Log ingestion run
    run_started = datetime.utcnow().isoformat()
    cursor.execute(
        "INSERT INTO ingestion_log (run_started_at, states_queried, since_days, status) VALUES (?, ?, ?, ?)",
        (run_started, ",".join(states), since_days, "running")
    )
    log_id = cursor.lastrowid
    conn.commit()
    
    session = get_session()
    all_inspections = []
    
    try:
        # Search each state
        for state in states:
            try:
                results = search_osha_inspections(session, state, since_date)
                all_inspections.extend(results)
                stats["results_found"] += len(results)
            except Exception as e:
                logger.error(f"Error searching state {state}: {e}")
                stats["errors_count"] += 1
        
        # Dedupe by activity_nr (may appear in multiple states)
        seen_activity_nrs = set()
        unique_inspections = []
        
        for insp in all_inspections:
            activity_nr = insp.get("activity_nr")
            if activity_nr and activity_nr not in seen_activity_nrs:
                seen_activity_nrs.add(activity_nr)
                unique_inspections.append(insp)
        
        logger.info(f"Found {len(unique_inspections)} unique inspections to process")
        
        # Fetch detail pages (up to max)
        details_to_fetch = unique_inspections[:max_details]
        
        for i, insp in enumerate(details_to_fetch):
            detail_url = insp.get("detail_url")
            if not detail_url:
                continue
            
            logger.info(f"Fetching detail {i+1}/{len(details_to_fetch)}: {insp.get('activity_nr')}")
            
            try:
                response = fetch_with_retry(session, detail_url)
                if response:
                    stats["details_fetched"] += 1
                    
                    # Parse detail page
                    detail_data = parse_inspection_detail(response.text, detail_url)
                    
                    # Merge with search results (detail is canonical)
                    merged = {**insp, **detail_data}
                    
                    # Upsert to database
                    is_new, is_updated = upsert_inspection(conn, merged)
                    
                    if is_new:
                        stats["rows_inserted"] += 1
                    elif is_updated:
                        stats["rows_updated"] += 1
                    
                    # Commit periodically
                    if (i + 1) % 10 == 0:
                        conn.commit()
                        logger.info(f"Progress: {i+1}/{len(details_to_fetch)} details processed")
                        
            except Exception as e:
                logger.error(f"Error processing {detail_url}: {e}")
                stats["errors_count"] += 1
        
        # Final commit
        conn.commit()
        
        # Update log
        cursor.execute(
            """UPDATE ingestion_log SET 
                run_completed_at = ?, results_found = ?, details_fetched = ?,
                rows_inserted = ?, rows_updated = ?, errors_count = ?, status = ?
            WHERE id = ?""",
            (
                datetime.utcnow().isoformat(),
                stats["results_found"],
                stats["details_fetched"],
                stats["rows_inserted"],
                stats["rows_updated"],
                stats["errors_count"],
                "completed",
                log_id,
            )
        )
        conn.commit()
        
    except Exception as e:
        logger.error(f"Ingestion error: {e}")
        cursor.execute(
            "UPDATE ingestion_log SET status = ?, error_message = ? WHERE id = ?",
            ("failed", str(e), log_id)
        )
        conn.commit()
        raise
        
    finally:
        conn.close()
    
    return stats


def refresh_invalid_records(db_path: str, max_details: int) -> dict:
    """
    Re-fetch and re-parse records that have parse_invalid=1.
    Returns stats dict.
    """
    stats = {
        "invalid_found": 0,
        "refreshed": 0,
        "fixed": 0,
        "still_invalid": 0,
        "errors": 0,
    }
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Find records with parse_invalid=1 or NULL establishment_name
    cursor.execute("""
        SELECT activity_nr, source_url 
        FROM inspections 
        WHERE parse_invalid = 1 
           OR establishment_name IS NULL 
           OR establishment_name LIKE '%%.015'
           OR establishment_name LIKE '%%.%'
        LIMIT ?
    """, (max_details,))
    
    invalid_records = cursor.fetchall()
    stats["invalid_found"] = len(invalid_records)
    
    if not invalid_records:
        logger.info("No invalid records found to refresh")
        conn.close()
        return stats
    
    logger.info(f"Found {len(invalid_records)} invalid records to refresh")
    
    session = get_session()
    
    for i, (activity_nr, source_url) in enumerate(invalid_records):
        if not source_url:
            # Construct URL from activity_nr
            source_url = f"https://www.osha.gov/ords/imis/establishment.inspection_detail?id={activity_nr}.015"
        
        logger.info(f"Refreshing {i+1}/{len(invalid_records)}: {activity_nr}")
        
        try:
            response = fetch_with_retry(session, source_url)
            if response:
                stats["refreshed"] += 1
                
                # Re-parse
                detail_data = parse_inspection_detail(response.text, source_url)
                detail_data["activity_nr"] = activity_nr
                
                # Upsert (will update existing)
                is_new, is_updated = upsert_inspection(conn, detail_data)
                
                # Check if now valid
                if validate_establishment_name(detail_data.get("establishment_name")):
                    stats["fixed"] += 1
                    logger.info(f"Fixed: {activity_nr} -> {detail_data.get('establishment_name')}")
                else:
                    stats["still_invalid"] += 1
                    logger.warning(f"Still invalid: {activity_nr}")
                
                if (i + 1) % 10 == 0:
                    conn.commit()
                    logger.info(f"Progress: {i+1}/{len(invalid_records)} refreshed")
                    
        except Exception as e:
            logger.error(f"Error refreshing {activity_nr}: {e}")
            stats["errors"] += 1
    
    conn.commit()
    conn.close()
    
    return stats


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Ingest OSHA inspection data into SQLite database"
    )
    parser.add_argument(
        "--db",
        required=True,
        help="Path to SQLite database file"
    )
    parser.add_argument(
        "--since-days",
        type=int,
        default=2,
        help="Look back this many days (default: 2)"
    )
    parser.add_argument(
        "--states",
        default="VA,MD,DC",
        help="Comma-separated state codes (default: VA,MD,DC)"
    )
    parser.add_argument(
        "--max-details",
        type=int,
        default=500,
        help="Max detail pages to fetch (default: 500)"
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: INFO)"
    )
    parser.add_argument(
        "--refresh-invalid",
        action="store_true",
        help="Re-fetch and re-parse records with invalid establishment_name"
    )
    
    args = parser.parse_args()
    
    setup_logging(args.log_level)
    
    # Handle refresh-invalid mode
    if args.refresh_invalid:
        logger.info(f"Starting refresh of invalid records: db={args.db}")
        
        try:
            stats = refresh_invalid_records(
                db_path=args.db,
                max_details=args.max_details,
            )
            
            logger.info(f"Refresh complete: {stats}")
            print(f"\nRefresh Summary:")
            print(f"  Invalid found:    {stats['invalid_found']}")
            print(f"  Refreshed:        {stats['refreshed']}")
            print(f"  Fixed:            {stats['fixed']}")
            print(f"  Still invalid:    {stats['still_invalid']}")
            print(f"  Errors:           {stats['errors']}")
            
        except Exception as e:
            logger.error(f"Refresh failed: {e}")
            sys.exit(1)
        
        return
    
    # Normal ingestion mode
    states = [s.strip().upper() for s in args.states.split(",")]
    
    logger.info(f"Starting OSHA ingestion: db={args.db}, since_days={args.since_days}, states={states}")
    
    try:
        stats = run_ingestion(
            db_path=args.db,
            since_days=args.since_days,
            states=states,
            max_details=args.max_details,
        )
        
        logger.info(f"Ingestion complete: {stats}")
        print(f"\nIngestion Summary:")
        print(f"  Results found:   {stats['results_found']}")
        print(f"  Details fetched: {stats['details_fetched']}")
        print(f"  Rows inserted:   {stats['rows_inserted']}")
        print(f"  Rows updated:    {stats['rows_updated']}")
        print(f"  Errors:          {stats['errors_count']}")
        
    except Exception as e:
        logger.error(f"Ingestion failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
