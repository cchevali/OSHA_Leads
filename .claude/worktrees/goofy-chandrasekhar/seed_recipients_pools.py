#!/usr/bin/env python3
"""
Seed territory recipient pools (TX/CA/FL) with business-domain contacts
and apply hygiene filters:
- suppression list
- prior sent finals
- free-email domain exclusion
"""

import csv
from pathlib import Path

import outbound_cold_email as oce


OUT_DIR = Path(__file__).parent / "out"
TX_PATH = OUT_DIR / "recipients_tx.csv"
CA_PATH = OUT_DIR / "recipients_ca.csv"
FL_PATH = OUT_DIR / "recipients_fl.csv"
DEFAULT_PATH = OUT_DIR / "recipients.csv"

FREE_EMAIL_DOMAINS = {
    "gmail.com",
    "yahoo.com",
    "hotmail.com",
    "outlook.com",
    "live.com",
    "msn.com",
    "aol.com",
    "icloud.com",
    "me.com",
    "proton.me",
    "protonmail.com",
    "comcast.net",
    "att.net",
    "verizon.net",
    "sbcglobal.net",
}


TX_POOL = [
    {"company_name": "North Texas Safety Consulting Services", "domain": "ntscs.com", "contact_email": "scott@ntscs.com", "contact_role": "Owner/Executive", "city": "Archer City", "state": "TX"},
    {"company_name": "JSA Safety & Consulting", "domain": "jsa-safety.com", "contact_email": "stefanie@jsa-safety.com", "contact_role": "Owner/Executive", "city": "Poteet", "state": "TX"},
    {"company_name": "JSA Safety & Consulting", "domain": "jsa-safety.com", "contact_email": "amy@jsa-safety.com", "contact_role": "CFO", "city": "Poteet", "state": "TX"},
    {"company_name": "OSHA Pros USA", "domain": "osha.net", "contact_email": "info@osha-pros.com", "contact_role": "Support", "city": "North Richland Hills", "state": "TX"},
    {"company_name": "Scott & Associates Safety Consulting", "domain": "oshasafetypro.com", "contact_email": "riskcsp@att.net", "contact_role": "Owner/Executive", "city": "Houston", "state": "TX"},
    {"company_name": "Texas Safety Doc Consulting", "domain": "txsafetydoc.com", "contact_email": "info@txsafetydoc.com", "contact_role": "General", "city": "San Antonio", "state": "TX"},
    {"company_name": "Tex-Safe Consulting", "domain": "texsafe.net", "contact_email": "info@texsafe.net", "contact_role": "General", "city": "Fort Worth", "state": "TX"},
    {"company_name": "Houston Safety Experts", "domain": "houstonsafetyexperts.com", "contact_email": "info@houstonsafetyexperts.com", "contact_role": "General", "city": "Houston", "state": "TX"},
    {"company_name": "Safe T Professionals", "domain": "safetprofessionals.com", "contact_email": "info@safetprofessionals.com", "contact_role": "General", "city": "Houston", "state": "TX"},
    {"company_name": "Texas Safety Consulting Group", "domain": "txsafetyconsulting.com", "contact_email": "info@txsafetyconsulting.com", "contact_role": "General", "city": "Dallas", "state": "TX"},
    {"company_name": "Evergreen Safety Council", "domain": "evergreensafetycouncil.org", "contact_email": "info@evergreensafetycouncil.org", "contact_role": "General", "city": "Houston", "state": "TX"},
    {"company_name": "AIO Safety Consulting", "domain": "aiosafety.com", "contact_email": "info@aiosafety.com", "contact_role": "General", "city": "Houston", "state": "TX"},
    {"company_name": "OSHA Texas Training Institute", "domain": "oshatexas.com", "contact_email": "info@oshatexas.com", "contact_role": "Support", "city": "Austin", "state": "TX"},
    {"company_name": "KPA Safety Consulting", "domain": "kpa.io", "contact_email": "sales@kpa.io", "contact_role": "Sales", "city": "Houston", "state": "TX"},
    {"company_name": "Safety by Design Consulting", "domain": "safetybydesignconsulting.com", "contact_email": "info@safetybydesignconsulting.com", "contact_role": "General", "city": "Plano", "state": "TX"},
    {"company_name": "Texas Construction Safety Group", "domain": "txconstructionsafety.com", "contact_email": "info@txconstructionsafety.com", "contact_role": "General", "city": "Houston", "state": "TX"},
    {"company_name": "Alliance Safety Council Texas", "domain": "alliancesafetycouncil.org", "contact_email": "training@alliancesafetycouncil.org", "contact_role": "Training", "city": "Houston", "state": "TX"},
    {"company_name": "Texas Compliance Consulting", "domain": "txcomplianceconsulting.com", "contact_email": "contact@txcomplianceconsulting.com", "contact_role": "General", "city": "San Antonio", "state": "TX"},
    {"company_name": "Compliance Resource Group", "domain": "complianceresourcegroup.com", "contact_email": "info@complianceresourcegroup.com", "contact_role": "General", "city": "Dallas", "state": "TX"},
    {"company_name": "Texas OSHA Defense Counsel", "domain": "texasoshadefense.com", "contact_email": "intake@texasoshadefense.com", "contact_role": "Intake", "city": "Houston", "state": "TX"},
    {"company_name": "The Carlson Law Firm", "domain": "carlsonattorneys.com", "contact_email": "info@carlsonattorneys.com", "contact_role": "Intake", "city": "Killeen", "state": "TX"},
    {"company_name": "The Law Office of Glenn S. Goza", "domain": "glenngoza.com", "contact_email": "info@glenngoza.com", "contact_role": "Intake", "city": "Houston", "state": "TX"},
    {"company_name": "Herrman & Herrman", "domain": "herrmanandherrman.com", "contact_email": "intake@herrmanandherrman.com", "contact_role": "Intake", "city": "Houston", "state": "TX"},
    {"company_name": "Mullen & Mullen", "domain": "mullenandmullen.com", "contact_email": "info@mullenandmullen.com", "contact_role": "Intake", "city": "Dallas", "state": "TX"},
    {"company_name": "Oberheiden P.C.", "domain": "federal-lawyer.com", "contact_email": "intake@federal-lawyer.com", "contact_role": "Intake", "city": "Dallas", "state": "TX"},
    {"company_name": "Pappas Grubbs Price", "domain": "pappasgrubbs.com", "contact_email": "info@pappasgrubbs.com", "contact_role": "General", "city": "Houston", "state": "TX"},
    {"company_name": "Sorrels Law", "domain": "sorrelslaw.com", "contact_email": "info@sorrelslaw.com", "contact_role": "Intake", "city": "Houston", "state": "TX"},
    {"company_name": "The Krist Law Firm", "domain": "krist.com", "contact_email": "intake@krist.com", "contact_role": "Intake", "city": "Houston", "state": "TX"},
    {"company_name": "Simmons and Fletcher", "domain": "simmonsandfletcher.com", "contact_email": "info@simmonsandfletcher.com", "contact_role": "Intake", "city": "Houston", "state": "TX"},
    {"company_name": "Fleming Law", "domain": "flemingattorneys.com", "contact_email": "info@flemingattorneys.com", "contact_role": "Intake", "city": "Houston", "state": "TX"},
    {"company_name": "Amini & Conant", "domain": "aminiconant.com", "contact_email": "intake@aminiconant.com", "contact_role": "Intake", "city": "Houston", "state": "TX"},
    {"company_name": "Moore & Associates", "domain": "mooreandassociates.net", "contact_email": "info@mooreandassociates.net", "contact_role": "Intake", "city": "Houston", "state": "TX"},
    {"company_name": "Orange Law", "domain": "orangelaw.us", "contact_email": "contact@orangelaw.us", "contact_role": "Intake", "city": "Houston", "state": "TX"},
    {"company_name": "Law Offices of Tavss Fletcher", "domain": "tavss.com", "contact_email": "info@tavss.com", "contact_role": "Intake", "city": "Houston", "state": "TX"},
    {"company_name": "Texas Trial Lawyers Association", "domain": "ttla.com", "contact_email": "info@ttla.com", "contact_role": "General", "city": "Austin", "state": "TX"},
    {"company_name": "Adam Henderson Law", "domain": "713lawdawg.com", "contact_email": "info@713lawdawg.com", "contact_role": "Intake", "city": "Houston", "state": "TX"},
]

CA_POOL = [
    {"company_name": "California Safety Training Corporation", "domain": "californiasafetytrainingcorp.com", "contact_email": "info@californiasafetytrainingcorp.com", "contact_role": "General", "city": "Los Angeles", "state": "CA"},
    {"company_name": "Cal/OSHA Safety Consulting", "domain": "caloshasafety.com", "contact_email": "info@caloshasafety.com", "contact_role": "General", "city": "San Diego", "state": "CA"},
    {"company_name": "Safety Center Inc.", "domain": "getsafetytrained.com", "contact_email": "info@getsafetytrained.com", "contact_role": "Training", "city": "Sacramento", "state": "CA"},
    {"company_name": "HASC", "domain": "hasc.com", "contact_email": "info@hasc.com", "contact_role": "Training", "city": "Torrance", "state": "CA"},
    {"company_name": "ClickSafety", "domain": "clicksafety.com", "contact_email": "sales@clicksafety.com", "contact_role": "Sales", "city": "Walnut Creek", "state": "CA"},
    {"company_name": "National Safety Council - Southern California", "domain": "nsc.org", "contact_email": "info@nsc.org", "contact_role": "General", "city": "Los Angeles", "state": "CA"},
    {"company_name": "Alliance Safety Council", "domain": "alliancesafetycouncil.org", "contact_email": "training@alliancesafetycouncil.org", "contact_role": "Training", "city": "Long Beach", "state": "CA"},
    {"company_name": "Cal Safety Compliance", "domain": "calsafetycompliance.com", "contact_email": "info@calsafetycompliance.com", "contact_role": "General", "city": "Anaheim", "state": "CA"},
    {"company_name": "Safety Services Company", "domain": "safetyservicescompany.com", "contact_email": "info@safetyservicescompany.com", "contact_role": "General", "city": "Irvine", "state": "CA"},
    {"company_name": "KPA California", "domain": "kpa.io", "contact_email": "sales@kpa.io", "contact_role": "Sales", "city": "San Francisco", "state": "CA"},
    {"company_name": "Cal OSHA Defense Lawyers", "domain": "caloshadefense.com", "contact_email": "intake@caloshadefense.com", "contact_role": "Intake", "city": "Los Angeles", "state": "CA"},
    {"company_name": "Ogletree Deakins California", "domain": "ogletree.com", "contact_email": "info@ogletree.com", "contact_role": "General", "city": "San Francisco", "state": "CA"},
    {"company_name": "Fisher Phillips California", "domain": "fisherphillips.com", "contact_email": "info@fisherphillips.com", "contact_role": "General", "city": "Los Angeles", "state": "CA"},
    {"company_name": "Jackson Lewis California", "domain": "jacksonlewis.com", "contact_email": "info@jacksonlewis.com", "contact_role": "General", "city": "Los Angeles", "state": "CA"},
    {"company_name": "Seyfarth Shaw", "domain": "seyfarth.com", "contact_email": "info@seyfarth.com", "contact_role": "General", "city": "San Francisco", "state": "CA"},
]

FL_POOL = [
    {"company_name": "Safety Alliance", "domain": "safetyalliance.org", "contact_email": "info@safetyalliance.org", "contact_role": "General", "city": "West Palm Beach", "state": "FL"},
    {"company_name": "USF SafetyFlorida", "domain": "usf.edu", "contact_email": "safetyflorida@usf.edu", "contact_role": "Program", "city": "Tampa", "state": "FL"},
    {"company_name": "Florida Safety Council", "domain": "floridasafety.org", "contact_email": "info@floridasafety.org", "contact_role": "Training", "city": "Orlando", "state": "FL"},
    {"company_name": "National Safety Council Florida", "domain": "nsc.org", "contact_email": "info@nsc.org", "contact_role": "General", "city": "Orlando", "state": "FL"},
    {"company_name": "OSHA Pros Florida", "domain": "osha-pros.com", "contact_email": "info@osha-pros.com", "contact_role": "Training", "city": "Fort Myers", "state": "FL"},
    {"company_name": "HAZWOPER Center", "domain": "hazwopercenter.com", "contact_email": "info@osha-pros.com", "contact_role": "Training", "city": "Fort Myers Beach", "state": "FL"},
    {"company_name": "Safety Services Company Florida", "domain": "safetyservicescompany.com", "contact_email": "info@safetyservicescompany.com", "contact_role": "General", "city": "Jacksonville", "state": "FL"},
    {"company_name": "Florida OSHA Defense Counsel", "domain": "floridaoshadefense.com", "contact_email": "intake@floridaoshadefense.com", "contact_role": "Intake", "city": "Miami", "state": "FL"},
    {"company_name": "Fisher Phillips Florida", "domain": "fisherphillips.com", "contact_email": "info@fisherphillips.com", "contact_role": "General", "city": "Fort Lauderdale", "state": "FL"},
    {"company_name": "Ogletree Deakins Florida", "domain": "ogletree.com", "contact_email": "info@ogletree.com", "contact_role": "General", "city": "Tampa", "state": "FL"},
    {"company_name": "Jackson Lewis Florida", "domain": "jacksonlewis.com", "contact_email": "info@jacksonlewis.com", "contact_role": "General", "city": "Miami", "state": "FL"},
    {"company_name": "Akerman LLP", "domain": "akerman.com", "contact_email": "info@akerman.com", "contact_role": "General", "city": "Miami", "state": "FL"},
    {"company_name": "Shutts & Bowen", "domain": "shutts.com", "contact_email": "info@shutts.com", "contact_role": "General", "city": "Miami", "state": "FL"},
    {"company_name": "Gunster", "domain": "gunster.com", "contact_email": "info@gunster.com", "contact_role": "General", "city": "West Palm Beach", "state": "FL"},
    {"company_name": "Cole Scott & Kissane", "domain": "csklegal.com", "contact_email": "info@csklegal.com", "contact_role": "General", "city": "Miami", "state": "FL"},
]


def is_free_email(email: str) -> bool:
    domain = email.split("@")[-1].strip().lower()
    return domain in FREE_EMAIL_DOMAINS


def dedupe_rows(rows: list[dict]) -> list[dict]:
    seen = set()
    out = []
    for row in rows:
        email = (row.get("contact_email") or "").strip().lower()
        if not email or "@" not in email:
            continue
        if email in seen:
            continue
        seen.add(email)
        out.append(row)
    return out


def apply_hygiene(rows: list[dict]) -> tuple[list[dict], dict]:
    suppression = oce.load_suppression_list()
    sent = oce.get_already_sent_all_time()
    stats = {"suppressed": 0, "prior_sent": 0, "free_domain": 0}
    clean = []
    for row in rows:
        email = (row.get("contact_email") or "").strip().lower()
        if email in suppression:
            stats["suppressed"] += 1
            continue
        if email in sent:
            stats["prior_sent"] += 1
            continue
        if is_free_email(email):
            stats["free_domain"] += 1
            continue
        clean.append(row)
    return clean, stats


def write_pool(rows: list[dict], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = ["company_name", "domain", "contact_email", "contact_role", "city", "state"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    tx_rows = dedupe_rows(TX_POOL)
    ca_rows = dedupe_rows(CA_POOL)
    fl_rows = dedupe_rows(FL_POOL)

    tx_clean, tx_stats = apply_hygiene(tx_rows)
    ca_clean, ca_stats = apply_hygiene(ca_rows)
    fl_clean, fl_stats = apply_hygiene(fl_rows)

    write_pool(tx_clean, TX_PATH)
    write_pool(ca_clean, CA_PATH)
    write_pool(fl_clean, FL_PATH)
    write_pool(tx_clean, DEFAULT_PATH)  # TX active list for workflow default

    print("[OK] Wrote pools:")
    print(f"  TX: {TX_PATH} ({len(tx_clean)} rows) stats={tx_stats}")
    print(f"  CA: {CA_PATH} ({len(ca_clean)} rows) stats={ca_stats}")
    print(f"  FL: {FL_PATH} ({len(fl_clean)} rows) stats={fl_stats}")
    print(f"  DEFAULT recipients.csv: {DEFAULT_PATH} ({len(tx_clean)} rows)")


if __name__ == "__main__":
    main()
