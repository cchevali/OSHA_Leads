#!/usr/bin/env python3
"""
Seed TX recipients for OSHA defense/safety services outreach.

Outputs:
  out/recipients.csv (business-contact schema)
"""

import csv
from pathlib import Path

import outbound_cold_email as oce


OUT_DIR = Path(__file__).parent / "out"
OUT_PATH = OUT_DIR / "recipients.csv"


FREE_EMAIL_DOMAINS = {
    "gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "live.com", "msn.com",
    "aol.com", "icloud.com", "me.com", "proton.me", "protonmail.com",
    "comcast.net", "att.net", "verizon.net", "sbcglobal.net",
}


SEEDS = [
    # From Target List Factory status (TX safety consultants)
    {"company_name": "Jan Koehn M.S. CIH Inc", "domain": "jkinc.biz", "contact_email": "mail@jkinc.biz", "contact_role": "Owner/Executive", "city": "Houston", "state": "TX"},
    {"company_name": "Spear & Lancaster LLC", "domain": "jespear.com", "contact_email": "jerome.spear@jespear.com", "contact_role": "Owner/Executive", "city": "Spring", "state": "TX"},
    {"company_name": "Atlas Technical Consultants", "domain": "oneatlas.com", "contact_email": "alex.peck@oneatlas.com", "contact_role": "Consultant", "city": "Houston", "state": "TX"},
    {"company_name": "Clean Environments Inc", "domain": "cleanenvironments.com", "contact_email": "gregs@cleanenvironments.com", "contact_role": "Consultant", "city": "San Antonio", "state": "TX"},
    {"company_name": "EnviROSH Services Inc", "domain": "envirosh.com", "contact_email": "lloyd.andrew@envirosh.com", "contact_role": "Consultant", "city": "Houston", "state": "TX"},
    {"company_name": "Terracon Consultants Inc", "domain": "terracon.com", "contact_email": "kevin.maloney@terracon.com", "contact_role": "Consultant", "city": "Houston", "state": "TX"},
    {"company_name": "Bernardino LLC", "domain": "bernardino-oehs.com", "contact_email": "contact@bernardino-oehs.com", "contact_role": "Consultant", "city": "McAllen", "state": "TX"},
    {"company_name": "Baer Engineering", "domain": "baereng.com", "contact_email": "info@baereng.com", "contact_role": "General", "city": "Austin", "state": "TX"},
    {"company_name": "John A. Jurgiel & Associates", "domain": "jurgiel.com", "contact_email": "dromo@jurgiel.com", "contact_role": "Consultant", "city": "Houston", "state": "TX"},
    {"company_name": "CTEH", "domain": "cteh.com", "contact_email": "cledbetter@cteh.com", "contact_role": "Consultant", "city": "Houston", "state": "TX"},
    # TX safety/OSHA services
    {"company_name": "Aggie Safety", "domain": "aggiesafety.com", "contact_email": "info@aggiesafety.com", "contact_role": "General", "city": "Houston", "state": "TX"},
    {"company_name": "Houston Safety Pro", "domain": "houstonsafetypro.com", "contact_email": "support@houstonsafetypro.com", "contact_role": "Support", "city": "Houston", "state": "TX"},
    {"company_name": "Costello Safety Consulting", "domain": "costellohse.com", "contact_email": "information@costellohse.com", "contact_role": "General", "city": "Houston", "state": "TX"},
    {"company_name": "Axis Safety Consulting", "domain": "axissafetyconsulting.com", "contact_email": "partners@axissafetyconsulting.com", "contact_role": "Partners", "city": "Austin", "state": "TX"},
    {"company_name": "DFW Safety Consulting", "domain": "dfwsafetyconsulting.com", "contact_email": "joe@dfwsafetyconsulting.com", "contact_role": "Owner/Executive", "city": "Fort Worth", "state": "TX"},
    {"company_name": "Safety First Consulting", "domain": "safetyfirstconsulting.com", "contact_email": "help@safetyfirstconsulting.com", "contact_role": "Support", "city": "Georgetown", "state": "TX"},
    {"company_name": "Indigo Compliance", "domain": "indigocompliance.com", "contact_email": "info@indigocompliance.com", "contact_role": "General", "city": "Dallas", "state": "TX"},
    {"company_name": "Texas Steel Fabrication Safety Consulting", "domain": "txsteelfabsafetyconsulting.com", "contact_email": "info@txsteelfabsafetyconsulting.com", "contact_role": "General", "city": "Austin", "state": "TX"},
    {"company_name": "Advanced Safety Consulting", "domain": "ascsafetytx.com", "contact_email": "info@ascsafetytx.com", "contact_role": "General", "city": "San Antonio", "state": "TX"},
    {"company_name": "JSA Safety & Consulting", "domain": "jsa-safety.com", "contact_email": "info@jsa-safety.com", "contact_role": "General", "city": "Poteet", "state": "TX"},
    {"company_name": "OccuPros", "domain": "occupros.com", "contact_email": "info@occupros.com", "contact_role": "General", "city": "Arlington", "state": "TX"},
    {"company_name": "The Compliance Edge", "domain": "thecomplianceedge.com", "contact_email": "sales@thecomplianceedge.com", "contact_role": "Sales", "city": "Sugar Land", "state": "TX"},
    {"company_name": "MHK Safety", "domain": "mhksafety.com", "contact_email": "cs@mhksafety.com", "contact_role": "Support", "city": "Houston", "state": "TX"},
    {"company_name": "CORE Safety Group", "domain": "coresafety.com", "contact_email": "info@coresafety.com", "contact_role": "General", "city": "Irving", "state": "TX"},
    {"company_name": "Safety Consultants USA", "domain": "safetyconsultantsusa.com", "contact_email": "contact@safetyconsultantsusa.com", "contact_role": "General", "city": "Dallas", "state": "TX"},
    {"company_name": "Summit HSIH", "domain": "summithsih.com", "contact_email": "service@summithsih.com", "contact_role": "Support", "city": "San Antonio", "state": "TX"},
    {"company_name": "TGE Resources", "domain": "tgeresources.com", "contact_email": "rdfranks@tgeresources.com", "contact_role": "Consultant", "city": "Houston", "state": "TX"},
    {"company_name": "Houston Integrity Consultants (PROtect)", "domain": "protect.llc", "contact_email": "info@protect.llc", "contact_role": "General", "city": "Houston", "state": "TX"},
    {"company_name": "Environmental IQ", "domain": "enviqtx.com", "contact_email": "nasser@enviqtx.com", "contact_role": "Consultant", "city": "Austin", "state": "TX"},
    {"company_name": "OSHA-PRO", "domain": "oshapro.us", "contact_email": "support@oshapro.us", "contact_role": "Support", "city": "Houston", "state": "TX"},
    {"company_name": "Construction Safety Consultants LLC", "domain": "safetyconsultantsllc.com", "contact_email": "scsc@safetyconsultantsllc.com", "contact_role": "General", "city": "Round Rock", "state": "TX"},
    {"company_name": "Safety Council of East Texas", "domain": "etsafety.org", "contact_email": "chris@etsafety.org", "contact_role": "General", "city": "Longview", "state": "TX"},
    {"company_name": "Onsite Safety", "domain": "onsitesafety.com", "contact_email": "support@onsitesafety.com", "contact_role": "Support", "city": "Dallas", "state": "TX"},
    {"company_name": "Greenberg Safety", "domain": "greenbergsafety.com", "contact_email": "vitaliy@greenbergsafety.com", "contact_role": "Owner/Executive", "city": "Austin", "state": "TX"},
    {"company_name": "Safety Alliance", "domain": "safetyalliance.org", "contact_email": "info@safetyalliance.org", "contact_role": "General", "city": "Houston", "state": "TX"},
]


def is_free_domain(email: str) -> bool:
    domain = email.split("@")[-1].lower().strip()
    return domain in FREE_EMAIL_DOMAINS


def main() -> None:
    OUT_DIR.mkdir(exist_ok=True)

    suppressed = oce.load_suppression_list()
    already_sent = oce.get_already_sent_all_time()

    seeded_count = len(SEEDS)
    suppressed_filtered = 0
    prior_filtered = 0
    free_filtered = 0

    filtered = []
    for row in SEEDS:
        email = (row.get("contact_email") or "").strip().lower()
        if not email or "@" not in email:
            continue
        if email in suppressed:
            suppressed_filtered += 1
            continue
        if email in already_sent:
            prior_filtered += 1
            continue
        if is_free_domain(email):
            free_filtered += 1
            continue
        filtered.append(row)

    # Write recipients.csv (business-contact schema)
    fieldnames = ["company_name", "domain", "contact_email", "contact_role", "city", "state"]
    with open(OUT_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(filtered)

    print(f"[OK] recipients.csv written: {OUT_PATH}")
    print("COUNTS")
    print(f"  seeded: {seeded_count}")
    print(f"  suppressed-filtered: {suppressed_filtered}")
    print(f"  prior-send-filtered: {prior_filtered}")
    print(f"  free-email-filtered: {free_filtered}")
    print(f"  sendable: {len(filtered)}")


if __name__ == "__main__":
    main()
