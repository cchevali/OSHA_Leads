#!/usr/bin/env python3
"""Set up Wally's TX_TRIANGLE_V1 trial subscriber and customer config."""

import argparse
import json
import os
import sqlite3
from datetime import date, timedelta
from pathlib import Path

from lead_filters import merge_territory_definition

TERRITORY_CODE = "TX_TRIANGLE_V1"

TERRITORY_DEF = {
    "description": "Texas Triangle OSHA area offices: Austin, Dallas/Fort Worth, Houston, San Antonio",
    "states": ["TX"],
    "office_patterns": [
        r"\baustin\b",
        r"\bdallas\b",
        r"\bfort[\s-]*worth\b",
        r"\bdallas[\s/-]*fort[\s-]*worth\b",
        r"\bhouston\b",
        r"\bsan[\s-]*antonio\b",
    ],
    "fallback_city_patterns": [
        r"\baustin\b",
        r"\bdallas\b",
        r"\bfort[\s-]*worth\b",
        r"\bhouston\b",
        r"\bpasadena\b",
        r"\bpearland\b",
        r"\bsugar[\s-]*land\b",
        r"\bthe[\s-]*woodlands\b",
        r"\bkaty\b",
        r"\bbaytown\b",
        r"\bsan[\s-]*antonio\b",
    ],
}


def ensure_schema(db_path: str, schema_path: str) -> None:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)

    # If this is an existing DB, apply additive migrations *before* running schema.sql,
    # because schema.sql may create indexes that reference new columns.
    tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}

    if "inspections" in tables:
        cols = {row[1] for row in conn.execute("PRAGMA table_info(inspections)")}
        if "area_office" not in cols:
            conn.execute("ALTER TABLE inspections ADD COLUMN area_office TEXT")
        if "record_hash" not in cols:
            conn.execute("ALTER TABLE inspections ADD COLUMN record_hash TEXT")
        if "changed_at" not in cols:
            conn.execute("ALTER TABLE inspections ADD COLUMN changed_at DATETIME")

    if "subscribers" in tables:
        subscriber_cols = {row[1] for row in conn.execute("PRAGMA table_info(subscribers)")}
        if "include_low_fallback" not in subscriber_cols:
            conn.execute("ALTER TABLE subscribers ADD COLUMN include_low_fallback INTEGER NOT NULL DEFAULT 0")
        if "recipients_json" not in subscriber_cols:
            conn.execute("ALTER TABLE subscribers ADD COLUMN recipients_json TEXT")
        if "last_sent_at" not in subscriber_cols:
            conn.execute("ALTER TABLE subscribers ADD COLUMN last_sent_at DATETIME")
        if "send_enabled" not in subscriber_cols:
            conn.execute("ALTER TABLE subscribers ADD COLUMN send_enabled INTEGER NOT NULL DEFAULT 0")

    with open(schema_path, "r", encoding="utf-8") as f:
        conn.executescript(f.read())

    conn.commit()
    conn.close()


def upsert_territory(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        INSERT INTO territories
            (territory_code, description, states_json, office_patterns_json, fallback_city_patterns_json, active)
        VALUES (?, ?, ?, ?, ?, 1)
        ON CONFLICT(territory_code) DO UPDATE SET
            description=excluded.description,
            states_json=excluded.states_json,
            office_patterns_json=excluded.office_patterns_json,
            fallback_city_patterns_json=excluded.fallback_city_patterns_json,
            active=1
        """,
        (
            TERRITORY_CODE,
            TERRITORY_DEF["description"],
            json.dumps(TERRITORY_DEF["states"]),
            json.dumps(TERRITORY_DEF["office_patterns"]),
            json.dumps(TERRITORY_DEF["fallback_city_patterns"]),
        ),
    )


def upsert_subscriber(
    conn: sqlite3.Connection,
    recipients: list[str],
    customer_id: str,
) -> None:
    start_date = date.today()
    end_date = start_date + timedelta(days=14)
    primary_email = recipients[0].lower()

    conn.execute(
        """
        INSERT INTO subscribers
            (subscriber_key, display_name, email, recipients_json, territory_code, content_filter, include_low_fallback,
             trial_length_days, trial_started_at, trial_ends_at, active, send_enabled,
             send_time_local, timezone, customer_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 0, ?, ?, ?)
        ON CONFLICT(subscriber_key) DO UPDATE SET
            display_name=excluded.display_name,
            email=excluded.email,
            recipients_json=excluded.recipients_json,
            territory_code=excluded.territory_code,
            content_filter=excluded.content_filter,
            include_low_fallback=excluded.include_low_fallback,
            trial_length_days=excluded.trial_length_days,
            trial_started_at=excluded.trial_started_at,
            trial_ends_at=excluded.trial_ends_at,
            active=1,
            send_time_local=excluded.send_time_local,
            timezone=excluded.timezone,
            customer_id=excluded.customer_id
        """,
        (
            "wally_trial",
            "Wally",
            primary_email,
            json.dumps([email.lower() for email in recipients]),
            TERRITORY_CODE,
            "high_medium",
            1,
            14,
            start_date.isoformat(),
            end_date.isoformat(),
            "08:00",
            "America/Chicago",
            customer_id,
        ),
    )


def write_customer_config(
    path: str,
    customer_id: str,
    recipients: list[str],
    chase_email: str,
    brand_name: str,
    mailing_address: str,
) -> None:
    config = {
        "customer_id": customer_id,
        "subscriber_key": "wally_trial",
        "subscriber_name": "Wally",
        "trial_length_days": 14,
        "active": True,
        "territory_code": TERRITORY_CODE,
        "send_time_local": "08:00",
        "timezone": "America/Chicago",
        "content_filter": "high_medium",
        "include_low_fallback": True,
        "states": ["TX"],
        "opened_window_days": 14,
        "new_only_days": 1,
        "top_k_overall": 30,
        "top_k_per_state": 30,
        "recipients": [email.lower() for email in recipients],
        "email_recipients": [email.lower() for email in recipients],
        "pilot_mode": True,
        "pilot_whitelist": [chase_email.lower()] + [email.lower() for email in recipients],
        "brand_name": brand_name,
        "mailing_address": mailing_address,
    }

    config_path = Path(path)
    config_path.parent.mkdir(parents=True, exist_ok=True)
    with open(config_path, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=2)
        f.write("\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Setup Wally trial subscriber and territory")
    parser.add_argument("--db", default="data/osha.sqlite", help="SQLite database path")
    parser.add_argument("--schema", default="schema.sql", help="Schema SQL path")
    parser.add_argument("--customer-id", default="wally_trial_tx_triangle_v1")
    parser.add_argument("--customer-config", default="customers/wally_trial_tx_triangle_v1.json")
    parser.add_argument("--wally-email", default=os.getenv("WALLY_EMAIL", "wgs@indigocompliance.com"))
    parser.add_argument(
        "--extra-recipient",
        default=os.getenv("WALLY_EXTRA_EMAIL", "brandon@indigoenergyservices.com"),
        help="Additional recipient for fanout delivery",
    )
    parser.add_argument("--chase-email", default=os.getenv("CHASE_EMAIL", "cchevali+oshasmoke@gmail.com"))
    parser.add_argument("--brand-name", default=os.getenv("BRAND_NAME", "MicroFlowOps"))
    parser.add_argument(
        "--mailing-address",
        default=os.getenv("MAILING_ADDRESS", "11539 Links Dr, Reston, VA 20190"),
    )

    args = parser.parse_args()

    ensure_schema(args.db, args.schema)
    merge_territory_definition(TERRITORY_CODE, TERRITORY_DEF)
    recipients = [args.wally_email, args.extra_recipient]

    conn = sqlite3.connect(args.db)
    upsert_territory(conn)
    upsert_subscriber(conn, recipients, args.customer_id)
    conn.commit()
    conn.close()

    write_customer_config(
        path=args.customer_config,
        customer_id=args.customer_id,
        recipients=recipients,
        chase_email=args.chase_email,
        brand_name=args.brand_name,
        mailing_address=args.mailing_address,
    )

    print("Wally trial setup complete")
    print(f"  DB: {args.db}")
    print(f"  Recipients: {', '.join(recipients)}")
    print(f"  Territory: {TERRITORY_CODE}")
    print(f"  Customer config: {args.customer_config}")


if __name__ == "__main__":
    main()
