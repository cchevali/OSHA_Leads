#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from pathlib import Path
from typing import Any

import crm_light
from lead_filters import filter_by_cbsa_allowlist


def _read_json_payload(args: argparse.Namespace) -> dict[str, Any]:
    if args.stdin_json:
        raw = sys.stdin.read()
    elif args.payload_file:
        raw = Path(args.payload_file).read_text(encoding="utf-8")
    else:
        raw = str(args.payload_json or "")
    if not raw.strip():
        raise ValueError("ERR_PAYLOAD_REQUIRED")
    payload = json.loads(raw)
    if not isinstance(payload, dict):
        raise ValueError("ERR_PAYLOAD_OBJECT_REQUIRED")
    return payload


def _print_json(payload: dict[str, Any]) -> None:
    print(json.dumps(payload, sort_keys=True))


def _print_config(subscriber_db: Path, command: str) -> None:
    price_map = crm_light.resolve_stripe_price_map_from_env()
    payload = {
        "command": command,
        "crm_db": str(subscriber_db),
        "schema_version_target": crm_light.CRM_SCHEMA_VERSION,
        "plan_caps": {
            "pilot": crm_light.PLAN_MAX_METROS.get("pilot"),
            "core": crm_light.PLAN_MAX_METROS.get("core"),
            "multi": crm_light.PLAN_MAX_METROS.get("multi"),
        },
        "stripe_price_id_mapping": price_map,
        "stripe_price_id_core_present": bool(str(os.getenv("STRIPE_PRICE_ID_CORE", "")).strip()),
        "stripe_price_id_multi_present": bool(str(os.getenv("STRIPE_PRICE_ID_MULTI", "")).strip()),
        "stripe_price_id_pilot_present": bool(str(os.getenv("STRIPE_PRICE_ID_PILOT", "")).strip()),
        "web_stripe_webhook_secret_present": bool(str(os.getenv("WEB_STRIPE_WEBHOOK_SECRET", "")).strip()),
        "stripe_webhook_secret_present": bool(str(os.getenv("STRIPE_WEBHOOK_SECRET", "")).strip()),
    }
    _print_json(payload)


def _load_inspection(db_path: str, inspection_value: str) -> dict[str, Any] | None:
    target = str(inspection_value or "").strip()
    if not target:
        return None
    base = target.split(".", 1)[0]
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(
            """
            SELECT
                activity_nr,
                lead_key,
                source_url,
                site_city,
                site_state,
                site_zip,
                mail_zip,
                area_office
            FROM inspections
            WHERE activity_nr = ?
               OR activity_nr = ?
               OR source_url LIKE ?
               OR source_url LIKE ?
            ORDER BY activity_nr ASC
            LIMIT 1
            """,
            (target, base, f"%id={target}%", f"%id={base}%"),
        ).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def _extract_inspection_nr(lead: dict[str, Any], fallback: str) -> str:
    source_url = str(lead.get("source_url") or "").strip()
    if "id=" in source_url:
        return source_url.split("id=", 1)[-1].split("&", 1)[0].strip() or fallback
    activity_nr = str(lead.get("activity_nr") or "").strip()
    if activity_nr:
        return activity_nr
    return fallback


def cmd_onboarding_submit(args: argparse.Namespace) -> int:
    crm_db = crm_light.ensure_database(args.crm_db or None)
    if args.print_config:
        _print_config(crm_db, "onboarding-submit")
        return 0

    payload = _read_json_payload(args)
    cbsa_codes_raw = payload.get("cbsa_codes")
    cbsa_codes = cbsa_codes_raw if isinstance(cbsa_codes_raw, list) else []
    with crm_light.open_conn(crm_db) as conn:
        crm_light.init_schema(conn)
        result = crm_light.upsert_subscriber_onboarding(
            conn,
            subscriber_key=str(payload.get("subscriber_key") or ""),
            email=str(payload.get("email") or ""),
            plan_code=str(payload.get("plan_code") or ""),
            cbsa_codes=[str(item or "") for item in cbsa_codes],
            source=str(payload.get("source") or "web_onboarding"),
            dry_run=bool(args.dry_run),
        )
    _print_json(result)
    if result.get("ok"):
        return 0
    return 2


def cmd_stripe_ingest(args: argparse.Namespace) -> int:
    crm_db = crm_light.ensure_database(args.crm_db or None)
    if args.print_config:
        _print_config(crm_db, "stripe-ingest")
        return 0

    payload = _read_json_payload(args)
    with crm_light.open_conn(crm_db) as conn:
        crm_light.init_schema(conn)
        result = crm_light.ingest_stripe_subscription_event(
            conn,
            payload,
            dry_run=bool(args.dry_run),
        )

    token = str(result.get("token") or "")
    event_id = str(result.get("event_id") or "")
    plan_code = str(result.get("plan_code") or "")
    print(
        f"STRIPE_INGEST token={token} event_id={event_id} plan_code={plan_code} dry_run={'YES' if args.dry_run else 'NO'}",
        file=sys.stderr,
    )
    _print_json(result)
    if result.get("ok"):
        return 0
    return 2


def cmd_audit_match(args: argparse.Namespace) -> int:
    crm_db = crm_light.ensure_database(args.crm_db or None)
    if args.print_config:
        _print_config(crm_db, "audit-match")
        return 0

    subscriber_key = crm_light.normalize_subscriber_key(str(args.subscriber_key or ""))
    email = crm_light.normalize_email(str(args.email or ""))
    inspection = str(args.inspection or "").strip()
    with crm_light.open_conn(crm_db) as conn:
        crm_light.init_schema(conn)
        entitlement = crm_light.get_subscriber_entitlement(
            conn,
            subscriber_key=subscriber_key,
            email=email,
            active_only=False,
        )
        resolved_key = (
            subscriber_key
            or crm_light.normalize_subscriber_key(str((entitlement or {}).get("subscriber_key") or ""))
            or crm_light.derive_subscriber_key_from_email(email)
        )
        allowlist = crm_light.get_subscriber_cbsa_allowlist(conn, resolved_key)

    if not allowlist:
        payload = {
            "ok": False,
            "err_code": "ERR_SUBSCRIBER_CBSA_ALLOWLIST_EMPTY",
            "subscriber_key": resolved_key,
            "inspection": inspection,
        }
        _print_json(payload)
        return 2

    lead = _load_inspection(args.db, inspection)
    if not lead:
        payload = {
            "ok": True,
            "present_in_data": False,
            "matched": False,
            "inspection": inspection,
            "inspection_nr": inspection,
            "reason_token": "INSPECTION_NOT_FOUND",
            "resolved_cbsa": "",
            "subscriber_cbsa_set": allowlist,
            "subscriber_key": resolved_key,
        }
        _print_json(payload)
        return 0

    filtered, _stats, debug_rows = filter_by_cbsa_allowlist([lead], allowlist, include_debug=True)
    debug = debug_rows[0] if debug_rows else {}
    payload = {
        "ok": True,
        "present_in_data": True,
        "matched": len(filtered) == 1,
        "inspection": inspection,
        "inspection_nr": _extract_inspection_nr(lead, inspection),
        "reason_token": str(debug.get("match_reason") or ("CBSA_MATCH" if filtered else "CBSA_MISMATCH")),
        "resolved_cbsa": str(debug.get("resolved_cbsa") or ""),
        "subscriber_cbsa_set": allowlist,
        "subscriber_key": resolved_key,
        "plan_code": str((entitlement or {}).get("plan_code") or ""),
        "max_metros": int((entitlement or {}).get("max_metros") or 0),
    }
    _print_json(payload)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Subscription registry operations for onboarding, Stripe ingestion, and metro audit."
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    onboarding = sub.add_parser("onboarding-submit")
    onboarding.add_argument("--payload-json", default="")
    onboarding.add_argument("--payload-file", default="")
    onboarding.add_argument("--stdin-json", action="store_true")
    onboarding.add_argument("--crm-db", default="")
    onboarding.add_argument("--print-config", action="store_true")
    onboarding.add_argument("--dry-run", action="store_true")

    stripe = sub.add_parser("stripe-ingest")
    stripe.add_argument("--payload-json", default="")
    stripe.add_argument("--payload-file", default="")
    stripe.add_argument("--stdin-json", action="store_true")
    stripe.add_argument("--crm-db", default="")
    stripe.add_argument("--print-config", action="store_true")
    stripe.add_argument("--dry-run", action="store_true")

    audit = sub.add_parser("audit-match")
    audit.add_argument("--inspection", required=True)
    audit.add_argument("--subscriber-key", default="")
    audit.add_argument("--email", default="")
    audit.add_argument("--db", default="data/osha.sqlite")
    audit.add_argument("--crm-db", default="")
    audit.add_argument("--print-config", action="store_true")
    audit.add_argument("--dry-run", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        if args.cmd == "onboarding-submit":
            return cmd_onboarding_submit(args)
        if args.cmd == "stripe-ingest":
            return cmd_stripe_ingest(args)
        if args.cmd == "audit-match":
            return cmd_audit_match(args)
        raise RuntimeError(f"ERR_UNKNOWN_COMMAND {args.cmd}")
    except Exception as exc:
        payload = {"ok": False, "err_code": "ERR_SUBSCRIPTION_REGISTRY_OP", "detail": str(exc)}
        _print_json(payload)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
