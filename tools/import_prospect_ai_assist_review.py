#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import sqlite3
import sys
import tempfile
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import seed_recipients_pools as pools
from outreach import crm_admin
from outreach import crm_store
from outreach import run_prospect_generation as generation
from outreach import us_state
from runtime_data_dir import resolve_data_dir

ERR_AI_ASSIST_IMPORT_CONFIG = "ERR_AI_ASSIST_IMPORT_CONFIG"
ERR_AI_ASSIST_IMPORT_INPUT = "ERR_AI_ASSIST_IMPORT_INPUT"
PASS_AI_ASSIST_IMPORT = "PASS_AI_ASSIST_IMPORT"
REQUIRED_COLUMNS = (
    "state",
    "decision",
    "firm",
    "website",
    "contact_name",
    "title",
    "email",
    "source_urls",
    "confidence",
    "evidence_snippet",
)


def _emit(key: str, value: str | int) -> None:
    print(f"{key}={value}")


def _local_today_stamp() -> str:
    return datetime.now().astimezone().date().isoformat()


def _default_batch_id() -> str:
    return f"{_local_today_stamp()}_AIASSIST"


def _normalize_email(value: str) -> str:
    return generation._normalize_email(value)


def _normalize_state(value: str) -> str:
    return us_state.normalize_us_state(value)


def _normalize_domain(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if "@" in text:
        return generation._email_domain(text)
    return generation._domain_from_website(text)


def _candidate_key(row: dict[str, str], email: str, domain: str, state: str) -> str:
    payload = "|".join(
        [
            state,
            email,
            domain,
            str(row.get("firm") or "").strip().lower(),
            str(row.get("contact_name") or "").strip().lower(),
            str(row.get("title") or "").strip().lower(),
        ]
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _prospect_id_for_email(email: str) -> str:
    return f"ai_assist_{hashlib.sha1(email.encode('utf-8')).hexdigest()[:16]}"


def _parse_source_urls(raw: str) -> list[str]:
    text = str(raw or "").strip()
    if not text:
        return []
    urls: list[str] = []
    seen: set[str] = set()
    for part in [segment.strip() for segment in text.replace("\r", "\n").split("|")]:
        if not part:
            continue
        if part in seen:
            continue
        seen.add(part)
        urls.append(part)
    return urls


def _coerce_confidence(value: str) -> int:
    text = str(value or "").strip()
    if not text:
        return 0
    try:
        parsed = int(float(text))
    except Exception:
        return 0
    return max(0, min(100, parsed))


def _load_csv_rows(input_path: Path) -> list[dict[str, str]]:
    with open(input_path, "r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise ValueError("missing_header")
        clean_fieldnames = [str(name or "").lstrip("\ufeff") for name in list(reader.fieldnames or [])]
        missing = [column for column in REQUIRED_COLUMNS if column not in clean_fieldnames]
        if missing:
            raise ValueError(f"missing_columns={','.join(missing)}")
        rows: list[dict[str, str]] = []
        for raw_row in reader:
            clean: dict[str, str] = {}
            for key, value in dict(raw_row).items():
                clean[str(key or "").lstrip("\ufeff")] = "" if value is None else str(value)
            decision = str(clean.get("decision") or "").strip().lower()
            if decision not in {"accept", "reject"}:
                raise ValueError(f"invalid_decision={decision or 'blank'}")
            rows.append(clean)
        return rows


def _load_do_not_contact_sets(conn: sqlite3.Connection | None) -> tuple[set[str], set[str]]:
    if conn is None:
        return set(), set()
    email_matches: set[str] = set()
    domain_matches: set[str] = set()
    if not crm_store.connect:
        return email_matches, domain_matches
    try:
        rows = conn.execute(
            """
            SELECT email, website
            FROM prospects
            WHERE lower(trim(COALESCE(status, ''))) = 'do_not_contact'
            """
        ).fetchall()
    except Exception:
        return email_matches, domain_matches
    for row in rows:
        email = _normalize_email(str(row[0] or ""))
        if generation._valid_email(email):
            email_matches.add(email)
            domain = generation._email_domain(email)
            if domain:
                domain_matches.add(domain)
        website_domain = _normalize_domain(str(row[1] or ""))
        if website_domain:
            domain_matches.add(website_domain)
    return email_matches, domain_matches


def _write_seed_csv(path: Path, rows: list[dict[str, str]], created_at: str) -> None:
    fieldnames = [
        "prospect_id",
        "firm",
        "contact_name",
        "email",
        "title",
        "city",
        "state",
        "website",
        "source",
        "source_fit_tier",
        "default_send_eligible",
        "email_status",
        "enrichment_lane",
        "score",
        "status",
        "created_at",
    ]
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    "prospect_id": row["prospect_id"],
                    "firm": row["firm"],
                    "contact_name": row["contact_name"],
                    "email": row["email"],
                    "title": row["title"],
                    "city": "",
                    "state": row["state"],
                    "website": row["website"],
                    "source": "ai_assist_manual",
                    "source_fit_tier": "recoverable_consultant",
                    "default_send_eligible": "1",
                    "email_status": "",
                    "enrichment_lane": "ai_assist",
                    "score": "",
                    "status": "new",
                    "created_at": created_at,
                }
            )


def _upsert_audit_rows(conn: sqlite3.Connection, audit_rows: list[dict[str, str | int]]) -> None:
    if not audit_rows:
        return
    conn.executemany(
        f"""
        INSERT INTO {crm_store.AI_ASSIST_CANDIDATE_TABLE}(
            batch_id, candidate_key, state, decision, firm, website, domain, contact_name, title, email,
            source_urls_json, confidence, evidence_snippet, verification_status, rejection_reason, prospect_id,
            created_at, updated_at
        ) VALUES (
            :batch_id, :candidate_key, :state, :decision, :firm, :website, :domain, :contact_name, :title, :email,
            :source_urls_json, :confidence, :evidence_snippet, :verification_status, :rejection_reason, :prospect_id,
            :created_at, :updated_at
        )
        ON CONFLICT(batch_id, candidate_key) DO UPDATE SET
            state = excluded.state,
            decision = excluded.decision,
            firm = excluded.firm,
            website = excluded.website,
            domain = excluded.domain,
            contact_name = excluded.contact_name,
            title = excluded.title,
            email = excluded.email,
            source_urls_json = excluded.source_urls_json,
            confidence = excluded.confidence,
            evidence_snippet = excluded.evidence_snippet,
            verification_status = excluded.verification_status,
            rejection_reason = excluded.rejection_reason,
            prospect_id = excluded.prospect_id,
            updated_at = excluded.updated_at
        """,
        audit_rows,
    )


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="Import a manually reviewed AI-assist discovery augmentation batch.")
    ap.add_argument("--input", default="", help="Reviewed AI-assist CSV input path.")
    ap.add_argument("--batch", default="", help="Optional batch id override. Defaults to YYYY-MM-DD_AIASSIST.")
    ap.add_argument("--print-config", action="store_true", help="Print resolved config and exit.")
    ap.add_argument("--dry-run", action="store_true", help="Validate and report without mutating CRM.")
    return ap


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    batch_id = str(args.batch or "").strip() or _default_batch_id()
    data_dir_resolution = resolve_data_dir(REPO_ROOT)
    db_path = crm_store.crm_db_path()

    _emit("AI_ASSIST_BATCH_ID", batch_id)
    _emit("AI_ASSIST_IMPORT_DATA_DIR", str(data_dir_resolution.effective_path))
    _emit("AI_ASSIST_IMPORT_DATA_DIR_SOURCE", str(data_dir_resolution.source or "default"))
    _emit("AI_ASSIST_IMPORT_CRM_DB", str(db_path))
    _emit("AI_ASSIST_IMPORT_EXPECTED_COLUMNS", ",".join(REQUIRED_COLUMNS))

    if args.print_config:
        _emit("AI_ASSIST_IMPORT_DRY_RUN", 1 if args.dry_run else 0)
        return 0

    input_path = Path(str(args.input or "").strip()).expanduser().resolve(strict=False)
    if not str(args.input or "").strip():
        print(f"{ERR_AI_ASSIST_IMPORT_CONFIG} detail=missing_input", file=sys.stderr)
        return 2
    if not input_path.exists():
        print(f"{ERR_AI_ASSIST_IMPORT_INPUT} detail=missing_input path={input_path}", file=sys.stderr)
        return 2

    try:
        rows = _load_csv_rows(input_path)
    except Exception as exc:
        print(f"{ERR_AI_ASSIST_IMPORT_INPUT} detail={exc}", file=sys.stderr)
        return 2

    now_iso = crm_store.utc_now_iso()
    conn: sqlite3.Connection | None = None
    if db_path.exists() or not args.dry_run:
        if not args.dry_run:
            crm_store.ensure_database(db_path)
        conn = crm_store.connect(db_path)
        crm_store.init_schema(conn)

    try:
        suppressed_emails = generation._load_suppression_set(data_dir_resolution.effective_path, conn)
        existing_emails = generation._existing_crm_emails(conn)
        do_not_contact_emails, do_not_contact_domains = _load_do_not_contact_sets(conn)
        audit_rows: list[dict[str, str | int]] = []
        seed_rows: list[dict[str, str]] = []
        per_state: dict[str, Counter] = defaultdict(Counter)
        totals: Counter = Counter()
        batch_seen_emails: set[str] = set()

        for row in rows:
            state = _normalize_state(str(row.get("state") or ""))
            decision = str(row.get("decision") or "").strip().lower()
            email = _normalize_email(str(row.get("email") or ""))
            website = str(row.get("website") or "").strip()
            email_domain = generation._email_domain(email)
            website_domain = _normalize_domain(website)
            domain = email_domain or website_domain
            candidate_key = _candidate_key(row, email=email, domain=domain, state=state)
            source_urls = _parse_source_urls(str(row.get("source_urls") or ""))
            confidence = _coerce_confidence(str(row.get("confidence") or ""))
            audit_payload: dict[str, str | int] = {
                "batch_id": batch_id,
                "candidate_key": candidate_key,
                "state": state,
                "decision": decision,
                "firm": str(row.get("firm") or "").strip(),
                "website": website,
                "domain": domain,
                "contact_name": str(row.get("contact_name") or "").strip(),
                "title": str(row.get("title") or "").strip(),
                "email": email,
                "source_urls_json": json.dumps(source_urls),
                "confidence": confidence,
                "evidence_snippet": str(row.get("evidence_snippet") or "").strip(),
                "verification_status": "review_rejected" if decision == "reject" else "pending_verification",
                "rejection_reason": "",
                "prospect_id": "",
                "created_at": now_iso,
                "updated_at": now_iso,
            }

            totals["candidates"] += 1
            per_state[state]["candidates"] += 1

            if decision == "reject":
                totals["rejected"] += 1
                per_state[state]["rejected"] += 1
                audit_payload["rejection_reason"] = "operator_rejected"
                audit_rows.append(audit_payload)
                continue

            totals["accepted"] += 1
            per_state[state]["accepted"] += 1

            rejection_reason = ""
            if not state:
                rejection_reason = "missing_state"
            elif not generation._valid_email(email):
                rejection_reason = "invalid_email"
            elif not domain:
                rejection_reason = "missing_domain"
            elif email_domain in pools.FREE_EMAIL_DOMAINS:
                rejection_reason = "free_domain"
            elif email in suppressed_emails:
                rejection_reason = "suppressed_email"
            elif email in do_not_contact_emails:
                rejection_reason = "do_not_contact_email"
            elif domain in do_not_contact_domains:
                rejection_reason = "do_not_contact_domain"
            elif email in batch_seen_emails:
                rejection_reason = "duplicate_in_batch"
            elif email in existing_emails:
                rejection_reason = "duplicate_email_in_crm"

            if rejection_reason:
                audit_payload["verification_status"] = "rejected_by_verification"
                audit_payload["rejection_reason"] = rejection_reason
                audit_rows.append(audit_payload)
                continue

            batch_seen_emails.add(email)
            prospect_id = _prospect_id_for_email(email)
            audit_payload["verification_status"] = "verified"
            audit_payload["prospect_id"] = prospect_id
            audit_rows.append(audit_payload)

            seed_rows.append(
                {
                    "prospect_id": prospect_id,
                    "firm": str(row.get("firm") or "").strip(),
                    "contact_name": str(row.get("contact_name") or "").strip(),
                    "email": email,
                    "title": str(row.get("title") or "").strip(),
                    "state": state,
                    "website": website,
                }
            )
            totals["verified"] += 1
            per_state[state]["verified"] += 1

        _emit("AI_ASSIST_CANDIDATES_TOTAL", int(totals.get("candidates", 0)))
        _emit("AI_ASSIST_ACCEPTED_TOTAL", int(totals.get("accepted", 0)))
        _emit("AI_ASSIST_REJECTED_TOTAL", int(totals.get("rejected", 0)))
        _emit("AI_ASSIST_VERIFIED_TOTAL", int(totals.get("verified", 0)))
        for state in sorted(per_state.keys()):
            token_state = state or "UNKNOWN"
            _emit(f"AI_ASSIST_CANDIDATES_TOTAL_STATE_{token_state}", int(per_state[state].get("candidates", 0)))
            _emit(f"AI_ASSIST_ACCEPTED_TOTAL_STATE_{token_state}", int(per_state[state].get("accepted", 0)))
            _emit(f"AI_ASSIST_REJECTED_TOTAL_STATE_{token_state}", int(per_state[state].get("rejected", 0)))
            _emit(f"AI_ASSIST_VERIFIED_TOTAL_STATE_{token_state}", int(per_state[state].get("verified", 0)))

        if args.dry_run:
            _emit("AI_ASSIST_IMPORT_DRY_RUN", 1)
            print(f"{PASS_AI_ASSIST_IMPORT} status=DRY_RUN")
            return 0

        if conn is None:
            crm_store.ensure_database(db_path)
            conn = crm_store.connect(db_path)
            crm_store.init_schema(conn)

        _upsert_audit_rows(conn, audit_rows)
        conn.commit()
    finally:
        if conn is not None:
            conn.close()

    seed_rc = 0
    if seed_rows:
        temp_path: Path | None = None
        try:
            temp_dir = data_dir_resolution.effective_path / "prospect_discovery"
            temp_dir.mkdir(parents=True, exist_ok=True)
            with tempfile.NamedTemporaryFile(
                prefix="ai_assist_import_",
                suffix=".csv",
                dir=str(temp_dir),
                delete=False,
            ) as handle:
                temp_path = Path(handle.name)
            _write_seed_csv(temp_path, seed_rows, created_at=now_iso)
            seed_rc = crm_admin._seed_from_csv(temp_path, archive_dir=None, no_archive=True)
        finally:
            if temp_path is not None and temp_path.exists():
                try:
                    temp_path.unlink()
                except Exception:
                    pass
    if seed_rc != 0:
        return seed_rc

    print(f"{PASS_AI_ASSIST_IMPORT} status=OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
