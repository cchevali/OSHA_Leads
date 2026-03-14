#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import sqlite3
import sys
import tempfile
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
import re

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import seed_recipients_pools as pools
from outreach import crm_admin
from outreach import contact_normalization
from outreach import crm_store
from outreach import run_prospect_generation as generation
from outreach import us_state
from runtime_data_dir import resolve_data_dir

ERR_AI_ASSIST_IMPORT_CONFIG = "ERR_AI_ASSIST_IMPORT_CONFIG"
ERR_AI_ASSIST_IMPORT_INPUT = "ERR_AI_ASSIST_IMPORT_INPUT"
ERR_AI_ASSIST_IMPORT_DRIFT = "ERR_AI_ASSIST_IMPORT_DRIFT"
PASS_AI_ASSIST_IMPORT = "PASS_AI_ASSIST_IMPORT"
PASS_AI_ASSIST_PENDING_IMPORT = "PASS_AI_ASSIST_PENDING_IMPORT"
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
PENDING_REVIEW_FILENAME_RE = re.compile(r"^prospect_ai_assist_review_(\d{8})_reviewed\.csv$")
STALE_STARTED_MINUTES = 30
TRACKING_STATUS_STARTED = "started"
TRACKING_STATUS_COMPLETED = "completed"
TRACKING_STATUS_FAILED = "failed"


def _emit(key: str, value: str | int) -> None:
    print(f"{key}={value}")


def _local_today_stamp() -> str:
    return datetime.now().astimezone().date().isoformat()


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _default_batch_id(input_path: Path | None = None) -> str:
    path = input_path or Path("")
    match = PENDING_REVIEW_FILENAME_RE.match(path.name)
    if match:
        token = match.group(1)
        return f"{token[:4]}-{token[4:6]}-{token[6:8]}_AIASSIST"
    return f"{_local_today_stamp()}_AIASSIST"


def _normalize_email(value: str) -> str:
    return contact_normalization.normalize_email(value)


def _normalize_state(value: str) -> str:
    return us_state.normalize_us_state(value)


def _normalize_domain(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if "@" in text:
        return contact_normalization.email_domain(text)
    return generation._domain_from_website(contact_normalization.normalize_website(text))


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
    text = contact_normalization.normalize_source_urls(raw)
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


def _malformed_row_error(*, row_number: int, field: str) -> ValueError:
    return ValueError(f"malformed_row row={row_number} field={field}")


def _normalize_review_field(*, field: str, value: str, row_number: int) -> str:
    raw = str(value or "")
    text = raw.strip()
    has_markup = contact_normalization.has_markup_artifact(raw)
    if field == "website":
        normalized = contact_normalization.normalize_website(text)
        if has_markup and (not normalized or contact_normalization.has_markup_artifact(normalized)):
            raise _malformed_row_error(row_number=row_number, field=field)
        return normalized
    if field == "contact_name":
        normalized = contact_normalization.normalize_contact_name(text)
        if has_markup and (not normalized or contact_normalization.has_markup_artifact(normalized)):
            raise _malformed_row_error(row_number=row_number, field=field)
        return normalized
    if field == "email":
        normalized = contact_normalization.normalize_email(text)
        if has_markup and not contact_normalization.valid_email(normalized):
            raise _malformed_row_error(row_number=row_number, field=field)
        return normalized
    if field == "source_urls":
        normalized = contact_normalization.normalize_source_urls(text)
        if has_markup and (not normalized or contact_normalization.has_markup_artifact(normalized)):
            raise _malformed_row_error(row_number=row_number, field=field)
        return normalized
    if field == "evidence_snippet":
        normalized = contact_normalization.normalize_evidence_snippet(text)
        if has_markup and (not normalized or contact_normalization.has_markup_artifact(normalized)):
            raise _malformed_row_error(row_number=row_number, field=field)
        return normalized
    if field == "state":
        return text.upper()
    if field == "decision":
        return text.lower()
    return text


def _normalize_review_rows(rows: list[dict[str, str]]) -> tuple[list[dict[str, str]], int, int]:
    normalized_rows: list[dict[str, str]] = []
    normalized_row_total = 0
    normalized_field_total = 0
    for row_number, row in enumerate(rows, start=2):
        clean: dict[str, str] = {}
        row_field_changes = 0
        for field in REQUIRED_COLUMNS:
            raw_value = "" if row.get(field) is None else str(row.get(field))
            normalized_value = _normalize_review_field(field=field, value=raw_value, row_number=row_number)
            clean[field] = normalized_value
            if normalized_value != raw_value.strip():
                row_field_changes += 1
        if row_field_changes:
            normalized_row_total += 1
            normalized_field_total += row_field_changes
        normalized_rows.append(clean)
    return normalized_rows, normalized_row_total, normalized_field_total


def _split_first(text: str, delimiter: str) -> tuple[str, str]:
    left, found, right = str(text or "").partition(delimiter)
    if not found:
        return str(text or "").strip(), ""
    return left.strip(), right.strip()


def _parse_markdownish_field(value: str) -> tuple[str, str, str]:
    text = str(value or "").strip()
    match = re.match(r"^\[(.*?)\]\((.*?)\)\s*(.*)$", text)
    if not match:
        return text, "", ""
    label = str(match.group(1) or "").strip()
    target = str(match.group(2) or "").strip()
    tail = str(match.group(3) or "").strip()
    return label, target, tail


def _parse_contact_block(value: str) -> tuple[str, str]:
    label, target, tail = _parse_markdownish_field(value)
    source = label or target
    website, contact_head = _split_first(source, ",")
    contact_name = " ".join(part for part in [contact_head, tail] if part).strip()
    return contact_normalization.normalize_website(website), contact_normalization.normalize_contact_name(contact_name)


def _parse_email_block(value: str) -> str:
    label, target, _tail = _parse_markdownish_field(value)
    if label and "@" in label:
        return contact_normalization.normalize_email(label)
    if target.lower().startswith("mailto:"):
        return contact_normalization.normalize_email(target.split(":", 1)[1].strip())
    return contact_normalization.normalize_email(label.strip() or target.strip())


def _parse_source_blob(value: str) -> tuple[str, str, str]:
    label, target, tail = _parse_markdownish_field(value)
    source = label or target or str(value or "").strip()
    urls_text = ""
    confidence = ""
    snippet_head = ""
    parts = source.rsplit(",", 2)
    if len(parts) == 3:
        urls_text, confidence, snippet_head = parts
    elif len(parts) == 2:
        urls_text, confidence = parts
    elif len(parts) == 1:
        urls_text = parts[0]
    evidence_snippet = " ".join(part for part in [snippet_head.strip(), tail] if part).strip()
    return urls_text.strip(), confidence.strip(), evidence_snippet


def _parse_compact_review_line(state: str, decision: str, remainder: str) -> dict[str, str]:
    firm, website_blob = str(remainder or "").strip().rsplit(",[", 1)
    label, target, tail = _parse_markdownish_field("[" + website_blob)
    source = label or target
    parts = str(source or "").split(",", 6)
    if len(parts) != 7:
        raise ValueError("invalid_review_row_shape")
    website, contact_name, title, email, source_urls, confidence, snippet_head = [str(part or "").strip() for part in parts]
    evidence_snippet = " ".join(part for part in [snippet_head, tail] if part).strip()
    return {
        "state": str(state or "").strip(),
        "decision": str(decision or "").strip(),
        "firm": str(firm or "").strip(),
        "website": website,
        "contact_name": contact_name,
        "title": title,
        "email": email,
        "source_urls": source_urls,
        "confidence": confidence,
        "evidence_snippet": evidence_snippet,
    }


def _parse_review_line(line: str) -> dict[str, str]:
    state, _found, remainder = str(line or "").partition(",")
    if not _found:
        raise ValueError("invalid_row_missing_state")
    decision, _found, remainder = remainder.partition(",")
    if not _found:
        raise ValueError("invalid_row_missing_decision")
    match = re.match(
        r"^(?P<firm_and_contact>.*),(?P<title>[^,]+),(?P<email_block>\[[^\]]+\]\(mailto:[^)]+\)),(?P<source_blob>.*)$",
        remainder.strip(),
    )
    if match is None:
        return _parse_compact_review_line(state=state, decision=decision, remainder=remainder)
    firm_and_contact = str(match.group("firm_and_contact") or "").strip()
    firm, website_and_contact = firm_and_contact.rsplit(",[", 1)
    website, contact_name = _parse_contact_block("[" + website_and_contact)
    email = _parse_email_block(str(match.group("email_block") or ""))
    source_urls, confidence, evidence_snippet = _parse_source_blob(str(match.group("source_blob") or ""))
    return {
        "state": str(state or "").strip(),
        "decision": str(decision or "").strip(),
        "firm": str(firm or "").strip(),
        "website": website,
        "contact_name": contact_name,
        "title": str(match.group("title") or "").strip(),
        "email": email,
        "source_urls": source_urls,
        "confidence": confidence,
        "evidence_snippet": evidence_snippet,
    }


def _load_markdown_review_rows(input_path: Path) -> list[dict[str, str]]:
    lines = input_path.read_text(encoding="utf-8-sig").splitlines()
    if not lines:
        raise ValueError("missing_header")
    header = [part.strip() for part in str(lines[0] or "").split(",")]
    missing = [column for column in REQUIRED_COLUMNS if column not in header]
    if missing:
        raise ValueError(f"missing_columns={','.join(missing)}")
    rows: list[dict[str, str]] = []
    for raw_line in lines[1:]:
        if not str(raw_line or "").strip():
            continue
        clean = _parse_review_line(raw_line)
        decision = str(clean.get("decision") or "").strip().lower()
        if decision not in {"accept", "reject"}:
            raise ValueError(f"invalid_decision={decision or 'blank'}")
        rows.append(clean)
    return rows


def _load_csv_rows(input_path: Path) -> list[dict[str, str]]:
    with open(input_path, "r", newline="", encoding="utf-8-sig") as handle:
        raw_text = handle.read()
    with io.StringIO(raw_text) as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise ValueError("missing_header")
        clean_fieldnames = [str(name or "").lstrip("\ufeff") for name in list(reader.fieldnames or [])]
        missing = [column for column in REQUIRED_COLUMNS if column not in clean_fieldnames]
        if missing:
            raise ValueError(f"missing_columns={','.join(missing)}")
        rows: list[dict[str, str]] = []
        for raw_row in reader:
            if None in raw_row:
                return _load_markdown_review_rows(input_path)
            clean: dict[str, str] = {}
            for key, value in dict(raw_row).items():
                clean[str(key or "").lstrip("\ufeff")] = "" if value is None else str(value)
            decision = str(clean.get("decision") or "").strip().lower()
            if decision not in {"accept", "reject"}:
                raise ValueError(f"invalid_decision={decision or 'blank'}")
            rows.append(clean)
        return rows


def _sha256_file(path: Path) -> str:
    hasher = hashlib.sha256()
    with open(path, "rb") as handle:
        while True:
            chunk = handle.read(65536)
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()


def _discover_pending_review_files(review_dir: Path) -> list[Path]:
    if not review_dir.exists():
        return []
    rows: list[tuple[str, str, Path]] = []
    for path in review_dir.iterdir():
        if not path.is_file():
            continue
        match = PENDING_REVIEW_FILENAME_RE.match(path.name)
        if not match:
            continue
        rows.append((match.group(1), path.name.lower(), path.resolve(strict=False)))
    rows.sort(key=lambda item: (item[0], item[1]))
    return [item[2] for item in rows]


def _load_do_not_contact_sets(conn: sqlite3.Connection | None) -> tuple[set[str], set[str]]:
    if conn is None:
        return set(), set()
    email_matches: set[str] = set()
    domain_matches: set[str] = set()
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


def _load_existing_prospect_map(conn: sqlite3.Connection | None) -> dict[str, dict[str, str]]:
    if conn is None:
        return {}
    rows = conn.execute(
        """
        SELECT email, prospect_id, source
        FROM prospects
        WHERE email IS NOT NULL AND trim(email) <> ''
        """
    ).fetchall()
    out: dict[str, dict[str, str]] = {}
    for row in rows:
        email = _normalize_email(str(row["email"] or ""))
        if not email:
            continue
        out[email] = {
            "prospect_id": str(row["prospect_id"] or "").strip(),
            "source": str(row["source"] or "").strip(),
        }
    return out


def _load_batch_candidate_map(conn: sqlite3.Connection | None, batch_id: str) -> dict[str, dict[str, str]]:
    if conn is None:
        return {}
    rows = conn.execute(
        f"""
        SELECT candidate_key, verification_status, rejection_reason, prospect_id, email
        FROM {crm_store.AI_ASSIST_CANDIDATE_TABLE}
        WHERE batch_id = ?
        """,
        (batch_id,),
    ).fetchall()
    out: dict[str, dict[str, str]] = {}
    for row in rows:
        out[str(row["candidate_key"] or "")] = {
            "verification_status": str(row["verification_status"] or "").strip(),
            "rejection_reason": str(row["rejection_reason"] or "").strip(),
            "prospect_id": str(row["prospect_id"] or "").strip(),
            "email": _normalize_email(str(row["email"] or "")),
        }
    return out


def _load_verified_email_batches(conn: sqlite3.Connection | None, batch_id: str) -> dict[str, str]:
    if conn is None:
        return {}
    rows = conn.execute(
        f"""
        SELECT email, batch_id
        FROM {crm_store.AI_ASSIST_CANDIDATE_TABLE}
        WHERE verification_status = 'verified'
          AND batch_id <> ?
          AND email IS NOT NULL
          AND trim(email) <> ''
        """,
        (batch_id,),
    ).fetchall()
    out: dict[str, str] = {}
    for row in rows:
        email = _normalize_email(str(row["email"] or ""))
        if email and email not in out:
            out[email] = str(row["batch_id"] or "").strip()
    return out


def _load_batch_tracking_row(conn: sqlite3.Connection | None, batch_id: str) -> sqlite3.Row | None:
    if conn is None:
        return None
    return conn.execute(
        f"""
        SELECT batch_id, source_path, source_filename, source_file_hash, status, started_at,
               completed_at, last_error, candidates_total, accepted_total, rejected_total,
               verified_total, created_at, updated_at
        FROM {crm_store.AI_ASSIST_IMPORT_BATCH_TABLE}
        WHERE batch_id = ?
        LIMIT 1
        """,
        (batch_id,),
    ).fetchone()


def _batch_preview(rows: list[dict[str, str]]) -> tuple[set[str], Counter]:
    candidate_keys: set[str] = set()
    totals: Counter = Counter()
    for row in rows:
        state = _normalize_state(str(row.get("state") or ""))
        decision = str(row.get("decision") or "").strip().lower()
        email = _normalize_email(str(row.get("email") or ""))
        website = str(row.get("website") or "").strip()
        domain = generation._email_domain(email) or _normalize_domain(website)
        candidate_keys.add(_candidate_key(row, email=email, domain=domain, state=state))
        totals["candidates"] += 1
        if decision == "accept":
            totals["accepted"] += 1
        elif decision == "reject":
            totals["rejected"] += 1
    return candidate_keys, totals


def _legacy_completed_batch_totals(
    conn: sqlite3.Connection | None,
    batch_id: str,
    rows: list[dict[str, str]],
) -> Counter | None:
    if conn is None:
        return None
    batch_candidate_map = _load_batch_candidate_map(conn, batch_id)
    if not batch_candidate_map:
        return None
    candidate_keys, preview_totals = _batch_preview(rows)
    if not candidate_keys or not candidate_keys.issubset(set(batch_candidate_map.keys())):
        return None
    verified_total = 0
    for candidate_key in candidate_keys:
        existing = batch_candidate_map.get(candidate_key) or {}
        if str(existing.get("verification_status") or "").strip().lower() == "verified":
            verified_total += 1
    preview_totals["verified"] = verified_total
    return preview_totals


def _parse_iso_datetime(value: str) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    normalized = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except Exception:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _batch_claim_state(existing_row: sqlite3.Row | None, now_dt: datetime) -> str:
    if existing_row is None:
        return "new"
    status = str(existing_row["status"] or "").strip().lower()
    if status == TRACKING_STATUS_COMPLETED:
        return "skip_completed"
    if status == TRACKING_STATUS_STARTED:
        started_at = _parse_iso_datetime(str(existing_row["started_at"] or "")) or _parse_iso_datetime(
            str(existing_row["updated_at"] or "")
        )
        if started_at is not None and (now_dt - started_at) <= timedelta(minutes=STALE_STARTED_MINUTES):
            return "skip_in_progress"
        return "resume_stale_started"
    if status == TRACKING_STATUS_FAILED:
        return "resume_failed"
    return "resume_other"


def _begin_batch_tracking(
    conn: sqlite3.Connection,
    *,
    batch_id: str,
    source_path: Path,
    source_hash: str,
    now_iso: str,
) -> tuple[str, sqlite3.Row | None]:
    existing_row = _load_batch_tracking_row(conn, batch_id)
    if existing_row is not None:
        existing_hash = str(existing_row["source_file_hash"] or "").strip()
        if existing_hash and existing_hash != source_hash:
            raise ValueError(
                f"{ERR_AI_ASSIST_IMPORT_DRIFT} batch_id={batch_id} "
                f"expected_hash={existing_hash} got_hash={source_hash}"
            )
    claim_state = _batch_claim_state(existing_row, _utc_now())
    if claim_state in {"skip_completed", "skip_in_progress"}:
        return claim_state, existing_row

    source_path_text = str(source_path.resolve(strict=False))
    source_filename = source_path.name
    if existing_row is None:
        conn.execute(
            f"""
            INSERT INTO {crm_store.AI_ASSIST_IMPORT_BATCH_TABLE}(
                batch_id, source_path, source_filename, source_file_hash, status, started_at,
                completed_at, last_error, candidates_total, accepted_total, rejected_total,
                verified_total, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, '', '', 0, 0, 0, 0, ?, ?)
            """,
            (
                batch_id,
                source_path_text,
                source_filename,
                source_hash,
                TRACKING_STATUS_STARTED,
                now_iso,
                now_iso,
                now_iso,
            ),
        )
    else:
        conn.execute(
            f"""
            UPDATE {crm_store.AI_ASSIST_IMPORT_BATCH_TABLE}
            SET source_path = ?,
                source_filename = ?,
                source_file_hash = ?,
                status = ?,
                started_at = ?,
                completed_at = '',
                last_error = '',
                updated_at = ?
            WHERE batch_id = ?
            """,
            (
                source_path_text,
                source_filename,
                source_hash,
                TRACKING_STATUS_STARTED,
                now_iso,
                now_iso,
                batch_id,
            ),
        )
    conn.commit()
    return claim_state, existing_row


def _create_completed_batch_tracking(
    conn: sqlite3.Connection,
    *,
    batch_id: str,
    source_path: Path,
    source_hash: str,
    candidates_total: int,
    accepted_total: int,
    rejected_total: int,
    verified_total: int,
    now_iso: str,
) -> None:
    source_path_text = str(source_path.resolve(strict=False))
    source_filename = source_path.name
    conn.execute(
        f"""
        INSERT INTO {crm_store.AI_ASSIST_IMPORT_BATCH_TABLE}(
            batch_id, source_path, source_filename, source_file_hash, status, started_at,
            completed_at, last_error, candidates_total, accepted_total, rejected_total,
            verified_total, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, '', ?, ?, ?, ?, ?, ?)
        ON CONFLICT(batch_id) DO UPDATE SET
            source_path = excluded.source_path,
            source_filename = excluded.source_filename,
            source_file_hash = excluded.source_file_hash,
            status = excluded.status,
            started_at = excluded.started_at,
            completed_at = excluded.completed_at,
            last_error = '',
            candidates_total = excluded.candidates_total,
            accepted_total = excluded.accepted_total,
            rejected_total = excluded.rejected_total,
            verified_total = excluded.verified_total,
            updated_at = excluded.updated_at
        """,
        (
            batch_id,
            source_path_text,
            source_filename,
            source_hash,
            TRACKING_STATUS_COMPLETED,
            now_iso,
            now_iso,
            int(candidates_total),
            int(accepted_total),
            int(rejected_total),
            int(verified_total),
            now_iso,
            now_iso,
        ),
    )
    conn.commit()


def _finish_batch_tracking(
    conn: sqlite3.Connection,
    *,
    batch_id: str,
    status: str,
    last_error: str,
    candidates_total: int,
    accepted_total: int,
    rejected_total: int,
    verified_total: int,
    now_iso: str,
) -> None:
    completed_at = now_iso if status == TRACKING_STATUS_COMPLETED else ""
    conn.execute(
        f"""
        UPDATE {crm_store.AI_ASSIST_IMPORT_BATCH_TABLE}
        SET status = ?,
            completed_at = ?,
            last_error = ?,
            candidates_total = ?,
            accepted_total = ?,
            rejected_total = ?,
            verified_total = ?,
            updated_at = ?
        WHERE batch_id = ?
        """,
        (
            status,
            completed_at,
            str(last_error or "").strip(),
            int(candidates_total),
            int(accepted_total),
            int(rejected_total),
            int(verified_total),
            now_iso,
            batch_id,
        ),
    )
    conn.commit()


def _base_audit_payload(
    *,
    batch_id: str,
    candidate_key: str,
    state: str,
    decision: str,
    firm: str,
    website: str,
    domain: str,
    contact_name: str,
    title: str,
    email: str,
    source_urls: list[str],
    confidence: int,
    evidence_snippet: str,
    now_iso: str,
) -> dict[str, str | int]:
    return {
        "batch_id": batch_id,
        "candidate_key": candidate_key,
        "state": state,
        "decision": decision,
        "firm": firm,
        "website": website,
        "domain": domain,
        "contact_name": contact_name,
        "title": title,
        "email": email,
        "source_urls_json": json.dumps(source_urls),
        "confidence": confidence,
        "evidence_snippet": evidence_snippet,
        "verification_status": "pending_verification",
        "rejection_reason": "",
        "prospect_id": "",
        "created_at": now_iso,
        "updated_at": now_iso,
    }


def _print_totals(*, totals: Counter, per_state: dict[str, Counter], dry_run: bool, final_status: str) -> None:
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
    if dry_run:
        _emit("AI_ASSIST_IMPORT_DRY_RUN", 1)
        print(f"{PASS_AI_ASSIST_IMPORT} status=DRY_RUN")
    else:
        print(f"{PASS_AI_ASSIST_IMPORT} status={final_status}")


def _import_review_file(
    *,
    input_path: Path,
    batch_id_override: str = "",
    dry_run: bool = False,
) -> tuple[int, str]:
    data_dir_resolution = resolve_data_dir(REPO_ROOT)
    db_path = crm_store.crm_db_path()
    batch_id = str(batch_id_override or "").strip() or _default_batch_id(input_path)
    input_path = input_path.expanduser().resolve(strict=False)

    _emit("AI_ASSIST_BATCH_ID", batch_id)
    _emit("AI_ASSIST_IMPORT_INPUT_PATH", str(input_path))
    _emit("AI_ASSIST_IMPORT_DATA_DIR", str(data_dir_resolution.effective_path))
    _emit("AI_ASSIST_IMPORT_DATA_DIR_SOURCE", str(data_dir_resolution.source or "default"))
    _emit("AI_ASSIST_IMPORT_CRM_DB", str(db_path))
    _emit("AI_ASSIST_IMPORT_EXPECTED_COLUMNS", ",".join(REQUIRED_COLUMNS))

    if not input_path.exists():
        print(f"{ERR_AI_ASSIST_IMPORT_INPUT} detail=missing_input path={input_path}", file=sys.stderr)
        return 2, "MISSING_INPUT"

    try:
        rows = _load_csv_rows(input_path)
    except Exception as exc:
        print(f"{ERR_AI_ASSIST_IMPORT_INPUT} detail={exc}", file=sys.stderr)
        return 2, "INVALID_INPUT"
    try:
        rows, normalized_rows_total, normalized_fields_total = _normalize_review_rows(rows)
    except Exception as exc:
        print(f"{ERR_AI_ASSIST_IMPORT_INPUT} detail={exc}", file=sys.stderr)
        return 2, "INVALID_INPUT"

    now_iso = crm_store.utc_now_iso()
    source_hash = _sha256_file(input_path)
    _emit("AI_ASSIST_IMPORT_SOURCE_FILE_HASH", source_hash)
    _emit("AI_ASSIST_IMPORT_NORMALIZED_ROWS", normalized_rows_total)
    _emit("AI_ASSIST_IMPORT_NORMALIZED_FIELDS", normalized_fields_total)

    conn: sqlite3.Connection | None = None
    claim_state = "dry_run"
    seed_rc = 0
    final_status = "OK"
    batch_failure_detail = ""
    try:
        if db_path.exists() or not dry_run:
            if not dry_run:
                crm_store.ensure_database(db_path)
            conn = crm_store.connect(db_path)
            crm_store.init_schema(conn)

        if not dry_run and conn is not None:
            legacy_totals = _legacy_completed_batch_totals(conn, batch_id, rows)
            if _load_batch_tracking_row(conn, batch_id) is None and legacy_totals is not None:
                _create_completed_batch_tracking(
                    conn,
                    batch_id=batch_id,
                    source_path=input_path,
                    source_hash=source_hash,
                    candidates_total=int(legacy_totals.get("candidates", 0)),
                    accepted_total=int(legacy_totals.get("accepted", 0)),
                    rejected_total=int(legacy_totals.get("rejected", 0)),
                    verified_total=int(legacy_totals.get("verified", 0)),
                    now_iso=now_iso,
                )
                _emit("AI_ASSIST_IMPORT_BATCH_STATE", "skip_completed")
                print(f"{PASS_AI_ASSIST_IMPORT} status=SKIPPED_ALREADY_COMPLETED")
                return 0, "SKIPPED_ALREADY_COMPLETED"
            try:
                claim_state, _existing_row = _begin_batch_tracking(
                    conn,
                    batch_id=batch_id,
                    source_path=input_path,
                    source_hash=source_hash,
                    now_iso=now_iso,
                )
            except ValueError as exc:
                print(str(exc), file=sys.stderr)
                return 2, "DRIFT"
            _emit("AI_ASSIST_IMPORT_BATCH_STATE", claim_state)
            if claim_state == "skip_completed":
                print(f"{PASS_AI_ASSIST_IMPORT} status=SKIPPED_ALREADY_COMPLETED")
                return 0, "SKIPPED_ALREADY_COMPLETED"
            if claim_state == "skip_in_progress":
                print(f"{PASS_AI_ASSIST_IMPORT} status=SKIPPED_IN_PROGRESS")
                return 0, "SKIPPED_IN_PROGRESS"

        suppressed_emails = generation._load_suppression_set(data_dir_resolution.effective_path, conn)
        existing_prospects = _load_existing_prospect_map(conn)
        batch_candidate_map = _load_batch_candidate_map(conn, batch_id)
        verified_email_other_batches = _load_verified_email_batches(conn, batch_id)
        do_not_contact_emails, do_not_contact_domains = _load_do_not_contact_sets(conn)

        candidate_rows: list[dict[str, object]] = []
        seed_rows: list[dict[str, str]] = []
        totals: Counter = Counter()
        per_state: dict[str, Counter] = defaultdict(Counter)
        batch_seen_emails: set[str] = set()
        allow_recover_existing = bool(
            (not dry_run) and claim_state in {"resume_failed", "resume_stale_started", "resume_other"}
        )

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
            firm = str(row.get("firm") or "").strip()
            contact_name = str(row.get("contact_name") or "").strip()
            title = str(row.get("title") or "").strip()
            evidence_snippet = str(row.get("evidence_snippet") or "").strip()
            prospect_id = _prospect_id_for_email(email) if email else ""
            prior_batch_row = batch_candidate_map.get(candidate_key) or {}
            candidate: dict[str, object] = {
                "decision": decision,
                "state": state,
                "email": email,
                "website": website,
                "domain": domain,
                "candidate_key": candidate_key,
                "prospect_id": prospect_id,
                "audit_payload": _base_audit_payload(
                    batch_id=batch_id,
                    candidate_key=candidate_key,
                    state=state,
                    decision=decision,
                    firm=firm,
                    website=website,
                    domain=domain,
                    contact_name=contact_name,
                    title=title,
                    email=email,
                    source_urls=source_urls,
                    confidence=confidence,
                    evidence_snippet=evidence_snippet,
                    now_iso=now_iso,
                ),
                "final_verification_status": "",
                "final_rejection_reason": "",
                "seed_requested": False,
            }

            totals["candidates"] += 1
            per_state[state]["candidates"] += 1

            if decision == "reject":
                totals["rejected"] += 1
                per_state[state]["rejected"] += 1
                candidate["final_verification_status"] = "review_rejected"
                candidate["final_rejection_reason"] = "operator_rejected"
                candidate_rows.append(candidate)
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

            if rejection_reason:
                candidate["final_verification_status"] = "rejected_by_verification"
                candidate["final_rejection_reason"] = rejection_reason
                candidate_rows.append(candidate)
                continue

            batch_seen_emails.add(email)
            existing_row = existing_prospects.get(email) or {}
            existing_prospect_id = str(existing_row.get("prospect_id") or "").strip()
            existing_source = str(existing_row.get("source") or "").strip().lower()
            prior_same_batch_verified = (
                str(prior_batch_row.get("verification_status") or "").strip() == "verified"
                and str(prior_batch_row.get("prospect_id") or "").strip() == prospect_id
            )
            resume_verified = bool(
                allow_recover_existing
                and existing_prospect_id == prospect_id
                and existing_source.startswith("ai_assist_manual")
                and email not in verified_email_other_batches
            )

            if prior_same_batch_verified or resume_verified:
                candidate["final_verification_status"] = "verified"
                candidate_rows.append(candidate)
                continue
            if email in existing_prospects:
                candidate["final_verification_status"] = "rejected_by_verification"
                candidate["final_rejection_reason"] = "duplicate_email_in_crm"
                candidate_rows.append(candidate)
                continue

            candidate["seed_requested"] = True
            seed_rows.append(
                {
                    "prospect_id": prospect_id,
                    "firm": firm,
                    "contact_name": contact_name,
                    "email": email,
                    "title": title,
                    "state": state,
                    "website": website,
                }
            )
            candidate_rows.append(candidate)

        if dry_run:
            for candidate in candidate_rows:
                if str(candidate.get("decision") or "") != "accept":
                    continue
                if str(candidate.get("final_verification_status") or "") == "verified" or bool(
                    candidate.get("seed_requested")
                ):
                    totals["verified"] += 1
                    per_state[str(candidate.get("state") or "")]["verified"] += 1
                    candidate["final_verification_status"] = "verified"
                    candidate["final_rejection_reason"] = ""
            _print_totals(totals=totals, per_state=per_state, dry_run=True, final_status="DRY_RUN")
            return 0, "DRY_RUN"

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

        existing_prospects = _load_existing_prospect_map(conn)
        seed_failed_total = 0
        for candidate in candidate_rows:
            if str(candidate.get("decision") or "") == "reject":
                continue
            if str(candidate.get("final_verification_status") or "") in {"review_rejected", "rejected_by_verification"}:
                continue
            email = str(candidate.get("email") or "")
            prospect_id = str(candidate.get("prospect_id") or "")
            existing_row = existing_prospects.get(email) or {}
            existing_prospect_id = str(existing_row.get("prospect_id") or "").strip()
            existing_source = str(existing_row.get("source") or "").strip().lower()
            if existing_prospect_id == prospect_id and existing_source.startswith("ai_assist_manual"):
                candidate["final_verification_status"] = "verified"
                totals["verified"] += 1
                per_state[str(candidate.get("state") or "")]["verified"] += 1
                continue
            if email in existing_prospects:
                candidate["final_verification_status"] = "rejected_by_verification"
                candidate["final_rejection_reason"] = "duplicate_email_in_crm"
                continue
            candidate["final_verification_status"] = "seed_failed"
            candidate["final_rejection_reason"] = "seed_failed"
            seed_failed_total += 1

        audit_rows: list[dict[str, str | int]] = []
        for candidate in candidate_rows:
            audit_payload = dict(candidate["audit_payload"] or {})
            audit_payload["verification_status"] = str(candidate.get("final_verification_status") or "")
            audit_payload["rejection_reason"] = str(candidate.get("final_rejection_reason") or "")
            if str(audit_payload["verification_status"] or "") == "verified":
                audit_payload["prospect_id"] = str(candidate.get("prospect_id") or "")
            else:
                audit_payload["prospect_id"] = ""
            audit_rows.append(audit_payload)

        _upsert_audit_rows(conn, audit_rows)
        conn.commit()

        if seed_rc != 0:
            final_status = "FAILED"
            batch_failure_detail = f"seed_rc={seed_rc}"
        elif seed_failed_total > 0:
            final_status = "FAILED"
            batch_failure_detail = f"seed_failed_total={seed_failed_total}"

        if conn is not None:
            _finish_batch_tracking(
                conn,
                batch_id=batch_id,
                status=TRACKING_STATUS_COMPLETED if final_status == "OK" else TRACKING_STATUS_FAILED,
                last_error=batch_failure_detail,
                candidates_total=int(totals.get("candidates", 0)),
                accepted_total=int(totals.get("accepted", 0)),
                rejected_total=int(totals.get("rejected", 0)),
                verified_total=int(totals.get("verified", 0)),
                now_iso=crm_store.utc_now_iso(),
            )

        _print_totals(totals=totals, per_state=per_state, dry_run=False, final_status=final_status)
        if final_status != "OK":
            return (seed_rc or 1), final_status
        return 0, final_status
    finally:
        if conn is not None:
            conn.close()


def run_pending_imports(*, dry_run: bool = False) -> int:
    data_dir_resolution = resolve_data_dir(REPO_ROOT)
    review_dir = data_dir_resolution.effective_path / "audits" / "ai_assist"
    files = _discover_pending_review_files(review_dir)
    _emit("AI_ASSIST_PENDING_IMPORT_DATA_DIR", str(data_dir_resolution.effective_path))
    _emit("AI_ASSIST_PENDING_IMPORT_DATA_DIR_SOURCE", str(data_dir_resolution.source or "default"))
    _emit("AI_ASSIST_PENDING_IMPORT_DIR", str(review_dir))
    _emit("AI_ASSIST_PENDING_IMPORT_DISCOVERED_TOTAL", len(files))
    _emit("AI_ASSIST_PENDING_IMPORT_DRY_RUN", 1 if dry_run else 0)

    imported_batches = 0
    skipped_batches = 0
    for input_path in files:
        batch_id = _default_batch_id(input_path)
        _emit("AI_ASSIST_PENDING_IMPORT_FILE", str(input_path))
        _emit("AI_ASSIST_PENDING_IMPORT_BATCH_ID", batch_id)
        rc, status = _import_review_file(input_path=input_path, batch_id_override=batch_id, dry_run=dry_run)
        if rc != 0:
            return rc
        if status in {"SKIPPED_ALREADY_COMPLETED", "SKIPPED_IN_PROGRESS"}:
            skipped_batches += 1
        elif status in {"OK", "DRY_RUN", "FAILED"}:
            imported_batches += 1

    if not files:
        print(f"{PASS_AI_ASSIST_PENDING_IMPORT} status=NO_PENDING")
        return 0
    print(
        f"{PASS_AI_ASSIST_PENDING_IMPORT} status=OK "
        f"imported_batches={imported_batches} skipped_batches={skipped_batches}"
    )
    return 0


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="Import reviewed AI-assist discovery augmentation batches.")
    ap.add_argument("--input", default="", help="Reviewed AI-assist CSV input path.")
    ap.add_argument("--batch", default="", help="Optional batch id override.")
    ap.add_argument("--pending", action="store_true", help="Import pending reviewed files from DATA_DIR\\audits\\ai_assist.")
    ap.add_argument("--print-config", action="store_true", help="Print resolved config and exit.")
    ap.add_argument("--dry-run", action="store_true", help="Validate and report without mutating CRM.")
    return ap


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if bool(args.pending) and str(args.input or "").strip():
        print(f"{ERR_AI_ASSIST_IMPORT_CONFIG} detail=modes_mutually_exclusive", file=sys.stderr)
        return 2

    data_dir_resolution = resolve_data_dir(REPO_ROOT)
    db_path = crm_store.crm_db_path()
    _emit("AI_ASSIST_IMPORT_DATA_DIR", str(data_dir_resolution.effective_path))
    _emit("AI_ASSIST_IMPORT_DATA_DIR_SOURCE", str(data_dir_resolution.source or "default"))
    _emit("AI_ASSIST_IMPORT_CRM_DB", str(db_path))
    _emit("AI_ASSIST_IMPORT_EXPECTED_COLUMNS", ",".join(REQUIRED_COLUMNS))

    if args.pending:
        review_dir = data_dir_resolution.effective_path / "audits" / "ai_assist"
        files = _discover_pending_review_files(review_dir)
        _emit("AI_ASSIST_PENDING_IMPORT_DIR", str(review_dir))
        _emit("AI_ASSIST_PENDING_IMPORT_DISCOVERED_TOTAL", len(files))
        for idx, path in enumerate(files, start=1):
            _emit(f"AI_ASSIST_PENDING_IMPORT_FILE_{idx}", str(path))
        if args.print_config:
            _emit("AI_ASSIST_IMPORT_DRY_RUN", 1 if args.dry_run else 0)
            return 0
        return run_pending_imports(dry_run=bool(args.dry_run))

    input_text = str(args.input or "").strip()
    if args.print_config:
        batch_id = (str(args.batch or "").strip() or _default_batch_id(Path(input_text))) if input_text else _default_batch_id()
        _emit("AI_ASSIST_BATCH_ID", batch_id)
        _emit("AI_ASSIST_IMPORT_INPUT_PATH", str(Path(input_text).expanduser().resolve(strict=False)) if input_text else "")
        _emit("AI_ASSIST_IMPORT_DRY_RUN", 1 if args.dry_run else 0)
        return 0
    if not input_text:
        print(f"{ERR_AI_ASSIST_IMPORT_CONFIG} detail=missing_input", file=sys.stderr)
        return 2
    rc, _status = _import_review_file(
        input_path=Path(input_text),
        batch_id_override=str(args.batch or "").strip(),
        dry_run=bool(args.dry_run),
    )
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
