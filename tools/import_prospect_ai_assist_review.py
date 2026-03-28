#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import os
import sqlite3
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
import re
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import seed_recipients_pools as pools
import ai_assist_paths
from outreach import contact_normalization
from outreach import crm_store
from outreach import run_prospect_generation as generation
from outreach.prospect_enrich_email import CORP_SUFFIXES
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
OPTIONAL_COLUMNS = ("seed_id",)
SEED_INDEX_FILENAME = "seed_index.json"
PENDING_REVIEW_FILENAME_RE = re.compile(
    r"^prospect_ai_assist_review_(\d{8})(?:_packet_(\d{3}))?_reviewed\.csv$"
)
BATCH_PACKET_ID_RE = re.compile(r"^(\d{4})-(\d{2})-(\d{2})_AIASSIST(?:_P(\d{3}))?$")
STALE_STARTED_MINUTES = 30
TRACKING_STATUS_STARTED = "started"
TRACKING_STATUS_COMPLETED = "completed"
TRACKING_STATUS_FAILED = "failed"
COMMON_MULTI_LABEL_TLDS = {
    "co.uk",
    "org.uk",
    "gov.uk",
    "ac.uk",
    "com.au",
    "net.au",
    "org.au",
    "com.br",
    "com.mx",
    "co.nz",
    "com.sg",
    "com.hk",
}
STDIN_CSV_FENCE_RE = re.compile(r"^\s*```(?:csv)?\s*\r?\n(?P<body>.*?)(?:\r?\n)?```\s*$", re.IGNORECASE | re.DOTALL)
STDIN_REQUIRED_HEADER = ",".join(REQUIRED_COLUMNS)
STDIN_OPTIONAL_HEADER = STDIN_REQUIRED_HEADER + ",seed_id"
MANUAL_SOURCE = "manual_user_supplied"
MANUAL_TITLE_ALIASES = {"title", "role", "job title", "position"}
MANUAL_FIELD_ALIASES = {
    "company": "firm",
    "company name": "firm",
    "firm": "firm",
    "business": "firm",
    "business name": "firm",
    "contact": "contact_name",
    "contact name": "contact_name",
    "name": "contact_name",
    "person": "contact_name",
    "email": "email",
    "contact email": "email",
    "website": "website",
    "url": "website",
    "domain": "website",
    "site": "website",
    "web": "website",
    "state": "state",
    "title": "title",
    "role": "title",
    "job title": "title",
    "position": "title",
    "note": "notes",
    "notes": "notes",
    "comment": "notes",
    "comments": "notes",
}
MANUAL_ALLOWED_ACTION_REASONS = (
    "created_new_contact",
    "created_second_contact_existing_company",
    "updated_existing_contact",
    "updated_existing_company",
    "staged_missing_email",
    "staged_missing_identity",
    "invalid_state_value",
    "state_out_of_scope",
    "free_domain",
    "suppressed_email",
    "do_not_contact_email",
    "do_not_contact_domain",
    "duplicate_in_batch",
)
EMAIL_TOKEN_RE = re.compile(r"[A-Z0-9.!#$%&'*+/=?^_`{|}~-]+@[A-Z0-9-]+(?:\.[A-Z0-9-]+)+", flags=re.I)
MANUAL_LABEL_RE = re.compile(r"^\s*(?P<label>[A-Za-z][A-Za-z0-9 /_-]{1,40})\s*[:=-]\s*(?P<value>.+?)\s*$")


def _emit(key: str, value: str | int) -> None:
    print(f"{key}={value}")


def _local_now() -> datetime:
    return datetime.now().astimezone()


def _local_today_stamp() -> str:
    return _local_now().date().isoformat()


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _default_batch_id(input_path: Path | None = None) -> str:
    path = input_path or Path("")
    match = PENDING_REVIEW_FILENAME_RE.match(path.name)
    if match:
        token = match.group(1)
        packet_token = str(match.group(2) or "").strip()
        batch_id = f"{token[:4]}-{token[4:6]}-{token[6:8]}_AIASSIST"
        if packet_token:
            return f"{batch_id}_P{packet_token}"
        return batch_id
    return f"{_local_today_stamp()}_AIASSIST"


def _default_manual_batch_id(now: datetime | None = None) -> str:
    stamp = (now or _local_now()).strftime("%Y-%m-%d_AIASSIST_MANUAL_%H%M%S")
    return stamp


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


def _normalize_text(value: Any) -> str:
    return " ".join(str(value or "").replace("\r", " ").replace("\n", " ").split()).strip()


def _manual_row_summary(row: dict[str, str]) -> str:
    parts = [
        _normalize_text(row.get("firm") or ""),
        _normalize_text(row.get("contact_name") or ""),
        _extract_email_token(str(row.get("email") or "")) or _normalize_email(str(row.get("email") or "")),
        _normalize_manual_website(row.get("website") or ""),
        _normalize_text(row.get("state") or ""),
        _normalize_text(row.get("_notes") or ""),
    ]
    return " | ".join([part for part in parts if part]).strip()


def _manual_label_key(value: str) -> str:
    normalized = re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", str(value or "").strip().lower())).strip()
    return MANUAL_FIELD_ALIASES.get(normalized, "")


def _extract_email_token(value: str) -> str:
    match = EMAIL_TOKEN_RE.search(str(value or ""))
    return str(match.group(0) or "").strip().lower() if match else ""


def _normalize_manual_website(value: str) -> str:
    canonical = contact_normalization.canonicalize_http_url(str(value or ""))
    if canonical:
        return canonical
    return contact_normalization.normalize_website(str(value or ""))


def _append_notes(existing: str, value: str) -> str:
    left = _normalize_text(existing)
    right = _normalize_text(value)
    if not right:
        return left
    if not left:
        return right
    if right == left:
        return left
    return f"{left} | {right}"


def _looks_like_person_name(value: str) -> bool:
    text = _normalize_text(value)
    if not text:
        return False
    if "@" in text or "." in text or len(text.split()) < 2 or len(text.split()) > 4:
        return False
    return bool(re.fullmatch(r"[A-Za-z .'-]{3,80}", text))


def _manual_field_segments(value: str) -> list[str]:
    text = str(value or "").replace("\r", "\n")
    if not text.strip():
        return []
    pieces = [part.strip() for part in re.split(r"[,\t|;]+", text) if part.strip()]
    return pieces or [_normalize_text(text)]


def _normalize_firm_key(value: str) -> str:
    text = _normalize_text(value).upper()
    if not text:
        return ""
    tokens = re.split(r"\s+", re.sub(r"[^A-Z0-9 ]", " ", text))
    while tokens and tokens[-1] in CORP_SUFFIXES:
        tokens.pop()
    return "".join(re.sub(r"[^A-Z0-9]", "", token) for token in tokens if token)


def _root_domain(domain: str) -> str:
    host = _normalize_text(domain).lower().strip(".")
    if not host or re.fullmatch(r"\d+\.\d+\.\d+\.\d+", host):
        return host
    parts = [part for part in host.split(".") if part]
    if len(parts) <= 2:
        return ".".join(parts)
    suffix = ".".join(parts[-2:])
    if suffix in COMMON_MULTI_LABEL_TLDS and len(parts) >= 3:
        return ".".join(parts[-3:])
    return ".".join(parts[-2:])


def _active_state_scope_list() -> list[str]:
    return generation._parse_states(os.getenv("OUTREACH_STATES", "")) or list(generation.DEFAULT_STATE_SCOPE_ALL)


def _active_state_scope() -> set[str]:
    return {str(state or "").strip().upper() for state in _active_state_scope_list() if str(state or "").strip()}


def _allow_free_domains() -> bool:
    return generation._bool_env(os.getenv("OUTREACH_ALLOW_FREE_DOMAINS", "0"))


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
        for field in OPTIONAL_COLUMNS:
            if field not in row:
                continue
            raw_value = "" if row.get(field) is None else str(row.get(field))
            normalized_value = raw_value.strip()
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
    return _load_markdown_review_rows_from_text(input_path.read_text(encoding="utf-8-sig"))


def _load_markdown_review_rows_from_text(raw_text: str) -> list[dict[str, str]]:
    lines = str(raw_text or "").splitlines()
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


def _contains_embedded_csv_header(raw_text: str) -> bool:
    saw_nonempty = False
    for line in str(raw_text or "").splitlines():
        candidate = str(line or "").strip().lstrip("\ufeff")
        if not candidate:
            continue
        if candidate in {STDIN_REQUIRED_HEADER, STDIN_OPTIONAL_HEADER}:
            return saw_nonempty
        saw_nonempty = True
    return False


def _load_csv_rows_from_text(raw_text: str) -> list[dict[str, str]]:
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
                return _load_markdown_review_rows_from_text(raw_text)
            clean: dict[str, str] = {}
            for key, value in dict(raw_row).items():
                clean[str(key or "").lstrip("\ufeff")] = "" if value is None else str(value)
            decision = str(clean.get("decision") or "").strip().lower()
            if decision not in {"accept", "reject"}:
                raise ValueError(f"invalid_decision={decision or 'blank'}")
            rows.append(clean)
        return rows


def _extract_csv_candidate_text(raw_text: str) -> str:
    text = str(raw_text or "").lstrip("\ufeff").strip()
    if not text:
        raise ValueError("missing_input")
    fence_match = STDIN_CSV_FENCE_RE.fullmatch(text)
    if fence_match:
        text = str(fence_match.group("body") or "").strip()
    first_nonempty_line = ""
    for line in text.splitlines():
        candidate = str(line or "").strip().lstrip("\ufeff")
        if candidate:
            first_nonempty_line = candidate
            break
    if first_nonempty_line not in {STDIN_REQUIRED_HEADER, STDIN_OPTIONAL_HEADER}:
        raise ValueError("stdin_commentary_detected")
    return text


def _split_manual_blocks(raw_text: str) -> list[str]:
    text = str(raw_text or "").replace("\r\n", "\n").replace("\r", "\n").strip()
    if not text:
        return []
    blocks = [block.strip() for block in re.split(r"\n\s*\n+", text) if block.strip()]
    if len(blocks) > 1:
        return blocks
    nonempty_lines = [str(line or "").strip() for line in text.splitlines() if str(line or "").strip()]
    email_lines = [line for line in nonempty_lines if _extract_email_token(line)]
    if len(email_lines) >= 2 and len(email_lines) == len(nonempty_lines):
        return nonempty_lines
    return [text]


def _parse_manual_block(raw_block: str) -> dict[str, str]:
    parsed: dict[str, str] = {
        "state": "",
        "decision": "accept",
        "firm": "",
        "website": "",
        "contact_name": "",
        "title": "",
        "email": "",
        "source_urls": "",
        "confidence": "100",
        "evidence_snippet": "",
        "_notes": "",
        "_invalid_state": "",
    }
    leftovers: list[str] = []
    for raw_line in str(raw_block or "").splitlines():
        line = _normalize_text(raw_line)
        if not line:
            continue
        match = MANUAL_LABEL_RE.match(raw_line)
        if match:
            field = _manual_label_key(str(match.group("label") or ""))
            value = _normalize_text(str(match.group("value") or ""))
            if field == "state":
                state = _normalize_state(value)
                if state:
                    parsed["state"] = state
                elif value:
                    parsed["_invalid_state"] = value
                continue
            if field == "email":
                parsed["email"] = _extract_email_token(value) or parsed["email"]
                continue
            if field == "website":
                parsed["website"] = _normalize_manual_website(value) or parsed["website"]
                continue
            if field in {"firm", "contact_name", "title"}:
                parsed[field] = value or parsed[field]
                continue
            if field == "notes":
                parsed["_notes"] = _append_notes(parsed["_notes"], value)
                continue

        for segment in _manual_field_segments(line):
            email = _extract_email_token(segment)
            if email and not parsed["email"]:
                parsed["email"] = email
                continue
            website = _normalize_manual_website(segment)
            if website and not parsed["website"]:
                parsed["website"] = website
                continue
            state = _normalize_state(segment)
            if state and not parsed["state"]:
                parsed["state"] = state
                continue
            leftovers.append(_normalize_text(segment))

    leftovers = [item for item in leftovers if item]
    if not parsed["firm"] and leftovers:
        parsed["firm"] = leftovers.pop(0)
    if not parsed["contact_name"]:
        contact_idx = next((idx for idx, value in enumerate(leftovers) if _looks_like_person_name(value)), -1)
        if contact_idx >= 0:
            parsed["contact_name"] = leftovers.pop(contact_idx)
    if not parsed["title"] and leftovers:
        parsed["title"] = leftovers.pop(0)
    for leftover in leftovers:
        parsed["_notes"] = _append_notes(parsed["_notes"], leftover)

    parsed["evidence_snippet"] = parsed["_notes"] or _normalize_text(raw_block)
    return parsed


def _load_manual_rows_from_text(raw_text: str) -> list[dict[str, str]]:
    if _contains_embedded_csv_header(raw_text):
        raise ValueError("stdin_commentary_detected")
    rows: list[dict[str, str]] = []
    for block in _split_manual_blocks(raw_text):
        row = _parse_manual_block(block)
        if any(
            str(row.get(field) or "").strip()
            for field in ("firm", "website", "contact_name", "title", "email", "state", "evidence_snippet")
        ):
            rows.append(
                {
                    key: str(value or "")
                    for key, value in row.items()
                    if (not key.startswith("_")) or key == "_invalid_state"
                }
            )
    if not rows:
        raise ValueError("missing_manual_rows")
    return rows


def _load_input_rows_from_text(raw_text: str) -> tuple[list[dict[str, str]], str]:
    last_error: Exception | None = None
    candidates = [str(raw_text or "").lstrip("\ufeff")]
    try:
        extracted_text = _extract_csv_candidate_text(raw_text)
        if extracted_text not in candidates:
            candidates.append(extracted_text)
    except Exception as exc:
        last_error = exc
    for candidate in candidates:
        try:
            return _load_csv_rows_from_text(candidate), "reviewed_csv"
        except Exception as exc:
            last_error = exc
    return _load_manual_rows_from_text(raw_text), "manual_text"


def _load_input_rows(input_path: Path) -> tuple[list[dict[str, str]], str]:
    with open(input_path, "r", newline="", encoding="utf-8-sig") as handle:
        return _load_input_rows_from_text(handle.read())


def _load_csv_rows(input_path: Path) -> list[dict[str, str]]:
    rows, parse_mode = _load_input_rows(input_path)
    if parse_mode != "reviewed_csv":
        raise ValueError(f"expected_reviewed_csv got={parse_mode}")
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


def _sha256_text(text: str) -> str:
    return hashlib.sha256(str(text or "").encode("utf-8")).hexdigest()


def _discover_pending_review_files(
    review_dirs: list[ai_assist_paths.PathCandidate],
) -> list[tuple[Path, bool]]:
    rows: list[tuple[str, int, int, str, Path, bool]] = []
    for review_dir in review_dirs:
        try:
            if not review_dir.path.exists() or (not review_dir.path.is_dir()):
                continue
        except Exception:
            continue
        for path in review_dir.path.iterdir():
            if not path.is_file():
                continue
            match = PENDING_REVIEW_FILENAME_RE.match(path.name)
            if not match:
                continue
            packet_number = int(match.group(2) or "0")
            rows.append(
                (
                    match.group(1),
                    1 if review_dir.is_legacy else 0,
                    packet_number,
                    path.name.lower(),
                    path.resolve(strict=False),
                    bool(review_dir.is_legacy),
                )
            )
    rows.sort(key=lambda item: (item[0], item[2], item[1], item[3]))
    return [(item[4], item[5]) for item in rows]


def _pending_review_candidates() -> tuple[list[ai_assist_paths.PathCandidate], list[tuple[Path, bool]]]:
    review_dirs = ai_assist_paths.prospect_pending_import_candidates(REPO_ROOT)
    files = _discover_pending_review_files(review_dirs)
    return review_dirs, files


def _seed_index_path_for_review_input(input_path: Path, batch_id: str) -> Path | None:
    match = PENDING_REVIEW_FILENAME_RE.match(input_path.name)
    date_token = ""
    if match:
        date_token = str(match.group(1) or "").strip()
    else:
        batch_match = BATCH_PACKET_ID_RE.match(str(batch_id or "").strip())
        if batch_match:
            date_token = "".join(batch_match.groups()[:3])
    if not date_token:
        return None
    packet_dir = ai_assist_paths.prospect_audit_dir(repo_root=REPO_ROOT) / f"prospect_ai_assist_review_{date_token}_packets"
    seed_index_path = packet_dir / SEED_INDEX_FILENAME
    return seed_index_path if seed_index_path.exists() else None


def _load_seed_index(input_path: Path, batch_id: str) -> dict[str, dict[str, Any]]:
    seed_index_path = _seed_index_path_for_review_input(input_path, batch_id)
    if seed_index_path is None:
        return {}
    try:
        payload = json.loads(seed_index_path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if not isinstance(payload, dict):
        return {}
    rows = payload.get("seeds") if isinstance(payload.get("seeds"), dict) else payload
    if not isinstance(rows, dict):
        return {}
    out: dict[str, dict[str, Any]] = {}
    for seed_id, seed_meta in rows.items():
        if not isinstance(seed_meta, dict):
            continue
        normalized_seed_id = str(seed_id or "").strip()
        if not normalized_seed_id:
            continue
        out[normalized_seed_id] = dict(seed_meta)
    return out


def _json_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item or "").strip() for item in value if str(item or "").strip()]
    if isinstance(value, str):
        text = str(value or "").strip()
        if not text:
            return []
        try:
            decoded = json.loads(text)
        except Exception:
            return [text]
        if isinstance(decoded, list):
            return [str(item or "").strip() for item in decoded if str(item or "").strip()]
    return []


def _seed_provenance_for_row(
    *,
    row: dict[str, str],
    seed_index: dict[str, dict[str, Any]],
) -> dict[str, str]:
    seed_id = str(row.get("seed_id") or "").strip()
    if not seed_id:
        return {
            "seed_id": "",
            "seed_source_token": "",
            "seed_source": "",
            "seed_source_url": "",
            "source_record_id": "",
            "license_number": "",
            "state_lic_license_class_norm": "",
            "state_lic_hard_negative_class": "",
            "state_lic_positive_families_json": "[]",
            "state_lic_negative_families_json": "[]",
            "state_lic_packet_exclusion_reason": "",
        }
    seed_meta = dict(seed_index.get(seed_id) or {})
    return {
        "seed_id": seed_id,
        "seed_source_token": str(seed_meta.get("seed_source_token") or ""),
        "seed_source": str(seed_meta.get("seed_source") or ""),
        "seed_source_url": str(seed_meta.get("seed_source_url") or ""),
        "source_record_id": str(seed_meta.get("source_record_id") or ""),
        "license_number": str(seed_meta.get("license_number") or ""),
        "state_lic_license_class_norm": str(seed_meta.get("state_lic_license_class_norm") or ""),
        "state_lic_hard_negative_class": str(seed_meta.get("state_lic_hard_negative_class") or ""),
        "state_lic_positive_families_json": json.dumps(_json_list(seed_meta.get("state_lic_positive_families"))),
        "state_lic_negative_families_json": json.dumps(_json_list(seed_meta.get("state_lic_negative_families"))),
        "state_lic_packet_exclusion_reason": str(seed_meta.get("state_lic_packet_exclusion_reason") or ""),
    }


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


def _prospect_record_from_row(row: sqlite3.Row) -> dict[str, Any]:
    email = _normalize_email(str(row["email"] or ""))
    website = _normalize_manual_website(str(row["website"] or ""))
    return {
        "prospect_id": str(row["prospect_id"] or "").strip(),
        "firm": _normalize_text(str(row["firm"] or "")),
        "contact_name": _normalize_text(str(row["contact_name"] or "")),
        "email": email,
        "title": _normalize_text(str(row["title"] or "")),
        "city": _normalize_text(str(row["city"] or "")),
        "state": _normalize_state(str(row["state"] or "")),
        "website": website,
        "source": _normalize_text(str(row["source"] or "")),
        "source_fit_tier": _normalize_text(str(row["source_fit_tier"] or "")) or "recoverable_consultant",
        "default_send_eligible": int(row["default_send_eligible"] or 1),
        "email_status": _normalize_text(str(row["email_status"] or "")),
        "enrichment_lane": _normalize_text(str(row["enrichment_lane"] or "")),
        "score": int(row["score"] or 0),
        "status": _normalize_text(str(row["status"] or "")) or "new",
        "created_at": _normalize_text(str(row["created_at"] or "")),
        "last_contacted_at": _normalize_text(str(row["last_contacted_at"] or "")),
        "root_domain": _root_domain(generation._email_domain(email) or _normalize_domain(website)),
        "firm_key": _normalize_firm_key(str(row["firm"] or "")),
    }


def _load_existing_company_maps(conn: sqlite3.Connection | None) -> tuple[dict[str, list[dict[str, Any]]], dict[str, list[dict[str, Any]]]]:
    root_domain_map: dict[str, list[dict[str, Any]]] = defaultdict(list)
    firm_key_map: dict[str, list[dict[str, Any]]] = defaultdict(list)
    if conn is None:
        return root_domain_map, firm_key_map
    rows = conn.execute(
        """
        SELECT prospect_id, firm, contact_name, email, title, city, state, website, source,
               source_fit_tier, default_send_eligible, email_status, enrichment_lane,
               score, status, created_at, last_contacted_at
        FROM prospects
        """
    ).fetchall()
    for row in rows:
        record = _prospect_record_from_row(row)
        if str(record.get("root_domain") or ""):
            root_domain_map[str(record["root_domain"])].append(record)
        if str(record.get("firm_key") or ""):
            firm_key_map[str(record["firm_key"])].append(record)
    return root_domain_map, firm_key_map


def _best_company_match(candidates: list[dict[str, Any]], *, root_domain: str, firm_key: str) -> dict[str, Any] | None:
    if not candidates:
        return None
    for candidate in candidates:
        if root_domain and str(candidate.get("root_domain") or "") == root_domain:
            return candidate
    for candidate in candidates:
        if firm_key and str(candidate.get("firm_key") or "") == firm_key:
            return candidate
    return candidates[0]


def _prefer_company_value(existing: str, incoming: str) -> str:
    current = _normalize_text(existing)
    new_value = _normalize_text(incoming)
    if not new_value:
        return current
    if not current:
        return new_value
    if _normalize_firm_key(current) and _normalize_firm_key(current) == _normalize_firm_key(new_value):
        return current if len(current) >= len(new_value) else new_value
    return current


def _merge_existing_prospect(existing: dict[str, Any], incoming: dict[str, str], *, company_match: bool) -> tuple[dict[str, Any], bool]:
    payload = dict(existing)
    company_updated = False

    incoming_firm = _normalize_text(incoming.get("firm") or "")
    preferred_firm = _prefer_company_value(str(existing.get("firm") or ""), incoming_firm)
    if preferred_firm != str(existing.get("firm") or ""):
        payload["firm"] = preferred_firm
        company_updated = True

    incoming_website = _normalize_manual_website(incoming.get("website") or "")
    if incoming_website and not str(existing.get("website") or "").strip():
        payload["website"] = incoming_website
        company_updated = True

    incoming_state = _normalize_state(incoming.get("state") or "")
    if incoming_state and not str(existing.get("state") or "").strip():
        payload["state"] = incoming_state
        company_updated = True

    if not company_match:
        if _normalize_text(incoming.get("contact_name") or ""):
            payload["contact_name"] = _normalize_text(incoming.get("contact_name") or "")
        if _normalize_text(incoming.get("title") or ""):
            payload["title"] = _normalize_text(incoming.get("title") or "")
        if incoming_website:
            payload["website"] = incoming_website
        if incoming_state:
            payload["state"] = incoming_state
    return payload, company_updated


def _build_manual_prospect_payload(
    *,
    email: str,
    firm: str,
    website: str,
    contact_name: str,
    title: str,
    state: str,
    now_iso: str,
    existing: dict[str, Any] | None = None,
) -> dict[str, Any]:
    existing_row = dict(existing or {})
    status = str(existing_row.get("status") or "").strip().lower() or "new"
    source = str(existing_row.get("source") or "").strip() or MANUAL_SOURCE
    source_fit_tier = str(existing_row.get("source_fit_tier") or "").strip().lower() or "recoverable_consultant"
    default_send_eligible = int(existing_row.get("default_send_eligible") or 1)
    enrichment_lane = str(existing_row.get("enrichment_lane") or "").strip() or "ai_assist"
    return {
        "prospect_id": str(existing_row.get("prospect_id") or _prospect_id_for_email(email)).strip(),
        "firm": _normalize_text(firm),
        "contact_name": _normalize_text(contact_name),
        "email": email,
        "title": _normalize_text(title),
        "city": str(existing_row.get("city") or ""),
        "state": _normalize_state(state) or str(existing_row.get("state") or ""),
        "website": _normalize_manual_website(website) or str(existing_row.get("website") or ""),
        "source": source,
        "source_fit_tier": source_fit_tier,
        "default_send_eligible": default_send_eligible,
        "email_status": str(existing_row.get("email_status") or ""),
        "enrichment_lane": enrichment_lane,
        "score": int(existing_row.get("score") or 0),
        "status": status,
        "created_at": str(existing_row.get("created_at") or now_iso),
        "last_contacted_at": str(existing_row.get("last_contacted_at") or "") or None,
    }


def _write_prospect_row(conn: sqlite3.Connection, payload: dict[str, Any]) -> None:
    conn.execute(
        """
        INSERT INTO prospects(
            prospect_id, firm, contact_name, email, title, city, state, website, source,
            source_fit_tier, default_send_eligible, email_status, enrichment_lane,
            score, status, created_at, last_contacted_at
        ) VALUES (
            :prospect_id, :firm, :contact_name, :email, :title, :city, :state, :website, :source,
            :source_fit_tier, :default_send_eligible, :email_status, :enrichment_lane,
            :score, :status, :created_at, :last_contacted_at
        )
        ON CONFLICT(prospect_id) DO UPDATE SET
            firm = excluded.firm,
            contact_name = excluded.contact_name,
            email = excluded.email,
            title = excluded.title,
            city = excluded.city,
            state = excluded.state,
            website = excluded.website,
            source = excluded.source,
            source_fit_tier = excluded.source_fit_tier,
            default_send_eligible = excluded.default_send_eligible,
            email_status = excluded.email_status,
            enrichment_lane = excluded.enrichment_lane,
            score = excluded.score,
            status = excluded.status,
            last_contacted_at = COALESCE(excluded.last_contacted_at, prospects.last_contacted_at)
        """,
        payload,
    )


def _upsert_company_cache(
    *,
    email_map: dict[str, dict[str, Any]],
    root_domain_map: dict[str, list[dict[str, Any]]],
    firm_key_map: dict[str, list[dict[str, Any]]],
    payload: dict[str, Any],
) -> None:
    record = {
        **dict(payload),
        "email": _normalize_email(str(payload.get("email") or "")),
        "website": _normalize_manual_website(str(payload.get("website") or "")),
        "state": _normalize_state(str(payload.get("state") or "")),
    }
    record["root_domain"] = _root_domain(generation._email_domain(str(record.get("email") or "")) or _normalize_domain(str(record.get("website") or "")))
    record["firm_key"] = _normalize_firm_key(str(record.get("firm") or ""))

    email = str(record.get("email") or "")
    if email:
        email_map[email] = record

    for mapping_name, key_name in ((root_domain_map, "root_domain"), (firm_key_map, "firm_key")):
        key = str(record.get(key_name) or "")
        if not key:
            continue
        existing_rows = list(mapping_name.get(key) or [])
        replaced = False
        for idx, existing_row in enumerate(existing_rows):
            if str(existing_row.get("prospect_id") or "") == str(record.get("prospect_id") or ""):
                existing_rows[idx] = record
                replaced = True
                break
        if not replaced:
            existing_rows.append(record)
        mapping_name[key] = existing_rows


def _upsert_audit_rows(conn: sqlite3.Connection, audit_rows: list[dict[str, str | int]]) -> None:
    if not audit_rows:
        return
    conn.executemany(
        f"""
        INSERT INTO {crm_store.AI_ASSIST_CANDIDATE_TABLE}(
            batch_id, candidate_key, state, decision, firm, website, domain, contact_name, title, email,
            source_urls_json, confidence, evidence_snippet, verification_status, rejection_reason, prospect_id,
            created_at, updated_at, seed_id, seed_source_token, seed_source, seed_source_url, source_record_id,
            license_number, state_lic_license_class_norm, state_lic_hard_negative_class,
            state_lic_positive_families_json, state_lic_negative_families_json, state_lic_packet_exclusion_reason
        ) VALUES (
            :batch_id, :candidate_key, :state, :decision, :firm, :website, :domain, :contact_name, :title, :email,
            :source_urls_json, :confidence, :evidence_snippet, :verification_status, :rejection_reason, :prospect_id,
            :created_at, :updated_at, :seed_id, :seed_source_token, :seed_source, :seed_source_url, :source_record_id,
            :license_number, :state_lic_license_class_norm, :state_lic_hard_negative_class,
            :state_lic_positive_families_json, :state_lic_negative_families_json, :state_lic_packet_exclusion_reason
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
            updated_at = excluded.updated_at,
            seed_id = excluded.seed_id,
            seed_source_token = excluded.seed_source_token,
            seed_source = excluded.seed_source,
            seed_source_url = excluded.seed_source_url,
            source_record_id = excluded.source_record_id,
            license_number = excluded.license_number,
            state_lic_license_class_norm = excluded.state_lic_license_class_norm,
            state_lic_hard_negative_class = excluded.state_lic_hard_negative_class,
            state_lic_positive_families_json = excluded.state_lic_positive_families_json,
            state_lic_negative_families_json = excluded.state_lic_negative_families_json,
            state_lic_packet_exclusion_reason = excluded.state_lic_packet_exclusion_reason
        """,
        audit_rows,
    )


def _load_existing_prospect_map(conn: sqlite3.Connection | None) -> dict[str, dict[str, str]]:
    if conn is None:
        return {}
    rows = conn.execute(
        """
        SELECT prospect_id, firm, contact_name, email, title, city, state, website, source,
               source_fit_tier, default_send_eligible, email_status, enrichment_lane,
               score, status, created_at, last_contacted_at
        FROM prospects
        WHERE email IS NOT NULL AND trim(email) <> ''
        """
    ).fetchall()
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        record = _prospect_record_from_row(row)
        email = _normalize_email(str(record.get("email") or ""))
        if not email:
            continue
        out[email] = record
    return out


def _load_existing_root_domains(conn: sqlite3.Connection | None) -> set[str]:
    if conn is None:
        return set()
    rows = conn.execute(
        """
        SELECT email, website
        FROM prospects
        """
    ).fetchall()
    out: set[str] = set()
    for row in rows:
        email_domain = _normalize_domain(str(row["email"] or ""))
        website_domain = _normalize_domain(str(row["website"] or ""))
        root = _root_domain(email_domain or website_domain)
        if root:
            out.add(root)
    return out


def _load_existing_firm_keys(conn: sqlite3.Connection | None) -> set[str]:
    if conn is None:
        return set()
    rows = conn.execute(
        """
        SELECT firm
        FROM prospects
        WHERE firm IS NOT NULL AND trim(firm) <> ''
        """
    ).fetchall()
    out: set[str] = set()
    for row in rows:
        firm_key = _normalize_firm_key(str(row["firm"] or ""))
        if firm_key:
            out.add(firm_key)
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


def _tracked_batch_state(
    conn: sqlite3.Connection | None,
    *,
    batch_id: str,
    source_hash: str,
) -> tuple[str, sqlite3.Row | None]:
    existing_row = _load_batch_tracking_row(conn, batch_id)
    if existing_row is None:
        return "", None
    existing_hash = str(existing_row["source_file_hash"] or "").strip()
    if existing_hash and existing_hash != source_hash:
        raise ValueError(
            f"{ERR_AI_ASSIST_IMPORT_DRIFT} batch_id={batch_id} "
            f"expected_hash={existing_hash} got_hash={source_hash}"
        )
    return _batch_claim_state(existing_row, _utc_now()), existing_row


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
    claim_state, existing_row = _tracked_batch_state(
        conn,
        batch_id=batch_id,
        source_hash=source_hash,
    )
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
    provenance: dict[str, str] | None = None,
) -> dict[str, str | int]:
    payload: dict[str, str | int] = {
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
    if provenance:
        payload.update(dict(provenance))
    return payload


def _print_totals(*, totals: Counter, per_state: dict[str, Counter], dry_run: bool, final_status: str) -> None:
    _emit("AI_ASSIST_CANDIDATES_TOTAL", int(totals.get("candidates", 0)))
    _emit("AI_ASSIST_ACCEPTED_TOTAL", int(totals.get("accepted", 0)))
    _emit("AI_ASSIST_REJECTED_TOTAL", int(totals.get("rejected", 0)))
    _emit("AI_ASSIST_VERIFIED_TOTAL", int(totals.get("verified", 0)))
    _emit("AI_ASSIST_CREATED_NEW_TOTAL", int(totals.get("created_new_contact", 0)))
    _emit(
        "AI_ASSIST_CREATED_SECOND_CONTACT_TOTAL",
        int(totals.get("created_second_contact_existing_company", 0)),
    )
    _emit("AI_ASSIST_UPDATED_CONTACT_TOTAL", int(totals.get("updated_existing_contact", 0)))
    _emit("AI_ASSIST_UPDATED_COMPANY_TOTAL", int(totals.get("updated_existing_company", 0)))
    _emit("AI_ASSIST_STAGED_TOTAL", int(totals.get("staged", 0)))
    _emit("AI_ASSIST_SKIPPED_DUPLICATE_TOTAL", int(totals.get("skipped_duplicate", 0)))
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
    input_path: Path | None = None,
    batch_id_override: str = "",
    dry_run: bool = False,
    stdin_text: str = "",
) -> tuple[int, str]:
    data_dir_resolution = resolve_data_dir(REPO_ROOT)
    db_path = crm_store.crm_db_path()
    is_stdin = bool(str(stdin_text or ""))
    batch_id = str(batch_id_override or "").strip() or (
        _default_manual_batch_id() if is_stdin else _default_batch_id(input_path)
    )
    tracking_source_path = (
        Path(f"prospect_ai_assist_manual_{batch_id}.csv")
        if is_stdin
        else (input_path or Path("")).expanduser().resolve(strict=False)
    )
    display_input_path = "<stdin>" if is_stdin else str(tracking_source_path)
    active_states = _active_state_scope()

    _emit("AI_ASSIST_BATCH_ID", batch_id)
    _emit("AI_ASSIST_IMPORT_INPUT_MODE", "stdin" if is_stdin else "file")
    _emit("AI_ASSIST_IMPORT_INPUT_PATH", display_input_path)
    _emit("AI_ASSIST_IMPORT_DATA_DIR", str(data_dir_resolution.effective_path))
    _emit("AI_ASSIST_IMPORT_DATA_DIR_SOURCE", str(data_dir_resolution.source or "default"))
    _emit("AI_ASSIST_IMPORT_CRM_DB", str(db_path))
    _emit("AI_ASSIST_IMPORT_EXPECTED_COLUMNS", ",".join(REQUIRED_COLUMNS))
    _emit("AI_ASSIST_IMPORT_ACTIVE_STATES", ",".join(_active_state_scope_list()))
    _emit("AI_ASSIST_IMPORT_ALLOW_FREE_DOMAINS", 1 if _allow_free_domains() else 0)

    if (not is_stdin) and (not tracking_source_path.exists()):
        print(f"{ERR_AI_ASSIST_IMPORT_INPUT} detail=missing_input path={tracking_source_path}", file=sys.stderr)
        return 2, "MISSING_INPUT"

    source_hash = ""
    raw_input_text = str(stdin_text or "")
    if is_stdin:
        if not raw_input_text.strip():
            print(f"{ERR_AI_ASSIST_IMPORT_INPUT} detail=missing_stdin", file=sys.stderr)
            return 2, "INVALID_INPUT"
        source_hash = _sha256_text(raw_input_text)
        _emit("AI_ASSIST_IMPORT_STDIN_BYTES", len(raw_input_text.encode("utf-8")))
    else:
        source_hash = _sha256_file(tracking_source_path)
    _emit("AI_ASSIST_IMPORT_SOURCE_FILE_HASH", source_hash)

    conn: sqlite3.Connection | None = None
    claim_state = "dry_run"
    final_status = "OK"
    batch_failure_detail = ""
    try:
        if db_path.exists() or not dry_run:
            if not dry_run:
                crm_store.ensure_database(db_path)
            conn = crm_store.connect(db_path)
            crm_store.init_schema(conn)

        if conn is not None:
            try:
                tracked_state, _existing_tracking_row = _tracked_batch_state(
                    conn,
                    batch_id=batch_id,
                    source_hash=source_hash,
                )
            except ValueError as exc:
                print(str(exc), file=sys.stderr)
                return 2, "DRIFT"
            if tracked_state in {"skip_completed", "skip_in_progress"}:
                _emit("AI_ASSIST_IMPORT_NORMALIZED_ROWS", 0)
                _emit("AI_ASSIST_IMPORT_NORMALIZED_FIELDS", 0)
                _emit("AI_ASSIST_IMPORT_BATCH_STATE", tracked_state)
                if tracked_state == "skip_completed":
                    print(f"{PASS_AI_ASSIST_IMPORT} status=SKIPPED_ALREADY_COMPLETED")
                    return 0, "SKIPPED_ALREADY_COMPLETED"
                print(f"{PASS_AI_ASSIST_IMPORT} status=SKIPPED_IN_PROGRESS")
                return 0, "SKIPPED_IN_PROGRESS"

        try:
            rows, parse_mode = _load_input_rows_from_text(raw_input_text) if is_stdin else _load_input_rows(tracking_source_path)
        except Exception as exc:
            print(f"{ERR_AI_ASSIST_IMPORT_INPUT} detail={exc}", file=sys.stderr)
            return 2, "INVALID_INPUT"
        try:
            if parse_mode == "reviewed_csv":
                rows, normalized_rows_total, normalized_fields_total = _normalize_review_rows(rows)
            else:
                normalized_rows_total = len(rows)
                normalized_fields_total = 0
        except Exception as exc:
            print(f"{ERR_AI_ASSIST_IMPORT_INPUT} detail={exc}", file=sys.stderr)
            return 2, "INVALID_INPUT"
        seed_index = {} if (is_stdin or parse_mode != "reviewed_csv") else _load_seed_index(tracking_source_path, batch_id)

        now_iso = crm_store.utc_now_iso()
        _emit("AI_ASSIST_IMPORT_PARSE_MODE", parse_mode)
        _emit("AI_ASSIST_IMPORT_NORMALIZED_ROWS", normalized_rows_total)
        _emit("AI_ASSIST_IMPORT_NORMALIZED_FIELDS", normalized_fields_total)
        _emit("AI_ASSIST_IMPORT_SEED_INDEX_ROWS", len(seed_index))

        if not dry_run and conn is not None:
            if parse_mode == "reviewed_csv":
                legacy_totals = _legacy_completed_batch_totals(conn, batch_id, rows)
                if _load_batch_tracking_row(conn, batch_id) is None and legacy_totals is not None:
                    _create_completed_batch_tracking(
                        conn,
                        batch_id=batch_id,
                        source_path=tracking_source_path,
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
                    source_path=tracking_source_path,
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
        root_domain_map, firm_key_map = _load_existing_company_maps(conn)
        do_not_contact_emails, do_not_contact_domains = _load_do_not_contact_sets(conn)

        candidate_rows: list[dict[str, object]] = []
        totals: Counter = Counter()
        per_state: dict[str, Counter] = defaultdict(Counter)
        batch_seen_emails: set[str] = set()
        allow_free_domains = _allow_free_domains()

        for row in rows:
            state = _normalize_state(str(row.get("state") or ""))
            decision = str(row.get("decision") or "").strip().lower()
            firm = _normalize_text(row.get("firm") or "")
            website = _normalize_manual_website(row.get("website") or "")
            contact_name = _normalize_text(row.get("contact_name") or "")
            title = _normalize_text(row.get("title") or "")
            email = _extract_email_token(str(row.get("email") or "")) or _normalize_email(str(row.get("email") or ""))
            email_domain = generation._email_domain(email)
            website_domain = _normalize_domain(website)
            domain = email_domain or website_domain
            candidate_key = _candidate_key(row, email=email, domain=domain, state=state)
            source_urls = _parse_source_urls(str(row.get("source_urls") or ""))
            confidence = _coerce_confidence(str(row.get("confidence") or ""))
            evidence_snippet = _normalize_text(str(row.get("evidence_snippet") or "")) or _manual_row_summary(row)
            invalid_state_value = _normalize_text(str(row.get("_invalid_state") or ""))
            root_domain = _root_domain(domain)
            firm_key = _normalize_firm_key(firm)
            provenance = _seed_provenance_for_row(row=row, seed_index=seed_index)
            candidate: dict[str, object] = {
                "decision": decision,
                "state": state,
                "email": email,
                "website": website,
                "domain": domain,
                "candidate_key": candidate_key,
                "prospect_id": "",
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
                    provenance=provenance,
                ),
                "final_verification_status": "",
                "final_rejection_reason": "",
                "upsert_payload": None,
                "company_update_payload": None,
                "company_update_requested": False,
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
            verification_status = ""
            if invalid_state_value and not state:
                verification_status = "rejected_by_verification"
                rejection_reason = "invalid_state_value"
            elif state and state not in active_states:
                verification_status = "rejected_by_verification"
                rejection_reason = "state_out_of_scope"
            elif not email:
                verification_status = "staged_incomplete"
                rejection_reason = "staged_missing_email"
                totals["staged"] += 1
            elif not generation._valid_email(email):
                verification_status = "rejected_by_verification"
                rejection_reason = "invalid_email"
            elif (not allow_free_domains) and email_domain in pools.FREE_EMAIL_DOMAINS:
                verification_status = "rejected_by_verification"
                rejection_reason = "free_domain"
            elif email in suppressed_emails:
                verification_status = "rejected_by_verification"
                rejection_reason = "suppressed_email"
            elif email in do_not_contact_emails:
                verification_status = "rejected_by_verification"
                rejection_reason = "do_not_contact_email"
            elif domain and domain in do_not_contact_domains:
                verification_status = "rejected_by_verification"
                rejection_reason = "do_not_contact_domain"
            elif email in batch_seen_emails:
                verification_status = "rejected_by_verification"
                rejection_reason = "duplicate_in_batch"
                totals["skipped_duplicate"] += 1
            elif not firm and not domain:
                verification_status = "staged_incomplete"
                rejection_reason = "staged_missing_identity"
                totals["staged"] += 1

            if verification_status:
                candidate["final_verification_status"] = verification_status
                candidate["final_rejection_reason"] = rejection_reason
                candidate_rows.append(candidate)
                continue

            batch_seen_emails.add(email)
            incoming_fields = {
                "firm": firm,
                "website": website,
                "state": state,
                "contact_name": contact_name,
                "title": title,
                "email": email,
            }
            existing_row = dict(existing_prospects.get(email) or {})
            if existing_row:
                merged_existing, company_updated = _merge_existing_prospect(
                    existing_row,
                    incoming_fields,
                    company_match=False,
                )
                payload = _build_manual_prospect_payload(
                    email=email,
                    firm=str(merged_existing.get("firm") or ""),
                    website=str(merged_existing.get("website") or ""),
                    contact_name=str(merged_existing.get("contact_name") or ""),
                    title=str(merged_existing.get("title") or ""),
                    state=str(merged_existing.get("state") or ""),
                    now_iso=now_iso,
                    existing=merged_existing,
                )
                candidate["prospect_id"] = str(payload["prospect_id"] or "")
                candidate["upsert_payload"] = payload
                candidate["company_update_requested"] = bool(company_updated)
                candidate["final_rejection_reason"] = "updated_existing_contact"
                candidate_rows.append(candidate)
                continue

            company_candidates: list[dict[str, Any]] = []
            seen_company_ids: set[str] = set()
            for pool in (list(root_domain_map.get(root_domain) or []), list(firm_key_map.get(firm_key) or [])):
                for item in pool:
                    prospect_id = str(item.get("prospect_id") or "")
                    if prospect_id and prospect_id not in seen_company_ids:
                        seen_company_ids.add(prospect_id)
                        company_candidates.append(dict(item))
            matched_company = _best_company_match(company_candidates, root_domain=root_domain, firm_key=firm_key)
            if matched_company:
                company_template, company_updated = _merge_existing_prospect(
                    matched_company,
                    incoming_fields,
                    company_match=True,
                )
                if company_updated:
                    candidate["company_update_requested"] = True
                    candidate["company_update_payload"] = _build_manual_prospect_payload(
                        email=str(matched_company.get("email") or ""),
                        firm=str(company_template.get("firm") or ""),
                        website=str(company_template.get("website") or ""),
                        contact_name=str(matched_company.get("contact_name") or ""),
                        title=str(matched_company.get("title") or ""),
                        state=str(company_template.get("state") or ""),
                        now_iso=now_iso,
                        existing=company_template,
                    )
                payload = _build_manual_prospect_payload(
                    email=email,
                    firm=str(company_template.get("firm") or firm),
                    website=str(company_template.get("website") or website),
                    contact_name=contact_name,
                    title=title,
                    state=state or str(company_template.get("state") or ""),
                    now_iso=now_iso,
                )
                candidate["prospect_id"] = str(payload["prospect_id"] or "")
                candidate["upsert_payload"] = payload
                candidate["final_rejection_reason"] = "created_second_contact_existing_company"
                candidate_rows.append(candidate)
                continue

            payload = _build_manual_prospect_payload(
                email=email,
                firm=firm,
                website=website,
                contact_name=contact_name,
                title=title,
                state=state,
                now_iso=now_iso,
            )
            candidate["prospect_id"] = str(payload["prospect_id"] or "")
            candidate["upsert_payload"] = payload
            candidate["final_rejection_reason"] = "created_new_contact"
            candidate_rows.append(candidate)

        if dry_run:
            for candidate in candidate_rows:
                payload = dict(candidate.get("upsert_payload") or {})
                if not payload:
                    continue
                action = str(candidate.get("final_rejection_reason") or "")
                totals["verified"] += 1
                per_state[str(candidate.get("state") or "")]["verified"] += 1
                candidate["final_verification_status"] = "verified"
                if action in MANUAL_ALLOWED_ACTION_REASONS:
                    totals[action] += 1
                if bool(candidate.get("company_update_requested")):
                    totals["updated_existing_company"] += 1
            _print_totals(totals=totals, per_state=per_state, dry_run=True, final_status="DRY_RUN")
            return 0, "DRY_RUN"

        for candidate in candidate_rows:
            payload = dict(candidate.get("upsert_payload") or {})
            if not payload:
                continue
            try:
                company_update_payload = dict(candidate.get("company_update_payload") or {})
                if company_update_payload:
                    _write_prospect_row(conn, company_update_payload)
                    _upsert_company_cache(
                        email_map=existing_prospects,
                        root_domain_map=root_domain_map,
                        firm_key_map=firm_key_map,
                        payload=company_update_payload,
                    )
                _write_prospect_row(conn, payload)
                _upsert_company_cache(
                    email_map=existing_prospects,
                    root_domain_map=root_domain_map,
                    firm_key_map=firm_key_map,
                    payload=payload,
                )
            except sqlite3.IntegrityError:
                candidate["final_verification_status"] = "rejected_by_verification"
                candidate["final_rejection_reason"] = "duplicate_in_batch"
                totals["skipped_duplicate"] += 1
                continue

            action = str(candidate.get("final_rejection_reason") or "")
            candidate["final_verification_status"] = "verified"
            totals["verified"] += 1
            per_state[str(candidate.get("state") or "")]["verified"] += 1
            if action in MANUAL_ALLOWED_ACTION_REASONS:
                totals[action] += 1
            if bool(candidate.get("company_update_requested")):
                totals["updated_existing_company"] += 1

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

        try:
            _upsert_audit_rows(conn, audit_rows)
            conn.commit()
        except Exception as exc:
            final_status = "FAILED"
            batch_failure_detail = str(exc)

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
            return 1, final_status
        return 0, final_status
    finally:
        if conn is not None:
            conn.close()


def run_pending_imports(*, dry_run: bool = False) -> int:
    data_dir_resolution = resolve_data_dir(REPO_ROOT)
    review_dirs, files = _pending_review_candidates()
    _emit("AI_ASSIST_PENDING_IMPORT_DATA_DIR", str(data_dir_resolution.effective_path))
    _emit("AI_ASSIST_PENDING_IMPORT_DATA_DIR_SOURCE", str(data_dir_resolution.source or "default"))
    if review_dirs:
        _emit("AI_ASSIST_PENDING_IMPORT_DIR", str(review_dirs[0].path))
    _emit("AI_ASSIST_PENDING_IMPORT_DIRS", ";".join(str(candidate.path) for candidate in review_dirs) or "none")
    _emit("AI_ASSIST_PENDING_IMPORT_DISCOVERED_TOTAL", len(files))
    _emit("AI_ASSIST_PENDING_IMPORT_DRY_RUN", 1 if dry_run else 0)

    imported_batches = 0
    skipped_batches = 0
    for input_path, used_legacy_dir in files:
        batch_id = _default_batch_id(input_path)
        _emit("AI_ASSIST_PENDING_IMPORT_FILE", str(input_path))
        _emit("AI_ASSIST_PENDING_IMPORT_BATCH_ID", batch_id)
        if used_legacy_dir or ai_assist_paths.prospect_path_uses_legacy_dir(input_path, REPO_ROOT):
            _emit("WARN_AI_ASSIST_PENDING_IMPORT_LEGACY_DIR", str(input_path))
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
    ap.add_argument("--stdin", action="store_true", help="Read reviewed AI-assist CSV from stdin.")
    ap.add_argument("--batch", default="", help="Optional batch id override.")
    ap.add_argument(
        "--pending",
        action="store_true",
        help="Import pending reviewed files from DATA_DIR\\imports\\prospect_ai_assist.",
    )
    ap.add_argument("--print-config", action="store_true", help="Print resolved config and exit.")
    ap.add_argument("--dry-run", action="store_true", help="Validate and report without mutating CRM.")
    return ap


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    input_text = str(args.input or "").strip()
    selected_modes = int(bool(args.pending)) + int(bool(input_text)) + int(bool(args.stdin))
    if selected_modes > 1:
        print(f"{ERR_AI_ASSIST_IMPORT_CONFIG} detail=modes_mutually_exclusive", file=sys.stderr)
        return 2

    data_dir_resolution = resolve_data_dir(REPO_ROOT)
    db_path = crm_store.crm_db_path()
    _emit("AI_ASSIST_IMPORT_DATA_DIR", str(data_dir_resolution.effective_path))
    _emit("AI_ASSIST_IMPORT_DATA_DIR_SOURCE", str(data_dir_resolution.source or "default"))
    _emit("AI_ASSIST_IMPORT_CRM_DB", str(db_path))
    _emit("AI_ASSIST_IMPORT_EXPECTED_COLUMNS", ",".join(REQUIRED_COLUMNS))

    if args.pending:
        review_dirs, files = _pending_review_candidates()
        if review_dirs:
            _emit("AI_ASSIST_PENDING_IMPORT_DIR", str(review_dirs[0].path))
        _emit("AI_ASSIST_PENDING_IMPORT_DIRS", ";".join(str(candidate.path) for candidate in review_dirs) or "none")
        _emit("AI_ASSIST_PENDING_IMPORT_DISCOVERED_TOTAL", len(files))
        for idx, item in enumerate(files, start=1):
            path, _used_legacy_dir = item
            _emit(f"AI_ASSIST_PENDING_IMPORT_FILE_{idx}", str(path))
        if args.print_config:
            _emit("AI_ASSIST_IMPORT_DRY_RUN", 1 if args.dry_run else 0)
            return 0
        return run_pending_imports(dry_run=bool(args.dry_run))

    if args.print_config:
        batch_id = str(args.batch or "").strip()
        if not batch_id:
            if args.stdin:
                batch_id = _default_manual_batch_id()
            elif input_text:
                batch_id = _default_batch_id(Path(input_text))
            else:
                batch_id = _default_batch_id()
        _emit("AI_ASSIST_BATCH_ID", batch_id)
        _emit("AI_ASSIST_IMPORT_INPUT_MODE", "stdin" if args.stdin else ("file" if input_text else ""))
        _emit("AI_ASSIST_IMPORT_ACTIVE_STATES", ",".join(_active_state_scope_list()))
        _emit("AI_ASSIST_IMPORT_INPUT_PATH", "<stdin>" if args.stdin else (str(Path(input_text).expanduser().resolve(strict=False)) if input_text else ""))
        _emit("AI_ASSIST_IMPORT_DRY_RUN", 1 if args.dry_run else 0)
        return 0
    if args.stdin:
        rc, _status = _import_review_file(
            input_path=None,
            batch_id_override=str(args.batch or "").strip(),
            dry_run=bool(args.dry_run),
            stdin_text=sys.stdin.read(),
        )
        return rc
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
