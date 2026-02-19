#!/usr/bin/env python3
"""
Import bounce outcomes from an IMAP mailbox into outreach CRM.

This importer supports:
1) direct DSN/bounce messages
2) Zoho moderation notifications that contain forwarded DSN blocks

It is intentionally read-only on IMAP state and uses a DATA_DIR-aware UID state
file for idempotency.
"""

from __future__ import annotations

import argparse
import contextlib
import csv
import email
import imaplib
import json
import os
import re
import sqlite3
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from email.header import decode_header
from email.utils import parseaddr
from pathlib import Path
from typing import Any

try:  # pragma: no cover
    from dotenv import load_dotenv

    load_dotenv()
except Exception:  # pragma: no cover
    pass


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from outreach import crm_store


ERR_BOUNCE_IMAP_CONFIG_MISSING = "ERR_BOUNCE_IMAP_CONFIG_MISSING"
ERR_BOUNCE_IMAP_CONNECT = "ERR_BOUNCE_IMAP_CONNECT"
ERR_BOUNCE_IMAP_AUTH = "ERR_BOUNCE_IMAP_AUTH"
ERR_BOUNCE_IMPORT_LOCKED = "ERR_BOUNCE_IMPORT_LOCKED"
ERR_BOUNCE_IMPORT_CRM = "ERR_BOUNCE_IMPORT_CRM"
ERR_BOUNCE_IMPORT_PARSE = "ERR_BOUNCE_IMPORT_PARSE"

PASS_BOUNCE_IMPORT_PRINT_CONFIG = "PASS_BOUNCE_IMPORT_PRINT_CONFIG"
PASS_BOUNCE_IMPORT_DRY_RUN = "PASS_BOUNCE_IMPORT_DRY_RUN"
PASS_BOUNCE_IMPORT_APPLY = "PASS_BOUNCE_IMPORT_APPLY"

WARN_BOUNCE_UIDVALIDITY_CHANGED = "WARN_BOUNCE_UIDVALIDITY_CHANGED"

MODERATION_SUBJECT_PREFIX = "email held for moderation -"
DEFAULT_MAX_MESSAGES = 400

DSN_MARKERS = (
    "final-recipient:",
    "status:",
    "diagnostic-code:",
    "delivery status notification",
    "undelivered",
    "returned mail",
    "mailer-daemon",
    "postmaster",
    "user unknown",
    "invalid recipient",
)

HARD_BOUNCE_HINTS = (
    "user unknown",
    "invalid recipient",
    "mailbox unavailable",
    "mailbox not found",
    "does not exist",
    "recipient address rejected",
)

DEFAULT_SUPPRESSION_FIELDS = ["email", "reason", "source", "timestamp", "evidence_msg_id"]


@dataclass(frozen=True)
class BounceParse:
    recipient_email: str
    bounce_class: str
    smtp_status: str
    smtp_code: str
    diagnostic_code: str
    final_recipient: str
    original_to: str
    source: str
    subject: str
    source_message_id: str


def _norm_email(value: str) -> str:
    return (value or "").strip().lower()


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _compact(value: object, max_len: int = 220) -> str:
    text = " ".join(str(value or "").replace("\r", " ").replace("\n", " ").split())
    if len(text) <= max_len:
        return text
    return text[:max_len] + "..."


def _safe_int(raw: object, default: int = 0) -> int:
    try:
        return int(str(raw).strip())
    except Exception:
        return int(default)


def _state_path(data_dir: Path) -> Path:
    return data_dir / "bounce_import_state.json"


def _lock_path(data_dir: Path) -> Path:
    return data_dir / "bounce_import.lock"


def _suppression_csv_path(data_dir: Path) -> Path:
    return data_dir / "suppression.csv"


def _load_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)
    except Exception:
        return {}
    if isinstance(payload, dict):
        return payload
    return {}


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with open(tmp_path, "w", encoding="utf-8", newline="\n") as f:
        json.dump(payload, f, ensure_ascii=True, indent=2, sort_keys=True)
        f.write("\n")
    os.replace(tmp_path, path)


@contextlib.contextmanager
def _acquire_lock(lock_file: Path):
    lock_file.parent.mkdir(parents=True, exist_ok=True)
    fd = None
    try:
        fd = os.open(str(lock_file), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.write(fd, f"pid={os.getpid()} ts={_utc_now_iso()}".encode("utf-8", errors="ignore"))
        yield
    except FileExistsError as exc:
        raise RuntimeError(f"{ERR_BOUNCE_IMPORT_LOCKED} path={lock_file.resolve()}") from exc
    finally:
        if fd is not None:
            try:
                os.close(fd)
            except Exception:
                pass
            try:
                lock_file.unlink(missing_ok=True)
            except Exception:
                pass


def _decode_header_value(value: str) -> str:
    if not value:
        return ""
    parts = decode_header(value)
    decoded: list[str] = []
    for part, enc in parts:
        if isinstance(part, bytes):
            try:
                decoded.append(part.decode(enc or "utf-8", errors="replace"))
            except Exception:
                decoded.append(part.decode("utf-8", errors="replace"))
        else:
            decoded.append(str(part))
    return "".join(decoded).strip()


def _extract_plain_body(msg: email.message.Message) -> str:
    if msg.is_multipart():
        for part in msg.walk():
            ctype = (part.get_content_type() or "").lower()
            disp = (part.get("Content-Disposition") or "").lower()
            if ctype == "text/plain" and "attachment" not in disp:
                try:
                    return str(part.get_content())
                except Exception:
                    raw = part.get_payload(decode=True)
                    if isinstance(raw, bytes):
                        return raw.decode(errors="replace")
        for part in msg.walk():
            ctype = (part.get_content_type() or "").lower()
            disp = (part.get("Content-Disposition") or "").lower()
            if ctype == "text/html" and "attachment" not in disp:
                try:
                    html_body = str(part.get_content())
                except Exception:
                    raw = part.get_payload(decode=True)
                    html_body = raw.decode(errors="replace") if isinstance(raw, bytes) else str(raw or "")
                return re.sub(r"<[^>]+>", " ", html_body)
        return ""
    try:
        return str(msg.get_content())
    except Exception:
        raw = msg.get_payload(decode=True)
        if isinstance(raw, bytes):
            return raw.decode(errors="replace")
    return ""


def _header_text(msg: email.message.Message) -> str:
    lines = []
    for k, v in msg.items():
        lines.append(f"{k}: {_decode_header_value(v)}")
    return "\n".join(lines)


def _extract_uidvalidity(conn: imaplib.IMAP4_SSL) -> str:
    try:
        resp = conn.response("UIDVALIDITY")
    except Exception:
        return ""
    if not resp or len(resp) < 2:
        return ""
    items = resp[1] or []
    for item in items:
        text = item.decode(errors="ignore") if isinstance(item, bytes) else str(item)
        match = re.search(r"(\d+)", text)
        if match:
            return match.group(1)
    return ""


def _imap_connect(cfg: dict[str, Any]) -> imaplib.IMAP4_SSL:
    host = str(cfg["imap_host"])
    port = int(cfg["imap_port"])
    user = str(cfg["imap_user"])
    password = str(cfg["imap_pass"])

    try:
        conn = imaplib.IMAP4_SSL(host, port)
    except Exception as exc:
        raise RuntimeError(f"{ERR_BOUNCE_IMAP_CONNECT} host={host} port={port} err={type(exc).__name__}") from exc

    try:
        conn.login(user, password)
    except imaplib.IMAP4.error as exc:
        try:
            conn.logout()
        except Exception:
            pass
        raise RuntimeError(f"{ERR_BOUNCE_IMAP_AUTH} user={user} err={_compact(exc)}") from exc
    except Exception as exc:
        try:
            conn.logout()
        except Exception:
            pass
        raise RuntimeError(f"{ERR_BOUNCE_IMAP_AUTH} user={user} err={type(exc).__name__}") from exc
    return conn


def _search_uids(conn: imaplib.IMAP4_SSL, start_uid: int, max_messages: int) -> list[int]:
    criterion = f"UID {max(1, int(start_uid))}:*"
    typ, data = conn.uid("search", None, criterion)
    if typ != "OK" or not data:
        return []
    raw = data[0].decode(errors="ignore") if isinstance(data[0], bytes) else str(data[0] or "")
    values = [v for v in raw.strip().split() if v]
    uids = sorted({_safe_int(v, 0) for v in values if _safe_int(v, 0) > 0})
    if max_messages > 0:
        return uids[:max_messages]
    return uids


def _fetch_uid_message(conn: imaplib.IMAP4_SSL, uid: int) -> email.message.Message:
    typ, data = conn.uid("fetch", str(uid), "(RFC822)")
    if typ != "OK":
        raise RuntimeError(f"fetch_failed uid={uid}")
    raw: bytes | None = None
    for part in data or []:
        if isinstance(part, tuple) and len(part) >= 2 and isinstance(part[1], bytes):
            raw = part[1]
            break
    if raw is None:
        raise RuntimeError(f"fetch_empty uid={uid}")
    return email.message_from_bytes(raw, policy=email.policy.default)


def _looks_like_candidate(subject: str, sender: str, headers_text: str, body_text: str) -> bool:
    subject_lower = (subject or "").strip().lower()
    if subject_lower.startswith(MODERATION_SUBJECT_PREFIX):
        return True
    text = f"{subject}\n{sender}\n{headers_text}\n{body_text}".lower()
    return any(marker in text for marker in DSN_MARKERS)


def _extract_original_to(text: str) -> str:
    fallback = ""
    in_forward = False
    for line in text.splitlines():
        raw = line.strip()
        if not raw:
            continue
        low = raw.lower()
        if "forwarded message" in low or low.startswith("begin forwarded message") or low.startswith("original message"):
            in_forward = True
            continue
        if not low.startswith("to:"):
            continue
        target = raw.split(":", 1)[1].strip()
        parsed = parseaddr(target)[1]
        email_addr = _norm_email(parsed or target.strip("<>"))
        if "@" not in email_addr:
            continue
        if in_forward:
            return email_addr
        if not fallback:
            fallback = email_addr
    return fallback


def _extract_final_recipient(text: str) -> str:
    patterns = [
        r"Final-Recipient:\s*(?:[^;\r\n]+;\s*)?([^\s<>\r\n;]+@[^\s<>\r\n;]+)",
        r"Original-Recipient:\s*(?:[^;\r\n]+;\s*)?([^\s<>\r\n;]+@[^\s<>\r\n;]+)",
        r"rfc822;\s*([^\s<>\r\n;]+@[^\s<>\r\n;]+)",
        r"<([^>]+@[^>]+)>\s*was not found",
        r"User\s+([^\s<>\r\n;]+@[^\s<>\r\n;]+)\s+not found",
        r"recipient\s+address\s+rejected:\s*([^\s<>\r\n;]+@[^\s<>\r\n;]+)",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if not match:
            continue
        email_addr = _norm_email(match.group(1))
        if "@" in email_addr and "mailer-daemon" not in email_addr and "postmaster" not in email_addr:
            return email_addr
    return ""


def _extract_status_and_diagnostic(text: str) -> tuple[str, str, str]:
    unfolded = re.sub(r"\r?\n[ \t]+", " ", text)
    status = ""
    smtp_code = ""
    diagnostic = ""

    match_status = re.search(r"(?im)^\s*Status:\s*([245]\.\d+\.\d+)\s*$", text)
    if not match_status:
        match_status = re.search(r"(?i)\bStatus:\s*([245]\.\d+\.\d+)", unfolded)
    if match_status:
        status = match_status.group(1).strip()

    match_diag = re.search(r"(?i)Diagnostic-Code:\s*([^\r\n]+)", unfolded)
    if match_diag:
        diagnostic = match_diag.group(1).strip()

    code_source = diagnostic or unfolded
    match_code = re.search(r"\b([245]\d{2})\b", code_source)
    if match_code:
        smtp_code = match_code.group(1)

    return status, smtp_code, diagnostic


def _classify_bounce(status: str, smtp_code: str, diagnostic: str, text: str) -> str:
    status_norm = (status or "").strip()
    code_norm = (smtp_code or "").strip()
    dsn_text = f"{diagnostic}\n{text}".lower()

    if status_norm.startswith("5") or code_norm.startswith("5"):
        return "hard"
    if status_norm.startswith("4") or code_norm.startswith("4"):
        return "soft"
    if any(token in dsn_text for token in HARD_BOUNCE_HINTS):
        return "hard"
    return "soft"


def _parse_bounce(subject: str, sender: str, headers_text: str, body_text: str, message_id: str) -> BounceParse | None:
    if not _looks_like_candidate(subject, sender, headers_text, body_text):
        return None

    combined = f"{headers_text}\n{body_text}"
    final_recipient = _extract_final_recipient(combined)
    if not final_recipient:
        return None

    status, smtp_code, diagnostic = _extract_status_and_diagnostic(combined)
    bounce_class = _classify_bounce(status, smtp_code, diagnostic, combined)
    source = "moderation" if (subject or "").strip().lower().startswith(MODERATION_SUBJECT_PREFIX) else "dsn"
    original_to = _extract_original_to(combined)
    return BounceParse(
        recipient_email=final_recipient,
        bounce_class=bounce_class,
        smtp_status=status,
        smtp_code=smtp_code,
        diagnostic_code=diagnostic,
        final_recipient=final_recipient,
        original_to=original_to,
        source=source,
        subject=(subject or "").strip(),
        source_message_id=(message_id or "").strip(),
    )


def _load_suppression_index(path: Path) -> tuple[set[str], list[str]]:
    if not path.exists():
        return set(), list(DEFAULT_SUPPRESSION_FIELDS)
    emails: set[str] = set()
    with open(path, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = list(reader.fieldnames or DEFAULT_SUPPRESSION_FIELDS)
        for row in reader:
            email_addr = _norm_email(row.get("email", ""))
            if email_addr:
                emails.add(email_addr)
    return emails, fieldnames


def _append_suppression_csv(
    path: Path,
    fieldnames: list[str],
    recipient_email: str,
    reason: str,
    source: str,
    timestamp: str,
    evidence_msg_id: str,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not path.exists()
    row_values = {
        "email": recipient_email,
        "reason": reason,
        "source": source,
        "timestamp": timestamp,
        "evidence_msg_id": evidence_msg_id,
    }
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        out_row: dict[str, str] = {}
        for key in fieldnames:
            out_row[key] = row_values.get(key.lower(), "")
        writer.writerow(out_row)


def _find_prospect_id(conn: sqlite3.Connection, recipient_email: str) -> str:
    row = conn.execute(
        "SELECT prospect_id FROM prospects WHERE lower(email) = ? LIMIT 1",
        (_norm_email(recipient_email),),
    ).fetchone()
    if not row:
        return ""
    return str(row[0] or "").strip()


def _insert_bounce_event(
    conn: sqlite3.Connection,
    created_at_utc: str,
    parsed: BounceParse,
    source_uid_fingerprint: str,
    metadata_json: str,
    prospect_id: str,
) -> bool:
    msg_id = (parsed.source_message_id or "").strip()
    if msg_id:
        dedupe_row = conn.execute(
            """
            SELECT 1
            FROM bounce_events
            WHERE source_message_id = ?
              AND recipient_email = ?
              AND bounce_class = ?
              AND smtp_status = ?
              AND smtp_code = ?
            LIMIT 1
            """,
            (msg_id, parsed.recipient_email, parsed.bounce_class, parsed.smtp_status, parsed.smtp_code),
        ).fetchone()
        if dedupe_row:
            return False

    conn.execute(
        """
        INSERT INTO bounce_events(
            created_at_utc,
            recipient_email,
            bounce_class,
            smtp_status,
            smtp_code,
            diagnostic_code,
            final_recipient,
            original_to,
            source,
            subject,
            source_message_id,
            source_uid_fingerprint,
            metadata_json,
            prospect_id
        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(source_uid_fingerprint) DO NOTHING
        """,
        (
            created_at_utc,
            parsed.recipient_email,
            parsed.bounce_class,
            parsed.smtp_status,
            parsed.smtp_code,
            parsed.diagnostic_code,
            parsed.final_recipient,
            parsed.original_to,
            parsed.source,
            parsed.subject,
            parsed.source_message_id,
            source_uid_fingerprint,
            metadata_json,
            prospect_id,
        ),
    )
    changed = conn.execute("SELECT changes()").fetchone()
    return bool(changed and int(changed[0] or 0) > 0)


def _write_hard_bounce_operational(
    conn: sqlite3.Connection,
    recipient_email: str,
    prospect_id: str,
    event_ts: str,
    metadata_json: str,
) -> None:
    conn.execute(
        """
        INSERT INTO suppression(email, reason, ts)
        VALUES(?, ?, ?)
        ON CONFLICT(email) DO UPDATE SET
            reason = excluded.reason,
            ts = excluded.ts
        """,
        (recipient_email, "bounced", event_ts),
    )
    if not prospect_id:
        return
    conn.execute(
        """
        INSERT INTO outreach_events(
            prospect_id,
            ts,
            event_type,
            batch_id,
            metadata_json
        ) VALUES(?, ?, ?, ?, ?)
        """,
        (prospect_id, event_ts, "bounced", "", metadata_json),
    )
    conn.execute("UPDATE prospects SET status = ? WHERE prospect_id = ?", ("bounced", prospect_id))


def _resolve_config(args: argparse.Namespace, allow_missing_password: bool = False) -> tuple[bool, dict[str, Any], str]:
    host = (os.getenv("BOUNCE_IMAP_HOST") or "imappro.zoho.com").strip()
    port_raw = (os.getenv("BOUNCE_IMAP_PORT") or "993").strip()
    user = (os.getenv("BOUNCE_IMAP_USER") or "cchevali@zohomail.com").strip()
    password = (os.getenv("BOUNCE_IMAP_PASS") or os.getenv("IMAP_PASS") or "").strip()
    folder = (str(args.folder or "").strip() or (os.getenv("BOUNCE_IMAP_FOLDER") or "INBOX").strip() or "INBOX")

    try:
        port = int(port_raw)
    except Exception:
        return False, {}, f"{ERR_BOUNCE_IMAP_CONFIG_MISSING} invalid_port value={_compact(port_raw, 64)}"
    if port < 1:
        return False, {}, f"{ERR_BOUNCE_IMAP_CONFIG_MISSING} invalid_port value={port}"

    missing: list[str] = []
    if not host:
        missing.append("BOUNCE_IMAP_HOST")
    if not user:
        missing.append("BOUNCE_IMAP_USER")
    if (not allow_missing_password) and (not password):
        missing.append("BOUNCE_IMAP_PASS")
    if missing:
        return False, {}, f"{ERR_BOUNCE_IMAP_CONFIG_MISSING} missing={','.join(missing)}"

    max_messages = int(max(1, int(args.max_messages)))
    cfg = {
        "imap_host": host,
        "imap_port": port,
        "imap_user": user,
        "imap_pass": password,
        "imap_folder": folder,
        "max_messages": max_messages,
    }
    return True, cfg, ""


def _print_config(cfg: dict[str, Any], data_dir: Path, state_file: Path, lock_file: Path, suppression_csv: Path, crm_db: Path) -> None:
    print(f"{PASS_BOUNCE_IMPORT_PRINT_CONFIG} imap_host={cfg['imap_host']}")
    print(f"{PASS_BOUNCE_IMPORT_PRINT_CONFIG} imap_port={cfg['imap_port']}")
    print(f"{PASS_BOUNCE_IMPORT_PRINT_CONFIG} imap_user={cfg['imap_user']}")
    print(f"{PASS_BOUNCE_IMPORT_PRINT_CONFIG} imap_folder={cfg['imap_folder']}")
    print(f"{PASS_BOUNCE_IMPORT_PRINT_CONFIG} max_messages={cfg['max_messages']}")
    print(f"{PASS_BOUNCE_IMPORT_PRINT_CONFIG} data_dir={data_dir.resolve()}")
    print(f"{PASS_BOUNCE_IMPORT_PRINT_CONFIG} state_file={state_file.resolve()}")
    print(f"{PASS_BOUNCE_IMPORT_PRINT_CONFIG} lock_file={lock_file.resolve()}")
    print(f"{PASS_BOUNCE_IMPORT_PRINT_CONFIG} suppression_csv={suppression_csv.resolve()}")
    print(f"{PASS_BOUNCE_IMPORT_PRINT_CONFIG} crm_db={crm_db.resolve()}")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Import DSN/moderation bounce signals from IMAP into outreach CRM.")
    ap.add_argument("--print-config", action="store_true", help="Print resolved config and exit.")
    ap.add_argument("--dry-run", action="store_true", help="Parse and classify messages without writing outputs.")
    ap.add_argument("--max-messages", type=int, default=DEFAULT_MAX_MESSAGES, help="Max candidate UIDs to scan per run.")
    ap.add_argument("--folder", default="", help="Optional IMAP folder override.")
    args = ap.parse_args(argv)

    ok_cfg, cfg, cfg_err = _resolve_config(args, allow_missing_password=bool(args.print_config))
    if not ok_cfg:
        print(cfg_err, file=sys.stderr)
        print(f"BOUNCE_IMPORT_COMPLETE status={ERR_BOUNCE_IMAP_CONFIG_MISSING}")
        return 2

    data_dir = crm_store.data_dir()
    state_file = _state_path(data_dir)
    lock_file = _lock_path(data_dir)
    suppression_csv = _suppression_csv_path(data_dir)
    crm_db_path = crm_store.crm_db_path()

    if args.print_config:
        _print_config(cfg, data_dir, state_file, lock_file, suppression_csv, crm_db_path)
        print("BOUNCE_IMPORT_COMPLETE status=PRINT_CONFIG")
        return 0

    try:
        lock_scope = _acquire_lock(lock_file)
        lock_scope.__enter__()
    except RuntimeError as exc:
        msg = str(exc)
        print(msg, file=sys.stderr)
        print(f"BOUNCE_IMPORT_COMPLETE status={ERR_BOUNCE_IMPORT_LOCKED}")
        return 2

    try:
        state = _load_state(state_file)
        conn_imap: imaplib.IMAP4_SSL | None = None
        conn_db: sqlite3.Connection | None = None
        suppression_seen: set[str] = set()
        suppression_fields: list[str] = list(DEFAULT_SUPPRESSION_FIELDS)

        uids_found = 0
        uids_processed = 0
        candidates_seen = 0
        parse_errors = 0
        hard_bounces = 0
        soft_bounces = 0
        duplicates_skipped = 0
        suppression_appends = 0
        moderation_hard_seen = 0

        uidvalidity = ""
        last_uid_processed = _safe_int(state.get("last_uid_processed", 0), 0)
        previous_uidvalidity = str(state.get("uidvalidity") or "").strip()

        try:
            conn_imap = _imap_connect(cfg)
            typ, _ = conn_imap.select(str(cfg["imap_folder"]), readonly=True)
            if typ != "OK":
                print(
                    f"{ERR_BOUNCE_IMAP_CONNECT} select_failed folder={cfg['imap_folder']}",
                    file=sys.stderr,
                )
                print(f"BOUNCE_IMPORT_COMPLETE status={ERR_BOUNCE_IMAP_CONNECT}")
                return 2
            uidvalidity = _extract_uidvalidity(conn_imap)
            if previous_uidvalidity and uidvalidity and previous_uidvalidity != uidvalidity:
                print(
                    f"{WARN_BOUNCE_UIDVALIDITY_CHANGED} old={previous_uidvalidity} new={uidvalidity}",
                    file=sys.stderr,
                )
                last_uid_processed = 0

            start_uid = max(1, int(last_uid_processed) + 1)
            uids = _search_uids(conn_imap, start_uid=start_uid, max_messages=int(cfg["max_messages"]))
            uids_found = len(uids)

            if not args.dry_run:
                crm_db = crm_store.ensure_database()
                conn_db = crm_store.connect(crm_db)
                crm_store.init_schema(conn_db)
                suppression_seen, suppression_fields = _load_suppression_index(suppression_csv)

            for uid in uids:
                processed_this_uid = False
                try:
                    msg = _fetch_uid_message(conn_imap, uid)
                except Exception as exc:
                    parse_errors += 1
                    print(
                        f"{ERR_BOUNCE_IMPORT_PARSE} uid={uid} err={_compact(exc)}",
                        file=sys.stderr,
                    )
                    processed_this_uid = True
                    last_uid_processed = uid
                    uids_processed += 1
                    continue

                subject = _decode_header_value(msg.get("Subject", ""))
                sender = _decode_header_value(msg.get("From", ""))
                message_id = _decode_header_value(msg.get("Message-ID", "")) or f"imap_uid:{uid}"
                headers_text = _header_text(msg)
                body_text = _extract_plain_body(msg)

                if not _looks_like_candidate(subject, sender, headers_text, body_text):
                    processed_this_uid = True
                else:
                    candidates_seen += 1
                    parsed = _parse_bounce(subject, sender, headers_text, body_text, message_id)
                    if not parsed:
                        parse_errors += 1
                        print(
                            f"{ERR_BOUNCE_IMPORT_PARSE} uid={uid} reason=missing_final_recipient",
                            file=sys.stderr,
                        )
                        processed_this_uid = True
                    else:
                        if parsed.bounce_class == "hard":
                            hard_bounces += 1
                        else:
                            soft_bounces += 1

                        uid_fingerprint = (
                            f"imap:{cfg['imap_host']}:{cfg['imap_user']}:{cfg['imap_folder']}:{uidvalidity}:{uid}"
                        )
                        event_ts = _utc_now_iso()
                        metadata = {
                            "source": "import_bounces_imap",
                            "imap_host": cfg["imap_host"],
                            "imap_user": cfg["imap_user"],
                            "imap_folder": cfg["imap_folder"],
                            "uidvalidity": uidvalidity,
                            "uid": uid,
                            "from": sender,
                            "subject": parsed.subject,
                            "final_recipient": parsed.final_recipient,
                            "status": parsed.smtp_status,
                            "smtp_code": parsed.smtp_code,
                            "diagnostic_code": parsed.diagnostic_code,
                            "original_to": parsed.original_to,
                            "source_type": parsed.source,
                        }
                        metadata_json = json.dumps(metadata, separators=(",", ":"), ensure_ascii=True)

                        if args.dry_run:
                            if parsed.source == "moderation" and parsed.bounce_class == "hard":
                                moderation_hard_seen = 1
                            processed_this_uid = True
                        else:
                            assert conn_db is not None
                            prospect_id = _find_prospect_id(conn_db, parsed.recipient_email)
                            inserted = False
                            try:
                                conn_db.execute("BEGIN")
                                inserted = _insert_bounce_event(
                                    conn=conn_db,
                                    created_at_utc=event_ts,
                                    parsed=parsed,
                                    source_uid_fingerprint=uid_fingerprint,
                                    metadata_json=metadata_json,
                                    prospect_id=prospect_id,
                                )
                                if inserted and parsed.bounce_class == "hard":
                                    _write_hard_bounce_operational(
                                        conn=conn_db,
                                        recipient_email=parsed.recipient_email,
                                        prospect_id=prospect_id,
                                        event_ts=event_ts,
                                        metadata_json=metadata_json,
                                    )
                                    if parsed.recipient_email not in suppression_seen:
                                        _append_suppression_csv(
                                            path=suppression_csv,
                                            fieldnames=suppression_fields,
                                            recipient_email=parsed.recipient_email,
                                            reason="bounced",
                                            source="import_bounces_imap",
                                            timestamp=event_ts,
                                            evidence_msg_id=parsed.source_message_id,
                                        )
                                        suppression_seen.add(parsed.recipient_email)
                                        suppression_appends += 1
                                    if parsed.source == "moderation":
                                        moderation_hard_seen = 1
                                if not inserted:
                                    duplicates_skipped += 1
                                conn_db.commit()
                                processed_this_uid = True
                            except Exception as exc:
                                try:
                                    conn_db.rollback()
                                except Exception:
                                    pass
                                print(
                                    f"{ERR_BOUNCE_IMPORT_CRM} uid={uid} err={type(exc).__name__} detail={_compact(exc)}",
                                    file=sys.stderr,
                                )
                                print(f"BOUNCE_IMPORT_COMPLETE status={ERR_BOUNCE_IMPORT_CRM}")
                                return 2

                if processed_this_uid:
                    last_uid_processed = uid
                    uids_processed += 1

            if not args.dry_run:
                next_state = {
                    "imap_host": cfg["imap_host"],
                    "imap_user": cfg["imap_user"],
                    "imap_folder": cfg["imap_folder"],
                    "uidvalidity": uidvalidity,
                    "last_uid_processed": int(last_uid_processed),
                    "updated_at_utc": _utc_now_iso(),
                    "messages_scanned_last": int(uids_found),
                    "hard_bounces_last": int(hard_bounces),
                    "run_id_last": f"imap-bounce-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}",
                }
                _write_json_atomic(state_file, next_state)

            print(f"BOUNCE_STATE_PATH={state_file.resolve()}")
            print(f"BOUNCE_STATE_UIDVALIDITY={uidvalidity}")
            print(f"BOUNCE_STATE_LAST_UID={int(last_uid_processed)}")
            print(f"BOUNCE_IMPORT_UIDS_FOUND={uids_found}")
            print(f"BOUNCE_IMPORT_UIDS_PROCESSED={uids_processed}")
            if moderation_hard_seen:
                print("BOUNCE_IMPORT_MODERATION_NOTICE_SEEN=1")

            token = PASS_BOUNCE_IMPORT_DRY_RUN if args.dry_run else PASS_BOUNCE_IMPORT_APPLY
            print(
                f"{token} candidates_seen={candidates_seen} hard_bounces={hard_bounces} soft_bounces={soft_bounces} "
                f"parse_errors={parse_errors} duplicates_skipped={duplicates_skipped} suppression_appends={suppression_appends}"
            )
            print(f"BOUNCE_IMPORT_COMPLETE status={'DRY_RUN' if args.dry_run else 'OK'}")
            return 0
        except RuntimeError as exc:
            msg = str(exc)
            if msg.startswith(ERR_BOUNCE_IMAP_AUTH):
                print(msg, file=sys.stderr)
                print(f"BOUNCE_IMPORT_COMPLETE status={ERR_BOUNCE_IMAP_AUTH}")
                return 2
            if msg.startswith(ERR_BOUNCE_IMAP_CONNECT):
                print(msg, file=sys.stderr)
                print(f"BOUNCE_IMPORT_COMPLETE status={ERR_BOUNCE_IMAP_CONNECT}")
                return 2
            print(f"{ERR_BOUNCE_IMAP_CONNECT} err={_compact(msg)}", file=sys.stderr)
            print(f"BOUNCE_IMPORT_COMPLETE status={ERR_BOUNCE_IMAP_CONNECT}")
            return 2
        except Exception as exc:
            print(f"{ERR_BOUNCE_IMAP_CONNECT} err={type(exc).__name__} detail={_compact(exc)}", file=sys.stderr)
            print(f"BOUNCE_IMPORT_COMPLETE status={ERR_BOUNCE_IMAP_CONNECT}")
            return 2
        finally:
            if conn_db is not None:
                try:
                    conn_db.close()
                except Exception:
                    pass
            if conn_imap is not None:
                try:
                    conn_imap.logout()
                except Exception:
                    pass
    finally:
        try:
            lock_scope.__exit__(None, None, None)
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(main())
