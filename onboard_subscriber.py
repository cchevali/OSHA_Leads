"""
Email-only YES-reply onboarding utility.

Takes a strict KEY=VALUE block (copy/paste from an email reply), upserts a subscriber
into data/osha.sqlite, writes an untracked customer config under customers/, sends a
confirmation email, and appends an onboarding audit log row.

Scope: provisioning/onboarding only (no UI).
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
import sqlite3
import sys
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from email.mime.text import MIMEText
from pathlib import Path
from typing import Any

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover
    load_dotenv = None

from lead_filters import load_territory_definitions, normalize_content_filter


DEFAULT_DB = "data/osha.sqlite"
DEFAULT_SCHEMA = "schema.sql"
DEFAULT_OUTPUT_DIR = "out"

DEFAULT_SEND_TIME_LOCAL = "08:00"
DEFAULT_TIMEZONE = "America/Chicago"
DEFAULT_THRESHOLD = "MEDIUM"
DEFAULT_TRIAL_LENGTH_DAYS = 7


TERRITORY_ALIASES = {
    "TX_TRIANGLE": "TX_TRIANGLE_V1",
}


_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
_TIME_RE = re.compile(r"^(?P<h>\d{2}):(?P<m>\d{2})$")


class OnboardingError(RuntimeError):
    pass


@dataclass(frozen=True)
class OnboardingRequest:
    territory_tag: str
    territory_code: str
    send_time_local: str
    timezone: str
    threshold: str
    content_filter: str
    include_low_fallback: bool
    recipients: list[str]
    display_name: str
    subscriber_key: str
    notes: str
    trial_length_days: int


def _repo_root() -> Path:
    return Path(__file__).resolve().parent


def _load_env(repo_root: Path) -> None:
    if load_dotenv is None:
        return
    dotenv_path = repo_root / ".env"
    if dotenv_path.exists():
        load_dotenv(dotenv_path=dotenv_path, override=False)


def _read_text(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")


def _parse_block(text: str) -> dict[str, str]:
    """
    Parse a KEY=VALUE block. Ignores blank lines and lines starting with '#'.
    """
    values: dict[str, str] = {}
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            raise OnboardingError(f"Invalid line (expected KEY=VALUE): {raw}")
        k, v = line.split("=", 1)
        key = k.strip().upper()
        val = v.strip()
        if not key:
            raise OnboardingError(f"Invalid line (empty key): {raw}")
        values[key] = val
    return values


def _normalize_recipients(value: str) -> list[str]:
    if not value.strip():
        return []
    parts = re.split(r"[;,]", value)
    cleaned: list[str] = []
    seen: set[str] = set()
    for part in parts:
        email = part.strip().lower()
        if not email:
            continue
        if not _EMAIL_RE.match(email):
            raise OnboardingError(f"Invalid recipient email: {email}")
        if email not in seen:
            seen.add(email)
            cleaned.append(email)
    return cleaned


def _validate_send_time_local(value: str) -> str:
    text = (value or "").strip()
    if not text:
        return DEFAULT_SEND_TIME_LOCAL
    m = _TIME_RE.match(text)
    if not m:
        raise OnboardingError("SEND_TIME_LOCAL must be HH:MM (24-hour), e.g. 08:00")
    hour = int(m.group("h"))
    minute = int(m.group("m"))
    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        raise OnboardingError("SEND_TIME_LOCAL out of range (expected 00:00..23:59)")
    return f"{hour:02d}:{minute:02d}"


def _validate_timezone(value: str) -> str:
    tz_name = (value or "").strip() or DEFAULT_TIMEZONE
    if ZoneInfo is None:
        # In the unlikely event zoneinfo isn't available, only basic validation.
        if "/" not in tz_name:
            raise OnboardingError("TIMEZONE must be an IANA name (e.g. America/Chicago)")
        return tz_name
    try:
        ZoneInfo(tz_name)
    except Exception as exc:
        raise OnboardingError(f"Invalid TIMEZONE '{tz_name}': {exc}") from exc
    return tz_name


def _threshold_to_content_filter(value: str) -> tuple[str, bool, str]:
    text = (value or "").strip().upper() or DEFAULT_THRESHOLD
    if text in {"MEDIUM", "HIGH_MEDIUM", "HIGH+MEDIUM", "HIGH_MED"}:
        return "high_medium", True, "MEDIUM"
    if text in {"HIGH", "HIGH_ONLY"}:
        return "high_only", False, "HIGH"
    if text in {"ALL", "ANY"}:
        return "all", False, "ALL"
    raise OnboardingError("THRESHOLD must be MEDIUM, HIGH, or ALL")


def _resolve_territory_code(tag: str) -> tuple[str, str]:
    raw = (tag or "").strip()
    if not raw:
        raise OnboardingError("TERRITORY is required (e.g. TX_TRIANGLE)")

    normalized = raw.strip().upper()
    code = TERRITORY_ALIASES.get(normalized, normalized)

    defs = load_territory_definitions()
    if code in defs:
        return normalized, code
    # Convenience: accept tags without a version suffix when *_V1 exists.
    if f"{code}_V1" in defs:
        return normalized, f"{code}_V1"
    raise OnboardingError(f"Unknown territory '{raw}'. Expected one of: {', '.join(sorted(defs.keys()))}")


def _generate_subscriber_key(territory_code: str, primary_email: str) -> str:
    digest = hashlib.sha1(f"{territory_code.lower()}|{primary_email.lower()}".encode("utf-8")).hexdigest()[:10]
    return f"sub_{territory_code.lower()}_{digest}"


def _ensure_schema(db_path: str, schema_path: str) -> None:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    try:
        with open(schema_path, "r", encoding="utf-8") as f:
            conn.executescript(f.read())
        conn.commit()
    finally:
        conn.close()


def _upsert_territory_from_json(conn: sqlite3.Connection, territory_code: str) -> None:
    defs = load_territory_definitions()
    terr = defs.get(territory_code)
    if not terr:
        raise OnboardingError(f"Territory not found in territories.json: {territory_code}")
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
            territory_code,
            (terr.get("description") or "").strip(),
            json.dumps([str(s).upper() for s in (terr.get("states") or [])]),
            json.dumps(list(terr.get("office_patterns") or [])),
            json.dumps(list(terr.get("fallback_city_patterns") or [])),
        ),
    )


def _existing_subscriber_key_for_email(conn: sqlite3.Connection, email: str) -> str | None:
    cur = conn.cursor()
    cur.execute(
        "SELECT subscriber_key FROM subscribers WHERE lower(email) = ? LIMIT 1",
        (email.lower(),),
    )
    row = cur.fetchone()
    return str(row[0]) if row else None


def _subscriber_exists(conn: sqlite3.Connection, subscriber_key: str) -> bool:
    cur = conn.cursor()
    cur.execute(
        "SELECT 1 FROM subscribers WHERE subscriber_key = ? LIMIT 1",
        (subscriber_key,),
    )
    return cur.fetchone() is not None


def _upsert_subscriber(conn: sqlite3.Connection, req: OnboardingRequest) -> None:
    start = date.today()
    end = start + timedelta(days=int(req.trial_length_days))
    recipients_json = json.dumps(req.recipients, ensure_ascii=True)
    primary_email = req.recipients[0].lower()

    # Preserve trial dates on update unless operator explicitly resets them (not supported yet).
    conn.execute(
        """
        INSERT INTO subscribers
            (subscriber_key, display_name, email, recipients_json, territory_code, content_filter, include_low_fallback,
             trial_length_days, trial_started_at, trial_ends_at, active, send_enabled,
             send_time_local, timezone, customer_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 1, ?, ?, ?)
        ON CONFLICT(subscriber_key) DO UPDATE SET
            display_name=excluded.display_name,
            email=excluded.email,
            recipients_json=excluded.recipients_json,
            territory_code=excluded.territory_code,
            content_filter=excluded.content_filter,
            include_low_fallback=excluded.include_low_fallback,
            active=1,
            send_enabled=1,
            send_time_local=excluded.send_time_local,
            timezone=excluded.timezone,
            customer_id=excluded.customer_id,
            updated_at=CURRENT_TIMESTAMP
        """,
        (
            req.subscriber_key,
            req.display_name,
            primary_email,
            recipients_json,
            req.territory_code,
            req.content_filter,
            1 if req.include_low_fallback else 0,
            int(req.trial_length_days),
            start.isoformat(),
            end.isoformat(),
            req.send_time_local,
            req.timezone,
            req.subscriber_key,
        ),
    )


def _next_scheduled_run_local(send_time_local: str, tz_name: str) -> tuple[str, str]:
    if ZoneInfo is None:
        # Fallback: can't reliably compute; return generic.
        return "next scheduled run", f"{send_time_local} {tz_name}"
    tz = ZoneInfo(tz_name)
    now = datetime.now(tz)
    hh, mm = [int(x) for x in send_time_local.split(":")]
    candidate = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
    if now <= candidate:
        return candidate.strftime("%Y-%m-%d"), candidate.strftime("%H:%M %Z")
    candidate = candidate + timedelta(days=1)
    return candidate.strftime("%Y-%m-%d"), candidate.strftime("%H:%M %Z")


def _is_suppressed(conn: sqlite3.Connection, email: str) -> bool:
    cur = conn.cursor()
    cur.execute(
        "SELECT 1 FROM suppression_list WHERE lower(email_or_domain) = ? LIMIT 1",
        (email.lower(),),
    )
    if cur.fetchone():
        return True
    domain = email.split("@")[-1].lower()
    cur.execute(
        "SELECT 1 FROM suppression_list WHERE lower(email_or_domain) = ? LIMIT 1",
        (domain,),
    )
    return cur.fetchone() is not None


def _send_email(to_email: str, subject: str, body: str) -> None:
    import smtplib

    smtp_host = os.environ.get("SMTP_HOST", "smtp.zoho.com").strip()
    smtp_port = int(os.environ.get("SMTP_PORT", "587"))
    smtp_user = os.environ.get("SMTP_USER", "").strip()
    smtp_pass = os.environ.get("SMTP_PASS", "").strip()
    if not smtp_user or not smtp_pass:
        raise OnboardingError("SMTP_USER/SMTP_PASS missing in environment (.env)")

    from_email = (os.environ.get("FROM_EMAIL", "").strip() or smtp_user).strip()
    from_name = (os.environ.get("FROM_NAME", "").strip() or os.environ.get("BRAND_NAME", "").strip() or "MicroFlowOps")
    reply_to = (os.environ.get("REPLY_TO_EMAIL", "").strip() or "support@microflowops.com").strip()

    msg = MIMEText(body, _charset="utf-8")
    msg["Subject"] = subject
    msg["From"] = f"{from_name} <{from_email}>"
    msg["To"] = to_email
    msg["Reply-To"] = reply_to

    with smtplib.SMTP(smtp_host, smtp_port) as server:
        server.starttls()
        server.login(smtp_user, smtp_pass)
        server.send_message(msg)


def _send_confirmation_emails(db_path: str, req: OnboardingRequest) -> dict[str, Any]:
    brand = (os.environ.get("BRAND_NAME", "").strip() or "MicroFlowOps").strip()
    subj = f"[{brand}] Confirmed: OSHA Activity Signals ({req.territory_tag})"
    run_date, run_time = _next_scheduled_run_local(req.send_time_local, req.timezone)
    recipients_text = ", ".join(req.recipients)

    body = (
        f"You're set up for {brand} OSHA Activity Signals.\n\n"
        f"Territory: {req.territory_tag} ({req.territory_code})\n"
        f"Send time: {req.send_time_local} ({req.timezone})\n"
        f"Threshold: {req.threshold}\n"
        f"Recipients: {recipients_text}\n\n"
        f"Start date: {run_date} at {run_time} (next scheduled run)\n\n"
        "Unsubscribe: reply \"unsubscribe\" at any time. We maintain a suppression list and honor it.\n"
        "Disclaimer: independent alert service; not affiliated with OSHA; informational only.\n"
    )
    if req.notes.strip():
        body += f"\nNotes:\n{req.notes.strip()}\n"

    conn = sqlite3.connect(db_path)
    try:
        sent = 0
        suppressed = 0
        errors: list[str] = []
        for email in req.recipients:
            if _is_suppressed(conn, email):
                suppressed += 1
                continue
            try:
                _send_email(email, subj, body)
                sent += 1
            except Exception as exc:
                errors.append(f"{email}: {exc}")
        return {"subject": subj, "sent": sent, "suppressed": suppressed, "errors": errors}
    finally:
        conn.close()


def _append_audit_row(out_dir: str, row: dict[str, str]) -> str:
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    path = Path(out_dir) / "onboarding_audit_log.csv"
    fieldnames = [
        "timestamp_utc",
        "prospect_emails",
        "subscriber_key",
        "territory",
        "send_time_local",
        "timezone",
        "threshold",
        "operator_action",
        "status",
    ]
    exists = path.exists()
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not exists:
            writer.writeheader()
        writer.writerow(row)
    return str(path)


def _build_customer_config(req: OnboardingRequest) -> dict[str, Any]:
    defs = load_territory_definitions()
    terr = defs.get(req.territory_code, {})
    states = [str(s).upper() for s in (terr.get("states") or [])] or ["TX"]

    brand_name = (os.environ.get("BRAND_NAME", "").strip() or "MicroFlowOps").strip()
    mailing_address = (os.environ.get("MAILING_ADDRESS", "").strip() or "").strip()
    operator_email = (os.environ.get("OPERATOR_EMAIL", "").strip() or os.environ.get("REPLY_TO_EMAIL", "").strip() or "support@microflowops.com").strip()

    return {
        "customer_id": req.subscriber_key,
        "subscriber_key": req.subscriber_key,
        "subscriber_name": req.display_name,
        "trial_length_days": int(req.trial_length_days),
        "active": True,
        "territory_code": req.territory_code,
        "send_time_local": req.send_time_local,
        "send_window_minutes": 60,
        "timezone": req.timezone,
        "content_filter": req.content_filter,
        "include_low_fallback": bool(req.include_low_fallback),
        "states": states,
        "opened_window_days": 14,
        "new_only_days": 1,
        "top_k_overall": 30,
        "top_k_per_state": 30,
        # Recipients are sourced from the subscribers table; keep config recipients empty.
        "email_recipients": [],
        "pilot_mode": True,
        "pilot_whitelist": [operator_email.lower()] + [email.lower() for email in req.recipients],
        "brand_name": brand_name,
        "mailing_address": mailing_address,
        "allow_live_send": True,
    }


def _write_customer_config(path: str, payload: dict[str, Any]) -> str:
    out_path = Path(path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return str(out_path)


def _build_request(values: dict[str, str]) -> OnboardingRequest:
    territory_tag, territory_code = _resolve_territory_code(values.get("TERRITORY", ""))
    send_time_local = _validate_send_time_local(values.get("SEND_TIME_LOCAL", ""))
    tz_name = _validate_timezone(values.get("TIMEZONE", ""))
    content_filter, include_low_fallback, threshold_norm = _threshold_to_content_filter(values.get("THRESHOLD", ""))

    # Validate content filter mapping against existing normalization rules.
    content_filter = normalize_content_filter(content_filter)

    recipients = _normalize_recipients(values.get("RECIPIENTS", ""))
    if not recipients:
        raise OnboardingError("RECIPIENTS is required (comma-separated emails)")

    display_name = (values.get("FIRM_NAME") or values.get("DISPLAY_NAME") or "").strip() or recipients[0]

    notes = (values.get("NOTES") or "").strip()
    try:
        trial_len = int((values.get("TRIAL_LENGTH_DAYS") or "").strip() or DEFAULT_TRIAL_LENGTH_DAYS)
    except ValueError as exc:
        raise OnboardingError("TRIAL_LENGTH_DAYS must be an integer") from exc
    if trial_len <= 0 or trial_len > 90:
        raise OnboardingError("TRIAL_LENGTH_DAYS out of range (1..90)")

    subscriber_key = (values.get("SUBSCRIBER_KEY") or "").strip()
    if subscriber_key:
        subscriber_key = re.sub(r"[^a-zA-Z0-9_\\-]+", "_", subscriber_key)
    else:
        subscriber_key = _generate_subscriber_key(territory_code, recipients[0])

    return OnboardingRequest(
        territory_tag=territory_tag,
        territory_code=territory_code,
        send_time_local=send_time_local,
        timezone=tz_name,
        threshold=threshold_norm,
        content_filter=content_filter,
        include_low_fallback=bool(include_low_fallback),
        recipients=recipients,
        display_name=display_name,
        subscriber_key=subscriber_key,
        notes=notes,
        trial_length_days=trial_len,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Onboard a YES reply into an active subscriber (email-only).")
    parser.add_argument("--db", default=DEFAULT_DB, help="SQLite DB path (default: data/osha.sqlite)")
    parser.add_argument("--schema", default=DEFAULT_SCHEMA, help="Schema SQL path (default: schema.sql)")
    parser.add_argument(
        "--reply-block-file",
        default="",
        help="Path to a text file containing the KEY=VALUE onboarding block. If omitted, reads stdin.",
    )
    parser.add_argument(
        "--customer-config-out",
        default="",
        help="Where to write the untracked customer config JSON (default: customers/<subscriber_key>.json).",
    )
    parser.add_argument("--no-send-confirm", action="store_true", help="Do not send confirmation email.")
    parser.add_argument("--outdir", default=DEFAULT_OUTPUT_DIR, help="Output dir for audit log (default: out/)")

    args = parser.parse_args()

    repo_root = _repo_root()
    # Most repo helpers (territories.json, schema.sql) assume repo-root relative paths.
    os.chdir(repo_root)
    _load_env(repo_root)

    block_text = ""
    if args.reply_block_file:
        block_text = _read_text(args.reply_block_file)
    else:
        block_text = sys.stdin.read()
    if not block_text.strip():
        raise OnboardingError("Empty onboarding block. Provide --reply-block-file or pipe via stdin.")

    values = _parse_block(block_text)
    req = _build_request(values)

    # Ensure schema exists.
    _ensure_schema(args.db, args.schema)

    conn = sqlite3.connect(args.db)
    try:
        # Ensure the territory exists (FK) and stays in sync with territories.json.
        _upsert_territory_from_json(conn, req.territory_code)

        # If the email already exists under a different subscriber_key, reuse it unless user forced a key.
        existing_for_email = _existing_subscriber_key_for_email(conn, req.recipients[0])
        if existing_for_email and ("SUBSCRIBER_KEY" not in values):
            req = OnboardingRequest(**{**req.__dict__, "subscriber_key": existing_for_email})  # type: ignore[arg-type]

        # If the key exists but is tied to another email, stop (avoid accidental reassignment).
        if _subscriber_exists(conn, req.subscriber_key):
            cur = conn.cursor()
            cur.execute(
                "SELECT lower(email) FROM subscribers WHERE subscriber_key = ? LIMIT 1",
                (req.subscriber_key,),
            )
            row = cur.fetchone()
            if row and str(row[0] or "").strip().lower() not in {req.recipients[0].lower()}:
                raise OnboardingError(
                    f"subscriber_key '{req.subscriber_key}' already exists for a different email ({row[0]}). "
                    "Provide SUBSCRIBER_KEY explicitly or use a different primary recipient."
                )

        _upsert_subscriber(conn, req)
        conn.commit()
    finally:
        conn.close()

    config_payload = _build_customer_config(req)
    config_out = args.customer_config_out.strip() or str(Path("customers") / f"{req.subscriber_key}.json")
    config_path = _write_customer_config(config_out, config_payload)

    confirm_result = {"sent": 0, "suppressed": 0, "errors": []}
    if not args.no_send_confirm:
        confirm_result = _send_confirmation_emails(args.db, req)

    ts_utc = datetime.now(timezone.utc).isoformat()
    status = f"upsert_ok;confirm_sent={confirm_result.get('sent',0)};confirm_suppressed={confirm_result.get('suppressed',0)}"
    if confirm_result.get("errors"):
        status += f";confirm_errors={len(confirm_result['errors'])}"

    audit_path = _append_audit_row(
        args.outdir,
        {
            "timestamp_utc": ts_utc,
            "prospect_emails": ";".join(req.recipients),
            "subscriber_key": req.subscriber_key,
            "territory": req.territory_code,
            "send_time_local": req.send_time_local,
            "timezone": req.timezone,
            "threshold": req.threshold,
            "operator_action": "upsert",
            "status": status,
        },
    )

    # Operator-friendly output
    print(f"OK subscriber_key={req.subscriber_key}")
    print(f"OK customer_config={config_path}")
    print(f"OK audit_log={audit_path}")
    if confirm_result.get("errors"):
        for err in confirm_result["errors"]:
            print(f"WARN confirm_email_failed {err}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except OnboardingError as exc:
        print(f"ONBOARDING_ERROR {exc}", file=sys.stderr)
        raise SystemExit(1)
