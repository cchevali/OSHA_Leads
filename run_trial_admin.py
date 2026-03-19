from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sqlite3
import sys
from html import escape
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore[assignment]

import crm_light
import trial_audit
from email_footer import build_footer_html, build_footer_text
from lead_filters import load_territory_definitions, merge_territory_definition, resolve_territory_code
from runtime_data_dir import resolve_osha_db_path
from send_digest_email import (
    build_unsubscribe_payload,
    resolve_branding,
    resolve_digest_display_label,
    send_email,
)
from runtime_guard import render_runtime_lines, run_runtime_preflight, validate_live_osha_db_path


def _default_leads_db_path() -> str:
    return str(resolve_osha_db_path(Path(__file__).resolve().parent).effective_path)


def _count_status_live_primary_weekdays(
    conn: sqlite3.Connection,
    *,
    subscriber_key: str,
    start_date: str,
    tz_name: str,
    primary_recipient: str,
) -> int:
    zone = crm_light._resolve_tz(tz_name)  # type: ignore[attr-defined]
    rows = conn.execute(
        """
        SELECT ts_utc, variant, meta_json
        FROM send_events
        WHERE subscriber_key = ?
          AND status = 'SENT'
          AND ts_utc >= ?
        ORDER BY ts_utc ASC, id ASC
        """,
        (subscriber_key, f"{start_date}T00:00:00+00:00"),
    ).fetchall()
    local_dates: set[str] = set()
    for row in rows:
        meta = crm_light._safe_meta_dict(str(row["meta_json"] or ""))  # type: ignore[attr-defined]
        if not crm_light._is_trial_delivery_event(  # type: ignore[attr-defined]
            variant=str(row["variant"] or ""),
            meta=meta,
            primary_recipient=primary_recipient,
        ):
            continue
        dt_utc = crm_light._parse_utc_ts(str(row["ts_utc"] or ""))  # type: ignore[attr-defined]
        if dt_utc is None:
            continue
        local_date = dt_utc.astimezone(zone).date()
        if local_date.weekday() >= 5:
            continue
        local_dates.add(local_date.isoformat())
    return len(local_dates)


_RE_SUBSCRIBER_KEY = re.compile(r"^[A-Za-z0-9_.-]{1,80}$")
_RE_STATE_CODE = re.compile(r"^[A-Z]{2}$")
TERRITORY_ALIASES: dict[str, str] = {
    "TX_TRIANGLE_V1": "TX_TRI",
    "TX_TRIANGLE": "TX_TRI",
    "TX_TRI_V1": "TX_TRI",
}
DEFAULT_SENDS_LIMIT = 14
TRIAL_SENDS_TARGET = 14
CONVERSION_SUBJECT_PREFIX = "Keep your OSHA signal digest running"
CONVERSION_CHECKOUT_TEXT = "Activate secure checkout"
_GENERIC_CONVERSION_LABELS = {
    "",
    "{territory_label}",
    "coverage area",
    "your coverage area",
    "territory",
    "your territory",
}
_ALIAS_NAME_MARKERS = {
    "admin",
    "alerts",
    "billing",
    "bot",
    "demo",
    "dev",
    "digest",
    "hello",
    "info",
    "internal",
    "mail",
    "noreply",
    "notify",
    "ops",
    "qa",
    "sample",
    "smoke",
    "stage",
    "staging",
    "support",
    "team",
    "test",
    "trial",
}
_HUMAN_NAME_PART_RE = re.compile(r"^[A-Za-z][A-Za-z'-]{0,29}$")


@dataclass(frozen=True)
class TrialAddRequest:
    subscriber_key: str
    email: str
    territory_code: str
    tz: str
    start_date: str
    sends_limit: int


def _normalize_subscriber_key(value: str) -> str:
    return (value or "").strip().lower()


def _validate_subscriber_key(value: str) -> str:
    sk = _normalize_subscriber_key(value)
    if not sk or not _RE_SUBSCRIBER_KEY.match(sk):
        raise ValueError("invalid subscriber_key (expected 1-80 chars from [A-Za-z0-9_.-])")
    return sk


def _normalize_email(value: str) -> str:
    return (value or "").strip().lower()


def _validate_email(value: str) -> str:
    email = _normalize_email(value)
    if not email or "@" not in email or "." not in email.split("@", 1)[-1]:
        raise ValueError("invalid email")
    return email


def _normalize_territory(value: str) -> str:
    raw = (value or "").strip().upper()
    if not raw:
        return raw
    defs = load_territory_definitions()
    canonical = resolve_territory_code(raw, defs)
    if canonical in defs:
        return canonical
    return TERRITORY_ALIASES.get(raw, raw)


def _normalize_states_input(raw_values: str | list[str]) -> list[str]:
    parts: list[str] = []
    if isinstance(raw_values, str):
        candidates = [raw_values]
    else:
        candidates = list(raw_values or [])
    for token in candidates:
        for piece in str(token or "").split(","):
            value = piece.strip().upper()
            if value:
                parts.append(value)
    out: list[str] = []
    seen: set[str] = set()
    for state in parts:
        if not _RE_STATE_CODE.match(state):
            raise ValueError(f"invalid state code '{state}' (expected 2-letter USPS code)")
        if state in seen:
            continue
        seen.add(state)
        out.append(state)
    if not out:
        raise ValueError("states required")
    return out


def _state_set_territory_code(subscriber_key: str) -> str:
    return f"{str(subscriber_key or '').strip().upper()}_STATES"


def _build_state_set_territory_definition(subscriber_key: str, states: list[str]) -> tuple[str, dict[str, Any]]:
    normalized_states = _normalize_states_input(states)
    code = _state_set_territory_code(subscriber_key)
    states_csv = ",".join(normalized_states)
    return code, {
        "display_name": f"{code} Trial States",
        "label": f"Trial states {states_csv}",
        "description": f"Trial-only full-state territory for {states_csv}.",
        "kind": "STATE_SET",
        "states": normalized_states,
        "cbsas": [],
        "aliases": [],
        "office_patterns": [],
        "fallback_city_patterns": [],
    }


def _resolve_sends_limit_from_state(trial_state: dict[str, Any] | None) -> int:
    if trial_state and trial_state.get("sends_limit") is not None:
        try:
            n = int(trial_state.get("sends_limit"))
            if n >= 1:
                return n
        except Exception:
            pass
    raw = (os.getenv("TRIAL_SENDS_LIMIT_DEFAULT") or "").strip()
    if raw:
        try:
            n = int(raw)
            if n >= 1:
                return n
        except Exception:
            return DEFAULT_SENDS_LIMIT
    return DEFAULT_SENDS_LIMIT


def _resolve_default_sends_limit() -> int:
    return _resolve_sends_limit_from_state(None)


def _resolve_conversion_url() -> str:
    return (os.getenv("TRIAL_CONVERSION_URL") or "").strip()


def _resolve_territory_label(territory_code: str) -> str:
    code = (territory_code or "").strip().upper()
    if not code:
        return "{territory_label}"
    defs = load_territory_definitions()
    canonical = resolve_territory_code(code, defs)
    terr = defs.get(canonical) or defs.get(code) or {}
    label = str(terr.get("label") or terr.get("display_name") or terr.get("description") or "").strip()
    return label or code or "{territory_label}"


def _normalize_conversion_territory_label(raw_value: str) -> str:
    value = " ".join(str(raw_value or "").strip().split())
    if not value:
        return ""
    # Avoid doubled terminal punctuation when templates append sentence punctuation.
    normalized = re.sub(r"[.]+$", "", value).strip()
    if normalized.lower() in _GENERIC_CONVERSION_LABELS:
        return ""
    return normalized


def _resolve_conversion_display_label(territory_code: str) -> str:
    code = (territory_code or "").strip().upper()
    if not code:
        return ""
    defs = load_territory_definitions()
    canonical = resolve_territory_code(code, defs)
    terr = defs.get(canonical) or defs.get(code) or {}
    label = resolve_digest_display_label(
        config=terr,
        territory_code=canonical or code,
        states=list(terr.get("states") or []),
    )
    return _normalize_conversion_territory_label(label)


def _build_conversion_subject(display_label: str) -> str:
    label = _normalize_conversion_territory_label(display_label)
    if label:
        return f"{CONVERSION_SUBJECT_PREFIX} — {label}"
    return CONVERSION_SUBJECT_PREFIX


def _build_conversion_opener(display_label: str) -> str:
    label = _normalize_conversion_territory_label(display_label)
    opener = "You've been receiving the weekday OSHA activity digest"
    if label:
        opener += f" for {label}"
    return f"{opener} over the past couple of weeks. Wanted to check in before the trial ends."


def _has_checkout_url(stripe_link: str) -> bool:
    value = (stripe_link or "").strip()
    return value.startswith("http://") or value.startswith("https://")


def _contains_alias_marker(value: str) -> bool:
    tokens = [token for token in re.split(r"[^A-Za-z]+", str(value or "").lower()) if token]
    return any(token in _ALIAS_NAME_MARKERS for token in tokens)


def _normalize_human_name(value: Any) -> str:
    normalized = " ".join(str(value or "").strip().split())
    if not normalized:
        return ""
    if any(ch.isdigit() for ch in normalized):
        return ""
    if any(ch in normalized for ch in "@+_/\\|"):
        return ""
    if _contains_alias_marker(normalized):
        return ""
    parts = normalized.split()
    if not parts or len(parts) > 3:
        return ""
    rendered: list[str] = []
    for part in parts:
        clean = part.strip(".,")
        if not clean or not _HUMAN_NAME_PART_RE.fullmatch(clean):
            return ""
        rendered.append(clean.lower().title())
    return " ".join(rendered)


def _parse_name_from_email(email: str) -> str:
    local = str(email or "").strip().split("@", 1)[0].strip()
    if not local or "+" in local:
        return ""
    if any(ch.isdigit() for ch in local):
        return ""
    if _contains_alias_marker(local):
        return ""
    if re.search(r"[a-z][A-Z]|[A-Z].*[a-z].*[A-Z]", local):
        return ""
    parts = [part for part in re.split(r"[._-]+", local) if part]
    if not parts or len(parts) > 3:
        return ""
    if len(parts) == 1 and len(parts[0]) > 12:
        return ""
    rendered: list[str] = []
    for part in parts:
        if not _HUMAN_NAME_PART_RE.fullmatch(part):
            return ""
        rendered.append(part.lower().title())
    candidate = " ".join(rendered)
    return _normalize_human_name(candidate)


def _resolve_explicit_recipient_name(
    conn: sqlite3.Connection,
    *,
    subscriber_key: str,
    recipient_email: str,
    subscriber: dict[str, Any] | None,
) -> str:
    for candidate in [
        (subscriber or {}).get("first_name"),
        (subscriber or {}).get("display_name"),
        (subscriber or {}).get("name"),
    ]:
        name = _normalize_human_name(candidate)
        if name:
            return name
    entitlement = crm_light.get_subscriber_entitlement(
        conn,
        subscriber_key=subscriber_key,
        email=recipient_email,
        active_only=True,
    )
    if not entitlement:
        return ""
    try:
        recipients = json.loads(str(entitlement.get("recipients_json") or "[]"))
    except Exception:
        return ""
    if not isinstance(recipients, list):
        return ""
    normalized_email = _normalize_email(recipient_email)
    fallback_names: list[str] = []
    for item in recipients:
        if not isinstance(item, dict):
            continue
        name = _normalize_human_name(item.get("name"))
        if not name:
            continue
        item_email = _normalize_email(item.get("email"))
        if normalized_email and item_email == normalized_email:
            return name
        fallback_names.append(name)
    if len(fallback_names) == 1:
        return fallback_names[0]
    return ""


def _resolve_conversion_recipient_name(
    conn: sqlite3.Connection,
    *,
    subscriber_key: str,
    recipient_email: str,
    subscriber: dict[str, Any] | None,
) -> str:
    explicit = _resolve_explicit_recipient_name(
        conn,
        subscriber_key=subscriber_key,
        recipient_email=recipient_email,
        subscriber=subscriber,
    )
    if explicit:
        return explicit
    return _parse_name_from_email(recipient_email)


def _build_conversion_reply_cta(stripe_link: str) -> str:
    if _has_checkout_url(stripe_link):
        return (
            'Reply "go" if you\'d like me to confirm coverage first. '
            "To activate immediately, use the secure checkout link below."
        )
    return 'Reply "go" if you\'d like me to confirm coverage and send the activation details.'


def _build_conversion_questions_line() -> str:
    return "If you'd like me to double-check coverage first, reply with the metros you care about and I'll confirm them before you activate."


def _build_conversion_salutation(recipient_name: str) -> str:
    name = _normalize_human_name(recipient_name)
    if name:
        return f"Hi {name},"
    return "Hi,"


def _render_conversion_email_body_text(*, recipient_name: str, display_label: str, stripe_link: str) -> str:
    link = (stripe_link or "").strip() or "{stripe_link}"
    lines = [
        _build_conversion_salutation(recipient_name),
        "",
        _build_conversion_opener(display_label),
        "",
        "If you'd like to keep it running:",
        f"1. {_build_conversion_reply_cta(link)}",
        "2. Or activate here:",
        f"{CONVERSION_CHECKOUT_TEXT}: {link}",
        "",
        _build_conversion_questions_line(),
        "",
        "— Chase",
        "MicroFlowOps",
        "",
    ]
    return "\n".join(lines)


def render_conversion_email_html(
    *,
    recipient_name: str,
    display_label: str,
    stripe_link: str,
) -> str:
    name = (recipient_name or "").strip() or "{recipient_name}"
    link = (stripe_link or "").strip() or "{stripe_link}"
    parts = [
        "<!doctype html>",
        "<html><body>",
        f"<p>{escape(_build_conversion_salutation(name))}</p>",
        f"<p>{escape(_build_conversion_opener(display_label))}</p>",
        "<p>If you'd like to keep it running:</p>",
        "<ol>",
        f"<li>{escape(_build_conversion_reply_cta(link))}</li>",
        "<li>Or activate here:<br>",
    ]
    if _has_checkout_url(link):
        parts.append(f'<a href="{escape(link)}">{CONVERSION_CHECKOUT_TEXT}</a>')
    else:
        parts.append(f"{CONVERSION_CHECKOUT_TEXT}: {escape(link)}")
    parts.extend(
        [
            "</li>",
            "</ol>",
            f"<p>{escape(_build_conversion_questions_line())}</p>",
            "<p>— Chase<br>MicroFlowOps</p>",
            "</body></html>",
        ]
    )
    return "".join(parts)


def render_conversion_email_text(
    *,
    recipient_name: str,
    primary_recipient: str,
    display_label: str,
    stripe_link: str,
) -> str:
    name = (recipient_name or "").strip() or "{recipient_name}"
    recipient = (primary_recipient or "").strip().lower() or "{primary_recipient}"
    subject = _build_conversion_subject(display_label)
    body = _render_conversion_email_body_text(
        recipient_name=name,
        display_label=display_label,
        stripe_link=stripe_link,
    )
    return f"To: {recipient}\n\nSubject: {subject}\n\n{body}"


def _load_conversion_context(
    subscriber_key: str,
    crm_db_path: str | Path | None,
) -> tuple[Path, dict[str, Any], dict[str, Any], str, str, str]:
    sk = _validate_subscriber_key(subscriber_key)
    path = crm_light.resolve_crm_db_path(crm_db_path)
    if not path.exists():
        raise RuntimeError(f"CONFIG_ERROR crm_db missing path={path}")
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        sub = crm_light.get_subscriber(conn, sk)
        if not sub:
            raise RuntimeError(f"CONFIG_ERROR subscriber not found subscriber_key={sk}")
        trial = crm_light.get_trial_state(conn, sk)
        if not trial:
            raise RuntimeError(f"CONFIG_ERROR trial_state not found subscriber_key={sk}")
        recipient_email = str(sub.get("email") or "").strip().lower()
        recipient_name = _resolve_conversion_recipient_name(
            conn,
            subscriber_key=sk,
            recipient_email=recipient_email,
            subscriber=sub,
        )
    finally:
        conn.close()
    display_label = _resolve_conversion_display_label(str(sub.get("territory_code") or ""))
    return path, sub, trial, recipient_name, recipient_email, display_label


def write_conversion_draft(
    subscriber_key: str,
    crm_db_path: str | Path | None,
    emit_stdout: bool = True,
) -> Path:
    path, sub, trial, recipient_name, recipient_email, display_label = _load_conversion_context(
        subscriber_key=subscriber_key,
        crm_db_path=crm_db_path,
    )
    stripe_link = _resolve_conversion_url()
    text_body = render_conversion_email_text(
        recipient_name=recipient_name,
        primary_recipient=recipient_email,
        display_label=display_label,
        stripe_link=stripe_link,
    )
    html_body = render_conversion_email_html(
        recipient_name=recipient_name,
        display_label=display_label,
        stripe_link=stripe_link,
    )
    artifact_path = crm_light.data_dir() / "trials" / _validate_subscriber_key(subscriber_key) / "conversion_email.txt"
    html_artifact_path = artifact_path.with_suffix(".html")
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    artifact_path.write_text(text_body, encoding="utf-8")
    html_artifact_path.write_text(html_body, encoding="utf-8")
    if emit_stdout:
        print("OK conversion-draft")
        print(f"subscriber_key={_validate_subscriber_key(subscriber_key)}")
        print(f"crm_db={path}")
        print(f"start_date={str(trial.get('start_date') or '').strip()}")
        print(f"recipient_name={recipient_name}")
        print(f"territory_label={display_label or '{territory_label}'}")
        print(f"stripe_link={stripe_link or '{stripe_link}'}")
        print(f"conversion_path={artifact_path}")
        print(f"conversion_html_path={html_artifact_path}")
    return artifact_path


def _parse_ts_utc(value: str) -> str:
    raw = (value or "").strip()
    if not raw:
        raise ValueError("ts_utc required")
    candidate = raw[:-1] + "+00:00" if raw.upper().endswith("Z") else raw
    try:
        dt = datetime.fromisoformat(candidate)
    except Exception as exc:
        raise ValueError(f"invalid ts_utc: {exc}") from exc
    if dt.tzinfo is None or dt.utcoffset() is None:
        raise ValueError("ts_utc must include timezone offset or Z")
    return dt.astimezone(timezone.utc).isoformat()


def _ensure_schema(db_path: str, schema_path: str) -> None:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    row: Any = None
    try:
        conn.execute("PRAGMA foreign_keys = ON")
        tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}

        if "inspections" in tables:
            cols = {r[1] for r in conn.execute("PRAGMA table_info(inspections)")}
            if "lead_key" not in cols:
                conn.execute("ALTER TABLE inspections ADD COLUMN lead_key TEXT")
            if "area_office" not in cols:
                conn.execute("ALTER TABLE inspections ADD COLUMN area_office TEXT")
            if "record_hash" not in cols:
                conn.execute("ALTER TABLE inspections ADD COLUMN record_hash TEXT")
            if "changed_at" not in cols:
                conn.execute("ALTER TABLE inspections ADD COLUMN changed_at DATETIME")

        if "subscribers" in tables:
            subscriber_cols = {r[1] for r in conn.execute("PRAGMA table_info(subscribers)")}
            if "include_low_fallback" not in subscriber_cols:
                conn.execute("ALTER TABLE subscribers ADD COLUMN include_low_fallback INTEGER NOT NULL DEFAULT 0")
            if "recipients_json" not in subscriber_cols:
                conn.execute("ALTER TABLE subscribers ADD COLUMN recipients_json TEXT")
            if "last_sent_at" not in subscriber_cols:
                conn.execute("ALTER TABLE subscribers ADD COLUMN last_sent_at DATETIME")
            if "send_enabled" not in subscriber_cols:
                conn.execute("ALTER TABLE subscribers ADD COLUMN send_enabled INTEGER NOT NULL DEFAULT 0")

        schema_text = Path(schema_path).read_text(encoding="utf-8")
        conn.executescript(schema_text)
        conn.commit()
    finally:
        conn.close()


def _upsert_territory(conn: sqlite3.Connection, territory_code: str) -> None:
    defs = load_territory_definitions()
    canonical_code = resolve_territory_code(territory_code, defs)
    terr = defs.get(canonical_code) or defs.get(territory_code)
    if not terr:
        raise ValueError(f"unknown territory_code={territory_code}")
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
            canonical_code,
            str(terr.get("description") or territory_code),
            json.dumps(list(terr.get("states") or [])),
            json.dumps(list(terr.get("office_patterns") or [])),
            json.dumps(list(terr.get("fallback_city_patterns") or [])),
        ),
    )


def _upsert_leads_db_subscriber(conn: sqlite3.Connection, req: TrialAddRequest) -> None:
    display_name = req.subscriber_key
    trial_started_at = req.start_date
    trial_ends_at = None
    try:
        d0 = date.fromisoformat(req.start_date)
        trial_ends_at = (d0 + timedelta(days=14)).isoformat()
    except Exception:
        trial_ends_at = None
    recipients_json = json.dumps([req.email])
    conn.execute(
        """
        INSERT INTO subscribers
            (subscriber_key, display_name, email, recipients_json, territory_code, content_filter, include_low_fallback,
             trial_length_days, trial_started_at, trial_ends_at, active, send_enabled, send_time_local, timezone, customer_id)
        VALUES (?, ?, ?, ?, ?, 'high_medium', 1, 14, ?, ?, 1, 1, '08:00', ?, ?)
        ON CONFLICT(subscriber_key) DO UPDATE SET
            display_name=excluded.display_name,
            email=excluded.email,
            recipients_json=excluded.recipients_json,
            territory_code=excluded.territory_code,
            trial_started_at=excluded.trial_started_at,
            trial_ends_at=excluded.trial_ends_at,
            active=1,
            send_enabled=1,
            timezone=excluded.timezone
        """,
        (
            req.subscriber_key,
            display_name,
            req.email,
            recipients_json,
            req.territory_code,
            trial_started_at,
            trial_ends_at,
            req.tz,
            req.subscriber_key,
        ),
    )


def add_trial(
    req: TrialAddRequest,
    leads_db_path: str,
    schema_path: str,
    crm_db_path: str | Path | None,
) -> None:
    crm_light.ensure_database(crm_db_path)
    with crm_light.open_conn(crm_db_path) as conn:
        crm_light.init_schema(conn)
        crm_light.upsert_subscriber(
            conn,
            subscriber_key=req.subscriber_key,
            email=req.email,
            territory_code=req.territory_code,
            tz=req.tz,
            status="trial",
        )
        crm_light.upsert_trial_state(
            conn,
            subscriber_key=req.subscriber_key,
            start_date=req.start_date,
            sends_limit=req.sends_limit,
        )

    _ensure_schema(leads_db_path, schema_path)
    conn2 = sqlite3.connect(leads_db_path)
    try:
        conn2.execute("PRAGMA foreign_keys = ON")
        _upsert_territory(conn2, req.territory_code)
        _upsert_leads_db_subscriber(conn2, req)
        conn2.commit()
    finally:
        conn2.close()


def append_event(
    subscriber_key: str,
    status: str,
    variant: str,
    run_id: str,
    crm_db_path: str | Path | None,
    ts_utc: str = "",
    primary_recipient: str = "",
    send_mode: str = "",
    local_date: str = "",
    meta_source: str = "trial_admin_backfill",
) -> int:
    sk = _validate_subscriber_key(subscriber_key)
    if (ts_utc or "").strip():
        normalized_ts_utc = _parse_ts_utc(ts_utc)
    else:
        normalized_ts_utc = datetime.now(timezone.utc).isoformat(timespec="seconds")
    normalized_status = ((status or "").strip().upper() or "SENT")
    normalized_variant = ((variant or "").strip() or "DAILY")
    normalized_run_id = (
        (run_id or "").strip()
        or datetime.now(timezone.utc).strftime("backfill_%Y%m%d%H%M%S")
    )
    normalized_send_mode = (send_mode or "").strip().upper()
    normalized_primary_recipient = (primary_recipient or "").strip().lower()
    normalized_local_date = (local_date or "").strip()
    normalized_meta_source = (meta_source or "").strip() or "trial_admin_backfill"

    crm_light.ensure_database(crm_db_path)
    with crm_light.open_conn(crm_db_path) as conn:
        crm_light.init_schema(conn)
        sub = crm_light.get_subscriber(conn, sk)
        if not sub:
            raise ValueError(f"subscriber not found subscriber_key={sk}")
        if not normalized_primary_recipient:
            normalized_primary_recipient = str(sub.get("email") or "").strip().lower()
        if normalized_send_mode == "LIVE" and not normalized_local_date:
            tz_name = str(sub.get("tz") or "").strip() or "America/Chicago"
            dt_utc = datetime.fromisoformat(normalized_ts_utc)
            if ZoneInfo is not None:
                try:
                    normalized_local_date = dt_utc.astimezone(ZoneInfo(tz_name)).date().isoformat()
                except Exception:
                    normalized_local_date = dt_utc.date().isoformat()
            else:
                normalized_local_date = dt_utc.date().isoformat()
        event_meta: dict[str, Any] = {"source": normalized_meta_source}
        if normalized_send_mode:
            event_meta["send_mode"] = normalized_send_mode
        if normalized_primary_recipient:
            event_meta["primary_recipient"] = normalized_primary_recipient
        if normalized_local_date:
            event_meta["local_date"] = normalized_local_date
        event_id = crm_light.append_send_event(
            conn,
            subscriber_key=sk,
            variant=normalized_variant,
            status=normalized_status,
            run_id=normalized_run_id,
            meta=event_meta,
            ts_utc=normalized_ts_utc,
        )

    print("OK append-event")
    print(f"subscriber_key={sk}")
    print(f"status={normalized_status}")
    print(f"ts_utc={normalized_ts_utc}")
    print(f"variant={normalized_variant}")
    print(f"run_id={normalized_run_id}")
    if normalized_send_mode:
        print(f"send_mode={normalized_send_mode}")
    if normalized_primary_recipient:
        print(f"primary_recipient={normalized_primary_recipient}")
    if normalized_local_date:
        print(f"local_date={normalized_local_date}")
    print(f"event_id={event_id}")
    return 0


def show_trial(subscriber_key: str, crm_db_path: str | Path | None, recent: int) -> int:
    sk = _validate_subscriber_key(subscriber_key)
    resolved_db = crm_light.ensure_database(crm_db_path)
    print(f"crm_db={resolved_db}")
    with crm_light.open_conn(crm_db_path) as conn:
        crm_light.init_schema(conn)
        sub = crm_light.get_subscriber(conn, sk)
        if not sub:
            print(f"CONFIG_ERROR subscriber not found subscriber_key={sk}", file=sys.stderr)
            return 1
        trial = crm_light.get_trial_state(conn, sk)
        if not trial:
            print(f"CONFIG_ERROR trial_state not found subscriber_key={sk}", file=sys.stderr)
            return 1

        start_date = str(trial.get("start_date") or "").strip()
        sends_limit = _resolve_sends_limit_from_state(trial)
        default_sends_limit = _resolve_default_sends_limit()
        primary_recipient = str(sub.get("email") or "").strip().lower()
        tz_name = str(sub.get("tz") or "").strip() or "America/Chicago"
        sent_count = crm_light.count_trial_delivery_days(
            conn,
            sk,
            start_date,
            tz_name=tz_name,
            primary_recipient=primary_recipient,
            weekdays_only=True,
        )
        sent_rows_raw = crm_light.count_successful_sends(conn, sk, start_date)
        expired = sent_count >= sends_limit
        sends_remaining = max(0, int(sends_limit - sent_count))
        last_sent_at = crm_light.get_last_sent_at(conn, sk, start_date=start_date) or ""
        events = crm_light.get_recent_send_events(conn, sk, limit=max(1, int(recent or 10)))

    print(f"subscriber_key={sk}")
    print(f"email={str(sub.get('email') or '').strip()}")
    print(f"territory_code={str(sub.get('territory_code') or '').strip()}")
    print(f"subscriber_status={str(sub.get('status') or '').strip()}")
    print(f"start_date={start_date}")
    print(f"sends_limit={sends_limit}")
    print(f"effective_sends_limit={sends_limit}")
    print(f"default_sends_limit={default_sends_limit}")
    print(f"sent_count={sent_count}")
    print(f"sent_rows_raw={sent_rows_raw}")
    print(f"sends_remaining={sends_remaining}")
    print("expiry_basis=UNIQUE_WEEKDAY_SENT_DAYS")
    print(f"expired={'YES' if expired else 'NO'}")
    print(f"notified_at_utc={str(trial.get('notified_at_utc') or '').strip()}")
    print(f"ended_at_utc={str(trial.get('ended_at_utc') or '').strip()}")
    print(f"last_sent_at={last_sent_at}")
    if sent_rows_raw != sent_count:
        print("NOTE sent_rows_raw includes duplicates; expiry uses sent_count only")
    print(f"recent_events={len(events)}")
    for event in events:
        ts = str(event.get("ts_utc") or "").strip()
        status = str(event.get("status") or "").strip()
        variant = str(event.get("variant") or "").strip()
        print(f"event ts_utc={ts} status={status} variant={variant}")
    return 0


def list_trials(crm_db_path: str | Path | None, status_filter: str) -> int:
    sf = (status_filter or "").strip().lower()
    if sf and sf not in {"active", "expired"}:
        print("CONFIG_ERROR status must be active|expired", file=sys.stderr)
        return 1

    resolved_db = crm_light.ensure_database(crm_db_path)
    print(f"crm_db={resolved_db}")
    with crm_light.open_conn(crm_db_path) as conn:
        crm_light.init_schema(conn)
        rows = conn.execute(
            """
            SELECT s.subscriber_key, s.email, s.territory_code, s.status, s.tz, t.start_date, t.sends_limit, t.notified_at_utc, t.ended_at_utc
            FROM subscribers s
            JOIN trial_state t ON t.subscriber_key = s.subscriber_key
            ORDER BY s.subscriber_key ASC
            """
        ).fetchall()

        emitted = 0
        for row in rows:
            item = dict(row)
            key = str(item.get("subscriber_key") or "").strip().lower()
            start_date = str(item.get("start_date") or "").strip()
            sends_limit = _resolve_sends_limit_from_state(item)
            sent = crm_light.count_trial_delivery_days(
                conn,
                key,
                start_date,
                tz_name=str(item.get("tz") or "").strip() or "America/Chicago",
                primary_recipient=str(item.get("email") or "").strip().lower(),
                weekdays_only=True,
            )
            expired = sent >= sends_limit
            if sf == "active" and expired:
                continue
            if sf == "expired" and not expired:
                continue
            print(
                f"subscriber_key={key} territory_code={str(item.get('territory_code') or '').strip()} "
                f"sent={sent}/{sends_limit} expired={'YES' if expired else 'NO'}"
            )
            emitted += 1

    if emitted == 0:
        print("no_trials_found")
    return 0


def _today_in_new_york() -> date:
    if ZoneInfo is not None:
        return datetime.now(ZoneInfo("America/New_York")).date()
    return datetime.now(timezone.utc).date()


def _resolve_status_as_of(as_of: str | None) -> date:
    raw = (as_of or "").strip()
    if not raw:
        return _today_in_new_york()
    return date.fromisoformat(raw)


def build_trial_status(
    subscriber_key: str,
    crm_db_path: str | Path | None = None,
    as_of: str | None = None,
) -> tuple[dict[str, str], int]:
    sk = _validate_subscriber_key(subscriber_key)
    as_of_date = _resolve_status_as_of(as_of)
    path = crm_light.resolve_crm_db_path(crm_db_path)
    if not path.exists():
        raise RuntimeError(f"CONFIG_ERROR crm_db missing path={path}")

    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        sub = crm_light.get_subscriber(conn, sk)
        if not sub:
            raise RuntimeError(f"CONFIG_ERROR subscriber not found subscriber_key={sk}")
        trial = crm_light.get_trial_state(conn, sk)
        if not trial:
            raise RuntimeError(f"CONFIG_ERROR trial_state not found subscriber_key={sk}")

        start_date = str(trial.get("start_date") or "").strip()
        if not start_date:
            raise RuntimeError("CONFIG_ERROR start_date missing in trial_state")
        start = date.fromisoformat(start_date)
        sends_limit = _resolve_sends_limit_from_state(trial)
        sends_limit_raw = trial.get("sends_limit")
        primary_recipient = str(sub.get("email") or "").strip().lower()
        tz_name = str(sub.get("tz") or "").strip() or "America/Chicago"
        sends_used = _count_status_live_primary_weekdays(
            conn,
            subscriber_key=sk,
            start_date=start_date,
            tz_name=tz_name,
            primary_recipient=primary_recipient,
        )
        first_sent = crm_light.get_first_sent_at(conn, sk, start_date=start_date)
        last_sent = crm_light.get_last_sent_at(conn, sk, start_date=start_date)
    finally:
        conn.close()

    days_since = (as_of_date - start).days
    expired_by_sends = 1 if sends_used >= sends_limit else 0
    # Backward-compatible key: now means "14 successful sends elapsed".
    elapsed_14 = 1 if sends_used >= TRIAL_SENDS_TARGET else 0
    trial_expired = 1 if sends_used >= sends_limit else 0

    conversion_artifact = crm_light.data_dir() / "trials" / sk / "conversion_email.txt"
    conversion_exists = conversion_artifact.exists()
    if trial_expired and conversion_exists:
        next_hint = "manual_followup"
    elif trial_expired and not conversion_exists:
        next_hint = "send_conversion"
    else:
        next_hint = "continue_trial"

    ordered = {
        "TRIAL_SUBSCRIBER_KEY": sk,
        "TRIAL_START_DATE": start_date,
        "TRIAL_FIRST_SENT_UTC": first_sent or "NONE",
        "TRIAL_LAST_SENT_UTC": last_sent or "NONE",
        "TRIAL_DAYS_SINCE_START": str(days_since),
        "TRIAL_SENDS_USED": str(sends_used),
        "TRIAL_SENDS_LIMIT": str(int(sends_limit_raw)) if sends_limit_raw is not None else "NONE",
        "TRIAL_EXPIRED_BY_SENDS": str(expired_by_sends),
        "TRIAL_14_DAY_ELAPSED": str(elapsed_14),
        "TRIAL_NEXT_ACTION_HINT": next_hint,
        "TRIAL_EXPIRED": str(trial_expired),
    }
    return ordered, 0


def print_trial_status(
    subscriber_key: str,
    crm_db_path: str | Path | None = None,
    as_of: str | None = None,
) -> int:
    try:
        ordered, code = build_trial_status(
            subscriber_key=subscriber_key,
            crm_db_path=crm_db_path,
            as_of=as_of,
        )
    except Exception as exc:
        msg = str(exc)
        if msg.startswith("CONFIG_ERROR"):
            print(msg, file=sys.stderr)
        else:
            print(f"CONFIG_ERROR {msg}", file=sys.stderr)
        return 1
    for k, v in ordered.items():
        print(f"{k}={v}")
    return code


def _calendar_days_to_weekday_sends(days: int) -> int:
    n = int(days)
    if n < 0 or (n % 7) != 0:
        raise ValueError("ERR_TRIAL_EXTENSION_DAYS_NOT_MULTIPLE_OF_7")
    return int((n // 7) * 5)


def _resolve_customer_config_for_subscriber(subscriber_key: str, explicit_path: str = "") -> tuple[Path | None, dict[str, Any]]:
    raw = (explicit_path or "").strip()
    if raw:
        path = Path(raw)
        if path.exists():
            try:
                return path, json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                return path, {}
        return path, {}

    candidates = [
        Path("customers") / f"{subscriber_key}.json",
        Path("customers") / f"{subscriber_key}_trial.json",
        Path("customers") / "wally_trial_tx_triangle_v1.json",
    ]
    for path in candidates:
        if not path.exists():
            continue
        try:
            cfg = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if str(cfg.get("subscriber_key") or "").strip().lower() == subscriber_key:
            return path, cfg
    customers_dir = Path("customers")
    if customers_dir.exists():
        for path in sorted(customers_dir.glob("*.json")):
            try:
                cfg = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                continue
            if str(cfg.get("subscriber_key") or "").strip().lower() == subscriber_key:
                return path, cfg
    return None, {}


def _collect_customer_recipients(config: dict[str, Any], fallback_email: str) -> list[str]:
    values = config.get("recipients") or config.get("email_recipients") or []
    out: list[str] = []
    seen: set[str] = set()
    if isinstance(values, list):
        for item in values:
            email = str(item or "").strip().lower()
            if email and email not in seen:
                seen.add(email)
                out.append(email)
    fallback = str(fallback_email or "").strip().lower()
    if not out and fallback:
        out.append(fallback)
    return out


def _scope_enhancement_latch_key(subscriber_key: str, from_date: str, to_date: str) -> str:
    return f"scope_enhancement|subscriber={subscriber_key}|from={from_date}|to={to_date}"


def _scope_enhancement_subject() -> str:
    return "Texas Triangle coverage update — trial extended 7 days"


def _scope_rows_for_email(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    def _key(row: dict[str, Any]) -> tuple[str, str, str]:
        event_date = str(row.get("event_date") or "").strip()
        observed = str(row.get("observed_at_local") or "").strip()
        activity = str(row.get("activity_nr") or "").strip()
        return (event_date, observed, activity)

    filtered = [
        dict(row)
        for row in list(rows or [])
        if str(row.get("tier") or "").strip().lower() in {"high", "medium"}
    ]
    filtered.sort(key=_key, reverse=True)
    return filtered


def generate_missed_signals_report(
    *,
    subscriber_key: str,
    leads_db_path: str,
    crm_db_path: str | Path | None,
    from_date: str,
    to_date: str,
    customer_config_path: str = "",
) -> dict[str, Any]:
    sk = _validate_subscriber_key(subscriber_key)
    start = date.fromisoformat((from_date or "").strip())
    end = date.fromisoformat((to_date or "").strip())
    if end < start:
        raise ValueError("to_date must be >= from_date")
    crm_light.ensure_database(crm_db_path)
    with crm_light.open_conn(crm_db_path) as conn:
        crm_light.init_schema(conn)
        sub = crm_light.get_subscriber(conn, sk)
        trial = crm_light.get_trial_state(conn, sk)
    if not sub or not trial:
        raise ValueError(f"subscriber/trial missing subscriber_key={sk}")

    cfg_path, customer_cfg = _resolve_customer_config_for_subscriber(sk, explicit_path=customer_config_path)
    territory_raw = (
        str(sub.get("territory_code") or "").strip()
        or str(customer_cfg.get("territory_code") or "TX_TRI").strip()
    )
    territory_code = _normalize_territory(territory_raw)
    states = [
        str(item).strip().upper()
        for item in (customer_cfg.get("states") or ["TX"])
        if str(item).strip()
    ]
    if not states:
        states = ["TX"]
    tz_name = str(sub.get("tz") or "").strip() or "America/Chicago"
    primary_recipient = str(sub.get("email") or "").strip().lower()

    expected_by_key = trial_audit.collect_expected_signals_for_range(
        leads_db_path=leads_db_path,
        territory_code=territory_code,
        states=states,
        start_date=start.isoformat(),
        end_date=end.isoformat(),
    )
    delivered_keys, delivery_meta = trial_audit.collect_delivered_keys_for_range(
        repo_root=Path(__file__).resolve().parent,
        leads_db_path=leads_db_path,
        subscriber_key=sk,
        primary_recipient=primary_recipient,
        tz_name=tz_name,
        customer_config=customer_cfg,
        start_date=start.isoformat(),
        end_date=end.isoformat(),
        crm_db_path=crm_db_path,
        data_root=Path(__file__).resolve().parent / "out",
    )
    missed_rows = trial_audit.build_missed_signal_rows(
        expected_by_key=expected_by_key,
        delivered_keys=delivered_keys,
        tz_name=tz_name,
    )
    out_dir = crm_light.data_dir() / "trials" / sk / "audit"
    csv_path, txt_path = trial_audit.write_missed_signals_artifacts(
        out_dir=out_dir,
        start_date=start.isoformat(),
        end_date=end.isoformat(),
        missed_rows=missed_rows,
        expected_total=len(expected_by_key),
        delivered_total=len(delivered_keys),
    )
    return {
        "subscriber_key": sk,
        "territory_code": territory_code,
        "from_date": start.isoformat(),
        "to_date": end.isoformat(),
        "expected_total": len(expected_by_key),
        "delivered_total": len(delivered_keys),
        "missed_total": len(missed_rows),
        "missed_rows": missed_rows,
        "delivery_meta": delivery_meta,
        "csv_path": csv_path,
        "txt_path": txt_path,
        "customer_config_path": cfg_path,
        "customer_config": customer_cfg,
        "primary_recipient": primary_recipient,
        "recipients": _collect_customer_recipients(customer_cfg, primary_recipient),
        "tz_name": tz_name,
    }


def extend_all_trials(
    *,
    days: int,
    reason: str,
    crm_db_path: str | Path | None,
) -> dict[str, Any]:
    delta = _calendar_days_to_weekday_sends(days)
    normalized_reason = str(reason or "").strip()
    if not normalized_reason:
        raise ValueError("reason required")
    crm_light.ensure_database(crm_db_path)
    applied = 0
    skipped_expired = 0
    skipped_idempotent = 0
    scanned = 0
    with crm_light.open_conn(crm_db_path) as conn:
        crm_light.init_schema(conn)
        rows = conn.execute(
            """
            SELECT s.subscriber_key, s.email, s.tz, t.start_date, t.sends_limit
            FROM subscribers s
            JOIN trial_state t ON t.subscriber_key = s.subscriber_key
            WHERE lower(trim(s.status)) = 'trial'
            ORDER BY s.subscriber_key ASC
            """
        ).fetchall()
        for row in rows:
            scanned += 1
            sk = str(row["subscriber_key"] or "").strip().lower()
            start_date = str(row["start_date"] or "").strip()
            sends_limit = _resolve_sends_limit_from_state(dict(row))
            sent_count = crm_light.count_trial_delivery_days(
                conn,
                sk,
                start_date,
                tz_name=str(row["tz"] or "").strip() or "America/Chicago",
                primary_recipient=str(row["email"] or "").strip().lower(),
                weekdays_only=True,
            )
            if sent_count >= sends_limit:
                skipped_expired += 1
                continue
            adjustment_key = f"extend_all_trials|reason={normalized_reason}"
            inserted = crm_light.record_trial_adjustment_once(
                conn,
                subscriber_key=sk,
                adjustment_key=adjustment_key,
                adjustment_type="EXTEND_ALL_TRIALS",
                delta_sends=delta,
                reason=normalized_reason,
                meta={
                    "days": int(days),
                    "weekday_delta": delta,
                    "old_sends_limit": sends_limit,
                    "sent_count": sent_count,
                },
                commit=False,
            )
            if not inserted:
                skipped_idempotent += 1
                continue
            conn.execute(
                "UPDATE trial_state SET sends_limit = ? WHERE subscriber_key = ?",
                (int(sends_limit + delta), sk),
            )
            applied += 1
        conn.commit()
    return {
        "days": int(days),
        "weekday_delta": delta,
        "reason": normalized_reason,
        "scanned": scanned,
        "applied": applied,
        "skipped_expired": skipped_expired,
        "skipped_idempotent": skipped_idempotent,
    }


def _event_meta_dict(raw_json: str) -> dict[str, Any]:
    text = str(raw_json or "").strip()
    if not text:
        return {}
    try:
        payload = json.loads(text)
    except Exception:
        return {}
    if isinstance(payload, dict):
        return payload
    return {}


def _scope_window_from_meta(raw_meta_json: str) -> tuple[str, str]:
    meta = _event_meta_dict(raw_meta_json)
    return (
        str(meta.get("from_date") or "").strip(),
        str(meta.get("to_date") or "").strip(),
    )


def _has_custom_limit_adjustment(conn: sqlite3.Connection, subscriber_key: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM trial_adjustments
        WHERE subscriber_key = ?
          AND (
              lower(trim(adjustment_type)) = 'custom_limit'
              OR lower(trim(reason)) LIKE 'custom_limit%'
              OR lower(trim(adjustment_key)) LIKE 'custom_limit%'
          )
        LIMIT 1
        """,
        (str(subscriber_key or "").strip().lower(),),
    ).fetchone()
    return row is not None


def normalize_trials(*, apply: bool, crm_db_path: str | Path | None) -> dict[str, int]:
    default_limit = _resolve_default_sends_limit()
    crm_light.ensure_database(crm_db_path)
    updated_limits = 0
    superseded_events = 0
    skipped = 0

    with crm_light.open_conn(crm_db_path) as conn:
        crm_light.init_schema(conn)
        trial_rows = conn.execute(
            """
            SELECT s.subscriber_key, t.sends_limit
            FROM subscribers s
            JOIN trial_state t ON t.subscriber_key = s.subscriber_key
            WHERE lower(trim(s.status)) = 'trial'
            ORDER BY s.subscriber_key ASC
            """
        ).fetchall()

        for row in trial_rows:
            sk = str(row["subscriber_key"] or "").strip().lower()
            current_limit = _resolve_sends_limit_from_state(dict(row))
            if current_limit >= default_limit:
                skipped += 1
                continue
            if _has_custom_limit_adjustment(conn, sk):
                skipped += 1
                continue
            delta = int(default_limit - current_limit)
            if delta <= 0:
                skipped += 1
                continue

            inserted = True
            if apply:
                inserted = crm_light.record_trial_adjustment_once(
                    conn,
                    subscriber_key=sk,
                    adjustment_key="normalize_to_default|reason=normalize_to_default",
                    adjustment_type="NORMALIZE_TO_DEFAULT",
                    delta_sends=delta,
                    reason="normalize_to_default",
                    meta={
                        "old_sends_limit": int(current_limit),
                        "new_sends_limit": int(default_limit),
                        "default_sends_limit": int(default_limit),
                    },
                    commit=False,
                )
            if inserted:
                updated_limits += 1
                if apply:
                    conn.execute(
                        "UPDATE trial_state SET sends_limit = ? WHERE subscriber_key = ?",
                        (int(current_limit + delta), sk),
                    )
            else:
                skipped += 1

        sent_rows = conn.execute(
            """
            SELECT se.id, se.subscriber_key, se.ts_utc, se.meta_json
            FROM send_events se
            JOIN subscribers s ON s.subscriber_key = se.subscriber_key
            WHERE lower(trim(s.status)) = 'trial'
              AND se.variant = 'SCOPE_ENHANCEMENT'
              AND se.status = 'SCOPE_ENHANCEMENT_SENT'
            ORDER BY se.subscriber_key ASC, se.ts_utc DESC, se.id DESC
            """
        ).fetchall()

        grouped: dict[tuple[str, str, str], list[sqlite3.Row]] = {}
        for row in sent_rows:
            sk = str(row["subscriber_key"] or "").strip().lower()
            from_date, to_date = _scope_window_from_meta(str(row["meta_json"] or ""))
            grouped.setdefault((sk, from_date, to_date), []).append(row)

        superseded_ids: list[int] = []
        for key_rows in grouped.values():
            if len(key_rows) <= 1:
                continue
            keep_id = int(max(key_rows, key=lambda item: (str(item["ts_utc"] or ""), int(item["id"])))["id"])
            for row in key_rows:
                row_id = int(row["id"])
                if row_id != keep_id:
                    superseded_ids.append(row_id)

        superseded_events = len(superseded_ids)
        if apply and superseded_ids:
            conn.executemany(
                "UPDATE send_events SET status = 'SUPERSEDED' WHERE id = ?",
                [(row_id,) for row_id in superseded_ids],
            )

        if apply:
            conn.commit()

    return {
        "updated_limits": int(updated_limits),
        "superseded_events": int(superseded_events),
        "skipped": int(skipped),
    }


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name = ? LIMIT 1",
        (str(table_name or "").strip(),),
    ).fetchone()
    return row is not None


def _table_has_column(conn: sqlite3.Connection, table_name: str, column_name: str) -> bool:
    try:
        rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    except Exception:
        return False
    wanted = str(column_name or "").strip().lower()
    return any(str(row["name"] if isinstance(row, sqlite3.Row) else row[1]).strip().lower() == wanted for row in rows)


def _min_non_empty_text(*values: str) -> str:
    items = [str(v or "").strip() for v in values if str(v or "").strip()]
    if not items:
        return ""
    return min(items)


def _max_optional_int(a: int | None, b: int | None) -> int | None:
    values: list[int] = []
    if a is not None:
        values.append(int(a))
    if b is not None:
        values.append(int(b))
    if not values:
        return None
    return max(values)


def _resolve_reconcile_subscriber_keys(
    source_conn: sqlite3.Connection,
    *,
    scope: str,
    explicit_keys: list[str],
) -> list[str]:
    normalized_scope = str(scope or "").strip().lower() or "all"
    if normalized_scope not in {"all", "active_prod", "explicit"}:
        raise ValueError("scope must be one of: all,active_prod,explicit")

    if normalized_scope == "explicit":
        keys: list[str] = []
        seen: set[str] = set()
        for item in list(explicit_keys or []):
            sk = _validate_subscriber_key(str(item or ""))
            if sk in seen:
                continue
            seen.add(sk)
            keys.append(sk)
        if not keys:
            raise ValueError("subscriber-key required when scope=explicit")
        return sorted(keys)

    keys_set: set[str] = set()

    if normalized_scope == "active_prod":
        if _table_exists(source_conn, "subscribers"):
            rows = source_conn.execute(
                """
                SELECT subscriber_key
                FROM subscribers
                WHERE lower(trim(status)) NOT IN ('trial', 'inactive', 'disabled', 'cancelled', 'canceled')
                """
            ).fetchall()
            for row in rows:
                sk = str(row["subscriber_key"] or "").strip().lower()
                if sk:
                    keys_set.add(sk)
        return sorted(keys_set)

    if _table_exists(source_conn, "subscribers"):
        rows = source_conn.execute("SELECT subscriber_key FROM subscribers").fetchall()
        for row in rows:
            sk = str(row["subscriber_key"] or "").strip().lower()
            if sk:
                keys_set.add(sk)
    if _table_exists(source_conn, "trial_state"):
        rows = source_conn.execute("SELECT subscriber_key FROM trial_state").fetchall()
        for row in rows:
            sk = str(row["subscriber_key"] or "").strip().lower()
            if sk:
                keys_set.add(sk)
    if _table_exists(source_conn, "send_events"):
        rows = source_conn.execute("SELECT DISTINCT subscriber_key FROM send_events").fetchall()
        for row in rows:
            sk = str(row["subscriber_key"] or "").strip().lower()
            if sk:
                keys_set.add(sk)
    if _table_exists(source_conn, "trial_adjustments"):
        rows = source_conn.execute("SELECT DISTINCT subscriber_key FROM trial_adjustments").fetchall()
        for row in rows:
            sk = str(row["subscriber_key"] or "").strip().lower()
            if sk:
                keys_set.add(sk)
    if _table_exists(source_conn, "trial_latches"):
        rows = source_conn.execute("SELECT DISTINCT subscriber_key FROM trial_latches").fetchall()
        for row in rows:
            sk = str(row["subscriber_key"] or "").strip().lower()
            if sk:
                keys_set.add(sk)
    return sorted(keys_set)


def reconcile_ledgers(
    *,
    source_crm_db_path: str | Path,
    target_crm_db_path: str | Path,
    scope: str,
    subscriber_keys: list[str],
    apply: bool,
    trial_state_merge: str = "max",
    emit_tokens: bool = True,
) -> int:
    source_path = Path(str(source_crm_db_path)).expanduser().resolve(strict=False)
    target_path = crm_light.resolve_crm_db_path(str(target_crm_db_path or "").strip() or None)
    if not source_path.exists():
        raise ValueError(f"source crm db missing path={source_path}")
    merge_mode = str(trial_state_merge or "max").strip().lower() or "max"
    if merge_mode not in {"max", "source"}:
        raise ValueError("trial_state_merge must be one of: max,source")

    crm_light.ensure_database(target_path)

    source_conn = sqlite3.connect(f"file:{source_path}?mode=ro", uri=True)
    source_conn.row_factory = sqlite3.Row
    try:
        target_conn = sqlite3.connect(str(target_path))
        target_conn.row_factory = sqlite3.Row
        try:
            target_conn.execute("PRAGMA foreign_keys = ON")
            crm_light.init_schema(target_conn)

            keys = _resolve_reconcile_subscriber_keys(
                source_conn,
                scope=scope,
                explicit_keys=list(subscriber_keys or []),
            )
            source_sub_cache: dict[str, sqlite3.Row | None] = {}

            counts: dict[str, dict[str, int]] = {
                "subscribers": {"inserted": 0, "skipped": 0},
                "trial_state": {"inserted": 0, "skipped": 0},
                "send_events": {"inserted": 0, "skipped": 0},
                "trial_adjustments": {"inserted": 0, "skipped": 0},
                "trial_latches": {"inserted": 0, "skipped": 0},
            }

            def _source_subscriber(sk: str) -> sqlite3.Row | None:
                if sk in source_sub_cache:
                    return source_sub_cache[sk]
                row = None
                if _table_exists(source_conn, "subscribers"):
                    row = source_conn.execute(
                        """
                        SELECT subscriber_key, email, territory_code, tz, created_at_utc, status
                        FROM subscribers
                        WHERE subscriber_key = ?
                        LIMIT 1
                        """,
                        (sk,),
                    ).fetchone()
                source_sub_cache[sk] = row
                return row

            def _target_has_subscriber(sk: str) -> bool:
                row = target_conn.execute(
                    "SELECT 1 FROM subscribers WHERE subscriber_key = ? LIMIT 1",
                    (sk,),
                ).fetchone()
                return row is not None

            def _ensure_target_subscriber(sk: str) -> bool:
                if _target_has_subscriber(sk):
                    return True
                row = _source_subscriber(sk)
                if row is None:
                    return False
                if apply:
                    target_conn.execute(
                        """
                        INSERT OR IGNORE INTO subscribers
                            (subscriber_key, email, territory_code, tz, created_at_utc, status)
                        VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (
                            str(row["subscriber_key"] or "").strip().lower(),
                            str(row["email"] or "").strip().lower(),
                            str(row["territory_code"] or "").strip().upper(),
                            str(row["tz"] or "").strip(),
                            str(row["created_at_utc"] or "").strip() or datetime.now(timezone.utc).isoformat(),
                            str(row["status"] or "").strip() or "trial",
                        ),
                    )
                return True

            # subscribers: insert missing only
            for sk in keys:
                src = _source_subscriber(sk)
                if src is None:
                    counts["subscribers"]["skipped"] += 1
                    continue
                exists = _target_has_subscriber(sk)
                if exists:
                    counts["subscribers"]["skipped"] += 1
                    continue
                counts["subscribers"]["inserted"] += 1
                if apply:
                    target_conn.execute(
                        """
                        INSERT INTO subscribers
                            (subscriber_key, email, territory_code, tz, created_at_utc, status)
                        VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (
                            str(src["subscriber_key"] or "").strip().lower(),
                            str(src["email"] or "").strip().lower(),
                            str(src["territory_code"] or "").strip().upper(),
                            str(src["tz"] or "").strip(),
                            str(src["created_at_utc"] or "").strip() or datetime.now(timezone.utc).isoformat(),
                            str(src["status"] or "").strip() or "trial",
                        ),
                    )

            # trial_state: deterministic merge
            if _table_exists(source_conn, "trial_state"):
                for sk in keys:
                    src = source_conn.execute(
                        """
                        SELECT subscriber_key, start_date, sends_limit, notified_at_utc, ended_at_utc
                        FROM trial_state
                        WHERE subscriber_key = ?
                        LIMIT 1
                        """,
                        (sk,),
                    ).fetchone()
                    if src is None:
                        counts["trial_state"]["skipped"] += 1
                        continue
                    if not _ensure_target_subscriber(sk):
                        counts["trial_state"]["skipped"] += 1
                        continue

                    dst = target_conn.execute(
                        """
                        SELECT subscriber_key, start_date, sends_limit, notified_at_utc, ended_at_utc
                        FROM trial_state
                        WHERE subscriber_key = ?
                        LIMIT 1
                        """,
                        (sk,),
                    ).fetchone()
                    src_start = str(src["start_date"] or "").strip()
                    src_limit = int(src["sends_limit"]) if src["sends_limit"] is not None else None
                    src_notified = str(src["notified_at_utc"] or "").strip()
                    src_ended = str(src["ended_at_utc"] or "").strip()

                    dst_start = str(dst["start_date"] or "").strip() if dst is not None else ""
                    dst_limit = int(dst["sends_limit"]) if (dst is not None and dst["sends_limit"] is not None) else None
                    dst_notified = str(dst["notified_at_utc"] or "").strip() if dst is not None else ""
                    dst_ended = str(dst["ended_at_utc"] or "").strip() if dst is not None else ""

                    if merge_mode == "source":
                        merged_start = src_start
                        merged_limit = src_limit
                        merged_notified = src_notified
                        merged_ended = src_ended
                    else:
                        merged_start = dst_start or src_start
                        merged_limit = _max_optional_int(dst_limit, src_limit)
                        merged_notified = _min_non_empty_text(dst_notified, src_notified)
                        merged_ended = _min_non_empty_text(dst_ended, src_ended)

                    unchanged = (
                        dst is not None
                        and dst_start == merged_start
                        and dst_limit == merged_limit
                        and dst_notified == merged_notified
                        and dst_ended == merged_ended
                    )
                    if unchanged:
                        counts["trial_state"]["skipped"] += 1
                        continue

                    counts["trial_state"]["inserted"] += 1
                    if apply:
                        target_conn.execute(
                            """
                            INSERT INTO trial_state
                                (subscriber_key, start_date, sends_limit, notified_at_utc, ended_at_utc)
                            VALUES (?, ?, ?, ?, ?)
                            ON CONFLICT(subscriber_key) DO UPDATE SET
                                start_date=excluded.start_date,
                                sends_limit=excluded.sends_limit,
                                notified_at_utc=excluded.notified_at_utc,
                                ended_at_utc=excluded.ended_at_utc
                            """,
                            (sk, merged_start, merged_limit, merged_notified or None, merged_ended or None),
                        )

            # send_events: insert-if-missing by natural dedupe tuple
            if _table_exists(source_conn, "send_events"):
                src_has_recipient = _table_has_column(source_conn, "send_events", "recipient_email")
                dst_has_recipient = _table_has_column(target_conn, "send_events", "recipient_email")
                for sk in keys:
                    select_cols = "id, subscriber_key, ts_utc, variant, status, run_id, meta_json"
                    if src_has_recipient:
                        select_cols = "id, subscriber_key, recipient_email, ts_utc, variant, status, run_id, meta_json"
                    rows = source_conn.execute(
                        f"""
                        SELECT {select_cols}
                        FROM send_events
                        WHERE subscriber_key = ?
                        ORDER BY id ASC
                        """,
                        (sk,),
                    ).fetchall()
                    if not rows:
                        continue
                    if not _ensure_target_subscriber(sk):
                        counts["send_events"]["skipped"] += len(rows)
                        continue
                    for row in rows:
                        recipient = str(row["recipient_email"] or "").strip().lower() if src_has_recipient else ""
                        if dst_has_recipient:
                            existing = target_conn.execute(
                                """
                                SELECT 1
                                FROM send_events
                                WHERE subscriber_key = ?
                                  AND recipient_email = ?
                                  AND ts_utc = ?
                                  AND variant = ?
                                  AND status = ?
                                  AND run_id = ?
                                LIMIT 1
                                """,
                                (
                                    sk,
                                    recipient,
                                    str(row["ts_utc"] or "").strip(),
                                    str(row["variant"] or "").strip(),
                                    str(row["status"] or "").strip(),
                                    str(row["run_id"] or "").strip(),
                                ),
                            ).fetchone()
                        else:
                            existing = target_conn.execute(
                                """
                                SELECT 1
                                FROM send_events
                                WHERE subscriber_key = ?
                                  AND ts_utc = ?
                                  AND variant = ?
                                  AND status = ?
                                  AND run_id = ?
                                LIMIT 1
                                """,
                                (
                                    sk,
                                    str(row["ts_utc"] or "").strip(),
                                    str(row["variant"] or "").strip(),
                                    str(row["status"] or "").strip(),
                                    str(row["run_id"] or "").strip(),
                                ),
                            ).fetchone()
                        if existing is not None:
                            counts["send_events"]["skipped"] += 1
                            continue
                        counts["send_events"]["inserted"] += 1
                        if apply:
                            if dst_has_recipient:
                                target_conn.execute(
                                    """
                                    INSERT INTO send_events
                                        (subscriber_key, recipient_email, ts_utc, variant, status, run_id, meta_json)
                                    VALUES (?, ?, ?, ?, ?, ?, ?)
                                    """,
                                    (
                                        sk,
                                        recipient,
                                        str(row["ts_utc"] or "").strip(),
                                        str(row["variant"] or "").strip(),
                                        str(row["status"] or "").strip(),
                                        str(row["run_id"] or "").strip(),
                                        str(row["meta_json"] or "").strip() or "{}",
                                    ),
                                )
                            else:
                                target_conn.execute(
                                    """
                                    INSERT INTO send_events
                                        (subscriber_key, ts_utc, variant, status, run_id, meta_json)
                                    VALUES (?, ?, ?, ?, ?, ?)
                                    """,
                                    (
                                        sk,
                                        str(row["ts_utc"] or "").strip(),
                                        str(row["variant"] or "").strip(),
                                        str(row["status"] or "").strip(),
                                        str(row["run_id"] or "").strip(),
                                        str(row["meta_json"] or "").strip() or "{}",
                                    ),
                                )

            # trial_adjustments: insert-if-missing by (subscriber_key, adjustment_key)
            if _table_exists(source_conn, "trial_adjustments"):
                for sk in keys:
                    rows = source_conn.execute(
                        """
                        SELECT subscriber_key, adjustment_key, adjustment_type, delta_sends, reason, meta_json, created_at_utc
                        FROM trial_adjustments
                        WHERE subscriber_key = ?
                        ORDER BY id ASC
                        """,
                        (sk,),
                    ).fetchall()
                    if not rows:
                        continue
                    if not _ensure_target_subscriber(sk):
                        counts["trial_adjustments"]["skipped"] += len(rows)
                        continue
                    for row in rows:
                        key = str(row["adjustment_key"] or "").strip()
                        existing = target_conn.execute(
                            """
                            SELECT 1
                            FROM trial_adjustments
                            WHERE subscriber_key = ? AND adjustment_key = ?
                            LIMIT 1
                            """,
                            (sk, key),
                        ).fetchone()
                        if existing is not None:
                            counts["trial_adjustments"]["skipped"] += 1
                            continue
                        counts["trial_adjustments"]["inserted"] += 1
                        if apply:
                            target_conn.execute(
                                """
                                INSERT INTO trial_adjustments
                                    (subscriber_key, adjustment_key, adjustment_type, delta_sends, reason, meta_json, created_at_utc)
                                VALUES (?, ?, ?, ?, ?, ?, ?)
                                """,
                                (
                                    sk,
                                    key,
                                    str(row["adjustment_type"] or "").strip(),
                                    int(row["delta_sends"] or 0),
                                    str(row["reason"] or "").strip(),
                                    str(row["meta_json"] or "").strip() or "{}",
                                    str(row["created_at_utc"] or "").strip() or datetime.now(timezone.utc).isoformat(),
                                ),
                            )

            # trial_latches: insert-if-missing by latch_key
            if _table_exists(source_conn, "trial_latches"):
                for sk in keys:
                    rows = source_conn.execute(
                        """
                        SELECT latch_key, subscriber_key, action, meta_json, created_at_utc
                        FROM trial_latches
                        WHERE subscriber_key = ?
                        ORDER BY latch_key ASC
                        """,
                        (sk,),
                    ).fetchall()
                    if not rows:
                        continue
                    if not _ensure_target_subscriber(sk):
                        counts["trial_latches"]["skipped"] += len(rows)
                        continue
                    for row in rows:
                        latch_key = str(row["latch_key"] or "").strip()
                        existing = target_conn.execute(
                            "SELECT 1 FROM trial_latches WHERE latch_key = ? LIMIT 1",
                            (latch_key,),
                        ).fetchone()
                        if existing is not None:
                            counts["trial_latches"]["skipped"] += 1
                            continue
                        counts["trial_latches"]["inserted"] += 1
                        if apply:
                            target_conn.execute(
                                """
                                INSERT INTO trial_latches
                                    (latch_key, subscriber_key, action, meta_json, created_at_utc)
                                VALUES (?, ?, ?, ?, ?)
                                """,
                                (
                                    latch_key,
                                    sk,
                                    str(row["action"] or "").strip(),
                                    str(row["meta_json"] or "").strip() or "{}",
                                    str(row["created_at_utc"] or "").strip() or datetime.now(timezone.utc).isoformat(),
                                ),
                            )

            if apply:
                target_conn.commit()

            if emit_tokens:
                print(f"RECONCILE_LEDGERS_SOURCE_DB={source_path}")
                print(f"RECONCILE_LEDGERS_TARGET_DB={target_path}")
                print(f"RECONCILE_LEDGERS_SCOPE={str(scope or '').strip().lower() or 'all'}")
                print(f"RECONCILE_LEDGERS_TRIAL_STATE_MERGE={merge_mode}")
                print(f"RECONCILE_LEDGERS_SUBSCRIBERS_SCANNED={len(keys)}")
                print(f"RECONCILE_LEDGERS_SUBSCRIBERS inserted={counts['subscribers']['inserted']} skipped={counts['subscribers']['skipped']}")
                print(f"RECONCILE_LEDGERS_TRIAL_STATE inserted={counts['trial_state']['inserted']} skipped={counts['trial_state']['skipped']}")
                print(f"RECONCILE_LEDGERS_SEND_EVENTS inserted={counts['send_events']['inserted']} skipped={counts['send_events']['skipped']}")
                print(
                    "RECONCILE_LEDGERS_TRIAL_ADJUSTMENTS "
                    f"inserted={counts['trial_adjustments']['inserted']} skipped={counts['trial_adjustments']['skipped']}"
                )
                print(
                    "RECONCILE_LEDGERS_TRIAL_LATCHES "
                    f"inserted={counts['trial_latches']['inserted']} skipped={counts['trial_latches']['skipped']}"
                )
                print(f"RECONCILE_LEDGERS_MODE={'APPLY' if apply else 'DRY_RUN'}")
            return 0
        finally:
            target_conn.close()
    finally:
        source_conn.close()


def set_trial_limit(
    *,
    subscriber_key: str,
    sends_limit: int,
    reason: str,
    crm_db_path: str | Path | None,
    apply: bool,
) -> int:
    sk = _validate_subscriber_key(subscriber_key)
    new_limit = int(sends_limit)
    if new_limit < 1:
        raise ValueError("sends_limit must be >= 1")
    normalized_reason = str(reason or "").strip()
    if not normalized_reason:
        raise ValueError("reason required")
    adjustment_key = f"set_trial_limit|reason={normalized_reason}"

    resolved_db = crm_light.ensure_database(crm_db_path)
    with crm_light.open_conn(resolved_db) as conn:
        crm_light.init_schema(conn)
        sub = crm_light.get_subscriber(conn, sk)
        if not sub:
            raise ValueError(f"subscriber not found subscriber_key={sk}")
        trial = crm_light.get_trial_state(conn, sk)
        if not trial:
            raise ValueError(f"trial_state not found subscriber_key={sk}")
        current_limit = _resolve_sends_limit_from_state(trial)
        idempotent = crm_light.has_trial_adjustment(
            conn,
            subscriber_key=sk,
            adjustment_key=adjustment_key,
        )
        would_apply = (not idempotent) and (current_limit != new_limit)
        if apply and (not idempotent):
            crm_light.record_trial_adjustment_once(
                conn,
                subscriber_key=sk,
                adjustment_key=adjustment_key,
                adjustment_type="SET_TRIAL_LIMIT",
                delta_sends=int(new_limit - current_limit),
                reason=normalized_reason,
                meta={
                    "old_sends_limit": int(current_limit),
                    "new_sends_limit": int(new_limit),
                    "reason": normalized_reason,
                },
                commit=False,
            )
            conn.execute(
                "UPDATE trial_state SET sends_limit = ? WHERE subscriber_key = ?",
                (int(new_limit), sk),
            )
            conn.commit()

    mirror_status = "SKIP"
    mirror_secondary = (Path(__file__).resolve().parent / "out" / "crm_light.sqlite").resolve(strict=False)
    if apply and str(os.getenv("MFO_DATA_DIR_EFFECTIVE") or "").strip():
        if mirror_secondary.exists() and mirror_secondary != Path(resolved_db).resolve(strict=False):
            try:
                reconcile_ledgers(
                    source_crm_db_path=resolved_db,
                    target_crm_db_path=mirror_secondary,
                    scope="explicit",
                    subscriber_keys=[sk],
                    apply=True,
                    trial_state_merge="source",
                    emit_tokens=False,
                )
                mirror_status = "OK"
            except Exception as exc:
                mirror_status = f"WARN:{type(exc).__name__}"
                print(
                    f"WARN_SET_TRIAL_LIMIT_MIRROR_FAILED subscriber_key={sk} "
                    f"secondary_db={mirror_secondary} detail={exc}"
                )
        else:
            mirror_status = "SKIP"

    print("SET_TRIAL_LIMIT")
    print(f"subscriber_key={sk}")
    print(f"crm_db={resolved_db}")
    print(f"adjustment_key={adjustment_key}")
    print(f"reason={normalized_reason}")
    print(f"previous_sends_limit={int(current_limit)}")
    print(f"sends_limit={int(new_limit)}")
    print(f"idempotent={'YES' if idempotent else 'NO'}")
    print(f"applied={'YES' if (apply and not idempotent) else 'NO'}")
    print(f"dry_run={'YES' if not apply else 'NO'}")
    print(f"changed={'YES' if would_apply else 'NO'}")
    print(f"mirror_status={mirror_status}")
    return 0


def _render_scope_enhancement_text(*, rows: list[dict[str, Any]], extend_days: int) -> str:
    subject = _scope_enhancement_subject()
    filtered_rows = _scope_rows_for_email(rows)
    lines = [
        f"Subject: {subject}",
        "",
        "Hi there,",
        "",
        "We’ve shipped an improvement to how MicroFlowOps matches signals to your Texas Triangle (metro footprint and boundary handling). This produces a more complete set of qualifying OSHA activity for the same territory going forward.",
        "",
        "4 CBSAs:",
        "",
        "Dallas–Fort Worth–Arlington (CBSA 19100) — includes Frisco, Plano, Arlington",
        "Houston–The Woodlands–Sugar Land (CBSA 26420)",
        "San Antonio–New Braunfels (CBSA 41700)",
        "Austin–Round Rock–Georgetown (CBSA 12420)",
        "",
        "Because this improvement affects matching, a small set of qualifying signals since your trial start date (Feb 4, 2026) may not have appeared in prior digests. We’ve included the full list of High and Medium signals below (Feb 4, 2026 through Feb 20, 2026).",
        "",
    ]
    if not filtered_rows:
        lines.append("No High or Medium qualifying signals were found in this window.")
    else:
        lines.append("Priority | Company | City | Signal | Observed | Event date | Link")
        lines.append("---------|---------|------|--------|----------|------------|-----")
        for row in filtered_rows:
            city_state = f"{row.get('city','')}, {row.get('state','')}".strip().strip(",")
            url = str(row.get("osha_url") or "").strip() or "-"
            observed = str(row.get("observed_at_local") or "").strip() or "-"
            signal = str(row.get("signal") or "").strip()
            activity = str(row.get("activity_nr") or "").strip()
            if activity:
                signal = f"{signal} ({activity})" if signal else activity
            lines.append(
                f"{row.get('priority','')} | {row.get('company','')} | {city_state} | "
                f"{signal or '-'} | {observed} | {row.get('event_date','')} | {url}"
            )
    lines.extend(
        [
            "",
            f"To make sure you get a full window to evaluate the improved feed, we extended all active trials by {int(extend_days)} days. No action is required—this is already applied.",
            "",
            "If you have questions on any specific item in the list, reply here and we’ll clarify.",
            "",
            "— Chase",
            "MicroFlowOps",
            "microflowops.com",
        ]
    )
    return "\n".join(lines).rstrip() + "\n"


def _render_scope_enhancement_html(*, rows: list[dict[str, Any]], extend_days: int) -> str:
    subject = _scope_enhancement_subject()
    filtered_rows = _scope_rows_for_email(rows)
    parts: list[str] = []
    parts.append("<!doctype html>")
    parts.append("<html><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">")
    parts.append(
        "<style>"
        ".signals-table{border-collapse:collapse;width:100%;}"
        ".signals-table th,.signals-table td{border:1px solid #d1d5db;padding:8px 10px;text-align:left;vertical-align:top;}"
        ".signals-table th{background:#f8fafc;font-size:12px;letter-spacing:.02em;text-transform:uppercase;color:#374151;}"
        ".signals-table td{font-size:14px;color:#111827;}"
        "@media only screen and (max-width:640px){"
        ".signals-table thead{display:none !important;}"
        ".signals-table,.signals-table tbody,.signals-table tr,.signals-table td{display:block !important;width:100% !important;}"
        ".signals-table tr{border:1px solid #d1d5db !important;border-radius:10px !important;margin:0 0 10px 0 !important;overflow:hidden !important;}"
        ".signals-table td{border:none !important;border-bottom:1px solid #e5e7eb !important;padding:10px 12px !important;}"
        ".signals-table td:last-child{border-bottom:none !important;}"
        ".signals-table td::before{content:attr(data-label);display:block;font-size:11px;letter-spacing:0.12em;text-transform:uppercase;color:#6b7280;font-weight:700;margin-bottom:4px;}"
        "}"
        "</style>"
    )
    parts.append("</head><body>")
    parts.append(f"<h1>{escape(subject)}</h1>")
    parts.append("<p>Hi there,</p>")
    parts.append(
        "<p>We’ve shipped an improvement to how MicroFlowOps matches signals to your Texas Triangle "
        "(metro footprint and boundary handling). This produces a more complete set of qualifying OSHA activity "
        "for the same territory going forward.</p>"
    )
    parts.append("<p><strong>4 CBSAs:</strong></p>")
    parts.append("<ul>")
    parts.append("<li>Dallas–Fort Worth–Arlington (CBSA 19100) — includes Frisco, Plano, Arlington</li>")
    parts.append("<li>Houston–The Woodlands–Sugar Land (CBSA 26420)</li>")
    parts.append("<li>San Antonio–New Braunfels (CBSA 41700)</li>")
    parts.append("<li>Austin–Round Rock–Georgetown (CBSA 12420)</li>")
    parts.append("</ul>")
    parts.append(
        "<p>Because this improvement affects matching, a small set of qualifying signals since your trial start date "
        "(Feb 4, 2026) may not have appeared in prior digests. We’ve included the full list of High and Medium signals "
        "below (Feb 4, 2026 through Feb 20, 2026).</p>"
    )
    if not filtered_rows:
        parts.append("<p>No High or Medium qualifying signals were found in this window.</p>")
    else:
        parts.append('<table class="signals-table" border="1" cellpadding="6" cellspacing="0" style="border-collapse: collapse; width: 100%;">')
        parts.append("<thead><tr><th>Priority</th><th>Company</th><th>City</th><th>Signal</th><th>Observed</th><th>Event date</th></tr></thead>")
        parts.append("<tbody>")
        for row in filtered_rows:
            city_state = f"{row.get('city','')}, {row.get('state','')}".strip().strip(",") or "-"
            url = str(row.get("osha_url") or "").strip()
            company = escape(str(row.get("company") or "").strip() or "-")
            company_html = f'<a href="{escape(url)}">{company}</a>' if url else company
            signal = str(row.get("signal") or "").strip()
            activity = str(row.get("activity_nr") or "").strip()
            if activity:
                signal = f"{signal} ({activity})" if signal else activity
            observed = str(row.get("observed_at_local") or "").strip() or "-"
            event_date = str(row.get("event_date") or "").strip() or "-"
            parts.append(
                "<tr>"
                f"<td data-label=\"Priority\">{escape(str(row.get('priority') or ''))}</td>"
                f"<td data-label=\"Company\">{company_html}</td>"
                f"<td data-label=\"City\">{escape(city_state)}</td>"
                f"<td data-label=\"Signal\">{escape(signal or '-')}</td>"
                f"<td data-label=\"Observed\">{escape(observed)}</td>"
                f"<td data-label=\"Event date\">{escape(event_date)}</td>"
                "</tr>"
            )
        parts.append("</tbody></table>")
    parts.append(
        f"<p>To make sure you get a full window to evaluate the improved feed, we extended all active trials by {int(extend_days)} days. "
        "No action is required—this is already applied.</p>"
    )
    parts.append("<p>If you have questions on any specific item in the list, reply here and we’ll clarify.</p>")
    parts.append("<p>— Chase<br>MicroFlowOps<br><a href=\"https://microflowops.com\">microflowops.com</a></p>")
    parts.append("</body></html>")
    return "".join(parts)


def scope_enhancement(
    *,
    subscriber_key: str,
    leads_db_path: str,
    crm_db_path: str | Path | None,
    from_date: str,
    to_date: str,
    extend_days: int,
    send_live: bool,
    confirm_live_send: bool = False,
    customer_config_path: str = "",
) -> int:
    if send_live:
        runtime_mode = str(os.getenv("MFO_RUNTIME_MODE") or "manual").strip().lower() or "manual"
        preflight = run_runtime_preflight(
            mode=runtime_mode,
            intent="send",
            dry_run=False,
            task_log_root=str(os.getenv("TASK_LOG_ROOT") or ""),
            run_summary_root=str(os.getenv("RUN_SUMMARY_ROOT") or ""),
            require_confirm_live_send=True,
            confirm_live_send=bool(confirm_live_send),
        )
        for line in render_runtime_lines(preflight):
            print(line)
        if not preflight.ok:
            return 2
        osha_db_error = validate_live_osha_db_path(leads_db_path, Path(__file__).resolve().parent)
        if osha_db_error:
            print(osha_db_error)
            return 2

    report = generate_missed_signals_report(
        subscriber_key=subscriber_key,
        leads_db_path=leads_db_path,
        crm_db_path=crm_db_path,
        from_date=from_date,
        to_date=to_date,
        customer_config_path=customer_config_path,
    )
    sk = str(report.get("subscriber_key") or "").strip().lower()
    out_dir = crm_light.data_dir() / "trials" / sk
    out_dir.mkdir(parents=True, exist_ok=True)
    text_path = out_dir / "scope_enhancement_email.txt"
    html_path = out_dir / "scope_enhancement_email.html"
    text_body = _render_scope_enhancement_text(rows=list(report.get("missed_rows") or []), extend_days=extend_days)
    html_body = _render_scope_enhancement_html(rows=list(report.get("missed_rows") or []), extend_days=extend_days)
    text_path.write_text(text_body, encoding="utf-8")
    html_path.write_text(html_body, encoding="utf-8")
    print(f"SCOPE_ENHANCEMENT_ARTIFACT text={text_path}")
    print(f"SCOPE_ENHANCEMENT_ARTIFACT html={html_path}")

    extension_reason = f"scope_enhancement_{to_date}"
    if send_live:
        ext = extend_all_trials(days=int(extend_days), reason=extension_reason, crm_db_path=crm_db_path)
        print(
            "EXTEND_ALL_TRIALS "
            f"days={ext['days']} weekday_delta={ext['weekday_delta']} reason={ext['reason']} "
            f"applied={ext['applied']} skipped_expired={ext['skipped_expired']} skipped_idempotent={ext['skipped_idempotent']}"
        )

    run_id = f"scope_enhancement_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    latch_key = _scope_enhancement_latch_key(sk, from_date, to_date)
    crm_light.ensure_database(crm_db_path)
    with crm_light.open_conn(crm_db_path) as conn:
        crm_light.init_schema(conn)
        sub = crm_light.get_subscriber(conn, sk)
        if not sub:
            raise ValueError(f"subscriber not found subscriber_key={sk}")
        if send_live and crm_light.has_trial_latch(conn, latch_key=latch_key):
            crm_light.append_send_event(
                conn,
                subscriber_key=sk,
                variant="SCOPE_ENHANCEMENT",
                status="SKIP_SCOPE_ENHANCEMENT_ALREADY_SENT",
                run_id=run_id,
                meta={"from_date": from_date, "to_date": to_date, "latch_key": latch_key},
                ts_utc="",
            )
            print("SKIP_SCOPE_ENHANCEMENT_ALREADY_SENT")
            return 0

    customer_cfg = dict(report.get("customer_config") or {})
    if not customer_cfg:
        customer_cfg = {
            "brand_name": (os.getenv("BRAND_NAME") or "MicroFlowOps").strip() or "MicroFlowOps",
            "mailing_address": (os.getenv("MAILING_ADDRESS") or "").strip(),
        }
    branding = resolve_branding(customer_cfg)
    recipients = list(report.get("recipients") or [])
    if not recipients:
        recipients = [str(report.get("primary_recipient") or "").strip().lower()]
    recipients = [item for item in recipients if str(item or "").strip()]
    if not recipients:
        raise ValueError("no recipients resolved")

    subject = _scope_enhancement_subject()
    sent_count = 0
    errors: list[str] = []
    for recipient in recipients:
        list_unsub, list_unsub_post, one_click_url, _token = build_unsubscribe_payload(
            recipient=recipient,
            campaign_id=str(customer_cfg.get("customer_id") or sk),
            reply_to_email=branding["reply_to"],
            dry_run=(not send_live),
        )
        footer_disclaimer = "This report contains public OSHA inspection data for informational purposes only. Not legal advice."
        footer_text = build_footer_text(
            brand_name=branding.get("brand_legal_name") or branding.get("brand_name") or "",
            mailing_address=branding.get("mailing_address") or "",
            disclaimer=footer_disclaimer,
            reply_to=branding.get("reply_to") or "",
            unsub_url=one_click_url or None,
            include_separator=True,
        )
        footer_html = build_footer_html(
            brand_name=branding.get("brand_legal_name") or branding.get("brand_name") or "",
            mailing_address=branding.get("mailing_address") or "",
            disclaimer=footer_disclaimer,
            reply_to=branding.get("reply_to") or "",
            unsub_url=one_click_url or None,
        )
        body_text_with_footer = text_body.rstrip() + "\n\n" + footer_text.strip() + "\n"
        body_html_with_footer = html_body.replace("</body></html>", f"{footer_html}</body></html>")
        ok, message_id, error = send_email(
            recipient=recipient,
            subject=subject,
            html_body=body_html_with_footer,
            text_body=body_text_with_footer,
            customer_id=str(customer_cfg.get("customer_id") or sk),
            territory_code=str(report.get("territory_code") or ""),
            branding=branding,
            dry_run=(not send_live),
            list_unsub=list_unsub,
            list_unsub_post=list_unsub_post,
        )
        if ok:
            sent_count += 1
            if send_live:
                print(f"SCOPE_ENHANCEMENT_SENT recipient={recipient} message_id={message_id}")
            else:
                print(f"SCOPE_ENHANCEMENT_DRY_RUN recipient={recipient}")
        else:
            errors.append(f"{recipient}:{error}")

    status = "SCOPE_ENHANCEMENT_DRY_RUN"
    if send_live:
        status = "SCOPE_ENHANCEMENT_SENT" if (sent_count == len(recipients) and not errors) else "SCOPE_ENHANCEMENT_ERROR"

    with crm_light.open_conn(crm_db_path) as conn:
        crm_light.init_schema(conn)
        crm_light.append_send_event(
            conn,
            subscriber_key=sk,
            variant="SCOPE_ENHANCEMENT",
            status=status,
            run_id=run_id,
            meta={
                "from_date": from_date,
                "to_date": to_date,
                "subject": subject,
                "recipients": recipients,
                "sent_count": sent_count,
                "errors": errors,
                "missed_total": int(report.get("missed_total") or 0),
                "csv_path": str(report.get("csv_path") or ""),
            },
            ts_utc="",
        )
        if send_live and status == "SCOPE_ENHANCEMENT_SENT":
            crm_light.create_trial_latch_once(
                conn,
                latch_key=latch_key,
                subscriber_key=sk,
                action="SCOPE_ENHANCEMENT",
                meta={
                    "from_date": from_date,
                    "to_date": to_date,
                    "run_id": run_id,
                    "sent_count": sent_count,
                },
            )
    if errors:
        print(f"ERR_SCOPE_ENHANCEMENT_SEND_FAILED details={';'.join(errors)}")
        return 1 if send_live else 0
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description="Trial admin: upsert subscriber registry + trial state without manual DB edits."
    )
    sub = ap.add_subparsers(dest="cmd", required=True)

    add = sub.add_parser("add-trial", help="Upsert a trial participant into crm_light + leads DB.")
    add.add_argument("--subscriber-key", required=True)
    add.add_argument("--email", required=True)
    add_scope = add.add_mutually_exclusive_group(required=True)
    add_scope.add_argument(
        "--territory",
        help="Territory code or alias (e.g., TX_TRIANGLE_V1 -> TX_TRI).",
    )
    add_scope.add_argument(
        "--states",
        default="",
        help="Comma-separated state scope (for example: CA,OR,WA).",
    )
    add.add_argument("--tz", default="America/Chicago")
    add.add_argument("--start-date", required=True, help="YYYY-MM-DD")
    add.add_argument("--sends-limit", type=int, default=DEFAULT_SENDS_LIMIT)
    add.add_argument("--db", default=_default_leads_db_path(), help=r"Leads SQLite database path (default: ${DATA_DIR}\osha.sqlite)")
    add.add_argument("--schema", default="schema.sql", help="Schema SQL path (default: schema.sql)")
    add.add_argument("--crm-db", default="", help="Optional override path for crm_light sqlite.")

    show = sub.add_parser("show", help="Show trial state + recent send events for a subscriber.")
    show.add_argument("--subscriber-key", required=True)
    show.add_argument("--recent", type=int, default=10, help="How many recent events to print (default: 10)")
    show.add_argument("--crm-db", default="", help="Optional override path for crm_light sqlite.")

    list_cmd = sub.add_parser("list-trials", help="List trial subscribers with sent count/limit and expiry status.")
    list_cmd.add_argument("--status", default="", help="Optional filter: active|expired")
    list_cmd.add_argument("--crm-db", default="", help="Optional override path for crm_light sqlite.")

    append = sub.add_parser("append-event", help="Append a single send event (for historical backfill).")
    append.add_argument("--subscriber-key", required=True)
    append.add_argument("--ts-utc", default="", help="Optional ISO8601 timestamp with Z or timezone offset (defaults to current UTC).")
    append.add_argument("--status", default="SENT")
    append.add_argument("--variant", default="DAILY")
    append.add_argument("--run-id", default="", help="Optional run id; default backfill_<yyyymmddhhmmss>.")
    append.add_argument("--primary-recipient", default="", help="Optional primary recipient for delivery-day counting.")
    append.add_argument("--send-mode", default="", help="Optional mode token (e.g., LIVE, SAFE, DRY_RUN).")
    append.add_argument("--local-date", default="", help="Optional subscriber-local date YYYY-MM-DD.")
    append.add_argument("--meta-source", default="trial_admin_backfill", help="Optional source token for meta_json.")
    append.add_argument("--crm-db", default="", help="Optional override path for crm_light sqlite.")

    status = sub.add_parser("status", help="Print deterministic trial status block for a subscriber.")
    status.add_argument("--subscriber-key", required=True)
    status.add_argument("--as-of", default="", help="Optional as-of date YYYY-MM-DD (default: America/New_York today)")
    status.add_argument("--crm-db", default="", help="Optional override path for crm_light sqlite.")

    conversion = sub.add_parser(
        "conversion-draft",
        help="Write conversion draft artifacts using the same template as expiry path.",
    )
    conversion.add_argument("--subscriber-key", required=True)
    conversion.add_argument("--crm-db", default="", help="Optional override path for crm_light sqlite.")

    missed = sub.add_parser(
        "missed-signals-report",
        help="Write missed-signals artifacts for a trial subscriber over a date range.",
    )
    missed.add_argument("--subscriber-key", required=True)
    missed.add_argument("--db", default=_default_leads_db_path(), help=r"Leads SQLite db path (default: ${DATA_DIR}\osha.sqlite).")
    missed.add_argument("--from", dest="from_date", required=True, help="Start date YYYY-MM-DD.")
    missed.add_argument("--to", dest="to_date", required=True, help="End date YYYY-MM-DD.")
    missed.add_argument("--customer", default="", help="Optional customer config path override.")
    missed.add_argument("--crm-db", default="", help="Optional override path for crm_light sqlite.")

    extend = sub.add_parser(
        "extend-all-trials",
        help="Extend all active trials by a calendar-day delta (must be a multiple of 7) converted to weekday sends.",
    )
    extend.add_argument("--days", type=int, required=True, help="Calendar-day extension; must be a multiple of 7 (e.g., 7, 14).")
    extend.add_argument("--reason", required=True, help="Idempotency reason token.")
    extend.add_argument("--crm-db", default="", help="Optional override path for crm_light sqlite.")

    normalize = sub.add_parser(
        "normalize-trials",
        help="Normalize legacy trial limits and supersede duplicate scope-enhancement send events.",
    )
    normalize_mode = normalize.add_mutually_exclusive_group(required=True)
    normalize_mode.add_argument("--apply", action="store_true")
    normalize_mode.add_argument("--dry-run", action="store_true")
    normalize.add_argument("--crm-db", default="", help="Optional override path for crm_light sqlite.")

    reconcile = sub.add_parser(
        "reconcile-ledgers",
        help="Reconcile trial ledgers from a source CRM DB into a target CRM DB.",
    )
    reconcile.add_argument("--source-crm-db", required=True, help="Source crm_light sqlite path (read-only source).")
    reconcile.add_argument(
        "--target-crm-db",
        default="",
        help="Target crm_light sqlite path (alias of --crm-db).",
    )
    reconcile.add_argument(
        "--crm-db",
        default="",
        help="Target crm_light sqlite path (canonical target).",
    )
    reconcile.add_argument("--scope", default="all", choices=["all", "active_prod", "explicit"])
    reconcile.add_argument(
        "--trial-state-merge",
        default="max",
        choices=["max", "source"],
        help="Merge policy for trial_state fields (default: max; source for source-preferred sync).",
    )
    reconcile.add_argument(
        "--subscriber-key",
        action="append",
        default=[],
        help="Repeatable subscriber key (required when --scope explicit).",
    )
    reconcile_mode = reconcile.add_mutually_exclusive_group(required=True)
    reconcile_mode.add_argument("--dry-run", action="store_true")
    reconcile_mode.add_argument("--apply", action="store_true")

    set_limit = sub.add_parser(
        "set-trial-limit",
        help="Set a subscriber trial sends_limit with idempotent adjustment audit.",
    )
    set_limit.add_argument("--subscriber-key", required=True)
    set_limit.add_argument("--sends-limit", type=int, required=True)
    set_limit.add_argument("--reason", required=True)
    set_limit.add_argument("--crm-db", default="", help="Optional override path for crm_light sqlite.")
    set_limit_mode = set_limit.add_mutually_exclusive_group(required=True)
    set_limit_mode.add_argument("--dry-run", action="store_true")
    set_limit_mode.add_argument("--apply", action="store_true")

    scope = sub.add_parser(
        "scope-enhancement",
        help="Generate and optionally send one-time scope-enhancement email with missed signals.",
    )
    scope.add_argument("--subscriber-key", required=True)
    scope.add_argument("--db", default=_default_leads_db_path(), help=r"Leads SQLite db path (default: ${DATA_DIR}\osha.sqlite).")
    scope.add_argument("--from", dest="from_date", required=True, help="Start date YYYY-MM-DD.")
    scope.add_argument("--to", dest="to_date", required=True, help="End date YYYY-MM-DD.")
    scope.add_argument("--extend-days", type=int, default=7, help="Calendar days to extend active trials.")
    scope.add_argument("--customer", default="", help="Optional customer config path override.")
    scope.add_argument("--crm-db", default="", help="Optional override path for crm_light sqlite.")
    scope_mode = scope.add_mutually_exclusive_group(required=True)
    scope_mode.add_argument("--dry-run", action="store_true")
    scope_mode.add_argument("--send-live", action="store_true")
    scope.add_argument(
        "--confirm-live-send",
        action="store_true",
        help="Manual live-send confirmation flag (not required for trusted scheduled runtime).",
    )

    args = ap.parse_args(argv)

    crm_db: Path | None = crm_light.resolve_crm_db_path(None)
    if hasattr(args, "crm_db"):
        crm_db = crm_light.resolve_crm_db_path(str(args.crm_db or "").strip() or None)

    if args.cmd == "show":
        try:
            return show_trial(
                str(args.subscriber_key),
                crm_db_path=crm_db,
                recent=int(args.recent),
            )
        except Exception as exc:
            print(f"CONFIG_ERROR {exc}", file=sys.stderr)
            return 1

    if args.cmd == "list-trials":
        try:
            return list_trials(
                crm_db_path=crm_db,
                status_filter=str(args.status or ""),
            )
        except Exception as exc:
            print(f"CONFIG_ERROR {exc}", file=sys.stderr)
            return 1

    if args.cmd == "append-event":
        try:
            return append_event(
                subscriber_key=str(args.subscriber_key),
                ts_utc=str(args.ts_utc or ""),
                status=str(args.status),
                variant=str(args.variant),
                run_id=str(args.run_id),
                primary_recipient=str(args.primary_recipient),
                send_mode=str(args.send_mode),
                local_date=str(args.local_date),
                meta_source=str(args.meta_source),
                crm_db_path=crm_db,
            )
        except Exception as exc:
            print(f"CONFIG_ERROR {exc}", file=sys.stderr)
            return 1

    if args.cmd == "status":
        return print_trial_status(
            subscriber_key=str(args.subscriber_key),
            crm_db_path=crm_db,
            as_of=str(args.as_of or ""),
        )

    if args.cmd == "conversion-draft":
        try:
            write_conversion_draft(
                subscriber_key=str(args.subscriber_key),
                crm_db_path=crm_db,
                emit_stdout=True,
            )
            return 0
        except Exception as exc:
            msg = str(exc)
            if msg.startswith("CONFIG_ERROR"):
                print(msg, file=sys.stderr)
            else:
                print(f"CONFIG_ERROR {msg}", file=sys.stderr)
            return 1

    if args.cmd == "missed-signals-report":
        try:
            result = generate_missed_signals_report(
                subscriber_key=str(args.subscriber_key),
                leads_db_path=str(args.db),
                crm_db_path=crm_db,
                from_date=str(args.from_date),
                to_date=str(args.to_date),
                customer_config_path=str(args.customer or ""),
            )
            print(
                "MISSED_SIGNALS_REPORT "
                f"subscriber_key={result['subscriber_key']} "
                f"from={result['from_date']} to={result['to_date']} "
                f"expected_now={result['expected_total']} "
                f"delivered={result['delivered_total']} "
                f"missed={result['missed_total']}"
            )
            print(f"csv_path={result['csv_path']}")
            print(f"txt_path={result['txt_path']}")
            return 0
        except Exception as exc:
            print(f"CONFIG_ERROR {exc}", file=sys.stderr)
            return 1

    if args.cmd == "extend-all-trials":
        try:
            result = extend_all_trials(
                days=int(args.days),
                reason=str(args.reason),
                crm_db_path=crm_db,
            )
            print(
                "EXTEND_ALL_TRIALS "
                f"days={result['days']} "
                f"weekday_delta={result['weekday_delta']} "
                f"reason={result['reason']} "
                f"scanned={result['scanned']} "
                f"applied={result['applied']} "
                f"skipped_expired={result['skipped_expired']} "
                f"skipped_idempotent={result['skipped_idempotent']}"
            )
            return 0
        except Exception as exc:
            if str(exc).strip() == "ERR_TRIAL_EXTENSION_DAYS_NOT_MULTIPLE_OF_7":
                print("ERR_TRIAL_EXTENSION_DAYS_NOT_MULTIPLE_OF_7", file=sys.stderr)
                return 1
            print(f"CONFIG_ERROR {exc}", file=sys.stderr)
            return 1

    if args.cmd == "normalize-trials":
        try:
            result = normalize_trials(
                apply=bool(args.apply),
                crm_db_path=crm_db,
            )
            if args.apply:
                print(
                    "NORMALIZE_TRIALS_APPLIED "
                    f"updated_limits={result['updated_limits']} "
                    f"superseded_events={result['superseded_events']} "
                    f"skipped={result['skipped']}"
                )
            else:
                print(
                    "NORMALIZE_TRIALS_DRY_RUN "
                    f"updated_limits={result['updated_limits']} "
                    f"superseded_events={result['superseded_events']} "
                    f"skipped={result['skipped']}"
                )
            return 0
        except Exception as exc:
            print(f"CONFIG_ERROR {exc}", file=sys.stderr)
            return 1

    if args.cmd == "reconcile-ledgers":
        try:
            source_db = Path(str(args.source_crm_db or "").strip()).expanduser().resolve(strict=False)
            target_crm_db_arg = str(args.crm_db or "").strip()
            target_crm_db_alt = str(args.target_crm_db or "").strip()
            if target_crm_db_arg and target_crm_db_alt:
                arg_path = crm_light.resolve_crm_db_path(target_crm_db_arg)
                alt_path = crm_light.resolve_crm_db_path(target_crm_db_alt)
                if arg_path != alt_path:
                    raise ValueError("--target-crm-db and --crm-db must match when both are provided")
            target_db_text = target_crm_db_alt or target_crm_db_arg
            target_db = crm_light.resolve_crm_db_path(target_db_text or None)
            return reconcile_ledgers(
                source_crm_db_path=source_db,
                target_crm_db_path=target_db,
                scope=str(args.scope or "all"),
                subscriber_keys=[str(item or "") for item in list(args.subscriber_key or [])],
                apply=bool(args.apply),
                trial_state_merge=str(args.trial_state_merge or "max"),
                emit_tokens=True,
            )
        except Exception as exc:
            print(f"CONFIG_ERROR {exc}", file=sys.stderr)
            return 1

    if args.cmd == "set-trial-limit":
        try:
            return set_trial_limit(
                subscriber_key=str(args.subscriber_key),
                sends_limit=int(args.sends_limit),
                reason=str(args.reason),
                crm_db_path=crm_db,
                apply=bool(args.apply),
            )
        except Exception as exc:
            print(f"CONFIG_ERROR {exc}", file=sys.stderr)
            return 1

    if args.cmd == "scope-enhancement":
        try:
            return scope_enhancement(
                subscriber_key=str(args.subscriber_key),
                leads_db_path=str(args.db),
                crm_db_path=crm_db,
                from_date=str(args.from_date),
                to_date=str(args.to_date),
                extend_days=int(args.extend_days),
                send_live=bool(args.send_live),
                confirm_live_send=bool(args.confirm_live_send),
                customer_config_path=str(args.customer or ""),
            )
        except Exception as exc:
            print(f"CONFIG_ERROR {exc}", file=sys.stderr)
            return 1

    try:
        subscriber_key = _validate_subscriber_key(args.subscriber_key)
        email = _validate_email(args.email)
        if str(args.states or "").strip():
            normalized_states = _normalize_states_input(str(args.states))
            territory_code, definition = _build_state_set_territory_definition(subscriber_key, normalized_states)
            merge_territory_definition(territory_code, definition)
        else:
            territory_code = _normalize_territory(args.territory)
        tz = (args.tz or "").strip() or "America/Chicago"
        start_date = (args.start_date or "").strip()
        date.fromisoformat(start_date)
        sends_limit = int(args.sends_limit)
        if sends_limit <= 0:
            raise ValueError("sends_limit must be >= 1")
        req = TrialAddRequest(
            subscriber_key=subscriber_key,
            email=email,
            territory_code=territory_code,
            tz=tz,
            start_date=start_date,
            sends_limit=sends_limit,
        )
        add_trial(
            req,
            leads_db_path=str(args.db),
            schema_path=str(args.schema),
            crm_db_path=crm_db,
        )
        print("OK add-trial")
        print(f"subscriber_key={subscriber_key}")
        print(f"territory_code={territory_code}")
        if str(args.states or "").strip():
            print(f"states={','.join(_normalize_states_input(str(args.states)))}")
        print(f"start_date={start_date}")
        print(f"sends_limit={sends_limit}")
        return 0
    except Exception as exc:
        print(f"CONFIG_ERROR {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
