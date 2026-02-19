from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore[assignment]

import crm_light
from lead_filters import load_territory_definitions, resolve_territory_code

_RE_SUBSCRIBER_KEY = re.compile(r"^[A-Za-z0-9_.-]{1,80}$")
TERRITORY_ALIASES: dict[str, str] = {
    "TX_TRIANGLE_V1": "TX_TRI",
    "TX_TRIANGLE": "TX_TRI",
    "TX_TRI_V1": "TX_TRI",
}
DEFAULT_SENDS_LIMIT = 14
TRIAL_SENDS_TARGET = 14
CONVERSION_TEMPLATE_TEXT = (
    "To: {primary_recipient}\n\n"
    "Subject: Keep your OSHA signal digest running - {territory_label}\n\n"
    "Hi {recipient_name},\n\n"
    "Thanks for trying MicroFlowOps. Over the trial you've been receiving the weekday OSHA activity digest for {territory_label}.\n\n"
    "Quick note on \"0 new\": it simply means nothing new was first-seen since the prior weekday send, so there's nothing to report that day.\n\n"
    "If you'd like to keep the feed running without interruption:\n"
    "• Reply \"go\" and confirm the metros/cities you want covered (current default: {territory_label}), and I'll switch you to the paid feed the same day.\n"
    "• Or activate via Stripe here: {stripe_link}\n\n"
    "If you'd rather confirm fit before paying, reply with your target metros/cities and I'll confirm coverage first.\n\n"
    "Want any tweaks (add/remove metros, add recipients, different send time)? Just reply with what you want and I'll tune it.\n\n"
    "— Chase\n"
    "MicroFlowOps\n\n"
    "P.S. If it's not a fit, just reply \"stop\" and I'll close it out.\n"
)


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


def _resolve_conversion_url() -> str:
    return (os.getenv("TRIAL_CONVERSION_URL") or "").strip()


def _resolve_territory_label(territory_code: str) -> str:
    code = (territory_code or "").strip().upper()
    if not code:
        return "{territory_label}"
    defs = load_territory_definitions()
    canonical = resolve_territory_code(code, defs)
    terr = defs.get(canonical) or defs.get(code) or {}
    label = str(terr.get("description") or "").strip()
    return label or code or "{territory_label}"


def _derive_recipient_name(email: str, subscriber_key: str) -> str:
    local = str(email or "").strip().split("@", 1)[0].strip()
    if local:
        text = local.replace(".", " ").replace("_", " ").replace("-", " ")
        cleaned = " ".join(part for part in text.split() if part)
        if cleaned:
            return cleaned.title()
    sk = (subscriber_key or "").strip()
    return sk or "{recipient_name}"


def render_conversion_email_text(
    *,
    recipient_name: str,
    primary_recipient: str,
    territory_label: str,
    stripe_link: str,
) -> str:
    return CONVERSION_TEMPLATE_TEXT.format(
        recipient_name=(recipient_name or "").strip() or "{recipient_name}",
        primary_recipient=(primary_recipient or "").strip().lower() or "{primary_recipient}",
        territory_label=(territory_label or "").strip() or "{territory_label}",
        stripe_link=(stripe_link or "").strip() or "{stripe_link}",
    )


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
    finally:
        conn.close()
    recipient_email = str(sub.get("email") or "").strip().lower()
    recipient_name = _derive_recipient_name(recipient_email, sk)
    territory_label = _resolve_territory_label(str(sub.get("territory_code") or ""))
    return path, sub, trial, recipient_name, recipient_email, territory_label


def write_conversion_draft(
    subscriber_key: str,
    crm_db_path: str | Path | None,
    emit_stdout: bool = True,
) -> Path:
    path, sub, trial, recipient_name, recipient_email, territory_label = _load_conversion_context(
        subscriber_key=subscriber_key,
        crm_db_path=crm_db_path,
    )
    stripe_link = _resolve_conversion_url()
    body = render_conversion_email_text(
        recipient_name=recipient_name,
        primary_recipient=recipient_email,
        territory_label=territory_label,
        stripe_link=stripe_link,
    )
    artifact_path = crm_light.data_dir() / "trials" / _validate_subscriber_key(subscriber_key) / "conversion_email.txt"
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    artifact_path.write_text(body, encoding="utf-8")
    if emit_stdout:
        print("OK conversion-draft")
        print(f"subscriber_key={_validate_subscriber_key(subscriber_key)}")
        print(f"crm_db={path}")
        print(f"start_date={str(trial.get('start_date') or '').strip()}")
        print(f"recipient_name={recipient_name}")
        print(f"territory_label={territory_label}")
        print(f"stripe_link={stripe_link or '{stripe_link}'}")
        print(f"conversion_path={artifact_path}")
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
    ts_utc: str,
    status: str,
    variant: str,
    run_id: str,
    crm_db_path: str | Path | None,
    primary_recipient: str = "",
    send_mode: str = "",
    local_date: str = "",
    meta_source: str = "trial_admin_backfill",
) -> int:
    sk = _validate_subscriber_key(subscriber_key)
    normalized_ts_utc = _parse_ts_utc(ts_utc)
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
        last_sent_at = crm_light.get_last_sent_at(conn, sk, start_date=start_date) or ""
        events = crm_light.get_recent_send_events(conn, sk, limit=max(1, int(recent or 10)))

    print(f"subscriber_key={sk}")
    print(f"email={str(sub.get('email') or '').strip()}")
    print(f"territory_code={str(sub.get('territory_code') or '').strip()}")
    print(f"subscriber_status={str(sub.get('status') or '').strip()}")
    print(f"start_date={start_date}")
    print(f"sends_limit={sends_limit}")
    print(f"sent_count={sent_count}")
    print(f"sent_rows_raw={sent_rows_raw}")
    print(f"expired={'YES' if expired else 'NO'}")
    print(f"notified_at_utc={str(trial.get('notified_at_utc') or '').strip()}")
    print(f"ended_at_utc={str(trial.get('ended_at_utc') or '').strip()}")
    print(f"last_sent_at={last_sent_at}")
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
        sends_used = crm_light.count_trial_delivery_days(
            conn,
            sk,
            start_date,
            tz_name=tz_name,
            primary_recipient=primary_recipient,
            weekdays_only=True,
        )
        sends_rows_raw = crm_light.count_successful_sends(conn, sk, start_date)
        first_sent = crm_light.get_first_sent_at(conn, sk, start_date=start_date)
        last_sent = crm_light.get_last_sent_at(conn, sk, start_date=start_date)
    finally:
        conn.close()

    days_since = (as_of_date - start).days
    expired_by_sends = 1 if sends_rows_raw >= sends_limit else 0
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


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description="Trial admin: upsert subscriber registry + trial state without manual DB edits."
    )
    sub = ap.add_subparsers(dest="cmd", required=True)

    add = sub.add_parser("add-trial", help="Upsert a trial participant into crm_light + leads DB.")
    add.add_argument("--subscriber-key", required=True)
    add.add_argument("--email", required=True)
    add.add_argument(
        "--territory",
        required=True,
        help="Territory code or alias (e.g., TX_TRIANGLE_V1 -> TX_TRI).",
    )
    add.add_argument("--tz", default="America/Chicago")
    add.add_argument("--start-date", required=True, help="YYYY-MM-DD")
    add.add_argument("--sends-limit", type=int, default=DEFAULT_SENDS_LIMIT)
    add.add_argument("--db", default="data/osha.sqlite", help="Leads SQLite database path (default: data/osha.sqlite)")
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
    append.add_argument("--ts-utc", required=True, help="ISO8601 timestamp with Z or timezone offset.")
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
        help="Write plain-text conversion draft artifact using the same template as expiry path.",
    )
    conversion.add_argument("--subscriber-key", required=True)
    conversion.add_argument("--crm-db", default="", help="Optional override path for crm_light sqlite.")

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
                ts_utc=str(args.ts_utc),
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

    try:
        subscriber_key = _validate_subscriber_key(args.subscriber_key)
        email = _validate_email(args.email)
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
        print(f"start_date={start_date}")
        print(f"sends_limit={sends_limit}")
        return 0
    except Exception as exc:
        print(f"CONFIG_ERROR {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
