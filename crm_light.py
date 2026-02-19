from __future__ import annotations

import json
import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore[assignment]

REPO_ROOT = Path(__file__).resolve().parent


def resolve_crm_db_path(db_path: str | Path | None = None) -> Path:
    raw_override = str(db_path or "").strip()
    if raw_override:
        return Path(raw_override).expanduser().resolve(strict=False)

    raw_data_dir = (os.getenv("DATA_DIR") or "").strip()
    if raw_data_dir:
        return (Path(raw_data_dir).expanduser().resolve(strict=False) / "crm_light.sqlite").resolve(strict=False)

    return (REPO_ROOT / "out" / "crm_light.sqlite").resolve(strict=False)


def data_dir() -> Path:
    return resolve_crm_db_path().parent


def crm_light_db_path() -> Path:
    return resolve_crm_db_path()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def connect(db_path: str | Path | None = None) -> sqlite3.Connection:
    path = resolve_crm_db_path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


@contextmanager
def open_conn(db_path: str | Path | None = None) -> Iterator[sqlite3.Connection]:
    conn = connect(db_path)
    try:
        yield conn
    finally:
        try:
            conn.close()
        except Exception:
            pass


def init_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS subscribers (
            subscriber_key TEXT PRIMARY KEY,
            email TEXT NOT NULL DEFAULT '',
            territory_code TEXT NOT NULL DEFAULT '',
            tz TEXT NOT NULL DEFAULT '',
            created_at_utc TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'trial'
        );

        CREATE TABLE IF NOT EXISTS send_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            subscriber_key TEXT NOT NULL,
            ts_utc TEXT NOT NULL,
            variant TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL,
            run_id TEXT NOT NULL DEFAULT '',
            meta_json TEXT NOT NULL DEFAULT '{}',
            FOREIGN KEY (subscriber_key) REFERENCES subscribers(subscriber_key)
        );

        CREATE INDEX IF NOT EXISTS idx_send_events_sub_ts
            ON send_events (subscriber_key, ts_utc);
        CREATE INDEX IF NOT EXISTS idx_send_events_sub_status_ts
            ON send_events (subscriber_key, status, ts_utc);

        CREATE TABLE IF NOT EXISTS trial_state (
            subscriber_key TEXT PRIMARY KEY,
            start_date TEXT NOT NULL DEFAULT '',
            sends_limit INTEGER,
            notified_at_utc TEXT,
            ended_at_utc TEXT,
            FOREIGN KEY (subscriber_key) REFERENCES subscribers(subscriber_key)
        );
        """
    )
    conn.commit()


def ensure_database(db_path: str | Path | None = None) -> Path:
    path = resolve_crm_db_path(db_path)
    with open_conn(path) as conn:
        init_schema(conn)
    return path


def get_subscriber(conn: sqlite3.Connection, subscriber_key: str) -> dict[str, Any] | None:
    sk = (subscriber_key or "").strip().lower()
    if not sk:
        return None
    row = conn.execute(
        """
        SELECT subscriber_key, email, territory_code, tz, created_at_utc, status
        FROM subscribers
        WHERE subscriber_key = ?
        LIMIT 1
        """,
        (sk,),
    ).fetchone()
    return dict(row) if row else None


def upsert_subscriber(
    conn: sqlite3.Connection,
    subscriber_key: str,
    email: str,
    territory_code: str,
    tz: str,
    status: str,
) -> None:
    sk = (subscriber_key or "").strip().lower()
    if not sk:
        raise ValueError("subscriber_key required")
    conn.execute(
        """
        INSERT INTO subscribers (subscriber_key, email, territory_code, tz, created_at_utc, status)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(subscriber_key) DO UPDATE SET
            email=excluded.email,
            territory_code=excluded.territory_code,
            tz=excluded.tz,
            status=excluded.status
        """,
        (
            sk,
            (email or "").strip().lower(),
            (territory_code or "").strip().upper(),
            (tz or "").strip(),
            utc_now_iso(),
            (status or "trial").strip(),
        ),
    )
    conn.commit()


def get_trial_state(conn: sqlite3.Connection, subscriber_key: str) -> dict[str, Any] | None:
    sk = (subscriber_key or "").strip().lower()
    if not sk:
        return None
    row = conn.execute(
        """
        SELECT subscriber_key, start_date, sends_limit, notified_at_utc, ended_at_utc
        FROM trial_state
        WHERE subscriber_key = ?
        LIMIT 1
        """,
        (sk,),
    ).fetchone()
    return dict(row) if row else None


def upsert_trial_state(
    conn: sqlite3.Connection,
    subscriber_key: str,
    start_date: str,
    sends_limit: int | None,
) -> None:
    sk = (subscriber_key or "").strip().lower()
    if not sk:
        raise ValueError("subscriber_key required")
    conn.execute(
        """
        INSERT INTO trial_state (subscriber_key, start_date, sends_limit)
        VALUES (?, ?, ?)
        ON CONFLICT(subscriber_key) DO UPDATE SET
            start_date=excluded.start_date,
            sends_limit=excluded.sends_limit
        """,
        (sk, (start_date or "").strip(), sends_limit),
    )
    conn.commit()


def set_trial_notified_at(conn: sqlite3.Connection, subscriber_key: str, ts_utc: str) -> None:
    sk = (subscriber_key or "").strip().lower()
    if not sk:
        return
    conn.execute(
        "UPDATE trial_state SET notified_at_utc = COALESCE(notified_at_utc, ?) WHERE subscriber_key = ?",
        ((ts_utc or "").strip(), sk),
    )
    conn.commit()


def set_trial_ended_at(conn: sqlite3.Connection, subscriber_key: str, ts_utc: str) -> None:
    sk = (subscriber_key or "").strip().lower()
    if not sk:
        return
    conn.execute(
        "UPDATE trial_state SET ended_at_utc = COALESCE(ended_at_utc, ?) WHERE subscriber_key = ?",
        ((ts_utc or "").strip(), sk),
    )
    conn.commit()


def append_send_event(
    conn: sqlite3.Connection,
    subscriber_key: str,
    variant: str,
    status: str,
    run_id: str,
    meta: dict[str, Any] | None,
    ts_utc: str,
) -> int:
    sk = (subscriber_key or "").strip().lower()
    if not sk:
        raise ValueError("subscriber_key required")
    payload = json.dumps(meta or {}, sort_keys=True)
    ts = (ts_utc or utc_now_iso()).strip()
    cur = conn.execute(
        """
        INSERT INTO send_events (subscriber_key, ts_utc, variant, status, run_id, meta_json)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            sk,
            ts,
            (variant or "").strip(),
            (status or "").strip(),
            (run_id or "").strip(),
            payload,
        ),
    )
    conn.commit()
    return int(cur.lastrowid or 0)


def count_successful_sends(conn: sqlite3.Connection, subscriber_key: str, start_date: str) -> int:
    sk = (subscriber_key or "").strip().lower()
    if not sk:
        return 0
    sd = (start_date or "").strip()
    if not sd:
        return 0
    start_iso = f"{sd}T00:00:00+00:00"
    row = conn.execute(
        """
        SELECT COUNT(1) c
        FROM send_events
        WHERE subscriber_key = ?
          AND status = 'SENT'
          AND ts_utc >= ?
        """,
        (sk, start_iso),
    ).fetchone()
    return int(row["c"] if row else 0)


def _safe_meta_dict(raw_meta_json: str) -> dict[str, Any]:
    text = (raw_meta_json or "").strip()
    if not text:
        return {}
    try:
        payload = json.loads(text)
    except Exception:
        return {}
    if isinstance(payload, dict):
        return payload
    return {}


def _parse_utc_ts(ts_utc: str) -> datetime | None:
    raw = (ts_utc or "").strip()
    if not raw:
        return None
    candidate = raw[:-1] + "+00:00" if raw.upper().endswith("Z") else raw
    try:
        dt = datetime.fromisoformat(candidate)
    except Exception:
        return None
    if dt.tzinfo is None or dt.utcoffset() is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _resolve_tz(tz_name: str) -> Any:
    if ZoneInfo is not None:
        try:
            return ZoneInfo((tz_name or "").strip() or "America/Chicago")
        except Exception:
            try:
                return ZoneInfo("America/Chicago")
            except Exception:
                pass
    return timezone.utc


def _is_trial_delivery_event(
    *,
    variant: str,
    meta: dict[str, Any],
    primary_recipient: str,
) -> bool:
    normalized_variant = (variant or "").strip().upper()
    if normalized_variant and normalized_variant != "DAILY":
        return False

    normalized_primary = (primary_recipient or "").strip().lower()
    event_recipient = (
        str(meta.get("primary_recipient") or meta.get("recipient") or meta.get("to") or "")
        .strip()
        .lower()
    )
    if event_recipient and normalized_primary and event_recipient != normalized_primary:
        return False

    send_mode = str(meta.get("send_mode") or meta.get("mode") or "").strip().upper()
    if send_mode and send_mode != "LIVE":
        return False

    return True


def count_trial_delivery_days(
    conn: sqlite3.Connection,
    subscriber_key: str,
    start_date: str,
    tz_name: str,
    primary_recipient: str,
    weekdays_only: bool = True,
) -> int:
    sk = (subscriber_key or "").strip().lower()
    if not sk:
        return 0
    sd = (start_date or "").strip()
    if not sd:
        return 0
    zone = _resolve_tz(tz_name)
    start_iso = f"{sd}T00:00:00+00:00"
    rows = conn.execute(
        """
        SELECT ts_utc, variant, meta_json
        FROM send_events
        WHERE subscriber_key = ?
          AND status = 'SENT'
          AND ts_utc >= ?
        ORDER BY ts_utc ASC, id ASC
        """,
        (sk, start_iso),
    ).fetchall()

    local_dates: set[str] = set()
    for row in rows:
        ts_utc = str(row["ts_utc"] or "").strip()
        variant = str(row["variant"] or "").strip()
        meta = _safe_meta_dict(str(row["meta_json"] or ""))
        if not _is_trial_delivery_event(
            variant=variant,
            meta=meta,
            primary_recipient=primary_recipient,
        ):
            continue
        dt_utc = _parse_utc_ts(ts_utc)
        if dt_utc is None:
            continue
        local_date = dt_utc.astimezone(zone).date()
        if weekdays_only and local_date.weekday() >= 5:
            continue
        local_dates.add(local_date.isoformat())

    return len(local_dates)


def has_trial_delivery_on_local_date(
    conn: sqlite3.Connection,
    subscriber_key: str,
    start_date: str,
    tz_name: str,
    primary_recipient: str,
    local_date_text: str,
) -> bool:
    sk = (subscriber_key or "").strip().lower()
    if not sk:
        return False
    sd = (start_date or "").strip()
    if not sd:
        return False
    target = (local_date_text or "").strip()
    if not target:
        return False
    zone = _resolve_tz(tz_name)
    start_iso = f"{sd}T00:00:00+00:00"
    rows = conn.execute(
        """
        SELECT ts_utc, variant, meta_json
        FROM send_events
        WHERE subscriber_key = ?
          AND status = 'SENT'
          AND ts_utc >= ?
        ORDER BY ts_utc ASC, id ASC
        """,
        (sk, start_iso),
    ).fetchall()
    for row in rows:
        ts_utc = str(row["ts_utc"] or "").strip()
        variant = str(row["variant"] or "").strip()
        meta = _safe_meta_dict(str(row["meta_json"] or ""))
        if not _is_trial_delivery_event(
            variant=variant,
            meta=meta,
            primary_recipient=primary_recipient,
        ):
            continue
        dt_utc = _parse_utc_ts(ts_utc)
        if dt_utc is None:
            continue
        if dt_utc.astimezone(zone).date().isoformat() == target:
            return True
    return False


def get_last_sent_at(conn: sqlite3.Connection, subscriber_key: str, start_date: str | None = None) -> str | None:
    sk = (subscriber_key or "").strip().lower()
    if not sk:
        return None
    params: list[str] = [sk]
    where = "subscriber_key = ? AND status = 'SENT'"
    sd = (start_date or "").strip()
    if sd:
        where += " AND ts_utc >= ?"
        params.append(f"{sd}T00:00:00+00:00")
    row = conn.execute(
        f"""
        SELECT MAX(ts_utc) last_sent_at
        FROM send_events
        WHERE {where}
        """,
        tuple(params),
    ).fetchone()
    if not row:
        return None
    value = row["last_sent_at"]
    if value:
        return str(value).strip()
    return None


def get_first_sent_at(conn: sqlite3.Connection, subscriber_key: str, start_date: str | None = None) -> str | None:
    sk = (subscriber_key or "").strip().lower()
    if not sk:
        return None
    params: list[str] = [sk]
    where = "subscriber_key = ? AND status = 'SENT'"
    sd = (start_date or "").strip()
    if sd:
        where += " AND ts_utc >= ?"
        params.append(f"{sd}T00:00:00+00:00")
    row = conn.execute(
        f"""
        SELECT MIN(ts_utc) first_sent_at
        FROM send_events
        WHERE {where}
        """,
        tuple(params),
    ).fetchone()
    if not row:
        return None
    value = row["first_sent_at"]
    if value:
        return str(value).strip()
    return None


def get_recent_send_events(conn: sqlite3.Connection, subscriber_key: str, limit: int) -> list[dict[str, Any]]:
    sk = (subscriber_key or "").strip().lower()
    if not sk:
        return []
    n = max(1, int(limit or 10))
    rows = conn.execute(
        """
        SELECT id, subscriber_key, ts_utc, variant, status, run_id, meta_json
        FROM send_events
        WHERE subscriber_key = ?
        ORDER BY ts_utc DESC, id DESC
        LIMIT ?
        """,
        (sk, n),
    ).fetchall()
    return [dict(r) for r in rows]
