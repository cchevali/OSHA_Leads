from __future__ import annotations

import json
import os
import sqlite3
import hashlib
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore[assignment]

REPO_ROOT = Path(__file__).resolve().parent
PLAN_MAX_METROS: dict[str, int] = {
    "pilot": 4,
    "core": 4,
    "multi": 10,
}
PAID_PLAN_CODES = {"core", "multi"}
CRM_SCHEMA_VERSION = 6


def normalize_subscriber_key(value: str | None) -> str:
    return (value or "").strip().lower()


def normalize_email(value: str | None) -> str:
    return (value or "").strip().lower()


def normalize_cbsa_code(value: str | None) -> str:
    digits = "".join(ch for ch in str(value or "").strip() if ch.isdigit())
    if not digits:
        return ""
    return digits.zfill(5)


def normalize_plan_code(value: str | None) -> str:
    text = (value or "").strip().lower().replace("-", "_")
    aliases = {
        "pilot": "pilot",
        "trial": "pilot",
        "starter": "pilot",
        "core": "core",
        "multi": "multi",
        "multi_territory": "multi",
        "multi_territory_plan": "multi",
    }
    return aliases.get(text, text)


def plan_max_metros(plan_code: str | None) -> int | None:
    normalized = normalize_plan_code(plan_code)
    if normalized in PLAN_MAX_METROS:
        return int(PLAN_MAX_METROS[normalized])
    return None


def is_paid_plan(plan_code: str | None) -> bool:
    return normalize_plan_code(plan_code) in PAID_PLAN_CODES


def derive_subscriber_key_from_email(email: str | None) -> str:
    normalized = normalize_email(email)
    if not normalized:
        return ""
    digest = hashlib.sha1(normalized.encode("utf-8")).hexdigest()[:10]
    return f"sub_{digest}"


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
    _apply_schema_migrations(conn)
    conn.commit()


def _ensure_schema_version_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_version (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            version INTEGER NOT NULL,
            updated_at_utc TEXT NOT NULL
        )
        """
    )
    row = conn.execute("SELECT version FROM schema_version WHERE id = 1").fetchone()
    if row is None:
        conn.execute(
            "INSERT INTO schema_version (id, version, updated_at_utc) VALUES (1, 0, ?)",
            (utc_now_iso(),),
        )


def _get_schema_version(conn: sqlite3.Connection) -> int:
    _ensure_schema_version_table(conn)
    row = conn.execute("SELECT version FROM schema_version WHERE id = 1").fetchone()
    if row is None:
        return 0
    return int(row["version"] if isinstance(row, sqlite3.Row) else row[0])


def _set_schema_version(conn: sqlite3.Connection, version: int) -> None:
    conn.execute(
        "UPDATE schema_version SET version = ?, updated_at_utc = ? WHERE id = 1",
        (int(version), utc_now_iso()),
    )


def _apply_schema_migrations(conn: sqlite3.Connection) -> None:
    version = _get_schema_version(conn)

    if version < 1:
        _set_schema_version(conn, 1)
        version = 1

    if version < 2:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS subscriptions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stripe_subscription_id TEXT NOT NULL UNIQUE,
                plan_code TEXT NOT NULL,
                max_metros INTEGER NOT NULL,
                status TEXT NOT NULL,
                customer_email TEXT NOT NULL DEFAULT '',
                stripe_customer_id TEXT NOT NULL DEFAULT '',
                source_event_id TEXT NOT NULL DEFAULT '',
                created_at_utc TEXT NOT NULL,
                updated_at_utc TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_subscriptions_status
                ON subscriptions (status);
            CREATE INDEX IF NOT EXISTS idx_subscriptions_customer_email
                ON subscriptions (customer_email);
            CREATE INDEX IF NOT EXISTS idx_subscriptions_customer_id
                ON subscriptions (stripe_customer_id);
            """
        )
        _set_schema_version(conn, 2)
        version = 2

    if version < 3:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS subscriber_entitlements (
                subscriber_key TEXT PRIMARY KEY,
                email TEXT NOT NULL DEFAULT '',
                plan_code TEXT NOT NULL,
                max_metros INTEGER NOT NULL,
                active INTEGER NOT NULL DEFAULT 1,
                source TEXT NOT NULL DEFAULT '',
                created_at_utc TEXT NOT NULL,
                updated_at_utc TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_subscriber_entitlements_email
                ON subscriber_entitlements (email);
            CREATE INDEX IF NOT EXISTS idx_subscriber_entitlements_active
                ON subscriber_entitlements (active);
            """
        )
        _set_schema_version(conn, 3)
        version = 3

    if version < 4:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS subscriber_cbsa (
                subscriber_key TEXT NOT NULL,
                cbsa_code TEXT NOT NULL,
                created_at_utc TEXT NOT NULL,
                PRIMARY KEY (subscriber_key, cbsa_code)
            );

            CREATE INDEX IF NOT EXISTS idx_subscriber_cbsa_subscriber
                ON subscriber_cbsa (subscriber_key);
            """
        )
        _set_schema_version(conn, 4)
        version = 4

    if version < 5:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS stripe_event_log (
                event_id TEXT PRIMARY KEY,
                event_type TEXT NOT NULL,
                processed_at_utc TEXT NOT NULL
            );
            """
        )
        _set_schema_version(conn, 5)
        version = 5

    if version < 6:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS trial_adjustments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                subscriber_key TEXT NOT NULL,
                adjustment_key TEXT NOT NULL,
                adjustment_type TEXT NOT NULL,
                delta_sends INTEGER NOT NULL DEFAULT 0,
                reason TEXT NOT NULL DEFAULT '',
                meta_json TEXT NOT NULL DEFAULT '{}',
                created_at_utc TEXT NOT NULL,
                UNIQUE (subscriber_key, adjustment_key),
                FOREIGN KEY (subscriber_key) REFERENCES subscribers(subscriber_key)
            );

            CREATE INDEX IF NOT EXISTS idx_trial_adjustments_subscriber
                ON trial_adjustments (subscriber_key, adjustment_type);

            CREATE TABLE IF NOT EXISTS trial_latches (
                latch_key TEXT PRIMARY KEY,
                subscriber_key TEXT NOT NULL,
                action TEXT NOT NULL,
                meta_json TEXT NOT NULL DEFAULT '{}',
                created_at_utc TEXT NOT NULL,
                FOREIGN KEY (subscriber_key) REFERENCES subscribers(subscriber_key)
            );

            CREATE INDEX IF NOT EXISTS idx_trial_latches_subscriber
                ON trial_latches (subscriber_key, action);
            """
        )
        _set_schema_version(conn, 6)
        version = 6


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


def record_trial_adjustment_once(
    conn: sqlite3.Connection,
    *,
    subscriber_key: str,
    adjustment_key: str,
    adjustment_type: str,
    delta_sends: int,
    reason: str,
    meta: dict[str, Any] | None = None,
    commit: bool = True,
) -> bool:
    sk = normalize_subscriber_key(subscriber_key)
    if not sk:
        raise ValueError("subscriber_key required")
    key = str(adjustment_key or "").strip()
    if not key:
        raise ValueError("adjustment_key required")
    cur = conn.execute(
        """
        INSERT OR IGNORE INTO trial_adjustments (
            subscriber_key,
            adjustment_key,
            adjustment_type,
            delta_sends,
            reason,
            meta_json,
            created_at_utc
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            sk,
            key,
            str(adjustment_type or "").strip(),
            int(delta_sends),
            str(reason or "").strip(),
            json.dumps(meta or {}, sort_keys=True),
            utc_now_iso(),
        ),
    )
    if commit:
        conn.commit()
    return int(cur.rowcount or 0) > 0


def has_trial_adjustment(
    conn: sqlite3.Connection,
    *,
    subscriber_key: str,
    adjustment_key: str,
) -> bool:
    sk = normalize_subscriber_key(subscriber_key)
    key = str(adjustment_key or "").strip()
    if not sk or not key:
        return False
    row = conn.execute(
        """
        SELECT 1
        FROM trial_adjustments
        WHERE subscriber_key = ? AND adjustment_key = ?
        LIMIT 1
        """,
        (sk, key),
    ).fetchone()
    return row is not None


def create_trial_latch_once(
    conn: sqlite3.Connection,
    *,
    latch_key: str,
    subscriber_key: str,
    action: str,
    meta: dict[str, Any] | None = None,
    commit: bool = True,
) -> bool:
    lk = str(latch_key or "").strip()
    sk = normalize_subscriber_key(subscriber_key)
    if not lk:
        raise ValueError("latch_key required")
    if not sk:
        raise ValueError("subscriber_key required")
    cur = conn.execute(
        """
        INSERT OR IGNORE INTO trial_latches (
            latch_key,
            subscriber_key,
            action,
            meta_json,
            created_at_utc
        ) VALUES (?, ?, ?, ?, ?)
        """,
        (
            lk,
            sk,
            str(action or "").strip(),
            json.dumps(meta or {}, sort_keys=True),
            utc_now_iso(),
        ),
    )
    if commit:
        conn.commit()
    return int(cur.rowcount or 0) > 0


def has_trial_latch(conn: sqlite3.Connection, *, latch_key: str) -> bool:
    lk = str(latch_key or "").strip()
    if not lk:
        return False
    row = conn.execute(
        """
        SELECT 1
        FROM trial_latches
        WHERE latch_key = ?
        LIMIT 1
        """,
        (lk,),
    ).fetchone()
    return row is not None


def get_schema_version(conn: sqlite3.Connection) -> int:
    return _get_schema_version(conn)


def has_stripe_event(conn: sqlite3.Connection, event_id: str) -> bool:
    normalized_event_id = str(event_id or "").strip()
    if not normalized_event_id:
        return False
    row = conn.execute(
        "SELECT 1 FROM stripe_event_log WHERE event_id = ? LIMIT 1",
        (normalized_event_id,),
    ).fetchone()
    return row is not None


def record_stripe_event_once(
    conn: sqlite3.Connection,
    event_id: str,
    event_type: str,
    *,
    commit: bool = True,
) -> bool:
    normalized_event_id = str(event_id or "").strip()
    if not normalized_event_id:
        raise ValueError("event_id required")
    cur = conn.execute(
        """
        INSERT OR IGNORE INTO stripe_event_log (event_id, event_type, processed_at_utc)
        VALUES (?, ?, ?)
        """,
        (normalized_event_id, str(event_type or "").strip(), utc_now_iso()),
    )
    if commit:
        conn.commit()
    return int(cur.rowcount or 0) > 0


def upsert_subscription(
    conn: sqlite3.Connection,
    *,
    stripe_subscription_id: str,
    plan_code: str,
    max_metros: int,
    status: str,
    customer_email: str = "",
    stripe_customer_id: str = "",
    source_event_id: str = "",
    commit: bool = True,
) -> None:
    subscription_id = str(stripe_subscription_id or "").strip()
    if not subscription_id:
        raise ValueError("stripe_subscription_id required")
    normalized_plan = normalize_plan_code(plan_code)
    if not normalized_plan:
        raise ValueError("plan_code required")
    max_metros_value = int(max_metros)
    if max_metros_value < 1:
        raise ValueError("max_metros must be >= 1")
    ts = utc_now_iso()
    conn.execute(
        """
        INSERT INTO subscriptions (
            stripe_subscription_id,
            plan_code,
            max_metros,
            status,
            customer_email,
            stripe_customer_id,
            source_event_id,
            created_at_utc,
            updated_at_utc
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(stripe_subscription_id) DO UPDATE SET
            plan_code=excluded.plan_code,
            max_metros=excluded.max_metros,
            status=excluded.status,
            customer_email=excluded.customer_email,
            stripe_customer_id=excluded.stripe_customer_id,
            source_event_id=excluded.source_event_id,
            updated_at_utc=excluded.updated_at_utc
        """,
        (
            subscription_id,
            normalized_plan,
            max_metros_value,
            str(status or "").strip().lower(),
            normalize_email(customer_email),
            str(stripe_customer_id or "").strip(),
            str(source_event_id or "").strip(),
            ts,
            ts,
        ),
    )
    if commit:
        conn.commit()


def upsert_subscriber_entitlement(
    conn: sqlite3.Connection,
    *,
    subscriber_key: str,
    email: str,
    plan_code: str,
    max_metros: int,
    active: bool,
    source: str,
    commit: bool = True,
) -> None:
    sk = normalize_subscriber_key(subscriber_key)
    if not sk:
        raise ValueError("subscriber_key required")
    normalized_plan = normalize_plan_code(plan_code)
    if not normalized_plan:
        raise ValueError("plan_code required")
    metros = int(max_metros)
    if metros < 1:
        raise ValueError("max_metros must be >= 1")
    ts = utc_now_iso()
    conn.execute(
        """
        INSERT INTO subscriber_entitlements (
            subscriber_key,
            email,
            plan_code,
            max_metros,
            active,
            source,
            created_at_utc,
            updated_at_utc
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(subscriber_key) DO UPDATE SET
            email=excluded.email,
            plan_code=excluded.plan_code,
            max_metros=excluded.max_metros,
            active=excluded.active,
            source=excluded.source,
            updated_at_utc=excluded.updated_at_utc
        """,
        (
            sk,
            normalize_email(email),
            normalized_plan,
            metros,
            1 if active else 0,
            str(source or "").strip(),
            ts,
            ts,
        ),
    )
    if commit:
        conn.commit()


def get_subscriber_entitlement(
    conn: sqlite3.Connection,
    *,
    subscriber_key: str | None = None,
    email: str | None = None,
    active_only: bool = True,
) -> dict[str, Any] | None:
    sk = normalize_subscriber_key(subscriber_key)
    em = normalize_email(email)
    where_clauses: list[str] = []
    params: list[Any] = []
    if sk:
        where_clauses.append("subscriber_key = ?")
        params.append(sk)
    if em:
        where_clauses.append("email = ?")
        params.append(em)
    if not where_clauses:
        return None
    where = " OR ".join(where_clauses)
    if active_only:
        where = f"({where}) AND active = 1"
    row = conn.execute(
        f"""
        SELECT subscriber_key, email, plan_code, max_metros, active, source, created_at_utc, updated_at_utc
        FROM subscriber_entitlements
        WHERE {where}
        ORDER BY updated_at_utc DESC
        LIMIT 1
        """,
        tuple(params),
    ).fetchone()
    return dict(row) if row else None


def replace_subscriber_cbsa_allowlist(
    conn: sqlite3.Connection,
    *,
    subscriber_key: str,
    cbsa_codes: list[str],
) -> list[str]:
    sk = normalize_subscriber_key(subscriber_key)
    if not sk:
        raise ValueError("subscriber_key required")
    normalized = sorted({normalize_cbsa_code(code) for code in cbsa_codes if normalize_cbsa_code(code)})
    conn.execute("DELETE FROM subscriber_cbsa WHERE subscriber_key = ?", (sk,))
    ts = utc_now_iso()
    for code in normalized:
        conn.execute(
            """
            INSERT INTO subscriber_cbsa (subscriber_key, cbsa_code, created_at_utc)
            VALUES (?, ?, ?)
            """,
            (sk, code, ts),
        )
    conn.commit()
    return normalized


def get_subscriber_cbsa_allowlist(conn: sqlite3.Connection, subscriber_key: str | None) -> list[str]:
    sk = normalize_subscriber_key(subscriber_key)
    if not sk:
        return []
    rows = conn.execute(
        """
        SELECT cbsa_code
        FROM subscriber_cbsa
        WHERE subscriber_key = ?
        ORDER BY cbsa_code ASC
        """,
        (sk,),
    ).fetchall()
    return [str(row["cbsa_code"] or "").strip() for row in rows if str(row["cbsa_code"] or "").strip()]


def resolve_stripe_price_map_from_env() -> dict[str, str]:
    mapping: dict[str, str] = {}
    env_pairs = (
        ("STRIPE_PRICE_ID_PILOT", "pilot"),
        ("STRIPE_PRICE_ID_CORE", "core"),
        ("STRIPE_PRICE_ID_MULTI", "multi"),
    )
    for env_key, plan_code in env_pairs:
        value = str(os.getenv(env_key, "")).strip()
        if value:
            mapping[value] = plan_code
    return mapping


def resolve_plan_from_stripe_payload(
    *,
    price_id: str | None,
) -> tuple[str, int]:
    normalized_price = str(price_id or "").strip()
    mapping = resolve_stripe_price_map_from_env()
    core_price_id = str(os.getenv("STRIPE_PRICE_ID_CORE", "")).strip()
    if not mapping or (not core_price_id and normalized_price and normalized_price not in mapping):
        raise ValueError("ERR_STRIPE_PRICE_MAP_MISSING")
    if not normalized_price:
        raise ValueError("ERR_STRIPE_PRICE_ID_MISSING")
    if normalized_price not in mapping:
        raise ValueError("ERR_STRIPE_PRICE_ID_UNMAPPED")
    code = normalize_plan_code(mapping[normalized_price])
    metros = plan_max_metros(code)
    if metros is None:
        raise ValueError("ERR_STRIPE_PLAN_CODE_UNMAPPED")
    return code, metros


def ingest_stripe_subscription_event(
    conn: sqlite3.Connection,
    event_payload: dict[str, Any],
    *,
    dry_run: bool = False,
) -> dict[str, Any]:
    event_id = str(event_payload.get("id") or "").strip()
    event_type = str(event_payload.get("type") or "").strip()
    data_object = ((event_payload.get("data") or {}).get("object") or {})
    if not event_id or not event_type or not isinstance(data_object, dict):
        return {
            "ok": False,
            "token": "ERR_STRIPE_EVENT_INVALID",
            "event_id": event_id,
            "event_type": event_type,
        }

    if event_type not in {
        "customer.subscription.created",
        "customer.subscription.updated",
        "customer.subscription.deleted",
    }:
        return {
            "ok": True,
            "token": "STRIPE_EVENT_IGNORED_UNSUPPORTED_TYPE",
            "event_id": event_id,
            "event_type": event_type,
        }

    subscription_id = str(data_object.get("id") or "").strip()
    customer_id = str(data_object.get("customer") or "").strip()
    status = str(data_object.get("status") or "").strip().lower()
    metadata = data_object.get("metadata") if isinstance(data_object.get("metadata"), dict) else {}
    items = data_object.get("items") if isinstance(data_object.get("items"), dict) else {}
    item_rows = items.get("data") if isinstance(items.get("data"), list) else []
    price_id = ""
    if item_rows:
        first_item = item_rows[0] if isinstance(item_rows[0], dict) else {}
        price = first_item.get("price") if isinstance(first_item.get("price"), dict) else {}
        price_id = str(price.get("id") or "").strip()

    if not subscription_id:
        return {
            "ok": False,
            "token": "ERR_STRIPE_SUBSCRIPTION_ID_MISSING",
            "event_id": event_id,
            "event_type": event_type,
        }

    try:
        plan_code, max_metros = resolve_plan_from_stripe_payload(
            price_id=price_id,
        )
    except Exception as exc:
        return {
            "ok": False,
            "token": str(exc),
            "event_id": event_id,
            "event_type": event_type,
            "stripe_subscription_id": subscription_id,
            "stripe_price_id": price_id,
        }

    email = normalize_email(
        str(
            data_object.get("customer_email")
            or (metadata or {}).get("customer_email")
            or (metadata or {}).get("email")
            or ""
        )
    )
    subscriber_key = normalize_subscriber_key(
        str((metadata or {}).get("subscriber_key") or derive_subscriber_key_from_email(email))
    )
    active = status not in {"canceled", "incomplete_expired", "unpaid"}

    if not dry_run:
        if has_stripe_event(conn, event_id):
            return {
                "ok": True,
                "token": "STRIPE_EVENT_DUPLICATE",
                "event_id": event_id,
                "event_type": event_type,
            }
        try:
            conn.execute("BEGIN")
            inserted = record_stripe_event_once(conn, event_id, event_type, commit=False)
            if not inserted:
                conn.rollback()
                return {
                    "ok": True,
                    "token": "STRIPE_EVENT_DUPLICATE",
                    "event_id": event_id,
                    "event_type": event_type,
                }
            upsert_subscription(
                conn,
                stripe_subscription_id=subscription_id,
                plan_code=plan_code,
                max_metros=max_metros,
                status=status or "unknown",
                customer_email=email,
                stripe_customer_id=customer_id,
                source_event_id=event_id,
                commit=False,
            )
            if subscriber_key:
                upsert_subscriber_entitlement(
                    conn,
                    subscriber_key=subscriber_key,
                    email=email,
                    plan_code=plan_code,
                    max_metros=max_metros,
                    active=active,
                    source="stripe_webhook_price_id",
                    commit=False,
                )
            conn.commit()
        except Exception as exc:
            try:
                conn.rollback()
            except Exception:
                pass
            return {
                "ok": False,
                "token": "ERR_STRIPE_INGEST_WRITE_FAILED",
                "event_id": event_id,
                "event_type": event_type,
                "stripe_subscription_id": subscription_id,
                "stripe_price_id": price_id,
                "detail": str(exc),
            }

    return {
        "ok": True,
        "token": "STRIPE_EVENT_PROCESSED",
        "event_id": event_id,
        "event_type": event_type,
        "stripe_subscription_id": subscription_id,
        "stripe_customer_id": customer_id,
        "customer_email": email,
        "plan_code": plan_code,
        "max_metros": max_metros,
        "status": status,
        "subscriber_key": subscriber_key,
        "dry_run": bool(dry_run),
    }


def upsert_subscriber_onboarding(
    conn: sqlite3.Connection,
    *,
    subscriber_key: str | None,
    email: str | None,
    plan_code: str | None,
    cbsa_codes: list[str],
    source: str = "onboarding",
    dry_run: bool = False,
) -> dict[str, Any]:
    normalized_email = normalize_email(email)
    resolved_subscriber_key = normalize_subscriber_key(subscriber_key) or derive_subscriber_key_from_email(normalized_email)
    if not resolved_subscriber_key:
        return {"ok": False, "err_code": "ERR_SUBSCRIBER_KEY_REQUIRED"}

    entitlement = get_subscriber_entitlement(
        conn,
        subscriber_key=resolved_subscriber_key,
        email=normalized_email,
        active_only=True,
    )
    effective_plan = ""
    max_metros_value = None
    if entitlement:
        effective_plan = normalize_plan_code(str(entitlement.get("plan_code") or ""))
        try:
            max_metros_value = int(entitlement.get("max_metros") or 0)
        except Exception:
            max_metros_value = None

    if not effective_plan:
        effective_plan = normalize_plan_code(plan_code)
    if max_metros_value is None or max_metros_value < 1:
        max_metros_value = plan_max_metros(effective_plan)
    if max_metros_value is None:
        return {"ok": False, "err_code": "ERR_PLAN_CODE_UNKNOWN", "plan_code": effective_plan}

    normalized_cbsas = sorted({normalize_cbsa_code(code) for code in cbsa_codes if normalize_cbsa_code(code)})
    if len(normalized_cbsas) > int(max_metros_value):
        return {
            "ok": False,
            "err_code": "ERR_MAX_METROS_EXCEEDED",
            "selected_count": len(normalized_cbsas),
            "max_metros": int(max_metros_value),
            "plan_code": effective_plan,
            "contact_path": "/contact?source=onboarding&intent=expand",
        }

    if not dry_run:
        upsert_subscriber_entitlement(
            conn,
            subscriber_key=resolved_subscriber_key,
            email=normalized_email,
            plan_code=effective_plan,
            max_metros=int(max_metros_value),
            active=True,
            source=source,
        )
        replace_subscriber_cbsa_allowlist(
            conn,
            subscriber_key=resolved_subscriber_key,
            cbsa_codes=normalized_cbsas,
        )

    return {
        "ok": True,
        "subscriber_key": resolved_subscriber_key,
        "email": normalized_email,
        "plan_code": effective_plan,
        "max_metros": int(max_metros_value),
        "selected_count": len(normalized_cbsas),
        "cbsa_codes": normalized_cbsas,
        "dry_run": bool(dry_run),
    }
