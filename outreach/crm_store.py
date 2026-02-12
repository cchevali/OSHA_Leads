import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
OUTREACH_EVENTS_MIGRATION_COLUMNS = {
    "attributed_send_event_id": "INTEGER",
    "attributed_batch_id": "TEXT",
    "attributed_state_at_send": "TEXT",
    "attributed_model": "TEXT",
}


def data_dir() -> Path:
    raw = (os.getenv("DATA_DIR") or "").strip()
    if raw:
        p = Path(raw)
        return p if p.is_absolute() else (REPO_ROOT / p)
    return REPO_ROOT / "out"


def crm_db_path() -> Path:
    return data_dir() / "crm.sqlite"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def connect(db_path: Path | None = None) -> sqlite3.Connection:
    path = db_path or crm_db_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name = ? LIMIT 1",
        (table_name,),
    ).fetchone()
    return bool(row)


def _table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    return {str(r[1]) for r in conn.execute(f"PRAGMA table_info({table_name})") if len(r) > 1}


def ensure_outreach_events_columns(conn: sqlite3.Connection) -> None:
    if not _table_exists(conn, "outreach_events"):
        return
    existing = _table_columns(conn, "outreach_events")
    for name, col_type in OUTREACH_EVENTS_MIGRATION_COLUMNS.items():
        if name in existing:
            continue
        conn.execute(f"ALTER TABLE outreach_events ADD COLUMN {name} {col_type}")


def init_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS prospects (
            prospect_id TEXT PRIMARY KEY,
            firm TEXT NOT NULL DEFAULT '',
            contact_name TEXT NOT NULL DEFAULT '',
            email TEXT NOT NULL UNIQUE,
            title TEXT NOT NULL DEFAULT '',
            city TEXT NOT NULL DEFAULT '',
            state TEXT NOT NULL DEFAULT '',
            website TEXT NOT NULL DEFAULT '',
            source TEXT NOT NULL DEFAULT '',
            score INTEGER NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'new',
            created_at TEXT NOT NULL,
            last_contacted_at TEXT
        );

        CREATE TABLE IF NOT EXISTS outreach_events (
            event_id INTEGER PRIMARY KEY AUTOINCREMENT,
            prospect_id TEXT NOT NULL,
            ts TEXT NOT NULL,
            event_type TEXT NOT NULL,
            batch_id TEXT NOT NULL DEFAULT '',
            metadata_json TEXT NOT NULL DEFAULT '{}',
            FOREIGN KEY (prospect_id) REFERENCES prospects(prospect_id)
        );

        CREATE TABLE IF NOT EXISTS suppression (
            email TEXT PRIMARY KEY,
            reason TEXT NOT NULL DEFAULT '',
            ts TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS trials (
            prospect_id TEXT NOT NULL,
            territory_code TEXT NOT NULL,
            started_at TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'active',
            PRIMARY KEY (prospect_id, territory_code),
            FOREIGN KEY (prospect_id) REFERENCES prospects(prospect_id)
        );

        CREATE INDEX IF NOT EXISTS idx_prospects_state ON prospects(state);
        CREATE INDEX IF NOT EXISTS idx_prospects_status ON prospects(status);
        CREATE INDEX IF NOT EXISTS idx_events_prospect ON outreach_events(prospect_id);
        CREATE INDEX IF NOT EXISTS idx_events_type_ts ON outreach_events(event_type, ts);
        CREATE INDEX IF NOT EXISTS idx_trials_status ON trials(status);
        """
    )
    ensure_outreach_events_columns(conn)
    conn.commit()


def ensure_database(path: Path | None = None) -> Path:
    db_path = path or crm_db_path()
    conn = connect(db_path)
    try:
        init_schema(conn)
    finally:
        conn.close()
    return db_path
