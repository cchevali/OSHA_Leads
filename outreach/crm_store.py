import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from runtime_data_dir import DataDirResolution, resolve_data_dir

REPO_ROOT = Path(__file__).resolve().parents[1]
OUTREACH_EVENTS_MIGRATION_COLUMNS = {
    "attributed_send_event_id": "INTEGER",
    "attributed_batch_id": "TEXT",
    "attributed_state_at_send": "TEXT",
    "attributed_model": "TEXT",
}
PROSPECTS_MIGRATION_COLUMNS = {
    "source_fit_tier": "TEXT NOT NULL DEFAULT 'recoverable_consultant'",
    "default_send_eligible": "INTEGER NOT NULL DEFAULT 1",
    "email_status": "TEXT NOT NULL DEFAULT ''",
    "enrichment_lane": "TEXT NOT NULL DEFAULT ''",
}
AI_ASSIST_CANDIDATE_MIGRATION_COLUMNS = {
    "seed_id": "TEXT NOT NULL DEFAULT ''",
    "seed_source_token": "TEXT NOT NULL DEFAULT ''",
    "seed_source": "TEXT NOT NULL DEFAULT ''",
    "seed_source_url": "TEXT NOT NULL DEFAULT ''",
    "source_record_id": "TEXT NOT NULL DEFAULT ''",
    "license_number": "TEXT NOT NULL DEFAULT ''",
    "state_lic_license_class_norm": "TEXT NOT NULL DEFAULT ''",
    "state_lic_hard_negative_class": "TEXT NOT NULL DEFAULT ''",
    "state_lic_positive_families_json": "TEXT NOT NULL DEFAULT '[]'",
    "state_lic_negative_families_json": "TEXT NOT NULL DEFAULT '[]'",
    "state_lic_packet_exclusion_reason": "TEXT NOT NULL DEFAULT ''",
}
AI_ASSIST_CANDIDATE_TABLE = "ai_assist_candidates"
AI_ASSIST_IMPORT_BATCH_TABLE = "ai_assist_import_batches"
AI_ASSIST_CANDIDATE_BATCH_KEY_INDEX = "idx_ai_assist_candidates_batch_key_unique"
VALID_SOURCE_FIT_TIERS = ("core_consultant", "recoverable_consultant", "adjacent_contractor")


def data_dir_resolution() -> DataDirResolution:
    return resolve_data_dir(REPO_ROOT)


def data_dir() -> Path:
    return data_dir_resolution().effective_path


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


def _index_exists(conn: sqlite3.Connection, index_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='index' AND name = ? LIMIT 1",
        (index_name,),
    ).fetchone()
    return bool(row)


def ensure_outreach_events_columns(conn: sqlite3.Connection) -> None:
    if not _table_exists(conn, "outreach_events"):
        return
    existing = _table_columns(conn, "outreach_events")
    for name, col_type in OUTREACH_EVENTS_MIGRATION_COLUMNS.items():
        if name in existing:
            continue
        conn.execute(f"ALTER TABLE outreach_events ADD COLUMN {name} {col_type}")


def ensure_prospect_columns(conn: sqlite3.Connection) -> None:
    if not _table_exists(conn, "prospects"):
        return
    existing = _table_columns(conn, "prospects")
    for name, col_type in PROSPECTS_MIGRATION_COLUMNS.items():
        if name in existing:
            continue
        conn.execute(f"ALTER TABLE prospects ADD COLUMN {name} {col_type}")


def ensure_prospect_metadata_defaults(conn: sqlite3.Connection) -> None:
    if not _table_exists(conn, "prospects"):
        return
    existing = _table_columns(conn, "prospects")
    if "source_fit_tier" not in existing or "default_send_eligible" not in existing:
        return

    valid_tiers = ",".join([f"'{tier}'" for tier in VALID_SOURCE_FIT_TIERS])
    tier_needs_normalize = conn.execute(
        f"""
        SELECT 1
        FROM prospects
        WHERE lower(trim(COALESCE(source_fit_tier, ''))) IN ({valid_tiers})
          AND COALESCE(source_fit_tier, '') <> lower(trim(COALESCE(source_fit_tier, '')))
        LIMIT 1
        """
    ).fetchone()
    if tier_needs_normalize:
        conn.execute(
            f"""
            UPDATE prospects
            SET source_fit_tier = lower(trim(COALESCE(source_fit_tier, '')))
            WHERE lower(trim(COALESCE(source_fit_tier, ''))) IN ({valid_tiers})
            """
        )

    tier_needs_default = conn.execute(
        f"""
        SELECT 1
        FROM prospects
        WHERE source_fit_tier IS NULL
           OR trim(source_fit_tier) = ''
           OR lower(trim(source_fit_tier)) NOT IN ({valid_tiers})
        LIMIT 1
        """
    ).fetchone()
    if tier_needs_default:
        conn.execute(
            f"""
            UPDATE prospects
            SET source_fit_tier = 'recoverable_consultant'
            WHERE source_fit_tier IS NULL
               OR trim(source_fit_tier) = ''
               OR lower(trim(source_fit_tier)) NOT IN ({valid_tiers})
            """
        )

    sendable_needs_normalize = conn.execute(
        """
        SELECT 1
        FROM prospects
        WHERE lower(trim(CAST(COALESCE(default_send_eligible, '') AS TEXT))) NOT IN ('0', '1')
        LIMIT 1
        """
    ).fetchone()
    if sendable_needs_normalize:
        conn.execute(
            """
            UPDATE prospects
            SET default_send_eligible = CASE
                WHEN lower(trim(CAST(COALESCE(default_send_eligible, '') AS TEXT))) IN ('1', 'true', 'yes', 'on') THEN 1
                WHEN lower(trim(CAST(COALESCE(default_send_eligible, '') AS TEXT))) IN ('0', 'false', 'no', 'off') THEN 0
                ELSE 1
            END
            """
        )


def ensure_prospect_indexes(conn: sqlite3.Connection) -> None:
    if not _table_exists(conn, "prospects"):
        return
    existing = _table_columns(conn, "prospects")
    if "default_send_eligible" in existing:
        conn.execute("CREATE INDEX IF NOT EXISTS idx_prospects_send_eligible ON prospects(default_send_eligible);")


def ensure_ai_assist_candidate_columns(conn: sqlite3.Connection) -> None:
    if not _table_exists(conn, AI_ASSIST_CANDIDATE_TABLE):
        return
    existing = _table_columns(conn, AI_ASSIST_CANDIDATE_TABLE)
    for name, col_type in AI_ASSIST_CANDIDATE_MIGRATION_COLUMNS.items():
        if name in existing:
            continue
        conn.execute(f"ALTER TABLE {AI_ASSIST_CANDIDATE_TABLE} ADD COLUMN {name} {col_type}")


def ensure_ai_assist_candidate_table(conn: sqlite3.Connection) -> None:
    conn.executescript(
        f"""
        CREATE TABLE IF NOT EXISTS {AI_ASSIST_CANDIDATE_TABLE} (
            candidate_id INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_id TEXT NOT NULL,
            candidate_key TEXT NOT NULL,
            state TEXT NOT NULL DEFAULT '',
            decision TEXT NOT NULL DEFAULT '',
            firm TEXT NOT NULL DEFAULT '',
            website TEXT NOT NULL DEFAULT '',
            domain TEXT NOT NULL DEFAULT '',
            contact_name TEXT NOT NULL DEFAULT '',
            title TEXT NOT NULL DEFAULT '',
            email TEXT NOT NULL DEFAULT '',
            source_urls_json TEXT NOT NULL DEFAULT '[]',
            confidence INTEGER NOT NULL DEFAULT 0,
            evidence_snippet TEXT NOT NULL DEFAULT '',
            verification_status TEXT NOT NULL DEFAULT '',
            rejection_reason TEXT NOT NULL DEFAULT '',
            prospect_id TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(batch_id, candidate_key)
        );
        CREATE INDEX IF NOT EXISTS idx_ai_assist_candidates_batch_state
            ON {AI_ASSIST_CANDIDATE_TABLE}(batch_id, state);
        CREATE INDEX IF NOT EXISTS idx_ai_assist_candidates_batch_status
            ON {AI_ASSIST_CANDIDATE_TABLE}(batch_id, verification_status);
        """
    )
    if not _index_exists(conn, AI_ASSIST_CANDIDATE_BATCH_KEY_INDEX):
        conn.execute(
            f"""
            DELETE FROM {AI_ASSIST_CANDIDATE_TABLE}
            WHERE candidate_id NOT IN (
                SELECT MAX(candidate_id)
                FROM {AI_ASSIST_CANDIDATE_TABLE}
                GROUP BY batch_id, candidate_key
            )
            """
        )
        conn.execute(
            f"""
            CREATE UNIQUE INDEX IF NOT EXISTS {AI_ASSIST_CANDIDATE_BATCH_KEY_INDEX}
            ON {AI_ASSIST_CANDIDATE_TABLE}(batch_id, candidate_key)
            """
        )
    ensure_ai_assist_candidate_columns(conn)


def ensure_ai_assist_import_batch_table(conn: sqlite3.Connection) -> None:
    conn.executescript(
        f"""
        CREATE TABLE IF NOT EXISTS {AI_ASSIST_IMPORT_BATCH_TABLE} (
            batch_id TEXT PRIMARY KEY,
            source_path TEXT NOT NULL DEFAULT '',
            source_filename TEXT NOT NULL DEFAULT '',
            source_file_hash TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL DEFAULT '',
            started_at TEXT NOT NULL DEFAULT '',
            completed_at TEXT NOT NULL DEFAULT '',
            last_error TEXT NOT NULL DEFAULT '',
            candidates_total INTEGER NOT NULL DEFAULT 0,
            accepted_total INTEGER NOT NULL DEFAULT 0,
            rejected_total INTEGER NOT NULL DEFAULT 0,
            verified_total INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_ai_assist_import_batches_status
            ON {AI_ASSIST_IMPORT_BATCH_TABLE}(status, updated_at);
        """
    )


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
            source_fit_tier TEXT NOT NULL DEFAULT 'recoverable_consultant',
            default_send_eligible INTEGER NOT NULL DEFAULT 1,
            email_status TEXT NOT NULL DEFAULT '',
            enrichment_lane TEXT NOT NULL DEFAULT '',
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

        CREATE TABLE IF NOT EXISTS bounce_events (
            event_id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at_utc TEXT NOT NULL,
            recipient_email TEXT NOT NULL,
            bounce_class TEXT NOT NULL,
            smtp_status TEXT NOT NULL DEFAULT '',
            smtp_code TEXT NOT NULL DEFAULT '',
            diagnostic_code TEXT NOT NULL DEFAULT '',
            final_recipient TEXT NOT NULL DEFAULT '',
            original_to TEXT NOT NULL DEFAULT '',
            source TEXT NOT NULL,
            subject TEXT NOT NULL DEFAULT '',
            source_message_id TEXT NOT NULL DEFAULT '',
            source_uid_fingerprint TEXT NOT NULL UNIQUE,
            metadata_json TEXT NOT NULL DEFAULT '{}',
            prospect_id TEXT NOT NULL DEFAULT ''
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
        CREATE INDEX IF NOT EXISTS idx_bounce_events_recipient ON bounce_events(recipient_email);
        CREATE INDEX IF NOT EXISTS idx_bounce_events_created_at ON bounce_events(created_at_utc);
        CREATE INDEX IF NOT EXISTS idx_bounce_events_class ON bounce_events(bounce_class);
        """
    )
    ensure_prospect_columns(conn)
    ensure_prospect_metadata_defaults(conn)
    ensure_prospect_indexes(conn)
    ensure_outreach_events_columns(conn)
    ensure_ai_assist_candidate_table(conn)
    ensure_ai_assist_import_batch_table(conn)
    conn.commit()


def ensure_database(path: Path | None = None) -> Path:
    db_path = path or crm_db_path()
    conn = connect(db_path)
    try:
        init_schema(conn)
    finally:
        conn.close()
    return db_path
