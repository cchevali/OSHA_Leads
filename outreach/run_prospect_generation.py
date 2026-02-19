import argparse
import csv
import hashlib
import os
import shutil
import sqlite3
import sys
import tempfile
from collections import Counter
from datetime import date, datetime
from pathlib import Path

try:  # pragma: no cover
    from dotenv import load_dotenv

    load_dotenv()
except Exception:  # pragma: no cover
    pass


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from outreach import crm_store
from outreach import prospect_sources_aiha
import seed_recipients_pools as pools


ERR_GENERATOR_FAILED = "ERR_GENERATOR_FAILED"
PASS_GENERATOR_PRINT_CONFIG = "PASS_GENERATOR_PRINT_CONFIG"
WARN_GENERATOR_INBOX_ARCHIVE = "WARN_GENERATOR_INBOX_ARCHIVE"
WARN_INBOX_PATH_DEPRECATED = "WARN_INBOX_PATH_DEPRECATED"
WARN_AUTOGROWTH_SOURCE_FAILED = "WARN_AUTOGROWTH_SOURCE_FAILED"
WARN_AUTOGROW_SAFETY_NET_FORCED = "WARN_AUTOGROW_SAFETY_NET_FORCED"

OUTPUT_SUBDIR = ("prospect_discovery",)
OUTPUT_FILENAME = "prospects_latest.csv"

INBOX_NEW_SUBDIR = ("prospect_generation", "inbox")
INBOX_OLD_SUBDIR = ("prospect_discovery", "inbox")
INBOX_PROCESSED_SUBDIR = "processed"

GENERATION_CACHE_SUBDIR = ("prospect_generation", "cache", "aiha")
GENERATION_DIAGNOSTICS_SUBDIR = ("prospect_generation", "diagnostics")

AUTOGROW_ALLOWED_SOURCES = {"AIHA"}
EXCLUDED_STATUSES = {"do_not_contact", "unsubscribed", "bounced", "converted"}

POOL_ROWS_BY_STATE = {
    "TX": pools.TX_POOL,
    "CA": pools.CA_POOL,
    "FL": pools.FL_POOL,
}


def _valid_email(value: str) -> bool:
    email = (value or "").strip().lower()
    if not email or "@" not in email:
        return False
    local, _, domain = email.partition("@")
    if not local or not domain:
        return False
    if "." not in domain:
        return False
    if domain.startswith(".") or domain.endswith("."):
        return False
    return True


def _normalize_email(value: str) -> str:
    return (value or "").strip().lower()


def _normalize_state(value: str) -> str:
    return (value or "").strip().upper()


def _normalize_text(value: str) -> str:
    return (value or "").strip()


def _email_domain(email: str) -> str:
    e = _normalize_email(email)
    if "@" not in e:
        return ""
    return e.split("@", 1)[1].strip().lower()


def _output_path(data_dir: Path) -> Path:
    return data_dir.joinpath(*OUTPUT_SUBDIR) / OUTPUT_FILENAME


def _generation_inbox_dir(data_dir: Path) -> Path:
    return data_dir.joinpath(*INBOX_NEW_SUBDIR)


def _legacy_inbox_dir(data_dir: Path) -> Path:
    return data_dir.joinpath(*INBOX_OLD_SUBDIR)


def _generation_cache_dir(data_dir: Path) -> Path:
    return data_dir.joinpath(*GENERATION_CACHE_SUBDIR)


def _generation_diagnostics_dir(data_dir: Path) -> Path:
    return data_dir.joinpath(*GENERATION_DIAGNOSTICS_SUBDIR)


def _discovery_fields() -> list[str]:
    return ["prospect_id", "firm", "email", "title", "city", "state", "source", "contact_name", "website"]


def _prospect_id(state: str, domain: str, email: str) -> str:
    base = f"{state}|{(domain or '').strip().lower()}|{_normalize_email(email)}"
    digest = hashlib.sha1(base.encode("utf-8")).hexdigest()[:16]
    return f"gen_{digest}"


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name = ? LIMIT 1",
        (table_name,),
    ).fetchone()
    return bool(row)


def _table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    return {str(r[1]) for r in conn.execute(f"PRAGMA table_info({table_name})") if len(r) > 1}


def _bool_env(value: str) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _parse_int_env(raw: str, default: int, minimum: int) -> int:
    text = str(raw or "").strip()
    if not text:
        return default
    value = int(text)
    if value < minimum:
        raise ValueError(f"value_below_minimum raw={text} minimum={minimum}")
    return value


def _parse_for_date(raw: str) -> date:
    text = str(raw or "").strip()
    if not text:
        return datetime.now().date()
    return datetime.strptime(text, "%Y-%m-%d").date()


def _parse_states(raw: str) -> list[str]:
    states: list[str] = []
    for token in str(raw or "").split(","):
        state = _normalize_state(token)
        if not state:
            continue
        if state not in states:
            states.append(state)
    return states


def _choose_state(states: list[str], run_date: date) -> str:
    if not states:
        return ""
    idx = run_date.weekday() % len(states)
    return states[idx]


def _parse_autogrow_config() -> dict:
    enabled = _bool_env(os.getenv("PROSPECT_AUTOGROW_ENABLED", "0"))
    safety_net_enabled = _bool_env(os.getenv("PROSPECT_AUTOGROW_SAFETY_NET_ENABLED", "1"))

    source_tokens: list[str] = []
    for token in str(os.getenv("PROSPECT_AUTOGROW_SOURCES", "") or "").split(","):
        item = _normalize_state(token)
        if not item:
            continue
        if item not in source_tokens:
            source_tokens.append(item)

    invalid = [item for item in source_tokens if item not in AUTOGROW_ALLOWED_SOURCES]
    if invalid:
        raise ValueError(f"invalid_autogrow_sources={','.join(invalid)}")

    backlog_target = _parse_int_env(os.getenv("PROSPECT_AUTOGROW_BACKLOG_TARGET", ""), default=60, minimum=1)
    max_fetch_pages = _parse_int_env(os.getenv("PROSPECT_AUTOGROW_MAX_FETCH_PAGES_PER_RUN", ""), default=6, minimum=1)
    sleep_ms = _parse_int_env(os.getenv("PROSPECT_AUTOGROW_HTTP_SLEEP_MS", ""), default=800, minimum=0)

    return {
        "enabled": enabled,
        "safety_net_enabled": safety_net_enabled,
        "sources": source_tokens,
        "backlog_target": backlog_target,
        "max_fetch_pages": max_fetch_pages,
        "sleep_ms": sleep_ms,
    }


def _build_clean_state_rows(states: list[str]) -> tuple[dict[str, list[dict[str, str]]], int]:
    state_rows: dict[str, list[dict[str, str]]] = {}
    for state, seed_rows in POOL_ROWS_BY_STATE.items():
        deduped = pools.dedupe_rows(seed_rows)
        cleaned, _stats = pools.apply_hygiene(deduped)
        state_rows[state] = cleaned

    rows_read = 0
    seen_states: set[str] = set()
    for state in states:
        s = _normalize_state(state)
        if not s or s in seen_states:
            continue
        seen_states.add(s)
        rows_read += int(len(state_rows.get(s, [])))
        state_rows.setdefault(s, [])
    return state_rows, rows_read


def _write_legacy_pool_files(state_rows: dict[str, list[dict[str, str]]]) -> None:
    pools.write_pool(state_rows.get("TX", []), pools.TX_PATH)
    pools.write_pool(state_rows.get("CA", []), pools.CA_PATH)
    pools.write_pool(state_rows.get("FL", []), pools.FL_PATH)
    pools.write_pool(state_rows.get("TX", []), pools.DEFAULT_PATH)


def _read_legacy_pool_files() -> list[dict[str, str]]:
    ordered_paths = [pools.TX_PATH, pools.CA_PATH, pools.FL_PATH]
    out: list[dict[str, str]] = []
    for path in ordered_paths:
        if not path.exists():
            continue
        with open(path, "r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                out.append({str(k or ""): str(v or "") for k, v in dict(row).items()})
    return out


def _state_rows_to_combined_input(state_rows: dict[str, list[dict[str, str]]], states: list[str]) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    seen_states: set[str] = set()
    for state in states:
        s = _normalize_state(state)
        if not s or s in seen_states:
            continue
        seen_states.add(s)
        out.extend(state_rows.get(s, []))
    return out


def _to_discovery_rows(input_rows: list[dict[str, str]]) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    seen_emails: set[str] = set()

    for row in input_rows:
        email = _normalize_email(row.get("email") or row.get("contact_email") or "")
        if not _valid_email(email):
            continue
        if email in seen_emails:
            continue
        seen_emails.add(email)

        state = _normalize_state(row.get("state") or "")
        domain = _normalize_text(row.get("domain") or "").lower() or _email_domain(email)
        prospect_id = _normalize_text(row.get("prospect_id") or "")
        if not prospect_id:
            prospect_id = _prospect_id(state=state, domain=domain, email=email)
        out.append(
            {
                "prospect_id": prospect_id,
                "firm": _normalize_text(row.get("firm") or row.get("company_name") or ""),
                "email": email,
                "title": _normalize_text(row.get("title") or row.get("contact_role") or ""),
                "city": _normalize_text(row.get("city") or ""),
                "state": state,
                "source": _normalize_text(row.get("source") or "seed_recipients_pools"),
                "contact_name": _normalize_text(row.get("contact_name") or ""),
                "website": _normalize_text(row.get("website") or ""),
            }
        )
    return out


def _write_output_atomic(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = _discovery_fields()
    with tempfile.NamedTemporaryFile(
        "w",
        newline="",
        encoding="utf-8",
        dir=str(path.parent),
        prefix="prospects_latest_",
        suffix=".tmp",
        delete=False,
    ) as tmp:
        writer = csv.DictWriter(tmp, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
        tmp_path = Path(tmp.name)

    os.replace(str(tmp_path), str(path))


def _clean_csv_row(row: dict[str, str]) -> dict[str, str]:
    clean: dict[str, str] = {}
    for k, v in dict(row).items():
        key = str(k or "").lstrip("\ufeff")
        clean[key] = str(v or "")
    return clean


def _inbox_input_paths(inbox_dir: Path) -> list[Path]:
    if not inbox_dir.exists():
        return []
    paths = [p for p in inbox_dir.glob("*.csv") if p.is_file()]
    return sorted(paths, key=lambda p: p.name.lower())


def _collect_inbox_files(data_dir: Path) -> tuple[Path, Path, list[dict], int]:
    new_dir = _generation_inbox_dir(data_dir)
    old_dir = _legacy_inbox_dir(data_dir)
    items: list[dict] = []

    new_paths = _inbox_input_paths(new_dir)
    for path in new_paths:
        items.append({"path": path, "deprecated": False, "inbox_dir": new_dir})

    old_paths = _inbox_input_paths(old_dir)
    for path in old_paths:
        items.append({"path": path, "deprecated": True, "inbox_dir": old_dir})

    return new_dir, old_dir, items, len(old_paths)


def _inbox_rows(items: list[dict]) -> tuple[list[dict[str, str]], int, int, int]:
    normalized_rows: list[dict[str, str]] = []
    rows_read = 0
    rows_accepted = 0
    rows_missing_state = 0

    for item in items:
        path = Path(item["path"])
        with open(path, "r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for raw_row in reader:
                row = _clean_csv_row(raw_row)
                rows_read += 1

                email = _normalize_email(row.get("email") or row.get("contact_email") or "")
                if not _valid_email(email):
                    continue

                state = _normalize_state(row.get("state") or "")
                if not state:
                    rows_missing_state += 1

                source = _normalize_text(row.get("source") or "")
                if not source:
                    source = f"inbox_drop:{path.stem}"

                normalized_rows.append(
                    {
                        "prospect_id": _normalize_text(row.get("prospect_id") or ""),
                        "company_name": _normalize_text(row.get("firm") or row.get("company_name") or ""),
                        "contact_email": email,
                        "contact_role": _normalize_text(row.get("title") or row.get("contact_role") or ""),
                        "contact_name": _normalize_text(row.get("contact_name") or ""),
                        "city": _normalize_text(row.get("city") or ""),
                        "state": state,
                        "domain": _normalize_text(row.get("domain") or "").lower() or _email_domain(email),
                        "source": source,
                        "website": _normalize_text(row.get("website") or ""),
                    }
                )
                rows_accepted += 1

    return normalized_rows, rows_read, rows_accepted, rows_missing_state


def _archive_inbox_files(items: list[dict], run_date: date) -> int:
    if not items:
        return 0
    archived = 0
    for item in items:
        path = Path(item["path"])
        inbox_dir = Path(item["inbox_dir"])
        day_dir = inbox_dir / INBOX_PROCESSED_SUBDIR / run_date.isoformat()
        day_dir.mkdir(parents=True, exist_ok=True)
        dest = day_dir / path.name
        try:
            shutil.move(str(path), str(dest))
            archived += 1
        except Exception as exc:
            print(f"{WARN_GENERATOR_INBOX_ARCHIVE} path={path.resolve()} err={exc}")
    return archived


def _connect_crm_if_exists(crm_db: Path) -> sqlite3.Connection | None:
    if not crm_db.exists():
        return None
    conn = sqlite3.connect(str(crm_db))
    conn.row_factory = sqlite3.Row
    return conn


def _load_suppression_csv(data_dir: Path) -> set[str]:
    suppression_path = data_dir / "suppression.csv"
    suppressed: set[str] = set()
    if not suppression_path.exists():
        return suppressed
    try:
        with open(suppression_path, "r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                email = _normalize_email((row or {}).get("email") or "")
                if _valid_email(email):
                    suppressed.add(email)
    except Exception:
        return suppressed
    return suppressed


def _load_suppression_db(conn: sqlite3.Connection | None) -> set[str]:
    if conn is None or not _table_exists(conn, "suppression"):
        return set()
    out: set[str] = set()
    try:
        for row in conn.execute("SELECT email FROM suppression"):
            email = _normalize_email(str(row[0] or ""))
            if _valid_email(email):
                out.add(email)
    except Exception:
        return out
    return out


def _load_suppression_set(data_dir: Path, conn: sqlite3.Connection | None) -> set[str]:
    return _load_suppression_csv(data_dir) | _load_suppression_db(conn)


def _existing_crm_emails(conn: sqlite3.Connection | None) -> set[str]:
    if conn is None or not _table_exists(conn, "prospects"):
        return set()
    out: set[str] = set()
    try:
        for row in conn.execute("SELECT email FROM prospects"):
            email = _normalize_email(str(row[0] or ""))
            if _valid_email(email):
                out.add(email)
    except Exception:
        return out
    return out


def _fetch_prior_sent_ids(conn: sqlite3.Connection | None) -> set[str]:
    if conn is None or not _table_exists(conn, "outreach_events"):
        return set()
    out: set[str] = set()
    try:
        rows = conn.execute("SELECT DISTINCT prospect_id FROM outreach_events WHERE event_type = 'sent'").fetchall()
        for row in rows:
            pid = _normalize_text(str(row[0] or ""))
            if pid:
                out.add(pid)
    except Exception:
        return out
    return out


def compute_uncontacted_backlog(conn: sqlite3.Connection | None, state: str, suppressed_emails: set[str]) -> int:
    if conn is None or not _table_exists(conn, "prospects"):
        return 0

    columns = _table_columns(conn, "prospects")
    if "prospect_id" not in columns or "email" not in columns:
        return 0

    sent_ids = _fetch_prior_sent_ids(conn)
    status_col = "status" if "status" in columns else "''"
    last_contacted_col = "last_contacted_at" if "last_contacted_at" in columns else "''"

    rows = conn.execute(
        f"""
        SELECT prospect_id, email, {status_col} AS status, {last_contacted_col} AS last_contacted_at
        FROM prospects
        WHERE UPPER(TRIM(COALESCE(state, ''))) = ?
        """,
        (_normalize_state(state),),
    ).fetchall()

    count = 0
    for row in rows:
        email = _normalize_email(str(row["email"] or ""))
        if not _valid_email(email):
            continue
        if _email_domain(email) in pools.FREE_EMAIL_DOMAINS:
            continue
        if email in suppressed_emails:
            continue

        status = _normalize_text(str(row["status"] or "")).lower()
        if status in EXCLUDED_STATUSES:
            continue

        prospect_id = _normalize_text(str(row["prospect_id"] or ""))
        if prospect_id and prospect_id in sent_ids:
            continue

        if _normalize_text(str(row["last_contacted_at"] or "")):
            continue
        count += 1
    return count


def _pool_totals_by_state(conn: sqlite3.Connection | None, states: list[str]) -> dict[str, int]:
    totals: dict[str, int] = {}
    unique_states: list[str] = []
    for state in states:
        s = _normalize_state(state)
        if not s or s in unique_states:
            continue
        unique_states.append(s)
        totals[s] = 0

    if conn is None or not unique_states or not _table_exists(conn, "prospects"):
        return totals

    placeholders = ",".join("?" for _ in unique_states)
    rows = conn.execute(
        f"""
        SELECT UPPER(TRIM(COALESCE(state, ''))) AS state_key, COUNT(*) AS row_count
        FROM prospects
        WHERE UPPER(TRIM(COALESCE(state, ''))) IN ({placeholders})
        GROUP BY UPPER(TRIM(COALESCE(state, '')))
        """,
        tuple(unique_states),
    ).fetchall()
    for row in rows:
        state_key = _normalize_state(str(row["state_key"] or ""))
        if not state_key:
            continue
        totals[state_key] = max(0, int(row["row_count"] or 0))
    return totals


def _default_state_autogrow_report(state: str, backlog_current: int, new_needed: int, cache_dir: Path) -> dict:
    return {
        "state": _normalize_state(state),
        "backlog_current": max(0, int(backlog_current)),
        "new_needed": max(0, int(new_needed)),
        "aiha_candidate": 0,
        "aiha_accepted": 0,
        "aiha_cache_path": prospect_sources_aiha._cache_path(cache_dir, _normalize_state(state)),
        "aiha_cache_used": False,
        "aiha_cache_age_days": -1,
        "aiha_pages_fetched": 0,
        "aiha_parse_mode": "FAILED",
        "aiha_rejected": Counter(),
        "diagnostics_path": None,
    }


def _filter_autogrow_candidates(
    rows: list[dict[str, str]],
    target_state: str,
    suppressed_emails: set[str],
    existing_crm_emails: set[str],
) -> tuple[list[dict[str, str]], Counter]:
    target = _normalize_state(target_state)
    seen_batch: set[str] = set()
    accepted: list[dict[str, str]] = []
    counters: Counter = Counter()

    for row in rows:
        email = _normalize_email(row.get("email") or row.get("contact_email") or "")
        if not _valid_email(email):
            counters["invalid_email"] += 1
            continue

        if _email_domain(email) in pools.FREE_EMAIL_DOMAINS:
            counters["free_domain"] += 1
            continue

        if email in suppressed_emails:
            counters["suppressed"] += 1
            continue

        if email in existing_crm_emails:
            counters["already_in_crm"] += 1
            continue

        state = _normalize_state(row.get("state") or "")
        if state != target:
            counters["state_mismatch"] += 1
            continue

        if email in seen_batch:
            counters["duplicate_in_batch"] += 1
            continue

        seen_batch.add(email)
        accepted.append(
            {
                "prospect_id": _normalize_text(row.get("prospect_id") or ""),
                "company_name": _normalize_text(row.get("firm") or row.get("company_name") or ""),
                "contact_email": email,
                "contact_role": _normalize_text(row.get("title") or row.get("contact_role") or "EHS Consultant"),
                "contact_name": _normalize_text(row.get("contact_name") or ""),
                "city": _normalize_text(row.get("city") or ""),
                "state": state,
                "domain": _normalize_text(row.get("domain") or "").lower() or _email_domain(email),
                "website": _normalize_text(row.get("website") or ""),
                "source": _normalize_text(row.get("source") or "aiha_consultants_listing"),
            }
        )

    accepted.sort(key=lambda r: (_normalize_email(r.get("contact_email") or ""), _normalize_text(r.get("company_name") or "")))
    return accepted, counters


def _print_tokens(
    path: Path,
    rows_read: int,
    rows_written: int,
    status: str,
    generation_inbox_dir: Path,
    inbox_files_found: int,
    inbox_rows_read: int,
    inbox_rows_accepted: int,
    inbox_rows_missing_state: int,
    autogrow: dict,
    selected_report: dict,
    state_reports: list[dict],
    diagnostics_path: Path | None,
    inbox_files_archived: int | None = None,
) -> None:
    print(f"GENERATOR_OUTPUT_PATH={path.resolve()}")
    print(f"GENERATOR_ROWS_READ={rows_read}")
    print(f"GENERATOR_ROWS_WRITTEN={rows_written}")
    print(f"GENERATOR_INBOX_DIR={generation_inbox_dir.resolve()}")
    print(f"GENERATOR_INBOX_FILES_FOUND={inbox_files_found}")
    print(f"GENERATOR_INBOX_ROWS_READ={inbox_rows_read}")
    print(f"GENERATOR_INBOX_ROWS_ACCEPTED={inbox_rows_accepted}")
    print(f"GENERATOR_INBOX_ROWS_MISSING_STATE={inbox_rows_missing_state}")
    if inbox_files_archived is not None:
        print(f"GENERATOR_INBOX_FILES_ARCHIVED={inbox_files_archived}")

    print(f"GENERATOR_AUTOGROW_ENABLED={1 if autogrow['enabled'] else 0}")
    print(f"GENERATOR_AUTOGROW_SOURCES={','.join(autogrow['sources'])}")
    print(f"GENERATOR_AUTOGROW_SELECTED_STATE={autogrow['selected_state']}")
    print(f"GENERATOR_AUTOGROW_BACKLOG_TARGET={autogrow['backlog_target']}")
    print(f"GENERATOR_AUTOGROW_BACKLOG_CURRENT={autogrow['backlog_current']}")
    print(f"GENERATOR_AUTOGROW_NEW_NEEDED={autogrow['new_needed']}")
    print(f"GENERATOR_AUTOGROW_MAX_FETCH_PAGES_PER_RUN={autogrow['max_fetch_pages']}")
    print(f"GENERATOR_AUTOGROW_HTTP_SLEEP_MS={autogrow['sleep_ms']}")
    print(f"GENERATOR_AUTOGROW_SAFETY_NET_FORCED={1 if autogrow['safety_net_forced'] else 0}")
    print(f"GENERATOR_AUTOGROW_SAFETY_NET_STATES={','.join(autogrow['safety_net_states']) if autogrow['safety_net_states'] else 'none'}")
    print(f"GENERATOR_AUTOGROW_TOTAL_STATES={int(autogrow['total_states'])}")
    print(f"GENERATOR_AUTOGROW_TOTAL_ACCEPTED={int(autogrow['total_accepted'])}")

    print(f"GENERATOR_AIHA_CACHE_PATH={Path(selected_report['aiha_cache_path']).resolve()}")
    print(f"GENERATOR_AIHA_CACHE_USED={'YES' if selected_report.get('aiha_cache_used') else 'NO'}")
    cache_age = selected_report.get("aiha_cache_age_days")
    print(f"GENERATOR_AIHA_CACHE_AGE_DAYS={cache_age if cache_age is not None else -1}")
    print(f"GENERATOR_AIHA_PAGES_FETCHED={int(selected_report.get('aiha_pages_fetched') or 0)}")
    print(f"GENERATOR_AIHA_PAGE_PARSE_MODE={selected_report.get('aiha_parse_mode') or 'FAILED'}")
    print(f"GENERATOR_AIHA_ROWS_CANDIDATE={int(selected_report.get('aiha_candidate') or 0)}")
    print(f"GENERATOR_AIHA_ROWS_ACCEPTED={int(selected_report.get('aiha_accepted') or 0)}")

    aiha_rejected = selected_report.get("aiha_rejected") or Counter()
    print(f"GENERATOR_AIHA_REJECTED_INVALID_EMAIL={int(aiha_rejected.get('invalid_email', 0))}")
    print(f"GENERATOR_AIHA_REJECTED_FREE_DOMAIN={int(aiha_rejected.get('free_domain', 0))}")
    print(f"GENERATOR_AIHA_REJECTED_SUPPRESSED={int(aiha_rejected.get('suppressed', 0))}")
    print(f"GENERATOR_AIHA_REJECTED_ALREADY_IN_CRM={int(aiha_rejected.get('already_in_crm', 0))}")
    print(f"GENERATOR_AIHA_REJECTED_STATE_MISMATCH={int(aiha_rejected.get('state_mismatch', 0))}")
    print(f"GENERATOR_AIHA_REJECTED_DUPLICATE_IN_BATCH={int(aiha_rejected.get('duplicate_in_batch', 0))}")

    for report in state_reports:
        state = _normalize_state(str(report.get("state") or ""))
        if not state:
            continue
        print(
            "GENERATOR_AUTOGROW_STATE="
            f"{state} backlog_current={int(report.get('backlog_current') or 0)} "
            f"new_needed={int(report.get('new_needed') or 0)} "
            f"aiha_candidate={int(report.get('aiha_candidate') or 0)} "
            f"aiha_accepted={int(report.get('aiha_accepted') or 0)}"
        )

    if diagnostics_path is not None:
        print(f"GENERATOR_DIAGNOSTICS_PATH={diagnostics_path.resolve()}")

    print(f"GENERATOR_COMPLETE status={status}")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Generate deterministic discovery CSV feed from seed pools + optional autogrow.")
    ap.add_argument("--print-config", action="store_true", help="Print resolved output path and exit.")
    ap.add_argument("--dry-run", action="store_true", help="Compute rows only; do not write output files.")
    ap.add_argument("--for-date", default="", help="Override run date (YYYY-MM-DD) for selected_state/backlog preview.")
    args = ap.parse_args(argv)

    try:
        run_date = _parse_for_date(args.for_date)
    except Exception:
        print(f"{ERR_GENERATOR_FAILED} stage=for_date err=invalid_for_date", file=sys.stderr)
        return 2

    data_dir = crm_store.data_dir()
    output_path = _output_path(data_dir)
    generation_inbox_dir, legacy_inbox_dir, inbox_items, deprecated_file_count = _collect_inbox_files(data_dir)
    cache_dir = _generation_cache_dir(data_dir)
    diagnostics_dir = _generation_diagnostics_dir(data_dir)

    states = _parse_states(os.getenv("OUTREACH_STATES", "TX"))
    if not states:
        print(f"{ERR_GENERATOR_FAILED} stage=states err=OUTREACH_STATES empty", file=sys.stderr)
        return 2
    selected_state = _choose_state(states, run_date)

    try:
        autogrow_cfg = _parse_autogrow_config()
    except Exception as exc:
        print(f"{ERR_GENERATOR_FAILED} stage=autogrow_config err={exc}", file=sys.stderr)
        return 2

    crm_db = crm_store.crm_db_path()
    conn = _connect_crm_if_exists(crm_db)
    try:
        suppressed_emails = _load_suppression_set(data_dir=data_dir, conn=conn)
        existing_crm_emails = _existing_crm_emails(conn)
        backlog_by_state = {
            _normalize_state(state): compute_uncontacted_backlog(
                conn=conn,
                state=state,
                suppressed_emails=suppressed_emails,
            )
            for state in states
        }
        pool_totals_by_state = _pool_totals_by_state(conn=conn, states=states)
    finally:
        if conn is not None:
            conn.close()

    backlog_target = int(autogrow_cfg["backlog_target"])
    deficits_by_state = {
        _normalize_state(state): max(0, int(backlog_target) - int(backlog_by_state.get(_normalize_state(state), 0)))
        for state in states
    }

    effective_sources = list(autogrow_cfg["sources"])
    safety_net_states: list[str] = []
    safety_net_forced = False
    autogrow_enabled = bool(autogrow_cfg["enabled"])
    if not autogrow_enabled and bool(autogrow_cfg.get("safety_net_enabled")):
        for state in states:
            s = _normalize_state(state)
            if not s:
                continue
            backlog_current = int(backlog_by_state.get(s, 0))
            pool_total = int(pool_totals_by_state.get(s, 0))
            if backlog_current <= 0 and pool_total > 0:
                safety_net_states.append(s)
        if safety_net_states:
            safety_net_forced = True
            autogrow_enabled = True
            if not effective_sources:
                effective_sources = ["AIHA"]
            print(
                f"{WARN_AUTOGROW_SAFETY_NET_FORCED} states={','.join(safety_net_states)} "
                f"sources={','.join(effective_sources) if effective_sources else '(none)'}"
            )

    state_reports: list[dict] = []
    state_report_by_state: dict[str, dict] = {}
    for state in states:
        s = _normalize_state(state)
        report = _default_state_autogrow_report(
            state=s,
            backlog_current=int(backlog_by_state.get(s, 0)),
            new_needed=int(deficits_by_state.get(s, 0)),
            cache_dir=cache_dir,
        )
        state_reports.append(report)
        state_report_by_state[s] = report

    selected_report = state_report_by_state.get(
        _normalize_state(selected_state),
        _default_state_autogrow_report(
            state=selected_state,
            backlog_current=0,
            new_needed=0,
            cache_dir=cache_dir,
        ),
    )

    autogrow_state = {
        "enabled": bool(autogrow_enabled),
        "sources": list(effective_sources),
        "selected_state": selected_state,
        "backlog_target": int(backlog_target),
        "backlog_current": int(selected_report["backlog_current"]),
        "new_needed": int(selected_report["new_needed"]),
        "max_fetch_pages": int(autogrow_cfg["max_fetch_pages"]),
        "sleep_ms": int(autogrow_cfg["sleep_ms"]),
        "safety_net_forced": bool(safety_net_forced),
        "safety_net_states": list(safety_net_states),
        "total_states": int(len(states)),
        "total_accepted": 0,
    }
    diagnostics_path: Path | None = None
    autogrow_rows: list[dict[str, str]] = []
    rows_read_autogrow = 0
    existing_email_reservoir = set(existing_crm_emails)

    if args.print_config:
        print(f"{PASS_GENERATOR_PRINT_CONFIG} data_dir={data_dir.resolve()}")
        print(f"{PASS_GENERATOR_PRINT_CONFIG} output_path={output_path.resolve()}")
        print(f"{PASS_GENERATOR_PRINT_CONFIG} inbox_dir={generation_inbox_dir.resolve()}")
        print(f"{PASS_GENERATOR_PRINT_CONFIG} legacy_inbox_dir={legacy_inbox_dir.resolve()}")
        print(f"{PASS_GENERATOR_PRINT_CONFIG} cache_dir={cache_dir.resolve()}")
        print(f"{PASS_GENERATOR_PRINT_CONFIG} diagnostics_dir={diagnostics_dir.resolve()}")
        print(f"{PASS_GENERATOR_PRINT_CONFIG} selected_state={selected_state}")
        print(f"{PASS_GENERATOR_PRINT_CONFIG} run_date={run_date.isoformat()}")
        _print_tokens(
            path=output_path,
            rows_read=0,
            rows_written=0,
            status="PRINT_CONFIG",
            generation_inbox_dir=generation_inbox_dir,
            inbox_files_found=len(inbox_items),
            inbox_rows_read=0,
            inbox_rows_accepted=0,
            inbox_rows_missing_state=0,
            autogrow=autogrow_state,
            selected_report=selected_report,
            state_reports=state_reports,
            diagnostics_path=None,
            inbox_files_archived=None,
        )
        return 0

    try:
        state_rows, rows_read_seed = _build_clean_state_rows(states=states)
    except Exception as exc:
        print(f"{ERR_GENERATOR_FAILED} stage=build_rows err={exc}", file=sys.stderr)
        return 2

    try:
        inbox_rows, inbox_rows_read, inbox_rows_accepted, inbox_rows_missing_state = _inbox_rows(inbox_items)
    except Exception as exc:
        print(f"{ERR_GENERATOR_FAILED} stage=read_inbox err={exc}", file=sys.stderr)
        return 2

    if deprecated_file_count > 0:
        print(
            f"{WARN_INBOX_PATH_DEPRECATED} old={legacy_inbox_dir.resolve()} "
            f"new={generation_inbox_dir.resolve()} files={deprecated_file_count}"
        )

    if autogrow_enabled and "AIHA" in effective_sources:
        for state in states:
            s = _normalize_state(state)
            report = state_report_by_state.get(s)
            if report is None:
                continue
            if int(report["new_needed"]) <= 0:
                continue
            if safety_net_forced and s not in safety_net_states:
                continue
            if len(s) != 2:
                print(f"{WARN_AUTOGROWTH_SOURCE_FAILED} source=aiha state={s} err=unsupported_state_code")
                continue

            result = prospect_sources_aiha.fetch_aiha_state_rows(
                state=s,
                run_date=run_date,
                max_pages=int(autogrow_cfg["max_fetch_pages"]),
                sleep_ms=int(autogrow_cfg["sleep_ms"]),
                cache_dir=cache_dir,
                diagnostics_dir=diagnostics_dir,
                allow_cache_write=not bool(args.dry_run),
            )

            candidate_rows = list(result.get("rows") or [])
            report["aiha_candidate"] = int(len(candidate_rows))
            report["aiha_cache_used"] = bool(result.get("cache_used"))
            cache_age = result.get("cache_age_days")
            report["aiha_cache_age_days"] = int(cache_age) if isinstance(cache_age, int) else -1
            report["aiha_pages_fetched"] = max(0, int(result.get("pages_fetched") or 0))
            report["aiha_parse_mode"] = str(result.get("parse_mode") or "FAILED")

            filtered_rows, aiha_rejected = _filter_autogrow_candidates(
                rows=candidate_rows,
                target_state=s,
                suppressed_emails=suppressed_emails,
                existing_crm_emails=existing_email_reservoir,
            )
            accepted_rows = filtered_rows[: int(report["new_needed"])]
            report["aiha_accepted"] = int(len(accepted_rows))
            report["aiha_rejected"] = aiha_rejected
            autogrow_rows.extend(accepted_rows)
            rows_read_autogrow += int(report["aiha_candidate"])
            for row in accepted_rows:
                email = _normalize_email(row.get("contact_email") or row.get("email") or "")
                if email:
                    existing_email_reservoir.add(email)

            diag = result.get("diagnostics_path")
            if isinstance(diag, Path):
                report["diagnostics_path"] = diag
            elif diag:
                report["diagnostics_path"] = Path(str(diag))

            if result.get("error"):
                print(f"{WARN_AUTOGROWTH_SOURCE_FAILED} source=aiha state={s} err={result.get('error')}")

    autogrow_state["total_accepted"] = int(sum(int(r.get("aiha_accepted") or 0) for r in state_reports))
    if isinstance(selected_report.get("diagnostics_path"), Path):
        diagnostics_path = selected_report.get("diagnostics_path")
    else:
        for report in state_reports:
            diag = report.get("diagnostics_path")
            if isinstance(diag, Path):
                diagnostics_path = diag
                break

    rows_read_total = rows_read_seed + inbox_rows_read + rows_read_autogrow

    if args.dry_run:
        seed_rows = _state_rows_to_combined_input(state_rows=state_rows, states=states)
        rows = _to_discovery_rows(inbox_rows + seed_rows + autogrow_rows)
        _print_tokens(
            path=output_path,
            rows_read=rows_read_total,
            rows_written=len(rows),
            status="DRY_RUN",
            generation_inbox_dir=generation_inbox_dir,
            inbox_files_found=len(inbox_items),
            inbox_rows_read=inbox_rows_read,
            inbox_rows_accepted=inbox_rows_accepted,
            inbox_rows_missing_state=inbox_rows_missing_state,
            autogrow=autogrow_state,
            selected_report=selected_report,
            state_reports=state_reports,
            diagnostics_path=diagnostics_path,
        )
        return 0

    try:
        _write_legacy_pool_files(state_rows)
        seed_rows = _state_rows_to_combined_input(state_rows=state_rows, states=states)
        rows = _to_discovery_rows(inbox_rows + seed_rows + autogrow_rows)
        _write_output_atomic(path=output_path, rows=rows)
        archived_count = _archive_inbox_files(items=inbox_items, run_date=run_date)
    except Exception as exc:
        print(f"{ERR_GENERATOR_FAILED} stage=write_output err={exc}", file=sys.stderr)
        return 2

    _print_tokens(
        path=output_path,
        rows_read=rows_read_total,
        rows_written=len(rows),
        status="OK",
        generation_inbox_dir=generation_inbox_dir,
        inbox_files_found=len(inbox_items),
        inbox_rows_read=inbox_rows_read,
        inbox_rows_accepted=inbox_rows_accepted,
        inbox_rows_missing_state=inbox_rows_missing_state,
        inbox_files_archived=archived_count,
        autogrow=autogrow_state,
        selected_report=selected_report,
        state_reports=state_reports,
        diagnostics_path=diagnostics_path,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
