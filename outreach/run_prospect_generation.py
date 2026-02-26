import argparse
import csv
import hashlib
import os
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
from outreach import prospect_sources_apollo
from outreach import prospect_sources_aiha
from outreach import prospect_sources_ohs_bg
import seed_recipients_pools as pools


ERR_GENERATOR_FAILED = "ERR_GENERATOR_FAILED"
PASS_GENERATOR_PRINT_CONFIG = "PASS_GENERATOR_PRINT_CONFIG"
WARN_AUTOGROWTH_SOURCE_FAILED = "WARN_AUTOGROWTH_SOURCE_FAILED"
APOLLO_FORBIDDEN_HINT = "CHECK_MASTER_KEY_OR_ENDPOINT_SCOPES"
APOLLO_DOCTOR_NOT_FOUND_HINT = "CHECK_METHOD_AND_BASE_URL"

OUTPUT_SUBDIR = ("prospect_discovery",)
OUTPUT_FILENAME = "prospects_latest.csv"

GENERATION_CACHE_ROOT_SUBDIR = ("prospect_generation", "cache")
GENERATION_DIAGNOSTICS_SUBDIR = ("prospect_generation", "diagnostics")

AUTOGROW_ALLOWED_SOURCES = {"AIHA", "OHS_BG", "APOLLO"}
AUTOGROW_REJECT_KEYS = (
    "invalid_email",
    "free_domain",
    "suppressed",
    "already_in_crm",
    "missing_state",
    "state_mismatch",
    "duplicate_in_batch",
)
EXCLUDED_STATUSES = {"do_not_contact", "unsubscribed", "bounced", "converted"}
APOLLO_DEFAULT_PERSON_TITLES = [
    "labor and employment attorney",
    "employment attorney",
    "osha attorney",
    "workplace safety attorney",
    "litigation attorney",
    "partner",
    "counsel",
    "ehs consultant",
    "safety consultant",
    "industrial hygienist",
    "safety director",
    "ehs director",
    "principal consultant",
    "owner",
]


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


def _generation_cache_dir(data_dir: Path) -> Path:
    # Backward-compatible alias used by existing print-config/tests: AIHA cache dir.
    return data_dir.joinpath(*GENERATION_CACHE_ROOT_SUBDIR) / "aiha"


def _generation_cache_root_dir(data_dir: Path) -> Path:
    return data_dir.joinpath(*GENERATION_CACHE_ROOT_SUBDIR)


def _autogrow_source_cache_dir(cache_root_dir: Path, source_token: str) -> Path:
    token = _normalize_state(source_token)
    if token == "AIHA":
        return cache_root_dir / "aiha"
    if token == "OHS_BG":
        return cache_root_dir / "ohs_bg"
    return cache_root_dir / token.lower()


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


def _parse_csv_items(raw: str) -> list[str]:
    items: list[str] = []
    for token in str(raw or "").split(","):
        value = _normalize_text(token)
        if not value:
            continue
        if value not in items:
            items.append(value)
    return items


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


def _parse_apollo_config(autogrow_sources: list[str]) -> dict:
    source_tokens = [_normalize_state(s) for s in list(autogrow_sources or [])]
    apollo_source_enabled = "APOLLO" in source_tokens
    api_key = _normalize_text(os.getenv("APOLLO_API_KEY", ""))
    enrich_enabled = _bool_env(os.getenv("APOLLO_ENRICH_ENABLED", "0"))
    enrich_max_per_run = _parse_int_env(os.getenv("APOLLO_ENRICH_MAX_PER_RUN", ""), default=50, minimum=1)
    person_locations_mode = _normalize_text(os.getenv("APOLLO_PERSON_LOCATIONS_MODE", "state")).lower() or "state"
    if person_locations_mode != "state":
        raise ValueError("invalid_APOLLO_PERSON_LOCATIONS_MODE")
    person_titles = _parse_csv_items(os.getenv("APOLLO_PERSON_TITLES", ""))
    if not person_titles:
        person_titles = list(APOLLO_DEFAULT_PERSON_TITLES)
    if apollo_source_enabled and not api_key:
        raise ValueError("missing_APOLLO_API_KEY")
    return {
        "source_enabled": apollo_source_enabled,
        "api_key": api_key,
        "enrich_enabled": enrich_enabled,
        "enrich_max_per_run": enrich_max_per_run,
        "person_titles": person_titles,
        "person_locations_mode": person_locations_mode,
    }


def _build_clean_state_rows() -> tuple[dict[str, list[dict[str, str]]], int]:
    state_rows: dict[str, list[dict[str, str]]] = {}
    rows_read = 0
    pools_by_state = {
        "TX": pools.TX_POOL,
        "CA": pools.CA_POOL,
        "FL": pools.FL_POOL,
    }

    for state, seed_rows in pools_by_state.items():
        deduped = pools.dedupe_rows(seed_rows)
        cleaned, _stats = pools.apply_hygiene(deduped)
        state_rows[state] = cleaned
        rows_read += len(cleaned)
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


def _state_rows_to_combined_input(state_rows: dict[str, list[dict[str, str]]]) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    for state in ["TX", "CA", "FL"]:
        out.extend(state_rows.get(state, []))
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


def _count_crm_pool_total(conn: sqlite3.Connection | None, state: str) -> int:
    if conn is None or not _table_exists(conn, "prospects"):
        return 0
    try:
        row = conn.execute(
            """
            SELECT COUNT(*)
            FROM prospects
            WHERE UPPER(TRIM(COALESCE(state, ''))) = ?
            """,
            (_normalize_state(state),),
        ).fetchone()
    except Exception:
        return 0
    if not row:
        return 0
    try:
        return max(0, int(row[0] or 0))
    except Exception:
        return 0


def _filter_autogrow_candidates(
    rows: list[dict[str, str]],
    target_state: str,
    suppressed_emails: set[str],
    existing_crm_emails: set[str],
    preseen_batch_emails: set[str] | None = None,
) -> tuple[list[dict[str, str]], Counter]:
    target = _normalize_state(target_state)
    seen_batch: set[str] = set()
    preseen_batch: set[str] = set(preseen_batch_emails or set())
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

        if email in preseen_batch:
            counters["duplicate_in_batch"] += 1
            continue

        if email in existing_crm_emails:
            counters["already_in_crm"] += 1
            continue

        state = _normalize_state(row.get("state") or "")
        if not state:
            counters["missing_state"] += 1
            continue
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
    autogrow: dict,
    aiha_result: dict,
    aiha_rejected: Counter,
    ohs_bg_result: dict,
    ohs_bg_rejected: Counter,
    apollo_cfg: dict,
    apollo_result: dict,
    apollo_rejected: Counter,
    diagnostics_path: Path | None,
) -> None:
    print(f"GENERATOR_OUTPUT_PATH={path.resolve()}")
    print(f"GENERATOR_ROWS_READ={rows_read}")
    print(f"GENERATOR_ROWS_WRITTEN={rows_written}")

    print(f"GENERATOR_AUTOGROW_ENABLED={1 if autogrow['enabled'] else 0}")
    print(f"GENERATOR_AUTOGROW_SOURCES={','.join(autogrow['sources'])}")
    autogrow_states = [str(s or "").strip().upper() for s in list(autogrow.get("states") or []) if str(s or "").strip()]
    print(f"GENERATOR_AUTOGROW_STATES={','.join(autogrow_states)}")
    print(f"GENERATOR_AUTOGROW_SOURCES_EMPTY={1 if autogrow.get('sources_empty') else 0}")
    print(f"GENERATOR_AUTOGROW_SELECTED_STATE={autogrow['selected_state']}")
    print(f"GENERATOR_AUTOGROW_BACKLOG_TARGET={autogrow['backlog_target']}")
    print(f"GENERATOR_AUTOGROW_BACKLOG_CURRENT={autogrow['backlog_current']}")
    print(f"GENERATOR_AUTOGROW_NEW_NEEDED={autogrow['new_needed']}")
    print(f"GENERATOR_AUTOGROW_MAX_FETCH_PAGES_PER_RUN={autogrow['max_fetch_pages']}")
    print(f"GENERATOR_AUTOGROW_HTTP_SLEEP_MS={autogrow['sleep_ms']}")
    safety_net_forced = bool(autogrow.get("safety_net_forced"))
    print(f"GENERATOR_AUTOGROW_SAFETY_NET_FORCED={1 if safety_net_forced else 0}")
    safety_net_states = [str(s or "").strip().upper() for s in list(autogrow.get("safety_net_states") or []) if str(s or "").strip()]
    print(f"GENERATOR_AUTOGROW_SAFETY_NET_STATES={','.join(safety_net_states) if safety_net_states else 'none'}")
    state_details = list(autogrow.get("state_details") or [])
    print(f"GENERATOR_AUTOGROW_TOTAL_STATES={int(autogrow.get('total_states') or len(state_details))}")
    print(f"GENERATOR_AUTOGROW_TOTAL_ACCEPTED={int(autogrow.get('total_accepted') or 0)}")
    for detail in state_details:
        state = _normalize_state(str(detail.get("state") or ""))
        if not state:
            continue
        print(
            "GENERATOR_AUTOGROW_STATE="
            f"{state} "
            f"backlog_current={int(detail.get('backlog_current') or 0)} "
            f"new_needed={int(detail.get('new_needed') or 0)} "
            f"aiha_candidate={int(detail.get('aiha_candidate') or 0)} "
            f"aiha_accepted={int(detail.get('aiha_accepted') or 0)} "
            f"ohs_bg_candidate={int(detail.get('ohs_bg_candidate') or 0)} "
            f"ohs_bg_accepted={int(detail.get('ohs_bg_accepted') or 0)} "
            f"apollo_candidate={int(detail.get('apollo_candidate') or 0)} "
            f"apollo_accepted={int(detail.get('apollo_accepted') or 0)}"
        )
        for source_label, prefix in (("AIHA", "aiha"), ("OHS_BG", "ohs_bg"), ("APOLLO", "apollo")):
            print(
                "GENERATOR_AUTOGROW_SOURCE_STATE "
                f"source={source_label} "
                f"state={state} "
                f"rows_candidate={int(detail.get(f'{prefix}_candidate') or 0)} "
                f"rows_accepted={int(detail.get(f'{prefix}_accepted') or 0)} "
                f"rejected_invalid_email={int(detail.get(f'{prefix}_rejected_invalid_email') or 0)} "
                f"rejected_free_domain={int(detail.get(f'{prefix}_rejected_free_domain') or 0)} "
                f"rejected_suppressed={int(detail.get(f'{prefix}_rejected_suppressed') or 0)} "
                f"rejected_already_in_crm={int(detail.get(f'{prefix}_rejected_already_in_crm') or 0)} "
                f"rejected_missing_state={int(detail.get(f'{prefix}_rejected_missing_state') or 0)} "
                f"rejected_state_mismatch={int(detail.get(f'{prefix}_rejected_state_mismatch') or 0)} "
                f"rejected_duplicate_in_batch={int(detail.get(f'{prefix}_rejected_duplicate_in_batch') or 0)}"
            )
    backlog_target = max(0, int(autogrow.get("backlog_target") or 0))
    for detail in state_details:
        state = _normalize_state(str(detail.get("state") or ""))
        if not state:
            continue
        backlog_current = max(0, int(detail.get("backlog_current") or 0))
        gap = max(0, backlog_target - backlog_current)
        if gap <= 0:
            continue
        print(
            "GENERATOR_STATE_BACKLOG_BELOW_TARGET "
            f"state={state} backlog_current={backlog_current} target={backlog_target} gap={gap}"
        )
    disabled_gap_states: list[str] = []
    if not bool(autogrow.get("enabled")):
        for detail in state_details:
            state = _normalize_state(str(detail.get("state") or ""))
            if not state:
                continue
            gap = max(0, backlog_target - int(detail.get("backlog_current") or 0))
            if gap > 0:
                disabled_gap_states.append(f"{state}:{gap}")
    print(
        "GENERATOR_AUTOGROW_DISABLED_BACKLOG_GAP="
        f"{1 if disabled_gap_states else 0} "
        f"states={','.join(disabled_gap_states) if disabled_gap_states else 'none'}"
    )

    print(f"GENERATOR_AIHA_CACHE_PATH={Path(aiha_result['cache_path']).resolve()}")
    print(f"GENERATOR_AIHA_CACHE_USED={'YES' if aiha_result.get('cache_used') else 'NO'}")
    cache_age = aiha_result.get("cache_age_days")
    print(f"GENERATOR_AIHA_CACHE_AGE_DAYS={cache_age if cache_age is not None else -1}")
    print(f"GENERATOR_AIHA_PAGES_FETCHED={int(aiha_result.get('pages_fetched') or 0)}")
    print(f"GENERATOR_AIHA_PAGE_PARSE_MODE={aiha_result.get('parse_mode') or 'FAILED'}")
    print(f"GENERATOR_AIHA_ROWS_CANDIDATE={int(aiha_result.get('rows_candidate') or 0)}")
    print(f"GENERATOR_AIHA_ROWS_ACCEPTED={int(aiha_result.get('rows_accepted') or 0)}")

    print(f"GENERATOR_AIHA_REJECTED_INVALID_EMAIL={int(aiha_rejected.get('invalid_email', 0))}")
    print(f"GENERATOR_AIHA_REJECTED_FREE_DOMAIN={int(aiha_rejected.get('free_domain', 0))}")
    print(f"GENERATOR_AIHA_REJECTED_SUPPRESSED={int(aiha_rejected.get('suppressed', 0))}")
    print(f"GENERATOR_AIHA_REJECTED_ALREADY_IN_CRM={int(aiha_rejected.get('already_in_crm', 0))}")
    print(f"GENERATOR_AIHA_REJECTED_MISSING_STATE={int(aiha_rejected.get('missing_state', 0))}")
    print(f"GENERATOR_AIHA_REJECTED_STATE_MISMATCH={int(aiha_rejected.get('state_mismatch', 0))}")
    print(f"GENERATOR_AIHA_REJECTED_DUPLICATE_IN_BATCH={int(aiha_rejected.get('duplicate_in_batch', 0))}")

    print(f"GENERATOR_OHS_BG_CACHE_PATH={Path(ohs_bg_result['cache_path']).resolve()}")
    print(f"GENERATOR_OHS_BG_CACHE_USED={'YES' if ohs_bg_result.get('cache_used') else 'NO'}")
    ohs_cache_age = ohs_bg_result.get("cache_age_days")
    print(f"GENERATOR_OHS_BG_CACHE_AGE_DAYS={ohs_cache_age if ohs_cache_age is not None else -1}")
    print(f"GENERATOR_OHS_BG_PAGES_FETCHED={int(ohs_bg_result.get('pages_fetched') or 0)}")
    print(f"GENERATOR_OHS_BG_PAGE_PARSE_MODE={ohs_bg_result.get('parse_mode') or 'FAILED'}")
    print(f"GENERATOR_OHS_BG_ROWS_CANDIDATE={int(ohs_bg_result.get('rows_candidate') or 0)}")
    print(f"GENERATOR_OHS_BG_ROWS_ACCEPTED={int(ohs_bg_result.get('rows_accepted') or 0)}")
    print(f"GENERATOR_OHS_BG_REJECTED_INVALID_EMAIL={int(ohs_bg_rejected.get('invalid_email', 0))}")
    print(f"GENERATOR_OHS_BG_REJECTED_FREE_DOMAIN={int(ohs_bg_rejected.get('free_domain', 0))}")
    print(f"GENERATOR_OHS_BG_REJECTED_SUPPRESSED={int(ohs_bg_rejected.get('suppressed', 0))}")
    print(f"GENERATOR_OHS_BG_REJECTED_ALREADY_IN_CRM={int(ohs_bg_rejected.get('already_in_crm', 0))}")
    print(f"GENERATOR_OHS_BG_REJECTED_MISSING_STATE={int(ohs_bg_rejected.get('missing_state', 0))}")
    print(f"GENERATOR_OHS_BG_REJECTED_STATE_MISMATCH={int(ohs_bg_rejected.get('state_mismatch', 0))}")
    print(f"GENERATOR_OHS_BG_REJECTED_DUPLICATE_IN_BATCH={int(ohs_bg_rejected.get('duplicate_in_batch', 0))}")

    print(f"GENERATOR_APOLLO_ENABLED={1 if apollo_cfg.get('source_enabled') else 0}")
    print(f"GENERATOR_APOLLO_ENRICH_ENABLED={1 if apollo_cfg.get('enrich_enabled') else 0}")
    print(f"GENERATOR_APOLLO_ENRICH_MAX_PER_RUN={int(apollo_cfg.get('enrich_max_per_run') or 0)}")
    print(f"GENERATOR_APOLLO_PERSON_TITLES={','.join(list(apollo_cfg.get('person_titles') or []))}")
    print(f"GENERATOR_APOLLO_PERSON_LOCATIONS_MODE={apollo_cfg.get('person_locations_mode') or 'state'}")
    print(f"GENERATOR_APOLLO_CACHE_PATH={Path(apollo_result['cache_path']).resolve()}")
    print(f"GENERATOR_APOLLO_CACHE_USED={'YES' if apollo_result.get('cache_used') else 'NO'}")
    apollo_cache_age = apollo_result.get("cache_age_days")
    print(f"GENERATOR_APOLLO_CACHE_AGE_DAYS={apollo_cache_age if apollo_cache_age is not None else -1}")
    print(f"GENERATOR_APOLLO_PAGE_PARSE_MODE={apollo_result.get('parse_mode') or 'FAILED'}")
    print(f"GENERATOR_APOLLO_SEARCH_PAGES_FETCHED={int(apollo_result.get('search_pages_fetched') or 0)}")
    print(f"GENERATOR_APOLLO_SEARCH_ROWS_RETURNED={int(apollo_result.get('search_rows_returned') or 0)}")
    print(f"GENERATOR_APOLLO_SEARCH_ROWS_HAS_EMAIL_TRUE={int(apollo_result.get('search_rows_has_email_true') or 0)}")
    print(f"GENERATOR_APOLLO_SEARCH_ROWS_DEDUPED_ID={int(apollo_result.get('search_rows_deduped_id') or 0)}")
    print(f"GENERATOR_APOLLO_ENRICH_ATTEMPTED={int(apollo_result.get('enrich_attempted') or 0)}")
    print(f"GENERATOR_APOLLO_ENRICHED={int(apollo_result.get('enriched') or 0)}")
    print(f"GENERATOR_APOLLO_ENRICH_NO_MATCH={int(apollo_result.get('enrich_no_match') or 0)}")
    print(f"GENERATOR_APOLLO_ENRICH_SKIPPED_CREDIT_CAP={int(apollo_result.get('enrich_skipped_credit_cap') or 0)}")
    print(f"GENERATOR_APOLLO_CREDIT_CAP_HIT={1 if apollo_result.get('credit_cap_hit') else 0}")
    print(
        "GENERATOR_APOLLO_FORBIDDEN="
        f"{1 if apollo_result.get('forbidden') else 0} "
        f"hint={APOLLO_FORBIDDEN_HINT}"
    )
    print(f"GENERATOR_APOLLO_REJECTED_INVALID_EMAIL={int(apollo_rejected.get('invalid_email', 0))}")
    print(f"GENERATOR_APOLLO_REJECTED_FREE_DOMAIN={int(apollo_rejected.get('free_domain', 0))}")
    print(f"GENERATOR_APOLLO_REJECTED_SUPPRESSED={int(apollo_rejected.get('suppressed', 0))}")
    print(f"GENERATOR_APOLLO_REJECTED_ALREADY_IN_CRM={int(apollo_rejected.get('already_in_crm', 0))}")
    print(f"GENERATOR_APOLLO_REJECTED_MISSING_STATE={int(apollo_rejected.get('missing_state', 0))}")
    print(f"GENERATOR_APOLLO_REJECTED_STATE_MISMATCH={int(apollo_rejected.get('state_mismatch', 0))}")
    print(f"GENERATOR_APOLLO_REJECTED_DUPLICATE_IN_BATCH={int(apollo_rejected.get('duplicate_in_batch', 0))}")

    if diagnostics_path is not None:
        print(f"GENERATOR_DIAGNOSTICS_PATH={diagnostics_path.resolve()}")

    print(f"GENERATOR_COMPLETE status={status}")


def _default_autogrow_source_result(cache_path: Path, enabled: bool, sources_empty: bool) -> dict:
    return {
        "cache_path": cache_path,
        "cache_used": False,
        "cache_age_days": None,
        "pages_fetched": 0,
        "parse_mode": ("SKIP_NO_SOURCES" if enabled and sources_empty else "FAILED"),
        "rows_candidate": 0,
        "rows_accepted": 0,
    }


def _fetch_autogrow_source_rows(
    source_token: str,
    state: str,
    run_date: date,
    max_fetch_pages: int,
    sleep_ms: int,
    cache_root_dir: Path,
    diagnostics_dir: Path,
    allow_cache_write: bool,
    apollo_cfg: dict | None = None,
    apollo_enrich_limit: int = 0,
) -> dict:
    token = _normalize_state(source_token)
    cache_dir = _autogrow_source_cache_dir(cache_root_dir, token)
    if token == "AIHA":
        return prospect_sources_aiha.fetch_aiha_state_rows(
            state=state,
            run_date=run_date,
            max_pages=max_fetch_pages,
            sleep_ms=sleep_ms,
            cache_dir=cache_dir,
            diagnostics_dir=diagnostics_dir,
            allow_cache_write=allow_cache_write,
        )
    if token == "OHS_BG":
        return prospect_sources_ohs_bg.fetch_ohs_bg_state_rows(
            state=state,
            run_date=run_date,
            max_pages=max_fetch_pages,
            sleep_ms=sleep_ms,
            cache_dir=cache_dir,
            diagnostics_dir=diagnostics_dir,
            allow_cache_write=allow_cache_write,
        )
    if token == "APOLLO":
        cfg = dict(apollo_cfg or {})
        return prospect_sources_apollo.fetch_apollo_state_rows(
            state=state,
            run_date=run_date,
            max_pages=max_fetch_pages,
            sleep_ms=sleep_ms,
            cache_dir=cache_dir,
            diagnostics_dir=diagnostics_dir,
            api_key=str(cfg.get("api_key") or ""),
            enrich_enabled=bool(cfg.get("enrich_enabled")),
            enrich_limit=max(0, int(apollo_enrich_limit)),
            person_titles=list(cfg.get("person_titles") or []),
            person_locations_mode=str(cfg.get("person_locations_mode") or "state"),
            allow_cache_write=allow_cache_write,
        )
    raise ValueError(f"unsupported_source={token}")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Generate deterministic discovery CSV feed from seed pools + optional autogrow.")
    ap.add_argument("--print-config", action="store_true", help="Print resolved output path and exit.")
    ap.add_argument("--apollo-doctor", action="store_true", help="Check Apollo master-key endpoint access and exit.")
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
    cache_dir = _generation_cache_dir(data_dir)
    cache_root_dir = _generation_cache_root_dir(data_dir)
    diagnostics_dir = _generation_diagnostics_dir(data_dir)

    if args.apollo_doctor:
        try:
            apollo_cfg_for_doctor = _parse_apollo_config(["APOLLO"])
        except Exception as exc:
            print(f"APOLLO_DOCTOR_ERROR=1 stage=config err={exc}")
            return 0
        try:
            doctor = prospect_sources_apollo.doctor_apollo_api(
                api_key=str(apollo_cfg_for_doctor.get("api_key") or ""),
                sleep_ms=0,
                diagnostics_dir=diagnostics_dir,
            )
        except Exception as exc:
            print(f"APOLLO_DOCTOR_ERROR=1 stage=request err={exc}")
            return 0
        diag = doctor.get("diagnostics_path")
        resolved_diag: Path | None = diag if isinstance(diag, Path) else (Path(str(diag)) if diag else None)
        if doctor.get("forbidden"):
            print(f"APOLLO_DOCTOR_FORBIDDEN=1 hint={APOLLO_FORBIDDEN_HINT}")
            if resolved_diag is not None:
                print(f"APOLLO_DOCTOR_DIAGNOSTICS_PATH={resolved_diag.resolve()}")
            return 0
        if doctor.get("not_found"):
            print(f"APOLLO_DOCTOR_NOT_FOUND=1 hint={APOLLO_DOCTOR_NOT_FOUND_HINT}")
            if resolved_diag is not None:
                print(f"APOLLO_DOCTOR_DIAGNOSTICS_PATH={resolved_diag.resolve()}")
            return 0
        if doctor.get("ok"):
            print("APOLLO_DOCTOR_OK=1")
            return 0
        print(
            "APOLLO_DOCTOR_HTTP_ERROR=1 "
            f"status={int(doctor.get('status') or 0)} "
            f"content_type={doctor.get('content_type') or 'unknown'}"
        )
        if resolved_diag is not None:
            print(f"APOLLO_DOCTOR_DIAGNOSTICS_PATH={resolved_diag.resolve()}")
        return 0

    states = _parse_states(os.getenv("OUTREACH_STATES", "TX"))
    if not states:
        print(f"{ERR_GENERATOR_FAILED} stage=states err=OUTREACH_STATES empty", file=sys.stderr)
        return 2
    selected_state = _choose_state(states, run_date)
    autogrow_states = _parse_states(os.getenv("PROSPECT_AUTOGROW_STATES", "")) or list(states)

    try:
        autogrow_cfg = _parse_autogrow_config()
    except Exception as exc:
        print(f"{ERR_GENERATOR_FAILED} stage=autogrow_config err={exc}", file=sys.stderr)
        return 2
    try:
        apollo_cfg = _parse_apollo_config(list(autogrow_cfg.get("sources") or []))
    except Exception as exc:
        print(f"{ERR_GENERATOR_FAILED} stage=apollo_config err={exc}", file=sys.stderr)
        return 2

    crm_db = crm_store.crm_db_path()
    conn = _connect_crm_if_exists(crm_db)
    autogrow_state_details: list[dict[str, object]] = []
    selected_backlog_current = 0
    selected_new_needed = 0
    safety_net_forced_states: list[str] = []
    try:
        suppressed_emails = _load_suppression_set(data_dir=data_dir, conn=conn)
        existing_crm_emails = _existing_crm_emails(conn)
        for state_item in autogrow_states:
            backlog_current_item = compute_uncontacted_backlog(conn=conn, state=state_item, suppressed_emails=suppressed_emails)
            pool_total_current = _count_crm_pool_total(conn=conn, state=state_item)
            safety_forced = bool(
                (not bool(autogrow_cfg["enabled"]))
                and bool(autogrow_cfg.get("safety_net_enabled"))
                and int(pool_total_current) > 0
                and int(backlog_current_item) == 0
            )
            effective_autogrow = bool(autogrow_cfg["enabled"]) or safety_forced
            new_needed_item = max(0, int(autogrow_cfg["backlog_target"]) - int(backlog_current_item)) if effective_autogrow else 0
            state_norm = _normalize_state(state_item)
            if safety_forced and state_norm and state_norm not in safety_net_forced_states:
                safety_net_forced_states.append(state_norm)
            detail: dict[str, object] = {
                "state": state_norm,
                "pool_total_current": int(pool_total_current),
                "backlog_current": int(backlog_current_item),
                "new_needed": int(new_needed_item),
                "effective_autogrow": bool(effective_autogrow),
                "safety_net_forced": bool(safety_forced),
                "aiha_candidate": 0,
                "aiha_accepted": 0,
                "ohs_bg_candidate": 0,
                "ohs_bg_accepted": 0,
                "apollo_candidate": 0,
                "apollo_accepted": 0,
            }
            for reject_key in AUTOGROW_REJECT_KEYS:
                detail[f"aiha_rejected_{reject_key}"] = 0
                detail[f"ohs_bg_rejected_{reject_key}"] = 0
                detail[f"apollo_rejected_{reject_key}"] = 0
            autogrow_state_details.append(detail)
            if state_norm == _normalize_state(selected_state):
                selected_backlog_current = int(backlog_current_item)
                selected_new_needed = int(new_needed_item)
    finally:
        if conn is not None:
            conn.close()

    autogrow_state = {
        "enabled": bool(autogrow_cfg["enabled"]),
        "states": list(autogrow_states),
        "sources": list(autogrow_cfg["sources"]),
        "sources_empty": len(list(autogrow_cfg["sources"])) == 0,
        "selected_state": selected_state,
        "backlog_target": int(autogrow_cfg["backlog_target"]),
        "backlog_current": int(selected_backlog_current),
        "new_needed": int(selected_new_needed),
        "max_fetch_pages": int(autogrow_cfg["max_fetch_pages"]),
        "sleep_ms": int(autogrow_cfg["sleep_ms"]),
        "safety_net_forced": bool(safety_net_forced_states),
        "safety_net_states": list(safety_net_forced_states),
        "state_details": autogrow_state_details,
        "total_states": len(autogrow_state_details),
        "total_accepted": 0,
    }

    sources_empty = bool(autogrow_cfg["enabled"]) and len(list(autogrow_cfg["sources"])) == 0
    aiha_result = _default_autogrow_source_result(
        cache_path=prospect_sources_aiha._cache_path(_autogrow_source_cache_dir(cache_root_dir, "AIHA"), selected_state),
        enabled=bool(autogrow_cfg["enabled"]),
        sources_empty=sources_empty,
    )
    aiha_rejected: Counter = Counter()
    ohs_bg_result = _default_autogrow_source_result(
        cache_path=prospect_sources_ohs_bg._cache_path(_autogrow_source_cache_dir(cache_root_dir, "OHS_BG"), selected_state),
        enabled=bool(autogrow_cfg["enabled"]),
        sources_empty=sources_empty,
    )
    ohs_bg_rejected: Counter = Counter()
    apollo_result: dict[str, object] = {
        "cache_path": prospect_sources_apollo._cache_path(_autogrow_source_cache_dir(cache_root_dir, "APOLLO"), selected_state),
        "cache_used": False,
        "cache_age_days": None,
        "parse_mode": ("SKIP_NO_SOURCES" if sources_empty else "FAILED"),
        "search_pages_fetched": 0,
        "search_rows_returned": 0,
        "search_rows_has_email_true": 0,
        "search_rows_deduped_id": 0,
        "enrich_attempted": 0,
        "enriched": 0,
        "enrich_no_match": 0,
        "enrich_skipped_credit_cap": 0,
        "credit_cap_hit": False,
        "forbidden": False,
    }
    apollo_rejected: Counter = Counter()
    apollo_enrich_remaining = int(apollo_cfg.get("enrich_max_per_run") or 0)
    diagnostics_path: Path | None = None
    autogrow_rows: list[dict[str, str]] = []

    if args.print_config:
        print(f"{PASS_GENERATOR_PRINT_CONFIG} data_dir={data_dir.resolve()}")
        print(f"{PASS_GENERATOR_PRINT_CONFIG} output_path={output_path.resolve()}")
        print(f"{PASS_GENERATOR_PRINT_CONFIG} cache_dir={cache_dir.resolve()}")
        print(f"{PASS_GENERATOR_PRINT_CONFIG} diagnostics_dir={diagnostics_dir.resolve()}")
        print(f"{PASS_GENERATOR_PRINT_CONFIG} selected_state={selected_state}")
        print(f"{PASS_GENERATOR_PRINT_CONFIG} run_date={run_date.isoformat()}")
        _print_tokens(
            path=output_path,
            rows_read=0,
            rows_written=0,
            status="PRINT_CONFIG",
            autogrow=autogrow_state,
            aiha_result=aiha_result,
            aiha_rejected=aiha_rejected,
            ohs_bg_result=ohs_bg_result,
            ohs_bg_rejected=ohs_bg_rejected,
            apollo_cfg=apollo_cfg,
            apollo_result=apollo_result,
            apollo_rejected=apollo_rejected,
            diagnostics_path=None,
        )
        return 0

    try:
        state_rows, rows_read_seed = _build_clean_state_rows()
    except Exception as exc:
        print(f"{ERR_GENERATOR_FAILED} stage=build_rows err={exc}", file=sys.stderr)
        return 2

    autogrow_seen_emails: set[str] = set()
    selected_state_norm = _normalize_state(selected_state)
    source_prefix_map = {"AIHA": "aiha", "OHS_BG": "ohs_bg", "APOLLO": "apollo"}
    for detail in autogrow_state_details:
        state_detail = _normalize_state(str(detail.get("state") or ""))
        if not state_detail or not bool(detail.get("effective_autogrow")):
            continue
        state_new_needed = max(0, int(detail.get("new_needed") or 0))
        if state_new_needed <= 0:
            continue

        source_order: list[str] = []
        for source_token in list(autogrow_cfg["sources"]):
            source_norm = _normalize_state(str(source_token or ""))
            if source_norm and source_norm not in source_order:
                source_order.append(source_norm)
        if bool(detail.get("safety_net_forced")) and "AIHA" not in source_order:
            source_order.insert(0, "AIHA")

        remaining_needed = state_new_needed
        for source_token in source_order:
            if remaining_needed <= 0:
                break
            if source_token not in AUTOGROW_ALLOWED_SOURCES:
                continue

            apollo_limit_for_state = 0
            if source_token == "APOLLO":
                apollo_limit_for_state = min(max(0, remaining_needed), max(0, apollo_enrich_remaining))

            result = _fetch_autogrow_source_rows(
                source_token=source_token,
                state=state_detail,
                run_date=run_date,
                max_fetch_pages=int(autogrow_cfg["max_fetch_pages"]),
                sleep_ms=int(autogrow_cfg["sleep_ms"]),
                cache_root_dir=cache_root_dir,
                diagnostics_dir=diagnostics_dir,
                allow_cache_write=not bool(args.dry_run),
                apollo_cfg=apollo_cfg,
                apollo_enrich_limit=apollo_limit_for_state,
            )
            rows_candidate = list(result.get("rows") or [])
            filtered_rows, rejected = _filter_autogrow_candidates(
                rows=rows_candidate,
                target_state=state_detail,
                suppressed_emails=suppressed_emails,
                existing_crm_emails=set(existing_crm_emails),
                preseen_batch_emails=set(autogrow_seen_emails),
            )
            accepted_rows = filtered_rows[:remaining_needed]
            remaining_needed = max(0, remaining_needed - len(accepted_rows))

            source_prefix = source_prefix_map.get(source_token, "")
            if source_prefix:
                detail[f"{source_prefix}_candidate"] = len(rows_candidate)
                detail[f"{source_prefix}_accepted"] = len(accepted_rows)
                for reject_key in AUTOGROW_REJECT_KEYS:
                    detail[f"{source_prefix}_rejected_{reject_key}"] = int(rejected.get(reject_key, 0))

            if source_token == "APOLLO":
                apollo_result["search_pages_fetched"] = int(apollo_result.get("search_pages_fetched") or 0) + int(
                    result.get("pages_fetched") or 0
                )
                apollo_result["search_rows_returned"] = int(apollo_result.get("search_rows_returned") or 0) + int(
                    result.get("search_rows_returned") or 0
                )
                apollo_result["search_rows_has_email_true"] = int(
                    apollo_result.get("search_rows_has_email_true") or 0
                ) + int(result.get("search_rows_has_email_true") or 0)
                apollo_result["search_rows_deduped_id"] = int(apollo_result.get("search_rows_deduped_id") or 0) + int(
                    result.get("search_rows_deduped_id") or 0
                )
                apollo_result["enrich_attempted"] = int(apollo_result.get("enrich_attempted") or 0) + int(
                    result.get("enrich_attempted") or 0
                )
                apollo_result["enriched"] = int(apollo_result.get("enriched") or 0) + int(result.get("enriched") or 0)
                apollo_result["enrich_no_match"] = int(apollo_result.get("enrich_no_match") or 0) + int(
                    result.get("enrich_no_match") or 0
                )
                apollo_result["enrich_skipped_credit_cap"] = int(
                    apollo_result.get("enrich_skipped_credit_cap") or 0
                ) + int(result.get("enrich_skipped_credit_cap") or 0)
                apollo_result["credit_cap_hit"] = bool(apollo_result.get("credit_cap_hit")) or bool(
                    result.get("credit_cap_hit")
                )
                apollo_result["forbidden"] = bool(apollo_result.get("forbidden")) or bool(result.get("forbidden"))
                apollo_rejected.update(rejected)
                apollo_enrich_remaining = max(0, apollo_enrich_remaining - int(result.get("enrich_attempted") or 0))

            autogrow_rows.extend(accepted_rows)
            for row in accepted_rows:
                email = _normalize_email(row.get("contact_email") or row.get("email") or "")
                if email:
                    autogrow_seen_emails.add(email)

            diag = result.get("diagnostics_path")
            resolved_diag: Path | None = diag if isinstance(diag, Path) else (Path(str(diag)) if diag else None)
            if resolved_diag is not None:
                if state_detail == selected_state_norm:
                    diagnostics_path = resolved_diag
                elif diagnostics_path is None:
                    diagnostics_path = resolved_diag

            if state_detail == selected_state_norm:
                if source_token == "AIHA":
                    aiha_result.update(result)
                    aiha_result["rows_candidate"] = len(rows_candidate)
                    aiha_result["rows_accepted"] = len(accepted_rows)
                    aiha_rejected = rejected
                elif source_token == "OHS_BG":
                    ohs_bg_result.update(result)
                    ohs_bg_result["rows_candidate"] = len(rows_candidate)
                    ohs_bg_result["rows_accepted"] = len(accepted_rows)
                    ohs_bg_rejected = rejected
                elif source_token == "APOLLO":
                    apollo_result.update(
                        {
                            "cache_path": result.get("cache_path") or apollo_result.get("cache_path"),
                            "cache_used": bool(result.get("cache_used")),
                            "cache_age_days": result.get("cache_age_days"),
                            "parse_mode": result.get("parse_mode") or apollo_result.get("parse_mode"),
                        }
                    )

            if result.get("error"):
                source_label = "aiha" if source_token == "AIHA" else ("ohs_bg" if source_token == "OHS_BG" else "apollo")
                print(f"{WARN_AUTOGROWTH_SOURCE_FAILED} source={source_label} state={state_detail} err={result.get('error')}")

    autogrow_state["total_accepted"] = int(
        sum(
            int(d.get("aiha_accepted") or 0) + int(d.get("ohs_bg_accepted") or 0) + int(d.get("apollo_accepted") or 0)
            for d in autogrow_state_details
        )
    )
    rows_read_total = rows_read_seed + int(
        sum(
            int(d.get("aiha_candidate") or 0) + int(d.get("ohs_bg_candidate") or 0) + int(d.get("apollo_candidate") or 0)
            for d in autogrow_state_details
        )
    )

    if args.dry_run:
        seed_rows = _state_rows_to_combined_input(state_rows)
        rows = _to_discovery_rows(seed_rows + autogrow_rows)
        _print_tokens(
            path=output_path,
            rows_read=rows_read_total,
            rows_written=len(rows),
            status="DRY_RUN",
            autogrow=autogrow_state,
            aiha_result=aiha_result,
            aiha_rejected=aiha_rejected,
            ohs_bg_result=ohs_bg_result,
            ohs_bg_rejected=ohs_bg_rejected,
            apollo_cfg=apollo_cfg,
            apollo_result=apollo_result,
            apollo_rejected=apollo_rejected,
            diagnostics_path=diagnostics_path,
        )
        return 0

    try:
        _write_legacy_pool_files(state_rows)
        generated_rows = _read_legacy_pool_files()
        rows = _to_discovery_rows(generated_rows + autogrow_rows)
        _write_output_atomic(path=output_path, rows=rows)
    except Exception as exc:
        print(f"{ERR_GENERATOR_FAILED} stage=write_output err={exc}", file=sys.stderr)
        return 2

    _print_tokens(
        path=output_path,
        rows_read=rows_read_total,
        rows_written=len(rows),
        status="OK",
        autogrow=autogrow_state,
        aiha_result=aiha_result,
        aiha_rejected=aiha_rejected,
        ohs_bg_result=ohs_bg_result,
        ohs_bg_rejected=ohs_bg_rejected,
        apollo_cfg=apollo_cfg,
        apollo_result=apollo_result,
        apollo_rejected=apollo_rejected,
        diagnostics_path=diagnostics_path,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
