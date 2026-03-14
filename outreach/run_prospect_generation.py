import argparse
import csv
import hashlib
import os
import re
import sqlite3
import sys
import tempfile
from collections import Counter
from datetime import date, datetime
from pathlib import Path
from urllib.parse import urlparse

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
from outreach import prospect_sources_bcsp
from outreach import prospect_enrich_email
from outreach import prospect_sources_ohs_bg
from outreach import prospect_sources_osha_news
from outreach import prospect_sources_state_lic
from outreach import scraper_engine
from outreach import source_policy
from outreach import us_state
import seed_recipients_pools as pools


ERR_GENERATOR_FAILED = "ERR_GENERATOR_FAILED"
PASS_GENERATOR_PRINT_CONFIG = "PASS_GENERATOR_PRINT_CONFIG"
WARN_AUTOGROWTH_SOURCE_FAILED = "WARN_AUTOGROWTH_SOURCE_FAILED"
WARN_APOLLO_FREE_TIER_API_BLOCKED = "WARN_APOLLO_FREE_TIER_API_BLOCKED"
APOLLO_FORBIDDEN_HINT = "CHECK_MASTER_KEY_OR_ENDPOINT_SCOPES"
APOLLO_DOCTOR_NOT_FOUND_HINT = "CHECK_METHOD_AND_BASE_URL"

OUTPUT_SUBDIR = ("prospect_discovery",)
OUTPUT_FILENAME = "prospects_latest.csv"

GENERATION_CACHE_ROOT_SUBDIR = ("prospect_generation", "cache")
GENERATION_DIAGNOSTICS_SUBDIR = ("prospect_generation", "diagnostics")

AUTOGROW_SOURCE_PREFIX = source_policy.autogrow_source_prefix_map(include_unimplemented=False)
AUTOGROW_SOURCE_LABEL = {k: str(v or "").lower() for k, v in AUTOGROW_SOURCE_PREFIX.items()}
AUTOGROW_ALLOWED_SOURCES = set(source_policy.implemented_autogrow_sources())
AUTOGROW_SUPPORTED_SOURCES = set(source_policy.supported_autogrow_sources(include_unimplemented=True))
CRAWL4AI_AUTOGROW_SOURCES = {"OSHA_NEWS"}
AUTOGROW_REJECT_KEYS = (
    "invalid_email",
    "free_domain",
    "suppressed",
    "already_in_crm",
    "missing_state",
    "state_mismatch",
    "duplicate_in_batch",
    "role_mismatch",
    "role_inbox",
    "fit_mismatch",
)
AIHA_NET_NEW_LOSS_KEYS = (
    "duplicate_email",
    "duplicate_domain",
    "state_out_of_scope",
    "free_domain",
    "already_known_crm",
    "default_send_ineligible",
)
OHS_PARSE_COUNTER_KEYS = (
    "fetched_pages",
    "candidate_rows_seen",
    "parsed_rows_accepted",
    "parsed_rows_rejected",
    "hard_parse_failures",
    "auth_gated_pages",
    "non_profile_links_filtered",
)
OHS_PARSE_REASON_KEYS = (
    "selector_missing",
    "empty_listing",
    "missing_firm",
    "invalid_city_state",
    "missing_contact_fields",
    "state_filtered_out",
)
EXCLUDED_STATUSES = {"do_not_contact", "unsubscribed", "bounced", "converted"}
ROLE_INBOX_LOCALS = {
    "info",
    "contact",
    "admin",
    "office",
    "support",
    "sales",
    "hello",
    "help",
    "billing",
    "accounts",
    "careers",
    "jobs",
    "hr",
}
APOLLO_ROLE_FIT_TOKENS = (
    "owner",
    "founder",
    "co-founder",
    "president",
    "principal",
    "managing partner",
    "managing director",
    "partner",
    "practice lead",
    "senior consultant",
    "principal consultant",
    "consultant",
)
APOLLO_CONSULTANT_FIT_TOKENS = (
    "consult",
    "industrial hygiene",
    "oehs",
    "ehs",
    "hse",
    "osha",
    "safety",
    "risk",
    "compliance",
)
APOLLO_DEFAULT_PERSON_TITLES = [
    "owner",
    "founder",
    "co-founder",
    "president",
    "principal",
    "managing partner",
    "partner",
    "practice lead",
    "senior consultant",
    "principal consultant",
]
TDLR_STATE_LIC_SOURCE_KEY = "STATE_LIC"
TDLR_STATE_LIC_WORK_EMAIL_SOURCE_KEY = "STATE_LIC_WORK_EMAIL"
GENERATOR_FILTER_KEYS = (
    "missing_state",
    "state_mismatch",
    "missing_email",
    "suppressed",
    "free_domain",
    "already_sent_or_ineligible",
    "other",
)
DEFAULT_STATE_SCOPE_ALL = ("TX", "CA", "FL")
VALID_SOURCE_FIT_TIERS = {"core_consultant", "recoverable_consultant", "adjacent_contractor"}
US_STATE_NAME_TO_ABBR = dict(us_state.US_STATE_NAME_TO_ABBR)
US_STATE_ABBREVIATIONS = set(us_state.US_STATE_ABBREVIATIONS)


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


def _email_local_part(email: str) -> str:
    value = _normalize_email(email)
    if "@" not in value:
        return ""
    local = value.split("@", 1)[0]
    return local.split("+", 1)[0]


def _is_role_inbox_email(email: str) -> bool:
    return _email_local_part(email) in ROLE_INBOX_LOCALS


def _normalize_state(value: str) -> str:
    return us_state.normalize_state_token(value)


def _normalize_us_state(value: str) -> str:
    return us_state.normalize_us_state(value)


def _normalize_text(value: str) -> str:
    return (value or "").strip()


def _clean_city(value: str) -> str:
    text = _normalize_text(value)
    if not text:
        return ""
    compact = re.sub(r"\s+", " ", text).strip(" ,")
    if not compact:
        return ""
    compact = re.sub(r",?\s+[A-Z]{2}\s+\d{5}(?:-\d{4})?$", "", compact).strip(" ,")
    parts = [p.strip() for p in compact.split(",") if p.strip()]
    if len(parts) > 1:
        for candidate in reversed(parts):
            if re.fullmatch(r"[A-Za-z .'-]{2,}", candidate):
                return candidate
    if re.search(r"\d", compact):
        if parts:
            for candidate in parts:
                if re.fullmatch(r"[A-Za-z .'-]{2,}", candidate):
                    return candidate
        return ""
    return compact


def _ascii_safe_text(value: str) -> str:
    return _normalize_text(value).encode("ascii", "backslashreplace").decode("ascii")


def _email_domain(email: str) -> str:
    e = _normalize_email(email)
    if "@" not in e:
        return ""
    return e.split("@", 1)[1].strip().lower()


def _domain_from_website(value: str) -> str:
    text = _normalize_text(value)
    if not text:
        return ""
    try:
        parsed = urlparse(text if "://" in text else f"https://{text}")
    except Exception:
        return ""
    host = _normalize_text(parsed.netloc or parsed.path).lower()
    if host.startswith("www."):
        host = host[4:]
    return host


def _row_domain_value(row: dict[str, str], email: str) -> str:
    explicit = _normalize_text(row.get("domain") or "").lower()
    if explicit:
        return explicit
    via_email = _email_domain(email)
    if via_email:
        return via_email
    return _domain_from_website(row.get("website") or "")


def _source_fit_defaults(source: str) -> tuple[str, int]:
    return source_policy.source_fit_defaults(source)


def _coerce_boolish_int(value: str, default: int) -> int:
    text = _normalize_text(value).lower()
    if not text:
        return 1 if int(default) else 0
    if text in {"1", "true", "yes", "on"}:
        return 1
    if text in {"0", "false", "no", "off"}:
        return 0
    try:
        return 1 if int(text) != 0 else 0
    except Exception:
        return 1 if int(default) else 0


def _coerce_source_fit_tier(value: str, source: str) -> str:
    tier = _normalize_text(value)
    if tier in VALID_SOURCE_FIT_TIERS:
        return tier
    return _source_fit_defaults(source)[0]


def _apollo_role_fit_text(row: dict[str, str]) -> str:
    fields = [
        _normalize_text(row.get("title") or row.get("contact_role") or ""),
        _normalize_text(row.get("contact_name") or ""),
    ]
    return " ".join([f for f in fields if f]).lower()


def _apollo_consultant_fit_text(row: dict[str, str]) -> str:
    fields = [
        _normalize_text(row.get("firm") or row.get("company_name") or ""),
        _normalize_text(row.get("title") or row.get("contact_role") or ""),
        _normalize_text(row.get("website") or ""),
        _normalize_text(row.get("source") or ""),
    ]
    return " ".join([f for f in fields if f]).lower()


def _contains_any_token(text: str, tokens: tuple[str, ...]) -> bool:
    haystack = str(text or "").lower()
    if not haystack:
        return False
    return any(token in haystack for token in tokens)


def _source_family(source: str) -> str:
    return source_policy.source_family(source)


def _effective_default_send_eligible(source: str, sendable_raw: object) -> int:
    source_text = _normalize_text(source)
    family = _source_family(source_text)
    _default_tier, default_send = _source_fit_defaults(source_text)
    if family in {"STATE_LIC", "APOLLO", "AIHA", "OHS_BG"}:
        return int(default_send)
    raw_text = "" if sendable_raw is None else str(sendable_raw)
    return _coerce_boolish_int(raw_text, default_send)


def _row_is_effectively_sendable(row: dict[str, object], *, skip_role_inboxes: bool) -> bool:
    email = _normalize_email(str(row.get("email") or row.get("contact_email") or ""))
    if not _valid_email(email):
        return False
    if _email_domain(email) in pools.FREE_EMAIL_DOMAINS:
        return False
    if bool(skip_role_inboxes) and _is_role_inbox_email(email):
        return False
    return _effective_default_send_eligible(str(row.get("source") or ""), row.get("default_send_eligible")) == 1


def _row_has_nonfree_work_email(row: dict[str, object]) -> bool:
    email = _normalize_email(str(row.get("email") or row.get("contact_email") or ""))
    if not _valid_email(email):
        return False
    return _email_domain(email) not in pools.FREE_EMAIL_DOMAINS


def _promote_state_lic_work_email_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    promoted_rows: list[dict[str, str]] = []
    for row in list(rows or []):
        updated = dict(row)
        source = _normalize_text(updated.get("source") or TDLR_STATE_LIC_SOURCE_KEY)
        if _source_family(source) == "STATE_LIC" and _row_has_nonfree_work_email(updated):
            updated["source"] = TDLR_STATE_LIC_WORK_EMAIL_SOURCE_KEY
            updated["source_fit_tier"] = "adjacent_contractor"
            updated["default_send_eligible"] = "1"
        promoted_rows.append(updated)
    return promoted_rows


def _generator_row_observability(rows: list[dict[str, str]]) -> dict[str, object]:
    source_counts: Counter = Counter()
    tier_counts: Counter = Counter()
    email_status_counts: Counter = Counter()
    default_send_eligible_total = 0
    for row in list(rows or []):
        source = _normalize_text(row.get("source") or "")
        family = _source_family(source)
        source_counts[family] += 1
        tier = _coerce_source_fit_tier(row.get("source_fit_tier") or "", source)
        tier_counts[tier] += 1
        default_send = _effective_default_send_eligible(source, row.get("default_send_eligible"))
        if default_send == 1:
            default_send_eligible_total += 1
        status = _normalize_text(row.get("email_status") or "").lower() or "blank"
        email_status_counts[status] += 1
    return {
        "source_counts": dict(source_counts),
        "tier_counts": dict(tier_counts),
        "email_status_counts": dict(email_status_counts),
        "default_send_eligible_total": int(default_send_eligible_total),
    }


def _default_aiha_loss_counters() -> Counter:
    return Counter({key: 0 for key in AIHA_NET_NEW_LOSS_KEYS})


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


def _source_cache_path_for_state(cache_root_dir: Path, source_token: str, state: str) -> Path:
    token = _normalize_state(source_token)
    cache_dir = _autogrow_source_cache_dir(cache_root_dir, token)
    if token == "AIHA":
        return prospect_sources_aiha._cache_path(cache_dir, state)
    if token == "OHS_BG":
        return prospect_sources_ohs_bg._cache_path(cache_dir, state)
    if token == "APOLLO":
        return prospect_sources_apollo._cache_path(cache_dir, state)
    if token == "BCSP":
        return prospect_sources_bcsp._cache_path(cache_dir, state)
    if token == "OSHA_NEWS":
        return prospect_sources_osha_news._cache_path(cache_dir, state)
    if token == "STATE_LIC":
        return prospect_sources_state_lic._cache_path(cache_dir, state)
    return cache_dir / f"state_{_normalize_state(state)}.json"


def _discovery_fields() -> list[str]:
    return [
        "prospect_id",
        "firm",
        "email",
        "title",
        "city",
        "state",
        "source",
        "source_fit_tier",
        "default_send_eligible",
        "email_status",
        "enrichment_lane",
        "contact_name",
        "website",
    ]


def _enrichment_lane_from_status(email_status: str) -> str:
    status = _normalize_text(email_status).lower()
    if not status:
        return "unknown"
    if status.startswith("hunter_"):
        return "provider_hunter"
    if status in {"pattern_generated", "scraped_from_site", "scraped_from_source"}:
        return "pattern_or_site"
    return "unknown"


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
    return us_state.parse_state_csv(raw, strict_us=True)


def _resolve_state_scope(override_raw: str, env_states_default: list[str]) -> list[str] | None:
    text = _normalize_text(override_raw)
    if not text:
        return list(env_states_default)
    if text.lower() == "all":
        return None
    parsed = _parse_states(text)
    if not parsed:
        raise ValueError("invalid_states_scope")
    return parsed


def _states_for_selection(state_scope: list[str] | None) -> list[str]:
    if state_scope is None:
        return list(DEFAULT_STATE_SCOPE_ALL)
    return list(state_scope)


def _state_scope_token(state_scope: list[str] | None) -> str:
    if state_scope is None:
        return "all"
    ordered = [_normalize_state(s) for s in list(state_scope or []) if _normalize_state(s)]
    return ",".join(ordered) if ordered else "none"


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

    invalid, unimplemented = source_policy.validate_autogrow_source_tokens(source_tokens)
    if invalid:
        raise ValueError(f"invalid_autogrow_sources={','.join(invalid)}")
    if unimplemented:
        raise ValueError(f"unimplemented_autogrow_sources={','.join(unimplemented)}")

    backlog_target = _parse_int_env(os.getenv("PROSPECT_AUTOGROW_BACKLOG_TARGET", ""), default=60, minimum=1)
    max_fetch_pages = _parse_int_env(os.getenv("PROSPECT_AUTOGROW_MAX_FETCH_PAGES_PER_RUN", ""), default=6, minimum=1)
    sleep_ms = _parse_int_env(os.getenv("PROSPECT_AUTOGROW_HTTP_SLEEP_MS", ""), default=800, minimum=0)
    llm_enabled = _bool_env(os.getenv("PROSPECT_AUTOGROW_LLM_ENABLED", "0"))

    return {
        "enabled": enabled,
        "safety_net_enabled": safety_net_enabled,
        "sources": source_tokens,
        "backlog_target": backlog_target,
        "max_fetch_pages": max_fetch_pages,
        "sleep_ms": sleep_ms,
        "llm_enabled": llm_enabled,
    }


def _parse_apollo_config(autogrow_sources: list[str]) -> dict:
    source_tokens = [_normalize_state(s) for s in list(autogrow_sources or [])]
    apollo_source_enabled = "APOLLO" in source_tokens
    api_key = _normalize_text(os.getenv("APOLLO_API_KEY", ""))
    enrich_enabled = _bool_env(os.getenv("APOLLO_ENRICH_ENABLED", "0"))
    enrich_max_per_run = _parse_int_env(os.getenv("APOLLO_ENRICH_MAX_PER_RUN", ""), default=50, minimum=1)
    allow_role_inbox = _bool_env(os.getenv("APOLLO_ALLOW_ROLE_INBOX", "0"))
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
        "allow_role_inbox": allow_role_inbox,
        "person_titles": person_titles,
        "person_locations_mode": person_locations_mode,
    }


def _generation_hunter_usage_path(data_dir: Path) -> Path:
    return data_dir / "prospect_generation" / "hunter_usage.json"


def _parse_enrich_config(data_dir: Path) -> dict:
    return {
        "domain_enabled": _bool_env(os.getenv("PROSPECT_ENRICH_DOMAIN_ENABLED", "0")),
        "hunter_enabled": _bool_env(os.getenv("PROSPECT_ENRICH_HUNTER_ENABLED", "0")),
        "hunter_api_key": _normalize_text(os.getenv("HUNTER_API_KEY", "")),
        "hunter_usage_path": _generation_hunter_usage_path(data_dir),
        "hunter_cap": prospect_enrich_email.HUNTER_FREE_MONTHLY_CAP,
    }


def _build_clean_state_rows(state_scope: list[str] | None) -> tuple[dict[str, list[dict[str, str]]], int]:
    state_rows: dict[str, list[dict[str, str]]] = {}
    rows_read = 0
    pools_by_state = {
        "TX": pools.TX_POOL,
        "CA": pools.CA_POOL,
        "FL": pools.FL_POOL,
    }
    scope = None if state_scope is None else {_normalize_state(s) for s in list(state_scope or []) if _normalize_state(s)}

    for state, seed_rows in pools_by_state.items():
        if scope is not None and state not in scope:
            state_rows[state] = []
            continue
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


def _state_rows_to_combined_input(
    state_rows: dict[str, list[dict[str, str]]], state_scope: list[str] | None
) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    ordered = [_normalize_state(s) for s in list(_states_for_selection(state_scope)) if _normalize_state(s)]
    for state in ordered:
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

        source = _normalize_text(row.get("source") or "seed_recipients_pools")
        _source_fit_default, sendable_default = _source_fit_defaults(source)
        email_status = _normalize_text(row.get("email_status") or "")
        enrichment_lane = _normalize_text(row.get("enrichment_lane") or "") or _enrichment_lane_from_status(email_status)
        state = _normalize_us_state(row.get("state") or "")
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
                "city": _clean_city(row.get("city") or ""),
                "state": state,
                "source": source,
                "source_fit_tier": _coerce_source_fit_tier(row.get("source_fit_tier") or "", source),
                "default_send_eligible": str(
                    _coerce_boolish_int(row.get("default_send_eligible") or "", sendable_default)
                ),
                "email_status": email_status,
                "enrichment_lane": enrichment_lane,
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


def _existing_crm_domains(conn: sqlite3.Connection | None) -> set[str]:
    if conn is None or not _table_exists(conn, "prospects"):
        return set()
    out: set[str] = set()
    try:
        rows = conn.execute("SELECT email, website FROM prospects").fetchall()
        for row in rows:
            email = _normalize_email(str(row[0] or ""))
            if _valid_email(email):
                domain = _email_domain(email)
                if domain:
                    out.add(domain)
            website = _normalize_text(str(row[1] or ""))
            if website:
                website_domain = _domain_from_website(website)
                if website_domain:
                    out.add(website_domain)
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


def compute_uncontacted_backlog(
    conn: sqlite3.Connection | None,
    state: str,
    suppressed_emails: set[str],
    skip_role_inboxes: bool | None = None,
) -> int:
    if conn is None or not _table_exists(conn, "prospects"):
        return 0

    columns = _table_columns(conn, "prospects")
    if "prospect_id" not in columns or "email" not in columns:
        return 0

    target_state = _normalize_state(state)
    if not target_state:
        return 0
    if skip_role_inboxes is None:
        skip_role_inboxes = _bool_env(os.getenv("OUTREACH_SKIP_ROLE_INBOXES", "1"))

    sent_ids = _fetch_prior_sent_ids(conn)
    status_col = "status" if "status" in columns else "''"
    last_contacted_col = "last_contacted_at" if "last_contacted_at" in columns else "''"
    default_send_col = "default_send_eligible" if "default_send_eligible" in columns else "1"

    rows = conn.execute(
        f"""
        SELECT prospect_id, email, state, source, {status_col} AS status, {last_contacted_col} AS last_contacted_at,
               {default_send_col} AS default_send_eligible
        FROM prospects
        """
    ).fetchall()

    count = 0
    for row in rows:
        if _normalize_state(str(row["state"] or "")) != target_state:
            continue
        email = _normalize_email(str(row["email"] or ""))
        if email in suppressed_emails:
            continue
        if not _row_is_effectively_sendable(dict(row), skip_role_inboxes=bool(skip_role_inboxes)):
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
    target_state = _normalize_state(state)
    if not target_state:
        return 0
    try:
        rows = conn.execute("SELECT state FROM prospects").fetchall()
    except Exception:
        return 0
    return sum(1 for row in rows if _normalize_state(str(row[0] or "")) == target_state)


def _default_input_cohort() -> dict[str, object]:
    return {
        "crm_total": 0,
        "eligible": 0,
        "excluded": 0,
        "filtered": {key: 0 for key in GENERATOR_FILTER_KEYS},
    }


def _compute_input_cohort(
    conn: sqlite3.Connection | None, states_scope: list[str] | None, suppressed_emails: set[str]
) -> dict[str, object]:
    if conn is None or not _table_exists(conn, "prospects"):
        return _default_input_cohort()

    columns = _table_columns(conn, "prospects")
    if "prospect_id" not in columns or "email" not in columns or "state" not in columns:
        return _default_input_cohort()

    status_col = "status" if "status" in columns else "''"
    last_contacted_col = "last_contacted_at" if "last_contacted_at" in columns else "''"
    default_send_col = "default_send_eligible" if "default_send_eligible" in columns else "1"
    sent_ids = _fetch_prior_sent_ids(conn)
    scope = None if states_scope is None else {_normalize_state(s) for s in list(states_scope or []) if _normalize_state(s)}
    filtered: Counter = Counter()
    eligible = 0
    crm_total = 0

    rows = conn.execute(
        f"""
        SELECT prospect_id, email, state, source, {status_col} AS status, {last_contacted_col} AS last_contacted_at,
               {default_send_col} AS default_send_eligible
        FROM prospects
        """
    ).fetchall()
    for row in rows:
        crm_total += 1
        state = _normalize_state(str(row["state"] or ""))
        if not state:
            filtered["missing_state"] += 1
            continue
        if scope is not None and state not in scope:
            filtered["state_mismatch"] += 1
            continue

        email = _normalize_email(str(row["email"] or ""))
        if not email:
            filtered["missing_email"] += 1
            continue
        if not _valid_email(email):
            filtered["other"] += 1
            continue
        if _email_domain(email) in pools.FREE_EMAIL_DOMAINS:
            filtered["free_domain"] += 1
            continue
        if email in suppressed_emails:
            filtered["suppressed"] += 1
            continue
        if _effective_default_send_eligible(str(row["source"] or ""), row["default_send_eligible"]) != 1:
            filtered["already_sent_or_ineligible"] += 1
            continue

        status = _normalize_text(str(row["status"] or "")).lower()
        prospect_id = _normalize_text(str(row["prospect_id"] or ""))
        if status in EXCLUDED_STATUSES:
            filtered["already_sent_or_ineligible"] += 1
            continue
        if prospect_id and prospect_id in sent_ids:
            filtered["already_sent_or_ineligible"] += 1
            continue
        if _normalize_text(str(row["last_contacted_at"] or "")):
            filtered["already_sent_or_ineligible"] += 1
            continue
        eligible += 1

    excluded = int(crm_total - eligible)
    filtered_total = int(sum(int(filtered.get(key, 0)) for key in GENERATOR_FILTER_KEYS))
    if filtered_total != excluded:
        filtered["other"] += int(excluded - filtered_total)
    filtered_normalized = {key: int(max(0, int(filtered.get(key, 0)))) for key in GENERATOR_FILTER_KEYS}
    excluded = int(sum(int(filtered_normalized.get(key, 0)) for key in GENERATOR_FILTER_KEYS))
    if int(eligible + excluded) != int(crm_total):
        delta = int(crm_total - (eligible + excluded))
        filtered_normalized["other"] = max(0, int(filtered_normalized.get("other", 0)) + delta)
        excluded = int(sum(int(filtered_normalized.get(key, 0)) for key in GENERATOR_FILTER_KEYS))

    return {
        "crm_total": int(crm_total),
        "eligible": int(eligible),
        "excluded": int(excluded),
        "filtered": filtered_normalized,
    }


def _filter_autogrow_candidates(
    rows: list[dict[str, str]],
    target_state: str,
    suppressed_emails: set[str],
    existing_crm_emails: set[str],
    preseen_batch_emails: set[str] | None = None,
    source_token: str = "",
    apollo_allow_role_inbox: bool = False,
) -> tuple[list[dict[str, str]], Counter]:
    target = _normalize_us_state(target_state)
    source_norm = _normalize_state(source_token)
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

        state = _normalize_us_state(row.get("state") or "")
        if not state:
            counters["missing_state"] += 1
            continue
        if state != target:
            counters["state_mismatch"] += 1
            continue

        if source_norm == "APOLLO":
            role_fit_text = _apollo_role_fit_text(row)
            if not _contains_any_token(role_fit_text, APOLLO_ROLE_FIT_TOKENS):
                counters["role_mismatch"] += 1
                continue
            if (not apollo_allow_role_inbox) and _is_role_inbox_email(email):
                counters["role_inbox"] += 1
                continue
            consultant_fit_text = _apollo_consultant_fit_text(row)
            if not _contains_any_token(consultant_fit_text, APOLLO_CONSULTANT_FIT_TOKENS):
                counters["fit_mismatch"] += 1
                continue

        if email in seen_batch:
            counters["duplicate_in_batch"] += 1
            continue

        source = _normalize_text(row.get("source") or "")
        if not source:
            if source_norm == "AIHA":
                source = "aiha_consultants_listing"
            elif source_norm == "OHS_BG":
                source = "ohs_buyers_guide"
            elif source_norm == "STATE_LIC":
                source = "STATE_LIC"
            elif source_norm == "APOLLO":
                source = "apollo_export_csv"
            else:
                source = "aiha_consultants_listing"
        _source_fit_default, sendable_default = _source_fit_defaults(source)

        seen_batch.add(email)
        accepted.append(
            {
                "prospect_id": _normalize_text(row.get("prospect_id") or ""),
                "company_name": _normalize_text(row.get("firm") or row.get("company_name") or ""),
                "contact_email": email,
                "contact_role": _normalize_text(row.get("title") or row.get("contact_role") or "EHS Consultant"),
                "contact_name": _normalize_text(row.get("contact_name") or ""),
                "city": _clean_city(row.get("city") or ""),
                "state": state,
                "domain": _normalize_text(row.get("domain") or "").lower() or _email_domain(email),
                "website": _normalize_text(row.get("website") or ""),
                "source": source,
                "source_fit_tier": _coerce_source_fit_tier(row.get("source_fit_tier") or "", source),
                "default_send_eligible": str(
                    _coerce_boolish_int(row.get("default_send_eligible") or "", sendable_default)
                ),
            }
        )

    accepted.sort(key=lambda r: (_normalize_email(r.get("contact_email") or ""), _normalize_text(r.get("company_name") or "")))
    return accepted, counters


def _aiha_loss_counters_from_candidates(
    rows: list[dict[str, str]],
    target_state: str,
    existing_crm_emails: set[str],
    existing_crm_domains: set[str],
    preseen_batch_emails: set[str] | None = None,
    preseen_batch_domains: set[str] | None = None,
) -> Counter:
    target = _normalize_us_state(target_state)
    counters = _default_aiha_loss_counters()
    seen_emails: set[str] = set(preseen_batch_emails or set())
    seen_domains: set[str] = set(preseen_batch_domains or set())

    for row in list(rows or []):
        email = _normalize_email(row.get("email") or row.get("contact_email") or "")
        if _valid_email(email):
            if email in seen_emails:
                counters["duplicate_email"] += 1
            seen_emails.add(email)
            if email in existing_crm_emails:
                counters["already_known_crm"] += 1
            if _email_domain(email) in pools.FREE_EMAIL_DOMAINS:
                counters["free_domain"] += 1

        state = _normalize_us_state(row.get("state") or "")
        if not state or (target and state != target):
            counters["state_out_of_scope"] += 1

        source = _normalize_text(row.get("source") or "")
        _source_fit_default, sendable_default = _source_fit_defaults(source)
        sendable = _coerce_boolish_int(row.get("default_send_eligible") or "", sendable_default)
        if sendable != 1:
            counters["default_send_ineligible"] += 1

        domain = _row_domain_value(row, email)
        if domain:
            if domain in seen_domains or domain in existing_crm_domains:
                counters["duplicate_domain"] += 1
            seen_domains.add(domain)

    return counters


def _default_apollo_result(cache_root_dir: Path, selected_state: str, *, sources_empty: bool) -> dict[str, object]:
    return {
        "cache_path": _source_cache_path_for_state(cache_root_dir, "APOLLO", selected_state),
        "cache_used": False,
        "cache_age_days": None,
        "parse_mode": ("SKIP_NO_SOURCES" if sources_empty else "FAILED"),
        "search_pages_fetched": 0,
        "search_rows_returned": 0,
        "search_rows_has_email_true": 0,
        "search_rows_role_fit_true": 0,
        "search_rows_deduped_id": 0,
        "imported_sendable": 0,
        "enrich_attempted": 0,
        "enriched": 0,
        "enrich_no_match": 0,
        "enrich_skipped_credit_cap": 0,
        "credit_cap_hit": False,
        "forbidden": False,
    }


def _default_generator_enrich_metrics() -> dict[str, int]:
    return {
        "attempted": 0,
        "domain_resolved": 0,
        "email_guessed": 0,
        "hunter_attempted": 0,
        "hunter_verified": 0,
        "hunter_no_match": 0,
        "hunter_error": 0,
        "still_no_email": 0,
        "hunter_skipped_cap": 0,
    }


def _merge_generator_enrich_metrics(dest: dict[str, int], src: dict | None) -> None:
    source = dict(src or {})
    for key in (
        "attempted",
        "domain_resolved",
        "email_guessed",
        "hunter_attempted",
        "hunter_verified",
        "hunter_no_match",
        "hunter_error",
        "still_no_email",
        "hunter_skipped_cap",
    ):
        dest[key] = int(dest.get(key, 0)) + int(source.get(key, 0) or 0)


def _probe_autogrow_runtime() -> tuple[dict[str, object], dict[str, dict[str, object]]]:
    crawl_probe = scraper_engine.probe_crawl4ai_runtime()
    availability: dict[str, dict[str, object]] = {}
    for key in ("BCSP", "OSHA_NEWS", "STATE_LIC"):
        availability[key] = dict(scraper_engine.probe_source_availability(key))
    return dict(crawl_probe), availability


def _run_apollo_doctor_only(diagnostics_dir: Path) -> int:
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


def _run_generator_doctor(diagnostics_dir: Path, autogrow_sources: list[str]) -> int:
    sources_checked = 0
    pass_count = 0
    warn_count = 0
    err_count = 0

    crawl_probe = scraper_engine.probe_crawl4ai_runtime()
    sources_checked += 1
    if bool(crawl_probe.get("crawl4ai_installed")) and bool(crawl_probe.get("playwright_browsers_installed")):
        print("PASS_DOCTOR_CRAWL4AI crawl4ai_installed=YES playwright_browsers_installed=YES")
        pass_count += 1
    else:
        print(
            "WARN_DOCTOR_CRAWL4AI "
            f"crawl4ai_installed={'YES' if crawl_probe.get('crawl4ai_installed') else 'NO'} "
            f"playwright_browsers_installed={'YES' if crawl_probe.get('playwright_browsers_installed') else 'NO'} "
            f"reason={_ascii_safe_text(str(crawl_probe.get('error_reason') or 'unknown'))}"
        )
        warn_count += 1

    enabled = []
    for token in list(autogrow_sources or []):
        key = _normalize_state(token)
        if key and key not in enabled:
            enabled.append(key)

    for token in enabled:
        if token == "BCSP":
            sources_checked += 1
            avail = scraper_engine.probe_source_availability("BCSP")
            if not avail.get("available"):
                print(
                    "WARN_DOCTOR_BCSP "
                    f"available=NO reason={_ascii_safe_text(str(avail.get('reason') or 'unknown'))}"
                )
                warn_count += 1
                continue
            probe = prospect_sources_bcsp.doctor_probe_bcsp()
            if probe.get("ok"):
                print(f"PASS_DOCTOR_BCSP status={int(probe.get('status') or 0)} url={probe.get('url') or ''}")
                pass_count += 1
            else:
                print(
                    f"WARN_DOCTOR_BCSP status={int(probe.get('status') or 0)} "
                    f"url={probe.get('url') or ''} "
                    f"err={_ascii_safe_text(str(probe.get('error') or 'unreachable'))}"
                )
                warn_count += 1
        elif token == "OSHA_NEWS":
            sources_checked += 1
            avail = scraper_engine.probe_source_availability("OSHA_NEWS")
            if not avail.get("available"):
                print(
                    "WARN_DOCTOR_OSHA_NEWS "
                    f"available=NO reason={_ascii_safe_text(str(avail.get('reason') or 'unknown'))}"
                )
                warn_count += 1
                continue
            probe = prospect_sources_osha_news.doctor_probe_osha_news()
            if probe.get("ok"):
                print(f"PASS_DOCTOR_OSHA_NEWS status={int(probe.get('status') or 0)} url={probe.get('url') or ''}")
                pass_count += 1
            else:
                print(
                    f"WARN_DOCTOR_OSHA_NEWS status={int(probe.get('status') or 0)} "
                    f"url={probe.get('url') or ''} "
                    f"err={_ascii_safe_text(str(probe.get('error') or 'unreachable'))}"
                )
                warn_count += 1
        elif token == "STATE_LIC":
            sources_checked += 1
            probe = prospect_sources_state_lic.doctor_probe_state_lic()
            if probe.get("ok"):
                print(f"PASS_DOCTOR_STATE_LIC status={int(probe.get('status') or 0)} url={probe.get('url') or ''}")
                pass_count += 1
            else:
                print(
                    f"WARN_DOCTOR_STATE_LIC status={int(probe.get('status') or 0)} "
                    f"url={probe.get('url') or ''} "
                    f"err={_ascii_safe_text(str(probe.get('error') or 'unreachable'))}"
                )
                warn_count += 1
        elif token == "APOLLO":
            sources_checked += 1
            try:
                apollo_cfg = _parse_apollo_config(["APOLLO"])
                doctor = prospect_sources_apollo.doctor_apollo_api(
                    api_key=str(apollo_cfg.get("api_key") or ""),
                    sleep_ms=0,
                    diagnostics_dir=diagnostics_dir,
                )
            except Exception as exc:
                print(f"WARN_DOCTOR_APOLLO err={_ascii_safe_text(str(exc))}")
                warn_count += 1
                continue
            if doctor.get("forbidden"):
                print(f"{WARN_APOLLO_FREE_TIER_API_BLOCKED} stage=doctor status=403")
                warn_count += 1
            elif doctor.get("ok"):
                print("PASS_DOCTOR_APOLLO ok=1")
                pass_count += 1
            else:
                print(
                    f"WARN_DOCTOR_APOLLO status={int(doctor.get('status') or 0)} "
                    f"err={_ascii_safe_text(str(doctor.get('error') or 'http_error'))}"
                )
                warn_count += 1

    print(
        "GENERATOR_DOCTOR_COMPLETE "
        f"sources_checked={sources_checked} pass={pass_count} warn={warn_count} err={err_count}"
    )
    return 0 if err_count == 0 else 2


def _print_source_result_tokens(source_key: str, result: dict, rejected: Counter | None = None) -> None:
    token = _normalize_state(source_key)
    if not token:
        return
    prefix = f"GENERATOR_{token}"
    cache_path = result.get("cache_path")
    cache_path_obj = Path(str(cache_path)) if cache_path else None
    if cache_path_obj is not None:
        print(f"{prefix}_CACHE_PATH={cache_path_obj.resolve()}")
    print(f"{prefix}_CACHE_USED={'YES' if result.get('cache_used') else 'NO'}")
    cache_age = result.get("cache_age_days")
    print(f"{prefix}_CACHE_AGE_DAYS={cache_age if cache_age is not None else -1}")
    print(f"{prefix}_PAGES_FETCHED={int(result.get('pages_fetched') or 0)}")
    print(f"{prefix}_PAGE_PARSE_MODE={result.get('parse_mode') or 'FAILED'}")
    print(f"{prefix}_ROWS_CANDIDATE={int(result.get('rows_candidate') or 0)}")
    print(f"{prefix}_ROWS_ACCEPTED={int(result.get('rows_accepted') or 0)}")
    if rejected is None:
        return
    for reject_key in AUTOGROW_REJECT_KEYS:
        print(f"{prefix}_REJECTED_{reject_key.upper()}={int(rejected.get(reject_key, 0))}")


def _print_tokens(
    path: Path,
    rows_read: int,
    rows_written: int,
    status: str,
    autogrow: dict,
    enrich_cfg: dict,
    enrich_metrics: dict,
    aiha_result: dict,
    aiha_rejected: Counter,
    aiha_loss_counters: Counter,
    ohs_bg_result: dict,
    ohs_bg_rejected: Counter,
    apollo_cfg: dict,
    apollo_result: dict,
    apollo_rejected: Counter,
    diagnostics_path: Path | None,
    crawl_probe: dict | None = None,
    source_availability: dict | None = None,
    extra_source_results: dict | None = None,
    extra_source_rejected: dict | None = None,
    print_availability: bool = False,
    row_observability: dict | None = None,
) -> None:
    print(f"GENERATOR_OUTPUT_PATH={path.resolve()}")
    print(f"GENERATOR_ROWS_READ={rows_read}")
    print(f"GENERATOR_ROWS_WRITTEN={rows_written}")

    scope_all = bool(autogrow.get("state_scope_all"))
    raw_scope = None if scope_all else list(autogrow.get("state_scope") or [])
    print(f"GENERATOR_STATE_SCOPE={_state_scope_token(raw_scope)}")
    print(f"GENERATOR_AUTOGROW_ENABLED={1 if autogrow['enabled'] else 0}")
    print(f"GENERATOR_AUTOGROW_SOURCES={','.join(autogrow['sources'])}")
    print("GENERATOR_SOURCE_POLICY=CONSULTANT_FIRST")
    print(
        "GENERATOR_SOURCE_POLICY_PRIMARY="
        f"{','.join(list(source_policy.CONSULTANT_PRIMARY_SOURCES))}"
    )
    print(
        "GENERATOR_SOURCE_POLICY_OVERFLOW="
        f"{','.join(list(source_policy.CONSULTANT_OVERFLOW_SOURCES))}"
    )
    print(
        "GENERATOR_SOURCE_POLICY_SECONDARY="
        f"{','.join(list(source_policy.CONSULTANT_SECONDARY_SOURCES))}"
    )
    print("GENERATOR_APOLLO_OVERFLOW_ONLY=1")
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
    safety_net_reason = _normalize_text(str(autogrow.get("safety_net_reason") or ""))
    safety_net_detail_states = [str(s or "").strip().upper() for s in list(autogrow.get("safety_net_forced_details") or []) if str(s or "").strip()]
    if safety_net_forced:
        print(
            "GENERATOR_AUTOGROW_SAFETY_NET_FORCED=1 "
            f"reason={safety_net_reason or 'SENDABLE_BELOW_FLOOR'} "
            f"states={','.join(safety_net_detail_states) if safety_net_detail_states else 'none'}"
        )
    else:
        print("GENERATOR_AUTOGROW_SAFETY_NET_FORCED=0")
    safety_net_states = [str(s or "").strip().upper() for s in list(autogrow.get("safety_net_states") or []) if str(s or "").strip()]
    print(f"GENERATOR_AUTOGROW_SAFETY_NET_STATES={','.join(safety_net_states) if safety_net_states else 'none'}")
    state_details = list(autogrow.get("state_details") or [])
    print(f"GENERATOR_AUTOGROW_TOTAL_STATES={int(autogrow.get('total_states') or len(state_details))}")
    print(f"GENERATOR_AUTOGROW_TOTAL_ACCEPTED={int(autogrow.get('total_accepted') or 0)}")
    input_cohort = dict(autogrow.get("input_cohort") or {})
    input_filtered = dict(input_cohort.get("filtered") or {})
    print(f"GENERATOR_FILTERED_MISSING_STATE={int(input_filtered.get('missing_state') or 0)}")
    print(f"GENERATOR_FILTERED_STATE_MISMATCH={int(input_filtered.get('state_mismatch') or 0)}")
    print(f"GENERATOR_FILTERED_MISSING_EMAIL={int(input_filtered.get('missing_email') or 0)}")
    print(f"GENERATOR_FILTERED_SUPPRESSED={int(input_filtered.get('suppressed') or 0)}")
    print(f"GENERATOR_FILTERED_FREE_DOMAIN={int(input_filtered.get('free_domain') or 0)}")
    print(
        "GENERATOR_FILTERED_ALREADY_SENT_OR_INELIGIBLE="
        f"{int(input_filtered.get('already_sent_or_ineligible') or 0)}"
    )
    print(f"GENERATOR_FILTERED_OTHER={int(input_filtered.get('other') or 0)}")
    excluded_breakdown = ",".join(
        [f"{key}:{int(input_filtered.get(key) or 0)}" for key in GENERATOR_FILTER_KEYS]
    )
    print(
        "GENERATOR_INPUT_COHORT "
        f"crm_total={int(input_cohort.get('crm_total') or 0)} "
        f"eligible={int(input_cohort.get('eligible') or 0)} "
        f"excluded={int(input_cohort.get('excluded') or 0)} "
        f"excluded_breakdown={excluded_breakdown or 'none'}"
    )
    observability = dict(row_observability or {})
    source_counts = dict(observability.get("source_counts") or {})
    tier_counts = dict(observability.get("tier_counts") or {})
    email_status_counts = dict(observability.get("email_status_counts") or {})
    for family in ("SEED", "AIHA", "OHS_BG", "APOLLO", "BCSP", "OSHA_NEWS", "STATE_LIC", "UNKNOWN"):
        print(f"GENERATOR_SOURCE_COUNT_{family}={int(source_counts.get(family, 0))}")
    for tier in ("core_consultant", "recoverable_consultant", "adjacent_contractor"):
        print(f"GENERATOR_TIER_COUNT_{tier.upper()}={int(tier_counts.get(tier, 0))}")
    print(f"GENERATOR_EMAIL_STATUS_PATTERN_GENERATED={int(email_status_counts.get('pattern_generated', 0))}")
    print(f"GENERATOR_EMAIL_STATUS_HUNTER_VERIFIED={int(email_status_counts.get('hunter_verified', 0))}")
    print(f"GENERATOR_EMAIL_STATUS_SCRAPED_FROM_SITE={int(email_status_counts.get('scraped_from_site', 0))}")
    print(f"GENERATOR_EMAIL_STATUS_SCRAPED_FROM_SOURCE={int(email_status_counts.get('scraped_from_source', 0))}")
    print(f"GENERATOR_EMAIL_STATUS_BLANK={int(email_status_counts.get('blank', 0))}")
    print(f"GENERATOR_DEFAULT_SEND_ELIGIBLE_TOTAL={int(observability.get('default_send_eligible_total') or 0)}")
    provider_lane_enabled = bool(enrich_cfg.get("hunter_enabled")) and bool(_normalize_text(enrich_cfg.get("hunter_api_key") or ""))
    print(
        "GENERATOR_ENRICH_MODE="
        f"{'dual_mode_provider_fallback' if provider_lane_enabled else 'pattern_only'}"
    )
    print(f"GENERATOR_ENRICH_ATTEMPTED={int(enrich_metrics.get('attempted') or 0)}")
    print(f"GENERATOR_ENRICH_DOMAIN_RESOLVED={int(enrich_metrics.get('domain_resolved') or 0)}")
    print(f"GENERATOR_ENRICH_EMAIL_GUESSED={int(enrich_metrics.get('email_guessed') or 0)}")
    print(f"GENERATOR_ENRICH_HUNTER_ATTEMPTED={int(enrich_metrics.get('hunter_attempted') or 0)}")
    print(f"GENERATOR_ENRICH_HUNTER_VERIFIED={int(enrich_metrics.get('hunter_verified') or 0)}")
    print(f"GENERATOR_ENRICH_HUNTER_NO_MATCH={int(enrich_metrics.get('hunter_no_match') or 0)}")
    print(f"GENERATOR_ENRICH_HUNTER_ERROR={int(enrich_metrics.get('hunter_error') or 0)}")
    print(f"GENERATOR_ENRICH_STILL_NO_EMAIL={int(enrich_metrics.get('still_no_email') or 0)}")
    print(f"GENERATOR_ENRICH_HUNTER_SKIPPED_CAP={int(enrich_metrics.get('hunter_skipped_cap') or 0)}")
    if print_availability:
        cp = dict(crawl_probe or {})
        print(f"crawl4ai_installed={'YES' if cp.get('crawl4ai_installed') else 'NO'}")
        print(f"playwright_browsers_installed={'YES' if cp.get('playwright_browsers_installed') else 'NO'}")
        av_map = dict(source_availability or {})
        for source_key in ("BCSP", "OSHA_NEWS", "STATE_LIC"):
            av = dict(av_map.get(source_key) or {})
            available = bool(av.get("available"))
            reason = _normalize_text(av.get("reason") or ("ok" if available else "unknown"))
            print(f"{source_key}_available={'YES' if available else 'NO'} reason={reason}")
        print(f"enrich_domain_enabled={'YES' if enrich_cfg.get('domain_enabled') else 'NO'}")
        print(f"enrich_hunter_enabled={'YES' if enrich_cfg.get('hunter_enabled') else 'NO'}")
        print("apollo_api_accessible=NO free_plan_web_ui_manual")
    for detail in state_details:
        state = _normalize_state(str(detail.get("state") or ""))
        if not state:
            continue
        print(
            "GENERATOR_AUTOGROW_STATE="
            f"{state} "
            f"backlog_current={int(detail.get('backlog_current') or 0)} "
            f"backlog_sendable_current={int(detail.get('backlog_sendable_current') or detail.get('backlog_current') or 0)} "
            f"new_needed={int(detail.get('new_needed') or 0)} "
            f"aiha_candidate={int(detail.get('aiha_candidate') or 0)} "
            f"aiha_accepted={int(detail.get('aiha_accepted') or 0)} "
            f"ohs_bg_candidate={int(detail.get('ohs_bg_candidate') or 0)} "
            f"ohs_bg_accepted={int(detail.get('ohs_bg_accepted') or 0)} "
            f"apollo_candidate={int(detail.get('apollo_candidate') or 0)} "
            f"apollo_accepted={int(detail.get('apollo_accepted') or 0)} "
            f"ohs_bg_base_max_pages={int(detail.get('ohs_bg_base_max_pages') or 0)} "
            f"ohs_bg_effective_max_pages={int(detail.get('ohs_bg_effective_max_pages') or 0)} "
            f"ohs_bg_deeper_enabled={int(detail.get('ohs_bg_deeper_enabled') or 0)}"
        )
        for source_label, prefix in AUTOGROW_SOURCE_PREFIX.items():
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
                f"rejected_duplicate_in_batch={int(detail.get(f'{prefix}_rejected_duplicate_in_batch') or 0)} "
                f"max_fetch_pages={int(detail.get(f'{prefix}_max_fetch_pages') or 0)} "
                f"pages_fetched={int(detail.get(f'{prefix}_pages_fetched') or 0)} "
                f"backlog_credit={int(detail.get(f'{prefix}_backlog_credit') or 0)}"
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
    for loss_key in AIHA_NET_NEW_LOSS_KEYS:
        print(
            f"GENERATOR_AIHA_LOSS_{loss_key.upper()}="
            f"{int(aiha_loss_counters.get(loss_key, 0))}"
        )

    print(f"GENERATOR_OHS_BG_CACHE_PATH={Path(ohs_bg_result['cache_path']).resolve()}")
    print(f"GENERATOR_OHS_BG_CACHE_USED={'YES' if ohs_bg_result.get('cache_used') else 'NO'}")
    ohs_cache_age = ohs_bg_result.get("cache_age_days")
    print(f"GENERATOR_OHS_BG_CACHE_AGE_DAYS={ohs_cache_age if ohs_cache_age is not None else -1}")
    print(f"GENERATOR_OHS_BG_PAGES_FETCHED={int(ohs_bg_result.get('pages_fetched') or 0)}")
    print(f"GENERATOR_OHS_BG_PAGE_PARSE_MODE={ohs_bg_result.get('parse_mode') or 'FAILED'}")
    print(f"GENERATOR_OHS_BG_AUTH_MODE={ohs_bg_result.get('auth_mode') or 'PUBLIC'}")
    print(f"GENERATOR_OHS_BG_ROWS_CANDIDATE={int(ohs_bg_result.get('rows_candidate') or 0)}")
    print(f"GENERATOR_OHS_BG_ROWS_ACCEPTED={int(ohs_bg_result.get('rows_accepted') or 0)}")
    print(f"GENERATOR_OHS_BG_REJECTED_INVALID_EMAIL={int(ohs_bg_rejected.get('invalid_email', 0))}")
    print(f"GENERATOR_OHS_BG_REJECTED_FREE_DOMAIN={int(ohs_bg_rejected.get('free_domain', 0))}")
    print(f"GENERATOR_OHS_BG_REJECTED_SUPPRESSED={int(ohs_bg_rejected.get('suppressed', 0))}")
    print(f"GENERATOR_OHS_BG_REJECTED_ALREADY_IN_CRM={int(ohs_bg_rejected.get('already_in_crm', 0))}")
    print(f"GENERATOR_OHS_BG_REJECTED_MISSING_STATE={int(ohs_bg_rejected.get('missing_state', 0))}")
    print(f"GENERATOR_OHS_BG_REJECTED_STATE_MISMATCH={int(ohs_bg_rejected.get('state_mismatch', 0))}")
    print(f"GENERATOR_OHS_BG_REJECTED_DUPLICATE_IN_BATCH={int(ohs_bg_rejected.get('duplicate_in_batch', 0))}")
    ohs_parse_counters = dict(ohs_bg_result.get("parse_counters") or {})
    ohs_parse_reasons = dict(ohs_bg_result.get("parse_reasons") or {})
    for key in OHS_PARSE_COUNTER_KEYS:
        print(f"GENERATOR_OHS_BG_PARSE_{key.upper()}={int(ohs_parse_counters.get(key, 0))}")
    for key in OHS_PARSE_REASON_KEYS:
        print(f"GENERATOR_OHS_BG_PARSE_REASON_{key.upper()}={int(ohs_parse_reasons.get(key, 0))}")

    print(f"GENERATOR_APOLLO_ENABLED={1 if apollo_cfg.get('source_enabled') else 0}")
    print(f"GENERATOR_APOLLO_ENRICH_ENABLED={1 if apollo_cfg.get('enrich_enabled') else 0}")
    print(f"GENERATOR_APOLLO_ENRICH_MAX_PER_RUN={int(apollo_cfg.get('enrich_max_per_run') or 0)}")
    print(f"GENERATOR_APOLLO_ALLOW_ROLE_INBOX={1 if apollo_cfg.get('allow_role_inbox') else 0}")
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
    print(f"GENERATOR_APOLLO_SEARCH_ROWS_ROLE_FIT_TRUE={int(apollo_result.get('search_rows_role_fit_true') or 0)}")
    print(f"GENERATOR_APOLLO_SEARCH_ROWS_DEDUPED_ID={int(apollo_result.get('search_rows_deduped_id') or 0)}")
    print(f"GENERATOR_APOLLO_IMPORTED_SENDABLE={int(apollo_result.get('imported_sendable') or 0)}")
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
    print(f"GENERATOR_APOLLO_REJECTED_ROLE_MISMATCH={int(apollo_rejected.get('role_mismatch', 0))}")
    print(f"GENERATOR_APOLLO_REJECTED_ROLE_INBOX={int(apollo_rejected.get('role_inbox', 0))}")
    print(f"GENERATOR_APOLLO_REJECTED_FIT_MISMATCH={int(apollo_rejected.get('fit_mismatch', 0))}")
    print(
        "GENERATOR_APOLLO_USABLE_YIELD "
        f"searched={int(apollo_result.get('search_rows_returned') or 0)} "
        f"has_email={int(apollo_result.get('search_rows_has_email_true') or 0)} "
        f"role_fit={int(apollo_result.get('search_rows_role_fit_true') or 0)} "
        f"imported_sendable={int(apollo_result.get('imported_sendable') or 0)}"
    )
    for source_key in ("BCSP", "OSHA_NEWS", "STATE_LIC"):
        src_results = dict(extra_source_results or {})
        src_rejected = dict(extra_source_rejected or {})
        _print_source_result_tokens(
            source_key,
            dict(src_results.get(source_key) or {}),
            src_rejected.get(source_key) if isinstance(src_rejected.get(source_key), Counter) else Counter(src_rejected.get(source_key) or {}),
        )

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
        "auth_mode": "PUBLIC",
        "rows_candidate": 0,
        "rows_accepted": 0,
        "parse_counters": {k: 0 for k in OHS_PARSE_COUNTER_KEYS},
        "parse_reasons": {k: 0 for k in OHS_PARSE_REASON_KEYS},
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
    if token == "BCSP":
        return prospect_sources_bcsp.fetch_bcsp_state_rows(
            state=state,
            run_date=run_date,
            max_pages=max_fetch_pages,
            sleep_ms=sleep_ms,
            cache_dir=cache_dir,
            diagnostics_dir=diagnostics_dir,
            allow_cache_write=allow_cache_write,
        )
    if token == "OSHA_NEWS":
        return prospect_sources_osha_news.fetch_osha_news_state_rows(
            state=state,
            run_date=run_date,
            max_pages=max_fetch_pages,
            sleep_ms=sleep_ms,
            cache_dir=cache_dir,
            diagnostics_dir=diagnostics_dir,
            allow_cache_write=allow_cache_write,
        )
    if token == TDLR_STATE_LIC_SOURCE_KEY:
        return prospect_sources_state_lic.fetch_state_lic_state_rows(
            state=state,
            run_date=run_date,
            max_pages=max_fetch_pages,
            sleep_ms=sleep_ms,
            cache_dir=cache_dir,
            diagnostics_dir=diagnostics_dir,
            allow_cache_write=allow_cache_write,
        )
    raise ValueError(f"unsupported_source={token}")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Generate deterministic discovery CSV feed from seed pools + optional autogrow.")
    ap.add_argument("--print-config", action="store_true", help="Print resolved output path and exit.")
    ap.add_argument("--doctor", action="store_true", help="Run source/runtime readiness checks and exit.")
    ap.add_argument("--apollo-doctor", action="store_true", help="Check Apollo master-key endpoint access and exit.")
    ap.add_argument("--dry-run", action="store_true", help="Compute rows only; do not write output files.")
    ap.add_argument("--for-date", default="", help="Override run date (YYYY-MM-DD) for selected_state/backlog preview.")
    ap.add_argument("--states", default="", help="Override state scope: 'all' or comma-separated states (example: TX,CA,FL).")
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
        return _run_apollo_doctor_only(diagnostics_dir=diagnostics_dir)

    env_states_default = _parse_states(os.getenv("OUTREACH_STATES", "")) or list(DEFAULT_STATE_SCOPE_ALL)
    if not env_states_default:
        print(f"{ERR_GENERATOR_FAILED} stage=states err=state_scope_default empty", file=sys.stderr)
        return 2
    try:
        effective_states = _resolve_state_scope(str(args.states or ""), env_states_default)
    except Exception as exc:
        print(f"{ERR_GENERATOR_FAILED} stage=states err={exc}", file=sys.stderr)
        return 2
    if effective_states is not None and not effective_states:
        print(f"{ERR_GENERATOR_FAILED} stage=states err=state_scope empty", file=sys.stderr)
        return 2
    selection_states = _states_for_selection(effective_states)
    selected_state = _choose_state(selection_states, run_date)
    autogrow_states = list(selection_states)

    try:
        autogrow_cfg = _parse_autogrow_config()
    except Exception as exc:
        print(f"{ERR_GENERATOR_FAILED} stage=autogrow_config err={exc}", file=sys.stderr)
        return 2
    if args.doctor:
        return _run_generator_doctor(
            diagnostics_dir=diagnostics_dir,
            autogrow_sources=list(autogrow_cfg.get("sources") or []),
        )
    try:
        apollo_cfg = _parse_apollo_config(list(autogrow_cfg.get("sources") or []))
    except Exception as exc:
        print(f"{ERR_GENERATOR_FAILED} stage=apollo_config err={exc}", file=sys.stderr)
        return 2
    enrich_cfg = _parse_enrich_config(data_dir)

    crm_db = crm_store.crm_db_path()
    conn = _connect_crm_if_exists(crm_db)
    autogrow_state_details: list[dict[str, object]] = []
    selected_backlog_current = 0
    selected_new_needed = 0
    safety_net_forced_states: list[str] = []
    safety_net_forced_state_details: list[str] = []
    cohort_summary: dict[str, object] = _default_input_cohort()
    existing_crm_emails: set[str] = set()
    existing_crm_domains: set[str] = set()
    try:
        suppressed_emails = _load_suppression_set(data_dir=data_dir, conn=conn)
        cohort_summary = _compute_input_cohort(conn=conn, states_scope=effective_states, suppressed_emails=suppressed_emails)
        existing_crm_emails = _existing_crm_emails(conn)
        existing_crm_domains = _existing_crm_domains(conn)
        skip_role_inboxes = _bool_env(os.getenv("OUTREACH_SKIP_ROLE_INBOXES", "1"))
        safety_net_floor = 3
        for state_item in autogrow_states:
            backlog_current_item = compute_uncontacted_backlog(
                conn=conn,
                state=state_item,
                suppressed_emails=suppressed_emails,
                skip_role_inboxes=skip_role_inboxes,
            )
            pool_total_current = _count_crm_pool_total(conn=conn, state=state_item)
            safety_forced = bool(
                (not bool(autogrow_cfg["enabled"]))
                and bool(autogrow_cfg.get("safety_net_enabled"))
                and int(backlog_current_item) < int(safety_net_floor)
            )
            effective_autogrow = bool(autogrow_cfg["enabled"]) or safety_forced
            new_needed_item = max(0, int(autogrow_cfg["backlog_target"]) - int(backlog_current_item)) if effective_autogrow else 0
            state_norm = _normalize_state(state_item)
            if safety_forced and state_norm and state_norm not in safety_net_forced_states:
                safety_net_forced_states.append(state_norm)
            if safety_forced and state_norm:
                safety_net_forced_state_details.append(f"{state_norm}:{int(backlog_current_item)}")
            detail: dict[str, object] = {
                "state": state_norm,
                "pool_total_current": int(pool_total_current),
                "backlog_current": int(backlog_current_item),
                "backlog_sendable_current": int(backlog_current_item),
                "new_needed": int(new_needed_item),
                "effective_autogrow": bool(effective_autogrow),
                "safety_net_forced": bool(safety_forced),
                "safety_net_reason": "SENDABLE_BELOW_FLOOR" if safety_forced else "",
                "safety_net_floor": int(safety_net_floor),
                "ohs_bg_base_max_pages": int(autogrow_cfg["max_fetch_pages"]),
                "ohs_bg_effective_max_pages": int(autogrow_cfg["max_fetch_pages"]),
                "ohs_bg_deeper_enabled": 0,
            }
            for prefix in AUTOGROW_SOURCE_PREFIX.values():
                detail[f"{prefix}_candidate"] = 0
                detail[f"{prefix}_accepted"] = 0
                detail[f"{prefix}_max_fetch_pages"] = int(autogrow_cfg["max_fetch_pages"])
                detail[f"{prefix}_pages_fetched"] = 0
                detail[f"{prefix}_backlog_credit"] = 0
                for reject_key in AUTOGROW_REJECT_KEYS:
                    detail[f"{prefix}_rejected_{reject_key}"] = 0
            autogrow_state_details.append(detail)
            if state_norm == _normalize_state(selected_state):
                selected_backlog_current = int(backlog_current_item)
                selected_new_needed = int(new_needed_item)
    finally:
        if conn is not None:
            conn.close()

    autogrow_state = {
        "enabled": bool(autogrow_cfg["enabled"]),
        "state_scope_all": effective_states is None,
        "state_scope": list(effective_states or []),
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
        "safety_net_forced_details": list(safety_net_forced_state_details),
        "safety_net_reason": "SENDABLE_BELOW_FLOOR" if bool(safety_net_forced_states) else "",
        "state_details": autogrow_state_details,
        "total_states": len(autogrow_state_details),
        "total_accepted": 0,
        "input_cohort": dict(cohort_summary),
    }

    sources_empty = bool(autogrow_cfg["enabled"]) and len(list(autogrow_cfg["sources"])) == 0
    crawl_probe, source_availability = _probe_autogrow_runtime()
    aiha_result = _default_autogrow_source_result(
        cache_path=_source_cache_path_for_state(cache_root_dir, "AIHA", selected_state),
        enabled=bool(autogrow_cfg["enabled"]),
        sources_empty=sources_empty,
    )
    aiha_rejected: Counter = Counter()
    aiha_loss_counters: Counter = _default_aiha_loss_counters()
    ohs_bg_result = _default_autogrow_source_result(
        cache_path=_source_cache_path_for_state(cache_root_dir, "OHS_BG", selected_state),
        enabled=bool(autogrow_cfg["enabled"]),
        sources_empty=sources_empty,
    )
    ohs_bg_rejected: Counter = Counter()
    apollo_result: dict[str, object] = _default_apollo_result(cache_root_dir, selected_state, sources_empty=sources_empty)
    apollo_rejected: Counter = Counter()
    bcsp_result = _default_autogrow_source_result(
        cache_path=_source_cache_path_for_state(cache_root_dir, "BCSP", selected_state),
        enabled=bool(autogrow_cfg["enabled"]),
        sources_empty=sources_empty,
    )
    bcsp_rejected: Counter = Counter()
    osha_news_result = _default_autogrow_source_result(
        cache_path=_source_cache_path_for_state(cache_root_dir, "OSHA_NEWS", selected_state),
        enabled=bool(autogrow_cfg["enabled"]),
        sources_empty=sources_empty,
    )
    osha_news_rejected: Counter = Counter()
    state_lic_result = _default_autogrow_source_result(
        cache_path=_source_cache_path_for_state(cache_root_dir, "STATE_LIC", selected_state),
        enabled=bool(autogrow_cfg["enabled"]),
        sources_empty=sources_empty,
    )
    state_lic_rejected: Counter = Counter()
    extra_source_results: dict[str, dict] = {
        "BCSP": bcsp_result,
        "OSHA_NEWS": osha_news_result,
        "STATE_LIC": state_lic_result,
    }
    extra_source_rejected: dict[str, Counter] = {
        "BCSP": bcsp_rejected,
        "OSHA_NEWS": osha_news_rejected,
        "STATE_LIC": state_lic_rejected,
    }
    apollo_enrich_remaining = int(apollo_cfg.get("enrich_max_per_run") or 0)
    diagnostics_path: Path | None = None
    autogrow_rows: list[dict[str, str]] = []
    autogrow_seen_domains: set[str] = set()
    enrich_metrics = _default_generator_enrich_metrics()

    if args.print_config:
        print(f"{PASS_GENERATOR_PRINT_CONFIG} data_dir={data_dir.resolve()}")
        print(f"{PASS_GENERATOR_PRINT_CONFIG} output_path={output_path.resolve()}")
        print(f"{PASS_GENERATOR_PRINT_CONFIG} cache_dir={cache_dir.resolve()}")
        print(f"{PASS_GENERATOR_PRINT_CONFIG} diagnostics_dir={diagnostics_dir.resolve()}")
        print(f"{PASS_GENERATOR_PRINT_CONFIG} state_scope={_state_scope_token(effective_states)}")
        print(f"{PASS_GENERATOR_PRINT_CONFIG} selected_state={selected_state}")
        print(f"{PASS_GENERATOR_PRINT_CONFIG} run_date={run_date.isoformat()}")
        _print_tokens(
            path=output_path,
            rows_read=int(cohort_summary.get("eligible") or 0),
            rows_written=0,
            status="PRINT_CONFIG",
            autogrow=autogrow_state,
            enrich_cfg=enrich_cfg,
            enrich_metrics=enrich_metrics,
            aiha_result=aiha_result,
            aiha_rejected=aiha_rejected,
            aiha_loss_counters=aiha_loss_counters,
            ohs_bg_result=ohs_bg_result,
            ohs_bg_rejected=ohs_bg_rejected,
            apollo_cfg=apollo_cfg,
            apollo_result=apollo_result,
            apollo_rejected=apollo_rejected,
            diagnostics_path=None,
            crawl_probe=crawl_probe,
            source_availability=source_availability,
            extra_source_results=extra_source_results,
            extra_source_rejected=extra_source_rejected,
            print_availability=True,
            row_observability=_generator_row_observability([]),
        )
        return 0

    try:
        state_rows, _rows_read_seed = _build_clean_state_rows(effective_states)
    except Exception as exc:
        print(f"{ERR_GENERATOR_FAILED} stage=build_rows err={exc}", file=sys.stderr)
        return 2

    autogrow_seen_emails: set[str] = set()
    selected_state_norm = _normalize_state(selected_state)
    source_prefix_map = dict(AUTOGROW_SOURCE_PREFIX)
    for detail in autogrow_state_details:
        state_detail = _normalize_state(str(detail.get("state") or ""))
        if not state_detail or not bool(detail.get("effective_autogrow")):
            continue
        state_new_needed = max(0, int(detail.get("new_needed") or 0))
        if state_new_needed <= 0:
            continue

        source_order = source_policy.autogrow_source_order(list(autogrow_cfg["sources"]))
        if bool(detail.get("safety_net_forced")) and "AIHA" not in source_order:
            source_order.insert(0, "AIHA")

        remaining_needed = state_new_needed
        remaining_slots = state_new_needed
        for source_token in source_order:
            if remaining_slots <= 0:
                break
            if source_token not in AUTOGROW_ALLOWED_SOURCES:
                continue

            effective_max_fetch_pages = int(autogrow_cfg["max_fetch_pages"])
            if source_token == "OHS_BG":
                is_priority_state = bool(
                    (state_detail == selected_state_norm) or bool(detail.get("safety_net_forced"))
                )
                if is_priority_state and remaining_needed > 0:
                    effective_max_fetch_pages = min(max(1, effective_max_fetch_pages) * 2, 12)
                detail["ohs_bg_effective_max_pages"] = int(effective_max_fetch_pages)
                detail["ohs_bg_deeper_enabled"] = 1 if int(effective_max_fetch_pages) > int(autogrow_cfg["max_fetch_pages"]) else 0

            apollo_limit_for_state = 0
            if source_token == "APOLLO":
                apollo_limit_for_state = min(max(0, remaining_needed), max(0, apollo_enrich_remaining))

            result = _fetch_autogrow_source_rows(
                source_token=source_token,
                state=state_detail,
                run_date=run_date,
                max_fetch_pages=int(effective_max_fetch_pages),
                sleep_ms=int(autogrow_cfg["sleep_ms"]),
                cache_root_dir=cache_root_dir,
                diagnostics_dir=diagnostics_dir,
                allow_cache_write=not bool(args.dry_run),
                apollo_cfg=apollo_cfg,
                apollo_enrich_limit=apollo_limit_for_state,
            )
            rows_candidate = list(result.get("rows") or [])
            if rows_candidate:
                enrich_out = prospect_enrich_email.enrich_autogrow_rows(
                    rows=rows_candidate,
                    domain_enabled=bool(enrich_cfg.get("domain_enabled")),
                    hunter_enabled=(bool(enrich_cfg.get("hunter_enabled")) and not bool(args.dry_run)),
                    hunter_api_key=str(enrich_cfg.get("hunter_api_key") or ""),
                    sleep_ms=int(autogrow_cfg.get("sleep_ms") or 0),
                    hunter_usage_path=Path(enrich_cfg.get("hunter_usage_path") or _generation_hunter_usage_path(data_dir)),
                )
                rows_candidate = list(enrich_out.get("rows") or rows_candidate)
                _merge_generator_enrich_metrics(enrich_metrics, enrich_out.get("metrics"))
            if source_token == TDLR_STATE_LIC_SOURCE_KEY:
                rows_candidate = _promote_state_lic_work_email_rows(rows_candidate)
            if source_token == "AIHA":
                aiha_loss_counters.update(
                    _aiha_loss_counters_from_candidates(
                        rows=rows_candidate,
                        target_state=state_detail,
                        existing_crm_emails=set(existing_crm_emails),
                        existing_crm_domains=set(existing_crm_domains),
                        preseen_batch_emails=set(autogrow_seen_emails),
                        preseen_batch_domains=set(autogrow_seen_domains),
                    )
                )
            filtered_rows, rejected = _filter_autogrow_candidates(
                rows=rows_candidate,
                target_state=state_detail,
                suppressed_emails=suppressed_emails,
                existing_crm_emails=set(existing_crm_emails),
                preseen_batch_emails=set(autogrow_seen_emails),
                source_token=source_token,
                apollo_allow_role_inbox=bool(apollo_cfg.get("allow_role_inbox")),
            )
            accepted_rows = filtered_rows[:remaining_slots]
            sendable_accepted = sum(
                1
                for row in accepted_rows
                if _row_is_effectively_sendable(row, skip_role_inboxes=bool(skip_role_inboxes))
            )
            backlog_credit = int(sendable_accepted) if source_policy.counts_toward_consultant_backlog(source_token) else 0
            remaining_needed = max(0, remaining_needed - int(backlog_credit))
            remaining_slots = max(0, remaining_slots - int(len(accepted_rows)))

            source_prefix = source_prefix_map.get(source_token, "")
            if source_prefix:
                detail[f"{source_prefix}_candidate"] = len(rows_candidate)
                detail[f"{source_prefix}_accepted"] = len(accepted_rows)
                detail[f"{source_prefix}_max_fetch_pages"] = int(effective_max_fetch_pages)
                detail[f"{source_prefix}_pages_fetched"] = int(result.get("pages_fetched") or 0)
                detail[f"{source_prefix}_backlog_credit"] = int(backlog_credit)
                for reject_key in AUTOGROW_REJECT_KEYS:
                    detail[f"{source_prefix}_rejected_{reject_key}"] = int(rejected.get(reject_key, 0))

            if source_token == "APOLLO":
                role_fit_true = max(0, len(rows_candidate) - int(rejected.get("role_mismatch") or 0))
                apollo_result["search_pages_fetched"] = int(apollo_result.get("search_pages_fetched") or 0) + int(
                    result.get("pages_fetched") or 0
                )
                apollo_result["search_rows_returned"] = int(apollo_result.get("search_rows_returned") or 0) + int(
                    result.get("search_rows_returned") or 0
                )
                apollo_result["search_rows_has_email_true"] = int(
                    apollo_result.get("search_rows_has_email_true") or 0
                ) + int(result.get("search_rows_has_email_true") or 0)
                apollo_result["search_rows_role_fit_true"] = int(
                    apollo_result.get("search_rows_role_fit_true") or 0
                ) + int(role_fit_true)
                apollo_result["search_rows_deduped_id"] = int(apollo_result.get("search_rows_deduped_id") or 0) + int(
                    result.get("search_rows_deduped_id") or 0
                )
                apollo_result["imported_sendable"] = int(apollo_result.get("imported_sendable") or 0) + int(
                    sendable_accepted
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
                if bool(result.get("forbidden")) or int(result.get("error_status") or 0) == 403:
                    print(f"{WARN_APOLLO_FREE_TIER_API_BLOCKED} state={state_detail}")

            autogrow_rows.extend(accepted_rows)
            for row in accepted_rows:
                email = _normalize_email(row.get("contact_email") or row.get("email") or "")
                if email:
                    autogrow_seen_emails.add(email)
                domain = _row_domain_value(row, email)
                if domain:
                    autogrow_seen_domains.add(domain)

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
                elif source_token == "BCSP":
                    bcsp_result.update(result)
                    bcsp_result["rows_candidate"] = len(rows_candidate)
                    bcsp_result["rows_accepted"] = len(accepted_rows)
                    bcsp_rejected = rejected
                    extra_source_rejected["BCSP"] = bcsp_rejected
                elif source_token == "OSHA_NEWS":
                    osha_news_result.update(result)
                    osha_news_result["rows_candidate"] = len(rows_candidate)
                    osha_news_result["rows_accepted"] = len(accepted_rows)
                    osha_news_rejected = rejected
                    extra_source_rejected["OSHA_NEWS"] = osha_news_rejected
                elif source_token == "STATE_LIC":
                    state_lic_result.update(result)
                    state_lic_result["rows_candidate"] = len(rows_candidate)
                    state_lic_result["rows_accepted"] = len(accepted_rows)
                    state_lic_rejected = rejected
                    extra_source_rejected["STATE_LIC"] = state_lic_rejected

            warn_token = _normalize_text(result.get("warn_token") or "")
            if warn_token:
                print(f"{warn_token} source={AUTOGROW_SOURCE_LABEL.get(source_token, source_token.lower())} state={state_detail}")
            if result.get("error"):
                source_label = AUTOGROW_SOURCE_LABEL.get(source_token, source_token.lower())
                print(f"{WARN_AUTOGROWTH_SOURCE_FAILED} source={source_label} state={state_detail} err={result.get('error')}")

    autogrow_state["total_accepted"] = int(
        sum(
            sum(int(d.get(f"{prefix}_accepted") or 0) for prefix in AUTOGROW_SOURCE_PREFIX.values())
            for d in autogrow_state_details
        )
    )
    rows_read_total = int(cohort_summary.get("eligible") or 0)

    if args.dry_run:
        seed_rows = _state_rows_to_combined_input(state_rows, effective_states)
        rows = _to_discovery_rows(seed_rows + autogrow_rows)
        _print_tokens(
            path=output_path,
            rows_read=rows_read_total,
            rows_written=len(rows),
            status="DRY_RUN",
            autogrow=autogrow_state,
            enrich_cfg=enrich_cfg,
            enrich_metrics=enrich_metrics,
            aiha_result=aiha_result,
            aiha_rejected=aiha_rejected,
            aiha_loss_counters=aiha_loss_counters,
            ohs_bg_result=ohs_bg_result,
            ohs_bg_rejected=ohs_bg_rejected,
            apollo_cfg=apollo_cfg,
            apollo_result=apollo_result,
            apollo_rejected=apollo_rejected,
            diagnostics_path=diagnostics_path,
            crawl_probe=crawl_probe,
            source_availability=source_availability,
            extra_source_results=extra_source_results,
            extra_source_rejected=extra_source_rejected,
            row_observability=_generator_row_observability(rows),
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
        enrich_cfg=enrich_cfg,
        enrich_metrics=enrich_metrics,
        aiha_result=aiha_result,
        aiha_rejected=aiha_rejected,
        aiha_loss_counters=aiha_loss_counters,
        ohs_bg_result=ohs_bg_result,
        ohs_bg_rejected=ohs_bg_rejected,
        apollo_cfg=apollo_cfg,
        apollo_result=apollo_result,
        apollo_rejected=apollo_rejected,
        diagnostics_path=diagnostics_path,
        crawl_probe=crawl_probe,
        source_availability=source_availability,
        extra_source_results=extra_source_results,
        extra_source_rejected=extra_source_rejected,
        row_observability=_generator_row_observability(rows),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
