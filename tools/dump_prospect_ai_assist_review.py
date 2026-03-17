#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import os
import re
import shutil
import sqlite3
import sys
import tempfile
from collections import Counter
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import ai_assist_paths
from outreach import crm_store
from outreach import prospect_sources_aiha
from outreach import prospect_sources_state_lic
from outreach import run_prospect_generation as generation
from outreach import state_lic_precision
from outreach import source_policy
from outreach.prospect_enrich_email import CORP_SUFFIXES
from runtime_data_dir import resolve_data_dir

ERR_AI_ASSIST_DUMP_CONFIG = "ERR_AI_ASSIST_DUMP_CONFIG"
AI_ASSIST_DUMP_DEFAULT_BACKLOG_TARGET = 60
AI_ASSIST_DUMP_DEFAULT_ENABLED = "1"
AI_ASSIST_DUMP_DEFAULT_RAW_TARGET = 30
AI_ASSIST_DUMP_DEFAULT_PACKET_SIZE = 10
AI_ASSIST_DEFAULT_AUTOGROW_SOURCES = ("AIHA", "OHS_BG", "STATE_LIC")
AI_ASSIST_PUBLIC_SOURCES = ("AIHA", "OHS_BG", "BCSP", "OSHA_NEWS", "STATE_LIC")
SEED_COLUMNS = (
    "firm",
    "website",
    "state",
    "city",
    "phone",
    "address",
    "seed_source",
    "seed_source_url",
    "source_record_id",
    "license_number",
    "seed_id",
)
REVIEW_COLUMNS = (
    "state",
    "decision",
    "firm",
    "website",
    "contact_name",
    "title",
    "email",
    "source_urls",
    "confidence",
    "evidence_snippet",
    "seed_id",
)
COMMON_MULTI_LABEL_TLDS = {
    "co.uk",
    "org.uk",
    "gov.uk",
    "ac.uk",
    "com.au",
    "net.au",
    "org.au",
    "com.br",
    "com.mx",
    "co.nz",
    "com.sg",
    "com.hk",
}
CITY_STATE_ZIP_RE = re.compile(r"^(.+?)\s+([A-Z]{2})\s+\d")
AI_ASSIST_PACKET_MANIFEST_SCHEMA = "ai_assist_packet_manifest_v2"
DEDUP_LOCATOR_PRECEDENCE = (
    "license_number",
    "phone",
    "address",
    "city",
    "seed_source_url",
    "source_record_id",
)
EXCLUSION_KEYS = (
    "excluded_missing_minimum_locator",
    "excluded_already_in_crm",
    "excluded_bad_firm",
    "excluded_state_mismatch",
    "excluded_state_lic_fit_mismatch",
    "excluded_state_lic_hard_negative_class",
    "excluded_state_lic_negative_keyword_family",
    "excluded_state_lic_blank_website_no_positive_evidence",
    "excluded_state_lic_feedback_license_class",
    "excluded_state_lic_feedback_keyword_family",
    "excluded_duplicate_seed",
)
DIAGNOSTIC_STAGE_KEYS = (
    "raw",
    "identity_ready",
    "review_eligible",
    "safety_passed",
    "candidates",
    "selected",
)
STATE_LIC_REVIEW_ANCHOR_FIELDS = (
    "website",
    "phone",
    "address",
    "city",
    "license_number",
    "seed_source_url",
    "source_record_id",
)
SEED_INDEX_FILENAME = "seed_index.json"
STATE_LIC_DEFAULT_PACKET_CAP_PERCENT = 30
STATE_LIC_LOW_ACCEPT_RATE_PACKET_CAP_PERCENT = 20
STATE_LIC_LOW_ACCEPT_RATE_THRESHOLD = 0.15
STATE_LIC_LOW_ACCEPT_RATE_MIN_SAMPLE = 10
STATE_LIC_FEEDBACK_SUPPRESSION_MIN_SAMPLE = 5
STATE_LIC_SHADOW_PROFILE_PRODUCTION = "production"
STATE_LIC_SHADOW_PROFILE_DEFAULT_CLASSES_ONLY = "default_classes_only"
STATE_LIC_SHADOW_PROFILE_ALL_CONTRACTOR_ONLY = "all_contractor_only"
STATE_LIC_SHADOW_PACKET_PROFILES = (
    STATE_LIC_SHADOW_PROFILE_PRODUCTION,
    STATE_LIC_SHADOW_PROFILE_DEFAULT_CLASSES_ONLY,
    STATE_LIC_SHADOW_PROFILE_ALL_CONTRACTOR_ONLY,
)
STATE_LIC_SHADOW_DEFAULT_LICENSE_CLASSES = frozenset(
    {
        "electrical contractor",
        "elevator contractor",
        "appliance installation contractor",
    }
)


def _emit(key: str, value: str | int) -> None:
    print(f"{key}={value}")


def _stage_counter_template() -> Counter[str]:
    return Counter({key: 0 for key in DIAGNOSTIC_STAGE_KEYS})


def _exclusion_counter_template() -> Counter[str]:
    return Counter({key: 0 for key in EXCLUSION_KEYS})


def _ordered_stage_counts_by_source(
    counts_by_source: dict[str, Counter[str]],
    source_tokens: list[str],
) -> dict[str, dict[str, int]]:
    ordered: dict[str, dict[str, int]] = {}
    for source_token in list(source_tokens or []):
        source_counts = counts_by_source.get(source_token) or _stage_counter_template()
        ordered[source_token] = {
            key: int(source_counts.get(key, 0))
            for key in DIAGNOSTIC_STAGE_KEYS
        }
    return ordered


def _ordered_exclusion_counts_by_source(
    counts_by_source: dict[str, Counter[str]],
    source_tokens: list[str],
) -> dict[str, int]:
    ordered: dict[str, int] = {}
    for source_token in list(source_tokens or []):
        ordered[source_token] = int(sum((counts_by_source.get(source_token) or _exclusion_counter_template()).values()))
    return ordered


def _ordered_exclusion_counts_by_source_and_reason(
    counts_by_source: dict[str, Counter[str]],
    source_tokens: list[str],
) -> dict[str, dict[str, int]]:
    ordered: dict[str, dict[str, int]] = {}
    for source_token in list(source_tokens or []):
        source_counts = counts_by_source.get(source_token) or _exclusion_counter_template()
        ordered[source_token] = {
            key: int(source_counts.get(key, 0))
            for key in EXCLUSION_KEYS
            if int(source_counts.get(key, 0)) > 0
        }
    return ordered


def _ordered_nonzero_counter(counter: Counter[str]) -> dict[str, int]:
    ordered: dict[str, int] = {}
    for key, value in sorted(
        ((str(key or ""), int(value or 0)) for key, value in counter.items()),
        key=lambda item: item[0],
    ):
        if value > 0:
            ordered[key] = value
    return ordered


def _top_exclusion_reasons(counter: Counter[str], *, limit: int = 5) -> list[dict[str, int | str]]:
    ordered = sorted(
        ((key, int(counter.get(key, 0))) for key in EXCLUSION_KEYS if int(counter.get(key, 0)) > 0),
        key=lambda item: (-item[1], item[0]),
    )
    return [{"reason": key, "count": count} for key, count in ordered[: max(1, int(limit))]]


def _source_selection_breakdown(rows: list[dict[str, Any]], source_tokens: list[str]) -> dict[str, int]:
    counter = Counter(
        str(row.get("source_token") or "")
        for row in list(rows or [])
        if str(row.get("source_token") or "")
    )
    ordered: dict[str, int] = {}
    for source_token in list(source_tokens or []):
        ordered[source_token] = int(counter.get(source_token, 0))
    return ordered


def _iso_to_datetime(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except Exception:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.astimezone()
    return parsed


def _json_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item or "").strip() for item in value if str(item or "").strip()]
    text = str(value or "").strip()
    if not text:
        return []
    try:
        decoded = json.loads(text)
    except Exception:
        return []
    if not isinstance(decoded, list):
        return []
    return [str(item or "").strip() for item in decoded if str(item or "").strip()]


def _state_lic_overall_accept_snapshot(rows: list[sqlite3.Row]) -> dict[str, Any]:
    reviewed = 0
    accepted = 0
    for row in rows:
        decision = str(row["decision"] or "").strip().lower()
        if decision not in {"accept", "reject"}:
            continue
        reviewed += 1
        if decision == "accept":
            accepted += 1
    accept_rate = (accepted / reviewed) if reviewed else 0.0
    return {
        "reviewed": int(reviewed),
        "accepted": int(accepted),
        "accept_rate": round(float(accept_rate), 4),
    }


def _feedback_rate_summary(counter: Counter[str]) -> dict[str, dict[str, Any]]:
    summary: dict[str, dict[str, Any]] = {}
    for key in sorted({token.rsplit("|", 1)[0] for token in counter.keys() if "|" in token}):
        reviewed = int(counter.get(f"{key}|reviewed", 0))
        accepted = int(counter.get(f"{key}|accepted", 0))
        accept_rate = (accepted / reviewed) if reviewed else 0.0
        summary[key] = {
            "reviewed": reviewed,
            "accepted": accepted,
            "accept_rate": round(float(accept_rate), 4),
        }
    return summary


def _state_lic_feedback_snapshot(conn: sqlite3.Connection | None) -> dict[str, Any]:
    empty = {
        "window_days": 7,
        "source_accept_rates": {},
        "license_class_accept_rates": {},
        "keyword_family_accept_rates": {},
        "state_lic_overall": {"reviewed": 0, "accepted": 0, "accept_rate": 0.0},
        "state_lic_cap_percent": STATE_LIC_DEFAULT_PACKET_CAP_PERCENT,
        "state_lic_cap_reason": "default",
        "license_class_exclusions": [],
        "license_class_downranks": [],
        "keyword_family_exclusions": [],
        "keyword_family_downranks": [],
    }
    if conn is None:
        return empty
    if not crm_store._table_exists(conn, crm_store.AI_ASSIST_CANDIDATE_TABLE):
        return empty
    columns = crm_store._table_columns(conn, crm_store.AI_ASSIST_CANDIDATE_TABLE)
    required = {
        "decision",
        "updated_at",
        "seed_source_token",
        "state_lic_license_class_norm",
        "state_lic_positive_families_json",
        "state_lic_negative_families_json",
    }
    if not required.issubset(columns):
        return empty

    cutoff = datetime.now().astimezone() - timedelta(days=7)
    rows = conn.execute(
        f"""
        SELECT decision, updated_at, seed_source_token, state_lic_license_class_norm,
               state_lic_positive_families_json, state_lic_negative_families_json
        FROM {crm_store.AI_ASSIST_CANDIDATE_TABLE}
        WHERE updated_at IS NOT NULL AND trim(updated_at) <> ''
        """
    ).fetchall()
    filtered_rows: list[sqlite3.Row] = []
    source_counter: Counter[str] = Counter()
    class_counter: Counter[str] = Counter()
    family_counter: Counter[str] = Counter()
    for row in rows:
        updated_at = _iso_to_datetime(row["updated_at"])
        if updated_at is None or updated_at < cutoff:
            continue
        decision = str(row["decision"] or "").strip().lower()
        if decision not in {"accept", "reject"}:
            continue
        filtered_rows.append(row)
        source_token = str(row["seed_source_token"] or "").strip()
        if source_token:
            source_counter[f"{source_token}|reviewed"] += 1
            if decision == "accept":
                source_counter[f"{source_token}|accepted"] += 1
        class_norm = str(row["state_lic_license_class_norm"] or "").strip()
        if class_norm:
            class_counter[f"{class_norm}|reviewed"] += 1
            if decision == "accept":
                class_counter[f"{class_norm}|accepted"] += 1
        families = sorted(
            set(
                _json_list(row["state_lic_positive_families_json"])
                + _json_list(row["state_lic_negative_families_json"])
            )
        )
        for family in families:
            family_counter[f"{family}|reviewed"] += 1
            if decision == "accept":
                family_counter[f"{family}|accepted"] += 1

    source_summary = _feedback_rate_summary(source_counter)
    class_summary = _feedback_rate_summary(class_counter)
    family_summary = _feedback_rate_summary(family_counter)
    overall = source_summary.get("STATE_LIC") or _state_lic_overall_accept_snapshot(
        [row for row in filtered_rows if str(row["seed_source_token"] or "").strip() == "STATE_LIC"]
    )
    cap_percent = STATE_LIC_DEFAULT_PACKET_CAP_PERCENT
    cap_reason = "default"
    if (
        int(overall.get("reviewed", 0)) >= STATE_LIC_LOW_ACCEPT_RATE_MIN_SAMPLE
        and float(overall.get("accept_rate", 0.0)) < STATE_LIC_LOW_ACCEPT_RATE_THRESHOLD
    ):
        cap_percent = STATE_LIC_LOW_ACCEPT_RATE_PACKET_CAP_PERCENT
        cap_reason = "low_accept_rate"

    license_class_exclusions: list[str] = []
    license_class_downranks: list[str] = []
    for class_norm, stats in class_summary.items():
        reviewed = int(stats.get("reviewed", 0))
        accepted = int(stats.get("accepted", 0))
        accept_rate = float(stats.get("accept_rate", 0.0))
        if reviewed < STATE_LIC_FEEDBACK_SUPPRESSION_MIN_SAMPLE:
            continue
        if accepted == 0:
            license_class_exclusions.append(class_norm)
        elif accept_rate < STATE_LIC_LOW_ACCEPT_RATE_THRESHOLD:
            license_class_downranks.append(class_norm)

    keyword_family_exclusions: list[str] = []
    keyword_family_downranks: list[str] = []
    for family, stats in family_summary.items():
        reviewed = int(stats.get("reviewed", 0))
        accepted = int(stats.get("accepted", 0))
        accept_rate = float(stats.get("accept_rate", 0.0))
        if reviewed < STATE_LIC_FEEDBACK_SUPPRESSION_MIN_SAMPLE:
            continue
        if accepted == 0:
            keyword_family_exclusions.append(family)
        elif accept_rate < STATE_LIC_LOW_ACCEPT_RATE_THRESHOLD:
            keyword_family_downranks.append(family)

    return {
        "window_days": 7,
        "source_accept_rates": source_summary,
        "license_class_accept_rates": class_summary,
        "keyword_family_accept_rates": family_summary,
        "state_lic_overall": overall,
        "state_lic_cap_percent": int(cap_percent),
        "state_lic_cap_reason": cap_reason,
        "license_class_exclusions": sorted(license_class_exclusions),
        "license_class_downranks": sorted(license_class_downranks),
        "keyword_family_exclusions": sorted(keyword_family_exclusions),
        "keyword_family_downranks": sorted(keyword_family_downranks),
    }


def _state_lic_max_rows_for_cap(non_state_count: int, cap_percent: int) -> int:
    if non_state_count <= 0 or cap_percent <= 0 or cap_percent >= 100:
        return 0
    return int((non_state_count * cap_percent) // (100 - cap_percent))


def _reason_token_stdout_suffix(reason: str) -> str:
    return re.sub(r"[^A-Z0-9_]", "_", str(reason or "").upper())


def _local_today_date() -> date:
    return datetime.now().astimezone().date()


def _current_run_started_at() -> datetime:
    return datetime.now().astimezone()


def _parse_date(value: str) -> date:
    text = str(value or "").strip().lower()
    if not text or text == "today":
        return _local_today_date()
    return datetime.strptime(text, "%Y-%m-%d").date()


def _int_env(name: str, default: int) -> int:
    raw = str(os.getenv(name, "")).strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except Exception:
        return default
    return value if value > 0 else default


def _bool_env(name: str, default: str) -> int:
    raw = str(os.getenv(name, default)).strip().lower()
    return 1 if raw in {"1", "true", "yes", "on"} else 0


def _parse_states_arg(raw_states: list[str]) -> list[str]:
    if not list(raw_states or []):
        return []
    flattened: list[str] = []
    for raw in list(raw_states or []):
        flattened.extend([str(part or "").strip() for part in str(raw or "").split(",")])
    csv_text = ",".join([part for part in flattened if part])
    return generation._parse_states(csv_text) if csv_text else []


def _resolve_state_scope(raw_states: list[str]) -> list[str] | None:
    autogrow_env_states = generation._parse_states(os.getenv("PROSPECT_AUTOGROW_STATES", ""))
    outreach_env_states = generation._parse_states(os.getenv("OUTREACH_STATES", ""))
    env_states = autogrow_env_states or outreach_env_states or list(generation.DEFAULT_STATE_SCOPE_ALL)
    if not list(raw_states or []):
        return generation._resolve_state_scope("", env_states)
    return generation._resolve_state_scope(",".join(_parse_states_arg(raw_states)), env_states)


def _normalize_text(value: Any) -> str:
    return " ".join(str(value or "").replace("\r", " ").replace("\n", " ").split()).strip()


def _normalize_firm_key(value: str) -> str:
    text = _normalize_text(value).upper()
    if not text:
        return ""
    tokens = re.split(r"\s+", re.sub(r"[^A-Z0-9 ]", " ", text))
    while tokens and tokens[-1] in CORP_SUFFIXES:
        tokens.pop()
    return "".join(re.sub(r"[^A-Z0-9]", "", token) for token in tokens if token)


def _root_domain(domain: str) -> str:
    host = _normalize_text(domain).lower().strip(".")
    if not host or re.fullmatch(r"\d+\.\d+\.\d+\.\d+", host):
        return host
    parts = [part for part in host.split(".") if part]
    if len(parts) <= 2:
        return ".".join(parts)
    suffix = ".".join(parts[-2:])
    if suffix in COMMON_MULTI_LABEL_TLDS and len(parts) >= 3:
        return ".".join(parts[-3:])
    return ".".join(parts[-2:])


def _normalized_root_domains(domains: set[str]) -> set[str]:
    normalized: set[str] = set()
    for domain in set(domains or set()):
        root = _root_domain(str(domain or ""))
        if root:
            normalized.add(root)
    return normalized


def _resolve_source_tokens() -> tuple[list[str], str]:
    configured_raw = str(os.getenv("PROSPECT_AUTOGROW_SOURCES", "")).strip()
    configured = (
        [token for token in configured_raw.split(",") if str(token or "").strip()]
        if configured_raw
        else list(AI_ASSIST_DEFAULT_AUTOGROW_SOURCES)
    )
    ordered = source_policy.autogrow_source_order(configured)
    allowed = set(AI_ASSIST_PUBLIC_SOURCES)
    implemented = set(source_policy.implemented_autogrow_sources())
    filtered = [token for token in ordered if token in allowed and token in implemented]
    if filtered:
        return filtered, ""
    if configured_raw:
        return [], ",".join([str(token or "").strip().upper() for token in configured if str(token or "").strip()])
    return [token for token in AI_ASSIST_DEFAULT_AUTOGROW_SOURCES if token in implemented], ""


def _batch_run_token(run_started_at: datetime) -> str:
    return f"R{run_started_at.strftime('%H%M%S%f')}"


def _resolve_output_path(*, output: str, output_dir: str, for_date: date) -> tuple[Path, Path]:
    default_output_dir = ai_assist_paths.prospect_audit_dir(repo_root=REPO_ROOT)
    filename = f"prospect_ai_assist_review_{for_date.strftime('%Y%m%d')}.txt"
    output_text = str(output or "").strip()
    output_dir_text = str(output_dir or "").strip()
    if output_text:
        out_path = Path(output_text).expanduser().resolve(strict=False)
    else:
        out_dir = Path(output_dir_text).expanduser().resolve(strict=False) if output_dir_text else default_output_dir
        out_path = (out_dir / filename).resolve(strict=False)
    return out_path.parent.resolve(strict=False), out_path.resolve(strict=False)


def _packet_dir_for_output_path(output_path: Path) -> Path:
    return (output_path.parent / f"{output_path.stem}_packets").resolve(strict=False)


def _packet_manifest_path(packet_dir: Path) -> Path:
    return (packet_dir / "manifest.json").resolve(strict=False)


def _packet_seed_index_path(packet_dir: Path) -> Path:
    return (packet_dir / SEED_INDEX_FILENAME).resolve(strict=False)


def _packet_seed_filename(packet_number: int) -> str:
    return f"seed_packet_{packet_number:03d}.csv"


def _packet_prompt_filename(packet_number: int) -> str:
    return f"review_packet_{packet_number:03d}.txt"


def _packet_review_filename(run_date: date, packet_number: int) -> str:
    return f"prospect_ai_assist_review_{run_date.strftime('%Y%m%d')}_packet_{packet_number:03d}_reviewed.csv"


def _packet_batch_id(run_date: date, packet_number: int) -> str:
    return f"{run_date.isoformat()}_AIASSIST_P{packet_number:03d}"


def _packet_status_path(packet_dir: Path) -> Path:
    return (packet_dir / "packet_status.txt").resolve(strict=False)


def _chunk_rows(rows: list[dict[str, Any]], packet_size: int) -> list[list[dict[str, Any]]]:
    size = max(1, int(packet_size))
    return [rows[idx : idx + size] for idx in range(0, len(rows), size)]


def _reset_output_dir(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def _atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_fd, tmp_name = tempfile.mkstemp(prefix=f"{path.name}.", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(tmp_fd, "w", encoding="utf-8", newline="") as handle:
            handle.write(text)
        os.replace(tmp_name, str(path))
    finally:
        try:
            if os.path.exists(tmp_name):
                os.unlink(tmp_name)
        except Exception:
            pass


def _state_gap_snapshot(
    conn: sqlite3.Connection | None,
    *,
    states: list[str],
    suppressed_emails: set[str],
    backlog_target: int,
) -> list[dict[str, int | str]]:
    rows: list[dict[str, int | str]] = []
    for state in list(states or []):
        backlog_current = generation.compute_uncontacted_backlog(conn, state, suppressed_emails)
        crm_total = generation._count_crm_pool_total(conn, state)
        gap = max(0, int(backlog_target) - int(backlog_current))
        rows.append(
            {
                "state": state,
                "backlog_current": int(backlog_current),
                "crm_total": int(crm_total),
                "gap": int(gap),
            }
        )
    return [row for row in rows if int(row["gap"]) > 0]


def _existing_crm_firm_keys(conn: sqlite3.Connection | None) -> set[str]:
    if conn is None:
        return set()
    firm_keys: set[str] = set()
    try:
        rows = conn.execute("SELECT firm FROM prospects").fetchall()
    except Exception:
        return firm_keys
    for row in rows:
        firm_key = _normalize_firm_key(str(row[0] or ""))
        if firm_key:
            firm_keys.add(firm_key)
    return firm_keys


def _load_cache_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    if isinstance(payload, dict):
        rows = payload.get("rows") or []
        return [row for row in rows if isinstance(row, dict)]
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    return []


def _derive_seed_source_url(row: dict[str, Any], seed_source: str) -> str:
    explicit = _normalize_text(row.get("source_url") or "")
    if explicit:
        return explicit
    if seed_source.startswith("aiha_consultants_listing:"):
        page_id = seed_source.split(":", 1)[1].strip()
        if page_id:
            return prospect_sources_aiha.PAGE_URL_TEMPLATE.format(page_id=page_id)
    return ""


def _clean_seed_website(value: Any) -> str:
    website = generation.contact_normalization.normalize_website(str(value or ""))
    return website if generation._domain_from_website(website) else ""


def _parse_city_state_zip(value: Any, fallback_state: str) -> tuple[str, str]:
    text = _normalize_text(value)
    if not text:
        return "", generation._normalize_us_state(str(fallback_state or ""))
    match = CITY_STATE_ZIP_RE.search(text)
    if not match:
        return text, generation._normalize_us_state(str(fallback_state or ""))
    city = _normalize_text(match.group(1))
    state = generation._normalize_us_state(str(match.group(2) or "")) or generation._normalize_us_state(str(fallback_state or ""))
    return city, state


def _normalize_locator_value(field: str, value: Any) -> str:
    text = _normalize_text(value)
    if not text:
        return ""
    if field == "phone":
        digits = re.sub(r"[^0-9]", "", text)
        return digits or text.lower()
    if field == "license_number":
        return re.sub(r"[^A-Z0-9]", "", text.upper())
    if field == "source_record_id":
        return text.lower()
    if field == "seed_source_url":
        return text.lower()
    return text.lower()


def _primary_locator(seed: dict[str, Any]) -> tuple[str, str]:
    root_domain = _normalize_text(seed.get("root_domain") or "").lower()
    if root_domain:
        return "website", root_domain
    for field in DEDUP_LOCATOR_PRECEDENCE:
        locator_value = _normalize_locator_value(field, seed.get(field) or "")
        if locator_value:
            return field, locator_value
    return "", ""


def _seed_id(seed: dict[str, Any]) -> str:
    payload = "|".join(
        [
            str(seed.get("source_token") or ""),
            str(seed.get("seed_source") or ""),
            str(seed.get("state") or ""),
            str(seed.get("firm_key") or ""),
            str(seed.get("root_domain") or ""),
            str(seed.get("locator_type") or ""),
            str(seed.get("locator_value") or ""),
            str(seed.get("source_record_id") or ""),
            str(seed.get("license_number") or ""),
        ]
    )
    return f"seed_{hashlib.sha1(payload.encode('utf-8')).hexdigest()[:16]}"


def _row_city_and_state(row: dict[str, Any], *, fallback_state: str = "") -> tuple[str, str]:
    city = _normalize_text(row.get("city") or "")
    state = generation._normalize_us_state(str(row.get("state") or fallback_state or ""))
    if city:
        return city, state
    parsed_city, parsed_state = _parse_city_state_zip(row.get("business_city_state_zip") or "", state or fallback_state)
    return parsed_city, generation._normalize_us_state(parsed_state or state or fallback_state)


def _normalize_seed_row_generic(*, source_token: str, row: dict[str, Any]) -> dict[str, Any]:
    city, state = _row_city_and_state(row)
    seed_source = _normalize_text(row.get("source") or source_token) or source_token
    return {
        "firm": _normalize_text(row.get("firm") or row.get("company_name") or row.get("business_name") or ""),
        "website": _clean_seed_website(row.get("website") or ""),
        "state": state,
        "city": city,
        "phone": _normalize_text(row.get("phone") or row.get("business_telephone") or ""),
        "address": _normalize_text(row.get("address") or row.get("business_address_line1") or ""),
        "seed_source": seed_source,
        "seed_source_url": _derive_seed_source_url(row, seed_source),
        "source_record_id": _normalize_text(row.get("source_detail") or row.get("prospect_id") or ""),
        "license_number": _normalize_text(row.get("license_number") or ""),
        "source_token": source_token,
    }


def _normalize_seed_row_state_lic(*, source_token: str, row: dict[str, Any]) -> dict[str, Any]:
    annotated = prospect_sources_state_lic.annotate_state_lic_row(row)
    precision = state_lic_precision.classify_state_lic_row(
        {
            **row,
            **annotated,
            "firm": row.get("business_name") or row.get("company_name") or row.get("firm") or "",
            "source_record_id": row.get("source_detail") or row.get("prospect_id") or "",
            "seed_source_url": _derive_seed_source_url(row, _normalize_text(row.get("source") or source_token) or source_token),
            "phone": row.get("business_telephone") or row.get("phone") or "",
            "address": row.get("business_address_line1") or row.get("address") or "",
        },
        mode="packet_eligible",
    )
    city, state = _row_city_and_state(row, fallback_state=str(row.get("state") or ""))
    seed_source = _normalize_text(row.get("source") or source_token) or source_token
    return {
        "firm": _normalize_text(row.get("business_name") or row.get("company_name") or row.get("firm") or ""),
        "website": _clean_seed_website(row.get("website") or ""),
        "state": state,
        "city": city,
        "phone": _normalize_text(row.get("business_telephone") or row.get("phone") or ""),
        "address": _normalize_text(row.get("business_address_line1") or row.get("address") or ""),
        "seed_source": seed_source,
        "seed_source_url": _derive_seed_source_url(row, seed_source),
        "source_record_id": _normalize_text(row.get("source_detail") or row.get("prospect_id") or ""),
        "license_number": _normalize_text(row.get("license_number") or ""),
        "source_token": source_token,
        "state_lic_fit_status": _normalize_text(annotated.get("state_lic_fit_status") or ""),
        "state_lic_fit_score": int(annotated.get("state_lic_fit_score") or 0),
        "state_lic_fit_reasons": _normalize_text(annotated.get("state_lic_fit_reasons") or ""),
        "state_lic_consultant_eligible": bool(annotated.get("state_lic_consultant_eligible")),
        "state_lic_license_class_norm": _normalize_text(precision.get("state_lic_license_class_norm") or ""),
        "state_lic_hard_negative_class": _normalize_text(precision.get("state_lic_hard_negative_class") or ""),
        "state_lic_positive_families": list(precision.get("state_lic_positive_families") or []),
        "state_lic_negative_families": list(precision.get("state_lic_negative_families") or []),
        "state_lic_packet_eligible": bool(precision.get("state_lic_packet_eligible")),
        "state_lic_packet_exclusion_reason": _normalize_text(precision.get("state_lic_packet_exclusion_reason") or ""),
        "state_lic_send_eligible": bool(precision.get("state_lic_send_eligible")),
        "state_lic_strong_identity": bool(precision.get("state_lic_strong_identity")),
        "state_lic_strong_identity_anchor_count": int(precision.get("state_lic_strong_identity_anchor_count") or 0),
    }


def _normalize_seed_row(*, source_token: str, row: dict[str, Any]) -> dict[str, Any]:
    if source_token == "STATE_LIC":
        seed = _normalize_seed_row_state_lic(source_token=source_token, row=row)
    else:
        seed = _normalize_seed_row_generic(source_token=source_token, row=row)
    firm_key = _normalize_firm_key(str(seed.get("firm") or ""))
    root_domain = _root_domain(generation._domain_from_website(str(seed.get("website") or "")))
    locator_type, locator_value = _primary_locator({**seed, "root_domain": root_domain})
    seed.update(
        {
            "firm_key": firm_key,
            "root_domain": root_domain,
            "locator_type": locator_type,
            "locator_value": locator_value,
            "fit_sort_bucket": (0 if bool(seed.get("state_lic_consultant_eligible")) else 1) if source_token == "STATE_LIC" else 0,
            "fit_sort_score": int(seed.get("state_lic_fit_score") or 0) if source_token == "STATE_LIC" else 0,
        }
    )
    seed["seed_id"] = _seed_id(seed)
    return seed


def _identity_exclusion_key(*, seed: dict[str, Any], expected_state: str) -> str:
    expected_state_normalized = generation._normalize_us_state(str(expected_state or ""))
    state = str(seed.get("state") or "")
    if not str(seed.get("firm_key") or ""):
        return "excluded_bad_firm"
    if not state or (expected_state_normalized and state != expected_state_normalized):
        return "excluded_state_mismatch"
    return ""


def _normalize_state_lic_shadow_profile(value: str) -> str:
    normalized = _normalize_text(value).lower() or STATE_LIC_SHADOW_PROFILE_PRODUCTION
    if normalized not in STATE_LIC_SHADOW_PACKET_PROFILES:
        raise ValueError(f"invalid_state_lic_shadow_profile={value}")
    return normalized


def _state_lic_shadow_allows_contractor_family(*, seed: dict[str, Any], profile: str) -> bool:
    normalized_profile = _normalize_state_lic_shadow_profile(profile)
    if normalized_profile == STATE_LIC_SHADOW_PROFILE_PRODUCTION:
        return False
    if str(seed.get("source_token") or "") != "STATE_LIC":
        return False
    if str(seed.get("state_lic_packet_exclusion_reason") or "") != "negative_keyword_family":
        return False
    if _normalize_text(seed.get("state_lic_hard_negative_class") or ""):
        return False
    negative_families = {
        _normalize_text(family).lower()
        for family in list(seed.get("state_lic_negative_families") or [])
        if _normalize_text(family)
    }
    if negative_families != {"contractor"}:
        return False
    if normalized_profile == STATE_LIC_SHADOW_PROFILE_DEFAULT_CLASSES_ONLY:
        return (
            _normalize_text(seed.get("state_lic_license_class_norm") or "").lower()
            in STATE_LIC_SHADOW_DEFAULT_LICENSE_CLASSES
        )
    return normalized_profile == STATE_LIC_SHADOW_PROFILE_ALL_CONTRACTOR_ONLY


def _state_lic_packet_exclusion_reason_for_profile(*, seed: dict[str, Any], profile: str) -> str:
    packet_reason = str(seed.get("state_lic_packet_exclusion_reason") or "")
    if str(seed.get("source_token") or "") != "STATE_LIC":
        return packet_reason
    if not _state_lic_shadow_allows_contractor_family(seed=seed, profile=profile):
        return packet_reason
    has_positive = bool(list(seed.get("state_lic_positive_families") or []))
    if not _normalize_text(seed.get("website") or "") and not has_positive and not bool(seed.get("state_lic_strong_identity")):
        return "blank_website_no_positive_evidence"
    return ""


def _review_eligibility_exclusion_key(
    *,
    seed: dict[str, Any],
    source_token: str,
    state_lic_shadow_profile: str = STATE_LIC_SHADOW_PROFILE_PRODUCTION,
) -> str:
    if source_token == "STATE_LIC":
        packet_reason = _state_lic_packet_exclusion_reason_for_profile(
            seed=seed,
            profile=state_lic_shadow_profile,
        )
        if packet_reason == "hard_negative_class":
            return "excluded_state_lic_hard_negative_class"
        if packet_reason == "negative_keyword_family":
            return "excluded_state_lic_negative_keyword_family"
        if packet_reason == "blank_website_no_positive_evidence":
            return "excluded_state_lic_blank_website_no_positive_evidence"
        if packet_reason == "missing_firm":
            return "excluded_bad_firm"
        if packet_reason == "missing_state":
            return "excluded_state_mismatch"
        if not packet_reason:
            return ""
        for field in STATE_LIC_REVIEW_ANCHOR_FIELDS:
            if _normalize_text(seed.get(field) or ""):
                return ""
        return "excluded_missing_minimum_locator"
    if not str(seed.get("locator_value") or ""):
        return "excluded_missing_minimum_locator"
    return ""


def _state_lic_feedback_exclusion_key(seed: dict[str, Any], feedback_snapshot: dict[str, Any]) -> str:
    if str(seed.get("source_token") or "") != "STATE_LIC":
        return ""
    license_class_norm = str(seed.get("state_lic_license_class_norm") or "").strip()
    if license_class_norm and license_class_norm in set(feedback_snapshot.get("license_class_exclusions") or []):
        return "excluded_state_lic_feedback_license_class"
    families = sorted(
        set(
            list(seed.get("state_lic_positive_families") or [])
            + list(seed.get("state_lic_negative_families") or [])
        )
    )
    if any(family in set(feedback_snapshot.get("keyword_family_exclusions") or []) for family in families):
        return "excluded_state_lic_feedback_keyword_family"
    return ""


def _state_lic_feedback_sort_bucket(seed: dict[str, Any], feedback_snapshot: dict[str, Any]) -> int:
    if str(seed.get("source_token") or "") != "STATE_LIC":
        return 0
    license_class_norm = str(seed.get("state_lic_license_class_norm") or "").strip()
    if license_class_norm and license_class_norm in set(feedback_snapshot.get("license_class_downranks") or []):
        return 1
    families = sorted(
        set(
            list(seed.get("state_lic_positive_families") or [])
            + list(seed.get("state_lic_negative_families") or [])
        )
    )
    if any(family in set(feedback_snapshot.get("keyword_family_downranks") or []) for family in families):
        return 1
    return 0


def _crm_safety_exclusion_key(
    *,
    seed: dict[str, Any],
    crm_domains: set[str],
    crm_firm_keys: set[str],
) -> str:
    root_domain = str(seed.get("root_domain") or "")
    firm_key = str(seed.get("firm_key") or "")
    if (root_domain and root_domain in crm_domains) or firm_key in crm_firm_keys:
        return "excluded_already_in_crm"
    return ""


def _dedupe_pair(row: dict[str, Any]) -> tuple[str, str, str, str]:
    root_domain = str(row.get("root_domain") or "")
    if root_domain:
        return (
            str(row.get("firm_key") or ""),
            str(row.get("state") or ""),
            "website",
            root_domain,
        )
    return (
        str(row.get("firm_key") or ""),
        str(row.get("state") or ""),
        str(row.get("locator_type") or ""),
        str(row.get("locator_value") or ""),
    )


def _collect_candidates(
    *,
    data_dir: Path,
    states: list[str],
    source_tokens: list[str],
    crm_domains: set[str],
    crm_firm_keys: set[str],
    feedback_snapshot: dict[str, Any],
    state_lic_shadow_profile: str = STATE_LIC_SHADOW_PROFILE_PRODUCTION,
) -> dict[str, Any]:
    normalized_shadow_profile = _normalize_state_lic_shadow_profile(state_lic_shadow_profile)
    cache_root = data_dir / "prospect_generation" / "cache"
    review_candidates: list[dict[str, Any]] = []
    source_priority = {token: idx for idx, token in enumerate(source_tokens)}
    counters = _exclusion_counter_template()
    stage_counts_by_source: dict[str, Counter[str]] = {
        source_token: _stage_counter_template()
        for source_token in list(source_tokens or [])
    }
    exclusion_counts_by_source: dict[str, Counter[str]] = {
        source_token: _exclusion_counter_template()
        for source_token in list(source_tokens or [])
    }
    candidate_count_before_filters = 0
    observed_state_lic_fit_mismatch = 0
    state_lic_rows_scanned: list[dict[str, Any]] = []
    hard_negative_class_counts: Counter[str] = Counter()
    negative_keyword_family_counts: Counter[str] = Counter()
    feedback_suppression_counts: Counter[str] = Counter()
    for source_token in source_tokens:
        source_stage_counts = stage_counts_by_source.setdefault(source_token, _stage_counter_template())
        source_exclusion_counts = exclusion_counts_by_source.setdefault(source_token, _exclusion_counter_template())
        for state in states:
            cache_path = generation._source_cache_path_for_state(cache_root, source_token, state)
            for row in _load_cache_rows(cache_path):
                source_stage_counts["raw"] += 1
                candidate_count_before_filters += 1
                seed = _normalize_seed_row(source_token=source_token, row=row)
                if source_token == "STATE_LIC":
                    state_lic_rows_scanned.append(row)
                    if not bool(seed.get("state_lic_consultant_eligible")):
                        observed_state_lic_fit_mismatch += 1

                identity_exclusion_key = _identity_exclusion_key(seed=seed, expected_state=state)
                if identity_exclusion_key:
                    counters[identity_exclusion_key] += 1
                    source_exclusion_counts[identity_exclusion_key] += 1
                    continue
                source_stage_counts["identity_ready"] += 1

                review_exclusion_key = _review_eligibility_exclusion_key(
                    seed=seed,
                    source_token=source_token,
                    state_lic_shadow_profile=normalized_shadow_profile,
                )
                if review_exclusion_key:
                    counters[review_exclusion_key] += 1
                    source_exclusion_counts[review_exclusion_key] += 1
                    if review_exclusion_key == "excluded_state_lic_hard_negative_class":
                        hard_negative_class = str(seed.get("state_lic_hard_negative_class") or "unknown")
                        hard_negative_class_counts[hard_negative_class] += 1
                    elif review_exclusion_key == "excluded_state_lic_negative_keyword_family":
                        for family in list(seed.get("state_lic_negative_families") or []):
                            negative_keyword_family_counts[str(family or "")] += 1
                    continue

                feedback_exclusion_key = _state_lic_feedback_exclusion_key(seed, feedback_snapshot)
                if feedback_exclusion_key:
                    counters[feedback_exclusion_key] += 1
                    source_exclusion_counts[feedback_exclusion_key] += 1
                    if feedback_exclusion_key == "excluded_state_lic_feedback_license_class":
                        feedback_suppression_counts[f"license_class:{str(seed.get('state_lic_license_class_norm') or 'unknown')}"] += 1
                    else:
                        for family in sorted(
                            set(
                                list(seed.get("state_lic_positive_families") or [])
                                + list(seed.get("state_lic_negative_families") or [])
                            )
                        ):
                            feedback_suppression_counts[f"keyword_family:{str(family or '')}"] += 1
                    continue
                source_stage_counts["review_eligible"] += 1

                safety_exclusion_key = _crm_safety_exclusion_key(
                    seed=seed,
                    crm_domains=crm_domains,
                    crm_firm_keys=crm_firm_keys,
                )
                if safety_exclusion_key:
                    counters[safety_exclusion_key] += 1
                    source_exclusion_counts[safety_exclusion_key] += 1
                    continue
                source_stage_counts["safety_passed"] += 1
                seed["feedback_sort_bucket"] = _state_lic_feedback_sort_bucket(seed, feedback_snapshot)
                review_candidates.append(seed)

    ordered = sorted(
        review_candidates,
        key=lambda row: (
            source_priority.get(str(row.get("source_token") or ""), 9999),
            int(row.get("feedback_sort_bucket") or 0),
            int(row.get("fit_sort_bucket") or 0),
            -int(row.get("fit_sort_score") or 0),
            str(row.get("firm_key") or ""),
            str(row.get("state") or ""),
            str(row.get("root_domain") or ""),
            str(row.get("locator_type") or ""),
            str(row.get("locator_value") or ""),
            str(row.get("seed_source") or ""),
            str(row.get("source_record_id") or ""),
            str(row.get("website") or ""),
        ),
    )
    deduped: list[dict[str, Any]] = []
    seen_pairs: set[tuple[str, str, str, str]] = set()
    for row in ordered:
        pair = _dedupe_pair(row)
        source_token = str(row.get("source_token") or "")
        if pair in seen_pairs:
            counters["excluded_duplicate_seed"] += 1
            exclusion_counts_by_source.setdefault(source_token, _exclusion_counter_template())["excluded_duplicate_seed"] += 1
            continue
        seen_pairs.add(pair)
        deduped.append(row)
        stage_counts_by_source.setdefault(source_token, _stage_counter_template())["candidates"] += 1

    source_breakdown = Counter(
        str(row.get("source_token") or "")
        for row in deduped
        if str(row.get("source_token") or "")
    )
    identity_ready_count = sum(
        int((stage_counts_by_source.get(source_token) or _stage_counter_template()).get("identity_ready", 0))
        for source_token in list(source_tokens or [])
    )
    review_eligible_count = sum(
        int((stage_counts_by_source.get(source_token) or _stage_counter_template()).get("review_eligible", 0))
        for source_token in list(source_tokens or [])
    )
    safety_passed_count = sum(
        int((stage_counts_by_source.get(source_token) or _stage_counter_template()).get("safety_passed", 0))
        for source_token in list(source_tokens or [])
    )
    return {
        "candidates": deduped,
        "candidate_count_before_filters": int(candidate_count_before_filters),
        "candidate_count_after_filters": len(deduped),
        "identity_ready_count": int(identity_ready_count),
        "review_eligible_count": int(review_eligible_count),
        "safety_passed_count": int(safety_passed_count),
        "excluded_missing_minimum_locator": int(counters["excluded_missing_minimum_locator"]),
        "excluded_already_in_crm": int(counters["excluded_already_in_crm"]),
        "excluded_bad_firm": int(counters["excluded_bad_firm"]),
        "excluded_state_mismatch": int(counters["excluded_state_mismatch"]),
        "excluded_state_lic_fit_mismatch": int(counters["excluded_state_lic_fit_mismatch"]),
        "excluded_state_lic_hard_negative_class": int(counters["excluded_state_lic_hard_negative_class"]),
        "excluded_state_lic_negative_keyword_family": int(counters["excluded_state_lic_negative_keyword_family"]),
        "excluded_state_lic_blank_website_no_positive_evidence": int(
            counters["excluded_state_lic_blank_website_no_positive_evidence"]
        ),
        "excluded_state_lic_feedback_license_class": int(counters["excluded_state_lic_feedback_license_class"]),
        "excluded_state_lic_feedback_keyword_family": int(counters["excluded_state_lic_feedback_keyword_family"]),
        "excluded_duplicate_seed": int(counters["excluded_duplicate_seed"]),
        "source_breakdown": {key: int(source_breakdown[key]) for key in sorted(source_breakdown.keys())},
        "source_raw_breakdown": {
            source_token: int((stage_counts_by_source.get(source_token) or _stage_counter_template()).get("raw", 0))
            for source_token in list(source_tokens or [])
        },
        "source_review_eligible_breakdown": {
            source_token: int((stage_counts_by_source.get(source_token) or _stage_counter_template()).get("review_eligible", 0))
            for source_token in list(source_tokens or [])
        },
        "stage_counts_by_source": _ordered_stage_counts_by_source(stage_counts_by_source, source_tokens),
        "exclusion_counts_by_reason": _ordered_nonzero_counter(counters),
        "exclusion_counts_by_source": _ordered_exclusion_counts_by_source(exclusion_counts_by_source, source_tokens),
        "exclusion_counts_by_source_and_reason": _ordered_exclusion_counts_by_source_and_reason(
            exclusion_counts_by_source,
            source_tokens,
        ),
        "top_exclusion_reasons": _top_exclusion_reasons(counters),
        "observed_state_lic_fit_mismatch": int(observed_state_lic_fit_mismatch),
        "state_lic_license_type_breakdown": prospect_sources_state_lic.summarize_state_lic_license_types(state_lic_rows_scanned),
        "state_lic_hard_negative_class_breakdown": _ordered_nonzero_counter(hard_negative_class_counts),
        "state_lic_negative_keyword_family_breakdown": _ordered_nonzero_counter(negative_keyword_family_counts),
        "state_lic_feedback_suppression_breakdown": _ordered_nonzero_counter(feedback_suppression_counts),
        "state_lic_blank_website_no_positive_evidence_count": int(
            counters["excluded_state_lic_blank_website_no_positive_evidence"]
        ),
        "state_lic_feedback_snapshot": feedback_snapshot,
    }


def _state_lic_shadow_packet_profiles(
    *,
    data_dir: Path,
    states: list[str],
    source_tokens: list[str],
    crm_domains: set[str],
    crm_firm_keys: set[str],
    feedback_snapshot: dict[str, Any],
) -> dict[str, Any]:
    profiles: dict[str, Any] = {"measured_stage": "candidates"}
    for profile in STATE_LIC_SHADOW_PACKET_PROFILES:
        by_state: dict[str, int] = {}
        total = 0
        for state in list(states or []):
            diagnostics = _collect_candidates(
                data_dir=data_dir,
                states=[state],
                source_tokens=source_tokens,
                crm_domains=crm_domains,
                crm_firm_keys=crm_firm_keys,
                feedback_snapshot=feedback_snapshot,
                state_lic_shadow_profile=profile,
            )
            stage_counts = dict(diagnostics.get("stage_counts_by_source") or {})
            count = int((stage_counts.get("STATE_LIC") or {}).get("candidates", 0) or 0)
            by_state[state] = count
            total += count
        profiles[profile] = {
            "total": int(total),
            "by_state": {state: int(by_state.get(state, 0)) for state in list(states or [])},
        }
    return profiles


def _csv_block(*, fieldnames: tuple[str, ...], rows: list[dict[str, Any]]) -> str:
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=list(fieldnames), lineterminator="\n")
    writer.writeheader()
    for row in rows:
        writer.writerow({field: str(row.get(field) or "") for field in fieldnames})
    return buffer.getvalue().strip()


def _seed_rows(selected_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [{field: str(row.get(field) or "") for field in SEED_COLUMNS} for row in selected_rows]


def _seed_index_rows(selected_rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in list(selected_rows or []):
        seed_id = str(row.get("seed_id") or "").strip()
        if not seed_id:
            continue
        out[seed_id] = {
            "seed_source_token": str(row.get("source_token") or ""),
            "seed_source": str(row.get("seed_source") or ""),
            "seed_source_url": str(row.get("seed_source_url") or ""),
            "source_record_id": str(row.get("source_record_id") or ""),
            "license_number": str(row.get("license_number") or ""),
            "state_lic_license_class_norm": str(row.get("state_lic_license_class_norm") or ""),
            "state_lic_hard_negative_class": str(row.get("state_lic_hard_negative_class") or ""),
            "state_lic_positive_families": list(row.get("state_lic_positive_families") or []),
            "state_lic_negative_families": list(row.get("state_lic_negative_families") or []),
            "state_lic_packet_exclusion_reason": str(row.get("state_lic_packet_exclusion_reason") or ""),
        }
    return out


def _select_rows_with_state_lic_cap(
    *,
    candidate_rows: list[dict[str, Any]],
    raw_target: int,
    feedback_snapshot: dict[str, Any],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    non_state_rows = [row for row in list(candidate_rows or []) if str(row.get("source_token") or "") != "STATE_LIC"]
    state_lic_rows = [row for row in list(candidate_rows or []) if str(row.get("source_token") or "") == "STATE_LIC"]
    selected_non_state = non_state_rows[: max(0, int(raw_target))]
    remaining_target = max(0, int(raw_target) - len(selected_non_state))
    cap_percent = int(feedback_snapshot.get("state_lic_cap_percent") or STATE_LIC_DEFAULT_PACKET_CAP_PERCENT)
    max_state_lic_rows = _state_lic_max_rows_for_cap(len(selected_non_state), cap_percent)
    allowed_state_lic_rows = min(len(state_lic_rows), remaining_target, max_state_lic_rows)
    selected_state_lic = state_lic_rows[:allowed_state_lic_rows]
    selected_rows = list(selected_non_state) + list(selected_state_lic)
    state_lic_cap_limited_count = 0
    if remaining_target > 0:
        state_lic_cap_limited_count = max(0, min(len(state_lic_rows), remaining_target) - allowed_state_lic_rows)
    return selected_rows, {
        "state_lic_cap_percent": int(cap_percent),
        "state_lic_cap_reason": str(feedback_snapshot.get("state_lic_cap_reason") or "default"),
        "state_lic_candidate_count": len(state_lic_rows),
        "non_state_candidate_count": len(non_state_rows),
        "state_lic_selected_count": len(selected_state_lic),
        "state_lic_cap_limited_count": int(state_lic_cap_limited_count),
    }


def _packet_status_text(
    *,
    packet_count: int,
    selected_row_count: int,
    included_without_website: int,
    diagnostics: dict[str, Any],
) -> str:
    top_exclusions = diagnostics.get("top_exclusion_reasons") or []
    top_exclusion_text = "none"
    if top_exclusions:
        top_exclusion_text = " | ".join(
            f"{str(item.get('reason') or '')}={int(item.get('count') or 0)}"
            for item in list(top_exclusions)
            if str(item.get("reason") or "")
        )
    if packet_count > 0:
        return (
            f"PACKETS READY: {packet_count}\n"
            f"SELECTED ROWS: {selected_row_count}\n"
            f"ROWS WITH BLANK WEBSITE: {included_without_website}\n"
            f"TOP EXCLUSIONS: {top_exclusion_text}\n"
        )
    exclusion_lines: list[str] = []
    for key, value in sorted(
        ((key, int(diagnostics.get(key) or 0)) for key in EXCLUSION_KEYS),
        key=lambda item: (-item[1], item[0]),
    ):
        exclusion_lines.append(f"{key}={value}")
    return "NO PACKETS TODAY\n" + "\n".join(exclusion_lines[:5]) + "\n"


def _build_prompt_text(
    *,
    run_date: date,
    backlog_target: int,
    raw_target: int,
    source_tokens: list[str],
    gap_rows: list[dict[str, int | str]],
    selected_rows: list[dict[str, Any]],
    packet_number: int | None = None,
    packet_count: int = 0,
    packet_size: int = 0,
    reviewed_filename: str = "",
    suggested_batch_id: str = "",
    reviewed_drop_dir: Path | None = None,
) -> str:
    lines = [
        "# ============================================================",
        "# OSHA_LEADS - MANUAL AI-ASSIST DISCOVERY AUGMENTATION",
        "# ============================================================",
        "#",
        "# PURPOSE:",
        "# This is a controlled discovery augmentation lane for thin-state",
        "# consultant replenishment. It is not a sending workflow and it",
        "# does not bypass the repo's canonical discovery -> CRM path.",
        "#",
        "# WHEN TO USE:",
        "# Normal AIHA/OHS_BG replenishment and discovery already ran, but",
        "# one or more states are still below the backlog target.",
        "#",
        "# TARGET ICP:",
        "# Business contacts only for safety consultants and boutique",
        "# OSHA-facing firms. Prefer owner, founder, principal, partner,",
        "# president, or managing consultant roles at firms that actively",
        "# sell OSHA/safety consulting services.",
        "#",
        "# RULES:",
        "# - Business contacts only. No personal emails, no sensitive data.",
        "# - No outreach copy, cadence, score, or send-rule changes.",
        "# - Use the seed candidates below as the canonical research queue.",
        "# - website may be blank for valid seed rows, especially STATE_LIC.",
        "# - Prefer website review when present, but do not require it.",
        "# - Use city/phone/address/license/source URL context to identify the business.",
        "# - Return only rows you are confident are real, business-relevant",
        "#   consultant prospects for the listed state.",
        "# - Return reject if no named principal/contact can be verified.",
        "# - Do not invent websites or emails.",
        "# - Use business email addresses tied to the firm domain.",
        "# - Return standard CSV only.",
        "# - Use source_urls with | between multiple URLs in one field.",
        "# - Quote any field that contains a comma.",
        "# - Escape embedded double quotes by doubling them.",
        "# - Use plain text only. No markdown links, no mailto links, no",
        "#   code fences, no surrounding brackets, and no commentary.",
        "# - confidence must be an integer 0-100.",
        "# - evidence_snippet must be short, factual provenance.",
        "# - Preserve seed_id exactly when returning reviewed CSV rows.",
        "# - Return ONLY the CSV block. No commentary before or after.",
        "#",
        "# OUTPUT CSV HEADER:",
        "# state,decision,firm,website,contact_name,title,email,source_urls,confidence,evidence_snippet,seed_id",
        "# Use decision=accept for rows to import and decision=reject for rows to keep visible but blocked.",
        "#",
        '# VALID ACCEPT EXAMPLE:',
        '# TX,accept,"Safety Compliance Management, Inc.",https://www.scm-safety.com,Paul Gantt,President and Founder,info@scm-safety.com,https://www.scm-safety.com/team/paul-gantt-csp-chst-cet/|https://www.scm-safety.com,95,"President and Founder; San Ramon, CA; info@scm-safety.com on site"',
        "# VALID REJECT EXAMPLE:",
        "# TX,reject,Example Safety Group,https://example-safety.com,Alex Example,Owner,alex@example-safety.com,https://example-safety.com/about,35,Role or state fit is uncertain; keep blocked for manual review",
        "# INVALID EXAMPLE - DO NOT RETURN ANYTHING LIKE THIS:",
        '# TX,accept,Example Safety Group,[https://example-safety.com/,"Alex](https://example-safety.com/%22,%22Alex) Example",Owner,[alex@example-safety.com](mailto:alex@example-safety.com),[https://example-safety.com/about|https://example-safety.com/contact](https://example-safety.com/about|https://example-safety.com/contact),95,Owner listed on site',
        "#",
        f"# RUN DATE: {run_date.isoformat()}",
        f"# BACKLOG TARGET: {backlog_target}",
        f"# RAW TARGET: {raw_target}",
        f"# SOURCES: {','.join(source_tokens) or 'none'}",
        f"# PACKET SIZE: {packet_size if packet_size > 0 else len(selected_rows)}",
        "#",
        "# GAP STATES:",
    ]
    if gap_rows:
        for row in gap_rows:
            lines.append(
                "# - "
                f"{row['state']}: backlog_current={int(row['backlog_current'] or 0)} "
                f"crm_total={int(row['crm_total'] or 0)} gap={int(row['gap'] or 0)}"
            )
    else:
        lines.append("# - none")

    if packet_number is not None and packet_count > 0:
        lines.extend(
            [
                "#",
                f"# PACKET: {packet_number:03d}/{packet_count:03d}",
                f"# PACKET ROWS: {len(selected_rows)}",
            ]
        )
        if reviewed_filename:
            lines.append(f"# REVIEWED IMPORT FILENAME: {reviewed_filename}")
        if suggested_batch_id:
            lines.append(f"# SUGGESTED_BATCH_ID: {suggested_batch_id}")
        if reviewed_drop_dir is not None:
            lines.append(f"# DROP REVIEWED CSV IN: {reviewed_drop_dir}")

    lines.extend(
        [
            "#",
            "# SEED CANDIDATES CSV:",
            _csv_block(fieldnames=SEED_COLUMNS, rows=_seed_rows(selected_rows)),
            "#",
            "# RETURN CSV NOW:",
            ",".join(REVIEW_COLUMNS),
        ]
    )
    return "\n".join(lines).rstrip() + "\n"


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="Emit a nightly AI-assist review dump from existing autogrow cache rows.")
    ap.add_argument("--for-date", default="", help="Optional YYYY-MM-DD date override.")
    ap.add_argument("--states", nargs="+", default=[], help="Optional explicit state scope (comma-separated or list form).")
    ap.add_argument("--raw-target", type=int, default=0, help="Optional raw seed target override.")
    ap.add_argument("--packet-size", type=int, default=0, help="Optional per-packet review size override.")
    ap.add_argument("--print-config", action="store_true", help="Print resolved config and exit.")
    ap.add_argument("--dry-run", action="store_true", help="Do not write output.")
    ap.add_argument("--output-dir", default="", help="Optional dump output directory override.")
    ap.add_argument("--output", default="", help="Optional full dump path override.")
    return ap


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        run_date = _parse_date(args.for_date)
        run_started_at = _current_run_started_at()
        state_scope = _resolve_state_scope(list(args.states or []))
        raw_target = int(args.raw_target or _int_env("PROSPECT_AI_ASSIST_REVIEW_RAW_TARGET", AI_ASSIST_DUMP_DEFAULT_RAW_TARGET))
        packet_size = int(args.packet_size or _int_env("PROSPECT_AI_ASSIST_REVIEW_PACKET_SIZE", AI_ASSIST_DUMP_DEFAULT_PACKET_SIZE))
        if raw_target < 1:
            raise ValueError("raw_target_invalid")
        if packet_size < 1:
            raise ValueError("packet_size_invalid")
    except Exception as exc:
        print(f"{ERR_AI_ASSIST_DUMP_CONFIG} detail={exc}", file=sys.stderr)
        return 2

    states = generation._states_for_selection(state_scope)
    enabled = _bool_env("PROSPECT_AI_ASSIST_REVIEW_ENABLED", AI_ASSIST_DUMP_DEFAULT_ENABLED)
    backlog_target = _int_env("PROSPECT_AUTOGROW_BACKLOG_TARGET", AI_ASSIST_DUMP_DEFAULT_BACKLOG_TARGET)
    source_tokens, source_warning_configured = _resolve_source_tokens()
    run_token = _batch_run_token(run_started_at)
    out_dir, out_path = _resolve_output_path(
        output=str(args.output or ""),
        output_dir=str(args.output_dir or ""),
        for_date=run_date,
    )
    packet_dir = _packet_dir_for_output_path(out_path)
    manifest_path = _packet_manifest_path(packet_dir)
    data_dir_resolution = resolve_data_dir(REPO_ROOT)
    reviewed_drop_dir = ai_assist_paths.prospect_import_dir(repo_root=REPO_ROOT)

    conn: sqlite3.Connection | None = None
    db_path = crm_store.crm_db_path()
    if db_path.exists():
        conn = crm_store.connect(db_path)
    try:
        suppressed_emails = generation._load_suppression_set(data_dir_resolution.effective_path, conn)
        gap_rows = _state_gap_snapshot(
            conn,
            states=states,
            suppressed_emails=suppressed_emails,
            backlog_target=backlog_target,
        )
        crm_domains = _normalized_root_domains(generation._existing_crm_domains(conn))
        crm_firm_keys = _existing_crm_firm_keys(conn)
        feedback_snapshot = _state_lic_feedback_snapshot(conn)
    finally:
        if conn is not None:
            conn.close()

    candidates = _collect_candidates(
        data_dir=data_dir_resolution.effective_path,
        states=states,
        source_tokens=source_tokens,
        crm_domains=crm_domains,
        crm_firm_keys=crm_firm_keys,
        feedback_snapshot=feedback_snapshot,
    )
    candidate_rows = list(candidates.get("candidates") or [])
    selected_rows, cap_diagnostics = _select_rows_with_state_lic_cap(
        candidate_rows=candidate_rows,
        raw_target=raw_target,
        feedback_snapshot=feedback_snapshot,
    )
    stage_counts_by_source = {
        str(source_token): {
            key: int(value)
            for key, value in dict((candidates.get("stage_counts_by_source") or {}).get(source_token) or {}).items()
        }
        for source_token in list(source_tokens or [])
    }
    for source_token in list(source_tokens or []):
        stage_counts_by_source.setdefault(str(source_token), {})
        for key in DIAGNOSTIC_STAGE_KEYS:
            stage_counts_by_source[str(source_token)].setdefault(key, 0)
    for row in selected_rows:
        source_token = str(row.get("source_token") or "")
        if source_token:
            stage_counts_by_source.setdefault(source_token, {key: 0 for key in DIAGNOSTIC_STAGE_KEYS})
            stage_counts_by_source[source_token]["selected"] += 1

    packets = _chunk_rows(selected_rows, packet_size) if selected_rows else []
    included_without_website = sum(1 for row in selected_rows if not str(row.get("website") or "").strip())
    selected_source_breakdown = _source_selection_breakdown(selected_rows, source_tokens)
    prompt_text = _build_prompt_text(
        run_date=run_date,
        backlog_target=backlog_target,
        raw_target=raw_target,
        source_tokens=source_tokens,
        gap_rows=gap_rows,
        selected_rows=selected_rows,
        packet_count=len(packets),
        packet_size=packet_size,
    )
    gap_total = sum(int(row["gap"] or 0) for row in gap_rows)
    shortfall = max(0, raw_target - len(selected_rows))
    gap_states_csv = ",".join(str(row["state"] or "") for row in gap_rows)

    if data_dir_resolution.warning_token:
        print(data_dir_resolution.warning_token)
    scope_drift_warning = generation._autogrow_scope_drift_warning_token(prefix="WARN_AI_ASSIST_DUMP_SCOPE_DRIFT")
    if scope_drift_warning:
        print(scope_drift_warning)
    _emit("AI_ASSIST_DUMP_ENABLED", enabled)
    _emit("AI_ASSIST_DUMP_DATA_DIR", str(data_dir_resolution.effective_path))
    _emit("AI_ASSIST_DUMP_DATA_DIR_SOURCE", str(data_dir_resolution.source or "default"))
    _emit("AI_ASSIST_DUMP_FOR_DATE", run_date.isoformat())
    _emit("AI_ASSIST_DUMP_RUN_STARTED_AT", run_started_at.isoformat())
    _emit("AI_ASSIST_DUMP_RUN_TOKEN", run_token)
    _emit("AI_ASSIST_DUMP_STATES_SCOPE", ",".join(states))
    _emit("AI_ASSIST_DUMP_BACKLOG_TARGET", backlog_target)
    _emit("AI_ASSIST_DUMP_OUTPUT_DIR", str(out_dir))
    _emit("AI_ASSIST_DUMP_OUTPUT_PATH", str(out_path))
    _emit("AI_ASSIST_DUMP_PACKET_SIZE", packet_size)
    _emit("AI_ASSIST_DUMP_PACKET_COUNT", len(packets))
    _emit("AI_ASSIST_PACKET_DIR", str(packet_dir))
    _emit("AI_ASSIST_PACKET_MANIFEST_PATH", str(manifest_path))
    _emit("AI_ASSIST_DUMP_GAP_STATES", gap_states_csv or "none")
    _emit("AI_ASSIST_DUMP_GAP_TOTAL", gap_total)
    _emit("AI_ASSIST_DUMP_CANDIDATES_REQUESTED_TOTAL", raw_target)
    _emit("AI_ASSIST_DUMP_SOURCES", ",".join(source_tokens) or "none")
    _emit("AI_ASSIST_DUMP_RAW_TARGET", raw_target)
    _emit("AI_ASSIST_DUMP_RAW_INVENTORY_TOTAL", int(candidates.get("candidate_count_before_filters") or 0))
    _emit("AI_ASSIST_DUMP_IDENTITY_READY_TOTAL", int(candidates.get("identity_ready_count") or 0))
    _emit("AI_ASSIST_DUMP_REVIEW_ELIGIBLE_TOTAL", int(candidates.get("review_eligible_count") or 0))
    _emit("AI_ASSIST_DUMP_SAFETY_PASSED_TOTAL", int(candidates.get("safety_passed_count") or 0))
    _emit("AI_ASSIST_DUMP_CANDIDATES_TOTAL", int(candidates.get("candidate_count_after_filters") or 0))
    _emit("AI_ASSIST_DUMP_ROWS_WRITTEN", len(selected_rows))
    _emit("AI_ASSIST_DUMP_SHORTFALL", shortfall)
    _emit("AI_ASSIST_DUMP_OBSERVED_STATE_LIC_FIT_MISMATCH", int(candidates.get("observed_state_lic_fit_mismatch") or 0))
    _emit("AI_ASSIST_DUMP_STATE_LIC_CAP_PERCENT", int(cap_diagnostics.get("state_lic_cap_percent") or 0))
    _emit("AI_ASSIST_DUMP_STATE_LIC_CAP_LIMITED_COUNT", int(cap_diagnostics.get("state_lic_cap_limited_count") or 0))
    _emit(
        "AI_ASSIST_DUMP_STATE_LIC_ACCEPT_RATE_7D_REVIEWED",
        int((feedback_snapshot.get("state_lic_overall") or {}).get("reviewed", 0)),
    )
    _emit(
        "AI_ASSIST_DUMP_STATE_LIC_ACCEPT_RATE_7D_ACCEPTED",
        int((feedback_snapshot.get("state_lic_overall") or {}).get("accepted", 0)),
    )
    _emit(
        "AI_ASSIST_DUMP_STATE_LIC_ACCEPT_RATE_7D_BPS",
        int(round(float((feedback_snapshot.get("state_lic_overall") or {}).get("accept_rate", 0.0)) * 10000)),
    )

    for row in gap_rows:
        state = str(row["state"] or "")
        _emit(f"AI_ASSIST_DUMP_STATE_{state}_BACKLOG_CURRENT", int(row["backlog_current"] or 0))
        _emit(f"AI_ASSIST_DUMP_STATE_{state}_CRM_TOTAL", int(row["crm_total"] or 0))
        _emit(f"AI_ASSIST_DUMP_STATE_{state}_GAP", int(row["gap"] or 0))

    for source_token in list(source_tokens or []):
        source_stage_counts = dict(stage_counts_by_source.get(str(source_token)) or {})
        _emit(f"AI_ASSIST_DUMP_SOURCE_{source_token}_RAW", int(source_stage_counts.get("raw", 0)))
        _emit(f"AI_ASSIST_DUMP_SOURCE_{source_token}_IDENTITY_READY", int(source_stage_counts.get("identity_ready", 0)))
        _emit(f"AI_ASSIST_DUMP_SOURCE_{source_token}_REVIEW_ELIGIBLE", int(source_stage_counts.get("review_eligible", 0)))
        _emit(f"AI_ASSIST_DUMP_SOURCE_{source_token}_SAFETY_PASSED", int(source_stage_counts.get("safety_passed", 0)))
        _emit(f"AI_ASSIST_DUMP_SOURCE_{source_token}_CANDIDATES", int(source_stage_counts.get("candidates", 0)))
        _emit(f"AI_ASSIST_DUMP_SOURCE_{source_token}_SELECTED", int(source_stage_counts.get("selected", 0)))
        for reason in EXCLUSION_KEYS:
            source_reason_counts = dict((candidates.get("exclusion_counts_by_source_and_reason") or {}).get(source_token) or {})
            _emit(
                f"AI_ASSIST_DUMP_SOURCE_{source_token}_{_reason_token_stdout_suffix(reason)}",
                int(source_reason_counts.get(reason, 0)),
            )

    for class_name, count in dict(candidates.get("state_lic_hard_negative_class_breakdown") or {}).items():
        _emit(f"AI_ASSIST_DUMP_STATE_LIC_HARD_NEGATIVE_{_reason_token_stdout_suffix(class_name)}", int(count))
    for family, count in dict(candidates.get("state_lic_negative_keyword_family_breakdown") or {}).items():
        _emit(f"AI_ASSIST_DUMP_STATE_LIC_NEGATIVE_FAMILY_{_reason_token_stdout_suffix(family)}", int(count))
    for label, count in dict(candidates.get("state_lic_feedback_suppression_breakdown") or {}).items():
        _emit(f"AI_ASSIST_DUMP_STATE_LIC_FEEDBACK_SUPPRESSION_{_reason_token_stdout_suffix(label)}", int(count))

    raw_inventory_total = int(candidates.get("candidate_count_before_filters") or 0)
    identity_ready_total = int(candidates.get("identity_ready_count") or 0)
    review_eligible_total = int(candidates.get("review_eligible_count") or 0)
    candidate_total = int(candidates.get("candidate_count_after_filters") or 0)
    if shortfall > 0:
        print(
            f"WARN_AI_ASSIST_DUMP_SHORTFALL=1 requested={raw_target} "
            f"available={int(candidates.get('candidate_count_after_filters') or 0)} shortfall={shortfall}"
        )
    if source_tokens and raw_inventory_total == 0:
        print(f"WARN_AI_ASSIST_DUMP_NO_RAW_INVENTORY=1 sources={','.join(source_tokens)}")
    elif raw_inventory_total > 0 and identity_ready_total == 0:
        print(
            f"WARN_AI_ASSIST_DUMP_NORMALIZATION_STARVATION=1 raw={raw_inventory_total} "
            f"identity_ready={identity_ready_total}"
        )
    elif identity_ready_total > 0 and review_eligible_total == 0:
        print(
            f"WARN_AI_ASSIST_DUMP_REVIEW_FIT_STARVATION=1 identity_ready={identity_ready_total} "
            f"review_eligible={review_eligible_total}"
        )
    elif review_eligible_total > 0 and candidate_total == 0:
        print(
            f"WARN_AI_ASSIST_DUMP_SAFETY_FILTER_STARVATION=1 review_eligible={review_eligible_total} "
            f"safety_passed={int(candidates.get('safety_passed_count') or 0)} candidates={candidate_total}"
        )
    if int(cap_diagnostics.get("state_lic_cap_limited_count") or 0) > 0:
        print(
            f"WARN_AI_ASSIST_DUMP_STATE_LIC_CAP_UNDERFILL=1 cap_percent={int(cap_diagnostics.get('state_lic_cap_percent') or 0)} "
            f"raw_target={raw_target} selected={len(selected_rows)} cap_limited={int(cap_diagnostics.get('state_lic_cap_limited_count') or 0)}"
        )
    if source_warning_configured:
        print(f"WARN_AI_ASSIST_DUMP_NO_ELIGIBLE_SOURCES=1 configured={source_warning_configured}")

    if args.print_config:
        return 0

    if enabled != 1:
        _emit("AI_ASSIST_DUMP_SKIPPED", "1 reason=disabled")
        return 0

    if not gap_rows:
        _emit("AI_ASSIST_DUMP_SKIPPED", "1 reason=no_gap")
        return 0

    if prompt_text:
        print(prompt_text, end="")

    if args.dry_run:
        return 0

    _atomic_write_text(out_path, prompt_text)
    _reset_output_dir(packet_dir)
    manifest_packets: list[dict[str, Any]] = []
    seed_index_payload = {"seeds": _seed_index_rows(selected_rows)}
    for packet_number, packet_rows in enumerate(packets, start=1):
        seed_csv_path = (packet_dir / _packet_seed_filename(packet_number)).resolve(strict=False)
        prompt_packet_path = (packet_dir / _packet_prompt_filename(packet_number)).resolve(strict=False)
        reviewed_filename = _packet_review_filename(run_date, packet_number)
        suggested_batch_id = _packet_batch_id(run_date, packet_number)
        _atomic_write_text(seed_csv_path, _csv_block(fieldnames=SEED_COLUMNS, rows=_seed_rows(packet_rows)).rstrip() + "\n")
        _atomic_write_text(
            prompt_packet_path,
            _build_prompt_text(
                run_date=run_date,
                backlog_target=backlog_target,
                raw_target=raw_target,
                source_tokens=source_tokens,
                gap_rows=gap_rows,
                selected_rows=packet_rows,
                packet_number=packet_number,
                packet_count=len(packets),
                packet_size=packet_size,
                reviewed_filename=reviewed_filename,
                suggested_batch_id=suggested_batch_id,
                reviewed_drop_dir=reviewed_drop_dir,
            ),
        )
        manifest_packets.append(
            {
                "packet_number": packet_number,
                "row_count": len(packet_rows),
                "seed_csv_path": str(seed_csv_path),
                "review_prompt_path": str(prompt_packet_path),
                "reviewed_import_filename": reviewed_filename,
                "reviewed_import_path": str((reviewed_drop_dir / reviewed_filename).resolve(strict=False)),
                "suggested_batch_id": suggested_batch_id,
            }
        )
    _atomic_write_text(
        _packet_status_path(packet_dir),
        _packet_status_text(
            packet_count=len(packets),
            selected_row_count=len(selected_rows),
            included_without_website=included_without_website,
            diagnostics=candidates,
        ),
    )
    _atomic_write_text(_packet_seed_index_path(packet_dir), json.dumps(seed_index_payload, indent=2) + "\n")
    manifest_payload = {
        "schema_version": AI_ASSIST_PACKET_MANIFEST_SCHEMA,
        "run_date": run_date.isoformat(),
        "run_started_at": run_started_at.isoformat(),
        "run_token": run_token,
        "output_path": str(out_path),
        "packet_dir": str(packet_dir),
        "packet_size": packet_size,
        "packet_count": len(packets),
        "raw_target": raw_target,
        "selected_row_count": len(selected_rows),
        "candidate_count": int(candidates.get("candidate_count_after_filters") or 0),
        "candidate_count_before_filters": int(candidates.get("candidate_count_before_filters") or 0),
        "candidate_count_after_filters": int(candidates.get("candidate_count_after_filters") or 0),
        "identity_ready_count": int(candidates.get("identity_ready_count") or 0),
        "review_eligible_count": int(candidates.get("review_eligible_count") or 0),
        "safety_passed_count": int(candidates.get("safety_passed_count") or 0),
        "excluded_missing_minimum_locator": int(candidates.get("excluded_missing_minimum_locator") or 0),
        "excluded_already_in_crm": int(candidates.get("excluded_already_in_crm") or 0),
        "excluded_bad_firm": int(candidates.get("excluded_bad_firm") or 0),
        "excluded_state_mismatch": int(candidates.get("excluded_state_mismatch") or 0),
        "excluded_state_lic_fit_mismatch": int(candidates.get("excluded_state_lic_fit_mismatch") or 0),
        "excluded_state_lic_hard_negative_class": int(candidates.get("excluded_state_lic_hard_negative_class") or 0),
        "excluded_state_lic_negative_keyword_family": int(
            candidates.get("excluded_state_lic_negative_keyword_family") or 0
        ),
        "excluded_state_lic_blank_website_no_positive_evidence": int(
            candidates.get("excluded_state_lic_blank_website_no_positive_evidence") or 0
        ),
        "excluded_state_lic_feedback_license_class": int(
            candidates.get("excluded_state_lic_feedback_license_class") or 0
        ),
        "excluded_state_lic_feedback_keyword_family": int(
            candidates.get("excluded_state_lic_feedback_keyword_family") or 0
        ),
        "excluded_duplicate_seed": int(candidates.get("excluded_duplicate_seed") or 0),
        "observed_state_lic_fit_mismatch": int(candidates.get("observed_state_lic_fit_mismatch") or 0),
        "included_without_website": included_without_website,
        "source_breakdown": candidates.get("source_breakdown") or {},
        "source_raw_breakdown": candidates.get("source_raw_breakdown") or {},
        "source_review_eligible_breakdown": candidates.get("source_review_eligible_breakdown") or {},
        "selected_source_breakdown": selected_source_breakdown,
        "stage_counts_by_source": stage_counts_by_source,
        "exclusion_counts_by_reason": candidates.get("exclusion_counts_by_reason") or {},
        "exclusion_counts_by_source": candidates.get("exclusion_counts_by_source") or {},
        "exclusion_counts_by_source_and_reason": candidates.get("exclusion_counts_by_source_and_reason") or {},
        "top_exclusion_reasons": candidates.get("top_exclusion_reasons") or [],
        "state_lic_license_type_breakdown": candidates.get("state_lic_license_type_breakdown") or {},
        "state_lic_hard_negative_class_breakdown": candidates.get("state_lic_hard_negative_class_breakdown") or {},
        "state_lic_negative_keyword_family_breakdown": candidates.get("state_lic_negative_keyword_family_breakdown") or {},
        "state_lic_blank_website_no_positive_evidence_count": int(
            candidates.get("state_lic_blank_website_no_positive_evidence_count") or 0
        ),
        "state_lic_feedback_snapshot": feedback_snapshot,
        "state_lic_feedback_suppression_breakdown": candidates.get("state_lic_feedback_suppression_breakdown") or {},
        "state_lic_cap_percent": int(cap_diagnostics.get("state_lic_cap_percent") or 0),
        "state_lic_cap_reason": str(cap_diagnostics.get("state_lic_cap_reason") or ""),
        "state_lic_cap_limited_count": int(cap_diagnostics.get("state_lic_cap_limited_count") or 0),
        "gap_total": gap_total,
        "states_scope": states,
        "sources": source_tokens,
        "reviewed_drop_dir": str(reviewed_drop_dir),
        "seed_index_path": str(_packet_seed_index_path(packet_dir)),
        "packet_row_counts": [len(packet_rows) for packet_rows in packets],
        "packets": manifest_packets,
    }
    _atomic_write_text(manifest_path, json.dumps(manifest_payload, indent=2) + "\n")
    _emit("AI_ASSIST_DUMP_WRITTEN", 1)
    _emit("AI_ASSIST_DUMP_OUTPUT_PATH", str(out_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
