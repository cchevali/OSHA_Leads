#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import ai_assist_paths
from outreach import crm_store
from outreach import run_prospect_generation as generation
from outreach import source_policy
from runtime_data_dir import resolve_data_dir
from tools import dump_prospect_ai_assist_review as dump_tool
from tools import import_prospect_ai_assist_review as import_tool

ERR_PROSPECT_GROWTH_CONFIG = "ERR_PROSPECT_GROWTH_CONFIG"
PASS_PROSPECT_GROWTH = "PASS_PROSPECT_GROWTH"
OUTPUT_DIRNAME = "prospect_growth"
DEFAULT_WINDOW_DAYS = 14
MANIFEST_WINDOW_DAYS = 7
RECOMMEND = "RECOMMEND"
HOLD = "HOLD"


def _emit(key: str, value: str | int) -> None:
    print(f"{key}={value}")


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _bool_env(name: str, default: str = "0") -> int:
    raw = _normalize_text(os.getenv(name, default)).lower()
    return 1 if raw in {"1", "true", "yes", "on"} else 0


def _now_local() -> datetime:
    return datetime.now().astimezone()


def _parse_iso_datetime(value: Any) -> datetime | None:
    text = _normalize_text(value)
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except Exception:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc).astimezone()
    return parsed.astimezone()


def _timestamp_token(now_local: datetime) -> str:
    return now_local.strftime("%Y%m%d_%H%M%S")


def _window_start(now_local: datetime, days: int) -> datetime:
    return now_local - timedelta(days=max(1, int(days)))


def _safe_percent(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round((float(numerator) / float(denominator)) * 100.0, 2)


def _safe_rate(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round(float(numerator) / float(denominator), 4)


def _json_load(path: Path) -> dict[str, Any] | list[Any] | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _analysis_source_tokens(configured_sources: list[str], states: list[str], data_dir: Path) -> list[str]:
    ordered: list[str] = []
    for token in list(configured_sources or []):
        normalized = source_policy.normalize_source_token(token)
        if normalized and normalized not in ordered:
            ordered.append(normalized)

    if source_policy.is_autogrow_source_implemented("BCSP") and "BCSP" not in ordered:
        ordered.append("BCSP")

    cache_root = data_dir / "prospect_generation" / "cache"
    for token in source_policy.implemented_autogrow_sources():
        if token in ordered or token == "OSHA_NEWS":
            continue
        for state in list(states or []):
            path = generation._source_cache_path_for_state(cache_root, token, state)
            if path.exists():
                ordered.append(token)
                break
    return ordered


def _config_snapshot(now_local: datetime) -> dict[str, Any]:
    data_dir_resolution = resolve_data_dir(REPO_ROOT)
    states = generation._states_for_selection(dump_tool._resolve_state_scope([]))
    configured_sources, source_warning = dump_tool._resolve_source_tokens()
    analysis_sources = _analysis_source_tokens(configured_sources, states, data_dir_resolution.effective_path)
    backlog_target = dump_tool._int_env(
        "PROSPECT_AUTOGROW_BACKLOG_TARGET",
        dump_tool.AI_ASSIST_DUMP_DEFAULT_BACKLOG_TARGET,
    )
    raw_target = dump_tool._int_env(
        "PROSPECT_AI_ASSIST_REVIEW_RAW_TARGET",
        dump_tool.AI_ASSIST_DUMP_DEFAULT_RAW_TARGET,
    )
    packet_size = dump_tool._int_env(
        "PROSPECT_AI_ASSIST_REVIEW_PACKET_SIZE",
        dump_tool.AI_ASSIST_DUMP_DEFAULT_PACKET_SIZE,
    )
    enrich_cfg = generation._parse_enrich_config(data_dir_resolution.effective_path)
    output_dir = (data_dir_resolution.effective_path / "audits" / OUTPUT_DIRNAME).resolve(strict=False)
    stamp = _timestamp_token(now_local)
    return {
        "generated_at": now_local.isoformat(),
        "data_dir": str(data_dir_resolution.effective_path),
        "data_dir_source": str(data_dir_resolution.source or "default"),
        "warning_token": str(data_dir_resolution.warning_token or ""),
        "outreach_states": states,
        "configured_sources": configured_sources,
        "analysis_sources": analysis_sources,
        "source_warning_configured": source_warning,
        "backlog_target": int(backlog_target),
        "ai_assist_raw_target": int(raw_target),
        "ai_assist_packet_size": int(packet_size),
        "ai_assist_review_enabled": _bool_env("PROSPECT_AI_ASSIST_REVIEW_ENABLED", dump_tool.AI_ASSIST_DUMP_DEFAULT_ENABLED),
        "prospect_autogrow_enabled": _bool_env("PROSPECT_AUTOGROW_ENABLED", "1"),
        "enrichment": {
            "domain_enabled": int(bool(enrich_cfg.get("domain_enabled"))),
            "hunter_enabled": int(bool(enrich_cfg.get("hunter_enabled"))),
            "max_sites_per_run": int(enrich_cfg.get("max_sites_per_run") or 0),
            "hunter_cap": int(enrich_cfg.get("hunter_cap") or 0),
            "sleep_ms": int(enrich_cfg.get("sleep_ms") or 0),
        },
        "output_dir": str(output_dir),
        "output_text_path": str((output_dir / f"prospect_growth_decision_pack_{stamp}.txt").resolve(strict=False)),
        "output_json_path": str((output_dir / f"prospect_growth_decision_pack_{stamp}.json").resolve(strict=False)),
    }


def _cache_snapshot(
    *,
    data_dir: Path,
    states: list[str],
    source_tokens: list[str],
    now_local: datetime,
) -> dict[tuple[str, str], dict[str, Any]]:
    cache_root = data_dir / "prospect_generation" / "cache"
    snapshot: dict[tuple[str, str], dict[str, Any]] = {}
    for state in list(states or []):
        for source_token in list(source_tokens or []):
            cache_path = generation._source_cache_path_for_state(cache_root, source_token, state)
            payload = _json_load(cache_path) if cache_path.exists() else None
            rows: list[dict[str, Any]] = []
            fetched_at = None
            cache_max_age_days = 0
            parse_mode = ""
            pages_fetched = 0
            if isinstance(payload, dict):
                rows = [row for row in list(payload.get("rows") or []) if isinstance(row, dict)]
                fetched_at = _parse_iso_datetime(payload.get("fetched_at_utc"))
                cache_max_age_days = int(payload.get("cache_max_age_days") or 0)
                parse_mode = _normalize_text(payload.get("parse_mode") or "")
                pages_fetched = int(payload.get("pages_fetched") or 0)
            if fetched_at is None and cache_path.exists():
                fetched_at = datetime.fromtimestamp(cache_path.stat().st_mtime, tz=timezone.utc).astimezone()
            age_days = None
            if fetched_at is not None:
                age_days = round((now_local - fetched_at.astimezone()).total_seconds() / 86400.0, 2)
            is_stale = (not cache_path.exists()) or (
                age_days is not None and cache_max_age_days > 0 and float(age_days) > float(cache_max_age_days)
            )
            snapshot[(state, source_token)] = {
                "state": state,
                "source": source_token,
                "cache_path": str(cache_path.resolve(strict=False)),
                "exists": bool(cache_path.exists()),
                "row_count": len(rows),
                "fetched_at": fetched_at.isoformat() if fetched_at is not None else "",
                "cache_max_age_days": int(cache_max_age_days),
                "age_days": age_days,
                "parse_mode": parse_mode,
                "pages_fetched": int(pages_fetched),
                "is_stale": bool(is_stale),
            }
    return snapshot


def _crm_domains_and_firms(conn: sqlite3.Connection | None) -> tuple[set[str], set[str]]:
    crm_domains = dump_tool._normalized_root_domains(generation._existing_crm_domains(conn))
    crm_firm_keys = dump_tool._existing_crm_firm_keys(conn)
    return crm_domains, crm_firm_keys


def _current_funnel_snapshot(
    *,
    data_dir: Path,
    conn: sqlite3.Connection | None,
    states: list[str],
    source_tokens: list[str],
    raw_target: int,
) -> tuple[list[dict[str, Any]], dict[str, Any], dict[str, Any]]:
    crm_domains, crm_firm_keys = _crm_domains_and_firms(conn)
    feedback_snapshot = dump_tool._state_lic_feedback_snapshot(conn)
    rows: list[dict[str, Any]] = []
    aggregated_exclusions: Counter[str] = Counter()
    state_cap_limited: dict[str, int] = {}
    state_blank_selected: dict[str, int] = {}

    for state in list(states or []):
        diagnostics = dump_tool._collect_candidates(
            data_dir=data_dir,
            states=[state],
            source_tokens=source_tokens,
            crm_domains=crm_domains,
            crm_firm_keys=crm_firm_keys,
            feedback_snapshot=feedback_snapshot,
        )
        selected_rows, cap_diag = dump_tool._select_rows_with_state_lic_cap(
            candidate_rows=list(diagnostics.get("candidates") or []),
            raw_target=raw_target,
            feedback_snapshot=feedback_snapshot,
        )
        selected_by_source = Counter(str(row.get("source_token") or "") for row in list(selected_rows or []))
        blank_selected_by_source = Counter(
            str(row.get("source_token") or "")
            for row in list(selected_rows or [])
            if not _normalize_text(row.get("website") or "")
        )
        state_cap_limited[state] = int(cap_diag.get("state_lic_cap_limited_count") or 0)
        state_blank_selected[state] = sum(blank_selected_by_source.values())
        aggregated_exclusions.update(
            Counter({str(k): int(v or 0) for k, v in dict(diagnostics.get("exclusion_counts_by_reason") or {}).items()})
        )

        stage_counts_by_source = dict(diagnostics.get("stage_counts_by_source") or {})
        exclusion_counts_by_source_and_reason = dict(diagnostics.get("exclusion_counts_by_source_and_reason") or {})
        for source_token in list(source_tokens or []):
            stages = dict(stage_counts_by_source.get(source_token) or {})
            rows.append(
                {
                    "state": state,
                    "source": source_token,
                    "fetched_cache_rows": int(stages.get("raw", 0)),
                    "candidate_rows": int(stages.get("identity_ready", 0)),
                    "consultant_fit_rows": int(stages.get("review_eligible", 0)),
                    "crm_safety_passed_rows": int(stages.get("safety_passed", 0)),
                    "packet_eligible_rows": int(stages.get("candidates", 0)),
                    "selected_packet_rows": int(selected_by_source.get(source_token, 0)),
                    "blank_website_selected_rows": int(blank_selected_by_source.get(source_token, 0)),
                    "exclusion_counts": {
                        key: int(value)
                        for key, value in dict(exclusion_counts_by_source_and_reason.get(source_token) or {}).items()
                    },
                }
            )

    return rows, {
        "feedback_snapshot": feedback_snapshot,
        "aggregated_exclusions": dict(aggregated_exclusions),
    }, {
        "state_cap_limited": state_cap_limited,
        "state_blank_selected": state_blank_selected,
    }


def _recent_discovery_imports(
    conn: sqlite3.Connection | None,
    *,
    states: list[str],
    source_tokens: list[str],
    window_start: datetime,
) -> dict[tuple[str, str], dict[str, int]]:
    out: dict[tuple[str, str], dict[str, int]] = {
        (state, source): {"discovery_imported_rows": 0, "send_eligible_rows": 0}
        for state in list(states or [])
        for source in list(source_tokens or [])
    }
    if conn is None or not crm_store._table_exists(conn, "prospects"):
        return out
    rows = conn.execute(
        """
        SELECT state, source, status, created_at, default_send_eligible
        FROM prospects
        """
    ).fetchall()
    for row in rows:
        state = generation._normalize_us_state(str(row["state"] or ""))
        source_family = source_policy.source_family(str(row["source"] or ""))
        if state not in set(states) or source_family not in set(source_tokens):
            continue
        created_at = _parse_iso_datetime(row["created_at"])
        if created_at is None or created_at < window_start:
            continue
        key = (state, source_family)
        out.setdefault(key, {"discovery_imported_rows": 0, "send_eligible_rows": 0})
        out[key]["discovery_imported_rows"] += 1
        status = _normalize_text(row["status"]).lower()
        default_send_eligible = int(row["default_send_eligible"] or 0)
        if default_send_eligible == 1 and status not in generation.EXCLUDED_STATUSES:
            out[key]["send_eligible_rows"] += 1
    return out


def _merge_import_counts(
    funnel_rows: list[dict[str, Any]],
    import_counts: dict[tuple[str, str], dict[str, int]],
) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    for row in list(funnel_rows or []):
        key = (str(row.get("state") or ""), str(row.get("source") or ""))
        additions = dict(import_counts.get(key) or {})
        merged.append(
            {
                **row,
                "discovery_imported_rows": int(additions.get("discovery_imported_rows", 0)),
                "send_eligible_rows": int(additions.get("send_eligible_rows", 0)),
            }
        )
    return merged


def _backlog_posture(
    conn: sqlite3.Connection | None,
    *,
    states: list[str],
    backlog_target: int,
    data_dir: Path,
) -> list[dict[str, Any]]:
    suppressed_emails = generation._load_suppression_set(data_dir, conn)
    rows: list[dict[str, Any]] = []
    for state in list(states or []):
        backlog_current = generation.compute_uncontacted_backlog(conn, state, suppressed_emails)
        crm_total = generation._count_crm_pool_total(conn, state)
        rows.append(
            {
                "state": state,
                "backlog_current": int(backlog_current),
                "backlog_target": int(backlog_target),
                "gap": max(0, int(backlog_target) - int(backlog_current)),
                "crm_total": int(crm_total),
            }
        )
    return rows


def _crm_inventory_counts(
    conn: sqlite3.Connection | None,
    *,
    states: list[str],
    source_tokens: list[str],
) -> list[dict[str, Any]]:
    rows_out: list[dict[str, Any]] = []
    if conn is None or not crm_store._table_exists(conn, "prospects"):
        return rows_out
    rows = conn.execute(
        """
        SELECT state, source, status
        FROM prospects
        """
    ).fetchall()
    allowed_states = set(states)
    allowed_sources = set(source_tokens) | {"AI_ASSIST"}
    counter: Counter[tuple[str, str, str]] = Counter()
    for row in rows:
        state = generation._normalize_us_state(str(row["state"] or ""))
        source_family = source_policy.source_family(str(row["source"] or ""))
        if state not in allowed_states or source_family not in allowed_sources:
            continue
        counter[(state, source_family, _normalize_text(row["status"]))] += 1
    for (state, source_family, status), count in sorted(counter.items()):
        rows_out.append(
            {
                "state": state,
                "source": source_family,
                "status": status,
                "count": int(count),
            }
        )
    return rows_out


def _recent_manifest_history(
    *,
    data_dir: Path,
    now_local: datetime,
) -> list[dict[str, Any]]:
    manifest_root = ai_assist_paths.prospect_audit_dir(data_root=data_dir)
    window_start = _window_start(now_local, MANIFEST_WINDOW_DAYS)
    rows: list[dict[str, Any]] = []
    if not manifest_root.exists():
        return rows
    for manifest_path in manifest_root.glob("*_packets\\manifest.json"):
        payload = _json_load(manifest_path)
        if not isinstance(payload, dict):
            continue
        run_started_at = _parse_iso_datetime(payload.get("run_started_at"))
        if run_started_at is None:
            run_started_at = datetime.fromtimestamp(manifest_path.stat().st_mtime, tz=timezone.utc).astimezone()
        if run_started_at < window_start:
            continue
        packet_status_path = manifest_path.parent / "packet_status.txt"
        rows.append(
            {
                "run_date": _normalize_text(payload.get("run_date")),
                "run_started_at": run_started_at.isoformat(),
                "manifest_path": str(manifest_path.resolve(strict=False)),
                "packet_status_path": str(packet_status_path.resolve(strict=False)),
                "raw_target": int(payload.get("raw_target") or 0),
                "selected_row_count": int(payload.get("selected_row_count") or 0),
                "packet_count": int(payload.get("packet_count") or 0),
                "candidate_count_before_filters": int(payload.get("candidate_count_before_filters") or 0),
                "candidate_count_after_filters": int(payload.get("candidate_count_after_filters") or 0),
                "included_without_website": int(payload.get("included_without_website") or 0),
                "state_lic_cap_limited_count": int(payload.get("state_lic_cap_limited_count") or 0),
                "top_exclusion_reasons": list(payload.get("top_exclusion_reasons") or []),
                "packet_status_text": packet_status_path.read_text(encoding="utf-8").strip() if packet_status_path.exists() else "",
            }
        )
    rows.sort(key=lambda item: str(item.get("run_started_at") or ""), reverse=True)
    return rows


def _review_events_from_db(
    conn: sqlite3.Connection | None,
    *,
    window_start: datetime,
) -> tuple[list[dict[str, Any]], str]:
    if conn is None or not crm_store._table_exists(conn, crm_store.AI_ASSIST_CANDIDATE_TABLE):
        return [], "none"
    columns = crm_store._table_columns(conn, crm_store.AI_ASSIST_CANDIDATE_TABLE)
    required = {"state", "decision", "updated_at", "seed_source_token"}
    if not required.issubset(columns):
        return [], "files"
    rows = conn.execute(
        f"""
        SELECT state, decision, updated_at, seed_source_token
        FROM {crm_store.AI_ASSIST_CANDIDATE_TABLE}
        WHERE lower(trim(COALESCE(decision, ''))) IN ('accept', 'reject')
        """
    ).fetchall()
    events: list[dict[str, Any]] = []
    for row in rows:
        source_token = source_policy.normalize_source_token(str(row["seed_source_token"] or ""))
        state = generation._normalize_us_state(str(row["state"] or ""))
        updated_at = _parse_iso_datetime(row["updated_at"])
        if not source_token or updated_at is None or updated_at < window_start:
            continue
        events.append(
            {
                "state": state,
                "source": source_token,
                "decision": _normalize_text(row["decision"]).lower(),
                "at": updated_at,
            }
        )
    return events, "db"


def _review_events_from_files(
    *,
    window_start: datetime,
) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    review_dirs = [
        ai_assist_paths.prospect_audit_dir(repo_root=REPO_ROOT),
        ai_assist_paths.legacy_prospect_audit_dir(repo_root=REPO_ROOT),
    ]
    seen_paths: set[str] = set()
    for review_dir in review_dirs:
        try:
            candidates = list(review_dir.glob("prospect_ai_assist_review_*_reviewed.csv"))
        except Exception:
            candidates = []
        for path in candidates:
            key = str(path.resolve(strict=False)).lower()
            if key in seen_paths:
                continue
            seen_paths.add(key)
            file_dt = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).astimezone()
            if file_dt < window_start:
                continue
            try:
                rows = import_tool._load_csv_rows(path)
            except Exception:
                continue
            batch_id = import_tool._default_batch_id(path)
            seed_index = import_tool._load_seed_index(path, batch_id)
            for row in rows:
                source_meta = import_tool._seed_provenance_for_row(row=row, seed_index=seed_index)
                source_token = source_policy.normalize_source_token(str(source_meta.get("seed_source_token") or ""))
                state = generation._normalize_us_state(str(row.get("state") or ""))
                decision = _normalize_text(row.get("decision") or "").lower()
                if not source_token or decision not in {"accept", "reject"}:
                    continue
                events.append({"state": state, "source": source_token, "decision": decision, "at": file_dt})
    return events


def _review_outcomes(
    conn: sqlite3.Connection | None,
    *,
    states: list[str],
    source_tokens: list[str],
    now_local: datetime,
    days: int,
) -> tuple[list[dict[str, Any]], str]:
    window_start_14 = _window_start(now_local, days)
    events, strategy = _review_events_from_db(conn, window_start=window_start_14)
    if strategy != "db" or not events:
        events = _review_events_from_files(window_start=window_start_14)
        strategy = "files" if events else strategy

    by_group_14: dict[tuple[str, str], Counter[str]] = defaultdict(Counter)
    by_group_7: dict[tuple[str, str], Counter[str]] = defaultdict(Counter)
    window_start_7 = _window_start(now_local, 7)
    for event in list(events or []):
        state = generation._normalize_us_state(str(event.get("state") or ""))
        source = source_policy.normalize_source_token(str(event.get("source") or ""))
        decision = _normalize_text(event.get("decision") or "").lower()
        at = event.get("at")
        if state not in set(states) or source not in set(source_tokens) or decision not in {"accept", "reject"}:
            continue
        key = (state, source)
        by_group_14[key][decision] += 1
        if isinstance(at, datetime) and at >= window_start_7:
            by_group_7[key][decision] += 1

    rows: list[dict[str, Any]] = []
    for state in list(states or []):
        for source in list(source_tokens or []):
            counter_14 = by_group_14.get((state, source), Counter())
            counter_7 = by_group_7.get((state, source), Counter())
            accepts_14 = int(counter_14.get("accept", 0))
            rejects_14 = int(counter_14.get("reject", 0))
            reviewed_14 = accepts_14 + rejects_14
            accepts_7 = int(counter_7.get("accept", 0))
            rejects_7 = int(counter_7.get("reject", 0))
            reviewed_7 = accepts_7 + rejects_7
            rows.append(
                {
                    "state": state,
                    "source": source,
                    "reviewed_accepts": accepts_14,
                    "reviewed_rejects": rejects_14,
                    "accept_rate_7d": _safe_rate(accepts_7, reviewed_7),
                    "accept_rate_14d": _safe_rate(accepts_14, reviewed_14),
                }
            )
    return rows, strategy


def _freshness_rows(
    *,
    cache_snapshot: dict[tuple[str, str], dict[str, Any]],
    funnel_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    funnel_index = {
        (str(row.get("state") or ""), str(row.get("source") or "")): row
        for row in list(funnel_rows or [])
    }
    rows: list[dict[str, Any]] = []
    for key in sorted(cache_snapshot.keys()):
        cache_meta = dict(cache_snapshot.get(key) or {})
        state, source = key
        funnel = dict(funnel_index.get((state, source)) or {})
        disposition = "fresh_active"
        if not bool(cache_meta.get("exists")):
            disposition = "stale_missing"
        elif bool(cache_meta.get("is_stale")):
            disposition = "stale"
        elif int(funnel.get("packet_eligible_rows", 0)) <= 0:
            disposition = "merely_low_yield"
        rows.append(
            {
                **cache_meta,
                "freshness_disposition": disposition,
                "consultant_fit_rows": int(funnel.get("consultant_fit_rows", 0)),
                "packet_eligible_rows": int(funnel.get("packet_eligible_rows", 0)),
                "selected_packet_rows": int(funnel.get("selected_packet_rows", 0)),
            }
        )
    return rows


def _aggregate_source_metrics(rows: list[dict[str, Any]]) -> dict[str, Counter[str]]:
    out: dict[str, Counter[str]] = defaultdict(Counter)
    for row in list(rows or []):
        source = str(row.get("source") or "")
        if not source:
            continue
        counter = out[source]
        for key in (
            "fetched_cache_rows",
            "candidate_rows",
            "consultant_fit_rows",
            "crm_safety_passed_rows",
            "packet_eligible_rows",
            "selected_packet_rows",
            "discovery_imported_rows",
            "send_eligible_rows",
            "blank_website_selected_rows",
        ):
            counter[key] += int(row.get(key, 0) or 0)
    return out


def _freshness_index(rows: list[dict[str, Any]]) -> dict[str, Counter[str]]:
    out: dict[str, Counter[str]] = defaultdict(Counter)
    for row in list(rows or []):
        source = str(row.get("source") or "")
        disposition = str(row.get("freshness_disposition") or "")
        if source and disposition:
            out[source][disposition] += 1
    return out


def _manifest_rollup(rows: list[dict[str, Any]]) -> dict[str, int]:
    out = Counter()
    for row in list(rows or []):
        out["raw_target"] += int(row.get("raw_target") or 0)
        out["selected_row_count"] += int(row.get("selected_row_count") or 0)
        out["included_without_website"] += int(row.get("included_without_website") or 0)
        out["state_lic_cap_limited_count"] += int(row.get("state_lic_cap_limited_count") or 0)
    return dict(out)


def _recommendations(
    *,
    config: dict[str, Any],
    backlog_rows: list[dict[str, Any]],
    funnel_rows: list[dict[str, Any]],
    review_rows: list[dict[str, Any]],
    freshness_rows: list[dict[str, Any]],
    manifest_rows: list[dict[str, Any]],
    funnel_diagnostics: dict[str, Any],
) -> list[dict[str, Any]]:
    source_totals = _aggregate_source_metrics(funnel_rows)
    freshness = _freshness_index(freshness_rows)
    gap_total = sum(int(row.get("gap") or 0) for row in list(backlog_rows or []))
    manifest_rollup = _manifest_rollup(manifest_rows)
    latest_manifest = dict(manifest_rows[0]) if manifest_rows else {}
    state_lic = source_totals.get("STATE_LIC", Counter())
    aiha = source_totals.get("AIHA", Counter())
    ohs_bg = source_totals.get("OHS_BG", Counter())
    bcsp = source_totals.get("BCSP", Counter())
    primary_consultant_fit = int(aiha.get("consultant_fit_rows", 0)) + int(ohs_bg.get("consultant_fit_rows", 0))
    primary_imported = int(aiha.get("discovery_imported_rows", 0)) + int(ohs_bg.get("discovery_imported_rows", 0))
    primary_sendable = int(aiha.get("send_eligible_rows", 0)) + int(ohs_bg.get("send_eligible_rows", 0))
    state_lic_reviewed = 0
    state_lic_accepts = 0
    for row in list(review_rows or []):
        if str(row.get("source") or "") != "STATE_LIC":
            continue
        state_lic_reviewed += int(row.get("reviewed_accepts", 0) or 0) + int(row.get("reviewed_rejects", 0) or 0)
        state_lic_accepts += int(row.get("reviewed_accepts", 0) or 0)
    state_lic_accept_rate = _safe_percent(state_lic_accepts, state_lic_reviewed)

    keep_current = {
        "key": "KEEP_CURRENT_ARCHITECTURE",
        "status": RECOMMEND,
        "evidence": (
            f"backlog_gap_total={gap_total}; configured_sources={','.join(list(config.get('configured_sources') or [])) or 'none'}; "
            f"latest_manifest_selected={int(latest_manifest.get('selected_row_count') or 0)}/"
            f"{int(latest_manifest.get('raw_target') or 0)}"
        ),
    }

    demote_state_lic = {
        "key": "DEMOTE_STATE_LIC_TO_AUGMENT_ONLY",
        "status": RECOMMEND,
        "evidence": (
            f"STATE_LIC raw={int(state_lic.get('fetched_cache_rows', 0))} consultant_fit={int(state_lic.get('consultant_fit_rows', 0))} "
            f"selected={int(state_lic.get('selected_packet_rows', 0))}; "
            f"hard_negatives={int((funnel_diagnostics.get('aggregated_exclusions') or {}).get('excluded_state_lic_hard_negative_class', 0))}; "
            f"review_accept_rate_14d={state_lic_accept_rate}%"
        ),
    }

    enrichment_enabled = bool(int((config.get("enrichment") or {}).get("domain_enabled", 0))) or bool(
        int((config.get("enrichment") or {}).get("hunter_enabled", 0))
    )
    bcsp_failed_or_stale = (
        int(bcsp.get("fetched_cache_rows", 0)) == 0
        and int(freshness.get("BCSP", Counter()).get("stale", 0)) + int(freshness.get("BCSP", Counter()).get("stale_missing", 0)) > 0
    )
    enable_bounded_enrichment = {
        "key": "ENABLE_BOUNDED_ENRICHMENT_NEXT",
        "status": HOLD if enrichment_enabled else (RECOMMEND if primary_consultant_fit > 0 and gap_total > primary_sendable else HOLD),
        "evidence": (
            f"primary_consultant_fit={primary_consultant_fit} primary_imported_14d={primary_imported} "
            f"primary_send_eligible_14d={primary_sendable} enrichment_enabled={1 if enrichment_enabled else 0}"
        ),
    }

    reprobe_bcsp = {
        "key": "RE-PROBE_BCSP",
        "status": RECOMMEND if (enable_bounded_enrichment["status"] != RECOMMEND and bcsp_failed_or_stale) else HOLD,
        "evidence": (
            f"BCSP consultant_fit={int(bcsp.get('consultant_fit_rows', 0))} imported_14d={int(bcsp.get('discovery_imported_rows', 0))} "
            f"freshness_stale={int(freshness.get('BCSP', Counter()).get('stale', 0)) + int(freshness.get('BCSP', Counter()).get('stale_missing', 0))}"
        ),
    }

    new_source_required_status = HOLD
    if (
        enable_bounded_enrichment["status"] != RECOMMEND
        and reprobe_bcsp["status"] != RECOMMEND
        and gap_total > 0
        and primary_consultant_fit <= 0
        and primary_imported <= 0
    ):
        new_source_required_status = RECOMMEND
    new_source_required = {
        "key": "NEW_SOURCE_REQUIRED",
        "status": new_source_required_status,
        "evidence": (
            f"gap_total={gap_total} primary_consultant_fit={primary_consultant_fit} primary_imported_14d={primary_imported} "
            f"manifest_selected_7d={int(manifest_rollup.get('selected_row_count', 0))}"
        ),
    }

    return [
        keep_current,
        demote_state_lic,
        enable_bounded_enrichment,
        reprobe_bcsp,
        new_source_required,
    ]


def _render_text_report(report: dict[str, Any]) -> str:
    config = dict(report.get("config") or {})
    lines = [
        "PROSPECT GROWTH DECISION PACK",
        f"generated_at: {report.get('generated_at')}",
        f"window_days: {int(report.get('window_days') or 0)}",
        "",
        "EFFECTIVE CONFIG SNAPSHOT",
        f"- OUTREACH_STATES: {','.join(list(config.get('outreach_states') or [])) or 'none'}",
        f"- PROSPECT_AUTOGROW_SOURCES: {','.join(list(config.get('configured_sources') or [])) or 'none'}",
        f"- analysis_sources: {','.join(list(config.get('analysis_sources') or [])) or 'none'}",
        f"- backlog_target: {int(config.get('backlog_target') or 0)}",
        f"- ai_assist_raw_target: {int(config.get('ai_assist_raw_target') or 0)}",
        f"- ai_assist_packet_size: {int(config.get('ai_assist_packet_size') or 0)}",
        (
            "- enrichment: "
            f"domain_enabled={int((config.get('enrichment') or {}).get('domain_enabled', 0))} "
            f"hunter_enabled={int((config.get('enrichment') or {}).get('hunter_enabled', 0))} "
            f"max_sites_per_run={int((config.get('enrichment') or {}).get('max_sites_per_run', 0))} "
            f"hunter_cap={int((config.get('enrichment') or {}).get('hunter_cap', 0))}"
        ),
        "",
        "SOURCE-BY-SOURCE FUNNEL BY STATE",
    ]
    for row in list(report.get("funnel_by_state_source") or []):
        lines.append(
            "- "
            f"{row['state']} / {row['source']}: "
            f"cache_rows={int(row.get('fetched_cache_rows') or 0)} "
            f"candidate_rows={int(row.get('candidate_rows') or 0)} "
            f"consultant_fit_rows={int(row.get('consultant_fit_rows') or 0)} "
            f"packet_eligible_rows={int(row.get('packet_eligible_rows') or 0)} "
            f"selected_packet_rows={int(row.get('selected_packet_rows') or 0)} "
            f"discovery_imported_rows_14d={int(row.get('discovery_imported_rows') or 0)} "
            f"send_eligible_rows_14d={int(row.get('send_eligible_rows') or 0)}"
        )

    lines.extend(["", "AI-ASSIST REVIEW OUTCOMES BY SOURCE/STATE"])
    lines.append(f"- attribution_strategy: {report.get('review_attribution_strategy') or 'none'}")
    for row in list(report.get("review_outcomes") or []):
        lines.append(
            "- "
            f"{row['state']} / {row['source']}: "
            f"reviewed_accepts_14d={int(row.get('reviewed_accepts') or 0)} "
            f"reviewed_rejects_14d={int(row.get('reviewed_rejects') or 0)} "
            f"accept_rate_7d={round(float(row.get('accept_rate_7d') or 0.0) * 100.0, 2)}% "
            f"accept_rate_14d={round(float(row.get('accept_rate_14d') or 0.0) * 100.0, 2)}%"
        )

    lines.extend(["", "STARVATION DIAGNOSTICS"])
    for row in list(report.get("manifest_history_7d") or []):
        lines.append(
            "- "
            f"{row.get('run_date')}: selected={int(row.get('selected_row_count') or 0)}/"
            f"{int(row.get('raw_target') or 0)} packets={int(row.get('packet_count') or 0)} "
            f"blank_website={int(row.get('included_without_website') or 0)} "
            f"cap_limited={int(row.get('state_lic_cap_limited_count') or 0)}"
        )
        if _normalize_text(row.get("packet_status_text") or ""):
            lines.append(f"  packet_status: {str(row.get('packet_status_text') or '').replace(chr(10), ' | ')}")
    lines.append(
        "- top_exclusion_reasons_current: "
        + (
            "none"
            if not dict((report.get("funnel_diagnostics") or {}).get("aggregated_exclusions") or {})
            else " | ".join(
                f"{key}={int(value or 0)}"
                for key, value in sorted(
                    dict((report.get("funnel_diagnostics") or {}).get("aggregated_exclusions") or {}).items(),
                    key=lambda item: (-int(item[1] or 0), str(item[0])),
                )[:5]
            )
        )
    )
    lines.append(
        "- state_lic_cap_limited_by_state_current: "
        + " | ".join(
            f"{state}={int(value or 0)}"
            for state, value in sorted(dict((report.get("state_starvation") or {}).get("state_cap_limited") or {}).items())
        )
    )
    lines.append(
        "- blank_website_selected_by_state_current: "
        + " | ".join(
            f"{state}={int(value or 0)}"
            for state, value in sorted(dict((report.get("state_starvation") or {}).get("state_blank_selected") or {}).items())
        )
    )

    lines.extend(["", "CRM INVENTORY POSTURE"])
    for row in list(report.get("backlog_posture") or []):
        lines.append(
            "- "
            f"{row['state']}: backlog_current={int(row.get('backlog_current') or 0)} "
            f"target={int(row.get('backlog_target') or 0)} "
            f"gap={int(row.get('gap') or 0)} crm_total={int(row.get('crm_total') or 0)}"
        )
    for row in list(report.get("crm_inventory_counts") or []):
        lines.append(
            "- "
            f"{row['state']} / {row['source']} / {row['status'] or 'blank'}: "
            f"{int(row.get('count') or 0)}"
        )

    lines.extend(["", "FRESHNESS DIAGNOSTICS"])
    for row in list(report.get("freshness") or []):
        lines.append(
            "- "
            f"{row['state']} / {row['source']}: "
            f"fetched_at={row.get('fetched_at') or 'missing'} "
            f"age_days={row.get('age_days') if row.get('age_days') is not None else 'n/a'} "
            f"cache_max_age_days={int(row.get('cache_max_age_days') or 0)} "
            f"row_count={int(row.get('row_count') or 0)} "
            f"disposition={row.get('freshness_disposition') or 'unknown'}"
        )

    lines.extend(["", "DETERMINISTIC RECOMMENDATIONS"])
    for row in list(report.get("recommendations") or []):
        lines.append(f"- {row.get('key')}: {row.get('status')} - {row.get('evidence')}")
    return "\n".join(lines).rstrip() + "\n"


def _build_report(*, days: int, now_local: datetime) -> dict[str, Any]:
    config = _config_snapshot(now_local)
    data_dir = Path(str(config["data_dir"])).resolve(strict=False)
    conn: sqlite3.Connection | None = None
    db_path = crm_store.crm_db_path()
    if db_path.exists():
        conn = crm_store.connect(db_path)
    try:
        funnel_rows, funnel_diagnostics, state_starvation = _current_funnel_snapshot(
            data_dir=data_dir,
            conn=conn,
            states=list(config.get("outreach_states") or []),
            source_tokens=list(config.get("analysis_sources") or []),
            raw_target=int(config.get("ai_assist_raw_target") or 0),
        )
        import_counts = _recent_discovery_imports(
            conn,
            states=list(config.get("outreach_states") or []),
            source_tokens=list(config.get("analysis_sources") or []),
            window_start=_window_start(now_local, days),
        )
        merged_funnel_rows = _merge_import_counts(funnel_rows, import_counts)
        review_rows, review_strategy = _review_outcomes(
            conn,
            states=list(config.get("outreach_states") or []),
            source_tokens=list(config.get("analysis_sources") or []),
            now_local=now_local,
            days=days,
        )
        backlog_rows = _backlog_posture(
            conn,
            states=list(config.get("outreach_states") or []),
            backlog_target=int(config.get("backlog_target") or 0),
            data_dir=data_dir,
        )
        inventory_rows = _crm_inventory_counts(
            conn,
            states=list(config.get("outreach_states") or []),
            source_tokens=list(config.get("analysis_sources") or []),
        )
    finally:
        if conn is not None:
            conn.close()

    cache_snapshot = _cache_snapshot(
        data_dir=data_dir,
        states=list(config.get("outreach_states") or []),
        source_tokens=list(config.get("analysis_sources") or []),
        now_local=now_local,
    )
    freshness_rows = _freshness_rows(cache_snapshot=cache_snapshot, funnel_rows=merged_funnel_rows)
    manifest_rows = _recent_manifest_history(data_dir=data_dir, now_local=now_local)
    recommendations = _recommendations(
        config=config,
        backlog_rows=backlog_rows,
        funnel_rows=merged_funnel_rows,
        review_rows=review_rows,
        freshness_rows=freshness_rows,
        manifest_rows=manifest_rows,
        funnel_diagnostics=funnel_diagnostics,
    )
    return {
        "generated_at": now_local.isoformat(),
        "window_days": int(days),
        "config": config,
        "funnel_by_state_source": merged_funnel_rows,
        "review_outcomes": review_rows,
        "review_attribution_strategy": review_strategy,
        "manifest_history_7d": manifest_rows,
        "funnel_diagnostics": funnel_diagnostics,
        "state_starvation": state_starvation,
        "backlog_posture": backlog_rows,
        "crm_inventory_counts": inventory_rows,
        "freshness": freshness_rows,
        "recommendations": recommendations,
    }


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="Emit a read-only prospect growth decision pack from current cache, CRM, and AI-assist audit artifacts.")
    ap.add_argument("--days", type=int, default=DEFAULT_WINDOW_DAYS, help="Trailing analysis window in days (default: 14).")
    ap.add_argument("--print-config", action="store_true", help="Print resolved config and exit.")
    ap.add_argument("--dry-run", action="store_true", help="Build the report without writing artifacts.")
    ap.add_argument("--output-dir", default="", help="Optional output directory override.")
    return ap


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    now_local = _now_local()
    try:
        days = max(1, int(args.days or DEFAULT_WINDOW_DAYS))
        config = _config_snapshot(now_local)
    except Exception as exc:
        print(f"{ERR_PROSPECT_GROWTH_CONFIG} detail={exc}", file=sys.stderr)
        return 2

    output_dir = (
        Path(str(args.output_dir or "")).expanduser().resolve(strict=False)
        if _normalize_text(args.output_dir)
        else Path(str(config["output_dir"])).resolve(strict=False)
    )
    text_path = (output_dir / Path(str(config["output_text_path"])).name).resolve(strict=False)
    json_path = (output_dir / Path(str(config["output_json_path"])).name).resolve(strict=False)

    if _normalize_text(config.get("warning_token") or ""):
        print(str(config["warning_token"]))
    _emit("PROSPECT_GROWTH_DATA_DIR", str(config["data_dir"]))
    _emit("PROSPECT_GROWTH_DATA_DIR_SOURCE", str(config["data_dir_source"]))
    _emit("PROSPECT_GROWTH_DAYS", int(days))
    _emit("PROSPECT_GROWTH_STATES", ",".join(list(config.get("outreach_states") or [])) or "none")
    _emit("PROSPECT_GROWTH_SOURCES", ",".join(list(config.get("analysis_sources") or [])) or "none")
    _emit("PROSPECT_GROWTH_BACKLOG_TARGET", int(config.get("backlog_target") or 0))
    _emit("PROSPECT_GROWTH_RAW_TARGET", int(config.get("ai_assist_raw_target") or 0))
    _emit("PROSPECT_GROWTH_PACKET_SIZE", int(config.get("ai_assist_packet_size") or 0))
    _emit("PROSPECT_GROWTH_OUTPUT_DIR", str(output_dir))
    _emit("PROSPECT_GROWTH_OUTPUT_TEXT_PATH", str(text_path))
    _emit("PROSPECT_GROWTH_OUTPUT_JSON_PATH", str(json_path))

    if args.print_config:
        return 0

    report = _build_report(days=days, now_local=now_local)
    report["config"]["output_dir"] = str(output_dir)
    report["config"]["output_text_path"] = str(text_path)
    report["config"]["output_json_path"] = str(json_path)
    report_text = _render_text_report(report)
    report_json = json.dumps(report, indent=2) + "\n"

    if args.dry_run:
        print(report_text, end="")
        return 0

    output_dir.mkdir(parents=True, exist_ok=True)
    dump_tool._atomic_write_text(text_path, report_text)
    dump_tool._atomic_write_text(json_path, report_json)
    _emit("PROSPECT_GROWTH_WRITTEN", 1)
    _emit(PASS_PROSPECT_GROWTH, 1)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
