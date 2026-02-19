import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from geo.zip_cbsa import extract_zip5_from_text, resolve_lead_cbsa, zip_cbsa_dataset_status


DEFAULT_TERRITORIES = {
    "TX_TRI": {
        "display_name": "Texas Triangle",
        "label": "Texas Triangle (DFW + Houston + San Antonio + Austin)",
        "description": "Texas Triangle metros resolved by ZIP->CBSA with fallback city matching when CBSA cannot be resolved.",
        "kind": "CBSA_SET",
        "states": ["TX"],
        "cbsas": ["19100", "26420", "41700", "12420"],
        "aliases": ["TX_TRIANGLE_V1", "TX_TRIANGLE", "TX_TRI_V1"],
        "office_patterns": [
            r"\baustin\b",
            r"\bdallas\b",
            r"\bfort[\s-]*worth\b",
            r"\bdallas[\s/-]*fort[\s-]*worth\b",
            r"\bhouston\b",
            r"\bsan[\s-]*antonio\b",
        ],
        "fallback_city_patterns": [
            r"\baustin\b",
            r"\bdallas\b",
            r"\bfort[\s-]*worth\b",
            r"\bhouston\b",
            r"\bpasadena\b",
            r"\bpearland\b",
            r"\bsugar[\s-]*land\b",
            r"\bthe[\s-]*woodlands\b",
            r"\bkaty\b",
            r"\bbaytown\b",
            r"\bsan[\s-]*antonio\b",
        ],
    }
}

LEGACY_TERRITORY_ALIASES = {
    "TX_TRIANGLE_V1": "TX_TRI",
    "TX_TRIANGLE": "TX_TRI",
    "TX_TRI_V1": "TX_TRI",
}

CONTENT_FILTER_ALL = "all"
CONTENT_FILTER_HIGH_MEDIUM = "high_medium"
CONTENT_FILTER_HIGH_ONLY = "high_only"


def _parse_datetime(value: Any) -> datetime:
    if not value:
        return datetime.min

    text = str(value).strip().replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        pass

    for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue

    return datetime.min


def _coerce_datetime_aware_utc(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _normalized_datetime_sort_value(value: Any) -> datetime:
    dt = _coerce_datetime_aware_utc(_parse_datetime(value))
    if dt is None:
        return datetime.min.replace(tzinfo=timezone.utc)
    return dt


def load_territory_definitions(path: str = "territories.json") -> dict[str, dict[str, Any]]:
    definitions = dict(DEFAULT_TERRITORIES)
    json_path = Path(path)

    if json_path.exists():
        with open(json_path, "r", encoding="utf-8") as f:
            loaded = json.load(f)
        if isinstance(loaded, dict):
            for code, cfg in loaded.items():
                if isinstance(cfg, dict):
                    definitions[code] = cfg

    # Backward compatibility: alias entries that only define canonical_code should still
    # behave like the canonical territory when callers access defs[alias] directly.
    for code in list(definitions.keys()):
        cfg = definitions.get(code)
        if not isinstance(cfg, dict):
            continue
        canonical_code = str(cfg.get("canonical_code") or "").strip().upper()
        if not canonical_code:
            continue
        canonical_cfg = definitions.get(canonical_code)
        if not isinstance(canonical_cfg, dict):
            continue
        merged = dict(canonical_cfg)
        merged.update(cfg)
        merged.setdefault("label", canonical_cfg.get("label") or canonical_cfg.get("description"))
        definitions[code] = merged

    return definitions


def resolve_territory_code(
    territory_code: str | None,
    definitions: dict[str, dict[str, Any]] | None = None,
) -> str:
    raw = str(territory_code or "").strip().upper()
    if not raw:
        return ""

    defs = definitions or load_territory_definitions()
    if raw in defs:
        canonical = str(defs[raw].get("canonical_code") or raw).strip().upper()
        if canonical and canonical in defs:
            return canonical
        return raw

    target = LEGACY_TERRITORY_ALIASES.get(raw)
    if target and target in defs:
        return target

    for code, definition in defs.items():
        aliases = definition.get("aliases") or []
        if not isinstance(aliases, list):
            continue
        normalized = {str(alias or "").strip().upper() for alias in aliases}
        if raw in normalized:
            return str(code).strip().upper()

    return raw


def normalize_content_filter(value: str | None) -> str:
    normalized = (value or CONTENT_FILTER_HIGH_MEDIUM).strip().lower().replace("+", "_")
    normalized = normalized.replace("-", "_").replace(" ", "_")

    aliases = {
        "all": CONTENT_FILTER_ALL,
        "any": CONTENT_FILTER_ALL,
        "high_medium": CONTENT_FILTER_HIGH_MEDIUM,
        "high_med": CONTENT_FILTER_HIGH_MEDIUM,
        "highmedium": CONTENT_FILTER_HIGH_MEDIUM,
        "high_only": CONTENT_FILTER_HIGH_ONLY,
        "high": CONTENT_FILTER_HIGH_ONLY,
    }

    if normalized not in aliases:
        raise ValueError(f"Unsupported content_filter='{value}'")

    return aliases[normalized]


def apply_content_filter(leads: list[dict], content_filter: str | None) -> tuple[list[dict], int]:
    mode = normalize_content_filter(content_filter)
    if mode == CONTENT_FILTER_ALL:
        return list(leads), 0

    min_score = 10 if mode == CONTENT_FILTER_HIGH_ONLY else 6
    filtered = [lead for lead in leads if int(lead.get("lead_score") or 0) >= min_score]
    excluded = len(leads) - len(filtered)
    return filtered, excluded


def dedupe_by_activity_nr(leads: list[dict]) -> tuple[list[dict], int]:
    by_key: dict[str, dict] = {}

    for lead in leads:
        key = str(lead.get("lead_key") or lead.get("activity_nr") or lead.get("lead_id") or "").strip()
        if not key:
            continue

        current = by_key.get(key)
        if not current:
            by_key[key] = lead
            continue

        current_key = (
            int(current.get("lead_score") or 0),
            _normalized_datetime_sort_value(current.get("first_seen_at")),
            _normalized_datetime_sort_value(current.get("last_seen_at")),
            _normalized_datetime_sort_value(current.get("date_opened")),
        )
        candidate_key = (
            int(lead.get("lead_score") or 0),
            _normalized_datetime_sort_value(lead.get("first_seen_at")),
            _normalized_datetime_sort_value(lead.get("last_seen_at")),
            _normalized_datetime_sort_value(lead.get("date_opened")),
        )

        if candidate_key > current_key:
            by_key[key] = lead

    deduped = sorted(
        by_key.values(),
        key=lambda row: (
            int(row.get("lead_score") or 0),
            _normalized_datetime_sort_value(row.get("date_opened")),
            _normalized_datetime_sort_value(row.get("first_seen_at")),
        ),
        reverse=True,
    )

    removed = len(leads) - len(deduped)
    return deduped, removed


def _matches_any(text: str, patterns: list[str]) -> bool:
    return any(re.search(pattern, text, flags=re.IGNORECASE) for pattern in patterns)


def _normalize_location_text(text: str) -> str:
    cleaned = " ".join(str(text or "").strip().split())
    return cleaned.upper()


def filter_by_territory(
    leads: list[dict],
    territory_code: str | None,
    definitions: dict[str, dict[str, Any]] | None = None,
    include_debug: bool = False,
) -> tuple[list[dict], dict[str, int]] | tuple[list[dict], dict[str, int], list[dict[str, Any]]]:
    if not territory_code:
        stats = {
            "excluded_state": 0,
            "excluded_territory": 0,
            "matched_by_office": 0,
            "matched_by_fallback": 0,
            "matched_by_cbsa": 0,
        }
        if include_debug:
            return list(leads), stats, []
        return list(leads), stats

    defs = definitions or load_territory_definitions()
    requested_code = str(territory_code or "").strip().upper()
    canonical_code = resolve_territory_code(requested_code, defs)
    if canonical_code not in defs:
        raise ValueError(f"Unknown territory_code='{territory_code}'")

    territory = defs[canonical_code]
    states = [s.upper() for s in territory.get("states", [])]
    kind = str(territory.get("kind") or "LEGACY_REGEX").strip().upper()
    cbsa_set = {
        "".join(ch for ch in str(cbsa or "").strip() if ch.isdigit()).zfill(5)
        for cbsa in (territory.get("cbsas") or [])
        if str(cbsa or "").strip()
    }
    office_patterns = territory.get("office_patterns", [])
    fallback_patterns = territory.get("fallback_city_patterns", [])

    filtered: list[dict] = []
    stats = {
        "excluded_state": 0,
        "excluded_territory": 0,
        "matched_by_office": 0,
        "matched_by_fallback": 0,
        "matched_by_cbsa": 0,
    }
    debug_rows: list[dict[str, Any]] = []
    dataset_incomplete = False
    if include_debug and kind == "CBSA_SET" and cbsa_set:
        dataset_status = zip_cbsa_dataset_status()
        dataset_incomplete = bool(dataset_status.get("dataset_incomplete"))

    def _inspection_nr_from_lead(lead_row: dict[str, Any]) -> str:
        url = str(lead_row.get("source_url") or "").strip()
        match = re.search(r"id=([0-9.]+)", url, flags=re.IGNORECASE)
        if match:
            return match.group(1)
        return str(lead_row.get("inspection_nr") or lead_row.get("activity_nr") or "").strip()

    def _add_debug_row(
        lead_row: dict[str, Any],
        matched: bool,
        match_reason: str,
        resolved_cbsa: str = "",
    ) -> None:
        if not include_debug:
            return
        debug_rows.append(
            {
                "inspection_nr": _inspection_nr_from_lead(lead_row),
                "lead_key": str(lead_row.get("lead_key") or lead_row.get("activity_nr") or "").strip(),
                "site_city": str(lead_row.get("site_city") or "").strip(),
                "site_zip": extract_zip5_from_text(lead_row.get("site_zip")) or "",
                "resolved_cbsa": resolved_cbsa,
                "territory_code": canonical_code or requested_code,
                "matched": "Y" if matched else "N",
                "match_reason": match_reason,
                "dataset_incomplete": dataset_incomplete,
            }
        )

    for lead in leads:
        state = str(lead.get("site_state") or "").upper()
        if states and state not in states:
            stats["excluded_state"] += 1
            _add_debug_row(lead, matched=False, match_reason="STATE_NO_MATCH")
            continue

        if kind == "CBSA_SET" and cbsa_set:
            resolution = resolve_lead_cbsa(lead)
            if resolution.cbsa:
                if resolution.cbsa in cbsa_set:
                    filtered.append(lead)
                    stats["matched_by_cbsa"] += 1
                    match_reason = "CBSA_MATCH"
                    if resolution.used_mail_fallback:
                        match_reason = "FALLBACK_USED|CBSA_MATCH"
                    _add_debug_row(
                        lead,
                        matched=True,
                        match_reason=match_reason,
                        resolved_cbsa=resolution.cbsa,
                    )
                    continue
                stats["excluded_territory"] += 1
                no_match_reason = "CBSA_NO_MATCH"
                if resolution.used_mail_fallback:
                    no_match_reason = "FALLBACK_USED|CBSA_NO_MATCH"
                _add_debug_row(
                    lead,
                    matched=False,
                    match_reason=no_match_reason,
                    resolved_cbsa=resolution.cbsa,
                )
                continue

            # CBSA unavailable. Fall back to legacy pattern matching only in this case.
            office_text = " ".join(
                str(lead.get(field) or "")
                for field in ("area_office", "office", "osha_office")
            )
            if office_text.strip() and office_patterns and _matches_any(office_text, office_patterns):
                filtered.append(lead)
                stats["matched_by_office"] += 1
                _add_debug_row(
                    lead,
                    matched=True,
                    match_reason=f"FALLBACK_USED|OFFICE_MATCH|{resolution.reason}",
                )
                continue

            fallback_fields = [
                lead.get("site_city"),
                lead.get("mail_city"),
                lead.get("site_address1"),
            ]
            city_text = " ".join(_normalize_location_text(value) for value in fallback_fields if value)
            if fallback_patterns and _matches_any(city_text, fallback_patterns):
                filtered.append(lead)
                stats["matched_by_fallback"] += 1
                _add_debug_row(
                    lead,
                    matched=True,
                    match_reason=f"FALLBACK_USED|{resolution.reason}",
                )
                continue

            stats["excluded_territory"] += 1
            _add_debug_row(
                lead,
                matched=False,
                match_reason=resolution.reason,
            )
            continue

        office_text = " ".join(
            str(lead.get(field) or "")
            for field in ("area_office", "office", "osha_office")
        )
        if office_text.strip() and office_patterns and _matches_any(office_text, office_patterns):
            filtered.append(lead)
            stats["matched_by_office"] += 1
            _add_debug_row(lead, matched=True, match_reason="OFFICE_MATCH")
            continue

        fallback_fields = [
            lead.get("site_city"),
            lead.get("mail_city"),
            lead.get("site_address1"),
        ]
        city_text = " ".join(_normalize_location_text(value) for value in fallback_fields if value)
        if fallback_patterns and _matches_any(city_text, fallback_patterns):
            filtered.append(lead)
            stats["matched_by_fallback"] += 1
            _add_debug_row(lead, matched=True, match_reason="FALLBACK_CITY_MATCH")
            continue

        stats["excluded_territory"] += 1
        _add_debug_row(lead, matched=False, match_reason="TERRITORY_NO_MATCH")

    if include_debug:
        return filtered, stats, debug_rows
    return filtered, stats


def filter_by_cbsa_allowlist(
    leads: list[dict],
    cbsa_allowlist: list[str],
    include_debug: bool = False,
) -> tuple[list[dict], dict[str, int]] | tuple[list[dict], dict[str, int], list[dict[str, Any]]]:
    cbsa_set = {
        "".join(ch for ch in str(cbsa or "").strip() if ch.isdigit()).zfill(5)
        for cbsa in (cbsa_allowlist or [])
        if str(cbsa or "").strip()
    }
    filtered: list[dict] = []
    stats = {
        "excluded_state": 0,
        "excluded_territory": 0,
        "matched_by_office": 0,
        "matched_by_fallback": 0,
        "matched_by_cbsa": 0,
    }
    debug_rows: list[dict[str, Any]] = []

    dataset_incomplete = False
    if include_debug:
        dataset_status = zip_cbsa_dataset_status()
        dataset_incomplete = bool(dataset_status.get("dataset_incomplete"))

    def _inspection_nr_from_lead(lead_row: dict[str, Any]) -> str:
        url = str(lead_row.get("source_url") or "").strip()
        match = re.search(r"id=([0-9.]+)", url, flags=re.IGNORECASE)
        if match:
            return match.group(1)
        return str(lead_row.get("inspection_nr") or lead_row.get("activity_nr") or "").strip()

    def _add_debug_row(
        lead_row: dict[str, Any],
        matched: bool,
        match_reason: str,
        resolved_cbsa: str = "",
    ) -> None:
        if not include_debug:
            return
        debug_rows.append(
            {
                "inspection_nr": _inspection_nr_from_lead(lead_row),
                "lead_key": str(lead_row.get("lead_key") or lead_row.get("activity_nr") or "").strip(),
                "site_city": str(lead_row.get("site_city") or "").strip(),
                "site_zip": extract_zip5_from_text(lead_row.get("site_zip")) or "",
                "resolved_cbsa": resolved_cbsa,
                "territory_code": "CBSA_ALLOWLIST",
                "matched": "Y" if matched else "N",
                "match_reason": match_reason,
                "dataset_incomplete": dataset_incomplete,
            }
        )

    if not cbsa_set:
        for lead in leads:
            stats["excluded_territory"] += 1
            _add_debug_row(lead, matched=False, match_reason="CBSA_ALLOWLIST_EMPTY")
        if include_debug:
            return [], stats, debug_rows
        return [], stats

    for lead in leads:
        resolution = resolve_lead_cbsa(lead)
        if resolution.cbsa:
            if resolution.cbsa in cbsa_set:
                filtered.append(lead)
                stats["matched_by_cbsa"] += 1
                reason = "CBSA_MATCH"
                if resolution.used_mail_fallback:
                    reason = "FALLBACK_USED|CBSA_MATCH"
                _add_debug_row(lead, matched=True, match_reason=reason, resolved_cbsa=resolution.cbsa)
                continue
            stats["excluded_territory"] += 1
            reason = "CBSA_MISMATCH"
            if resolution.used_mail_fallback:
                reason = "FALLBACK_USED|CBSA_MISMATCH"
            _add_debug_row(lead, matched=False, match_reason=reason, resolved_cbsa=resolution.cbsa)
            continue

        stats["excluded_territory"] += 1
        reason = f"CBSA_UNRESOLVED|{resolution.reason}"
        if resolution.used_mail_fallback:
            reason = f"FALLBACK_USED|{reason}"
        _add_debug_row(lead, matched=False, match_reason=reason, resolved_cbsa="")

    if include_debug:
        return filtered, stats, debug_rows
    return filtered, stats


def merge_territory_definition(code: str, definition: dict, path: str = "territories.json") -> None:
    json_path = Path(path)
    current: dict[str, Any] = {}

    if json_path.exists():
        with open(json_path, "r", encoding="utf-8") as f:
            loaded = json.load(f)
        if isinstance(loaded, dict):
            current = loaded

    current[code] = definition
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(current, f, indent=2)
        f.write("\n")
