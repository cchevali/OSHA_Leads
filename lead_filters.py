import json
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any


DEFAULT_TERRITORIES = {
    "TX_TRIANGLE_V1": {
        "description": "Texas Triangle OSHA area offices: Austin, Dallas/Fort Worth, Houston, San Antonio",
        "states": ["TX"],
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

    return definitions


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
        key = str(lead.get("activity_nr") or lead.get("lead_id") or "").strip()
        if not key:
            continue

        current = by_key.get(key)
        if not current:
            by_key[key] = lead
            continue

        current_key = (
            int(current.get("lead_score") or 0),
            _parse_datetime(current.get("first_seen_at")),
            _parse_datetime(current.get("last_seen_at")),
            _parse_datetime(current.get("date_opened")),
        )
        candidate_key = (
            int(lead.get("lead_score") or 0),
            _parse_datetime(lead.get("first_seen_at")),
            _parse_datetime(lead.get("last_seen_at")),
            _parse_datetime(lead.get("date_opened")),
        )

        if candidate_key > current_key:
            by_key[key] = lead

    deduped = sorted(
        by_key.values(),
        key=lambda row: (
            int(row.get("lead_score") or 0),
            _parse_datetime(row.get("date_opened")),
            _parse_datetime(row.get("first_seen_at")),
        ),
        reverse=True,
    )

    removed = len(leads) - len(deduped)
    return deduped, removed


def _matches_any(text: str, patterns: list[str]) -> bool:
    return any(re.search(pattern, text, flags=re.IGNORECASE) for pattern in patterns)


def filter_by_territory(
    leads: list[dict],
    territory_code: str | None,
    definitions: dict[str, dict[str, Any]] | None = None,
) -> tuple[list[dict], dict[str, int]]:
    if not territory_code:
        return list(leads), {
            "excluded_state": 0,
            "excluded_territory": 0,
            "matched_by_office": 0,
            "matched_by_fallback": 0,
        }

    defs = definitions or load_territory_definitions()
    if territory_code not in defs:
        raise ValueError(f"Unknown territory_code='{territory_code}'")

    territory = defs[territory_code]
    states = [s.upper() for s in territory.get("states", [])]
    office_patterns = territory.get("office_patterns", [])
    fallback_patterns = territory.get("fallback_city_patterns", [])

    filtered: list[dict] = []
    stats = {
        "excluded_state": 0,
        "excluded_territory": 0,
        "matched_by_office": 0,
        "matched_by_fallback": 0,
    }

    for lead in leads:
        state = str(lead.get("site_state") or "").upper()
        if states and state not in states:
            stats["excluded_state"] += 1
            continue

        office_text = " ".join(
            str(lead.get(field) or "")
            for field in ("area_office", "office", "osha_office")
        )

        if office_text.strip() and office_patterns and _matches_any(office_text, office_patterns):
            filtered.append(lead)
            stats["matched_by_office"] += 1
            continue

        # Equivalent fallback field: city when office metadata is absent in source record.
        city_text = str(lead.get("site_city") or "")
        if fallback_patterns and _matches_any(city_text, fallback_patterns):
            filtered.append(lead)
            stats["matched_by_fallback"] += 1
            continue

        stats["excluded_territory"] += 1

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
