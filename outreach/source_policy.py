from __future__ import annotations

from typing import Iterable


CONSULTANT_PRIMARY_SOURCES: tuple[str, ...] = ("AIHA", "OHS_BG")
CONSULTANT_OVERFLOW_SOURCES: tuple[str, ...] = ("APOLLO",)
CONSULTANT_SECONDARY_SOURCES: tuple[str, ...] = ("BCSP", "OSHA_NEWS", "STATE_LIC")


def normalize_source_token(value: str) -> str:
    return (value or "").strip().upper()


def source_fit_defaults(source: str) -> tuple[str, int]:
    text = (source or "").strip().lower()
    if text.startswith("state_lic"):
        return "adjacent_contractor", 0
    if text.startswith("apollo"):
        return "core_consultant", 1
    if text.startswith("aiha_consultants_listing:"):
        return "recoverable_consultant", 1
    if text.startswith("ohs_buyers_guide:"):
        return "recoverable_consultant", 1
    return "recoverable_consultant", 1


def source_family(source: str) -> str:
    text = (source or "").strip().lower()
    if not text:
        return "UNKNOWN"
    if text.startswith("aiha_consultants_listing"):
        return "AIHA"
    if text.startswith("ohs_buyers_guide"):
        return "OHS_BG"
    if text.startswith("apollo"):
        return "APOLLO"
    if text.startswith("bcsp"):
        return "BCSP"
    if text.startswith("osha_news"):
        return "OSHA_NEWS"
    if text.startswith("state_lic"):
        return "STATE_LIC"
    if text == "seed" or text.startswith("seed_recipients_pools"):
        return "SEED"
    return "UNKNOWN"


def source_family_from_token(token: str) -> str:
    text = normalize_source_token(token)
    if text in {"SEED", "AIHA", "OHS_BG", "APOLLO", "BCSP", "OSHA_NEWS", "STATE_LIC"}:
        return text
    return "UNKNOWN"


def autogrow_source_order(configured_tokens: Iterable[str]) -> list[str]:
    unique: list[str] = []
    for raw in configured_tokens:
        token = normalize_source_token(raw)
        if token and token not in unique:
            unique.append(token)

    primary = [token for token in CONSULTANT_PRIMARY_SOURCES if token in unique]
    overflow = [token for token in CONSULTANT_OVERFLOW_SOURCES if token in unique]
    secondary = [token for token in unique if token in CONSULTANT_SECONDARY_SOURCES]
    unknown = [token for token in unique if token not in set(primary + overflow + secondary)]
    return primary + overflow + secondary + unknown


def counts_toward_consultant_backlog(source_token: str) -> bool:
    token = normalize_source_token(source_token)
    if token in CONSULTANT_PRIMARY_SOURCES:
        return True
    if token in CONSULTANT_OVERFLOW_SOURCES:
        return True
    return False


def is_secondary_source(source_token: str) -> bool:
    return normalize_source_token(source_token) in CONSULTANT_SECONDARY_SOURCES
