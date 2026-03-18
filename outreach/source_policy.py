from __future__ import annotations

from typing import Iterable


LEGACY_SOURCE_TOKEN_FAMILIES: dict[str, str] = {
    "AIHA": "AIHA",
    "APOLLO": "APOLLO",
    "BCSP": "BCSP",
    "BLUEBOOK": "BLUEBOOK",
    "OHS_BG": "OHS_BG",
    "OSHA_NEWS": "OSHA_NEWS",
    "SEED": "SEED",
    "STATE_LIC": "STATE_LIC",
    "AI_ASSIST": "AI_ASSIST",
    "MANUAL": "MANUAL",
}

FIXED_DEFAULT_SOURCE_TOKENS: tuple[str, ...] = (
    "AIHA",
    "APOLLO",
    "BLUEBOOK",
    "OHS_BG",
    "STATE_LIC",
)
CANONICAL_PUBLIC_CONTACT_SOURCE_TOKENS: tuple[str, ...] = ("MANUAL",)
CONSULTANT_PRIMARY_SOURCES: tuple[str, ...] = ()
CONSULTANT_OVERFLOW_SOURCES: tuple[str, ...] = ()
CONSULTANT_SECONDARY_SOURCES: tuple[str, ...] = ()
CONSULTANT_BACKLOG_SOURCE_TOKENS: tuple[str, ...] = ()
FIXED_DEFAULT_SOURCE_FAMILIES = tuple(
    LEGACY_SOURCE_TOKEN_FAMILIES[token]
    for token in FIXED_DEFAULT_SOURCE_TOKENS
    if token in LEGACY_SOURCE_TOKEN_FAMILIES
)
FIXED_DEFAULT_SOURCE_FAMILY_SET = set(FIXED_DEFAULT_SOURCE_FAMILIES)
CANONICAL_PUBLIC_CONTACT_SOURCE_FAMILIES = tuple(
    LEGACY_SOURCE_TOKEN_FAMILIES[token]
    for token in CANONICAL_PUBLIC_CONTACT_SOURCE_TOKENS
    if token in LEGACY_SOURCE_TOKEN_FAMILIES
)
CANONICAL_PUBLIC_CONTACT_SOURCE_FAMILY_SET = set(CANONICAL_PUBLIC_CONTACT_SOURCE_FAMILIES)


def normalize_source_token(value: str) -> str:
    return (value or "").strip().upper()


def supported_autogrow_sources(include_unimplemented: bool = True) -> tuple[str, ...]:
    del include_unimplemented
    return ()


def implemented_autogrow_sources() -> tuple[str, ...]:
    return ()


def unimplemented_autogrow_sources() -> tuple[str, ...]:
    return ()


def is_autogrow_source_implemented(token: str) -> bool:
    del token
    return False


def autogrow_source_prefix_map(include_unimplemented: bool = False) -> dict[str, str]:
    del include_unimplemented
    return {}


def validate_autogrow_source_tokens(configured_tokens: Iterable[str]) -> tuple[list[str], list[str]]:
    invalid: list[str] = []
    seen: list[str] = []
    for raw in configured_tokens:
        token = normalize_source_token(raw)
        if not token or token in seen:
            continue
        seen.append(token)
        invalid.append(token)
    return invalid, []


def source_fit_defaults(source: str) -> tuple[str, int]:
    text = (source or "").strip().lower()
    if text.startswith("ai_assist_manual"):
        return "recoverable_consultant", 1
    if text.startswith("firm_site_") or text.startswith("manual_") or text.startswith("public_site_"):
        return "recoverable_consultant", 1
    if text.startswith("state_lic_work_email"):
        return "adjacent_contractor", 1
    if text.startswith("state_lic"):
        return "adjacent_contractor", 0
    if text.startswith("apollo"):
        return "core_consultant", 1
    if text.startswith("aiha_consultants_listing:"):
        return "recoverable_consultant", 1
    if text.startswith("bluebook:"):
        return "recoverable_consultant", 1
    if text.startswith("ohs_buyers_guide:"):
        return "recoverable_consultant", 1
    return "recoverable_consultant", 1


def source_family(source: str) -> str:
    text = (source or "").strip().lower()
    if not text:
        return "UNKNOWN"
    if text.startswith("ai_assist_manual"):
        return "AI_ASSIST"
    if text.startswith("firm_site_") or text.startswith("manual_") or text.startswith("public_site_"):
        return "MANUAL"
    if text.startswith("aiha_consultants_listing"):
        return "AIHA"
    if text.startswith("bluebook"):
        return "BLUEBOOK"
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
    if not text:
        return "UNKNOWN"
    return LEGACY_SOURCE_TOKEN_FAMILIES.get(text, "UNKNOWN")


def autogrow_source_order(configured_tokens: Iterable[str]) -> list[str]:
    ordered: list[str] = []
    for raw in configured_tokens:
        token = normalize_source_token(raw)
        if token and token not in ordered:
            ordered.append(token)
    return sorted(ordered)


def counts_toward_consultant_backlog(source_token: str) -> bool:
    return normalize_source_token(source_token) in CONSULTANT_BACKLOG_SOURCE_TOKENS


def is_secondary_source(source_token: str) -> bool:
    return normalize_source_token(source_token) in CONSULTANT_SECONDARY_SOURCES


def source_uses_fixed_defaults(source: str) -> bool:
    return source_family(source) in FIXED_DEFAULT_SOURCE_FAMILY_SET


def uses_canonical_public_contact_resolution(source: str) -> bool:
    family = source_family(source)
    if family != "UNKNOWN":
        return family in CANONICAL_PUBLIC_CONTACT_SOURCE_FAMILY_SET
    token = normalize_source_token(source)
    return token in CANONICAL_PUBLIC_CONTACT_SOURCE_FAMILY_SET
