from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Iterable


_REGISTRY_PATH = Path(__file__).resolve().with_name("autogrow_source_registry.json")


def normalize_source_token(value: str) -> str:
    return (value or "").strip().upper()


@lru_cache(maxsize=1)
def _registry_payload() -> dict:
    raw = json.loads(_REGISTRY_PATH.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"invalid_autogrow_source_registry path={_REGISTRY_PATH}")
    return raw


@lru_cache(maxsize=1)
def autogrow_source_registry() -> dict[str, dict[str, object]]:
    entries = _registry_payload().get("sources") or []
    registry: dict[str, dict[str, object]] = {}
    for item in entries:
        if not isinstance(item, dict):
            continue
        token = normalize_source_token(str(item.get("token") or ""))
        if not token:
            continue
        registry[token] = dict(item)
    return registry


def supported_autogrow_sources(include_unimplemented: bool = True) -> tuple[str, ...]:
    ordered = sorted(
        autogrow_source_registry().items(),
        key=lambda pair: int((pair[1] or {}).get("sort_order") or 0),
    )
    tokens: list[str] = []
    for token, meta in ordered:
        if not include_unimplemented and not bool(meta.get("implemented")):
            continue
        tokens.append(token)
    return tuple(tokens)


def implemented_autogrow_sources() -> tuple[str, ...]:
    return supported_autogrow_sources(include_unimplemented=False)


def unimplemented_autogrow_sources() -> tuple[str, ...]:
    tokens: list[str] = []
    for token, meta in autogrow_source_registry().items():
        if not bool(meta.get("implemented")):
            tokens.append(token)
    return tuple(
        sorted(tokens, key=lambda item: int((autogrow_source_registry().get(item) or {}).get("sort_order") or 0))
    )


def is_autogrow_source_implemented(token: str) -> bool:
    meta = autogrow_source_registry().get(normalize_source_token(token)) or {}
    return bool(meta.get("implemented"))


def autogrow_source_prefix_map(include_unimplemented: bool = False) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for token in supported_autogrow_sources(include_unimplemented=include_unimplemented):
        meta = autogrow_source_registry().get(token) or {}
        prefix = str(meta.get("prefix") or "").strip().lower()
        if prefix:
            mapping[token] = prefix
    return mapping


def validate_autogrow_source_tokens(configured_tokens: Iterable[str]) -> tuple[list[str], list[str]]:
    valid_supported = set(supported_autogrow_sources(include_unimplemented=True))
    configured: list[str] = []
    invalid: list[str] = []
    unimplemented: list[str] = []
    for raw in configured_tokens:
        token = normalize_source_token(raw)
        if not token:
            continue
        if token in configured:
            continue
        configured.append(token)
        if token not in valid_supported:
            invalid.append(token)
            continue
        if not is_autogrow_source_implemented(token):
            unimplemented.append(token)
    return invalid, unimplemented


def _consultant_bucket_sources(bucket: str) -> tuple[str, ...]:
    tokens: list[str] = []
    for token in supported_autogrow_sources(include_unimplemented=False):
        meta = autogrow_source_registry().get(token) or {}
        if str(meta.get("consultant_bucket") or "").strip().lower() == bucket:
            tokens.append(token)
    return tuple(tokens)


def _unique_source_tokens(tokens: Iterable[str]) -> tuple[str, ...]:
    ordered: list[str] = []
    for raw in tokens:
        token = normalize_source_token(raw)
        if token and token not in ordered:
            ordered.append(token)
    return tuple(ordered)


def source_fit_defaults(source: str) -> tuple[str, int]:
    text = (source or "").strip().lower()
    if text.startswith("ai_assist_manual"):
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
    if text in {"SEED", "AI_ASSIST"}:
        return text
    meta = autogrow_source_registry().get(text) or {}
    family = str(meta.get("family") or "").strip().upper()
    return family or "UNKNOWN"


CONSULTANT_PRIMARY_SOURCES: tuple[str, ...] = _consultant_bucket_sources("primary")
CONSULTANT_OVERFLOW_SOURCES: tuple[str, ...] = _consultant_bucket_sources("overflow")
CONSULTANT_SECONDARY_SOURCES: tuple[str, ...] = _consultant_bucket_sources("secondary")
CONSULTANT_BACKLOG_SOURCE_TOKENS: tuple[str, ...] = _unique_source_tokens(
    ["STATE_LIC", *CONSULTANT_PRIMARY_SOURCES, *CONSULTANT_OVERFLOW_SOURCES, "BLUEBOOK"]
)
FIXED_DEFAULT_SOURCE_TOKENS: tuple[str, ...] = _unique_source_tokens(
    ["STATE_LIC", "APOLLO", *CONSULTANT_PRIMARY_SOURCES, "OHS_BG", "BLUEBOOK"]
)
CANONICAL_PUBLIC_CONTACT_SOURCE_TOKENS: tuple[str, ...] = _unique_source_tokens(
    [*CONSULTANT_PRIMARY_SOURCES, "BLUEBOOK"]
)
FIXED_DEFAULT_SOURCE_FAMILIES: tuple[str, ...] = tuple(
    source_family_from_token(token)
    for token in FIXED_DEFAULT_SOURCE_TOKENS
    if source_family_from_token(token) != "UNKNOWN"
)
FIXED_DEFAULT_SOURCE_FAMILY_SET = set(FIXED_DEFAULT_SOURCE_FAMILIES)
CANONICAL_PUBLIC_CONTACT_SOURCE_FAMILIES: tuple[str, ...] = tuple(
    source_family_from_token(token)
    for token in CANONICAL_PUBLIC_CONTACT_SOURCE_TOKENS
    if source_family_from_token(token) != "UNKNOWN"
)
CANONICAL_PUBLIC_CONTACT_SOURCE_FAMILY_SET = set(CANONICAL_PUBLIC_CONTACT_SOURCE_FAMILIES)


def autogrow_source_order(configured_tokens: Iterable[str]) -> list[str]:
    unique: list[str] = []
    for raw in configured_tokens:
        token = normalize_source_token(raw)
        if token and token not in unique:
            unique.append(token)

    order_map = {
        token: int((autogrow_source_registry().get(token) or {}).get("sort_order") or 9999)
        for token in unique
    }
    return sorted(unique, key=lambda token: (order_map.get(token, 9999), token))


def counts_toward_consultant_backlog(source_token: str) -> bool:
    token = normalize_source_token(source_token)
    return token in CONSULTANT_BACKLOG_SOURCE_TOKENS


def is_secondary_source(source_token: str) -> bool:
    return normalize_source_token(source_token) in CONSULTANT_SECONDARY_SOURCES


def source_uses_fixed_defaults(source: str) -> bool:
    family = source_family(source)
    return family in FIXED_DEFAULT_SOURCE_FAMILY_SET


def uses_canonical_public_contact_resolution(source: str) -> bool:
    family = source_family(source)
    if family != "UNKNOWN":
        return family in CANONICAL_PUBLIC_CONTACT_SOURCE_FAMILY_SET
    token = normalize_source_token(source)
    return token in CANONICAL_PUBLIC_CONTACT_SOURCE_FAMILY_SET
