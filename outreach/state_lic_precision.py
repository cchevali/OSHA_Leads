from __future__ import annotations

import re
from typing import Any

import seed_recipients_pools as pools


STATE_LIC_PRECISION_MODES = ("consultant_fit", "packet_eligible", "send_eligible")
STATE_LIC_PACKET_IDENTITY_ANCHORS = (
    "phone",
    "address",
    "city",
    "license_number",
    "seed_source_url",
    "source_record_id",
)
STATE_LIC_PACKET_NEGATIVE_EXCLUSION_FAMILIES = (
    "hvac",
    "air_conditioning",
    "cooling",
    "heating",
    "refrigeration",
    "climate",
    "mechanical",
    "ventilation",
    "duct",
    "contractor",
    "chiller",
)
STATE_LIC_HARD_NEGATIVE_CLASS_TX_ENV_AC = "tx_environmental_air_conditioning"
STATE_LIC_POSITIVE_FAMILIES: tuple[tuple[str, int, tuple[str, ...]], ...] = (
    ("safety", 5, ("safety",)),
    ("industrial_hygiene", 5, ("industrial hygiene", "industrial hygienist", "industrial hygien", "ih")),
    ("osha", 5, ("osha",)),
    ("ehs_hse", 4, ("ehs", "hse")),
    ("compliance", 4, ("compliance",)),
    ("risk", 3, ("risk", "risk management")),
    ("loss_control", 3, ("loss control",)),
    ("training", 3, ("training", "trainer")),
    ("consulting", 3, ("consulting", "consultant", "consult", "consultancy", "advisory", "advisor", "adviser")),
    ("occupational", 4, ("occupational", "occupational safety", "occupational health")),
    ("credentials", 5, ("cih", "csp", "chst", "sms")),
)
STATE_LIC_NEGATIVE_FAMILIES: tuple[tuple[str, int, tuple[str, ...]], ...] = (
    ("hvac", 5, ("hvac",)),
    ("air_conditioning", 5, ("air conditioning", "air conditioner", "a c")),
    ("cooling", 4, ("cooling",)),
    ("heating", 4, ("heating",)),
    ("refrigeration", 4, ("refrigeration",)),
    ("climate", 3, ("climate",)),
    ("mechanical", 4, ("mechanical",)),
    ("ventilation", 4, ("ventilation",)),
    ("duct", 4, ("duct", "ductwork")),
    ("contractor", 2, ("contractor", "contracting")),
    ("chiller", 4, ("chill", "chiller")),
)


def _normalize_text(value: Any) -> str:
    return " ".join(str(value or "").replace("\r", " ").replace("\n", " ").split()).strip()


def _normalize_state(value: Any) -> str:
    text = _normalize_text(value).upper()
    return text if len(text) == 2 else text


def _classifier_text(*values: Any) -> str:
    compact = " ".join(_normalize_text(value) for value in values if _normalize_text(value))
    lowered = compact.lower()
    if not lowered:
        return " "
    normalized = re.sub(r"[^a-z0-9]+", " ", lowered).strip()
    return f" {normalized} " if normalized else " "


def _token_present(text: str, needle: str) -> bool:
    normalized = _classifier_text(needle)
    token = normalized.strip()
    return bool(token) and normalized in text


def _match_weighted_families(
    text: str,
    groups: tuple[tuple[str, int, tuple[str, ...]], ...],
) -> tuple[list[str], int]:
    matched: list[str] = []
    score = 0
    for family, weight, needles in groups:
        if any(_token_present(text, needle) for needle in needles):
            matched.append(family)
            score += int(weight)
    return matched, score


def _license_class_norm(row: dict[str, Any]) -> str:
    values = [
        _normalize_text(row.get("license_type") or ""),
        _normalize_text(row.get("license_subtype") or ""),
        _normalize_text(row.get("title") or ""),
    ]
    parts: list[str] = []
    seen: set[str] = set()
    for value in values:
        if not value:
            continue
        normalized = _classifier_text(value).strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        parts.append(normalized)
    return " | ".join(parts)


def _hard_negative_class(row: dict[str, Any], license_class_norm: str, classifier_text: str) -> str:
    state = _normalize_state(row.get("state") or "")
    if state != "TX":
        return ""
    combined = f" {license_class_norm} {classifier_text} "
    if (
        " environmental air conditioning " in combined
        or " a c contractor " in combined
        or " ac contractor " in combined
        or " air conditioning contractor " in combined
    ):
        return STATE_LIC_HARD_NEGATIVE_CLASS_TX_ENV_AC
    return ""


def _has_nonfree_work_email(row: dict[str, Any]) -> bool:
    email = _normalize_text(row.get("email") or row.get("contact_email") or "").lower()
    if not email or "@" not in email:
        return False
    domain = email.rsplit("@", 1)[1].strip()
    return bool(domain) and domain not in pools.FREE_EMAIL_DOMAINS


def _strong_identity_anchor_count(row: dict[str, Any]) -> int:
    count = 0
    for field in STATE_LIC_PACKET_IDENTITY_ANCHORS:
        if _normalize_text(row.get(field) or ""):
            count += 1
    return count


def classify_state_lic_row(row: dict[str, Any], *, mode: str = "consultant_fit") -> dict[str, Any]:
    if mode not in STATE_LIC_PRECISION_MODES:
        raise ValueError(f"invalid_state_lic_precision_mode={mode}")

    firm = _normalize_text(row.get("firm") or row.get("company_name") or row.get("business_name") or "")
    state = _normalize_state(row.get("state") or "")
    website = _normalize_text(row.get("website") or "")
    city = _normalize_text(row.get("city") or "")
    license_class_norm = _license_class_norm(row)
    classifier_text = _classifier_text(
        firm,
        row.get("owner_name") or row.get("contact_name") or "",
        row.get("title") or row.get("contact_role") or "",
        row.get("license_type") or "",
        row.get("license_subtype") or "",
        city,
        row.get("source_detail") or row.get("source_record_id") or row.get("prospect_id") or "",
        row.get("business_address_line1") or row.get("address") or "",
    )
    positive_families, positive_score = _match_weighted_families(classifier_text, STATE_LIC_POSITIVE_FAMILIES)
    negative_families, negative_score = _match_weighted_families(classifier_text, STATE_LIC_NEGATIVE_FAMILIES)
    hard_negative_class = _hard_negative_class(row, license_class_norm, classifier_text)
    strong_identity_anchor_count = _strong_identity_anchor_count(row)
    strong_identity = bool(firm and state and strong_identity_anchor_count >= 2)
    has_positive = bool(positive_families)
    has_negative = any(family in STATE_LIC_PACKET_NEGATIVE_EXCLUSION_FAMILIES for family in negative_families)
    consultant_fit = bool(
        not hard_negative_class
        and has_positive
        and int(positive_score) > int(negative_score)
    )

    packet_exclusion_reason = ""
    if not firm:
        packet_exclusion_reason = "missing_firm"
    elif not state:
        packet_exclusion_reason = "missing_state"
    elif hard_negative_class:
        packet_exclusion_reason = "hard_negative_class"
    elif has_negative:
        packet_exclusion_reason = "negative_keyword_family"
    elif not website and not has_positive and not strong_identity:
        packet_exclusion_reason = "blank_website_no_positive_evidence"
    packet_eligible = packet_exclusion_reason == ""
    send_eligible = bool(consultant_fit and _has_nonfree_work_email(row))

    fit_reasons: list[str] = []
    if positive_families:
        fit_reasons.extend(f"+{family}" for family in positive_families)
    else:
        fit_reasons.append("no_positive_signal")
    fit_reasons.extend(f"-{family}" for family in negative_families)
    if hard_negative_class:
        fit_reasons.append(f"!{hard_negative_class}")

    return {
        "state_lic_license_class_norm": license_class_norm,
        "state_lic_hard_negative_class": hard_negative_class,
        "state_lic_positive_families": list(positive_families),
        "state_lic_negative_families": list(negative_families),
        "state_lic_evidence_score": int(positive_score) - int(negative_score),
        "state_lic_consultant_fit": bool(consultant_fit),
        "state_lic_packet_eligible": bool(packet_eligible),
        "state_lic_send_eligible": bool(send_eligible),
        "state_lic_packet_exclusion_reason": packet_exclusion_reason,
        "state_lic_positive_signal_count": len(positive_families),
        "state_lic_negative_signal_count": len(negative_families),
        "state_lic_strong_identity_anchor_count": int(strong_identity_anchor_count),
        "state_lic_strong_identity": bool(strong_identity),
        "state_lic_fit_status": "consultant_candidate" if consultant_fit else "fit_mismatch",
        "state_lic_fit_score": int(positive_score) - int(negative_score),
        "state_lic_fit_reasons": ",".join(fit_reasons),
        "state_lic_consultant_eligible": bool(consultant_fit),
    }
