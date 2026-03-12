from __future__ import annotations

import html
import re
from collections import Counter
from datetime import datetime, timezone
from typing import Any


PRIORITY_ORDER = {"SUPPRESS": -1, "LOW": 0, "MEDIUM": 1, "HIGH": 2}
EVENT_CLASS_ORDER = {
    "other": 0,
    "inspection": 1,
    "planned": 2,
    "referral": 3,
    "complaint": 4,
    "accident": 5,
}
COMPANY_SUFFIXES = {
    "inc",
    "incorporated",
    "llc",
    "ltd",
    "limited",
    "co",
    "company",
    "corp",
    "corporation",
    "lp",
    "llp",
    "plc",
}
REASON_SENTENCES = {
    "referral_or_complaint": "Active worker or agency attention is likely.",
    "multi_employer_site": "Multiple same-site signals suggest a concentrated operational issue.",
    "serious_willful_repeat": "Serious or repeat context raises urgency.",
    "naics_emphasis": "This industry is in a higher OSHA attention lane.",
    "planned_low_hazard": "This appears lower urgency and more routine.",
    "open_no_insp_low_info": "Limited detail keeps urgency lower for now.",
    "rules_default": "This is one of the stronger recent operational signals in the territory.",
}


def _parse_dt(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    for fmt in ("%Y-%m-%d",):
        try:
            return datetime.strptime(text, fmt).replace(tzinfo=timezone.utc)
        except Exception:
            pass
    try:
        dt = datetime.fromisoformat(text)
    except Exception:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _timestamp_value(value: Any) -> float:
    dt = _parse_dt(value)
    if not dt:
        return 0.0
    return dt.timestamp()


def _stable_key(row: dict[str, Any]) -> str:
    return str(row.get("lead_key") or row.get("activity_nr") or row.get("lead_id") or "").strip()


def _priority_rank(row: dict[str, Any]) -> int:
    token = str(row.get("effective_priority") or row.get("rules_priority") or "LOW").strip().upper()
    return int(PRIORITY_ORDER.get(token, 0))


def event_class_token(value: Any) -> str:
    text = " ".join(str(value or "").strip().lower().split())
    if not text:
        return "other"
    if "accident" in text:
        return "accident"
    if "complaint" in text:
        return "complaint"
    if "referral" in text:
        return "referral"
    if "planned" in text or "programmed" in text:
        return "planned"
    if "inspection" in text:
        return "inspection"
    return "other"


def _event_class_rank(row: dict[str, Any]) -> int:
    return int(EVENT_CLASS_ORDER.get(event_class_token(row.get("inspection_type")), 0))


def _event_date_value(row: dict[str, Any]) -> float:
    return _timestamp_value(row.get("date_opened"))


def _recency_value(row: dict[str, Any]) -> float:
    return _timestamp_value(row.get("last_seen_at") or row.get("first_seen_at") or row.get("date_opened"))


def sort_key(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        -_priority_rank(row),
        -_event_class_rank(row),
        -_event_date_value(row),
        -_recency_value(row),
        _stable_key(row),
    )


def sort_rows_for_digest(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    ordered = list(rows or [])
    ordered.sort(key=sort_key)
    return ordered


def _normalize_company_root(value: Any) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return ""
    text = re.sub(r"[^\w\s]", " ", text)
    text = " ".join(text.split())
    if not text:
        return ""
    tokens = [token for token in text.split(" ") if token not in COMPANY_SUFFIXES]
    return " ".join(tokens).strip()


def _normalize_city(value: Any) -> str:
    return " ".join(str(value or "").strip().lower().split())


def _normalize_state(value: Any) -> str:
    return str(value or "").strip().upper()


def _normalize_event_day(value: Any) -> str:
    dt = _parse_dt(value)
    if not dt:
        return ""
    return dt.date().isoformat()


def collapse_group_key(row: dict[str, Any]) -> tuple[str, str, str, str, str] | None:
    company = _normalize_company_root(row.get("establishment_name"))
    city = _normalize_city(row.get("site_city"))
    state = _normalize_state(row.get("site_state"))
    signal_type = event_class_token(row.get("inspection_type"))
    event_day = _normalize_event_day(row.get("date_opened"))
    if not company or not city or not state or not signal_type or not event_day:
        return None
    return (company, city, state, signal_type, event_day)


def _rule_reason_sentence(row: dict[str, Any]) -> str:
    for token in list(row.get("triage_overlay_reasons") or []):
        key = str(token or "").strip().lower()
        sentence = REASON_SENTENCES.get(key)
        if sentence:
            return sentence
    return REASON_SENTENCES["rules_default"]


def reason_sentence(row: dict[str, Any]) -> str:
    decision_source = str(row.get("decision_source") or "").strip().lower()
    delta_direction = str(row.get("delta_direction") or "").strip().lower()
    ai_reason = " ".join(str(row.get("ai_reason") or "").strip().split())
    if decision_source == "ai_overlay" and delta_direction in {"raised", "lowered"} and ai_reason:
        text = ai_reason
        if text[-1] not in ".!?":
            text += "."
        return text
    return _rule_reason_sentence(row)


def _state_summary(rows: list[dict[str, Any]]) -> str:
    counts = Counter()
    for row in rows:
        state = _normalize_state(row.get("site_state"))
        if state:
            counts[state] += 1
    if not counts:
        return "the configured territory"
    ordered = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    labels = [state for state, _count in ordered[:2]]
    if len(labels) == 1:
        return labels[0]
    return " and ".join(labels)


def _event_summary(rows: list[dict[str, Any]]) -> str:
    counts = Counter(event_class_token(row.get("inspection_type")) for row in rows)
    counts.pop("other", None)
    if not counts:
        return "mixed signals"
    ordered = sorted(counts.items(), key=lambda item: (-item[1], -EVENT_CLASS_ORDER.get(item[0], 0), item[0]))
    labels = [token.replace("_", " ") for token, _count in ordered[:2]]
    if len(labels) == 1:
        return labels[0]
    return " and ".join(labels)


def _operational_implication(rows: list[dict[str, Any]]) -> str:
    tokens = {event_class_token(row.get("inspection_type")) for row in rows}
    if {"accident", "complaint", "referral"} & tokens:
        return "Operationally, this points to near-term worker or agency attention and merits fast site follow-up."
    if tokens == {"planned"} or tokens == {"planned", "inspection"}:
        return "Operationally, this looks more like routine oversight than an acute escalation."
    return "Operationally, this helps focus follow-up on the strongest site signals first."


def _intro_text(
    *,
    raw_row_count: int,
    visible_rows: list[dict[str, Any]],
    section_kind: str,
) -> str:
    if not visible_rows:
        if section_kind in {"starter_snapshot", "snapshot_not_new"}:
            return "No recent OSHA signals were available in the snapshot window."
        if section_kind == "daily_new":
            return "No new OSHA activity signals were rendered today."
        return ""

    visible_count = len(visible_rows)
    if section_kind == "starter_snapshot":
        opening = "These recent signals are context from the starter snapshot and were not newly observed today."
    elif section_kind == "snapshot_not_new":
        opening = "These recent signals are context from the snapshot window and were not newly observed today."
    else:
        if raw_row_count != visible_count:
            opening = (
                f"{raw_row_count} newly observed signals condense into {visible_count} distinct matters "
                "after collapsing similar same-day entries."
            )
        else:
            opening = f"{visible_count} newly observed signals stand out today."
    states = _state_summary(visible_rows)
    events = _event_summary(visible_rows)
    implication = _operational_implication(visible_rows)
    return f"{opening} Most activity is in {states}, led by {events} signals. {implication}"


def build_digest_presentation(
    rows: list[dict[str, Any]],
    *,
    section_kind: str,
    top_pick_limit: int = 5,
) -> dict[str, Any]:
    groups: dict[tuple[str, str, str, str, str] | str, list[dict[str, Any]]] = {}
    for index, row in enumerate(list(rows or [])):
        key = collapse_group_key(row)
        groups.setdefault(key if key is not None else f"row:{index}", []).append(row)

    visible_rows: list[dict[str, Any]] = []
    collapsed_groups: list[dict[str, Any]] = []
    for members in groups.values():
        ordered_members = sort_rows_for_digest(members)
        representative = dict(ordered_members[0])
        representative_key = _stable_key(representative)
        hidden_keys = sorted(
            [_stable_key(row) for row in ordered_members[1:] if _stable_key(row)],
        )
        representative["presentation_representative_key"] = representative_key
        representative["presentation_collapsed_member_keys"] = hidden_keys
        representative["presentation_collapsed_hidden_count"] = len(hidden_keys)
        representative["presentation_reason_sentence"] = reason_sentence(representative)
        representative["presentation_top_pick_rank"] = None
        visible_rows.append(representative)
        collapsed_groups.append(
            {
                "representative_key": representative_key,
                "hidden_member_keys": hidden_keys,
                "hidden_count": len(hidden_keys),
            }
        )

    visible_rows = sort_rows_for_digest(visible_rows)
    top_picks: list[dict[str, Any]] = []
    top_pick_heading = None
    if section_kind in {"daily_new", "starter_snapshot", "snapshot_not_new"} and visible_rows:
        count = max(0, min(int(top_pick_limit), len(visible_rows)))
        heading = "Most important today" if section_kind == "daily_new" else "Most important in recent activity"
        for rank, row in enumerate(visible_rows[:count], start=1):
            row["presentation_top_pick_rank"] = rank
            top_picks.append(row)
        top_pick_heading = heading

    intro_text = _intro_text(raw_row_count=len(list(rows or [])), visible_rows=visible_rows, section_kind=section_kind)
    return {
        "raw_row_count": len(list(rows or [])),
        "visible_row_count": len(visible_rows),
        "visible_rows": visible_rows,
        "collapsed_groups": collapsed_groups,
        "top_picks": top_picks,
        "top_pick_heading": top_pick_heading,
        "intro_text": intro_text,
        "intro_html": html.escape(intro_text),
    }
