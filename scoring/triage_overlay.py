from __future__ import annotations

import json
from collections import Counter
from datetime import datetime, timezone
from typing import Any, Callable

from scoring import ai_triage
from scoring import osha_detail_cache


TRIAL_STALE_HIGH_DAYS = 10
TRIAL_STALE_MEDIUM_DAYS = 14
OUTREACH_STALE_DAYS = 21

PROMOTE_TYPES = {"fat/cat", "fat cat", "fat-cat", "accident"}
DEMOTE_TYPES = {"referral", "planned", "unprog rel", "unprogrammed related", "unprog_rel"}


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


def _days_old(item: dict[str, Any]) -> int | None:
    now = datetime.now(timezone.utc)
    for key in ("date_opened", "changed_at", "last_seen_at", "first_seen_at"):
        dt = _parse_dt(item.get(key))
        if not dt:
            continue
        return max(0, int((now - dt).total_seconds() // 86400))
    return None


def _norm_text(value: Any) -> str:
    return " ".join(str(value or "").strip().lower().split())


def _norm_type(value: Any) -> str:
    text = _norm_text(value)
    if not text:
        return ""
    if "fat" in text and "cat" in text:
        return "fat/cat"
    if "accident" in text:
        return "accident"
    if "unprog" in text and "rel" in text:
        return "unprog rel"
    if "unprogrammed" in text and "related" in text:
        return "unprog rel"
    if "referral" in text:
        return "referral"
    if "planned" in text or "programmed" in text:
        return "planned"
    if "complaint" in text:
        return "complaint"
    return text


def _current_priority(item: dict[str, Any]) -> str:
    explicit = _norm_text(item.get("current_priority"))
    if explicit in {"high", "medium", "low", "other"}:
        return explicit
    try:
        score = int(item.get("lead_score") or 0)
    except Exception:
        score = 0
    if score >= 10:
        return "high"
    if score >= 6:
        return "medium"
    if score >= 0:
        return "low"
    return "other"


def _item_key(item: dict[str, Any]) -> str:
    return str(item.get("activity_nr") or item.get("lead_key") or item.get("lead_id") or "").strip()


def _detail_markers(detail_row: dict[str, Any] | None) -> set[str]:
    if not detail_row:
        return set()
    markers = set(osha_detail_cache.parse_marker_json(detail_row.get("emphasis_markers_json")))
    markers.update(osha_detail_cache.parse_marker_json(detail_row.get("related_activity_markers_json")))
    return {str(x).strip().lower() for x in markers if str(x).strip()}


def _has_emphasis_or_severity(markers: set[str]) -> bool:
    return any(m.startswith("emphasis_") or m.startswith("severity_") for m in markers)


def _rules_decision(item: dict[str, Any], detail_row: dict[str, Any] | None, mode: str) -> dict[str, Any]:
    key = _item_key(item)
    current = _current_priority(item)
    detail_type = _norm_type((detail_row or {}).get("inspection_type")) or _norm_type(item.get("inspection_type"))
    markers = _detail_markers(detail_row)
    days_old = _days_old(item)
    reasons: list[str] = []
    action = "keep"
    conf = 0.55

    if detail_type in PROMOTE_TYPES:
        action = "promote_candidate" if current in {"medium", "low", "other"} else "keep"
        conf = 0.98 if detail_type == "accident" else 0.99
        reasons.append("accident" if detail_type == "accident" else "fat_cat")
        return {
            "activity_nr": str(item.get("activity_nr") or "").strip(),
            "lead_key": str(item.get("lead_key") or "").strip(),
            "current_priority": current,
            "action": action,
            "confidence": conf,
            "reasons": reasons[:4],
            "provenance": {"source": "rules_cached_detail"},
            "_rules_type": detail_type,
            "_markers": sorted(markers),
            "_days_old": days_old,
            "_item_key": key,
        }

    if detail_type == "complaint":
        reasons.append("complaint")

    if detail_type in DEMOTE_TYPES:
        reasons.append("unprog_rel" if detail_type == "unprog rel" else detail_type)
        if not _has_emphasis_or_severity(markers):
            if days_old is not None and days_old >= (OUTREACH_STALE_DAYS if mode == "outreach_examples" else TRIAL_STALE_MEDIUM_DAYS):
                reasons.append("stale")
            if current == "high":
                action = "downgrade_to_medium"
                conf = 0.86
                if days_old is not None and days_old >= TRIAL_STALE_HIGH_DAYS:
                    action = "downgrade_to_low"
                    conf = 0.90
            elif current == "medium":
                if mode == "outreach_examples":
                    action = "remove_from_customer_email"
                    conf = 0.88
                else:
                    action = "downgrade_to_low"
                    conf = 0.84
            elif current in {"low", "other"}:
                action = "remove_from_customer_email"
                conf = 0.87

    if not detail_row:
        reasons.append("low_info")
        conf = min(conf, 0.60)
    elif not _norm_text((detail_row or {}).get("inspection_type")):
        reasons.append("low_info")
        conf = min(conf, 0.62)

    for token in sorted(markers):
        if token.startswith("emphasis_trench") and "emphasis_trench" not in reasons:
            reasons.append("emphasis_trench")
        elif token.startswith("emphasis_fall") and "emphasis_fall" not in reasons:
            reasons.append("emphasis_fall")

    if days_old is not None:
        stale_threshold = OUTREACH_STALE_DAYS if mode == "outreach_examples" else (TRIAL_STALE_HIGH_DAYS if current == "high" else TRIAL_STALE_MEDIUM_DAYS)
        if days_old >= stale_threshold and "stale" not in reasons:
            reasons.append("stale")

    if action == "keep" and not reasons:
        reasons = ["keep_default"]

    return {
        "activity_nr": str(item.get("activity_nr") or "").strip(),
        "lead_key": str(item.get("lead_key") or "").strip(),
        "current_priority": current,
        "action": action,
        "confidence": max(0.0, min(1.0, float(conf))),
        "reasons": reasons[:4],
        "provenance": {"source": "rules_cached_detail"},
        "_rules_type": detail_type,
        "_markers": sorted(markers),
        "_days_old": days_old,
        "_item_key": key,
    }


def _map_ai_to_action(ai_payload: dict[str, Any], current_priority: str) -> str:
    decision = _norm_text(ai_payload.get("decision"))
    tier = _norm_text(ai_payload.get("suggested_tier"))
    if decision == "remove":
        return "remove_from_customer_email"
    if decision == "promote_candidate":
        return "promote_candidate"
    if decision == "downgrade":
        if tier == "medium":
            return "downgrade_to_medium"
        if tier == "low":
            return "downgrade_to_low"
        if tier == "none":
            return "remove_from_customer_email"
        return "downgrade_to_medium" if current_priority == "high" else "downgrade_to_low"
    return "keep"


def _apply_ai_caps(
    rules_decision: dict[str, Any],
    ai_payload: dict[str, Any] | None,
    detail_row: dict[str, Any] | None,
) -> dict[str, Any]:
    if not ai_payload:
        return rules_decision

    current = str(rules_decision.get("current_priority") or "other")
    ai_conf = max(0.0, min(1.0, float(ai_payload.get("confidence") or 0.0)))
    ai_reasons = [str(x).strip().lower() for x in (ai_payload.get("reasons") or []) if str(x).strip()][:4] or ["ai_unspecified"]
    ai_action = _map_ai_to_action(ai_payload, current_priority=current)
    rules_type = str(rules_decision.get("_rules_type") or "")
    markers = set(rules_decision.get("_markers") or [])
    hard_severity = any(m.startswith("severity_") for m in markers)

    if ai_action == "promote_candidate" and not (rules_type in {"fat/cat", "accident"} or hard_severity):
        return rules_decision

    if current == "high" and ai_action == "remove_from_customer_email" and ai_conf < 0.90:
        return rules_decision

    merged = dict(rules_decision)
    merged["action"] = ai_action
    merged["confidence"] = ai_conf
    merged["reasons"] = ai_reasons
    merged["provenance"] = {
        "source": "ai_cached",
        "prompt_version": str(ai_payload.get("prompt_version") or ai_triage.AI_PROMPT_VERSION),
        "content_sha256": str(ai_payload.get("content_sha256") or ""),
    }
    return merged


def triage(
    items: list[dict[str, Any]],
    detail_cache_lookup: Callable[[dict[str, Any]], dict[str, Any] | None] | dict[str, dict[str, Any]] | None,
    mode: str,
) -> list[dict[str, Any]]:
    mode_norm = str(mode or "").strip().lower()
    if mode_norm not in {"trial_render", "outreach_examples"}:
        raise ValueError(f"invalid_mode={mode}")

    if callable(detail_cache_lookup):
        lookup_fn = detail_cache_lookup
    else:
        rows = detail_cache_lookup or {}

        def lookup_fn(item: dict[str, Any]) -> dict[str, Any] | None:
            key = str(item.get("activity_nr") or "").strip()
            if key and key in rows:
                return dict(rows[key])
            lk = str(item.get("lead_key") or "").strip()
            if lk and lk in rows:
                return dict(rows[lk])
            return None

    out: list[dict[str, Any]] = []
    for item in list(items or []):
        detail_row = lookup_fn(item) or None
        rule_decision = _rules_decision(item, detail_row, mode_norm)
        content_sha = str((detail_row or {}).get("content_sha256") or "").strip()
        ai_payload = None
        if content_sha and str(rule_decision.get("_item_key") or "").strip():
            ai_payload = ai_triage.get_or_compute(
                item_key=str(rule_decision.get("_item_key")),
                mode=mode_norm,
                content_sha256=content_sha,
                item={
                    **item,
                    "current_priority": rule_decision.get("current_priority"),
                },
                detail_row=detail_row or {},
            )
        final_decision = _apply_ai_caps(rule_decision, ai_payload, detail_row)
        # Drop internal fields.
        cleaned = {
            "activity_nr": final_decision.get("activity_nr", ""),
            "lead_key": final_decision.get("lead_key", ""),
            "current_priority": final_decision.get("current_priority", "other"),
            "action": final_decision.get("action", "keep"),
            "confidence": max(0.0, min(1.0, float(final_decision.get("confidence") or 0.0))),
            "reasons": [str(x).strip().lower() for x in (final_decision.get("reasons") or []) if str(x).strip()][:4],
            "provenance": final_decision.get("provenance") or {"source": "rules_cached_detail"},
        }
        if not cleaned["reasons"]:
            cleaned["reasons"] = ["keep_default"]
        out.append(cleaned)
    return out


def decisions_by_activity(decisions: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for d in decisions or []:
        key = str(d.get("activity_nr") or d.get("lead_key") or "").strip()
        if key:
            out[key] = d
    return out


def apply_trial_overlay_to_leads(leads: list[dict[str, Any]], decisions: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, int], list[dict[str, Any]]]:
    by_key = decisions_by_activity(decisions)
    stats = Counter()
    promoted: list[dict[str, Any]] = []
    out: list[dict[str, Any]] = []
    for row in list(leads or []):
        lead = dict(row)
        key = str(lead.get("activity_nr") or lead.get("lead_key") or "").strip()
        d = by_key.get(key)
        if not d:
            out.append(lead)
            continue
        action = str(d.get("action") or "keep")
        lead["triage_overlay_action"] = action
        lead["triage_overlay_confidence"] = float(d.get("confidence") or 0.0)
        lead["triage_overlay_reasons"] = list(d.get("reasons") or [])
        if action == "remove_from_customer_email":
            stats["removed"] += 1
            continue
        if action == "downgrade_to_medium":
            lead["triage_priority_override"] = "medium"
            lead["lead_score"] = 6
            stats["downgraded_to_medium"] += 1
        elif action == "downgrade_to_low":
            lead["triage_priority_override"] = "low"
            lead["lead_score"] = 0
            stats["downgraded_to_low"] += 1
        elif action == "promote_candidate":
            promoted.append(lead)
            stats["promote_candidates"] += 1
        out.append(lead)
    stats["kept"] = len(out)
    return out, {str(k): int(v) for k, v in stats.items()}, promoted


def summarize_outreach_example_triage(decisions: list[dict[str, Any]]) -> tuple[str, str, str]:
    if not decisions:
        return "NO_ELIGIBLE_SIGNALS", "", ""

    removed = [d for d in decisions if str(d.get("action")) == "remove_from_customer_email"]
    changed = [d for d in decisions if str(d.get("action")) in {"remove_from_customer_email", "downgrade_to_low", "downgrade_to_medium"}]
    if removed and len(removed) == len(decisions):
        action = "NO_ELIGIBLE_SIGNALS"
    elif removed:
        action = "REPLACED_SOME"
    elif changed:
        action = "DROPPED_SOME"
    else:
        action = "KEEP_ALL"

    if action == "AI_DISABLED":
        return action, "", ""
    pool = changed or list(decisions)
    conf = min(max(0.0, min(1.0, float(d.get("confidence") or 0.0))) for d in pool) if pool else 0.0
    reason_counts: Counter[str] = Counter()
    for d in pool:
        for token in (d.get("reasons") or []):
            t = str(token or "").strip().lower()
            if t:
                reason_counts[t] += 1
    ordered = sorted(reason_counts.items(), key=lambda kv: (-kv[1], kv[0]))
    reasons = ";".join([k for k, _v in ordered[:4]])
    return action, f"{conf:.2f}", reasons


def build_outreach_signal_triage_records(
    *,
    batch_id: str,
    prospect_id: str,
    original_signals: list[dict[str, Any]],
    final_signals: list[dict[str, Any]],
    decisions: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    final_keys = {str(r.get("activity_nr") or r.get("lead_key") or "").strip() for r in (final_signals or [])}
    decision_map = decisions_by_activity(decisions)
    records: list[dict[str, Any]] = []
    for idx, row in enumerate(list(original_signals or []), start=1):
        key = str(row.get("activity_nr") or row.get("lead_key") or "").strip()
        d = decision_map.get(key, {})
        records.append(
            {
                "batch_id": str(batch_id or ""),
                "prospect_id": str(prospect_id or ""),
                "example_index": int(idx),
                "activity_nr": str(row.get("activity_nr") or ""),
                "lead_key": str(row.get("lead_key") or ""),
                "original_selected": 1,
                "final_included": 1 if key and key in final_keys else 0,
                "decision_action": str(d.get("action") or "keep"),
                "confidence": max(0.0, min(1.0, float(d.get("confidence") or 0.0))),
                "reasons": list(d.get("reasons") or []),
                "source": str((d.get("provenance") or {}).get("source") or "rules_cached_detail"),
            }
        )
    return records


def json_pretty(data: Any) -> str:
    return json.dumps(data, indent=2, sort_keys=True) + "\n"
