from __future__ import annotations

import json
import os
from collections import Counter
from datetime import datetime, timezone
from typing import Any, Callable

from scoring import ai_triage
from scoring import rules_config


PRIORITY_ORDER = {"LOW": 0, "MEDIUM": 1, "HIGH": 2, "SUPPRESS": -1}
NO_INSP_TOKENS = ("no insp", "10 or fewer")


def _bool_env(name: str, default: bool = False) -> bool:
    raw = (os.getenv(name) or "").strip().lower()
    if not raw:
        return bool(default)
    return raw in {"1", "true", "yes", "on"}


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


def _norm_text(value: Any) -> str:
    return " ".join(str(value or "").strip().lower().split())


def _norm_digits(value: Any) -> str:
    return "".join(ch for ch in str(value or "") if ch.isdigit())


def _norm_state(value: Any) -> str:
    return str(value or "").strip().upper()


def _item_key(item: dict[str, Any]) -> str:
    return str(item.get("activity_nr") or item.get("lead_key") or item.get("lead_id") or "").strip()


def _priority_from_score(score: int) -> str:
    if int(score) >= 10:
        return "HIGH"
    if int(score) >= 6:
        return "MEDIUM"
    return "LOW"


def _priority_to_score(priority: str) -> int:
    p = str(priority or "").strip().upper()
    if p == "HIGH":
        return 10
    if p == "MEDIUM":
        return 6
    return 0


def _current_priority(item: dict[str, Any]) -> str:
    explicit = str(item.get("current_priority") or "").strip().upper()
    if explicit in {"HIGH", "MEDIUM", "LOW"}:
        return explicit
    try:
        score = int(item.get("lead_score") or 0)
    except Exception:
        score = 0
    return _priority_from_score(score)


def _priority_rank(priority: str) -> int:
    return int(PRIORITY_ORDER.get(str(priority or "").strip().upper(), -1))


def _inspection_type(value: Any) -> str:
    text = _norm_text(value)
    if not text:
        return ""
    if "referral" in text:
        return "referral"
    if "complaint" in text:
        return "complaint"
    if "planned" in text or "programmed" in text:
        return "planned"
    if "accident" in text:
        return "accident"
    return text


def _is_scope_no_insp(scope: Any) -> bool:
    text = _norm_text(scope)
    return any(token in text for token in NO_INSP_TOKENS)


def _days_old_from_date_opened(value: Any) -> int | None:
    dt = _parse_dt(value)
    if not dt:
        return None
    now = datetime.now(timezone.utc)
    return max(0, int((now - dt).total_seconds() // 86400))


def _coalesce(item: dict[str, Any], detail_row: dict[str, Any] | None, key: str) -> Any:
    if detail_row:
        value = detail_row.get(key)
        if value not in (None, ""):
            return value
    return item.get(key)


def _safe_int(value: Any) -> int:
    try:
        return int(value)
    except Exception:
        return 0


def _is_construction_or_industrial_naics(code: Any) -> bool:
    digits = _norm_digits(code)
    if not digits:
        return False
    if digits.startswith("23"):
        return True
    if digits.startswith("21") or digits.startswith("22"):
        return True
    if digits.startswith("31") or digits.startswith("32") or digits.startswith("33"):
        return True
    if digits.startswith("48") or digits.startswith("49"):
        return True
    return False


def _action_from_priorities(current_priority: str, final_priority: str) -> str:
    current = str(current_priority or "").strip().upper()
    final = str(final_priority or "").strip().upper()
    if final == "SUPPRESS":
        return "remove_from_customer_email"
    if _priority_rank(final) > _priority_rank(current):
        return "promote_candidate"
    if _priority_rank(final) < _priority_rank(current):
        if final == "MEDIUM":
            return "downgrade_to_medium"
        return "downgrade_to_low"
    return "keep"


def _resolve_freshness_max_days() -> int:
    raw = (os.getenv("SIGNAL_FRESHNESS_MAX_DAYS") or "").strip()
    if not raw:
        return 30
    try:
        return max(1, int(raw))
    except Exception:
        return 30


def _multi_employer_site_counts(
    items: list[dict[str, Any]],
    lookup_fn: Callable[[dict[str, Any]], dict[str, Any] | None],
) -> dict[str, int]:
    grouped: dict[tuple[str, str, str], list[tuple[str, datetime]]] = {}
    for row in items or []:
        key = _item_key(row)
        if not key:
            continue
        detail = lookup_fn(row) or None
        address = _norm_text(_coalesce(row, detail, "site_address1"))
        city = _norm_text(_coalesce(row, detail, "site_city"))
        state = _norm_state(_coalesce(row, detail, "site_state"))
        opened_dt = _parse_dt(_coalesce(row, detail, "date_opened"))
        if not address or not city or not state or not opened_dt:
            continue
        group_key = (address, city, state)
        grouped.setdefault(group_key, []).append((key, opened_dt))

    out: dict[str, int] = {}
    for entries in grouped.values():
        if len(entries) < 2:
            continue
        for idx, (item_key, opened_dt) in enumerate(entries):
            count = 0
            for jdx, (_other_key, other_dt) in enumerate(entries):
                if idx == jdx:
                    count += 1
                    continue
                day_gap = abs(int((opened_dt - other_dt).total_seconds() // 86400))
                if day_gap <= 14:
                    count += 1
            if count >= 2:
                out[item_key] = max(int(out.get(item_key, 0)), int(count))
    return out


def _rules_decision(
    item: dict[str, Any],
    detail_row: dict[str, Any] | None,
    *,
    mode: str,
    site_cluster_count: int,
    freshness_max_days: int,
) -> dict[str, Any]:
    del mode
    key = _item_key(item)
    current_priority = _current_priority(item)
    base_score = _safe_int(item.get("lead_score") or _priority_to_score(current_priority))

    establishment_name = _coalesce(item, detail_row, "establishment_name")
    inspection_type = _inspection_type(_coalesce(item, detail_row, "inspection_type"))
    scope = _coalesce(item, detail_row, "scope")
    case_status = _norm_text(_coalesce(item, detail_row, "case_status"))
    date_opened = _coalesce(item, detail_row, "date_opened")
    naics_value = _coalesce(item, detail_row, "naics")
    naics_digits = _norm_digits(naics_value)
    site_state = _norm_state(_coalesce(item, detail_row, "site_state"))
    mail_state = _norm_state(_coalesce(item, detail_row, "mail_state"))

    reasons: list[str] = []
    low_lock = False

    # Suppress: closed and no-inspection/10-or-fewer.
    if case_status == "closed" and _is_scope_no_insp(scope):
        print(f"SIGNAL_SUPPRESS activity_nr={key} reason=NO_INSP_CLOSED")
        return {
            "activity_nr": str(item.get("activity_nr") or ""),
            "lead_key": str(item.get("lead_key") or ""),
            "current_priority": current_priority,
            "rules_priority": "SUPPRESS",
            "final_priority": "SUPPRESS",
            "ai_priority": "NONE",
            "ai_applied": 0,
            "action": "remove_from_customer_email",
            "confidence": 0.99,
            "reasons": ["no_insp_closed"],
            "provenance": {"source": "rules_deterministic"},
            "_item_key": key,
            "_low_lock": False,
        }

    # Suppress: stale by date_opened.
    age_days = _days_old_from_date_opened(date_opened)
    if age_days is not None and age_days > freshness_max_days:
        print(
            f"SIGNAL_SUPPRESS activity_nr={key} reason=STALE age_days={age_days} max_days={freshness_max_days}"
        )
        return {
            "activity_nr": str(item.get("activity_nr") or ""),
            "lead_key": str(item.get("lead_key") or ""),
            "current_priority": current_priority,
            "rules_priority": "SUPPRESS",
            "final_priority": "SUPPRESS",
            "ai_priority": "NONE",
            "ai_applied": 0,
            "action": "remove_from_customer_email",
            "confidence": 0.99,
            "reasons": ["stale"],
            "provenance": {"source": "rules_deterministic"},
            "_item_key": key,
            "_low_lock": False,
        }

    # Suppress: non-target NAICS.
    suppress_rule = rules_config.match_naics_suppress(naics_digits)
    if suppress_rule is not None:
        reason_token = _norm_text(suppress_rule.reason).replace(" ", "_") or "naics_suppress"
        print(f"SIGNAL_SUPPRESS activity_nr={key} reason={reason_token.upper()}")
        return {
            "activity_nr": str(item.get("activity_nr") or ""),
            "lead_key": str(item.get("lead_key") or ""),
            "current_priority": current_priority,
            "rules_priority": "SUPPRESS",
            "final_priority": "SUPPRESS",
            "ai_priority": "NONE",
            "ai_applied": 0,
            "action": "remove_from_customer_email",
            "confidence": 0.99,
            "reasons": [reason_token],
            "provenance": {"source": "rules_deterministic"},
            "_item_key": key,
            "_low_lock": False,
        }

    # Suppress: large national enterprise.
    enterprise_rule = rules_config.match_enterprise_pattern(establishment_name)
    if enterprise_rule and mail_state and site_state and mail_state != site_state:
        print(f"SIGNAL_SUPPRESS activity_nr={key} reason=ENTERPRISE_NATIONAL")
        return {
            "activity_nr": str(item.get("activity_nr") or ""),
            "lead_key": str(item.get("lead_key") or ""),
            "current_priority": current_priority,
            "rules_priority": "SUPPRESS",
            "final_priority": "SUPPRESS",
            "ai_priority": "NONE",
            "ai_applied": 0,
            "action": "remove_from_customer_email",
            "confidence": 0.99,
            "reasons": ["enterprise_national"],
            "provenance": {"source": "rules_deterministic"},
            "_item_key": key,
            "_low_lock": False,
        }

    score = int(base_score)

    if inspection_type in {"referral", "complaint"}:
        score += 6
        reasons.append("referral_or_complaint")

    naics_boost_rules = rules_config.match_naics_boost(naics_digits)
    if naics_boost_rules:
        boost_points = sum(int(r.boost_points) for r in naics_boost_rules)
        if boost_points > 0:
            score += int(boost_points)
            reasons.append("naics_emphasis")

    if int(site_cluster_count) >= 2:
        score += 4
        reasons.append("multi_employer_site")
        print(
            f"SIGNAL_BOOST activity_nr={key} reason=MULTI_EMPLOYER_SITE site_cluster_count={int(site_cluster_count)}"
        )

    serious = _safe_int(_coalesce(item, detail_row, "serious_violations"))
    willful = _safe_int(_coalesce(item, detail_row, "willful_violations"))
    repeat = _safe_int(_coalesce(item, detail_row, "repeat_violations"))
    if serious > 0 or willful > 0 or repeat > 0:
        score += 4
        reasons.append("serious_willful_repeat")

    if inspection_type == "planned" and not _is_construction_or_industrial_naics(naics_digits):
        low_lock = True
        reasons.append("planned_low_hazard")

    if case_status == "open" and _is_scope_no_insp(scope):
        low_lock = True
        if "open_no_insp_low_info" not in reasons:
            reasons.append("open_no_insp_low_info")

    rules_priority = _priority_from_score(score)
    if low_lock:
        rules_priority = "LOW"

    if not reasons:
        reasons = ["rules_default"]

    return {
        "activity_nr": str(item.get("activity_nr") or ""),
        "lead_key": str(item.get("lead_key") or ""),
        "current_priority": current_priority,
        "rules_priority": rules_priority,
        "final_priority": rules_priority,
        "ai_priority": "NONE",
        "ai_applied": 0,
        "action": _action_from_priorities(current_priority, rules_priority),
        "confidence": 0.90,
        "reasons": reasons[:6],
        "provenance": {"source": "rules_deterministic"},
        "_item_key": key,
        "_low_lock": bool(low_lock),
    }


def _ai_item_payload(item: dict[str, Any], detail_row: dict[str, Any] | None) -> dict[str, Any]:
    return {
        "activity_nr": item.get("activity_nr"),
        "establishment_name": _coalesce(item, detail_row, "establishment_name"),
        "site_city": _coalesce(item, detail_row, "site_city"),
        "site_state": _coalesce(item, detail_row, "site_state"),
        "naics": _coalesce(item, detail_row, "naics"),
        "naics_desc": _coalesce(item, detail_row, "naics_desc"),
        "sic": _coalesce(item, detail_row, "sic"),
        "inspection_type": _coalesce(item, detail_row, "inspection_type"),
        "scope": _coalesce(item, detail_row, "scope"),
        "case_status": _coalesce(item, detail_row, "case_status"),
        "emphasis": _coalesce(item, detail_row, "emphasis"),
        "violations_count": _coalesce(item, detail_row, "violations_count"),
        "serious_violations": _coalesce(item, detail_row, "serious_violations"),
        "willful_violations": _coalesce(item, detail_row, "willful_violations"),
        "repeat_violations": _coalesce(item, detail_row, "repeat_violations"),
    }


def triage(
    items: list[dict[str, Any]],
    detail_cache_lookup: Callable[[dict[str, Any]], dict[str, Any] | None] | dict[str, dict[str, Any]] | None,
    mode: str,
    *,
    allow_ai: bool = True,
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

    freshness_max_days = _resolve_freshness_max_days()
    site_cluster_counts = _multi_employer_site_counts(list(items or []), lookup_fn)

    ai_evaluated = 0
    ai_raised = 0
    ai_unchanged = 0
    ai_cached_hits = 0
    ai_api_calls = 0
    ai_unavailable = 0
    ai_requested = bool(allow_ai) and bool(ai_triage.enabled())

    out: list[dict[str, Any]] = []
    for item in list(items or []):
        detail_row = lookup_fn(item) or None
        key = _item_key(item)
        rule_decision = _rules_decision(
            item,
            detail_row,
            mode=mode_norm,
            site_cluster_count=int(site_cluster_counts.get(key, 0)),
            freshness_max_days=freshness_max_days,
        )

        final_priority = str(rule_decision.get("rules_priority") or "LOW").upper()
        ai_priority = "NONE"
        ai_applied = 0

        if final_priority != "SUPPRESS" and ai_requested and key:
            ai_evaluated += 1
            payload = ai_triage.get_or_compute(
                item_key=key,
                mode=mode_norm,
                item=_ai_item_payload(item, detail_row),
                detail_row=dict(detail_row or {}),
            )
            if payload:
                ai_priority = str(payload.get("priority") or "LOW").strip().upper()
                if ai_priority not in {"HIGH", "MEDIUM", "LOW"}:
                    ai_priority = "LOW"
                if int(payload.get("cached") or 0) == 1:
                    ai_cached_hits += 1
                else:
                    ai_api_calls += 1
                if (
                    not bool(rule_decision.get("_low_lock"))
                    and _priority_rank(ai_priority) > _priority_rank(final_priority)
                ):
                    final_priority = ai_priority
                    ai_applied = 1
                    ai_raised += 1
                else:
                    ai_unchanged += 1
                print(
                    "AI_TRIAGE "
                    f"signal={key} "
                    f"rules={rule_decision.get('rules_priority')} "
                    f"ai={ai_priority} "
                    f"final={final_priority} "
                    f"reason={str(payload.get('reason') or '').strip()}"
                )
            else:
                ai_unavailable = 1
                ai_unchanged += 1
                print(
                    "AI_TRIAGE "
                    f"signal={key} "
                    f"rules={rule_decision.get('rules_priority')} "
                    "ai=NONE "
                    f"final={final_priority} "
                    "reason=unavailable"
                )

        action = _action_from_priorities(str(rule_decision.get("current_priority") or "LOW"), final_priority)
        reasons = [str(x).strip().lower() for x in (rule_decision.get("reasons") or []) if str(x).strip()]
        if ai_applied:
            reasons.append("ai_raise")

        decision = {
            "activity_nr": str(rule_decision.get("activity_nr") or ""),
            "lead_key": str(rule_decision.get("lead_key") or ""),
            "current_priority": str(rule_decision.get("current_priority") or "LOW"),
            "rules_priority": str(rule_decision.get("rules_priority") or "LOW"),
            "final_priority": final_priority,
            "ai_priority": ai_priority,
            "ai_applied": int(ai_applied),
            "action": action,
            "confidence": 0.95 if final_priority == "SUPPRESS" else 0.90,
            "reasons": reasons[:8] if reasons else ["rules_default"],
            "provenance": {
                "source": "ai_cached" if ai_applied else "rules_deterministic",
                "prompt_hash": ai_triage.prompt_hash() if ai_applied else "",
            },
        }
        out.append(decision)

    if ai_requested:
        print(f"AI_TRIAGE_EVALUATED={ai_evaluated}")
        print(f"AI_TRIAGE_RAISED={ai_raised} UNCHANGED={ai_unchanged}")
        print(f"AI_TRIAGE_CACHED_HITS={ai_cached_hits} API_CALLS={ai_api_calls}")
        print(f"AI_TRIAGE_UNAVAILABLE={1 if ai_unavailable else 0}")
        if ai_unavailable:
            print("WARN_AI_TRIAGE_UNAVAILABLE")

    return out


def decisions_by_activity(decisions: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for d in decisions or []:
        key = str(d.get("activity_nr") or d.get("lead_key") or "").strip()
        if key:
            out[key] = d
    return out


def apply_trial_overlay_to_leads(
    leads: list[dict[str, Any]],
    decisions: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, int], list[dict[str, Any]]]:
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

        current_priority = str(d.get("current_priority") or "LOW").upper()
        final_priority = str(d.get("final_priority") or "LOW").upper()
        action = str(d.get("action") or "keep")

        lead["triage_overlay_action"] = action
        lead["triage_overlay_confidence"] = float(d.get("confidence") or 0.0)
        lead["triage_overlay_reasons"] = list(d.get("reasons") or [])
        lead["triage_rules_priority"] = str(d.get("rules_priority") or "")
        lead["triage_final_priority"] = final_priority

        if final_priority == "SUPPRESS":
            stats["removed"] += 1
            stats["suppressed"] += 1
            continue

        old_score = _safe_int(lead.get("lead_score") or 0)
        new_score = _priority_to_score(final_priority)
        lead["lead_score"] = int(new_score)
        if new_score != old_score:
            lead["triage_priority_override"] = final_priority.lower()

        if _priority_rank(final_priority) > _priority_rank(current_priority):
            stats["raised"] += 1
            stats["promote_candidates"] += 1
            promoted.append(lead)
        elif _priority_rank(final_priority) < _priority_rank(current_priority):
            if final_priority == "MEDIUM":
                stats["downgraded_to_medium"] += 1
            elif final_priority == "LOW":
                stats["downgraded_to_low"] += 1

        if final_priority == "HIGH":
            stats["final_high"] += 1
        elif final_priority == "MEDIUM":
            stats["final_medium"] += 1
        elif final_priority == "LOW":
            stats["final_low"] += 1

        out.append(lead)

    stats["kept"] = len(out)
    return out, {str(k): int(v) for k, v in stats.items()}, promoted


def summarize_outreach_example_triage(decisions: list[dict[str, Any]]) -> tuple[str, str, str]:
    if not decisions:
        return "NO_ELIGIBLE_SIGNALS", "", ""

    suppressed = []
    for d in decisions:
        final_priority = str(d.get("final_priority") or "").upper()
        action_hint = str(d.get("action") or "").strip().lower()
        if final_priority == "SUPPRESS" or action_hint == "remove_from_customer_email":
            suppressed.append(d)
    if suppressed and len(suppressed) == len(decisions):
        action = "NO_ELIGIBLE_SIGNALS"
    elif suppressed:
        action = "REPLACED_SOME"
    else:
        action = "KEEP_ALL"

    pool = suppressed or list(decisions)
    reason_counts: Counter[str] = Counter()
    for d in pool:
        for token in (d.get("reasons") or []):
            t = str(token or "").strip().lower()
            if t:
                reason_counts[t] += 1
    ordered = sorted(reason_counts.items(), key=lambda kv: (-kv[1], kv[0]))
    reasons = ";".join([k for k, _v in ordered[:4]])
    return action, "0.90", reasons


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
                "rules_priority": str(d.get("rules_priority") or ""),
                "final_priority": str(d.get("final_priority") or ""),
                "ai_priority": str(d.get("ai_priority") or "NONE"),
                "ai_applied": int(d.get("ai_applied") or 0),
                "confidence": max(0.0, min(1.0, float(d.get("confidence") or 0.0))),
                "reasons": list(d.get("reasons") or []),
                "source": str((d.get("provenance") or {}).get("source") or "rules_deterministic"),
            }
        )
    return records


def json_pretty(data: Any) -> str:
    return json.dumps(data, indent=2, sort_keys=True) + "\n"
