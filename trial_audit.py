from __future__ import annotations

import csv
import json
import os
import re
import sqlite3
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import crm_light
from lead_filters import filter_by_territory
from send_digest_email import get_leads_for_period

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore[assignment]


_RE_TERRITORY_DEBUG = re.compile(r"TERRITORY_DEBUG_WRITTEN path=(?P<path>\S+) rows=(?P<rows>\d+)")
_RE_TIER_AUDIT = re.compile(
    r"TIER_AUDIT_WRITTEN path=(?P<path>\S+) high=(?P<high>\d+) medium=(?P<medium>\d+) low=(?P<low>\d+)"
)
_RE_RUN_DIAGNOSTICS = re.compile(r"RUN_DIAGNOSTICS .*selected_for_digest=(?P<selected>\d+)")
_RE_LOWS_PREF = re.compile(r"LOW_SIGNALS_PREF lows_enabled=(?P<enabled>YES|NO)")
_RE_WINDOW_CHECK = re.compile(r"WINDOW_CHECK .*now_local=(?P<now_local>[^ ]+)")


def _resolve_zone(tz_name: str):
    if ZoneInfo is not None:
        try:
            return ZoneInfo((tz_name or "").strip() or "America/Chicago")
        except Exception:
            try:
                return ZoneInfo("America/Chicago")
            except Exception:
                pass
    return timezone.utc


def _parse_utc_ts(value: str) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    text = raw[:-1] + "+00:00" if raw.upper().endswith("Z") else raw
    try:
        dt = datetime.fromisoformat(text)
    except Exception:
        return None
    if dt.tzinfo is None or dt.utcoffset() is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _parse_any_ts(value: Any) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    text = raw[:-1] + "+00:00" if raw.upper().endswith("Z") else raw
    try:
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None or dt.utcoffset() is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        pass
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(raw, fmt)
            return dt.replace(tzinfo=timezone.utc)
        except Exception:
            continue
    return None


def _meta_dict(raw: str) -> dict[str, Any]:
    text = (raw or "").strip()
    if not text:
        return {}
    try:
        loaded = json.loads(text)
    except Exception:
        return {}
    if isinstance(loaded, dict):
        return loaded
    return {}


def _event_is_live_daily(variant: str, meta: dict[str, Any], primary_recipient: str) -> bool:
    normalized_variant = (variant or "").strip().upper()
    if normalized_variant and normalized_variant != "DAILY":
        return False
    normalized_primary = (primary_recipient or "").strip().lower()
    event_recipient = (
        str(meta.get("primary_recipient") or meta.get("recipient") or meta.get("to") or "").strip().lower()
    )
    if event_recipient and normalized_primary and event_recipient != normalized_primary:
        return False
    send_mode = str(meta.get("send_mode") or meta.get("mode") or "").strip().upper()
    if send_mode and send_mode != "LIVE":
        return False
    return True


def _lead_key(lead: dict[str, Any]) -> str:
    return str(lead.get("lead_key") or lead.get("activity_nr") or lead.get("lead_id") or "").strip()


def _lead_score(lead: dict[str, Any]) -> int:
    try:
        return int(lead.get("lead_score") or 0)
    except Exception:
        return 0


def _tier_counts(leads: list[dict[str, Any]]) -> dict[str, int]:
    counts = {"high": 0, "medium": 0, "low": 0}
    for lead in leads:
        score = _lead_score(lead)
        if score >= 10:
            counts["high"] += 1
        elif score >= 6:
            counts["medium"] += 1
        else:
            counts["low"] += 1
    return counts


def _sort_low_priority(leads: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = list(leads or [])
    rows.sort(
        key=lambda lead: str(lead.get("last_seen_at") or lead.get("first_seen_at") or lead.get("date_opened") or ""),
        reverse=True,
    )
    return rows


def _append_unique_in_order(keys: list[str], candidate_keys: list[str]) -> list[str]:
    seen = set(keys)
    for item in candidate_keys:
        key = str(item or "").strip()
        if key and key not in seen:
            seen.add(key)
            keys.append(key)
    return keys


def sent_payload_path(subscriber_key: str, local_date: str, data_root: Path | None = None) -> Path:
    root = data_root or Path("out")
    sk = (subscriber_key or "").strip().lower()
    d = (local_date or "").strip()
    return root / "trials" / sk / "sent" / d / "payload.json"


def load_sent_payload(
    subscriber_key: str,
    local_date: str,
    data_root: Path | None = None,
    repo_root: Path | None = None,
) -> tuple[dict[str, Any] | None, Path]:
    candidates: list[Path] = []
    if data_root is not None:
        candidates.append(sent_payload_path(subscriber_key=subscriber_key, local_date=local_date, data_root=data_root))
    else:
        if repo_root is not None:
            candidates.append(
                sent_payload_path(subscriber_key=subscriber_key, local_date=local_date, data_root=repo_root / "out")
            )
        candidates.append(
            sent_payload_path(subscriber_key=subscriber_key, local_date=local_date, data_root=crm_light.data_dir())
        )

    last_path = candidates[0] if candidates else sent_payload_path(subscriber_key=subscriber_key, local_date=local_date)
    for path in candidates:
        last_path = path
        if not path.exists():
            continue
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if isinstance(payload, dict):
            return payload, path
    return None, last_path


def _resolve_run_log_path(for_date: str, repo_root: Path) -> Path | None:
    direct = repo_root / "out" / f"run_log_{for_date}.txt"
    if direct.exists():
        return direct
    logs_dir = repo_root / "logs" / for_date
    if logs_dir.exists():
        for item in sorted(logs_dir.glob("deliver_daily_*.json")):
            try:
                payload = json.loads(item.read_text(encoding="utf-8"))
            except Exception:
                continue
            run_log = str(payload.get("run_log") or "").strip()
            if not run_log:
                continue
            candidate = Path(run_log)
            if not candidate.is_absolute():
                candidate = (repo_root / run_log).resolve()
            if candidate.exists():
                return candidate
    return None


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    with open(path, "r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            cleaned: dict[str, str] = {}
            for k, v in (row or {}).items():
                cleaned[str(k or "").strip()] = str(v or "").strip()
            rows.append(cleaned)
    return rows


def _load_leads_by_keys(db_path: str, keys: list[str]) -> dict[str, dict[str, Any]]:
    cleaned = [str(k or "").strip() for k in keys if str(k or "").strip()]
    if not cleaned:
        return {}
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        placeholders = ",".join("?" for _ in cleaned)
        rows = conn.execute(
            f"""
            SELECT
                lead_key,
                activity_nr,
                lead_score,
                date_opened,
                first_seen_at,
                last_seen_at,
                inspection_type,
                establishment_name,
                site_city,
                site_state,
                source_url
            FROM inspections
            WHERE lead_key IN ({placeholders})
            """,
            tuple(cleaned),
        ).fetchall()
        out: dict[str, dict[str, Any]] = {}
        for row in rows:
            key = str(row["lead_key"] or "").strip()
            if key:
                out[key] = dict(row)
        return out
    finally:
        conn.close()


def _sort_content_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = list(rows or [])
    out.sort(
        key=lambda row: (
            int(row.get("lead_score") or 0),
            str(row.get("date_opened") or ""),
            str(row.get("first_seen_at") or ""),
        ),
        reverse=True,
    )
    return out


def infer_rendered_from_logs(
    *,
    repo_root: Path,
    leads_db_path: str,
    subscriber_key: str,
    for_date: str,
    customer_config: dict[str, Any],
) -> dict[str, Any]:
    del subscriber_key
    run_log = _resolve_run_log_path(for_date=for_date, repo_root=repo_root)
    if run_log is None or not run_log.exists():
        return {
            "source": "none",
            "shown_lead_keys": [],
            "low_available_lead_keys": [],
            "tier_counts": {"high": 0, "medium": 0, "low": 0},
            "lows_enabled": False,
            "diagnostics": {"reason": "run_log_missing"},
        }

    selected_for_digest: int | None = None
    lows_enabled = False
    territory_debug_path: Path | None = None
    tier_counts: dict[str, int] | None = None
    tier_audit_path: Path | None = None
    diagnostics: dict[str, Any] = {"run_log": str(run_log)}

    for line in run_log.read_text(encoding="utf-8", errors="replace").splitlines():
        m_window = _RE_WINDOW_CHECK.search(line)
        if m_window:
            diagnostics["now_local"] = str(m_window.group("now_local") or "").strip()
        m_diag = _RE_RUN_DIAGNOSTICS.search(line)
        if m_diag:
            try:
                selected_for_digest = int(m_diag.group("selected"))
            except Exception:
                selected_for_digest = None
        m_lows = _RE_LOWS_PREF.search(line)
        if m_lows:
            lows_enabled = str(m_lows.group("enabled") or "").strip().upper() == "YES"
        m_debug = _RE_TERRITORY_DEBUG.search(line)
        if m_debug:
            raw = str(m_debug.group("path") or "").strip()
            if raw:
                candidate = Path(raw)
                if not candidate.is_absolute():
                    candidate = (repo_root / raw).resolve()
                if candidate.exists():
                    territory_debug_path = candidate
        m_tier = _RE_TIER_AUDIT.search(line)
        if m_tier:
            raw = str(m_tier.group("path") or "").strip()
            if raw:
                candidate = Path(raw)
                if not candidate.is_absolute():
                    candidate = (repo_root / raw).resolve()
                if candidate.exists():
                    tier_audit_path = candidate
            try:
                tier_counts = {
                    "high": int(m_tier.group("high")),
                    "medium": int(m_tier.group("medium")),
                    "low": int(m_tier.group("low")),
                }
            except Exception:
                tier_counts = None

    shown_lead_keys: list[str] = []
    low_available_keys: list[str] = []
    tier_audit_payload: dict[str, Any] = {}
    if tier_audit_path and tier_audit_path.exists():
        diagnostics["tier_audit_path"] = str(tier_audit_path)
        try:
            loaded = json.loads(tier_audit_path.read_text(encoding="utf-8"))
            if isinstance(loaded, dict):
                tier_audit_payload = loaded
        except Exception:
            tier_audit_payload = {}
        win = str(tier_audit_payload.get("window_start") or "").strip()
        if win:
            diagnostics["window_start"] = win
        if tier_counts is None and tier_audit_payload:
            counts = tier_audit_payload.get("tier_counts") or {}
            try:
                tier_counts = {
                    "high": int(counts.get("high", 0)),
                    "medium": int(counts.get("medium", 0)),
                    "low": int(counts.get("low", 0)),
                }
            except Exception:
                tier_counts = None
    if territory_debug_path and territory_debug_path.exists():
        debug_rows = _read_csv_rows(territory_debug_path)
        diagnostics["territory_debug_path"] = str(territory_debug_path)
        debug_keys = [str(item.get("lead_key") or "").strip() for item in debug_rows if str(item.get("lead_key") or "").strip()]
        lead_map = _load_leads_by_keys(leads_db_path, debug_keys)
        content_rows = _sort_content_rows(list(lead_map.values()))
        priority_rows = [row for row in content_rows if int(row.get("lead_score") or 0) >= 6]
        low_rows = [row for row in content_rows if int(row.get("lead_score") or 0) < 6]
        if selected_for_digest is None:
            selected_for_digest = len(priority_rows)
        if selected_for_digest < 0:
            selected_for_digest = 0
        shown_leads = priority_rows[:selected_for_digest]
        shown_lead_keys = [_lead_key(item) for item in shown_leads if _lead_key(item)]
        low_available_keys = [_lead_key(item) for item in low_rows if _lead_key(item)]
        if lows_enabled:
            try:
                low_limit = int(customer_config.get("low_signals_limit", os.getenv("LOW_SIGNALS_LIMIT", "8")))
            except Exception:
                low_limit = 8
            low_limit = max(0, min(25, int(low_limit)))
            low_shown = _sort_low_priority(low_rows)[:low_limit]
            shown_lead_keys = _append_unique_in_order(shown_lead_keys, [_lead_key(item) for item in low_shown])
        if tier_counts is None:
            tier_counts = _tier_counts(content_rows)

    elif tier_audit_payload:
        samples = tier_audit_payload.get("samples") if isinstance(tier_audit_payload, dict) else {}
        sample_activity: list[str] = []
        for bucket in ("high", "medium"):
            entries = samples.get(bucket, []) if isinstance(samples, dict) else []
            if isinstance(entries, list):
                for entry in entries:
                    nr = str((entry or {}).get("activity_nr") or "").strip()
                    if nr:
                        sample_activity.append(nr)
        if sample_activity:
            conn = sqlite3.connect(leads_db_path)
            conn.row_factory = sqlite3.Row
            try:
                placeholders = ",".join("?" for _ in sample_activity)
                rows = conn.execute(
                    f"""
                    SELECT lead_key, activity_nr, lead_score
                    FROM inspections
                    WHERE activity_nr IN ({placeholders})
                    """,
                    tuple(sample_activity),
                ).fetchall()
                for row in rows:
                    key = str(row["lead_key"] or row["activity_nr"] or "").strip()
                    if key:
                        shown_lead_keys.append(key)
            finally:
                conn.close()
        sample_low: list[str] = []
        entries = samples.get("low", []) if isinstance(samples, dict) else []
        if isinstance(entries, list):
            sample_low = [str((entry or {}).get("activity_nr") or "").strip() for entry in entries if str((entry or {}).get("activity_nr") or "").strip()]
        if sample_low:
            conn = sqlite3.connect(leads_db_path)
            conn.row_factory = sqlite3.Row
            try:
                placeholders = ",".join("?" for _ in sample_low)
                rows = conn.execute(
                    f"""
                    SELECT lead_key, activity_nr
                    FROM inspections
                    WHERE activity_nr IN ({placeholders})
                    """,
                    tuple(sample_low),
                ).fetchall()
                for row in rows:
                    key = str(row["lead_key"] or row["activity_nr"] or "").strip()
                    if key:
                        low_available_keys.append(key)
            finally:
                conn.close()
        if tier_counts is None:
            tier_counts = {"high": 0, "medium": 0, "low": 0}

    if tier_counts is None:
        tier_counts = {"high": 0, "medium": 0, "low": 0}
    return {
        "source": "fallback_log_artifacts",
        "shown_lead_keys": [item for item in shown_lead_keys if item],
        "low_available_lead_keys": [item for item in low_available_keys if item],
        "tier_counts": tier_counts,
        "lows_enabled": lows_enabled,
        "diagnostics": diagnostics,
    }


def load_rendered_digest_for_date(
    *,
    repo_root: Path,
    leads_db_path: str,
    subscriber_key: str,
    for_date: str,
    customer_config: dict[str, Any],
    data_root: Path | None = None,
) -> dict[str, Any]:
    payload, payload_path = load_sent_payload(
        subscriber_key=subscriber_key,
        local_date=for_date,
        data_root=data_root,
        repo_root=repo_root,
    )
    if payload is not None:
        shown = [str(item or "").strip() for item in (payload.get("selected_lead_keys") or []) if str(item or "").strip()]
        low_keys = [
            str(item or "").strip()
            for item in (payload.get("low_available_lead_keys") or [])
            if str(item or "").strip()
        ]
        tier_raw = payload.get("tier_counts") or {}
        tier_counts = {
            "high": int(tier_raw.get("high", 0)),
            "medium": int(tier_raw.get("medium", 0)),
            "low": int(tier_raw.get("low", 0)),
        }
        return {
            "source": "payload_artifact",
            "payload_path": str(payload_path),
            "shown_lead_keys": shown,
            "low_available_lead_keys": low_keys,
            "tier_counts": tier_counts,
            "lows_enabled": bool(payload.get("lows_enabled", False)),
            "subject": str(payload.get("subject") or "").strip(),
            "render_sha256": str(payload.get("render_sha256") or "").strip(),
            "payload": payload,
        }
    inferred = infer_rendered_from_logs(
        repo_root=repo_root,
        leads_db_path=leads_db_path,
        subscriber_key=subscriber_key,
        for_date=for_date,
        customer_config=customer_config,
    )
    inferred["payload_path"] = str(payload_path)
    return inferred


def load_live_daily_events(
    *,
    subscriber_key: str,
    tz_name: str,
    primary_recipient: str,
    crm_db_path: str | Path | None,
) -> list[dict[str, Any]]:
    db_path = crm_light.resolve_crm_db_path(crm_db_path)
    if not db_path.exists():
        return []
    zone = _resolve_zone(tz_name)
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT ts_utc, variant, status, run_id, meta_json
            FROM send_events
            WHERE subscriber_key = ?
            ORDER BY ts_utc ASC, id ASC
            """,
            ((subscriber_key or "").strip().lower(),),
        ).fetchall()
    finally:
        conn.close()
    out: list[dict[str, Any]] = []
    for row in rows:
        status = str(row["status"] or "").strip().upper()
        if status != "SENT":
            continue
        ts_utc = str(row["ts_utc"] or "").strip()
        dt_utc = _parse_utc_ts(ts_utc)
        if dt_utc is None:
            continue
        meta = _meta_dict(str(row["meta_json"] or ""))
        variant = str(row["variant"] or "").strip()
        if not _event_is_live_daily(variant=variant, meta=meta, primary_recipient=primary_recipient):
            continue
        local_dt = dt_utc.astimezone(zone)
        out.append(
            {
                "ts_utc": ts_utc,
                "run_id": str(row["run_id"] or "").strip(),
                "local_date": local_dt.date().isoformat(),
                "local_iso": local_dt.isoformat(),
            }
        )
    return out


def compute_expected_daily_digest(
    *,
    leads_db_path: str,
    customer_config: dict[str, Any],
    territory_code: str,
    for_date: str,
    previous_sent_ts_utc: str | None,
    lows_enabled: bool,
    max_first_seen_utc: str | None = None,
) -> dict[str, Any]:
    states = [
        str(state).strip().upper()
        for state in (customer_config.get("states") or ["TX"])
        if str(state).strip()
    ]
    if not states:
        states = ["TX"]
    try:
        for_date_obj = date.fromisoformat(str(for_date))
    except Exception as exc:
        raise ValueError(f"invalid for_date={for_date}") from exc
    ref_now = datetime.fromisoformat(f"{for_date_obj.isoformat()}T12:00:00")
    try:
        since_days = int(customer_config.get("opened_window_days") or 14)
    except Exception:
        since_days = 14
    try:
        new_only_days = int(customer_config.get("new_only_days") or 1)
    except Exception:
        new_only_days = 1
    baseline_on_first_send = bool(customer_config.get("baseline_on_first_send", True))
    content_filter = str(customer_config.get("content_filter") or "high_medium")
    include_low_fallback = bool(customer_config.get("include_low_fallback", False))

    prev_dt = _parse_utc_ts(previous_sent_ts_utc or "")
    snapshot_mode = bool(baseline_on_first_send and prev_dt is None)
    if snapshot_mode:
        use_opened_window = True
        window_start = None
        strict_first_seen_after = None
        skip_first_seen_filter = True
        new_only_cutoff = None
    else:
        use_opened_window = False
        window_start = prev_dt
        strict_first_seen_after = prev_dt
        skip_first_seen_filter = True
        new_only_cutoff = None

    conn = sqlite3.connect(leads_db_path)
    conn.row_factory = sqlite3.Row
    try:
        leads, low_fallback, stats = get_leads_for_period(
            conn=conn,
            states=states,
            since_days=since_days,
            new_only_days=new_only_days,
            skip_first_seen_filter=skip_first_seen_filter,
            territory_code=territory_code,
            content_filter=content_filter,
            include_low_fallback=include_low_fallback,
            window_start=window_start,
            new_only_cutoff=new_only_cutoff,
            strict_first_seen_after=strict_first_seen_after,
            include_changed=False,
            use_opened_window=use_opened_window,
            reference_now=ref_now,
        )
        all_leads_deduped, _, _ = get_leads_for_period(
            conn=conn,
            states=states,
            since_days=since_days,
            new_only_days=new_only_days,
            skip_first_seen_filter=skip_first_seen_filter,
            territory_code=territory_code,
            content_filter="all",
            include_low_fallback=False,
            window_start=window_start,
            new_only_cutoff=new_only_cutoff,
            strict_first_seen_after=strict_first_seen_after,
            include_changed=False,
            use_opened_window=use_opened_window,
            reference_now=ref_now,
        )
    finally:
        conn.close()

    max_dt = _parse_any_ts(max_first_seen_utc) if str(max_first_seen_utc or "").strip() else None

    def _bounded(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if max_dt is None:
            return list(rows or [])
        kept: list[dict[str, Any]] = []
        for item in list(rows or []):
            first_seen = _parse_any_ts(item.get("first_seen_at"))
            if first_seen is None or first_seen <= max_dt:
                kept.append(item)
        return kept

    leads = _bounded(leads)
    low_fallback = _bounded(low_fallback)
    all_leads_deduped = _bounded(all_leads_deduped)

    tier_counts = _tier_counts(all_leads_deduped)
    low_available_keys = [_lead_key(item) for item in all_leads_deduped if _lead_score(item) < 6 and _lead_key(item)]

    try:
        top_k = int(customer_config.get("top_k_overall", 50))
    except Exception:
        top_k = 50
    top_k = max(1, top_k)
    main_shown = list(leads)[: min(len(leads), top_k)]
    shown_keys = [_lead_key(item) for item in main_shown if _lead_key(item)]

    low_priority_shown_keys: list[str] = []
    if lows_enabled and content_filter not in {"all", "low"}:
        low_priority_all = [item for item in all_leads_deduped if _lead_score(item) < 6]
        try:
            low_limit = int(customer_config.get("low_signals_limit", os.getenv("LOW_SIGNALS_LIMIT", "8")))
        except Exception:
            low_limit = 8
        low_limit = max(0, min(25, low_limit))
        low_priority = _sort_low_priority(low_priority_all)[:low_limit]
        low_priority_shown_keys = [_lead_key(item) for item in low_priority if _lead_key(item)]
        shown_keys = _append_unique_in_order(shown_keys, low_priority_shown_keys)

    low_fallback_keys: list[str] = []
    if low_fallback:
        low_fallback_keys = [_lead_key(item) for item in low_fallback if _lead_key(item)]
        shown_keys = _append_unique_in_order(shown_keys, low_fallback_keys)

    return {
        "for_date": for_date_obj.isoformat(),
        "snapshot_mode": snapshot_mode,
        "previous_sent_ts_utc": previous_sent_ts_utc or "",
        "window_start": window_start.isoformat() if isinstance(window_start, datetime) else None,
        "strict_first_seen_after": strict_first_seen_after.isoformat() if isinstance(strict_first_seen_after, datetime) else None,
        "content_filter": content_filter,
        "tier_counts": tier_counts,
        "stats": stats,
        "shown_lead_keys": shown_keys,
        "main_shown_lead_keys": [_lead_key(item) for item in main_shown if _lead_key(item)],
        "low_priority_shown_lead_keys": low_priority_shown_keys,
        "low_fallback_lead_keys": low_fallback_keys,
        "low_available_lead_keys": low_available_keys,
        "lows_enabled": bool(lows_enabled),
        "max_first_seen_utc": max_first_seen_utc or "",
    }


def digest_diff(expected_keys: list[str], rendered_keys: list[str]) -> dict[str, list[str]]:
    expected_set = {str(item or "").strip() for item in expected_keys if str(item or "").strip()}
    rendered_set = {str(item or "").strip() for item in rendered_keys if str(item or "").strip()}
    missing = sorted(expected_set - rendered_set)
    unexpected = sorted(rendered_set - expected_set)
    return {"missing": missing, "unexpected": unexpected}


def load_lead_rows_for_range(
    *,
    leads_db_path: str,
    start_date: str,
    end_date: str,
    states: list[str],
) -> list[dict[str, Any]]:
    conn = sqlite3.connect(leads_db_path)
    conn.row_factory = sqlite3.Row
    try:
        placeholders = ",".join("?" for _ in states)
        rows = conn.execute(
            f"""
            SELECT
                lead_key,
                activity_nr,
                lead_score,
                establishment_name,
                site_city,
                site_state,
                inspection_type,
                date_opened,
                first_seen_at,
                source_url
            FROM inspections
            WHERE parse_invalid = 0
              AND site_state IN ({placeholders})
              AND date(first_seen_at) >= date(?)
              AND date(first_seen_at) <= date(?)
            ORDER BY first_seen_at ASC, lead_score DESC
            """,
            tuple(states + [start_date, end_date]),
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def collect_expected_signals_for_range(
    *,
    leads_db_path: str,
    territory_code: str,
    states: list[str],
    start_date: str,
    end_date: str,
) -> dict[str, dict[str, Any]]:
    rows = load_lead_rows_for_range(
        leads_db_path=leads_db_path,
        start_date=start_date,
        end_date=end_date,
        states=states,
    )
    matched, _stats = filter_by_territory(rows, territory_code)
    out: dict[str, dict[str, Any]] = {}
    for item in matched:
        key = _lead_key(item)
        if not key:
            continue
        if key not in out:
            out[key] = dict(item)
    return out


def collect_delivered_keys_for_range(
    *,
    repo_root: Path,
    leads_db_path: str,
    subscriber_key: str,
    primary_recipient: str,
    tz_name: str,
    customer_config: dict[str, Any],
    start_date: str,
    end_date: str,
    crm_db_path: str | Path | None,
    data_root: Path | None = None,
) -> tuple[set[str], dict[str, Any]]:
    events = load_live_daily_events(
        subscriber_key=subscriber_key,
        tz_name=tz_name,
        primary_recipient=primary_recipient,
        crm_db_path=crm_db_path,
    )
    start_obj = date.fromisoformat(start_date)
    end_obj = date.fromisoformat(end_date)
    target_dates = sorted(
        {
            str(item.get("local_date") or "").strip()
            for item in events
            if str(item.get("local_date") or "").strip()
            and start_obj <= date.fromisoformat(str(item.get("local_date"))) <= end_obj
        }
    )
    delivered: set[str] = set()
    per_date: dict[str, Any] = {}
    for local_date in target_dates:
        rendered = load_rendered_digest_for_date(
            repo_root=repo_root,
            leads_db_path=leads_db_path,
            subscriber_key=subscriber_key,
            for_date=local_date,
            customer_config=customer_config,
            data_root=data_root,
        )
        shown = [str(item or "").strip() for item in (rendered.get("shown_lead_keys") or []) if str(item or "").strip()]
        per_date[local_date] = {
            "source": str(rendered.get("source") or ""),
            "shown_count": len(shown),
            "shown_lead_keys": shown,
        }
        for key in shown:
            delivered.add(key)
    return delivered, {"dates": target_dates, "per_date": per_date}


def build_missed_signal_rows(
    *,
    expected_by_key: dict[str, dict[str, Any]],
    delivered_keys: set[str],
    tz_name: str,
) -> list[dict[str, Any]]:
    zone = _resolve_zone(tz_name)
    rows: list[dict[str, Any]] = []
    for key, item in expected_by_key.items():
        if key in delivered_keys:
            continue
        score = _lead_score(item)
        if score >= 10:
            tier = "high"
            priority = "High"
        elif score >= 6:
            tier = "medium"
            priority = "Medium"
        else:
            tier = "low"
            priority = "Low"
        observed_dt = _parse_any_ts(item.get("first_seen_at"))
        observed_local = ""
        if observed_dt is not None:
            observed_local = observed_dt.astimezone(zone).strftime("%Y-%m-%d %H:%M")
        rows.append(
            {
                "priority": priority,
                "tier": tier,
                "company": str(item.get("establishment_name") or "").strip(),
                "city": str(item.get("site_city") or "").strip(),
                "state": str(item.get("site_state") or "").strip().upper(),
                "signal": str(item.get("inspection_type") or "").strip(),
                "event_date": str(item.get("date_opened") or "").strip(),
                "observed_at_local": observed_local,
                "activity_nr": str(item.get("activity_nr") or "").strip(),
                "lead_key": key,
                "osha_url": str(item.get("source_url") or "").strip(),
            }
        )
    rows.sort(
        key=lambda row: (
            row.get("event_date") or "",
            row.get("priority") or "",
            row.get("activity_nr") or "",
        )
    )
    return rows


def write_missed_signals_artifacts(
    *,
    out_dir: Path,
    start_date: str,
    end_date: str,
    missed_rows: list[dict[str, Any]],
    expected_total: int,
    delivered_total: int,
) -> tuple[Path, Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    stem = f"missed_signals_{start_date}_to_{end_date}"
    csv_path = out_dir / f"{stem}.csv"
    txt_path = out_dir / f"{stem}.txt"

    fields = [
        "priority",
        "tier",
        "company",
        "city",
        "state",
        "signal",
        "event_date",
        "observed_at_local",
        "activity_nr",
        "lead_key",
        "osha_url",
    ]
    with open(csv_path, "w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        for row in missed_rows:
            writer.writerow({k: row.get(k, "") for k in fields})

    lines = [
        f"Missed Signals Report {start_date} to {end_date}",
        f"expected_now={expected_total}",
        f"delivered={delivered_total}",
        f"missed={len(missed_rows)}",
        "",
    ]
    if missed_rows:
        for row in missed_rows[:100]:
            lines.append(
                f"{row.get('priority','')} | {row.get('event_date','')} | {row.get('company','')} | "
                f"{row.get('city','')}, {row.get('state','')} | {row.get('lead_key','')}"
            )
        if len(missed_rows) > 100:
            lines.append("")
            lines.append(f"... {len(missed_rows) - 100} more rows in CSV")
    txt_path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    return csv_path, txt_path
