from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import crm_light
import trial_audit
from lead_filters import filter_by_territory, load_territory_definitions, resolve_territory_code
from run_trial_admin import _resolve_customer_config_for_subscriber
from scoring import osha_detail_cache as scoring_osha_detail_cache
from scoring import triage_overlay as scoring_triage_overlay
import send_digest_email


ERR_AUDIT_TRIAL_SIGNALS_CONFIG = "ERR_AUDIT_TRIAL_SIGNALS_CONFIG"


def _emit(key: str, value: object) -> None:
    print(f"{key}={value}")


def _error(detail: str) -> int:
    print(f"{ERR_AUDIT_TRIAL_SIGNALS_CONFIG} {detail}", file=sys.stderr)
    return 1


def _has_column(conn: sqlite3.Connection, table: str, column: str) -> bool:
    try:
        rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    except Exception:
        return False
    for row in rows:
        try:
            name = str(row[1] or "")
        except Exception:
            name = ""
        if name.lower() == column.lower():
            return True
    return False


def _row_key(row: dict[str, Any]) -> str:
    return str(row.get("lead_key") or row.get("activity_nr") or row.get("lead_id") or "").strip()


def _norm_date(value: str) -> str:
    return date.fromisoformat(str(value or "").strip()).isoformat()


def _score_value(row: dict[str, Any]) -> int:
    try:
        return int(row.get("lead_score") or 0)
    except Exception:
        return 0


def _tier_for_score(score: int) -> str:
    high_min = int(send_digest_email.TIER_THRESHOLDS.get("high_min", 10))
    med_min = int(send_digest_email.TIER_THRESHOLDS.get("medium_min", 6))
    if int(score) >= high_min:
        return "High"
    if int(score) >= med_min:
        return "Medium"
    return "Low"


def _resolve_output_path(subscriber_key: str, since_date: str, through_date: str) -> Path:
    sk = crm_light.normalize_subscriber_key(subscriber_key)
    return crm_light.data_dir() / "trials" / sk / f"signal_audit_{since_date}_{through_date}.json"


def _read_customer_config(subscriber_key: str, explicit_customer_path: str) -> tuple[Path, dict[str, Any]]:
    path, cfg = _resolve_customer_config_for_subscriber(crm_light.normalize_subscriber_key(subscriber_key), explicit_customer_path)
    if path is None:
        raise ValueError(f"customer_config_not_found subscriber_key={subscriber_key}")
    if not isinstance(cfg, dict) or not cfg:
        raise ValueError(f"customer_config_invalid path={path}")
    return path, cfg


def _load_subscriber_context(
    *,
    subscriber_key: str,
    crm_db_path: Path,
    customer_config: dict[str, Any],
) -> dict[str, str]:
    tz_name = str(customer_config.get("timezone") or customer_config.get("tz") or "America/Chicago").strip() or "America/Chicago"
    recipients = customer_config.get("recipients") or customer_config.get("email_recipients") or []
    primary_recipient = ""
    if isinstance(recipients, list) and recipients:
        primary_recipient = str(recipients[0] or "").strip().lower()

    if crm_db_path.exists():
        conn = sqlite3.connect(f"file:{crm_db_path}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
        try:
            row = conn.execute(
                """
                SELECT subscriber_key, email, territory_code, tz
                FROM subscribers
                WHERE subscriber_key = ?
                LIMIT 1
                """,
                (crm_light.normalize_subscriber_key(subscriber_key),),
            ).fetchone()
            if row:
                if str(row["email"] or "").strip():
                    primary_recipient = str(row["email"] or "").strip().lower()
                if str(row["tz"] or "").strip():
                    tz_name = str(row["tz"] or "").strip()
        finally:
            conn.close()
    return {"primary_recipient": primary_recipient, "tz_name": tz_name}


def _query_opened_range_rows(db_path: Path, since_date: str, through_date: str) -> list[dict[str, Any]]:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        lead_key_expr = "lead_key" if _has_column(conn, "inspections", "lead_key") else "('osha:activity:' || activity_nr) AS lead_key"
        mail_zip_expr = "mail_zip" if _has_column(conn, "inspections", "mail_zip") else "NULL AS mail_zip"
        site_county_expr = "site_county" if _has_column(conn, "inspections", "site_county") else "NULL AS site_county"
        area_office_expr = "area_office" if _has_column(conn, "inspections", "area_office") else "NULL AS area_office"
        changed_at_expr = "changed_at" if _has_column(conn, "inspections", "changed_at") else "NULL AS changed_at"
        rows = conn.execute(
            f"""
            SELECT
                {lead_key_expr},
                activity_nr,
                date_opened,
                inspection_type,
                scope,
                case_status,
                establishment_name,
                site_city,
                site_state,
                site_zip,
                {mail_zip_expr},
                {site_county_expr},
                {area_office_expr},
                lead_score,
                first_seen_at,
                last_seen_at,
                {changed_at_expr},
                source_url
            FROM inspections
            WHERE parse_invalid = 0
              AND UPPER(COALESCE(site_state, '')) = 'TX'
              AND date(date_opened) >= date(?)
              AND date(date_opened) <= date(?)
            ORDER BY date_opened ASC, activity_nr ASC
            """,
            (since_date, through_date),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def _collect_territory_debug(
    rows: list[dict[str, Any]],
    territory_code: str,
) -> tuple[dict[str, dict[str, Any]], set[str]]:
    _filtered, _stats, debug_rows = filter_by_territory(rows, territory_code, include_debug=True)
    by_key: dict[str, dict[str, Any]] = {}
    matched_keys: set[str] = set()
    for item in debug_rows:
        key = str(item.get("lead_key") or "").strip()
        if not key:
            continue
        by_key[key] = dict(item)
        if str(item.get("matched") or "").strip().upper() == "Y":
            matched_keys.add(key)
    # Fallback if debug rows omitted for any reason.
    for row in rows:
        key = _row_key(row)
        if key and key not in by_key:
            by_key[key] = {
                "lead_key": key,
                "resolved_cbsa": "",
                "matched": "N",
                "match_reason": "UNKNOWN",
                "unmatched_reason": "UNKNOWN",
                "resolution_source": "NONE",
            }
    return by_key, matched_keys


def _load_delivered_rendered_sets(
    *,
    subscriber_key: str,
    leads_db_path: Path,
    customer_config: dict[str, Any],
    crm_db_path: Path,
    tz_name: str,
    primary_recipient: str,
    since_date: str,
    through_date: str,
) -> tuple[set[str], dict[str, set[str]], list[dict[str, Any]]]:
    events_all = trial_audit.load_live_daily_events(
        subscriber_key=subscriber_key,
        tz_name=tz_name,
        primary_recipient=primary_recipient,
        crm_db_path=crm_db_path,
    )
    start_obj = date.fromisoformat(since_date)
    end_obj = date.fromisoformat(through_date)
    events_in_range = []
    for event in events_all:
        local_date = str(event.get("local_date") or "").strip()
        if not local_date:
            continue
        try:
            d = date.fromisoformat(local_date)
        except Exception:
            continue
        if start_obj <= d <= end_obj:
            events_in_range.append(dict(event))
    per_date_shown: dict[str, set[str]] = {}
    delivered: set[str] = set()
    for event in events_in_range:
        local_date = str(event.get("local_date") or "")
        rendered = trial_audit.load_rendered_digest_for_date(
            repo_root=REPO_ROOT,
            leads_db_path=str(leads_db_path),
            subscriber_key=subscriber_key,
            for_date=local_date,
            customer_config=customer_config,
            data_root=crm_light.data_dir(),
        )
        shown = {
            str(x or "").strip()
            for x in (rendered.get("shown_lead_keys") or [])
            if str(x or "").strip()
        }
        per_date_shown[local_date] = shown
        delivered |= shown
    return delivered, per_date_shown, events_all


def _daily_filter_run_params(customer_config: dict[str, Any], previous_sent_ts_utc: str | None) -> dict[str, Any]:
    baseline_on_first_send = bool(customer_config.get("baseline_on_first_send", True))
    prev_dt = trial_audit._parse_utc_ts(previous_sent_ts_utc or "")  # type: ignore[attr-defined]
    snapshot_mode = bool(baseline_on_first_send and prev_dt is None)
    skip_first_seen_filter = False
    use_opened_window = False
    window_start = None
    new_only_cutoff = None
    strict_first_seen_after = None
    include_changed = False
    if snapshot_mode:
        use_opened_window = True
        skip_first_seen_filter = True
    elif prev_dt is not None:
        window_start = prev_dt
        strict_first_seen_after = prev_dt
        skip_first_seen_filter = True
    return {
        "snapshot_mode": snapshot_mode,
        "skip_first_seen_filter": skip_first_seen_filter,
        "use_opened_window": use_opened_window,
        "window_start": window_start,
        "new_only_cutoff": new_only_cutoff,
        "strict_first_seen_after": strict_first_seen_after,
        "include_changed": include_changed,
    }


def _map_exclusion_reason_token(token: str) -> str:
    text = str(token or "").strip().upper()
    if "CONTENT_FILTER" in text:
        return "below threshold"
    if "DEDUPE" in text:
        return "dedupe"
    if "NEW_ONLY" in text:
        return "no new first_seen"
    if "TIME_WINDOW" in text:
        return "stale"
    if "TERRITORY" in text or "CBSA_" in text or "STATE_NO_MATCH" in text:
        return "outside territory"
    return ""


def _reconstruct_filter_exclusions(
    *,
    subscriber_key: str,
    leads_db_path: Path,
    customer_config: dict[str, Any],
    territory_code: str,
    events_all: list[dict[str, Any]],
    since_date: str,
    through_date: str,
) -> tuple[dict[str, str], dict[str, set[str]], dict[str, set[str]], dict[str, Any]]:
    states = [str(s).strip().upper() for s in (customer_config.get("states") or ["TX"]) if str(s).strip()]
    if not states:
        states = ["TX"]
    start_obj = date.fromisoformat(since_date)
    end_obj = date.fromisoformat(through_date)
    # Match production entitlement lookup behavior (read-only and tolerant of missing tables).
    entitlement, subscriber_cbsa_allowlist = send_digest_email._load_subscriber_entitlement_and_allowlist(  # type: ignore[attr-defined]
        subscriber_key=subscriber_key,
        email="",
    )
    del entitlement

    reason_by_key: dict[str, str] = {}
    pre_overlay_by_date: dict[str, set[str]] = {}
    all_candidate_keys_by_date: dict[str, set[str]] = {}
    per_date_debug: dict[str, Any] = {}

    sorted_events = sorted(
        [dict(e) for e in (events_all or []) if str(e.get("ts_utc") or "").strip()],
        key=lambda e: str(e.get("ts_utc") or ""),
    )
    for idx, event in enumerate(sorted_events):
        local_date = str(event.get("local_date") or "").strip()
        if not local_date:
            continue
        try:
            event_date = date.fromisoformat(local_date)
        except Exception:
            continue
        if not (start_obj <= event_date <= end_obj):
            continue
        prev_ts = str(sorted_events[idx - 1].get("ts_utc") or "").strip() if idx > 0 else ""
        ref_now = datetime.fromisoformat(f"{local_date}T12:00:00")
        params = _daily_filter_run_params(customer_config, prev_ts or None)
        conn = sqlite3.connect(str(leads_db_path))
        conn.row_factory = sqlite3.Row
        try:
            leads, _low_fallback, stats, _territory_debug, exclusion_rows = send_digest_email.get_leads_for_period(
                conn=conn,
                states=states,
                since_days=int(customer_config.get("opened_window_days") or 14),
                new_only_days=int(customer_config.get("new_only_days") or 1),
                skip_first_seen_filter=bool(params["skip_first_seen_filter"]),
                territory_code=territory_code,
                content_filter=str(customer_config.get("content_filter") or "high_medium"),
                include_low_fallback=bool(customer_config.get("include_low_fallback", False)),
                window_start=params["window_start"],
                new_only_cutoff=params["new_only_cutoff"],
                strict_first_seen_after=params["strict_first_seen_after"],
                include_changed=bool(params["include_changed"]),
                use_opened_window=bool(params["use_opened_window"]),
                reference_now=ref_now,
                subscriber_cbsa_allowlist=subscriber_cbsa_allowlist,
                return_debug=True,
            )
            pre_overlay_by_date[local_date] = {_row_key(r) for r in leads if _row_key(r)}
            all_candidate_keys_by_date[local_date] = set(pre_overlay_by_date[local_date])
            per_date_debug[local_date] = {"stats": dict(stats), "exclusion_count": len(exclusion_rows)}
            for item in exclusion_rows:
                key = str(item.get("lead_key") or item.get("activity_nr") or "").strip()
                reason = _map_exclusion_reason_token(str(item.get("reason") or item.get("stage") or ""))
                if key and reason and key not in reason_by_key:
                    reason_by_key[key] = reason
        finally:
            conn.close()
    return reason_by_key, pre_overlay_by_date, all_candidate_keys_by_date, per_date_debug


def _compute_triage_overlay_annotations(
    *,
    rows_by_key: dict[str, dict[str, Any]],
    matched_keys: set[str],
    pre_overlay_by_date: dict[str, set[str]],
) -> tuple[dict[str, dict[str, str]], set[str]]:
    matched_rows = [dict(rows_by_key[k]) for k in sorted(matched_keys) if k in rows_by_key]
    activity_nrs = [str(r.get("activity_nr") or "").strip() for r in matched_rows if str(r.get("activity_nr") or "").strip()]
    cache_rows = scoring_osha_detail_cache.load_detail_cache_rows(None, activity_nrs)
    decisions = scoring_triage_overlay.triage(matched_rows, cache_rows, mode="trial_render")
    decision_map = scoring_triage_overlay.decisions_by_activity(decisions)

    annotations: dict[str, dict[str, str]] = {}
    for key in matched_keys:
        row = rows_by_key.get(key, {})
        d = decision_map.get(str(row.get("activity_nr") or key).strip()) or decision_map.get(key) or {}
        action = str(d.get("action") or "").strip()
        reasons = [str(x).strip().lower() for x in (d.get("reasons") or []) if str(x).strip()]
        provenance_source = str((d.get("provenance") or {}).get("source") or "").strip()
        annotations[key] = {
            "triage_decision": action,
            "triage_reason": ";".join(reasons[:4]),
            "ai_triage_decision": action if provenance_source == "ai_cached" and action else "",
        }

    changed_membership_keys: set[str] = set()
    if pre_overlay_by_date:
        # Build once for lookup efficiency.
        decisions_list = [d for d in decisions]
        for local_date, pre_keys in pre_overlay_by_date.items():
            leads = [dict(rows_by_key[k]) for k in sorted(pre_keys) if k in rows_by_key]
            post_leads, _stats, _promoted = scoring_triage_overlay.apply_trial_overlay_to_leads(leads, decisions_list)
            post_keys = {_row_key(r) for r in post_leads if _row_key(r)}
            changed_membership_keys |= (set(pre_keys) ^ post_keys)
    return annotations, changed_membership_keys


def build_audit_payload(
    *,
    subscriber_key: str,
    leads_db_path: Path,
    crm_db_path: Path,
    customer_path: Path,
    customer_config: dict[str, Any],
    since_date: str,
    through_date: str,
    with_triage_overlay: bool,
) -> dict[str, Any]:
    territory_code = resolve_territory_code(
        str(customer_config.get("territory_code") or "TX_TRI"),
        load_territory_definitions(),
    )
    subscriber_ctx = _load_subscriber_context(
        subscriber_key=subscriber_key,
        crm_db_path=crm_db_path,
        customer_config=customer_config,
    )
    all_rows = _query_opened_range_rows(leads_db_path, since_date, through_date)
    territory_debug_by_key, matched_keys = _collect_territory_debug(all_rows, territory_code)

    delivered_keys, delivered_sets_by_date, events_all = _load_delivered_rendered_sets(
        subscriber_key=subscriber_key,
        leads_db_path=leads_db_path,
        customer_config=customer_config,
        crm_db_path=crm_db_path,
        tz_name=subscriber_ctx["tz_name"],
        primary_recipient=subscriber_ctx["primary_recipient"],
        since_date=since_date,
        through_date=through_date,
    )

    exclusion_reason_by_key, pre_overlay_by_date, _all_candidate_sets, per_date_filter_debug = _reconstruct_filter_exclusions(
        subscriber_key=subscriber_key,
        leads_db_path=leads_db_path,
        customer_config=customer_config,
        territory_code=territory_code,
        events_all=events_all,
        since_date=since_date,
        through_date=through_date,
    )

    rows_by_key = {_row_key(r): dict(r) for r in all_rows if _row_key(r)}
    triage_annotations: dict[str, dict[str, str]] = {}
    changed_membership_keys: set[str] = set()
    if with_triage_overlay:
        triage_annotations, changed_membership_keys = _compute_triage_overlay_annotations(
            rows_by_key=rows_by_key,
            matched_keys=matched_keys,
            pre_overlay_by_date=pre_overlay_by_date,
        )

    report_rows: list[dict[str, Any]] = []
    for row in all_rows:
        key = _row_key(row)
        terr = territory_debug_by_key.get(key, {})
        score = _score_value(row)
        in_territory = str(terr.get("matched") or "").strip().upper() == "Y"
        delivered = key in delivered_keys
        excluded_reason = ""
        if not in_territory:
            excluded_reason = "outside territory"
        elif not delivered:
            excluded_reason = exclusion_reason_by_key.get(key, "")

        out_row = {
            "date_opened": str(row.get("date_opened") or "").strip(),
            "activity_nr": str(row.get("activity_nr") or "").strip(),
            "establishment_name": str(row.get("establishment_name") or "").strip(),
            "site_city": str(row.get("site_city") or "").strip(),
            "site_zip": str(row.get("site_zip") or "").strip(),
            "resolved_cbsa": str(terr.get("resolved_cbsa") or "").strip(),
            "score": int(score),
            "tier": _tier_for_score(score),
            "was_delivered": "Y" if delivered else "N",
            "excluded_reason": excluded_reason,
            "lead_key": key,
            "territory_matched": "Y" if in_territory else "N",
            "territory_match_reason": str(terr.get("match_reason") or "").strip(),
            "territory_unmatched_reason": str(terr.get("unmatched_reason") or "").strip(),
        }
        if with_triage_overlay:
            ann = triage_annotations.get(key, {})
            out_row["triage_decision"] = str(ann.get("triage_decision") or "")
            out_row["triage_reason"] = str(ann.get("triage_reason") or "")
            out_row["ai_triage_decision"] = str(ann.get("ai_triage_decision") or "")
            out_row["would_have_changed_delivery"] = "Y" if key in changed_membership_keys else "N"
        report_rows.append(out_row)

    summary = {
        "total_tx_opened_rows": len(all_rows),
        "territory_matched": sum(1 for r in report_rows if r["territory_matched"] == "Y"),
        "delivered": sum(1 for r in report_rows if r["was_delivered"] == "Y"),
        "undelivered_matched": sum(1 for r in report_rows if r["territory_matched"] == "Y" and r["was_delivered"] == "N"),
        "excluded_reason_counts": {},
        "tier_counts_all": {"High": 0, "Medium": 0, "Low": 0},
        "tier_counts_matched": {"High": 0, "Medium": 0, "Low": 0},
        "tier_counts_delivered": {"High": 0, "Medium": 0, "Low": 0},
    }
    reason_counts: dict[str, int] = {}
    for r in report_rows:
        tier = str(r.get("tier") or "")
        if tier in summary["tier_counts_all"]:
            summary["tier_counts_all"][tier] += 1
        if r.get("territory_matched") == "Y" and tier in summary["tier_counts_matched"]:
            summary["tier_counts_matched"][tier] += 1
        if r.get("was_delivered") == "Y" and tier in summary["tier_counts_delivered"]:
            summary["tier_counts_delivered"][tier] += 1
        rsn = str(r.get("excluded_reason") or "").strip()
        if rsn:
            reason_counts[rsn] = reason_counts.get(rsn, 0) + 1
    summary["excluded_reason_counts"] = dict(sorted(reason_counts.items()))

    return {
        "generated_at_utc": datetime.utcnow().isoformat() + "Z",
        "config": {
            "subscriber_key": crm_light.normalize_subscriber_key(subscriber_key),
            "since_date": since_date,
            "through_date": through_date,
            "with_triage_overlay": bool(with_triage_overlay),
            "leads_db_path": str(leads_db_path),
            "crm_db_path": str(crm_db_path),
            "customer_path": str(customer_path),
            "customer_id": str(customer_config.get("customer_id") or ""),
            "territory_code": territory_code,
            "tz_name": subscriber_ctx["tz_name"],
            "primary_recipient": subscriber_ctx["primary_recipient"],
        },
        "send_events": {
            "in_range_local_dates": sorted(delivered_sets_by_date.keys()),
            "rendered_delivered_key_counts_by_date": {
                d: len(v) for d, v in sorted(delivered_sets_by_date.items())
            },
        },
        "per_date_filter_debug": per_date_filter_debug,
        "summary": summary,
        "rows": report_rows,
    }


def _print_table(payload: dict[str, Any], with_triage_overlay: bool) -> None:
    base_cols = [
        "date_opened",
        "activity_nr",
        "establishment_name",
        "site_city",
        "site_zip",
        "resolved_cbsa",
        "score",
        "tier",
        "was_delivered",
        "excluded_reason",
    ]
    extra_cols = ["triage_decision", "triage_reason", "ai_triage_decision", "would_have_changed_delivery"] if with_triage_overlay else []
    cols = base_cols + extra_cols
    print("\t".join(cols))
    for row in payload.get("rows") or []:
        print("\t".join(str(row.get(c, "")) for c in cols))


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="Deterministic trial signal audit for a subscriber/date range.")
    ap.add_argument("--subscriber-key", required=True)
    ap.add_argument("--since-date", required=True, help="YYYY-MM-DD inclusive")
    ap.add_argument("--through-date", required=True, help="YYYY-MM-DD inclusive")
    ap.add_argument("--db", default="data/osha.sqlite", help="Leads SQLite db path")
    ap.add_argument("--crm-db", default="", help="Optional crm_light sqlite path override")
    ap.add_argument("--customer", default="", help="Optional customer config JSON path")
    ap.add_argument("--with-triage-overlay", action="store_true", help="Append rules/AI triage overlay columns (read-only cache use)")
    ap.add_argument("--print-config", action="store_true", help="Print resolved config and exit")
    ap.add_argument("--dry-run", action="store_true", help="No artifact writes")
    return ap


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        since_date = _norm_date(args.since_date)
        through_date = _norm_date(args.through_date)
    except Exception:
        return _error("invalid_date expected=YYYY-MM-DD")
    if since_date > through_date:
        return _error(f"date_range_invalid since_date={since_date} through_date={through_date}")

    leads_db_path = Path(args.db).expanduser().resolve(strict=False)
    crm_db_path = crm_light.resolve_crm_db_path(str(args.crm_db or "").strip() or None)
    try:
        customer_path, customer_config = _read_customer_config(args.subscriber_key, str(args.customer or ""))
    except Exception as exc:
        return _error(str(exc))
    out_path = _resolve_output_path(args.subscriber_key, since_date, through_date)

    _emit("SIGNAL_AUDIT_SUBSCRIBER_KEY", crm_light.normalize_subscriber_key(args.subscriber_key))
    _emit("SIGNAL_AUDIT_SINCE_DATE", since_date)
    _emit("SIGNAL_AUDIT_THROUGH_DATE", through_date)
    _emit("SIGNAL_AUDIT_LEADS_DB", str(leads_db_path))
    _emit("SIGNAL_AUDIT_CRM_DB", str(crm_db_path))
    _emit("SIGNAL_AUDIT_CUSTOMER_PATH", str(customer_path))
    _emit("SIGNAL_AUDIT_OUTPUT_JSON", str(out_path))
    _emit("SIGNAL_AUDIT_TRIAGE_OVERLAY", 1 if args.with_triage_overlay else 0)
    _emit("SIGNAL_AUDIT_DRY_RUN", 1 if args.dry_run else 0)

    if args.print_config:
        _emit("SIGNAL_AUDIT_COMPLETE", "status=PRINT_CONFIG")
        return 0

    if not leads_db_path.exists():
        return _error(f"leads_db_missing path={leads_db_path}")

    payload = build_audit_payload(
        subscriber_key=str(args.subscriber_key),
        leads_db_path=leads_db_path,
        crm_db_path=crm_db_path,
        customer_path=customer_path,
        customer_config=customer_config,
        since_date=since_date,
        through_date=through_date,
        with_triage_overlay=bool(args.with_triage_overlay),
    )
    _print_table(payload, with_triage_overlay=bool(args.with_triage_overlay))

    if not args.dry_run:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        _emit("SIGNAL_AUDIT_JSON_WRITTEN", str(out_path))

    _emit("SIGNAL_AUDIT_TOTAL_ROWS", int((payload.get("summary") or {}).get("total_tx_opened_rows", 0)))
    _emit("SIGNAL_AUDIT_COMPLETE", "status=OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
