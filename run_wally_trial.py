#!/usr/bin/env python3
"""Run Wally trial workflow: preflight, estimate counts, preview, live send, and schedule."""

import argparse
import csv
import json
import os
import re
import sqlite3
import subprocess
import sys
import xml.etree.ElementTree as ET
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlparse, urlunparse

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover
    load_dotenv = None

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore[assignment]

from export_daily import export_daily
import crm_light
import run_trial_admin
from send_digest_email import get_leads_for_period
from lead_filters import filter_by_territory, load_territory_definitions, normalize_content_filter, resolve_territory_code
from geo.zip_cbsa import zip_cbsa_dataset_status

DEFAULT_TRIAL_TARGET_LOCAL_HHMM = "09:00"
DEFAULT_TRIAL_CATCHUP_MAX_MINUTES = 180
PROJECT_CONTEXT_SOFT_CHECK_CMD = ["--check", "--soft"]
WALLY_TRIAL_SUBSCRIBER_KEY = "wally_trial"
TRIAL_WEEKDAYS_ONLY = True
TRIAL_WEEKEND_SKIP_TOKEN = "SKIP_NON_WEEKDAY"
TRIAL_SCHEDULE_WEEKDAYS = "MON,TUE,WED,THU,FRI"


def load_environment(repo_root: Path) -> None:
    if load_dotenv is None:
        return
    dotenv_path = repo_root / ".env"
    if dotenv_path.exists():
        load_dotenv(dotenv_path=dotenv_path, override=False)


def resolve_customer_path(customer_arg: str, repo_root: Path) -> Path:
    candidate = Path(customer_arg)
    if candidate.exists():
        return candidate

    by_name = repo_root / "customers" / customer_arg
    if by_name.exists():
        return by_name

    if not customer_arg.lower().endswith(".json"):
        by_name_json = repo_root / "customers" / f"{customer_arg}.json"
        if by_name_json.exists():
            return by_name_json

    return candidate


def parse_recipients(config: dict) -> list[str]:
    recipients = config.get("recipients") or config.get("email_recipients") or []
    if not isinstance(recipients, list):
        return []
    cleaned = []
    seen = set()
    for recipient in recipients:
        email = str(recipient).strip().lower()
        if email and email not in seen:
            seen.add(email)
            cleaned.append(email)
    return cleaned


def preflight(customer_path: Path, require_smtp: bool = True) -> tuple[bool, str]:
    if not customer_path.exists():
        return False, f"CONFIG_ERROR missing variables: CUSTOMER_CONFIG({customer_path})"

    with open(customer_path, "r", encoding="utf-8") as f:
        config = json.load(f)

    missing = []
    brand_name = (config.get("brand_name") or os.getenv("BRAND_NAME") or "").strip()
    mailing_address = (config.get("mailing_address") or os.getenv("MAILING_ADDRESS") or "").strip()

    if not brand_name:
        missing.append("BRAND_NAME")
    if not mailing_address:
        missing.append("MAILING_ADDRESS")

    recipients = parse_recipients(config)
    if not recipients:
        missing.append("RECIPIENTS")

    if require_smtp:
        for key in ["SMTP_HOST", "SMTP_PORT", "SMTP_USER", "SMTP_PASS"]:
            if not os.getenv(key, "").strip():
                missing.append(key)

    if missing:
        return False, f"CONFIG_ERROR missing variables: {', '.join(missing)}"

    # Best-effort prefs endpoint reachability check. Do not fail preflight; just warn so operators
    # can see broken prefs endpoints before a run. The digest sender independently disables links
    # when endpoints are unavailable.
    prefs_ok, prefs_detail = _prefs_links_reachable(timeout_s=2.0)
    if not prefs_ok and prefs_detail not in {"env_disabled"}:
        print(f"PREFS_LINKS_DISABLED detail={prefs_detail}", flush=True)
    return True, "PREFLIGHT_OK"


def _coerce_trial_target_local_hhmm(value: object) -> str:
    text = str(value or "").strip()
    parts = text.split(":")
    if len(parts) != 2:
        return DEFAULT_TRIAL_TARGET_LOCAL_HHMM
    try:
        hour = int(parts[0])
        minute = int(parts[1])
    except ValueError:
        return DEFAULT_TRIAL_TARGET_LOCAL_HHMM
    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        return DEFAULT_TRIAL_TARGET_LOCAL_HHMM
    return f"{hour:02d}:{minute:02d}"


def _coerce_trial_catchup_max_minutes(value: object) -> int:
    try:
        minutes = int(value)
    except (TypeError, ValueError):
        return DEFAULT_TRIAL_CATCHUP_MAX_MINUTES
    if minutes < 0:
        return DEFAULT_TRIAL_CATCHUP_MAX_MINUTES
    return minutes


def _load_customer_config_or_exit(customer_path: Path) -> dict:
    if not customer_path.exists():
        print(f"CONFIG_ERROR missing variables: CUSTOMER_CONFIG({customer_path})")
        raise SystemExit(1)
    try:
        with open(customer_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as exc:
        print(f"CONFIG_ERROR invalid CUSTOMER_CONFIG({customer_path}) error={type(exc).__name__}")
        raise SystemExit(1)


def print_trial_config(customer_path: Path, allow_weekend_send: bool = False) -> None:
    config = _load_customer_config_or_exit(customer_path)
    target = _coerce_trial_target_local_hhmm(config.get("trial_target_local_hhmm"))
    catchup = _coerce_trial_catchup_max_minutes(config.get("trial_catchup_max_minutes"))
    tz_name = str(config.get("timezone") or config.get("tz") or "").strip() or "America/Chicago"
    local_now = datetime.now(_resolve_zone(tz_name))
    weekday_name = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"][int(local_now.weekday())]
    print(f"trial_target_local_hhmm={target}")
    print(f"trial_catchup_max_minutes={catchup}")
    print(f"TRIAL_WEEKDAYS_ONLY={1 if TRIAL_WEEKDAYS_ONLY else 0}")
    print(f"TRIAL_SCHEDULE_WEEKDAYS={TRIAL_SCHEDULE_WEEKDAYS}")
    print(f"trial_effective_timezone={tz_name}")
    print(f"trial_effective_local_date={local_now.date().isoformat()}")
    print(f"trial_effective_weekday={weekday_name}")
    print(f"trial_allow_weekend_send={'YES' if allow_weekend_send else 'NO'}")


def _prefs_links_reachable(timeout_s: float = 2.0) -> tuple[bool, str]:
    """
    Detect whether the unsub prefs endpoints exist and are reachable.

    When disabled/missing/unreachable, callers should set PREFS_LINKS_DISABLED to avoid shipping broken links.
    """
    # Unit tests should be deterministic and offline-safe.
    if "unittest" in sys.modules:
        return True, "skipped_unittest"

    if os.getenv("PREFS_LINKS_DISABLED", "").strip().lower() in {"1", "true", "yes"}:
        return False, "env_disabled"

    base_endpoint = (os.getenv("PREFS_ENDPOINT_BASE", "") or "https://unsub.microflowops.com").strip()
    if not base_endpoint:
        return False, "missing_base"

    try:
        parsed = urlparse(base_endpoint)
        base = urlunparse(parsed._replace(path="", params="", query="", fragment="")).rstrip("/")
    except Exception:
        base = base_endpoint.rstrip("/")

    url = f"{base}/prefs/enable_lows?t=invalid.invalid"

    # Prefer requests (nicer TLS + redirects), fall back to stdlib urllib.
    try:
        import requests  # type: ignore

        resp = requests.get(url, timeout=timeout_s, allow_redirects=False)
        if resp.status_code == 404:
            return False, "http_404"
        if resp.status_code >= 500:
            return False, f"http_{resp.status_code}"
        return True, f"http_{resp.status_code}"
    except Exception:
        pass

    try:
        import urllib.request
        import urllib.error

        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            status = int(getattr(resp, "status", 200))
            if status == 404:
                return False, "http_404"
            if status >= 500:
                return False, f"http_{status}"
            return True, f"http_{status}"
    except urllib.error.HTTPError as e:
        code = int(getattr(e, "code", 0) or 0)
        if code == 404:
            return False, "http_404"
        if code >= 500:
            return False, f"http_{code}"
        return True, f"http_{code}"
    except Exception as exc:
        return False, f"error={type(exc).__name__}"


def estimate_daily_counts(
    db_path: str,
    out_dir: str,
    territory_code: str,
    content_filter: str,
    lookback_days: int,
) -> Path:
    rows = []
    today = date.today()

    for offset in range(lookback_days):
        as_of = today - timedelta(days=offset)
        stats = export_daily(
            db_path=db_path,
            outdir=out_dir,
            as_of_date=as_of.isoformat(),
            territory_code=territory_code,
            content_filter=content_filter,
        )
        rows.append(
            {
                "as_of_date": as_of.isoformat(),
                "sendable_leads": stats["sendable_leads"],
                "excluded_by_territory": stats["excluded_by_territory"],
                "excluded_by_content_filter": stats["excluded_by_content_filter"],
                "deduped_records_removed": stats["deduped_records_removed"],
            }
        )

    rows.sort(key=lambda row: row["as_of_date"])
    output_path = Path(out_dir) / f"wally_trial_daily_counts_{today.isoformat()}.csv"

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "as_of_date",
                "sendable_leads",
                "excluded_by_territory",
                "excluded_by_content_filter",
                "deduped_records_removed",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)

    return output_path


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
    text = str(value or "").strip()
    if not text:
        return None
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except Exception:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _meta_dict(raw: str) -> dict:
    text = (raw or "").strip()
    if not text:
        return {}
    try:
        loaded = json.loads(text)
        if isinstance(loaded, dict):
            return loaded
    except Exception:
        pass
    return {}


def _is_trial_delivery_event(*, variant: str, meta: dict, primary_recipient: str) -> bool:
    normalized_variant = (variant or "").strip().upper()
    if normalized_variant and normalized_variant != "DAILY":
        return False
    normalized_primary = (primary_recipient or "").strip().lower()
    event_recipient = str(meta.get("primary_recipient") or meta.get("recipient") or meta.get("to") or "").strip().lower()
    if event_recipient and normalized_primary and event_recipient != normalized_primary:
        return False
    send_mode = str(meta.get("send_mode") or meta.get("mode") or "").strip().upper()
    if send_mode and send_mode != "LIVE":
        return False
    return True


def _resolve_send_window(config: dict) -> tuple[int, int]:
    send_time = str(config.get("send_time_local") or "08:00").strip()
    hour = 8
    minute = 0
    try:
        hh, mm = send_time.split(":", 1)
        hour = max(0, min(23, int(hh)))
        minute = max(0, min(59, int(mm)))
    except Exception:
        hour = 8
        minute = 0
    try:
        window = int(config.get("send_window_minutes", 60))
    except Exception:
        window = 60
    if window < 1:
        window = 60
    return hour * 60 + minute, window


def _within_send_window(local_dt: datetime, start_minute: int, window_minutes: int) -> bool:
    minute_of_day = local_dt.hour * 60 + local_dt.minute
    return start_minute <= minute_of_day <= (start_minute + window_minutes)


def _extract_inspection_nr(lead: dict) -> str:
    url = str(lead.get("source_url") or "").strip()
    match = re.search(r"id=([0-9.]+)", url, flags=re.IGNORECASE)
    if match:
        return match.group(1)
    return str(lead.get("inspection_nr") or lead.get("activity_nr") or "").strip()


def _legacy_tx_tri_definitions(definitions: dict[str, dict]) -> dict[str, dict]:
    legacy = dict(definitions)
    legacy["TX_TRI_LEGACY_AUDIT"] = {
        "description": "Legacy regex matcher for Texas Triangle (pre-CBSA)",
        "kind": "LEGACY_REGEX",
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
    return legacy


def _has_column(conn: sqlite3.Connection, table: str, column: str) -> bool:
    try:
        rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    except Exception:
        return False
    return any(str(row[1]).lower() == column.lower() for row in rows)


def _resolve_inspection(db_path: str, inspection_value: str) -> dict | None:
    raw = str(inspection_value or "").strip()
    if not raw:
        return None
    base = raw.split(".", 1)[0]
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        site_county_expr = "site_county" if _has_column(conn, "inspections", "site_county") else "NULL AS site_county"
        area_office_expr = "area_office" if _has_column(conn, "inspections", "area_office") else "NULL AS area_office"
        row = conn.execute(
            f"""
            SELECT
                activity_nr,
                lead_key,
                establishment_name,
                site_city,
                site_state,
                site_zip,
                mail_zip,
                {site_county_expr},
                {area_office_expr},
                source_url
            FROM inspections
            WHERE activity_nr = ?
               OR activity_nr = ?
               OR source_url LIKE ?
               OR source_url LIKE ?
            ORDER BY activity_nr ASC
            LIMIT 1
            """,
            (raw, base, f"%id={raw}%", f"%id={base}%"),
        ).fetchone()
        if not row:
            return None
        return dict(row)
    finally:
        conn.close()


def _check_inspection_status(db_path: str, territory_code: str, inspection_value: str) -> dict:
    dataset_status = zip_cbsa_dataset_status()
    dataset_incomplete = bool(dataset_status.get("dataset_incomplete"))
    dataset_source_label = str(dataset_status.get("source_label") or "").strip()
    lead = _resolve_inspection(db_path, inspection_value)
    if not lead:
        return {
            "input": inspection_value,
            "present_in_db": False,
            "present_in_data": False,
            "activity_nr": "",
            "inspection_nr": inspection_value,
            "territory_code": territory_code,
            "matched": False,
            "site_city": "",
            "site_zip": "",
            "mail_zip": "",
            "site_county": "",
            "inspection_office": "",
            "resolved_cbsa": "",
            "resolution_source": "NONE",
            "reason_token": "INSPECTION_NOT_FOUND",
            "unmatched_reason": "INSPECTION_NOT_FOUND",
            "match_reason": "INSPECTION_NOT_FOUND",
            "legacy_matched": False,
            "legacy_reason": "INSPECTION_NOT_FOUND",
            "dataset_incomplete": dataset_incomplete,
            "dataset_source_label": dataset_source_label,
        }

    filtered, _stats, debug_rows = filter_by_territory([lead], territory_code, include_debug=True)
    current_row = debug_rows[0] if debug_rows else {}
    definitions = load_territory_definitions()
    legacy_defs = _legacy_tx_tri_definitions(definitions)
    legacy_filtered, _legacy_stats, legacy_debug = filter_by_territory(
        [lead],
        "TX_TRI_LEGACY_AUDIT",
        definitions=legacy_defs,
        include_debug=True,
    )
    legacy_row = legacy_debug[0] if legacy_debug else {}
    reason_token = str(current_row.get("match_reason") or ("CBSA_MATCH" if filtered else "CBSA_NO_MATCH"))
    inspection_office = str(
        current_row.get("inspection_office")
        or lead.get("area_office")
        or lead.get("office")
        or lead.get("osha_office")
        or ""
    )
    return {
        "input": inspection_value,
        "present_in_db": True,
        "present_in_data": True,
        "activity_nr": str(lead.get("activity_nr") or ""),
        "inspection_nr": _extract_inspection_nr(lead),
        "territory_code": territory_code,
        "matched": bool(filtered),
        "site_city": str(current_row.get("site_city") or lead.get("site_city") or ""),
        "site_zip": str(current_row.get("site_zip") or lead.get("site_zip") or ""),
        "mail_zip": str(current_row.get("mail_zip") or lead.get("mail_zip") or ""),
        "site_county": str(current_row.get("site_county") or lead.get("site_county") or ""),
        "inspection_office": inspection_office,
        "resolved_cbsa": str(current_row.get("resolved_cbsa") or ""),
        "resolution_source": str(current_row.get("resolution_source") or "NONE"),
        "reason_token": reason_token,
        "unmatched_reason": "" if filtered else reason_token,
        "match_reason": reason_token,
        "legacy_matched": bool(legacy_filtered),
        "legacy_reason": str(legacy_row.get("match_reason") or ("LEGACY_MATCH" if legacy_filtered else "LEGACY_NO_MATCH")),
        "dataset_incomplete": dataset_incomplete,
        "dataset_source_label": dataset_source_label,
    }


def run_wally_audit(
    *,
    db_path: str,
    customer_path: Path,
    as_of: str,
    check_inspection: str,
) -> int:
    subscriber_key = WALLY_TRIAL_SUBSCRIBER_KEY
    crm_db = crm_light.resolve_crm_db_path(None)
    if not crm_db.exists():
        print(f"CONFIG_ERROR crm_db missing path={crm_db}")
        return 1
    if not Path(db_path).exists():
        print(f"CONFIG_ERROR leads_db missing path={db_path}")
        return 1
    dataset_status = zip_cbsa_dataset_status()
    dataset_incomplete = bool(dataset_status.get("dataset_incomplete"))
    dataset_source_label = str(dataset_status.get("source_label") or "").strip()

    as_of_date = date.today()
    if str(as_of or "").strip():
        as_of_date = date.fromisoformat(str(as_of).strip())

    with crm_light.open_conn(crm_db) as conn:
        crm_light.init_schema(conn)
        sub = crm_light.get_subscriber(conn, subscriber_key)
        trial = crm_light.get_trial_state(conn, subscriber_key)
        if not sub or not trial:
            print(f"CONFIG_ERROR missing subscriber/trial state for subscriber_key={subscriber_key}")
            return 1
        start_date = str(trial.get("start_date") or "").strip()
        if not start_date:
            print("CONFIG_ERROR missing trial start_date")
            return 1
        start = date.fromisoformat(start_date)
        tz_name = str(sub.get("tz") or "").strip() or "America/Chicago"
        primary_recipient = str(sub.get("email") or "").strip().lower()
        territory_code = str(sub.get("territory_code") or "").strip()
        rows = conn.execute(
            """
            SELECT id, ts_utc, status, variant, run_id, meta_json
            FROM send_events
            WHERE subscriber_key = ?
              AND ts_utc >= ?
            ORDER BY ts_utc ASC, id ASC
            """,
            (subscriber_key, f"{start_date}T00:00:00+00:00"),
        ).fetchall()

    zone = _resolve_zone(tz_name)
    trial_events: list[dict] = []
    for row in rows:
        status = str(row["status"] or "").strip().upper()
        if status != "SENT":
            continue
        ts_utc = str(row["ts_utc"] or "").strip()
        dt_utc = _parse_utc_ts(ts_utc)
        if dt_utc is None:
            continue
        meta = _meta_dict(str(row["meta_json"] or ""))
        variant = str(row["variant"] or "")
        if not _is_trial_delivery_event(variant=variant, meta=meta, primary_recipient=primary_recipient):
            continue
        dt_local = dt_utc.astimezone(zone)
        trial_events.append(
            {
                "id": int(row["id"]),
                "ts_utc": ts_utc,
                "local_iso": dt_local.isoformat(),
                "local_date": dt_local.date().isoformat(),
                "local_weekday": int(dt_local.weekday()),
                "run_id": str(row["run_id"] or ""),
            }
        )

    expected_dates: list[str] = []
    d = start
    while d <= as_of_date:
        if d.weekday() < 5:
            expected_dates.append(d.isoformat())
        d += timedelta(days=1)

    actual_by_date: dict[str, list[dict]] = {}
    for event in trial_events:
        if int(event["local_weekday"]) >= 5:
            continue
        actual_by_date.setdefault(str(event["local_date"]), []).append(event)
    missing_dates = [item for item in expected_dates if item not in actual_by_date]
    duplicate_dates = {k: len(v) for k, v in actual_by_date.items() if len(v) > 1}

    customer_cfg: dict = {}
    if customer_path.exists():
        try:
            customer_cfg = json.loads(customer_path.read_text(encoding="utf-8"))
        except Exception:
            customer_cfg = {}
    send_start_minute, send_window_minutes = _resolve_send_window(customer_cfg)
    window_violations: list[dict] = []
    for event in trial_events:
        local_iso = str(event.get("local_iso") or "")
        try:
            local_dt = datetime.fromisoformat(local_iso)
        except Exception:
            continue
        if not _within_send_window(local_dt, send_start_minute, send_window_minutes):
            window_violations.append(
                {
                    "ts_utc": event["ts_utc"],
                    "local_iso": local_iso,
                    "local_date": event["local_date"],
                    "reason": "OUTSIDE_SEND_WINDOW",
                }
            )

    # Deterministic no-send rebuild for each expected weekday.
    states = [str(state).strip().upper() for state in (customer_cfg.get("states") or ["TX"]) if str(state).strip()]
    if not states:
        states = ["TX"]
    content_filter = normalize_content_filter(str(customer_cfg.get("content_filter") or "high_medium"))
    include_low_fallback = bool(customer_cfg.get("include_low_fallback", True))
    baseline_on_first_send = bool(customer_cfg.get("baseline_on_first_send", True))
    territory_for_audit_raw = territory_code or str(customer_cfg.get("territory_code") or "TX_TRI")
    territory_for_audit = resolve_territory_code(territory_for_audit_raw, load_territory_definitions())
    since_days = int(customer_cfg.get("opened_window_days") or 14)
    new_only_days = int(customer_cfg.get("new_only_days") or 1)

    leads_conn = sqlite3.connect(db_path)
    leads_conn.row_factory = sqlite3.Row
    try:
        per_day: list[dict] = []
        all_exclusions: list[dict] = []
        for expected_date in expected_dates:
            ref_now = datetime.fromisoformat(f"{expected_date}T12:00:00")
            prior_events = [item for item in trial_events if str(item.get("local_date") or "") < expected_date]
            prior_events.sort(key=lambda item: str(item.get("ts_utc") or ""))
            previous_sent_dt = _parse_utc_ts(prior_events[-1]["ts_utc"]) if prior_events else None
            snapshot_mode = baseline_on_first_send and previous_sent_dt is None
            if snapshot_mode:
                use_opened_window = True
                skip_first_seen_filter = True
                window_start = None
                new_only_cutoff = None
                strict_first_seen_after = None
                include_changed = False
            else:
                use_opened_window = False
                skip_first_seen_filter = True
                window_start = previous_sent_dt
                new_only_cutoff = None
                strict_first_seen_after = previous_sent_dt
                include_changed = False

            leads, _low_fallback, stats, _territory_debug, exclusions = get_leads_for_period(
                conn=leads_conn,
                states=states,
                since_days=since_days,
                new_only_days=new_only_days,
                skip_first_seen_filter=skip_first_seen_filter,
                territory_code=territory_for_audit,
                content_filter=content_filter,
                include_low_fallback=include_low_fallback,
                window_start=window_start,
                new_only_cutoff=new_only_cutoff,
                strict_first_seen_after=strict_first_seen_after,
                include_changed=include_changed,
                use_opened_window=use_opened_window,
                reference_now=ref_now,
                return_debug=True,
            )
            per_day.append(
                {
                    "date": expected_date,
                    "expected_send_day": True,
                    "actual_send_events": len(actual_by_date.get(expected_date, [])),
                    "included_count": len(leads),
                    "included_activity_nrs": [str(lead.get("activity_nr") or "") for lead in leads[:50]],
                    "stats": stats,
                }
            )
            for item in exclusions:
                row = dict(item)
                row["as_of_date"] = expected_date
                row["territory_code"] = territory_for_audit
                row.setdefault("dataset_incomplete", str(dataset_incomplete).lower())
                all_exclusions.append(row)
    finally:
        leads_conn.close()

    inspection_details = {}
    if str(check_inspection or "").strip():
        inspection_details = _check_inspection_status(
            db_path=db_path,
            territory_code=territory_for_audit,
            inspection_value=str(check_inspection),
        )

    out_dir = crm_light.data_dir() / "trials" / subscriber_key
    out_dir.mkdir(parents=True, exist_ok=True)
    audit_events_path = out_dir / "audit_events.json"
    audit_exclusions_path = out_dir / "audit_exclusions.csv"
    audit_report_path = out_dir / "audit_report.md"

    payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "subscriber_key": subscriber_key,
        "as_of_date": as_of_date.isoformat(),
        "start_date": start_date,
        "timezone": tz_name,
        "territory_code": territory_for_audit,
        "dataset_incomplete": dataset_incomplete,
        "dataset_source_label": dataset_source_label,
        "expected_send_dates": expected_dates,
        "actual_send_dates": sorted(actual_by_date.keys()),
        "missing_send_dates": missing_dates,
        "duplicate_send_dates": duplicate_dates,
        "window_violations": window_violations,
        "events": trial_events,
        "per_day": per_day,
        "check_inspection": inspection_details,
    }
    audit_events_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")

    exclusion_fields = [
        "as_of_date",
        "inspection_nr",
        "lead_key",
        "activity_nr",
        "site_city",
        "site_state",
        "site_zip",
        "mail_zip",
        "site_county",
        "inspection_office",
        "resolved_cbsa",
        "resolution_source",
        "unmatched_reason",
        "stage",
        "reason",
        "territory_code",
        "dataset_incomplete",
    ]
    with open(audit_exclusions_path, "w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=exclusion_fields)
        writer.writeheader()
        for row in all_exclusions:
            writer.writerow({field: row.get(field, "") for field in exclusion_fields})

    lines = [
        "# Wally Trial Audit",
        "",
        f"- Subscriber: `{subscriber_key}`",
        f"- As of: `{as_of_date.isoformat()}`",
        f"- Territory code: `{territory_for_audit}`",
        f"- ZIP->CBSA dataset incomplete: `{dataset_incomplete}`",
        f"- ZIP->CBSA source label: `{dataset_source_label or 'UNKNOWN'}`",
        f"- Expected weekday send dates: `{len(expected_dates)}`",
        f"- Actual send dates: `{len(actual_by_date)}`",
        f"- Missing send dates: `{', '.join(missing_dates) if missing_dates else 'NONE'}`",
        f"- Duplicate send dates: `{json.dumps(duplicate_dates) if duplicate_dates else 'NONE'}`",
        f"- Send window violations: `{len(window_violations)}`",
        "",
        f"- `audit_events.json`: `{audit_events_path}`",
        f"- `audit_exclusions.csv`: `{audit_exclusions_path}`",
    ]
    if inspection_details:
        lines.extend(
            [
                "",
                "## Check Inspection",
                f"- Input: `{inspection_details.get('input')}`",
                f"- Present in data: `{inspection_details.get('present_in_data')}`",
                f"- Activity: `{inspection_details.get('activity_nr')}`",
                f"- Inspection: `{inspection_details.get('inspection_nr')}`",
                f"- Establishment geo: city=`{inspection_details.get('site_city')}` site_zip=`{inspection_details.get('site_zip')}` mail_zip=`{inspection_details.get('mail_zip')}` county=`{inspection_details.get('site_county')}`",
                f"- Inspection office (info only): `{inspection_details.get('inspection_office')}`",
                f"- CBSA matcher: `matched={inspection_details.get('matched')}` reason=`{inspection_details.get('reason_token')}` resolved_cbsa=`{inspection_details.get('resolved_cbsa')}` source=`{inspection_details.get('resolution_source')}` unmatched_reason=`{inspection_details.get('unmatched_reason')}`",
                f"- Legacy matcher: `matched={inspection_details.get('legacy_matched')}` reason=`{inspection_details.get('legacy_reason')}`",
                f"- ZIP->CBSA dataset incomplete: `{inspection_details.get('dataset_incomplete')}`",
                f"- ZIP->CBSA source label: `{inspection_details.get('dataset_source_label')}`",
            ]
        )
    audit_report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    print(f"AUDIT_OK report={audit_report_path}")
    print(f"AUDIT_EVENTS path={audit_events_path}")
    print(f"AUDIT_EXCLUSIONS path={audit_exclusions_path}")
    if inspection_details:
        print(
            "CHECK_INSPECTION "
            f"present={inspection_details.get('present_in_data')} "
            f"matched={inspection_details.get('matched')} "
            f"reason={inspection_details.get('reason_token')}"
        )
    return 0


def run_preview_send(db_path: str, customer_config: str, chase_email: str) -> None:
    cmd = [
        sys.executable,
        "send_digest_email.py",
        "--db",
        db_path,
        "--customer",
        customer_config,
        "--mode",
        "daily",
        "--recipient-override",
        chase_email,
        "--dry-run",
        "--disable-pilot-guard",
    ]
    subprocess.run(cmd, check=True)


def _append_wally_trial_sent_event(run_id_prefix: str) -> None:
    ts_utc = datetime.now(timezone.utc).isoformat(timespec="seconds")
    run_id = f"{run_id_prefix}_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    try:
        code = run_trial_admin.append_event(
            subscriber_key=WALLY_TRIAL_SUBSCRIBER_KEY,
            ts_utc=ts_utc,
            status="SENT",
            variant="DAILY",
            run_id=run_id,
            crm_db_path=None,
        )
        if code != 0:
            print(
                f"WARN_TRIAL_LEDGER_APPEND_FAILED subscriber_key={WALLY_TRIAL_SUBSCRIBER_KEY} code={code}",
                flush=True,
            )
    except Exception as exc:
        print(
            f"WARN_TRIAL_LEDGER_APPEND_FAILED subscriber_key={WALLY_TRIAL_SUBSCRIBER_KEY} detail={exc}",
            flush=True,
        )


def _wally_local_day_context(customer_config: str) -> dict[str, str | bool]:
    try:
        with open(customer_config, "r", encoding="utf-8") as f:
            cfg = json.load(f)
    except Exception:
        cfg = {}
    tz_name = str(cfg.get("timezone") or cfg.get("tz") or "").strip() or "America/Chicago"
    subscriber_key = str(cfg.get("subscriber_key") or "").strip() or WALLY_TRIAL_SUBSCRIBER_KEY
    now_local = datetime.now(_resolve_zone(tz_name))
    weekday_idx = int(now_local.weekday())
    weekday_name = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"][weekday_idx]
    return {
        "subscriber_key": subscriber_key,
        "timezone": tz_name,
        "local_date": now_local.date().isoformat(),
        "weekday_name": weekday_name,
        "is_weekend": weekday_idx >= 5,
    }


def run_live_send(
    db_path: str,
    customer_config: str,
    admin_email: str,
    send_live: bool,
    allow_weekend_send: bool = False,
) -> None:
    day_ctx = _wally_local_day_context(customer_config)
    if send_live and TRIAL_WEEKDAYS_ONLY and (not allow_weekend_send) and bool(day_ctx.get("is_weekend")):
        print(
            f"{TRIAL_WEEKEND_SKIP_TOKEN} subscriber_key={day_ctx['subscriber_key']} "
            f"local_date={day_ctx['local_date']} weekday={day_ctx['weekday_name']} gate=trial_weekdays_only",
            flush=True,
        )
        return
    cmd = [
        sys.executable,
        "deliver_daily.py",
        "--db",
        db_path,
        "--customer",
        customer_config,
        "--mode",
        "daily",
        "--since-days",
        "14",
        "--admin-email",
        admin_email,
    ]
    if send_live:
        cmd.append("--send-live")
    subprocess.run(cmd, check=True)
    if send_live:
        _append_wally_trial_sent_event(run_id_prefix="manual_wally_trial")


def _load_subscriber_last_sent_at(db_path: str, subscriber_key: str) -> str | None:
    if not subscriber_key:
        return None
    import sqlite3

    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("SELECT last_sent_at FROM subscribers WHERE subscriber_key = ? LIMIT 1", (subscriber_key,))
        row = cur.fetchone()
        if not row:
            return None
        return row[0]
    finally:
        try:
            conn.close()
        except Exception:
            pass


def run_test_send(db_path: str, customer_config: str) -> None:
    # Test-only laptop entrypoint: force snapshot send to Chase, without mutating send state.
    chase_email = "cchevali+oshasmoke@gmail.com"
    print(f"TEST_SEND variant=starter_snapshot recipient={chase_email} state_mutation=NO", flush=True)

    with open(customer_config, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    subscriber_key = (cfg.get("subscriber_key") or "").strip()
    last_sent_before = _load_subscriber_last_sent_at(db_path, subscriber_key)

    cmd = [
        sys.executable,
        "send_digest_email.py",
        "--db",
        db_path,
        "--customer",
        customer_config,
        "--mode",
        "daily",
        "--smoke-cchevali",
        "--force-starter-snapshot",
        "--no-state-mutation",
        "--log-level",
        "ERROR",
    ]
    subprocess.run(cmd, check=True)

    last_sent_after = _load_subscriber_last_sent_at(db_path, subscriber_key)
    if last_sent_after != last_sent_before:
        raise SystemExit(
            f"TEST_SEND_STATE_MUTATION last_sent_at_before={last_sent_before!r} last_sent_at_after={last_sent_after!r}"
        )


def run_test_send_daily(db_path: str, customer_config: str, dry_run: bool = False) -> None:
    # Test-only laptop entrypoint: render the daily "new since last send" variant to Chase,
    # without mutating send state.
    chase_email = "cchevali+oshasmoke@gmail.com"
    print(f"TEST_SEND variant=daily_new_since_last_send recipient={chase_email} state_mutation=NO", flush=True)

    with open(customer_config, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    subscriber_key = (cfg.get("subscriber_key") or "").strip()
    last_sent_before = _load_subscriber_last_sent_at(db_path, subscriber_key)

    # Resilience: the real PC config is git-ignored; default snapshot_when_0_new to YES for the Wally trial
    # when the key is missing, without mutating the source file.
    cfg_for_send = cfg
    is_wally_trial = (cfg.get("customer_id") == "wally_trial_tx_triangle_v1") or (subscriber_key == "wally_trial")
    if is_wally_trial and "snapshot_when_0_new" not in cfg:
        print("TRIAL_DEFAULT snapshot_when_0_new=YES (config_missing)")
        cfg_for_send = dict(cfg)
        cfg_for_send["snapshot_when_0_new"] = True
        cfg_for_send.setdefault("snapshot_recent_limit", 8)

        tmp_path = Path("out") / "wally_trial_test_send_daily.customer.json"
        tmp_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path.write_text(json.dumps(cfg_for_send, indent=2) + "\n", encoding="utf-8")
        customer_config = str(tmp_path)

    cmd = [
        sys.executable,
        "send_digest_email.py",
        "--db",
        db_path,
        "--customer",
        customer_config,
        "--mode",
        "daily",
        "--smoke-cchevali",
        "--no-state-mutation",
        "--log-level",
        "ERROR",
    ]
    if dry_run:
        cmd.append("--dry-run")
    subprocess.run(cmd, check=True)

    last_sent_after = _load_subscriber_last_sent_at(db_path, subscriber_key)
    if last_sent_after != last_sent_before:
        raise SystemExit(
            f"TEST_SEND_STATE_MUTATION last_sent_at_before={last_sent_before!r} last_sent_at_after={last_sent_after!r}"
        )


def write_batch_runner(batch_path: Path, project_root: Path, customer_config: str, db_path: str, admin_email: str) -> None:
    customer_rel = _relative_batch_path(project_root, customer_config)
    lines = [
        "@echo off",
        "cd /d \"%~dp0\"",
        "if not exist out mkdir out",
        "set RUN_TMP=out\\wally_trial_last_run.log",
        "echo [%date% %time%] Wally trial run start >> out\\wally_trial_task.log",
        "echo [%date% %time%] === RUN HEADER === >> out\\wally_trial_task.log",
        "echo [%date% %time%] batch=%~f0 >> out\\wally_trial_task.log",
        "echo [%date% %time%] cwd=%cd% >> out\\wally_trial_task.log",
        "for /f \"delims=\" %%p in ('where python 2^>nul') do echo [%date% %time%] python=%%p >> out\\wally_trial_task.log",
        "if errorlevel 1 echo [%date% %time%] python=NOT_FOUND >> out\\wally_trial_task.log",
        (
            "powershell -NoProfile -ExecutionPolicy Bypass "
            f"-File \"%~dp0scripts\\run_with_secrets.ps1\" "
            f"python deliver_daily.py --db \"{db_path}\" --customer \"%~dp0{customer_rel}\" "
            f"--mode daily --since-days 14 --admin-email \"{admin_email}\" --send-live "
            "> \"%RUN_TMP%\" 2>&1"
        ),
        "set RUN_EXIT=%ERRORLEVEL%",
        "type \"%RUN_TMP%\" >> out\\wally_trial_task.log",
        "findstr /C:\"CONFIG_ERROR\" \"%RUN_TMP%\" >nul",
        "if %ERRORLEVEL%==0 echo [%date% %time%] CONFIG_ERROR detected >> out\\wally_trial_task.log",
        "if %RUN_EXIT% EQU 0 (",
        "  for /f \"delims=\" %%t in ('powershell -NoProfile -Command \"(Get-Date).ToUniversalTime().ToString(\\\"yyyy-MM-ddTHH:mm:ssK\\\")\"') do set TRIAL_TS_UTC=%%t",
        "  for /f \"delims=\" %%r in ('powershell -NoProfile -Command \"(Get-Date).ToUniversalTime().ToString(\\\"yyyyMMddTHHmmssZ\\\")\"') do set TRIAL_RUN_ID=scheduler_wally_trial_%%r",
        "  py -3 run_trial_admin.py append-event --subscriber-key wally_trial --status SENT --variant DAILY --ts-utc \"%TRIAL_TS_UTC%\" --run-id \"%TRIAL_RUN_ID%\" >> out\\wally_trial_task.log 2>&1",
        "  if errorlevel 1 echo [%date% %time%] WARN_TRIAL_LEDGER_APPEND_FAILED subscriber_key=wally_trial run_id=%TRIAL_RUN_ID% >> out\\wally_trial_task.log",
        ")",
        "if %RUN_EXIT% NEQ 0 echo [%date% %time%] ERROR: Wally trial run failed >> out\\wally_trial_task.log",
        "if %RUN_EXIT% EQU 0 echo [%date% %time%] SUCCESS: Wally trial run completed >> out\\wally_trial_task.log",
        "exit /b %RUN_EXIT%",
    ]
    batch_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def enable_schedule(task_name: str, batch_path: Path) -> None:
    batch_text = _sanitize_task_path(batch_path)
    scheduler_user, scheduler_password = _resolve_scheduler_credentials()
    cmd = [
        "schtasks",
        "/Create",
        "/F",
        "/SC",
        "WEEKLY",
        "/D",
        TRIAL_SCHEDULE_WEEKDAYS,
        "/ST",
        "08:00",
        "/TN",
        task_name,
        "/TR",
        build_task_action(batch_text),
        "/RU",
        scheduler_user,
        "/RP",
        scheduler_password,
    ]
    subprocess.run(cmd, check=True)


def _sanitize_task_path(path: Path) -> str:
    batch_text = str(path).strip()
    while batch_text.endswith('"') or batch_text.endswith("'"):
        batch_text = batch_text[:-1]
    return batch_text.strip()


def _relative_batch_path(project_root: Path, path_text: str) -> str:
    path = Path(path_text)
    try:
        root = project_root.resolve()
        if path.is_absolute():
            rel = path.resolve().relative_to(root)
        else:
            rel = path
        rel_text = str(rel)
    except Exception:
        rel_text = path.name
    rel_text = rel_text.replace("/", "\\").lstrip("\\/")
    return rel_text


def build_task_action(batch_text: str) -> str:
    return f'cmd /c ""{batch_text}""'


def _strip_quotes(value: str) -> str:
    text = (value or "").strip()
    if text.startswith('"') and text.endswith('"') and len(text) >= 2:
        text = text[1:-1]
    return text.strip()


def _normalize_command(command: str) -> str:
    cleaned = _strip_quotes(command)
    base = os.path.basename(cleaned).lower()
    if base in ("cmd.exe", "cmd"):
        return "cmd"
    return cleaned


def format_task_to_run(command: str, arguments: str | None) -> str:
    cmd = _normalize_command(command)
    args = (arguments or "").strip()
    if args:
        return f"{cmd} {args}"
    return cmd


def extract_exec_action(xml_text: str) -> str | None:
    try:
        root = ET.fromstring(xml_text.strip())
    except Exception:
        return None
    namespace = ""
    if root.tag.startswith("{"):
        namespace = root.tag.split("}", 1)[0] + "}"
    exec_node = root.find(f".//{namespace}Exec")
    if exec_node is None:
        return None
    command = exec_node.findtext(f"{namespace}Command", default="").strip()
    if not command:
        return None
    arguments = exec_node.findtext(f"{namespace}Arguments", default="").strip()
    return format_task_to_run(command, arguments)


def query_task_to_run(task_name: str) -> str | None:
    cmd = ["schtasks", "/Query", "/TN", task_name, "/XML"]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        return None
    return extract_exec_action(result.stdout)


def _parse_schtasks_list_fields(text: str) -> dict[str, str]:
    fields: dict[str, str] = {}
    for line in (text or "").splitlines():
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        key = key.strip()
        if not key:
            continue
        fields[key] = value.strip()
    return fields


def query_task_logon_mode(task_name: str) -> str | None:
    cmd = ["schtasks", "/Query", "/TN", task_name, "/V", "/FO", "LIST"]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        return None
    fields = _parse_schtasks_list_fields(result.stdout)
    return (fields.get("Logon Mode") or "").strip() or None


def verify_schedule_action(task_name: str, expected_action: str, expected_logon_mode: str = "Password") -> None:
    actual = query_task_to_run(task_name)
    actual_logon_mode = query_task_logon_mode(task_name)
    verify_schedule_action_from_actual(expected_action, actual, expected_logon_mode, actual_logon_mode)


def is_non_interactive_logon_mode(mode: str | None) -> bool:
    normalized = (mode or "").strip().lower()
    if not normalized:
        return False
    return "interactive only" not in normalized


def verify_schedule_action_from_actual(
    expected_action: str,
    actual: str | None,
    expected_logon_mode: str = "Password",
    actual_logon_mode: str | None = None,
) -> None:
    hint = "run --enable-schedule"
    if not actual:
        print(f"SCHEDULE_CHECK_FAILED expected={expected_action} actual=MISSING_TASK_TO_RUN hint={hint}")
        raise SystemExit(1)
    if actual != expected_action:
        print(f"SCHEDULE_CHECK_FAILED expected={expected_action} actual={actual} hint={hint}")
        raise SystemExit(1)
    if not actual_logon_mode:
        print(f"SCHEDULE_CHECK_FAILED expected_logon_mode={expected_logon_mode} actual_logon_mode=MISSING_LOGON_MODE hint={hint}")
        raise SystemExit(1)
    if not is_non_interactive_logon_mode(actual_logon_mode):
        print(f"SCHEDULE_CHECK_FAILED expected_logon_mode={expected_logon_mode} actual_logon_mode={actual_logon_mode} hint={hint}")
        raise SystemExit(1)
    print(f"SCHEDULE_OK /TR={actual} /LOGON_MODE={actual_logon_mode}")


def _resolve_scheduler_credentials() -> tuple[str, str]:
    scheduler_user = (os.getenv("TASK_SCHED_USER") or "").strip()
    if not scheduler_user:
        username = (os.getenv("USERNAME") or "").strip()
        userdomain = (os.getenv("USERDOMAIN") or "").strip()
        if username and userdomain:
            scheduler_user = f"{userdomain}\\{username}"
        else:
            scheduler_user = username
    if not scheduler_user:
        print("CONFIG_ERROR missing variables: TASK_SCHED_USER")
        raise SystemExit(1)
    scheduler_password = (os.getenv("TASK_SCHED_PASSWORD") or "").strip()
    if not scheduler_password:
        print("CONFIG_ERROR missing variables: TASK_SCHED_PASSWORD")
        raise SystemExit(1)
    return scheduler_user, scheduler_password


def run_project_context_soft_check(repo_root: Path) -> None:
    script_path = repo_root / "tools" / "project_context_pack.py"
    if not script_path.exists():
        print("WARN_CONTEXT_PACK_SCRIPT_MISSING tools/project_context_pack.py")
        return

    commands = [
        ["py", "-3", str(script_path)] + PROJECT_CONTEXT_SOFT_CHECK_CMD,
        [sys.executable, str(script_path)] + PROJECT_CONTEXT_SOFT_CHECK_CMD,
    ]
    for idx, cmd in enumerate(commands):
        try:
            proc = subprocess.run(cmd, capture_output=True, text=True, cwd=str(repo_root))
        except FileNotFoundError:
            if idx == 0:
                continue
            print("WARN_CONTEXT_PACK_CHECK_FAILED runner_not_found")
            return
        except Exception as e:
            print(f"WARN_CONTEXT_PACK_CHECK_FAILED error={type(e).__name__}")
            return

        stdout = (proc.stdout or "").strip()
        stderr = (proc.stderr or "").strip()
        if stdout:
            for line in stdout.splitlines():
                print(line)
        if stderr:
            for line in stderr.splitlines():
                print(line)
        if proc.returncode != 0:
            print(f"WARN_CONTEXT_PACK_CHECK_FAILED returncode={proc.returncode}")
        return


def run_doctor(customer_path: Path, repo_root: Path, task_name: str, check_scheduler: bool) -> int:
    # Non-sending health check: validate config/env (including SMTP vars) and, if available,
    # verify the Task Scheduler action matches the repo's expected batch runner.
    run_project_context_soft_check(repo_root)

    ok, msg = preflight(customer_path, require_smtp=True)
    if not ok:
        print(f"DOCTOR_FAIL preflight={msg}")
        return 1

    # Scheduler verification runs only on the operator PC (Task Scheduler is local-machine state).
    # Default: skip so --doctor never calls schtasks unless explicitly opted in.
    if not check_scheduler:
        print("DOCTOR_NOTE scheduler_check=SKIPPED (opt-in)")
    else:
        # Task Scheduler verification (best-effort): do not attempt to create/modify tasks.
        if "query_task_to_run" in globals() and "build_task_action" in globals():
            try:
                batch_path = (repo_root / "run_wally_trial_daily.bat").resolve()
                expected_action = build_task_action(_sanitize_task_path(batch_path))
                actual = query_task_to_run(task_name)
                if not actual:
                    print("DOCTOR_NOTE scheduler_check=SKIPPED (task missing or schtasks unavailable)")
                elif actual != expected_action:
                    print(f"DOCTOR_FAIL scheduler_check=BAD expected={expected_action} actual={actual}")
                    return 1
                else:
                    print(f"DOCTOR_NOTE scheduler_check=OK /TR={actual}")
            except Exception as e:
                print(f"DOCTOR_NOTE scheduler_check=SKIPPED error={type(e).__name__}")
        else:
            print("DOCTOR_NOTE scheduler_check=SKIPPED (not implemented)")

    print("DOCTOR_OK")
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Wally trial workflow")
    parser.add_argument("customer_path", nargs="?", default="", help="Customer config path or name (optional)")
    parser.add_argument("--db", default="data/osha.sqlite")
    parser.add_argument("--customer", default="customers/wally_trial_tx_triangle_v1.json")
    parser.add_argument(
        "--status",
        action="store_true",
        help="Print deterministic trial status block for canonical Wally subscriber and exit.",
    )
    parser.add_argument(
        "--as-of",
        default="",
        help="Optional as-of date YYYY-MM-DD for --status (default: America/New_York today).",
    )
    parser.add_argument(
        "--audit",
        action="store_true",
        help="Run deterministic no-send audit and write audit artifacts under out/trials/<subscriber_key>/.",
    )
    parser.add_argument("--out-dir", default="out")
    parser.add_argument("--territory-code", default="TX_TRI")
    parser.add_argument("--content-filter", default="high_medium")
    parser.add_argument("--lookback-days", type=int, default=14)
    parser.add_argument(
        "--check-inspection",
        default="",
        help="Optional inspection id/activity id to explain in audit output (e.g., 1874533.015).",
    )
    parser.add_argument(
        "--chase-email",
        default=(os.getenv("OSHA_SMOKE_TO") or os.getenv("CHASE_EMAIL") or "cchevali+oshasmoke@gmail.com"),
    )
    parser.add_argument("--admin-email", default="support@microflowops.com")
    parser.add_argument("--send-live", action="store_true", help="Trigger first live send to Wally")
    parser.add_argument(
        "--allow-weekend-send",
        action="store_true",
        help="Emergency/manual override: allow trial live send on Sat/Sun.",
    )
    parser.add_argument(
        "--test-send",
        action="store_true",
        help="Laptop-safe: force a Starter Snapshot send to cchevali+oshasmoke@gmail.com without mutating send state",
    )
    parser.add_argument(
        "--test-send-daily",
        action="store_true",
        help="Laptop-safe: send the daily 'new since last send' variant to cchevali+oshasmoke@gmail.com without mutating send state",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="When used with --test-send-daily, render only (no send).",
    )
    parser.add_argument("--enable-schedule", action="store_true", help="Create 08:00 local weekday scheduled task")
    parser.add_argument("--check-schedule", action="store_true", help="Verify scheduled task action and logon mode")
    parser.add_argument("--task-name", default="OSHA Wally Trial Daily")
    parser.add_argument("--preflight-only", action="store_true", help="Check config/env and exit")
    parser.add_argument(
        "--print-config",
        action="store_true",
        help="Print resolved trial scheduling config (non-secret) and exit",
    )
    parser.add_argument(
        "--doctor",
        action="store_true",
        help="Non-sending health check (same validations as --preflight-only; scheduler check is opt-in)",
    )
    parser.add_argument(
        "--doctor-check-scheduler",
        action="store_true",
        help="When used with --doctor: verify Task Scheduler /TR action via schtasks (operator PC only)",
    )

    args = parser.parse_args()

    if args.status:
        print(f"TRIAL_WEEKDAYS_ONLY={1 if TRIAL_WEEKDAYS_ONLY else 0}")
        print(f"TRIAL_SCHEDULE_WEEKDAYS={TRIAL_SCHEDULE_WEEKDAYS}")
        raise SystemExit(
            run_trial_admin.print_trial_status(
                subscriber_key=WALLY_TRIAL_SUBSCRIBER_KEY,
                as_of=str(args.as_of or ""),
            )
        )

    repo_root = Path(__file__).resolve().parent
    load_environment(repo_root)

    customer_arg = args.customer_path if args.customer_path else args.customer
    customer_path = resolve_customer_path(customer_arg, repo_root)

    if args.audit:
        raise SystemExit(
            run_wally_audit(
                db_path=args.db,
                customer_path=customer_path,
                as_of=str(args.as_of or ""),
                check_inspection=str(args.check_inspection or ""),
            )
        )

    if args.print_config:
        print_trial_config(customer_path, allow_weekend_send=bool(args.allow_weekend_send))
        raise SystemExit(0)

    if args.test_send_daily:
        # Allow a single "night-before" command: scheduler verification (PC-only) + non-mutating daily test send.
        if args.doctor_check_scheduler:
            code = run_doctor(
                customer_path=customer_path,
                repo_root=repo_root,
                task_name=args.task_name,
                check_scheduler=True,
            )
            if code != 0:
                raise SystemExit(code)
        run_test_send_daily(db_path=args.db, customer_config=str(customer_path), dry_run=bool(args.dry_run))
        raise SystemExit(0)

    if args.test_send:
        run_test_send(db_path=args.db, customer_config=str(customer_path))
        raise SystemExit(0)

    if args.doctor:
        raise SystemExit(
            run_doctor(
                customer_path=customer_path,
                repo_root=repo_root,
                task_name=args.task_name,
                check_scheduler=bool(args.doctor_check_scheduler),
            )
        )

    if args.preflight_only:
        ok, msg = preflight(customer_path, require_smtp=True)
        print(msg)
        raise SystemExit(0 if ok else 1)

    batch_path = repo_root / "run_wally_trial_daily.bat"
    batch_path_resolved = batch_path.resolve()
    expected_action = build_task_action(_sanitize_task_path(batch_path_resolved))

    if args.check_schedule:
        verify_schedule_action(args.task_name, expected_action)
        raise SystemExit(0)

    Path(args.out_dir).mkdir(parents=True, exist_ok=True)

    counts_path = estimate_daily_counts(
        db_path=args.db,
        out_dir=args.out_dir,
        territory_code=args.territory_code,
        content_filter=args.content_filter,
        lookback_days=args.lookback_days,
    )
    print(f"Daily-count estimate written: {counts_path}")

    run_preview_send(args.db, str(customer_path), args.chase_email)
    print(f"Preview dry-run sent to Chase override: {args.chase_email}")

    if args.send_live:
        run_live_send(
            args.db,
            str(customer_path),
            args.admin_email,
            True,
            allow_weekend_send=bool(args.allow_weekend_send),
        )
        print("First live send triggered via deliver_daily.py")

    write_batch_runner(
        batch_path=batch_path,
        project_root=repo_root,
        customer_config=str(customer_path),
        db_path=args.db,
        admin_email=args.admin_email,
    )
    print(f"Batch runner written: {batch_path.name}")

    if args.enable_schedule:
        enable_schedule(args.task_name, batch_path_resolved)
        verify_schedule_action(args.task_name, expected_action)
        print(f"SCHEDULE_WEEKDAYS={TRIAL_SCHEDULE_WEEKDAYS}")
        print(f"Scheduled task enabled: {args.task_name} at 08:00 local weekdays (set host timezone to America/Chicago)")


if __name__ == "__main__":
    main()
