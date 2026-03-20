import hashlib
import json
import os
import re
import time
from collections import Counter
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import requests_warning_compat
import requests

from outreach import state_lic_precision


SOCRATA_URL = "https://data.texas.gov/resource/7358-krk7.json"
USER_AGENT = "OSHA_Leads/1.0 (+https://microflowops.com)"
CACHE_MAX_AGE_DAYS = 3
DEFAULT_TDLR_LICENSE_TYPES = [
    "Electrical Contractor",
    "Elevator Contractor",
    "Appliance Installation Contractor",
]
CITY_STATE_ZIP_RE = re.compile(r"^(.+?)\s+([A-Z]{2})\s+\d")
def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _normalize_text(value: Any) -> str:
    return " ".join(str(value or "").replace("\r", " ").replace("\n", " ").split()).strip()


def _parse_iso(value: str) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except Exception:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _cache_path(cache_dir: Path, state: str) -> Path:
    return cache_dir / f"state_{str(state or '').strip().upper()}.json"


def _cache_age_days(payload: dict) -> int | None:
    ts = _parse_iso(str(payload.get("fetched_at_utc") or ""))
    if ts is None:
        return None
    days = (datetime.now(timezone.utc) - ts).total_seconds() / 86400.0
    return 0 if days < 0 else int(days)


def _cache_is_fresh(payload: dict) -> bool:
    age = _cache_age_days(payload)
    return age is not None and age < CACHE_MAX_AGE_DAYS


def _read_cache(path: Path) -> dict | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _write_cache(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _write_diagnostic(diagnostics_dir: Path, state: str, payload: dict) -> Path:
    diagnostics_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = diagnostics_dir / f"state_lic_{state.lower()}_{ts}.json"
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return path


def _parse_license_types_env() -> list[str]:
    raw = str(os.getenv("PROSPECT_AUTOGROW_STATE_LIC_TX_LICENSE_TYPES", "") or "").strip()
    if not raw:
        return list(DEFAULT_TDLR_LICENSE_TYPES)
    out: list[str] = []
    for token in raw.split(","):
        item = _normalize_text(token)
        if item and item not in out:
            out.append(item)
    return out


def resolve_state_lic_license_types() -> list[str]:
    return _parse_license_types_env()


def _build_where_clause(state: str, license_types: list[str]) -> str:
    _ = state
    clauses: list[str] = []
    if license_types:
        escaped = []
        for item in license_types:
            escaped_item = str(item or "").replace("'", "''")
            escaped.append(f"'{escaped_item}'")
        clauses.append(f"license_type IN ({','.join(escaped)})")
    else:
        clauses.append("license_type IS NOT NULL")
    return " AND ".join(clauses)


def _build_query_url(state: str, license_types: list[str], limit: int, offset: int) -> str:
    params = {
        "$select": ",".join(
            [
                "license_type",
                "license_number",
                "business_name",
                "owner_name",
                "business_address_line1",
                "business_city_state_zip",
                "business_county",
                "business_telephone",
                "license_expiration_date_mmddccyy",
                "license_subtype",
            ]
        ),
        "$where": _build_where_clause(state, license_types),
        "$limit": int(limit),
        "$offset": int(offset),
    }
    return f"{SOCRATA_URL}?{urlencode(params)}"


def _prospect_id_from_license(state: str, license_number: str) -> str:
    base = f"state_lic|{str(state or '').strip().upper()}|{_normalize_text(license_number)}".encode("utf-8")
    return f"state_lic_{hashlib.sha1(base).hexdigest()[:16]}"


def _parse_city_state_zip(value: Any, fallback_state: str) -> tuple[str, str]:
    text = _normalize_text(value)
    if not text:
        return "", str(fallback_state or "").strip().upper()
    m = CITY_STATE_ZIP_RE.search(text)
    if not m:
        return text, str(fallback_state or "").strip().upper()
    city = _normalize_text(m.group(1))
    state = _normalize_text(m.group(2)).upper() or str(fallback_state or "").strip().upper()
    return city, state


def evaluate_state_lic_consultant_fit(
    *,
    firm: Any = "",
    owner_name: Any = "",
    license_type: Any = "",
    license_subtype: Any = "",
    city: Any = "",
    source_detail: Any = "",
) -> dict[str, Any]:
    return state_lic_precision.classify_state_lic_row(
        {
            "firm": firm,
            "owner_name": owner_name,
            "license_type": license_type,
            "license_subtype": license_subtype,
            "city": city,
            "source_detail": source_detail,
        },
        mode="consultant_fit",
    )


def annotate_state_lic_row(row: dict[str, Any]) -> dict[str, Any]:
    city = _normalize_text(row.get("city") or "")
    if not city:
        city, _ = _parse_city_state_zip(row.get("business_city_state_zip") or "", str(row.get("state") or ""))
    annotated = dict(row)
    annotated["city"] = annotated.get("city") or city
    annotated.update(state_lic_precision.classify_state_lic_row(annotated, mode="consultant_fit"))
    return annotated


def summarize_state_lic_license_types(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for row in list(rows or []):
        license_type = _normalize_text(row.get("license_type") or row.get("title") or "")
        counts[license_type or "UNKNOWN"] += 1
    return {key: int(counts[key]) for key in sorted(counts.keys())}


def _map_tdlr_row(raw: dict[str, Any], state: str) -> dict[str, str]:
    license_number = _normalize_text(raw.get("license_number") or "")
    business_name = _normalize_text(raw.get("business_name") or "")
    owner_name = _normalize_text(raw.get("owner_name") or "")
    city, state_val = _parse_city_state_zip(raw.get("business_city_state_zip") or "", fallback_state=state)
    license_type = _normalize_text(raw.get("license_type") or "")
    license_exp = _normalize_text(raw.get("license_expiration_date_mmddccyy") or "")
    license_subtype = _normalize_text(raw.get("license_subtype") or "")
    business_county = _normalize_text(raw.get("business_county") or "")
    business_phone = _normalize_text(raw.get("business_telephone") or "")
    business_address_line1 = _normalize_text(raw.get("business_address_line1") or "")
    row: dict[str, Any] = {
        "prospect_id": _prospect_id_from_license(state_val, license_number),
        "firm": business_name,
        "company_name": business_name,
        "email": "",
        "contact_email": "",
        "contact_name": owner_name,
        "title": license_type or "Licensed Contractor",
        "contact_role": license_type or "Licensed Contractor",
        "city": city,
        "state": state_val,
        "website": "",
        "domain": "",
        "source": "STATE_LIC",
        "source_detail": f"tdlr:{license_number}",
        "license_type": license_type,
        "license_number": license_number,
        "license_subtype": license_subtype,
        "business_county": business_county,
        "business_telephone": business_phone,
        "business_address_line1": business_address_line1,
        "license_expiration_date_mmddccyy": license_exp,
        "email_status": "pending",
    }
    return annotate_state_lic_row(row)


def _default_fetcher(url: str) -> tuple[int, Any]:
    resp = requests.get(url, timeout=25, headers={"User-Agent": USER_AGENT})
    try:
        payload = resp.json()
    except Exception:
        payload = None
    return int(resp.status_code), payload


def doctor_probe_state_lic(timeout_sec: int = 10) -> dict[str, Any]:
    try:
        tiny = f"{SOCRATA_URL}?$limit=1"
        resp = requests.get(tiny, timeout=timeout_sec, headers={"User-Agent": USER_AGENT})
        return {"ok": int(resp.status_code) == 200, "status": int(resp.status_code), "url": tiny}
    except Exception as exc:
        return {"ok": False, "status": 0, "url": SOCRATA_URL, "error": f"{type(exc).__name__}:{exc}"}


def fetch_state_lic_state_rows(
    state: str,
    run_date: date,
    max_pages: int,
    sleep_ms: int,
    cache_dir: Path,
    diagnostics_dir: Path,
    fetcher=None,
    allow_cache_write: bool = True,
) -> dict:
    _ = run_date
    state_norm = str(state or "").strip().upper()
    if len(state_norm) != 2:
        raise ValueError("invalid_state")
    if max_pages < 1:
        raise ValueError("invalid_max_pages")
    if sleep_ms < 0:
        raise ValueError("invalid_sleep_ms")
    if state_norm != "TX":
        return {
            "rows": [],
            "cache_used": False,
            "cache_age_days": None,
            "cache_path": _cache_path(cache_dir, state_norm),
            "pages_fetched": 0,
            "parse_mode": "UNSUPPORTED_STATE",
            "diagnostics_path": None,
            "error": f"unsupported_state={state_norm}",
        }

    license_types = _parse_license_types_env() if state_norm == "TX" else []
    cache_path = _cache_path(cache_dir, state_norm)
    cached_payload = _read_cache(cache_path)
    if cached_payload and _cache_is_fresh(cached_payload):
        cached_rows = [annotate_state_lic_row(row) for row in list(cached_payload.get("rows") or []) if isinstance(row, dict)]
        return {
            "rows": cached_rows,
            "cache_used": True,
            "cache_age_days": _cache_age_days(cached_payload),
            "cache_path": cache_path,
            "pages_fetched": int(cached_payload.get("pages_fetched") or 0),
            "parse_mode": str(cached_payload.get("parse_mode") or "FAILED"),
            "diagnostics_path": None,
            "effective_license_types": list(cached_payload.get("effective_license_types") or license_types),
            "license_type_breakdown": summarize_state_lic_license_types(cached_rows),
        }
    page_limit = 1000
    pages_fetched = 0
    urls: list[str] = []
    rows_all: list[dict[str, str]] = []
    fetch = fetcher or _default_fetcher

    try:
        for page_idx in range(max_pages):
            if page_idx > 0 and sleep_ms > 0:
                time.sleep(float(sleep_ms) / 1000.0)
            offset = page_idx * page_limit
            url = _build_query_url(state_norm, license_types=license_types, limit=page_limit, offset=offset)
            urls.append(url)
            status, payload = fetch(url)
            pages_fetched += 1
            if int(status) != 200:
                raise RuntimeError(f"socrata_status={status}")
            if not isinstance(payload, list):
                raise RuntimeError("socrata_payload_not_list")
            if not payload:
                break
            for item in payload:
                if not isinstance(item, dict):
                    continue
                rows_all.append(_map_tdlr_row(item, state=state_norm))
            if len(payload) < page_limit:
                break

        parse_mode = "SOCRATA" if rows_all or pages_fetched > 0 else "FAILED"
        payload_cache = {
            "source": "STATE_LIC",
            "state": state_norm,
            "provider": "TDLR_SOCRATA",
            "fetched_at_utc": _utc_now_iso(),
            "cache_max_age_days": CACHE_MAX_AGE_DAYS,
            "pages_fetched": pages_fetched,
            "parse_mode": parse_mode,
            "effective_license_types": list(license_types),
            "license_type_breakdown": summarize_state_lic_license_types(rows_all),
            "urls": urls,
            "rows": rows_all,
        }
        if allow_cache_write:
            _write_cache(cache_path, payload_cache)
        return {
            "rows": rows_all,
            "cache_used": False,
            "cache_age_days": _cache_age_days(payload_cache),
            "cache_path": cache_path,
            "pages_fetched": pages_fetched,
            "parse_mode": parse_mode,
            "diagnostics_path": None,
            "effective_license_types": list(license_types),
            "license_type_breakdown": summarize_state_lic_license_types(rows_all),
        }
    except Exception as exc:
        diag = _write_diagnostic(
            diagnostics_dir,
            state_norm,
            {
                "source": "STATE_LIC",
                "provider": "TDLR_SOCRATA",
                "state": state_norm,
                "generated_at_utc": _utc_now_iso(),
                "error": str(exc),
                "pages_fetched": pages_fetched,
                "urls": urls,
                "cache_path": str(cache_path),
                "todo_fallback": "bulk_csv_download_not_implemented_phase1",
            },
        )
        return {
            "rows": [],
            "cache_used": False,
            "cache_age_days": _cache_age_days(cached_payload or {}),
            "cache_path": cache_path,
            "pages_fetched": pages_fetched,
            "parse_mode": "FAILED",
            "diagnostics_path": diag,
            "error": str(exc),
        }
