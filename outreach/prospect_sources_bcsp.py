import json
import os
import re
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import requests_warning_compat
import requests
from bs4 import BeautifulSoup


BASE_URL = "https://directory.bcsp.org/"
SEARCH_URL = "https://directory.bcsp.org/search_results.php"
USER_AGENT = "OSHA_Leads/1.0 (+https://microflowops.com)"
CACHE_MAX_AGE_DAYS = 7
PAGE_SIZE = 20

US_COUNTRY_TOKENS = {"US", "USA", "UNITED STATES", "UNITED STATES OF AMERICA"}
NON_ACTIVE_MARKERS = {"INACTIVE", "EXPIRED", "SUSPENDED"}


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


def _cache_age_days(payload: dict, as_of: datetime | None = None) -> int | None:
    ts = _parse_iso(str(payload.get("fetched_at_utc") or ""))
    if ts is None:
        return None
    now = as_of or datetime.now(timezone.utc)
    days = (now - ts).total_seconds() / 86400.0
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
    path = diagnostics_dir / f"bcsp_{state.lower()}_{ts}.json"
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return path


def _parse_csv_env(name: str, default_items: list[str]) -> list[str]:
    raw = str(os.getenv(name, "") or "").strip()
    if not raw:
        return list(default_items)
    out: list[str] = []
    for token in raw.split(","):
        item = _normalize_text(token)
        if item and item not in out:
            out.append(item)
    return out


def _split_name(full_name: str) -> tuple[str, str]:
    parts = [p for p in _normalize_text(full_name).split(" ") if p]
    if not parts:
        return "", ""
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], parts[-1]


def _parse_location(location_text: str, fallback_state: str) -> tuple[str, str, str]:
    text = _normalize_text(location_text)
    if not text:
        return "", _normalize_text(fallback_state).upper(), ""
    parts = [p.strip() for p in re.split(r"\s*[|,]\s*", text) if p.strip()]
    city = ""
    state = _normalize_text(fallback_state).upper()
    country = ""
    if len(parts) >= 1:
        city = _normalize_text(parts[0])
    if len(parts) >= 2 and re.fullmatch(r"[A-Za-z]{2}", parts[1] or ""):
        state = _normalize_text(parts[1]).upper()
        if len(parts) >= 3:
            country = _normalize_text(parts[2]).upper()
    else:
        m = re.search(r"^(.*?),\s*([A-Z]{2})(?:\s+(.*))?$", text)
        if m:
            city = _normalize_text(m.group(1))
            state = _normalize_text(m.group(2)).upper() or state
            country = _normalize_text(m.group(3)).upper()
    return city, state, country


def _country_is_us(country_text: str, location_text: str, state_text: str) -> bool:
    country = _normalize_text(country_text).upper()
    if country:
        return country in US_COUNTRY_TOKENS
    state = _normalize_text(state_text).upper()
    if re.fullmatch(r"[A-Z]{2}", state):
        return True
    text = _normalize_text(location_text).upper()
    if "UNITED STATES" in text or re.search(r"\bUSA\b", text):
        return True
    return False


def _is_active_listing(node: Any) -> bool:
    status_node = None
    try:
        status_node = node.select_one(".listing_status, .listing-status, .status")
    except Exception:
        status_node = None
    status_text = _normalize_text(status_node.get_text(" ", strip=True) if status_node is not None else "")
    blob = _normalize_text(node.get_text(" ", strip=True))
    blob_upper = blob.upper()
    status_upper = status_text.upper()
    if any(marker in status_upper for marker in NON_ACTIVE_MARKERS):
        return False
    if any(marker in blob_upper for marker in NON_ACTIVE_MARKERS):
        return False
    if status_upper:
        return "ACTIVE" in status_upper
    return "ACTIVE" in blob_upper


def _listing_nodes(soup: BeautifulSoup) -> list[Any]:
    selectors = [
        ".bcsp-listing",
        ".search-result",
        ".directory-result",
        ".listing",
        ".result-card",
    ]
    nodes: list[Any] = []
    seen: set[int] = set()
    for selector in selectors:
        for node in soup.select(selector):
            ident = id(node)
            if ident in seen:
                continue
            seen.add(ident)
            nodes.append(node)
    if nodes:
        return nodes
    for name_node in soup.select(".listing_name, .listing-name"):
        parent = name_node.find_parent(["div", "article", "li", "tr"])
        if parent is None:
            continue
        ident = id(parent)
        if ident in seen:
            continue
        seen.add(ident)
        nodes.append(parent)
    return nodes


def parse_bcsp_page(page_html: str, *, state: str, page_ref: str) -> tuple[list[dict[str, str]], str]:
    soup = BeautifulSoup(page_html or "", "html.parser")
    cards = _listing_nodes(soup)
    rows: list[dict[str, str]] = []
    seen_keys: set[str] = set()
    state_filter = _normalize_text(state).upper()
    for card in cards:
        card_text = _normalize_text(card.get_text(" ", strip=True))
        if not card_text:
            continue
        if not _is_active_listing(card):
            continue

        name_node = card.select_one(".listing_name, .listing-name, .name, h2, h3")
        location_node = card.select_one(".listing_location, .listing-location, .location, .locality")
        cred_node = card.select_one(".listing_credential, .listing-credential, .credentials, .credential")
        status_node = card.select_one(".listing_status, .listing-status, .status")

        contact_name = _normalize_text(name_node.get_text(" ", strip=True) if name_node is not None else "")
        location_text = _normalize_text(location_node.get_text(" ", strip=True) if location_node is not None else "")
        city, state_val, country = _parse_location(location_text, fallback_state=state_filter)
        if not _country_is_us(country, location_text, state_val):
            continue
        if state_filter and state_val and state_val != state_filter:
            continue

        credentials = _normalize_text(cred_node.get_text(" ", strip=True) if cred_node is not None else "")
        status_text = _normalize_text(status_node.get_text(" ", strip=True) if status_node is not None else "ACTIVE")
        first_name, last_name = _split_name(contact_name)
        key = "|".join([contact_name.lower(), city.lower(), state_val.upper(), credentials.lower()])
        if key in seen_keys:
            continue
        seen_keys.add(key)

        rows.append(
            {
                "prospect_id": "",
                "firm": "",
                "company_name": "",
                "email": "",
                "contact_email": "",
                "contact_name": contact_name,
                "first_name": first_name,
                "last_name": last_name,
                "title": "BCSP Credential Holder",
                "contact_role": "BCSP Credential Holder",
                "credentials": credentials,
                "city": city,
                "state": state_val,
                "website": "",
                "phone": "",
                "listing_status": status_text or "ACTIVE",
                "listing_location": location_text,
                "source": "BCSP",
                "source_detail": page_ref,
            }
        )
    return rows, ("BCSP_LISTINGS" if rows else "FAILED")


def _default_fetcher(url: str) -> tuple[int, str]:
    resp = requests.get(url, timeout=25, headers={"User-Agent": USER_AGENT})
    return int(resp.status_code), str(resp.text or "")


def _build_page_url(state: str, credential: str, offset: int) -> str:
    params = {
        "directory_search": "",
        "state": str(state or "").strip().upper(),
        "credential": _normalize_text(credential),
        "start_on_page": int(offset),
        "show_per_page": PAGE_SIZE,
    }
    return f"{SEARCH_URL}?{urlencode(params)}"


def probe_bcsp_state_search(
    state: str = "TX",
    credential: str = "CSP",
    timeout_sec: int = 10,
) -> dict[str, Any]:
    state_norm = _normalize_text(state).upper() or "TX"
    credential_norm = _normalize_text(credential) or "CSP"
    url = _build_page_url(state_norm, credential_norm, 0)
    try:
        resp = requests.get(url, timeout=timeout_sec, headers={"User-Agent": USER_AGENT})
    except Exception as exc:
        return {
            "ok": False,
            "status": 0,
            "url": url,
            "reason": "request_failed",
            "error": f"{type(exc).__name__}:{exc}",
        }
    status = int(resp.status_code)
    if status != 200:
        return {
            "ok": False,
            "status": status,
            "url": url,
            "reason": f"http_status_{status}",
        }

    html = str(resp.text or "")
    rows, parse_mode = parse_bcsp_page(
        html,
        state=state_norm,
        page_ref=f"credential={credential_norm}&start_on_page=0",
    )
    raw_listing_count = len(_listing_nodes(BeautifulSoup(html, "html.parser")))
    if rows:
        return {
            "ok": True,
            "status": status,
            "url": url,
            "reason": "state_search_ok",
            "parse_mode": parse_mode,
            "rows_found": len(rows),
            "raw_listing_count": raw_listing_count,
        }
    if raw_listing_count > 0:
        return {
            "ok": False,
            "status": status,
            "url": url,
            "reason": "unfiltered_global_results",
            "parse_mode": parse_mode,
            "rows_found": 0,
            "raw_listing_count": raw_listing_count,
            "error": "state_query_not_respected",
        }
    return {
        "ok": False,
        "status": status,
        "url": url,
        "reason": "parse_failed",
        "parse_mode": parse_mode,
        "rows_found": 0,
        "raw_listing_count": raw_listing_count,
        "error": "no_state_rows_parsed",
    }


def doctor_probe_bcsp(timeout_sec: int = 10) -> dict[str, Any]:
    return probe_bcsp_state_search(timeout_sec=timeout_sec)


def fetch_bcsp_state_rows(
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

    cache_path = _cache_path(cache_dir, state_norm)
    cached_payload = _read_cache(cache_path)
    if cached_payload and _cache_is_fresh(cached_payload):
        return {
            "rows": list(cached_payload.get("rows") or []),
            "cache_used": True,
            "cache_age_days": _cache_age_days(cached_payload),
            "cache_path": cache_path,
            "pages_fetched": int(cached_payload.get("pages_fetched") or 0),
            "parse_mode": str(cached_payload.get("parse_mode") or "FAILED"),
            "diagnostics_path": None,
        }

    credentials = _parse_csv_env("PROSPECT_AUTOGROW_BCSP_CREDENTIALS", ["CSP", "ASP", "CHST", "OHST"])
    pages_fetched = 0
    rows_all: list[dict[str, Any]] = []
    page_modes: list[str] = []
    page_urls: list[str] = []
    fetch = fetcher or _default_fetcher

    try:
        for credential in credentials:
            offset = 0
            while pages_fetched < max_pages:
                if pages_fetched > 0 and sleep_ms > 0:
                    time.sleep(float(sleep_ms) / 1000.0)
                url = _build_page_url(state_norm, credential=credential, offset=offset)
                page_urls.append(url)
                status, html = fetch(url)
                pages_fetched += 1
                if int(status) != 200:
                    page_modes.append("FAILED")
                    break
                parsed_rows, mode = parse_bcsp_page(
                    str(html or ""),
                    state=state_norm,
                    page_ref=f"credential={_normalize_text(credential)}&start_on_page={offset}",
                )
                page_modes.append(mode)
                rows_all.extend(parsed_rows)
                if not parsed_rows:
                    break
                offset += PAGE_SIZE
                if pages_fetched >= max_pages:
                    break

        parse_mode = "FAILED"
        if rows_all:
            parse_mode = page_modes[0] if len(set(page_modes)) == 1 else "MULTI"
        if parse_mode == "FAILED" and pages_fetched > 0:
            parse_mode = page_modes[0] if page_modes else "FAILED"
        if pages_fetched == 0:
            raise RuntimeError("page_fetch_failed")

        payload = {
            "source": "BCSP",
            "state": state_norm,
            "fetched_at_utc": _utc_now_iso(),
            "cache_max_age_days": CACHE_MAX_AGE_DAYS,
            "pages_fetched": pages_fetched,
            "parse_mode": parse_mode,
            "page_urls": page_urls,
            "rows": rows_all,
        }
        if allow_cache_write:
            _write_cache(cache_path, payload)
        return {
            "rows": rows_all,
            "cache_used": False,
            "cache_age_days": _cache_age_days(payload),
            "cache_path": cache_path,
            "pages_fetched": pages_fetched,
            "parse_mode": parse_mode,
            "diagnostics_path": None,
        }
    except Exception as exc:
        diag = _write_diagnostic(
            diagnostics_dir,
            state_norm,
            {
                "source": "BCSP",
                "state": state_norm,
                "generated_at_utc": _utc_now_iso(),
                "error": str(exc),
                "pages_fetched": pages_fetched,
                "page_urls": page_urls,
                "cache_path": str(cache_path),
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
