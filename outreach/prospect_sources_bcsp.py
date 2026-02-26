import json
import os
import re
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import requests
from bs4 import BeautifulSoup

from outreach import scraper_engine


BASE_URL = "https://directory.bcsp.org/"
SEARCH_URL = "https://directory.bcsp.org/search"
USER_AGENT = "OSHA_Leads/1.0 (+https://microflowops.com)"
CACHE_MAX_AGE_DAYS = 7


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


def parse_bcsp_page(page_html: str, *, state: str, page_ref: str) -> tuple[list[dict[str, str]], str]:
    soup = BeautifulSoup(page_html or "", "html.parser")
    cards = soup.select(".bcsp-card, .directory-result, .result-card")
    if not cards:
        cards = soup.select("article, .card")
    rows: list[dict[str, str]] = []
    seen_keys: set[str] = set()
    for idx, card in enumerate(cards):
        card_text = _normalize_text(card.get_text(" ", strip=True))
        if not card_text:
            continue
        state_match = re.search(r"\b([A-Z]{2})\b", card_text)
        state_val = _normalize_text(state_match.group(1) if state_match else state).upper()
        if state and state_val and state_val != str(state or "").strip().upper():
            continue

        name_node = card.select_one(".name, .credential-holder, h2, h3")
        name = _normalize_text(name_node.get_text(" ", strip=True) if name_node is not None else "")
        company_node = card.select_one(".company, .employer, .organization")
        company = _normalize_text(company_node.get_text(" ", strip=True) if company_node is not None else "")
        city_node = card.select_one(".city, .locality")
        city = _normalize_text(city_node.get_text(" ", strip=True) if city_node is not None else "")
        cred_node = card.select_one(".credentials, .certifications")
        credentials = _normalize_text(cred_node.get_text(" ", strip=True) if cred_node is not None else "")
        industry_node = card.select_one(".industry, .specialty")
        industry = _normalize_text(industry_node.get_text(" ", strip=True) if industry_node is not None else "")
        link_node = card.select_one("a[href]")
        website = ""
        if link_node is not None:
            href = _normalize_text(link_node.get("href") or "")
            if href.lower().startswith(("http://", "https://")) and "bcsp" not in href.lower():
                website = href
        contacts = scraper_engine.extract_contacts_regex(str(card))
        email = _normalize_text((contacts.get("emails") or [""])[0]).lower()
        phone = _normalize_text((contacts.get("phones") or [""])[0])

        first_name, last_name = _split_name(name)
        key = "|".join([name.lower(), company.lower(), city.lower(), state_val])
        if key in seen_keys:
            continue
        seen_keys.add(key)

        rows.append(
            {
                "prospect_id": "",
                "firm": company or _normalize_text(f"{first_name} {last_name}").strip(),
                "company_name": company or _normalize_text(f"{first_name} {last_name}").strip(),
                "email": email,
                "contact_email": email,
                "contact_name": name,
                "first_name": first_name,
                "last_name": last_name,
                "title": "BCSP Credential Holder",
                "contact_role": "BCSP Credential Holder",
                "credentials": credentials,
                "industry": industry,
                "city": city,
                "state": state_val,
                "website": website,
                "phone": phone,
                "source": "BCSP",
                "source_detail": page_ref,
            }
        )
    return rows, ("BCSP_CARDS" if rows else "FAILED")


def _default_fetcher(url: str) -> tuple[int, str]:
    resp = requests.get(url, timeout=25, headers={"User-Agent": USER_AGENT})
    return int(resp.status_code), str(resp.text or "")


def _build_page_url(state: str, page_idx: int, credentials: list[str], industry: str) -> str:
    params = {
        "state": str(state or "").strip().upper(),
        "page": int(page_idx) + 1,
    }
    if credentials:
        params["credentials"] = ",".join(credentials)
    if industry:
        params["industry"] = industry
    return f"{SEARCH_URL}?{urlencode(params)}"


def doctor_probe_bcsp(timeout_sec: int = 10) -> dict[str, Any]:
    try:
        resp = requests.get(BASE_URL, timeout=timeout_sec, headers={"User-Agent": USER_AGENT})
        return {"ok": int(resp.status_code) == 200, "status": int(resp.status_code), "url": BASE_URL}
    except Exception as exc:
        return {"ok": False, "status": 0, "url": BASE_URL, "error": f"{type(exc).__name__}:{exc}"}


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

    if fetcher is None:
        availability = scraper_engine.probe_source_availability("BCSP")
        if not availability.get("available"):
            warn_token = str(availability.get("warn_token") or "")
            reason = str(availability.get("reason") or "unavailable")
            return {
                "rows": [],
                "cache_used": False,
                "cache_age_days": _cache_age_days(cached_payload or {}),
                "cache_path": cache_path,
                "pages_fetched": 0,
                "parse_mode": "FAILED",
                "diagnostics_path": None,
                "error": f"bcsp_unavailable err={reason}",
                "warn_token": warn_token,
            }

    credentials = _parse_csv_env("PROSPECT_AUTOGROW_BCSP_CREDENTIALS", ["CSP", "ASP", "CHST", "OHST"])
    industry = _normalize_text(os.getenv("PROSPECT_AUTOGROW_BCSP_INDUSTRY", ""))
    pages_fetched = 0
    rows_all: list[dict[str, Any]] = []
    page_modes: list[str] = []
    page_urls: list[str] = []

    try:
        for page_idx in range(max_pages):
            url = _build_page_url(state_norm, page_idx, credentials=credentials, industry=industry)
            page_urls.append(url)
            if fetcher is not None:
                status, html = fetcher(url)
                page = {"ok": int(status) == 200, "status": int(status), "html": str(html or ""), "url": url}
            else:
                page = scraper_engine.crawl_page(url, mode="browser", sleep_ms=(sleep_ms if page_idx > 0 else 0))
            pages_fetched += 1
            if not page.get("ok"):
                page_modes.append("FAILED")
                continue
            parsed_rows, mode = parse_bcsp_page(str(page.get("html") or ""), state=state_norm, page_ref=f"page={page_idx + 1}")
            page_modes.append(mode)
            rows_all.extend(parsed_rows)
            if not parsed_rows and page_idx > 0:
                break

        rows_all = scraper_engine.apply_email_resolution_waterfall(rows_all, sleep_ms=0)
        parse_mode = "FAILED"
        if rows_all:
            parse_mode = page_modes[0] if len(set(page_modes)) == 1 else "MULTI"
        if parse_mode == "FAILED":
            raise RuntimeError("page_parse_failed")

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

