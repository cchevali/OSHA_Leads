import json
import re
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import requests_warning_compat
import requests
from bs4 import BeautifulSoup

from outreach import scraper_engine


BASE_URL = "https://www.osha.gov"
LISTING_URL = "https://www.osha.gov/news/newsreleases/enforcement"
USER_AGENT = "OSHA_Leads/1.0 (+https://microflowops.com)"
CACHE_MAX_AGE_DAYS = 3


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
    path = diagnostics_dir / f"osha_news_{state.lower()}_{ts}.json"
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return path


def doctor_probe_osha_news(timeout_sec: int = 10) -> dict[str, Any]:
    try:
        resp = requests.get(LISTING_URL, timeout=timeout_sec, headers={"User-Agent": USER_AGENT})
        return {"ok": int(resp.status_code) == 200, "status": int(resp.status_code), "url": LISTING_URL}
    except Exception as exc:
        return {"ok": False, "status": 0, "url": LISTING_URL, "error": f"{type(exc).__name__}:{exc}"}


def parse_osha_news_listing(html: str, *, state: str) -> tuple[list[str], str]:
    soup = BeautifulSoup(html or "", "html.parser")
    urls: list[str] = []
    seen: set[str] = set()
    state_norm = _normalize_text(state).upper()
    for a in soup.select("a[href]"):
        href = _normalize_text(a.get("href") or "")
        text = _normalize_text(a.get_text(" ", strip=True))
        if "/news/newsreleases/" not in href and "/news/newsreleases/" not in text.lower():
            if "news release" not in text.lower():
                continue
        full = urljoin(BASE_URL, href)
        if "/news/newsreleases/" not in full:
            continue
        if state_norm and state_norm not in text.upper() and f"/{state_norm.lower()}/" not in full.lower():
            # Keep national pages as fallback; don't overfilter.
            pass
        if full not in seen:
            seen.add(full)
            urls.append(full)
    return urls, ("LINKS" if urls else "FAILED")


def _extract_penalty(text: str) -> str:
    match = re.search(r"\$[\d,]+(?:\.\d{2})?", text or "")
    return _normalize_text(match.group(0) if match else "")


def _extract_city_state(text: str) -> tuple[str, str]:
    match = re.search(r"\b([A-Za-z .'-]+),\s*([A-Z]{2})\b", text or "")
    if not match:
        return "", ""
    return _normalize_text(match.group(1)), _normalize_text(match.group(2)).upper()


def _extract_company(title: str, body_text: str) -> str:
    text = _normalize_text(title or "")
    m = re.search(r"OSHA cites ([^,.;]+)", text, flags=re.I)
    if m:
        return _normalize_text(m.group(1))
    m = re.search(r"proposes penalties to ([^,.;]+)", text, flags=re.I)
    if m:
        return _normalize_text(m.group(1))
    m = re.search(r"for ([A-Z][A-Za-z0-9& .'-]{3,})", _normalize_text(body_text or ""))
    return _normalize_text(m.group(1) if m else "")


def parse_osha_news_release(html: str, *, url: str) -> tuple[dict[str, str] | None, str]:
    soup = BeautifulSoup(html or "", "html.parser")
    title = _normalize_text((soup.select_one("h1") or soup.select_one("title") or {}).get_text(" ", strip=True) if (soup.select_one("h1") or soup.select_one("title")) else "")
    body = soup.select_one("main") or soup.select_one(".main-content") or soup
    body_text = _normalize_text(body.get_text(" ", strip=True))
    city, state = _extract_city_state(body_text)
    company = _extract_company(title, body_text)
    penalty = _extract_penalty(body_text)
    violation_type = "enforcement_press_release" if body_text else ""
    if not company and not city and not penalty:
        return None, "FAILED"
    row = {
        "prospect_id": "",
        "firm": company,
        "company_name": company,
        "email": "",
        "contact_email": "",
        "contact_name": "",
        "title": "OSHA Enforcement Contact",
        "contact_role": "OSHA Enforcement Contact",
        "city": city,
        "state": state,
        "website": "",
        "domain": "",
        "source": "OSHA_NEWS",
        "source_detail": url,
        "penalty_amount": penalty,
        "violation_type": violation_type,
    }
    return row, "ARTICLE"


def _default_fetcher(url: str) -> tuple[int, str]:
    resp = requests.get(url, timeout=25, headers={"User-Agent": USER_AGENT})
    return int(resp.status_code), str(resp.text or "")


def fetch_osha_news_state_rows(
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
        availability = scraper_engine.probe_source_availability("OSHA_NEWS")
        if not availability.get("available"):
            return {
                "rows": [],
                "cache_used": False,
                "cache_age_days": _cache_age_days(cached_payload or {}),
                "cache_path": cache_path,
                "pages_fetched": 0,
                "parse_mode": "FAILED",
                "diagnostics_path": None,
                "error": f"osha_news_unavailable err={availability.get('reason')}",
                "warn_token": str(availability.get("warn_token") or ""),
            }

    pages_fetched = 0
    article_urls: list[str] = []
    rows_all: list[dict[str, Any]] = []
    page_modes: list[str] = []
    try:
        if fetcher is not None:
            status, html = fetcher(LISTING_URL)
            listing_page = {"ok": int(status) == 200, "status": int(status), "html": str(html or ""), "url": LISTING_URL}
        else:
            listing_page = scraper_engine.crawl_page(LISTING_URL, mode="light", sleep_ms=0)
        pages_fetched += 1
        if not listing_page.get("ok"):
            raise RuntimeError(f"listing_status={listing_page.get('status')}")
        article_urls, list_mode = parse_osha_news_listing(str(listing_page.get("html") or ""), state=state_norm)
        page_modes.append(list_mode)
        article_urls = article_urls[:max_pages]

        for idx, url in enumerate(article_urls):
            if fetcher is not None:
                status, html = fetcher(url)
                page = {"ok": int(status) == 200, "status": int(status), "html": str(html or ""), "url": url}
            else:
                page = scraper_engine.crawl_page(url, mode="light", sleep_ms=(sleep_ms if idx > 0 else 0))
            pages_fetched += 1
            if not page.get("ok"):
                page_modes.append("FAILED")
                continue
            row, mode = parse_osha_news_release(str(page.get("html") or ""), url=url)
            page_modes.append(mode)
            if row is not None:
                row_state = _normalize_text(row.get("state") or "").upper()
                if row_state and row_state != state_norm:
                    continue
                rows_all.append(row)

        rows_all = scraper_engine.apply_email_resolution_waterfall(rows_all, sleep_ms=0)
        parse_mode = "FAILED"
        if rows_all:
            parse_mode = "ARTICLE"
        elif "LINKS" in page_modes:
            parse_mode = "NO_MATCHES"

        payload = {
            "source": "OSHA_NEWS",
            "state": state_norm,
            "fetched_at_utc": _utc_now_iso(),
            "cache_max_age_days": CACHE_MAX_AGE_DAYS,
            "pages_fetched": pages_fetched,
            "parse_mode": parse_mode,
            "article_urls": article_urls,
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
                "source": "OSHA_NEWS",
                "state": state_norm,
                "generated_at_utc": _utc_now_iso(),
                "error": str(exc),
                "pages_fetched": pages_fetched,
                "article_urls": article_urls,
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
