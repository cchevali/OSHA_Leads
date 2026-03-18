import json
import re
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlencode, urljoin

import requests
from bs4 import BeautifulSoup

from outreach import contact_normalization
import seed_recipients_pools as pools


BASE_URL = "https://www.thebluebook.com/"
SEARCH_URL = "https://www.thebluebook.com/search.html"
CATEGORY_SEARCH_TERM = "Safety Consultants--Training/Inspections"
USER_AGENT = "OSHA_Leads/1.0 (+https://microflowops.com)"
CACHE_MAX_AGE_DAYS = 7
EMAIL_RE = re.compile(r"[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}", flags=re.I)
CITY_STATE_ZIP_RE = re.compile(r"([A-Za-z .'-]+),\s*([A-Z]{2})\s+\d{5}(?:-\d{4})?$")
CONSULTANCY_RELEVANCE_TOKENS = (
    "consult",
    "safety",
    "compliance",
    "industrial hygiene",
    "training",
    "inspection",
    "osha",
    "ehs",
    "hse",
)
STATE_REGION_IDS: dict[str, tuple[int, ...]] = {
    "TX": (12, 13, 18),
    "CA": (3, 7, 9, 33, 36),
    "FL": (4, 16),
}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _normalize_text(value: Any) -> str:
    return " ".join(str(value or "").replace("\r", " ").replace("\n", " ").split()).strip()


def _normalize_email(value: str) -> str:
    return contact_normalization.normalize_email(value)


def _valid_email(value: str) -> bool:
    return contact_normalization.valid_email(value)


def _email_domain(value: str) -> str:
    return contact_normalization.email_domain(value)


def _normalize_website(value: str) -> str:
    return contact_normalization.normalize_website(value)


def _is_nonfree_email(value: str) -> bool:
    email = _normalize_email(value)
    if not _valid_email(email):
        return False
    return _email_domain(email) not in pools.FREE_EMAIL_DOMAINS


def _normalize_state(value: str) -> str:
    return _normalize_text(value).upper()


def _parse_iso(value: str) -> datetime | None:
    text = _normalize_text(value)
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except Exception:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _cache_path(cache_dir: Path, state: str) -> Path:
    return cache_dir / f"state_{_normalize_state(state)}.json"


def _cache_age_days(payload: dict, as_of: datetime | None = None) -> int | None:
    fetched_at = _parse_iso(str(payload.get("fetched_at_utc") or ""))
    if fetched_at is None:
        return None
    now = as_of or datetime.now(timezone.utc)
    days = (now - fetched_at).total_seconds() / 86400.0
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
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = diagnostics_dir / f"bluebook_{_normalize_state(state).lower()}_{stamp}.json"
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return path


def _default_fetcher(url: str) -> tuple[int, str, str]:
    response = requests.get(url, timeout=25, headers={"User-Agent": USER_AGENT})
    return int(response.status_code or 0), str(response.text or ""), str(response.url or url)


def _coerce_fetch_result(result: Any, url: str) -> tuple[int, str, str]:
    if isinstance(result, tuple):
        if len(result) == 3:
            return int(result[0] or 0), str(result[1] or ""), str(result[2] or url)
        if len(result) == 2:
            return int(result[0] or 0), str(result[1] or ""), str(url)
    return 0, "", str(url)


def _build_search_url(*, region_id: int, page: int) -> str:
    query = urlencode(
        {
            "searchTerm": CATEGORY_SEARCH_TERM,
            "region": str(int(region_id)),
            "page": str(max(1, int(page))),
        }
    )
    return f"{SEARCH_URL}?{query}"


def _split_name(full_name: str) -> tuple[str, str]:
    parts = [part for part in _normalize_text(full_name).split(" ") if part]
    if not parts:
        return "", ""
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], parts[-1]


def _parse_city_state(address_text: str, fallback_state: str) -> tuple[str, str]:
    text = _normalize_text(address_text)
    match = CITY_STATE_ZIP_RE.search(text)
    if match:
        return _normalize_text(match.group(1)), _normalize_state(match.group(2))
    parts = [part.strip() for part in text.split(",") if part.strip()]
    if len(parts) >= 2 and re.fullmatch(r"[A-Z]{2}", parts[-1].upper()):
        return _normalize_text(parts[-2]), _normalize_state(parts[-1])
    return "", _normalize_state(fallback_state)


def _card_links(node: Any) -> tuple[str, str]:
    profile_url = ""
    contact_url = ""
    for link in node.select("a[href]"):
        href = _normalize_text(link.get("href") or "")
        if not href:
            continue
        absolute = urljoin(BASE_URL, href)
        lower_href = absolute.lower()
        if "/iproview/" not in lower_href:
            continue
        if "locations-contacts" in lower_href and not contact_url:
            contact_url = absolute
        elif not profile_url:
            profile_url = absolute
    return profile_url, (contact_url or profile_url)


def parse_bluebook_search_page(page_html: str, *, page_url: str) -> tuple[list[dict[str, str]], str]:
    soup = BeautifulSoup(page_html or "", "html.parser")
    cards = soup.select(".single_result_wrapper")
    rows: list[dict[str, str]] = []
    seen_ids: set[str] = set()
    for card in cards:
        card_text = _normalize_text(card.get_text(" ", strip=True))
        if not card_text:
            continue
        profile_id = _normalize_text(card.get("data-proviewid") or "")
        firm_node = card.select_one("h3.cname, h3")
        firm = _normalize_text(firm_node.get_text(" ", strip=True) if firm_node is not None else "")
        if not firm:
            continue
        profile_url, contact_url = _card_links(card)
        if not contact_url:
            continue
        if not profile_id:
            match = re.search(r"/iproview/(\d+)", contact_url, flags=re.I)
            profile_id = match.group(1) if match else ""
        if not profile_id or profile_id in seen_ids:
            continue
        seen_ids.add(profile_id)
        rows.append(
            {
                "firm": firm,
                "profile_id": profile_id,
                "profile_url": profile_url,
                "contact_url": contact_url,
                "page_url": page_url,
                "search_blob": card_text,
            }
        )
    return rows, ("BLUEBOOK_SEARCH_RESULTS" if rows else "FAILED")


def _extract_card_email(card: Any) -> str:
    matches = EMAIL_RE.findall(_normalize_text(card.get_text(" ", strip=True)))
    for match in matches:
        email = _normalize_email(match)
        if _is_nonfree_email(email):
            return email
    return ""


def _extract_card_website(card: Any) -> str:
    link = card.select_one("a.pvLoc-website[href]")
    if link is None:
        for candidate in card.select("a[href]"):
            href = _normalize_website(candidate.get("href") or "")
            if href and "thebluebook.com" not in href.lower():
                return href
        return ""
    return _normalize_website(link.get("href") or "")


def _extract_card_phone(card: Any) -> str:
    phone_link = card.select_one("a.pvLoc-phone")
    if phone_link is not None:
        return _normalize_text(phone_link.get_text(" ", strip=True))
    return ""


def _extract_contact_name_and_title(card: Any) -> tuple[str, str]:
    contact_name = ""
    title = ""
    name_node = card.select_one(".mt-3 b, b")
    if name_node is not None:
        contact_name = _normalize_text(name_node.get_text(" ", strip=True))
        title_text = _normalize_text(name_node.parent.get_text(" ", strip=True) if name_node.parent is not None else "")
        if title_text.startswith(contact_name):
            title = _normalize_text(title_text[len(contact_name) :].strip(" ,-"))
    return contact_name, title


def _is_consultancy_relevant(blob: str) -> bool:
    text = _normalize_text(blob).lower()
    return any(token in text for token in CONSULTANCY_RELEVANCE_TOKENS)


def parse_bluebook_contact_page(
    page_html: str,
    *,
    state: str,
    contact_url: str,
    profile_url: str,
    profile_id: str,
    firm: str,
) -> tuple[list[dict[str, str]], str]:
    soup = BeautifulSoup(page_html or "", "html.parser")
    cards = soup.select(".card")
    rows: list[dict[str, str]] = []
    state_filter = _normalize_state(state)
    for idx, card in enumerate(cards, start=1):
        blob = _normalize_text(card.get_text(" ", strip=True))
        if not blob or not _is_consultancy_relevant(blob):
            continue
        address_node = card.select_one(".col-12.mb-2")
        address = _normalize_text(address_node.get_text(" ", strip=True) if address_node is not None else "")
        city, row_state = _parse_city_state(address, state_filter)
        if row_state and row_state != state_filter:
            continue
        website = _extract_card_website(card)
        email = _extract_card_email(card)
        if not email and not website:
            continue
        phone = _extract_card_phone(card)
        contact_name, title = _extract_contact_name_and_title(card)
        first_name, last_name = _split_name(contact_name)
        record_id = f"bluebook:{profile_id}:{idx}"
        rows.append(
            {
                "prospect_id": "",
                "firm": firm,
                "company_name": firm,
                "email": email,
                "contact_email": email,
                "contact_name": contact_name,
                "first_name": first_name,
                "last_name": last_name,
                "title": title or "Safety Consultant",
                "contact_role": title or "Safety Consultant",
                "city": city,
                "state": row_state or state_filter,
                "website": website,
                "phone": phone,
                "address": address,
                "source": f"bluebook:{profile_id}",
                "source_detail": record_id,
                "source_record_id": record_id,
                "source_url": contact_url,
                "profile_url": profile_url,
            }
        )
    return rows, ("BLUEBOOK_CONTACT_PAGE" if rows else "FAILED")


def doctor_probe_bluebook(fetcher=None) -> dict[str, Any]:
    url = _build_search_url(region_id=STATE_REGION_IDS["TX"][0], page=1)
    fetch = fetcher or _default_fetcher
    try:
        status, html, final_url = _coerce_fetch_result(fetch(url), url)
    except Exception as exc:
        return {"ok": False, "status": 0, "url": url, "error": f"{type(exc).__name__}:{exc}", "rows_found": 0}
    rows, parse_mode = parse_bluebook_search_page(html, page_url=final_url)
    return {
        "ok": status == 200 and bool(rows),
        "status": int(status),
        "url": final_url,
        "rows_found": len(rows),
        "parse_mode": parse_mode,
        "error": "" if status == 200 else f"http_status_{status}",
    }


def _page_schedule(state: str, max_pages: int) -> list[tuple[int, int]]:
    region_ids = STATE_REGION_IDS.get(_normalize_state(state), ())
    schedule: list[tuple[int, int]] = []
    page_num = 1
    while len(schedule) < max(0, int(max_pages)) and region_ids:
        for region_id in region_ids:
            if len(schedule) >= max(0, int(max_pages)):
                break
            schedule.append((int(region_id), int(page_num)))
        page_num += 1
    return schedule


def fetch_bluebook_state_rows(
    *,
    state: str,
    run_date: date,
    max_pages: int,
    sleep_ms: int,
    cache_dir: Path,
    diagnostics_dir: Path,
    fetcher=None,
    allow_cache_write: bool,
) -> dict[str, Any]:
    state_token = _normalize_state(state)
    cache_path = _cache_path(cache_dir, state_token)
    cached = _read_cache(cache_path)
    if isinstance(cached, dict) and _cache_is_fresh(cached):
        rows = [row for row in list(cached.get("rows") or []) if isinstance(row, dict)]
        return {
            "rows": rows,
            "cache_path": cache_path,
            "cache_used": True,
            "cache_age_days": _cache_age_days(cached),
            "pages_fetched": int(cached.get("pages_fetched") or 0),
            "parse_mode": str(cached.get("parse_mode") or "BLUEBOOK_PUBLIC"),
        }

    fetch = fetcher or _default_fetcher
    pages_fetched = 0
    search_cards = 0
    rows: list[dict[str, str]] = []
    seen_profiles: set[str] = set()
    diagnostics_pages: list[dict[str, Any]] = []
    for region_id, page_num in _page_schedule(state_token, max_pages):
        search_url = _build_search_url(region_id=region_id, page=page_num)
        try:
            status, html, final_url = _coerce_fetch_result(fetch(search_url), search_url)
        except Exception as exc:
            diagnostics_pages.append(
                {
                    "url": search_url,
                    "region_id": region_id,
                    "page": page_num,
                    "status": 0,
                    "error": f"{type(exc).__name__}:{exc}",
                }
            )
            continue
        diagnostics_pages.append(
            {
                "url": final_url,
                "region_id": region_id,
                "page": page_num,
                "status": status,
            }
        )
        if status != 200:
            continue
        pages_fetched += 1
        cards, _parse_mode = parse_bluebook_search_page(html, page_url=final_url)
        search_cards += len(cards)
        for card in cards:
            profile_id = _normalize_text(card.get("profile_id") or "")
            if not profile_id or profile_id in seen_profiles:
                continue
            seen_profiles.add(profile_id)
            contact_url = _normalize_text(card.get("contact_url") or "")
            if not contact_url:
                continue
            if sleep_ms > 0:
                time.sleep(float(max(0, int(sleep_ms))) / 1000.0)
            try:
                contact_status, contact_html, contact_final_url = _coerce_fetch_result(fetch(contact_url), contact_url)
            except Exception:
                continue
            if contact_status != 200:
                continue
            parsed_rows, _contact_mode = parse_bluebook_contact_page(
                contact_html,
                state=state_token,
                contact_url=contact_final_url,
                profile_url=_normalize_text(card.get("profile_url") or ""),
                profile_id=profile_id,
                firm=_normalize_text(card.get("firm") or ""),
            )
            rows.extend(parsed_rows)

    payload = {
        "source": "BLUEBOOK",
        "state": state_token,
        "run_date": run_date.isoformat(),
        "fetched_at_utc": _utc_now_iso(),
        "cache_max_age_days": CACHE_MAX_AGE_DAYS,
        "pages_fetched": int(pages_fetched),
        "parse_mode": ("BLUEBOOK_PUBLIC" if rows else "FAILED"),
        "search_cards": int(search_cards),
        "rows": rows,
    }
    if allow_cache_write:
        _write_cache(cache_path, payload)
    diagnostic_payload = {
        "source": "BLUEBOOK",
        "state": state_token,
        "run_date": run_date.isoformat(),
        "pages_fetched": int(pages_fetched),
        "search_cards": int(search_cards),
        "rows_returned": len(rows),
        "pages": diagnostics_pages,
    }
    diagnostic_path = _write_diagnostic(diagnostics_dir, state_token, diagnostic_payload)
    return {
        "rows": rows,
        "cache_path": cache_path,
        "cache_used": False,
        "cache_age_days": 0,
        "pages_fetched": int(pages_fetched),
        "parse_mode": payload["parse_mode"],
        "diagnostics_path": diagnostic_path,
    }
