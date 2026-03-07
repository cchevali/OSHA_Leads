import json
import re
import time
from collections import Counter
from datetime import date, datetime, timezone
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from outreach import scraper_engine


SEARCH_URL_TEMPLATE = "https://www.ohsonline.com/Directory/SearchResults.aspx?state={state}"
BROWSER_CATEGORY_URL = "https://buyersguide.ohsonline.com/category/consulting/consulting-occupational-health-safety/"
USER_AGENT = "OSHA_Leads/1.0 (+https://microflowops.com)"
CACHE_MAX_AGE_DAYS = 7
ALLOWLIST_TERMS = (
    "consult",
    "consulting",
    "safety",
    "ehs",
    "hse",
    "industrial hygiene",
    "occupational",
    "compliance",
    "training",
    "risk",
    "osha",
)
OHS_PARSE_REASON_KEYS = (
    "selector_missing",
    "empty_listing",
    "missing_firm",
    "invalid_city_state",
    "missing_contact_fields",
)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _parse_iso(value: str) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    normalized = text.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(normalized)
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
    if days < 0:
        return 0
    return int(days)


def _cache_is_fresh(payload: dict, as_of: datetime | None = None) -> bool:
    age = _cache_age_days(payload, as_of=as_of)
    if age is None:
        return False
    return age < CACHE_MAX_AGE_DAYS


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


def _default_fetcher(url: str) -> tuple[int, str]:
    resp = requests.get(url, timeout=25, headers={"User-Agent": USER_AGENT})
    return int(resp.status_code), str(resp.text or "")


def _default_browser_fetcher(url: str) -> dict:
    page = scraper_engine.crawl_page(url, mode="browser", headless=True)
    status = int(page.get("status") or (200 if page.get("ok") else 0))
    html = str(page.get("html") or page.get("markdown") or "")
    return {
        "ok": bool(page.get("ok")) and bool(html),
        "status": status,
        "html": html,
        "error": str(page.get("error") or ""),
        "warn_token": str(page.get("warn_token") or ""),
    }


def _normalize_text(value: str) -> str:
    return " ".join(str(value or "").replace("\r", " ").replace("\n", " ").split()).strip()


def _normalize_email(value: str) -> str:
    return _normalize_text(value).lower()


def _valid_email(value: str) -> bool:
    email = _normalize_email(value)
    if "@" not in email:
        return False
    local, _, domain = email.partition("@")
    if not local or not domain or "." not in domain:
        return False
    if domain.startswith(".") or domain.endswith("."):
        return False
    return True


def _normalize_state(value: str) -> str:
    text = _normalize_text(value).upper()
    if len(text) == 2 and text.isalpha():
        return text
    return ""


def _domain_from_website(value: str) -> str:
    text = _normalize_text(value)
    if not text:
        return ""
    try:
        parsed = urlparse(text if "://" in text else f"https://{text}")
    except Exception:
        return ""
    host = _normalize_text(parsed.netloc or parsed.path).lower()
    if host.startswith("www."):
        host = host[4:]
    return host


def _extract_emails(text: str) -> list[str]:
    emails = re.findall(r"[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}", text, flags=re.I)
    out: list[str] = []
    seen: set[str] = set()
    for item in emails:
        email = _normalize_email(item)
        if _valid_email(email) and email not in seen:
            seen.add(email)
            out.append(email)
    return out


def _extract_city_state(text: str) -> tuple[str, str]:
    match = re.search(r"([A-Za-z .'-]+),\s*([A-Z]{2})(?:\s+\d{5}(?:-\d{4})?)?", text)
    if not match:
        return "", ""
    city = _normalize_text(match.group(1))
    state = _normalize_state(match.group(2))
    return city, state


def _allowlisted(text: str) -> bool:
    lowered = _normalize_text(text).lower()
    if not lowered:
        return False
    return any(term in lowered for term in ALLOWLIST_TERMS)


def _row_from_jsonld(item: dict, idx: int) -> list[dict[str, str]]:
    name = _normalize_text(item.get("name") or "")
    email = _normalize_email(item.get("email") or "")
    if not email:
        return []
    address = item.get("address") or {}
    if isinstance(address, list):
        address = address[0] if address else {}
    if not isinstance(address, dict):
        address = {}
    city = _normalize_text(address.get("addressLocality") or "")
    state = _normalize_state(address.get("addressRegion") or "")
    website = _normalize_text(item.get("url") or "")
    desc = _normalize_text(item.get("description") or "")
    category = _normalize_text(item.get("category") or "")
    if not _allowlisted(" ".join([name, desc, category])):
        return []
    return [
        {
            "prospect_id": "",
            "firm": name,
            "company_name": name,
            "email": email,
            "contact_email": email,
            "title": "Safety Consultant",
            "contact_role": "Safety Consultant",
            "contact_name": "",
            "city": city,
            "state": state,
            "website": website,
            "source": f"ohs_buyers_guide:jsonld-{idx}",
        }
    ]


def _candidate_blocks(soup: BeautifulSoup) -> list:
    selectors = [
        "[data-listing-id]",
        ".directory-listing",
        ".listing",
        ".result",
        ".search-result",
        "article",
        ".card",
    ]
    blocks = []
    seen_ids: set[int] = set()
    for selector in selectors:
        for node in soup.select(selector):
            node_id = id(node)
            if node_id in seen_ids:
                continue
            seen_ids.add(node_id)
            blocks.append(node)
    if blocks:
        return blocks
    return [soup]


def parse_ohs_bg_page(page_html: str, page_ref: str) -> tuple[list[dict[str, str]], str, dict[str, int]]:
    soup = BeautifulSoup(page_html or "", "html.parser")
    rows: list[dict[str, str]] = []
    diag_counts = {"allowlist_rejected": 0}
    seen_emails: set[str] = set()

    mode = "FAILED"
    jsonld_count = 0
    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        raw = str(script.string or script.get_text() or "").strip()
        if not raw:
            continue
        try:
            payload = json.loads(raw)
        except Exception:
            continue
        items = payload if isinstance(payload, list) else [payload]
        for idx, item in enumerate(items):
            if not isinstance(item, dict):
                continue
            for row in _row_from_jsonld(item, idx=idx):
                jsonld_count += 1
                email = _normalize_email(row.get("email") or "")
                if email and email not in seen_emails:
                    seen_emails.add(email)
                    rows.append(row)
    if jsonld_count > 0:
        mode = "JSON_LD"

    blocks = _candidate_blocks(soup)
    text_mode_rows = 0
    for idx, block in enumerate(blocks):
        block_text = _normalize_text(block.get_text(" ", strip=True))
        if not block_text:
            continue

        if not _allowlisted(block_text):
            has_mailto = any(
                _normalize_text(a.get("href") or "").lower().startswith("mailto:")
                for a in block.find_all("a")
            )
            if has_mailto or _extract_emails(block_text):
                diag_counts["allowlist_rejected"] += 1
            continue

        mailto_links = []
        for a in block.find_all("a"):
            href = _normalize_text(a.get("href") or "")
            if href.lower().startswith("mailto:"):
                candidate = href.split(":", 1)[1].split("?", 1)[0].strip().lower()
                if candidate:
                    mailto_links.append(candidate)
        emails = mailto_links or _extract_emails(block_text)
        if not emails:
            continue

        website = ""
        for a in block.find_all("a"):
            href = _normalize_text(a.get("href") or "")
            if href.lower().startswith("mailto:"):
                continue
            if href.lower().startswith(("http://", "https://")):
                website = href
                break

        firm = ""
        for selector in ("h1", "h2", "h3", "h4", ".title", ".listing-title", "strong"):
            node = block.select_one(selector)
            if node:
                firm = _normalize_text(node.get_text(" ", strip=True))
                if firm:
                    break
        if not firm:
            firm = _normalize_text(block.get("data-company") or "") or _normalize_text(block.get("data-title") or "")

        city, state = _extract_city_state(block_text)
        source_tag = _normalize_text(block.get("data-listing-id") or "") or f"{page_ref}-{idx + 1}"
        for email in emails:
            if email in seen_emails:
                continue
            seen_emails.add(email)
            rows.append(
                {
                    "prospect_id": "",
                    "firm": firm,
                    "company_name": firm,
                    "email": email,
                    "contact_email": email,
                    "title": "Safety Consultant",
                    "contact_role": "Safety Consultant",
                    "contact_name": "",
                    "city": city,
                    "state": state,
                    "website": website,
                    "source": f"ohs_buyers_guide:{source_tag}",
                }
            )
            text_mode_rows += 1

    if text_mode_rows > 0 and mode == "FAILED":
        mode = "TEXT"
    elif text_mode_rows > 0 and mode != "FAILED":
        mode = "MIXED"

    if not rows:
        return [], "FAILED", diag_counts
    return rows, mode, diag_counts


def _next_page_url(page_html: str, current_url: str) -> str:
    soup = BeautifulSoup(page_html or "", "html.parser")
    for a in soup.find_all("a"):
        text = _normalize_text(a.get_text(" ", strip=True)).lower()
        href = _normalize_text(a.get("href") or "")
        if not href:
            continue
        rel = " ".join([_normalize_text(x) for x in (a.get("rel") or [])]).lower()
        if "next" in rel or text in {"next", "next >", "next >>"}:
            return urljoin(current_url, href)
    return ""


def _write_diagnostic(diagnostics_dir: Path, state: str, payload: dict) -> Path:
    diagnostics_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = diagnostics_dir / f"ohs_bg_{state.lower()}_{ts}.json"
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return path


def _empty_parse_counters() -> dict[str, int]:
    return {
        "fetched_pages": 0,
        "candidate_rows_seen": 0,
        "parsed_rows_accepted": 0,
        "parsed_rows_rejected": 0,
        "hard_parse_failures": 0,
    }


def _empty_parse_reasons() -> dict[str, int]:
    return {key: 0 for key in OHS_PARSE_REASON_KEYS}


def _normalize_website(value: str) -> str:
    text = _normalize_text(value)
    if not text:
        return ""
    if text.lower().startswith(("http://", "https://")):
        return text
    if text.startswith("//"):
        return f"https:{text}"
    return f"https://{text.lstrip('/')}"


def _extract_jsonld_objects(soup: BeautifulSoup) -> list[dict]:
    out: list[dict] = []
    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        raw = str(script.string or script.get_text() or "").strip()
        if not raw:
            continue
        try:
            payload = json.loads(raw)
        except Exception:
            continue
        items = payload if isinstance(payload, list) else [payload]
        for item in items:
            if isinstance(item, dict):
                out.append(item)
    return out


def _extract_company_links(category_html: str, page_url: str) -> tuple[list[str], str]:
    soup = BeautifulSoup(category_html or "", "html.parser")
    urls: list[str] = []
    seen: set[str] = set()
    for a in soup.select("a[href*='/company/']"):
        href = _normalize_text(a.get("href") or "")
        if not href:
            continue
        full_url = urljoin(page_url, href)
        parsed = urlparse(full_url)
        if "/company/" not in parsed.path:
            continue
        normalized = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
        if normalized not in seen:
            seen.add(normalized)
            urls.append(normalized)
    if urls:
        return urls, ""
    has_listing_scaffold = bool(
        soup.select(".supplier-listing, .listing, .result, .search-result, article, [data-company-id]")
    )
    return [], ("empty_listing" if has_listing_scaffold else "selector_missing")


def _company_id_from_url(company_url: str, fallback_index: int) -> str:
    text = _normalize_text(company_url)
    match = re.search(r"/company/[^/]+/(\d+)(?:/|$)", text)
    if match:
        return match.group(1)
    return str(max(1, int(fallback_index)))


def _extract_contact_name(text: str) -> str:
    match = re.search(
        r"(?:Contact(?: Name)?|Primary Contact)\s*[:\-]\s*([A-Za-z][A-Za-z .,'\-]{1,80})",
        text,
        flags=re.I,
    )
    if not match:
        return ""
    return _normalize_text(match.group(1))


def _extract_website_from_company(soup: BeautifulSoup, jsonld_objects: list[dict]) -> str:
    for a in soup.select("a.action-btn--website, a[href*='http://'], a[href*='https://']"):
        href = _normalize_text(a.get("href") or "")
        onclick = _normalize_text(a.get("onclick") or "")
        if onclick:
            match = re.search(r"window\.open\(['\"]([^'\"]+)['\"]", onclick)
            if match:
                website = _normalize_website(match.group(1))
                if _domain_from_website(website) and "buyersguide.ohsonline.com" not in website.lower():
                    return website
        if href.lower().startswith(("http://", "https://")):
            if "buyersguide.ohsonline.com" in href.lower():
                continue
            website = _normalize_website(href)
            if _domain_from_website(website):
                return website
    for item in jsonld_objects:
        website = _normalize_website(item.get("url") or "")
        if _domain_from_website(website):
            return website
    return ""


def _extract_city_state_from_company(soup: BeautifulSoup, jsonld_objects: list[dict], fallback_text: str) -> tuple[str, str]:
    for selector in (
        ".listing-header__address",
        ".supplier-location",
        ".company-address",
        "[itemprop='address']",
    ):
        node = soup.select_one(selector)
        if node is None:
            continue
        city, state = _extract_city_state(_normalize_text(node.get_text(" ", strip=True)))
        if city and state:
            return city, state
    for item in jsonld_objects:
        address = item.get("address") or {}
        if isinstance(address, list):
            address = address[0] if address else {}
        if isinstance(address, dict):
            city = _normalize_text(address.get("addressLocality") or "")
            state = _normalize_state(address.get("addressRegion") or "")
            if city and state:
                return city, state
    return _extract_city_state(fallback_text)


def _extract_firm_from_company(soup: BeautifulSoup, jsonld_objects: list[dict]) -> str:
    for selector in ("h1", ".listing-header__title", ".company-header__title", ".supplier-name", ".company-name"):
        node = soup.select_one(selector)
        if node is None:
            continue
        name = _normalize_text(node.get_text(" ", strip=True))
        if name:
            return name
    for item in jsonld_objects:
        name = _normalize_text(item.get("name") or "")
        if name:
            return name
    return ""


def _extract_emails_from_company(company_html: str, soup: BeautifulSoup) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for a in soup.find_all("a"):
        href = _normalize_text(a.get("href") or "")
        if href.lower().startswith("mailto:"):
            candidate = _normalize_email(href.split(":", 1)[1].split("?", 1)[0])
            if _valid_email(candidate) and candidate not in seen:
                seen.add(candidate)
                out.append(candidate)
    for item in _extract_emails(f"{company_html}\n{soup.get_text(' ', strip=True)}"):
        if item not in seen:
            seen.add(item)
            out.append(item)
    return out


def _parse_company_profile(
    company_html: str,
    company_url: str,
    state_filter: str,
    company_index: int,
) -> tuple[dict[str, str] | None, str, dict]:
    soup = BeautifulSoup(company_html or "", "html.parser")
    all_text = _normalize_text(soup.get_text(" ", strip=True))
    jsonld_objects = _extract_jsonld_objects(soup)
    company_id = _company_id_from_url(company_url, fallback_index=company_index)
    diag = {
        "company_url": company_url,
        "company_id": company_id,
        "status": "rejected",
        "reason": "",
    }

    firm = _extract_firm_from_company(soup, jsonld_objects)
    if not firm:
        diag["reason"] = "missing_firm"
        return None, "missing_firm", diag

    city, state = _extract_city_state_from_company(soup, jsonld_objects, fallback_text=all_text)
    if not city or not state:
        diag["reason"] = "invalid_city_state"
        return None, "invalid_city_state", diag
    if state_filter and state != state_filter:
        diag["reason"] = "invalid_city_state"
        return None, "invalid_city_state", diag

    website = _extract_website_from_company(soup, jsonld_objects)
    emails = _extract_emails_from_company(company_html, soup)
    email = emails[0] if emails else ""
    contact_name = _extract_contact_name(all_text)

    if not website and not email and not contact_name:
        diag["reason"] = "missing_contact_fields"
        return None, "missing_contact_fields", diag

    source = f"ohs_buyers_guide:company-{company_id}"
    row = {
        "prospect_id": "",
        "firm": firm,
        "company_name": firm,
        "email": email,
        "contact_email": email,
        "title": "Safety Consultant",
        "contact_role": "Safety Consultant",
        "contact_name": contact_name,
        "city": city,
        "state": state,
        "website": website,
        "source": source,
    }
    diag["status"] = "accepted"
    diag["reason"] = ""
    diag["firm"] = firm
    diag["city"] = city
    diag["state"] = state
    return row, "", diag


def _dedupe_company_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    by_source: dict[str, dict[str, str]] = {}
    for row in rows:
        source = _normalize_text(row.get("source") or "")
        if not source:
            continue
        existing = by_source.get(source)
        if existing is None:
            by_source[source] = row
            continue
        existing_has_email = _valid_email(existing.get("email") or existing.get("contact_email") or "")
        row_has_email = _valid_email(row.get("email") or row.get("contact_email") or "")
        if row_has_email and not existing_has_email:
            by_source[source] = row
    return list(by_source.values())


def _recover_missing_site_email(
    rows: list[dict[str, str]],
    max_attempts: int,
    sleep_ms: int,
    contact_fetcher=None,
) -> dict[str, int]:
    attempts = 0
    recovered = 0
    pages_fetched = 0
    for row in rows:
        email = _normalize_email(row.get("email") or row.get("contact_email") or "")
        if _valid_email(email):
            continue
        website = _normalize_text(row.get("website") or "")
        if not website:
            continue
        if attempts >= max_attempts:
            break
        attempts += 1
        pages = scraper_engine.fetch_contact_pages_for_domain(
            website,
            sleep_ms=max(0, int(sleep_ms or 0)),
            fetcher=contact_fetcher,
        )
        pages_fetched += len(pages)
        found = ""
        for item in pages:
            contacts = dict(item.get("contacts") or {})
            for candidate in list(contacts.get("emails") or []):
                email_candidate = _normalize_email(candidate)
                if _valid_email(email_candidate):
                    found = email_candidate
                    break
            if found:
                break
        if found:
            row["email"] = found
            row["contact_email"] = found
            recovered += 1
    return {"attempted": attempts, "recovered": recovered, "site_pages_fetched": pages_fetched}


def _fetch_browser_rows(
    state_norm: str,
    max_pages: int,
    sleep_ms: int,
    browser_fetcher,
    contact_fetcher,
    parse_counters: dict[str, int],
    parse_reasons: Counter,
) -> tuple[list[dict[str, str]], list[dict], list[dict], str]:
    rows: list[dict[str, str]] = []
    page_diagnostics: list[dict] = []
    row_diagnostics: list[dict] = []
    browser_error = ""
    company_urls: list[str] = []
    seen_company_urls: set[str] = set()

    for page_idx in range(max_pages):
        if page_idx > 0 and sleep_ms > 0:
            time.sleep(float(sleep_ms) / 1000.0)
        page_url = BROWSER_CATEGORY_URL if page_idx == 0 else f"{BROWSER_CATEGORY_URL}?Page={page_idx + 1}"
        page = browser_fetcher(page_url)
        parse_counters["fetched_pages"] += 1
        status = int(page.get("status") or 0)
        html = str(page.get("html") or "")
        if status != 200 or not html:
            parse_counters["hard_parse_failures"] += 1
            parse_reasons["selector_missing"] += 1
            page_diagnostics.append(
                {
                    "url": page_url,
                    "status": status,
                    "error": str(page.get("error") or "browser_fetch_failed"),
                    "reason": "selector_missing",
                }
            )
            browser_error = str(page.get("error") or f"http_status={status}")
            break

        links, reason = _extract_company_links(html, page_url=page_url)
        parse_counters["candidate_rows_seen"] += int(len(links))
        if not links:
            parse_counters["parsed_rows_rejected"] += 1
            parse_reasons[reason or "empty_listing"] += 1
            page_diagnostics.append({"url": page_url, "status": status, "reason": reason or "empty_listing"})
        else:
            page_diagnostics.append({"url": page_url, "status": status, "links_found": len(links)})
        for link in links:
            if link in seen_company_urls:
                continue
            seen_company_urls.add(link)
            company_urls.append(link)

    max_company_pages = max(1, int(max_pages) * 20)
    for company_idx, company_url in enumerate(company_urls[:max_company_pages], start=1):
        if company_idx > 1 and sleep_ms > 0:
            time.sleep(float(sleep_ms) / 1000.0)
        page = browser_fetcher(company_url)
        parse_counters["fetched_pages"] += 1
        status = int(page.get("status") or 0)
        html = str(page.get("html") or "")
        if status != 200 or not html:
            parse_counters["hard_parse_failures"] += 1
            parse_counters["parsed_rows_rejected"] += 1
            parse_reasons["selector_missing"] += 1
            row_diagnostics.append(
                {
                    "company_url": company_url,
                    "status": status,
                    "status_tag": "rejected",
                    "reason": "selector_missing",
                    "error": str(page.get("error") or "company_fetch_failed"),
                }
            )
            browser_error = str(page.get("error") or f"http_status={status}")
            continue

        row, reject_reason, diag = _parse_company_profile(
            company_html=html,
            company_url=company_url,
            state_filter=state_norm,
            company_index=company_idx,
        )
        if row is None:
            parse_counters["parsed_rows_rejected"] += 1
            parse_reasons[reject_reason or "missing_contact_fields"] += 1
            row_diagnostics.append(diag)
            continue

        parse_counters["parsed_rows_accepted"] += 1
        rows.append(row)
        row_diagnostics.append(diag)

    deduped_rows = _dedupe_company_rows(rows)
    email_recovery = _recover_missing_site_email(
        deduped_rows,
        max_attempts=max(1, int(max_pages) * 5),
        sleep_ms=sleep_ms,
        contact_fetcher=contact_fetcher,
    )
    for key, value in email_recovery.items():
        if isinstance(value, int):
            parse_counters.setdefault(f"email_recovery_{key}", 0)
            parse_counters[f"email_recovery_{key}"] = int(value)
    return deduped_rows, page_diagnostics, row_diagnostics, browser_error


def _fetch_legacy_rows(
    state_norm: str,
    max_pages: int,
    sleep_ms: int,
    fetch,
    parse_counters: dict[str, int],
    parse_reasons: Counter,
) -> tuple[list[dict[str, str]], str, list[str], dict[str, int]]:
    url = SEARCH_URL_TEMPLATE.format(state=state_norm)
    page_modes: list[str] = []
    all_rows: list[dict[str, str]] = []
    page_urls: list[str] = []
    legacy_diag_counts = {"allowlist_rejected": 0}

    for page_idx in range(max_pages):
        if page_idx > 0 and sleep_ms > 0:
            time.sleep(float(sleep_ms) / 1000.0)
        status, html = fetch(url)
        parse_counters["fetched_pages"] += 1
        page_urls.append(url)
        if int(status) != 200:
            parse_counters["hard_parse_failures"] += 1
            parse_counters["parsed_rows_rejected"] += 1
            parse_reasons["selector_missing"] += 1
            page_modes.append("FAILED")
            break
        page_rows, page_mode, page_diag = parse_ohs_bg_page(html, page_ref=f"p{page_idx + 1}")
        page_modes.append(page_mode)
        parse_counters["candidate_rows_seen"] += int(len(page_rows))
        if page_rows:
            parse_counters["parsed_rows_accepted"] += int(len(page_rows))
            all_rows.extend(page_rows)
        else:
            parse_counters["parsed_rows_rejected"] += 1
            parse_reasons["empty_listing"] += 1
        legacy_diag_counts["allowlist_rejected"] += int(page_diag.get("allowlist_rejected", 0))

        next_url = _next_page_url(html, current_url=url)
        if not next_url or next_url == url:
            break
        url = next_url

    parse_mode = "FAILED"
    if all_rows:
        parse_mode = page_modes[0] if len(set(page_modes)) == 1 else "MULTI"
    return all_rows, parse_mode, page_urls, legacy_diag_counts


def fetch_ohs_bg_state_rows(
    state: str,
    run_date: date,
    max_pages: int,
    sleep_ms: int,
    cache_dir: Path,
    diagnostics_dir: Path,
    fetcher=None,
    allow_cache_write: bool = True,
    browser_fetcher=None,
    contact_fetcher=None,
) -> dict:
    _ = run_date
    state_norm = str(state or "").strip().upper()
    if len(state_norm) != 2:
        raise ValueError("invalid_state")
    if max_pages < 1:
        raise ValueError("invalid_max_pages")
    if sleep_ms < 0:
        raise ValueError("invalid_sleep_ms")

    parse_counters = _empty_parse_counters()
    parse_reasons = Counter(_empty_parse_reasons())
    cache_path = _cache_path(cache_dir, state_norm)
    cached_payload = _read_cache(cache_path)
    if cached_payload and _cache_is_fresh(cached_payload):
        rows = list(cached_payload.get("rows") or [])
        cached_parse_counters = dict(cached_payload.get("parse_counters") or {})
        cached_parse_reasons = dict(cached_payload.get("parse_reasons") or {})
        counters_out = dict(_empty_parse_counters())
        counters_out.update({k: int(v or 0) for k, v in cached_parse_counters.items()})
        reasons_out = dict(_empty_parse_reasons())
        reasons_out.update({k: int(v or 0) for k, v in cached_parse_reasons.items()})
        return {
            "rows": rows,
            "cache_used": True,
            "cache_age_days": _cache_age_days(cached_payload),
            "cache_path": cache_path,
            "pages_fetched": int(counters_out.get("fetched_pages") or cached_payload.get("pages_fetched") or 0),
            "parse_mode": str(cached_payload.get("parse_mode") or "FAILED"),
            "diagnostics_path": None,
            "parse_counters": counters_out,
            "parse_reasons": reasons_out,
            "fetch_strategy": str(cached_payload.get("fetch_strategy") or ""),
        }

    fetch = fetcher or _default_fetcher
    use_browser = not (fetcher is not None and browser_fetcher is None)
    browser_fetch = browser_fetcher or _default_browser_fetcher
    contact_fetch = contact_fetcher or _default_fetcher

    rows: list[dict[str, str]] = []
    parse_mode = "FAILED"
    fetch_strategy = "LEGACY_ONLY" if not use_browser else "HYBRID_BROWSER_PRIMARY"
    error_text = ""
    browser_had_candidates_no_hard_fail = False
    page_diagnostics: list[dict] = []
    row_diagnostics: list[dict] = []
    legacy_page_urls: list[str] = []
    legacy_diag_counts = {"allowlist_rejected": 0}

    try:
        if use_browser:
            browser_rows, browser_pages_diag, browser_rows_diag, browser_error = _fetch_browser_rows(
                state_norm=state_norm,
                max_pages=max_pages,
                sleep_ms=sleep_ms,
                browser_fetcher=browser_fetch,
                contact_fetcher=contact_fetch,
                parse_counters=parse_counters,
                parse_reasons=parse_reasons,
            )
            rows.extend(browser_rows)
            page_diagnostics.extend(browser_pages_diag)
            row_diagnostics.extend(browser_rows_diag)
            browser_had_candidates_no_hard_fail = bool(parse_counters.get("candidate_rows_seen")) and int(
                parse_counters.get("hard_parse_failures") or 0
            ) == 0
            if browser_error:
                error_text = browser_error

        if rows:
            parse_mode = "BROWSER"
        elif use_browser and browser_had_candidates_no_hard_fail:
            parse_mode = "BROWSER_EMPTY"
            fetch_strategy = "HYBRID_BROWSER_PRIMARY"
        else:
            legacy_rows, legacy_mode, page_urls, legacy_diag = _fetch_legacy_rows(
                state_norm=state_norm,
                max_pages=max_pages,
                sleep_ms=sleep_ms,
                fetch=fetch,
                parse_counters=parse_counters,
                parse_reasons=parse_reasons,
            )
            legacy_page_urls = list(page_urls)
            legacy_diag_counts = dict(legacy_diag)
            rows.extend(legacy_rows)
            if legacy_rows:
                parse_mode = f"LEGACY_{legacy_mode}" if use_browser else str(legacy_mode)
                fetch_strategy = "HYBRID_FALLBACK_LEGACY" if use_browser else "LEGACY_ONLY"
            else:
                fetch_strategy = "HYBRID_FAILED" if use_browser else "LEGACY_ONLY_FAILED"

        if not rows and parse_mode not in {"BROWSER_EMPTY"}:
            raise RuntimeError(error_text or "page_parse_failed")

        payload = {
            "source": "ohs_buyers_guide",
            "state": state_norm,
            "fetched_at_utc": _utc_now_iso(),
            "cache_max_age_days": CACHE_MAX_AGE_DAYS,
            "pages_fetched": int(parse_counters.get("fetched_pages") or 0),
            "parse_mode": parse_mode,
            "fetch_strategy": fetch_strategy,
            "page_urls": legacy_page_urls,
            "diag_counts": legacy_diag_counts,
            "parse_counters": dict(parse_counters),
            "parse_reasons": {k: int(parse_reasons.get(k, 0)) for k in OHS_PARSE_REASON_KEYS},
            "rows": rows,
        }
        if allow_cache_write:
            _write_cache(cache_path, payload)

        diagnostics_payload = {
            "source": "ohs_buyers_guide",
            "state": state_norm,
            "generated_at_utc": _utc_now_iso(),
            "fetch_strategy": fetch_strategy,
            "parse_mode": parse_mode,
            "cache_path": str(cache_path),
            "parse_counters": dict(parse_counters),
            "parse_reasons": {k: int(parse_reasons.get(k, 0)) for k in OHS_PARSE_REASON_KEYS},
            "page_diagnostics": list(page_diagnostics)[:40],
            "row_diagnostics": list(row_diagnostics)[:120],
            "legacy_page_urls": legacy_page_urls[:20],
            "legacy_diag_counts": legacy_diag_counts,
        }
        diagnostics_path = _write_diagnostic(diagnostics_dir, state_norm, diagnostics_payload)
        return {
            "rows": rows,
            "cache_used": False,
            "cache_age_days": _cache_age_days(payload),
            "cache_path": cache_path,
            "pages_fetched": int(parse_counters.get("fetched_pages") or 0),
            "parse_mode": parse_mode,
            "diagnostics_path": diagnostics_path,
            "parse_counters": dict(parse_counters),
            "parse_reasons": {k: int(parse_reasons.get(k, 0)) for k in OHS_PARSE_REASON_KEYS},
            "fetch_strategy": fetch_strategy,
        }
    except Exception as exc:
        diagnostics_payload = {
            "source": "ohs_buyers_guide",
            "state": state_norm,
            "error": str(exc),
            "generated_at_utc": _utc_now_iso(),
            "cache_path": str(cache_path),
            "fetch_strategy": fetch_strategy,
            "parse_mode": "FAILED",
            "parse_counters": dict(parse_counters),
            "parse_reasons": {k: int(parse_reasons.get(k, 0)) for k in OHS_PARSE_REASON_KEYS},
            "page_diagnostics": list(page_diagnostics)[:40],
            "row_diagnostics": list(row_diagnostics)[:120],
            "legacy_page_urls": legacy_page_urls[:20],
            "legacy_diag_counts": legacy_diag_counts,
        }
        diagnostics_path = _write_diagnostic(diagnostics_dir, state_norm, diagnostics_payload)
        return {
            "rows": [],
            "cache_used": False,
            "cache_age_days": _cache_age_days(cached_payload or {}),
            "cache_path": cache_path,
            "pages_fetched": int(parse_counters.get("fetched_pages") or 0),
            "parse_mode": "FAILED",
            "diagnostics_path": diagnostics_path,
            "error": str(exc),
            "parse_counters": dict(parse_counters),
            "parse_reasons": {k: int(parse_reasons.get(k, 0)) for k in OHS_PARSE_REASON_KEYS},
            "fetch_strategy": fetch_strategy,
        }
