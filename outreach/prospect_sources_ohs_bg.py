import json
import re
import time
from datetime import date, datetime, timezone
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup


SEARCH_URL_TEMPLATE = "https://www.ohsonline.com/Directory/SearchResults.aspx?state={state}"
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


def _normalize_text(value: str) -> str:
    return " ".join(str(value or "").replace("\r", " ").replace("\n", " ").split()).strip()


def _extract_emails(text: str) -> list[str]:
    emails = re.findall(r"[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}", text, flags=re.I)
    out: list[str] = []
    seen: set[str] = set()
    for item in emails:
        email = _normalize_text(item).lower()
        if email and email not in seen:
            seen.add(email)
            out.append(email)
    return out


def _extract_city_state(text: str) -> tuple[str, str]:
    match = re.search(r"([A-Za-z .'-]+),\s*([A-Z]{2})(?:\s+\d{5}(?:-\d{4})?)?", text)
    if not match:
        return "", ""
    return _normalize_text(match.group(1)), _normalize_text(match.group(2)).upper()


def _allowlisted(text: str) -> bool:
    lowered = _normalize_text(text).lower()
    if not lowered:
        return False
    return any(term in lowered for term in ALLOWLIST_TERMS)


def _row_from_jsonld(item: dict, idx: int) -> list[dict[str, str]]:
    name = _normalize_text(item.get("name") or "")
    email = _normalize_text(item.get("email") or "").lower()
    if not email:
        return []
    address = item.get("address") or {}
    if isinstance(address, list):
        address = address[0] if address else {}
    if not isinstance(address, dict):
        address = {}
    city = _normalize_text(address.get("addressLocality") or "")
    state = _normalize_text(address.get("addressRegion") or "").upper()
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
                email = str(row.get("email") or "").strip().lower()
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


def fetch_ohs_bg_state_rows(
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

    fetch = fetcher or _default_fetcher
    cache_path = _cache_path(cache_dir, state_norm)
    cached_payload = _read_cache(cache_path)
    if cached_payload and _cache_is_fresh(cached_payload):
        rows = list(cached_payload.get("rows") or [])
        return {
            "rows": rows,
            "cache_used": True,
            "cache_age_days": _cache_age_days(cached_payload),
            "cache_path": cache_path,
            "pages_fetched": int(cached_payload.get("pages_fetched") or 0),
            "parse_mode": str(cached_payload.get("parse_mode") or "FAILED"),
            "diagnostics_path": None,
        }

    url = SEARCH_URL_TEMPLATE.format(state=state_norm)
    pages_fetched = 0
    page_modes: list[str] = []
    all_rows: list[dict[str, str]] = []
    diag_counts = {"allowlist_rejected": 0}
    page_urls: list[str] = []

    try:
        for page_idx in range(max_pages):
            if page_idx > 0 and sleep_ms > 0:
                time.sleep(float(sleep_ms) / 1000.0)
            status, html = fetch(url)
            pages_fetched += 1
            page_urls.append(url)
            if int(status) != 200:
                page_modes.append("FAILED")
                break
            page_rows, page_mode, page_diag = parse_ohs_bg_page(html, page_ref=f"p{page_idx + 1}")
            page_modes.append(page_mode)
            all_rows.extend(page_rows)
            diag_counts["allowlist_rejected"] += int(page_diag.get("allowlist_rejected", 0))

            next_url = _next_page_url(html, current_url=url)
            if not next_url or next_url == url:
                break
            url = next_url

        parse_mode = "FAILED"
        if all_rows:
            parse_mode = page_modes[0] if len(set(page_modes)) == 1 else "MULTI"

        if parse_mode == "FAILED":
            raise RuntimeError("page_parse_failed")

        payload = {
            "source": "ohs_buyers_guide",
            "state": state_norm,
            "fetched_at_utc": _utc_now_iso(),
            "cache_max_age_days": CACHE_MAX_AGE_DAYS,
            "pages_fetched": pages_fetched,
            "parse_mode": parse_mode,
            "page_urls": page_urls,
            "diag_counts": diag_counts,
            "rows": all_rows,
        }
        if allow_cache_write:
            _write_cache(cache_path, payload)
        return {
            "rows": all_rows,
            "cache_used": False,
            "cache_age_days": _cache_age_days(payload),
            "cache_path": cache_path,
            "pages_fetched": pages_fetched,
            "parse_mode": parse_mode,
            "diagnostics_path": None,
        }
    except Exception as exc:
        diagnostics_payload = {
            "source": "ohs_buyers_guide",
            "state": state_norm,
            "error": str(exc),
            "generated_at_utc": _utc_now_iso(),
            "cache_path": str(cache_path),
            "pages_fetched": pages_fetched,
            "page_urls": page_urls,
            "diag_counts": diag_counts,
        }
        diagnostics_path = _write_diagnostic(diagnostics_dir, state_norm, diagnostics_payload)
        return {
            "rows": [],
            "cache_used": False,
            "cache_age_days": _cache_age_days(cached_payload or {}),
            "cache_path": cache_path,
            "pages_fetched": pages_fetched,
            "parse_mode": "FAILED",
            "diagnostics_path": diagnostics_path,
            "error": str(exc),
        }
