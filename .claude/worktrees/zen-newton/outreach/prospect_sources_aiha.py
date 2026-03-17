import json
import re
import time
from collections import Counter
from datetime import date, datetime, timezone
from pathlib import Path

import requests
from bs4 import BeautifulSoup

from outreach import contact_normalization

TOC_URL = "https://info.aiha.org/consultants-listing/toc/"
PAGE_URL_TEMPLATE = "https://info.aiha.org/consultants-listing/{page_id}"
USER_AGENT = "OSHA_Leads/1.0 (+https://microflowops.com)"
CACHE_MAX_AGE_DAYS = 7
AIHA_REJECT_TOKENS = (
    "multi_person_contact",
    "placeholder_firm",
    "invalid_city_state",
    "missing_email",
)
PLACEHOLDER_FIRM_KEYS = {
    "inc",
    "llc",
    "consulting",
    "associates",
    "associate",
    "company",
    "co",
    "corporation",
    "corp",
}

US_STATE_NAMES = {
    "ALABAMA": "AL",
    "ALASKA": "AK",
    "ARIZONA": "AZ",
    "ARKANSAS": "AR",
    "CALIFORNIA": "CA",
    "COLORADO": "CO",
    "CONNECTICUT": "CT",
    "DELAWARE": "DE",
    "DISTRICT OF COLUMBIA": "DC",
    "FLORIDA": "FL",
    "GEORGIA": "GA",
    "HAWAII": "HI",
    "IDAHO": "ID",
    "ILLINOIS": "IL",
    "INDIANA": "IN",
    "IOWA": "IA",
    "KANSAS": "KS",
    "KENTUCKY": "KY",
    "LOUISIANA": "LA",
    "MAINE": "ME",
    "MARYLAND": "MD",
    "MASSACHUSETTS": "MA",
    "MICHIGAN": "MI",
    "MINNESOTA": "MN",
    "MISSISSIPPI": "MS",
    "MISSOURI": "MO",
    "MONTANA": "MT",
    "NEBRASKA": "NE",
    "NEVADA": "NV",
    "NEW HAMPSHIRE": "NH",
    "NEW JERSEY": "NJ",
    "NEW MEXICO": "NM",
    "NEW YORK": "NY",
    "NORTH CAROLINA": "NC",
    "NORTH DAKOTA": "ND",
    "OHIO": "OH",
    "OKLAHOMA": "OK",
    "OREGON": "OR",
    "PENNSYLVANIA": "PA",
    "RHODE ISLAND": "RI",
    "SOUTH CAROLINA": "SC",
    "SOUTH DAKOTA": "SD",
    "TENNESSEE": "TN",
    "TEXAS": "TX",
    "UTAH": "UT",
    "VERMONT": "VT",
    "VIRGINIA": "VA",
    "WASHINGTON": "WA",
    "WEST VIRGINIA": "WV",
    "WISCONSIN": "WI",
    "WYOMING": "WY",
}


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
    return cache_dir / f"state_{state.upper()}.json"


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


def _rows_for_state(rows: list[dict] | None, state: str) -> list[dict]:
    state_norm = _normalize_state_abbrev(state)
    if not state_norm:
        return []
    return [row for row in list(rows or []) if _normalize_state_abbrev(str(row.get("state") or "")) == state_norm]


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


def _state_abbrev_from_label(label: str) -> str | None:
    text = str(label or "").strip()
    if not text:
        return None
    upper = text.upper()
    if upper in US_STATE_NAMES:
        return US_STATE_NAMES[upper]
    if len(upper) == 2 and upper.isalpha():
        return upper
    return None


def parse_toc_state_starts(toc_html: str) -> dict[str, int]:
    soup = BeautifulSoup(toc_html or "", "html.parser")
    state_starts: dict[str, int] = {}
    for a in soup.find_all("a"):
        href = str(a.get("href") or "").strip()
        text = str(a.get_text(" ", strip=True) or "").strip()
        if not href or not text:
            continue
        match = re.search(r"/consultants-listing/(\d+)-(\d+)", href)
        if not match:
            continue
        state = _state_abbrev_from_label(text)
        if not state:
            continue
        start = int(match.group(1))
        if state not in state_starts:
            state_starts[state] = start
    return state_starts


def _page_ids_for_state(state_starts: dict[str, int], state: str, max_pages: int) -> list[str]:
    target = str(state or "").strip().upper()
    if target not in state_starts:
        return []
    start = int(state_starts[target])
    starts_sorted = sorted(set(int(v) for v in state_starts.values()))
    next_start = None
    for candidate in starts_sorted:
        if candidate > start:
            next_start = candidate
            break

    page_ids: list[str] = []
    current = start
    if next_start is None:
        for _ in range(max_pages):
            page_ids.append(f"{current}-{current + 1}")
            current += 2
        return page_ids

    while current < next_start and len(page_ids) < max_pages:
        page_ids.append(f"{current}-{current + 1}")
        current += 2
    return page_ids


def _normalize_text(text: str) -> str:
    return " ".join(str(text or "").replace("\r", " ").replace("\n", " ").split()).strip()


def _is_noise_paragraph(text: str) -> bool:
    value = _normalize_text(text)
    if not value:
        return True
    if value.isdigit():
        return True
    lowered = value.lower()
    if lowered in {"geographical listing", "powered by flippingbook"}:
        return True
    if lowered.startswith("the american industrial hygiene association"):
        return True
    return False


def _extract_page_paragraphs(html: str) -> tuple[list[str], str]:
    soup = BeautifulSoup(html or "", "html.parser")
    primary = [_normalize_text(p.get_text(" ", strip=True)) for p in soup.select("#text-container p")]
    primary = [p for p in primary if not _is_noise_paragraph(p)]
    if primary:
        return primary, "TEXT_CONTAINER"

    fallback = [_normalize_text(p.get_text(" ", strip=True)) for p in soup.find_all("p")]
    fallback = [p for p in fallback if not _is_noise_paragraph(p)]
    if fallback:
        return fallback, "FALLBACK"
    return [], "FAILED"


def _split_segments(paragraphs: list[str]) -> list[str]:
    if not paragraphs:
        return []

    direct_segments: list[str] = []
    for paragraph in paragraphs:
        p = _normalize_text(paragraph)
        if not p:
            continue
        lower = p.lower()
        has_listing_type = ("commercial" in lower) or ("residential" in lower)
        has_listing_structure = ("contact:" in lower) or ("website:" in lower) or ("specialty:" in lower)
        if has_listing_type and has_listing_structure:
            direct_segments.append(p)
    if direct_segments:
        return direct_segments

    merged = " ".join(paragraphs)
    if not merged:
        return []

    start_pattern = re.compile(
        r"(?=(?:^|\s)([A-Z0-9][A-Za-z0-9&'().,/\- ]{2,}?)\s+(?:Commercial|Residential|Commercial/Residential)\b)"
    )
    starts = [m.start() for m in start_pattern.finditer(merged)]
    if not starts:
        return [merged]

    segments: list[str] = []
    for i, start in enumerate(starts):
        end = starts[i + 1] if i + 1 < len(starts) else len(merged)
        seg = _normalize_text(merged[start:end])
        if seg:
            segments.append(seg)
    return segments


def _extract_city_state(text: str) -> tuple[str, str]:
    matches = list(re.finditer(r"([A-Za-z .'-]+),\s*([A-Z]{2})\s+\d{5}(?:-\d{4})?", text))
    if not matches:
        return "", ""
    match = matches[-1]
    city = _normalize_text(match.group(1))
    state = _normalize_text(match.group(2)).upper()
    return city, state


def _extract_firm(text: str) -> str:
    match = re.match(r"([A-Z0-9][A-Za-z0-9&'().,/\- ]{2,}?)\s+(?:Commercial|Residential|Commercial/Residential)\b", text)
    if not match:
        return ""
    return _normalize_text(match.group(1))


def _extract_contact_name(text: str) -> str:
    match = re.search(r"Contact:\s*(.+?)(?=\s+Contact Email:|\s+Specialty:|$)", text)
    if not match:
        return ""
    return _normalize_text(match.group(1))


def _extract_website(text: str) -> str:
    match = re.search(r"Website:\s*(.+?)(?=\s+Contact:|\s+Contact Email:|\s+Specialty:|$)", text)
    if not match:
        return ""
    raw = _normalize_text(match.group(1)).replace(" ", "")
    if not raw:
        return ""
    if raw.lower().startswith(("contact:", "contactemail:", "specialty:")):
        return ""
    return contact_normalization.canonicalize_http_url(raw)


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


def _clean_city(city: str) -> str:
    text = _normalize_text(city)
    if not text:
        return ""
    compact = re.sub(r"\s+", " ", text).strip(" ,")
    if not compact:
        return ""
    compact = re.sub(r",?\s+[A-Z]{2}\s+\d{5}(?:-\d{4})?$", "", compact).strip(" ,")
    parts = [p.strip() for p in compact.split(",") if p.strip()]
    if len(parts) > 1:
        for candidate in reversed(parts):
            if re.fullmatch(r"[A-Za-z .'-]{2,}", candidate):
                return candidate
    tokens = compact.split()
    street_tokens = {
        "street",
        "st",
        "road",
        "rd",
        "avenue",
        "ave",
        "boulevard",
        "blvd",
        "drive",
        "dr",
        "lane",
        "ln",
        "way",
        "suite",
        "ste",
        "highway",
        "hwy",
    }
    if len(tokens) >= 3 and any(t.lower().strip(".") in street_tokens for t in tokens[:-1]):
        for width in (2, 3):
            if len(tokens) < width:
                continue
            candidate = " ".join(tokens[-width:])
            if re.fullmatch(r"[A-Za-z .'-]{2,}", candidate):
                return candidate
    if re.search(r"\d", compact):
        for candidate in parts:
            if re.fullmatch(r"[A-Za-z .'-]{2,}", candidate):
                return candidate
        return ""
    if re.fullmatch(r"[A-Za-z .'-]{2,}", compact):
        return compact
    return ""


def _normalize_state_abbrev(value: str) -> str:
    text = _normalize_text(value).upper()
    if len(text) == 2 and text in US_STATE_NAMES.values():
        return text
    return ""


def _firm_key(value: str) -> str:
    return re.sub(r"[^a-z0-9 ]+", "", _normalize_text(value).lower()).strip()


def _is_placeholder_firm(firm: str) -> bool:
    key = _firm_key(firm)
    if not key:
        return True
    if key in PLACEHOLDER_FIRM_KEYS:
        return True
    return False


def _is_multi_person_contact(contact_name: str) -> bool:
    text = _normalize_text(contact_name)
    if not text:
        return True
    lowered = text.lower()
    if " and " in lowered or " & " in lowered or ";" in text:
        return True
    if "/" in text and re.search(r"[A-Za-z]\s*/\s*[A-Za-z]", text):
        return True
    if re.search(r",\s*[A-Z][a-z]+(?:\s+[A-Z][A-Za-z.'-]+)+", text):
        return True
    return False


def _is_valid_city_state(city: str, state: str) -> bool:
    city_norm = _clean_city(city)
    state_norm = _normalize_state_abbrev(state)
    if not city_norm or not state_norm:
        return False
    return bool(re.fullmatch(r"[A-Za-z .'-]{2,}", city_norm))


def parse_aiha_page_with_diagnostics(page_html: str, page_id: str) -> dict:
    paragraphs, mode = _extract_page_paragraphs(page_html)
    if not paragraphs:
        return {
            "rows": [],
            "mode": "FAILED",
            "reject_counts": {token: 0 for token in AIHA_REJECT_TOKENS},
            "row_diagnostics": [],
        }

    segments = _split_segments(paragraphs)
    rows: list[dict[str, str]] = []
    reject_counts: Counter = Counter()
    row_diagnostics: list[dict[str, str]] = []
    for idx, segment in enumerate(segments, start=1):
        segment_text = _normalize_text(segment)
        if not segment_text:
            continue
        if not re.search(r"\b(?:commercial(?:/residential)?|residential)\b", segment_text, flags=re.I):
            continue
        firm = _extract_firm(segment_text)
        city_raw, state_raw = _extract_city_state(segment_text)
        city = _clean_city(city_raw)
        state = _normalize_state_abbrev(state_raw)
        website = _extract_website(segment_text)
        contact_name = _extract_contact_name(segment_text)
        emails = _extract_emails(segment_text)
        email = emails[0] if emails else ""

        reject_token = ""
        if _is_multi_person_contact(contact_name):
            reject_token = "multi_person_contact"
        elif _is_placeholder_firm(firm):
            reject_token = "placeholder_firm"
        elif not _is_valid_city_state(city, state):
            reject_token = "invalid_city_state"
        elif not email:
            reject_token = "missing_email"

        diag = {
            "page_id": str(page_id or ""),
            "segment_index": str(idx),
            "status": "accepted" if not reject_token else "rejected",
            "reject_token": reject_token,
            "firm": _normalize_text(firm),
            "contact_name": _normalize_text(contact_name),
            "email": _normalize_text(email).lower(),
            "city": _normalize_text(city),
            "state": _normalize_text(state).upper(),
        }

        if reject_token:
            reject_counts[reject_token] += 1
            row_diagnostics.append(diag)
            continue

        row_diagnostics.append(diag)
        rows.append(
            {
                "prospect_id": "",
                "firm": _normalize_text(firm),
                "company_name": _normalize_text(firm),
                "email": _normalize_text(email).lower(),
                "contact_email": _normalize_text(email).lower(),
                "title": "EHS Consultant",
                "contact_role": "EHS Consultant",
                "contact_name": _normalize_text(contact_name),
                "city": _normalize_text(city),
                "state": _normalize_text(state).upper(),
                "website": _normalize_text(website),
                "source": f"aiha_consultants_listing:{page_id}",
            }
        )

    reject_counts_out = {token: int(reject_counts.get(token, 0)) for token in AIHA_REJECT_TOKENS}
    return {
        "rows": rows,
        "mode": mode,
        "reject_counts": reject_counts_out,
        "row_diagnostics": row_diagnostics,
    }


def parse_aiha_page(page_html: str, page_id: str) -> tuple[list[dict[str, str]], str]:
    parsed = parse_aiha_page_with_diagnostics(page_html=page_html, page_id=page_id)
    return list(parsed.get("rows") or []), str(parsed.get("mode") or "FAILED")


def _write_diagnostic(diagnostics_dir: Path, state: str, payload: dict) -> Path:
    diagnostics_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = diagnostics_dir / f"aiha_{state.lower()}_{ts}.json"
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return path


def _effective_parse_mode(page_modes: list[str], total_rows: int) -> str:
    _ = total_rows
    if not page_modes:
        return "FAILED"
    if all(m == "FAILED" for m in page_modes):
        return "FAILED"
    if any(m == "FALLBACK" for m in page_modes):
        return "FALLBACK"
    if any(m == "TEXT_CONTAINER" for m in page_modes):
        return "TEXT_CONTAINER"
    return "FAILED"


def fetch_aiha_state_rows(
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
        rows = _rows_for_state(list(cached_payload.get("rows") or []), state_norm)
        reject_counts = dict(cached_payload.get("reject_counts") or {})
        row_diagnostics = list(cached_payload.get("row_diagnostics") or [])
        return {
            "rows": rows,
            "cache_used": True,
            "cache_age_days": _cache_age_days(cached_payload),
            "cache_path": cache_path,
            "pages_fetched": int(cached_payload.get("pages_fetched") or 0),
            "parse_mode": str(cached_payload.get("parse_mode") or "FAILED"),
            "reject_counts": {token: int(reject_counts.get(token, 0)) for token in AIHA_REJECT_TOKENS},
            "row_diagnostics": row_diagnostics,
            "diagnostics_path": None,
        }

    diagnostics_path = None
    try:
        status, toc_html = fetch(TOC_URL)
        if int(status) != 200:
            raise RuntimeError(f"toc_status={status}")
        state_starts = parse_toc_state_starts(toc_html)
        page_ids = _page_ids_for_state(state_starts, state_norm, max_pages=max_pages)
        if not page_ids:
            raise RuntimeError(f"state_not_found={state_norm}")

        all_rows: list[dict[str, str]] = []
        page_modes: list[str] = []
        reject_counts: Counter = Counter()
        row_diagnostics: list[dict[str, str]] = []
        pages_fetched = 0
        for idx, page_id in enumerate(page_ids):
            if idx > 0 and sleep_ms > 0:
                time.sleep(float(sleep_ms) / 1000.0)
            status, page_html = fetch(PAGE_URL_TEMPLATE.format(page_id=page_id))
            pages_fetched += 1
            if int(status) != 200:
                page_modes.append("FAILED")
                continue
            page_parse = parse_aiha_page_with_diagnostics(page_html=page_html, page_id=page_id)
            page_mode = str(page_parse.get("mode") or "FAILED")
            page_modes.append(page_mode)
            page_rows = _rows_for_state(list(page_parse.get("rows") or []), state_norm)
            all_rows.extend(page_rows)
            for token, count in dict(page_parse.get("reject_counts") or {}).items():
                if token in AIHA_REJECT_TOKENS:
                    reject_counts[token] += int(count or 0)
            row_diagnostics.extend(list(page_parse.get("row_diagnostics") or []))

        parse_mode = _effective_parse_mode(page_modes, total_rows=len(all_rows))
        if parse_mode == "FAILED":
            raise RuntimeError("page_parse_failed")

        payload = {
            "source": "aiha_consultants_listing",
            "state": state_norm,
            "fetched_at_utc": _utc_now_iso(),
            "cache_max_age_days": CACHE_MAX_AGE_DAYS,
            "pages_fetched": pages_fetched,
            "parse_mode": parse_mode,
            "rows": all_rows,
            "reject_counts": {token: int(reject_counts.get(token, 0)) for token in AIHA_REJECT_TOKENS},
            "row_diagnostics": row_diagnostics,
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
            "reject_counts": {token: int(reject_counts.get(token, 0)) for token in AIHA_REJECT_TOKENS},
            "row_diagnostics": row_diagnostics,
            "diagnostics_path": None,
        }
    except Exception as exc:
        diagnostics_payload = {
            "source": "aiha_consultants_listing",
            "state": state_norm,
            "error": str(exc),
            "generated_at_utc": _utc_now_iso(),
            "cache_path": str(cache_path),
        }
        diagnostics_path = _write_diagnostic(diagnostics_dir, state_norm, diagnostics_payload)
        return {
            "rows": [],
            "cache_used": False,
            "cache_age_days": _cache_age_days(cached_payload or {}),
            "cache_path": cache_path,
            "pages_fetched": 0,
            "parse_mode": "FAILED",
            "diagnostics_path": diagnostics_path,
            "error": str(exc),
        }
