import json
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Callable

import requests


SEARCH_URL = "https://api.apollo.io/api/v1/mixed_people/api_search"
ENRICH_URL = "https://api.apollo.io/api/v1/people/bulk_match"
USER_AGENT = "OSHA_Leads/1.0 (+https://microflowops.com)"
CACHE_MAX_AGE_DAYS = 7
SEARCH_PAGE_SIZE = 100
ENRICH_BATCH_SIZE = 10


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


def _write_diagnostic(diagnostics_dir: Path, state: str, payload: dict) -> Path:
    diagnostics_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = diagnostics_dir / f"apollo_{state.lower()}_{ts}.json"
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return path


def _normalize_text(value: str) -> str:
    return " ".join(str(value or "").replace("\r", " ").replace("\n", " ").split()).strip()


def _normalize_state(value: str) -> str:
    return str(value or "").strip().upper()


def _normalize_email(value: str) -> str:
    return str(value or "").strip().lower()


def _email_domain(email: str) -> str:
    e = _normalize_email(email)
    if "@" not in e:
        return ""
    return e.split("@", 1)[1].strip().lower()


def _default_post_json(url: str, payload: dict, api_key: str) -> tuple[int, dict]:
    headers = {
        "Content-Type": "application/json",
        "X-Api-Key": api_key,
        "User-Agent": USER_AGENT,
    }
    resp = requests.post(url, json=payload, headers=headers, timeout=30)
    status_code = int(resp.status_code)
    try:
        parsed = resp.json()
    except Exception:
        parsed = {"raw_text": str(resp.text or "")}
    return status_code, parsed


def _chunked(items: list[dict], size: int) -> list[list[dict]]:
    if size <= 0:
        return [items]
    return [items[i : i + size] for i in range(0, len(items), size)]


def _coalesce(state_candidate: str, fallback: str) -> str:
    state = _normalize_state(state_candidate)
    return state or _normalize_state(fallback)


def _extract_search_people(payload: dict) -> list[dict]:
    candidates: list[dict] = []
    people = payload.get("people")
    if isinstance(people, list):
        candidates.extend([p for p in people if isinstance(p, dict)])
    if not candidates:
        data = payload.get("data")
        if isinstance(data, dict):
            people2 = data.get("people") or data.get("contacts")
            if isinstance(people2, list):
                candidates.extend([p for p in people2 if isinstance(p, dict)])
    return candidates


def _extract_bulk_people(payload: dict) -> list[dict]:
    for key in ("matches", "people", "persons", "data"):
        value = payload.get(key)
        if isinstance(value, list):
            return [v for v in value if isinstance(v, dict)]
        if isinstance(value, dict):
            for nested_key in ("matches", "people", "persons"):
                nested = value.get(nested_key)
                if isinstance(nested, list):
                    return [v for v in nested if isinstance(v, dict)]
    return []


def _search_person_id(person: dict) -> str:
    return _normalize_text(person.get("id") or person.get("person_id") or person.get("apollo_id") or "")


def _search_has_email(person: dict) -> bool:
    raw = person.get("has_email")
    if isinstance(raw, bool):
        return raw
    text = str(raw or "").strip().lower()
    return text in {"1", "true", "yes", "on"}


def _map_enriched_person_to_row(enriched: dict, target_state: str) -> dict[str, str] | None:
    person = enriched.get("person") if isinstance(enriched.get("person"), dict) else enriched
    if not isinstance(person, dict):
        return None

    email = _normalize_email(
        person.get("email")
        or person.get("work_email")
        or person.get("email_address")
        or person.get("primary_email")
        or ""
    )
    if not email:
        return None

    org = person.get("organization") if isinstance(person.get("organization"), dict) else {}
    org_name = _normalize_text(org.get("name") or person.get("organization_name") or person.get("company_name") or "")
    website = _normalize_text(org.get("website_url") or org.get("website") or person.get("website_url") or "")
    domain = _normalize_text(org.get("primary_domain") or org.get("domain") or person.get("organization_domain") or "").lower()

    state = _coalesce(
        person.get("state")
        or person.get("state_code")
        or person.get("person_state")
        or ((person.get("location") or {}) if isinstance(person.get("location"), dict) else {}).get("state"),
        target_state,
    )

    city = _normalize_text(
        person.get("city")
        or ((person.get("location") or {}) if isinstance(person.get("location"), dict) else {}).get("city")
        or ""
    )
    title = _normalize_text(person.get("title") or person.get("job_title") or "")
    name = _normalize_text(person.get("name") or person.get("full_name") or "")

    apollo_id = _search_person_id(person)
    source_suffix = f":{apollo_id}" if apollo_id else ""
    return {
        "prospect_id": "",
        "company_name": org_name,
        "contact_email": email,
        "contact_role": title or "Contact",
        "contact_name": name,
        "city": city,
        "state": state,
        "domain": domain or _email_domain(email),
        "website": website,
        "source": f"apollo:bulk_match{source_suffix}",
    }


def fetch_apollo_state_rows(
    state: str,
    run_date: date,
    max_pages: int,
    sleep_ms: int,
    cache_dir: Path,
    diagnostics_dir: Path,
    api_key: str,
    enrich_enabled: bool,
    enrich_limit: int,
    person_titles: list[str],
    person_locations_mode: str = "state",
    fetcher: Callable[[str, dict, str], tuple[int, dict]] | None = None,
    allow_cache_write: bool = True,
) -> dict:
    _ = run_date
    state_norm = _normalize_state(state)
    if len(state_norm) != 2:
        raise ValueError("invalid_state")
    if max_pages < 1:
        raise ValueError("invalid_max_pages")
    if sleep_ms < 0:
        raise ValueError("invalid_sleep_ms")
    if enrich_limit < 0:
        raise ValueError("invalid_enrich_limit")
    if _normalize_text(person_locations_mode).lower() != "state":
        raise ValueError("invalid_person_locations_mode")
    if not _normalize_text(api_key):
        raise ValueError("missing_apollo_api_key")

    cache_path = _cache_path(cache_dir, state_norm)
    cached_payload = _read_cache(cache_path)
    if cached_payload and _cache_is_fresh(cached_payload):
        return {
            "rows": list(cached_payload.get("rows") or []),
            "cache_used": True,
            "cache_age_days": _cache_age_days(cached_payload),
            "cache_path": cache_path,
            "pages_fetched": int(cached_payload.get("pages_fetched") or 0),
            "parse_mode": str(cached_payload.get("parse_mode") or "API"),
            "search_rows_returned": int(cached_payload.get("search_rows_returned") or 0),
            "search_rows_has_email_true": int(cached_payload.get("search_rows_has_email_true") or 0),
            "search_rows_deduped_id": int(cached_payload.get("search_rows_deduped_id") or 0),
            "enrich_attempted": int(cached_payload.get("enrich_attempted") or 0),
            "enriched": int(cached_payload.get("enriched") or 0),
            "enrich_no_match": int(cached_payload.get("enrich_no_match") or 0),
            "enrich_skipped_credit_cap": int(cached_payload.get("enrich_skipped_credit_cap") or 0),
            "credit_cap_hit": bool(cached_payload.get("credit_cap_hit")),
            "diagnostics_path": None,
        }

    post_json = fetcher or _default_post_json
    diagnostics_path = None
    try:
        person_titles_clean = [_normalize_text(t) for t in list(person_titles or []) if _normalize_text(t)]
        if not person_titles_clean:
            raise ValueError("empty_person_titles")

        seen_ids: set[str] = set()
        search_rows_returned = 0
        search_rows_has_email_true = 0
        search_rows_deduped_id = 0
        pages_fetched = 0
        search_people_for_enrich: list[dict] = []
        total_pages_hint = None

        for page in range(1, max_pages + 1):
            if page > 1 and sleep_ms > 0:
                time.sleep(float(sleep_ms) / 1000.0)
            payload = {
                "page": page,
                "per_page": SEARCH_PAGE_SIZE,
                "person_titles": person_titles_clean,
                "person_locations": [f"{state_norm}, US"],
            }
            status, search_resp = post_json(SEARCH_URL, payload, api_key)
            pages_fetched += 1
            if int(status) != 200:
                raise RuntimeError(f"search_status={status}")
            people = _extract_search_people(search_resp)
            if total_pages_hint is None:
                total_pages_hint = search_resp.get("total_pages") or (search_resp.get("pagination") or {}).get("total_pages")
            if not people:
                break
            for person in people:
                search_rows_returned += 1
                if not _search_has_email(person):
                    continue
                search_rows_has_email_true += 1
                person_id = _search_person_id(person)
                if not person_id:
                    continue
                if person_id in seen_ids:
                    search_rows_deduped_id += 1
                    continue
                seen_ids.add(person_id)
                search_people_for_enrich.append(person)
            if len(people) < SEARCH_PAGE_SIZE:
                break
            try:
                if total_pages_hint is not None and int(total_pages_hint) <= page:
                    break
            except Exception:
                pass

        if not enrich_enabled or enrich_limit <= 0:
            rows: list[dict[str, str]] = []
            payload = {
                "source": "apollo",
                "state": state_norm,
                "fetched_at_utc": _utc_now_iso(),
                "cache_max_age_days": CACHE_MAX_AGE_DAYS,
                "pages_fetched": pages_fetched,
                "parse_mode": "API_SEARCH_ONLY",
                "rows": rows,
                "search_rows_returned": search_rows_returned,
                "search_rows_has_email_true": search_rows_has_email_true,
                "search_rows_deduped_id": search_rows_deduped_id,
                "enrich_attempted": 0,
                "enriched": 0,
                "enrich_no_match": 0,
                "enrich_skipped_credit_cap": max(0, len(search_people_for_enrich)),
                "credit_cap_hit": len(search_people_for_enrich) > 0,
            }
            if allow_cache_write:
                _write_cache(cache_path, payload)
            return {
                "rows": rows,
                "cache_used": False,
                "cache_age_days": _cache_age_days(payload),
                "cache_path": cache_path,
                "pages_fetched": pages_fetched,
                "parse_mode": "API_SEARCH_ONLY",
                "search_rows_returned": search_rows_returned,
                "search_rows_has_email_true": search_rows_has_email_true,
                "search_rows_deduped_id": search_rows_deduped_id,
                "enrich_attempted": 0,
                "enriched": 0,
                "enrich_no_match": 0,
                "enrich_skipped_credit_cap": max(0, len(search_people_for_enrich)),
                "credit_cap_hit": len(search_people_for_enrich) > 0,
                "diagnostics_path": None,
            }

        candidates_to_enrich = search_people_for_enrich[: max(0, enrich_limit)]
        enrich_skipped_credit_cap = max(0, len(search_people_for_enrich) - len(candidates_to_enrich))
        credit_cap_hit = enrich_skipped_credit_cap > 0
        enrich_attempted = 0
        enriched = 0
        enrich_no_match = 0
        rows: list[dict[str, str]] = []
        seen_row_emails: set[str] = set()

        for batch in _chunked(candidates_to_enrich, ENRICH_BATCH_SIZE):
            if not batch:
                continue
            if sleep_ms > 0 and enrich_attempted > 0:
                time.sleep(float(sleep_ms) / 1000.0)
            details = []
            for person in batch:
                person_id = _search_person_id(person)
                if not person_id:
                    continue
                details.append({"id": person_id})
            if not details:
                continue
            enrich_attempted += len(details)
            status, enrich_resp = post_json(ENRICH_URL, {"details": details}, api_key)
            if int(status) != 200:
                raise RuntimeError(f"enrich_status={status}")
            matches = _extract_bulk_people(enrich_resp)
            if not matches:
                enrich_no_match += len(details)
                continue
            matched_ids: set[str] = set()
            for match in matches:
                row = _map_enriched_person_to_row(match, target_state=state_norm)
                if row is None:
                    enrich_no_match += 1
                    continue
                apollo_id = _search_person_id(match)
                if apollo_id:
                    matched_ids.add(apollo_id)
                email = _normalize_email(row.get("contact_email") or "")
                if email and email in seen_row_emails:
                    continue
                if email:
                    seen_row_emails.add(email)
                rows.append(row)
                enriched += 1
            enrich_no_match += max(0, len(details) - len(matched_ids))

        payload = {
            "source": "apollo",
            "state": state_norm,
            "fetched_at_utc": _utc_now_iso(),
            "cache_max_age_days": CACHE_MAX_AGE_DAYS,
            "pages_fetched": pages_fetched,
            "parse_mode": "API",
            "rows": rows,
            "search_rows_returned": search_rows_returned,
            "search_rows_has_email_true": search_rows_has_email_true,
            "search_rows_deduped_id": search_rows_deduped_id,
            "enrich_attempted": enrich_attempted,
            "enriched": enriched,
            "enrich_no_match": enrich_no_match,
            "enrich_skipped_credit_cap": enrich_skipped_credit_cap,
            "credit_cap_hit": credit_cap_hit,
        }
        if allow_cache_write:
            _write_cache(cache_path, payload)
        return {
            "rows": rows,
            "cache_used": False,
            "cache_age_days": _cache_age_days(payload),
            "cache_path": cache_path,
            "pages_fetched": pages_fetched,
            "parse_mode": "API",
            "search_rows_returned": search_rows_returned,
            "search_rows_has_email_true": search_rows_has_email_true,
            "search_rows_deduped_id": search_rows_deduped_id,
            "enrich_attempted": enrich_attempted,
            "enriched": enriched,
            "enrich_no_match": enrich_no_match,
            "enrich_skipped_credit_cap": enrich_skipped_credit_cap,
            "credit_cap_hit": credit_cap_hit,
            "diagnostics_path": None,
        }
    except Exception as exc:
        diagnostics_payload = {
            "source": "apollo",
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
            "search_rows_returned": 0,
            "search_rows_has_email_true": 0,
            "search_rows_deduped_id": 0,
            "enrich_attempted": 0,
            "enriched": 0,
            "enrich_no_match": 0,
            "enrich_skipped_credit_cap": 0,
            "credit_cap_hit": False,
            "diagnostics_path": diagnostics_path,
            "error": str(exc),
        }
