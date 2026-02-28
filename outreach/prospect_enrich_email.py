import json
import re
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

from bs4 import BeautifulSoup
import requests

from outreach import scraper_engine


USER_AGENT = "OSHA_Leads/1.0 (+https://microflowops.com)"
HUNTER_FREE_MONTHLY_CAP = 25
RESOLVE_OK_STATUSES = {200, 301, 302}
WEBSITE_EMAIL_CACHE_TTL_DAYS = 14
WEBSITE_PATHS = ["/", "/contact", "/contact-us", "/about", "/about-us", "/team"]
ROLE_INBOX_LOCALS = {
    "info",
    "contact",
    "admin",
    "office",
    "support",
    "sales",
    "hello",
    "help",
    "billing",
    "accounts",
    "careers",
    "jobs",
    "hr",
}
CORP_SUFFIXES = {
    "LLC",
    "L.L.C",
    "INC",
    "INC.",
    "CORP",
    "CORPORATION",
    "CO",
    "CO.",
    "COMPANY",
    "LTD",
    "LTD.",
    "LP",
    "LLP",
    "PLC",
}


def _normalize_text(value: Any) -> str:
    return " ".join(str(value or "").replace("\r", " ").replace("\n", " ").split()).strip()


def _normalize_email(value: str) -> str:
    return str(value or "").strip().lower()


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


def _email_local_part(value: str) -> str:
    email = _normalize_email(value)
    if "@" not in email:
        return ""
    return email.split("@", 1)[0].split("+", 1)[0]


def _email_domain(value: str) -> str:
    email = _normalize_email(value)
    if "@" not in email:
        return ""
    return email.split("@", 1)[1].strip().lower()


def _is_role_inbox_email(value: str) -> bool:
    return _email_local_part(value) in ROLE_INBOX_LOCALS


def _domain_from_url(value: str) -> str:
    raw = _normalize_text(value).lower()
    if not raw:
        return ""
    try:
        parsed = urlparse(raw if "://" in raw else f"https://{raw}")
    except Exception:
        return ""
    host = (parsed.netloc or parsed.path or "").strip().lower()
    if host.startswith("www."):
        host = host[4:]
    return host


def _email_matches_domain(email: str, domain: str) -> bool:
    dom = _normalize_text(domain).lower()
    email_dom = _email_domain(email)
    if not dom or not email_dom:
        return False
    return email_dom == dom or email_dom.endswith(f".{dom}")


def _normalized_firm_tokens(firm: str) -> list[str]:
    cleaned = re.sub(r"[^\w\s]", " ", _normalize_text(firm).upper())
    tokens = [t for t in cleaned.split() if t]
    while tokens and tokens[-1] in CORP_SUFFIXES:
        tokens.pop()
    return tokens


def _normalized_company_key(value: str) -> str:
    tokens = _normalized_firm_tokens(value)
    return "".join(re.sub(r"[^A-Z0-9]", "", t) for t in tokens)


def _candidate_domains_for_firm(firm: str) -> list[str]:
    tokens = _normalized_firm_tokens(firm)
    if not tokens:
        return []
    base_tokens = [re.sub(r"[^A-Z0-9]", "", t) for t in tokens]
    base_tokens = [t.lower() for t in base_tokens if t]
    if not base_tokens:
        return []
    compact = "".join(base_tokens)
    hyphenated = "-".join(base_tokens)
    out: list[str] = []
    for cand in (compact, hyphenated):
        if not cand:
            continue
        dom = f"{cand}.com"
        if dom not in out:
            out.append(dom)
    return out


def _name_parts(owner_name: str) -> tuple[str, str]:
    parts = [re.sub(r"[^a-z0-9]", "", p.lower()) for p in _normalize_text(owner_name).split()]
    parts = [p for p in parts if p]
    if not parts:
        return "", ""
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], parts[-1]


def _candidate_record(email: str, source: str) -> dict[str, str]:
    normalized = _normalize_email(email)
    return {
        "email": normalized,
        "source": _normalize_text(source),
        "kind": "role_inbox" if _is_role_inbox_email(normalized) else "person",
    }


def _guess_email_candidate_records(owner_name: str, firm: str, domain: str) -> list[dict[str, str]]:
    dom = _normalize_text(domain).lower()
    if not dom:
        return []
    first, last = _name_parts(owner_name)
    owner_is_company = _normalized_company_key(owner_name) and (_normalized_company_key(owner_name) == _normalized_company_key(firm))

    ordered: list[dict[str, str]] = []
    if owner_is_company:
        ordered.append(_candidate_record(f"info@{dom}", "domain_guess"))
    if first:
        ordered.append(_candidate_record(f"{first}@{dom}", "pattern_guess"))
        if last:
            ordered.append(_candidate_record(f"{first}.{last}@{dom}", "pattern_guess"))
            ordered.append(_candidate_record(f"{first[:1]}{last}@{dom}", "pattern_guess"))
    ordered.append(_candidate_record(f"info@{dom}", "domain_guess"))

    out: list[dict[str, str]] = []
    seen: set[str] = set()
    for cand in ordered:
        email = _normalize_email(cand.get("email") or "")
        if _valid_email(email) and email not in seen:
            seen.add(email)
            out.append({"email": email, "source": cand["source"], "kind": cand["kind"]})
    return out


def _email_candidates(owner_name: str, firm: str, domain: str) -> list[str]:
    return [str(c.get("email") or "") for c in _guess_email_candidate_records(owner_name, firm, domain)]


def _default_head_fetcher(url: str) -> dict[str, Any]:
    try:
        resp = requests.head(url, timeout=10, allow_redirects=False, headers={"User-Agent": USER_AGENT})
        return {
            "status": int(resp.status_code or 0),
            "url": str(resp.url or url),
            "headers": dict(resp.headers or {}),
        }
    except Exception as exc:
        return {"status": 0, "url": url, "error": f"{type(exc).__name__}:{exc}", "headers": {}}


def _default_website_fetcher(url: str) -> dict[str, Any]:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT}, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            content = resp.read()
            charset = "utf-8"
            content_type = str(getattr(resp, "headers", {}).get("Content-Type", "") or "")
            if "charset=" in content_type:
                charset = content_type.split("charset=", 1)[1].split(";", 1)[0].strip() or "utf-8"
            html = content.decode(charset, errors="replace")
            return {"status": int(getattr(resp, "status", 200) or 200), "url": url, "html": html, "error": ""}
    except urllib.error.HTTPError as exc:
        body = ""
        try:
            body = exc.read().decode("utf-8", errors="replace")
        except Exception:
            body = ""
        return {"status": int(exc.code or 0), "url": url, "html": body, "error": f"HTTPError:{exc.code}"}
    except urllib.error.URLError as exc:
        reason = str(getattr(exc, "reason", exc) or exc)
        return {"status": 0, "url": url, "html": "", "error": f"URLError:{reason}"}
    except TimeoutError as exc:
        return {"status": 0, "url": url, "html": "", "error": f"TimeoutError:{exc}"}
    except Exception as exc:
        return {"status": 0, "url": url, "html": "", "error": f"{type(exc).__name__}:{exc}"}


def _cache_is_fresh(payload: dict[str, Any], now_utc: datetime | None = None) -> bool:
    fetched_raw = _normalize_text(payload.get("fetched_at_utc") or "")
    if not fetched_raw:
        return False
    try:
        fetched = datetime.fromisoformat(fetched_raw.replace("Z", "+00:00"))
    except Exception:
        return False
    if fetched.tzinfo is None:
        fetched = fetched.replace(tzinfo=timezone.utc)
    now = now_utc or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    return fetched >= (now.astimezone(timezone.utc) - timedelta(days=WEBSITE_EMAIL_CACHE_TTL_DAYS))


def _read_website_cache(path: Path, now_utc: datetime | None = None) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not _cache_is_fresh(payload, now_utc=now_utc):
        return None
    return payload


def _write_website_cache(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _website_cache_path(cache_dir: Path, domain: str) -> Path:
    safe = re.sub(r"[^a-z0-9._-]", "_", _normalize_text(domain).lower())
    return cache_dir / f"{safe}.json"


def _extract_site_candidates(html: str, domain: str) -> list[dict[str, str]]:
    text = str(html or "")
    out: list[dict[str, str]] = []
    seen: set[str] = set()

    for raw in re.findall(r"mailto:([^\"'\s>#]+)", text, flags=re.I):
        decoded = unquote(str(raw or "")).split("?", 1)[0].strip()
        email = _normalize_email(decoded)
        if not _valid_email(email):
            continue
        if not _email_matches_domain(email, domain):
            continue
        if email in seen:
            continue
        seen.add(email)
        out.append(_candidate_record(email, "website_mailto"))

    try:
        visible_blob = BeautifulSoup(text, "html.parser").get_text(" ", strip=True)
    except Exception:
        visible_blob = text

    for raw in re.findall(r"[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}", visible_blob, flags=re.I):
        email = _normalize_email(raw)
        if not _valid_email(email):
            continue
        if not _email_matches_domain(email, domain):
            continue
        if email in seen:
            continue
        seen.add(email)
        out.append(_candidate_record(email, "website_visible"))

    return out


def _candidate_priority(candidate: dict[str, str]) -> int:
    source = _normalize_text(candidate.get("source") or "")
    kind = _normalize_text(candidate.get("kind") or "")
    if source == "website_mailto" and kind == "person":
        return 10
    if source == "website_visible" and kind == "person":
        return 20
    if source == "website_mailto" and kind == "role_inbox":
        return 30
    if source == "website_visible" and kind == "role_inbox":
        return 40
    if source == "pattern_guess" and kind == "person":
        return 50
    if source == "pattern_guess" and kind == "role_inbox":
        return 60
    if source == "domain_guess" and kind == "person":
        return 70
    return 80


def _dedupe_and_rank_candidates(candidates: list[dict[str, str]]) -> list[dict[str, str]]:
    sorted_in = sorted(list(candidates or []), key=_candidate_priority)
    out: list[dict[str, str]] = []
    seen: set[str] = set()
    for cand in sorted_in:
        email = _normalize_email(cand.get("email") or "")
        source = _normalize_text(cand.get("source") or "")
        kind = _normalize_text(cand.get("kind") or "")
        if not _valid_email(email):
            continue
        if source not in {"website_mailto", "website_visible", "pattern_guess", "domain_guess"}:
            continue
        if kind not in {"person", "role_inbox"}:
            kind = "role_inbox" if _is_role_inbox_email(email) else "person"
        if email in seen:
            continue
        seen.add(email)
        out.append({"email": email, "source": source, "kind": kind})
    return out


def _choose_best_candidate(candidates: list[dict[str, str]], allow_role_inbox: bool) -> dict[str, str] | None:
    for cand in list(candidates or []):
        if _normalize_text(cand.get("kind") or "") == "role_inbox" and not bool(allow_role_inbox):
            continue
        return dict(cand)
    return None


def _contains_captcha(text: str) -> bool:
    lowered = str(text or "").lower()
    if "captcha" in lowered:
        return True
    if "cloudflare" in lowered and "attention required" in lowered:
        return True
    return False


def _crawl_domain_candidates(
    *,
    domain: str,
    website: str,
    sleep_ms: int,
    max_pages_per_site: int,
    website_fetcher,
) -> dict[str, Any]:
    host = _normalize_text(domain).lower() or _domain_from_url(website)
    if not host:
        return {
            "candidates": [],
            "attempted_urls": [],
            "reason": "no_contact_page",
            "http_status": 0,
            "blocked_403": False,
            "person_found": False,
            "role_found": False,
        }

    runtime: dict[str, Any] | None = None
    paths = list(WEBSITE_PATHS[: max(1, int(max_pages_per_site or 1))])
    attempted_urls: list[str] = []
    candidates: list[dict[str, str]] = []
    blocked_403 = False
    any_page_ok = False
    saw_timeout = False
    saw_captcha = False
    last_status = 0

    for idx, path in enumerate(paths):
        url = f"https://{host}{path}"
        attempted_urls.append(url)
        if idx > 0 and sleep_ms > 0:
            time.sleep(float(sleep_ms) / 1000.0)

        resp = website_fetcher(url)
        status = int(resp.get("status") or 0)
        html = str(resp.get("html") or "")
        error = _normalize_text(resp.get("error") or "")

        if status in {403, 429}:
            blocked_403 = True
            if runtime is None:
                runtime = scraper_engine.probe_crawl4ai_runtime()
            if runtime.get("crawl4ai_installed") and runtime.get("playwright_browsers_installed"):
                crawl_resp = scraper_engine.crawl_page(url, mode="browser", headless=True, sleep_ms=0)
                if bool(crawl_resp.get("ok")) and str(crawl_resp.get("html") or ""):
                    status = int(crawl_resp.get("status") or status or 200)
                    html = str(crawl_resp.get("html") or "")
                    error = _normalize_text(crawl_resp.get("error") or "")
                else:
                    last_status = status
                    continue
            else:
                last_status = status
                continue

        last_status = status
        if "timeout" in error.lower():
            saw_timeout = True

        if _contains_captcha(html):
            saw_captcha = True
            break

        if status == 200 and html:
            any_page_ok = True
            page_candidates = _extract_site_candidates(html, host)
            candidates.extend(page_candidates)
            if any((_normalize_text(c.get("kind") or "") == "person") for c in page_candidates):
                break

    ranked = _dedupe_and_rank_candidates(candidates)
    person_found = any((_normalize_text(c.get("kind") or "") == "person") for c in ranked)
    role_found = any((_normalize_text(c.get("kind") or "") == "role_inbox") for c in ranked)

    reason = ""
    if saw_captcha:
        reason = "captcha"
    elif blocked_403 and not ranked:
        reason = "403"
    elif saw_timeout and not ranked:
        reason = "timeout"
    elif not any_page_ok and not ranked:
        reason = "no_contact_page"
    elif not ranked:
        reason = "no_email_found"

    return {
        "candidates": ranked,
        "attempted_urls": attempted_urls,
        "reason": reason,
        "http_status": int(last_status or 0),
        "blocked_403": bool(blocked_403),
        "person_found": bool(person_found),
        "role_found": bool(role_found),
    }


def _current_month_key(now_utc: datetime | None = None) -> str:
    now = now_utc or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    return now.astimezone(timezone.utc).strftime("%Y-%m")


def _read_hunter_usage(path: Path, now_utc: datetime | None = None) -> dict[str, Any]:
    month = _current_month_key(now_utc)
    if not path.exists():
        return {"month": month, "calls": 0}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"month": month, "calls": 0}
    stored_month = _normalize_text(payload.get("month") or "")
    try:
        calls = max(0, int(payload.get("calls") or 0))
    except Exception:
        calls = 0
    if stored_month != month:
        return {"month": month, "calls": 0}
    return {"month": month, "calls": calls}


def _write_hunter_usage(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    safe = {
        "month": _normalize_text(payload.get("month") or ""),
        "calls": max(0, int(payload.get("calls") or 0)),
    }
    path.write_text(json.dumps(safe, indent=2) + "\n", encoding="utf-8")


def enrich_autogrow_rows(
    rows: list[dict[str, Any]],
    *,
    domain_enabled: bool,
    hunter_enabled: bool,
    hunter_api_key: str,
    sleep_ms: int,
    hunter_usage_path: Path,
    head_fetcher=None,
    now_utc: datetime | None = None,
    website_cache_dir: Path | None = None,
    max_sites_per_run: int = 25,
    max_pages_per_site: int = 5,
    allow_role_inbox: bool = False,
    allow_cache_write: bool = True,
    website_fetcher=None,
) -> dict[str, Any]:
    out_rows: list[dict[str, Any]] = [dict(r or {}) for r in list(rows or [])]
    metrics = {
        "attempted": 0,
        "domain_resolved": 0,
        "email_guessed": 0,
        "hunter_verified": 0,
        "still_no_email": 0,
        "hunter_skipped_cap": 0,
        "website_enrich_attempted": 0,
        "website_enrich_enriched": 0,
        "website_enrich_person_found": 0,
        "website_enrich_role_inbox_found": 0,
        "website_enrich_blocked_403": 0,
        "website_enrich_no_email": 0,
        "website_sites_crawled": 0,
    }
    diagnostics: list[dict[str, Any]] = []
    needs_review: list[dict[str, Any]] = []

    if not domain_enabled and not hunter_enabled:
        return {
            "rows": out_rows,
            "metrics": metrics,
            "diagnostics": diagnostics,
            "needs_review": needs_review,
        }

    head = head_fetcher or _default_head_fetcher
    get_page = website_fetcher or _default_website_fetcher
    domain_cache: dict[str, dict[str, Any]] = {}
    website_result_cache: dict[str, dict[str, Any]] = {}
    needs_review_domains: set[str] = set()
    head_calls = 0
    crawled_sites = 0

    hunter_usage = (
        _read_hunter_usage(hunter_usage_path, now_utc=now_utc)
        if hunter_enabled
        else {"month": _current_month_key(now_utc), "calls": 0}
    )
    hunter_key_present = bool(_normalize_text(hunter_api_key))

    if website_cache_dir is None:
        website_cache_dir = hunter_usage_path.parent / "website_email"

    for idx, row in enumerate(out_rows):
        firm = _normalize_text(row.get("firm") or row.get("company_name") or "")
        current_email = _normalize_email(row.get("email") or row.get("contact_email") or "")
        if not firm or _valid_email(current_email):
            continue

        metrics["attempted"] += 1
        diag: dict[str, Any] = {
            "row_index": idx,
            "firm": firm,
            "contact_name": _normalize_text(row.get("contact_name") or ""),
        }

        resolved_domain = _normalize_text(row.get("domain") or "").lower()
        website = _normalize_text(row.get("website") or "")
        if not resolved_domain and website:
            resolved_domain = _domain_from_url(website)

        domain_candidates = _candidate_domains_for_firm(firm)
        diag["domain_candidates"] = list(domain_candidates)

        if domain_enabled and not resolved_domain and domain_candidates:
            cache_key = _normalized_company_key(firm)
            cached = domain_cache.get(cache_key)
            if cached is None:
                cached = {"domain": "", "website": "", "status": 0}
                for candidate in domain_candidates:
                    url = f"https://{candidate}"
                    if head_calls > 0 and sleep_ms > 0:
                        time.sleep(float(sleep_ms) / 1000.0)
                    resp = head(url)
                    head_calls += 1
                    status = int(resp.get("status") or 0)
                    if status in RESOLVE_OK_STATUSES:
                        cached = {"domain": candidate, "website": url, "status": status}
                        break
                domain_cache[cache_key] = cached
            resolved_domain = _normalize_text(cached.get("domain") or "").lower()
            if resolved_domain:
                if not website:
                    website = _normalize_text(cached.get("website") or f"https://{resolved_domain}")
                metrics["domain_resolved"] += 1

        if resolved_domain:
            row["domain"] = resolved_domain
        if website:
            row["website"] = website
        diag["resolved_domain"] = resolved_domain

        website_candidates: list[dict[str, str]] = []
        source_value = _normalize_text(row.get("source") or "").upper()
        website_enrich_eligible = bool(
            domain_enabled
            and resolved_domain
            and not source_value.startswith("OSHA_TRAINERS")
            and not _valid_email(_normalize_email(row.get("email") or row.get("contact_email") or ""))
        )

        if website_enrich_eligible:
            metrics["website_enrich_attempted"] += 1
            domain_key = _normalize_text(resolved_domain).lower()
            website_result = website_result_cache.get(domain_key)

            if website_result is None:
                cache_path = _website_cache_path(website_cache_dir, domain_key)
                cached_payload = _read_website_cache(cache_path, now_utc=now_utc)
                if cached_payload is not None:
                    website_result = {
                        "candidates": list(cached_payload.get("candidates") or []),
                        "attempted_urls": list(cached_payload.get("attempted_urls") or []),
                        "reason": _normalize_text(cached_payload.get("reason") or ""),
                        "http_status": int(cached_payload.get("http_status") or 0),
                        "blocked_403": bool(cached_payload.get("blocked_403")),
                        "person_found": bool(cached_payload.get("person_found")),
                        "role_found": bool(cached_payload.get("role_found")),
                    }
                elif crawled_sites < max(0, int(max_sites_per_run or 0)):
                    website_result = _crawl_domain_candidates(
                        domain=domain_key,
                        website=website,
                        sleep_ms=max(0, int(sleep_ms or 0)),
                        max_pages_per_site=max(1, int(max_pages_per_site or 1)),
                        website_fetcher=get_page,
                    )
                    crawled_sites += 1
                    metrics["website_sites_crawled"] += 1
                    if allow_cache_write:
                        cache_payload = {
                            "domain": domain_key,
                            "fetched_at_utc": (now_utc or datetime.now(timezone.utc)).astimezone(timezone.utc).replace(microsecond=0).isoformat(),
                            "cache_ttl_days": WEBSITE_EMAIL_CACHE_TTL_DAYS,
                            "candidates": list(website_result.get("candidates") or []),
                            "attempted_urls": list(website_result.get("attempted_urls") or []),
                            "reason": _normalize_text(website_result.get("reason") or ""),
                            "http_status": int(website_result.get("http_status") or 0),
                            "blocked_403": bool(website_result.get("blocked_403")),
                            "person_found": bool(website_result.get("person_found")),
                            "role_found": bool(website_result.get("role_found")),
                        }
                        _write_website_cache(cache_path, cache_payload)
                else:
                    website_result = {
                        "candidates": [],
                        "attempted_urls": [],
                        "reason": "",
                        "http_status": 0,
                        "blocked_403": False,
                        "person_found": False,
                        "role_found": False,
                    }

                website_result_cache[domain_key] = dict(website_result)

            website_candidates = list(website_result.get("candidates") or [])
            if bool(website_result.get("blocked_403")):
                metrics["website_enrich_blocked_403"] += 1
            if bool(website_result.get("person_found")):
                metrics["website_enrich_person_found"] += 1
            if bool(website_result.get("role_found")):
                metrics["website_enrich_role_inbox_found"] += 1

            reason = _normalize_text(website_result.get("reason") or "")
            if reason in {"403", "captcha", "no_contact_page", "no_email_found", "timeout"} and domain_key not in needs_review_domains:
                needs_review_domains.add(domain_key)
                needs_review.append(
                    {
                        "domain": domain_key,
                        "website": website or f"https://{domain_key}",
                        "reason": reason,
                        "http_status": int(website_result.get("http_status") or 0),
                        "attempted_urls": "|".join(list(website_result.get("attempted_urls") or [])),
                    }
                )

            if reason == "no_email_found":
                metrics["website_enrich_no_email"] += 1

            diag["website_reason"] = reason
            diag["website_attempted_urls"] = list(website_result.get("attempted_urls") or [])
            diag["website_http_status"] = int(website_result.get("http_status") or 0)

        if resolved_domain and not _valid_email(_normalize_email(row.get("email") or row.get("contact_email") or "")):
            owner_name = _normalize_text(row.get("contact_name") or "")
            guess_candidates = _guess_email_candidate_records(owner_name, firm, resolved_domain)
            all_candidates = _dedupe_and_rank_candidates(website_candidates + guess_candidates)
            row["email_candidates_json"] = json.dumps(all_candidates, separators=(",", ":"))
            diag["email_candidates"] = list(all_candidates)

            best = _choose_best_candidate(all_candidates, allow_role_inbox=bool(allow_role_inbox))
            if best is not None:
                best_email = _normalize_email(best.get("email") or "")
                row["email"] = best_email
                row["contact_email"] = best_email
                row["email_source"] = _normalize_text(best.get("source") or "")
                row["email_kind"] = _normalize_text(best.get("kind") or "")
                row["email_status"] = _normalize_text(row.get("email_status") or "pattern_generated")
                if row["email_source"] in {"pattern_guess", "domain_guess"}:
                    metrics["email_guessed"] += 1
                if row["email_source"] in {"website_mailto", "website_visible"}:
                    metrics["website_enrich_enriched"] += 1
            else:
                row["email_candidates_json"] = json.dumps(all_candidates, separators=(",", ":"))

        eligible_for_hunter = bool(hunter_enabled and hunter_key_present and resolved_domain)
        if eligible_for_hunter:
            if int(hunter_usage.get("calls") or 0) >= HUNTER_FREE_MONTHLY_CAP:
                metrics["hunter_skipped_cap"] += 1
                diag["hunter_status"] = "skipped_cap"
            else:
                # Stub-only first pass: keep counter plumbing but do not call live Hunter API yet.
                diag["hunter_status"] = "stub_not_implemented"
                diag["hunter_domain"] = resolved_domain

        final_email = _normalize_email(row.get("email") or row.get("contact_email") or "")
        if not _valid_email(final_email):
            metrics["still_no_email"] += 1
            if "email_candidates" not in diag:
                diag["email_candidates"] = []
        diagnostics.append(diag)

    if hunter_enabled:
        _write_hunter_usage(hunter_usage_path, hunter_usage)

    return {
        "rows": out_rows,
        "metrics": metrics,
        "diagnostics": diagnostics,
        "needs_review": needs_review,
    }
