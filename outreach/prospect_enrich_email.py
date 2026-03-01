import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import requests


USER_AGENT = "OSHA_Leads/1.0 (+https://microflowops.com)"
HUNTER_FREE_MONTHLY_CAP = 25
RESOLVE_OK_STATUSES = {200, 301, 302}
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


def _email_candidates(owner_name: str, firm: str, domain: str) -> list[str]:
    dom = _normalize_text(domain).lower()
    if not dom:
        return []
    first, last = _name_parts(owner_name)
    owner_is_company = _normalized_company_key(owner_name) and (_normalized_company_key(owner_name) == _normalized_company_key(firm))
    ordered: list[str] = []
    if owner_is_company:
        ordered.append(f"info@{dom}")
    if first:
        ordered.append(f"{first}@{dom}")
        if last:
            ordered.append(f"{first}.{last}@{dom}")
            ordered.append(f"{first[:1]}{last}@{dom}")
    ordered.append(f"info@{dom}")
    out: list[str] = []
    seen: set[str] = set()
    for email in ordered:
        e = _normalize_email(email)
        if _valid_email(e) and e not in seen:
            seen.add(e)
            out.append(e)
    return out


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
) -> dict[str, Any]:
    out_rows: list[dict[str, Any]] = [dict(r or {}) for r in list(rows or [])]
    metrics = {
        "attempted": 0,
        "domain_resolved": 0,
        "email_guessed": 0,
        "hunter_verified": 0,
        "still_no_email": 0,
        "hunter_skipped_cap": 0,
    }
    diagnostics: list[dict[str, Any]] = []

    if not domain_enabled and not hunter_enabled:
        return {"rows": out_rows, "metrics": metrics, "diagnostics": diagnostics}

    head = head_fetcher or _default_head_fetcher
    domain_cache: dict[str, dict[str, Any]] = {}
    head_calls = 0
    hunter_usage = _read_hunter_usage(hunter_usage_path, now_utc=now_utc) if hunter_enabled else {"month": _current_month_key(now_utc), "calls": 0}
    hunter_key_present = bool(_normalize_text(hunter_api_key))

    for idx, row in enumerate(out_rows):
        firm = _normalize_text(row.get("firm") or row.get("company_name") or "")
        current_email = _normalize_email(row.get("email") or row.get("contact_email") or "")
        if not firm or _valid_email(current_email):
            continue

        metrics["attempted"] += 1
        diag: dict[str, Any] = {"row_index": idx, "firm": firm, "contact_name": _normalize_text(row.get("contact_name") or "")}

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

        if resolved_domain and not _valid_email(_normalize_email(row.get("email") or row.get("contact_email") or "")):
            owner_name = _normalize_text(row.get("contact_name") or "")
            candidates = _email_candidates(owner_name, firm, resolved_domain)
            diag["email_candidates"] = list(candidates)
            if candidates:
                best = candidates[0]
                row["email"] = best
                row["contact_email"] = best
                row["email_status"] = _normalize_text(row.get("email_status") or "pattern_generated")
                metrics["email_guessed"] += 1

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
    return {"rows": out_rows, "metrics": metrics, "diagnostics": diagnostics}
