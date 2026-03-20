import re
from typing import Any
from urllib.parse import urljoin, urlparse

import requests_warning_compat
import requests
from bs4 import BeautifulSoup

from outreach import contact_normalization
import seed_recipients_pools as pools


BASE_URL = "https://www.thomasnet.com/"
USER_AGENT = "OSHA_Leads/1.0 (+https://microflowops.com)"
EMAIL_RE = re.compile(r"[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}", flags=re.I)
CONSULTANCY_TOKENS = (
    "consult",
    "safety",
    "osha",
    "compliance",
    "industrial hygiene",
    "occupational health",
    "training",
    "ehs",
    "hse",
)
QUALIFICATION_THRESHOLDS = {
    "unique_consultancy_relevant_firms": 15,
    "website_link_rate": 0.80,
    "public_site_email_yield": 0.25,
    "minimum_state_count": 2,
    "minimum_states_meeting_target": 2,
    "crm_overlap_rate": 0.50,
}


def _normalize_text(value: Any) -> str:
    return " ".join(str(value or "").replace("\r", " ").replace("\n", " ").split()).strip()


def _normalize_email(value: str) -> str:
    return contact_normalization.normalize_email(value)


def _valid_email(value: str) -> bool:
    return contact_normalization.valid_email(value)


def _normalize_website(value: str) -> str:
    return contact_normalization.normalize_website(value)


def _email_domain(value: str) -> str:
    return contact_normalization.email_domain(value)


def _root_domain_from_website(value: str) -> str:
    website = _normalize_website(value)
    if not website:
        return ""
    try:
        parsed = urlparse(website if "://" in website else f"https://{website}")
    except Exception:
        return ""
    host = _normalize_text(parsed.netloc or parsed.path).lower()
    if host.startswith("www."):
        host = host[4:]
    return host


def _firm_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]", "", _normalize_text(value).lower())


def _is_nonfree_email(value: str) -> bool:
    email = _normalize_email(value)
    if not _valid_email(email):
        return False
    return _email_domain(email) not in pools.FREE_EMAIL_DOMAINS


def _is_consultancy_relevant(blob: str) -> bool:
    text = _normalize_text(blob).lower()
    return any(token in text for token in CONSULTANCY_TOKENS)


def parse_thomasnet_result_page(page_html: str, *, page_url: str) -> tuple[list[dict[str, str]], str]:
    soup = BeautifulSoup(page_html or "", "html.parser")
    rows: list[dict[str, str]] = []
    selectors = [".search-result", ".result", ".company-card", "li.result"]
    nodes: list[Any] = []
    seen_nodes: set[int] = set()
    for selector in selectors:
        for node in soup.select(selector):
            marker = id(node)
            if marker in seen_nodes:
                continue
            seen_nodes.add(marker)
            nodes.append(node)
    for node in nodes:
        blob = _normalize_text(node.get_text(" ", strip=True))
        if not blob:
            continue
        name_node = node.select_one("h2, h3, .company-name, .result-title")
        firm = _normalize_text(name_node.get_text(" ", strip=True) if name_node is not None else "")
        if not firm:
            continue
        profile_url = ""
        website = ""
        for link in node.select("a[href]"):
            href = _normalize_text(link.get("href") or "")
            if not href:
                continue
            absolute = urljoin(BASE_URL, href)
            if "thomasnet.com" in absolute.lower() and not profile_url:
                profile_url = absolute
            elif "thomasnet.com" not in absolute.lower() and not website:
                website = _normalize_website(absolute)
        rows.append(
            {
                "firm": firm,
                "profile_url": profile_url,
                "website": website,
                "category_blob": blob,
                "page_url": page_url,
            }
        )
    return rows, ("THOMASNET_RESULTS" if rows else "FAILED")


def parse_thomasnet_profile_page(page_html: str, *, profile_url: str) -> tuple[dict[str, str], str]:
    soup = BeautifulSoup(page_html or "", "html.parser")
    blob = _normalize_text(soup.get_text(" ", strip=True))
    website = ""
    email = ""
    for link in soup.select("a[href]"):
        href = _normalize_text(link.get("href") or "")
        if not href:
            continue
        if href.lower().startswith("mailto:"):
            candidate = _normalize_email(href.split(":", 1)[1])
            if not email and _is_nonfree_email(candidate):
                email = candidate
            continue
        absolute = urljoin(profile_url, href)
        if "thomasnet.com" not in absolute.lower() and not website:
            website = _normalize_website(absolute)
    return {
        "website": website,
        "email": email,
        "profile_blob": blob,
    }, ("THOMASNET_PROFILE" if blob else "FAILED")


def evaluate_thomasnet_qualification(
    *,
    rows: list[dict[str, Any]],
    crm_domains: set[str] | None = None,
    crm_firm_keys: set[str] | None = None,
    suppressed_emails: set[str] | None = None,
) -> dict[str, Any]:
    crm_domain_set = {str(item or "").strip().lower() for item in set(crm_domains or set()) if str(item or "").strip()}
    crm_firm_key_set = {str(item or "").strip().lower() for item in set(crm_firm_keys or set()) if str(item or "").strip()}
    suppressed_email_set = {
        _normalize_email(str(item or "")) for item in set(suppressed_emails or set()) if _normalize_email(str(item or ""))
    }

    relevant_rows = [dict(row or {}) for row in list(rows or []) if _is_consultancy_relevant(str((row or {}).get("blob") or (row or {}).get("category_blob") or (row or {}).get("firm") or ""))]
    unique_firm_keys = {_firm_key(str(row.get("firm") or "")) for row in relevant_rows if _firm_key(str(row.get("firm") or ""))}
    unique_consultancy_relevant_firms = len(unique_firm_keys)

    website_firm_keys = {
        _firm_key(str(row.get("firm") or ""))
        for row in relevant_rows
        if _firm_key(str(row.get("firm") or "")) and _root_domain_from_website(str(row.get("website") or ""))
    }
    website_link_rate = (
        float(len(website_firm_keys)) / float(unique_consultancy_relevant_firms)
        if unique_consultancy_relevant_firms > 0
        else 0.0
    )

    website_backed_contactable_firms = {
        _firm_key(str(row.get("firm") or ""))
        for row in relevant_rows
        if _firm_key(str(row.get("firm") or ""))
        and _root_domain_from_website(str(row.get("website") or ""))
        and _is_nonfree_email(str(row.get("email") or ""))
    }
    public_site_email_yield = (
        float(len(website_backed_contactable_firms)) / float(len(website_firm_keys))
        if website_firm_keys
        else 0.0
    )

    overlap_firms: set[str] = set()
    net_new_by_state: dict[str, int] = {"TX": 0, "CA": 0, "FL": 0}
    seen_contactable_keys: set[str] = set()
    for row in relevant_rows:
        firm = _firm_key(str(row.get("firm") or ""))
        state = _normalize_text(row.get("state") or "").upper()
        website_domain = _root_domain_from_website(str(row.get("website") or ""))
        email = _normalize_email(str(row.get("email") or ""))
        overlap = (firm and firm in crm_firm_key_set) or (website_domain and website_domain in crm_domain_set)
        if overlap and firm:
            overlap_firms.add(firm)
        if state not in net_new_by_state:
            continue
        if overlap:
            continue
        if not website_domain or not _is_nonfree_email(email) or email in suppressed_email_set:
            continue
        dedupe_key = "|".join([state, firm or website_domain, website_domain, email])
        if dedupe_key in seen_contactable_keys:
            continue
        seen_contactable_keys.add(dedupe_key)
        net_new_by_state[state] += 1

    crm_overlap_rate = (
        float(len(overlap_firms)) / float(unique_consultancy_relevant_firms)
        if unique_consultancy_relevant_firms > 0
        else 0.0
    )
    states_meeting_target = sum(
        1 for state in ("TX", "CA", "FL") if int(net_new_by_state.get(state) or 0) >= QUALIFICATION_THRESHOLDS["minimum_state_count"]
    )

    qualified = (
        unique_consultancy_relevant_firms >= QUALIFICATION_THRESHOLDS["unique_consultancy_relevant_firms"]
        and website_link_rate >= QUALIFICATION_THRESHOLDS["website_link_rate"]
        and public_site_email_yield >= QUALIFICATION_THRESHOLDS["public_site_email_yield"]
        and states_meeting_target >= QUALIFICATION_THRESHOLDS["minimum_states_meeting_target"]
        and crm_overlap_rate < QUALIFICATION_THRESHOLDS["crm_overlap_rate"]
    )
    return {
        "qualified": bool(qualified),
        "unique_consultancy_relevant_firms": int(unique_consultancy_relevant_firms),
        "website_link_rate": round(website_link_rate, 4),
        "public_site_email_yield": round(public_site_email_yield, 4),
        "state_contactable_rows": {state: int(net_new_by_state.get(state) or 0) for state in ("TX", "CA", "FL")},
        "states_meeting_target": int(states_meeting_target),
        "crm_overlap_rate": round(crm_overlap_rate, 4),
        "thresholds": dict(QUALIFICATION_THRESHOLDS),
    }


def doctor_probe_thomasnet(fetcher=None) -> dict[str, Any]:
    url = urljoin(BASE_URL, "products/safety-consulting-services-20093834-1.html")
    request = fetcher or (lambda target: requests.get(target, timeout=20, headers={"User-Agent": USER_AGENT}))
    try:
        response = request(url)
        status = int(getattr(response, "status_code", 0) or 0)
        text = str(getattr(response, "text", "") or "")
        final_url = str(getattr(response, "url", url) or url)
    except Exception as exc:
        return {"ok": False, "status": 0, "url": url, "error": f"{type(exc).__name__}:{exc}"}
    blocked = status in {401, 403, 429} or "access denied" in text.lower() or "captcha" in text.lower()
    return {
        "ok": status == 200 and not blocked,
        "status": status,
        "url": final_url,
        "blocked": bool(blocked),
        "error": ("public_access_blocked" if blocked else ("" if status == 200 else f"http_status_{status}")),
    }
