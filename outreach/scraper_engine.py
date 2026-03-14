import asyncio
import io
import os
import re
import time
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup


WARN_CRAWL4AI_NOT_INSTALLED = "WARN_CRAWL4AI_NOT_INSTALLED"
WARN_PLAYWRIGHT_BROWSERS_MISSING = "WARN_PLAYWRIGHT_BROWSERS_MISSING"

_EMAIL_RE = re.compile(r"[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}", flags=re.I)
_PHONE_RE = re.compile(r"(?:\+1[\s\-.]?)?(?:\(?\d{3}\)?[\s\-.]?)\d{3}[\s\-.]?\d{4}")


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


def _domain_from_website(url: str) -> str:
    raw = str(url or "").strip()
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


def _lazy_import_crawl4ai() -> tuple[Any | None, str]:
    try:
        import importlib

        module = importlib.import_module("crawl4ai")
        return module, ""
    except Exception as exc:
        return None, f"{type(exc).__name__}:{exc}"


def _probe_playwright_browser_launch() -> tuple[bool, str]:
    try:
        from playwright.sync_api import sync_playwright
    except Exception as exc:
        return False, f"playwright_import_failed:{type(exc).__name__}:{exc}"

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            browser.close()
        return True, ""
    except Exception as exc:
        return False, f"chromium_launch_failed:{type(exc).__name__}:{exc}"


def _quiet_browser_logs_enabled() -> bool:
    raw = _normalize_text(os.getenv("SCRAPER_ENGINE_QUIET_BROWSER_LOGS", "1")).lower()
    return raw not in {"0", "false", "no", "off"}


def probe_crawl4ai_runtime() -> dict[str, Any]:
    module, import_error = _lazy_import_crawl4ai()
    if module is None:
        return {
            "crawl4ai_installed": False,
            "playwright_browsers_installed": False,
            "error_reason": import_error or "crawl4ai_import_failed",
            "warn_token": WARN_CRAWL4AI_NOT_INSTALLED,
        }
    browsers_ok, browser_error = _probe_playwright_browser_launch()
    warn_token = "" if browsers_ok else WARN_PLAYWRIGHT_BROWSERS_MISSING
    return {
        "crawl4ai_installed": True,
        "playwright_browsers_installed": bool(browsers_ok),
        "error_reason": browser_error,
        "warn_token": warn_token,
    }


def probe_source_availability(source_key: str) -> dict[str, Any]:
    token = _normalize_text(source_key).upper()
    if token == "BCSP":
        from outreach import prospect_sources_bcsp

        probe = prospect_sources_bcsp.doctor_probe_bcsp()
        return {
            "source": token,
            "available": bool(probe.get("ok")),
            "reason": str(probe.get("reason") or ("state_search_ok" if probe.get("ok") else "unknown")),
            "warn_token": "",
            "status": int(probe.get("status") or 0),
            "url": str(probe.get("url") or ""),
            "parse_mode": str(probe.get("parse_mode") or ""),
            "rows_found": int(probe.get("rows_found") or 0),
            "error": str(probe.get("error") or ""),
        }
    if token == "OSHA_NEWS":
        runtime = probe_crawl4ai_runtime()
        if not runtime.get("crawl4ai_installed"):
            return {
                "source": token,
                "available": False,
                "reason": "crawl4ai_not_installed",
                "warn_token": WARN_CRAWL4AI_NOT_INSTALLED,
                "runtime": runtime,
            }
        if not runtime.get("playwright_browsers_installed"):
            return {
                "source": token,
                "available": False,
                "reason": "playwright_browsers_missing",
                "warn_token": WARN_PLAYWRIGHT_BROWSERS_MISSING,
                "runtime": runtime,
            }
        return {"source": token, "available": True, "reason": "ok", "warn_token": "", "runtime": runtime}
    if token == "STATE_LIC":
        return {"source": token, "available": True, "reason": "http_api", "warn_token": ""}
    return {"source": token, "available": True, "reason": "unknown_or_not_required", "warn_token": ""}


async def _crawl_with_crawl4ai_async(url: str, headless: bool = True) -> dict[str, Any]:
    module, import_error = _lazy_import_crawl4ai()
    if module is None:
        return {"ok": False, "url": url, "status": 0, "html": "", "error": import_error}

    try:
        AsyncWebCrawler = getattr(module, "AsyncWebCrawler")
    except Exception as exc:
        return {"ok": False, "url": url, "status": 0, "html": "", "error": f"missing_AsyncWebCrawler:{exc}"}

    browser_cfg = None
    run_cfg = None
    try:
        BrowserConfig = getattr(module, "BrowserConfig", None)
        CrawlerRunConfig = getattr(module, "CrawlerRunConfig", None)
        if BrowserConfig is not None:
            browser_cfg = BrowserConfig(headless=headless, browser_type="chromium")
        if CrawlerRunConfig is not None:
            run_cfg = CrawlerRunConfig()
    except Exception:
        browser_cfg = None
        run_cfg = None

    try:
        if browser_cfg is not None:
            crawler = AsyncWebCrawler(config=browser_cfg)
        else:
            crawler = AsyncWebCrawler()
        async with crawler:
            kwargs = {"url": url}
            if run_cfg is not None:
                kwargs["config"] = run_cfg
            try:
                result = await crawler.arun(**kwargs)
            except TypeError:
                kwargs.pop("config", None)
                result = await crawler.arun(**kwargs)
        html = str(getattr(result, "html", "") or getattr(result, "cleaned_html", "") or "")
        markdown = str(getattr(result, "markdown", "") or "")
        status = int(getattr(result, "status_code", 200) or 200)
        ok = bool(getattr(result, "success", True)) and bool(html or markdown)
        return {"ok": ok, "url": url, "status": status, "html": html, "markdown": markdown, "result": result}
    except Exception as exc:
        return {"ok": False, "url": url, "status": 0, "html": "", "error": f"{type(exc).__name__}:{exc}"}


def crawl_page(
    url: str,
    *,
    mode: str = "browser",
    headless: bool = True,
    sleep_ms: int = 0,
    fetcher=None,
) -> dict[str, Any]:
    if sleep_ms > 0:
        time.sleep(float(sleep_ms) / 1000.0)

    if fetcher is not None:
        fetched = fetcher(url)
        if isinstance(fetched, dict):
            return dict(fetched)
        if isinstance(fetched, tuple) and len(fetched) >= 2:
            status = int(fetched[0] or 0)
            html = str(fetched[1] or "")
            return {"ok": status == 200, "url": url, "status": status, "html": html}
        return {"ok": False, "url": url, "status": 0, "html": "", "error": "invalid_fetcher_response"}

    availability = probe_crawl4ai_runtime()
    if not availability.get("crawl4ai_installed"):
        return {
            "ok": False,
            "url": url,
            "status": 0,
            "html": "",
            "error": str(availability.get("error_reason") or "crawl4ai_not_installed"),
            "warn_token": WARN_CRAWL4AI_NOT_INSTALLED,
        }
    if not availability.get("playwright_browsers_installed"):
        return {
            "ok": False,
            "url": url,
            "status": 0,
            "html": "",
            "error": str(availability.get("error_reason") or "playwright_browsers_missing"),
            "warn_token": WARN_PLAYWRIGHT_BROWSERS_MISSING,
        }

    quiet_logs = _quiet_browser_logs_enabled()
    try:
        if quiet_logs:
            with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
                result = asyncio.run(_crawl_with_crawl4ai_async(url, headless=headless))
        else:
            result = asyncio.run(_crawl_with_crawl4ai_async(url, headless=headless))
    except RuntimeError:
        loop = asyncio.new_event_loop()
        try:
            if quiet_logs:
                with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
                    result = loop.run_until_complete(_crawl_with_crawl4ai_async(url, headless=headless))
            else:
                result = loop.run_until_complete(_crawl_with_crawl4ai_async(url, headless=headless))
        finally:
            loop.close()
    if result.get("ok"):
        return result

    # Fallback for static pages when Crawl4AI runtime is present but crawl failed.
    if mode == "light":
        try:
            resp = requests.get(url, timeout=20, headers={"User-Agent": "OSHA_Leads/1.0 (+https://microflowops.com)"})
            return {"ok": int(resp.status_code) == 200, "url": url, "status": int(resp.status_code), "html": str(resp.text or "")}
        except Exception as exc:
            return {
                "ok": False,
                "url": url,
                "status": 0,
                "html": "",
                "error": f"light_fetch_failed:{type(exc).__name__}:{exc}",
                "warn_token": str(result.get("warn_token") or ""),
            }
    return result


def crawl_page_with_storage_state(
    url: str,
    *,
    storage_state_path: str,
    headless: bool = True,
    sleep_ms: int = 0,
) -> dict[str, Any]:
    if sleep_ms > 0:
        time.sleep(float(sleep_ms) / 1000.0)
    state_file = Path(str(storage_state_path or "")).expanduser()
    if not str(state_file):
        return {"ok": False, "url": url, "status": 0, "html": "", "error": "missing_storage_state_path"}
    if not state_file.exists():
        return {"ok": False, "url": url, "status": 0, "html": "", "error": "missing_storage_state_file"}
    try:
        from playwright.sync_api import sync_playwright
    except Exception as exc:
        return {"ok": False, "url": url, "status": 0, "html": "", "error": f"playwright_import_failed:{type(exc).__name__}:{exc}"}

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=headless)
            context = browser.new_context(storage_state=str(state_file))
            page = context.new_page()
            response = page.goto(url, wait_until="domcontentloaded", timeout=45000)
            try:
                page.wait_for_load_state("networkidle", timeout=45000)
            except Exception:
                pass
            html = str(page.content() or "")
            status = int(response.status) if response is not None else 0
            final_url = str(page.url or url)
            title = str(page.title() or "")
            context.close()
            browser.close()
        return {
            "ok": bool(html),
            "url": url,
            "final_url": final_url,
            "status": status,
            "html": html,
            "title": title,
        }
    except Exception as exc:
        return {"ok": False, "url": url, "status": 0, "html": "", "error": f"playwright_fetch_failed:{type(exc).__name__}:{exc}"}


def extract_contacts_regex(text_or_html: str) -> dict[str, list[str]]:
    text = str(text_or_html or "")
    soup = BeautifulSoup(text, "html.parser")
    blob = " ".join([text, soup.get_text(" ", strip=True)])
    emails = []
    seen_emails: set[str] = set()
    for item in _EMAIL_RE.findall(blob):
        email = _normalize_email(item)
        if _valid_email(email) and email not in seen_emails:
            seen_emails.add(email)
            emails.append(email)
    phones = []
    seen_phones: set[str] = set()
    for item in _PHONE_RE.findall(blob):
        normalized = _normalize_text(item)
        if normalized and normalized not in seen_phones:
            seen_phones.add(normalized)
            phones.append(normalized)
    return {"emails": emails, "phones": phones}


def extract_structured_css(html: str, selectors: dict[str, str]) -> dict[str, Any]:
    soup = BeautifulSoup(html or "", "html.parser")
    rows: list[dict[str, str]] = []
    row_selector = selectors.get("rows") or ""
    if not row_selector:
        return {"rows": rows, "mode": "NO_ROWS_SELECTOR"}
    for node in soup.select(row_selector):
        row: dict[str, str] = {}
        for field, selector in selectors.items():
            if field == "rows":
                continue
            target = node.select_one(selector)
            row[field] = _normalize_text(target.get_text(" ", strip=True) if target is not None else "")
        rows.append(row)
    return {"rows": rows, "mode": "CSS"}


def extract_llm_optional(text: str, instruction: str = "") -> dict[str, Any]:
    enabled = str(os.getenv("PROSPECT_AUTOGROW_LLM_ENABLED", "0") or "").strip().lower() in {"1", "true", "yes", "on"}
    if not enabled:
        return {"enabled": False, "mode": "DISABLED", "rows": [], "instruction": instruction}
    # Intentionally no default LLM integration; keep zero-cost default.
    return {"enabled": True, "mode": "NOT_CONFIGURED", "rows": [], "instruction": instruction}


def fetch_contact_pages_for_domain(domain: str, *, sleep_ms: int = 0, fetcher=None) -> list[dict[str, Any]]:
    host = _domain_from_website(domain)
    if not host:
        return []
    base = f"https://{host}"
    paths = ["", "/contact", "/contact-us", "/about", "/about-us"]
    out: list[dict[str, Any]] = []
    seen_urls: set[str] = set()
    for idx, path in enumerate(paths):
        url = f"{base}{path}"
        if url in seen_urls:
            continue
        seen_urls.add(url)
        page = crawl_page(url, mode="light", sleep_ms=(sleep_ms if idx > 0 else 0), fetcher=fetcher)
        contacts = extract_contacts_regex(str(page.get("html") or ""))
        out.append({"url": url, "page": page, "contacts": contacts})
    return out


def _email_patterns_for_name(first_name: str, last_name: str, domain: str) -> list[str]:
    first = re.sub(r"[^a-z0-9]", "", (first_name or "").lower())
    last = re.sub(r"[^a-z0-9]", "", (last_name or "").lower())
    dom = (domain or "").strip().lower()
    if not first or not dom:
        return []
    patterns = [f"{first}@{dom}"]
    if last:
        patterns.append(f"{first}.{last}@{dom}")
        patterns.append(f"{first[:1]}{last}@{dom}")
    out: list[str] = []
    seen: set[str] = set()
    for email in patterns:
        if _valid_email(email) and email not in seen:
            seen.add(email)
            out.append(email)
    return out


def apply_email_resolution_waterfall(rows: list[dict[str, Any]], *, sleep_ms: int = 0, fetcher=None) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row_in in list(rows or []):
        row = dict(row_in or {})
        email = _normalize_email(row.get("email") or row.get("contact_email") or "")
        if _valid_email(email):
            row["email"] = email
            row["contact_email"] = email
            row["email_status"] = _normalize_text(row.get("email_status") or "scraped_from_source")
            out.append(row)
            continue

        website = _normalize_text(row.get("website") or "")
        domain = _normalize_text(row.get("domain") or "").lower() or _domain_from_website(website)
        if domain:
            row["domain"] = domain

        if website and domain:
            for item in fetch_contact_pages_for_domain(domain, sleep_ms=sleep_ms, fetcher=fetcher):
                emails = list((item.get("contacts") or {}).get("emails") or [])
                if emails:
                    site_email = _normalize_email(emails[0])
                    if _valid_email(site_email):
                        row["email"] = site_email
                        row["contact_email"] = site_email
                        row["email_status"] = "scraped_from_site"
                        break

        if not _valid_email(str(row.get("email") or row.get("contact_email") or "")) and domain:
            first_name = _normalize_text(row.get("first_name") or "")
            last_name = _normalize_text(row.get("last_name") or "")
            if not first_name and not last_name:
                contact_name = _normalize_text(row.get("contact_name") or "")
                parts = [p for p in contact_name.split(" ") if p]
                if parts:
                    first_name = parts[0]
                if len(parts) > 1:
                    last_name = parts[-1]
            candidates = _email_patterns_for_name(first_name, last_name, domain)
            if candidates:
                row["email"] = candidates[0]
                row["contact_email"] = candidates[0]
                row["email_status"] = "pattern_generated"

        if not _valid_email(str(row.get("email") or row.get("contact_email") or "")):
            row["email_status"] = _normalize_text(row.get("email_status") or "pending")
        out.append(row)
    return out
