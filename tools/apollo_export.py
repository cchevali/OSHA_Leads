from __future__ import annotations

import argparse
import csv
import os
import random
import re
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

APOLLO_HOME_URL = "https://app.apollo.io/"

ERR_APOLLO_EXPORT_PLAYWRIGHT_MISSING = "ERR_APOLLO_EXPORT_PLAYWRIGHT_MISSING"
ERR_APOLLO_EXPORT_NO_PROFILE = "ERR_APOLLO_EXPORT_NO_PROFILE"
ERR_APOLLO_EXPORT_SESSION_EXPIRED = "ERR_APOLLO_EXPORT_SESSION_EXPIRED"
ERR_APOLLO_EXPORT_TABLE_TIMEOUT = "ERR_APOLLO_EXPORT_TABLE_TIMEOUT"
ERR_APOLLO_EXPORT_DOWNLOAD_TIMEOUT = "ERR_APOLLO_EXPORT_DOWNLOAD_TIMEOUT"
ERR_APOLLO_EXPORT_NO_SEARCH_URL = "ERR_APOLLO_EXPORT_NO_SEARCH_URL"
ERR_APOLLO_EXPORT_SELECTOR_MISSING = "ERR_APOLLO_EXPORT_SELECTOR_MISSING"
WARN_APOLLO_EXPORT_NO_RESULTS = "WARN_APOLLO_EXPORT_NO_RESULTS"
WARN_APOLLO_EXPORT_DATA_DIR_UNSET = "WARN_APOLLO_EXPORT_DATA_DIR_UNSET"

SELECTORS: dict[str, dict[str, list[Any]]] = {
    "results_table": {
        "roles": [
            {"role": "table", "name": "People"},
            {"role": "grid", "name": "People"},
            {"role": "table", "name": "Contacts"},
            {"role": "grid", "name": "Contacts"},
        ],
        "texts": [
            "People",
            "Contacts",
        ],
        "css": [
            "table",
            "[role='grid']",
            "[data-testid*='table']",
        ],
    },
    "results_rows": {
        "css": [
            "table tbody tr",
            "[role='row']",
            "[data-testid*='table'] tbody tr",
        ],
    },
    "select_all_checkbox": {
        "roles": [
            {"role": "checkbox", "name": "Select all"},
            {"role": "checkbox", "name": "Select All"},
        ],
        "texts": [
            "Select all",
        ],
        "css": [
            "thead input[type='checkbox']",
            "[role='columnheader'] input[type='checkbox']",
            "[aria-label*='Select all']",
        ],
    },
    "export_button": {
        "roles": [
            {"role": "button", "name": "Export"},
        ],
        "texts": [
            "Export",
        ],
        "css": [
            "button:has-text('Export')",
            "[role='button']:has-text('Export')",
        ],
    },
    "export_modal_verified_option": {
        "roles": [
            {"role": "radio", "name": "Verified emails only"},
            {"role": "checkbox", "name": "Verified emails only"},
            {"role": "radio", "name": "Verified Emails Only"},
            {"role": "checkbox", "name": "Verified Emails Only"},
        ],
        "texts": [
            "Verified emails only",
            "Verified Emails Only",
        ],
        "css": [
            "[role='radio']:has-text('Verified emails')",
            "[role='checkbox']:has-text('Verified emails')",
            "label:has-text('Verified emails')",
            "text=Verified emails only",
        ],
    },
    "export_confirm_button": {
        "roles": [
            {"role": "button", "name": "Export all selected"},
            {"role": "button", "name": "Export Selected"},
            {"role": "button", "name": "Export"},
        ],
        "texts": [
            "Export all selected",
            "Export Selected",
            "Export",
        ],
        "css": [
            "button:has-text('Export all selected')",
            "button:has-text('Export Selected')",
            "[role='button']:has-text('Export')",
        ],
    },
    "next_page_button": {
        "roles": [
            {"role": "button", "name": "Next"},
            {"role": "link", "name": "Next"},
            {"role": "button", "name": "Next page"},
            {"role": "link", "name": "Next page"},
        ],
        "texts": [
            "Next",
            "Next page",
        ],
        "css": [
            "button[aria-label*='Next']",
            "a[aria-label*='Next']",
            "button:has-text('Next')",
            "a:has-text('Next')",
        ],
    },
    "login_indicators": {
        "roles": [
            {"role": "button", "name": "Log in"},
            {"role": "button", "name": "Sign in"},
        ],
        "texts": [
            "Sign in",
            "Log in",
            "Continue with Google",
        ],
        "css": [
            "input[type='email']",
            "input[type='password']",
            "form[action*='login']",
        ],
    },
}


@dataclass
class ExportConfig:
    repo_root: Path
    data_dir: Path
    profile_path: Path
    inbox_path: Path
    screenshot_path: Path
    search_url: str
    max_pages: int
    delay_min: float
    delay_max: float
    browser_channel: str
    headless: bool
    dry_run: bool
    profile_setup: bool
    print_config: bool
    data_dir_from_env: bool


@dataclass
class ExportStats:
    pages_processed: int = 0
    files_downloaded: int = 0
    total_rows: int = 0
    output_files: list[str] = field(default_factory=list)
    status: str = "ERROR"


class ExportError(Exception):
    def __init__(self, token: str, detail: str):
        super().__init__(f"{token} {detail}".strip())
        self.token = token
        self.detail = detail


def _emit_kv(key: str, value: Any) -> None:
    print(f"{key}={value}")


def _emit_error(token: str, detail: str) -> None:
    suffix = str(detail or "").strip()
    if suffix:
        print(f"{token} {suffix}")
        return
    print(token)


def _playwright_install_help() -> str:
    return "install='pip install playwright' setup='py -3 -m playwright install chromium'"


def _resolve_data_dir(env: dict[str, str]) -> tuple[Path, bool]:
    raw = str(env.get("DATA_DIR") or "").strip()
    if raw:
        candidate = Path(raw)
        if not candidate.is_absolute():
            candidate = REPO_ROOT / candidate
        return candidate, True
    return REPO_ROOT / "out", False


def _resolve_config(args: argparse.Namespace, env: dict[str, str] | None = None) -> ExportConfig:
    use_env = dict(env or os.environ)
    data_dir, from_env = _resolve_data_dir(use_env)
    return ExportConfig(
        repo_root=REPO_ROOT,
        data_dir=data_dir,
        profile_path=data_dir / "apollo_export" / "browser_profile",
        inbox_path=data_dir / "prospect_generation" / "inbox",
        screenshot_path=data_dir / "apollo_export" / "dry_run_screenshot.png",
        search_url=str(args.search_url or "").strip(),
        max_pages=int(args.max_pages),
        delay_min=float(args.delay_min),
        delay_max=float(args.delay_max),
        browser_channel=str(args.chrome_channel or "chrome").strip() or "chrome",
        headless=bool(args.headless),
        dry_run=bool(args.dry_run),
        profile_setup=bool(args.profile_setup),
        print_config=bool(args.print_config),
        data_dir_from_env=from_env,
    )


def _check_playwright_runtime() -> tuple[bool, str]:
    try:
        from playwright.sync_api import sync_playwright
    except Exception as exc:
        return False, f"playwright_import_failed err={type(exc).__name__}:{exc}"

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            browser.close()
        return True, ""
    except Exception as exc:
        return False, f"chromium_launch_failed err={type(exc).__name__}:{exc}"


def _sleep_human(cfg: ExportConfig) -> None:
    delay = random.uniform(float(cfg.delay_min), float(cfg.delay_max))
    if delay <= 0:
        return
    time.sleep(delay)


def _find_first_locator(page: Any, selector_key: str) -> Any | None:
    block = dict(SELECTORS.get(selector_key) or {})

    for role_spec in list(block.get("roles") or []):
        try:
            role = str(role_spec.get("role") or "").strip()
            if not role:
                continue
            name_text = str(role_spec.get("name") or "").strip()
            if name_text:
                locator = page.get_by_role(role, name=re.compile(name_text, re.I))
            else:
                locator = page.get_by_role(role)
            if locator.count() > 0:
                return locator.first
        except Exception:
            continue

    for text in list(block.get("texts") or []):
        try:
            locator = page.get_by_text(re.compile(str(text), re.I))
            if locator.count() > 0:
                return locator.first
        except Exception:
            continue

    for css in list(block.get("css") or []):
        try:
            locator = page.locator(str(css))
            if locator.count() > 0:
                return locator.first
        except Exception:
            continue

    return None


def _is_login_page(page: Any) -> bool:
    try:
        url = str(page.url or "").lower()
    except Exception:
        url = ""
    for part in ["/login", "/signin", "/auth"]:
        if part in url:
            return True
    return _find_first_locator(page, "login_indicators") is not None


def _wait_for_results_table(page: Any, timeout_ms: int = 30000) -> bool:
    deadline = time.time() + (float(timeout_ms) / 1000.0)
    while time.time() < deadline:
        locator = _find_first_locator(page, "results_table")
        if locator is not None:
            try:
                if locator.is_visible(timeout=500):
                    return True
            except Exception:
                try:
                    if locator.count() > 0:
                        return True
                except Exception:
                    pass
        time.sleep(0.5)
    return False


def _count_visible_results(page: Any) -> int:
    for css in list((SELECTORS.get("results_rows") or {}).get("css") or []):
        try:
            locator = page.locator(str(css))
            count = int(locator.count())
            if count <= 0:
                continue
            if str(css).strip() == "[role='row']":
                return max(0, count - 1)
            return max(0, count)
        except Exception:
            continue
    return 0


def _build_output_filename(ts: datetime | None = None) -> str:
    stamp = (ts or datetime.now()).strftime("%Y%m%d_%H%M%S")
    return f"apollo_export_{stamp}.csv"


def _count_csv_rows(path: Path) -> int:
    try:
        with open(path, "r", newline="", encoding="utf-8-sig") as f:
            reader = csv.reader(f)
            count = sum(1 for _ in reader)
        return max(0, count - 1)
    except Exception:
        return 0


def _save_download_to_inbox(download: Any, inbox_path: Path, ts: datetime | None = None) -> tuple[Path, int]:
    inbox_path.mkdir(parents=True, exist_ok=True)
    base = _build_output_filename(ts=ts)
    target = inbox_path / base
    stem = target.stem
    suffix = target.suffix
    idx = 1
    while target.exists():
        target = inbox_path / f"{stem}_{idx}{suffix}"
        idx += 1
    download.save_as(str(target))
    return target, _count_csv_rows(target)


def _emit_live_block(stats: ExportStats, cfg: ExportConfig) -> None:
    _emit_kv("APOLLO_EXPORT_PROFILE_PATH", cfg.profile_path.resolve())
    _emit_kv("APOLLO_EXPORT_INBOX_PATH", cfg.inbox_path.resolve())
    _emit_kv("APOLLO_EXPORT_SEARCH_URL", cfg.search_url or "none")
    _emit_kv("APOLLO_EXPORT_PAGES_PROCESSED", int(stats.pages_processed))
    _emit_kv("APOLLO_EXPORT_FILES_DOWNLOADED", int(stats.files_downloaded))
    _emit_kv("APOLLO_EXPORT_TOTAL_ROWS", int(stats.total_rows))
    _emit_kv("APOLLO_EXPORT_OUTPUT_FILES", ",".join(stats.output_files) if stats.output_files else "none")
    _emit_kv("APOLLO_EXPORT_COMPLETE", f"status={stats.status}")


def _require_locator(page: Any, selector_key: str, token: str, detail: str) -> Any:
    locator = _find_first_locator(page, selector_key)
    if locator is None:
        raise ExportError(token, detail)
    return locator


def _locator_enabled(locator: Any) -> bool:
    try:
        if locator.is_disabled():
            return False
    except Exception:
        pass
    try:
        aria = str(locator.get_attribute("aria-disabled") or "").strip().lower()
        if aria == "true":
            return False
    except Exception:
        pass
    return True


def _ensure_verified_option_selected(page: Any) -> None:
    locator = _require_locator(
        page,
        "export_modal_verified_option",
        ERR_APOLLO_EXPORT_SELECTOR_MISSING,
        "selector=export_modal_verified_option",
    )
    checked = False
    try:
        aria_checked = str(locator.get_attribute("aria-checked") or "").strip().lower()
        if aria_checked == "true":
            checked = True
    except Exception:
        pass
    if not checked:
        try:
            checked = bool(locator.is_checked())
        except Exception:
            checked = False
    if checked:
        return
    locator.click(timeout=10000)


def _click_action(page: Any, cfg: ExportConfig, selector_key: str, detail: str) -> Any:
    locator = _require_locator(page, selector_key, ERR_APOLLO_EXPORT_SELECTOR_MISSING, detail)
    locator.click(timeout=10000)
    _sleep_human(cfg)
    return locator


def _run_profile_setup(cfg: ExportConfig) -> int:
    ok, detail = _check_playwright_runtime()
    if not ok:
        _emit_error(ERR_APOLLO_EXPORT_PLAYWRIGHT_MISSING, f"{detail} {_playwright_install_help()}")
        return 1

    from playwright.sync_api import sync_playwright

    cfg.profile_path.mkdir(parents=True, exist_ok=True)
    context = None
    try:
        with sync_playwright() as p:
            context = p.chromium.launch_persistent_context(
                str(cfg.profile_path),
                channel=cfg.browser_channel,
                headless=False,
                accept_downloads=True,
            )
            page = context.pages[0] if context.pages else context.new_page()
            page.goto(APOLLO_HOME_URL, wait_until="domcontentloaded", timeout=30000)
            _emit_kv("APOLLO_EXPORT_PROFILE_SETUP", "OPEN")
            _emit_kv("APOLLO_EXPORT_PROFILE_PATH", cfg.profile_path.resolve())
            print("APOLLO_EXPORT_PROFILE_SETUP_ACTION login_to_apollo_then_close_browser_window")

            while True:
                try:
                    if len(context.pages) == 0:
                        break
                except Exception:
                    break
                time.sleep(1.0)
    finally:
        try:
            if context is not None:
                context.close()
        except Exception:
            pass

    _emit_kv("APOLLO_EXPORT_PROFILE_SETUP_COMPLETE", "OK")
    return 0


def _validate_export_mode_inputs(cfg: ExportConfig) -> None:
    if not cfg.search_url:
        raise ExportError(ERR_APOLLO_EXPORT_NO_SEARCH_URL, "remediation=provide_--search-url")
    if not cfg.profile_path.exists() or not cfg.profile_path.is_dir():
        raise ExportError(
            ERR_APOLLO_EXPORT_NO_PROFILE,
            "remediation=run_--profile-setup profile_path=" + str(cfg.profile_path.resolve()),
        )


def _run_dry_run(page: Any, cfg: ExportConfig) -> tuple[ExportStats, str | None]:
    if _is_login_page(page):
        raise ExportError(ERR_APOLLO_EXPORT_SESSION_EXPIRED, "remediation=run_--profile-setup")
    if not _wait_for_results_table(page, timeout_ms=30000):
        raise ExportError(ERR_APOLLO_EXPORT_TABLE_TIMEOUT, "selector=results_table timeout_ms=30000")
    visible_rows = _count_visible_results(page)
    cfg.screenshot_path.parent.mkdir(parents=True, exist_ok=True)
    page.screenshot(path=str(cfg.screenshot_path), full_page=True)

    stats = ExportStats(
        pages_processed=1,
        files_downloaded=0,
        total_rows=max(0, visible_rows),
        output_files=[],
        status="DRY_RUN",
    )
    if visible_rows <= 0:
        return stats, WARN_APOLLO_EXPORT_NO_RESULTS
    return stats, None


def _run_live(page: Any, cfg: ExportConfig) -> tuple[ExportStats, str | None]:
    if _is_login_page(page):
        raise ExportError(ERR_APOLLO_EXPORT_SESSION_EXPIRED, "remediation=run_--profile-setup")
    if not _wait_for_results_table(page, timeout_ms=30000):
        raise ExportError(ERR_APOLLO_EXPORT_TABLE_TIMEOUT, "selector=results_table timeout_ms=30000")

    stats = ExportStats(status="OK")
    for _ in range(int(cfg.max_pages)):
        if _is_login_page(page):
            raise ExportError(ERR_APOLLO_EXPORT_SESSION_EXPIRED, "remediation=run_--profile-setup")
        if not _wait_for_results_table(page, timeout_ms=30000):
            raise ExportError(ERR_APOLLO_EXPORT_TABLE_TIMEOUT, "selector=results_table timeout_ms=30000")

        visible_rows = _count_visible_results(page)
        if visible_rows <= 0:
            if stats.pages_processed == 0:
                return stats, WARN_APOLLO_EXPORT_NO_RESULTS
            break

        stats.pages_processed += 1

        _click_action(page, cfg, "select_all_checkbox", "selector=select_all_checkbox")
        _click_action(page, cfg, "export_button", "selector=export_button")
        _ensure_verified_option_selected(page)

        confirm = _require_locator(
            page,
            "export_confirm_button",
            ERR_APOLLO_EXPORT_SELECTOR_MISSING,
            "selector=export_confirm_button",
        )
        try:
            with page.expect_download(timeout=60000) as dl_info:
                confirm.click(timeout=10000)
            download = dl_info.value
        except Exception as exc:
            raise ExportError(ERR_APOLLO_EXPORT_DOWNLOAD_TIMEOUT, f"timeout_ms=60000 err={type(exc).__name__}:{exc}") from exc

        _sleep_human(cfg)
        saved_path, rows = _save_download_to_inbox(download, cfg.inbox_path, ts=datetime.now())
        stats.files_downloaded += 1
        stats.total_rows += int(rows)
        stats.output_files.append(saved_path.name)

        if stats.pages_processed >= int(cfg.max_pages):
            break

        next_locator = _find_first_locator(page, "next_page_button")
        if next_locator is None:
            break
        if not _locator_enabled(next_locator):
            break

        next_locator.click(timeout=10000)
        _sleep_human(cfg)
        try:
            page.wait_for_load_state("domcontentloaded", timeout=30000)
        except Exception:
            pass

    return stats, None


def _run_export_mode(cfg: ExportConfig) -> tuple[ExportStats, str | None]:
    _validate_export_mode_inputs(cfg)

    ok, detail = _check_playwright_runtime()
    if not ok:
        raise ExportError(ERR_APOLLO_EXPORT_PLAYWRIGHT_MISSING, f"{detail} {_playwright_install_help()}")

    from playwright.sync_api import sync_playwright

    context = None
    try:
        with sync_playwright() as p:
            context = p.chromium.launch_persistent_context(
                str(cfg.profile_path),
                channel=cfg.browser_channel,
                headless=bool(cfg.headless),
                accept_downloads=True,
            )
            page = context.pages[0] if context.pages else context.new_page()
            page.goto(cfg.search_url, wait_until="domcontentloaded", timeout=30000)
            _sleep_human(cfg)
            if cfg.dry_run:
                return _run_dry_run(page, cfg)
            return _run_live(page, cfg)
    finally:
        try:
            if context is not None:
                context.close()
        except Exception:
            pass


def _print_config(cfg: ExportConfig) -> int:
    ok, detail = _check_playwright_runtime()
    _emit_kv("apollo_export_profile_path", cfg.profile_path.resolve())
    _emit_kv("apollo_export_inbox_path", cfg.inbox_path.resolve())
    _emit_kv("apollo_export_playwright_installed", "YES" if ok else "NO")
    _emit_kv("apollo_export_profile_exists", "YES" if cfg.profile_path.exists() else "NO")
    _emit_kv("apollo_export_search_url", cfg.search_url or "none")
    _emit_kv("apollo_export_max_pages", int(cfg.max_pages))
    _emit_kv("apollo_export_delay_min", float(cfg.delay_min))
    _emit_kv("apollo_export_delay_max", float(cfg.delay_max))
    _emit_kv("browser_channel", cfg.browser_channel)
    _emit_kv("apollo_export_headless", "YES" if cfg.headless else "NO")
    if not cfg.data_dir_from_env:
        print(f"{WARN_APOLLO_EXPORT_DATA_DIR_UNSET} data_dir_default={cfg.data_dir.resolve()}")
    if not ok:
        _emit_kv("apollo_export_playwright_error", detail or "unknown")
    return 0


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="Automate Apollo saved-search CSV export into generator inbox.")
    ap.add_argument("--profile-setup", action="store_true", help="Launch persistent profile browser for manual Apollo login.")
    ap.add_argument("--print-config", action="store_true", help="Print resolved config and exit.")
    ap.add_argument("--dry-run", action="store_true", help="Navigate and preview only; no exports.")
    ap.add_argument("--search-url", default="", help="Apollo saved-search URL.")
    ap.add_argument("--max-pages", type=int, default=1, help="Max result pages to export (default: 1).")
    ap.add_argument("--delay-min", type=float, default=2.0, help="Minimum random delay between actions in seconds.")
    ap.add_argument("--delay-max", type=float, default=5.0, help="Maximum random delay between actions in seconds.")
    ap.add_argument("--chrome-channel", default="chrome", help="Browser channel for Playwright persistent context (default: chrome).")
    ap.add_argument("--headless", action="store_true", help="Run Chromium headless (default: visible).")
    return ap


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if int(args.max_pages) < 1:
        _emit_error(ERR_APOLLO_EXPORT_NO_SEARCH_URL, "invalid_max_pages")
        return 1
    if float(args.delay_min) < 0:
        _emit_error(ERR_APOLLO_EXPORT_NO_SEARCH_URL, "invalid_delay_min")
        return 1
    if float(args.delay_max) < float(args.delay_min):
        _emit_error(ERR_APOLLO_EXPORT_NO_SEARCH_URL, "invalid_delay_range")
        return 1

    cfg = _resolve_config(args)

    if cfg.print_config:
        return _print_config(cfg)
    if cfg.profile_setup:
        return _run_profile_setup(cfg)

    stats = ExportStats(status="ERROR")
    try:
        stats, warn_token = _run_export_mode(cfg)
        if warn_token:
            print(warn_token)
        _emit_live_block(stats, cfg)
        return 0
    except ExportError as exc:
        _emit_error(exc.token, exc.detail)
        stats.status = "ERROR"
        _emit_live_block(stats, cfg)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
