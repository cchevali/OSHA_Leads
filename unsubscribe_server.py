import argparse
import html
import json
import os
import subprocess
import time
import csv
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
import re
from urllib.parse import urlparse, parse_qs, urlencode

# Load environment variables from .env if available
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from unsubscribe_utils import (
    add_to_suppression,
    is_suppressed_email,
    lookup_email_for_token,
    lookup_token_record,
    sign_check,
    sign_registration,
    sign_stats,
    set_include_lows_pref,
    store_unsub_token,
    verify_unsub_token,
)

RATE_LIMIT_WINDOW_S = 60
RATE_LIMIT_MAX_REQ = 120
_rate_state = {}

_TERRITORY_INDEX = None

# Lightweight per-subscriber_key throttle for rendering the low-priority preview (preference write still succeeds).
_PREFS_PREVIEW_RATE_STATE: dict[str, float] = {}
_PREFS_PREVIEW_RATE_DEFAULT_S = 10


def _territory_display_name(code: str, territory: dict) -> str:
    display = (territory.get("display_name") or territory.get("name") or "").strip()
    if display:
        return display
    description = (territory.get("description") or "").strip()
    if description:
        for token in [" OSHA", " area offices"]:
            if token in description:
                return description.split(token, 1)[0].strip()
        return description
    return code


def _load_territory_index() -> dict:
    """
    Build a lookup of acceptable territory identifiers -> canonical territory_code.
    Accepts both codes (e.g. TX_TRIANGLE_V1) and display names (e.g. Texas Triangle).
    """
    global _TERRITORY_INDEX
    if _TERRITORY_INDEX is not None:
        return _TERRITORY_INDEX

    index: dict[str, str] = {}
    try:
        root = Path(__file__).resolve().parent
        terr_path = root / "territories.json"
        territories = json.loads(terr_path.read_text(encoding="utf-8"))
        if isinstance(territories, dict):
            for code, terr in territories.items():
                if not isinstance(code, str) or not isinstance(terr, dict):
                    continue
                code_norm = code.strip().upper()
                if code_norm:
                    index[code_norm] = code_norm
                display = _territory_display_name(code_norm, terr)
                disp_norm = display.strip().upper()
                if disp_norm:
                    index[disp_norm] = code_norm
    except Exception:
        index = {}

    _TERRITORY_INDEX = index
    return index


def _resolve_territory(raw: str | None) -> tuple[str | None, str | None]:
    """
    Resolve raw territory input to (territory_code, display_name).
    Returns (None, None) if invalid/unknown.
    """
    text = (raw or "").strip()
    if not text:
        return None, None
    key = text.upper()
    idx = _load_territory_index()
    code = idx.get(key)
    if not code:
        return None, None
    # Re-compute a human display name from territories.json if possible.
    try:
        root = Path(__file__).resolve().parent
        terr = json.loads((root / "territories.json").read_text(encoding="utf-8")).get(code, {})
        if isinstance(terr, dict):
            return code, _territory_display_name(code, terr)
    except Exception:
        pass
    return code, code


def _client_ip(handler: BaseHTTPRequestHandler) -> str:
    # Prefer X-Forwarded-For from Caddy; fall back to socket peer IP.
    xff = (handler.headers.get("X-Forwarded-For") or "").strip()
    if xff:
        return xff.split(",")[0].strip()
    return (handler.client_address[0] or "").strip()


def _rate_limited(ip: str) -> bool:
    now = time.time()
    window_start, count = _rate_state.get(ip, (now, 0))
    if now - window_start > RATE_LIMIT_WINDOW_S:
        _rate_state[ip] = (now, 1)
        return False
    count += 1
    _rate_state[ip] = (window_start, count)
    return count > RATE_LIMIT_MAX_REQ


def _prefs_preview_rate_limited(subscriber_key: str) -> tuple[bool, int]:
    """
    Best-effort throttle to reduce abuse of the preview renderer.

    Returns: (is_limited, retry_after_seconds)
    """
    try:
        window_s = int(os.getenv("PREFS_PREVIEW_RATE_LIMIT_S", str(_PREFS_PREVIEW_RATE_DEFAULT_S)).strip() or "0")
    except Exception:
        window_s = _PREFS_PREVIEW_RATE_DEFAULT_S
    if window_s <= 0:
        return False, 0

    key = (subscriber_key or "").strip().lower()
    if not key:
        return False, 0

    now = time.time()
    last = float(_PREFS_PREVIEW_RATE_STATE.get(key, 0.0) or 0.0)
    if last and (now - last) < float(window_s):
        remaining = int(float(window_s) - (now - last) + 0.999)
        return True, max(1, remaining)

    _PREFS_PREVIEW_RATE_STATE[key] = now

    # Best-effort cleanup to avoid unbounded growth.
    if len(_PREFS_PREVIEW_RATE_STATE) > 5000:
        cutoff = now - float(max(window_s, 60) * 10)
        for k, ts in list(_PREFS_PREVIEW_RATE_STATE.items()):
            if float(ts or 0.0) < cutoff:
                _PREFS_PREVIEW_RATE_STATE.pop(k, None)

    return False, 0


def _resolve_preview_db_path() -> Path | None:
    env_path = (os.getenv("OSHA_DB_PATH") or os.getenv("DB_PATH") or "").strip()
    if env_path:
        p = Path(env_path).expanduser()
        return p if p.exists() else None

    # Default to the repo's conventional path when running co-located with the pipeline.
    root = Path(__file__).resolve().parent
    p = root / "data" / "osha.sqlite"
    return p if p.exists() else None


def _load_recent_low_priority_preview(
    territory_code: str, days: int = 14, limit: int = 20
) -> tuple[list[dict], int, str | None, Exception | None]:
    """
    Return (rows, error_message, exception). Rows are low-priority only and territory-scoped.
    Shared logic: uses the same query + tier threshold used by the digest renderer.
    """
    db_path = _resolve_preview_db_path()
    if not db_path:
        return [], 0, "db_missing", None

    try:
        from lead_filters import load_territory_definitions
    except Exception as e:
        return [], 0, "territory_defs_import_error", e

    root = Path(__file__).resolve().parent
    try:
        defs = load_territory_definitions(str(root / "territories.json"))
        terr = defs.get(territory_code)
        if not isinstance(terr, dict):
            return [], 0, "unknown_territory", None
        states = [str(s or "").strip().upper() for s in (terr.get("states") or []) if str(s or "").strip()]
        if not states:
            return [], 0, "territory_no_states", None
    except Exception as e:
        return [], 0, "territory_defs_load_error", e

    try:
        import sqlite3
        from send_digest_email import TIER_THRESHOLDS, get_leads_for_period
    except Exception as e:
        return [], 0, "digest_logic_import_error", e

    try:
        conn = sqlite3.connect(str(db_path))
    except Exception as e:
        return [], 0, "db_open_error", e

    try:
        all_rows, _, _ = get_leads_for_period(
            conn=conn,
            states=states,
            since_days=int(days),
            new_only_days=int(days),
            skip_first_seen_filter=True,
            territory_code=territory_code,
            content_filter="all",
            include_low_fallback=False,
            window_start=None,
            new_only_cutoff=None,
            include_changed=False,
            use_opened_window=True,
        )
    except Exception as e:
        try:
            conn.close()
        except Exception:
            pass
        return [], 0, "query_error", e

    try:
        conn.close()
    except Exception:
        pass

    try:
        medium_min = int(TIER_THRESHOLDS.get("medium_min", 6))
    except Exception:
        medium_min = 6

    low_only = [row for row in (all_rows or []) if int(row.get("lead_score") or 0) < medium_min]
    low_only.sort(
        key=lambda lead: str((lead.get("last_seen_at") or lead.get("first_seen_at") or lead.get("date_opened") or "")),
        reverse=True,
    )
    total = len(low_only)
    limit = max(0, min(50, int(limit)))
    return low_only[:limit], total, None, None


def _log_preview_unavailable(
    subscriber_key: str,
    territory_code: str,
    reason: str,
    exc: Exception | None = None,
) -> None:
    payload = {
        "event": "PREVIEW_UNAVAILABLE",
        "subscriber_key": (subscriber_key or "").strip().lower()[:120],
        "territory_code": (territory_code or "").strip().upper()[:80],
        "reason": (reason or "unknown").strip()[:200],
        "exception_class": (exc.__class__.__name__ if exc else "None"),
    }
    try:
        print(f"[WARN] {json.dumps(payload, separators=(',',':'))}")
    except Exception:
        # Best-effort: never break the handler on logging failures.
        pass


def _recent_low_priority_preview_html(
    subscriber_key: str,
    territory_code: str,
    title: str = "Recent low signals",
    limit: int = 5,
) -> str:
    safe_title = html.escape(title)

    def _empty_state(message: str) -> str:
        return (
            "<div style=\"margin: 10px 0 0 0; padding: 10px 12px; border-radius: 10px; "
            "background: #f9fafb; border: 1px solid #e5e7eb; color: #374151;\">"
            f"{html.escape(message)}"
            "</div>"
        )

    try:
        out = [
            "<div style=\"margin-top: 18px; padding-top: 12px; border-top: 1px solid #e5e7eb;\">",
            f"<h2 style=\"margin: 0 0 8px 0; font-size: 16px; color: #111827;\">{safe_title}</h2>",
            "<p style=\"margin: 0 0 10px 0; color: #374151; font-size: 13px;\">"
            "Business-only fields. Last 14 days for this territory.</p>",
        ]

        limited, retry_s = _prefs_preview_rate_limited(subscriber_key)
        if limited:
            out.append(
                "<div style=\"margin: 10px 0 0 0; padding: 10px 12px; border-radius: 10px; "
                "background: #fffbeb; border: 1px solid #fde68a; color: #92400e;\">"
                f"Preview is temporarily throttled. Please retry in {int(retry_s)} seconds."
                "</div>"
            )
            out.append("</div>")
            return "".join(out)

        rows, total, err, exc = _load_recent_low_priority_preview(territory_code=territory_code, days=14, limit=limit)
        if err:
            _log_preview_unavailable(subscriber_key, territory_code, reason=err, exc=exc)
            out.append(_empty_state("Preview unavailable right now. Low-priority rows will appear starting the next scheduled digest."))
            out.append("</div>")
            return "".join(out)

        if int(total or 0) <= 0:
            out.append(_empty_state("0 low signals in the last 14 days for this territory."))
            out.append("</div>")
            return "".join(out)

        show_n = len(rows or [])
        out.append(
            "<div style=\"margin: 10px 0 10px 0; color: #374151; font-size: 13px;\">"
            f"<strong>{int(total)}</strong> low signals found (showing up to {int(show_n)} most recent).</div>"
        )

        def _cell(text: str) -> str:
            return html.escape((text or "").strip())

        def _observed_date(lead: dict) -> str:
            ts = str(lead.get("last_seen_at") or lead.get("first_seen_at") or "").strip()
            if ts and len(ts) >= 10:
                return ts[:10]
            opened = str(lead.get("date_opened") or "").strip()
            return opened[:10] if opened else "-"

        def _inspection_id(lead: dict) -> str:
            val = str(lead.get("activity_nr") or lead.get("lead_id") or "").strip()
            return val

        out.append(
            "<div style=\"overflow-x:auto; border: 1px solid #e5e7eb; border-radius: 10px;\">"
            "<table style=\"width:100%; border-collapse: collapse; font-size: 13px;\">"
            "<thead><tr style=\"background:#f3f4f6;\">"
            "<th style=\"text-align:left; padding: 10px; border-bottom: 1px solid #e5e7eb;\">Observed</th>"
            "<th style=\"text-align:left; padding: 10px; border-bottom: 1px solid #e5e7eb;\">Establishment</th>"
            "<th style=\"text-align:left; padding: 10px; border-bottom: 1px solid #e5e7eb;\">Location</th>"
            "<th style=\"text-align:left; padding: 10px; border-bottom: 1px solid #e5e7eb;\">Inspection</th>"
            "<th style=\"text-align:left; padding: 10px; border-bottom: 1px solid #e5e7eb;\">Label</th>"
            "<th style=\"text-align:left; padding: 10px; border-bottom: 1px solid #e5e7eb;\">Source</th>"
            "</tr></thead><tbody>"
        )
        for lead in rows:
            observed = _cell(_observed_date(lead))
            name = _cell(str(lead.get("establishment_name") or ""))
            city = _cell(str(lead.get("site_city") or ""))
            state = _cell(str(lead.get("site_state") or ""))
            loc = _cell(", ".join([p for p in [city, state] if p]))
            insp = _cell(_inspection_id(lead))
            label = _cell(str(lead.get("inspection_type") or ""))
            url = str(lead.get("source_url") or "").strip()
            href = html.escape(url, quote=True)
            src = (
                f"<a href=\"{href}\" target=\"_blank\" rel=\"noopener noreferrer\" style=\"color:#0b5fff;\">OSHA</a>"
                if (url.startswith("http://") or url.startswith("https://"))
                else ""
            )
            out.append(
                "<tr>"
                f"<td style=\"padding: 10px; border-bottom: 1px solid #f3f4f6; white-space: nowrap;\">{observed}</td>"
                f"<td style=\"padding: 10px; border-bottom: 1px solid #f3f4f6;\">{name}</td>"
                f"<td style=\"padding: 10px; border-bottom: 1px solid #f3f4f6;\">{loc}</td>"
                f"<td style=\"padding: 10px; border-bottom: 1px solid #f3f4f6; white-space: nowrap;\">{insp}</td>"
                f"<td style=\"padding: 10px; border-bottom: 1px solid #f3f4f6;\">{label}</td>"
                f"<td style=\"padding: 10px; border-bottom: 1px solid #f3f4f6;\">{src}</td>"
                "</tr>"
            )
        out.append("</tbody></table></div></div>")
        return "".join(out)
    except Exception as e:
        _log_preview_unavailable(subscriber_key, territory_code, reason="exception", exc=e)
        return (
            "<div style=\"margin-top: 18px; padding-top: 12px; border-top: 1px solid #e5e7eb;\">"
            f"<h2 style=\"margin: 0 0 8px 0; font-size: 16px; color: #111827;\">{safe_title}</h2>"
            + _empty_state("Preview unavailable right now. Low-priority rows will appear starting the next scheduled digest.")
            + "</div>"
        )


_RE_TERRITORY_CODE = re.compile(r"^[A-Z0-9_]{2,64}$")
# Subscriber keys come from email links; allow a conservative set and cap length to avoid abuse.
# Allowed: 1-80 chars from [A-Za-z0-9_.-] plus underscore.
_RE_SUBSCRIBER_KEY = re.compile(r"^[A-Za-z0-9_.-]{1,80}$")


def _normalize_territory_code(value: str | None) -> str:
    return (value or "").strip().upper()


def _normalize_subscriber_key(value: str | None) -> str:
    # Canonicalize to lowercase for storage/lookup; diagnostics should show the raw value.
    return (value or "").strip().lower()


def _valid_territory_code(value: str) -> bool:
    return bool(value) and bool(_RE_TERRITORY_CODE.match(value))


def _valid_subscriber_key(value: str) -> bool:
    return bool(value) and bool(_RE_SUBSCRIBER_KEY.match(value))


def _latest_lows_enabled_pref(subscriber_key: str, territory_code: str) -> tuple[bool, str | None, Exception | None]:
    """
    Best-effort include_lows for (subscriber_key, territory_code) based on prefs.csv.

    Digest preference is subscriber-scoped, so we intentionally ignore email here and use the newest row.
    Returns (include_lows, error_reason, exception).
    """
    try:
        import unsubscribe_utils as _uu
    except Exception as e:
        return False, "prefs_import_error", e

    sk = _normalize_subscriber_key(subscriber_key)
    terr = _normalize_territory_code(territory_code)
    if not sk or not terr:
        return False, None, None

    path = getattr(_uu, "PREFS_PATH", None)
    if not path or not Path(path).exists():
        return False, None, None

    include_lows = False
    best_ts = ""
    try:
        with open(Path(path), "r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if not row:
                    continue
                row_sk = _normalize_subscriber_key(row.get("subscriber_key"))
                row_terr = _normalize_territory_code(row.get("territory"))
                if row_sk != sk or row_terr != terr:
                    continue
                ts = (row.get("updated_at") or "").strip()
                if ts and best_ts and ts <= best_ts:
                    continue
                best_ts = ts or best_ts
                val = str(row.get("include_lows") or "").strip().lower()
                include_lows = val in {"1", "true", "yes"}
        return bool(include_lows), None, None
    except Exception as e:
        return False, "prefs_read_error", e


def _resolve_git_sha() -> str:
    """
    Best-effort git sha for observability.
    Prefer env var, fall back to `git rev-parse` when available, else "unknown".
    """
    env_sha = (os.getenv("MFO_UNSUB_SHA") or os.getenv("GIT_SHA") or "").strip()
    if env_sha:
        return env_sha
    try:
        root = Path(__file__).resolve().parent
        out = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=str(root), timeout=1)
        sha = out.decode("utf-8", errors="replace").strip()
        if sha:
            return sha
    except Exception:
        pass
    return "unknown"


_GIT_SHA = _resolve_git_sha()


class UnsubHandler(BaseHTTPRequestHandler):
    server_version = "UnsubServer/1.0"

    def end_headers(self) -> None:
        self.send_header("X-MFO-Unsub-SHA", _GIT_SHA)
        super().end_headers()

    def do_GET(self):
        ip = _client_ip(self)
        if _rate_limited(ip):
            self.send_response(HTTPStatus.TOO_MANY_REQUESTS)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.end_headers()
            self.wfile.write(b"Too many requests.\n")
            return

        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/")

        params = parse_qs(parsed.query)

        if path == "/__version":
            payload = json.dumps({"git_sha": _GIT_SHA}).encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return

        if path == "/api/prefs":
            expected_key = (os.getenv("MFO_INTERNAL_API_KEY") or "").strip()
            provided_key = (self.headers.get("X-MFO-API-Key") or "").strip()
            if not expected_key or not provided_key or (provided_key != expected_key):
                self.send_response(HTTPStatus.UNAUTHORIZED)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.end_headers()
                self.wfile.write(b"{\"error\":\"unauthorized\"}\n")
                return

            raw_subscriber_key = (params.get("subscriber_key") or [""])[0]
            raw_territory_code = (params.get("territory_code") or [""])[0]
            subscriber_key = _normalize_subscriber_key(raw_subscriber_key)
            territory_code = _normalize_territory_code(raw_territory_code)
            if not subscriber_key or not _valid_subscriber_key(subscriber_key):
                self.send_response(HTTPStatus.BAD_REQUEST)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.end_headers()
                self.wfile.write(b"{\"error\":\"invalid_subscriber_key\"}\n")
                return
            if not territory_code or not _valid_territory_code(territory_code):
                self.send_response(HTTPStatus.BAD_REQUEST)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.end_headers()
                self.wfile.write(b"{\"error\":\"invalid_territory_code\"}\n")
                return

            include_lows, err, exc = _latest_lows_enabled_pref(subscriber_key, territory_code)
            if err:
                _log_preview_unavailable(subscriber_key, territory_code, reason=err, exc=exc)

            payload = json.dumps(
                {"subscriber_key": subscriber_key, "territory_code": territory_code, "lows_enabled": bool(include_lows)},
                separators=(",", ":"),
            ).encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return

        if path == "/unsubscribe":
            token = (params.get("token") or [""])[0]
            token_id = verify_unsub_token(token)
            if not token_id:
                self.send_response(HTTPStatus.BAD_REQUEST)
                self.end_headers()
                self.wfile.write(b"Invalid unsubscribe link.")
                return

            email = lookup_email_for_token(token_id)
            if not email:
                self.send_response(HTTPStatus.NOT_FOUND)
                self.end_headers()
                self.wfile.write(b"Invalid or expired token.")
                return

            add_to_suppression(email, "unsubscribe", "one_click")

            body = "You're unsubscribed."
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(body.encode("utf-8"))))
            self.end_headers()
            self.wfile.write(body.encode("utf-8"))
            return

        def _render_html(title: str, inner_html: str, status: int = 200) -> None:
            body = (
                "<!doctype html><html><head><meta charset=\"utf-8\">"
                "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">"
                f"<title>{title}</title></head>"
                "<body style=\"font-family: Arial, sans-serif; padding: 24px; background: #f7f9fc;\">"
                "<div style=\"max-width: 720px; margin: 0 auto; background: #ffffff; border: 1px solid #e5e7eb; "
                "border-radius: 12px; padding: 20px;\">"
                "<div style=\"font-size: 12px; letter-spacing: 0.22em; text-transform: uppercase; color: #6b7280;\">"
                "MicroFlowOps</div>"
                f"<h1 style=\"margin: 10px 0 0 0; font-size: 22px; color: #111827;\">{title}</h1>"
                f"{inner_html}"
                "</div></body></html>"
            )
            data = body.encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def _prefs_diag_block(raw_subscriber_key: str, raw_territory_code: str) -> str:
            sk = html.escape((raw_subscriber_key or "").strip())
            terr = html.escape((raw_territory_code or "").strip())
            return (
                "<div style=\"margin-top: 16px; padding: 12px; border: 1px solid #e5e7eb; "
                "border-radius: 10px; background: #f9fafb;\">"
                "<div style=\"font-size: 12px; letter-spacing: 0.14em; text-transform: uppercase; "
                "color: #6b7280; font-weight: 700;\">Diagnostics</div>"
                "<pre style=\"margin: 10px 0 0 0; white-space: pre-wrap; word-break: break-word; "
                "font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; "
                "font-size: 12px; color: #111827;\">"
                f"subscriber_key={sk}\nterritory_code={terr}</pre>"
                "</div>"
            )

        def _parse_prefs_territory(campaign_id: str) -> tuple[str | None, str | None]:
            # Expected campaign_id: prefs|<customer_id>|terr=TX_TRIANGLE_V1
            text = (campaign_id or "").strip()
            parts = [p.strip() for p in text.split("|") if p.strip()]
            terr = None
            for p in parts:
                if p.lower().startswith("terr="):
                    terr = p.split("=", 1)[1].strip()
                    break
            if not terr:
                return None, None
            return _resolve_territory(terr)

        def _token_is_expired(created_at: str) -> bool:
            from datetime import datetime, timezone, timedelta

            try:
                ttl_days = int(os.getenv("UNSUB_TOKEN_TTL_DAYS", "45"))
            except Exception:
                ttl_days = 45
            if ttl_days <= 0:
                return False
            if not created_at:
                return False
            try:
                ts_dt = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
                if ts_dt.tzinfo is None:
                    ts_dt = ts_dt.replace(tzinfo=timezone.utc)
            except Exception:
                return False
            cutoff = datetime.now(timezone.utc) - timedelta(days=ttl_days)
            return ts_dt < cutoff

        if path == "/prefs":
            raw_subscriber_key = (params.get("subscriber_key") or [""])[0]
            raw_territory_code = (params.get("territory_code") or [""])[0]
            signed = (
                (params.get("token") or params.get("TOKEN") or params.get("t") or params.get("T") or [""])[0].strip()
            )
            token_id = verify_unsub_token(signed)
            if not token_id:
                _render_html(
                    "Invalid link",
                    "<p style=\"color:#374151; margin-top: 12px;\">This preference link is invalid. Please request a fresh email.</p>"
                    + _prefs_diag_block(raw_subscriber_key, raw_territory_code),
                    status=int(HTTPStatus.BAD_REQUEST),
                )
                return

            record = lookup_token_record(token_id) or {}
            email = (record.get("email") or "").strip().lower()
            created_at = (record.get("created_at") or "").strip()
            if not email:
                _render_html(
                    "Link expired",
                    "<p style=\"color:#374151; margin-top: 12px;\">This preference link is expired. Please request a fresh email.</p>",
                    status=int(HTTPStatus.NOT_FOUND),
                )
                return
            if _token_is_expired(created_at):
                _render_html(
                    "Link expired",
                    "<p style=\"color:#374151; margin-top: 12px;\">This preference link is expired. Please request a fresh email.</p>",
                    status=int(HTTPStatus.NOT_FOUND),
                )
                return

            subscriber_key = _normalize_subscriber_key(raw_subscriber_key)
            territory_code = _normalize_territory_code(raw_territory_code)

            if not territory_code:
                _render_html(
                    "Invalid link",
                    "<p style=\"color:#374151; margin-top: 12px;\">This preference link is missing territory_code. Please request a fresh email.</p>"
                    + _prefs_diag_block(raw_subscriber_key, raw_territory_code),
                    status=int(HTTPStatus.BAD_REQUEST),
                )
                return
            if not _valid_territory_code(territory_code):
                _render_html(
                    "Invalid link",
                    "<p style=\"color:#374151; margin-top: 12px;\">This preference link has an invalid territory_code format. Please request a fresh email.</p>"
                    + _prefs_diag_block(raw_subscriber_key, raw_territory_code),
                    status=int(HTTPStatus.BAD_REQUEST),
                )
                return

            if not subscriber_key:
                _render_html(
                    "Invalid link",
                    "<p style=\"color:#374151; margin-top: 12px;\">This preference link is missing subscriber_key. Please request a fresh email.</p>"
                    + _prefs_diag_block(raw_subscriber_key, raw_territory_code),
                    status=int(HTTPStatus.BAD_REQUEST),
                )
                return
            if not _valid_subscriber_key(subscriber_key):
                _render_html(
                    "Invalid link",
                    "<p style=\"color:#374151; margin-top: 12px;\">This preference link has an invalid subscriber_key format. Please request a fresh email.</p>"
                    + _prefs_diag_block(raw_subscriber_key, raw_territory_code),
                    status=int(HTTPStatus.BAD_REQUEST),
                )
                return

            _resolved_code, territory_display = _resolve_territory(territory_code)
            include_lows, err, exc = _latest_lows_enabled_pref(subscriber_key, territory_code)
            if err:
                _log_preview_unavailable(subscriber_key, territory_code, reason=err, exc=exc)
                include_lows = False

            qs = urlencode({"token": signed, "subscriber_key": subscriber_key, "territory_code": territory_code})
            enable_url = f"/prefs/enable_lows?{qs}"
            disable_url = f"/prefs/disable_lows?{qs}"

            state = "ON" if include_lows else "OFF"
            state_bg = "#ecfdf5" if include_lows else "#fef2f2"
            state_border = "#a7f3d0" if include_lows else "#fecaca"
            state_color = "#065f46" if include_lows else "#991b1b"

            preview = _recent_low_priority_preview_html(subscriber_key=subscriber_key, territory_code=territory_code)
            inner = (
                "<p style=\"color:#374151; margin-top: 12px;\">"
                f"Territory: <strong>{html.escape(territory_display or territory_code)}</strong></p>"
                f"<div style=\"margin-top: 10px; padding: 10px 12px; border-radius: 12px; "
                f"background: {state_bg}; border: 1px solid {state_border}; color: {state_color}; font-weight: 900;\">"
                f"Low signals: {state}</div>"
                "<div style=\"margin-top: 14px; display:flex; gap:10px; flex-wrap:wrap;\">"
                f"<a href=\"{enable_url}\" style=\"display:inline-block; background:#0b5fff; color:#ffffff; text-decoration:none; "
                "padding:10px 14px; border-radius:10px; font-weight:700;\">Enable lows</a>"
                f"<a href=\"{disable_url}\" style=\"display:inline-block; background:#6b7280; color:#ffffff; text-decoration:none; "
                "padding:10px 14px; border-radius:10px; font-weight:700;\">Disable lows</a>"
                "</div>"
                + preview
            )
            _render_html("Preferences", inner, status=int(HTTPStatus.OK))
            return

        if path in {"/prefs/enable_lows", "/prefs/disable_lows"}:
            raw_subscriber_key = (params.get("subscriber_key") or [""])[0]
            raw_territory_code = (params.get("territory_code") or [""])[0]
            signed = (
                (params.get("token") or params.get("TOKEN") or params.get("t") or params.get("T") or [""])[0].strip()
            )
            token_id = verify_unsub_token(signed)
            if not token_id:
                _render_html(
                    "Invalid link",
                    "<p style=\"color:#374151; margin-top: 12px;\">This preference link is invalid. Please request a fresh email.</p>"
                    + _prefs_diag_block(raw_subscriber_key, raw_territory_code),
                    status=int(HTTPStatus.BAD_REQUEST),
                )
                return

            record = lookup_token_record(token_id) or {}
            email = (record.get("email") or "").strip().lower()
            campaign_id = (record.get("campaign_id") or "").strip()
            created_at = (record.get("created_at") or "").strip()
            if not email:
                _render_html(
                    "Link expired",
                    "<p style=\"color:#374151; margin-top: 12px;\">This preference link is expired. Please request a fresh email.</p>",
                    status=int(HTTPStatus.NOT_FOUND),
                )
                return
            if _token_is_expired(created_at):
                _render_html(
                    "Link expired",
                    "<p style=\"color:#374151; margin-top: 12px;\">This preference link is expired. Please request a fresh email.</p>",
                    status=int(HTTPStatus.NOT_FOUND),
                )
                return

            subscriber_key = _normalize_subscriber_key(raw_subscriber_key)
            territory_code = _normalize_territory_code(raw_territory_code)

            if not territory_code:
                _render_html(
                    "Invalid link",
                    "<p style=\"color:#374151; margin-top: 12px;\">This preference link is missing territory_code. Please request a fresh email.</p>"
                    + _prefs_diag_block(raw_subscriber_key, raw_territory_code),
                    status=int(HTTPStatus.BAD_REQUEST),
                )
                return
            if not _valid_territory_code(territory_code):
                _render_html(
                    "Invalid link",
                    "<p style=\"color:#374151; margin-top: 12px;\">This preference link has an invalid territory_code format. Please request a fresh email.</p>"
                    + _prefs_diag_block(raw_subscriber_key, raw_territory_code),
                    status=int(HTTPStatus.BAD_REQUEST),
                )
                return

            if not subscriber_key:
                _render_html(
                    "Invalid link",
                    "<p style=\"color:#374151; margin-top: 12px;\">This preference link is missing subscriber_key. Please request a fresh email.</p>"
                    + _prefs_diag_block(raw_subscriber_key, raw_territory_code),
                    status=int(HTTPStatus.BAD_REQUEST),
                )
                return
            if not _valid_subscriber_key(subscriber_key):
                _render_html(
                    "Invalid link",
                    "<p style=\"color:#374151; margin-top: 12px;\">This preference link has an invalid subscriber_key format. Please request a fresh email.</p>"
                    + _prefs_diag_block(raw_subscriber_key, raw_territory_code),
                    status=int(HTTPStatus.BAD_REQUEST),
                )
                return

            # Best-effort display label: if territory_code is known, show a friendly name; else show the code.
            _resolved_code, territory_display = _resolve_territory(territory_code)

            include = path.endswith("/enable_lows")
            set_include_lows_pref(
                email=email,
                subscriber_key=subscriber_key,
                territory=territory_code,
                include_lows=include,
                source="prefs_enable_lows" if include else "prefs_disable_lows",
            )

            from datetime import datetime, timezone
            import hashlib

            ts = datetime.now(timezone.utc).isoformat()
            email_tag = hashlib.sha256(email.encode("utf-8")).hexdigest()[:10]
            action = "enable_lows" if include else "disable_lows"
            print(f"[AUDIT] prefs_{action} email_sha={email_tag} territory={territory_code} ip={ip} at={ts}")

            other_path = "/prefs/disable_lows" if include else "/prefs/enable_lows"
            other_label = "Disable lows" if include else "Enable lows"
            other_qs = urlencode({"token": signed, "subscriber_key": subscriber_key, "territory_code": territory_code})
            other_url = f"{other_path}?{other_qs}"

            banner_text = "Low-priority signals enabled" if include else "Low-priority signals disabled"
            banner = (
                "<div style=\"margin-top: 12px; padding: 12px 14px; border-radius: 12px; "
                "background: #ecfdf5; border: 1px solid #a7f3d0; color: #065f46; font-weight: 800;\">"
                f"{html.escape(banner_text)}"
                "</div>"
            )
            expectation = (
                "<p style=\"color:#374151; margin-top: 12px;\">"
                "<strong>Will start next scheduled digest; preview is immediate.</strong>"
                "</p>"
                if include
                else (
                    "<p style=\"color:#374151; margin-top: 12px;\">"
                    "<strong>Will stop starting with the next scheduled digest.</strong>"
                    "</p>"
                )
            )

            preview = _recent_low_priority_preview_html(subscriber_key=subscriber_key, territory_code=territory_code)
            prefs_url = f"/prefs?{urlencode({'token': signed, 'subscriber_key': subscriber_key, 'territory_code': territory_code})}"
            state_line = (
                "<p style=\"color:#374151; margin-top: 12px;\">"
                f"Low signals: <strong>{'ON' if include else 'OFF'}</strong>.</p>"
            )

            inner = (
                banner
                + expectation
                + state_line
                + preview
                + "<p style=\"color:#374151; margin-top: 12px;\">"
                f"Preference updated for <strong>{territory_display or territory_code}</strong>."
                "</p>"
                f"<p style=\"margin-top: 14px;\"><a href=\"{prefs_url}\" style=\"color:#0b5fff;\">Back to preferences</a></p>"
                f"<p style=\"margin-top: 14px;\"><a href=\"{other_url}\" "
                "style=\"display:inline-block; background:#0b5fff; color:#ffffff; text-decoration:none; "
                "padding:10px 14px; border-radius:10px; font-weight:700;\">"
                f"{other_label}</a></p>"
            )
            _render_html("Preference updated", inner, status=int(HTTPStatus.OK))
            return

        self.send_response(HTTPStatus.NOT_FOUND)
        self.end_headers()
        self.wfile.write(b"Not found")

    def do_HEAD(self):
        # Make verification curl -I predictable.
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/")
        if path in {"/__version", "/unsubscribe", "/prefs", "/prefs/enable_lows", "/prefs/disable_lows"}:
            self.send_response(HTTPStatus.OK)
            self.end_headers()
            return
        self.send_response(HTTPStatus.NOT_FOUND)
        self.end_headers()

    def do_POST(self):
        ip = _client_ip(self)
        if _rate_limited(ip):
            self.send_response(HTTPStatus.TOO_MANY_REQUESTS)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.end_headers()
            self.wfile.write(b"Too many requests.\n")
            return

        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/")

        secret = (os.getenv("UNSUB_SECRET") or "").strip()
        if not secret:
            self.send_response(HTTPStatus.INTERNAL_SERVER_ERROR)
            self.end_headers()
            return

        try:
            length = int(self.headers.get("Content-Length", "0"))
        except Exception:
            length = 0
        if length <= 0 or length > 10_000:
            self.send_response(HTTPStatus.BAD_REQUEST)
            self.end_headers()
            return

        try:
            body = self.rfile.read(length).decode("utf-8")
            payload = json.loads(body)
        except Exception:
            self.send_response(HTTPStatus.BAD_REQUEST)
            self.end_headers()
            return

        if path == "/unsubscribe/check":
            email = (payload.get("email") or "").strip().lower()
            if not email or "@" not in email:
                self.send_response(HTTPStatus.BAD_REQUEST)
                self.end_headers()
                return
            auth = (self.headers.get("X-Unsub-Auth") or "").strip()
            expected = sign_check(email, secret)
            if not auth or auth != expected:
                self.send_response(HTTPStatus.UNAUTHORIZED)
                self.end_headers()
                return
            suppressed = is_suppressed_email(email)
            resp = json.dumps({"suppressed": bool(suppressed)}).encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(resp)))
            self.end_headers()
            self.wfile.write(resp)
            return

        if path == "/unsubscribe/stats":
            try:
                since_hours = int(payload.get("since_hours", 24))
            except Exception:
                since_hours = 24
            auth = (self.headers.get("X-Unsub-Auth") or "").strip()
            expected = sign_stats(since_hours, secret)
            if not auth or auth != expected:
                self.send_response(HTTPStatus.UNAUTHORIZED)
                self.end_headers()
                return
            # Parse suppression.csv and count new unsubs in window
            from datetime import datetime, timezone, timedelta
            import csv
            from pathlib import Path
            data_dir = (os.getenv("DATA_DIR") or "").strip()
            sup_path = Path(data_dir) / "suppression.csv" if data_dir else (Path(__file__).parent / "out" / "suppression.csv")
            cutoff = datetime.now(timezone.utc) - timedelta(hours=since_hours)
            count = 0
            if sup_path.exists():
                with open(sup_path, "r", newline="", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        if (row.get("reason") or "").strip().lower() != "unsubscribe":
                            continue
                        ts = (row.get("timestamp") or "").strip()
                        if not ts:
                            continue
                        try:
                            ts_dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                            if ts_dt.tzinfo is None:
                                ts_dt = ts_dt.replace(tzinfo=timezone.utc)
                        except Exception:
                            continue
                        if ts_dt >= cutoff:
                            count += 1
            resp = json.dumps({"new_unsubs": count, "since_hours": since_hours}).encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(resp)))
            self.end_headers()
            self.wfile.write(resp)
            return

        if path != "/unsubscribe/register":
            self.send_response(HTTPStatus.NOT_FOUND)
            self.end_headers()
            return

        token_id = (payload.get("token_id") or "").strip()
        email = (payload.get("email") or "").strip().lower()
        campaign_id = (payload.get("campaign_id") or "").strip()

        if not token_id or not email or "@" not in email:
            self.send_response(HTTPStatus.BAD_REQUEST)
            self.end_headers()
            return

        auth = (self.headers.get("X-Unsub-Auth") or "").strip()
        expected = sign_registration(token_id, email, secret)
        if not auth or auth != expected:
            self.send_response(HTTPStatus.UNAUTHORIZED)
            self.end_headers()
            return

        store_unsub_token(token_id, email, campaign_id or "unknown")
        self.send_response(HTTPStatus.NO_CONTENT)
        self.end_headers()

    def log_message(self, format, *args):
        # Quiet logging
        return


def main():
    parser = argparse.ArgumentParser(description="Minimal one-click unsubscribe HTTPS server")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8443)
    args = parser.parse_args()

    httpd = HTTPServer((args.host, args.port), UnsubHandler)
    print(f"[INFO] Unsubscribe server listening on http://{args.host}:{args.port}/unsubscribe")
    httpd.serve_forever()


if __name__ == "__main__":
    main()
