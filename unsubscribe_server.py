import argparse
import json
import os
import time
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import urlparse, parse_qs

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


class UnsubHandler(BaseHTTPRequestHandler):
    server_version = "UnsubServer/1.0"

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

        if path in {"/prefs/enable_lows", "/prefs/disable_lows"}:
            signed = (params.get("t") or params.get("TOKEN") or params.get("token") or [""])[0].strip()
            token_id = verify_unsub_token(signed)
            if not token_id:
                _render_html(
                    "Invalid link",
                    "<p style=\"color:#374151; margin-top: 12px;\">This preference link is invalid. Please request a fresh email.</p>",
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

            territory_code, territory_display = _parse_prefs_territory(campaign_id)
            if not territory_code:
                _render_html(
                    "Invalid link",
                    "<p style=\"color:#374151; margin-top: 12px;\">This preference link is missing territory information. Please request a fresh email.</p>",
                    status=int(HTTPStatus.BAD_REQUEST),
                )
                return

            include = path.endswith("/enable_lows")
            set_include_lows_pref(
                email=email,
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
            inner = (
                "<p style=\"color:#374151; margin-top: 12px;\">"
                f"Preference updated for <strong>{territory_display or territory_code}</strong>."
                "</p>"
                f"<p style=\"margin-top: 14px;\"><a href=\"{other_path}?t={signed}\" "
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
        if path in {"/unsubscribe", "/prefs/enable_lows", "/prefs/disable_lows"}:
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
