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

        if path == "/prefs/enable_lows":
            token = (params.get("TOKEN") or params.get("token") or [""])[0]
            raw_territory = (params.get("territory") or [""])[0]
            token_id = verify_unsub_token(token)
            if not token_id:
                self.send_response(HTTPStatus.BAD_REQUEST)
                self.end_headers()
                self.wfile.write(b"Invalid link.")
                return

            email = lookup_email_for_token(token_id)
            if not email:
                self.send_response(HTTPStatus.NOT_FOUND)
                self.end_headers()
                self.wfile.write(b"Invalid or expired token.")
                return

            territory_code, territory_display = _resolve_territory(raw_territory)
            if not territory_code:
                self.send_response(HTTPStatus.BAD_REQUEST)
                self.end_headers()
                self.wfile.write(b"Invalid territory.")
                return

            set_include_lows_pref(email=email, territory=territory_code, include_lows=True, source="one_click")
            from datetime import datetime, timezone
            ts = datetime.now(timezone.utc).isoformat()
            print(
                f"[AUDIT] prefs_enable_lows email={email} territory={territory_code} source=one_click ip={ip} at={ts}"
            )

            body = (
                "<!doctype html><html><head><meta charset=\"utf-8\">"
                "<title>Preference updated</title></head><body style=\"font-family: Arial, sans-serif; padding: 24px;\">"
                f"<h1 style=\"margin-top: 0;\">Preference updated</h1>"
                f"<p>You will receive low-priority signals in future digests for <strong>{territory_display or territory_code}</strong>. You can disable at any time.</p>"
                "</body></html>"
            )
            data = body.encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
            return

        self.send_response(HTTPStatus.NOT_FOUND)
        self.end_headers()
        self.wfile.write(b"Not found")

    def do_HEAD(self):
        # Make verification curl -I predictable.
        parsed = urlparse(self.path)
        if parsed.path.rstrip("/") != "/unsubscribe":
            self.send_response(HTTPStatus.NOT_FOUND)
            self.end_headers()
            return
        self.send_response(HTTPStatus.OK)
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
