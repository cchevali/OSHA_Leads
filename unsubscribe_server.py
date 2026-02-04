import argparse
import json
import os
import time
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, HTTPServer
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
    store_unsub_token,
    verify_unsub_token,
)

RATE_LIMIT_WINDOW_S = 60
RATE_LIMIT_MAX_REQ = 120
_rate_state = {}


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
        if parsed.path.rstrip("/") != "/unsubscribe":
            self.send_response(HTTPStatus.NOT_FOUND)
            self.end_headers()
            self.wfile.write(b"Not found")
            return

        params = parse_qs(parsed.query)
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
