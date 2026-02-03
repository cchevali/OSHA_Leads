import argparse
import ssl
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
    lookup_email_for_token,
    verify_unsub_token,
)


class UnsubHandler(BaseHTTPRequestHandler):
    server_version = "UnsubServer/1.0"

    def do_GET(self):
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

    def log_message(self, format, *args):
        # Quiet logging
        return


def main():
    parser = argparse.ArgumentParser(description="Minimal one-click unsubscribe HTTPS server")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8443)
    parser.add_argument("--cert", required=True, help="Path to TLS certificate (PEM)")
    parser.add_argument("--key", required=True, help="Path to TLS private key (PEM)")
    args = parser.parse_args()

    httpd = HTTPServer((args.host, args.port), UnsubHandler)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(certfile=args.cert, keyfile=args.key)
    httpd.socket = context.wrap_socket(httpd.socket, server_side=True)

    print(f"[INFO] Unsubscribe server listening on https://{args.host}:{args.port}/unsubscribe")
    httpd.serve_forever()


if __name__ == "__main__":
    main()
