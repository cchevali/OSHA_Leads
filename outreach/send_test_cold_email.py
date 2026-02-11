import argparse
import csv
import html as _html
import os
import sys
from pathlib import Path

try:  # pragma: no cover
    from dotenv import load_dotenv

    load_dotenv()
except Exception:  # pragma: no cover
    pass

# When invoked as `py -3 outreach/send_test_cold_email.py`, sys.path[0] is `outreach/`.
# Add repo root so imports like `send_digest_email` resolve reliably.
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


ERR_TEST_TO_MISMATCH = "ERR_TEST_TO_MISMATCH"
ERR_TEST_TO_MISSING = "ERR_TEST_TO_MISSING"
ERR_OUTBOX_SCHEMA = "ERR_OUTBOX_SCHEMA"
ERR_TEST_SEND = "ERR_TEST_SEND"
ERR_PROSPECT_NOT_FOUND = "ERR_PROSPECT_NOT_FOUND"

PASS_TEST_SEND = "PASS_TEST_SEND"

CANONICAL_TEST_TO_ENV = "OSHA_SMOKE_TO"
LEGACY_TEST_TO_ENV_KEYS = ["CHASE_EMAIL", "OUTREACH_TEST_TO"]

SEND_LABEL = "outreach_test"  # short, non-empty label for send_digest_email.send_email()
SEND_TERRITORY_CODE = "OUTREACH_TEST"


REQUIRED_OUTBOX_COLUMNS = ["prospect_id", "subject", "body", "unsubscribe_url"]
OPTIONAL_OUTBOX_COLUMNS = ["prefs_url", "email", "text_body", "html_body"]


def _norm_email(s: str) -> str:
    return (s or "").strip().lower()


def _read_outbox_rows(path: str) -> tuple[list[str], list[dict]]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(str(p))
    with open(p, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = list(reader.fieldnames or [])
        rows = [dict(r) for r in reader]
    return fieldnames, rows


def _validate_outbox_schema(fieldnames: list[str]) -> None:
    missing = [c for c in REQUIRED_OUTBOX_COLUMNS if c not in set(fieldnames)]
    if missing:
        raise ValueError(f"{ERR_OUTBOX_SCHEMA} missing_columns={','.join(missing)}")


def _select_row(rows: list[dict], prospect_id: str | None) -> dict:
    if not rows:
        raise ValueError(f"{ERR_OUTBOX_SCHEMA} empty_outbox")
    if not (prospect_id or "").strip():
        return rows[0]
    pid = (prospect_id or "").strip()
    for r in rows:
        if (r.get("prospect_id") or "").strip() == pid:
            return r
    raise ValueError(f"{ERR_PROSPECT_NOT_FOUND} prospect_id={pid}")


def _text_to_simple_html(text: str) -> str:
    # Preserve line breaks and spacing for debugging readability.
    safe = _html.escape(text or "")
    return (
        '<pre style="white-space: pre-wrap; font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, '
        "Liberation Mono, Courier New, monospace; font-size: 13px; line-height: 1.4;\">"
        + safe
        + "</pre>"
    )


def _resolve_expected_test_to() -> str:
    """
    Canonical test recipient config matches trial/test workflows:
    - primary: OSHA_SMOKE_TO
    - legacy aliases (only used if OSHA_SMOKE_TO is unset): CHASE_EMAIL, OUTREACH_TEST_TO
    """
    primary = _norm_email(os.getenv(CANONICAL_TEST_TO_ENV, ""))
    if primary:
        return primary
    for k in LEGACY_TEST_TO_ENV_KEYS:
        v = _norm_email(os.getenv(k, ""))
        if v:
            return v
    return ""


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description="Send exactly one test cold email from an outreach outbox CSV to an allowlisted recipient."
    )
    ap.add_argument("--outbox", required=True, help="Outbox CSV produced by outreach/generate_mailmerge.py")
    ap.add_argument(
        "--to",
        default="",
        help=f"Recipient email (must equal env {CANONICAL_TEST_TO_ENV}; default: that env value).",
    )
    ap.add_argument("--prospect-id", default="", help="Select a specific outbox row by prospect_id (default: first row)")
    ap.add_argument("--dry-run", action="store_true", help="Print rendered subject/body and exit 0; no send")
    ap.add_argument("--debug-header", action="store_true", help="Prepend diagnostic header to the email body (test only)")
    args = ap.parse_args(argv)

    expected = _resolve_expected_test_to()
    got = _norm_email(args.to) if (args.to or "").strip() else ""
    if not expected:
        print(
            f"{ERR_TEST_TO_MISSING} expected_env={CANONICAL_TEST_TO_ENV} (or legacy CHASE_EMAIL/OUTREACH_TEST_TO)",
            file=sys.stderr,
        )
        return 2
    if not got:
        got = expected
    if got != expected:
        print(f"{ERR_TEST_TO_MISMATCH} expected={expected} got={got}", file=sys.stderr)
        return 2

    try:
        fieldnames, rows = _read_outbox_rows(args.outbox)
        _validate_outbox_schema(fieldnames)
        row = _select_row(rows, args.prospect_id)
    except Exception as e:
        msg = str(e or "").strip()
        if any(tok in msg for tok in [ERR_OUTBOX_SCHEMA, ERR_PROSPECT_NOT_FOUND]):
            print(msg, file=sys.stderr)
            return 3
        print(f"{ERR_TEST_SEND} {msg}", file=sys.stderr)
        return 1

    prospect_id = (row.get("prospect_id") or "").strip()
    original_to = _norm_email(row.get("email", "")) if "email" in fieldnames else ""
    subject = (row.get("subject") or "").strip()
    base_text_body = (row.get("text_body") or row.get("body") or "").rstrip() + "\n"
    base_html_body = (row.get("html_body") or "").strip()
    unsubscribe_url = (row.get("unsubscribe_url") or "").strip()
    prefs_url = (row.get("prefs_url") or "").strip()

    test_subject = f"[TEST] {subject}".strip()
    debug_preamble = ""
    if args.debug_header:
        debug_preamble = (
            "TEST SEND (outreach)\n"
            f"- prospect_id: {prospect_id}\n"
            f"- original_outbox_email: {original_to or '(missing)'}\n"
            f"- unsubscribe_url: {unsubscribe_url or '(blank)'}\n"
            f"- prefs_url: {prefs_url or '(blank)'}\n"
            "\n"
        )

    send_text_body = (debug_preamble + base_text_body).rstrip() + "\n"
    if base_html_body:
        send_html_body = base_html_body
        if args.debug_header:
            # Keep debug preamble visible but separate from the rendered card.
            send_html_body = (
                _text_to_simple_html(debug_preamble.rstrip("\n"))
                + "\n"
                + base_html_body
            )
    else:
        send_html_body = _text_to_simple_html(send_text_body)

    # Reuse digest transport/config primitives.
    try:
        import send_digest_email as sde

        branding = sde.resolve_branding({})
        reply_to = (branding.get("reply_to") or os.getenv("REPLY_TO_EMAIL") or "support@microflowops.com").strip()
        mailto = f"mailto:{reply_to}?subject=unsubscribe"
        list_unsub = f"<{mailto}>"
        list_unsub_post = None
        if unsubscribe_url:
            list_unsub = f"<{mailto}>, <{unsubscribe_url}>"
            # RFC 8058 one-click hint (Gmail uses this for the top-of-email control).
            list_unsub_post = "List-Unsubscribe=One-Click"

        ok, message_id, err = sde.send_email(
            recipient=got,
            subject=test_subject,
            html_body=send_html_body,
            text_body=send_text_body,
            customer_id="",
            territory_code=SEND_TERRITORY_CODE,
            branding=branding,
            dry_run=bool(args.dry_run),
            list_unsub=list_unsub,
            list_unsub_post=list_unsub_post,
            label=SEND_LABEL,
        )
        if not ok:
            raise RuntimeError(err or "send_failed")
    except Exception as e:
        msg = str(e or "").strip()
        msg = msg.replace("\r", " ").replace("\n", " ").strip()
        if len(msg) > 220:
            msg = msg[:220] + "..."
        print(f"{ERR_TEST_SEND} {msg}", file=sys.stderr)
        return 1

    print("subject=" + test_subject)
    print("unsubscribe_url=" + unsubscribe_url)
    print("prefs_url=" + prefs_url)
    print("text_body=")
    print(send_text_body)
    if base_html_body:
        print(f"html_body_present=True html_body_len={len(base_html_body)}")
    if args.dry_run:
        print(f"{PASS_TEST_SEND} to={got} prospect_id={prospect_id} dry_run=True message_id={message_id}")
    else:
        print(f"{PASS_TEST_SEND} to={got} prospect_id={prospect_id} message_id={message_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
