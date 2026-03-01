import argparse
import os
import re
import smtplib
import sys
from datetime import datetime
from email.mime.text import MIMEText


ERR_EVENING_REMINDER_CONFIG = "ERR_EVENING_REMINDER_CONFIG"
ERR_EVENING_REMINDER_SEND = "ERR_EVENING_REMINDER_SEND"
PASS_EVENING_REMINDER_SENT = "PASS_EVENING_REMINDER_SENT"

DEFAULT_TO = "cchevali+oshasmoke@gmail.com"
DEFAULT_SUBJECT_OK = "OSHA evening ingest complete - manual steps reminder"
DEFAULT_SUBJECT_FAIL = "OSHA evening ingest failed - manual steps reminder"


def _emit(key: str, value: str) -> None:
    print(f"{key}={value}")


def _error(token: str, detail: str) -> int:
    print(f"{token} {detail}")
    return 1


def _valid_email(value: str) -> bool:
    return bool(re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", (value or "").strip()))


def _resolve_recipient() -> str:
    for key in ("OSHA_EVENING_MANUAL_STEPS_TO", "OSHA_SMOKE_TO", "CHASE_EMAIL"):
        value = (os.getenv(key) or "").strip().lower()
        if value:
            return value
    return DEFAULT_TO


def _build_body(ingest_exit_code: int) -> str:
    status_line = "Ingest status: OK" if ingest_exit_code == 0 else f"Ingest status: FAILED (exit_code={ingest_exit_code})"
    now_local = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines = [
        "Evening OSHA ingest run completed.",
        status_line,
        f"Generated: {now_local}",
        "",
        "Manual checklist:",
        "1) Review latest ingest/generation logs for anomalies.",
        "2) Validate CRM health:",
        r"   .\run_with_secrets.ps1 -- py -3 outreach\crm_admin.py stats",
        "3) Verify Apollo import sample (if Apollo was used today):",
        r"   .\run_with_secrets.ps1 -- py -3 outreach\crm_admin.py verify-import --csv .\apollo_export.csv",
    ]
    return "\n".join(lines)


def _send_alert_email(recipient: str, subject: str, body: str) -> tuple[bool, str]:
    smtp_host = (os.getenv("SMTP_HOST") or "").strip()
    smtp_port_text = (os.getenv("SMTP_PORT") or "").strip()
    smtp_user = (os.getenv("SMTP_USER") or "").strip()
    smtp_pass = (os.getenv("SMTP_PASS") or "").strip()
    if not (smtp_host and smtp_port_text and smtp_user and smtp_pass):
        return False, "missing_smtp_config"

    try:
        smtp_port = int(smtp_port_text)
    except ValueError:
        return False, f"invalid_smtp_port={smtp_port_text}"

    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = subject
    msg["From"] = smtp_user
    msg["To"] = recipient

    try:
        if smtp_port == 465:
            with smtplib.SMTP_SSL(smtp_host, smtp_port) as server:
                server.login(smtp_user, smtp_pass)
                server.send_message(msg)
        else:
            with smtplib.SMTP(smtp_host, smtp_port) as server:
                server.starttls()
                server.login(smtp_user, smtp_pass)
                server.send_message(msg)
    except Exception as exc:
        return False, re.sub(r"\s+", " ", str(exc)).strip() or exc.__class__.__name__
    return True, ""


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="Send a quick evening manual-steps reminder email after OSHA ingest.")
    ap.add_argument("--ingest-exit-code", type=int, default=0, help="Exit code from preceding ingest command.")
    ap.add_argument("--print-config", action="store_true", help="Print resolved config and exit.")
    return ap


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    recipient = _resolve_recipient()
    if not _valid_email(recipient):
        return _error(ERR_EVENING_REMINDER_CONFIG, f"invalid_recipient={recipient!r}")

    subject = DEFAULT_SUBJECT_OK if int(args.ingest_exit_code) == 0 else DEFAULT_SUBJECT_FAIL
    body = _build_body(int(args.ingest_exit_code))

    _emit("EVENING_MANUAL_REMINDER_TO", recipient)
    _emit("EVENING_MANUAL_REMINDER_SUBJECT", subject)
    _emit("EVENING_MANUAL_REMINDER_INGEST_EXIT_CODE", str(int(args.ingest_exit_code)))

    if args.print_config:
        print(f"{PASS_EVENING_REMINDER_SENT} status=PRINT_CONFIG")
        return 0

    ok, err = _send_alert_email(recipient, subject, body)
    if not ok:
        return _error(ERR_EVENING_REMINDER_SEND, err)

    print(f"{PASS_EVENING_REMINDER_SENT} status=SENT")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
