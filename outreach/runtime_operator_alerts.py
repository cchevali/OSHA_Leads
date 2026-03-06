from __future__ import annotations

import os
import smtplib
from email.mime.text import MIMEText
from typing import Mapping


def resolve_alert_recipient(env: Mapping[str, str] | None = None) -> str:
    source = env if env is not None else os.environ
    override = str(source.get("RUNTIME_ALERT_RECIPIENT") or "").strip()
    if override:
        return override
    return str(source.get("OSHA_SMOKE_TO") or "").strip()


def smtp_missing_key(env: Mapping[str, str] | None = None) -> str:
    source = env if env is not None else os.environ
    if not str(source.get("SMTP_HOST") or "").strip():
        return "SMTP_HOST"
    if not str(source.get("SMTP_PORT") or "").strip():
        return "SMTP_PORT"
    if not str(source.get("SMTP_USER") or "").strip():
        return "SMTP_USER"
    if not str(source.get("SMTP_PASS") or "").strip():
        return "SMTP_PASS"
    return ""


def send_plain_text_alert(
    *,
    recipient: str,
    subject: str,
    body: str,
    env: Mapping[str, str] | None = None,
) -> None:
    source = env if env is not None else os.environ
    smtp_host = str(source.get("SMTP_HOST") or "").strip()
    smtp_port = int(str(source.get("SMTP_PORT") or "0").strip())
    smtp_user = str(source.get("SMTP_USER") or "").strip()
    smtp_pass = str(source.get("SMTP_PASS") or "").strip()
    from_email = str(source.get("FROM_EMAIL") or smtp_user).strip()

    msg = MIMEText(body or "", _charset="utf-8")
    msg["Subject"] = subject
    msg["From"] = from_email
    msg["To"] = recipient

    if smtp_port == 465:
        with smtplib.SMTP_SSL(smtp_host, smtp_port) as server:
            server.login(smtp_user, smtp_pass)
            server.send_message(msg)
        return

    with smtplib.SMTP(smtp_host, smtp_port) as server:
        server.starttls()
        server.login(smtp_user, smtp_pass)
        server.send_message(msg)
