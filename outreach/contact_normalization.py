from __future__ import annotations

import re
from typing import Any
from urllib.parse import urlparse, urlunparse


_EMAIL_RE = re.compile(r"[A-Z0-9.!#$%&'*+/=?^_`{|}~-]+@[A-Z0-9-]+(?:\.[A-Z0-9-]+)+", flags=re.I)
_PLAIN_EMAIL_RE = re.compile(r"^[A-Z0-9.!#$%&'*+/=?^_`{|}~-]+@[A-Z0-9-]+(?:\.[A-Z0-9-]+)+$", flags=re.I)
_HTTP_URL_RE = re.compile(r"https?://[^\s<>\[\]\"'|)]+", flags=re.I)
_MARKDOWN_LINK_RE = re.compile(r"\[([^\]]*)\]\(([^)]*)\)")
_MARKDOWN_TARGET_RE = re.compile(r"\]\([^)]*\)")
_MAILTO_RE = re.compile(r"mailto:[^\s)>]+", flags=re.I)
_HOST_LABEL_RE = re.compile(r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$", flags=re.I)
_IPV4_RE = re.compile(r"^\d{1,3}(?:\.\d{1,3}){3}$")
_MARKUP_HINTS = ("mailto:", "[http", "](http", "](mailto:", "%22,%22")


def normalize_text(value: Any) -> str:
    return " ".join(str(value or "").replace("\r", " ").replace("\n", " ").split()).strip()


def has_markup_artifact(value: str) -> bool:
    text = normalize_text(value).lower()
    if not text:
        return False
    if any(token in text for token in _MARKUP_HINTS):
        return True
    return ("[" in text and "](" in text) or ("[" in text and "http" in text) or ("[" in text and "@" in text)


def _residual_without_markers(value: str, emails: list[str]) -> str:
    residual = normalize_text(value)
    for email in emails:
        residual = re.sub(re.escape(email), "", residual, flags=re.I)
    residual = _MAILTO_RE.sub("", residual)
    residual = _HTTP_URL_RE.sub("", residual)
    residual = residual.replace("mailto:", "")
    for token in ["[", "]", "(", ")", "<", ">", "\"", "'", ",", ";", ":", "|", "`", "."]:
        residual = residual.replace(token, "")
    residual = residual.replace("%22", "")
    return "".join(part for part in residual.split() if part)


def normalize_email(value: str) -> str:
    text = normalize_text(value)
    if not text:
        return ""
    candidates: list[str] = []
    for match in _EMAIL_RE.findall(text):
        email = str(match or "").strip().lower()
        if email and email not in candidates:
            candidates.append(email)
    if len(candidates) == 1 and not _residual_without_markers(text, candidates):
        return candidates[0]
    cleaned = text.strip().strip("\"'")
    if cleaned.lower().startswith("mailto:"):
        cleaned = cleaned.split(":", 1)[1].strip()
    cleaned = cleaned.strip("[]()<>\"' ")
    return cleaned.lower()


def valid_email(value: str) -> bool:
    return bool(_PLAIN_EMAIL_RE.fullmatch(normalize_text(value).strip().lower()))


def email_domain(value: str) -> str:
    email = normalize_email(value)
    if not _PLAIN_EMAIL_RE.fullmatch(email):
        return ""
    return email.split("@", 1)[1].strip().lower()


def _valid_host(host: str) -> bool:
    text = normalize_text(host).strip().lower().rstrip(".")
    if not text:
        return False
    if any(token in text for token in (" ", "/", "\\", ",", ";", "_", "@")):
        return False
    if text == "localhost":
        return True
    if _IPV4_RE.fullmatch(text):
        return all(0 <= int(part) <= 255 for part in text.split("."))
    labels = [label for label in text.split(".") if label]
    if len(labels) < 2:
        return False
    if any(not _HOST_LABEL_RE.fullmatch(label) for label in labels):
        return False
    if labels[-1].isdigit():
        return False
    return True


def canonicalize_http_url(value: str) -> str:
    raw = normalize_text(value).strip("[]()<>\"'.,; ")
    if not raw:
        return ""
    try:
        parsed = urlparse(raw if "://" in raw else f"https://{raw}")
    except Exception:
        return ""
    scheme = (parsed.scheme or "https").strip().lower()
    try:
        port = parsed.port
    except ValueError:
        return ""
    if parsed.username or parsed.password:
        return ""
    host = normalize_text(parsed.hostname or (parsed.path if not parsed.netloc else "")).strip().lower().rstrip(".,;:")
    path = parsed.path if parsed.netloc else ""
    if not _valid_host(host):
        return ""
    netloc = host if port is None else f"{host}:{port}"
    if path == "/":
        path = ""
    return urlunparse((scheme, netloc, path, "", parsed.query, ""))


def extract_http_urls(value: str) -> list[str]:
    text = normalize_text(value)
    if not text:
        return []
    out: list[str] = []
    for match in _HTTP_URL_RE.findall(text):
        candidate = canonicalize_http_url(str(match or ""))
        if candidate and candidate not in out:
            out.append(candidate)
    return out


def normalize_website(value: str) -> str:
    text = normalize_text(value)
    urls = extract_http_urls(text)
    if urls:
        return urls[0]
    if not has_markup_artifact(text):
        return text.strip("[]()<>\"' ")
    return text.strip()


def normalize_source_urls(value: str) -> str:
    text = normalize_text(value)
    urls = extract_http_urls(text)
    if urls:
        return "|".join(urls)
    if not has_markup_artifact(text):
        return text.strip()
    return ""


def _strip_markdown_noise(value: str) -> str:
    text = normalize_text(value)
    if not text:
        return ""
    text = _MARKDOWN_LINK_RE.sub(lambda match: normalize_text(match.group(1)), text)
    text = _MARKDOWN_TARGET_RE.sub("", text)
    text = _MAILTO_RE.sub("", text)
    return text


def normalize_contact_name(value: str) -> str:
    text = _strip_markdown_noise(value)
    text = _HTTP_URL_RE.sub("", text)
    text = text.strip("[]()<>\"' ,;:")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def normalize_evidence_snippet(value: str) -> str:
    text = _strip_markdown_noise(value)
    text = _HTTP_URL_RE.sub("", text)
    text = text.strip("[]()<>\"' ")
    text = re.sub(r"\s+", " ", text).strip()
    text = re.sub(r"\s+([,;:.])", r"\1", text)
    return text
