import base64
import csv
import hashlib
import hmac
import os
import secrets
from datetime import datetime, timezone
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent.resolve()

DATA_DIR = os.getenv("DATA_DIR", "").strip()
OUT_DIR = Path(DATA_DIR) if DATA_DIR else (SCRIPT_DIR / "out")
UNSUB_TOKEN_STORE_PATH = OUT_DIR / "unsub_tokens.csv"
SUPPRESSION_PATH = OUT_DIR / "suppression.csv"


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("ascii").rstrip("=")


def _b64url_decode(s: str) -> bytes:
    padding = "=" * (-len(s) % 4)
    return base64.urlsafe_b64decode(s + padding)


def get_unsub_secret() -> str:
    return os.getenv("UNSUB_SECRET", "").strip()


def sign_token(token_id: str, secret: str) -> str:
    mac = hmac.new(secret.encode("utf-8"), token_id.encode("utf-8"), hashlib.sha256).digest()
    return _b64url(mac)

def sign_registration(token_id: str, email: str, secret: str) -> str:
    """
    Sign a token registration payload so only the sender can register token->email mappings.
    Format: HMAC(secret, "{token_id}|{email_lower}").
    """
    email_norm = (email or "").strip().lower()
    payload = f"{token_id}|{email_norm}"
    mac = hmac.new(secret.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).digest()
    return _b64url(mac)

def sign_check(email: str, secret: str) -> str:
    """Sign a suppression check payload. Format: HMAC(secret, "check|{email_lower}")."""
    email_norm = (email or "").strip().lower()
    payload = f"check|{email_norm}"
    mac = hmac.new(secret.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).digest()
    return _b64url(mac)


def sign_stats(since_hours: int, secret: str) -> str:
    """Sign stats payload. Format: HMAC(secret, "stats|{since_hours}")."""
    payload = f"stats|{int(since_hours)}"
    mac = hmac.new(secret.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).digest()
    return _b64url(mac)


def generate_token_id() -> str:
    return secrets.token_urlsafe(24)


def create_unsub_token(email: str, campaign_id: str) -> str:
    secret = get_unsub_secret()
    if not secret:
        raise ValueError("UNSUB_SECRET is required to generate one-click tokens")

    token_id = generate_token_id()
    signature = sign_token(token_id, secret)
    signed_token = f"{token_id}.{signature}"

    store_unsub_token(token_id, email, campaign_id)
    return signed_token


def store_unsub_token(token_id: str, email: str, campaign_id: str) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    exists = UNSUB_TOKEN_STORE_PATH.exists()

    # Idempotent on token_id
    if exists:
        with open(UNSUB_TOKEN_STORE_PATH, "r", newline="", encoding="utf-8") as rf:
            reader = csv.DictReader(rf)
            for row in reader:
                if (row.get("token_id") or "").strip() == token_id:
                    return

    with open(UNSUB_TOKEN_STORE_PATH, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f, fieldnames=["token_id", "email", "campaign_id", "created_at"]
        )
        if not exists:
            writer.writeheader()
        writer.writerow(
            {
                "token_id": token_id,
                "email": email.strip().lower(),
                "campaign_id": campaign_id,
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
        )


def verify_unsub_token(signed_token: str) -> str | None:
    secret = get_unsub_secret()
    if not secret:
        return None
    if not signed_token or "." not in signed_token:
        return None
    token_id, sig = signed_token.split(".", 1)
    if not token_id or not sig:
        return None
    expected = sign_token(token_id, secret)
    if not hmac.compare_digest(expected, sig):
        return None
    return token_id


def lookup_email_for_token(token_id: str) -> str | None:
    if not UNSUB_TOKEN_STORE_PATH.exists():
        return None
    with open(UNSUB_TOKEN_STORE_PATH, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if (row.get("token_id") or "").strip() == token_id:
                email = (row.get("email") or "").strip().lower()
                return email or None
    return None


def ensure_suppression_header() -> None:
    if SUPPRESSION_PATH.exists():
        return
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    with open(SUPPRESSION_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["email", "reason", "source", "timestamp"])
        writer.writeheader()


def add_to_suppression(email: str, reason: str, source: str) -> bool:
    """Idempotently append email to suppression.csv. Returns True if added."""
    email_norm = (email or "").strip().lower()
    if not email_norm or "@" not in email_norm:
        return False

    ensure_suppression_header()

    # Check existing
    with open(SUPPRESSION_PATH, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if (row.get("email") or "").strip().lower() == email_norm:
                return False

    with open(SUPPRESSION_PATH, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["email", "reason", "source", "timestamp"])
        writer.writerow(
            {
                "email": email_norm,
                "reason": reason,
                "source": source,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
        )
    return True


def is_suppressed_email(email: str) -> bool:
    """Check if email exists in suppression.csv."""
    email_norm = (email or "").strip().lower()
    if not email_norm or "@" not in email_norm:
        return False
    if not SUPPRESSION_PATH.exists():
        return False
    with open(SUPPRESSION_PATH, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if (row.get("email") or "").strip().lower() == email_norm:
                return True
    return False
