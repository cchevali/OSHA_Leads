import base64
import contextlib
import csv
import hashlib
import hmac
import os
import secrets
import sys
from datetime import datetime, timezone
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent.resolve()

DATA_DIR = os.getenv("DATA_DIR", "").strip()
OUT_DIR = Path(DATA_DIR) if DATA_DIR else (SCRIPT_DIR / "out")
UNSUB_TOKEN_STORE_PATH = OUT_DIR / "unsub_tokens.csv"
SUPPRESSION_PATH = OUT_DIR / "suppression.csv"
UNSUBSCRIBE_EVENTS_PATH = OUT_DIR / "unsubscribe_events.csv"
PREFS_PATH = OUT_DIR / "prefs.csv"

try:  # pragma: no cover
    import fcntl  # type: ignore
except Exception:  # pragma: no cover
    fcntl = None

try:  # pragma: no cover
    import msvcrt  # type: ignore
except Exception:  # pragma: no cover
    msvcrt = None

_WARN_ONCE_KEYS: set[str] = set()


def _warn_once(key: str, message: str) -> None:
    if key in _WARN_ONCE_KEYS:
        return
    _WARN_ONCE_KEYS.add(key)
    print(f"[WARN] {message}", file=sys.stderr)


@contextlib.contextmanager
def _exclusive_lock(lock_path: Path):
    """
    Cross-platform advisory lock using a sidecar .lock file.

    - POSIX: fcntl.flock
    - Windows: msvcrt.locking
    """
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    f = open(lock_path, "a+", encoding="utf-8")
    try:
        if fcntl is not None:  # pragma: no cover
            fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        elif msvcrt is not None:  # pragma: no cover
            # Lock the first byte. Ensure file has at least 1 byte.
            f.seek(0, os.SEEK_END)
            if f.tell() == 0:
                f.write("0")
                f.flush()
            f.seek(0)
            msvcrt.locking(f.fileno(), msvcrt.LK_LOCK, 1)
        yield
    finally:
        try:
            if fcntl is not None:  # pragma: no cover
                fcntl.flock(f.fileno(), fcntl.LOCK_UN)
            elif msvcrt is not None:  # pragma: no cover
                f.seek(0)
                msvcrt.locking(f.fileno(), msvcrt.LK_UNLCK, 1)
        except Exception:
            pass
        try:
            f.close()
        except Exception:
            pass


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
        event_ts = datetime.now(timezone.utc).isoformat()
        writer.writerow(
            {
                "email": email_norm,
                "reason": reason,
                "source": source,
                "timestamp": event_ts,
            }
        )

    append_unsubscribe_event(email_norm, reason, source, event_ts)
    return True


def append_unsubscribe_event(email: str, reason: str, source: str, timestamp: str | None = None) -> None:
    """Append unsubscribe/suppression event log (append-only)."""
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    exists = UNSUBSCRIBE_EVENTS_PATH.exists()
    ts = timestamp or datetime.now(timezone.utc).isoformat()

    with open(UNSUBSCRIBE_EVENTS_PATH, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["timestamp", "email", "event_type", "reason", "source"],
        )
        if not exists:
            writer.writeheader()
        writer.writerow(
            {
                "timestamp": ts,
                "email": email,
                "event_type": "suppression_added",
                "reason": reason,
                "source": source,
            }
        )


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


def _normalize_territory(value: str | None) -> str:
    text = (value or "").strip()
    return text.upper()


def ensure_prefs_header(prefs_path: Path | None = None) -> Path:
    """
    Ensure prefs.csv exists with header.

    Schema (current-state, upserted):
      email,territory,include_lows,updated_at,source
    """
    path = Path(prefs_path) if prefs_path else PREFS_PATH
    if path.exists():
        return path
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["email", "territory", "include_lows", "updated_at", "source"],
        )
        writer.writeheader()
    return path


def set_include_lows_pref(
    email: str,
    territory: str,
    include_lows: bool,
    source: str = "one_click",
    prefs_path: Path | None = None,
) -> None:
    """Upsert include_lows preference for (email, territory)."""
    email_norm = (email or "").strip().lower()
    territory_norm = _normalize_territory(territory)
    if not email_norm or "@" not in email_norm:
        raise ValueError("invalid email")
    if not territory_norm:
        raise ValueError("invalid territory")

    path = Path(prefs_path) if prefs_path else PREFS_PATH
    lock_path = path.parent / (path.name + ".lock")
    with _exclusive_lock(lock_path):
        path = ensure_prefs_header(path)
        rows: list[dict] = []
        found = False
        if path.exists():
            with open(path, "r", newline="", encoding="utf-8") as rf:
                reader = csv.DictReader(rf)
                for row in reader:
                    if not row:
                        continue
                    row_email = (row.get("email") or "").strip().lower()
                    row_terr = _normalize_territory(row.get("territory"))
                    if row_email == email_norm and row_terr == territory_norm:
                        row = dict(row)
                        row["include_lows"] = "true" if include_lows else "false"
                        row["updated_at"] = datetime.now(timezone.utc).isoformat()
                        row["source"] = (source or "").strip() or "unknown"
                        found = True
                    rows.append(row)

        if not found:
            rows.append(
                {
                    "email": email_norm,
                    "territory": territory_norm,
                    "include_lows": "true" if include_lows else "false",
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                    "source": (source or "").strip() or "unknown",
                }
            )

        tmp_path = path.with_suffix(path.suffix + ".tmp")
        with open(tmp_path, "w", newline="", encoding="utf-8") as wf:
            writer = csv.DictWriter(
                wf,
                fieldnames=["email", "territory", "include_lows", "updated_at", "source"],
            )
            writer.writeheader()
            for row in rows:
                writer.writerow(
                    {
                        "email": (row.get("email") or "").strip().lower(),
                        "territory": _normalize_territory(row.get("territory")),
                        "include_lows": "true"
                        if str(row.get("include_lows") or "").strip().lower() in {"1", "true", "yes"}
                        else "false",
                        "updated_at": (row.get("updated_at") or "").strip(),
                        "source": (row.get("source") or "").strip(),
                    }
                )
        tmp_path.replace(path)


def get_include_lows_pref(
    email: str,
    territory: str,
    prefs_path: Path | None = None,
) -> bool:
    """Return include_lows for (email, territory). Defaults to False when missing."""
    email_norm = (email or "").strip().lower()
    territory_norm = _normalize_territory(territory)
    if not email_norm or "@" not in email_norm:
        return False
    if not territory_norm:
        return False

    path = Path(prefs_path) if prefs_path else PREFS_PATH
    if not path.exists():
        return False
    try:
        with open(path, "r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if not row:
                    continue
                row_email = (row.get("email") or "").strip().lower()
                row_terr = _normalize_territory(row.get("territory"))
                if row_email == email_norm and row_terr == territory_norm:
                    val = str(row.get("include_lows") or "").strip().lower()
                    return val in {"1", "true", "yes"}
    except Exception as exc:
        _warn_once("prefs_read_failed", f"prefs.csv unreadable ({path}): {exc}. Defaulting include_lows=false.")
        return False
    return False
