import argparse
import csv
import hashlib
import html as _html
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode, urlparse
from zoneinfo import ZoneInfo

try:  # pragma: no cover
    from dotenv import load_dotenv

    load_dotenv()
except Exception:  # pragma: no cover
    pass

# When invoked as `py -3 outreach/generate_mailmerge.py`, sys.path[0] is `outreach/`.
# Add repo root so imports like `unsubscribe_utils` and `send_digest_email` resolve reliably.
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


REQUIRED_INPUT_COLUMNS = [
    "prospect_id",
    "first_name",
    "last_name",
    "firm",
    "title",
    "email",
    "state",
    "city",
    "territory_code",
    "source",
    "notes",
]

DEFAULT_REPLY_TO_EMAIL = "support@microflowops.com"
ERR_ONE_CLICK_REQUIRED = "ERR_ONE_CLICK_REQUIRED"
ERR_SUPPRESSION_REQUIRED = "ERR_SUPPRESSION_REQUIRED"
ET_TZ = ZoneInfo("America/New_York")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_csv_rows(path: str) -> list[dict]:
    with open(path, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = [dict(r) for r in reader]
    return rows


def _ledger_path() -> Path:
    data_dir = (os.getenv("DATA_DIR") or "").strip()
    base = Path(data_dir) if data_dir else (REPO_ROOT / "out")
    return base / "outreach_export_ledger.jsonl"


def _load_ledger_prospect_ids(path: Path) -> set[str]:
    seen: set[str] = set()
    if not path.exists():
        return seen
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = (line or "").strip()
            if not s:
                continue
            try:
                obj = json.loads(s)
            except Exception:
                continue
            pid = (obj.get("prospect_id") or "").strip()
            if pid:
                seen.add(pid)
    return seen


def _append_ledger_records(path: Path, records: list[dict]) -> None:
    if not records:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, separators=(",", ":"), ensure_ascii=True) + "\n")


def _validate_required_columns(rows: list[dict], path: str) -> None:
    if not rows:
        raise ValueError(f"input has no rows: {path}")
    missing = [c for c in REQUIRED_INPUT_COLUMNS if c not in rows[0]]
    if missing:
        raise ValueError(f"input missing required columns: {', '.join(missing)}")


def _norm_state(s: str) -> str:
    return (s or "").strip().upper()


def _norm_email(s: str) -> str:
    return (s or "").strip().lower()


def _slug_for_subscriber_key(text: str) -> str:
    # unsubscribe_server.py allows [A-Za-z0-9_.-]; notably it does NOT allow underscore.
    raw = (text or "").strip().lower()
    out = []
    for ch in raw:
        if ("a" <= ch <= "z") or ("0" <= ch <= "9") or ch in ".-":
            out.append(ch)
        else:
            out.append("-")
    slug = "".join(out).strip("-.")
    return slug or "outreach"


def _subscriber_key_from_prospect_id(prospect_id: str, territory_code: str) -> str:
    # Deterministic and stable; does not embed raw email.
    pid = (prospect_id or "").strip()
    terr = (territory_code or "").strip()
    digest = hashlib.sha256((pid + "|" + terr).encode("utf-8")).digest()
    token = hashlib.sha1(digest).hexdigest()[:16]  # short, stable, URL-safe
    terr_slug = _slug_for_subscriber_key(terr)
    key = f"outreach.{terr_slug}.{token}"
    return key[:80]


def _unsub_host_base() -> tuple[str, str]:
    """
    Returns (host_base, unsubscribe_path_url).
    - host_base: scheme://netloc
    - unsubscribe_path_url: full URL to /unsubscribe endpoint
    """
    raw = (os.getenv("UNSUB_ENDPOINT_BASE") or "").strip()
    if not raw:
        return "", ""

    u = urlparse(raw)
    if not u.scheme or not u.netloc:
        return "", ""

    host_base = f"{u.scheme}://{u.netloc}"
    # Operators sometimes set UNSUB_ENDPOINT_BASE to https://host/unsubscribe (see send_digest_email.py).
    if u.path and "unsubscribe" in u.path.lower():
        unsub_url = host_base + u.path
    else:
        unsub_url = host_base + "/unsubscribe"
    return host_base, unsub_url


def _one_click_config_present() -> tuple[bool, str]:
    """
    Returns (ok, reason_token).
    reason_token is stable for ops grep.
    """
    raw = (os.getenv("UNSUB_ENDPOINT_BASE") or "").strip()
    secret = (os.getenv("UNSUB_SECRET") or "").strip()
    if not raw:
        return False, "missing_unsub_endpoint_base"
    if not secret:
        return False, "missing_unsub_secret"
    u = urlparse(raw)
    if not u.scheme or not u.netloc:
        return False, "invalid_unsub_endpoint_base"
    return True, ""


def _read_template_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _render_template(template_text: str, mapping: dict[str, str]) -> str:
    body = template_text
    for k, v in mapping.items():
        body = body.replace(k, v)
    return body


def _html_escape(s: str) -> str:
    return _html.escape(s or "", quote=True)


def _truncate_text(s: str, max_len: int) -> str:
    text = (s or "").strip()
    if len(text) <= max_len:
        return text
    return text[: max(0, max_len - 1)].rstrip() + "…"


def _format_recent_signal_line(lead: dict) -> str:
    est = _truncate_text((lead.get("establishment_name") or "").strip() or "Unknown establishment", 52)
    city = (lead.get("site_city") or "").strip()
    state = (lead.get("site_state") or "").strip()
    itype = (lead.get("inspection_type") or "").strip()
    opened = (lead.get("date_opened") or "").strip()

    parts = [est]
    loc = ", ".join([p for p in [city, state] if p])
    if loc:
        parts.append(f"({loc})")
    if itype:
        parts.append(f"| {itype}")
    if opened:
        parts.append(f"| Opened {opened}")
    return " ".join(parts).strip()


def _format_dt_et(dt_utc: datetime) -> str:
    # Use an explicit "ET" token to keep copy stable across DST.
    return dt_utc.astimezone(ET_TZ).strftime("%Y-%m-%d %H:%M") + " ET"


def _best_effort_recent_leads_and_refresh(db_path: str, state: str, limit: int = 5) -> tuple[list[dict], str]:
    """
    Reuse the digest's underlying datastore (SQLite inspections table) to generate:
    - a short Recent signals lead list (top N)
    - last refresh timestamp (ET)

    This must be best-effort: outreach exports should not hard-fail if the inspections
    data isn't available (suppression + one-click are the compliance gates).
    """
    now_utc = datetime.now(timezone.utc)
    fallback_refresh = _format_dt_et(now_utc)

    try:
        p = Path(db_path)
        if not p.exists():
            return [], fallback_refresh

        import sqlite3

        conn = sqlite3.connect(str(p))
        try:
            cur = conn.cursor()
            cur.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='inspections' LIMIT 1")
            if not cur.fetchone():
                return [], fallback_refresh

            # Recent signals: use the same selector logic as the digest (get_leads_for_period).
            recent: list[dict] = []
            try:
                import send_digest_email as sde

                leads, low_fallback, _stats = sde.get_leads_for_period(
                    conn=conn,
                    states=[state],
                    since_days=14,
                    new_only_days=36500,
                    skip_first_seen_filter=True,
                    territory_code=None,
                    content_filter="high_medium",
                    include_low_fallback=True,
                    window_start=None,
                    new_only_cutoff=None,
                    include_changed=True,
                    use_opened_window=False,
                )
                selected = leads if leads else low_fallback
                recent = list(selected[: max(0, int(limit))])
            except Exception:
                recent = []

            # Last refresh: prefer changed_at/last_seen_at/first_seen_at max for the state.
            cols = set()
            try:
                cur.execute("PRAGMA table_info(inspections)")
                cols = {str(r[1]) for r in cur.fetchall() if len(r) > 1}
            except Exception:
                cols = set()

            time_cols = [c for c in ["changed_at", "last_seen_at", "first_seen_at"] if c in cols]
            ts = None
            for c in time_cols:
                try:
                    cur.execute(
                        f"SELECT MAX({c}) FROM inspections WHERE site_state = ? AND parse_invalid = 0",
                        (state,),
                    )
                    ts = cur.fetchone()[0]
                    if ts:
                        break
                except Exception:
                    continue

            refresh_dt = None
            if ts:
                try:
                    # send_digest_email parsing handles Z and multiple formats.
                    import send_digest_email as sde

                    parsed = sde._parse_timestamp(str(ts))  # type: ignore[attr-defined]
                    if parsed:
                        if parsed.tzinfo is None:
                            refresh_dt = parsed.replace(tzinfo=timezone.utc)
                        else:
                            refresh_dt = parsed.astimezone(timezone.utc)
                except Exception:
                    refresh_dt = None

            last_refresh = _format_dt_et(refresh_dt or now_utc)
            return recent, last_refresh
        finally:
            conn.close()
    except Exception:
        return [], fallback_refresh


def _recent_signals_text_lines_from_leads(leads: list[dict]) -> str:
    if not leads:
        return "- (no recent signals found)"
    out = []
    for lead in leads:
        out.append("- " + _format_recent_signal_line(lead))
    return "\n".join([line for line in out if line.strip()]) or "- (no recent signals found)"


def _recent_signals_html_from_leads(leads: list[dict]) -> str:
    if not leads:
        return "<div style=\"font-size: 13px; color: #666;\">(no recent signals found)</div>"
    try:
        import outbound_cold_email as oce

        parts = [oce.format_lead_for_html(lead) for lead in leads]
        html = "\n".join([p for p in parts if (p or "").strip()]).strip()
        return html or "<div></div>"
    except Exception:
        # Fallback: non-linked rows.
        items = []
        for lead in leads:
            line = _format_recent_signal_line(lead)
            if line:
                items.append(line)
        if not items:
            items = ["(no recent signals found)"]
        return "\n".join(f"<div style=\"font-size: 13px; color: #1a1a1a;\">{_html_escape(i)}</div>" for i in items)


def _resolve_outreach_mailing_address() -> str:
    """
    Outreach cold email must include a real physical address. Default to the proven
    Wally/cold-outreach address unless env provides a non-placeholder override.
    """
    default_addr = "11539 Links Dr, Reston, VA 20190"
    cand = (
        (os.getenv("MAIL_FOOTER_ADDRESS") or "").strip()
        or (os.getenv("MAILING_ADDRESS") or "").strip()
    )
    if not cand:
        return default_addr
    low = cand.lower()
    for ph in ["123 main street", "123 main st", "your address here", "suite 100", "example"]:
        if ph in low:
            return default_addr
    return cand


def _load_local_suppression_set() -> set[str]:
    """
    Load the local suppression set from the canonical suppression CSV.

    Compliance gate: exports must enforce suppression, so we fail if the file is missing.
    """
    sup_path = None
    try:
        import outbound_cold_email as oce
        sup_path = Path(getattr(oce, "SUPPRESSION_PATH"))
        if not sup_path.exists():
            raise ValueError(f"{ERR_SUPPRESSION_REQUIRED} suppression.csv missing path={sup_path}")
        return set(oce.load_suppression_list())
    except Exception:
        pass

    try:
        import unsubscribe_utils as uu
        sup_path = Path(getattr(uu, "SUPPRESSION_PATH"))
    except Exception:
        sup_path = Path("out") / "suppression.csv"

    if not sup_path.exists():
        raise ValueError(f"{ERR_SUPPRESSION_REQUIRED} suppression.csv missing path={sup_path}")

    suppressed: set[str] = set()
    with open(sup_path, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            email = (row.get("email") or "").strip().lower()
            if email:
                suppressed.add(email)
    return suppressed


def _check_db_suppression(db_path: str, email: str) -> bool:
    if not db_path:
        return False
    try:
        p = Path(db_path)
        if not p.exists():
            return False
    except Exception:
        return False

    try:
        from send_digest_email import check_suppression
        return bool(check_suppression(str(db_path), email))
    except Exception:
        return False


def _is_suppressed(email: str, local_suppression: set[str], db_path: str) -> bool:
    e = _norm_email(email)
    if not e or "@" not in e:
        return False
    if e in local_suppression:
        return True
    if _check_db_suppression(db_path, e):
        return True
    return False


def _deterministic_unsub_token(email: str, campaign_id: str, token_id_seed: str) -> str:
    """
    Deterministic one-click token so repeated exports for the same prospect_id can remain stable.
    Requires UNSUB_SECRET.
    """
    from unsubscribe_utils import sign_token, store_unsub_token

    secret = (os.getenv("UNSUB_SECRET") or "").strip()
    if not secret:
        raise ValueError("UNSUB_SECRET is required to generate one-click tokens")

    seed = (token_id_seed or "").strip()
    if not seed:
        raise ValueError("token_id_seed is required")

    # Token id must be URL-safe; use hex.
    token_id = hashlib.sha256(seed.encode("utf-8")).hexdigest()
    signature = sign_token(token_id, secret)
    signed_token = f"{token_id}.{signature}"
    store_unsub_token(token_id, email, campaign_id)
    return signed_token


def _build_urls(
    email: str,
    prospect_id: str,
    subscriber_key: str,
    territory_code: str,
    batch: str,
    allow_mailto_fallback: bool,
) -> tuple[str, str]:
    """
    Returns (unsubscribe_url, prefs_url).
    - unsubscribe_url: https one-click when configured; otherwise mailto fallback
    - prefs_url: /prefs page when configured; otherwise blank
    """
    reply_to = (os.getenv("REPLY_TO_EMAIL") or DEFAULT_REPLY_TO_EMAIL).strip()
    ok, reason = _one_click_config_present()
    host_base, unsub_endpoint = _unsub_host_base() if ok else ("", "")

    if not host_base or not unsub_endpoint:
        if not allow_mailto_fallback:
            raise ValueError(f"{ERR_ONE_CLICK_REQUIRED} {reason}".strip())
        # Mailto-only is acceptable only when explicitly enabled.
        if reply_to:
            return f"mailto:{reply_to}?{urlencode({'subject': 'unsubscribe'})}", ""
        return "", ""

    try:
        campaign_id = f"outreach|batch={batch}|terr={territory_code}|sk={subscriber_key}|pid={prospect_id}"
        token_seed = f"outreach|{territory_code}|{prospect_id}"
        signed = _deterministic_unsub_token(email=email, campaign_id=campaign_id, token_id_seed=token_seed)
        qs = urlencode({"token": signed, "subscriber_key": subscriber_key, "territory_code": territory_code})
        unsubscribe_url = f"{unsub_endpoint}?{qs}"
        prefs_url = f"{host_base}/prefs?{qs}"
        return unsubscribe_url, prefs_url
    except Exception:
        if not allow_mailto_fallback:
            raise ValueError(f"{ERR_ONE_CLICK_REQUIRED} token_generation_failed")
        if reply_to:
            return f"mailto:{reply_to}?{urlencode({'subject': 'unsubscribe'})}", ""
        return "", ""


def _ensure_parent_dir(path: str) -> None:
    Path(path).resolve().parent.mkdir(parents=True, exist_ok=True)


def _write_outbox_csv(path: str, rows: list[dict], fieldnames: list[str]) -> None:
    _ensure_parent_dir(path)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for r in rows:
            writer.writerow(r)


def _append_run_log(batch: str, payload: dict) -> str:
    runs_dir = Path("outreach") / "outreach_runs"
    runs_dir.mkdir(parents=True, exist_ok=True)
    date_part = datetime.now().strftime("%Y-%m-%d")
    safe_batch = "".join(ch if ch.isalnum() or ch in "._-" else "_" for ch in (batch or "batch")).strip("_") or "batch"
    path = runs_dir / f"{date_part}_{safe_batch}.jsonl"
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(payload, separators=(",", ":"), ensure_ascii=True) + "\n")
    return str(path)


def _manifest_path_for_outbox(out_path: str) -> str:
    p = Path(out_path)
    stem = p.stem if p.suffix else p.name
    name = f"{stem}_manifest.csv"
    return str(p.with_name(name))


def _write_manifest_csv(path: str, rows: list[dict]) -> None:
    _ensure_parent_dir(path)
    fields = ["ts_utc", "batch", "state", "prospect_id", "email", "status", "reason"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow(r)


def main() -> int:
    ap = argparse.ArgumentParser(description="Generate a mail-merge outbox CSV with dedupe + suppression enforcement.")
    ap.add_argument("--input", required=True, help="Input prospects CSV (see outreach/prospects_schema.md).")
    ap.add_argument("--batch", required=True, help="Batch id (e.g., TX_W2). Used in output and logs.")
    ap.add_argument("--state", required=True, help="2-letter state filter (e.g., TX).")
    ap.add_argument("--out", required=True, help="Output outbox CSV path.")
    ap.add_argument(
        "--db",
        default=str(Path("data") / "osha.sqlite"),
        help="Optional SQLite db path for suppression_list domain/email suppression (default: data/osha.sqlite).",
    )
    ap.add_argument(
        "--template",
        default=str(Path("outreach") / "outreach_plain.txt"),
        help="Plain-text template path.",
    )
    ap.add_argument(
        "--html-template",
        default=str(Path("outreach") / "outreach_card.html"),
        help="HTML template path (rendered into html_body).",
    )
    ap.add_argument(
        "--allow-mailto-fallback",
        action="store_true",
        help="Allow mailto-only opt-out links when one-click unsubscribe config is missing.",
    )
    ap.add_argument(
        "--allow-repeat",
        action="store_true",
        help="Allow re-exporting prospect_ids already present in the outreach export ledger.",
    )
    args = ap.parse_args()

    rows = _load_csv_rows(args.input)
    _validate_required_columns(rows, args.input)

    total_input = len(rows)
    state_filter = _norm_state(args.state)
    batch = (args.batch or "").strip()

    template_text = _read_template_text(Path(args.template))
    html_template_text = ""
    try:
        html_template_text = _read_template_text(Path(args.html_template))
    except Exception:
        html_template_text = ""

    # Precompute state-level snippets for template rendering.
    recent_leads, last_refresh_et = _best_effort_recent_leads_and_refresh(
        db_path=str(args.db),
        state=state_filter,
        limit=5,
    )
    recent_signals_lines = _recent_signals_text_lines_from_leads(recent_leads)
    recent_signals_html = _recent_signals_html_from_leads(recent_leads)
    try:
        # Compliance gate: must be present before we write any outputs.
        local_suppression = _load_local_suppression_set()
    except ValueError as e:
        msg = str(e or "").strip()
        if ERR_SUPPRESSION_REQUIRED in msg:
            print(msg, file=sys.stderr)
            return 3
        raise

    # Default: hard fail when one-click is not configured. This is a compliance/ops gate.
    if not args.allow_mailto_fallback:
        ok, reason = _one_click_config_present()
        if not ok:
            print(f"{ERR_ONE_CLICK_REQUIRED} {reason}".strip(), file=sys.stderr)
            return 2

    ledger_path = _ledger_path()
    existing_exported_ids = set() if args.allow_repeat else _load_ledger_prospect_ids(ledger_path)

    # Filter to state batch, normalize, and dedupe by normalized email (keep first).
    selected: list[dict] = []
    manifest_rows: list[dict] = []
    ts_utc = _utc_now_iso()
    for r in rows:
        row_state = _norm_state(r.get("state", ""))
        if row_state != state_filter:
            manifest_rows.append(
                {
                    "ts_utc": ts_utc,
                    "batch": batch,
                    "state": state_filter,
                    "prospect_id": (r.get("prospect_id") or "").strip(),
                    "email": _norm_email(r.get("email", "")),
                    "status": "dropped",
                    "reason": "state_filtered",
                }
            )
            continue
        selected.append(r)

    seen_emails: set[str] = set()
    deduped_dropped = 0
    unique_rows: list[dict] = []
    for r in selected:
        email_norm = _norm_email(r.get("email", ""))
        if not email_norm or "@" not in email_norm:
            manifest_rows.append(
                {
                    "ts_utc": ts_utc,
                    "batch": batch,
                    "state": state_filter,
                    "prospect_id": (r.get("prospect_id") or "").strip(),
                    "email": email_norm,
                    "status": "dropped",
                    "reason": "invalid_email",
                }
            )
            continue
        if email_norm in seen_emails:
            deduped_dropped += 1
            manifest_rows.append(
                {
                    "ts_utc": ts_utc,
                    "batch": batch,
                    "state": state_filter,
                    "prospect_id": (r.get("prospect_id") or "").strip(),
                    "email": email_norm,
                    "status": "dropped",
                    "reason": "deduped",
                }
            )
            continue
        seen_emails.add(email_norm)
        unique_rows.append(r)

    suppressed_dropped = 0
    ledger_dropped = 0
    exported: list[dict] = []
    ledger_records: list[dict] = []
    for r in unique_rows:
        prospect_id = (r.get("prospect_id") or "").strip()
        if prospect_id and prospect_id in existing_exported_ids:
            ledger_dropped += 1
            manifest_rows.append(
                {
                    "ts_utc": ts_utc,
                    "batch": batch,
                    "state": state_filter,
                    "prospect_id": prospect_id,
                    "email": _norm_email(r.get("email", "")),
                    "status": "dropped",
                    "reason": "already_exported",
                }
            )
            continue

        email = _norm_email(r.get("email", ""))
        if email and _is_suppressed(email, local_suppression, args.db):
            suppressed_dropped += 1
            manifest_rows.append(
                {
                    "ts_utc": ts_utc,
                    "batch": batch,
                    "state": state_filter,
                    "prospect_id": (r.get("prospect_id") or "").strip(),
                    "email": email,
                    "status": "dropped",
                    "reason": "suppressed",
                }
            )
            continue

        territory_code = batch  # outbound territory for this export
        subscriber_key = _subscriber_key_from_prospect_id(prospect_id, territory_code)
        try:
            unsub_url, prefs_url = _build_urls(
                email=email,
                prospect_id=prospect_id,
                subscriber_key=subscriber_key,
                territory_code=territory_code,
                batch=batch,
                allow_mailto_fallback=bool(args.allow_mailto_fallback),
            )
        except ValueError as e:
            msg = str(e or "").strip()
            if msg.startswith(ERR_ONE_CLICK_REQUIRED):
                print(msg, file=sys.stderr)
                return 2
            raise

        first_name = (r.get("first_name") or "").strip()
        firm = (r.get("firm") or "").strip() or "your firm"
        prefs_link = prefs_url or unsub_url or ""

        subject = f"{state_filter} OSHA activity signals - {firm}".strip()
        text_body = _render_template(
            template_text,
            {
                "FIRST_NAME": first_name or "there",
                "FIRM": firm,
                "STATE": state_filter,
                "TERRITORY_CODE": territory_code,
                "RECENT_SIGNALS_LINES": recent_signals_lines,
                "LAST_REFRESH_ET": last_refresh_et,
                "UNSUBSCRIBE_URL": unsub_url or "",
                "PREFS_URL": prefs_url or prefs_link,
            },
        ).strip() + "\n"

        # Default to a simple HTML rendering if the template is missing.
        mailing_address = _resolve_outreach_mailing_address()
        html_body = ""
        if html_template_text.strip():
            microflowops_url = (os.getenv("MICROFLOWOPS_URL") or "https://microflowops.com").strip() or "https://microflowops.com"
            html_body = _render_template(
                html_template_text,
                {
                    "{{FIRST_NAME}}": _html_escape(first_name or "there"),
                    "{{FIRM}}": _html_escape(firm),
                    "{{STATE}}": _html_escape(state_filter),
                    "{{RECENT_SIGNALS_HTML}}": recent_signals_html,
                    "{{LAST_REFRESH_ET}}": _html_escape(last_refresh_et),
                    "{{UNSUBSCRIBE_URL}}": _html_escape(unsub_url or prefs_link),
                    "{{PREFS_URL}}": _html_escape(prefs_url or prefs_link),
                    "{{MAILING_ADDRESS}}": _html_escape(mailing_address),
                    "{{MICROFLOWOPS_URL}}": _html_escape(microflowops_url),
                },
            ).strip()
        else:
            # Keep this short; send_test_cold_email can always fall back to <pre> conversion too.
            html_body = (
                "<div style=\"font-family: system-ui, -apple-system, 'Segoe UI', Roboto, Arial, sans-serif;\">"
                "<pre style=\"white-space: pre-wrap; font-size: 13px; line-height: 1.4;\">"
                + _html_escape(text_body)
                + "</pre></div>"
            )

        out_row = dict(r)
        out_row.update(
            {
                "batch": batch,
                "territory_code": territory_code,
                "subscriber_key": subscriber_key,
                "unsubscribe_url": unsub_url,
                "prefs_url": prefs_url,
                "subject": subject,
                # Back-compat alias: `body` remains the same as text_body.
                "body": text_body,
                "text_body": text_body,
                "html_body": html_body,
            }
        )
        exported.append(out_row)
        manifest_rows.append(
            {
                "ts_utc": ts_utc,
                "batch": batch,
                "state": state_filter,
                "prospect_id": prospect_id,
                "email": email,
                "status": "exported",
                "reason": "",
            }
        )
        if prospect_id:
            ledger_records.append(
                {
                    "prospect_id": prospect_id,
                    "batch": batch,
                    "state": state_filter,
                    "exported_at_utc": _utc_now_iso(),
                }
            )

    out_fields = REQUIRED_INPUT_COLUMNS + [
        "batch",
        "subscriber_key",
        "unsubscribe_url",
        "prefs_url",
        "subject",
        "body",
        "text_body",
        "html_body",
    ]
    _write_outbox_csv(args.out, exported, out_fields)

    manifest_path = _manifest_path_for_outbox(args.out)
    _write_manifest_csv(manifest_path, manifest_rows)
    _append_ledger_records(ledger_path, ledger_records)

    run_payload = {
        "ts_utc": _utc_now_iso(),
        "batch": batch,
        "state": state_filter,
        "input_path": str(args.input),
        "out_path": str(args.out),
        "manifest_path": str(manifest_path),
        "db_path": str(args.db),
        "ledger_path": str(ledger_path),
        "counts": {
            "total_input": int(total_input),
            "state_selected": int(len(selected)),
            "deduped_dropped": int(deduped_dropped),
            "ledger_dropped": int(ledger_dropped),
            "suppressed_dropped": int(suppressed_dropped),
            "exported": int(len(exported)),
        },
    }
    log_path = _append_run_log(batch, run_payload)

    print(f"total_input={total_input}")
    print(f"deduped={deduped_dropped}")
    print(f"already_exported={ledger_dropped}")
    print(f"suppressed={suppressed_dropped}")
    print(f"exported={len(exported)}")
    print(f"ledger={ledger_path}")
    print(f"run_log={log_path}")
    print(f"outbox={args.out}")
    print(f"manifest={manifest_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
