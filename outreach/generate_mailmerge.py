import argparse
import csv
import hashlib
import html as _html
import json
import os
import sys
from collections import Counter
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

from scoring import osha_detail_cache as scoring_osha_detail_cache
from scoring import paths as scoring_paths
from scoring import triage_overlay as scoring_triage_overlay

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
DEFAULT_SAMPLE_FEED_URL = "https://microflowops.com/sample"
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


def _bool_env_enabled(name: str, default: bool = False) -> bool:
    raw = (os.getenv(name) or "").strip().lower()
    if not raw:
        return bool(default)
    return raw in {"1", "true", "yes", "on"}


def _outreach_triage_artifact_path(batch: str, dry_run_suffix: str) -> Path:
    safe_batch = "".join(ch if ch.isalnum() or ch in "._-" else "_" for ch in (batch or "batch")).strip("_") or "batch"
    return scoring_paths.data_root() / "outreach" / safe_batch / f"signals_triage_{safe_batch}_{dry_run_suffix}.json"


def _relpath_to_data_root(path: Path) -> str:
    root = scoring_paths.data_root()
    try:
        return str(path.resolve(strict=False).relative_to(root.resolve(strict=False)))
    except Exception:
        return str(path)


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
    for k in sorted(mapping.keys(), key=len, reverse=True):
        body = body.replace(k, mapping[k])
    return body


def _html_escape(s: str) -> str:
    return _html.escape(s or "", quote=True)


def _truncate_text(s: str, max_len: int) -> str:
    text = (s or "").strip()
    if len(text) <= max_len:
        return text
    return text[: max(0, max_len - 1)].rstrip() + "…"


STATE_FULL_NAMES = {
    "TX": "Texas",
    "CA": "California",
    "FL": "Florida",
}

STATE_METRO_EXAMPLES = {
    "TX": "Houston, DFW",
    "CA": "Los Angeles, Inland Empire",
    "FL": "Miami, Orlando",
}

SAMPLE_SIGNALS = [
    {
        "establishment_name": "Sample Industrial Services",
        "site_city": "Example City",
        "site_state": "",
        "inspection_type": "Accident",
        "date_opened": "2026-02-01",
        "first_seen_at": "2026-02-02T00:00:00Z",
        "lead_score": 10,
    },
    {
        "establishment_name": "Sample Roofing Group",
        "site_city": "Example City",
        "site_state": "",
        "inspection_type": "Complaint",
        "date_opened": "2026-01-28",
        "first_seen_at": "2026-01-29T00:00:00Z",
        "lead_score": 8,
    },
    {
        "establishment_name": "Sample Mechanical LLC",
        "site_city": "Example City",
        "site_state": "",
        "inspection_type": "Referral",
        "date_opened": "2026-01-24",
        "first_seen_at": "2026-01-25T00:00:00Z",
        "lead_score": 6,
    },
]


def _state_full_name(state: str) -> str:
    s = _norm_state(state)
    return STATE_FULL_NAMES.get(s, s or "your state")


def _state_metro_examples(state: str) -> str:
    s = _norm_state(state)
    return STATE_METRO_EXAMPLES.get(s, "your target metros")


def _segment_descriptor(segment: str, role_or_title: str) -> str:
    text = " ".join([str(segment or "").strip().lower(), str(role_or_title or "").strip().lower()]).strip()
    if not text:
        return ""
    if any(token in text for token in ["attorney", "law", "lawyer", "counsel", "defense", "partner"]):
        return "defense team"
    if any(token in text for token in ["safety consultant", "consulting"]):
        return "safety consulting team"
    if any(token in text for token in ["ehs", "hse", "safety"]):
        return "safety team"
    if "compliance" in text:
        return "compliance team"
    return ""


def _clean_company_name(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    normalized = " ".join(text.lower().split())
    if normalized in {"your firm", "your company", "company", "unknown", "n/a", "na", "none", "-", "--"}:
        return ""
    suffix_only = {
        "inc",
        "llc",
        "co",
        "company",
        "corp",
        "ltd",
        "lp",
    }
    normalized_token = "".join(ch for ch in normalized if ch.isalnum())
    if normalized_token in suffix_only:
        return ""
    return text


def _build_copy_tokens(
    *,
    state_full_name: str,
    state_metro_examples: str,
    first_name: str,
    firm_name: str,
    segment: str,
    role_or_title: str,
    recent_leads: list[dict] | None = None,
) -> dict[str, str]:
    signal_count = len(list(recent_leads or []))
    segment_desc = _segment_descriptor(segment=segment, role_or_title=role_or_title)
    clean_first = str(first_name or "").strip()
    clean_firm = _clean_company_name(firm_name)
    low_signal = signal_count == 1

    if clean_first:
        greeting_text = f"Hi {clean_first},"
        count_phrase = "a new OSHA inspection" if low_signal else "a few new OSHA inspections"
        opened_phrase = "opened recently" if low_signal else "most opened in the last two weeks"
        team_phrase = f"your team at {clean_firm}" if clean_firm else "your team"
        intro_text = (
            f"I spotted {count_phrase} in {state_full_name} that {team_phrase} might want to know about — "
            f"{opened_phrase} and none have citations yet:"
        )
        post_cards_text = ""
        trial_text = (
            "I track these daily across every state using public OSHA data. "
            "Happy to set up a short trial feed for whatever metros matter to you — just reply with the cities."
        )
    elif clean_firm:
        greeting_text = f"Hi - saw a few things {clean_firm} should probably have on their radar:"
        intro_text = ""
        post_cards_text = (
            f"These are new OSHA inspections opened in {state_full_name} in the last two weeks — "
            "none have citations yet."
        )
        trial_text = (
            "I track these daily across every state using public OSHA data. "
            "Happy to set up a trial feed for whatever metros matter to you — just reply with the cities."
        )
    else:
        greeting_text = (
            f"Hi - saw a new OSHA inspection in {state_full_name} that might be relevant to your team:"
            if low_signal
            else f"Hi - saw a few new OSHA inspections in {state_full_name} that might be relevant to your team:"
        )
        intro_text = ""
        post_cards_text = (
            "Opened recently and none have citations yet."
            if low_signal
            else "Most opened in the last two weeks and none have citations yet."
        )
        trial_text = (
            "I track these daily across every state using public OSHA data. "
            "Happy to set up a trial feed for whatever metros matter to you — just reply with the cities."
        )

    return {
        "SIGNAL_COUNT": str(max(0, signal_count)),
        "SEGMENT_DESCRIPTOR": segment_desc,
        "GREETING_LINE_TEXT": greeting_text,
        "GREETING_LINE_HTML": _html_escape(greeting_text),
        "INTRO_LINE_TEXT": intro_text,
        "INTRO_LINE_HTML": _html_escape(intro_text),
        "POST_CARDS_LINE_TEXT": post_cards_text,
        "POST_CARDS_LINE_HTML": _html_escape(post_cards_text),
        "TRIAL_LINE_TEXT": trial_text,
        "TRIAL_LINE_HTML": _html_escape(trial_text),
        # Legacy keys retained to avoid breaking custom templates.
        "OPENING_LINE_TEXT": intro_text,
        "OPENING_LINE_HTML": _html_escape(intro_text),
        "RELEVANCE_LINE_TEXT": "",
        "RELEVANCE_LINE_HTML": "",
        "CTA_LINE_TEXT": trial_text,
        "CTA_LINE_HTML": _html_escape(trial_text),
        "TRUST_LINE_TEXT": "",
        "TRUST_LINE_HTML": "",
        "COMPANY_INTRO_TEXT": "",
        "COMPANY_INTRO_HTML": "",
        "STATE_METRO_EXAMPLES_TEXT": state_metro_examples,
    }


def _row_text_value(row: dict, keys: list[str]) -> str:
    for key in keys:
        value = str((row or {}).get(key) or "").strip()
        if value:
            return value
    return ""


def _date_str(value: object) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if len(text) >= 10 and text[4] == "-" and text[7] == "-":
        return text[:10]
    try:
        normalized = text.replace("Z", "+00:00")
        return datetime.fromisoformat(normalized).date().isoformat()
    except Exception:
        return ""


def _observed_date(lead: dict) -> str:
    for key in ["first_seen_at", "changed_at", "last_seen_at"]:
        observed = _date_str(lead.get(key))
        if observed:
            return observed
    return _date_str(lead.get("date_opened"))


def _subject_short_date(value: object) -> str:
    date_value = _date_str(value)
    if not date_value:
        return ""
    try:
        dt = datetime.strptime(date_value, "%Y-%m-%d")
    except Exception:
        return str(date_value)
    return f"{dt.strftime('%b')} {dt.day}"


def _subject_opened_or_observed_date(lead: dict) -> str:
    opened = _subject_short_date(lead.get("date_opened"))
    if opened:
        return opened
    for key in ["first_seen_at", "changed_at", "last_seen_at"]:
        label = _subject_short_date(lead.get(key))
        if label:
            return label
    observed = _subject_short_date(_observed_date(lead))
    return observed or "recently"


def _format_recent_signal_line(lead: dict) -> str:
    est = _truncate_text((lead.get("establishment_name") or "").strip() or "Unknown establishment", 52)
    city = (lead.get("site_city") or "").strip()
    state = (lead.get("site_state") or "").strip()
    itype = (lead.get("inspection_type") or "").strip()
    opened = (lead.get("date_opened") or "").strip()
    observed = _observed_date(lead)

    parts = [est]
    loc = ", ".join([p for p in [city, state] if p])
    if loc:
        parts.append(f"({loc})")
    if itype:
        parts.append(f"| {itype}")
    if opened:
        parts.append(f"| Opened {opened}")
    if observed:
        parts.append(f"| Observed {observed}")
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

                leads, _low_fallback, _stats = sde.get_leads_for_period(
                    conn=conn,
                    states=[state],
                    since_days=14,
                    new_only_days=36500,
                    skip_first_seen_filter=True,
                    territory_code=None,
                    content_filter="all",
                    include_low_fallback=False,
                    window_start=None,
                    new_only_cutoff=None,
                    include_changed=True,
                    use_opened_window=False,
                )
                recent = list((leads or [])[: max(0, int(limit))])
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
        return ""
    out = []
    for lead in leads:
        out.append("- " + _format_recent_signal_line(lead))
    return "\n".join([line for line in out if line.strip()])


def _recent_signals_html_from_leads(leads: list[dict]) -> str:
    if not leads:
        return ""
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
        return "\n".join(f"<div style=\"font-size: 13px; color: #1a1a1a;\">{_html_escape(i)}</div>" for i in items)


def _inspections_columns(conn) -> set[str]:
    try:
        rows = conn.execute("PRAGMA table_info(inspections)").fetchall()
    except Exception:
        return set()
    return {str(r[1]) for r in rows if len(r) > 1}


def _history_rows_for_state(db_path: str, state: str, limit: int = 3) -> tuple[list[dict], str]:
    try:
        p = Path(db_path)
        if not p.exists():
            return [], "NONE"

        import sqlite3

        conn = sqlite3.connect(str(p))
        conn.row_factory = sqlite3.Row
        try:
            row = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='inspections' LIMIT 1"
            ).fetchone()
            if not row:
                return [], "NONE"

            cols = _inspections_columns(conn)
            if "site_state" not in cols:
                return [], "NONE"

            select_cols = []
            for col in [
                "establishment_name",
                "site_city",
                "site_state",
                "mail_state",
                "inspection_type",
                "date_opened",
                "first_seen_at",
                "last_seen_at",
                "changed_at",
                "lead_score",
                "source_url",
                "activity_nr",
            ]:
                if col in cols:
                    select_cols.append(col)
                else:
                    select_cols.append(f"'' AS {col}")

            where_parts = ["UPPER(COALESCE(site_state, '')) = ?"]
            if "parse_invalid" in cols:
                where_parts.append("COALESCE(parse_invalid, 0) = 0")
            where_clause = " AND ".join(where_parts)
            query = (
                "SELECT "
                + ", ".join(select_cols)
                + " FROM inspections WHERE "
                + where_clause
                + " ORDER BY COALESCE(date_opened, '') DESC, COALESCE(last_seen_at, '') DESC LIMIT ?"
            )
            rows = conn.execute(query, (_norm_state(state), max(1, int(limit)))).fetchall()
            out = [dict(r) for r in rows]
            max_date = "NONE"
            if "date_opened" in cols:
                max_row = conn.execute(
                    "SELECT MAX(date_opened) FROM inspections WHERE " + where_clause,
                    (_norm_state(state),),
                ).fetchone()
                max_date = str((max_row[0] if max_row else "") or "").strip() or "NONE"
            return out, max_date
        finally:
            conn.close()
    except Exception:
        return [], "NONE"


def _sample_rows() -> list[dict]:
    return [dict(r) for r in SAMPLE_SIGNALS]


def _subject_source_rows(db_path: str | None, state: str, recent_leads: list[dict] | None) -> list[dict]:
    if recent_leads:
        return list(recent_leads)
    if db_path:
        historical_rows, _max_opened = _history_rows_for_state(db_path=db_path, state=state, limit=3)
        if historical_rows:
            return list(historical_rows)
    return []


def _normalized_inspection_type(value: object) -> str:
    text = " ".join(str(value or "").strip().split()).lower()
    if not text:
        return ""
    return text


def _subject_primary_inspection_type(leads: list[dict] | None) -> str:
    labels = [_normalized_inspection_type((lead or {}).get("inspection_type")) for lead in (leads or [])]
    labels = [label for label in labels if label]
    if not labels:
        return ""
    counts = Counter(labels)
    if len(counts) == 1:
        return next(iter(counts))
    top_count = max(counts.values())
    winners = [label for label, count in counts.items() if count == top_count]
    if len(winners) != 1:
        return ""
    return winners[0]


def _truncate_subject(subject: str, max_len: int = 64) -> str:
    text = str(subject or "").strip()
    if len(text) <= max_len:
        return text
    return _truncate_text(text, max_len)


def _subject_for_multi_signal(*, signal_count: int, state_abbrev: str, primary_type: str) -> str:
    type_part = f" {primary_type}" if primary_type else ""
    candidates = [
        f"Quick heads up — {signal_count} new {state_abbrev}{type_part} inspections opened this month",
        f"Quick heads up — {signal_count} new {state_abbrev}{type_part} inspections opened",
        f"Quick heads up — {signal_count} new {state_abbrev}{type_part} inspections",
    ]
    if primary_type:
        candidates.extend(
            [
                f"Quick heads up — {signal_count} new {state_abbrev} inspections opened this month",
                f"Quick heads up — {signal_count} new {state_abbrev} inspections opened",
                f"Quick heads up — {signal_count} new {state_abbrev} inspections",
            ]
        )
    candidates.append(f"Heads up — {signal_count} new {state_abbrev} inspections")
    for candidate in candidates:
        if len(candidate) <= 64:
            return candidate
    return _truncate_subject(candidates[-1], max_len=64)


def _subject_for_single_signal(*, state_abbrev: str, opened_label: str) -> str:
    candidates = [
        f"Quick heads up — new {state_abbrev} inspection opened {opened_label}",
        f"Quick heads up — new {state_abbrev} inspection opened recently",
        f"Heads up — new {state_abbrev} inspection opened recently",
    ]
    for candidate in candidates:
        if len(candidate) <= 64:
            return candidate
    return _truncate_subject(candidates[-1], max_len=64)


def build_outreach_subject(
    state_or_label: str,
    recent_leads: list[dict] | None = None,
    db_path: str | None = None,
    segment_descriptor: str = "",
    state_full_name: str | None = None,
    signal_count: int | None = None,
) -> str:
    del segment_descriptor, state_full_name
    label = (_norm_state(state_or_label) or str(state_or_label or "").strip().upper() or "OSHA").strip()
    try:
        subject_signal_count = int(signal_count) if signal_count is not None else len(list(recent_leads or []))
    except Exception:
        subject_signal_count = len(list(recent_leads or []))
    subject_signal_count = max(0, int(subject_signal_count))
    source_rows = _subject_source_rows(db_path=db_path, state=label, recent_leads=recent_leads)
    subject_rows = list(recent_leads or []) or source_rows
    if subject_signal_count >= 2:
        primary_type = _subject_primary_inspection_type(subject_rows)
        return _subject_for_multi_signal(
            signal_count=subject_signal_count,
            state_abbrev=label,
            primary_type=primary_type,
        )
    row_for_date = subject_rows[0] if subject_rows else {}
    opened_label = _subject_opened_or_observed_date(row_for_date) if row_for_date else "recently"
    return _subject_for_single_signal(state_abbrev=label, opened_label=opened_label)


def _build_signal_template_tokens(db_path: str, state: str, recent_leads: list[dict], lookback_days: int = 14) -> dict[str, str]:
    del db_path, lookback_days
    state_code = _norm_state(state)
    state_name = _state_full_name(state_code)

    recent_text = _recent_signals_text_lines_from_leads(recent_leads)
    recent_html = _recent_signals_html_from_leads(recent_leads)

    tokens = {
        "STATE_FULL_NAME": state_name,
        "STATE_METRO_EXAMPLES": _state_metro_examples(state_code),
        "RECENT_SIGNALS_LINES": recent_text,
        "RECENT_SIGNALS_HTML": recent_html,
        "SIGNALS_WINDOW_NOTE_TEXT": "",
        "SIGNALS_WINDOW_NOTE_HTML": "",
        "SIGNALS_FALLBACK_TEXT": "",
        "SIGNALS_FALLBACK_HTML": "",
    }
    return tokens


def _triage_recent_signals_for_outreach(
    *,
    batch: str,
    recent_leads: list[dict],
    dry_run_suffix: str = "export",
) -> tuple[list[dict], dict]:
    overlay_enabled = _bool_env_enabled("OUTREACH_TRIAGE_OVERLAY_ENABLED", default=False)
    items = [
        {"activity_nr": str(r.get("activity_nr") or "").strip(), "url": str(r.get("source_url") or "").strip()}
        for r in (recent_leads or [])
        if str(r.get("activity_nr") or "").strip()
    ]
    cache_result = {"fetched": 0, "skipped_cached": 0, "failed": 0}
    try:
        cache_rows = scoring_osha_detail_cache.load_detail_cache_rows(None, [str(r.get("activity_nr") or "") for r in (recent_leads or [])])
        decisions = scoring_triage_overlay.triage(
            list(recent_leads or []),
            cache_rows,
            mode="outreach_examples",
            allow_ai=bool(overlay_enabled),
        )
    except Exception:
        decisions = []
    by_key = scoring_triage_overlay.decisions_by_activity(decisions)
    high_rows: list[dict] = []
    medium_rows: list[dict] = []
    low_rows: list[dict] = []
    suppressed = 0
    for row in list(recent_leads or []):
        key = str(row.get("activity_nr") or row.get("lead_key") or "").strip()
        d = by_key.get(key, {})
        final_priority = str(d.get("final_priority") or "").strip().upper()
        if not final_priority:
            action_hint = str(d.get("action") or "").strip().lower()
            if action_hint == "remove_from_customer_email":
                final_priority = "SUPPRESS"
            elif action_hint == "downgrade_to_medium":
                final_priority = "MEDIUM"
            elif action_hint == "downgrade_to_low":
                final_priority = "LOW"
            elif action_hint == "promote_candidate":
                final_priority = "HIGH"
            else:
                current_hint = str(d.get("current_priority") or "").strip().upper()
                if current_hint in {"HIGH", "MEDIUM", "LOW"}:
                    final_priority = current_hint
        if final_priority == "SUPPRESS":
            suppressed += 1
            continue
        if final_priority == "HIGH":
            high_rows.append(row)
        elif final_priority == "MEDIUM":
            medium_rows.append(row)
        elif final_priority == "LOW":
            low_rows.append(row)
        else:
            # Fallback to score tier when decision missing.
            try:
                score = int(row.get("lead_score") or 0)
            except Exception:
                score = 0
            if score >= 10:
                high_rows.append(row)
            elif score >= 6:
                medium_rows.append(row)
            else:
                low_rows.append(row)

    # Outreach examples are HIGH-first with MEDIUM backfill; LOW never shown.
    filtered = list(high_rows) + list(medium_rows)
    action, conf, reasons = scoring_triage_overlay.summarize_outreach_example_triage(decisions)
    if not overlay_enabled:
        action = "AI_DISABLED"
        conf = ""
        reasons = ""
    artifact_path = _outreach_triage_artifact_path(batch=batch, dry_run_suffix=dry_run_suffix)
    relpath = _relpath_to_data_root(artifact_path)
    print(
        "OUTREACH_TRIAGE_OVERLAY "
        f"enabled={1 if overlay_enabled else 0} recent_before={len(recent_leads or [])} recent_after={len(filtered)} "
        f"suppressed={suppressed} high={len(high_rows)} medium={len(medium_rows)} low_hidden={len(low_rows)} "
        f"cache_fetched={int(cache_result.get('fetched', 0))} "
        f"cache_skipped={int(cache_result.get('skipped_cached', 0))} "
        f"cache_failed={int(cache_result.get('failed', 0))}"
    )
    print(f"OUTREACH_CARD_EXAMPLES high={len(high_rows)} medium_backfill={len(medium_rows)} total={len(filtered)}")
    return filtered, {
        "enabled": bool(overlay_enabled),
        "ai_triage_action": action,
        "ai_triage_conf": conf,
        "ai_triage_reasons": reasons,
        "ai_triage_details_relpath": relpath if overlay_enabled else "",
        "decisions": decisions,
        "artifact_path": artifact_path if overlay_enabled else None,
    }


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
    fields = [
        "ts_utc",
        "batch",
        "state",
        "prospect_id",
        "email",
        "status",
        "reason",
        "ai_triage_action",
        "ai_triage_conf",
        "ai_triage_reasons",
        "ai_triage_details_relpath",
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow(r)


def _preview_text(text: str, max_lines: int = 14) -> str:
    lines = [ln.rstrip() for ln in str(text or "").splitlines()]
    clipped = lines[: max(1, int(max_lines))]
    return "\\n".join(clipped)


def _render_preview(args: argparse.Namespace) -> int:
    state_filter = _norm_state(args.state)
    limit = max(1, int(args.limit or 1))
    template_text = _read_template_text(Path(args.template))
    html_template_text = ""
    try:
        html_template_text = _read_template_text(Path(args.html_template))
    except Exception:
        html_template_text = ""

    recent_leads, last_refresh_et = _best_effort_recent_leads_and_refresh(
        db_path=str(args.db),
        state=state_filter,
        limit=12,
    )
    preview_batch = f"PREVIEW_{state_filter}"
    recent_leads, _preview_triage_ctx = _triage_recent_signals_for_outreach(
        batch=preview_batch,
        recent_leads=list(recent_leads or []),
        dry_run_suffix="preview",
    )
    recent_leads = list(recent_leads[:5])
    signal_tokens = _build_signal_template_tokens(
        db_path=str(args.db),
        state=state_filter,
        recent_leads=recent_leads,
        lookback_days=14,
    )

    rows: list[dict] = []
    if args.input:
        try:
            input_rows = _load_csv_rows(args.input)
        except Exception:
            input_rows = []
        for row in input_rows:
            if _norm_state(str(row.get("state") or "")) != state_filter:
                continue
            rows.append(dict(row))
            if len(rows) >= limit:
                break

    if not rows:
        rows = [
            {
                "prospect_id": "preview",
                "first_name": "",
                "firm": "",
                "title": "",
                "email": "preview@example.com",
                "state": state_filter,
            }
        ]

    unsub_url = "https://unsubscribe.example.internal/unsubscribe?token=preview"
    prefs_url = "https://unsubscribe.example.internal/prefs?token=preview"
    prefs_link = prefs_url
    mailing_address = _resolve_outreach_mailing_address()
    microflowops_url = (os.getenv("MICROFLOWOPS_URL") or "https://microflowops.com").strip() or "https://microflowops.com"

    for idx, row in enumerate(rows, start=1):
        first_name = _row_text_value(row, ["first_name", "contact_name"])
        firm_name_raw = _row_text_value(row, ["firm"])
        segment = _row_text_value(row, ["segment", "buyer_segment"])
        role_or_title = _row_text_value(row, ["role_or_title", "role", "contact_role", "title"])
        copy_tokens = _build_copy_tokens(
            state_full_name=signal_tokens["STATE_FULL_NAME"],
            state_metro_examples=signal_tokens["STATE_METRO_EXAMPLES"],
            first_name=first_name,
            firm_name=firm_name_raw,
            segment=segment,
            role_or_title=role_or_title,
            recent_leads=recent_leads,
        )
        subject = build_outreach_subject(
            state_filter,
            recent_leads=recent_leads,
            db_path=str(args.db),
            segment_descriptor=copy_tokens.get("SEGMENT_DESCRIPTOR", ""),
            state_full_name=signal_tokens["STATE_FULL_NAME"],
            signal_count=int(copy_tokens.get("SIGNAL_COUNT") or "0"),
        )
        text_body = _render_template(
            template_text,
            {
                "FIRST_NAME": first_name,
                "FIRM": firm_name_raw or "your firm",
                "STATE": state_filter,
                "STATE_FULL_NAME": signal_tokens["STATE_FULL_NAME"],
                "STATE_METRO_EXAMPLES": signal_tokens["STATE_METRO_EXAMPLES"],
                "TERRITORY_CODE": "PREVIEW",
                "RECENT_SIGNALS_LINES": signal_tokens["RECENT_SIGNALS_LINES"],
                "SIGNALS_WINDOW_NOTE_TEXT": signal_tokens["SIGNALS_WINDOW_NOTE_TEXT"],
                "SIGNALS_FALLBACK_TEXT": signal_tokens["SIGNALS_FALLBACK_TEXT"],
                "LAST_REFRESH_ET": last_refresh_et,
                "UNSUBSCRIBE_URL": unsub_url,
                "PREFS_URL": prefs_link,
                "SIGNAL_COUNT": copy_tokens["SIGNAL_COUNT"],
                "SEGMENT_DESCRIPTOR": copy_tokens["SEGMENT_DESCRIPTOR"],
                "GREETING_LINE_TEXT": copy_tokens["GREETING_LINE_TEXT"],
                "INTRO_LINE_TEXT": copy_tokens["INTRO_LINE_TEXT"],
                "POST_CARDS_LINE_TEXT": copy_tokens["POST_CARDS_LINE_TEXT"],
                "TRIAL_LINE_TEXT": copy_tokens["TRIAL_LINE_TEXT"],
                "OPENING_LINE_TEXT": copy_tokens["OPENING_LINE_TEXT"],
                "RELEVANCE_LINE_TEXT": copy_tokens["RELEVANCE_LINE_TEXT"],
                "CTA_LINE_TEXT": copy_tokens["CTA_LINE_TEXT"],
                "TRUST_LINE_TEXT": copy_tokens["TRUST_LINE_TEXT"],
                "COMPANY_INTRO_TEXT": copy_tokens["COMPANY_INTRO_TEXT"],
            },
        ).strip() + "\n"

        if html_template_text.strip():
            html_body = _render_template(
                html_template_text,
                {
                    "{{FIRST_NAME}}": _html_escape(first_name),
                    "{{FIRM}}": _html_escape(firm_name_raw or "your firm"),
                    "{{STATE}}": _html_escape(state_filter),
                    "{{STATE_FULL_NAME}}": _html_escape(signal_tokens["STATE_FULL_NAME"]),
                    "{{STATE_METRO_EXAMPLES}}": _html_escape(signal_tokens["STATE_METRO_EXAMPLES"]),
                    "{{RECENT_SIGNALS_HTML}}": signal_tokens["RECENT_SIGNALS_HTML"],
                    "{{SIGNALS_WINDOW_NOTE_HTML}}": signal_tokens["SIGNALS_WINDOW_NOTE_HTML"],
                    "{{SIGNALS_FALLBACK_HTML}}": signal_tokens["SIGNALS_FALLBACK_HTML"],
                    "{{LAST_REFRESH_ET}}": _html_escape(last_refresh_et),
                    "{{UNSUBSCRIBE_URL}}": _html_escape(unsub_url),
                    "{{PREFS_URL}}": _html_escape(prefs_link),
                    "{{MAILING_ADDRESS}}": _html_escape(mailing_address),
                    "{{MICROFLOWOPS_URL}}": _html_escape(microflowops_url),
                    "{{GREETING_LINE_HTML}}": copy_tokens["GREETING_LINE_HTML"],
                    "{{INTRO_LINE_HTML}}": copy_tokens["INTRO_LINE_HTML"],
                    "{{POST_CARDS_LINE_HTML}}": copy_tokens["POST_CARDS_LINE_HTML"],
                    "{{TRIAL_LINE_HTML}}": copy_tokens["TRIAL_LINE_HTML"],
                    "{{OPENING_LINE_HTML}}": copy_tokens["OPENING_LINE_HTML"],
                    "{{RELEVANCE_LINE_HTML}}": copy_tokens["RELEVANCE_LINE_HTML"],
                    "{{CTA_LINE_HTML}}": copy_tokens["CTA_LINE_HTML"],
                    "{{TRUST_LINE_HTML}}": copy_tokens["TRUST_LINE_HTML"],
                    "{{COMPANY_INTRO_HTML}}": copy_tokens["COMPANY_INTRO_HTML"],
                },
            ).strip()
        else:
            html_body = (
                "<div style=\"font-family: system-ui, -apple-system, 'Segoe UI', Roboto, Arial, sans-serif;\">"
                "<pre style=\"white-space: pre-wrap; font-size: 13px; line-height: 1.4;\">"
                + _html_escape(text_body)
                + "</pre></div>"
            )

        unsub_anchor_count = html_body.count(">Unsubscribe</a>")
        unsub_url_count = html_body.count("unsubscribe.example.internal/unsubscribe?token=preview")
        address_idx = html_body.find(mailing_address)
        pre_footer = html_body[:address_idx] if address_idx > 0 else html_body
        pre_footer_unsub_count = pre_footer.count("unsubscribe.example.internal/unsubscribe?token=preview")
        footer_unsub_present = unsub_anchor_count > 0
        unsubscribe_count_exactly_one = unsub_url_count == 1
        no_duplicate_unsub_pre_footer = pre_footer_unsub_count == 0

        print(f"PREVIEW_ROW={idx}")
        print(f"SUBJECT: {subject}")
        print(f"BODY_TEXT_PREVIEW: {_preview_text(text_body, max_lines=60)}")
        print(f"BODY_HTML_PREVIEW: {_preview_text(html_body, max_lines=80)}")
        print(
            "COMPLIANCE_CHECKS "
            f"footer_unsubscribe_present={str(footer_unsub_present).lower()} "
            f"unsubscribe_link_count_exactly_one={str(unsubscribe_count_exactly_one).lower()} "
            f"unsubscribe_link_count={unsub_url_count} "
            f"pre_footer_unsubscribe_link_count={pre_footer_unsub_count} "
            f"no_duplicate_unsubscribe_pre_footer={str(no_duplicate_unsub_pre_footer).lower()}"
        )
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="Generate a mail-merge outbox CSV with dedupe + suppression enforcement.")
    ap.add_argument("--input", help="Input prospects CSV (see outreach/prospects_schema.md).")
    ap.add_argument("--batch", help="Batch id (e.g., TX_W2). Used in output and logs.")
    ap.add_argument("--state", help="2-letter state filter (e.g., TX).")
    ap.add_argument("--out", help="Output outbox CSV path.")
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
    ap.add_argument(
        "--render-preview",
        action="store_true",
        help="Render deterministic preview output to stdout using template + signal context without writing artifacts.",
    )
    ap.add_argument(
        "--limit",
        type=int,
        default=1,
        help="Preview row limit when --render-preview is set.",
    )
    args = ap.parse_args()

    if args.render_preview:
        if not str(args.state or "").strip():
            ap.error("--state is required with --render-preview")
        return _render_preview(args)

    missing_required = [name for name, value in [("--input", args.input), ("--batch", args.batch), ("--state", args.state), ("--out", args.out)] if not str(value or "").strip()]
    if missing_required:
        ap.error(f"missing required arguments for export mode: {', '.join(missing_required)}")

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

    signal_fetch_limit = 12

    # Precompute state-level snippets for template rendering.
    recent_leads, last_refresh_et = _best_effort_recent_leads_and_refresh(
        db_path=str(args.db),
        state=state_filter,
        limit=signal_fetch_limit,
    )
    recent_leads_original = list(recent_leads or [])
    recent_leads, triage_ctx = _triage_recent_signals_for_outreach(
        batch=batch,
        recent_leads=recent_leads,
        dry_run_suffix="export",
    )
    recent_leads = list(recent_leads[:5])
    signal_tokens = _build_signal_template_tokens(
        db_path=str(args.db),
        state=state_filter,
        recent_leads=recent_leads,
        lookback_days=14,
    )
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
    triage_detail_records: list[dict] = []
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
        firm_name_raw = _row_text_value(r, ["firm"])
        firm = firm_name_raw or "your firm"
        segment = _row_text_value(r, ["segment", "buyer_segment"])
        role_or_title = _row_text_value(r, ["role_or_title", "role", "contact_role", "title"])
        prefs_link = prefs_url or unsub_url or ""
        copy_tokens = _build_copy_tokens(
            state_full_name=signal_tokens["STATE_FULL_NAME"],
            state_metro_examples=signal_tokens["STATE_METRO_EXAMPLES"],
            first_name=first_name,
            firm_name=firm_name_raw,
            segment=segment,
            role_or_title=role_or_title,
            recent_leads=recent_leads,
        )

        subject = build_outreach_subject(
            state_filter,
            recent_leads=recent_leads,
            db_path=str(args.db),
            segment_descriptor=copy_tokens.get("SEGMENT_DESCRIPTOR", ""),
            state_full_name=signal_tokens["STATE_FULL_NAME"],
            signal_count=int(copy_tokens.get("SIGNAL_COUNT") or "0"),
        )
        text_body = _render_template(
            template_text,
            {
                "FIRST_NAME": first_name,
                "FIRM": firm,
                "STATE": state_filter,
                "STATE_FULL_NAME": signal_tokens["STATE_FULL_NAME"],
                "STATE_METRO_EXAMPLES": signal_tokens["STATE_METRO_EXAMPLES"],
                "TERRITORY_CODE": territory_code,
                "RECENT_SIGNALS_LINES": signal_tokens["RECENT_SIGNALS_LINES"],
                "SIGNALS_WINDOW_NOTE_TEXT": signal_tokens["SIGNALS_WINDOW_NOTE_TEXT"],
                "SIGNALS_FALLBACK_TEXT": signal_tokens["SIGNALS_FALLBACK_TEXT"],
                "LAST_REFRESH_ET": last_refresh_et,
                "UNSUBSCRIBE_URL": unsub_url or "",
                "PREFS_URL": prefs_url or prefs_link,
                "SIGNAL_COUNT": copy_tokens["SIGNAL_COUNT"],
                "SEGMENT_DESCRIPTOR": copy_tokens["SEGMENT_DESCRIPTOR"],
                "GREETING_LINE_TEXT": copy_tokens["GREETING_LINE_TEXT"],
                "INTRO_LINE_TEXT": copy_tokens["INTRO_LINE_TEXT"],
                "POST_CARDS_LINE_TEXT": copy_tokens["POST_CARDS_LINE_TEXT"],
                "TRIAL_LINE_TEXT": copy_tokens["TRIAL_LINE_TEXT"],
                "OPENING_LINE_TEXT": copy_tokens["OPENING_LINE_TEXT"],
                "RELEVANCE_LINE_TEXT": copy_tokens["RELEVANCE_LINE_TEXT"],
                "CTA_LINE_TEXT": copy_tokens["CTA_LINE_TEXT"],
                "TRUST_LINE_TEXT": copy_tokens["TRUST_LINE_TEXT"],
                "COMPANY_INTRO_TEXT": copy_tokens["COMPANY_INTRO_TEXT"],
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
                    "{{FIRST_NAME}}": _html_escape(first_name),
                    "{{FIRM}}": _html_escape(firm),
                    "{{STATE}}": _html_escape(state_filter),
                    "{{STATE_FULL_NAME}}": _html_escape(signal_tokens["STATE_FULL_NAME"]),
                    "{{STATE_METRO_EXAMPLES}}": _html_escape(signal_tokens["STATE_METRO_EXAMPLES"]),
                    "{{RECENT_SIGNALS_HTML}}": signal_tokens["RECENT_SIGNALS_HTML"],
                    "{{SIGNALS_WINDOW_NOTE_HTML}}": signal_tokens["SIGNALS_WINDOW_NOTE_HTML"],
                    "{{SIGNALS_FALLBACK_HTML}}": signal_tokens["SIGNALS_FALLBACK_HTML"],
                    "{{LAST_REFRESH_ET}}": _html_escape(last_refresh_et),
                    "{{UNSUBSCRIBE_URL}}": _html_escape(unsub_url or prefs_link),
                    "{{PREFS_URL}}": _html_escape(prefs_url or prefs_link),
                    "{{MAILING_ADDRESS}}": _html_escape(mailing_address),
                    "{{MICROFLOWOPS_URL}}": _html_escape(microflowops_url),
                    "{{GREETING_LINE_HTML}}": copy_tokens["GREETING_LINE_HTML"],
                    "{{INTRO_LINE_HTML}}": copy_tokens["INTRO_LINE_HTML"],
                    "{{POST_CARDS_LINE_HTML}}": copy_tokens["POST_CARDS_LINE_HTML"],
                    "{{TRIAL_LINE_HTML}}": copy_tokens["TRIAL_LINE_HTML"],
                    "{{OPENING_LINE_HTML}}": copy_tokens["OPENING_LINE_HTML"],
                    "{{RELEVANCE_LINE_HTML}}": copy_tokens["RELEVANCE_LINE_HTML"],
                    "{{CTA_LINE_HTML}}": copy_tokens["CTA_LINE_HTML"],
                    "{{TRUST_LINE_HTML}}": copy_tokens["TRUST_LINE_HTML"],
                    "{{COMPANY_INTRO_HTML}}": copy_tokens["COMPANY_INTRO_HTML"],
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
                "ai_triage_action": triage_ctx["ai_triage_action"],
                "ai_triage_conf": triage_ctx["ai_triage_conf"],
                "ai_triage_reasons": triage_ctx["ai_triage_reasons"],
                "ai_triage_details_relpath": triage_ctx["ai_triage_details_relpath"],
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
                "ai_triage_action": triage_ctx["ai_triage_action"],
                "ai_triage_conf": triage_ctx["ai_triage_conf"],
                "ai_triage_reasons": triage_ctx["ai_triage_reasons"],
                "ai_triage_details_relpath": triage_ctx["ai_triage_details_relpath"],
            }
        )
        if triage_ctx.get("enabled"):
            triage_detail_records.extend(
                scoring_triage_overlay.build_outreach_signal_triage_records(
                    batch_id=batch,
                    prospect_id=prospect_id,
                    original_signals=list(recent_leads_original),
                    final_signals=list(recent_leads),
                    decisions=list(triage_ctx.get("decisions") or []),
                )
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
        "ai_triage_action",
        "ai_triage_conf",
        "ai_triage_reasons",
        "ai_triage_details_relpath",
    ]
    _write_outbox_csv(args.out, exported, out_fields)

    manifest_path = _manifest_path_for_outbox(args.out)
    if triage_ctx.get("enabled") and triage_ctx.get("artifact_path"):
        artifact_path = Path(triage_ctx["artifact_path"])
        artifact_path.parent.mkdir(parents=True, exist_ok=True)
        artifact_path.write_text(json.dumps(triage_detail_records, indent=2) + "\n", encoding="utf-8")
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
        "triage_overlay": {
            "enabled": bool(triage_ctx.get("enabled")),
            "action": triage_ctx.get("ai_triage_action"),
            "details_relpath": triage_ctx.get("ai_triage_details_relpath"),
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
    if triage_ctx.get("enabled") and triage_ctx.get("artifact_path"):
        print(f"outreach_triage_details={triage_ctx['artifact_path']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
