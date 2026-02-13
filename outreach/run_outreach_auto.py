import argparse
import csv
import json
import os
import sqlite3
import subprocess
import sys
import time
from collections import Counter
from datetime import date, datetime, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

try:  # pragma: no cover
    from dotenv import load_dotenv

    load_dotenv()
except Exception:  # pragma: no cover
    pass


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from outreach import crm_store
from outreach import generate_mailmerge as gm


ERR_AUTO_ENV = "ERR_AUTO_ENV"
ERR_AUTO_SMOKE_TO_MISSING = "ERR_AUTO_SMOKE_TO_MISSING"
ERR_AUTO_SUMMARY_TO_MISMATCH = "ERR_AUTO_SUMMARY_TO_MISMATCH"
ERR_AUTO_SUMMARY_SEND = "ERR_AUTO_SUMMARY_SEND"
ERR_AUTO_ONE_CLICK_REQUIRED = "ERR_AUTO_ONE_CLICK_REQUIRED"
ERR_AUTO_CRM_REQUIRED = "ERR_AUTO_CRM_REQUIRED"
ERR_AUTO_FOR_DATE_INVALID = "ERR_AUTO_FOR_DATE_INVALID"
ERR_AUTO_FOR_DATE_LIVE_SEND_BLOCKED = "ERR_AUTO_FOR_DATE_LIVE_SEND_BLOCKED"

PASS_AUTO_DRY_RUN = "PASS_AUTO_DRY_RUN"
PASS_AUTO_PRINT_CONFIG = "PASS_AUTO_PRINT_CONFIG"
PASS_AUTO_EXPORT = "PASS_AUTO_EXPORT"
PASS_AUTO_SUMMARY = "PASS_AUTO_SUMMARY"

ERR_DOCTOR_SECRETS_DECRYPT = "ERR_DOCTOR_SECRETS_DECRYPT"
ERR_DOCTOR_ENV_MISSING_PREFIX = "ERR_DOCTOR_ENV_MISSING_"
ERR_DOCTOR_ENV_INVALID_PREFIX = "ERR_DOCTOR_ENV_INVALID_"
ERR_DOCTOR_CRM_REQUIRED = "ERR_DOCTOR_CRM_REQUIRED"
ERR_DOCTOR_CRM_SCHEMA = "ERR_DOCTOR_CRM_SCHEMA"
ERR_DOCTOR_SUPPRESSION_REQUIRED = "ERR_DOCTOR_SUPPRESSION_REQUIRED"
ERR_DOCTOR_SUPPRESSION_UNREADABLE = "ERR_DOCTOR_SUPPRESSION_UNREADABLE"
ERR_DOCTOR_SUPPRESSION_STALE = "ERR_DOCTOR_SUPPRESSION_STALE"
ERR_DOCTOR_UNSUB_CONFIG = "ERR_DOCTOR_UNSUB_CONFIG"
ERR_DOCTOR_UNSUB_UNREACHABLE = "ERR_DOCTOR_UNSUB_UNREACHABLE"
ERR_DOCTOR_PROVIDER_CONFIG = "ERR_DOCTOR_PROVIDER_CONFIG"
ERR_DOCTOR_DRY_RUN_ARTIFACT = "ERR_DOCTOR_DRY_RUN_ARTIFACT"
ERR_DOCTOR_IDEMPOTENCY = "ERR_DOCTOR_IDEMPOTENCY"

PASS_DOCTOR_SECRETS_DECRYPT = "PASS_DOCTOR_SECRETS_DECRYPT"
PASS_DOCTOR_ENV = "PASS_DOCTOR_ENV"
PASS_DOCTOR_CRM_REQUIRED = "PASS_DOCTOR_CRM_REQUIRED"
PASS_DOCTOR_SUPPRESSION = "PASS_DOCTOR_SUPPRESSION"
PASS_DOCTOR_UNSUB = "PASS_DOCTOR_UNSUB"
PASS_DOCTOR_PROVIDER_CONFIG = "PASS_DOCTOR_PROVIDER_CONFIG"
PASS_DOCTOR_DRY_RUN_ARTIFACT = "PASS_DOCTOR_DRY_RUN_ARTIFACT"
PASS_DOCTOR_IDEMPOTENCY = "PASS_DOCTOR_IDEMPOTENCY"
PASS_DOCTOR_COMPLETE = "PASS_DOCTOR_COMPLETE"

DOCTOR_TIMEOUT_SECRETS_SECONDS = 90
DOCTOR_TIMEOUT_DRY_RUN_SECONDS = 120
DOCTOR_HTTP_TIMEOUT_SECONDS = 5.0
PROJECT_CONTEXT_SOFT_CHECK_CMD = ["--check", "--soft"]

EXCLUDED_STATUSES = {"do_not_contact", "unsubscribed", "bounced", "converted"}
ROLE_PRIORITY_RULES: list[tuple[int, tuple[str, ...], str]] = [
    (0, ("owner", "founder", "partner", "president", "ceo", "chief", "principal", "executive"), "decision_maker"),
    (1, ("safety manager", "safety director", "ehs", "hse", "osha", "safety"), "safety_leader"),
    (2, ("operations", "compliance", "plant manager", "general manager"), "operations_compliance"),
]
ROLE_INBOX_LOCALS = {"info", "support", "hello", "sales", "contact", "admin", "office"}
BUYER_SEGMENT_HINTS = ("attorney", "safety consultant", "ehs")
OPTIONAL_PROSPECT_COLUMNS = (
    "segment",
    "state_pref",
    "role",
    "contact_role",
    "buyer_segment",
)
FILTER_BREAKDOWN_FILTER_KEYS = (
    "suppressed",
    "invalid_email",
    "status_do_not_contact",
    "status_unsubscribed",
    "status_bounced",
    "status_converted",
    "already_contacted",
    "domain_dedup",
    "daily_limit",
)


def _norm_email(s: str) -> str:
    return (s or "").strip().lower()


def _compact_detail(text: str, max_len: int = 220) -> str:
    value = str(text or "").replace("\r", " ").replace("\n", " ").strip()
    value = " ".join(value.split())
    if not value:
        return "unknown"
    if len(value) > max_len:
        return value[:max_len] + "..."
    return value


def _is_valid_email_shape(email: str) -> bool:
    value = _norm_email(email)
    if "@" not in value:
        return False
    local, _, domain = value.partition("@")
    if not local or not domain:
        return False
    if "." not in domain:
        return False
    if domain.startswith(".") or domain.endswith("."):
        return False
    return True


def _doctor_error(token: str, detail: str = "") -> tuple[bool, str]:
    msg = token if not detail else f"{token} {detail}"
    return False, msg.strip()


def _parse_states(raw: str) -> list[str]:
    states = []
    for token in (raw or "").split(","):
        s = token.strip().upper()
        if not s:
            continue
        if s not in states:
            states.append(s)
    return states


def _daily_limit_with_source() -> tuple[int, str]:
    raw = (os.getenv("OUTREACH_DAILY_LIMIT") or "").strip()
    if not raw:
        return 200, "default"
    try:
        n = int(raw)
    except Exception:
        return 200, "default"
    return max(1, n), "env"


def _daily_limit() -> int:
    return _daily_limit_with_source()[0]


def _data_dir() -> Path:
    return crm_store.data_dir()


def _crm_db_path() -> Path:
    return crm_store.crm_db_path()


def _suppression_csv_path() -> Path:
    return _data_dir() / "suppression.csv"


def _export_ledger_path() -> Path:
    return _data_dir() / "outreach_export_ledger.jsonl"


def _parse_for_date(raw: str) -> tuple[bool, date, str]:
    text = (raw or "").strip()
    if not text:
        return True, datetime.now().date(), ""
    try:
        parsed = datetime.strptime(text, "%Y-%m-%d").date()
    except Exception:
        return False, datetime.now().date(), f"{ERR_AUTO_FOR_DATE_INVALID} value={_compact_detail(text, 64)}"
    return True, parsed, ""


def _choose_state(states: list[str], run_date: date) -> str:
    if not states:
        return ""
    idx = run_date.weekday() % len(states)
    return states[idx]


def _batch_id(state: str, run_date: date) -> str:
    return f"{run_date.isoformat()}_{state}"


def _resolve_summary_recipient(explicit_to: str) -> tuple[bool, str, str]:
    expected = _norm_email(os.getenv("OSHA_SMOKE_TO", ""))
    if not expected or "@" not in expected:
        return False, "", f"{ERR_AUTO_SMOKE_TO_MISSING} OSHA_SMOKE_TO not set"
    got = _norm_email(explicit_to) if (explicit_to or "").strip() else expected
    if got != expected:
        return False, "", f"{ERR_AUTO_SUMMARY_TO_MISMATCH} expected={expected} got={got}"
    return True, got, ""


def _send_summary_email(to_email: str, subject: str, text_body: str, html_body: str) -> tuple[bool, str]:
    try:
        import send_digest_email as sde
    except Exception as e:
        return False, f"import_send_digest_email_failed {e}"

    try:
        branding = sde.resolve_branding({})
        reply_to = (branding.get("reply_to") or os.getenv("REPLY_TO_EMAIL") or "support@microflowops.com").strip()
        mailto = f"mailto:{reply_to}?subject=unsubscribe"
        list_unsub = f"<{mailto}>"
        list_unsub_post = None

        ok, _msg_id, err = sde.send_email(
            recipient=to_email,
            subject=subject,
            html_body=html_body,
            text_body=text_body,
            customer_id="",
            territory_code="OUTREACH_AUTO",
            branding=branding,
            dry_run=False,
            list_unsub=list_unsub,
            list_unsub_post=list_unsub_post,
            label="outreach_auto_summary",
        )
        if not ok:
            return False, err or "send_failed"
        return True, ""
    except Exception as e:
        return False, str(e)


def _connect_existing_crm(path: Path) -> sqlite3.Connection:
    if not path.exists():
        raise FileNotFoundError(str(path))
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def _connect_existing_crm_readonly(path: Path) -> sqlite3.Connection:
    if not path.exists():
        raise FileNotFoundError(str(path))
    uri = "file:" + path.as_posix() + "?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name = ? LIMIT 1",
        (table_name,),
    ).fetchone()
    return bool(row)


def _require_schema(conn: sqlite3.Connection) -> bool:
    needed = ["prospects", "outreach_events", "suppression", "trials"]
    return all(_table_exists(conn, name) for name in needed)


def _load_suppression_emails(conn: sqlite3.Connection) -> set[str]:
    # Compliance gate: local suppression CSV must be present.
    csv_suppressed = set(gm._load_local_suppression_set())
    db_suppressed = {
        _norm_email(r[0])
        for r in conn.execute("SELECT email FROM suppression")
        if _norm_email(r[0])
    }
    return csv_suppressed | db_suppressed


def _fetch_prior_sent_ids(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute("SELECT DISTINCT prospect_id FROM outreach_events WHERE event_type = 'sent'").fetchall()
    return {str(r[0]) for r in rows if str(r[0] or "").strip()}


def _table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    return {str(r[1]) for r in conn.execute(f"PRAGMA table_info({table_name})") if len(r) > 1}


def _prospect_select_columns(conn: sqlite3.Connection) -> list[str]:
    base = [
        "prospect_id",
        "firm",
        "contact_name",
        "email",
        "title",
        "city",
        "state",
        "website",
        "source",
        "score",
        "status",
        "created_at",
        "last_contacted_at",
    ]
    existing = _table_columns(conn, "prospects")
    cols = list(base)
    for col in OPTIONAL_PROSPECT_COLUMNS:
        if col in existing and col not in cols:
            cols.append(col)
    return cols


def _norm_domain(email: str) -> str:
    value = _norm_email(email)
    if "@" not in value:
        return ""
    _local, _sep, domain = value.partition("@")
    return domain.strip().lower()


def _safe_text(value: str) -> str:
    return str(value or "").replace("\r", " ").replace("\n", " ").strip()


def _safe_csv_value(value: str) -> str:
    return _safe_text(value).replace(",", ";")


def _score_tier(score: int) -> str:
    if score >= 8:
        return "high"
    if score >= 6:
        return "medium"
    if score >= 4:
        return "low"
    return "below_low"


def _score_tier_rank(score: int) -> int:
    tier = _score_tier(score)
    if tier == "high":
        return 0
    if tier == "medium":
        return 1
    if tier == "low":
        return 2
    return 3


def _parse_sort_ts(value: str) -> float:
    text = (value or "").strip()
    if not text:
        return 0.0
    normalized = text.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(normalized)
    except Exception:
        return 0.0
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return float(dt.timestamp())


def _extract_role_or_title(row: sqlite3.Row) -> str:
    role = _safe_text(str(row["role"] or "")) if "role" in row.keys() else ""
    if role:
        return role
    contact_role = _safe_text(str(row["contact_role"] or "")) if "contact_role" in row.keys() else ""
    if contact_role:
        return contact_role
    return _safe_text(str(row["title"] or ""))


def _extract_segment(row: sqlite3.Row) -> str:
    for key in ["segment", "buyer_segment"]:
        if key in row.keys():
            value = _safe_text(str(row[key] or ""))
            if value:
                return value
    return ""


def _extract_state_pref(row: sqlite3.Row) -> str:
    if "state_pref" in row.keys():
        value = _safe_text(str(row["state_pref"] or "")).upper()
        if value:
            return value
    return _safe_text(str(row["state"] or "")).upper()


def _role_priority(role_or_title: str) -> tuple[int, str]:
    text = (role_or_title or "").strip().lower()
    if not text:
        return 3, "other"
    for rank, tokens, label in ROLE_PRIORITY_RULES:
        if any(token in text for token in tokens):
            return rank, label
    return 3, "other"


def _role_inbox_penalty(email: str) -> int:
    local_part = _norm_email(email).split("@", 1)[0] if "@" in _norm_email(email) else ""
    local = local_part.split("+", 1)[0]
    return 1 if local in ROLE_INBOX_LOCALS else 0


def _segment_penalty(segment: str) -> tuple[int, str]:
    text = (segment or "").strip().lower()
    if not text:
        return 1, "no"
    for token in BUYER_SEGMENT_HINTS:
        if token in text:
            return 0, "yes"
    return 1, "no"


def _rank_tuple_for_candidate(
    prospect_id: str,
    email: str,
    role_priority: int,
    role_inbox_penalty: int,
    score: int,
    segment_penalty: int,
    created_at: str,
) -> tuple:
    created_ts = _parse_sort_ts(created_at)
    return (
        int(role_priority),
        int(role_inbox_penalty),
        _score_tier_rank(int(score)),
        int(segment_penalty),
        -int(score),
        -created_ts,
        (created_at or ""),
        (prospect_id or ""),
        (email or ""),
    )


def _rank_reason_text(
    role_bucket_label: str,
    role_priority: int,
    role_inbox_penalty: int,
    score: int,
    segment_fit: str,
    created_at: str,
) -> str:
    return (
        f"role_bucket={role_bucket_label};role_priority={role_priority};"
        f"role_inbox_penalty={role_inbox_penalty};score_tier={_score_tier(score)};"
        f"score={score};segment_fit={segment_fit};created_at={_safe_text(created_at) or 'none'}"
    )


def _skip_reason(
    row: sqlite3.Row,
    suppressed_emails: set[str],
    sent_ids: set[str],
    allow_repeat: bool,
) -> str:
    status = str(row["status"] or "").strip().lower()
    if status in EXCLUDED_STATUSES:
        if status == "do_not_contact":
            return "status_do_not_contact"
        return f"status_{status}"

    email = _norm_email(str(row["email"] or ""))
    if not email or "@" not in email:
        return "invalid_email"
    if email in suppressed_emails:
        return "suppressed"

    if not allow_repeat:
        if str(row["prospect_id"]) in sent_ids:
            return "already_contacted"
        if str(row["last_contacted_at"] or "").strip():
            return "already_contacted"
    return ""


def _candidate_from_row(row: sqlite3.Row) -> dict:
    prospect_id = _safe_text(str(row["prospect_id"] or ""))
    email = _norm_email(str(row["email"] or ""))
    role_or_title = _extract_role_or_title(row)
    segment = _extract_segment(row)
    state_pref = _extract_state_pref(row)
    domain = _norm_domain(email)
    created_at = _safe_text(str(row["created_at"] or ""))
    try:
        score = int(row["score"] or 0)
    except Exception:
        score = 0

    role_rank, role_bucket_label = _role_priority(role_or_title)
    inbox_penalty = _role_inbox_penalty(email)
    segment_rank_penalty, segment_fit = _segment_penalty(segment)
    rank_tuple = _rank_tuple_for_candidate(
        prospect_id=prospect_id,
        email=email,
        role_priority=role_rank,
        role_inbox_penalty=inbox_penalty,
        score=score,
        segment_penalty=segment_rank_penalty,
        created_at=created_at,
    )
    rank_reason = _rank_reason_text(
        role_bucket_label=role_bucket_label,
        role_priority=role_rank,
        role_inbox_penalty=inbox_penalty,
        score=score,
        segment_fit=segment_fit,
        created_at=created_at,
    )
    return {
        "row": row,
        "prospect_id": prospect_id,
        "email": email,
        "domain": domain,
        "segment": segment,
        "role_or_title": role_or_title,
        "state_pref": state_pref,
        "score": int(score),
        "created_at": created_at,
        "rank_tuple": rank_tuple,
        "rank_tuple_text": "|".join([str(x) for x in rank_tuple]),
        "rank_reason": rank_reason,
    }


def _candidate_csv_row(candidate: dict) -> dict:
    return {
        "prospect_id": candidate["prospect_id"],
        "email": candidate["email"],
        "domain": candidate["domain"],
        "segment": candidate["segment"],
        "role_or_title": candidate["role_or_title"],
        "state_pref": candidate["state_pref"],
        "rank_reason": candidate["rank_reason"],
        "rank_tuple": candidate["rank_tuple_text"],
    }


def _select_candidates(
    conn: sqlite3.Connection,
    state: str,
    limit: int,
    suppressed_emails: set[str],
    allow_repeat: bool,
) -> tuple[list[dict], Counter, list[dict], dict[str, int]]:
    cols = _prospect_select_columns(conn)
    query = "SELECT " + ", ".join(cols) + " FROM prospects WHERE UPPER(COALESCE(state, '')) = ?"
    rows = conn.execute(query, ((state or "").upper(),)).fetchall()

    sent_ids = _fetch_prior_sent_ids(conn)

    skipped = Counter()
    manifest_rows: list[dict] = []
    ranked: list[dict] = []
    role_inbox_penalty_count = 0
    missing_state_pref_count = 0
    for row in rows:
        candidate = _candidate_from_row(row)
        if _role_inbox_penalty(candidate["email"]) > 0:
            role_inbox_penalty_count += 1
        if not _safe_text(candidate["state_pref"]):
            missing_state_pref_count += 1
        reason = _skip_reason(row, suppressed_emails=suppressed_emails, sent_ids=sent_ids, allow_repeat=allow_repeat)
        if reason:
            skipped[reason] += 1
            dropped = _candidate_csv_row(candidate)
            dropped.update({"status": "dropped", "reason": reason})
            manifest_rows.append(dropped)
            continue
        ranked.append(candidate)

    ranked.sort(key=lambda item: item["rank_tuple"])

    per_domain: list[dict] = []
    seen_domains: set[str] = set()
    for candidate in ranked:
        domain_key = candidate["domain"] or f"__nodomain__:{candidate['prospect_id']}"
        if domain_key in seen_domains:
            skipped["domain_dedup"] += 1
            dropped = _candidate_csv_row(candidate)
            dropped.update({"status": "dropped", "reason": "domain_dedup"})
            manifest_rows.append(dropped)
            continue
        seen_domains.add(domain_key)
        per_domain.append(candidate)

    selected = per_domain[:limit]
    overflow = per_domain[limit:]
    for candidate in overflow:
        skipped["daily_limit"] += 1
        dropped = _candidate_csv_row(candidate)
        dropped.update({"status": "dropped", "reason": "daily_limit"})
        manifest_rows.append(dropped)

    for candidate in selected:
        selected_row = _candidate_csv_row(candidate)
        selected_row.update({"status": "selected", "reason": ""})
        manifest_rows.append(selected_row)
    selection_stats = {
        "pool_total_selected_state": int(len(rows)),
        "eligible": int(len(ranked)),
        "role_inbox_penalty": int(role_inbox_penalty_count),
        "missing_state_pref": int(missing_state_pref_count),
    }
    return selected, skipped, manifest_rows, selection_stats


def _count_pool_total_all_states(conn: sqlite3.Connection) -> int:
    try:
        row = conn.execute("SELECT COUNT(*) FROM prospects").fetchone()
    except Exception:
        return 0
    if not row:
        return 0
    try:
        return max(0, int(row[0] or 0))
    except Exception:
        return 0


def _build_filter_breakdown(
    skipped: Counter,
    pool_total_all_states: int,
    pool_total_selected_state: int,
    eligible: int,
    selected_count: int,
    role_inbox_penalty: int,
    missing_state_pref: int,
    state: str,
) -> dict:
    filters: dict[str, int] = {}
    for key in FILTER_BREAKDOWN_FILTER_KEYS:
        filters[key] = max(0, int(skipped.get(key, 0)))
    for key in sorted(skipped.keys()):
        if key in filters:
            continue
        filters[str(key)] = max(0, int(skipped.get(key, 0)))
    return {
        "pool_total_all_states": max(0, int(pool_total_all_states)),
        "pool_total_selected_state": max(0, int(pool_total_selected_state)),
        "eligible": max(0, int(eligible)),
        "selected": max(0, int(selected_count)),
        "filters": filters,
        "gates": {
            "state_mismatch": max(0, int(pool_total_all_states) - int(pool_total_selected_state)),
            "role_inbox_penalty": max(0, int(role_inbox_penalty)),
            "missing_state_pref": max(0, int(missing_state_pref)),
            "weekend_block": False,
            "selected_state": _safe_text(state).upper(),
            "state_rotation_source": "weekday_index",
        },
    }


def _build_plan_diagnostics(
    run_date: date,
    state: str,
    batch: str,
    daily_limit: int,
    selected: list[dict],
    skipped: Counter,
    pool_total_all_states: int,
    selection_stats: dict[str, int],
) -> dict:
    filter_breakdown = _build_filter_breakdown(
        skipped=skipped,
        pool_total_all_states=max(0, int(pool_total_all_states)),
        pool_total_selected_state=max(0, int(selection_stats.get("pool_total_selected_state", 0))),
        eligible=max(0, int(selection_stats.get("eligible", 0))),
        selected_count=max(0, int(len(selected))),
        role_inbox_penalty=max(0, int(selection_stats.get("role_inbox_penalty", 0))),
        missing_state_pref=max(0, int(selection_stats.get("missing_state_pref", 0))),
        state=state,
    )
    return {
        "plan_date": run_date.isoformat(),
        "state": state,
        "batch_id": batch,
        "daily_limit": int(daily_limit),
        "will_send": int(len(selected)),
        "pool_total_all_states": int(filter_breakdown["pool_total_all_states"]),
        "pool_total_selected_state": int(filter_breakdown["pool_total_selected_state"]),
        "skip_breakdown": _plan_skip_breakdown(skipped),
        "filter_breakdown": filter_breakdown,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
    }


def _format_top_reasons(counts: Counter, limit: int = 5) -> str:
    if not counts:
        return "none"
    top = counts.most_common(limit)
    return ",".join([f"{k}:{v}" for k, v in top])


def _safe_batch_name(batch: str) -> str:
    raw = (batch or "").strip()
    safe = "".join(ch if ch.isalnum() or ch in "._-" else "_" for ch in raw).strip("_")
    return safe or "batch"


def _dry_run_paths(batch: str) -> tuple[Path, Path]:
    safe_batch = _safe_batch_name(batch)
    out_dir = (REPO_ROOT / "out" / "outreach" / safe_batch).resolve()
    outbox = out_dir / f"outbox_{safe_batch}_dry_run.csv"
    manifest = out_dir / f"outbox_{safe_batch}_dry_run_manifest.csv"
    return outbox, manifest


def _plan_diagnostics_path(batch: str) -> Path:
    safe_batch = _safe_batch_name(batch)
    out_dir = (REPO_ROOT / "out" / "outreach" / safe_batch).resolve()
    return out_dir / "plan_diagnostics.json"


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _write_plan_diagnostics(batch: str, payload: dict) -> Path:
    path = _plan_diagnostics_path(batch)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, sort_keys=True)
        f.write("\n")
    return path


def _write_dry_run_artifacts(batch: str, state: str, selected: list[dict], manifest_rows: list[dict]) -> tuple[Path, Path]:
    outbox_path, manifest_path = _dry_run_paths(batch)
    outbox_rows: list[dict] = []
    for candidate in selected:
        row = _candidate_csv_row(candidate)
        row.update({"batch": batch, "state": state})
        outbox_rows.append(row)
    _write_csv(
        outbox_path,
        [
            "prospect_id",
            "email",
            "domain",
            "segment",
            "role_or_title",
            "state_pref",
            "rank_reason",
            "rank_tuple",
            "batch",
            "state",
        ],
        outbox_rows,
    )

    ts_utc = datetime.now(timezone.utc).isoformat()
    manifest_out: list[dict] = []
    for item in manifest_rows:
        row = dict(item)
        row["ts_utc"] = ts_utc
        row["batch"] = batch
        row["state"] = state
        manifest_out.append(row)
    _write_csv(
        manifest_path,
        [
            "ts_utc",
            "batch",
            "state",
            "prospect_id",
            "email",
            "domain",
            "segment",
            "role_or_title",
            "state_pref",
            "status",
            "reason",
            "rank_reason",
            "rank_tuple",
        ],
        manifest_out,
    )
    return outbox_path, manifest_path


def _plan_skip_breakdown(skipped: Counter) -> dict[str, int]:
    suppressed = int(skipped.get("suppressed", 0))
    invalid_email = int(skipped.get("invalid_email", 0))
    do_not_contact = int(skipped.get("status_do_not_contact", 0))
    already_contacted = int(skipped.get("already_contacted", 0))
    other = int(sum(skipped.values()) - (suppressed + invalid_email + do_not_contact + already_contacted))
    return {
        "suppressed": max(0, suppressed),
        "invalid_email": max(0, invalid_email),
        "do_not_contact": max(0, do_not_contact),
        "already_contacted": max(0, already_contacted),
        "other": max(0, other),
    }


def _print_plan_output(
    run_date: date,
    state: str,
    batch: str,
    daily_limit: int,
    selected: list[dict],
    skipped: Counter,
    diagnostics: dict,
    diagnostics_path: Path,
) -> None:
    breakdown = _plan_skip_breakdown(skipped)
    filter_breakdown = diagnostics.get("filter_breakdown") or {}
    pool_total_selected_state = max(0, int(filter_breakdown.get("pool_total_selected_state", 0)))
    pool_total_all_states = max(0, int(filter_breakdown.get("pool_total_all_states", 0)))
    filter_breakdown_json = json.dumps(filter_breakdown, separators=(",", ":"), sort_keys=True)
    print(f"OUTREACH_PLAN_DATE={run_date.isoformat()}")
    print(f"OUTREACH_PLAN_STATE={state}")
    print(f"OUTREACH_PLAN_BATCH={batch}")
    print(f"OUTREACH_PLAN_DAILY_LIMIT={daily_limit}")
    print(f"OUTREACH_PLAN_POOL_TOTAL={pool_total_selected_state}")
    print(f"OUTREACH_PLAN_POOL_TOTAL_ALL_STATES={pool_total_all_states}")
    print(f"OUTREACH_PLAN_POOL_TOTAL_SELECTED_STATE={pool_total_selected_state}")
    print(f"OUTREACH_PLAN_WILL_SEND={len(selected)}")
    print(
        "OUTREACH_PLAN_SKIP_BREAKDOWN "
        f"suppressed={breakdown['suppressed']} "
        f"invalid_email={breakdown['invalid_email']} "
        f"do_not_contact={breakdown['do_not_contact']} "
        f"already_contacted={breakdown['already_contacted']} "
        f"other={breakdown['other']}"
    )
    print(f"OUTREACH_PLAN_FILTER_BREAKDOWN={filter_breakdown_json}")
    print(f"OUTREACH_PLAN_DIAGNOSTICS_PATH={diagnostics_path}")
    print("prospect_id,email,domain,segment,role_or_title,state_pref,rank_reason")
    for candidate in selected:
        print(
            ",".join(
                [
                    _safe_csv_value(candidate["prospect_id"]),
                    _safe_csv_value(candidate["email"]),
                    _safe_csv_value(candidate["domain"]),
                    _safe_csv_value(candidate["segment"]),
                    _safe_csv_value(candidate["role_or_title"]),
                    _safe_csv_value(candidate["state_pref"]),
                    _safe_csv_value(candidate["rank_reason"]),
                ]
            )
        )


def _render_outreach_payload(
    row: sqlite3.Row,
    state: str,
    batch: str,
    template_text: str,
    html_template_text: str,
    recent_signals_lines: str,
    recent_signals_html: str,
    last_refresh_et: str,
) -> tuple[str, str, str, str]:
    first_name = (str(row["contact_name"] or "").split(" ")[:1] or [""])[0].strip() or "there"
    firm = str(row["firm"] or "").strip() or "your firm"
    prospect_id = str(row["prospect_id"] or "").strip()
    email = _norm_email(str(row["email"] or ""))

    territory_code = batch
    subscriber_key = gm._subscriber_key_from_prospect_id(prospect_id, territory_code)
    unsub_url, prefs_url = gm._build_urls(
        email=email,
        prospect_id=prospect_id,
        subscriber_key=subscriber_key,
        territory_code=territory_code,
        batch=batch,
        allow_mailto_fallback=False,
    )
    prefs_link = prefs_url or unsub_url
    subject = f"{state} OSHA activity signals - {firm}".strip()

    text_body = (
        gm._render_template(
            template_text,
            {
                "FIRST_NAME": first_name,
                "FIRM": firm,
                "STATE": state,
                "TERRITORY_CODE": territory_code,
                "RECENT_SIGNALS_LINES": recent_signals_lines,
                "LAST_REFRESH_ET": last_refresh_et,
                "UNSUBSCRIBE_URL": unsub_url,
                "PREFS_URL": prefs_link,
            },
        ).strip()
        + "\n"
    )

    if html_template_text.strip():
        html_body = gm._render_template(
            html_template_text,
            {
                "{{FIRST_NAME}}": gm._html_escape(first_name),
                "{{FIRM}}": gm._html_escape(firm),
                "{{STATE}}": gm._html_escape(state),
                "{{RECENT_SIGNALS_HTML}}": recent_signals_html,
                "{{LAST_REFRESH_ET}}": gm._html_escape(last_refresh_et),
                "{{UNSUBSCRIBE_URL}}": gm._html_escape(unsub_url),
                "{{PREFS_URL}}": gm._html_escape(prefs_link),
                "{{MAILING_ADDRESS}}": gm._html_escape(gm._resolve_outreach_mailing_address()),
                "{{MICROFLOWOPS_URL}}": gm._html_escape(
                    (os.getenv("MICROFLOWOPS_URL") or "https://microflowops.com").strip() or "https://microflowops.com"
                ),
            },
        ).strip()
    else:
        html_body = (
            "<div style=\"font-family: system-ui, -apple-system, 'Segoe UI', Roboto, Arial, sans-serif;\">"
            "<pre style=\"white-space: pre-wrap; font-size: 13px; line-height: 1.4;\">"
            + gm._html_escape(text_body)
            + "</pre></div>"
        )

    return subject, text_body, html_body, unsub_url


def _send_outreach_email(
    row: sqlite3.Row,
    state: str,
    batch: str,
    template_text: str,
    html_template_text: str,
    recent_signals_lines: str,
    recent_signals_html: str,
    last_refresh_et: str,
) -> dict:
    import send_digest_email as sde

    subject, text_body, html_body, unsub_url = _render_outreach_payload(
        row=row,
        state=state,
        batch=batch,
        template_text=template_text,
        html_template_text=html_template_text,
        recent_signals_lines=recent_signals_lines,
        recent_signals_html=recent_signals_html,
        last_refresh_et=last_refresh_et,
    )

    branding = sde.resolve_branding({})
    reply_to = (branding.get("reply_to") or os.getenv("REPLY_TO_EMAIL") or "support@microflowops.com").strip()
    mailto = f"mailto:{reply_to}?subject=unsubscribe"
    list_unsub = f"<{mailto}>, <{unsub_url}>"
    list_unsub_post = "List-Unsubscribe=One-Click"

    ok, message_id, err = sde.send_email(
        recipient=_norm_email(str(row["email"] or "")),
        subject=subject,
        html_body=html_body,
        text_body=text_body,
        customer_id="",
        territory_code=batch,
        branding=branding,
        dry_run=False,
        list_unsub=list_unsub,
        list_unsub_post=list_unsub_post,
        label="outreach_auto_campaign",
    )
    return {
        "prospect_id": str(row["prospect_id"]),
        "email": _norm_email(str(row["email"] or "")),
        "ok": bool(ok),
        "message_id": message_id or "",
        "error": err or "",
        "subject": subject,
    }


def _write_events_and_status_updates(conn: sqlite3.Connection, batch: str, results: list[dict]) -> None:
    ts = datetime.now(timezone.utc).isoformat()
    cur = conn.cursor()
    conn.execute("BEGIN")
    for item in results:
        event_type = "sent" if item.get("ok") else "send_failed"
        metadata = {
            "email": item.get("email", ""),
            "message_id": item.get("message_id", ""),
            "error": item.get("error", ""),
            "subject": item.get("subject", ""),
        }
        cur.execute(
            """
            INSERT INTO outreach_events(prospect_id, ts, event_type, batch_id, metadata_json)
            VALUES(?, ?, ?, ?, ?)
            """,
            (
                item["prospect_id"],
                ts,
                event_type,
                batch,
                json.dumps(metadata, separators=(",", ":"), ensure_ascii=True),
            ),
        )
        if item.get("ok"):
            cur.execute(
                """
                UPDATE prospects
                SET status = 'contacted',
                    last_contacted_at = ?
                WHERE prospect_id = ?
                """,
                (ts, item["prospect_id"]),
            )
    conn.commit()


def _append_ledger_records(path: Path, batch: str, state: str, results: list[dict]) -> None:
    records = []
    ts = datetime.now(timezone.utc).isoformat()
    for item in results:
        if not item.get("ok"):
            continue
        records.append(
            {
                "prospect_id": item.get("prospect_id", ""),
                "batch": batch,
                "state": state,
                "exported_at_utc": ts,
            }
        )
    if not records:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, separators=(",", ":"), ensure_ascii=True) + "\n")


def _event_count_for_day(conn: sqlite3.Connection, event_type: str, run_date: date) -> int:
    day = run_date.isoformat()
    row = conn.execute(
        "SELECT COUNT(*) FROM outreach_events WHERE event_type = ? AND substr(ts, 1, 10) = ?",
        (event_type, day),
    ).fetchone()
    return int(row[0] or 0) if row else 0


def _doctor_check_secrets_decrypt() -> tuple[bool, str]:
    wrapper = REPO_ROOT / "run_with_secrets.ps1"
    if not wrapper.exists():
        return _doctor_error(ERR_DOCTOR_SECRETS_DECRYPT, f"wrapper_missing path={wrapper}")

    cmd = [
        "powershell",
        "-NoProfile",
        "-ExecutionPolicy",
        "Bypass",
        "-File",
        str(wrapper),
        "--diagnostics",
        "--check-decrypt",
    ]
    try:
        proc = subprocess.run(
            cmd,
            cwd=str(REPO_ROOT),
            capture_output=True,
            text=True,
            timeout=DOCTOR_TIMEOUT_SECRETS_SECONDS,
        )
    except subprocess.TimeoutExpired:
        return _doctor_error(ERR_DOCTOR_SECRETS_DECRYPT, "timeout")
    except Exception as e:
        return _doctor_error(ERR_DOCTOR_SECRETS_DECRYPT, f"subprocess_failed err={type(e).__name__}")

    lines = []
    if proc.stdout:
        lines.extend(proc.stdout.splitlines())
    if proc.stderr:
        lines.extend(proc.stderr.splitlines())
    pass_line = next((ln.strip() for ln in lines if ln.startswith("PASS:")), "")
    fail_line = next((ln.strip() for ln in lines if ln.startswith("FAIL:")), "")
    if proc.returncode != 0 or not pass_line:
        detail = fail_line if fail_line else _compact_detail((proc.stdout or "") + " " + (proc.stderr or ""))
        return _doctor_error(ERR_DOCTOR_SECRETS_DECRYPT, f"diag_failed code={proc.returncode} detail={detail}")

    print(f"{PASS_DOCTOR_SECRETS_DECRYPT} diagnostics=ok")
    return True, ""


def _doctor_context_pack_soft_check() -> None:
    script_path = REPO_ROOT / "tools" / "project_context_pack.py"
    if not script_path.exists():
        print("WARN_CONTEXT_PACK_SCRIPT_MISSING tools/project_context_pack.py")
        return

    commands = [
        ["py", "-3", str(script_path)] + PROJECT_CONTEXT_SOFT_CHECK_CMD,
        [sys.executable, str(script_path)] + PROJECT_CONTEXT_SOFT_CHECK_CMD,
    ]
    for idx, cmd in enumerate(commands):
        try:
            proc = subprocess.run(
                cmd,
                cwd=str(REPO_ROOT),
                capture_output=True,
                text=True,
            )
        except FileNotFoundError:
            if idx == 0:
                continue
            print("WARN_CONTEXT_PACK_CHECK_FAILED runner_not_found")
            return
        except Exception as e:
            print(f"WARN_CONTEXT_PACK_CHECK_FAILED error={type(e).__name__}")
            return

        lines: list[str] = []
        if proc.stdout:
            lines.extend([ln.strip() for ln in proc.stdout.splitlines() if ln.strip()])
        if proc.stderr:
            lines.extend([ln.strip() for ln in proc.stderr.splitlines() if ln.strip()])

        has_warn = any(ln.startswith("WARN_CONTEXT_PACK_") or ln.startswith("ERR_CONTEXT_PACK_") for ln in lines)
        if has_warn:
            for line in lines:
                if line.startswith("PASS_CONTEXT_PACK_CHECK"):
                    continue
                print(line)
            return

        if proc.returncode != 0:
            print(f"WARN_CONTEXT_PACK_CHECK_FAILED returncode={proc.returncode}")
            for line in lines:
                if line.startswith("PASS_CONTEXT_PACK_CHECK"):
                    continue
                print(line)
        return


def _doctor_parse_env() -> tuple[bool, str, dict]:
    ctx: dict[str, object] = {}

    raw_states = (os.getenv("OUTREACH_STATES") or "").strip()
    if not raw_states:
        return _doctor_error(ERR_DOCTOR_ENV_MISSING_PREFIX + "OUTREACH_STATES") + ({},)
    states = _parse_states(raw_states)
    if not states:
        return _doctor_error(ERR_DOCTOR_ENV_INVALID_PREFIX + "OUTREACH_STATES", f"value={_compact_detail(raw_states)}") + ({},)
    ctx["states"] = states

    raw_limit = (os.getenv("OUTREACH_DAILY_LIMIT") or "").strip()
    if not raw_limit:
        return _doctor_error(ERR_DOCTOR_ENV_MISSING_PREFIX + "OUTREACH_DAILY_LIMIT") + ({},)
    try:
        daily_limit = int(raw_limit)
    except Exception:
        return _doctor_error(ERR_DOCTOR_ENV_INVALID_PREFIX + "OUTREACH_DAILY_LIMIT", f"value={raw_limit}") + ({},)
    if daily_limit < 1:
        return _doctor_error(ERR_DOCTOR_ENV_INVALID_PREFIX + "OUTREACH_DAILY_LIMIT", f"value={daily_limit}") + ({},)
    ctx["daily_limit"] = daily_limit

    smoke_to = (os.getenv("OSHA_SMOKE_TO") or "").strip()
    if not smoke_to:
        return _doctor_error(ERR_DOCTOR_ENV_MISSING_PREFIX + "OSHA_SMOKE_TO") + ({},)
    if not _is_valid_email_shape(smoke_to):
        return _doctor_error(ERR_DOCTOR_ENV_INVALID_PREFIX + "OSHA_SMOKE_TO", f"value={_compact_detail(smoke_to, 120)}") + ({},)
    ctx["smoke_to"] = _norm_email(smoke_to)

    raw_max_age = (os.getenv("OUTREACH_SUPPRESSION_MAX_AGE_HOURS") or "").strip()
    if not raw_max_age:
        return _doctor_error(ERR_DOCTOR_ENV_MISSING_PREFIX + "OUTREACH_SUPPRESSION_MAX_AGE_HOURS") + ({},)
    try:
        suppression_max_age_hours = float(raw_max_age)
    except Exception:
        return _doctor_error(
            ERR_DOCTOR_ENV_INVALID_PREFIX + "OUTREACH_SUPPRESSION_MAX_AGE_HOURS",
            f"value={raw_max_age}",
        ) + ({},)
    if suppression_max_age_hours <= 0:
        return _doctor_error(
            ERR_DOCTOR_ENV_INVALID_PREFIX + "OUTREACH_SUPPRESSION_MAX_AGE_HOURS",
            f"value={suppression_max_age_hours}",
        ) + ({},)
    ctx["suppression_max_age_hours"] = suppression_max_age_hours

    print(
        f"{PASS_DOCTOR_ENV} outreach_states={','.join(states)} daily_limit={daily_limit} "
        f"smoke_to={ctx['smoke_to']} suppression_max_age_hours={suppression_max_age_hours:.1f}"
    )
    return True, "", ctx


def _doctor_check_crm() -> tuple[bool, str]:
    crm_db = _crm_db_path()
    if not crm_db.exists():
        return _doctor_error(ERR_DOCTOR_CRM_REQUIRED, f"crm_missing path={crm_db}")
    try:
        conn = _connect_existing_crm_readonly(crm_db)
    except Exception as e:
        return _doctor_error(ERR_DOCTOR_CRM_REQUIRED, f"crm_open_failed path={crm_db} err={type(e).__name__}")
    try:
        if not _require_schema(conn):
            return _doctor_error(ERR_DOCTOR_CRM_SCHEMA, f"schema_missing path={crm_db}")
    finally:
        conn.close()

    print(f"{PASS_DOCTOR_CRM_REQUIRED} crm_db={crm_db.resolve()}")
    return True, ""


def _doctor_check_suppression(ctx: dict[str, object]) -> tuple[bool, str]:
    suppression_csv = _suppression_csv_path()
    if not suppression_csv.exists():
        return _doctor_error(ERR_DOCTOR_SUPPRESSION_REQUIRED, f"path={suppression_csv}")

    try:
        with open(suppression_csv, "r", encoding="utf-8", newline="") as f:
            _ = f.read(1)
    except Exception as e:
        return _doctor_error(ERR_DOCTOR_SUPPRESSION_UNREADABLE, f"path={suppression_csv} err={type(e).__name__}")

    try:
        max_age_hours = float(ctx.get("suppression_max_age_hours", 0.0))
    except Exception:
        return _doctor_error(ERR_DOCTOR_ENV_INVALID_PREFIX + "OUTREACH_SUPPRESSION_MAX_AGE_HOURS")
    if max_age_hours <= 0:
        return _doctor_error(ERR_DOCTOR_ENV_INVALID_PREFIX + "OUTREACH_SUPPRESSION_MAX_AGE_HOURS")

    try:
        mtime = float(suppression_csv.stat().st_mtime)
    except Exception as e:
        return _doctor_error(ERR_DOCTOR_SUPPRESSION_UNREADABLE, f"path={suppression_csv} err={type(e).__name__}")
    age_hours = max(0.0, (time.time() - mtime) / 3600.0)
    if age_hours > max_age_hours:
        return _doctor_error(
            ERR_DOCTOR_SUPPRESSION_STALE,
            f"path={suppression_csv} age_hours={age_hours:.1f} max_age_hours={max_age_hours:.1f}",
        )

    print(f"{PASS_DOCTOR_SUPPRESSION} path={suppression_csv.resolve()} age_hours={age_hours:.1f} max_age_hours={max_age_hours:.1f}")
    return True, ""


def _doctor_probe_http(url: str) -> tuple[int, str]:
    try:
        req = Request(url, method="GET")
        with urlopen(req, timeout=DOCTOR_HTTP_TIMEOUT_SECONDS) as resp:
            return int(getattr(resp, "status", 200)), ""
    except HTTPError as e:
        return int(getattr(e, "code", 0) or 0), ""
    except URLError as e:
        return 0, _compact_detail(getattr(e, "reason", e))
    except Exception as e:
        return 0, type(e).__name__


def _doctor_is_reachable_status(status: int) -> bool:
    return bool(status and status < 500 and status != 404)


def _doctor_check_unsub() -> tuple[bool, str]:
    one_click_ok, reason = gm._one_click_config_present()
    if not one_click_ok:
        return _doctor_error(ERR_DOCTOR_UNSUB_CONFIG, reason or "missing_one_click_config")

    try:
        host_base, unsub_url = gm._unsub_host_base()
    except Exception as e:
        return _doctor_error(ERR_DOCTOR_UNSUB_CONFIG, f"resolve_failed err={type(e).__name__}")

    parsed = urlparse(host_base or "")
    if not parsed.scheme or not parsed.netloc:
        return _doctor_error(ERR_DOCTOR_UNSUB_CONFIG, "invalid_unsub_host_base")
    if not unsub_url:
        return _doctor_error(ERR_DOCTOR_UNSUB_CONFIG, "missing_unsubscribe_url")

    version_url = f"{parsed.scheme}://{parsed.netloc}/__version"
    version_status, version_err = _doctor_probe_http(version_url)
    if not _doctor_is_reachable_status(version_status):
        detail = f"url={version_url} status={version_status or 'error'}"
        if version_err:
            detail += f" err={version_err}"
        return _doctor_error(ERR_DOCTOR_UNSUB_UNREACHABLE, detail)

    unsub_status, unsub_err = _doctor_probe_http(unsub_url)
    if not _doctor_is_reachable_status(unsub_status):
        detail = f"url={unsub_url} status={unsub_status or 'error'}"
        if unsub_err:
            detail += f" err={unsub_err}"
        return _doctor_error(ERR_DOCTOR_UNSUB_UNREACHABLE, detail)

    print(f"{PASS_DOCTOR_UNSUB} version_status={version_status} unsubscribe_status={unsub_status}")
    return True, ""


def _doctor_check_provider() -> tuple[bool, str]:
    required = ["SMTP_HOST", "SMTP_PORT", "SMTP_USER", "SMTP_PASS"]
    missing = [key for key in required if not (os.getenv(key) or "").strip()]
    if missing:
        return _doctor_error(ERR_DOCTOR_PROVIDER_CONFIG, "missing=" + ",".join(missing))

    raw_port = (os.getenv("SMTP_PORT") or "").strip()
    try:
        smtp_port = int(raw_port)
    except Exception:
        return _doctor_error(ERR_DOCTOR_PROVIDER_CONFIG, f"invalid_smtp_port={raw_port}")
    if not (1 <= smtp_port <= 65535):
        return _doctor_error(ERR_DOCTOR_PROVIDER_CONFIG, f"invalid_smtp_port={smtp_port}")

    print(f"{PASS_DOCTOR_PROVIDER_CONFIG} smtp_port={smtp_port}")
    return True, ""


def _doctor_check_dry_run_artifact(allow_repeat: bool, run_date: date) -> tuple[bool, str]:
    entrypoint = REPO_ROOT / "run_outreach_auto.py"
    if not entrypoint.exists():
        return _doctor_error(ERR_DOCTOR_DRY_RUN_ARTIFACT, f"entrypoint_missing path={entrypoint}")

    cmd = [sys.executable, str(entrypoint), "--dry-run"]
    if allow_repeat:
        cmd.append("--allow-repeat")
    cmd.extend(["--for-date", run_date.isoformat()])
    try:
        proc = subprocess.run(
            cmd,
            cwd=str(REPO_ROOT),
            capture_output=True,
            text=True,
            timeout=DOCTOR_TIMEOUT_DRY_RUN_SECONDS,
        )
    except subprocess.TimeoutExpired:
        return _doctor_error(ERR_DOCTOR_DRY_RUN_ARTIFACT, "timeout")
    except Exception as e:
        return _doctor_error(ERR_DOCTOR_DRY_RUN_ARTIFACT, f"subprocess_failed err={type(e).__name__}")

    dry_run_text = ((proc.stdout or "") + "\n" + (proc.stderr or "")).strip()
    if proc.returncode != 0:
        return _doctor_error(
            ERR_DOCTOR_DRY_RUN_ARTIFACT,
            f"dry_run_failed code={proc.returncode} detail={_compact_detail(dry_run_text)}",
        )
    if PASS_AUTO_DRY_RUN not in dry_run_text:
        return _doctor_error(ERR_DOCTOR_DRY_RUN_ARTIFACT, f"missing_token={PASS_AUTO_DRY_RUN}")

    print(f"{PASS_DOCTOR_DRY_RUN_ARTIFACT} dry_run_token={PASS_AUTO_DRY_RUN}")
    return True, ""


def _doctor_check_idempotency(allow_repeat: bool) -> tuple[bool, str]:
    crm_db = _crm_db_path()
    try:
        conn = _connect_existing_crm_readonly(crm_db)
    except Exception as e:
        return _doctor_error(ERR_DOCTOR_IDEMPOTENCY, f"crm_open_failed path={crm_db} err={type(e).__name__}")
    try:
        row = conn.execute(
            """
            SELECT prospect_id, batch_id, COUNT(*) AS c
            FROM outreach_events
            WHERE event_type = 'sent'
            GROUP BY prospect_id, batch_id
            HAVING COUNT(*) > 1
            LIMIT 1
            """
        ).fetchone()
    finally:
        conn.close()

    if row:
        return _doctor_error(
            ERR_DOCTOR_IDEMPOTENCY,
            f"duplicate_sent_events prospect_id={row['prospect_id']} batch_id={row['batch_id']} count={int(row['c'] or 0)}",
        )

    print(f"{PASS_DOCTOR_IDEMPOTENCY} allow_repeat={bool(allow_repeat)} duplicates=0")
    return True, ""


def _run_doctor(allow_repeat: bool, run_date: date) -> tuple[bool, str]:
    _doctor_context_pack_soft_check()

    ok, msg = _doctor_check_secrets_decrypt()
    if not ok:
        return False, msg

    ok_env, msg_env, ctx = _doctor_parse_env()
    if not ok_env:
        return False, msg_env

    checks = [
        _doctor_check_crm,
        lambda: _doctor_check_suppression(ctx),
        _doctor_check_unsub,
        _doctor_check_provider,
        lambda: _doctor_check_dry_run_artifact(allow_repeat=allow_repeat, run_date=run_date),
        lambda: _doctor_check_idempotency(allow_repeat=allow_repeat),
    ]
    for check in checks:
        ok, msg = check()
        if not ok:
            return False, msg

    print(PASS_DOCTOR_COMPLETE)
    return True, ""


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Daily outreach automation: select->prioritize->send->record from SQLite CRM."
    )
    ap.add_argument("--doctor", action="store_true", help="Run non-sending readiness checks and exit.")
    ap.add_argument("--dry-run", action="store_true", help="Select and print actions only. No DB writes, no email.")
    ap.add_argument("--plan", action="store_true", help="Print deterministic no-send outreach plan and selected prospects.")
    ap.add_argument("--print-config", action="store_true", help="Print resolved config paths/state and exit.")
    ap.add_argument("--for-date", default="", help="Override run date (YYYY-MM-DD) for doctor/print-config/dry-run/plan.")
    ap.add_argument("--allow-repeat", action="store_true", help="Allow contacting previously contacted prospects.")
    ap.add_argument("--to", default="", help="Optional summary recipient override; must equal OSHA_SMOKE_TO.")
    args = ap.parse_args()

    ok_date, run_date, date_msg = _parse_for_date(str(args.for_date or ""))
    if not ok_date:
        print(date_msg, file=sys.stderr)
        return 2
    today_local = datetime.now().date()

    if args.doctor:
        ok, msg = _run_doctor(allow_repeat=bool(args.allow_repeat), run_date=run_date)
        if not ok:
            print(msg, file=sys.stderr)
            return 2
        return 0

    states = _parse_states(os.getenv("OUTREACH_STATES", "TX"))
    if not states:
        print(f"{ERR_AUTO_ENV} OUTREACH_STATES missing", file=sys.stderr)
        return 2

    state = _choose_state(states, run_date)
    batch = _batch_id(state, run_date)
    limit = _daily_limit()
    crm_db = _crm_db_path()
    suppression_csv = _suppression_csv_path()
    export_ledger = _export_ledger_path()

    if args.print_config:
        daily_limit, daily_limit_source = _daily_limit_with_source()
        trial_conversion_url_present = "YES" if (os.getenv("TRIAL_CONVERSION_URL") or "").strip() else "NO"
        print(f"{PASS_AUTO_PRINT_CONFIG} data_dir={_data_dir().resolve()}")
        print(f"{PASS_AUTO_PRINT_CONFIG} crm_db={crm_db.resolve()}")
        print(f"{PASS_AUTO_PRINT_CONFIG} suppression_csv={suppression_csv.resolve()}")
        print(f"{PASS_AUTO_PRINT_CONFIG} export_ledger={export_ledger.resolve()}")
        print(f"{PASS_AUTO_PRINT_CONFIG} outreach_daily_limit={daily_limit} source={daily_limit_source}")
        print(f"{PASS_AUTO_PRINT_CONFIG} outreach_states={','.join(states)} selected_state={state}")
        print(f"{PASS_AUTO_PRINT_CONFIG} batch_id={batch}")
        print(f"{PASS_AUTO_PRINT_CONFIG} run_date={run_date.isoformat()}")
        print(f"trial_conversion_url_present={trial_conversion_url_present}")
        return 0

    is_live_send = not bool(args.dry_run or args.plan or args.doctor or args.print_config)
    if is_live_send and run_date != today_local:
        print(
            f"{ERR_AUTO_FOR_DATE_LIVE_SEND_BLOCKED} for_date={run_date.isoformat()} today={today_local.isoformat()}",
            file=sys.stderr,
        )
        return 2

    if is_live_send:
        ok_to, summary_to, msg = _resolve_summary_recipient(args.to)
        if not ok_to:
            print(msg, file=sys.stderr)
            return 2
    else:
        ok_to, summary_to, _msg = _resolve_summary_recipient(args.to)
        summary_to = summary_to if ok_to else "(missing OSHA_SMOKE_TO)"

    if not crm_db.exists():
        print(f"{ERR_AUTO_CRM_REQUIRED} crm_missing path={crm_db}", file=sys.stderr)
        return 2

    try:
        conn = _connect_existing_crm(crm_db)
    except Exception as e:
        print(f"{ERR_AUTO_CRM_REQUIRED} crm_open_failed path={crm_db} err={e}", file=sys.stderr)
        return 2

    try:
        if not _require_schema(conn):
            print(f"{ERR_AUTO_CRM_REQUIRED} schema_missing path={crm_db}", file=sys.stderr)
            return 2

        try:
            suppressed_emails = _load_suppression_emails(conn)
        except ValueError as e:
            print(str(e), file=sys.stderr)
            return 3

        selected, skipped, manifest_rows, selection_stats = _select_candidates(
            conn=conn,
            state=state,
            limit=limit,
            suppressed_emails=suppressed_emails,
            allow_repeat=bool(args.allow_repeat),
        )
        pool_total_all_states = _count_pool_total_all_states(conn)
        plan_diagnostics = _build_plan_diagnostics(
            run_date=run_date,
            state=state,
            batch=batch,
            daily_limit=limit,
            selected=selected,
            skipped=skipped,
            pool_total_all_states=pool_total_all_states,
            selection_stats=selection_stats,
        )
        diagnostics_path: Path | None = None
        if args.plan or args.dry_run:
            diagnostics_path = _write_plan_diagnostics(batch=batch, payload=plan_diagnostics)

        selected_ids = [str(r["prospect_id"]) for r in selected]
        skipped_count = int(sum(skipped.values()))
        top_skip = _format_top_reasons(skipped, limit=5)

        if args.plan:
            _print_plan_output(
                run_date=run_date,
                state=state,
                batch=batch,
                daily_limit=limit,
                selected=selected,
                skipped=skipped,
                diagnostics=plan_diagnostics,
                diagnostics_path=diagnostics_path or _plan_diagnostics_path(batch),
            )
            return 0

        if args.dry_run:
            outbox_path, manifest_path = _write_dry_run_artifacts(
                batch=batch,
                state=state,
                selected=selected,
                manifest_rows=manifest_rows,
            )
            print(
                f"{PASS_AUTO_DRY_RUN} state={state} batch={batch} daily_limit={limit} crm_db={crm_db} allow_repeat={bool(args.allow_repeat)}"
            )
            print(f"{PASS_AUTO_DRY_RUN} would_contact_prospect_ids={','.join(selected_ids) if selected_ids else '(none)'}")
            print(f"{PASS_AUTO_DRY_RUN} skipped_count={skipped_count} top_skip_reasons={top_skip}")
            print(f"{PASS_AUTO_DRY_RUN} summary_to={summary_to}")
            print(f"{PASS_AUTO_DRY_RUN} outbox_path={outbox_path}")
            print(f"{PASS_AUTO_DRY_RUN} manifest_path={manifest_path}")
            print(f"OUTREACH_PLAN_DIAGNOSTICS_PATH={diagnostics_path or _plan_diagnostics_path(batch)}")
            return 0

        one_click_ok, reason = gm._one_click_config_present()
        if not one_click_ok:
            print(f"{ERR_AUTO_ONE_CLICK_REQUIRED} {reason}".strip(), file=sys.stderr)
            return 2

        template_text = gm._read_template_text(REPO_ROOT / "outreach" / "outreach_plain.txt")
        try:
            html_template_text = gm._read_template_text(REPO_ROOT / "outreach" / "outreach_card.html")
        except Exception:
            html_template_text = ""

        osha_db = str((os.getenv("OUTREACH_SIGNAL_DB") or "").strip() or (REPO_ROOT / "data" / "osha.sqlite"))
        recent_leads, last_refresh_et = gm._best_effort_recent_leads_and_refresh(
            db_path=osha_db,
            state=state,
            limit=5,
        )
        recent_signals_lines = gm._recent_signals_text_lines_from_leads(recent_leads)
        recent_signals_html = gm._recent_signals_html_from_leads(recent_leads)

        send_results: list[dict] = []
        for selected_candidate in selected:
            row = selected_candidate["row"]
            send_results.append(
                _send_outreach_email(
                    row=row,
                    state=state,
                    batch=batch,
                    template_text=template_text,
                    html_template_text=html_template_text,
                    recent_signals_lines=recent_signals_lines,
                    recent_signals_html=recent_signals_html,
                    last_refresh_et=last_refresh_et,
                )
            )

        _write_events_and_status_updates(conn, batch=batch, results=send_results)
        _append_ledger_records(path=export_ledger, batch=batch, state=state, results=send_results)

        contacted_count = sum(1 for r in send_results if r.get("ok"))
        failed_count = sum(1 for r in send_results if not r.get("ok"))
        new_replies = _event_count_for_day(conn, "replied", run_date)
        new_trials = _event_count_for_day(conn, "trial_started", run_date)
        new_conversions = _event_count_for_day(conn, "converted", run_date)
        next_actions = (
            "Review send failures and retry unresolved prospects."
            if failed_count
            else "Review replies and mark trial_started/converted via crm_admin.py mark."
        )
        if contacted_count == 0:
            next_actions = "Seed more prospects in crm.sqlite or use --allow-repeat for follow-up."

        print(
            f"{PASS_AUTO_EXPORT} batch={batch} state={state} contacted_count={contacted_count} "
            f"skipped_count={skipped_count} failed_count={failed_count}"
        )
        print(f"{PASS_AUTO_EXPORT} contacted_prospect_ids={','.join([r['prospect_id'] for r in send_results if r.get('ok')]) or '(none)'}")
        print(f"{PASS_AUTO_EXPORT} skipped_top_reasons={top_skip}")

        subject = f"[AUTO] Outreach {batch} contacted={contacted_count} skipped={skipped_count} failed={failed_count}"
        text_body = (
            "Outreach auto-run summary\n"
            f"- state: {state}\n"
            f"- batch: {batch}\n"
            f"- contacted_count: {contacted_count}\n"
            f"- skipped_count: {skipped_count}\n"
            f"- skipped_top_reasons: {top_skip}\n"
            f"- new_replies: {new_replies}\n"
            f"- new_trials: {new_trials}\n"
            f"- new_conversions: {new_conversions}\n"
            f"- next_actions: {next_actions}\n"
        )
        html_body = (
            "<div style=\"font-family: system-ui, -apple-system, 'Segoe UI', Roboto, Arial, sans-serif;\">"
            "<h3>Outreach Auto-Run Summary</h3>"
            f"<p><strong>state:</strong> {state}<br>"
            f"<strong>batch:</strong> {batch}<br>"
            f"<strong>contacted_count:</strong> {contacted_count}<br>"
            f"<strong>skipped_count:</strong> {skipped_count}<br>"
            f"<strong>skipped_top_reasons:</strong> {top_skip}<br>"
            f"<strong>new_replies:</strong> {new_replies}<br>"
            f"<strong>new_trials:</strong> {new_trials}<br>"
            f"<strong>new_conversions:</strong> {new_conversions}<br>"
            f"<strong>next_actions:</strong> {next_actions}</p>"
            "</div>"
        )
        ok_send, err = _send_summary_email(summary_to, subject, text_body, html_body)
        if not ok_send:
            print(f"{ERR_AUTO_SUMMARY_SEND} {err}", file=sys.stderr)
            return 1

        print(f"{PASS_AUTO_SUMMARY} to={summary_to} batch={batch}")
        if failed_count:
            return 1
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
