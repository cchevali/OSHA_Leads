import argparse
import csv
import json
import re
import sqlite3
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from shutil import copyfile

try:  # pragma: no cover
    from dotenv import load_dotenv

    load_dotenv()
except Exception:  # pragma: no cover
    pass


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from outreach import crm_store


SCHEMA_VERSION = "v1"
UNKNOWN_BATCH = "UNKNOWN"
UNKNOWN_STATE = "UNKNOWN"
NO_WRITE_PATH_SENTINEL = "(no-write)"
ERR_OPS_CRM_REQUIRED = "ERR_OPS_CRM_REQUIRED"
ERR_OPS_CRM_SCHEMA = "ERR_OPS_CRM_SCHEMA"

ROLE_LOCAL_PARTS = {
    "admin",
    "billing",
    "careers",
    "contact",
    "customerservice",
    "enquiries",
    "hello",
    "help",
    "hr",
    "info",
    "inquiries",
    "jobs",
    "marketing",
    "office",
    "sales",
    "service",
    "support",
    "team",
}

INFERRED_BOUNCE_REASON_TOKENS = (
    "abuse",
    "block",
    "bounce",
    "complaint",
    "hard",
    "invalid",
    "spam",
    "undeliver",
)

WINDOWS = {"7d": 7, "30d": 30}


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _parse_ts(raw: str) -> datetime | None:
    text = (raw or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except Exception:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def _norm_email(email: str) -> str:
    return (email or "").strip().lower()


def _email_domain(email: str) -> str:
    e = _norm_email(email)
    if "@" not in e:
        return ""
    return e.split("@", 1)[1].strip().lower()


def _safe_json(raw: str) -> dict:
    try:
        obj = json.loads(raw or "{}")
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _looks_valid_email(email: str) -> bool:
    return bool(re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", _norm_email(email)))


def _is_role_based_inbox(email: str) -> bool:
    e = _norm_email(email)
    if "@" not in e:
        return False
    local = e.split("@", 1)[0].split("+", 1)[0]
    return local in ROLE_LOCAL_PARTS


def _is_two_letter_state(text: str) -> bool:
    return bool(re.fullmatch(r"[A-Z]{2}", (text or "").strip().upper()))


def _state_from_batch(batch_id: str) -> str:
    batch = (batch_id or "").strip().upper()
    m = re.search(r"_([A-Z]{2})$", batch)
    if m:
        return m.group(1)
    return ""


def _cohort_key(batch_id: str, state_at_send: str) -> tuple[str, str]:
    batch = (batch_id or "").strip() or UNKNOWN_BATCH
    state = (state_at_send or "").strip().upper()
    if not _is_two_letter_state(state):
        state = UNKNOWN_STATE
    return batch, state


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name = ? LIMIT 1",
        (table_name,),
    ).fetchone()
    return bool(row)


def _table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    return {str(r[1]) for r in conn.execute(f"PRAGMA table_info({table_name})") if len(r) > 1}


def _resolve_default_suppression_csv() -> Path:
    return crm_store.data_dir() / "suppression.csv"


def _output_paths(now_utc: datetime) -> tuple[Path, Path]:
    root = crm_store.data_dir() / "outreach" / "ops_reports"
    day_dir = root / now_utc.strftime("%Y-%m-%d")
    artifact = day_dir / f"ops_report_{now_utc.strftime('%H%M%SZ')}.json"
    latest = root / "latest.json"
    return artifact, latest


def _state_from_sent_row(batch_id: str, metadata_json: str, prospect_state: str) -> str:
    meta = _safe_json(metadata_json)
    state = str(meta.get("state") or "").strip().upper()
    if _is_two_letter_state(state):
        return state
    from_batch = _state_from_batch(batch_id)
    if _is_two_letter_state(from_batch):
        return from_batch
    prospect_state_norm = (prospect_state or "").strip().upper()
    if _is_two_letter_state(prospect_state_norm):
        return prospect_state_norm
    return UNKNOWN_STATE


def _safe_int(raw) -> int:
    try:
        return int(raw)
    except Exception:
        return 0


def _load_sent_index(conn: sqlite3.Connection) -> dict:
    rows = conn.execute(
        """
        SELECT
            e.event_id,
            e.prospect_id,
            e.ts,
            e.batch_id,
            e.metadata_json,
            p.email AS prospect_email,
            p.state AS prospect_state
        FROM outreach_events e
        LEFT JOIN prospects p ON p.prospect_id = e.prospect_id
        WHERE e.event_type = 'sent'
        ORDER BY e.prospect_id, e.ts, e.event_id
        """
    ).fetchall()

    by_id = {}
    by_message_id = {}
    by_prospect = defaultdict(list)
    by_email = defaultdict(list)

    for row in rows:
        ts = _parse_ts(str(row["ts"] or ""))
        if not ts:
            continue
        metadata_json = str(row["metadata_json"] or "")
        meta = _safe_json(metadata_json)
        message_id = str(meta.get("message_id") or "").strip()
        event = {
            "event_id": int(row["event_id"]),
            "prospect_id": str(row["prospect_id"] or "").strip(),
            "ts": ts,
            "batch_id": str(row["batch_id"] or "").strip() or UNKNOWN_BATCH,
            "state_at_send": _state_from_sent_row(
                batch_id=str(row["batch_id"] or ""),
                metadata_json=metadata_json,
                prospect_state=str(row["prospect_state"] or ""),
            ),
            "message_id": message_id,
            "email": _norm_email(str(row["prospect_email"] or meta.get("email") or "")),
        }
        by_id[event["event_id"]] = event
        if message_id:
            by_message_id[message_id] = event
        pid = event["prospect_id"]
        if pid:
            by_prospect[pid].append(event)
        if event["email"]:
            by_email[event["email"]].append(event)

    for events in by_prospect.values():
        events.sort(key=lambda e: (e["ts"], e["event_id"]))
    for events in by_email.values():
        events.sort(key=lambda e: (e["ts"], e["event_id"]))

    return {
        "by_id": by_id,
        "by_message_id": by_message_id,
        "by_prospect": by_prospect,
        "by_email": by_email,
    }


def _last_touch_for_prospect(
    sent_index: dict,
    prospect_id: str,
    event_ts: datetime,
    attribution_window_days: int,
):
    candidates = sent_index["by_prospect"].get((prospect_id or "").strip(), [])
    if not candidates:
        return None
    lower_bound = event_ts - timedelta(days=max(1, attribution_window_days))
    for event in reversed(candidates):
        if event["ts"] > event_ts:
            continue
        if event["ts"] < lower_bound:
            break
        return event
    return None


def _last_touch_for_email(
    sent_index: dict,
    email: str,
    event_ts: datetime,
    attribution_window_days: int,
):
    candidates = sent_index["by_email"].get(_norm_email(email), [])
    if not candidates:
        return None
    lower_bound = event_ts - timedelta(days=max(1, attribution_window_days))
    for event in reversed(candidates):
        if event["ts"] > event_ts:
            continue
        if event["ts"] < lower_bound:
            break
        return event
    return None


def _resolve_row_cohort(
    row: sqlite3.Row,
    sent_index: dict,
    attribution_window_days: int,
    has_attr_send_event_id: bool,
    has_attr_batch_id: bool,
    has_attr_state_at_send: bool,
    prefer_event_batch: bool,
    lifecycle_persisted_only: bool,
) -> tuple[tuple[str, str], str]:
    event_ts = _parse_ts(str(row["ts"] or ""))
    metadata = _safe_json(str(row["metadata_json"] or ""))

    if prefer_event_batch:
        batch_direct = str(row["batch_id"] or "").strip()
        if batch_direct:
            state_direct = str(metadata.get("state") or "").strip().upper() or _state_from_batch(batch_direct)
            return _cohort_key(batch_direct, state_direct), "event_batch"

    if has_attr_send_event_id:
        attributed_id = _safe_int(row["attributed_send_event_id"])
        if attributed_id > 0:
            sent = sent_index["by_id"].get(attributed_id)
            if sent:
                return _cohort_key(sent["batch_id"], sent["state_at_send"]), "persisted_send_event_id"
            return _cohort_key("", ""), "unknown"

    if has_attr_batch_id or has_attr_state_at_send:
        attributed_batch = str(row["attributed_batch_id"] or "").strip()
        attributed_state = str(row["attributed_state_at_send"] or "").strip().upper()
        if attributed_batch:
            return _cohort_key(attributed_batch, attributed_state), "persisted_batch_state"

    if lifecycle_persisted_only:
        return _cohort_key("", ""), "unknown"

    msg_id = str(metadata.get("message_id") or metadata.get("send_message_id") or "").strip()
    if msg_id:
        sent = sent_index["by_message_id"].get(msg_id)
        if sent:
            return _cohort_key(sent["batch_id"], sent["state_at_send"]), "message_id"

    if not event_ts:
        if prefer_event_batch:
            return _cohort_key(str(row["batch_id"] or ""), _state_from_batch(str(row["batch_id"] or ""))), "event_batch"
        return _cohort_key("", ""), "unknown"

    sent = _last_touch_for_prospect(
        sent_index=sent_index,
        prospect_id=str(row["prospect_id"] or ""),
        event_ts=event_ts,
        attribution_window_days=attribution_window_days,
    )
    if sent:
        return _cohort_key(sent["batch_id"], sent["state_at_send"]), "last_touch_window"

    return _cohort_key("", ""), "unknown"


def _cohort_stats_bucket() -> dict:
    return {
        "sent": 0,
        "delivered_events": 0,
        "bounced_confirmed": 0,
        "bounced_inferred": 0,
        "replied": 0,
        "trial_started": 0,
        "converted": 0,
    }


def _in_window(event_ts: datetime | None, start_utc: datetime, end_utc: datetime) -> bool:
    if not event_ts:
        return False
    return start_utc <= event_ts <= end_utc


def _is_inferred_bounce_reason(reason: str) -> bool:
    text = (reason or "").strip().lower()
    if not text:
        return False
    return any(token in text for token in INFERRED_BOUNCE_REASON_TOKENS)


def _iter_suppression_entries(conn: sqlite3.Connection, csv_path: Path) -> list[dict]:
    entries: list[dict] = []
    if _table_exists(conn, "suppression"):
        rows = conn.execute("SELECT email, reason, ts FROM suppression").fetchall()
        for row in rows:
            entries.append(
                {
                    "email": _norm_email(str(row["email"] or "")),
                    "reason": str(row["reason"] or ""),
                    "ts": _parse_ts(str(row["ts"] or "")),
                    "source": "suppression_table",
                }
            )

    if csv_path.exists():
        with open(csv_path, "r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if not isinstance(row, dict):
                    continue
                ts_raw = (
                    row.get("timestamp")
                    or row.get("ts")
                    or row.get("created_at")
                    or row.get("updated_at")
                    or ""
                )
                entries.append(
                    {
                        "email": _norm_email(str(row.get("email") or "")),
                        "reason": str(row.get("reason") or ""),
                        "ts": _parse_ts(str(ts_raw)),
                        "source": "suppression_csv",
                    }
                )
    return entries


def _window_starts(now_utc: datetime) -> dict[str, datetime]:
    return {name: now_utc - timedelta(days=days) for name, days in WINDOWS.items()}


def _load_windows_report(
    conn: sqlite3.Connection,
    sent_index: dict,
    now_utc: datetime,
    attribution_window_days: int,
    suppression_csv: Path,
) -> tuple[dict, list[str]]:
    notes: list[str] = []
    starts = _window_starts(now_utc)
    window_buckets = {name: defaultdict(_cohort_stats_bucket) for name in WINDOWS}
    confirmed_emails = {name: set() for name in WINDOWS}

    event_cols = _table_columns(conn, "outreach_events")
    has_attr_send_event_id = "attributed_send_event_id" in event_cols
    has_attr_batch_id = "attributed_batch_id" in event_cols
    has_attr_state_at_send = "attributed_state_at_send" in event_cols
    attr_send_select = (
        "attributed_send_event_id" if has_attr_send_event_id else "NULL AS attributed_send_event_id"
    )
    attr_batch_select = "attributed_batch_id" if has_attr_batch_id else "'' AS attributed_batch_id"
    attr_state_select = (
        "attributed_state_at_send" if has_attr_state_at_send else "'' AS attributed_state_at_send"
    )
    prospect_email_by_id = {
        str(r["prospect_id"] or "").strip(): _norm_email(str(r["email"] or ""))
        for r in conn.execute("SELECT prospect_id, email FROM prospects")
    }

    for sent in sent_index["by_id"].values():
        for name, start in starts.items():
            if _in_window(sent["ts"], start_utc=start, end_utc=now_utc):
                bucket = window_buckets[name][_cohort_key(sent["batch_id"], sent["state_at_send"])]
                bucket["sent"] += 1

    all_events = conn.execute(
        f"""
        SELECT
            event_id,
            prospect_id,
            ts,
            event_type,
            batch_id,
            metadata_json,
            {attr_send_select},
            {attr_batch_select},
            {attr_state_select}
        FROM outreach_events
        WHERE event_type IN ('delivered', 'bounce', 'bounced', 'replied', 'trial_started', 'converted')
        """
    ).fetchall()

    lifecycle_types = {"replied", "trial_started", "converted"}
    for row in all_events:
        event_type = str(row["event_type"] or "").strip().lower()
        event_ts = _parse_ts(str(row["ts"] or ""))
        if not event_ts:
            continue
        for window_name, start in starts.items():
            if not _in_window(event_ts, start_utc=start, end_utc=now_utc):
                continue

            prefer_event_batch = event_type in {"delivered", "bounce", "bounced"}
            lifecycle_persisted_only = event_type in lifecycle_types
            cohort, basis = _resolve_row_cohort(
                row=row,
                sent_index=sent_index,
                attribution_window_days=attribution_window_days,
                has_attr_send_event_id=has_attr_send_event_id,
                has_attr_batch_id=has_attr_batch_id,
                has_attr_state_at_send=has_attr_state_at_send,
                prefer_event_batch=prefer_event_batch,
                lifecycle_persisted_only=lifecycle_persisted_only,
            )
            bucket = window_buckets[window_name][cohort]
            metadata = _safe_json(str(row["metadata_json"] or ""))

            if event_type == "delivered":
                bucket["delivered_events"] += 1
                continue

            if event_type in {"bounce", "bounced"}:
                bucket["bounced_confirmed"] += 1
                email = _norm_email(
                    str(metadata.get("email") or "")
                    or prospect_email_by_id.get(str(row["prospect_id"] or "").strip(), "")
                )
                if email:
                    confirmed_emails[window_name].add(email)
                continue

            if event_type in lifecycle_types:
                bucket[event_type] += 1
                if basis == "unknown":
                    notes.append(f"unattributed_{event_type}_event_id={row['event_id']}")

    suppression_entries = _iter_suppression_entries(conn, suppression_csv)
    suppression_missing_ts = 0
    suppression_bad_email = 0
    per_window_seen_inferred = {name: set() for name in WINDOWS}
    for entry in suppression_entries:
        email = _norm_email(entry.get("email", ""))
        reason = str(entry.get("reason") or "")
        ts = entry.get("ts")
        if not email:
            suppression_bad_email += 1
            continue
        if not _is_inferred_bounce_reason(reason):
            continue
        if not ts:
            suppression_missing_ts += 1
            continue
        for window_name, start in starts.items():
            if not _in_window(ts, start_utc=start, end_utc=now_utc):
                continue
            if email in confirmed_emails[window_name]:
                continue
            if email in per_window_seen_inferred[window_name]:
                continue
            sent = _last_touch_for_email(
                sent_index=sent_index,
                email=email,
                event_ts=ts,
                attribution_window_days=attribution_window_days,
            )
            if sent:
                cohort = _cohort_key(sent["batch_id"], sent["state_at_send"])
            else:
                cohort = _cohort_key("", "")
            window_buckets[window_name][cohort]["bounced_inferred"] += 1
            per_window_seen_inferred[window_name].add(email)

    if suppression_missing_ts:
        notes.append(f"suppression_rows_skipped_missing_ts={suppression_missing_ts}")
    if suppression_bad_email:
        notes.append(f"suppression_rows_skipped_bad_email={suppression_bad_email}")

    windows_out = {}
    for window_name, days in WINDOWS.items():
        rows_out = []
        totals = _cohort_stats_bucket()
        fallback_used = 0
        delivered_proxy_total = 0

        def _sort_key(item: tuple[tuple[str, str], dict]) -> tuple[int, str, str]:
            cohort, _stats = item
            unk = 1 if cohort == (UNKNOWN_BATCH, UNKNOWN_STATE) else 0
            return unk, cohort[0], cohort[1]

        for cohort, counts in sorted(window_buckets[window_name].items(), key=_sort_key):
            delivered_proxy = counts["delivered_events"] if counts["delivered_events"] > 0 else counts["sent"]
            if counts["delivered_events"] == 0 and counts["sent"] > 0:
                fallback_used += 1
            bounced_total = counts["bounced_confirmed"] + counts["bounced_inferred"]
            row_out = {
                "batch_id": cohort[0],
                "state_at_send": cohort[1],
                "sent": counts["sent"],
                "delivered_proxy": delivered_proxy,
                "bounced_confirmed": counts["bounced_confirmed"],
                "bounced_inferred": counts["bounced_inferred"],
                "bounced_total": bounced_total,
                "replied": counts["replied"],
                "trial_started": counts["trial_started"],
                "converted": counts["converted"],
                "reply_rate": round((counts["replied"] / delivered_proxy) if delivered_proxy else 0.0, 4),
                "trial_started_rate": round((counts["trial_started"] / delivered_proxy) if delivered_proxy else 0.0, 4),
                "converted_rate": round((counts["converted"] / delivered_proxy) if delivered_proxy else 0.0, 4),
                "bounce_rate_total": round((bounced_total / delivered_proxy) if delivered_proxy else 0.0, 4),
            }
            rows_out.append(row_out)
            delivered_proxy_total += delivered_proxy
            totals["sent"] += counts["sent"]
            totals["delivered_events"] += counts["delivered_events"]
            totals["bounced_confirmed"] += counts["bounced_confirmed"]
            totals["bounced_inferred"] += counts["bounced_inferred"]
            totals["replied"] += counts["replied"]
            totals["trial_started"] += counts["trial_started"]
            totals["converted"] += counts["converted"]

        bounced_total = totals["bounced_confirmed"] + totals["bounced_inferred"]
        totals_out = {
            "sent": totals["sent"],
            "delivered_proxy": delivered_proxy_total,
            "bounced_confirmed": totals["bounced_confirmed"],
            "bounced_inferred": totals["bounced_inferred"],
            "bounced_total": bounced_total,
            "replied": totals["replied"],
            "trial_started": totals["trial_started"],
            "converted": totals["converted"],
            "reply_rate": round((totals["replied"] / delivered_proxy_total) if delivered_proxy_total else 0.0, 4),
            "trial_started_rate": round((totals["trial_started"] / delivered_proxy_total) if delivered_proxy_total else 0.0, 4),
            "converted_rate": round((totals["converted"] / delivered_proxy_total) if delivered_proxy_total else 0.0, 4),
            "bounce_rate_total": round((bounced_total / delivered_proxy_total) if delivered_proxy_total else 0.0, 4),
        }
        if fallback_used:
            notes.append(f"{window_name}_delivered_proxy_fallback_cohorts={fallback_used}")

        windows_out[window_name] = {
            "days": days,
            "window_start_utc": _iso(starts[window_name]),
            "window_end_utc": _iso(now_utc),
            "cohorts": rows_out,
            "totals": totals_out,
        }

    deduped_notes = []
    seen_note = set()
    for n in notes:
        if n in seen_note:
            continue
        seen_note.add(n)
        deduped_notes.append(n)
    return windows_out, deduped_notes


def _load_list_quality(conn: sqlite3.Connection, now_utc: datetime) -> dict:
    starts = _window_starts(now_utc)
    cols = _table_columns(conn, "prospects")
    has_domain = "domain" in cols
    select_cols = "prospect_id, created_at, email, title"
    if has_domain:
        select_cols += ", domain"

    rows = conn.execute(f"SELECT {select_cols} FROM prospects").fetchall()
    out = {}
    for window_name, _days in WINDOWS.items():
        start = starts[window_name]
        selected = []
        for row in rows:
            created = _parse_ts(str(row["created_at"] or ""))
            if not _in_window(created, start_utc=start, end_utc=now_utc):
                continue
            selected.append(row)

        total = len(selected)
        valid = sum(1 for r in selected if _looks_valid_email(str(r["email"] or "")))
        role_based = sum(1 for r in selected if _is_role_based_inbox(str(r["email"] or "")))

        domain_counter = Counter()
        for row in selected:
            domain = ""
            if has_domain:
                domain = (str(row["domain"] or "").strip().lower())
            if not domain:
                domain = _email_domain(str(row["email"] or ""))
            if domain:
                domain_counter[domain] += 1

        duplicate_rows = sum(max(0, c - 1) for c in domain_counter.values())
        out[window_name] = {
            "new_prospects_count": total,
            "valid_email_format_pct": round((valid / total) if total else 0.0, 4),
            "duplicate_domain_rows": duplicate_rows,
            "duplicate_domain_pct": round((duplicate_rows / total) if total else 0.0, 4),
            "role_based_inbox_share_pct": round((role_based / total) if total else 0.0, 4),
        }
    return out


def _render_text(report: dict, json_path: str) -> str:
    lines = []
    lines.append("Outreach Ops Report")
    lines.append(f"generated_at_utc={report['generated_at_utc']}")

    for window_name in ["7d", "30d"]:
        window = report["windows"][window_name]
        lines.append("")
        lines.append(f"[{window_name}]")
        lines.append(
            "batch_id,state_at_send,sent,delivered_proxy,bounced_confirmed,bounced_inferred,"
            "bounced_total,replied,trial_started,converted,reply_rate,trial_started_rate,converted_rate,bounce_rate_total"
        )
        cohorts = window.get("cohorts", [])
        if not cohorts:
            lines.append("(none)")
        for row in cohorts:
            lines.append(
                f"{row['batch_id']},{row['state_at_send']},{row['sent']},{row['delivered_proxy']},"
                f"{row['bounced_confirmed']},{row['bounced_inferred']},{row['bounced_total']},"
                f"{row['replied']},{row['trial_started']},{row['converted']},"
                f"{row['reply_rate']:.4f},{row['trial_started_rate']:.4f},{row['converted_rate']:.4f},{row['bounce_rate_total']:.4f}"
            )
        t = window["totals"]
        lines.append(
            "TOTAL,"
            f"ALL,{t['sent']},{t['delivered_proxy']},{t['bounced_confirmed']},{t['bounced_inferred']},"
            f"{t['bounced_total']},{t['replied']},{t['trial_started']},{t['converted']},"
            f"{t['reply_rate']:.4f},{t['trial_started_rate']:.4f},{t['converted_rate']:.4f},{t['bounce_rate_total']:.4f}"
        )

    lines.append("")
    lines.append("[list_quality]")
    for window_name in ["7d", "30d"]:
        q = report["list_quality"][window_name]
        lines.append(
            f"{window_name} new_prospects_count={q['new_prospects_count']} valid_email_format_pct={q['valid_email_format_pct']:.4f} "
            f"duplicate_domain_rows={q['duplicate_domain_rows']} duplicate_domain_pct={q['duplicate_domain_pct']:.4f} "
            f"role_based_inbox_share_pct={q['role_based_inbox_share_pct']:.4f}"
        )

    lines.append("")
    if report.get("notes"):
        lines.append("[notes]")
        for note in report["notes"]:
            lines.append(f"- {note}")
    else:
        lines.append("[notes]")
        lines.append("- none")

    lines.append("")
    lines.append(f"OPS_REPORT_JSON_PATH={json_path}")
    lines.append(f"OPS_REPORT_SCHEMA_VERSION={SCHEMA_VERSION}")
    lines.append(f"OPS_REPORT_GENERATED_AT_UTC={report['generated_at_utc']}")
    return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser(description="Outreach operations report by batch/state and list-quality windows.")
    ap.add_argument("--print-config", action="store_true", help="Print resolved config and exit.")
    ap.add_argument("--dry-run", action="store_true", help="Compute report without sending or mutating DB state.")
    ap.add_argument("--no-write", action="store_true", help="Compute report without writing JSON artifacts.")
    ap.add_argument("--format", choices=["text", "json"], default="text", help="Output format.")
    ap.add_argument(
        "--attribution-window-days",
        type=int,
        default=30,
        help="Last-touch attribution window in days.",
    )
    ap.add_argument("--crm-db", default="", help="Optional override path to crm.sqlite.")
    ap.add_argument("--suppression-csv", default="", help="Optional override path to suppression.csv.")
    args = ap.parse_args()

    now_utc = _now_utc()
    crm_db = Path(args.crm_db).resolve() if (args.crm_db or "").strip() else crm_store.crm_db_path().resolve()
    suppression_csv = (
        Path(args.suppression_csv).resolve()
        if (args.suppression_csv or "").strip()
        else _resolve_default_suppression_csv().resolve()
    )
    artifact_path, latest_path = _output_paths(now_utc)

    if args.print_config:
        print(f"ops_report_schema_version={SCHEMA_VERSION}")
        print(f"crm_db={crm_db}")
        print(f"suppression_csv={suppression_csv}")
        print(f"attribution_window_days={max(1, int(args.attribution_window_days or 30))}")
        print(f"output_format={args.format}")
        print(f"artifact_path={artifact_path}")
        print(f"latest_path={latest_path}")
        print(f"dry_run={bool(args.dry_run)}")
        print(f"no_write={bool(args.no_write)}")
        return 0

    if not crm_db.exists():
        print(f"{ERR_OPS_CRM_REQUIRED} missing_crm_db path={crm_db}", file=sys.stderr)
        return 2

    conn = sqlite3.connect(str(crm_db))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        required = ["prospects", "outreach_events", "suppression", "trials"]
        missing = [name for name in required if not _table_exists(conn, name)]
        if missing:
            print(f"{ERR_OPS_CRM_SCHEMA} missing_tables={','.join(missing)}", file=sys.stderr)
            return 2
        crm_store.ensure_outreach_events_columns(conn)
        conn.commit()

        sent_index = _load_sent_index(conn)
        windows, notes = _load_windows_report(
            conn=conn,
            sent_index=sent_index,
            now_utc=now_utc,
            attribution_window_days=max(1, int(args.attribution_window_days or 30)),
            suppression_csv=suppression_csv,
        )
        list_quality = _load_list_quality(conn=conn, now_utc=now_utc)
    finally:
        conn.close()

    report = {
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": _iso(now_utc),
        "config": {
            "crm_db": str(crm_db),
            "suppression_csv": str(suppression_csv),
            "attribution_window_days": max(1, int(args.attribution_window_days or 30)),
            "format": args.format,
            "dry_run": bool(args.dry_run),
            "no_write": bool(args.no_write),
        },
        "windows": windows,
        "list_quality": list_quality,
        "notes": notes,
    }
    json_path_token = str(artifact_path) if not args.no_write else NO_WRITE_PATH_SENTINEL

    if not args.no_write:
        artifact_path.parent.mkdir(parents=True, exist_ok=True)
        latest_path.parent.mkdir(parents=True, exist_ok=True)
        payload = dict(report)
        payload["json_path"] = json_path_token
        with open(artifact_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, separators=(",", ":"), ensure_ascii=True, indent=2)
        copyfile(str(artifact_path), str(latest_path))

    if args.format == "json":
        payload = dict(report)
        payload["json_path"] = json_path_token
        print(json.dumps(payload, separators=(",", ":"), ensure_ascii=True))
        return 0

    print(_render_text(report=report, json_path=json_path_token))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
