import argparse
import csv
import hashlib
import json
import os
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

try:  # pragma: no cover
    from dotenv import load_dotenv

    load_dotenv()
except Exception:  # pragma: no cover
    pass


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from outreach import crm_store


ERR_CAPTURE_SYNC_TRIAGE_UNREADABLE = "ERR_CAPTURE_SYNC_TRIAGE_UNREADABLE"
ERR_CAPTURE_SYNC_SUPPRESSION_UNREADABLE = "ERR_CAPTURE_SYNC_SUPPRESSION_UNREADABLE"
ERR_CAPTURE_SYNC_CRM = "ERR_CAPTURE_SYNC_CRM"
ERR_CAPTURE_SYNC_WINDOW = "ERR_CAPTURE_SYNC_WINDOW"

PASS_CAPTURE_SYNC_PRINT_CONFIG = "PASS_CAPTURE_SYNC_PRINT_CONFIG"
PASS_CAPTURE_SYNC_DRY_RUN = "PASS_CAPTURE_SYNC_DRY_RUN"
PASS_CAPTURE_SYNC_APPLY = "PASS_CAPTURE_SYNC_APPLY"

_CAPTURE_SOURCE = "capture_sync"

_CATEGORY_MAP: dict[str, tuple[str, str]] = {
    "hot_interest": ("replied", "replied"),
    "question": ("replied", "replied"),
    "unsubscribe": ("do_not_contact", "do_not_contact"),
    "objection": ("do_not_contact", "do_not_contact"),
    "bounce": ("bounced", "bounced"),
}


def _norm_email(value: str) -> str:
    return (value or "").strip().lower()


def _compact(text: str, max_len: int = 220) -> str:
    value = " ".join(str(text or "").replace("\r", " ").replace("\n", " ").split())
    if len(value) <= max_len:
        return value
    return value[:max_len] + "..."


def _resolve_path(raw: str, default_path: Path) -> Path:
    text = (raw or "").strip()
    if not text:
        return default_path
    path = Path(text)
    if path.is_absolute():
        return path
    return REPO_ROOT / path


def _parse_ts(raw: str) -> datetime | None:
    text = (raw or "").strip()
    if not text:
        return None
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except Exception:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _safe_json(raw: str) -> dict:
    text = (raw or "").strip()
    if not text:
        return {}
    try:
        val = json.loads(text)
    except Exception:
        return {}
    if isinstance(val, dict):
        return val
    return {}


def _default_triage_log() -> Path:
    return REPO_ROOT / "out" / "inbox_triage_log.csv"


def _default_suppression_csv() -> Path:
    return REPO_ROOT / "out" / "suppression.csv"


def _load_suppression_evidence(path: Path) -> dict[str, str]:
    evidence: dict[str, str] = {}
    if not path.exists():
        return evidence
    with open(path, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            msg_id = (row.get("evidence_msg_id") or "").strip()
            email = _norm_email(row.get("email", ""))
            if msg_id and email:
                evidence[msg_id] = email
    return evidence


def _load_triage_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with open(path, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return [dict(r) for r in reader]


def _load_existing_capture(conn: sqlite3.Connection) -> tuple[set[str], dict[str, dict[str, object]]]:
    capture_keys: set[str] = set()
    linkage: dict[str, dict[str, object]] = {}

    rows = conn.execute(
        """
        SELECT
            event_id,
            prospect_id,
            attributed_send_event_id,
            attributed_batch_id,
            attributed_state_at_send,
            attributed_model,
            metadata_json
        FROM outreach_events
        WHERE event_type IN ('replied', 'do_not_contact', 'bounced')
        """
    ).fetchall()

    for row in rows:
        meta = _safe_json(str(row["metadata_json"] or ""))
        if str(meta.get("source") or "") != _CAPTURE_SOURCE:
            continue

        capture_key = str(meta.get("capture_key") or "").strip()
        if capture_key:
            capture_keys.add(capture_key)

        inbound_id = str(meta.get("inbound_message_id") or "").strip()
        if not inbound_id:
            continue
        if inbound_id in linkage:
            continue

        linkage[inbound_id] = {
            "prospect_id": str(row["prospect_id"] or "").strip(),
            "send_event_id": int(row["attributed_send_event_id"] or 0),
            "batch_id": str(row["attributed_batch_id"] or "").strip(),
            "state_at_send": str(row["attributed_state_at_send"] or "").strip().upper(),
            "model": str(row["attributed_model"] or "").strip() or "persisted_linkage",
        }

    return capture_keys, linkage


def _load_prospect_maps(conn: sqlite3.Connection) -> tuple[dict[str, str], dict[str, str]]:
    by_email: dict[str, str] = {}
    by_id_email: dict[str, str] = {}
    rows = conn.execute("SELECT prospect_id, email FROM prospects").fetchall()
    for row in rows:
        prospect_id = str(row["prospect_id"] or "").strip()
        email = _norm_email(str(row["email"] or ""))
        if prospect_id and email:
            by_email[email] = prospect_id
            by_id_email[prospect_id] = email
    return by_email, by_id_email


def _state_from_batch(batch_id: str) -> str:
    text = (batch_id or "").strip().upper()
    if len(text) >= 3 and "_" in text:
        tail = text.rsplit("_", 1)[-1]
        if len(tail) == 2 and tail.isalpha():
            return tail
    return ""


def _load_sent_index(conn: sqlite3.Connection) -> tuple[dict[str, dict], dict[str, list[dict]]]:
    by_message_id: dict[str, dict] = {}
    by_email: dict[str, list[dict]] = defaultdict(list)

    rows = conn.execute(
        """
        SELECT
            e.event_id,
            e.prospect_id,
            e.ts,
            e.batch_id,
            e.metadata_json,
            p.email AS prospect_email
        FROM outreach_events e
        LEFT JOIN prospects p ON p.prospect_id = e.prospect_id
        WHERE e.event_type = 'sent'
        ORDER BY e.ts, e.event_id
        """
    ).fetchall()

    for row in rows:
        meta = _safe_json(str(row["metadata_json"] or ""))
        message_id = str(meta.get("message_id") or "").strip()
        ts = _parse_ts(str(row["ts"] or ""))
        batch = str(row["batch_id"] or "").strip()
        event = {
            "event_id": int(row["event_id"] or 0),
            "prospect_id": str(row["prospect_id"] or "").strip(),
            "email": _norm_email(str(row["prospect_email"] or meta.get("email") or "")),
            "batch_id": batch,
            "state_at_send": str(meta.get("state") or "").strip().upper() or _state_from_batch(batch),
            "ts": ts,
        }
        if message_id:
            by_message_id[message_id] = event
        if event["email"]:
            by_email[event["email"]].append(event)

    for events in by_email.values():
        events.sort(key=lambda item: ((item.get("ts") or datetime.min.replace(tzinfo=timezone.utc)), int(item.get("event_id") or 0)))
    return by_message_id, by_email


def _last_touch_for_email(
    sent_by_email: dict[str, list[dict]],
    email: str,
    event_ts: datetime,
    window_days: int,
) -> dict | None:
    candidates = sent_by_email.get(_norm_email(email), [])
    if not candidates:
        return None

    lower_bound = event_ts - timedelta(days=max(1, window_days))
    for event in reversed(candidates):
        ts = event.get("ts")
        if not ts:
            continue
        if ts > event_ts:
            continue
        if ts < lower_bound:
            break
        return event
    return None


def _build_capture_key(inbound_message_id: str, event_type: str, matched_email: str, row_ts: str) -> str:
    msg = (inbound_message_id or "").strip()
    if msg:
        seed = f"{msg}|{event_type}|{matched_email}|{_CAPTURE_SOURCE}"
    else:
        seed = f"{row_ts}|{event_type}|{matched_email}|{_CAPTURE_SOURCE}"
    return hashlib.sha256(seed.encode("utf-8")).hexdigest()


def _parse_window_days(raw: str) -> tuple[bool, int, str]:
    text = (raw or "").strip()
    if not text:
        return True, 30, ""
    try:
        value = int(text)
    except Exception:
        return False, 0, f"{ERR_CAPTURE_SYNC_WINDOW} value={_compact(text, 64)}"
    if value < 1:
        return False, 0, f"{ERR_CAPTURE_SYNC_WINDOW} value={value}"
    return True, value, ""


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Sync inbound triage outcomes into outreach CRM lifecycle state.")
    ap.add_argument("--print-config", action="store_true", help="Print resolved config and exit.")
    ap.add_argument("--dry-run", action="store_true", help="Compute sync actions without DB writes.")
    ap.add_argument("--triage-log", default="", help="Optional override path to inbox_triage_log.csv.")
    ap.add_argument("--suppression-csv", default="", help="Optional override path to suppression.csv.")
    ap.add_argument("--attribution-window-days", default="30", help="Last-touch attribution window in days.")
    args = ap.parse_args(argv)

    ok_window, window_days, window_msg = _parse_window_days(str(args.attribution_window_days or ""))
    if not ok_window:
        print(window_msg, file=sys.stderr)
        return 2

    triage_path = _resolve_path(str(args.triage_log or ""), _default_triage_log())
    suppression_path = _resolve_path(str(args.suppression_csv or ""), _default_suppression_csv())
    crm_db = crm_store.ensure_database()

    if args.print_config:
        print(f"{PASS_CAPTURE_SYNC_PRINT_CONFIG} crm_db={crm_db.resolve()}")
        print(f"{PASS_CAPTURE_SYNC_PRINT_CONFIG} triage_log={triage_path.resolve()}")
        print(f"{PASS_CAPTURE_SYNC_PRINT_CONFIG} suppression_csv={suppression_path.resolve()}")
        print(f"{PASS_CAPTURE_SYNC_PRINT_CONFIG} attribution_window_days={window_days}")
        return 0

    try:
        triage_rows = _load_triage_rows(triage_path)
    except Exception as exc:
        print(
            f"{ERR_CAPTURE_SYNC_TRIAGE_UNREADABLE} path={triage_path.resolve()} err={type(exc).__name__}",
            file=sys.stderr,
        )
        return 2

    try:
        suppression_evidence = _load_suppression_evidence(suppression_path)
    except Exception as exc:
        print(
            f"{ERR_CAPTURE_SYNC_SUPPRESSION_UNREADABLE} path={suppression_path.resolve()} err={type(exc).__name__}",
            file=sys.stderr,
        )
        return 2

    try:
        conn = crm_store.connect(crm_db)
    except Exception as exc:
        print(f"{ERR_CAPTURE_SYNC_CRM} open_failed path={crm_db} err={type(exc).__name__}", file=sys.stderr)
        return 2

    now_utc = datetime.now(timezone.utc)
    rows_seen = 0
    rows_mapped = 0
    would_write = 0
    suppression_upserts = 0
    duplicates_skipped = 0
    unattributed_skipped = 0

    try:
        crm_store.init_schema(conn)
        sent_by_message, sent_by_email = _load_sent_index(conn)
        by_email, by_id_email = _load_prospect_maps(conn)
        existing_capture_keys, persisted_linkage = _load_existing_capture(conn)

        if not args.dry_run:
            conn.execute("BEGIN")

        for row in triage_rows:
            rows_seen += 1
            category = str(row.get("category") or "").strip().lower()
            mapped = _CATEGORY_MAP.get(category)
            if not mapped:
                continue

            event_type, next_status = mapped
            inbound_message_id = str(row.get("message_id") or "").strip()
            from_email = _norm_email(row.get("from_email", ""))
            row_ts_raw = str(row.get("timestamp") or "").strip()
            row_ts = _parse_ts(row_ts_raw) or now_utc
            triage_action = str(row.get("action") or "").strip()

            matched_email = from_email
            if category in {"unsubscribe", "objection", "bounce"}:
                suppressed_email = _norm_email(suppression_evidence.get(inbound_message_id, ""))
                if suppressed_email:
                    matched_email = suppressed_email

            capture_key = _build_capture_key(
                inbound_message_id=inbound_message_id,
                event_type=event_type,
                matched_email=matched_email,
                row_ts=row_ts_raw,
            )
            if capture_key in existing_capture_keys:
                duplicates_skipped += 1
                continue

            prospect_id = ""
            attr_send_event_id = 0
            attr_batch = ""
            attr_state = ""
            attr_model = ""

            persisted = persisted_linkage.get(inbound_message_id)
            if persisted:
                prospect_id = str(persisted.get("prospect_id") or "").strip()
                attr_send_event_id = int(persisted.get("send_event_id") or 0)
                attr_batch = str(persisted.get("batch_id") or "").strip()
                attr_state = str(persisted.get("state_at_send") or "").strip().upper()
                attr_model = "persisted_linkage"

            if not prospect_id and inbound_message_id:
                sent = sent_by_message.get(inbound_message_id)
                if sent:
                    prospect_id = str(sent.get("prospect_id") or "").strip()
                    matched_email = _norm_email(sent.get("email", "") or matched_email)
                    attr_send_event_id = int(sent.get("event_id") or 0)
                    attr_batch = str(sent.get("batch_id") or "").strip()
                    attr_state = str(sent.get("state_at_send") or "").strip().upper()
                    attr_model = "message_id"

            if not prospect_id and matched_email:
                sent = _last_touch_for_email(
                    sent_by_email=sent_by_email,
                    email=matched_email,
                    event_ts=row_ts,
                    window_days=window_days,
                )
                if sent:
                    prospect_id = str(sent.get("prospect_id") or "").strip()
                    attr_send_event_id = int(sent.get("event_id") or 0)
                    attr_batch = str(sent.get("batch_id") or "").strip()
                    attr_state = str(sent.get("state_at_send") or "").strip().upper()
                    attr_model = "last_touch_window"

            if not prospect_id and matched_email:
                prospect_id = str(by_email.get(matched_email, "") or "").strip()
                if prospect_id:
                    attr_model = "email_direct"

            if not prospect_id:
                unattributed_skipped += 1
                if next_status in {"do_not_contact", "bounced"} and matched_email:
                    suppression_upserts += 1
                    if not args.dry_run:
                        conn.execute(
                            """
                            INSERT INTO suppression(email, reason, ts)
                            VALUES(?, ?, ?)
                            ON CONFLICT(email) DO UPDATE SET
                                reason = excluded.reason,
                                ts = excluded.ts
                            """,
                            (matched_email, next_status, row_ts.isoformat()),
                        )
                continue

            rows_mapped += 1
            would_write += 1
            existing_capture_keys.add(capture_key)
            by_id_email.setdefault(prospect_id, matched_email)
            if not matched_email:
                matched_email = by_id_email.get(prospect_id, "")

            metadata = {
                "source": _CAPTURE_SOURCE,
                "capture_key": capture_key,
                "inbound_message_id": inbound_message_id,
                "triage_category": category,
                "triage_action": triage_action,
                "matched_email": matched_email,
                "attribution_method": attr_model or "unknown",
            }

            if not args.dry_run:
                conn.execute(
                    """
                    INSERT INTO outreach_events(
                        prospect_id,
                        ts,
                        event_type,
                        batch_id,
                        metadata_json,
                        attributed_send_event_id,
                        attributed_batch_id,
                        attributed_state_at_send,
                        attributed_model
                    ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        prospect_id,
                        row_ts.isoformat(),
                        event_type,
                        attr_batch,
                        json.dumps(metadata, separators=(",", ":"), ensure_ascii=True),
                        attr_send_event_id if attr_send_event_id > 0 else None,
                        attr_batch,
                        attr_state,
                        attr_model,
                    ),
                )
                conn.execute(
                    "UPDATE prospects SET status = ? WHERE prospect_id = ?",
                    (next_status, prospect_id),
                )

            if next_status in {"do_not_contact", "bounced"} and matched_email:
                suppression_upserts += 1
                if not args.dry_run:
                    conn.execute(
                        """
                        INSERT INTO suppression(email, reason, ts)
                        VALUES(?, ?, ?)
                        ON CONFLICT(email) DO UPDATE SET
                            reason = excluded.reason,
                            ts = excluded.ts
                        """,
                        (matched_email, next_status, row_ts.isoformat()),
                    )

        if not args.dry_run:
            conn.commit()

    except Exception as exc:
        if not args.dry_run:
            try:
                conn.rollback()
            except Exception:
                pass
        print(f"{ERR_CAPTURE_SYNC_CRM} sync_failed err={type(exc).__name__} detail={_compact(exc)}", file=sys.stderr)
        return 2
    finally:
        conn.close()

    token = PASS_CAPTURE_SYNC_DRY_RUN if args.dry_run else PASS_CAPTURE_SYNC_APPLY
    print(
        f"{token} crm_db={crm_db.resolve()} triage_log={triage_path.resolve()} suppression_csv={suppression_path.resolve()} "
        f"rows_seen={rows_seen} rows_mapped={rows_mapped} events_written={would_write} suppression_upserts={suppression_upserts} "
        f"duplicates_skipped={duplicates_skipped} unattributed_skipped={unattributed_skipped}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
