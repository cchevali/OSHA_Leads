from __future__ import annotations

import argparse
import csv
import json
import sqlite3
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import crm_light
from outreach import crm_store


def _emit(key: str, value: object) -> None:
    print(f"{key}={value}")


def _norm_email(value: str) -> str:
    return (value or "").strip().lower()


def _norm_sk(value: str) -> str:
    return crm_light.normalize_subscriber_key(value)


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table_name,),
    ).fetchone()
    return bool(row)


def _table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    try:
        rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    except Exception:
        return set()
    out: set[str] = set()
    for row in rows:
        try:
            out.add(str(row[1] or "").strip())
        except Exception:
            continue
    return out


def _safe_json(raw: str) -> dict[str, Any]:
    text = str(raw or "").strip()
    if not text:
        return {}
    try:
        payload = json.loads(text)
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    rows: list[dict[str, str]] = []
    with open(path, "r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            rows.append({str(k or "").strip(): str(v or "").strip() for k, v in (row or {}).items()})
    return rows


def _open_ro(path: Path) -> sqlite3.Connection | None:
    if not path.exists():
        return None
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _parse_entitlement_recipients(raw: str) -> list[str]:
    text = str(raw or "").strip()
    if not text:
        return []
    try:
        payload = json.loads(text)
    except Exception:
        return []
    out: list[str] = []
    seen: set[str] = set()
    if isinstance(payload, list):
        for item in payload:
            if isinstance(item, dict):
                email = _norm_email(str(item.get("email") or ""))
            else:
                email = _norm_email(str(item or ""))
            if email and email not in seen:
                seen.add(email)
                out.append(email)
    return out


def _diagnose_entitlement(crm_light_db: Path, subscriber_key: str, email: str) -> tuple[int, dict[str, Any]]:
    info: dict[str, Any] = {
        "subscriber_row_found": False,
        "subscriber_email": "",
        "subscriber_recipients": [],
        "other_subscriber_keys_for_email": [],
    }
    conn = _open_ro(crm_light_db)
    if conn is None:
        info["reason"] = "crm_light_db_missing"
        return 0, info
    try:
        if not _table_exists(conn, "subscriber_entitlements"):
            info["reason"] = "subscriber_entitlements_missing"
            return 0, info
        row = conn.execute(
            """
            SELECT subscriber_key, email, recipients_json, active
            FROM subscriber_entitlements
            WHERE subscriber_key = ?
            LIMIT 1
            """,
            (_norm_sk(subscriber_key),),
        ).fetchone()
        in_entitlement = 0
        if row:
            info["subscriber_row_found"] = True
            info["subscriber_email"] = _norm_email(str(row["email"] or ""))
            recips = _parse_entitlement_recipients(str(row["recipients_json"] or ""))
            info["subscriber_recipients"] = recips
            if email and (email == info["subscriber_email"] or email in recips):
                in_entitlement = 1

        # Detect if the email appears under a different subscriber_key in entitlements.
        others: list[str] = []
        rows = conn.execute(
            "SELECT subscriber_key, email, recipients_json FROM subscriber_entitlements"
        ).fetchall()
        for r in rows:
            sk = _norm_sk(str(r["subscriber_key"] or ""))
            em = _norm_email(str(r["email"] or ""))
            recips = _parse_entitlement_recipients(str(r["recipients_json"] or ""))
            if sk != _norm_sk(subscriber_key) and email and (email == em or email in recips):
                others.append(sk)
        info["other_subscriber_keys_for_email"] = sorted(set(others))
        return in_entitlement, info
    finally:
        conn.close()


def _recipient_from_meta(meta: dict[str, Any]) -> str:
    return _norm_email(str(meta.get("recipient") or meta.get("to") or meta.get("primary_recipient") or ""))


def _diagnose_send_events(crm_light_db: Path, subscriber_key: str, email: str) -> dict[str, Any]:
    out: dict[str, Any] = {
        "last_sent": "NEVER",
        "last_skip_reason": "NONE",
        "recent_matching_rows": [],
        "notes": [],
    }
    conn = _open_ro(crm_light_db)
    if conn is None:
        out["notes"].append("crm_light_db_missing")
        return out
    try:
        if not _table_exists(conn, "send_events"):
            out["notes"].append("send_events_missing")
            return out
        rows = conn.execute(
            """
            SELECT id, ts_utc, status, variant, run_id, meta_json
            FROM send_events
            WHERE subscriber_key = ?
            ORDER BY ts_utc DESC, id DESC
            """,
            (_norm_sk(subscriber_key),),
        ).fetchall()
        for row in rows:
            status = str(row["status"] or "").strip().upper()
            meta = _safe_json(str(row["meta_json"] or ""))
            event_email = _recipient_from_meta(meta)
            if email and event_email and event_email != email:
                continue
            if email and not event_email:
                # Unknown recipient attribution on this row; ignore for per-recipient diagnosis.
                continue
            out["recent_matching_rows"].append(
                {
                    "id": int(row["id"]),
                    "ts_utc": str(row["ts_utc"] or ""),
                    "status": status,
                    "variant": str(row["variant"] or ""),
                    "run_id": str(row["run_id"] or ""),
                    "recipient_meta": event_email,
                }
            )
            if out["last_sent"] == "NEVER" and status == "SENT":
                out["last_sent"] = str(row["ts_utc"] or "").strip() or "NEVER"
            if out["last_skip_reason"] == "NONE" and (status.startswith("SKIP_") or status.startswith("ERR_") or "BOUNCE" in status):
                out["last_skip_reason"] = status
            if out["last_sent"] != "NEVER" and out["last_skip_reason"] != "NONE":
                # Keep collecting only lightweight recent rows up to 20.
                if len(out["recent_matching_rows"]) >= 20:
                    break
        return out
    finally:
        conn.close()


def _diagnose_csv_suppression(suppression_csv: Path, email: str) -> tuple[bool, list[dict[str, str]]]:
    rows = _read_csv_rows(suppression_csv)
    hits = [r for r in rows if _norm_email(r.get("email", "")) == email]
    return bool(hits), hits


def _diagnose_sql_suppression_any(db_path: Path, email: str) -> tuple[bool, list[str]]:
    conn = _open_ro(db_path)
    if conn is None:
        return False, []
    try:
        tables = [
            str(r[0] or "")
            for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall()
            if str(r[0] or "")
        ]
        evidence: list[str] = []
        for table in tables:
            lower = table.lower()
            if "suppress" not in lower and lower not in {"unsubscribe", "unsubscribes"}:
                continue
            cols = _table_columns(conn, table)
            if "email" in cols:
                row = conn.execute(f"SELECT 1 FROM {table} WHERE lower(email)=? LIMIT 1", (email,)).fetchone()
                if row:
                    evidence.append(f"{table}.email")
                    continue
            if "email_or_domain" in cols:
                domain = email.split("@", 1)[-1] if "@" in email else ""
                row = conn.execute(
                    f"SELECT 1 FROM {table} WHERE lower(email_or_domain) IN (?, ?) LIMIT 1",
                    (email, domain.lower()),
                ).fetchone()
                if row:
                    evidence.append(f"{table}.email_or_domain")
                    continue
        return bool(evidence), evidence
    finally:
        conn.close()


def _diagnose_bounces(crm_db: Path, email: str, bounce_state_path: Path) -> tuple[bool, dict[str, Any]]:
    info: dict[str, Any] = {"bounce_events": 0, "bounce_import_state": {}}
    bounced = False
    conn = _open_ro(crm_db)
    if conn is not None:
        try:
            if _table_exists(conn, "bounce_events"):
                row = conn.execute(
                    "SELECT COUNT(*) AS c FROM bounce_events WHERE lower(recipient_email)=?",
                    (email,),
                ).fetchone()
                count = int((row["c"] if row and "c" in row.keys() else 0) or 0)
                info["bounce_events"] = count
                bounced = bounced or (count > 0)
        finally:
            conn.close()
    if bounce_state_path.exists():
        try:
            payload = json.loads(bounce_state_path.read_text(encoding="utf-8"))
            if isinstance(payload, dict):
                info["bounce_import_state"] = payload
        except Exception:
            info["bounce_import_state"] = {"parse_error": True}
    return bounced, info


def _diagnose_unsubscribe(
    *,
    email: str,
    unsub_tokens_csv: Path,
    unsubscribe_events_csv: Path,
    suppression_csv_rows: list[dict[str, str]],
) -> tuple[bool, dict[str, Any]]:
    token_rows = [r for r in _read_csv_rows(unsub_tokens_csv) if _norm_email(r.get("email", "")) == email]
    unsub_event_rows = []
    for r in _read_csv_rows(unsubscribe_events_csv):
        if _norm_email(r.get("email", "")) != email:
            continue
        reason = str(r.get("reason") or "").strip().lower()
        source = str(r.get("source") or "").strip().lower()
        if "unsubscribe" in reason or "one_click" in source:
            unsub_event_rows.append(r)
    suppression_unsub_rows = []
    for r in suppression_csv_rows:
        reason = str(r.get("reason") or "").strip().lower()
        source = str(r.get("source") or "").strip().lower()
        if "unsubscribe" in reason or "one_click" in source:
            suppression_unsub_rows.append(r)
    exercised = bool(unsub_event_rows or suppression_unsub_rows)
    info = {
        "token_rows_count": len(token_rows),
        "unsubscribe_event_rows_count": len(unsub_event_rows),
        "suppression_unsub_rows_count": len(suppression_unsub_rows),
    }
    return exercised, info


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="Diagnose why a recipient is/was not receiving trial digest emails.")
    ap.add_argument("--email", required=True)
    ap.add_argument("--subscriber-key", required=True)
    ap.add_argument("--print-config", action="store_true")
    ap.add_argument("--crm-light-db", default="", help="Optional crm_light.sqlite path override")
    ap.add_argument("--crm-db", default="", help="Optional outreach crm.sqlite path override")
    ap.add_argument("--data-dir", default="", help="Optional DATA_DIR override for CSV artifacts")
    ap.add_argument("--dry-run", action="store_true", help="Accepted for contract consistency; script is read-only.")
    return ap


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    email = _norm_email(args.email)
    subscriber_key = _norm_sk(args.subscriber_key)
    if not email or "@" not in email:
        print("ERR_DIAGNOSE_RECIPIENT invalid_email", file=sys.stderr)
        return 1
    if not subscriber_key:
        print("ERR_DIAGNOSE_RECIPIENT invalid_subscriber_key", file=sys.stderr)
        return 1

    crm_light_db = crm_light.resolve_crm_db_path(str(args.crm_light_db or "").strip() or None)
    data_dir = (
        Path(str(args.data_dir)).expanduser().resolve(strict=False)
        if str(args.data_dir or "").strip()
        else crm_light.data_dir()
    )
    crm_db = (
        Path(str(args.crm_db)).expanduser().resolve(strict=False)
        if str(args.crm_db or "").strip()
        else crm_store.crm_db_path().resolve(strict=False)
    )
    suppression_csv = data_dir / "suppression.csv"
    bounce_state_path = data_dir / "bounce_import_state.json"
    unsub_tokens_csv = data_dir / "unsub_tokens.csv"
    unsubscribe_events_csv = data_dir / "unsubscribe_events.csv"

    _emit("DIAG_EMAIL", email)
    _emit("DIAG_SUBSCRIBER_KEY", subscriber_key)
    _emit("DIAG_CRM_LIGHT_DB", str(crm_light_db))
    _emit("DIAG_CRM_DB", str(crm_db))
    _emit("DIAG_DATA_DIR", str(data_dir))
    _emit("DIAG_SUPPRESSION_CSV", str(suppression_csv))
    _emit("DIAG_BOUNCE_STATE_JSON", str(bounce_state_path))
    _emit("DIAG_UNSUB_TOKENS_CSV", str(unsub_tokens_csv))
    _emit("DIAG_UNSUB_EVENTS_CSV", str(unsubscribe_events_csv))
    _emit("DIAG_DRY_RUN", 1 if args.dry_run else 0)

    if args.print_config:
        _emit("DIAGNOSE_RECIPIENT_COMPLETE", "status=PRINT_CONFIG")
        return 0

    in_entitlement, entitlement_info = _diagnose_entitlement(crm_light_db, subscriber_key, email)
    send_info = _diagnose_send_events(crm_light_db, subscriber_key, email)

    suppressed_csv, suppression_csv_hits = _diagnose_csv_suppression(suppression_csv, email)
    suppressed_crm, suppressed_crm_evidence = _diagnose_sql_suppression_any(crm_db, email)
    suppressed_crm_light, suppressed_crm_light_evidence = _diagnose_sql_suppression_any(crm_light_db, email)
    suppressed = bool(suppressed_csv or suppressed_crm or suppressed_crm_light)

    bounced, bounce_info = _diagnose_bounces(crm_db, email, bounce_state_path)
    unsubscribed, unsub_info = _diagnose_unsubscribe(
        email=email,
        unsub_tokens_csv=unsub_tokens_csv,
        unsubscribe_events_csv=unsubscribe_events_csv,
        suppression_csv_rows=suppression_csv_hits,
    )

    _emit("DIAG_RECIPIENT_IN_ENTITLEMENT", 1 if in_entitlement else 0)
    _emit("DIAG_RECIPIENT_SUPPRESSED", 1 if suppressed else 0)
    _emit("DIAG_RECIPIENT_BOUNCED", 1 if bounced else 0)
    _emit("DIAG_RECIPIENT_UNSUBSCRIBED", 1 if unsubscribed else 0)
    _emit("DIAG_RECIPIENT_LAST_SENT", str(send_info.get("last_sent") or "NEVER"))
    _emit("DIAG_RECIPIENT_LAST_SKIP_REASON", str(send_info.get("last_skip_reason") or "NONE"))

    print("DIAG_SUMMARY")
    print(f"  entitlement_row_found={1 if entitlement_info.get('subscriber_row_found') else 0}")
    print(f"  entitlement_recipients={','.join(entitlement_info.get('subscriber_recipients') or []) or '-'}")
    print(f"  other_subscriber_keys_for_email={','.join(entitlement_info.get('other_subscriber_keys_for_email') or []) or '-'}")
    print(f"  suppressed_csv_hits={len(suppression_csv_hits)}")
    print(f"  suppressed_crm_evidence={','.join(suppressed_crm_evidence) or '-'}")
    print(f"  suppressed_crm_light_evidence={','.join(suppressed_crm_light_evidence) or '-'}")
    print(f"  bounce_events={int(bounce_info.get('bounce_events') or 0)}")
    print(f"  unsub_token_rows={int(unsub_info.get('token_rows_count') or 0)}")
    print(f"  unsubscribe_event_rows={int(unsub_info.get('unsubscribe_event_rows_count') or 0)}")
    print(f"  recent_matching_send_rows={len(send_info.get('recent_matching_rows') or [])}")
    _emit("DIAGNOSE_RECIPIENT_COMPLETE", "status=OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
