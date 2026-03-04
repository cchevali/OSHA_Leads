import argparse
import csv
import json
import re
import shutil
import sqlite3
import sys
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


ERR_CRM_INPUT_MISSING = "ERR_CRM_INPUT_MISSING"
ERR_CRM_MARK_MISSING = "ERR_CRM_MARK_MISSING"
ERR_CRM_STATS_DB_UNREADABLE = "ERR_CRM_STATS_DB_UNREADABLE"
ERR_CRM_VERIFY_INPUT_MISSING = "ERR_CRM_VERIFY_INPUT_MISSING"
ERR_CRM_VERIFY_INPUT_UNREADABLE = "ERR_CRM_VERIFY_INPUT_UNREADABLE"
ERR_CRM_VERIFY_DB_UNREADABLE = "ERR_CRM_VERIFY_DB_UNREADABLE"
PASS_CRM_SEED = "PASS_CRM_SEED"
PASS_CRM_MARK = "PASS_CRM_MARK"

VERIFY_IMPORT_SAMPLE_SIZE = 25
BLANK_SOURCE_LABEL = "(blank)"
EMAIL_SCAN_RE = re.compile(r"[^\s,;<>\"']+@[^\s,;<>\"']+")


def _norm_email(value: str) -> str:
    return (value or "").strip().lower()


def _norm_state(value: str) -> str:
    return (value or "").strip().upper()


def _title_score(title: str) -> int:
    text = (title or "").strip().lower()
    if not text:
        return 0
    score = 0
    for token, pts in [
        ("partner", 4),
        ("owner", 4),
        ("founder", 3),
        ("osha", 2),
        ("safety", 2),
    ]:
        if token in text:
            score += pts
    return score


def _coerce_score(raw: str, title: str) -> int:
    text = (raw or "").strip()
    if text:
        try:
            return int(text)
        except Exception:
            pass
    return _title_score(title)


def _contact_name(row: dict[str, str]) -> str:
    direct = (row.get("contact_name") or "").strip()
    if direct:
        return direct
    first = (row.get("first_name") or "").strip()
    last = (row.get("last_name") or "").strip()
    joined = " ".join([part for part in [first, last] if part]).strip()
    return joined


def _archive_input(input_path: Path, archive_dir: Path) -> Path:
    archive_dir.mkdir(parents=True, exist_ok=True)
    ts = crm_store.utc_now_iso().replace(":", "-").replace("+", "Z")
    dest = archive_dir / f"{input_path.stem}_{ts}{input_path.suffix}"
    shutil.move(str(input_path), str(dest))
    return dest


def _open_read_only_connection(db_path: Path) -> sqlite3.Connection | None:
    if not db_path.exists():
        return None
    uri = f"{db_path.resolve().as_uri()}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _prospects_table_exists(conn: sqlite3.Connection) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='prospects' LIMIT 1"
    ).fetchone()
    return bool(row)


def _stats() -> int:
    db_path = crm_store.crm_db_path()
    print(f"CRM_DB_PATH={db_path.resolve()}")

    if not db_path.exists():
        print("CRM_PROSPECTS_TOTAL=0")
        print("CRM_PROSPECTS_HAS_EMAIL=0")
        return 0

    try:
        conn = _open_read_only_connection(db_path)
    except sqlite3.Error as exc:
        print(f"{ERR_CRM_STATS_DB_UNREADABLE} path={db_path.resolve()} err={exc}", file=sys.stderr)
        return 2

    if conn is None:
        print("CRM_PROSPECTS_TOTAL=0")
        print("CRM_PROSPECTS_HAS_EMAIL=0")
        return 0

    try:
        if not _prospects_table_exists(conn):
            print("CRM_PROSPECTS_TOTAL=0")
            print("CRM_PROSPECTS_HAS_EMAIL=0")
            return 0

        total = int(conn.execute("SELECT COUNT(*) FROM prospects").fetchone()[0] or 0)
        has_email = int(
            conn.execute(
                "SELECT COUNT(*) FROM prospects WHERE email IS NOT NULL AND trim(email) <> ''"
            ).fetchone()[0]
            or 0
        )
        print(f"CRM_PROSPECTS_TOTAL={total}")
        print(f"CRM_PROSPECTS_HAS_EMAIL={has_email}")

        by_source_rows = conn.execute(
            """
            SELECT
                CASE
                    WHEN source IS NULL OR trim(source) = '' THEN ?
                    ELSE trim(source)
                END AS source_norm,
                COUNT(*) AS total,
                SUM(CASE WHEN email IS NOT NULL AND trim(email) <> '' THEN 1 ELSE 0 END) AS has_email,
                SUM(CASE WHEN website IS NOT NULL AND trim(website) <> '' THEN 1 ELSE 0 END) AS has_website
            FROM prospects
            GROUP BY source_norm
            ORDER BY lower(source_norm) ASC, source_norm ASC
            """,
            (BLANK_SOURCE_LABEL,),
        ).fetchall()
        for row in by_source_rows:
            print(
                f"CRM_BY_SOURCE source={row['source_norm']} total={int(row['total'] or 0)} "
                f"has_email={int(row['has_email'] or 0)} has_website={int(row['has_website'] or 0)}"
            )

        empty_by_source_rows = conn.execute(
            """
            SELECT
                CASE
                    WHEN source IS NULL OR trim(source) = '' THEN ?
                    ELSE trim(source)
                END AS source_norm,
                COUNT(*) AS total
            FROM prospects
            WHERE email IS NULL OR trim(email) = ''
            GROUP BY source_norm
            ORDER BY lower(source_norm) ASC, source_norm ASC
            """,
            (BLANK_SOURCE_LABEL,),
        ).fetchall()
        for row in empty_by_source_rows:
            print(f"CRM_EMPTY_EMAIL_BY_SOURCE source={row['source_norm']} total={int(row['total'] or 0)}")
    except sqlite3.Error as exc:
        print(f"{ERR_CRM_STATS_DB_UNREADABLE} path={db_path.resolve()} err={exc}", file=sys.stderr)
        return 2
    finally:
        conn.close()

    return 0


def _extract_csv_email_samples(csv_path: Path, sample_size: int) -> list[str]:
    samples: list[str] = []
    seen: set[str] = set()
    with open(csv_path, "r", newline="", encoding="utf-8-sig", errors="replace") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if len(samples) >= sample_size:
                break
            for value in row.values():
                text = str(value or "")
                if not text:
                    continue
                match = EMAIL_SCAN_RE.search(text)
                if not match:
                    continue
                email = _norm_email(match.group(0).strip().strip(".,;:"))
                if not email or "@" not in email:
                    continue
                if email in seen:
                    continue
                seen.add(email)
                samples.append(email)
                break
    return samples


def _verify_import(csv_path: Path) -> int:
    if not csv_path.exists():
        print(f"{ERR_CRM_VERIFY_INPUT_MISSING} path={csv_path.resolve()}", file=sys.stderr)
        return 2

    try:
        samples = _extract_csv_email_samples(csv_path, sample_size=VERIFY_IMPORT_SAMPLE_SIZE)
    except Exception as exc:
        print(f"{ERR_CRM_VERIFY_INPUT_UNREADABLE} path={csv_path.resolve()} err={exc}", file=sys.stderr)
        return 2

    db_path = crm_store.crm_db_path()
    matches = 0

    if db_path.exists():
        try:
            conn = _open_read_only_connection(db_path)
        except sqlite3.Error as exc:
            print(f"{ERR_CRM_VERIFY_DB_UNREADABLE} path={db_path.resolve()} err={exc}", file=sys.stderr)
            return 2

        if conn is not None:
            try:
                if _prospects_table_exists(conn):
                    cur = conn.cursor()
                    for email in samples:
                        row = cur.execute(
                            "SELECT COUNT(*) FROM prospects WHERE lower(email) = ?",
                            (email,),
                        ).fetchone()
                        matches += 1 if int((row[0] if row else 0) or 0) > 0 else 0
            except sqlite3.Error as exc:
                print(f"{ERR_CRM_VERIFY_DB_UNREADABLE} path={db_path.resolve()} err={exc}", file=sys.stderr)
                return 2
            finally:
                conn.close()

    sample_size = len(samples)
    match_rate = 0.0 if sample_size < 1 else (float(matches) * 100.0 / float(sample_size))
    print(f"CRM_VERIFY_IMPORT_SAMPLE_SIZE={sample_size}")
    print(f"CRM_VERIFY_IMPORT_MATCHES={matches}")
    print(f"CRM_VERIFY_IMPORT_MATCH_RATE={match_rate:.2f}")
    return 0


def _seed_from_csv(input_path: Path, archive_dir: Path | None, no_archive: bool) -> int:
    if not input_path.exists():
        print(f"{ERR_CRM_INPUT_MISSING} path={input_path}", file=sys.stderr)
        return 2

    db_path = crm_store.ensure_database()
    inserted = 0
    updated = 0
    skipped = 0
    ts = crm_store.utc_now_iso()

    with open(input_path, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = []
        for r in reader:
            clean = {}
            for k, v in dict(r).items():
                key = (k or "").lstrip("\ufeff")
                clean[key] = v
            rows.append(clean)

    with crm_store.connect(db_path) as conn:
        crm_store.init_schema(conn)
        cur = conn.cursor()
        for i, row in enumerate(rows, start=1):
            prospect_id = (row.get("prospect_id") or f"seed_{i}").strip()
            email = _norm_email(row.get("email", ""))
            if not email or "@" not in email:
                skipped += 1
                continue

            title = (row.get("title") or "").strip()
            firm = (row.get("firm") or "").strip()
            status = (row.get("status") or "new").strip().lower()
            if not status:
                status = "new"
            created_at = (row.get("created_at") or "").strip() or ts
            last_contacted_at = (row.get("last_contacted_at") or "").strip() or None

            payload = {
                "prospect_id": prospect_id,
                "firm": firm,
                "contact_name": _contact_name(row),
                "email": email,
                "title": title,
                "city": (row.get("city") or "").strip(),
                "state": _norm_state(row.get("state", "")),
                "website": (row.get("website") or "").strip(),
                "source": (row.get("source") or "csv_seed").strip(),
                "score": _coerce_score(row.get("score", ""), title),
                "status": status,
                "created_at": created_at,
                "last_contacted_at": last_contacted_at,
            }

            # Preserve UNIQUE(email) invariant without aborting the whole seed batch.
            email_owner = cur.execute(
                "SELECT prospect_id FROM prospects WHERE email = ? LIMIT 1",
                (email,),
            ).fetchone()
            if email_owner and str(email_owner[0] or "").strip() != prospect_id:
                skipped += 1
                continue

            cur.execute("SELECT 1 FROM prospects WHERE prospect_id = ?", (prospect_id,))
            existed = cur.fetchone() is not None
            cur.execute(
                """
                INSERT INTO prospects(
                    prospect_id, firm, contact_name, email, title, city, state, website, source,
                    score, status, created_at, last_contacted_at
                ) VALUES (
                    :prospect_id, :firm, :contact_name, :email, :title, :city, :state, :website, :source,
                    :score, :status, :created_at, :last_contacted_at
                )
                ON CONFLICT(prospect_id) DO UPDATE SET
                    firm = excluded.firm,
                    contact_name = excluded.contact_name,
                    email = excluded.email,
                    title = excluded.title,
                    city = excluded.city,
                    state = excluded.state,
                    website = excluded.website,
                    source = excluded.source,
                    score = excluded.score,
                    status = excluded.status,
                    last_contacted_at = COALESCE(excluded.last_contacted_at, prospects.last_contacted_at)
                """,
                payload,
            )
            if existed:
                updated += 1
            else:
                inserted += 1
        conn.commit()

    archived_to = ""
    if not no_archive:
        target_dir = archive_dir or (input_path.parent / "archived_prospects")
        archived_to = str(_archive_input(input_path, target_dir))

    print(f"{PASS_CRM_SEED} crm_db={db_path}")
    print(f"{PASS_CRM_SEED} inserted_count={inserted}")
    print(f"{PASS_CRM_SEED} updated_count={updated}")
    print(f"{PASS_CRM_SEED} skipped_count={skipped}")
    if archived_to:
        print(f"{PASS_CRM_SEED} archived_to={archived_to}")
    return 0


def _mark_event(prospect_id: str, event: str, territory_code: str, note: str) -> int:
    db_path = crm_store.ensure_database()
    ts = crm_store.utc_now_iso()
    event_norm = (event or "").strip().lower()
    status_map = {
        "replied": "replied",
        "trial_started": "trial_started",
        "converted": "converted",
        "do_not_contact": "do_not_contact",
    }
    next_status = status_map.get(event_norm, "")
    if not next_status:
        print(f"{ERR_CRM_MARK_MISSING} unsupported_event={event_norm}", file=sys.stderr)
        return 2

    with crm_store.connect(db_path) as conn:
        crm_store.init_schema(conn)
        cur = conn.cursor()
        row = cur.execute(
            "SELECT prospect_id, email FROM prospects WHERE prospect_id = ?",
            (prospect_id,),
        ).fetchone()
        if not row:
            print(f"{ERR_CRM_MARK_MISSING} prospect_id={prospect_id}", file=sys.stderr)
            return 2

        metadata = {"note": note or "", "territory_code": territory_code}
        conn.execute("BEGIN")
        cur.execute(
            "UPDATE prospects SET status = ? WHERE prospect_id = ?",
            (next_status, prospect_id),
        )
        cur.execute(
            """
            INSERT INTO outreach_events(prospect_id, ts, event_type, batch_id, metadata_json)
            VALUES(?, ?, ?, ?, ?)
            """,
            (prospect_id, ts, event_norm, territory_code, json.dumps(metadata, separators=(",", ":"))),
        )

        if event_norm == "trial_started":
            cur.execute(
                """
                INSERT INTO trials(prospect_id, territory_code, started_at, status)
                VALUES(?, ?, ?, ?)
                ON CONFLICT(prospect_id, territory_code) DO UPDATE SET
                    started_at = excluded.started_at,
                    status = excluded.status
                """,
                (prospect_id, territory_code, ts, "active"),
            )
        elif event_norm == "converted":
            cur.execute(
                """
                UPDATE trials SET status = 'converted'
                WHERE prospect_id = ? AND territory_code = ?
                """,
                (prospect_id, territory_code),
            )
        elif event_norm == "do_not_contact":
            email = _norm_email(row["email"])
            if email:
                cur.execute(
                    """
                    INSERT INTO suppression(email, reason, ts)
                    VALUES(?, ?, ?)
                    ON CONFLICT(email) DO UPDATE SET
                        reason = excluded.reason,
                        ts = excluded.ts
                    """,
                    (email, "do_not_contact", ts),
                )
        conn.commit()

    print(f"{PASS_CRM_MARK} crm_db={db_path}")
    print(f"{PASS_CRM_MARK} prospect_id={prospect_id}")
    print(f"{PASS_CRM_MARK} event={event_norm}")
    print(f"{PASS_CRM_MARK} status={next_status}")
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="SQLite CRM-lite admin for outreach prospects/events.")
    sub = ap.add_subparsers(dest="cmd", required=True)

    ap_seed = sub.add_parser("seed", help="Seed/import prospects CSV into crm.sqlite.")
    ap_seed.add_argument("--input", required=True, help="Path to prospects CSV.")
    ap_seed.add_argument("--archive-dir", default="", help="Optional archive destination for seeded CSV.")
    ap_seed.add_argument("--no-archive", action="store_true", help="Keep the input CSV in place.")

    ap_mark = sub.add_parser("mark", help="Mark prospect lifecycle event.")
    ap_mark.add_argument("--prospect-id", required=True, help="Prospect id.")
    ap_mark.add_argument(
        "--event",
        required=True,
        choices=["replied", "trial_started", "converted", "do_not_contact"],
        help="Event/status to record.",
    )
    ap_mark.add_argument("--territory-code", default="OUTREACH_AUTO", help="Territory code for event/trial rows.")
    ap_mark.add_argument("--note", default="", help="Optional operator note.")

    sub.add_parser("stats", help="Read-only CRM counts and per-source summaries.")

    ap_verify = sub.add_parser("verify-import", help="Read-only sample check that import CSV emails exist in CRM.")
    ap_verify.add_argument("--csv", required=True, help="Path to CSV to sample for email/import verification.")

    args = ap.parse_args(argv)

    if args.cmd == "seed":
        archive_dir = Path(args.archive_dir) if (args.archive_dir or "").strip() else None
        return _seed_from_csv(Path(args.input), archive_dir=archive_dir, no_archive=bool(args.no_archive))
    if args.cmd == "mark":
        return _mark_event(
            prospect_id=str(args.prospect_id),
            event=str(args.event),
            territory_code=str(args.territory_code),
            note=str(args.note),
        )
    if args.cmd == "stats":
        return _stats()
    if args.cmd == "verify-import":
        return _verify_import(Path(str(args.csv)))
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
