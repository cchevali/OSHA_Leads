import argparse
import csv
import json
import os
import re
import shutil
import sqlite3
import sys
from collections import Counter
from pathlib import Path
from urllib.parse import urlparse

try:  # pragma: no cover
    from dotenv import load_dotenv

    load_dotenv()
except Exception:  # pragma: no cover
    pass


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from outreach import crm_store
from outreach import prospect_sources_ohs_bg
from outreach import source_policy
from outreach import us_state
import seed_recipients_pools as pools


ERR_CRM_INPUT_MISSING = "ERR_CRM_INPUT_MISSING"
ERR_CRM_MARK_MISSING = "ERR_CRM_MARK_MISSING"
ERR_CRM_STATS_DB_UNREADABLE = "ERR_CRM_STATS_DB_UNREADABLE"
ERR_CRM_VERIFY_INPUT_MISSING = "ERR_CRM_VERIFY_INPUT_MISSING"
ERR_CRM_VERIFY_INPUT_UNREADABLE = "ERR_CRM_VERIFY_INPUT_UNREADABLE"
ERR_CRM_VERIFY_DB_UNREADABLE = "ERR_CRM_VERIFY_DB_UNREADABLE"
ERR_CRM_REPAIR_DB_UNREADABLE = "ERR_CRM_REPAIR_DB_UNREADABLE"
PASS_CRM_SEED = "PASS_CRM_SEED"
PASS_CRM_MARK = "PASS_CRM_MARK"
PASS_CRM_REPAIR = "PASS_CRM_REPAIR"

VERIFY_IMPORT_SAMPLE_SIZE = 25
BLANK_SOURCE_LABEL = "(blank)"
EMAIL_SCAN_RE = re.compile(r"[^\s,;<>\"']+@[^\s,;<>\"']+")
VALID_SOURCE_FIT_TIERS = {"core_consultant", "recoverable_consultant", "adjacent_contractor"}
AIHA_NET_NEW_LOSS_KEYS = (
    "duplicate_email",
    "duplicate_domain",
    "state_out_of_scope",
    "free_domain",
    "already_known_crm",
    "default_send_ineligible",
)
def _discovery_source_families() -> tuple[str, ...]:
    families: list[str] = ["SEED"]
    for token in source_policy.implemented_autogrow_sources():
        family = source_policy.source_family_from_token(token)
        if family and family not in {"SEED", "AI_ASSIST", "UNKNOWN"} and family not in families:
            families.append(family)
    families.extend(["AI_ASSIST", "UNKNOWN"])
    return tuple(families)


DISCOVERY_SOURCE_FAMILIES = _discovery_source_families()


def _norm_email(value: str) -> str:
    return (value or "").strip().lower()


def _norm_state(value: str) -> str:
    return us_state.normalize_state_token(value).upper()


def _norm_us_state(value: str) -> str:
    return us_state.normalize_us_state(value)


def _norm_source(value: str) -> str:
    return (value or "").strip()


def _email_domain(email: str) -> str:
    value = _norm_email(email)
    if "@" not in value:
        return ""
    return value.split("@", 1)[1].strip().lower()


def _domain_from_website(value: str) -> str:
    text = (value or "").strip()
    if not text:
        return ""
    try:
        parsed = urlparse(text if "://" in text else f"https://{text}")
    except Exception:
        return ""
    host = (parsed.netloc or parsed.path or "").strip().lower()
    if host.startswith("www."):
        host = host[4:]
    return host


def _row_domain_value(email: str, website: str) -> str:
    from_email = _email_domain(email)
    if from_email:
        return from_email
    return _domain_from_website(website)


def _parse_scope_states(raw: str) -> set[str]:
    return set(us_state.parse_state_csv(raw, strict_us=True))


def _domain_matches_any(host: str, domains: set[str]) -> bool:
    token = (host or "").strip().lower()
    if not token:
        return False
    for suffix in domains:
        normalized = str(suffix or "").strip().lower()
        if not normalized:
            continue
        if token == normalized or token.endswith(f".{normalized}"):
            return True
    return False


def _coerce_boolish_int(value: str, default: int = 1) -> int:
    text = (value or "").strip().lower()
    if not text:
        return 1 if int(default) else 0
    if text in {"1", "true", "yes", "on"}:
        return 1
    if text in {"0", "false", "no", "off"}:
        return 0
    try:
        return 1 if int(text) != 0 else 0
    except Exception:
        return 1 if int(default) else 0


def _source_fit_defaults(source: str) -> tuple[str, int]:
    return source_policy.source_fit_defaults(source)


def _coerce_source_fit_tier(value: str, source: str) -> str:
    tier = (value or "").strip().lower()
    if tier in VALID_SOURCE_FIT_TIERS:
        return tier
    return _source_fit_defaults(source)[0]


def _source_family(source: str) -> str:
    return source_policy.source_family(source)


def _is_valid_boolish(value: str) -> bool:
    text = (value or "").strip().lower()
    return text in {"1", "true", "yes", "on", "0", "false", "no", "off"}


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


def _table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    return {str(r[1]) for r in conn.execute(f"PRAGMA table_info({table_name})") if len(r) > 1}


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

        columns = _table_columns(conn, "prospects")
        default_send_select = (
            "CAST(COALESCE(default_send_eligible, 1) AS TEXT) AS default_send_eligible"
            if "default_send_eligible" in columns
            else "'1' AS default_send_eligible"
        )
        family_rows = conn.execute(
            f"""
            SELECT
                state,
                source,
                email,
                {default_send_select}
            FROM prospects
            """
        ).fetchall()
        family_counts: dict[tuple[str, str], dict[str, int]] = {}
        target_states = set(us_state.DEFAULT_OUTREACH_STATES)
        for row in family_rows:
            state_norm = _norm_us_state(str(row["state"] or ""))
            if state_norm not in target_states:
                continue
            family = _source_family(str(row["source"] or ""))
            key = (state_norm, family)
            bucket = family_counts.setdefault(
                key,
                {"total": 0, "has_email": 0, "default_send_eligible": 0},
            )
            bucket["total"] += 1
            email_norm = _norm_email(str(row["email"] or ""))
            if email_norm:
                bucket["has_email"] += 1
            if _coerce_boolish_int(str(row["default_send_eligible"] or ""), default=1) == 1:
                bucket["default_send_eligible"] += 1
        for state_norm, family in sorted(family_counts.keys()):
            bucket = dict(family_counts.get((state_norm, family)) or {})
            print(
                "CRM_BY_STATE_SOURCE_FAMILY "
                f"state={state_norm} source_family={family} total={int(bucket.get('total', 0))} "
                f"has_email={int(bucket.get('has_email', 0))} "
                f"default_send_eligible={int(bucket.get('default_send_eligible', 0))}"
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
    source_counts: Counter = Counter()
    tier_counts: Counter = Counter()
    backfill_source_counts: Counter = Counter()
    default_send_eligible_total = 0
    unknown_source_backfill_count = 0
    unknown_source_samples: list[str] = []
    aiha_loss_counters: Counter = Counter({key: 0 for key in AIHA_NET_NEW_LOSS_KEYS})
    aiha_seen_emails: set[str] = set()
    aiha_seen_domains: set[str] = set()
    outreach_scope_states = _parse_scope_states(os.getenv("OUTREACH_STATES", ""))

    with open(input_path, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = []
        for r in reader:
            clean = {}
            for k, v in dict(r).items():
                key = (k or "").lstrip("\ufeff")
                clean[key] = v
            rows.append(clean)

    conn = crm_store.connect(db_path)
    try:
        crm_store.init_schema(conn)
        cur = conn.cursor()
        existing_emails: set[str] = set()
        existing_domains: set[str] = set()
        try:
            existing_rows = cur.execute("SELECT email, website FROM prospects").fetchall()
            for existing_email_raw, existing_website_raw in existing_rows:
                existing_email = _norm_email(str(existing_email_raw or ""))
                if existing_email and "@" in existing_email:
                    existing_emails.add(existing_email)
                    existing_domain = _email_domain(existing_email)
                    if existing_domain:
                        existing_domains.add(existing_domain)
                existing_website_domain = _domain_from_website(str(existing_website_raw or ""))
                if existing_website_domain:
                    existing_domains.add(existing_website_domain)
        except Exception:
            existing_emails = set()
            existing_domains = set()
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

            source = _norm_source(row.get("source") or "csv_seed")
            source_family = _source_family(source)
            source_fit_default, sendable_default = _source_fit_defaults(source)
            source_fit_raw = (row.get("source_fit_tier") or "").strip().lower()
            sendable_raw = (row.get("default_send_eligible") or "").strip()
            source_fit_backfilled = source_fit_raw not in VALID_SOURCE_FIT_TIERS
            sendable_backfilled = not _is_valid_boolish(sendable_raw)
            payload = {
                "prospect_id": prospect_id,
                "firm": firm,
                "contact_name": _contact_name(row),
                "email": email,
                "title": title,
                "city": (row.get("city") or "").strip(),
                "state": _norm_state(row.get("state", "")),
                "website": (row.get("website") or "").strip(),
                "source": source,
                "source_fit_tier": _coerce_source_fit_tier(row.get("source_fit_tier") or "", source),
                "default_send_eligible": _coerce_boolish_int(
                    row.get("default_send_eligible") or "",
                    default=sendable_default,
                ),
                "email_status": (row.get("email_status") or "").strip(),
                "enrichment_lane": (row.get("enrichment_lane") or "").strip(),
                "score": _coerce_score(row.get("score", ""), title),
                "status": status,
                "created_at": created_at,
                "last_contacted_at": last_contacted_at,
            }
            source_counts[source_family] += 1
            tier_counts[str(payload["source_fit_tier"] or "")] += 1
            if int(payload["default_send_eligible"] or 0) == 1:
                default_send_eligible_total += 1
            if source_family == "AIHA":
                if email in aiha_seen_emails:
                    aiha_loss_counters["duplicate_email"] += 1
                aiha_seen_emails.add(email)
                if email in existing_emails:
                    aiha_loss_counters["already_known_crm"] += 1
                if _email_domain(email) in pools.FREE_EMAIL_DOMAINS:
                    aiha_loss_counters["free_domain"] += 1
                domain = _row_domain_value(email=email, website=str(payload.get("website") or ""))
                if domain and (domain in aiha_seen_domains or domain in existing_domains):
                    aiha_loss_counters["duplicate_domain"] += 1
                if domain:
                    aiha_seen_domains.add(domain)
                state_norm = _norm_state(str(payload.get("state") or ""))
                if outreach_scope_states and (state_norm not in outreach_scope_states):
                    aiha_loss_counters["state_out_of_scope"] += 1
                if int(payload.get("default_send_eligible") or 0) != 1:
                    aiha_loss_counters["default_send_ineligible"] += 1
            if source_fit_backfilled or sendable_backfilled:
                backfill_source_counts[source_family] += 1
                if source_family == "UNKNOWN":
                    unknown_source_backfill_count += 1
                    sample_value = source or BLANK_SOURCE_LABEL
                    if sample_value not in unknown_source_samples and len(unknown_source_samples) < 8:
                        unknown_source_samples.append(sample_value)

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
                    source_fit_tier, default_send_eligible, email_status, enrichment_lane,
                    score, status, created_at, last_contacted_at
                ) VALUES (
                    :prospect_id, :firm, :contact_name, :email, :title, :city, :state, :website, :source,
                    :source_fit_tier, :default_send_eligible, :email_status, :enrichment_lane,
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
                    source_fit_tier = excluded.source_fit_tier,
                    default_send_eligible = excluded.default_send_eligible,
                    email_status = excluded.email_status,
                    enrichment_lane = excluded.enrichment_lane,
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
                existing_emails.add(email)
                new_domain = _row_domain_value(email=email, website=str(payload.get("website") or ""))
                if new_domain:
                    existing_domains.add(new_domain)
        conn.commit()
    finally:
        conn.close()

    archived_to = ""
    if not no_archive:
        target_dir = archive_dir or (input_path.parent / "archived_prospects")
        archived_to = str(_archive_input(input_path, target_dir))

    print(f"{PASS_CRM_SEED} crm_db={db_path}")
    print(f"{PASS_CRM_SEED} inserted_count={inserted}")
    print(f"{PASS_CRM_SEED} updated_count={updated}")
    print(f"{PASS_CRM_SEED} skipped_count={skipped}")
    for family in DISCOVERY_SOURCE_FAMILIES:
        print(f"DISCOVERY_SOURCE_COUNT_{family}={int(source_counts.get(family, 0))}")
    for tier in ("core_consultant", "recoverable_consultant", "adjacent_contractor"):
        print(f"DISCOVERY_TIER_COUNT_{tier.upper()}={int(tier_counts.get(tier, 0))}")
    print(f"DISCOVERY_DEFAULT_SEND_ELIGIBLE_TOTAL={int(default_send_eligible_total)}")
    for family in DISCOVERY_SOURCE_FAMILIES:
        print(f"DISCOVERY_BACKFILL_SOURCE_COUNT_{family}={int(backfill_source_counts.get(family, 0))}")
    print(f"DISCOVERY_BACKFILL_UNKNOWN_SOURCE_COUNT={int(unknown_source_backfill_count)}")
    print(
        "DISCOVERY_BACKFILL_UNKNOWN_SOURCE_SAMPLE="
        f"{'|'.join(unknown_source_samples) if unknown_source_samples else 'none'}"
    )
    for key in AIHA_NET_NEW_LOSS_KEYS:
        print(f"DISCOVERY_AIHA_LOSS_{key.upper()}={int(aiha_loss_counters.get(key, 0))}")
    if archived_to:
        print(f"{PASS_CRM_SEED} archived_to={archived_to}")
    return 0


def _is_bad_ohs_bg_tracker_or_ad(email: str, website: str) -> bool:
    email_host = _email_domain(email)
    website_host = _domain_from_website(website)
    if _domain_matches_any(website_host, set(prospect_sources_ohs_bg.TRACKER_OR_AD_DOMAINS)):
        return True
    if _domain_matches_any(email_host, set(prospect_sources_ohs_bg.TRACKER_OR_AD_DOMAINS)):
        return True
    if _domain_matches_any(email_host, set(prospect_sources_ohs_bg.BLOCKED_EMAIL_DOMAINS)):
        return True
    return False


def _repair_prospects(apply: bool) -> int:
    db_path = crm_store.crm_db_path()
    if not db_path.exists():
        print(f"{ERR_CRM_REPAIR_DB_UNREADABLE} path={db_path.resolve()} err=missing_db", file=sys.stderr)
        return 2
    try:
        conn = crm_store.connect(db_path)
    except sqlite3.Error as exc:
        print(f"{ERR_CRM_REPAIR_DB_UNREADABLE} path={db_path.resolve()} err={exc}", file=sys.stderr)
        return 2

    counters: Counter = Counter(
        {
            "state_canonicalized": 0,
            "source_fit_repaired": 0,
            "default_send_repaired": 0,
            "bad_ohs_bg_quarantined": 0,
        }
    )
    updates: list[tuple[str, str, int, str]] = []
    try:
        if not _prospects_table_exists(conn):
            print(f"{PASS_CRM_REPAIR} mode={'apply' if apply else 'dry_run'} changed=0")
            print("state_canonicalized=0")
            print("source_fit_repaired=0")
            print("default_send_repaired=0")
            print("bad_ohs_bg_quarantined=0")
            return 0

        rows = conn.execute(
            """
            SELECT prospect_id, state, source, source_fit_tier, default_send_eligible, email, website
            FROM prospects
            """
        ).fetchall()
        for row in rows:
            prospect_id = str(row["prospect_id"] or "").strip()
            if not prospect_id:
                continue

            state_raw = str(row["state"] or "")
            source_raw = str(row["source"] or "")
            source_family = _source_family(source_raw)
            current_state = state_raw.strip()
            current_tier = str(row["source_fit_tier"] or "").strip().lower()
            send_raw = row["default_send_eligible"]
            current_send = _coerce_boolish_int("" if send_raw is None else str(send_raw), default=1)
            email = _norm_email(str(row["email"] or ""))
            website = str(row["website"] or "")

            repaired_state = _norm_us_state(current_state) or current_state
            if repaired_state != current_state:
                counters["state_canonicalized"] += 1

            repaired_tier = current_tier
            repaired_send = int(current_send)
            if source_policy.source_uses_fixed_defaults(source_raw):
                default_tier, default_send = _source_fit_defaults(source_raw)
                repaired_tier = str(default_tier or "").strip().lower()
                repaired_send = int(default_send)

            quarantine_row = source_family == "OHS_BG" and _is_bad_ohs_bg_tracker_or_ad(email=email, website=website)
            if quarantine_row and repaired_send != 0 and int(current_send) != 0:
                counters["bad_ohs_bg_quarantined"] += 1
            if quarantine_row:
                repaired_send = 0

            if repaired_tier != current_tier:
                counters["source_fit_repaired"] += 1
            if int(repaired_send) != int(current_send):
                counters["default_send_repaired"] += 1

            if (
                repaired_state != current_state
                or repaired_tier != current_tier
                or int(repaired_send) != int(current_send)
            ):
                updates.append((repaired_state, repaired_tier, int(repaired_send), prospect_id))

        if apply and updates:
            conn.executemany(
                """
                UPDATE prospects
                SET state = ?, source_fit_tier = ?, default_send_eligible = ?
                WHERE prospect_id = ?
                """,
                updates,
            )
            conn.commit()
    except sqlite3.Error as exc:
        print(f"{ERR_CRM_REPAIR_DB_UNREADABLE} path={db_path.resolve()} err={exc}", file=sys.stderr)
        return 2
    finally:
        conn.close()

    print(f"{PASS_CRM_REPAIR} mode={'apply' if apply else 'dry_run'} changed={len(updates)}")
    print(f"state_canonicalized={int(counters.get('state_canonicalized', 0))}")
    print(f"source_fit_repaired={int(counters.get('source_fit_repaired', 0))}")
    print(f"default_send_repaired={int(counters.get('default_send_repaired', 0))}")
    print(f"bad_ohs_bg_quarantined={int(counters.get('bad_ohs_bg_quarantined', 0))}")
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

    ap_repair = sub.add_parser(
        "repair-prospects",
        help="Repair prospect state/source-fit/sendability drift with optional apply mode.",
    )
    mode = ap_repair.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true", help="Compute and print repair counts without writing.")
    mode.add_argument("--apply", action="store_true", help="Apply repair updates.")

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
    if args.cmd == "repair-prospects":
        return _repair_prospects(apply=bool(args.apply))
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
