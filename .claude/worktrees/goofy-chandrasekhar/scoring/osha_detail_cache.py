from __future__ import annotations

import gzip
import hashlib
import json
import os
import re
import sqlite3
import time
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from bs4 import BeautifulSoup

import ingest_osha
from scoring import paths as scoring_paths


DETAIL_PARSE_VERSION = "osha_detail_cache_v1"
DEFAULT_TTL_DAYS = 30
MAX_FETCH_ATTEMPTS = 3
REQUEST_TIMEOUT_SECONDS = 30

_BOOL_TRUE = {"1", "true", "yes", "on"}
_FEDERAL_ACTIVITY_NR_RE = re.compile(r"^\d+(?:\.\d+)?$")


@dataclass
class CacheRunConfig:
    leads_db_path: Path
    cache_db_path: Path
    since_days: int = 14
    limit: int = 500
    sleep_ms: int = 800
    ttl_days: int = DEFAULT_TTL_DAYS
    dry_run: bool = False
    create_parents: bool = True


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_iso() -> str:
    return utc_now().isoformat()


def _parse_dt(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        try:
            return datetime.strptime(text, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except Exception:
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


def _normalize_activity_nr(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if not _FEDERAL_ACTIVITY_NR_RE.fullmatch(text):
        return ""
    if "." in text:
        text = text.split(".", 1)[0]
    return text.strip()


def _activity_source(value: Any) -> str:
    text = str(value or "").strip().lower()
    if text.startswith("stateplan:"):
        return "stateplan"
    return "non_federal"


def _detail_url_for_activity(activity_nr: str) -> str:
    act = _normalize_activity_nr(activity_nr)
    return f"https://www.osha.gov/ords/imis/establishment.inspection_detail?id={act}"


def _prefer_detail_url(source_url: Any, activity_nr: Any) -> str:
    url = str(source_url or "").strip()
    if "establishment.inspection_detail" in url.lower():
        return url
    return _detail_url_for_activity(str(activity_nr or ""))


def _sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    try:
        rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    except Exception:
        return set()
    return {str(r[1]) for r in rows if len(r) > 1}


def ensure_detail_cache_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS inspection_detail_cache (
            activity_nr TEXT PRIMARY KEY,
            url TEXT NOT NULL,
            final_url TEXT NOT NULL,
            http_status INTEGER,
            fetched_at_utc TEXT NOT NULL,
            content_sha256 TEXT,
            raw_html_gz BLOB,
            case_status TEXT,
            inspection_type TEXT,
            scope TEXT,
            advanced_notice TEXT,
            ownership TEXT,
            safety_health TEXT,
            union_status TEXT,
            naics TEXT,
            sic TEXT,
            office TEXT,
            date_opened TEXT,
            emphasis_markers_json TEXT,
            related_activity_markers_json TEXT,
            parse_version TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS cache_failures (
            activity_nr TEXT PRIMARY KEY,
            last_error_token TEXT,
            last_error_detail TEXT,
            last_http_status INTEGER,
            last_attempt_at_utc TEXT,
            retry_after_utc TEXT,
            failure_count INTEGER NOT NULL DEFAULT 0
        );
        """
    )
    conn.commit()


def connect_detail_cache(db_path: Path | str | None = None) -> sqlite3.Connection:
    path = scoring_paths.detail_cache_db_path() if db_path is None else Path(db_path)
    path = path.expanduser().resolve(strict=False)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    ensure_detail_cache_schema(conn)
    return conn


def _row_to_dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return {str(k): row[k] for k in row.keys()}


def get_cached_detail_row(conn: sqlite3.Connection, activity_nr: str) -> dict[str, Any] | None:
    row = conn.execute(
        "SELECT * FROM inspection_detail_cache WHERE activity_nr = ? LIMIT 1",
        (_normalize_activity_nr(activity_nr),),
    ).fetchone()
    return _row_to_dict(row)


def load_detail_cache_rows(
    cache_db_path: str | Path | None,
    activity_nrs: list[str],
) -> dict[str, dict[str, Any]]:
    wanted = sorted({_normalize_activity_nr(v) for v in activity_nrs if _normalize_activity_nr(v)})
    if not wanted:
        return {}
    path = scoring_paths.detail_cache_db_path() if cache_db_path is None else Path(cache_db_path)
    if not path.exists():
        return {}
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    try:
        placeholders = ",".join(["?"] * len(wanted))
        rows = conn.execute(
            f"SELECT * FROM inspection_detail_cache WHERE activity_nr IN ({placeholders})",
            tuple(wanted),
        ).fetchall()
        return {_normalize_activity_nr(r["activity_nr"]): _row_to_dict(r) or {} for r in rows}
    finally:
        conn.close()


def _failure_row(conn: sqlite3.Connection, activity_nr: str) -> dict[str, Any] | None:
    row = conn.execute(
        "SELECT * FROM cache_failures WHERE activity_nr = ? LIMIT 1",
        (_normalize_activity_nr(activity_nr),),
    ).fetchone()
    return _row_to_dict(row)


def _is_failure_cooldown_active(conn: sqlite3.Connection, activity_nr: str, now_utc: datetime) -> bool:
    row = _failure_row(conn, activity_nr)
    if not row:
        return False
    retry_after = _parse_dt(row.get("retry_after_utc"))
    return bool(retry_after and retry_after > now_utc)


def _record_failure(
    conn: sqlite3.Connection,
    activity_nr: str,
    error_token: str,
    error_detail: str,
    http_status: int | None = None,
    cooldown_minutes: int = 240,
) -> None:
    now = utc_now()
    retry_after = now + timedelta(minutes=max(1, int(cooldown_minutes)))
    existing = conn.execute(
        "SELECT failure_count FROM cache_failures WHERE activity_nr = ?",
        (_normalize_activity_nr(activity_nr),),
    ).fetchone()
    count = int(existing[0] or 0) + 1 if existing else 1
    conn.execute(
        """
        INSERT INTO cache_failures (
            activity_nr, last_error_token, last_error_detail, last_http_status,
            last_attempt_at_utc, retry_after_utc, failure_count
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(activity_nr) DO UPDATE SET
            last_error_token=excluded.last_error_token,
            last_error_detail=excluded.last_error_detail,
            last_http_status=excluded.last_http_status,
            last_attempt_at_utc=excluded.last_attempt_at_utc,
            retry_after_utc=excluded.retry_after_utc,
            failure_count=excluded.failure_count
        """,
        (
            _normalize_activity_nr(activity_nr),
            str(error_token or "").strip(),
            str(error_detail or "").strip(),
            None if http_status is None else int(http_status),
            now.isoformat(),
            retry_after.isoformat(),
            int(count),
        ),
    )
    conn.commit()


def _clear_failure(conn: sqlite3.Connection, activity_nr: str) -> None:
    conn.execute("DELETE FROM cache_failures WHERE activity_nr = ?", (_normalize_activity_nr(activity_nr),))
    conn.commit()


def _is_within_ttl(cached_row: dict[str, Any] | None, ttl_days: int, now_utc: datetime) -> bool:
    if not cached_row:
        return False
    fetched = _parse_dt(cached_row.get("fetched_at_utc"))
    if not fetched:
        return False
    return fetched >= (now_utc - timedelta(days=max(0, int(ttl_days))))


def _parse_label_value_text_pairs(html: str) -> dict[str, str]:
    soup = BeautifulSoup(html, "html.parser")
    out: dict[str, str] = {}
    for table in soup.find_all("table"):
        for row in table.find_all("tr"):
            cells = row.find_all(["td", "th"])
            if len(cells) < 2:
                continue
            k = " ".join(cells[0].get_text(" ", strip=True).split()).strip().rstrip(":").lower()
            v = " ".join(cells[1].get_text(" ", strip=True).split()).strip()
            if not k or not v or k in out:
                continue
            out[k] = v
    lines = [" ".join(x.split()) for x in soup.get_text("\n").splitlines() if x and x.strip()]
    for line in lines:
        if ":" not in line:
            continue
        k, v = line.split(":", 1)
        key = " ".join(k.split()).strip().lower()
        val = " ".join(v.split()).strip()
        if key and val and key not in out:
            out[key] = val
    return out


def _normalize_marker_tokens(text: str) -> list[str]:
    blob = str(text or "").lower()
    tokens: list[str] = []
    markers = [
        ("trench", "emphasis_trench"),
        ("excavat", "emphasis_trench"),
        ("fall", "emphasis_fall"),
        ("silica", "emphasis_silica"),
        ("heat", "emphasis_heat"),
        ("amput", "emphasis_amputation"),
        ("fatal", "severity_fatal"),
        ("hospital", "severity_hospitalization"),
        ("catastroph", "severity_catastrophe"),
        ("related activit", "related_activity"),
        ("related inspection", "related_activity"),
    ]
    for needle, token in markers:
        if needle in blob and token not in tokens:
            tokens.append(token)
    return tokens


def extract_detail_fields(html: str, final_url: str) -> dict[str, Any]:
    parsed = dict(ingest_osha.parse_inspection_detail(html, final_url) or {})
    labels = _parse_label_value_text_pairs(html)

    def pick(*keys: str) -> str | None:
        for key in keys:
            value = labels.get(key.lower())
            if value:
                return value
        return None

    office = parsed.get("area_office") or pick("area office", "office", "osha office")
    emphasis_text = str(parsed.get("emphasis") or pick("emphasis") or "").strip()
    raw_blob = " ".join(
        [
            emphasis_text,
            str(pick("related activity", "related activities") or ""),
            BeautifulSoup(html, "html.parser").get_text(" ", strip=True),
        ]
    )
    marker_tokens = _normalize_marker_tokens(raw_blob)
    emphasis_markers = sorted([t for t in marker_tokens if t.startswith("emphasis_") or t.startswith("severity_")])
    related_markers = sorted([t for t in marker_tokens if t.startswith("related_")])

    return {
        "case_status": str(parsed.get("case_status") or pick("case status") or "").strip() or None,
        "inspection_type": str(parsed.get("inspection_type") or pick("inspection type", "type") or "").strip() or None,
        "scope": str(parsed.get("scope") or pick("scope") or "").strip() or None,
        "advanced_notice": str(
            pick("advance notice", "advanced notice", "advance notice given", "advance notice?") or ""
        ).strip()
        or None,
        "ownership": str(pick("ownership", "ownership type") or "").strip() or None,
        "safety_health": str(parsed.get("safety_health") or pick("safety/health", "safety health") or "").strip()
        or None,
        "union_status": str(pick("union status", "union") or "").strip() or None,
        "naics": str(parsed.get("naics") or pick("naics") or "").strip() or None,
        "sic": str(parsed.get("sic") or pick("sic") or "").strip() or None,
        "office": str(office or "").strip() or None,
        "date_opened": str(parsed.get("date_opened") or pick("date opened", "open date") or "").strip() or None,
        "emphasis_markers_json": json.dumps(emphasis_markers, separators=(",", ":")),
        "related_activity_markers_json": json.dumps(related_markers, separators=(",", ":")),
        "parsed_payload": parsed,
    }


def _http_fetch_with_retry(
    session: Any,
    url: str,
    *,
    sleep_ms: int,
    max_attempts: int = MAX_FETCH_ATTEMPTS,
) -> tuple[int | None, bytes | None, str | None, str | None]:
    """Returns (http_status, content_bytes, final_url, error_token)."""
    last_status: int | None = None
    last_err = ""
    for attempt in range(1, max_attempts + 1):
        if sleep_ms > 0:
            time.sleep(max(0, int(sleep_ms)) / 1000.0)
        try:
            resp = session.get(url, timeout=REQUEST_TIMEOUT_SECONDS, allow_redirects=True)
            last_status = int(getattr(resp, "status_code", 0) or 0)
            if 200 <= last_status < 300:
                content = getattr(resp, "content", None)
                if content is None:
                    text = getattr(resp, "text", "")
                    content = str(text).encode("utf-8", errors="replace")
                return last_status, bytes(content), str(getattr(resp, "url", url) or url), None
            if last_status in (429, 500, 502, 503, 504) and attempt < max_attempts:
                last_err = f"http_{last_status}"
                continue
            return last_status, None, str(getattr(resp, "url", url) or url), f"http_{last_status}"
        except Exception as exc:  # pragma: no cover - exercised via mocks/tests mostly
            last_err = exc.__class__.__name__
            if attempt >= max_attempts:
                break
    return last_status, None, None, (last_err or "request_failed")


def _upsert_cache_row(
    conn: sqlite3.Connection,
    *,
    activity_nr: str,
    url: str,
    final_url: str,
    http_status: int | None,
    fetched_at_utc: str,
    content_sha256: str | None,
    raw_html_gz: bytes | None,
    extracted: dict[str, Any],
) -> None:
    conn.execute(
        """
        INSERT INTO inspection_detail_cache (
            activity_nr, url, final_url, http_status, fetched_at_utc, content_sha256, raw_html_gz,
            case_status, inspection_type, scope, advanced_notice, ownership, safety_health, union_status,
            naics, sic, office, date_opened, emphasis_markers_json, related_activity_markers_json, parse_version
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(activity_nr) DO UPDATE SET
            url=excluded.url,
            final_url=excluded.final_url,
            http_status=excluded.http_status,
            fetched_at_utc=excluded.fetched_at_utc,
            content_sha256=excluded.content_sha256,
            raw_html_gz=excluded.raw_html_gz,
            case_status=excluded.case_status,
            inspection_type=excluded.inspection_type,
            scope=excluded.scope,
            advanced_notice=excluded.advanced_notice,
            ownership=excluded.ownership,
            safety_health=excluded.safety_health,
            union_status=excluded.union_status,
            naics=excluded.naics,
            sic=excluded.sic,
            office=excluded.office,
            date_opened=excluded.date_opened,
            emphasis_markers_json=excluded.emphasis_markers_json,
            related_activity_markers_json=excluded.related_activity_markers_json,
            parse_version=excluded.parse_version
        """,
        (
            _normalize_activity_nr(activity_nr),
            str(url or "").strip(),
            str(final_url or url or "").strip(),
            None if http_status is None else int(http_status),
            str(fetched_at_utc or "").strip() or utc_now_iso(),
            str(content_sha256 or "").strip() or None,
            raw_html_gz,
            extracted.get("case_status"),
            extracted.get("inspection_type"),
            extracted.get("scope"),
            extracted.get("advanced_notice"),
            extracted.get("ownership"),
            extracted.get("safety_health"),
            extracted.get("union_status"),
            extracted.get("naics"),
            extracted.get("sic"),
            extracted.get("office"),
            extracted.get("date_opened"),
            extracted.get("emphasis_markers_json") or "[]",
            extracted.get("related_activity_markers_json") or "[]",
            DETAIL_PARSE_VERSION,
        ),
    )
    conn.commit()


def select_candidates_from_leads_db(
    leads_db_path: str | Path,
    since_days: int,
    limit: int,
) -> list[dict[str, str]]:
    path = Path(leads_db_path).expanduser().resolve(strict=False)
    if not path.exists():
        return []
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    try:
        if "inspections" not in {str(r[0]) for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}:
            return []
        cols = _table_columns(conn, "inspections")
        if "activity_nr" not in cols:
            return []
        source_url_expr = "source_url" if "source_url" in cols else "'' AS source_url"
        changed_at_expr = "changed_at" if "changed_at" in cols else "NULL AS changed_at"
        last_seen_expr = "last_seen_at" if "last_seen_at" in cols else "NULL AS last_seen_at"
        first_seen_expr = "first_seen_at" if "first_seen_at" in cols else "NULL AS first_seen_at"
        date_opened_expr = "date_opened" if "date_opened" in cols else "NULL AS date_opened"
        where_parts = ["TRIM(COALESCE(activity_nr, '')) <> ''"]
        if "parse_invalid" in cols:
            where_parts.append("COALESCE(parse_invalid, 0) = 0")
        query = f"""
            SELECT
                activity_nr,
                {source_url_expr},
                {changed_at_expr},
                {last_seen_expr},
                {first_seen_expr},
                {date_opened_expr}
            FROM inspections
            WHERE {' AND '.join(where_parts)}
            ORDER BY COALESCE(changed_at, last_seen_at, first_seen_at, date_opened, '') DESC
            LIMIT ?
        """
        rows = conn.execute(query, (max(1, int(limit)) * 5,)).fetchall()
        cutoff = utc_now() - timedelta(days=max(0, int(since_days)))
        out: list[dict[str, str]] = []
        seen: set[str] = set()
        for row in rows:
            raw_activity = str(row["activity_nr"] or "").strip()
            if not raw_activity:
                continue
            activity_nr = _normalize_activity_nr(raw_activity)
            dedupe_key = activity_nr if activity_nr else raw_activity.lower()
            if dedupe_key in seen:
                continue
            ts = (
                _parse_dt(row["changed_at"])
                or _parse_dt(row["last_seen_at"])
                or _parse_dt(row["first_seen_at"])
                or _parse_dt(row["date_opened"])
            )
            if ts and ts < cutoff:
                continue
            seen.add(dedupe_key)
            candidate_url = str(row["source_url"] or "").strip()
            if activity_nr:
                candidate_url = _prefer_detail_url(candidate_url, activity_nr)
            out.append(
                {
                    "activity_nr": activity_nr or raw_activity,
                    "url": candidate_url,
                }
            )
            if len(out) >= max(1, int(limit)):
                break
        return out
    finally:
        conn.close()


def ensure_cached_for_activities(
    *,
    activity_items: list[dict[str, Any]],
    cache_db_path: str | Path | None = None,
    sleep_ms: int = 800,
    ttl_days: int = DEFAULT_TTL_DAYS,
    dry_run: bool = False,
) -> dict[str, Any]:
    path = scoring_paths.detail_cache_db_path() if cache_db_path is None else Path(cache_db_path)
    if dry_run:
        return {
            "candidates": len(activity_items),
            "fetched": 0,
            "skipped_cached": 0,
            "failed": 0,
            "failed_reasons": {},
            "status": "DRY_RUN",
        }
    conn = connect_detail_cache(path)
    try:
        return _run_fetch_loop(
            conn=conn,
            candidates=activity_items,
            sleep_ms=sleep_ms,
            ttl_days=ttl_days,
        )
    finally:
        conn.close()


def _run_fetch_loop(
    *,
    conn: sqlite3.Connection,
    candidates: list[dict[str, Any]],
    sleep_ms: int,
    ttl_days: int,
) -> dict[str, Any]:
    stats = {
        "candidates": len(candidates),
        "fetched": 0,
        "skipped_cached": 0,
        "skipped_non_federal": 0,
        "failed": 0,
    }
    failure_reasons: Counter[str] = Counter()
    now = utc_now()
    session = ingest_osha.get_session()

    for cand in candidates:
        raw_activity_nr = str(cand.get("activity_nr") or "").strip()
        if not raw_activity_nr:
            stats["failed"] += 1
            failure_reasons["missing_activity_nr"] += 1
            continue
        activity_nr = _normalize_activity_nr(raw_activity_nr)
        if not activity_nr:
            source = _activity_source(raw_activity_nr)
            print(f"SKIP_DETAIL_CACHE_NON_FEDERAL activity_nr={raw_activity_nr} source={source}")
            stats["skipped_non_federal"] += 1
            continue
        url = _prefer_detail_url(cand.get("url"), activity_nr)
        cached = get_cached_detail_row(conn, activity_nr)
        if _is_within_ttl(cached, ttl_days=ttl_days, now_utc=now):
            stats["skipped_cached"] += 1
            continue
        if _is_failure_cooldown_active(conn, activity_nr, now):
            stats["failed"] += 1
            failure_reasons["cooldown_active"] += 1
            continue

        status, body, final_url, error_token = _http_fetch_with_retry(
            session,
            url,
            sleep_ms=max(0, int(sleep_ms)),
        )
        if body is None:
            stats["failed"] += 1
            token = str(error_token or "fetch_failed")
            failure_reasons[token] += 1
            _record_failure(
                conn,
                activity_nr=activity_nr,
                error_token=token,
                error_detail=token,
                http_status=status,
                cooldown_minutes=240 if token == "cooldown_active" else 120,
            )
            continue

        try:
            html = body.decode("utf-8", errors="replace")
            extracted = extract_detail_fields(html, final_url or url)
            digest = _sha256_hex(body)
            gz = gzip.compress(body)
            _upsert_cache_row(
                conn,
                activity_nr=activity_nr,
                url=url,
                final_url=final_url or url,
                http_status=status,
                fetched_at_utc=utc_now_iso(),
                content_sha256=digest,
                raw_html_gz=gz,
                extracted=extracted,
            )
            _clear_failure(conn, activity_nr)
            stats["fetched"] += 1
        except Exception as exc:
            stats["failed"] += 1
            token = "parse_failed"
            failure_reasons[token] += 1
            _record_failure(
                conn,
                activity_nr=activity_nr,
                error_token=token,
                error_detail=str(exc),
                http_status=status,
                cooldown_minutes=240,
            )

    stats["failed_reasons"] = dict(failure_reasons.most_common(10))
    stats["status"] = "OK"
    return stats


def run_cache(config: CacheRunConfig) -> dict[str, Any]:
    candidates = select_candidates_from_leads_db(
        leads_db_path=config.leads_db_path,
        since_days=config.since_days,
        limit=config.limit,
    )
    if config.dry_run:
        return {
            "candidates": len(candidates),
            "fetched": 0,
            "skipped_cached": 0,
            "failed": 0,
            "failed_reasons": {},
            "status": "DRY_RUN",
            "cache_db_path": str(config.cache_db_path),
            "leads_db_path": str(config.leads_db_path),
        }
    if config.create_parents:
        config.cache_db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = connect_detail_cache(config.cache_db_path)
    try:
        result = _run_fetch_loop(
            conn=conn,
            candidates=candidates,
            sleep_ms=config.sleep_ms,
            ttl_days=config.ttl_days,
        )
    finally:
        conn.close()
    result["cache_db_path"] = str(config.cache_db_path)
    result["leads_db_path"] = str(config.leads_db_path)
    return result


def summarize_failures(cache_db_path: str | Path) -> dict[str, int]:
    path = Path(cache_db_path).expanduser().resolve(strict=False)
    if not path.exists():
        return {}
    conn = sqlite3.connect(str(path))
    try:
        rows = conn.execute(
            """
            SELECT last_error_token, COUNT(*)
            FROM cache_failures
            GROUP BY last_error_token
            ORDER BY COUNT(*) DESC, last_error_token ASC
            """
        ).fetchall()
        return {str(k or "unknown"): int(v or 0) for k, v in rows}
    finally:
        conn.close()


def parse_marker_json(value: Any) -> list[str]:
    try:
        obj = json.loads(str(value or "[]"))
    except Exception:
        return []
    if not isinstance(obj, list):
        return []
    out: list[str] = []
    for item in obj:
        token = str(item or "").strip().lower()
        if token and token not in out:
            out.append(token)
    return out


def bool_env(name: str, default: bool = False) -> bool:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return bool(default)
    return raw.lower() in _BOOL_TRUE

