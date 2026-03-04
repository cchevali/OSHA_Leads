from __future__ import annotations

import csv
import hashlib
import json
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

from scoring import paths as scoring_paths


AI_PROMPT_VERSION = "ai_triage_v2_raise_only"
_DISABLED_EMITTED = False
_UNAVAILABLE_EMITTED = False
_AUTO_IMPORT_DONE = False


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _emit_disabled(reason: str = "", missing: str = "") -> None:
    global _DISABLED_EMITTED
    if _DISABLED_EMITTED:
        return
    parts = ["AI_FEATURES_DISABLED=1"]
    if missing:
        parts.append(f"missing={missing}")
    if reason:
        parts.append(f"reason={reason}")
    print(" ".join(parts))
    _DISABLED_EMITTED = True


def _emit_unavailable(detail: str = "") -> None:
    global _UNAVAILABLE_EMITTED
    if _UNAVAILABLE_EMITTED:
        return
    msg = "WARN_AI_TRIAGE_UNAVAILABLE"
    if detail:
        msg += f" detail={detail}"
    print(msg)
    _UNAVAILABLE_EMITTED = True


def _bool_env(name: str, default: bool = False) -> bool:
    raw = (os.getenv(name) or "").strip().lower()
    if not raw:
        return bool(default)
    return raw in {"1", "true", "yes", "on"}


def _compact_detail(value: Any, max_len: int = 180) -> str:
    text = " ".join(str(value or "").strip().split())
    if not text:
        return "unknown"
    if len(text) > int(max_len):
        text = text[: int(max_len)].rstrip() + "..."
    return text.replace(" ", "_")


def _strict_priority(value: Any) -> str:
    text = str(value or "").strip().upper()
    if text in {"HIGH", "MEDIUM", "LOW"}:
        return text
    return ""


def _auto_import_enabled() -> bool:
    return _bool_env("AI_REVIEW_AUTO_IMPORT_ENABLED", default=True)


def _auto_import_max_age_hours() -> float:
    raw = (os.getenv("AI_REVIEW_IMPORT_MAX_AGE_HOURS") or "").strip()
    if not raw:
        return 24.0
    try:
        n = float(raw)
    except Exception:
        return 24.0
    if n <= 0:
        return 24.0
    return n


def _candidate_import_dirs() -> list[Path]:
    data_dir_imports = scoring_paths.data_root() / "imports"
    raw_candidates = [
        (os.getenv("AI_REVIEW_IMPORT_DIR") or "").strip(),
        r"C:\osha_data\imports",
        str(data_dir_imports),
    ]
    out: list[Path] = []
    seen: set[str] = set()
    for raw in raw_candidates:
        if not raw:
            continue
        path = Path(raw).expanduser().resolve(strict=False)
        key = str(path).lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(path)
    return out


def _find_newest_ai_review_csv(dirs: list[Path]) -> Path | None:
    for directory in dirs:
        try:
            if not directory.exists() or (not directory.is_dir()):
                continue
        except Exception:
            continue
        newest: Path | None = None
        newest_mtime = -1.0
        for candidate in directory.glob("ai_review_*.csv"):
            try:
                if not candidate.is_file():
                    continue
                mtime = float(candidate.stat().st_mtime)
            except Exception:
                continue
            if mtime > newest_mtime or (mtime == newest_mtime and str(candidate) > str(newest)):
                newest = candidate.resolve(strict=False)
                newest_mtime = mtime
        if newest is not None:
            return newest
    return None


def _file_age_hours(path: Path) -> float:
    now_ts = datetime.now(timezone.utc).timestamp()
    mtime = float(path.stat().st_mtime)
    delta = max(0.0, now_ts - mtime)
    return delta / 3600.0


def _import_ai_review_csv_into_cache(
    *,
    csv_path: Path,
    prompt_hash: str,
    cache_db_path: str | Path | None = None,
) -> tuple[int, int, int]:
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = [str(name or "").strip() for name in (reader.fieldnames or [])]
        required = {"activity_nr", "ai_priority", "ai_reason"}
        if not required.issubset(set(fieldnames)):
            missing = ",".join(sorted(required.difference(set(fieldnames))))
            raise ValueError(f"missing_required_columns={missing}")

        total = 0
        rejected_invalid = 0
        upserts: list[tuple[str, str, str]] = []
        for row in reader:
            total += 1
            activity_nr = str((row or {}).get("activity_nr") or "").strip()
            priority = _strict_priority((row or {}).get("ai_priority"))
            reason = _normalize_reason((row or {}).get("ai_reason"))
            if not activity_nr or not priority:
                rejected_invalid += 1
                continue
            payload = {
                "priority": priority,
                "reason": reason,
                "prompt_hash": prompt_hash,
                "prompt_version": AI_PROMPT_VERSION,
                "model": "manual_import_auto",
                "cached": 1,
            }
            upserts.append(
                (
                    activity_nr,
                    json.dumps(payload, separators=(",", ":"), sort_keys=True),
                    _utc_now_iso(),
                )
            )

    imported = 0
    conn = connect_ai_cache(cache_db_path)
    try:
        for item_key, payload_json, created_at in upserts:
            conn.execute(
                """
                INSERT INTO ai_triage_cache(item_key, prompt_hash, model, response_json, created_at_utc)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(item_key, prompt_hash) DO UPDATE SET
                    model=excluded.model,
                    response_json=excluded.response_json,
                    created_at_utc=excluded.created_at_utc
                """,
                (
                    str(item_key),
                    str(prompt_hash),
                    "manual_import_auto",
                    str(payload_json),
                    str(created_at),
                ),
            )
            imported += 1
        conn.commit()
    finally:
        conn.close()

    return total, imported, rejected_invalid


def _maybe_auto_import_ai_review_csv_once(
    *,
    prompt_hash: str,
    cache_db_path: str | Path | None = None,
) -> None:
    global _AUTO_IMPORT_DONE
    if _AUTO_IMPORT_DONE:
        return
    _AUTO_IMPORT_DONE = True

    if not _auto_import_enabled():
        return

    dirs = _candidate_import_dirs()
    dirs_token = ";".join([str(path) for path in dirs]) or "none"
    newest = _find_newest_ai_review_csv(dirs)
    if newest is None:
        print(f"WARN_AI_REVIEW_AUTO_IMPORT_MISSING dirs={dirs_token} pattern=ai_review_*.csv")
        return

    try:
        age_hours = _file_age_hours(newest)
    except Exception as exc:
        print(
            "WARN_AI_REVIEW_AUTO_IMPORT_INVALID "
            f"path={newest} detail={_compact_detail(exc)}"
        )
        return

    max_age_hours = _auto_import_max_age_hours()
    if age_hours > max_age_hours:
        print(
            "WARN_AI_REVIEW_AUTO_IMPORT_STALE "
            f"path={newest} age_hours={age_hours:.2f} max_age_hours={max_age_hours:g}"
        )
        return

    try:
        total, imported, rejected_invalid = _import_ai_review_csv_into_cache(
            csv_path=newest,
            prompt_hash=prompt_hash,
            cache_db_path=cache_db_path,
        )
    except Exception as exc:
        print(
            "WARN_AI_REVIEW_AUTO_IMPORT_INVALID "
            f"path={newest} detail={_compact_detail(exc)}"
        )
        return

    print(
        "AI_REVIEW_AUTO_IMPORT_APPLIED "
        f"path={newest} total={total} imported={imported} "
        f"rejected_invalid={rejected_invalid} age_hours={age_hours:.2f}"
    )


def enabled() -> bool:
    return _bool_env("AI_TRIAGE_ENABLED", default=False)


def model_name() -> str:
    return (os.getenv("AI_TRIAGE_OPENAI_MODEL") or "").strip() or "gpt-4.1-mini"


def _api_key() -> str:
    return (os.getenv("OPENAI_API_KEY") or "").strip()


def _base_url() -> str:
    return (os.getenv("OPENAI_BASE_URL") or "").strip() or "https://api.openai.com/v1"


def _ai_cache_path() -> Path:
    return scoring_paths.ai_triage_cache_db_path()


def connect_ai_cache(db_path: str | Path | None = None) -> sqlite3.Connection:
    path = Path(db_path) if db_path else _ai_cache_path()
    path = path.expanduser().resolve(strict=False)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ai_triage_cache (
            item_key TEXT NOT NULL,
            prompt_hash TEXT NOT NULL,
            model TEXT NOT NULL,
            response_json TEXT NOT NULL,
            created_at_utc TEXT NOT NULL,
            PRIMARY KEY (item_key, prompt_hash)
        )
        """
    )
    conn.commit()
    return conn


def get_cached(conn: sqlite3.Connection, *, item_key: str, prompt_hash: str) -> dict[str, Any] | None:
    row = conn.execute(
        """
        SELECT response_json
        FROM ai_triage_cache
        WHERE item_key = ? AND prompt_hash = ?
        LIMIT 1
        """,
        (str(item_key or ""), str(prompt_hash or "")),
    ).fetchone()
    if not row:
        return None
    try:
        payload = json.loads(str(row["response_json"] or "{}"))
    except Exception:
        return None
    if isinstance(payload, dict):
        payload["cached"] = 1
        return payload
    return None


def put_cached(
    conn: sqlite3.Connection,
    *,
    item_key: str,
    prompt_hash: str,
    model: str,
    payload: dict[str, Any],
) -> None:
    conn.execute(
        """
        INSERT INTO ai_triage_cache(item_key, prompt_hash, model, response_json, created_at_utc)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(item_key, prompt_hash) DO UPDATE SET
            model=excluded.model,
            response_json=excluded.response_json,
            created_at_utc=excluded.created_at_utc
        """,
        (
            str(item_key or ""),
            str(prompt_hash or ""),
            str(model or ""),
            json.dumps(payload, separators=(",", ":"), sort_keys=True),
            _utc_now_iso(),
        ),
    )
    conn.commit()


def _strict_schema() -> dict[str, Any]:
    return {
        "name": "signal_priority",
        "schema": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "priority": {
                    "type": "string",
                    "enum": ["HIGH", "MEDIUM", "LOW"],
                },
                "reason": {"type": "string", "minLength": 1, "maxLength": 280},
            },
            "required": ["priority", "reason"],
        },
        "strict": True,
    }


def _system_prompt() -> str:
    return (
        "You are classifying OSHA inspection signals for relevance to independent safety "
        "consultants and OSHA defense attorneys who serve small to mid-size employers in "
        "construction, manufacturing, and industrial trades. "
        "Classify with these criteria: HIGH = active construction/industrial high-hazard signal, "
        "referral/complaint trigger, emphasis program NAICS, multi-employer site, or likely external safety-help need. "
        "MEDIUM = active inspection with moderate hazard profile, non-emphasis construction/industrial, "
        "or ambiguous company profile. "
        "LOW = routine planned inspection of low-hazard employer, large enterprise with in-house EHS, "
        "or minimal information content. "
        "Return strict JSON only."
    )


def prompt_hash() -> str:
    material = {
        "version": AI_PROMPT_VERSION,
        "system": _system_prompt(),
        "schema": _strict_schema()["schema"],
    }
    blob = json.dumps(material, separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def _build_prompt(item: dict[str, Any], detail_row: dict[str, Any], mode: str) -> tuple[str, str]:
    system = _system_prompt()
    user_payload = {
        "mode": str(mode or ""),
        "signal": {
            "activity_nr": str(item.get("activity_nr") or ""),
            "establishment_name": item.get("establishment_name"),
            "site_city": item.get("site_city"),
            "site_state": item.get("site_state"),
            "naics": item.get("naics"),
            "naics_desc": item.get("naics_desc"),
            "sic": item.get("sic"),
            "inspection_type": item.get("inspection_type"),
            "scope": item.get("scope"),
            "case_status": item.get("case_status"),
            "emphasis": item.get("emphasis"),
            "violations_count": item.get("violations_count"),
            "serious_violations": item.get("serious_violations"),
            "willful_violations": item.get("willful_violations"),
            "repeat_violations": item.get("repeat_violations"),
        },
        "detail_cache": {
            "inspection_type": detail_row.get("inspection_type"),
            "case_status": detail_row.get("case_status"),
            "scope": detail_row.get("scope"),
            "naics": detail_row.get("naics"),
            "sic": detail_row.get("sic"),
            "office": detail_row.get("office"),
            "date_opened": detail_row.get("date_opened"),
            "emphasis_markers_json": detail_row.get("emphasis_markers_json"),
            "related_activity_markers_json": detail_row.get("related_activity_markers_json"),
        },
    }
    return system, json.dumps(user_payload, separators=(",", ":"), sort_keys=True)


def _responses_api_call(*, system_text: str, user_text: str) -> dict[str, Any] | None:
    key = _api_key()
    if not key:
        _emit_unavailable("missing_openai_api_key")
        return None
    payload = {
        "model": model_name(),
        "temperature": 0,
        "max_output_tokens": 180,
        "input": [
            {"role": "system", "content": [{"type": "input_text", "text": system_text}]},
            {"role": "user", "content": [{"type": "input_text", "text": user_text}]},
        ],
        "text": {
            "format": {
                "type": "json_schema",
                "name": "signal_priority",
                "schema": _strict_schema()["schema"],
                "strict": True,
            }
        },
    }
    url = _base_url().rstrip("/") + "/responses"
    headers = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}
    for _attempt in range(2):
        try:
            resp = requests.post(url, headers=headers, json=payload, timeout=30)
            if resp.status_code >= 300:
                _emit_unavailable(f"http_{resp.status_code}")
                return None
            data = resp.json()
            text = ""
            if isinstance(data, dict):
                text = str(data.get("output_text") or "")
                if not text:
                    out = data.get("output") or []
                    if isinstance(out, list):
                        for item in out:
                            if not isinstance(item, dict):
                                continue
                            for c in (item.get("content") or []):
                                if isinstance(c, dict) and str(c.get("type")) in {"output_text", "text"}:
                                    text = str(c.get("text") or "")
                                    if text:
                                        break
                            if text:
                                break
            if not text:
                _emit_unavailable("empty_response_text")
                return None
            parsed = json.loads(text)
            if not isinstance(parsed, dict):
                _emit_unavailable("invalid_json_object")
                return None
            return parsed
        except Exception as exc:
            _emit_unavailable(f"request_error_{exc.__class__.__name__}")
            continue
    return None


def _normalize_priority(value: Any) -> str:
    text = str(value or "").strip().upper()
    if text in {"HIGH", "MEDIUM", "LOW"}:
        return text
    return "LOW"


def _normalize_reason(value: Any) -> str:
    text = " ".join(str(value or "").strip().split())
    if not text:
        return "No additional reasoning provided."
    if len(text) > 280:
        return text[:280].rstrip()
    return text


def get_or_compute(
    *,
    item_key: str,
    mode: str,
    item: dict[str, Any],
    detail_row: dict[str, Any],
    cache_db_path: str | Path | None = None,
) -> dict[str, Any] | None:
    if not enabled():
        _emit_disabled(reason="disabled")
        return None
    norm_key = str(item_key or "").strip()
    if not norm_key:
        return None

    p_hash = prompt_hash()
    _maybe_auto_import_ai_review_csv_once(
        prompt_hash=p_hash,
        cache_db_path=cache_db_path,
    )
    conn = connect_ai_cache(cache_db_path)
    try:
        cached = get_cached(conn, item_key=norm_key, prompt_hash=p_hash)
        if cached:
            return cached
        if not _api_key():
            _emit_unavailable("missing_openai_api_key")
            return None
        system_text, user_text = _build_prompt(item=item, detail_row=detail_row, mode=mode)
        parsed = _responses_api_call(system_text=system_text, user_text=user_text)
        if not parsed:
            return None
        normalized = {
            "priority": _normalize_priority(parsed.get("priority")),
            "reason": _normalize_reason(parsed.get("reason")),
            "prompt_hash": p_hash,
            "prompt_version": AI_PROMPT_VERSION,
            "model": model_name(),
            "cached": 0,
        }
        put_cached(
            conn,
            item_key=norm_key,
            prompt_hash=p_hash,
            model=model_name(),
            payload=normalized,
        )
        return normalized
    finally:
        conn.close()
