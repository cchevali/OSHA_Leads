from __future__ import annotations

import json
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

from scoring import paths as scoring_paths


AI_PROMPT_VERSION = "ai_triage_v1"
_DISABLED_EMITTED = False


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


def _bool_env(name: str, default: bool = False) -> bool:
    raw = (os.getenv(name) or "").strip().lower()
    if not raw:
        return bool(default)
    return raw in {"1", "true", "yes", "on"}


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
            prompt_version TEXT NOT NULL,
            content_sha256 TEXT NOT NULL,
            model TEXT NOT NULL,
            response_json TEXT NOT NULL,
            created_at_utc TEXT NOT NULL,
            PRIMARY KEY (item_key, prompt_version, content_sha256)
        )
        """
    )
    conn.commit()
    return conn


def get_cached(
    conn: sqlite3.Connection,
    *,
    item_key: str,
    prompt_version: str,
    content_sha256: str,
) -> dict[str, Any] | None:
    row = conn.execute(
        """
        SELECT response_json
        FROM ai_triage_cache
        WHERE item_key = ? AND prompt_version = ? AND content_sha256 = ?
        LIMIT 1
        """,
        (str(item_key or ""), str(prompt_version or ""), str(content_sha256 or "")),
    ).fetchone()
    if not row:
        return None
    try:
        return json.loads(str(row["response_json"] or "{}"))
    except Exception:
        return None


def put_cached(
    conn: sqlite3.Connection,
    *,
    item_key: str,
    prompt_version: str,
    content_sha256: str,
    model: str,
    payload: dict[str, Any],
) -> None:
    conn.execute(
        """
        INSERT INTO ai_triage_cache(item_key, prompt_version, content_sha256, model, response_json, created_at_utc)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(item_key, prompt_version, content_sha256) DO UPDATE SET
            model=excluded.model,
            response_json=excluded.response_json,
            created_at_utc=excluded.created_at_utc
        """,
        (
            str(item_key or ""),
            str(prompt_version or ""),
            str(content_sha256 or ""),
            str(model or ""),
            json.dumps(payload, separators=(",", ":"), sort_keys=True),
            _utc_now_iso(),
        ),
    )
    conn.commit()


def _strict_schema() -> dict[str, Any]:
    return {
        "name": "triage_decision",
        "schema": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "key": {"type": "string"},
                "decision": {
                    "type": "string",
                    "enum": ["keep", "downgrade", "remove", "promote_candidate"],
                },
                "suggested_tier": {
                    "type": "string",
                    "enum": ["high", "medium", "low", "none"],
                },
                "confidence": {"type": "number", "minimum": 0, "maximum": 1},
                "reasons": {
                    "type": "array",
                    "minItems": 1,
                    "maxItems": 4,
                    "items": {"type": "string"},
                },
            },
            "required": ["key", "decision", "suggested_tier", "confidence", "reasons"],
        },
        "strict": True,
    }


def _build_prompt(item: dict[str, Any], detail_row: dict[str, Any], mode: str) -> tuple[str, str]:
    system = (
        "You are classifying OSHA inspection signals for email triage. "
        "Return only the structured decision. Be conservative. Prefer keep unless strong reasons."
    )
    user_payload = {
        "mode": mode,
        "item": {
            "activity_nr": str(item.get("activity_nr") or ""),
            "lead_key": str(item.get("lead_key") or ""),
            "current_priority": str(item.get("current_priority") or ""),
            "lead_score": item.get("lead_score"),
            "date_opened": item.get("date_opened"),
            "first_seen_at": item.get("first_seen_at"),
            "last_seen_at": item.get("last_seen_at"),
            "changed_at": item.get("changed_at"),
            "inspection_type": item.get("inspection_type"),
            "case_status": item.get("case_status"),
            "emphasis": item.get("emphasis"),
        },
        "detail_cache": {
            "inspection_type": detail_row.get("inspection_type"),
            "case_status": detail_row.get("case_status"),
            "scope": detail_row.get("scope"),
            "advanced_notice": detail_row.get("advanced_notice"),
            "ownership": detail_row.get("ownership"),
            "safety_health": detail_row.get("safety_health"),
            "union_status": detail_row.get("union_status"),
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
        _emit_disabled(missing="OPENAI_API_KEY")
        return None
    payload = {
        "model": model_name(),
        "temperature": 0,
        "max_output_tokens": 200,
        "input": [
            {"role": "system", "content": [{"type": "input_text", "text": system_text}]},
            {"role": "user", "content": [{"type": "input_text", "text": user_text}]},
        ],
        "text": {"format": {"type": "json_schema", "name": "triage_decision", "schema": _strict_schema()["schema"], "strict": True}},
    }
    url = _base_url().rstrip("/") + "/responses"
    headers = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}
    for _attempt in range(2):
        try:
            resp = requests.post(url, headers=headers, json=payload, timeout=30)
            if resp.status_code >= 300:
                return None
            data = resp.json()
            # Best-effort parse across response variants.
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
                return None
            parsed = json.loads(text)
            if not isinstance(parsed, dict):
                return None
            return parsed
        except Exception:
            continue
    return None


def get_or_compute(
    *,
    item_key: str,
    mode: str,
    content_sha256: str,
    item: dict[str, Any],
    detail_row: dict[str, Any],
    cache_db_path: str | Path | None = None,
) -> dict[str, Any] | None:
    if not enabled():
        _emit_disabled(reason="disabled")
        return None
    if not _api_key():
        _emit_disabled(missing="OPENAI_API_KEY")
        return None
    if not str(item_key or "").strip() or not str(content_sha256 or "").strip():
        return None

    conn = connect_ai_cache(cache_db_path)
    try:
        cached = get_cached(
            conn,
            item_key=item_key,
            prompt_version=AI_PROMPT_VERSION,
            content_sha256=content_sha256,
        )
        if cached:
            return cached
        system_text, user_text = _build_prompt(item=item, detail_row=detail_row, mode=mode)
        parsed = _responses_api_call(system_text=system_text, user_text=user_text)
        if not parsed:
            return None
        normalized = {
            "key": str(parsed.get("key") or item_key),
            "decision": str(parsed.get("decision") or "keep"),
            "suggested_tier": str(parsed.get("suggested_tier") or "none"),
            "confidence": float(parsed.get("confidence") or 0.0),
            "reasons": [str(x).strip().lower() for x in (parsed.get("reasons") or []) if str(x).strip()][:4],
            "provenance": "ai_cached",
            "prompt_version": AI_PROMPT_VERSION,
            "content_sha256": str(content_sha256),
            "model": model_name(),
        }
        if not normalized["reasons"]:
            normalized["reasons"] = ["ai_unspecified"]
        put_cached(
            conn,
            item_key=item_key,
            prompt_version=AI_PROMPT_VERSION,
            content_sha256=content_sha256,
            model=model_name(),
            payload=normalized,
        )
        return normalized
    finally:
        conn.close()

