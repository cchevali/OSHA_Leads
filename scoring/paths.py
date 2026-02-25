from __future__ import annotations

import os
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def data_root() -> Path:
    raw = (os.getenv("DATA_DIR") or "").strip()
    if raw:
        p = Path(raw).expanduser()
        if not p.is_absolute():
            p = (REPO_ROOT / p)
        return p.resolve(strict=False)
    return (REPO_ROOT / "out").resolve(strict=False)


def scoring_root() -> Path:
    return (data_root() / "scoring").resolve(strict=False)


def cache_runs_dir() -> Path:
    return (scoring_root() / "cache_runs").resolve(strict=False)


def detail_cache_db_path() -> Path:
    return (scoring_root() / "osha_detail_cache.sqlite").resolve(strict=False)


def ai_triage_cache_db_path() -> Path:
    return (scoring_root() / "ai_triage_cache.sqlite").resolve(strict=False)


def default_leads_db_path() -> Path:
    return (REPO_ROOT / "data" / "osha.sqlite").resolve(strict=False)


def resolve_leads_db_path(db_path: str | Path | None = None) -> Path:
    raw = str(db_path or "").strip()
    if raw:
        return Path(raw).expanduser().resolve(strict=False)
    return default_leads_db_path()

