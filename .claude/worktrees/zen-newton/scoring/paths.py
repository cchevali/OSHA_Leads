from __future__ import annotations

from pathlib import Path

from runtime_data_dir import resolve_data_dir, resolve_osha_db_path

REPO_ROOT = Path(__file__).resolve().parents[1]


def data_root() -> Path:
    return resolve_data_dir(REPO_ROOT).effective_path


def scoring_root() -> Path:
    return (data_root() / "scoring").resolve(strict=False)


def cache_runs_dir() -> Path:
    return (scoring_root() / "cache_runs").resolve(strict=False)


def detail_cache_db_path() -> Path:
    return (scoring_root() / "osha_detail_cache.sqlite").resolve(strict=False)


def ai_triage_cache_db_path() -> Path:
    return (scoring_root() / "ai_triage_cache.sqlite").resolve(strict=False)


def default_leads_db_path() -> Path:
    return resolve_osha_db_path(REPO_ROOT).effective_path


def resolve_leads_db_path(db_path: str | Path | None = None) -> Path:
    raw = str(db_path or "").strip()
    if raw:
        return Path(raw).expanduser().resolve(strict=False)
    return default_leads_db_path()
