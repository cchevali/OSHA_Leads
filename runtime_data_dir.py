from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


VALID_DATA_DIR_SOURCES = {"inherited", "dotenv", "default"}


@dataclass(frozen=True)
class DataDirResolution:
    effective_path: Path
    source: str
    warning_token: str
    raw_value: str


@dataclass(frozen=True)
class OshaDbResolution:
    effective_path: Path
    source: str
    warning_token: str
    raw_value: str


def _repo_root_default() -> Path:
    return Path(__file__).resolve().parent


def _normalize_source(value: str) -> str:
    token = str(value or "").strip().lower()
    if token in VALID_DATA_DIR_SOURCES:
        return token
    return "default"


def _not_absolute_warn(raw_value: str) -> str:
    return f"WARN_DATA_DIR_NOT_ABSOLUTE=1 value={raw_value} behavior=UNSET_FOR_CHILD"


def resolve_data_dir(repo_root: Path | None = None) -> DataDirResolution:
    root = (repo_root or _repo_root_default()).resolve(strict=False)
    default_path = (root / "out").resolve(strict=False)

    mfo_effective = str(os.getenv("MFO_DATA_DIR_EFFECTIVE") or "").strip()
    if mfo_effective:
        mfo_path = Path(mfo_effective).expanduser()
        if mfo_path.is_absolute():
            return DataDirResolution(
                effective_path=mfo_path.resolve(strict=False),
                source=_normalize_source(os.getenv("MFO_DATA_DIR_SOURCE") or ""),
                warning_token="",
                raw_value=mfo_effective,
            )

    raw_env = os.getenv("DATA_DIR")
    raw = str(raw_env or "").strip()
    if raw and raw.lower() != "out":
        candidate = Path(raw).expanduser()
        if candidate.is_absolute():
            return DataDirResolution(
                effective_path=candidate.resolve(strict=False),
                source="inherited",
                warning_token="",
                raw_value=raw,
            )
        return DataDirResolution(
            effective_path=default_path,
            source="default",
            warning_token=_not_absolute_warn(raw),
            raw_value=raw,
        )

    if raw_env is not None:
        return DataDirResolution(
            effective_path=default_path,
            source="default",
            warning_token=_not_absolute_warn(raw),
            raw_value=raw,
        )

    return DataDirResolution(
        effective_path=default_path,
        source="default",
        warning_token="",
        raw_value="",
    )


def resolve_osha_db_path(repo_root: Path | None = None) -> OshaDbResolution:
    root = (repo_root or _repo_root_default()).resolve(strict=False)
    data_dir_resolution = resolve_data_dir(root)
    data_dir_default = (data_dir_resolution.effective_path / "osha.sqlite").resolve(strict=False)

    return OshaDbResolution(
        effective_path=data_dir_default,
        source="data_dir",
        warning_token=str(data_dir_resolution.warning_token or ""),
        raw_value="",
    )
