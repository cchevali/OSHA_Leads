from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from runtime_data_dir import resolve_data_dir


REPO_ROOT = Path(__file__).resolve().parent
LIVE_DATA_ROOT = Path(r"C:\osha_data").resolve(strict=False)

SIGNALS_AUDIT_DIRNAME = "signals_ai_review"
SIGNALS_IMPORT_DIRNAME = "signals_ai_review"


@dataclass(frozen=True)
class PathCandidate:
    path: Path
    is_legacy: bool = False
    source: str = ""


def _data_roots(repo_root: Path | None = None) -> list[Path]:
    resolution = resolve_data_dir(repo_root or REPO_ROOT)
    resolved_root = resolution.effective_path.resolve(strict=False)
    roots: list[Path] = [resolved_root]
    if resolution.source == "default":
        roots.insert(0, LIVE_DATA_ROOT)

    out: list[Path] = []
    seen: set[str] = set()
    for root in roots:
        key = str(root).lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(root)
    return out


def _dedupe_candidates(candidates: Iterable[PathCandidate]) -> list[PathCandidate]:
    out: list[PathCandidate] = []
    seen: set[str] = set()
    for candidate in candidates:
        key = str(candidate.path.resolve(strict=False)).lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(
            PathCandidate(
                path=candidate.path.resolve(strict=False),
                is_legacy=bool(candidate.is_legacy),
                source=str(candidate.source or ""),
            )
        )
    return out


def signals_audit_dir(data_root: Path | None = None, *, repo_root: Path | None = None) -> Path:
    root = (data_root or resolve_data_dir(repo_root or REPO_ROOT).effective_path).resolve(strict=False)
    return (root / "audits" / SIGNALS_AUDIT_DIRNAME).resolve(strict=False)


def signals_import_dir(data_root: Path | None = None, *, repo_root: Path | None = None) -> Path:
    root = (data_root or resolve_data_dir(repo_root or REPO_ROOT).effective_path).resolve(strict=False)
    return (root / "imports" / SIGNALS_IMPORT_DIRNAME).resolve(strict=False)


def legacy_signals_import_dir(data_root: Path | None = None, *, repo_root: Path | None = None) -> Path:
    root = (data_root or resolve_data_dir(repo_root or REPO_ROOT).effective_path).resolve(strict=False)
    return (root / "imports").resolve(strict=False)


def signals_import_candidates(repo_root: Path | None = None) -> list[PathCandidate]:
    override_dir = str(os.getenv("AI_REVIEW_IMPORT_DIR") or "").strip()
    candidates: list[PathCandidate] = []
    if override_dir:
        candidates.append(
            PathCandidate(
                path=Path(override_dir).expanduser().resolve(strict=False),
                is_legacy=False,
                source="override",
            )
        )
    for root in _data_roots(repo_root):
        candidates.append(PathCandidate(path=signals_import_dir(root), is_legacy=False, source="canonical"))
    for root in _data_roots(repo_root):
        candidates.append(PathCandidate(path=legacy_signals_import_dir(root), is_legacy=True, source="legacy"))
    return _dedupe_candidates(candidates)


def _path_is_within(path: Path, parent: Path) -> bool:
    try:
        path.resolve(strict=False).relative_to(parent.resolve(strict=False))
        return True
    except Exception:
        return False


def signals_path_uses_legacy_dir(path: Path, repo_root: Path | None = None) -> bool:
    candidate = path.resolve(strict=False)
    for root in _data_roots(repo_root):
        if _path_is_within(candidate, legacy_signals_import_dir(root)):
            if not _path_is_within(candidate, signals_import_dir(root)):
                return True
    return False
