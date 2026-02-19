from __future__ import annotations

import csv
import gzip
import json
import re
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
ZIP_TO_CBSA_PATH = REPO_ROOT / "data" / "geo" / "zip_to_cbsa.csv.gz"
CBSA_META_PATH = REPO_ROOT / "data" / "geo" / "cbsa_meta.csv"
ZIP_TO_CBSA_DATASET_META_PATH = REPO_ROOT / "data" / "geo" / "zip_to_cbsa.meta.json"
ZIP_TO_CBSA_SOURCES_PATH = REPO_ROOT / "data" / "geo" / "SOURCES.md"
ZIP5_RE = re.compile(r"(\d{5})(?:-\d{4})?")
DATASET_LABEL_RE = re.compile(r"Dataset label:\s*`([^`]+)`", flags=re.IGNORECASE)
INCOMPLETE_LABEL_RE = re.compile(r"\b(seed|incomplete|bootstrap)\b", flags=re.IGNORECASE)
_DATASET_WARNING_EMITTED = False


@dataclass(frozen=True)
class LeadCbsaResolution:
    zip5: str | None
    cbsa: str | None
    reason: str
    used_mail_fallback: bool


def _normalize_zip5(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    match = ZIP5_RE.search(text)
    if not match:
        digits = "".join(ch for ch in text if ch.isdigit())
        if len(digits) < 5:
            return ""
        return digits[:5]
    return match.group(1)


def _normalize_cbsa(value: Any) -> str:
    text = str(value or "").strip()
    digits = "".join(ch for ch in text if ch.isdigit())
    if not digits:
        return ""
    return digits.zfill(5)


def extract_zip5_from_text(value: Any) -> str | None:
    zip5 = _normalize_zip5(value)
    return zip5 or None


def _label_is_incomplete(source_label: str) -> bool:
    return bool(INCOMPLETE_LABEL_RE.search(str(source_label or "")))


@lru_cache(maxsize=1)
def _dataset_status() -> dict[str, Any]:
    status = {
        "source_label": "",
        "dataset_incomplete": False,
        "metadata_source": "none",
    }

    meta_path = ZIP_TO_CBSA_DATASET_META_PATH
    if meta_path.exists():
        try:
            payload = json.loads(meta_path.read_text(encoding="utf-8"))
            if isinstance(payload, dict):
                source_field = payload.get("source")
                source_label_raw: Any = payload.get("source_label") or payload.get("dataset_label")
                if not source_label_raw and isinstance(source_field, dict):
                    source_label_raw = source_field.get("label") or source_field.get("source_label")
                if not source_label_raw:
                    source_label_raw = source_field
                source_label = str(source_label_raw or "").strip()
                if source_label:
                    status["source_label"] = source_label
                if "dataset_incomplete" in payload:
                    status["dataset_incomplete"] = bool(payload.get("dataset_incomplete"))
                else:
                    status["dataset_incomplete"] = _label_is_incomplete(source_label)
                status["metadata_source"] = "meta_json"
                return status
        except Exception:
            # Fall through to SOURCES.md parsing.
            pass

    sources_path = ZIP_TO_CBSA_SOURCES_PATH
    if sources_path.exists():
        try:
            text = sources_path.read_text(encoding="utf-8")
            match = DATASET_LABEL_RE.search(text)
            if match:
                source_label = str(match.group(1) or "").strip()
                status["source_label"] = source_label
                status["dataset_incomplete"] = _label_is_incomplete(source_label)
            status["metadata_source"] = "sources_md"
        except Exception:
            pass

    return status


def _emit_dataset_warning_once() -> None:
    global _DATASET_WARNING_EMITTED
    if _DATASET_WARNING_EMITTED:
        return
    status = _dataset_status()
    if not bool(status.get("dataset_incomplete")):
        return
    source_label = str(status.get("source_label") or "unknown").replace('"', "'")
    print(f'WARN_ZIP_CBSA_DATASET_INCOMPLETE source_label="{source_label}"', flush=True)
    _DATASET_WARNING_EMITTED = True


def zip_cbsa_dataset_status() -> dict[str, Any]:
    status = dict(_dataset_status())
    _emit_dataset_warning_once()
    return status


@lru_cache(maxsize=1)
def _zip_to_cbsa_map() -> dict[str, str]:
    mapping: dict[str, str] = {}
    _emit_dataset_warning_once()
    path = ZIP_TO_CBSA_PATH
    if not path.exists():
        return mapping

    with gzip.open(path, "rt", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            zip5 = _normalize_zip5(row.get("ZIP5") or row.get("zip5") or row.get("ZIP") or row.get("zip"))
            cbsa = _normalize_cbsa(row.get("CBSA") or row.get("cbsa"))
            if zip5 and cbsa:
                mapping[zip5] = cbsa
    return mapping


@lru_cache(maxsize=1)
def _cbsa_label_map() -> dict[str, str]:
    mapping: dict[str, str] = {}
    path = CBSA_META_PATH
    if not path.exists():
        return mapping

    with open(path, "r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            cbsa = _normalize_cbsa(row.get("CBSA") or row.get("cbsa"))
            label = str(row.get("metro_label") or row.get("label") or "").strip()
            if cbsa and label:
                mapping[cbsa] = label
    return mapping


def resolve_cbsa(zip5: str | None) -> str | None:
    normalized = _normalize_zip5(zip5)
    if not normalized:
        return None
    return _zip_to_cbsa_map().get(normalized)


def resolve_metro_label(cbsa: str | None) -> str | None:
    normalized = _normalize_cbsa(cbsa)
    if not normalized:
        return None
    return _cbsa_label_map().get(normalized)


def resolve_lead_cbsa(lead: dict[str, Any]) -> LeadCbsaResolution:
    site_zip = extract_zip5_from_text(lead.get("site_zip"))
    if site_zip:
        cbsa = resolve_cbsa(site_zip)
        if cbsa:
            return LeadCbsaResolution(
                zip5=site_zip,
                cbsa=cbsa,
                reason="CBSA_MATCH",
                used_mail_fallback=False,
            )
        return LeadCbsaResolution(
            zip5=site_zip,
            cbsa=None,
            reason="ZIP_UNKNOWN",
            used_mail_fallback=False,
        )

    mail_zip = extract_zip5_from_text(lead.get("mail_zip"))
    if mail_zip:
        cbsa = resolve_cbsa(mail_zip)
        if cbsa:
            return LeadCbsaResolution(
                zip5=mail_zip,
                cbsa=cbsa,
                reason="FALLBACK_USED",
                used_mail_fallback=True,
            )
        return LeadCbsaResolution(
            zip5=mail_zip,
            cbsa=None,
            reason="ZIP_UNKNOWN",
            used_mail_fallback=True,
        )

    return LeadCbsaResolution(
        zip5=None,
        cbsa=None,
        reason="ZIP_MISSING",
        used_mail_fallback=False,
    )


def clear_caches() -> None:
    global _DATASET_WARNING_EMITTED
    _zip_to_cbsa_map.cache_clear()
    _cbsa_label_map.cache_clear()
    _dataset_status.cache_clear()
    _DATASET_WARNING_EMITTED = False
