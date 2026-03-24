from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


SCHEDULE_SCHEMA = "ops_console_schedule_v1"
DEFAULT_OUTREACH_SEND_LOCAL_HHMM = "08:00"
DEFAULT_TRIAL_SEND_LOCAL_HHMM = "09:00"
DEFAULT_EVENING_PREP_LOCAL_HHMM = "20:45"

_HHMM_RE = re.compile(r"^(?P<hour>\d{2}):(?P<minute>\d{2})$")


@dataclass(frozen=True)
class RuntimeSchedule:
    path: Path
    exists: bool
    source: str
    schema: str
    outreach_send_local_hhmm: str
    trial_default_send_local_hhmm: str
    evening_prep_local_hhmm: str
    updated_at_utc: str = ""
    updated_by: str = ""

    def as_dict(self) -> dict[str, str]:
        return {
            "schema": self.schema,
            "outreach_send_local_hhmm": self.outreach_send_local_hhmm,
            "trial_default_send_local_hhmm": self.trial_default_send_local_hhmm,
            "evening_prep_local_hhmm": self.evening_prep_local_hhmm,
            "updated_at_utc": self.updated_at_utc,
            "updated_by": self.updated_by,
        }


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def validate_local_hhmm(value: str, *, field_name: str) -> str:
    text = str(value or "").strip()
    match = _HHMM_RE.fullmatch(text)
    if match is None:
        raise ValueError(f"{field_name} must be HH:MM (24-hour)")
    hour = int(match.group("hour"))
    minute = int(match.group("minute"))
    if hour < 0 or hour > 23 or minute < 0 or minute > 59:
        raise ValueError(f"{field_name} out of range (expected 00:00-23:59)")
    return f"{hour:02d}:{minute:02d}"


def schedule_config_path(data_dir: str | Path) -> Path:
    root = Path(data_dir)
    return (root / "runtime" / "config" / "schedule_overrides.json").resolve(strict=False)


def _coerce_schedule_payload(raw: Any, *, path: Path, exists: bool) -> RuntimeSchedule:
    payload = raw if isinstance(raw, dict) else {}
    source = "file" if exists else "default"
    schema = str(payload.get("schema") or SCHEDULE_SCHEMA).strip() or SCHEDULE_SCHEMA
    return RuntimeSchedule(
        path=path,
        exists=exists,
        source=source,
        schema=schema,
        outreach_send_local_hhmm=validate_local_hhmm(
            str(payload.get("outreach_send_local_hhmm") or DEFAULT_OUTREACH_SEND_LOCAL_HHMM),
            field_name="outreach_send_local_hhmm",
        ),
        trial_default_send_local_hhmm=validate_local_hhmm(
            str(payload.get("trial_default_send_local_hhmm") or DEFAULT_TRIAL_SEND_LOCAL_HHMM),
            field_name="trial_default_send_local_hhmm",
        ),
        evening_prep_local_hhmm=validate_local_hhmm(
            str(payload.get("evening_prep_local_hhmm") or DEFAULT_EVENING_PREP_LOCAL_HHMM),
            field_name="evening_prep_local_hhmm",
        ),
        updated_at_utc=str(payload.get("updated_at_utc") or "").strip(),
        updated_by=str(payload.get("updated_by") or "").strip(),
    )


def load_runtime_schedule(data_dir: str | Path) -> RuntimeSchedule:
    path = schedule_config_path(data_dir)
    if not path.exists():
        return _coerce_schedule_payload({}, path=path, exists=False)
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise ValueError(f"invalid schedule override JSON path={path} detail={exc}") from exc
    return _coerce_schedule_payload(raw, path=path, exists=True)


def write_runtime_schedule(
    data_dir: str | Path,
    *,
    outreach_send_local_hhmm: str,
    trial_default_send_local_hhmm: str,
    evening_prep_local_hhmm: str,
    updated_by: str = "ops_console",
) -> RuntimeSchedule:
    path = schedule_config_path(data_dir)
    payload = {
        "schema": SCHEDULE_SCHEMA,
        "outreach_send_local_hhmm": validate_local_hhmm(
            outreach_send_local_hhmm,
            field_name="outreach_send_local_hhmm",
        ),
        "trial_default_send_local_hhmm": validate_local_hhmm(
            trial_default_send_local_hhmm,
            field_name="trial_default_send_local_hhmm",
        ),
        "evening_prep_local_hhmm": validate_local_hhmm(
            evening_prep_local_hhmm,
            field_name="evening_prep_local_hhmm",
        ),
        "updated_at_utc": _now_utc_iso(),
        "updated_by": str(updated_by or "").strip() or "ops_console",
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp_path.replace(path)
    return load_runtime_schedule(data_dir)
