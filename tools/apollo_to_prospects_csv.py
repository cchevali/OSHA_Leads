#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
import sys
import tempfile
from pathlib import Path
from urllib.parse import urlparse


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from outreach import crm_store


ERR_APOLLO_CONVERT_INPUT_MISSING = "ERR_APOLLO_CONVERT_INPUT_MISSING"
ERR_APOLLO_CONVERT_INPUT_NOT_FOUND = "ERR_APOLLO_CONVERT_INPUT_NOT_FOUND"
ERR_APOLLO_CONVERT_INPUT_UNREADABLE = "ERR_APOLLO_CONVERT_INPUT_UNREADABLE"
ERR_APOLLO_CONVERT_WRITE_FAILED = "ERR_APOLLO_CONVERT_WRITE_FAILED"

PASS_APOLLO_CONVERT_PRINT_CONFIG = "PASS_APOLLO_CONVERT_PRINT_CONFIG"
PASS_APOLLO_CONVERT_DRY_RUN = "PASS_APOLLO_CONVERT_DRY_RUN"
PASS_APOLLO_CONVERT_COMPLETE = "PASS_APOLLO_CONVERT_COMPLETE"

CORE_FIELDS = [
    "prospect_id",
    "firm",
    "email",
    "title",
    "city",
    "state",
    "source",
    "contact_name",
    "website",
]
MAPPED_HEADERS = {
    "first_name",
    "last_name",
    "title",
    "company_name",
    "email",
    "website",
    "city",
    "state",
    "source",
}


def _normalize_text(value: str) -> str:
    return str(value or "").strip()


def _normalize_state(value: str) -> str:
    return _normalize_text(value).upper()


def _normalize_email(value: str) -> str:
    return _normalize_text(value).lower()


def _normalize_header(value: str) -> str:
    text = _normalize_text(value).lower()
    text = re.sub(r"\s+", "_", text)
    return text


def _valid_email(value: str) -> bool:
    email = _normalize_email(value)
    if not email or "@" not in email:
        return False
    local, _, domain = email.partition("@")
    if not local or not domain:
        return False
    if "." not in domain:
        return False
    if domain.startswith(".") or domain.endswith("."):
        return False
    return True


def _email_domain(email: str) -> str:
    normalized = _normalize_email(email)
    if "@" not in normalized:
        return ""
    return normalized.split("@", 1)[1].strip().lower()


def _website_domain(website: str) -> str:
    raw = _normalize_text(website)
    if not raw:
        return ""
    parsed = urlparse(raw if "://" in raw else f"https://{raw}")
    host = _normalize_text(parsed.netloc or parsed.path).lower()
    if not host:
        return ""
    if "@" in host:
        host = host.split("@", 1)[1]
    host = host.split("/", 1)[0].split(":", 1)[0].strip(".")
    if host.startswith("www."):
        host = host[4:]
    return host


def _prospect_id(state: str, domain: str, email: str) -> str:
    base = f"{_normalize_state(state)}|{_normalize_text(domain).lower()}|{_normalize_email(email)}"
    digest = hashlib.sha1(base.encode("utf-8")).hexdigest()[:16]
    return f"gen_{digest}"


def _join_name(first_name: str, last_name: str) -> str:
    parts = [_normalize_text(first_name), _normalize_text(last_name)]
    return " ".join([p for p in parts if p]).strip()


def _resolve_output_path(explicit: str) -> Path:
    raw = _normalize_text(explicit)
    if raw:
        p = Path(raw)
        return p if p.is_absolute() else (REPO_ROOT / p)
    data_dir = crm_store.data_dir()
    return data_dir / "prospect_discovery" / "prospects_latest.csv"


def _resolve_diagnostics_path(explicit: str, output_path: Path) -> Path:
    raw = _normalize_text(explicit)
    if raw:
        p = Path(raw)
        return p if p.is_absolute() else (REPO_ROOT / p)
    return output_path.parent / "prospects_latest_apollo_diagnostics.json"


def _resolve_input_path(raw: str) -> Path | None:
    text = _normalize_text(raw)
    if not text:
        return None
    p = Path(text)
    return p if p.is_absolute() else (REPO_ROOT / p)


def _read_apollo_rows(path: Path) -> tuple[list[dict[str, str]], list[str]]:
    with open(path, "r", newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        raw_fieldnames = list(reader.fieldnames or [])
        normalized_names: list[str] = []
        field_map: dict[str, str] = {}
        counts: dict[str, int] = {}
        for raw in raw_fieldnames:
            base = _normalize_header(raw)
            if not base:
                base = "column"
            counts[base] = int(counts.get(base, 0)) + 1
            normalized = base if counts[base] == 1 else f"{base}_{counts[base]}"
            field_map[raw] = normalized
            normalized_names.append(normalized)

        rows: list[dict[str, str]] = []
        for row in reader:
            clean: dict[str, str] = {}
            for raw_name, value in dict(row).items():
                norm_name = field_map.get(raw_name or "", _normalize_header(str(raw_name or "")))
                if not norm_name:
                    continue
                clean[norm_name] = str(value or "")
            rows.append(clean)
    return rows, normalized_names


def _transform_rows(
    rows: list[dict[str, str]],
    normalized_headers: list[str],
) -> tuple[list[dict[str, str]], dict[str, object], list[str]]:
    seen_emails: set[str] = set()
    transformed: list[dict[str, str]] = []

    dropped_no_email = 0
    dropped_invalid_email = 0
    deduped = 0

    extra_headers = sorted([f"apollo_{h}" for h in normalized_headers if h and h not in MAPPED_HEADERS])

    for row in rows:
        email_raw = _normalize_text(row.get("email") or "")
        if not email_raw:
            dropped_no_email += 1
            continue

        email = _normalize_email(email_raw)
        if not _valid_email(email):
            dropped_invalid_email += 1
            continue
        if email in seen_emails:
            deduped += 1
            continue
        seen_emails.add(email)

        state = _normalize_state(row.get("state") or "")
        website = _normalize_text(row.get("website") or "")
        domain = _website_domain(website) or _email_domain(email)
        out_row: dict[str, str] = {
            "prospect_id": _prospect_id(state=state, domain=domain, email=email),
            "firm": _normalize_text(row.get("company_name") or ""),
            "email": email,
            "title": _normalize_text(row.get("title") or ""),
            "city": _normalize_text(row.get("city") or ""),
            "state": state,
            "source": _normalize_text(row.get("source") or "apollo_export_csv"),
            "contact_name": _join_name(row.get("first_name") or "", row.get("last_name") or ""),
            "website": website,
        }
        for col in extra_headers:
            source_key = col.removeprefix("apollo_")
            out_row[col] = _normalize_text(row.get(source_key) or "")
        transformed.append(out_row)

    transformed.sort(key=lambda r: _normalize_email(r.get("email") or ""))

    diagnostics: dict[str, object] = {
        "input_rows": int(len(rows)),
        "output_rows": int(len(transformed)),
        "dropped_no_email": int(dropped_no_email),
        "dropped_invalid_email": int(dropped_invalid_email),
        "deduped": int(deduped),
    }
    return transformed, diagnostics, extra_headers


def _write_csv_atomic(path: Path, rows: list[dict[str, str]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w",
        newline="",
        encoding="utf-8",
        dir=str(path.parent),
        prefix="apollo_to_prospects_",
        suffix=".tmp",
        delete=False,
    ) as tmp:
        writer = csv.DictWriter(tmp, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
        tmp_path = Path(tmp.name)
    os.replace(str(tmp_path), str(path))


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _print_config(input_path: Path | None, output_path: Path, diagnostics_path: Path) -> None:
    print(f"{PASS_APOLLO_CONVERT_PRINT_CONFIG} data_dir={crm_store.data_dir().resolve()}")
    print(f"{PASS_APOLLO_CONVERT_PRINT_CONFIG} input_path={(input_path.resolve() if input_path else '(missing)')}")
    print(f"{PASS_APOLLO_CONVERT_PRINT_CONFIG} output_path={output_path.resolve()}")
    print(f"{PASS_APOLLO_CONVERT_PRINT_CONFIG} diagnostics_path={diagnostics_path.resolve()}")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Convert Apollo contact export CSV to discovery-ready prospects CSV.")
    ap.add_argument("--input", required=True, default="", help="Path to Apollo export CSV.")
    ap.add_argument("--output", default="", help="Optional output prospects CSV path.")
    ap.add_argument("--diagnostics-out", default="", help="Optional diagnostics JSON path.")
    ap.add_argument("--print-config", action="store_true", help="Print resolved config and exit.")
    ap.add_argument("--dry-run", action="store_true", help="Validate/transform in memory only; no writes.")
    args = ap.parse_args(argv)

    input_path = _resolve_input_path(str(args.input or ""))
    output_path = _resolve_output_path(str(args.output or ""))
    diagnostics_path = _resolve_diagnostics_path(str(args.diagnostics_out or ""), output_path)

    if args.print_config:
        _print_config(input_path=input_path, output_path=output_path, diagnostics_path=diagnostics_path)
        return 0

    if input_path is None:
        print(f"{ERR_APOLLO_CONVERT_INPUT_MISSING} input_missing", file=sys.stderr)
        return 2
    if not input_path.exists():
        print(f"{ERR_APOLLO_CONVERT_INPUT_NOT_FOUND} path={input_path.resolve()}", file=sys.stderr)
        return 2

    try:
        apollo_rows, normalized_headers = _read_apollo_rows(input_path)
    except Exception as exc:
        print(f"{ERR_APOLLO_CONVERT_INPUT_UNREADABLE} path={input_path.resolve()} err={exc}", file=sys.stderr)
        return 2

    rows, metrics, extra_headers = _transform_rows(apollo_rows, normalized_headers)
    fieldnames = CORE_FIELDS + extra_headers
    diagnostics_payload: dict[str, object] = dict(metrics)
    diagnostics_payload.update(
        {
            "input_path": str(input_path.resolve()),
            "output_path": str(output_path.resolve()),
            "diagnostics_path": str(diagnostics_path.resolve()),
            "dry_run": bool(args.dry_run),
        }
    )

    if args.dry_run:
        print(
            f"{PASS_APOLLO_CONVERT_DRY_RUN} input_path={input_path.resolve()} "
            f"output_path={output_path.resolve()} rows_in={len(apollo_rows)} rows_out={len(rows)}"
        )
        print(
            "APOLLO_CONVERT_METRICS "
            f"input_rows={int(metrics['input_rows'])} output_rows={int(metrics['output_rows'])} "
            f"dropped_no_email={int(metrics['dropped_no_email'])} "
            f"dropped_invalid_email={int(metrics['dropped_invalid_email'])} deduped={int(metrics['deduped'])}"
        )
        return 0

    try:
        _write_csv_atomic(path=output_path, rows=rows, fieldnames=fieldnames)
        _write_json(path=diagnostics_path, payload=diagnostics_payload)
    except Exception as exc:
        print(f"{ERR_APOLLO_CONVERT_WRITE_FAILED} err={exc}", file=sys.stderr)
        return 2

    print(
        f"{PASS_APOLLO_CONVERT_COMPLETE} input_path={input_path.resolve()} "
        f"output_path={output_path.resolve()} diagnostics_path={diagnostics_path.resolve()}"
    )
    print(
        "APOLLO_CONVERT_METRICS "
        f"input_rows={int(metrics['input_rows'])} output_rows={int(metrics['output_rows'])} "
        f"dropped_no_email={int(metrics['dropped_no_email'])} "
        f"dropped_invalid_email={int(metrics['dropped_invalid_email'])} deduped={int(metrics['deduped'])}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
