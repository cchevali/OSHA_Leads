#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import sqlite3
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import ai_assist_paths
from outreach import crm_store
from outreach import run_prospect_generation as generation
from outreach.prospect_enrich_email import CORP_SUFFIXES
from runtime_data_dir import resolve_data_dir


PASS_CRM_AI_SKIP_EXPORT_PRINT_CONFIG = "PASS_CRM_AI_SKIP_EXPORT_PRINT_CONFIG"
PASS_CRM_AI_SKIP_EXPORT_DRY_RUN = "PASS_CRM_AI_SKIP_EXPORT_DRY_RUN"
PASS_CRM_AI_SKIP_EXPORT = "PASS_CRM_AI_SKIP_EXPORT"
ERR_CRM_AI_SKIP_EXPORT_DB_MISSING = "ERR_CRM_AI_SKIP_EXPORT_DB_MISSING"
ERR_CRM_AI_SKIP_EXPORT_DB_UNREADABLE = "ERR_CRM_AI_SKIP_EXPORT_DB_UNREADABLE"
ERR_CRM_AI_SKIP_EXPORT_DB_SCHEMA = "ERR_CRM_AI_SKIP_EXPORT_DB_SCHEMA"
OUTPUT_FILENAME = "crm_skip_list_for_ai.csv"
OUTPUT_COLUMNS = (
    "firm",
    "firm_key",
    "root_domain",
    "website",
    "states",
    "cities",
    "crm_statuses",
    "crm_sources",
    "contact_email_samples",
    "crm_record_count",
    "first_created_at",
    "last_created_at",
)
COMMON_MULTI_LABEL_TLDS = {
    "co.uk",
    "org.uk",
    "gov.uk",
    "ac.uk",
    "com.au",
    "net.au",
    "org.au",
    "com.br",
    "com.mx",
    "co.nz",
    "com.sg",
    "com.hk",
}


@dataclass(frozen=True)
class ExportResolution:
    crm_db_path: Path
    crm_db_source: str
    data_root: Path
    output_path: Path


def _normalize_text(value: Any) -> str:
    return " ".join(str(value or "").replace("\r", " ").replace("\n", " ").split()).strip()


def _normalize_email(value: Any) -> str:
    return _normalize_text(value).lower()


def _normalize_firm_key(value: str) -> str:
    import re

    text = _normalize_text(value).upper()
    if not text:
        return ""
    tokens = re.split(r"\s+", re.sub(r"[^A-Z0-9 ]", " ", text))
    while tokens and tokens[-1] in CORP_SUFFIXES:
        tokens.pop()
    return "".join(re.sub(r"[^A-Z0-9]", "", token) for token in tokens if token)


def _email_domain(email: str) -> str:
    text = _normalize_email(email)
    if "@" not in text:
        return ""
    return text.split("@", 1)[1].strip().lower()


def _root_domain(domain: str) -> str:
    import re

    host = _normalize_text(domain).lower().strip(".")
    if not host or re.fullmatch(r"\d+\.\d+\.\d+\.\d+", host):
        return host
    parts = [part for part in host.split(".") if part]
    if len(parts) <= 2:
        return ".".join(parts)
    suffix = ".".join(parts[-2:])
    if suffix in COMMON_MULTI_LABEL_TLDS and len(parts) >= 3:
        return ".".join(parts[-3:])
    return ".".join(parts[-2:])


def _crm_db_candidates() -> list[tuple[Path, str]]:
    resolution = resolve_data_dir(REPO_ROOT)
    candidates: list[tuple[Path, str]] = []
    if resolution.source == "default":
        candidates.append(((ai_assist_paths.LIVE_DATA_ROOT / "crm.sqlite").resolve(strict=False), "live_data_root"))
    candidates.append((crm_store.crm_db_path().resolve(strict=False), resolution.source or "data_dir"))
    deduped: list[tuple[Path, str]] = []
    seen: set[str] = set()
    for path, source in candidates:
        key = str(path).lower()
        if key in seen:
            continue
        seen.add(key)
        deduped.append((path, source))
    return deduped


def _resolve_output_path(*, output: str, output_dir: str, data_root: Path) -> Path:
    output_text = str(output or "").strip()
    if output_text:
        return Path(output_text).expanduser().resolve(strict=False)
    output_dir_text = str(output_dir or "").strip()
    if output_dir_text:
        return (Path(output_dir_text).expanduser().resolve(strict=False) / OUTPUT_FILENAME).resolve(strict=False)
    return (ai_assist_paths.prospect_audit_dir(data_root) / OUTPUT_FILENAME).resolve(strict=False)


def _resolve_export(output: str, output_dir: str) -> ExportResolution:
    selected_path = Path("")
    selected_source = ""
    for candidate_path, candidate_source in _crm_db_candidates():
        selected_path = candidate_path
        selected_source = candidate_source
        if candidate_path.exists():
            break
    data_root = selected_path.parent.resolve(strict=False)
    output_path = _resolve_output_path(output=output, output_dir=output_dir, data_root=data_root)
    return ExportResolution(
        crm_db_path=selected_path,
        crm_db_source=selected_source,
        data_root=data_root,
        output_path=output_path,
    )


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name = ? LIMIT 1",
        (table_name,),
    ).fetchone()
    return bool(row)


def _table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table_name})") if len(row) > 1}


def _open_read_only_connection(db_path: Path) -> sqlite3.Connection:
    uri = f"{db_path.resolve().as_uri()}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _prospect_select_sql(columns: set[str]) -> str:
    desired = {
        "prospect_id": "prospect_id",
        "firm": "firm",
        "email": "email",
        "website": "website",
        "state": "state",
        "city": "city",
        "status": "status",
        "source": "source",
        "created_at": "created_at",
    }
    select_parts: list[str] = []
    for alias, column in desired.items():
        if column in columns:
            select_parts.append(f"{column} AS {alias}")
        else:
            select_parts.append(f"'' AS {alias}")
    return "SELECT " + ", ".join(select_parts) + " FROM prospects"


def _sorted_join(values: set[str], *, limit: int = 0) -> str:
    ordered = sorted(value for value in values if value)
    if limit > 0:
        ordered = ordered[:limit]
    return "|".join(ordered)


def _pick_preferred_firm(values: set[str]) -> str:
    ordered = sorted((value for value in values if value), key=lambda item: (-len(item), item.lower(), item))
    return ordered[0] if ordered else ""


def _pick_preferred_website(values: set[str]) -> str:
    ordered = sorted((value for value in values if value), key=lambda item: (len(item), item.lower(), item))
    return ordered[0] if ordered else ""


def _collect_skip_rows(conn: sqlite3.Connection) -> list[dict[str, str]]:
    if not _table_exists(conn, "prospects"):
        raise RuntimeError("prospects_table_missing")
    columns = _table_columns(conn, "prospects")
    rows = conn.execute(_prospect_select_sql(columns)).fetchall()
    grouped: dict[str, dict[str, Any]] = {}
    for row in rows:
        firm = _normalize_text(row["firm"])
        firm_key = _normalize_firm_key(firm)
        website = _normalize_text(row["website"])
        email = _normalize_email(row["email"])
        raw_domain = _email_domain(email) or generation._domain_from_website(website)
        root_domain = _root_domain(raw_domain)
        group_key = root_domain or firm_key or _normalize_text(row["prospect_id"]) or f"row:{len(grouped) + 1}"
        group = grouped.setdefault(
            group_key,
            {
                "firms": set(),
                "firm_key": firm_key,
                "root_domain": root_domain,
                "websites": set(),
                "states": set(),
                "cities": set(),
                "statuses": set(),
                "sources": set(),
                "emails": set(),
                "created_at_values": [],
                "crm_record_count": 0,
            },
        )
        if firm:
            group["firms"].add(firm)
        if website:
            group["websites"].add(website)
        if row["state"]:
            group["states"].add(_normalize_text(row["state"]).upper())
        if row["city"]:
            group["cities"].add(_normalize_text(row["city"]))
        if row["status"]:
            group["statuses"].add(_normalize_text(row["status"]).lower())
        if row["source"]:
            group["sources"].add(_normalize_text(row["source"]))
        if email:
            group["emails"].add(email)
        created_at = _normalize_text(row["created_at"])
        if created_at:
            group["created_at_values"].append(created_at)
        group["crm_record_count"] = int(group["crm_record_count"]) + 1
        if not group["firm_key"] and firm_key:
            group["firm_key"] = firm_key
        if not group["root_domain"] and root_domain:
            group["root_domain"] = root_domain

    exported_rows: list[dict[str, str]] = []
    for group in grouped.values():
        created_values = sorted(value for value in list(group["created_at_values"]) if value)
        exported_rows.append(
            {
                "firm": _pick_preferred_firm(set(group["firms"])),
                "firm_key": str(group["firm_key"] or ""),
                "root_domain": str(group["root_domain"] or ""),
                "website": _pick_preferred_website(set(group["websites"])),
                "states": _sorted_join(set(group["states"])),
                "cities": _sorted_join(set(group["cities"])),
                "crm_statuses": _sorted_join(set(group["statuses"])),
                "crm_sources": _sorted_join(set(group["sources"])),
                "contact_email_samples": _sorted_join(set(group["emails"]), limit=3),
                "crm_record_count": str(int(group["crm_record_count"] or 0)),
                "first_created_at": created_values[0] if created_values else "",
                "last_created_at": created_values[-1] if created_values else "",
            }
        )
    exported_rows.sort(key=lambda item: ((item.get("firm") or "").lower(), (item.get("root_domain") or "").lower()))
    return exported_rows


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(OUTPUT_COLUMNS))
        writer.writeheader()
        writer.writerows(rows)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Export a shareable CRM skip-list CSV for external AI prospect research.")
    parser.add_argument("--output-dir", default="", help="Optional output directory override.")
    parser.add_argument("--output", default="", help="Optional full output CSV path override.")
    parser.add_argument("--print-config", action="store_true", help="Print resolved config and exit.")
    parser.add_argument("--dry-run", action="store_true", help="Compute export counts without writing output.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    resolved = _resolve_export(output=str(args.output or ""), output_dir=str(args.output_dir or ""))
    if args.print_config:
        print(f"{PASS_CRM_AI_SKIP_EXPORT_PRINT_CONFIG} crm_db={resolved.crm_db_path}")
        print(f"{PASS_CRM_AI_SKIP_EXPORT_PRINT_CONFIG} crm_db_source={resolved.crm_db_source}")
        print(f"{PASS_CRM_AI_SKIP_EXPORT_PRINT_CONFIG} data_root={resolved.data_root}")
        print(f"{PASS_CRM_AI_SKIP_EXPORT_PRINT_CONFIG} output_path={resolved.output_path}")
        return 0

    if not resolved.crm_db_path.exists():
        print(f"{ERR_CRM_AI_SKIP_EXPORT_DB_MISSING} path={resolved.crm_db_path}", file=sys.stderr)
        return 2

    try:
        conn = _open_read_only_connection(resolved.crm_db_path)
    except sqlite3.Error as exc:
        print(f"{ERR_CRM_AI_SKIP_EXPORT_DB_UNREADABLE} path={resolved.crm_db_path} err={exc}", file=sys.stderr)
        return 2

    try:
        rows = _collect_skip_rows(conn)
    except RuntimeError as exc:
        print(f"{ERR_CRM_AI_SKIP_EXPORT_DB_SCHEMA} path={resolved.crm_db_path} detail={exc}", file=sys.stderr)
        return 2
    finally:
        conn.close()

    root_domain_count = sum(1 for row in rows if str(row.get("root_domain") or "").strip())
    if args.dry_run:
        print(f"{PASS_CRM_AI_SKIP_EXPORT_DRY_RUN} crm_db={resolved.crm_db_path}")
        print(f"{PASS_CRM_AI_SKIP_EXPORT_DRY_RUN} output_path={resolved.output_path}")
        print(f"{PASS_CRM_AI_SKIP_EXPORT_DRY_RUN} rows={len(rows)}")
        print(f"{PASS_CRM_AI_SKIP_EXPORT_DRY_RUN} rows_with_root_domain={root_domain_count}")
        return 0

    _write_csv(resolved.output_path, rows)
    print(f"{PASS_CRM_AI_SKIP_EXPORT} crm_db={resolved.crm_db_path}")
    print(f"{PASS_CRM_AI_SKIP_EXPORT} output_path={resolved.output_path}")
    print(f"{PASS_CRM_AI_SKIP_EXPORT} rows={len(rows)}")
    print(f"{PASS_CRM_AI_SKIP_EXPORT} rows_with_root_domain={root_domain_count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
