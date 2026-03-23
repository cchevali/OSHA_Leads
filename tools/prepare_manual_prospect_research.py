#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from string import Template

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import ai_assist_paths
from outreach import run_prospect_generation as generation
from runtime_data_dir import resolve_data_dir
from tools import export_crm_ai_skip_list as skip_export


ERR_MANUAL_PROSPECT_RESEARCH_CONFIG = "ERR_MANUAL_PROSPECT_RESEARCH_CONFIG"
ERR_MANUAL_PROSPECT_RESEARCH_SKIP_LIST = "ERR_MANUAL_PROSPECT_RESEARCH_SKIP_LIST"
ERR_MANUAL_PROSPECT_RESEARCH_TEMPLATE = "ERR_MANUAL_PROSPECT_RESEARCH_TEMPLATE"
PASS_MANUAL_PROSPECT_RESEARCH_PRINT_CONFIG = "PASS_MANUAL_PROSPECT_RESEARCH_PRINT_CONFIG"
PASS_MANUAL_PROSPECT_RESEARCH_DRY_RUN = "PASS_MANUAL_PROSPECT_RESEARCH_DRY_RUN"
PASS_MANUAL_PROSPECT_RESEARCH = "PASS_MANUAL_PROSPECT_RESEARCH"
DEFAULT_TARGET_FIRMS = 50
PROMPT_TEMPLATE_PATH = (REPO_ROOT / "tools" / "templates" / "manual_prospect_deep_research_prompt.txt").resolve(
    strict=False
)
PROMPT_FILENAME_TEMPLATE = "manual_prospect_deep_research_{date_token}.txt"
CSV_HEADER = ",".join(
    (
        "state",
        "decision",
        "firm",
        "website",
        "contact_name",
        "title",
        "email",
        "source_urls",
        "confidence",
        "evidence_snippet",
    )
)
STATE_LIC_DIAGNOSTIC = (
    "STATE_LIC remains TX-only in this pipeline; treat PA/OH as live states and rely on manual Deep Research plus "
    "multi-state-capable sources such as AIHA, OHS_BG, BCSP, and OSHA_NEWS."
)


@dataclass(frozen=True)
class PrepResolution:
    run_date: str
    states: list[str]
    target_firms: int
    data_dir: Path
    data_dir_source: str
    crm_db_path: Path
    crm_db_source: str
    skip_list_path: Path
    skip_list_rows: int
    prompt_template_path: Path
    prompt_output_path: Path


def _emit(key: str, value: str | int) -> None:
    print(f"{key}={value}")


def _local_now() -> datetime:
    return datetime.now().astimezone()


def _parse_states_arg(raw_states: list[str]) -> list[str]:
    if not list(raw_states or []):
        return []
    flattened: list[str] = []
    for raw in list(raw_states or []):
        flattened.extend([str(part or "").strip() for part in str(raw or "").split(",")])
    csv_text = ",".join([part for part in flattened if part])
    return generation._parse_states(csv_text) if csv_text else []


def _resolve_state_scope(raw_states: list[str]) -> list[str] | None:
    env_states = generation._parse_states(os.getenv("OUTREACH_STATES", "")) or list(generation.DEFAULT_STATE_SCOPE_ALL)
    override = ",".join(_parse_states_arg(raw_states))
    return generation._resolve_state_scope(override, env_states)


def _load_skip_list_rows() -> tuple[skip_export.ExportResolution, list[dict[str, str]]]:
    resolved = skip_export._resolve_export(output="", output_dir="")
    if not resolved.crm_db_path.exists():
        raise FileNotFoundError(f"crm_db_missing path={resolved.crm_db_path}")
    try:
        conn = skip_export._open_read_only_connection(resolved.crm_db_path)
    except sqlite3.Error as exc:
        raise RuntimeError(f"crm_db_unreadable path={resolved.crm_db_path} err={exc}") from exc
    try:
        rows = skip_export._collect_skip_rows(conn)
    except RuntimeError as exc:
        raise RuntimeError(f"crm_db_schema detail={exc}") from exc
    finally:
        conn.close()
    return resolved, rows


def _prompt_output_path(*, audit_dir: Path, run_date: str) -> Path:
    date_token = run_date.replace("-", "")
    filename = PROMPT_FILENAME_TEMPLATE.format(date_token=date_token)
    return (audit_dir / filename).resolve(strict=False)


def _render_prompt(resolution: PrepResolution) -> str:
    if not resolution.prompt_template_path.exists():
        raise FileNotFoundError(f"missing_template path={resolution.prompt_template_path}")
    template = Template(resolution.prompt_template_path.read_text(encoding="utf-8"))
    return template.safe_substitute(
        {
            "RUN_DATE": resolution.run_date,
            "STATE_SCOPE": ",".join(resolution.states),
            "TARGET_FIRMS": str(resolution.target_firms),
            "SKIP_LIST_PATH": str(resolution.skip_list_path),
            "CSV_HEADER": CSV_HEADER,
            "STATE_LIC_DIAGNOSTIC": STATE_LIC_DIAGNOSTIC,
        }
    ).rstrip() + "\n"


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _build_resolution(raw_states: list[str], target_firms: int) -> PrepResolution:
    states = _resolve_state_scope(raw_states)
    if not states:
        raise ValueError("state_scope_empty")
    if target_firms < 1:
        raise ValueError("target_firms_invalid")
    data_dir_resolution = resolve_data_dir(REPO_ROOT)
    skip_resolved, skip_rows = _load_skip_list_rows()
    prompt_output_path = _prompt_output_path(audit_dir=ai_assist_paths.prospect_audit_dir(repo_root=REPO_ROOT), run_date=_local_now().date().isoformat())
    return PrepResolution(
        run_date=_local_now().date().isoformat(),
        states=list(states),
        target_firms=int(target_firms),
        data_dir=data_dir_resolution.effective_path,
        data_dir_source=str(data_dir_resolution.source or "default"),
        crm_db_path=skip_resolved.crm_db_path,
        crm_db_source=skip_resolved.crm_db_source,
        skip_list_path=skip_resolved.output_path,
        skip_list_rows=len(skip_rows),
        prompt_template_path=PROMPT_TEMPLATE_PATH,
        prompt_output_path=prompt_output_path,
    )


def _emit_resolution(resolution: PrepResolution, *, dry_run: bool) -> None:
    _emit("MANUAL_PROSPECT_RESEARCH_DATA_DIR", str(resolution.data_dir))
    _emit("MANUAL_PROSPECT_RESEARCH_DATA_DIR_SOURCE", resolution.data_dir_source)
    _emit("MANUAL_PROSPECT_RESEARCH_CRM_DB", str(resolution.crm_db_path))
    _emit("MANUAL_PROSPECT_RESEARCH_CRM_DB_SOURCE", resolution.crm_db_source)
    _emit("MANUAL_PROSPECT_RESEARCH_RUN_DATE", resolution.run_date)
    _emit("MANUAL_PROSPECT_RESEARCH_STATES_SCOPE", ",".join(resolution.states))
    _emit("MANUAL_PROSPECT_RESEARCH_TARGET_FIRMS", resolution.target_firms)
    _emit("MANUAL_PROSPECT_RESEARCH_SKIP_LIST_PATH", str(resolution.skip_list_path))
    _emit("MANUAL_PROSPECT_RESEARCH_SKIP_LIST_ROWS", resolution.skip_list_rows)
    _emit("MANUAL_PROSPECT_RESEARCH_PROMPT_TEMPLATE_PATH", str(resolution.prompt_template_path))
    _emit("MANUAL_PROSPECT_RESEARCH_PROMPT_OUTPUT_PATH", str(resolution.prompt_output_path))
    _emit("MANUAL_PROSPECT_RESEARCH_DRY_RUN", 1 if dry_run else 0)
    _emit("MANUAL_PROSPECT_RESEARCH_STATE_LIC_DIAGNOSTIC", STATE_LIC_DIAGNOSTIC)


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        description="Prepare the nightly manual Deep Research prospect artifact and refresh the CRM skip list."
    )
    ap.add_argument("--states", nargs="+", default=[], help="Optional explicit state scope (comma-separated or list form).")
    ap.add_argument("--target-firms", type=int, default=DEFAULT_TARGET_FIRMS, help="Target firm count for the prompt.")
    ap.add_argument("--print-config", action="store_true", help="Print resolved config and exit.")
    ap.add_argument("--dry-run", action="store_true", help="Validate and report without writing artifacts.")
    return ap


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        resolution = _build_resolution(list(args.states or []), int(args.target_firms or DEFAULT_TARGET_FIRMS))
    except FileNotFoundError as exc:
        detail = str(exc)
        token = ERR_MANUAL_PROSPECT_RESEARCH_TEMPLATE if "missing_template" in detail else ERR_MANUAL_PROSPECT_RESEARCH_SKIP_LIST
        print(f"{token} detail={detail}", file=sys.stderr)
        return 2
    except (RuntimeError, sqlite3.Error) as exc:
        print(f"{ERR_MANUAL_PROSPECT_RESEARCH_SKIP_LIST} detail={exc}", file=sys.stderr)
        return 2
    except Exception as exc:
        print(f"{ERR_MANUAL_PROSPECT_RESEARCH_CONFIG} detail={exc}", file=sys.stderr)
        return 2

    _emit_resolution(resolution, dry_run=bool(args.dry_run))

    try:
        prompt_text = _render_prompt(resolution)
    except Exception as exc:
        print(f"{ERR_MANUAL_PROSPECT_RESEARCH_TEMPLATE} detail={exc}", file=sys.stderr)
        return 2

    _emit("MANUAL_PROSPECT_RESEARCH_PROMPT_CHARS", len(prompt_text))
    if args.print_config:
        print(f"{PASS_MANUAL_PROSPECT_RESEARCH_PRINT_CONFIG} status=OK")
        return 0
    if args.dry_run:
        print(f"{PASS_MANUAL_PROSPECT_RESEARCH_DRY_RUN} status=OK")
        return 0

    try:
        skip_resolved, skip_rows = _load_skip_list_rows()
        skip_export._write_csv(skip_resolved.output_path, skip_rows)
        _write_text(resolution.prompt_output_path, prompt_text)
    except Exception as exc:
        print(f"{ERR_MANUAL_PROSPECT_RESEARCH_SKIP_LIST} detail={exc}", file=sys.stderr)
        return 2

    print(f"{PASS_MANUAL_PROSPECT_RESEARCH} status=OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
