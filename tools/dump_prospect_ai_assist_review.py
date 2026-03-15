#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sqlite3
import sys
import tempfile
from datetime import date, datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from outreach import crm_store
from outreach import prospect_sources_aiha
from outreach import run_prospect_generation as generation
from outreach import source_policy
from outreach.prospect_enrich_email import CORP_SUFFIXES
from runtime_data_dir import resolve_data_dir

ERR_AI_ASSIST_DUMP_CONFIG = "ERR_AI_ASSIST_DUMP_CONFIG"
AI_ASSIST_DUMP_DEFAULT_BACKLOG_TARGET = 60
AI_ASSIST_DUMP_DEFAULT_ENABLED = "1"
AI_ASSIST_PACKET_DEFAULT_RAW_TARGET = 30
AI_ASSIST_PACKET_DEFAULT_SIZE = 10
AI_ASSIST_DEFAULT_AUTOGROW_SOURCES = ("AIHA", "OHS_BG", "STATE_LIC")
AI_ASSIST_PACKET_PUBLIC_SOURCES = ("AIHA", "OHS_BG", "BCSP", "OSHA_NEWS", "STATE_LIC")
SEED_PACKET_COLUMNS = ("firm", "website", "state", "seed_source", "seed_source_url")
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


def _emit(key: str, value: str | int) -> None:
    print(f"{key}={value}")


def _local_today_date() -> date:
    return datetime.now().astimezone().date()


def _parse_date(value: str) -> date:
    text = str(value or "").strip().lower()
    if not text or text == "today":
        return _local_today_date()
    return datetime.strptime(text, "%Y-%m-%d").date()


def _int_env(name: str, default: int) -> int:
    raw = str(os.getenv(name, "")).strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except Exception:
        return default
    return value if value > 0 else default


def _bool_env(name: str, default: str) -> int:
    raw = str(os.getenv(name, default)).strip().lower()
    return 1 if raw in {"1", "true", "yes", "on"} else 0


def _parse_states_arg(raw_states: list[str]) -> list[str]:
    if not list(raw_states or []):
        return []
    flattened: list[str] = []
    for raw in list(raw_states or []):
        flattened.extend([str(part or "").strip() for part in str(raw or "").split(",")])
    csv_text = ",".join([part for part in flattened if part])
    return generation._parse_states(csv_text) if csv_text else []


def _resolve_state_scope(raw_states: list[str]) -> list[str] | None:
    autogrow_env_states = generation._parse_states(os.getenv("PROSPECT_AUTOGROW_STATES", ""))
    outreach_env_states = generation._parse_states(os.getenv("OUTREACH_STATES", ""))
    env_states = autogrow_env_states or outreach_env_states or list(generation.DEFAULT_STATE_SCOPE_ALL)
    if not list(raw_states or []):
        return generation._resolve_state_scope("", env_states)
    return generation._resolve_state_scope(",".join(_parse_states_arg(raw_states)), env_states)


def _normalize_text(value: Any) -> str:
    return " ".join(str(value or "").replace("\r", " ").replace("\n", " ").split()).strip()


def _normalize_firm_key(value: str) -> str:
    text = _normalize_text(value).upper()
    if not text:
        return ""
    tokens = re.split(r"\s+", re.sub(r"[^A-Z0-9 ]", " ", text))
    while tokens and tokens[-1] in CORP_SUFFIXES:
        tokens.pop()
    return "".join(re.sub(r"[^A-Z0-9]", "", token) for token in tokens if token)


def _root_domain(domain: str) -> str:
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


def _resolve_source_tokens() -> list[str]:
    configured_raw = str(os.getenv("PROSPECT_AUTOGROW_SOURCES", "")).strip()
    configured = (
        [token for token in configured_raw.split(",") if str(token or "").strip()]
        if configured_raw
        else list(AI_ASSIST_DEFAULT_AUTOGROW_SOURCES)
    )
    ordered = source_policy.autogrow_source_order(configured)
    allowed = set(AI_ASSIST_PACKET_PUBLIC_SOURCES)
    implemented = set(source_policy.implemented_autogrow_sources())
    filtered = [token for token in ordered if token in allowed and token in implemented]
    if filtered:
        return filtered
    return [token for token in AI_ASSIST_DEFAULT_AUTOGROW_SOURCES if token in implemented]


def _resolve_output_paths(*, output: str, output_dir: str, for_date: date) -> tuple[Path, Path]:
    data_dir = resolve_data_dir(REPO_ROOT).effective_path
    default_packet_dir = (data_dir / "audits" / "ai_assist" / f"{for_date.strftime('%Y%m%d')}_packets").resolve(
        strict=False
    )
    output_dir_text = str(output_dir or "").strip()
    output_text = str(output or "").strip()
    if output_text:
        manifest_path = Path(output_text).expanduser().resolve(strict=False)
        packet_dir = (
            Path(output_dir_text).expanduser().resolve(strict=False)
            if output_dir_text
            else manifest_path.parent.resolve(strict=False)
        )
        return packet_dir, manifest_path
    packet_dir = Path(output_dir_text).expanduser().resolve(strict=False) if output_dir_text else default_packet_dir
    return packet_dir, (packet_dir / "manifest.json").resolve(strict=False)


def _atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_fd, tmp_name = tempfile.mkstemp(prefix=f"{path.name}.", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(tmp_fd, "w", encoding="utf-8", newline="") as handle:
            handle.write(text)
        os.replace(tmp_name, str(path))
    finally:
        try:
            if os.path.exists(tmp_name):
                os.unlink(tmp_name)
        except Exception:
            pass


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    _atomic_write_text(path, json.dumps(payload, indent=2, sort_keys=True) + "\n")


def _atomic_write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_fd, tmp_name = tempfile.mkstemp(prefix=f"{path.name}.", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(tmp_fd, "w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(SEED_PACKET_COLUMNS))
            writer.writeheader()
            for row in rows:
                writer.writerow({column: str(row.get(column) or "") for column in SEED_PACKET_COLUMNS})
        os.replace(tmp_name, str(path))
    finally:
        try:
            if os.path.exists(tmp_name):
                os.unlink(tmp_name)
        except Exception:
            pass


def _state_gap_snapshot(
    conn: sqlite3.Connection | None,
    *,
    states: list[str],
    suppressed_emails: set[str],
    backlog_target: int,
) -> list[dict[str, int | str]]:
    rows: list[dict[str, int | str]] = []
    for state in list(states or []):
        backlog_current = generation.compute_uncontacted_backlog(conn, state, suppressed_emails)
        crm_total = generation._count_crm_pool_total(conn, state)
        gap = max(0, int(backlog_target) - int(backlog_current))
        rows.append(
            {
                "state": state,
                "backlog_current": int(backlog_current),
                "crm_total": int(crm_total),
                "gap": int(gap),
            }
        )
    return [row for row in rows if int(row["gap"]) > 0]


def _existing_crm_firm_keys(conn: sqlite3.Connection | None) -> set[str]:
    if conn is None:
        return set()
    firm_keys: set[str] = set()
    try:
        rows = conn.execute("SELECT firm FROM prospects").fetchall()
    except Exception:
        return firm_keys
    for row in rows:
        firm_key = _normalize_firm_key(str(row[0] or ""))
        if firm_key:
            firm_keys.add(firm_key)
    return firm_keys


def _load_cache_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    if isinstance(payload, dict):
        rows = payload.get("rows") or []
        return [row for row in rows if isinstance(row, dict)]
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    return []


def _derive_seed_source_url(row: dict[str, Any], seed_source: str) -> str:
    explicit = _normalize_text(row.get("source_url") or "")
    if explicit:
        return explicit
    if seed_source.startswith("aiha_consultants_listing:"):
        page_id = seed_source.split(":", 1)[1].strip()
        if page_id:
            return prospect_sources_aiha.PAGE_URL_TEMPLATE.format(page_id=page_id)
    return ""


def _candidate_row(
    *,
    source_token: str,
    row: dict[str, Any],
    crm_domains: set[str],
    crm_firm_keys: set[str],
) -> dict[str, Any] | None:
    firm = _normalize_text(row.get("firm") or row.get("company_name") or "")
    website = generation.contact_normalization.normalize_website(str(row.get("website") or ""))
    state = generation._normalize_us_state(str(row.get("state") or ""))
    if not firm or not website or not state:
        return None
    domain = generation._domain_from_website(website)
    root_domain = _root_domain(domain)
    firm_key = _normalize_firm_key(firm)
    if not root_domain or not firm_key:
        return None
    if root_domain in crm_domains or firm_key in crm_firm_keys:
        return None
    seed_source = _normalize_text(row.get("source") or source_token) or source_token
    return {
        "firm": firm,
        "website": website,
        "state": state,
        "seed_source": seed_source,
        "seed_source_url": _derive_seed_source_url(row, seed_source),
        "source_token": source_token,
        "firm_key": firm_key,
        "root_domain": root_domain,
    }


def _collect_candidates(
    *,
    data_dir: Path,
    states: list[str],
    source_tokens: list[str],
    crm_domains: set[str],
    crm_firm_keys: set[str],
) -> list[dict[str, Any]]:
    cache_root = data_dir / "prospect_generation" / "cache"
    candidates: list[dict[str, Any]] = []
    source_priority = {token: idx for idx, token in enumerate(source_tokens)}
    for source_token in source_tokens:
        for state in states:
            cache_path = generation._source_cache_path_for_state(cache_root, source_token, state)
            for row in _load_cache_rows(cache_path):
                candidate = _candidate_row(
                    source_token=source_token,
                    row=row,
                    crm_domains=crm_domains,
                    crm_firm_keys=crm_firm_keys,
                )
                if candidate is not None:
                    candidates.append(candidate)
    ordered = sorted(
        candidates,
        key=lambda row: (
            source_priority.get(str(row.get("source_token") or ""), 9999),
            str(row.get("firm_key") or ""),
            str(row.get("root_domain") or ""),
            str(row.get("state") or ""),
            str(row.get("seed_source") or ""),
            str(row.get("website") or ""),
        ),
    )
    deduped: list[dict[str, Any]] = []
    seen_pairs: set[tuple[str, str]] = set()
    for row in ordered:
        pair = (str(row.get("firm_key") or ""), str(row.get("root_domain") or ""))
        if pair in seen_pairs:
            continue
        seen_pairs.add(pair)
        deduped.append(row)
    return deduped


def _packet_rows(rows: list[dict[str, Any]], packet_size: int) -> list[list[dict[str, str]]]:
    packets: list[list[dict[str, str]]] = []
    current: list[dict[str, str]] = []
    for row in rows:
        current.append({column: str(row.get(column) or "") for column in SEED_PACKET_COLUMNS})
        if len(current) >= packet_size:
            packets.append(current)
            current = []
    if current:
        packets.append(current)
    return packets


def _research_prompt_text() -> str:
    return (
        "Use one seed_packet_###.csv at a time.\n"
        "Visit each firm's website before returning any row.\n"
        "Check the about, team, leadership, and contact pages first.\n"
        "Return CSV only with this exact header:\n"
        "state,decision,firm,website,contact_name,title,email,source_urls,confidence,evidence_snippet\n"
        "Rules:\n"
        "- Use decision=accept only for rows with a named person and a business email.\n"
        "- Use decision=reject when the firm is not a fit, the site is dead, or evidence is weak.\n"
        "- Use source_urls with | between multiple URLs.\n"
        "- No markdown, no code fences, no commentary.\n"
    )


def _review_prompt_text() -> str:
    return (
        "Review the returned reviewed CSV before import.\n"
        "Reject any row that uses an inferred contact, generic role only, duplicate firm/domain, or missing named person.\n"
        "Reject rows with free-email domains, weak evidence, dead websites, or contacts not clearly tied to the firm.\n"
        "Keep only rows that are business-relevant and supported by direct website evidence.\n"
        "Output the same reviewed CSV schema only.\n"
    )


def _build_manifest(
    *,
    run_date: date,
    packet_dir: Path,
    manifest_path: Path,
    states: list[str],
    gap_rows: list[dict[str, int | str]],
    source_tokens: list[str],
    raw_target: int,
    packet_size: int,
    candidates_total: int,
    selected_rows: list[dict[str, Any]],
    packet_files: list[Path],
) -> dict[str, Any]:
    gap_total = sum(int(row["gap"] or 0) for row in gap_rows)
    shortfall = max(0, raw_target - len(selected_rows))
    packets: list[dict[str, Any]] = []
    for idx, path in enumerate(packet_files, start=1):
        rows_in_packet = packet_size
        if idx == len(packet_files) and len(selected_rows) % packet_size:
            rows_in_packet = len(selected_rows) % packet_size
        elif not packet_files:
            rows_in_packet = 0
        packets.append(
            {
                "packet_index": idx,
                "seed_packet_filename": path.name,
                "input_rows": rows_in_packet,
                "suggested_reviewed_filename": f"seed_packet_{idx:03d}_reviewed.csv",
                "suggested_batch_id": f"{run_date.isoformat()}_AIASSIST_P{idx:03d}",
                "reviewed_rows": None,
            }
        )
    return {
        "manifest_version": 1,
        "run_date": run_date.isoformat(),
        "packet_dir": str(packet_dir),
        "manifest_path": str(manifest_path),
        "states": list(states),
        "gap_states": [str(row.get("state") or "") for row in gap_rows],
        "gap_total": gap_total,
        "sources": list(source_tokens),
        "raw_target": raw_target,
        "packet_size": packet_size,
        "candidates_total": candidates_total,
        "rows_written": len(selected_rows),
        "packet_files_written": len(packet_files),
        "shortfall": shortfall,
        "shortfall_warning": 1 if shortfall > 0 else 0,
        "prompt_research_filename": "prompt_research.txt",
        "prompt_review_filename": "prompt_review.txt",
        "packets": packets,
    }


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="Emit packetized AI-assist seed review artifacts from existing autogrow cache rows.")
    ap.add_argument("--for-date", default="", help="Optional YYYY-MM-DD date override.")
    ap.add_argument("--states", nargs="+", default=[], help="Optional explicit state scope (comma-separated or list form).")
    ap.add_argument("--raw-target", type=int, default=0, help="Optional raw seed target override.")
    ap.add_argument("--packet-size", type=int, default=0, help="Optional seed packet size override.")
    ap.add_argument("--print-config", action="store_true", help="Print resolved config and exit.")
    ap.add_argument("--dry-run", action="store_true", help="Do not write output.")
    ap.add_argument("--output-dir", default="", help="Optional packet output directory override.")
    ap.add_argument("--output", default="", help="Optional manifest.json path override.")
    return ap


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        run_date = _parse_date(args.for_date)
        state_scope = _resolve_state_scope(list(args.states or []))
        raw_target = int(args.raw_target or _int_env("PROSPECT_AI_ASSIST_REVIEW_RAW_TARGET", AI_ASSIST_PACKET_DEFAULT_RAW_TARGET))
        packet_size = int(args.packet_size or _int_env("PROSPECT_AI_ASSIST_REVIEW_PACKET_SIZE", AI_ASSIST_PACKET_DEFAULT_SIZE))
        if raw_target < 1:
            raise ValueError("raw_target_invalid")
        if packet_size < 1:
            raise ValueError("packet_size_invalid")
    except Exception as exc:
        print(f"{ERR_AI_ASSIST_DUMP_CONFIG} detail={exc}", file=sys.stderr)
        return 2

    states = generation._states_for_selection(state_scope)
    enabled = _bool_env("PROSPECT_AI_ASSIST_REVIEW_ENABLED", AI_ASSIST_DUMP_DEFAULT_ENABLED)
    backlog_target = _int_env("PROSPECT_AUTOGROW_BACKLOG_TARGET", AI_ASSIST_DUMP_DEFAULT_BACKLOG_TARGET)
    source_tokens = _resolve_source_tokens()
    packet_dir, manifest_path = _resolve_output_paths(
        output=str(args.output or ""),
        output_dir=str(args.output_dir or ""),
        for_date=run_date,
    )
    data_dir_resolution = resolve_data_dir(REPO_ROOT)

    conn: sqlite3.Connection | None = None
    db_path = crm_store.crm_db_path()
    if db_path.exists():
        conn = crm_store.connect(db_path)
    try:
        suppressed_emails = generation._load_suppression_set(data_dir_resolution.effective_path, conn)
        gap_rows = _state_gap_snapshot(
            conn,
            states=states,
            suppressed_emails=suppressed_emails,
            backlog_target=backlog_target,
        )
        crm_domains = generation._existing_crm_domains(conn)
        crm_firm_keys = _existing_crm_firm_keys(conn)
    finally:
        if conn is not None:
            conn.close()

    candidates = _collect_candidates(
        data_dir=data_dir_resolution.effective_path,
        states=states,
        source_tokens=source_tokens,
        crm_domains=crm_domains,
        crm_firm_keys=crm_firm_keys,
    )
    selected_rows = candidates[:raw_target]
    packets = _packet_rows(selected_rows, packet_size)
    packet_paths = [packet_dir / f"seed_packet_{idx:03d}.csv" for idx in range(1, len(packets) + 1)]
    manifest = _build_manifest(
        run_date=run_date,
        packet_dir=packet_dir,
        manifest_path=manifest_path,
        states=states,
        gap_rows=gap_rows,
        source_tokens=source_tokens,
        raw_target=raw_target,
        packet_size=packet_size,
        candidates_total=len(candidates),
        selected_rows=selected_rows,
        packet_files=packet_paths,
    )
    gap_total = sum(int(row["gap"] or 0) for row in gap_rows)
    shortfall = max(0, raw_target - len(selected_rows))
    gap_states_csv = ",".join(str(row["state"] or "") for row in gap_rows)

    if data_dir_resolution.warning_token:
        print(data_dir_resolution.warning_token)
    _emit("AI_ASSIST_DUMP_ENABLED", enabled)
    _emit("AI_ASSIST_DUMP_DATA_DIR", str(data_dir_resolution.effective_path))
    _emit("AI_ASSIST_DUMP_DATA_DIR_SOURCE", str(data_dir_resolution.source or "default"))
    _emit("AI_ASSIST_DUMP_FOR_DATE", run_date.isoformat())
    _emit("AI_ASSIST_DUMP_STATES_SCOPE", ",".join(states))
    _emit("AI_ASSIST_DUMP_BACKLOG_TARGET", backlog_target)
    _emit("AI_ASSIST_DUMP_OUTPUT_DIR", str(packet_dir))
    _emit("AI_ASSIST_DUMP_OUTPUT_PATH", str(manifest_path))
    _emit("AI_ASSIST_DUMP_GAP_STATES", gap_states_csv or "none")
    _emit("AI_ASSIST_DUMP_GAP_TOTAL", gap_total)
    _emit("AI_ASSIST_DUMP_CANDIDATES_REQUESTED_TOTAL", raw_target)
    _emit("AI_ASSIST_PACKET_DIR", str(packet_dir))
    _emit("AI_ASSIST_PACKET_MANIFEST_PATH", str(manifest_path))
    _emit("AI_ASSIST_PACKET_SOURCES", ",".join(source_tokens))
    _emit("AI_ASSIST_PACKET_RAW_TARGET", raw_target)
    _emit("AI_ASSIST_PACKET_SIZE", packet_size)
    _emit("AI_ASSIST_PACKET_CANDIDATES_TOTAL", len(candidates))
    _emit("AI_ASSIST_PACKET_ROWS_WRITTEN", len(selected_rows))
    _emit("AI_ASSIST_PACKET_FILES_WRITTEN", len(packet_paths))
    _emit("AI_ASSIST_PACKET_SHORTFALL", shortfall)

    for row in gap_rows:
        state = str(row["state"] or "")
        _emit(f"AI_ASSIST_DUMP_STATE_{state}_BACKLOG_CURRENT", int(row["backlog_current"] or 0))
        _emit(f"AI_ASSIST_DUMP_STATE_{state}_CRM_TOTAL", int(row["crm_total"] or 0))
        _emit(f"AI_ASSIST_DUMP_STATE_{state}_GAP", int(row["gap"] or 0))

    if shortfall > 0:
        print(
            f"WARN_AI_ASSIST_PACKET_SHORTFALL=1 requested={raw_target} available={len(candidates)} shortfall={shortfall}"
        )

    if args.print_config:
        return 0

    if enabled != 1:
        _emit("AI_ASSIST_DUMP_SKIPPED", "1 reason=disabled")
        return 0

    if not gap_rows:
        _emit("AI_ASSIST_DUMP_SKIPPED", "1 reason=no_gap")
        return 0

    if args.dry_run:
        return 0

    packet_dir.mkdir(parents=True, exist_ok=True)
    _atomic_write_text(packet_dir / "prompt_research.txt", _research_prompt_text())
    _atomic_write_text(packet_dir / "prompt_review.txt", _review_prompt_text())
    for packet_path, packet_rows in zip(packet_paths, packets):
        _atomic_write_csv(packet_path, packet_rows)
    _atomic_write_json(manifest_path, manifest)
    _emit("AI_ASSIST_DUMP_WRITTEN", 1)
    _emit("AI_ASSIST_DUMP_OUTPUT_PATH", str(manifest_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
