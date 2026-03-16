#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import io
import json
import os
import re
import shutil
import sqlite3
import sys
import tempfile
from datetime import date, datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import ai_assist_paths
from outreach import crm_store
from outreach import prospect_sources_aiha
from outreach import run_prospect_generation as generation
from outreach import source_policy
from outreach.prospect_enrich_email import CORP_SUFFIXES
from runtime_data_dir import resolve_data_dir

ERR_AI_ASSIST_DUMP_CONFIG = "ERR_AI_ASSIST_DUMP_CONFIG"
AI_ASSIST_DUMP_DEFAULT_BACKLOG_TARGET = 60
AI_ASSIST_DUMP_DEFAULT_ENABLED = "1"
AI_ASSIST_DUMP_DEFAULT_RAW_TARGET = 30
AI_ASSIST_DUMP_DEFAULT_PACKET_SIZE = 10
AI_ASSIST_DEFAULT_AUTOGROW_SOURCES = ("AIHA", "OHS_BG", "STATE_LIC")
AI_ASSIST_PUBLIC_SOURCES = ("AIHA", "OHS_BG", "BCSP", "OSHA_NEWS", "STATE_LIC")
SEED_COLUMNS = ("state", "firm", "website", "seed_source", "seed_source_url")
REVIEW_COLUMNS = (
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
AI_ASSIST_PACKET_MANIFEST_SCHEMA = "ai_assist_packet_manifest_v1"


def _emit(key: str, value: str | int) -> None:
    print(f"{key}={value}")


def _local_today_date() -> date:
    return datetime.now().astimezone().date()


def _current_run_started_at() -> datetime:
    return datetime.now().astimezone()


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


def _normalized_root_domains(domains: set[str]) -> set[str]:
    normalized: set[str] = set()
    for domain in set(domains or set()):
        root = _root_domain(str(domain or ""))
        if root:
            normalized.add(root)
    return normalized


def _resolve_source_tokens() -> tuple[list[str], str]:
    configured_raw = str(os.getenv("PROSPECT_AUTOGROW_SOURCES", "")).strip()
    configured = (
        [token for token in configured_raw.split(",") if str(token or "").strip()]
        if configured_raw
        else list(AI_ASSIST_DEFAULT_AUTOGROW_SOURCES)
    )
    ordered = source_policy.autogrow_source_order(configured)
    allowed = set(AI_ASSIST_PUBLIC_SOURCES)
    implemented = set(source_policy.implemented_autogrow_sources())
    filtered = [token for token in ordered if token in allowed and token in implemented]
    if filtered:
        return filtered, ""
    if configured_raw:
        return [], ",".join([str(token or "").strip().upper() for token in configured if str(token or "").strip()])
    return [token for token in AI_ASSIST_DEFAULT_AUTOGROW_SOURCES if token in implemented], ""


def _batch_run_token(run_started_at: datetime) -> str:
    return f"R{run_started_at.strftime('%H%M%S%f')}"


def _resolve_output_path(*, output: str, output_dir: str, for_date: date) -> tuple[Path, Path]:
    default_output_dir = ai_assist_paths.prospect_audit_dir(repo_root=REPO_ROOT)
    filename = f"prospect_ai_assist_review_{for_date.strftime('%Y%m%d')}.txt"
    output_text = str(output or "").strip()
    output_dir_text = str(output_dir or "").strip()
    if output_text:
        out_path = Path(output_text).expanduser().resolve(strict=False)
    else:
        out_dir = Path(output_dir_text).expanduser().resolve(strict=False) if output_dir_text else default_output_dir
        out_path = (out_dir / filename).resolve(strict=False)
    return out_path.parent.resolve(strict=False), out_path.resolve(strict=False)


def _packet_dir_for_output_path(output_path: Path) -> Path:
    return (output_path.parent / f"{output_path.stem}_packets").resolve(strict=False)


def _packet_manifest_path(packet_dir: Path) -> Path:
    return (packet_dir / "manifest.json").resolve(strict=False)


def _packet_seed_filename(packet_number: int) -> str:
    return f"seed_packet_{packet_number:03d}.csv"


def _packet_prompt_filename(packet_number: int) -> str:
    return f"review_packet_{packet_number:03d}.txt"


def _packet_review_filename(run_date: date, packet_number: int) -> str:
    return f"prospect_ai_assist_review_{run_date.strftime('%Y%m%d')}_packet_{packet_number:03d}_reviewed.csv"


def _packet_batch_id(run_date: date, packet_number: int) -> str:
    return f"{run_date.isoformat()}_AIASSIST_P{packet_number:03d}"


def _chunk_rows(rows: list[dict[str, Any]], packet_size: int) -> list[list[dict[str, Any]]]:
    size = max(1, int(packet_size))
    return [rows[idx : idx + size] for idx in range(0, len(rows), size)]


def _reset_output_dir(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


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


def _csv_block(*, fieldnames: tuple[str, ...], rows: list[dict[str, Any]]) -> str:
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=list(fieldnames), lineterminator="\n")
    writer.writeheader()
    for row in rows:
        writer.writerow({field: str(row.get(field) or "") for field in fieldnames})
    return buffer.getvalue().strip()


def _seed_rows(selected_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [{field: str(row.get(field) or "") for field in SEED_COLUMNS} for row in selected_rows]


def _build_prompt_text(
    *,
    run_date: date,
    backlog_target: int,
    raw_target: int,
    source_tokens: list[str],
    gap_rows: list[dict[str, int | str]],
    selected_rows: list[dict[str, Any]],
    packet_number: int | None = None,
    packet_count: int = 0,
    packet_size: int = 0,
    reviewed_filename: str = "",
    suggested_batch_id: str = "",
    reviewed_drop_dir: Path | None = None,
) -> str:
    lines = [
        "# ============================================================",
        "# OSHA_LEADS - MANUAL AI-ASSIST DISCOVERY AUGMENTATION",
        "# ============================================================",
        "#",
        "# PURPOSE:",
        "# This is a controlled discovery augmentation lane for thin-state",
        "# consultant replenishment. It is not a sending workflow and it",
        "# does not bypass the repo's canonical discovery -> CRM path.",
        "#",
        "# WHEN TO USE:",
        "# Normal AIHA/OHS_BG replenishment and discovery already ran, but",
        "# one or more states are still below the backlog target.",
        "#",
        "# TARGET ICP:",
        "# Business contacts only for safety consultants and boutique",
        "# OSHA-facing firms. Prefer owner, founder, principal, partner,",
        "# president, or managing consultant roles at firms that actively",
        "# sell OSHA/safety consulting services.",
        "#",
        "# RULES:",
        "# - Business contacts only. No personal emails, no sensitive data.",
        "# - No outreach copy, cadence, score, or send-rule changes.",
        "# - Use the seed candidates below as the canonical research queue.",
        "# - Visit the firm website before returning any row.",
        "# - Return only rows you are confident are real, business-relevant",
        "#   consultant prospects for the listed state.",
        "# - Use business email addresses tied to the firm domain.",
        "# - Return standard CSV only.",
        "# - Use source_urls with | between multiple URLs in one field.",
        "# - Quote any field that contains a comma.",
        "# - Escape embedded double quotes by doubling them.",
        "# - Use plain text only. No markdown links, no mailto links, no",
        "#   code fences, no surrounding brackets, and no commentary.",
        "# - confidence must be an integer 0-100.",
        "# - evidence_snippet must be short, factual provenance.",
        "# - Return ONLY the CSV block. No commentary before or after.",
        "#",
        "# OUTPUT CSV HEADER:",
        "# state,decision,firm,website,contact_name,title,email,source_urls,confidence,evidence_snippet",
        "# Use decision=accept for rows to import and decision=reject for rows to keep visible but blocked.",
        "#",
        '# VALID ACCEPT EXAMPLE:',
        '# TX,accept,"Safety Compliance Management, Inc.",https://www.scm-safety.com,Paul Gantt,President and Founder,info@scm-safety.com,https://www.scm-safety.com/team/paul-gantt-csp-chst-cet/|https://www.scm-safety.com,95,"President and Founder; San Ramon, CA; info@scm-safety.com on site"',
        "# VALID REJECT EXAMPLE:",
        "# TX,reject,Example Safety Group,https://example-safety.com,Alex Example,Owner,alex@example-safety.com,https://example-safety.com/about,35,Role or state fit is uncertain; keep blocked for manual review",
        "# INVALID EXAMPLE - DO NOT RETURN ANYTHING LIKE THIS:",
        '# TX,accept,Example Safety Group,[https://example-safety.com/,"Alex](https://example-safety.com/%22,%22Alex) Example",Owner,[alex@example-safety.com](mailto:alex@example-safety.com),[https://example-safety.com/about|https://example-safety.com/contact](https://example-safety.com/about|https://example-safety.com/contact),95,Owner listed on site',
        "#",
        f"# RUN DATE: {run_date.isoformat()}",
        f"# BACKLOG TARGET: {backlog_target}",
        f"# RAW TARGET: {raw_target}",
        f"# SOURCES: {','.join(source_tokens) or 'none'}",
        f"# PACKET SIZE: {packet_size if packet_size > 0 else len(selected_rows)}",
        "#",
        "# GAP STATES:",
    ]
    if gap_rows:
        for row in gap_rows:
            lines.append(
                "# - "
                f"{row['state']}: backlog_current={int(row['backlog_current'] or 0)} "
                f"crm_total={int(row['crm_total'] or 0)} gap={int(row['gap'] or 0)}"
            )
    else:
        lines.append("# - none")

    if packet_number is not None and packet_count > 0:
        lines.extend(
            [
                "#",
                f"# PACKET: {packet_number:03d}/{packet_count:03d}",
                f"# PACKET ROWS: {len(selected_rows)}",
            ]
        )
        if reviewed_filename:
            lines.append(f"# REVIEWED IMPORT FILENAME: {reviewed_filename}")
        if suggested_batch_id:
            lines.append(f"# SUGGESTED_BATCH_ID: {suggested_batch_id}")
        if reviewed_drop_dir is not None:
            lines.append(f"# DROP REVIEWED CSV IN: {reviewed_drop_dir}")

    lines.extend(
        [
            "#",
            "# SEED CANDIDATES CSV:",
            _csv_block(fieldnames=SEED_COLUMNS, rows=_seed_rows(selected_rows)),
            "#",
            "# RETURN CSV NOW:",
            ",".join(REVIEW_COLUMNS),
        ]
    )
    return "\n".join(lines).rstrip() + "\n"


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="Emit a nightly AI-assist review dump from existing autogrow cache rows.")
    ap.add_argument("--for-date", default="", help="Optional YYYY-MM-DD date override.")
    ap.add_argument("--states", nargs="+", default=[], help="Optional explicit state scope (comma-separated or list form).")
    ap.add_argument("--raw-target", type=int, default=0, help="Optional raw seed target override.")
    ap.add_argument("--packet-size", type=int, default=0, help="Optional per-packet review size override.")
    ap.add_argument("--print-config", action="store_true", help="Print resolved config and exit.")
    ap.add_argument("--dry-run", action="store_true", help="Do not write output.")
    ap.add_argument("--output-dir", default="", help="Optional dump output directory override.")
    ap.add_argument("--output", default="", help="Optional full dump path override.")
    return ap


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        run_date = _parse_date(args.for_date)
        run_started_at = _current_run_started_at()
        state_scope = _resolve_state_scope(list(args.states or []))
        raw_target = int(args.raw_target or _int_env("PROSPECT_AI_ASSIST_REVIEW_RAW_TARGET", AI_ASSIST_DUMP_DEFAULT_RAW_TARGET))
        packet_size = int(args.packet_size or _int_env("PROSPECT_AI_ASSIST_REVIEW_PACKET_SIZE", AI_ASSIST_DUMP_DEFAULT_PACKET_SIZE))
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
    source_tokens, source_warning_configured = _resolve_source_tokens()
    run_token = _batch_run_token(run_started_at)
    out_dir, out_path = _resolve_output_path(
        output=str(args.output or ""),
        output_dir=str(args.output_dir or ""),
        for_date=run_date,
    )
    packet_dir = _packet_dir_for_output_path(out_path)
    manifest_path = _packet_manifest_path(packet_dir)
    data_dir_resolution = resolve_data_dir(REPO_ROOT)
    reviewed_drop_dir = ai_assist_paths.prospect_import_dir(repo_root=REPO_ROOT)

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
        crm_domains = _normalized_root_domains(generation._existing_crm_domains(conn))
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
    packets = _chunk_rows(selected_rows, packet_size) if selected_rows else []
    prompt_text = _build_prompt_text(
        run_date=run_date,
        backlog_target=backlog_target,
        raw_target=raw_target,
        source_tokens=source_tokens,
        gap_rows=gap_rows,
        selected_rows=selected_rows,
        packet_count=len(packets),
        packet_size=packet_size,
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
    _emit("AI_ASSIST_DUMP_RUN_STARTED_AT", run_started_at.isoformat())
    _emit("AI_ASSIST_DUMP_RUN_TOKEN", run_token)
    _emit("AI_ASSIST_DUMP_STATES_SCOPE", ",".join(states))
    _emit("AI_ASSIST_DUMP_BACKLOG_TARGET", backlog_target)
    _emit("AI_ASSIST_DUMP_OUTPUT_DIR", str(out_dir))
    _emit("AI_ASSIST_DUMP_OUTPUT_PATH", str(out_path))
    _emit("AI_ASSIST_DUMP_PACKET_SIZE", packet_size)
    _emit("AI_ASSIST_DUMP_PACKET_COUNT", len(packets))
    _emit("AI_ASSIST_PACKET_DIR", str(packet_dir))
    _emit("AI_ASSIST_PACKET_MANIFEST_PATH", str(manifest_path))
    _emit("AI_ASSIST_DUMP_GAP_STATES", gap_states_csv or "none")
    _emit("AI_ASSIST_DUMP_GAP_TOTAL", gap_total)
    _emit("AI_ASSIST_DUMP_CANDIDATES_REQUESTED_TOTAL", raw_target)
    _emit("AI_ASSIST_DUMP_SOURCES", ",".join(source_tokens) or "none")
    _emit("AI_ASSIST_DUMP_RAW_TARGET", raw_target)
    _emit("AI_ASSIST_DUMP_CANDIDATES_TOTAL", len(candidates))
    _emit("AI_ASSIST_DUMP_ROWS_WRITTEN", len(selected_rows))
    _emit("AI_ASSIST_DUMP_SHORTFALL", shortfall)

    for row in gap_rows:
        state = str(row["state"] or "")
        _emit(f"AI_ASSIST_DUMP_STATE_{state}_BACKLOG_CURRENT", int(row["backlog_current"] or 0))
        _emit(f"AI_ASSIST_DUMP_STATE_{state}_CRM_TOTAL", int(row["crm_total"] or 0))
        _emit(f"AI_ASSIST_DUMP_STATE_{state}_GAP", int(row["gap"] or 0))

    if shortfall > 0:
        print(f"WARN_AI_ASSIST_DUMP_SHORTFALL=1 requested={raw_target} available={len(candidates)} shortfall={shortfall}")
    if source_warning_configured:
        print(f"WARN_AI_ASSIST_DUMP_NO_ELIGIBLE_SOURCES=1 configured={source_warning_configured}")

    if args.print_config:
        return 0

    if enabled != 1:
        _emit("AI_ASSIST_DUMP_SKIPPED", "1 reason=disabled")
        return 0

    if not gap_rows:
        _emit("AI_ASSIST_DUMP_SKIPPED", "1 reason=no_gap")
        return 0

    if prompt_text:
        print(prompt_text, end="")

    if args.dry_run:
        return 0

    _atomic_write_text(out_path, prompt_text)
    _reset_output_dir(packet_dir)
    manifest_packets: list[dict[str, Any]] = []
    for packet_number, packet_rows in enumerate(packets, start=1):
        seed_csv_path = (packet_dir / _packet_seed_filename(packet_number)).resolve(strict=False)
        prompt_packet_path = (packet_dir / _packet_prompt_filename(packet_number)).resolve(strict=False)
        reviewed_filename = _packet_review_filename(run_date, packet_number)
        suggested_batch_id = _packet_batch_id(run_date, packet_number)
        _atomic_write_text(seed_csv_path, _csv_block(fieldnames=SEED_COLUMNS, rows=_seed_rows(packet_rows)).rstrip() + "\n")
        _atomic_write_text(
            prompt_packet_path,
            _build_prompt_text(
                run_date=run_date,
                backlog_target=backlog_target,
                raw_target=raw_target,
                source_tokens=source_tokens,
                gap_rows=gap_rows,
                selected_rows=packet_rows,
                packet_number=packet_number,
                packet_count=len(packets),
                packet_size=packet_size,
                reviewed_filename=reviewed_filename,
                suggested_batch_id=suggested_batch_id,
                reviewed_drop_dir=reviewed_drop_dir,
            ),
        )
        manifest_packets.append(
            {
                "packet_number": packet_number,
                "row_count": len(packet_rows),
                "seed_csv_path": str(seed_csv_path),
                "review_prompt_path": str(prompt_packet_path),
                "reviewed_import_filename": reviewed_filename,
                "reviewed_import_path": str((reviewed_drop_dir / reviewed_filename).resolve(strict=False)),
                "suggested_batch_id": suggested_batch_id,
            }
        )
    manifest_payload = {
        "schema_version": AI_ASSIST_PACKET_MANIFEST_SCHEMA,
        "run_date": run_date.isoformat(),
        "run_started_at": run_started_at.isoformat(),
        "run_token": run_token,
        "output_path": str(out_path),
        "packet_dir": str(packet_dir),
        "packet_size": packet_size,
        "packet_count": len(packets),
        "raw_target": raw_target,
        "selected_row_count": len(selected_rows),
        "candidate_count": len(candidates),
        "gap_total": gap_total,
        "states_scope": states,
        "sources": source_tokens,
        "reviewed_drop_dir": str(reviewed_drop_dir),
        "packets": manifest_packets,
    }
    _atomic_write_text(manifest_path, json.dumps(manifest_payload, indent=2) + "\n")
    _emit("AI_ASSIST_DUMP_WRITTEN", 1)
    _emit("AI_ASSIST_DUMP_OUTPUT_PATH", str(out_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
