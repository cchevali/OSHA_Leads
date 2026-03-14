#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sqlite3
import sys
import tempfile
from datetime import date, datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from outreach import crm_store
from outreach import run_prospect_generation as generation
from runtime_data_dir import resolve_data_dir

ERR_AI_ASSIST_DUMP_CONFIG = "ERR_AI_ASSIST_DUMP_CONFIG"
AI_ASSIST_DUMP_DEFAULT_MAX_ROWS_PER_STATE = 40
AI_ASSIST_DUMP_DEFAULT_BACKLOG_TARGET = 60
AI_ASSIST_DUMP_DEFAULT_ENABLED = "1"


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
    env_states = generation._parse_states(os.getenv("OUTREACH_STATES", ""))
    if not list(raw_states or []):
        return generation._resolve_state_scope("", env_states)
    return generation._resolve_state_scope(",".join(_parse_states_arg(raw_states)), env_states)


def _resolve_output_path(*, output: str, output_dir: str, for_date: date) -> tuple[Path, Path]:
    filename = f"prospect_ai_assist_review_{for_date.strftime('%Y%m%d')}.txt"
    if str(output or "").strip():
        out_path = Path(str(output).strip()).expanduser().resolve(strict=False)
        return out_path.parent.resolve(strict=False), out_path.resolve(strict=False)
    data_dir = resolve_data_dir(REPO_ROOT).effective_path
    if str(output_dir or "").strip():
        out_dir = Path(str(output_dir).strip()).expanduser().resolve(strict=False)
    else:
        out_dir = (data_dir / "audits" / "ai_assist").resolve(strict=False)
    return out_dir, (out_dir / filename).resolve(strict=False)


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


def _render_prompt(*, for_date: date, backlog_target: int, max_rows_per_state: int, gap_rows: list[dict[str, int | str]]) -> str:
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
        "# VALID ACCEPT EXAMPLE:",
        '# TX,accept,"Safety Compliance Management, Inc.",https://www.scm-safety.com,Paul Gantt,President and Founder,info@scm-safety.com,https://www.scm-safety.com/team/paul-gantt-csp-chst-cet/|https://www.scm-safety.com,95,"President and Founder; San Ramon, CA; info@scm-safety.com on site"',
        "# VALID REJECT EXAMPLE:",
        "# TX,reject,Example Safety Group,https://example-safety.com,Alex Example,Owner,alex@example-safety.com,https://example-safety.com/about,35,Role or state fit is uncertain; keep blocked for manual review",
        "# INVALID EXAMPLE - DO NOT RETURN ANYTHING LIKE THIS:",
        '# TX,accept,Example Safety Group,[https://example-safety.com/,"Alex](https://example-safety.com/%22,%22Alex) Example",Owner,[alex@example-safety.com](mailto:alex@example-safety.com),[https://example-safety.com/about|https://example-safety.com/contact](https://example-safety.com/about|https://example-safety.com/contact),95,Owner listed on site',
        "#",
        f"# RUN DATE: {for_date.isoformat()}",
        f"# BACKLOG TARGET: {backlog_target}",
        f"# MAX ROWS PER STATE: {max_rows_per_state}",
        "#",
        "# GAP STATES:",
    ]
    for row in gap_rows:
        state = str(row["state"] or "")
        backlog_current = int(row["backlog_current"] or 0)
        crm_total = int(row["crm_total"] or 0)
        gap = int(row["gap"] or 0)
        request_rows = min(int(max_rows_per_state), gap)
        lines.append(
            f"# - {state}: backlog_current={backlog_current} crm_total={crm_total} gap={gap} requested_rows={request_rows}"
        )
    lines.extend(
        [
            "#",
            "# RETURN CSV NOW:",
            "state,decision,firm,website,contact_name,title,email,source_urls,confidence,evidence_snippet",
        ]
    )
    return "\n".join(lines).strip() + "\n"


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="Dump a manual AI-assist discovery augmentation prompt when backlog gaps remain.")
    ap.add_argument("--for-date", default="", help="Optional YYYY-MM-DD date override.")
    ap.add_argument("--states", nargs="+", default=[], help="Optional explicit state scope (comma-separated or list form).")
    ap.add_argument("--print-config", action="store_true", help="Print resolved config and exit.")
    ap.add_argument("--dry-run", action="store_true", help="Do not write output.")
    ap.add_argument("--output-dir", default="", help="Optional output directory override.")
    ap.add_argument("--output", default="", help="Optional full output path override.")
    return ap


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        run_date = _parse_date(args.for_date)
        state_scope = _resolve_state_scope(list(args.states or []))
    except Exception as exc:
        print(f"{ERR_AI_ASSIST_DUMP_CONFIG} detail={exc}", file=sys.stderr)
        return 2

    states = generation._states_for_selection(state_scope)
    enabled = _bool_env("PROSPECT_AI_ASSIST_REVIEW_ENABLED", AI_ASSIST_DUMP_DEFAULT_ENABLED)
    backlog_target = _int_env("PROSPECT_AUTOGROW_BACKLOG_TARGET", AI_ASSIST_DUMP_DEFAULT_BACKLOG_TARGET)
    max_rows_per_state = _int_env("PROSPECT_AI_ASSIST_MAX_ROWS_PER_STATE", AI_ASSIST_DUMP_DEFAULT_MAX_ROWS_PER_STATE)
    out_dir, out_path = _resolve_output_path(output=str(args.output or ""), output_dir=str(args.output_dir or ""), for_date=run_date)
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
    finally:
        if conn is not None:
            conn.close()

    requested_total = sum(min(max_rows_per_state, int(row["gap"] or 0)) for row in gap_rows)
    gap_total = sum(int(row["gap"] or 0) for row in gap_rows)
    gap_states_csv = ",".join(str(row["state"] or "") for row in gap_rows)

    if data_dir_resolution.warning_token:
        print(data_dir_resolution.warning_token)
    _emit("AI_ASSIST_DUMP_ENABLED", enabled)
    _emit("AI_ASSIST_DUMP_DATA_DIR", str(data_dir_resolution.effective_path))
    _emit("AI_ASSIST_DUMP_DATA_DIR_SOURCE", str(data_dir_resolution.source or "default"))
    _emit("AI_ASSIST_DUMP_FOR_DATE", run_date.isoformat())
    _emit("AI_ASSIST_DUMP_STATES_SCOPE", ",".join(states))
    _emit("AI_ASSIST_DUMP_BACKLOG_TARGET", backlog_target)
    _emit("AI_ASSIST_DUMP_MAX_ROWS_PER_STATE", max_rows_per_state)
    _emit("AI_ASSIST_DUMP_OUTPUT_DIR", str(out_dir))
    _emit("AI_ASSIST_DUMP_OUTPUT_PATH", str(out_path))
    _emit("AI_ASSIST_DUMP_GAP_STATES", gap_states_csv or "none")
    _emit("AI_ASSIST_DUMP_GAP_TOTAL", gap_total)
    _emit("AI_ASSIST_DUMP_CANDIDATES_REQUESTED_TOTAL", requested_total)

    for row in gap_rows:
        state = str(row["state"] or "")
        _emit(f"AI_ASSIST_DUMP_STATE_{state}_BACKLOG_CURRENT", int(row["backlog_current"] or 0))
        _emit(f"AI_ASSIST_DUMP_STATE_{state}_CRM_TOTAL", int(row["crm_total"] or 0))
        _emit(f"AI_ASSIST_DUMP_STATE_{state}_GAP", int(row["gap"] or 0))
        _emit(f"AI_ASSIST_DUMP_STATE_{state}_ROWS_REQUESTED", min(max_rows_per_state, int(row["gap"] or 0)))

    if args.print_config:
        return 0

    if enabled != 1:
        _emit("AI_ASSIST_DUMP_SKIPPED", "1 reason=disabled")
        return 0

    if not gap_rows:
        _emit("AI_ASSIST_DUMP_SKIPPED", "1 reason=no_gap")
        return 0

    output_text = _render_prompt(
        for_date=run_date,
        backlog_target=backlog_target,
        max_rows_per_state=max_rows_per_state,
        gap_rows=gap_rows,
    )
    print(output_text, end="")

    if not args.dry_run:
        _atomic_write_text(out_path, output_text)
        _emit("AI_ASSIST_DUMP_WRITTEN", 1)
        _emit("AI_ASSIST_DUMP_OUTPUT_PATH", str(out_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
