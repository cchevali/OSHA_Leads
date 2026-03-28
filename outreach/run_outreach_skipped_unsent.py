from __future__ import annotations

import argparse
import csv
import json
import os
import sqlite3
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path

from outreach import run_outreach_auto as roa


REPO_ROOT = Path(__file__).resolve().parents[1]

PASS_SKIPPED_EXTRA_PRINT_CONFIG = "PASS_SKIPPED_EXTRA_PRINT_CONFIG"
PASS_SKIPPED_EXTRA_DRY_RUN = "PASS_SKIPPED_EXTRA_DRY_RUN"
PASS_SKIPPED_EXTRA_EXPORT = "PASS_SKIPPED_EXTRA_EXPORT"
PASS_SKIPPED_EXTRA_SUMMARY = "PASS_SKIPPED_EXTRA_SUMMARY"
ERR_SKIPPED_EXTRA_CRM_REQUIRED = "ERR_SKIPPED_EXTRA_CRM_REQUIRED"
ERR_SKIPPED_EXTRA_MANIFEST_REQUIRED = "ERR_SKIPPED_EXTRA_MANIFEST_REQUIRED"
ERR_SKIPPED_EXTRA_STATES_REQUIRED = "ERR_SKIPPED_EXTRA_STATES_REQUIRED"
ERR_SKIPPED_EXTRA_LIMIT_REQUIRED = "ERR_SKIPPED_EXTRA_LIMIT_REQUIRED"
ERR_SKIPPED_EXTRA_MANIFEST_UNREADABLE = "ERR_SKIPPED_EXTRA_MANIFEST_UNREADABLE"
ERR_SKIPPED_EXTRA_CONFIRM_REQUIRED = "ERR_SKIPPED_EXTRA_CONFIRM_REQUIRED"
ERR_SKIPPED_EXTRA_FOR_DATE_LIVE_SEND_BLOCKED = "ERR_SKIPPED_EXTRA_FOR_DATE_LIVE_SEND_BLOCKED"
ERR_SKIPPED_EXTRA_ONE_CLICK_REQUIRED = "ERR_SKIPPED_EXTRA_ONE_CLICK_REQUIRED"
ERR_SKIPPED_EXTRA_SUMMARY_SEND = "ERR_SKIPPED_EXTRA_SUMMARY_SEND"
OUTREACH_SKIPPED_EXTRA_SKIP_NON_WEEKDAY = "OUTREACH_SKIPPED_EXTRA_SKIP_NON_WEEKDAY"
OUTREACH_SKIPPED_EXTRA_SKIP_ALREADY_SENT_TODAY = "OUTREACH_SKIPPED_EXTRA_SKIP_ALREADY_SENT_TODAY"
OUTREACH_SKIPPED_EXTRA_SKIP_NO_SIGNALS = "OUTREACH_SKIPPED_EXTRA_SKIP_NO_SIGNALS"
OUTREACH_SKIPPED_EXTRA_DUPLICATE_GUARD_DROPPED = "OUTREACH_SKIPPED_EXTRA_DUPLICATE_GUARD_DROPPED"

EXTRA_BATCH_LABEL = "SKIPPED_UNSENT_EXTRA"
EXTRA_APPROVED_DROPPED_REASONS = {"role_inbox_email", "not_default_send_eligible"}


@dataclass
class ExtraSelection:
    selected: list[dict]
    manifest_rows: list[dict]
    manifest_row_total: int
    approved_manifest_total: int
    pool_total: int
    skip_counts: Counter
    selected_by_state: Counter
    selected_by_reason: Counter
    no_signal_states: list[str]
    state_signal_counts: dict[str, int]
    state_signal_contexts: dict[str, dict[str, object]]


def _safe_text(value: object) -> str:
    return str(value or "").replace("\r", " ").replace("\n", " ").strip()


def _extra_batch_group(run_date: date) -> str:
    return f"{run_date.isoformat()}_{EXTRA_BATCH_LABEL}"


def _extra_batch_id(state: str, run_date: date) -> str:
    return f"{run_date.isoformat()}_{state}_{EXTRA_BATCH_LABEL}"


def _artifact_dir(batch_group: str) -> Path:
    safe = roa._safe_batch_name(batch_group)
    return (REPO_ROOT / "out" / "outreach" / safe).resolve()


def _artifact_paths(batch_group: str) -> tuple[Path, Path, Path]:
    out_dir = _artifact_dir(batch_group)
    safe = roa._safe_batch_name(batch_group)
    return (
        out_dir / f"outbox_{safe}.csv",
        out_dir / f"manifest_{safe}.csv",
        out_dir / "plan_diagnostics.json",
    )


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fieldnames})


def _format_count_map(counts: dict[str, int], *, sort_keys: bool = True) -> str:
    if not counts:
        return "none"
    items = list(counts.items())
    if sort_keys:
        items.sort(key=lambda item: str(item[0]))
    return ",".join([f"{key}:{int(value)}" for key, value in items])


def _id_preview(values: list[str], limit: int = 50) -> str:
    cleaned = [str(v).strip() for v in values if str(v or "").strip()]
    if not cleaned:
        return "(none)"
    if len(cleaned) <= limit:
        return ",".join(cleaned)
    head = ",".join(cleaned[:limit])
    return f"{head},...(+{len(cleaned) - limit} more)"


def _candidate_sort_key(candidate: dict, *, state_order: dict[str, int] | None = None) -> tuple[int, tuple, str]:
    state = _safe_text(candidate.get("state") or "")
    order = dict(state_order or {})
    return (
        int(order.get(state, len(order))),
        tuple(candidate.get("rank_tuple") or ()),
        _safe_text(candidate.get("prospect_id") or ""),
    )


def _manifest_sort_key(row: dict) -> tuple[str, str, str, str]:
    status = _safe_text(row.get("status") or "")
    return (
        "0" if status == "selected" else "1",
        _safe_text(row.get("state") or ""),
        _safe_text(row.get("prospect_id") or ""),
        _safe_text(row.get("reason") or ""),
    )


def _candidate_manifest_row(candidate: dict, *, status: str, reason: str, original_skip_reason: str) -> dict:
    row = dict(roa._candidate_csv_row(candidate))
    row.update(
        {
            "status": status,
            "reason": reason,
            "original_skip_reason": original_skip_reason,
            "batch": _safe_text(candidate.get("batch") or ""),
            "state": _safe_text(candidate.get("state") or ""),
        }
    )
    return row


def _manifest_source_row(
    source_row: dict,
    *,
    run_date: date,
    status: str,
    reason: str,
    original_skip_reason: str,
    state_override: str = "",
) -> dict:
    email = roa._norm_email(str(source_row.get("email") or ""))
    state = roa._normalize_us_state(state_override or str(source_row.get("state") or ""))
    return {
        "prospect_id": _safe_text(source_row.get("prospect_id") or ""),
        "email": email,
        "domain": roa._norm_domain(email),
        "segment": _safe_text(source_row.get("segment") or ""),
        "role_or_title": _safe_text(source_row.get("role_or_title") or source_row.get("title") or ""),
        "state_pref": _safe_text(source_row.get("state_pref") or state),
        "rank_reason": _safe_text(source_row.get("rank_reason") or ""),
        "rank_tuple": _safe_text(source_row.get("rank_tuple") or ""),
        "status": status,
        "reason": reason,
        "original_skip_reason": original_skip_reason,
        "batch": _extra_batch_id(state, run_date) if state else "",
        "state": state,
    }


def _load_manifest_rows(path: Path) -> list[dict]:
    with open(path, "r", newline="", encoding="utf-8-sig") as f:
        return [dict(row) for row in csv.DictReader(f)]


def _prospect_lookup_maps(conn: sqlite3.Connection) -> tuple[dict[str, sqlite3.Row], dict[str, sqlite3.Row]]:
    cols = roa._prospect_select_columns(conn)
    rows = conn.execute("SELECT " + ", ".join(cols) + " FROM prospects").fetchall()
    rows_by_prospect_id: dict[str, sqlite3.Row] = {}
    rows_by_email: dict[str, sqlite3.Row] = {}
    for row in rows:
        prospect_id = _safe_text(row["prospect_id"] if "prospect_id" in row.keys() else "")
        email = roa._norm_email(str(row["email"] or ""))
        if prospect_id and prospect_id not in rows_by_prospect_id:
            rows_by_prospect_id[prospect_id] = row
        if email and email not in rows_by_email:
            rows_by_email[email] = row
    return rows_by_prospect_id, rows_by_email


def _select_skipped_unsent_candidates(
    *,
    conn: sqlite3.Connection,
    manifest_path: Path,
    requested_states: list[str],
    limit: int,
    run_date: date,
    osha_db: str,
    suppressed_emails: set[str],
) -> ExtraSelection:
    manifest_input_rows = _load_manifest_rows(manifest_path)
    rows_by_prospect_id, rows_by_email = _prospect_lookup_maps(conn)
    sent_ids = roa._fetch_prior_sent_ids(conn)
    signal_window_days = roa._signal_window_days()
    state_order = {state: idx for idx, state in enumerate(requested_states)}
    selected: list[dict] = []
    manifest_rows: list[dict] = []
    skip_counts: Counter = Counter()
    selected_by_state: Counter = Counter()
    selected_by_reason: Counter = Counter()
    provisional_candidates: list[dict] = []
    state_signal_contexts: dict[str, dict[str, object]] = {}
    state_signal_counts: dict[str, int] = {}
    blocked_states: set[str] = set()
    seen_manifest_keys: set[str] = set()
    approved_manifest_total = 0

    for source_row in manifest_input_rows:
        status = _safe_text(source_row.get("status") or "").lower()
        original_skip_reason = _safe_text(source_row.get("reason") or "")
        if status != "dropped" or original_skip_reason not in EXTRA_APPROVED_DROPPED_REASONS:
            continue
        approved_manifest_total += 1

        manifest_state = roa._normalize_us_state(str(source_row.get("state") or ""))
        prospect_id = _safe_text(source_row.get("prospect_id") or "")
        email = roa._norm_email(str(source_row.get("email") or ""))

        if manifest_state not in state_order:
            skip_counts["state_scope_excluded"] += 1
            manifest_rows.append(
                _manifest_source_row(
                    source_row,
                    run_date=run_date,
                    status="dropped",
                    reason="state_scope_excluded",
                    original_skip_reason=original_skip_reason,
                    state_override=manifest_state,
                )
            )
            continue

        if not prospect_id and not email:
            skip_counts["manifest_identity_missing"] += 1
            manifest_rows.append(
                _manifest_source_row(
                    source_row,
                    run_date=run_date,
                    status="dropped",
                    reason="manifest_identity_missing",
                    original_skip_reason=original_skip_reason,
                    state_override=manifest_state,
                )
            )
            continue

        manifest_key = prospect_id or email
        if manifest_key in seen_manifest_keys:
            skip_counts["manifest_duplicate"] += 1
            manifest_rows.append(
                _manifest_source_row(
                    source_row,
                    run_date=run_date,
                    status="dropped",
                    reason="manifest_duplicate",
                    original_skip_reason=original_skip_reason,
                    state_override=manifest_state,
                )
            )
            continue
        seen_manifest_keys.add(manifest_key)

        row = rows_by_prospect_id.get(prospect_id) if prospect_id else None
        if row is None and email:
            row = rows_by_email.get(email)
        if row is None:
            skip_counts["crm_missing"] += 1
            manifest_rows.append(
                _manifest_source_row(
                    source_row,
                    run_date=run_date,
                    status="dropped",
                    reason="crm_missing",
                    original_skip_reason=original_skip_reason,
                    state_override=manifest_state,
                )
            )
            continue

        current_skip_reason = roa._skip_reason(
            row,
            suppressed_emails=suppressed_emails,
            sent_ids=sent_ids,
            allow_repeat=False,
            skip_role_inboxes=False,
            include_adjacent_contractors=True,
        )
        candidate = roa._candidate_from_row(row)
        state = roa._normalize_us_state(str(row["state"] or ""))
        candidate["state"] = state
        candidate["batch"] = _extra_batch_id(state, run_date) if state else ""
        candidate["original_skip_reason"] = original_skip_reason
        if current_skip_reason:
            drop_reason = "suppressed_compliance" if current_skip_reason == "suppressed" else current_skip_reason
            skip_counts[drop_reason] += 1
            manifest_rows.append(
                _candidate_manifest_row(
                    candidate,
                    status="dropped",
                    reason=drop_reason,
                    original_skip_reason=original_skip_reason,
                )
            )
            continue

        if state not in state_order:
            skip_counts["state_scope_excluded"] += 1
            manifest_rows.append(
                _candidate_manifest_row(
                    candidate,
                    status="dropped",
                    reason="state_scope_excluded",
                    original_skip_reason=original_skip_reason,
                )
            )
            continue

        provisional_candidates.append(candidate)

    pool_total = int(len(provisional_candidates))
    provisional_candidates.sort(key=lambda candidate: _candidate_sort_key(candidate, state_order=state_order))

    for state in sorted({str(candidate.get("state") or "") for candidate in provisional_candidates}, key=lambda item: state_order.get(item, len(state_order))):
        signal_ctx = roa._prepare_signal_content_with_triage(
            batch=_extra_batch_id(state, run_date),
            state=state,
            osha_db=osha_db,
            dry_run_suffix="manual_extra",
            run_date=run_date,
            signal_window_days=signal_window_days,
        )
        state_signal_contexts[state] = signal_ctx
        state_signal_counts[state] = int(signal_ctx.get("raw_signal_count") or 0)
        if roa._signal_guard_blocks_send(signal_ctx):
            blocked_states.add(state)

    for candidate in provisional_candidates:
        state = _safe_text(candidate.get("state") or "")
        signal_ctx = dict(state_signal_contexts.get(state) or {})
        if roa._signal_guard_blocks_send(signal_ctx):
            drop_reason = "signal_fetch_failed" if _safe_text(signal_ctx.get("signal_fetch_error_token") or "") else "no_signals"
            skip_counts[drop_reason] += 1
            manifest_rows.append(
                _candidate_manifest_row(
                    candidate,
                    status="dropped",
                    reason=drop_reason,
                    original_skip_reason=_safe_text(candidate.get("original_skip_reason") or ""),
                )
            )
            continue
        if len(selected) >= int(limit):
            skip_counts["limit_guard"] += 1
            manifest_rows.append(
                _candidate_manifest_row(
                    candidate,
                    status="dropped",
                    reason="limit_guard",
                    original_skip_reason=_safe_text(candidate.get("original_skip_reason") or ""),
                )
            )
            continue
        selected.append(candidate)
        selected_by_state[state] += 1
        selected_by_reason[_safe_text(candidate.get("original_skip_reason") or "")] += 1

    selected_manifest_rows = [
        _candidate_manifest_row(
            candidate,
            status="selected",
            reason="",
            original_skip_reason=_safe_text(candidate.get("original_skip_reason") or ""),
        )
        for candidate in selected
    ]
    manifest_rows = list(selected_manifest_rows) + sorted(manifest_rows, key=_manifest_sort_key)

    return ExtraSelection(
        selected=selected,
        manifest_rows=manifest_rows,
        manifest_row_total=int(len(manifest_input_rows)),
        approved_manifest_total=int(approved_manifest_total),
        pool_total=int(pool_total),
        skip_counts=skip_counts,
        selected_by_state=selected_by_state,
        selected_by_reason=selected_by_reason,
        no_signal_states=sorted(blocked_states),
        state_signal_counts={key: int(value) for key, value in sorted(state_signal_counts.items())},
        state_signal_contexts={key: dict(value) for key, value in state_signal_contexts.items()},
    )


def _write_dry_run_artifacts(batch_group: str, selection: ExtraSelection) -> tuple[Path, Path, Path]:
    outbox_path, manifest_path, diagnostics_path = _artifact_paths(batch_group)
    outbox_rows = []
    for candidate in selection.selected:
        row = dict(roa._candidate_csv_row(candidate))
        row.update(
            {
                "batch": _safe_text(candidate.get("batch") or ""),
                "state": _safe_text(candidate.get("state") or ""),
                "original_skip_reason": _safe_text(candidate.get("original_skip_reason") or ""),
            }
        )
        outbox_rows.append(row)

    _write_csv(
        outbox_path,
        [
            "prospect_id",
            "email",
            "domain",
            "segment",
            "role_or_title",
            "state_pref",
            "rank_reason",
            "rank_tuple",
            "batch",
            "state",
            "original_skip_reason",
        ],
        outbox_rows,
    )

    manifest_out = []
    ts_utc = datetime.now(timezone.utc).isoformat()
    for row in selection.manifest_rows:
        payload = dict(row)
        payload["ts_utc"] = ts_utc
        manifest_out.append(payload)

    _write_csv(
        manifest_path,
        [
            "ts_utc",
            "batch",
            "state",
            "prospect_id",
            "email",
            "domain",
            "segment",
            "role_or_title",
            "state_pref",
            "status",
            "reason",
            "original_skip_reason",
            "rank_reason",
            "rank_tuple",
        ],
        manifest_out,
    )

    diagnostics = {
        "batch_group": batch_group,
        "manifest_row_total": int(selection.manifest_row_total),
        "approved_manifest_total": int(selection.approved_manifest_total),
        "pool_total": int(selection.pool_total),
        "selected_count": int(len(selection.selected)),
        "selected_by_state": {key: int(value) for key, value in sorted(selection.selected_by_state.items())},
        "selected_by_reason": {key: int(value) for key, value in sorted(selection.selected_by_reason.items())},
        "skip_counts": {key: int(value) for key, value in sorted(selection.skip_counts.items())},
        "no_signal_states": list(selection.no_signal_states),
        "state_signal_counts": {key: int(value) for key, value in sorted(selection.state_signal_counts.items())},
        "selected_prospect_ids": [str(candidate.get("prospect_id") or "") for candidate in selection.selected],
    }
    diagnostics_path.parent.mkdir(parents=True, exist_ok=True)
    with open(diagnostics_path, "w", encoding="utf-8") as f:
        json.dump(diagnostics, f, indent=2, sort_keys=True)
        f.write("\n")
    return outbox_path, manifest_path, diagnostics_path


def _send_summary_email(
    *,
    summary_to: str,
    run_date: date,
    selected_ids: list[str],
    selection: ExtraSelection,
    contacted_count: int,
    failed_count: int,
    contacted_by_state: Counter,
    failed_by_state: Counter,
    configured_states: list[str],
) -> tuple[bool, str]:
    subject = (
        f"[AUTO EXTRA] Outreach skipped-unsent {run_date.isoformat()} "
        f"contacted={contacted_count} skipped={int(sum(selection.skip_counts.values()))} failed={failed_count}"
    )
    text_body = (
        "Outreach skipped-unsent extra run summary\n"
        f"- run_date: {run_date.isoformat()}\n"
        f"- configured_states: {','.join(configured_states) if configured_states else '(none)'}\n"
        f"- manifest_row_total: {int(selection.manifest_row_total)}\n"
        f"- approved_manifest_total: {int(selection.approved_manifest_total)}\n"
        f"- skipped_unsent_pool_total: {int(selection.pool_total)}\n"
        f"- selected_count: {int(len(selection.selected))}\n"
        f"- selected_by_state: {_format_count_map(selection.selected_by_state)}\n"
        f"- selected_by_reason: {_format_count_map(selection.selected_by_reason)}\n"
        f"- skipped_by_reason: {_format_count_map(selection.skip_counts)}\n"
        f"- no_signal_states: {','.join(selection.no_signal_states) if selection.no_signal_states else '(none)'}\n"
        f"- contacted_count: {contacted_count}\n"
        f"- contacted_by_state: {_format_count_map(contacted_by_state)}\n"
        f"- failed_count: {failed_count}\n"
        f"- failed_by_state: {_format_count_map(failed_by_state)}\n"
        f"- contacted_prospect_ids: {_id_preview(selected_ids)}\n"
    )
    html_body = (
        "<div style=\"font-family: system-ui, -apple-system, 'Segoe UI', Roboto, Arial, sans-serif;\">"
        "<h3>Outreach Skipped-Unsent Extra Run Summary</h3>"
        f"<p><strong>run_date:</strong> {run_date.isoformat()}<br>"
        f"<strong>configured_states:</strong> {','.join(configured_states) if configured_states else '(none)'}<br>"
        f"<strong>manifest_row_total:</strong> {int(selection.manifest_row_total)}<br>"
        f"<strong>approved_manifest_total:</strong> {int(selection.approved_manifest_total)}<br>"
        f"<strong>skipped_unsent_pool_total:</strong> {int(selection.pool_total)}<br>"
        f"<strong>selected_count:</strong> {int(len(selection.selected))}<br>"
        f"<strong>selected_by_state:</strong> {_format_count_map(selection.selected_by_state)}<br>"
        f"<strong>selected_by_reason:</strong> {_format_count_map(selection.selected_by_reason)}<br>"
        f"<strong>skipped_by_reason:</strong> {_format_count_map(selection.skip_counts)}<br>"
        f"<strong>no_signal_states:</strong> {','.join(selection.no_signal_states) if selection.no_signal_states else '(none)'}<br>"
        f"<strong>contacted_count:</strong> {contacted_count}<br>"
        f"<strong>contacted_by_state:</strong> {_format_count_map(contacted_by_state)}<br>"
        f"<strong>failed_count:</strong> {failed_count}<br>"
        f"<strong>failed_by_state:</strong> {_format_count_map(failed_by_state)}<br>"
        f"<strong>contacted_prospect_ids:</strong> {_id_preview(selected_ids)}</p>"
        "</div>"
    )
    return roa._send_summary_email(summary_to, subject, text_body, html_body)


def _emit_blocked_signal_tokens(selection: ExtraSelection) -> None:
    for state in selection.no_signal_states:
        signal_ctx = dict(selection.state_signal_contexts.get(state) or {})
        roa._emit_signal_guard_tokens(
            token=OUTREACH_SKIPPED_EXTRA_SKIP_NO_SIGNALS,
            state=state,
            signal_ctx=signal_ctx,
        )


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Send a manually scoped skipped-unsent outreach batch from a dropped-manifest while preserving suppression."
    )
    ap.add_argument("--dry-run", action="store_true", help="Select and print actions only. No DB writes, no email.")
    ap.add_argument("--print-config", action="store_true", help="Print resolved config paths and target counts, then exit.")
    ap.add_argument("--for-date", default="", help="Override run date (YYYY-MM-DD) for print-config and dry-run.")
    ap.add_argument("--manifest", default="", help="Path to a prior outreach manifest CSV containing dropped rows to recover.")
    ap.add_argument("--states", default="", help="Comma-separated state scope for this manual run.")
    ap.add_argument("--limit", type=int, default=0, help="Maximum total contacts to send from the supplied manifest.")
    ap.add_argument(
        "--allow-weekend-send",
        action="store_true",
        help="Emergency/manual override: allow live outreach sends on Sat/Sun.",
    )
    ap.add_argument(
        "--confirm-live-send",
        action="store_true",
        help="Manual live-send confirmation flag required for live sends.",
    )
    ap.add_argument("--to", default="", help="Optional summary recipient override; must equal OSHA_SMOKE_TO.")
    args = ap.parse_args()

    ok_date, run_date, date_msg = roa._parse_for_date(str(args.for_date or ""))
    if not ok_date:
        print(date_msg, file=sys.stderr)
        return 2

    manifest_raw = str(args.manifest or "").strip()
    if not manifest_raw:
        print(ERR_SKIPPED_EXTRA_MANIFEST_REQUIRED, file=sys.stderr)
        return 2
    manifest_path = Path(manifest_raw).expanduser()
    if not manifest_path.is_absolute():
        manifest_path = (REPO_ROOT / manifest_path).resolve()
    else:
        manifest_path = manifest_path.resolve()
    if not manifest_path.exists() or not manifest_path.is_file():
        print(f"{ERR_SKIPPED_EXTRA_MANIFEST_UNREADABLE} path={manifest_path}", file=sys.stderr)
        return 2

    requested_states = roa._parse_states(str(args.states or ""))
    if not requested_states:
        print(ERR_SKIPPED_EXTRA_STATES_REQUIRED, file=sys.stderr)
        return 2
    if int(args.limit or 0) <= 0:
        print(ERR_SKIPPED_EXTRA_LIMIT_REQUIRED, file=sys.stderr)
        return 2

    is_live_send = not bool(args.dry_run or args.print_config)
    if is_live_send and not bool(args.confirm_live_send):
        print(ERR_SKIPPED_EXTRA_CONFIRM_REQUIRED, file=sys.stderr)
        return 2

    local_now = roa._outreach_local_now()
    today_local = local_now["date"]
    configured_states = roa._parse_states(os.getenv("OUTREACH_STATES", roa.us_state.DEFAULT_OUTREACH_STATE_CSV))
    signal_window_days = roa._signal_window_days()
    crm_db = roa._crm_db_path()
    suppression_csv = roa._suppression_csv_path()
    export_ledger = roa._export_ledger_path()
    osha_db_resolution = roa.resolve_osha_db_path(REPO_ROOT)
    osha_db = str(osha_db_resolution.effective_path)
    runtime_mode = str(os.getenv("MFO_RUNTIME_MODE") or "manual").strip().lower() or "manual"
    runtime_ctx = roa.runtime_context_dict(mode=runtime_mode, intent="send", dry_run=bool(args.dry_run or args.print_config))
    batch_group = _extra_batch_group(run_date)

    if is_live_send:
        runtime_preflight = roa.run_runtime_preflight(
            mode=runtime_mode,
            intent="send",
            dry_run=False,
            task_log_root=str(os.getenv("TASK_LOG_ROOT") or ""),
            run_summary_root=str(os.getenv("RUN_SUMMARY_ROOT") or ""),
            require_confirm_live_send=True,
            confirm_live_send=True,
        )
        for line in roa.render_runtime_lines(runtime_preflight):
            print(line)
        if not runtime_preflight.ok:
            return 2

    if is_live_send and roa.OUTREACH_WEEKDAYS_ONLY and (not bool(args.allow_weekend_send)) and bool(local_now["is_weekend"]):
        print(
            f"{OUTREACH_SKIPPED_EXTRA_SKIP_NON_WEEKDAY} local_date={local_now['date_text']} "
            f"weekday={local_now['weekday_name']} gate=outreach_weekdays_only"
        )
        return 0
    if is_live_send and run_date != today_local:
        print(
            f"{ERR_SKIPPED_EXTRA_FOR_DATE_LIVE_SEND_BLOCKED} for_date={run_date.isoformat()} today={today_local.isoformat()}",
            file=sys.stderr,
        )
        return 2

    if is_live_send:
        ok_to, summary_to, msg = roa._resolve_summary_recipient(args.to)
        if not ok_to:
            print(msg, file=sys.stderr)
            return 2
    else:
        ok_to, summary_to, _msg = roa._resolve_summary_recipient(args.to)
        summary_to = summary_to if ok_to else "(missing OSHA_SMOKE_TO)"

    live_send_lock = None
    conn: sqlite3.Connection | None = None
    try:
        if is_live_send:
            live_send_lock = roa.acquire_runtime_lock(
                f"outreach_skipped_unsent_extra_{run_date.isoformat()}",
                repo_root=REPO_ROOT,
                metadata={"run_date": run_date.isoformat(), "task": "outreach_skipped_unsent_extra"},
            )
            if not live_send_lock.acquired:
                holder_meta = dict(live_send_lock.metadata or {})
                holder_pid = _safe_text(holder_meta.get("pid") or "") or "unknown"
                holder_host = _safe_text(holder_meta.get("hostname") or "") or "unknown"
                holder_started = _safe_text(holder_meta.get("acquired_at_utc") or "") or "unknown"
                print(
                    f"OUTREACH_SKIP_CONCURRENT_RUN=1 date={run_date.isoformat()} "
                    f"lock_path={live_send_lock.path} holder_pid={holder_pid} holder_host={holder_host} "
                    f"holder_started_at={holder_started} guard=LOCK"
                )
                return 0

        pending_import_rc = roa._run_ai_assist_pending_imports(dry_run=bool(args.dry_run or args.print_config))
        if pending_import_rc != 0:
            return pending_import_rc

        if not crm_db.exists():
            print(f"{ERR_SKIPPED_EXTRA_CRM_REQUIRED} crm_missing path={crm_db}", file=sys.stderr)
            return 2
        try:
            conn = roa._connect_existing_crm(crm_db)
        except Exception as exc:
            print(f"{ERR_SKIPPED_EXTRA_CRM_REQUIRED} crm_open_failed path={crm_db} err={exc}", file=sys.stderr)
            return 2
        if not roa._require_schema(conn):
            print(f"{ERR_SKIPPED_EXTRA_CRM_REQUIRED} schema_missing path={crm_db}", file=sys.stderr)
            return 2

        try:
            suppressed_emails = roa._load_suppression_emails(conn)
        except ValueError as exc:
            print(str(exc), file=sys.stderr)
            return 3

        if is_live_send:
            existing_batches = roa._sent_batches_for_day(conn, run_date)
            if existing_batches:
                existing_batches_text = ",".join(existing_batches) if existing_batches else "none"
                print(
                    f"{OUTREACH_SKIPPED_EXTRA_SKIP_ALREADY_SENT_TODAY}=1 date={run_date.isoformat()} "
                    f"existing_batches={existing_batches_text} guard=ON"
                )
                return 0

        try:
            selection = _select_skipped_unsent_candidates(
                conn=conn,
                manifest_path=manifest_path,
                requested_states=requested_states,
                limit=int(args.limit),
                run_date=run_date,
                osha_db=osha_db,
                suppressed_emails=suppressed_emails,
            )
        except Exception as exc:
            print(f"{ERR_SKIPPED_EXTRA_MANIFEST_UNREADABLE} path={manifest_path} err={type(exc).__name__}", file=sys.stderr)
            return 2
        selected_ids = [str(candidate.get("prospect_id") or "") for candidate in selection.selected]
        skipped_count = int(sum(selection.skip_counts.values()))

        if args.print_config:
            print(f"{PASS_SKIPPED_EXTRA_PRINT_CONFIG} crm_db={crm_db.resolve()}")
            print(f"{PASS_SKIPPED_EXTRA_PRINT_CONFIG} suppression_csv={suppression_csv.resolve()}")
            print(f"{PASS_SKIPPED_EXTRA_PRINT_CONFIG} export_ledger={export_ledger.resolve()}")
            print(f"{PASS_SKIPPED_EXTRA_PRINT_CONFIG} outreach_signal_db={Path(osha_db).resolve()}")
            print(f"{PASS_SKIPPED_EXTRA_PRINT_CONFIG} outreach_signal_db_source={osha_db_resolution.source}")
            if osha_db_resolution.warning_token:
                print(osha_db_resolution.warning_token)
            print(f"{PASS_SKIPPED_EXTRA_PRINT_CONFIG} batch_group={batch_group}")
            print(f"{PASS_SKIPPED_EXTRA_PRINT_CONFIG} run_date={run_date.isoformat()}")
            print(f"{PASS_SKIPPED_EXTRA_PRINT_CONFIG} manifest_path={manifest_path}")
            print(f"{PASS_SKIPPED_EXTRA_PRINT_CONFIG} requested_states={','.join(requested_states)}")
            print(f"{PASS_SKIPPED_EXTRA_PRINT_CONFIG} requested_limit={int(args.limit)}")
            print(f"outreach_signal_window_days={signal_window_days}")
            print(f"outreach_effective_timezone={local_now['timezone']}")
            print(f"outreach_effective_local_date={local_now['date_text']}")
            print(f"outreach_effective_weekday={local_now['weekday_name']}")
            print(f"outreach_allow_weekend_send={'YES' if args.allow_weekend_send else 'NO'}")
            print(f"outreach_states_config={','.join(configured_states) if configured_states else '(none)'}")
            print(f"manifest_row_total={int(selection.manifest_row_total)}")
            print(f"approved_manifest_total={int(selection.approved_manifest_total)}")
            print(f"skipped_unsent_pool_total={int(selection.pool_total)}")
            print(f"sendable_extra_count={int(len(selection.selected))}")
            print(f"selected_by_state={_format_count_map(selection.selected_by_state)}")
            print(f"selected_by_reason={_format_count_map(selection.selected_by_reason)}")
            print(f"skipped_by_reason={_format_count_map(selection.skip_counts)}")
            print(f"no_signal_states={','.join(selection.no_signal_states) if selection.no_signal_states else '(none)'}")
            print(f"summary_to={summary_to}")
            print(f"runtime_role={runtime_ctx.get('runtime_role', '')}")
            print(f"canonical_hostname={(runtime_ctx.get('canonical_hostname') or '(unset)')}")
            print(f"mfo_trusted_scheduled={(os.getenv('MFO_TRUSTED_SCHEDULED') or '0').strip() or '0'}")
            return 0

        if args.dry_run:
            outbox_path, manifest_path, diagnostics_path = _write_dry_run_artifacts(batch_group, selection)
            _emit_blocked_signal_tokens(selection)
            print(
                f"{PASS_SKIPPED_EXTRA_DRY_RUN} run_date={run_date.isoformat()} batch_group={batch_group} "
                f"crm_db={crm_db} selected_count={int(len(selection.selected))}"
            )
            print(f"{PASS_SKIPPED_EXTRA_DRY_RUN} would_contact_prospect_ids={_id_preview(selected_ids)}")
            print(f"{PASS_SKIPPED_EXTRA_DRY_RUN} skipped_count={skipped_count}")
            print(f"{PASS_SKIPPED_EXTRA_DRY_RUN} selected_by_state={_format_count_map(selection.selected_by_state)}")
            print(f"{PASS_SKIPPED_EXTRA_DRY_RUN} selected_by_reason={_format_count_map(selection.selected_by_reason)}")
            print(f"{PASS_SKIPPED_EXTRA_DRY_RUN} skipped_by_reason={_format_count_map(selection.skip_counts)}")
            print(f"{PASS_SKIPPED_EXTRA_DRY_RUN} summary_to={summary_to}")
            print(f"{PASS_SKIPPED_EXTRA_DRY_RUN} outbox_path={outbox_path}")
            print(f"{PASS_SKIPPED_EXTRA_DRY_RUN} manifest_path={manifest_path}")
            print(f"OUTREACH_PLAN_DIAGNOSTICS_PATH={diagnostics_path}")
            return 0

        one_click_ok, reason = roa.gm._one_click_config_present()
        if not one_click_ok:
            print(f"{ERR_SKIPPED_EXTRA_ONE_CLICK_REQUIRED} {reason}".strip(), file=sys.stderr)
            return 2

        template_text = roa.gm._read_template_text(REPO_ROOT / "outreach" / "outreach_plain.txt")
        try:
            html_template_text = roa.gm._read_template_text(REPO_ROOT / "outreach" / "outreach_card.html")
        except Exception:
            html_template_text = ""

        send_results_by_batch: dict[str, list[dict]] = defaultdict(list)
        contacted_by_state: Counter = Counter()
        failed_by_state: Counter = Counter()
        duplicate_guard_dropped = 0
        selected_state_set = {str(candidate.get("state") or "") for candidate in selection.selected}
        for state in [state for state in requested_states if state in selected_state_set]:
            state_candidates = [candidate for candidate in selection.selected if str(candidate.get("state") or "") == state]
            if not state_candidates:
                continue
            signal_ctx = dict(selection.state_signal_contexts.get(state) or {})
            last_refresh_et = str(signal_ctx.get("last_refresh_et") or "")
            signal_tokens = dict(signal_ctx.get("signal_tokens") or {})
            recent_signals_lines = str(signal_tokens.get("RECENT_SIGNALS_LINES") or "")
            recent_signals_html = str(signal_tokens.get("RECENT_SIGNALS_HTML") or "")
            recent_leads = list(signal_ctx.get("recent_leads") or [])

            for candidate in state_candidates:
                prospect_id = _safe_text(candidate.get("prospect_id") or "")
                if roa._has_prior_sent_event(conn, prospect_id):
                    duplicate_guard_dropped += 1
                    selection.skip_counts["duplicate_guard"] += 1
                    continue
                result = roa._send_outreach_email(
                    row=candidate["row"],
                    state=state,
                    batch=_safe_text(candidate.get("batch") or ""),
                    template_text=template_text,
                    html_template_text=html_template_text,
                    recent_signals_lines=recent_signals_lines,
                    recent_signals_html=recent_signals_html,
                    last_refresh_et=last_refresh_et,
                    signal_tokens=signal_tokens,
                    recent_leads=recent_leads,
                    candidate_ctx=candidate,
                )
                send_results_by_batch[_safe_text(candidate.get("batch") or "")].append(result)
                if result.get("ok"):
                    contacted_by_state[state] += 1
                else:
                    failed_by_state[state] += 1

        if duplicate_guard_dropped > 0:
            print(f"{OUTREACH_SKIPPED_EXTRA_DUPLICATE_GUARD_DROPPED}={duplicate_guard_dropped}")

        for batch, results in sorted(send_results_by_batch.items()):
            if not results:
                continue
            state = _safe_text(results[0].get("state") or "")
            roa._write_events_and_status_updates(conn, batch=batch, results=results)
            roa._append_ledger_records(path=export_ledger, batch=batch, state=state, results=results)

        contacted_count = int(sum(1 for results in send_results_by_batch.values() for item in results if item.get("ok")))
        failed_count = int(sum(1 for results in send_results_by_batch.values() for item in results if not item.get("ok")))
        skipped_count = int(sum(selection.skip_counts.values()))
        contacted_ids = [
            str(item.get("prospect_id") or "")
            for results in send_results_by_batch.values()
            for item in results
            if item.get("ok")
        ]

        _emit_blocked_signal_tokens(selection)
        print(
            f"{PASS_SKIPPED_EXTRA_EXPORT} run_date={run_date.isoformat()} batch_group={batch_group} "
            f"contacted_count={contacted_count} skipped_count={skipped_count} failed_count={failed_count}"
        )
        print(f"{PASS_SKIPPED_EXTRA_EXPORT} contacted_prospect_ids={_id_preview(contacted_ids)}")
        print(f"{PASS_SKIPPED_EXTRA_EXPORT} selected_by_state={_format_count_map(selection.selected_by_state)}")
        print(f"{PASS_SKIPPED_EXTRA_EXPORT} selected_by_reason={_format_count_map(selection.selected_by_reason)}")
        print(f"{PASS_SKIPPED_EXTRA_EXPORT} skipped_by_reason={_format_count_map(selection.skip_counts)}")
        print(f"{PASS_SKIPPED_EXTRA_EXPORT} contacted_by_state={_format_count_map(contacted_by_state)}")
        print(f"{PASS_SKIPPED_EXTRA_EXPORT} failed_by_state={_format_count_map(failed_by_state)}")

        ok_send, err = _send_summary_email(
            summary_to=summary_to,
            run_date=run_date,
            selected_ids=contacted_ids,
            selection=selection,
            contacted_count=contacted_count,
            failed_count=failed_count,
            contacted_by_state=contacted_by_state,
            failed_by_state=failed_by_state,
            configured_states=configured_states,
        )
        if not ok_send:
            print(f"{ERR_SKIPPED_EXTRA_SUMMARY_SEND} {err}", file=sys.stderr)
            return 1

        print(f"{PASS_SKIPPED_EXTRA_SUMMARY} to={summary_to} batch_group={batch_group}")
        if failed_count:
            return 1
        return 0
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
        if live_send_lock is not None:
            live_send_lock.release()


if __name__ == "__main__":
    raise SystemExit(main())
