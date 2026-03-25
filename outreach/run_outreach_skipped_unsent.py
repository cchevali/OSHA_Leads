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
ERR_SKIPPED_EXTRA_FOR_DATE_LIVE_SEND_BLOCKED = "ERR_SKIPPED_EXTRA_FOR_DATE_LIVE_SEND_BLOCKED"
ERR_SKIPPED_EXTRA_ONE_CLICK_REQUIRED = "ERR_SKIPPED_EXTRA_ONE_CLICK_REQUIRED"
ERR_SKIPPED_EXTRA_SUMMARY_SEND = "ERR_SKIPPED_EXTRA_SUMMARY_SEND"
OUTREACH_SKIPPED_EXTRA_SKIP_NON_WEEKDAY = "OUTREACH_SKIPPED_EXTRA_SKIP_NON_WEEKDAY"
OUTREACH_SKIPPED_EXTRA_SKIP_ALREADY_SENT_TODAY = "OUTREACH_SKIPPED_EXTRA_SKIP_ALREADY_SENT_TODAY"
OUTREACH_SKIPPED_EXTRA_SKIP_NO_SIGNALS = "OUTREACH_SKIPPED_EXTRA_SKIP_NO_SIGNALS"
OUTREACH_SKIPPED_EXTRA_DUPLICATE_GUARD_DROPPED = "OUTREACH_SKIPPED_EXTRA_DUPLICATE_GUARD_DROPPED"

EXTRA_BATCH_LABEL = "SKIPPED_UNSENT_EXTRA"
EXTRA_SELECTION_REASONS = {"role_inbox_email", "not_default_send_eligible"}


@dataclass
class ExtraSelection:
    selected: list[dict]
    manifest_rows: list[dict]
    pool_total: int
    skip_counts: Counter
    selected_by_state: Counter
    selected_by_reason: Counter
    no_signal_states: list[str]
    state_signal_counts: dict[str, int]


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


def _candidate_sort_key(candidate: dict) -> tuple[str, tuple, str]:
    return (
        _safe_text(candidate.get("state") or ""),
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


def _legacy_extra_selection_reason(row: sqlite3.Row) -> str:
    email = roa._norm_email(str(row["email"] or ""))
    if not roa._is_send_eligible_for_default(row, include_adjacent_contractors=False):
        return "not_default_send_eligible"
    if roa._is_role_inbox_email(email):
        return "role_inbox_email"
    return ""


def _select_skipped_unsent_candidates(
    *,
    conn: sqlite3.Connection,
    run_date: date,
    osha_db: str,
    suppressed_emails: set[str],
) -> ExtraSelection:
    cols = roa._prospect_select_columns(conn)
    rows = conn.execute("SELECT " + ", ".join(cols) + " FROM prospects").fetchall()
    sent_ids = roa._fetch_prior_sent_ids(conn)
    signal_window_days = roa._signal_window_days()
    signal_count_cache: dict[str, int] = {}
    no_signal_states: set[str] = set()
    selected: list[dict] = []
    manifest_rows: list[dict] = []
    skip_counts: Counter = Counter()
    selected_by_state: Counter = Counter()
    selected_by_reason: Counter = Counter()
    pool_total = 0

    def signal_count_for_state(state: str) -> int:
        if state not in signal_count_cache:
            signal_count_cache[state] = int(
                roa._count_real_signals_for_state_window(
                    db_path=osha_db,
                    state=state,
                    run_date=run_date,
                    window_days=signal_window_days,
                )
            )
        return int(signal_count_cache[state])

    for row in rows:
        email = roa._norm_email(str(row["email"] or ""))
        current_skip_reason = roa._skip_reason(
            row,
            suppressed_emails=suppressed_emails,
            sent_ids=sent_ids,
            allow_repeat=False,
            skip_role_inboxes=False,
            include_adjacent_contractors=True,
        )
        original_skip_reason = _legacy_extra_selection_reason(row)
        if current_skip_reason and current_skip_reason != "suppressed":
            continue
        if not original_skip_reason and current_skip_reason != "suppressed":
            continue

        candidate = roa._candidate_from_row(row)
        state = roa._normalize_us_state(str(row["state"] or ""))
        candidate["state"] = state
        candidate["batch"] = _extra_batch_id(state, run_date) if state else ""

        pool_total += 1
        if email in suppressed_emails or current_skip_reason == "suppressed":
            skip_counts["suppressed_compliance"] += 1
            manifest_rows.append(
                _candidate_manifest_row(
                    candidate,
                    status="dropped",
                    reason="suppressed_compliance",
                    original_skip_reason="suppressed",
                )
            )
            continue

        if original_skip_reason not in EXTRA_SELECTION_REASONS:
            continue

        if not state:
            skip_counts["missing_state"] += 1
            manifest_rows.append(
                _candidate_manifest_row(
                    candidate,
                    status="dropped",
                    reason="missing_state",
                    original_skip_reason=original_skip_reason,
                )
            )
            continue

        state_signal_count = signal_count_for_state(state)
        if state_signal_count <= 0:
            no_signal_states.add(state)
            skip_counts["no_signals"] += 1
            manifest_rows.append(
                _candidate_manifest_row(
                    candidate,
                    status="dropped",
                    reason="no_signals",
                    original_skip_reason=original_skip_reason,
                )
            )
            continue

        candidate["original_skip_reason"] = original_skip_reason
        selected.append(candidate)
        selected_by_state[state] += 1
        selected_by_reason[original_skip_reason] += 1

    selected.sort(key=_candidate_sort_key)
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
        pool_total=int(pool_total),
        skip_counts=skip_counts,
        selected_by_state=selected_by_state,
        selected_by_reason=selected_by_reason,
        no_signal_states=sorted(no_signal_states),
        state_signal_counts={key: int(value) for key, value in sorted(signal_count_cache.items())},
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


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Send the skipped, never-sent outreach pool as a one-off extra batch while preserving suppression."
    )
    ap.add_argument("--dry-run", action="store_true", help="Select and print actions only. No DB writes, no email.")
    ap.add_argument("--print-config", action="store_true", help="Print resolved config paths and target counts, then exit.")
    ap.add_argument("--for-date", default="", help="Override run date (YYYY-MM-DD) for print-config and dry-run.")
    ap.add_argument(
        "--allow-second-live-run-same-day",
        action="store_true",
        help="Allow this extra live batch to run even when another outreach batch already sent today.",
    )
    ap.add_argument(
        "--allow-weekend-send",
        action="store_true",
        help="Emergency/manual override: allow live outreach sends on Sat/Sun.",
    )
    ap.add_argument(
        "--confirm-live-send",
        action="store_true",
        help="Manual live-send confirmation flag (not required for trusted scheduled runtime).",
    )
    ap.add_argument("--to", default="", help="Optional summary recipient override; must equal OSHA_SMOKE_TO.")
    args = ap.parse_args()

    ok_date, run_date, date_msg = roa._parse_for_date(str(args.for_date or ""))
    if not ok_date:
        print(date_msg, file=sys.stderr)
        return 2

    local_now = roa._outreach_local_now()
    today_local = local_now["date"]
    configured_states = roa._parse_states(os.getenv("OUTREACH_STATES", "TX,CA,FL,PA,OH"))
    signal_window_days = roa._signal_window_days()
    crm_db = roa._crm_db_path()
    suppression_csv = roa._suppression_csv_path()
    export_ledger = roa._export_ledger_path()
    osha_db_resolution = roa.resolve_osha_db_path(REPO_ROOT)
    osha_db = str(osha_db_resolution.effective_path)
    runtime_mode = str(os.getenv("MFO_RUNTIME_MODE") or "manual").strip().lower() or "manual"
    runtime_ctx = roa.runtime_context_dict(mode=runtime_mode, intent="send", dry_run=bool(args.dry_run))
    batch_group = _extra_batch_group(run_date)

    is_live_send = not bool(args.dry_run or args.print_config)
    if is_live_send:
        runtime_preflight = roa.run_runtime_preflight(
            mode=runtime_mode,
            intent="send",
            dry_run=False,
            task_log_root=str(os.getenv("TASK_LOG_ROOT") or ""),
            run_summary_root=str(os.getenv("RUN_SUMMARY_ROOT") or ""),
            require_confirm_live_send=True,
            confirm_live_send=bool(args.confirm_live_send),
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

        if is_live_send and (not bool(args.allow_second_live_run_same_day)):
            existing_batches = roa._sent_batches_for_day(conn, run_date)
            if existing_batches:
                existing_batches_text = ",".join(existing_batches) if existing_batches else "none"
                print(
                    f"{OUTREACH_SKIPPED_EXTRA_SKIP_ALREADY_SENT_TODAY}=1 date={run_date.isoformat()} "
                    f"existing_batches={existing_batches_text} guard=ON"
                )
                return 0

        selection = _select_skipped_unsent_candidates(
            conn=conn,
            run_date=run_date,
            osha_db=osha_db,
            suppressed_emails=suppressed_emails,
        )
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
            print(f"outreach_signal_window_days={signal_window_days}")
            print(f"outreach_effective_timezone={local_now['timezone']}")
            print(f"outreach_effective_local_date={local_now['date_text']}")
            print(f"outreach_effective_weekday={local_now['weekday_name']}")
            print(f"outreach_allow_weekend_send={'YES' if args.allow_weekend_send else 'NO'}")
            print(f"outreach_states_config={','.join(configured_states) if configured_states else '(none)'}")
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
            for state in selection.no_signal_states:
                print(f"{OUTREACH_SKIPPED_EXTRA_SKIP_NO_SIGNALS} state={state} window_days={signal_window_days}")
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
        for state in sorted({str(candidate.get("state") or "") for candidate in selection.selected}):
            state_candidates = [candidate for candidate in selection.selected if str(candidate.get("state") or "") == state]
            if not state_candidates:
                continue
            signal_ctx = roa._prepare_signal_content_with_triage(
                batch=_extra_batch_id(state, run_date),
                state=state,
                osha_db=osha_db,
                dry_run_suffix="live",
            )
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

        for state in selection.no_signal_states:
            print(f"{OUTREACH_SKIPPED_EXTRA_SKIP_NO_SIGNALS} state={state} window_days={signal_window_days}")
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
