from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from collections import Counter
from dataclasses import dataclass
from datetime import date
from pathlib import Path

try:  # pragma: no cover
    from dotenv import load_dotenv

    load_dotenv()
except Exception:  # pragma: no cover
    pass

import outreach.run_outreach_auto as roa


REPO_ROOT = Path(__file__).resolve().parents[1]

ERR_RESEND_SOURCE_BATCHES_REQUIRED = "ERR_OUTREACH_RESEND_SOURCE_BATCHES_REQUIRED"
ERR_RESEND_CRM_REQUIRED = "ERR_OUTREACH_RESEND_CRM_REQUIRED"
ERR_RESEND_CONFIRM_REQUIRED = "ERR_OUTREACH_RESEND_CONFIRM_REQUIRED"
ERR_RESEND_FOR_DATE_LIVE_SEND_BLOCKED = "ERR_OUTREACH_RESEND_FOR_DATE_LIVE_SEND_BLOCKED"
ERR_RESEND_ONE_CLICK_REQUIRED = "ERR_OUTREACH_RESEND_ONE_CLICK_REQUIRED"
ERR_RESEND_SOURCE_BATCH_EMPTY = "ERR_OUTREACH_RESEND_SOURCE_BATCH_EMPTY"

PASS_RESEND_PRINT_CONFIG = "PASS_OUTREACH_RESEND_PRINT_CONFIG"
PASS_RESEND_DRY_RUN = "PASS_OUTREACH_RESEND_DRY_RUN"
PASS_RESEND_LIVE = "PASS_OUTREACH_RESEND_LIVE"

OUTREACH_RESEND_SKIP_NON_WEEKDAY = "OUTREACH_RESEND_SKIP_NON_WEEKDAY"
OUTREACH_RESEND_SKIP_ALREADY_SENT = "OUTREACH_RESEND_SKIP_ALREADY_SENT"
OUTREACH_RESEND_SKIP_NO_SIGNALS = "OUTREACH_RESEND_SKIP_NO_SIGNALS"
OUTREACH_RESEND_SKIP_SOURCE_EMPTY = "OUTREACH_RESEND_SKIP_SOURCE_EMPTY"

RESEND_BATCH_LABEL = "ZERO_SIGNAL_RECOVERY"


@dataclass(frozen=True)
class ResendCandidate:
    row: sqlite3.Row
    candidate_ctx: dict[str, object]


@dataclass(frozen=True)
class ResendBatchPlan:
    source_batch: str
    resend_batch: str
    state: str
    source_sent_count: int
    selected: list[ResendCandidate]
    skipped_counts: Counter
    skipped_details: list[dict[str, str]]
    signal_ctx: dict[str, object]


def _safe_text(value: object) -> str:
    return str(value or "").strip()


def _parse_source_batches(raw: str) -> list[str]:
    values: list[str] = []
    for item in str(raw or "").split(","):
        batch = _safe_text(item)
        if batch and batch not in values:
            values.append(batch)
    return values


def _resend_batch_id(source_state: str, run_date: date) -> str:
    state = roa._normalize_us_state(source_state)
    return f"{run_date.isoformat()}_{state}_{RESEND_BATCH_LABEL}" if state else f"{run_date.isoformat()}_{RESEND_BATCH_LABEL}"


def _connect_existing_crm(conn_path: Path) -> sqlite3.Connection:
    conn = roa._connect_existing_crm(conn_path)
    if not roa._require_schema(conn):
        raise RuntimeError("schema_missing")
    return conn


def _prospect_lookup_maps(conn: sqlite3.Connection) -> tuple[dict[str, sqlite3.Row], dict[str, sqlite3.Row]]:
    columns = roa._prospect_select_columns(conn)
    query = "SELECT " + ", ".join(columns) + " FROM prospects"
    rows = conn.execute(query).fetchall()
    by_id: dict[str, sqlite3.Row] = {}
    by_email: dict[str, sqlite3.Row] = {}
    for row in rows:
        prospect_id = _safe_text(row["prospect_id"] if "prospect_id" in row.keys() else "")
        email = roa._norm_email(str(row["email"] or "")) if "email" in row.keys() else ""
        if prospect_id and prospect_id not in by_id:
            by_id[prospect_id] = row
        if email and email not in by_email:
            by_email[email] = row
    return by_id, by_email


def _source_batch_events(conn: sqlite3.Connection, source_batch: str) -> list[dict[str, str]]:
    rows = conn.execute(
        """
        SELECT prospect_id, metadata_json
        FROM outreach_events
        WHERE batch_id = ? AND event_type = 'sent'
        ORDER BY event_id
        """,
        (source_batch,),
    ).fetchall()
    events: list[dict[str, str]] = []
    seen_keys: set[str] = set()
    for row in rows:
        prospect_id = _safe_text(row["prospect_id"] if "prospect_id" in row.keys() else "")
        metadata_raw = _safe_text(row["metadata_json"] if "metadata_json" in row.keys() else "")
        try:
            metadata = json.loads(metadata_raw or "{}")
        except Exception:
            metadata = {}
        event_email = roa._norm_email(str(metadata.get("email") or ""))
        event_state = roa._normalize_us_state(str(metadata.get("state") or ""))
        dedupe_key = prospect_id or event_email
        if not dedupe_key or dedupe_key in seen_keys:
            continue
        seen_keys.add(dedupe_key)
        events.append(
            {
                "prospect_id": prospect_id,
                "email": event_email,
                "state": event_state,
            }
        )
    return events


def _resolve_source_state(events: list[dict[str, str]], source_batch: str) -> str:
    states = []
    for item in events:
        state = roa._normalize_us_state(str(item.get("state") or ""))
        if state and state not in states:
            states.append(state)
    if len(states) == 1:
        return states[0]
    batch_tokens = source_batch.split("_")
    for token in batch_tokens:
        state = roa._normalize_us_state(token)
        if state:
            return state
    return ""


def _load_resend_plan(
    *,
    conn: sqlite3.Connection,
    source_batch: str,
    run_date: date,
    osha_db: str,
    suppressed_emails: set[str],
) -> ResendBatchPlan:
    events = _source_batch_events(conn, source_batch)
    source_sent_count = int(len(events))
    source_state = _resolve_source_state(events, source_batch)
    resend_batch = _resend_batch_id(source_state, run_date)
    skipped_counts: Counter = Counter()
    skipped_details: list[dict[str, str]] = []
    selected: list[ResendCandidate] = []
    rows_by_prospect_id, rows_by_email = _prospect_lookup_maps(conn)
    sent_ids = roa._fetch_prior_sent_ids(conn)

    for event in events:
        prospect_id = _safe_text(event.get("prospect_id") or "")
        email = roa._norm_email(str(event.get("email") or ""))
        row = rows_by_prospect_id.get(prospect_id) if prospect_id else None
        if row is None and email:
            row = rows_by_email.get(email)
        if row is None:
            skipped_counts["crm_missing"] += 1
            skipped_details.append(
                {
                    "source_batch": source_batch,
                    "prospect_id": prospect_id,
                    "email": email,
                    "reason": "crm_missing",
                }
            )
            continue

        current_state = roa._normalize_us_state(str(row["state"] or ""))
        if source_state and current_state and current_state != source_state:
            skipped_counts["state_scope_changed"] += 1
            skipped_details.append(
                {
                    "source_batch": source_batch,
                    "prospect_id": prospect_id,
                    "email": roa._norm_email(str(row["email"] or "")),
                    "reason": "state_scope_changed",
                }
            )
            continue

        reason = roa._skip_reason(
            row=row,
            suppressed_emails=suppressed_emails,
            sent_ids=sent_ids,
            allow_repeat=True,
            skip_role_inboxes=False,
            include_adjacent_contractors=True,
        )
        if reason:
            skipped_counts[reason] += 1
            skipped_details.append(
                {
                    "source_batch": source_batch,
                    "prospect_id": prospect_id,
                    "email": roa._norm_email(str(row["email"] or "")),
                    "reason": reason,
                }
            )
            continue

        candidate_ctx = roa._candidate_from_row(row)
        candidate_ctx["batch"] = resend_batch
        candidate_ctx["state"] = source_state or current_state
        candidate_ctx["source_batch"] = source_batch
        selected.append(ResendCandidate(row=row, candidate_ctx=candidate_ctx))

    signal_ctx = {}
    if source_state:
        signal_ctx = roa._prepare_signal_content_with_triage(
            batch=resend_batch,
            state=source_state,
            osha_db=osha_db,
            dry_run_suffix="exact_resend",
            run_date=run_date,
            signal_window_days=roa._signal_window_days(),
        )

    return ResendBatchPlan(
        source_batch=source_batch,
        resend_batch=resend_batch,
        state=source_state,
        source_sent_count=source_sent_count,
        selected=selected,
        skipped_counts=skipped_counts,
        skipped_details=skipped_details,
        signal_ctx=signal_ctx,
    )


def _resend_batch_already_sent(conn: sqlite3.Connection, batch_id: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM outreach_events
        WHERE batch_id = ? AND event_type = 'sent'
        LIMIT 1
        """,
        (batch_id,),
    ).fetchone()
    return bool(row)


def _format_counter(counter: Counter) -> str:
    if not counter:
        return "(none)"
    parts = [f"{key}:{int(counter[key])}" for key in sorted(counter.keys())]
    return ",".join(parts) if parts else "(none)"


def _id_preview(values: list[str], *, limit: int = 20) -> str:
    normalized = [_safe_text(value) for value in values if _safe_text(value)]
    if not normalized:
        return "(none)"
    if len(normalized) <= limit:
        return ",".join(normalized)
    return ",".join(normalized[:limit]) + f",...(+{len(normalized) - limit} more)"


def _send_summary_email(
    *,
    summary_to: str,
    run_date: date,
    plans: list[ResendBatchPlan],
    sent_results_by_batch: dict[str, list[dict]],
) -> tuple[bool, str]:
    contacted_total = 0
    failed_total = 0
    lines = [
        "Exact outreach resend summary",
        f"- run_date: {run_date.isoformat()}",
    ]
    html_lines = [
        "<div style=\"font-family: system-ui, -apple-system, 'Segoe UI', Roboto, Arial, sans-serif;\">",
        "<h3>Exact Outreach Resend Summary</h3>",
        f"<p><strong>run_date:</strong> {run_date.isoformat()}</p>",
        "<ul>",
    ]
    for plan in plans:
        results = list(sent_results_by_batch.get(plan.resend_batch) or [])
        contacted = sum(1 for item in results if item.get("ok"))
        failed = sum(1 for item in results if not item.get("ok"))
        contacted_total += int(contacted)
        failed_total += int(failed)
        signal_ctx = dict(plan.signal_ctx or {})
        lines.extend(
            [
                f"- source_batch: {plan.source_batch}",
                f"  resend_batch: {plan.resend_batch}",
                f"  state: {plan.state or '(unknown)'}",
                f"  source_sent_count: {int(plan.source_sent_count)}",
                f"  selected_count: {int(len(plan.selected))}",
                f"  skipped_by_reason: {_format_counter(plan.skipped_counts)}",
                f"  raw_signal_count: {int(signal_ctx.get('raw_signal_count') or 0)}",
                f"  recent_signal_source_count: {int(signal_ctx.get('recent_signal_source_count') or 0)}",
                f"  renderable_signal_count: {int(signal_ctx.get('renderable_signal_count') or 0)}",
                f"  signal_fetch_status: {_safe_text(signal_ctx.get('signal_fetch_status') or '') or 'unknown'}",
                f"  contacted_count: {int(contacted)}",
                f"  failed_count: {int(failed)}",
            ]
        )
        html_lines.append(
            "<li>"
            f"<strong>source_batch:</strong> {plan.source_batch}<br>"
            f"<strong>resend_batch:</strong> {plan.resend_batch}<br>"
            f"<strong>state:</strong> {plan.state or '(unknown)'}<br>"
            f"<strong>source_sent_count:</strong> {int(plan.source_sent_count)}<br>"
            f"<strong>selected_count:</strong> {int(len(plan.selected))}<br>"
            f"<strong>skipped_by_reason:</strong> {_format_counter(plan.skipped_counts)}<br>"
            f"<strong>raw_signal_count:</strong> {int(signal_ctx.get('raw_signal_count') or 0)}<br>"
            f"<strong>recent_signal_source_count:</strong> {int(signal_ctx.get('recent_signal_source_count') or 0)}<br>"
            f"<strong>renderable_signal_count:</strong> {int(signal_ctx.get('renderable_signal_count') or 0)}<br>"
            f"<strong>signal_fetch_status:</strong> {_safe_text(signal_ctx.get('signal_fetch_status') or '') or 'unknown'}<br>"
            f"<strong>contacted_count:</strong> {int(contacted)}<br>"
            f"<strong>failed_count:</strong> {int(failed)}"
            "</li>"
        )
    html_lines.extend(["</ul>", "</div>"])
    subject = f"[RECOVERY] Outreach resend {run_date.isoformat()} contacted={contacted_total} failed={failed_total}"
    return roa._send_summary_email(summary_to, subject, "\n".join(lines) + "\n", "\n".join(html_lines))


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Resend exact prior outreach batches to the same prospects while rechecking current suppression and signal guards."
    )
    ap.add_argument("--dry-run", action="store_true", help="Print the exact resend plan only. No DB writes, no email.")
    ap.add_argument("--print-config", action="store_true", help="Print resolved config paths and source batches, then exit.")
    ap.add_argument("--for-date", default="", help="Override run date (YYYY-MM-DD) for print-config and dry-run.")
    ap.add_argument("--source-batches", default="", help="Comma-separated prior sent batch ids to resend exactly.")
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

    source_batches = _parse_source_batches(str(args.source_batches or ""))
    if not source_batches:
        print(ERR_RESEND_SOURCE_BATCHES_REQUIRED, file=sys.stderr)
        return 2

    is_live_send = not bool(args.dry_run or args.print_config)
    if is_live_send and not bool(args.confirm_live_send):
        print(ERR_RESEND_CONFIRM_REQUIRED, file=sys.stderr)
        return 2

    local_now = roa._outreach_local_now()
    today_local = local_now["date"]
    runtime_mode = str(os.getenv("MFO_RUNTIME_MODE") or "manual").strip().lower() or "manual"
    runtime_ctx = roa.runtime_context_dict(mode=runtime_mode, intent="send", dry_run=bool(args.dry_run or args.print_config))
    crm_db = roa._crm_db_path()
    suppression_csv = roa._suppression_csv_path()
    export_ledger = roa._export_ledger_path()
    signal_window_days = roa._signal_window_days()
    osha_db_resolution = roa.resolve_osha_db_path(REPO_ROOT)
    osha_db = str(osha_db_resolution.effective_path)

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
            f"{OUTREACH_RESEND_SKIP_NON_WEEKDAY} local_date={local_now['date_text']} "
            f"weekday={local_now['weekday_name']} gate=outreach_weekdays_only"
        )
        return 0
    if is_live_send and run_date != today_local:
        print(
            f"{ERR_RESEND_FOR_DATE_LIVE_SEND_BLOCKED} for_date={run_date.isoformat()} today={today_local.isoformat()}",
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
                f"outreach_exact_resend_{run_date.isoformat()}",
                repo_root=REPO_ROOT,
                metadata={"run_date": run_date.isoformat(), "task": "outreach_exact_resend"},
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
            print(f"{ERR_RESEND_CRM_REQUIRED} crm_missing path={crm_db}", file=sys.stderr)
            return 2
        try:
            conn = _connect_existing_crm(crm_db)
        except Exception as exc:
            print(f"{ERR_RESEND_CRM_REQUIRED} path={crm_db} err={type(exc).__name__}", file=sys.stderr)
            return 2

        try:
            suppressed_emails = roa._load_suppression_emails(conn)
        except ValueError as exc:
            print(str(exc), file=sys.stderr)
            return 3

        plans = [
            _load_resend_plan(
                conn=conn,
                source_batch=source_batch,
                run_date=run_date,
                osha_db=osha_db,
                suppressed_emails=suppressed_emails,
            )
            for source_batch in source_batches
        ]

        if args.print_config:
            print(f"{PASS_RESEND_PRINT_CONFIG} crm_db={crm_db.resolve()}")
            print(f"{PASS_RESEND_PRINT_CONFIG} suppression_csv={suppression_csv.resolve()}")
            print(f"{PASS_RESEND_PRINT_CONFIG} export_ledger={export_ledger.resolve()}")
            print(f"{PASS_RESEND_PRINT_CONFIG} outreach_signal_db={Path(osha_db).resolve()}")
            print(f"{PASS_RESEND_PRINT_CONFIG} outreach_signal_db_source={osha_db_resolution.source}")
            if osha_db_resolution.warning_token:
                print(osha_db_resolution.warning_token)
            print(f"{PASS_RESEND_PRINT_CONFIG} run_date={run_date.isoformat()}")
            print(f"{PASS_RESEND_PRINT_CONFIG} source_batches={','.join(source_batches)}")
            print(f"outreach_signal_window_days={signal_window_days}")
            print(f"outreach_effective_timezone={local_now['timezone']}")
            print(f"outreach_effective_local_date={local_now['date_text']}")
            print(f"outreach_effective_weekday={local_now['weekday_name']}")
            print(f"outreach_allow_weekend_send={'YES' if args.allow_weekend_send else 'NO'}")
            print(f"summary_to={summary_to}")
            print(f"runtime_role={runtime_ctx.get('runtime_role', '')}")
            print(f"canonical_hostname={(runtime_ctx.get('canonical_hostname') or '(unset)')}")
            print(f"mfo_trusted_scheduled={(os.getenv('MFO_TRUSTED_SCHEDULED') or '0').strip() or '0'}")
            for plan in plans:
                signal_ctx = dict(plan.signal_ctx or {})
                print(
                    f"{PASS_RESEND_PRINT_CONFIG} source_batch={plan.source_batch} resend_batch={plan.resend_batch} "
                    f"state={plan.state or '(unknown)'} source_sent_count={int(plan.source_sent_count)} "
                    f"selected_count={int(len(plan.selected))} skipped_by_reason={_format_counter(plan.skipped_counts)} "
                    f"raw_signal_count={int(signal_ctx.get('raw_signal_count') or 0)} "
                    f"recent_signal_source_count={int(signal_ctx.get('recent_signal_source_count') or 0)} "
                    f"renderable_signal_count={int(signal_ctx.get('renderable_signal_count') or 0)} "
                    f"signal_fetch_status={_safe_text(signal_ctx.get('signal_fetch_status') or '') or 'unknown'}"
                )
            return 0

        if args.dry_run:
            total_selected = 0
            for plan in plans:
                signal_ctx = dict(plan.signal_ctx or {})
                total_selected += int(len(plan.selected))
                if roa._signal_guard_blocks_send(signal_ctx):
                    roa._emit_signal_guard_tokens(
                        token=OUTREACH_RESEND_SKIP_NO_SIGNALS,
                        state=plan.state,
                        signal_ctx=signal_ctx,
                    )
                print(
                    f"{PASS_RESEND_DRY_RUN} source_batch={plan.source_batch} resend_batch={plan.resend_batch} "
                    f"state={plan.state or '(unknown)'} source_sent_count={int(plan.source_sent_count)} "
                    f"selected_count={int(len(plan.selected))} skipped_by_reason={_format_counter(plan.skipped_counts)} "
                    f"raw_signal_count={int(signal_ctx.get('raw_signal_count') or 0)} "
                    f"recent_signal_source_count={int(signal_ctx.get('recent_signal_source_count') or 0)} "
                    f"renderable_signal_count={int(signal_ctx.get('renderable_signal_count') or 0)} "
                    f"signal_fetch_status={_safe_text(signal_ctx.get('signal_fetch_status') or '') or 'unknown'}"
                )
                print(
                    f"{PASS_RESEND_DRY_RUN} source_batch={plan.source_batch} "
                    f"would_contact_prospect_ids={_id_preview([item.candidate_ctx.get('prospect_id', '') for item in plan.selected])}"
                )
            print(f"{PASS_RESEND_DRY_RUN} total_selected_count={int(total_selected)}")
            print(f"{PASS_RESEND_DRY_RUN} summary_to={summary_to}")
            return 0

        one_click_ok, reason = roa.gm._one_click_config_present()
        if not one_click_ok:
            print(f"{ERR_RESEND_ONE_CLICK_REQUIRED} {reason}".strip(), file=sys.stderr)
            return 2

        template_text = roa.gm._read_template_text(REPO_ROOT / "outreach" / "outreach_plain.txt")
        try:
            html_template_text = roa.gm._read_template_text(REPO_ROOT / "outreach" / "outreach_card.html")
        except Exception:
            html_template_text = ""

        sent_results_by_batch: dict[str, list[dict]] = {}
        total_selected = 0
        for plan in plans:
            total_selected += int(len(plan.selected))
            signal_ctx = dict(plan.signal_ctx or {})
            if plan.source_sent_count <= 0:
                print(f"{OUTREACH_RESEND_SKIP_SOURCE_EMPTY} source_batch={plan.source_batch}")
                continue
            if not plan.state:
                print(f"{ERR_RESEND_SOURCE_BATCH_EMPTY} source_batch={plan.source_batch} detail=state_unresolved", file=sys.stderr)
                return 2
            if _resend_batch_already_sent(conn, plan.resend_batch):
                print(f"{OUTREACH_RESEND_SKIP_ALREADY_SENT} batch={plan.resend_batch} guard=ON")
                continue
            if roa._signal_guard_blocks_send(signal_ctx):
                roa._emit_signal_guard_tokens(
                    token=OUTREACH_RESEND_SKIP_NO_SIGNALS,
                    state=plan.state,
                    signal_ctx=signal_ctx,
                )
                continue

            signal_tokens = dict(signal_ctx.get("signal_tokens") or {})
            recent_signals_lines = str(signal_tokens.get("RECENT_SIGNALS_LINES") or "")
            recent_signals_html = str(signal_tokens.get("RECENT_SIGNALS_HTML") or "")
            last_refresh_et = _safe_text(signal_ctx.get("last_refresh_et") or "")
            batch_results: list[dict] = []
            for resend_candidate in plan.selected:
                batch_results.append(
                    roa._send_outreach_email(
                        row=resend_candidate.row,
                        state=plan.state,
                        batch=plan.resend_batch,
                        template_text=template_text,
                        html_template_text=html_template_text,
                        recent_signals_lines=recent_signals_lines,
                        recent_signals_html=recent_signals_html,
                        last_refresh_et=last_refresh_et,
                        signal_tokens=signal_tokens,
                        recent_leads=list(signal_ctx.get("recent_leads") or []),
                        candidate_ctx=dict(resend_candidate.candidate_ctx),
                    )
                )
            roa._write_events_and_status_updates(conn, plan.resend_batch, batch_results)
            roa._append_ledger_records(export_ledger, batch=plan.resend_batch, state=plan.state, results=batch_results)
            sent_results_by_batch[plan.resend_batch] = batch_results

            contacted_count = sum(1 for item in batch_results if item.get("ok"))
            failed_count = sum(1 for item in batch_results if not item.get("ok"))
            print(
                f"{PASS_RESEND_LIVE} source_batch={plan.source_batch} resend_batch={plan.resend_batch} "
                f"state={plan.state} contacted_count={int(contacted_count)} failed_count={int(failed_count)} "
                f"selected_count={int(len(plan.selected))}"
            )
            print(
                f"{PASS_RESEND_LIVE} resend_batch={plan.resend_batch} "
                f"contacted_prospect_ids={_id_preview([item.get('prospect_id', '') for item in batch_results if item.get('ok')])}"
            )

        ok_send, err = _send_summary_email(
            summary_to=summary_to,
            run_date=run_date,
            plans=plans,
            sent_results_by_batch=sent_results_by_batch,
        )
        if not ok_send:
            print(f"{roa.ERR_AUTO_SUMMARY_SEND} {err}", file=sys.stderr)
            return 1
        print(f"{PASS_RESEND_LIVE} total_selected_count={int(total_selected)}")
        print(f"{roa.PASS_AUTO_SUMMARY} to={summary_to} batch=exact_resend_{run_date.isoformat()}")
        failed_total = sum(
            1
            for batch_results in sent_results_by_batch.values()
            for item in batch_results
            if not item.get("ok")
        )
        if failed_total:
            return 1
        return 0
    finally:
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass
        if live_send_lock is not None:
            live_send_lock.release()


if __name__ == "__main__":
    raise SystemExit(main())
