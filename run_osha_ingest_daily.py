import argparse
import os
import re
import sqlite3
import sys
from pathlib import Path

import crm_light
import ingest_osha
from outreach import us_state
from lead_filters import load_territory_definitions, resolve_territory_code
from runtime_data_dir import resolve_osha_db_path
from runtime_guard import render_runtime_lines, run_runtime_preflight, runtime_context_dict


ERR_INGEST_DAILY_CONFIG = "ERR_INGEST_DAILY_CONFIG"
ERR_INGEST_DAILY_FAILED = "ERR_INGEST_DAILY_FAILED"
PASS_INGEST_DAILY_DOCTOR = "PASS_INGEST_DAILY_DOCTOR"
PASS_INGEST_DAILY_COMPLETE = "PASS_INGEST_DAILY_COMPLETE"


def _emit(key: str, value: str) -> None:
    print(f"{key}={value}")


def _error(token: str, detail: str) -> int:
    print(f"{token} {detail}")
    return 1


def _parse_states(raw: str) -> list[str]:
    states: list[str] = []
    for part in (raw or "").split(","):
        state = (part or "").strip().upper()
        if not state:
            continue
        if not re.fullmatch(r"[A-Z]{2,3}", state):
            raise ValueError(f"invalid_state={state}")
        if state not in states:
            states.append(state)
    return states


def _resolve_outreach_states() -> tuple[list[str], str]:
    env_states_raw = (os.getenv("OUTREACH_STATES") or "").strip()
    env_states = _parse_states(env_states_raw)
    if env_states:
        return env_states, "env"
    return list(us_state.DEFAULT_OUTREACH_STATES), "fallback"


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (str(table_name or "").strip(),),
    ).fetchone()
    return row is not None


def _table_has_column(conn: sqlite3.Connection, table_name: str, column_name: str) -> bool:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    for row in rows:
        if len(row) > 1 and str(row[1] or "").strip().lower() == str(column_name or "").strip().lower():
            return True
    return False


def _territory_states_for_scope(territory_code: str, definitions: dict[str, dict]) -> list[str]:
    raw = str(territory_code or "").strip()
    if not raw:
        return []
    canonical = resolve_territory_code(raw, definitions)
    if canonical in definitions:
        states = [str(s).strip().upper() for s in (definitions[canonical].get("states") or []) if str(s).strip()]
        return states
    fallback = raw.strip().upper()
    if len(fallback) == 2 and fallback.isalpha():
        return [fallback]
    return []


def _trial_live_states_from_crm() -> list[str]:
    try:
        db_path = crm_light.crm_light_db_path()
    except Exception:
        return []
    if not Path(db_path).exists():
        return []
    try:
        definitions = load_territory_definitions()
    except Exception:
        return []
    try:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
    except Exception:
        return []
    try:
        if not _table_exists(conn, "subscribers"):
            return []
        if not _table_has_column(conn, "subscribers", "status"):
            return []
        if not _table_has_column(conn, "subscribers", "territory_code"):
            return []
        rows = conn.execute(
            """
            SELECT DISTINCT trim(territory_code) AS territory_code
            FROM subscribers
            WHERE territory_code IS NOT NULL
              AND trim(territory_code) <> ''
              AND lower(trim(status)) IN ('trial', 'live', 'paid', 'active')
            ORDER BY upper(trim(territory_code))
            """
        ).fetchall()
        out: list[str] = []
        seen: set[str] = set()
        for row in rows:
            for state in _territory_states_for_scope(str(row["territory_code"] or ""), definitions):
                if state not in seen:
                    seen.add(state)
                    out.append(state)
        return out
    except Exception:
        return []
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _merge_scope_states(outreach_states: list[str], trial_live_states: list[str]) -> list[str]:
    base: list[str] = []
    seen: set[str] = set()
    for state in outreach_states:
        if state not in seen:
            seen.add(state)
            base.append(state)
    extra = sorted([state for state in trial_live_states if state not in seen])
    return base + extra


def _resolve_states(cli_states: str, scope_mode: str) -> tuple[list[str], str, list[str], str]:
    scope_source = "resolver" if scope_mode == "outreach_plus_trial_live" else "outreach"
    if (cli_states or "").strip():
        states = _parse_states(cli_states)
        return states, "cli", states, scope_source

    outreach_states, outreach_source = _resolve_outreach_states()
    if scope_mode == "outreach":
        return outreach_states, outreach_source, outreach_states, "outreach"

    trial_live_states = _trial_live_states_from_crm()
    merged_states = _merge_scope_states(outreach_states, trial_live_states)
    return merged_states, "resolver", merged_states, "resolver"


def _emit_common_tokens(
    db_path: Path,
    db_source: str,
    db_warning: str,
    states: list[str],
    since_days: int,
    max_details: int,
    source: str,
    scope_mode: str,
    scope_states: list[str],
    scope_source: str,
) -> None:
    _emit("INGEST_DB_PATH", str(db_path))
    _emit("INGEST_DB_SOURCE", str(db_source or ""))
    if db_warning:
        print(db_warning)
    _emit("INGEST_SCOPE_MODE", scope_mode)
    _emit("INGEST_SCOPE_STATES", ",".join(scope_states))
    _emit("INGEST_SCOPE_SOURCE", scope_source)
    _emit("INGEST_STATES", ",".join(states))
    _emit("INGEST_SINCE_DAYS", str(since_days))
    _emit("INGEST_MAX_DETAILS", str(max_details))
    _emit("INGEST_STATES_SOURCE", source)
    _emit("INGEST_STATES_FALLBACK_USED", "YES" if source == "fallback" else "NO")


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        description="Daily OSHA ingest wrapper driven by OUTREACH_STATES for outreach signal freshness."
    )
    ap.add_argument("--print-config", action="store_true", help="Print resolved config and exit.")
    ap.add_argument("--doctor", action="store_true", help="Run runtime/readiness checks and exit.")
    ap.add_argument("--dry-run", action="store_true", help="Print resolved config and skip ingest.")
    ap.add_argument(
        "--states",
        default="",
        help=f"Optional comma-separated state override (e.g., {us_state.DEFAULT_OUTREACH_STATE_CSV}).",
    )
    ap.add_argument(
        "--scope-mode",
        choices=["outreach", "outreach_plus_trial_live"],
        default="outreach",
        help="State scope resolver mode (default: outreach).",
    )
    ap.add_argument("--since-days", type=int, default=3, help="Lookback days for ingest search (default: 3).")
    ap.add_argument("--max-details", type=int, default=200, help="Max detail pages to fetch (default: 200).")
    return ap


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    selected_modes = int(bool(args.print_config)) + int(bool(args.doctor)) + int(bool(args.dry_run))
    if selected_modes > 1:
        return _error(ERR_INGEST_DAILY_CONFIG, "modes_mutually_exclusive")
    if args.since_days < 1:
        return _error(ERR_INGEST_DAILY_CONFIG, f"invalid_since_days={args.since_days}")
    if args.max_details < 1:
        return _error(ERR_INGEST_DAILY_CONFIG, f"invalid_max_details={args.max_details}")

    try:
        states, source, scope_states, scope_source = _resolve_states(args.states, args.scope_mode)
    except ValueError as exc:
        return _error(ERR_INGEST_DAILY_CONFIG, str(exc))
    if not states:
        return _error(ERR_INGEST_DAILY_CONFIG, "states_empty")

    repo_root = Path(__file__).resolve().parent
    db_resolution = resolve_osha_db_path(repo_root)
    db_path = db_resolution.effective_path
    _emit_common_tokens(
        db_path=db_path,
        db_source=db_resolution.source,
        db_warning=db_resolution.warning_token,
        states=states,
        since_days=args.since_days,
        max_details=args.max_details,
        source=source,
        scope_mode=str(args.scope_mode),
        scope_states=scope_states,
        scope_source=scope_source,
    )
    runtime_mode = str(os.getenv("MFO_RUNTIME_MODE") or "manual").strip().lower() or "manual"
    runtime_ctx = runtime_context_dict(mode=runtime_mode, intent="write", dry_run=bool(args.dry_run))
    _emit("INGEST_RUNTIME_ROLE", str(runtime_ctx.get("runtime_role") or ""))
    _emit("INGEST_CANONICAL_HOSTNAME", str(runtime_ctx.get("canonical_hostname") or "(unset)"))
    _emit("INGEST_ARTIFACT_SYNC_DIR", (os.getenv("ARTIFACT_SYNC_DIR") or "").strip() or "(unset)")
    _emit("INGEST_TASK_LOG_ROOT", (os.getenv("TASK_LOG_ROOT") or "").strip() or "(default)")
    _emit("INGEST_RUN_SUMMARY_ROOT", (os.getenv("RUN_SUMMARY_ROOT") or "").strip() or "(default)")
    _emit("INGEST_MFO_TRUSTED_SCHEDULED", (os.getenv("MFO_TRUSTED_SCHEDULED") or "0").strip() or "0")

    if args.print_config:
        print(f"{PASS_INGEST_DAILY_COMPLETE} status=PRINT_CONFIG")
        return 0
    if args.doctor:
        runtime_preflight = run_runtime_preflight(
            mode=runtime_mode,
            intent="write",
            dry_run=True,
            task_log_root=str(os.getenv("TASK_LOG_ROOT") or ""),
            run_summary_root=str(os.getenv("RUN_SUMMARY_ROOT") or ""),
        )
        for line in render_runtime_lines(runtime_preflight):
            print(line)
        if not runtime_preflight.ok:
            return 2
        print(f"{PASS_INGEST_DAILY_DOCTOR} status=OK")
        print(f"{PASS_INGEST_DAILY_COMPLETE} status=DOCTOR")
        return 0
    if args.dry_run:
        print(f"{PASS_INGEST_DAILY_COMPLETE} status=DRY_RUN")
        return 0

    runtime_preflight = run_runtime_preflight(
        mode=runtime_mode,
        intent="write",
        dry_run=False,
        task_log_root=str(os.getenv("TASK_LOG_ROOT") or ""),
        run_summary_root=str(os.getenv("RUN_SUMMARY_ROOT") or ""),
    )
    for line in render_runtime_lines(runtime_preflight):
        print(line)
    if not runtime_preflight.ok:
        return 2

    try:
        ingest_osha.setup_logging("INFO")
    except Exception:
        pass

    try:
        stats = ingest_osha.run_ingestion(
            db_path=str(db_path),
            since_days=int(args.since_days),
            states=list(states),
            max_details=int(args.max_details),
        )
    except Exception as exc:
        detail = re.sub(r"\s+", " ", str(exc)).strip() or exc.__class__.__name__
        print(f"{ERR_INGEST_DAILY_FAILED} {detail}")
        return 1

    _emit("INGEST_RESULTS_FOUND", str(int(stats.get("results_found", 0))))
    _emit("INGEST_DETAILS_FETCHED", str(int(stats.get("details_fetched", 0))))
    _emit("INGEST_ROWS_INSERTED", str(int(stats.get("rows_inserted", 0))))
    _emit("INGEST_ROWS_UPDATED", str(int(stats.get("rows_updated", 0))))
    _emit("INGEST_ERRORS_COUNT", str(int(stats.get("errors_count", 0))))
    print(f"{PASS_INGEST_DAILY_COMPLETE} status=OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
