#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import sqlite3
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scoring import ai_triage
from scoring import paths as scoring_paths
from scoring import triage_overlay


PRIORITY_RANK = {"LOW": 0, "MEDIUM": 1, "HIGH": 2}


def _norm_priority(value: object) -> str:
    text = str(value or "").strip().upper()
    if text in {"HIGH", "MEDIUM", "LOW"}:
        return text
    return ""


def _rows_from_csv(path: Path) -> list[dict[str, str]]:
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        out: list[dict[str, str]] = []
        for row in reader:
            out.append({str(k or "").strip(): str(v or "").strip() for k, v in row.items()})
        return out


def _cache_stats(cache_path: Path, prompt_hash: str) -> tuple[bool, int, int]:
    if not cache_path.exists():
        return (False, 0, 0)
    conn = sqlite3.connect(str(cache_path))
    try:
        table = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='ai_triage_cache' LIMIT 1"
        ).fetchone()
        if not table:
            return (True, 0, 0)
        total_rows = int(conn.execute("SELECT COUNT(*) FROM ai_triage_cache").fetchone()[0] or 0)
        rows_for_prompt = int(
            conn.execute("SELECT COUNT(*) FROM ai_triage_cache WHERE prompt_hash = ?", (str(prompt_hash or ""),)).fetchone()[0]
            or 0
        )
        return (True, total_rows, rows_for_prompt)
    finally:
        conn.close()


def _load_leads_for_activity_numbers(db_path: Path, activity_numbers: list[str]) -> list[dict]:
    if not activity_numbers:
        return []
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        placeholders = ",".join("?" for _ in activity_numbers)
        query = (
            "SELECT activity_nr, establishment_name, site_city, site_state, site_zip, "
            "inspection_type, scope, case_status, naics, naics_desc, sic, emphasis, "
            "violations_count, serious_violations, willful_violations, repeat_violations, "
            "date_opened, first_seen_at, lead_score, mail_state "
            f"FROM inspections WHERE activity_nr IN ({placeholders})"
        )
        rows = conn.execute(query, tuple(activity_numbers)).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Import manual AI triage CSV into AI triage cache.")
    ap.add_argument("--input", default="", help="Input CSV path with activity_nr,ai_priority,ai_reason columns.")
    ap.add_argument(
        "--db",
        default=str(scoring_paths.default_leads_db_path()),
        help="SQLite inspections DB path (default: data/osha.sqlite).",
    )
    ap.add_argument("--print-config", action="store_true", help="Print resolved config and exit.")
    ap.add_argument("--dry-run", action="store_true", help="Validate only; no cache writes.")
    args = ap.parse_args(argv)

    db_path = Path(str(args.db)).expanduser().resolve(strict=False)
    cache_path = scoring_paths.ai_triage_cache_db_path()
    prompt_hash = ai_triage.prompt_hash()
    cache_exists, cache_total_rows, cache_rows_for_prompt = _cache_stats(cache_path, prompt_hash)

    if args.print_config:
        print(f"db={db_path}")
        print(f"ai_cache={cache_path}")
        print(f"prompt_hash={prompt_hash}")
        print(f"ai_cache_exists={1 if cache_exists else 0}")
        print(f"ai_cache_rows={cache_total_rows}")
        print(f"ai_cache_rows_for_prompt={cache_rows_for_prompt}")
        return 0

    input_path = Path(str(args.input or "")).expanduser().resolve(strict=False)
    if not input_path.exists():
        print(f"ERR_IMPORT_AI_TRIAGE_INPUT_MISSING path={input_path}", file=sys.stderr)
        return 2
    if not db_path.exists():
        print(f"ERR_IMPORT_AI_TRIAGE_DB_MISSING path={db_path}", file=sys.stderr)
        return 2

    rows = _rows_from_csv(input_path)
    activity_numbers = sorted({str(r.get("activity_nr") or "").strip() for r in rows if str(r.get("activity_nr") or "").strip()})
    leads = _load_leads_for_activity_numbers(db_path, activity_numbers)
    decisions = triage_overlay.triage(leads, {}, mode="trial_render", allow_ai=False)
    decision_by_activity = {str(d.get("activity_nr") or "").strip(): d for d in decisions}

    total = 0
    accepted = 0
    raised = 0
    lowered = 0
    unchanged = 0
    rejected_suppress = 0
    rejected_invalid = 0
    pending_writes: list[tuple[str, dict]] = []
    for row in rows:
        total += 1
        activity_nr = str(row.get("activity_nr") or "").strip()
        ai_priority = _norm_priority(row.get("ai_priority"))
        ai_reason = " ".join(str(row.get("ai_reason") or "").strip().split())
        if not activity_nr or not ai_priority or activity_nr not in decision_by_activity:
            rejected_invalid += 1
            continue
        decision = decision_by_activity.get(activity_nr, {})
        rules_priority = str(decision.get("rules_priority") or "").strip().upper()
        if rules_priority == "SUPPRESS":
            rejected_suppress += 1
            continue
        rules_priority_norm = _norm_priority(rules_priority) or "LOW"
        ai_rank = PRIORITY_RANK.get(ai_priority, -1)
        rules_rank = PRIORITY_RANK.get(rules_priority_norm, 0)
        if ai_rank > rules_rank:
            raised += 1
        elif ai_rank < rules_rank:
            lowered += 1
        else:
            unchanged += 1
        payload = {
            "priority": ai_priority,
            "reason": ai_reason or "Imported manual AI triage result.",
            "prompt_hash": prompt_hash,
            "prompt_version": ai_triage.AI_PROMPT_VERSION,
            "model": "manual_import",
            "cached": 1,
        }
        pending_writes.append((activity_nr, payload))
        accepted += 1

    if not args.dry_run and pending_writes:
        conn = ai_triage.connect_ai_cache()
        try:
            for item_key, payload in pending_writes:
                ai_triage.put_cached(
                    conn,
                    item_key=item_key,
                    prompt_hash=prompt_hash,
                    model=str(payload.get("model") or "manual_import"),
                    payload=payload,
                )
        finally:
            conn.close()

    print(
        "IMPORT_AI_TRIAGE "
        f"total={total} "
        f"accepted={accepted} "
        f"raised={raised} "
        f"lowered={lowered} "
        f"unchanged={unchanged} "
        f"rejected_suppress={rejected_suppress} "
        f"rejected_invalid={rejected_invalid}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
