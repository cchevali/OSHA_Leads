import argparse
import csv
import io
import os
import sqlite3
import sys
from contextlib import redirect_stdout
from pathlib import Path

try:  # pragma: no cover
    from dotenv import load_dotenv

    load_dotenv()
except Exception:  # pragma: no cover
    pass


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from outreach import crm_admin, crm_store


ERR_DISCOVERY_INPUT_MISSING = "ERR_DISCOVERY_INPUT_MISSING"
ERR_DISCOVERY_INPUT_NOT_FOUND = "ERR_DISCOVERY_INPUT_NOT_FOUND"
ERR_DISCOVERY_INPUT_UNREADABLE = "ERR_DISCOVERY_INPUT_UNREADABLE"
ERR_DISCOVERY_SEED_FAILED = "ERR_DISCOVERY_SEED_FAILED"
ERR_DISCOVERY_NO_INPUT_SOURCE = "ERR_DISCOVERY_NO_INPUT_SOURCE"
WARN_DISCOVERY_NO_INPUT = "WARN_DISCOVERY_NO_INPUT"

PASS_DISCOVERY_PRINT_CONFIG = "PASS_DISCOVERY_PRINT_CONFIG"
PASS_DISCOVERY_DRY_RUN = "PASS_DISCOVERY_DRY_RUN"
PASS_DISCOVERY_UPSERT = "PASS_DISCOVERY_UPSERT"

SAMPLE_INPUT_PATH = REPO_ROOT / "outreach" / "sample_prospects.csv"


def _normalized_path_from_text(text: str) -> Path | None:
    raw = (text or "").strip()
    if not raw:
        return None
    p = Path(raw)
    if p.is_absolute():
        return p
    return REPO_ROOT / p


def _resolved_input_path(raw_input: str) -> Path | None:
    return _normalized_path_from_text(raw_input)


def _norm_email(value: str) -> str:
    return (value or "").strip().lower()


def _count_rows(path: Path) -> int:
    with open(path, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return sum(1 for _ in reader)


def _bool_env(value: str) -> bool:
    return (value or "").strip().lower() in {"1", "true", "yes", "on"}


def _scheduled_fallback_paths(data_dir: Path) -> list[tuple[str, Path]]:
    discovery_dir = data_dir / "prospect_discovery"
    return [
        ("data_dir_prospect_discovery_prospects_latest_csv", discovery_dir / "prospects_latest.csv"),
        ("data_dir_prospect_discovery_prospects_csv", discovery_dir / "prospects.csv"),
        ("data_dir_prospects_latest_csv", data_dir / "prospects_latest.csv"),
        ("data_dir_prospects_csv", data_dir / "prospects.csv"),
    ]


def _legacy_last_success_path(data_dir: Path) -> Path:
    # Legacy ledger remains for compatibility with existing operator artifacts.
    return data_dir / "discovery" / "last_success_input_path.txt"


def _csv_row_dicts(path: Path) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    with open(path, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            clean: dict[str, str] = {}
            for k, v in dict(row).items():
                key = (k or "").lstrip("\ufeff")
                clean[key] = v
            rows.append(clean)
    return rows


def _existing_email_owner_maps(crm_db: Path) -> tuple[dict[str, str], dict[str, str], set[str]]:
    email_owner: dict[str, str] = {}
    prospect_email: dict[str, str] = {}
    prospect_ids: set[str] = set()
    if not crm_db.exists():
        return email_owner, prospect_email, prospect_ids

    conn = sqlite3.connect(str(crm_db))
    try:
        table = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='prospects' LIMIT 1"
        ).fetchone()
        if not table:
            return email_owner, prospect_email, prospect_ids
        for prospect_id_raw, email_raw in conn.execute(
            "SELECT prospect_id, email FROM prospects"
        ).fetchall():
            prospect_id = str(prospect_id_raw or "").strip()
            email = _norm_email(str(email_raw or ""))
            if not prospect_id:
                continue
            prospect_ids.add(prospect_id)
            if email:
                email_owner[email] = prospect_id
                prospect_email[prospect_id] = email
    finally:
        conn.close()
    return email_owner, prospect_email, prospect_ids


def _analyze_discovery_input(path: Path, crm_db: Path) -> tuple[int, int, int, int]:
    rows = _csv_row_dicts(path)
    email_owner, prospect_email, prospect_ids = _existing_email_owner_maps(crm_db)

    rows_read = 0
    invalid_email = 0
    duplicate_email = 0
    inserted = 0
    updated = 0

    for i, row in enumerate(rows, start=1):
        rows_read += 1
        prospect_id = (row.get("prospect_id") or f"seed_{i}").strip()
        email = _norm_email(row.get("email", ""))
        if not email or "@" not in email:
            invalid_email += 1
            continue

        owner = email_owner.get(email, "")
        if owner and owner != prospect_id:
            duplicate_email += 1
            continue

        existed = prospect_id in prospect_ids
        if existed:
            updated += 1
        else:
            inserted += 1
            prospect_ids.add(prospect_id)

        previous_email = prospect_email.get(prospect_id, "")
        if previous_email and previous_email != email and email_owner.get(previous_email, "") == prospect_id:
            email_owner.pop(previous_email, None)
        email_owner[email] = prospect_id
        prospect_email[prospect_id] = email

    return rows_read, inserted + updated, invalid_email, duplicate_email


def _attempted_examples_text(attempted_paths: list[Path]) -> tuple[int, str]:
    attempted_abs = [str(p.resolve()) for p in attempted_paths]
    attempted_count = len(attempted_abs)
    if attempted_count < 1:
        return 0, "NONE"
    return attempted_count, "|".join(attempted_abs[:3])


def _print_discovery_metrics(
    input_path: Path | None,
    crm_db: Path,
    rows_read: int,
    prospects_upserted: int,
    skipped_invalid_email: int,
    skipped_duplicate_email: int,
    status: str,
) -> None:
    input_text = "NONE"
    if input_path is not None:
        input_text = str(input_path.resolve())

    print(f"DISCOVERY_INPUT_PATH={input_text}")
    print(f"DISCOVERY_CRM_DB={crm_db.resolve()}")
    print(f"DISCOVERY_ROWS_READ={rows_read}")
    print(f"DISCOVERY_PROSPECTS_UPSERTED={prospects_upserted}")
    print(f"DISCOVERY_SKIPPED_INVALID_EMAIL={skipped_invalid_email}")
    print(f"DISCOVERY_SKIPPED_DUPLICATE_EMAIL={skipped_duplicate_email}")
    print(f"DISCOVERY_COMPLETE status={status}")


def _resolve_legacy_checked_entries(
    env: dict[str, str],
    data_dir: Path,
    attempted_paths: list[Path],
) -> str:
    entries: list[str] = []
    prospect_env = _normalized_path_from_text(str(env.get("PROSPECT_DISCOVERY_INPUT", "") or ""))
    legacy_env = _normalized_path_from_text(str(env.get("DISCOVERY_INPUT_CSV", "") or ""))
    if prospect_env is None:
        entries.append("env:PROSPECT_DISCOVERY_INPUT=(unset)")
    else:
        entries.append(f"env:PROSPECT_DISCOVERY_INPUT={prospect_env.resolve()}")
    if legacy_env is None:
        entries.append("env:DISCOVERY_INPUT_CSV=(unset)")
    else:
        entries.append(f"env:DISCOVERY_INPUT_CSV={legacy_env.resolve()}")
    entries.extend([str(p.resolve()) for p in attempted_paths])
    if not entries:
        entries.append(str(data_dir.resolve()))
    return "; ".join(entries)


def _print_no_input_warning(attempted_paths: list[Path]) -> None:
    attempted_count, examples = _attempted_examples_text(attempted_paths)
    print(f"{WARN_DISCOVERY_NO_INPUT} attempted={attempted_count} examples={examples}")


def _persist_last_success_input(data_dir: Path, input_path: Path) -> None:
    last_success_file = _legacy_last_success_path(data_dir)
    last_success_file.parent.mkdir(parents=True, exist_ok=True)
    last_success_file.write_text(str(input_path.resolve()) + "\n", encoding="utf-8")


def _parse_seed_counts(text: str) -> tuple[int, int, int]:
    inserted = 0
    updated = 0
    skipped = 0
    for line in (text or "").splitlines():
        s = (line or "").strip()
        if not s.startswith(crm_admin.PASS_CRM_SEED):
            continue
        if "inserted_count=" in s:
            inserted = int((s.split("inserted_count=", 1)[1].strip() or 0))
        elif "updated_count=" in s:
            updated = int((s.split("updated_count=", 1)[1].strip() or 0))
        elif "skipped_count=" in s:
            skipped = int((s.split("skipped_count=", 1)[1].strip() or 0))
    return inserted, updated, skipped


def _validate_input_path(input_path: Path | None) -> Path | None:
    if input_path is None:
        _print_missing_input()
        return None
    if not input_path.exists():
        print(f"{ERR_DISCOVERY_INPUT_NOT_FOUND} path={input_path.resolve()}", file=sys.stderr)
        return None
    try:
        _count_rows(input_path)
    except Exception as exc:
        print(f"{ERR_DISCOVERY_INPUT_UNREADABLE} path={input_path.resolve()} err={exc}", file=sys.stderr)
        return None
    return input_path


def _resolve_mode(has_input: bool, dry_run: bool) -> str:
    if has_input:
        return "cli_explicit"
    if dry_run:
        return "dry_run"
    return "scheduled_no_arg"


def _resolve_scheduled_candidate_paths(
    env: dict[str, str],
    data_dir: Path,
) -> list[tuple[str, Path]]:
    candidates: list[tuple[str, Path]] = []
    preferred_env = _normalized_path_from_text(str(env.get("PROSPECT_DISCOVERY_INPUT", "") or ""))
    if preferred_env is not None:
        candidates.append(("env_PROSPECT_DISCOVERY_INPUT", preferred_env))

    legacy_env = _normalized_path_from_text(str(env.get("DISCOVERY_INPUT_CSV", "") or ""))
    if legacy_env is not None:
        candidates.append(("env_DISCOVERY_INPUT_CSV", legacy_env))

    candidates.extend(_scheduled_fallback_paths(data_dir))
    return candidates


def resolve_discovery_input_source(mode: str, env: dict[str, str], data_dir: Path) -> tuple[str, Path | None]:
    _ = mode
    for source_kind, candidate_path in _resolve_scheduled_candidate_paths(env=env, data_dir=data_dir):
        if candidate_path.exists():
            return source_kind, candidate_path
    return "none", None


def _resolve_input(
    mode: str,
    args: argparse.Namespace,
    env: dict[str, str],
    data_dir: Path,
) -> tuple[str, Path | None, list[Path]]:
    explicit = _resolved_input_path(str(args.input or ""))
    if explicit is not None:
        return "cli_input", explicit, [explicit]

    attempted_paths: list[Path] = []
    for source_kind, candidate_path in _resolve_scheduled_candidate_paths(env=env, data_dir=data_dir):
        attempted_paths.append(candidate_path)
        if candidate_path.exists():
            return source_kind, candidate_path, attempted_paths

    sample_allowed_env = _bool_env(str(env.get("DISCOVERY_ALLOW_SAMPLE", "") or ""))
    sample_requested = bool(args.sample)
    sample_allowed = sample_allowed_env or sample_requested
    if not sample_allowed:
        return "none", None, attempted_paths

    if mode == "scheduled_no_arg" and not sample_allowed_env:
        return "sample_blocked_scheduled", None, attempted_paths

    attempted_with_sample = attempted_paths + [SAMPLE_INPUT_PATH]
    if SAMPLE_INPUT_PATH.exists():
        if sample_requested:
            return "sample_flag", SAMPLE_INPUT_PATH, attempted_with_sample
        return "sample_env_toggle", SAMPLE_INPUT_PATH, attempted_with_sample

    return "sample_missing", SAMPLE_INPUT_PATH, attempted_with_sample


def _print_config(mode: str, source_kind: str, input_path: Path | None) -> None:
    print(f"{PASS_DISCOVERY_PRINT_CONFIG} data_dir={crm_store.data_dir().resolve()}")
    print(f"{PASS_DISCOVERY_PRINT_CONFIG} crm_db={crm_store.crm_db_path().resolve()}")
    resolved_input = input_path.resolve() if input_path else "(missing)"
    print(f"{PASS_DISCOVERY_PRINT_CONFIG} input_path={resolved_input}")
    print(f"{PASS_DISCOVERY_PRINT_CONFIG} source_kind={source_kind}")
    print(f"{PASS_DISCOVERY_PRINT_CONFIG} mode={mode}")


def _print_missing_input() -> None:
    print(f"{ERR_DISCOVERY_INPUT_MISSING} input_missing", file=sys.stderr)
    print(
        "REMEDIATION: .\\run_with_secrets.ps1 -- py -3 run_prospect_discovery.py --input C:\\path\\to\\prospects.csv",
        file=sys.stderr,
    )


def _print_no_input_source(
    mode: str,
    source_kind: str,
    env: dict[str, str],
    data_dir: Path,
    attempted_paths: list[Path],
) -> None:
    checked = _resolve_legacy_checked_entries(env=env, data_dir=data_dir, attempted_paths=attempted_paths)
    print(
        f"{ERR_DISCOVERY_NO_INPUT_SOURCE} mode={mode} source_kind={source_kind} checked={checked}",
        file=sys.stderr,
    )
    print(
        "REMEDIATION: set PROSPECT_DISCOVERY_INPUT (preferred) or DISCOVERY_INPUT_CSV",
        file=sys.stderr,
    )


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Seed/update outreach CRM from an input prospects CSV.")
    ap.add_argument("--input", default="", help="Input prospects CSV path.")
    ap.add_argument("--print-config", action="store_true", help="Print resolved config paths and exit.")
    ap.add_argument("--dry-run", action="store_true", help="Validate/inspect input only. No DB writes.")
    ap.add_argument(
        "--sample",
        action="store_true",
        help="Allow sample CSV fallback (dev/testing only; scheduled mode requires DISCOVERY_ALLOW_SAMPLE=1).",
    )
    args = ap.parse_args(argv)

    mode = _resolve_mode(has_input=bool((args.input or "").strip()), dry_run=bool(args.dry_run))
    data_dir = crm_store.data_dir()
    crm_db = crm_store.crm_db_path()
    env = dict(os.environ)
    source_kind, input_path, attempted_paths = _resolve_input(mode=mode, args=args, env=env, data_dir=data_dir)

    if args.print_config:
        _print_config(mode=mode, source_kind=source_kind, input_path=input_path)
        return 0

    if input_path is None:
        _print_no_input_source(
            mode=mode,
            source_kind=source_kind,
            env=env,
            data_dir=data_dir,
            attempted_paths=attempted_paths,
        )
        _print_no_input_warning(attempted_paths=attempted_paths)
        _print_discovery_metrics(
            input_path=None,
            crm_db=crm_db,
            rows_read=0,
            prospects_upserted=0,
            skipped_invalid_email=0,
            skipped_duplicate_email=0,
            status="NO_INPUT",
        )
        return 0

    valid_path = _validate_input_path(input_path)
    if valid_path is None:
        return 2

    try:
        rows_read, predicted_upserted, invalid_email, duplicate_email = _analyze_discovery_input(valid_path, crm_db)
    except Exception as exc:
        print(f"{ERR_DISCOVERY_INPUT_UNREADABLE} path={valid_path.resolve()} err={exc}", file=sys.stderr)
        return 2

    if args.dry_run:
        print(
            f"{PASS_DISCOVERY_DRY_RUN} data_dir={data_dir.resolve()} crm_db={crm_db.resolve()} "
            f"input_path={valid_path.resolve()} rows_read={rows_read} source_kind={source_kind} mode={mode}"
        )
        _print_discovery_metrics(
            input_path=valid_path,
            crm_db=crm_db,
            rows_read=rows_read,
            prospects_upserted=predicted_upserted,
            skipped_invalid_email=invalid_email,
            skipped_duplicate_email=duplicate_email,
            status="DRY_RUN",
        )
        return 0

    seed_out = io.StringIO()
    try:
        with redirect_stdout(seed_out):
            rc = crm_admin._seed_from_csv(valid_path, archive_dir=None, no_archive=True)
    except Exception as exc:
        print(f"{ERR_DISCOVERY_SEED_FAILED} path={valid_path.resolve()} err={exc}", file=sys.stderr)
        return 2

    captured = seed_out.getvalue()
    if captured:
        for line in captured.splitlines():
            if line.strip():
                print(line)

    if rc != 0:
        print(f"{ERR_DISCOVERY_SEED_FAILED} path={valid_path.resolve()} code={rc}", file=sys.stderr)
        return rc

    inserted, updated, skipped = _parse_seed_counts(captured)
    derived_duplicate = duplicate_email
    if skipped >= invalid_email:
        derived_duplicate = skipped - invalid_email
    print(
        f"{PASS_DISCOVERY_UPSERT} data_dir={data_dir.resolve()} crm_db={crm_db.resolve()} "
        f"input_path={valid_path.resolve()} rows_read={rows_read} inserted_count={inserted} "
        f"updated_count={updated} skipped_count={skipped} source_kind={source_kind} mode={mode}"
    )
    _print_discovery_metrics(
        input_path=valid_path,
        crm_db=crm_db,
        rows_read=rows_read,
        prospects_upserted=inserted + updated,
        skipped_invalid_email=invalid_email,
        skipped_duplicate_email=derived_duplicate,
        status="OK",
    )
    try:
        _persist_last_success_input(data_dir=data_dir, input_path=valid_path)
    except Exception:
        pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
