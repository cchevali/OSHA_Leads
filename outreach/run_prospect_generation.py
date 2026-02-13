import argparse
import csv
import hashlib
import io
import os
import sys
import tempfile
from pathlib import Path

try:  # pragma: no cover
    from dotenv import load_dotenv

    load_dotenv()
except Exception:  # pragma: no cover
    pass


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from outreach import crm_store
import seed_recipients_pools as pools


ERR_GENERATOR_FAILED = "ERR_GENERATOR_FAILED"
PASS_GENERATOR_PRINT_CONFIG = "PASS_GENERATOR_PRINT_CONFIG"

OUTPUT_SUBDIR = ("prospect_discovery",)
OUTPUT_FILENAME = "prospects_latest.csv"


def _valid_email(value: str) -> bool:
    email = (value or "").strip().lower()
    return bool(email) and ("@" in email)


def _normalize_email(value: str) -> str:
    return (value or "").strip().lower()


def _normalize_state(value: str) -> str:
    return (value or "").strip().upper()


def _output_path(data_dir: Path) -> Path:
    return data_dir.joinpath(*OUTPUT_SUBDIR) / OUTPUT_FILENAME


def _discovery_fields() -> list[str]:
    return ["prospect_id", "firm", "email", "title", "city", "state", "source"]


def _prospect_id(state: str, domain: str, email: str) -> str:
    base = f"{state}|{(domain or '').strip().lower()}|{_normalize_email(email)}"
    digest = hashlib.sha1(base.encode("utf-8")).hexdigest()[:16]
    return f"gen_{digest}"


def _build_clean_state_rows() -> tuple[dict[str, list[dict[str, str]]], int]:
    state_rows: dict[str, list[dict[str, str]]] = {}
    rows_read = 0
    pools_by_state = {
        "TX": pools.TX_POOL,
        "CA": pools.CA_POOL,
        "FL": pools.FL_POOL,
    }

    for state, seed_rows in pools_by_state.items():
        deduped = pools.dedupe_rows(seed_rows)
        cleaned, _stats = pools.apply_hygiene(deduped)
        state_rows[state] = cleaned
        rows_read += len(cleaned)
    return state_rows, rows_read


def _write_legacy_pool_files(state_rows: dict[str, list[dict[str, str]]]) -> None:
    pools.write_pool(state_rows.get("TX", []), pools.TX_PATH)
    pools.write_pool(state_rows.get("CA", []), pools.CA_PATH)
    pools.write_pool(state_rows.get("FL", []), pools.FL_PATH)
    pools.write_pool(state_rows.get("TX", []), pools.DEFAULT_PATH)


def _read_legacy_pool_files() -> list[dict[str, str]]:
    ordered_paths = [pools.TX_PATH, pools.CA_PATH, pools.FL_PATH]
    out: list[dict[str, str]] = []
    for path in ordered_paths:
        if not path.exists():
            continue
        with open(path, "r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                out.append({str(k or ""): str(v or "") for k, v in dict(row).items()})
    return out


def _state_rows_to_combined_input(state_rows: dict[str, list[dict[str, str]]]) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    for state in ["TX", "CA", "FL"]:
        out.extend(state_rows.get(state, []))
    return out


def _to_discovery_rows(input_rows: list[dict[str, str]]) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    seen_emails: set[str] = set()

    for row in input_rows:
        email = _normalize_email(row.get("contact_email", ""))
        if not _valid_email(email):
            continue
        if email in seen_emails:
            continue
        seen_emails.add(email)

        state = _normalize_state(row.get("state", ""))
        domain = (row.get("domain") or "").strip().lower()
        out.append(
            {
                "prospect_id": _prospect_id(state=state, domain=domain, email=email),
                "firm": (row.get("company_name") or "").strip(),
                "email": email,
                "title": (row.get("contact_role") or "").strip(),
                "city": (row.get("city") or "").strip(),
                "state": state,
                "source": "seed_recipients_pools",
            }
        )
    return out


def _write_output_atomic(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = _discovery_fields()
    with tempfile.NamedTemporaryFile(
        "w",
        newline="",
        encoding="utf-8",
        dir=str(path.parent),
        prefix="prospects_latest_",
        suffix=".tmp",
        delete=False,
    ) as tmp:
        writer = csv.DictWriter(tmp, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
        tmp_path = Path(tmp.name)

    os.replace(str(tmp_path), str(path))


def _print_tokens(path: Path, rows_read: int, rows_written: int, status: str) -> None:
    print(f"GENERATOR_OUTPUT_PATH={path.resolve()}")
    print(f"GENERATOR_ROWS_READ={rows_read}")
    print(f"GENERATOR_ROWS_WRITTEN={rows_written}")
    print(f"GENERATOR_COMPLETE status={status}")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Generate deterministic discovery CSV feed from legacy recipient pools.")
    ap.add_argument("--print-config", action="store_true", help="Print resolved output path and exit.")
    ap.add_argument("--dry-run", action="store_true", help="Compute rows only; do not write output files.")
    args = ap.parse_args(argv)

    data_dir = crm_store.data_dir()
    output_path = _output_path(data_dir)

    if args.print_config:
        print(f"{PASS_GENERATOR_PRINT_CONFIG} data_dir={data_dir.resolve()}")
        print(f"{PASS_GENERATOR_PRINT_CONFIG} output_path={output_path.resolve()}")
        return 0

    try:
        state_rows, rows_read = _build_clean_state_rows()
    except Exception as exc:
        print(f"{ERR_GENERATOR_FAILED} stage=build_rows err={exc}", file=sys.stderr)
        return 2

    if args.dry_run:
        rows = _to_discovery_rows(_state_rows_to_combined_input(state_rows))
        _print_tokens(path=output_path, rows_read=rows_read, rows_written=len(rows), status="DRY_RUN")
        return 0

    try:
        _write_legacy_pool_files(state_rows)
        generated_rows = _read_legacy_pool_files()
        rows = _to_discovery_rows(generated_rows)
        _write_output_atomic(path=output_path, rows=rows)
    except Exception as exc:
        print(f"{ERR_GENERATOR_FAILED} stage=write_output err={exc}", file=sys.stderr)
        return 2

    _print_tokens(path=output_path, rows_read=rows_read, rows_written=len(rows), status="OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
