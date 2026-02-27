import csv
import os
from datetime import datetime
from pathlib import Path


CANONICAL_FIELDS = [
    "first_name",
    "last_name",
    "email",
    "company",
    "role_or_title",
    "city",
    "state",
    "phone",
    "website",
    "linkedin_url",
    "employee_count",
    "industry",
]

_APOLLO_COLUMN_MAP = {
    "first name": "first_name",
    "last name": "last_name",
    "email": "email",
    "company": "company",
    "title": "role_or_title",
    "city": "city",
    "state": "state",
    "phone": "phone",
    "website": "website",
    "linkedin url": "linkedin_url",
    "# employees": "employee_count",
    "industry": "industry",
}


def _normalize_text(value: str) -> str:
    return (value or "").strip()


def _normalize_header(value: str) -> str:
    return _normalize_text(value).lower()


def _normalize_email(value: str) -> str:
    return _normalize_text(value).lower()


def _valid_email(value: str) -> bool:
    email = _normalize_email(value)
    if not email:
        return False
    if "@" not in email or "." not in email:
        return False
    return True


def list_pending_csv_files(inbox_path: Path) -> list[Path]:
    if not inbox_path.exists() or not inbox_path.is_dir():
        return []
    files = [p for p in inbox_path.iterdir() if p.is_file() and p.suffix.lower() == ".csv"]
    files.sort(key=lambda p: (p.name.lower(), p.name))
    return files


def _header_map(fieldnames: list[str] | None) -> dict[str, str]:
    mapped: dict[str, str] = {}
    if not fieldnames:
        return mapped

    canonical_lookup = {_normalize_header(v): v for v in CANONICAL_FIELDS}
    for raw in fieldnames:
        header = _normalize_header(raw)
        if not header:
            continue
        canonical = _APOLLO_COLUMN_MAP.get(header) or canonical_lookup.get(header)
        if canonical:
            mapped[raw] = canonical
    return mapped


def _normalize_row(row: dict[str, str], mapped_headers: dict[str, str]) -> dict[str, str]:
    out = {field: "" for field in CANONICAL_FIELDS}
    for raw_key, raw_value in dict(row or {}).items():
        canonical = mapped_headers.get(str(raw_key or ""))
        if not canonical:
            continue
        out[canonical] = _normalize_text(str(raw_value or ""))
    out["email"] = _normalize_email(out.get("email") or "")
    return out


def ingest_inbox_csv_files(inbox_path: Path, files: list[Path] | None = None) -> dict[str, object]:
    inbox_files = list(files or list_pending_csv_files(inbox_path))
    rows_read = 0
    rows_skipped_no_email = 0
    rows_skipped_dupe = 0
    rows_accepted: list[dict[str, str]] = []
    seen_batch_emails: set[str] = set()

    for path in inbox_files:
        with open(path, "r", newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            mapped_headers = _header_map(reader.fieldnames)
            for raw_row in reader:
                rows_read += 1
                row = _normalize_row(raw_row, mapped_headers)
                email = row.get("email") or ""
                if not _valid_email(email):
                    rows_skipped_no_email += 1
                    continue
                if email in seen_batch_emails:
                    rows_skipped_dupe += 1
                    continue
                seen_batch_emails.add(email)
                rows_accepted.append(row)

    return {
        "inbox_path": inbox_path,
        "files_found": inbox_files,
        "rows_read": rows_read,
        "rows_skipped_no_email": rows_skipped_no_email,
        "rows_skipped_dupe": rows_skipped_dupe,
        "rows_accepted": rows_accepted,
    }


def canonical_rows_to_generator_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    for row in rows:
        first = _normalize_text(row.get("first_name") or "")
        last = _normalize_text(row.get("last_name") or "")
        out.append(
            {
                "prospect_id": "",
                "company_name": _normalize_text(row.get("company") or ""),
                "contact_email": _normalize_email(row.get("email") or ""),
                "contact_role": _normalize_text(row.get("role_or_title") or ""),
                "contact_name": _normalize_text(f"{first} {last}".strip()),
                "city": _normalize_text(row.get("city") or ""),
                "state": _normalize_text(row.get("state") or "").upper(),
                "domain": "",
                "website": _normalize_text(row.get("website") or ""),
                "source": "manual_csv_inbox",
                "phone": _normalize_text(row.get("phone") or ""),
                "linkedin_url": _normalize_text(row.get("linkedin_url") or ""),
                "employee_count": _normalize_text(row.get("employee_count") or ""),
                "industry": _normalize_text(row.get("industry") or ""),
            }
        )
    return out


def move_processed_files(
    inbox_path: Path,
    files: list[Path],
    *,
    now_utc: datetime | None = None,
    dry_run: bool = False,
) -> list[str]:
    if dry_run or not files:
        return []

    ts = (now_utc or datetime.now()).strftime("%Y%m%d_%H%M%S")
    processed_dir = inbox_path / "processed"
    processed_dir.mkdir(parents=True, exist_ok=True)

    moved: list[str] = []
    for src in files:
        if not src.exists():
            continue
        base_target = processed_dir / f"{ts}_{src.name}"
        target = base_target
        counter = 1
        while target.exists():
            target = processed_dir / f"{ts}_{counter}_{src.name}"
            counter += 1
        os.replace(str(src), str(target))
        moved.append(src.name)
    return moved


def inbox_path_readable_or_creatable(inbox_path: Path) -> bool:
    if inbox_path.exists():
        return inbox_path.is_dir() and os.access(str(inbox_path), os.R_OK)

    cursor = inbox_path
    while not cursor.exists():
        parent = cursor.parent
        if parent == cursor:
            break
        cursor = parent
    if not cursor.exists():
        return False
    return cursor.is_dir() and os.access(str(cursor), os.W_OK)
