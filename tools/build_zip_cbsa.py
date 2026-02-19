#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import json
import re
from collections import defaultdict
from pathlib import Path
from typing import Any


def _normalize_zip5(value: Any) -> str:
    digits = "".join(ch for ch in str(value or "").strip() if ch.isdigit())
    if len(digits) < 5:
        return ""
    return digits[:5]


def _normalize_cbsa(value: Any) -> str:
    digits = "".join(ch for ch in str(value or "").strip() if ch.isdigit())
    if not digits:
        return ""
    return digits.zfill(5)


def _normalize_ratio(value: Any) -> float:
    raw = str(value or "").strip()
    if not raw:
        return 0.0
    try:
        return float(raw)
    except Exception:
        return 0.0


def _read_rows(path: Path) -> list[dict[str, str]]:
    with open(path, "r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh)
        return [dict(row) for row in reader]


def _pick(record: list[dict[str, str]]) -> tuple[str, str, float, str]:
    best_zip = ""
    best_cbsa = ""
    best_ratio = -1.0
    best_label = ""
    for row in record:
        zip5 = _normalize_zip5(
            row.get("ZIP")
            or row.get("zip")
            or row.get("ZIP_CODE")
            or row.get("zip_code")
            or row.get("ZIP5")
            or row.get("zip5")
        )
        cbsa = _normalize_cbsa(
            row.get("CBSA")
            or row.get("cbsa")
            or row.get("CBSA_CODE")
            or row.get("cbsa_code")
        )
        if not zip5 or not cbsa:
            continue

        ratio = _normalize_ratio(
            row.get("RES_RATIO")
            or row.get("res_ratio")
            or row.get("resratio")
            or row.get("TOT_RATIO")
            or row.get("tot_ratio")
            or row.get("ratio")
        )
        label = str(
            row.get("CBSA_TITLE")
            or row.get("cbsa_title")
            or row.get("metro_label")
            or row.get("label")
            or ""
        ).strip()
        if ratio > best_ratio:
            best_zip = zip5
            best_cbsa = cbsa
            best_ratio = ratio
            best_label = label
            continue
        if ratio == best_ratio and best_cbsa and cbsa < best_cbsa:
            best_zip = zip5
            best_cbsa = cbsa
            best_label = label
    return best_zip, best_cbsa, best_ratio, best_label


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _infer_source_label(input_path: Path) -> str:
    name = input_path.name
    match = re.search(r"(\d{4}[_-]?Q[1-4])", name, flags=re.IGNORECASE)
    if match:
        token = match.group(1).upper().replace("_", " ")
        return f"HUD USPS ZIP-CBSA {token}"
    return f"HUD USPS ZIP-CBSA ({name})"


def _is_incomplete_source_label(source_label: str) -> bool:
    return bool(re.search(r"\b(seed|incomplete|bootstrap)\b", source_label or "", flags=re.IGNORECASE))


def _default_zip_meta_json_path(out_path: Path) -> Path:
    if out_path.name.endswith(".csv.gz"):
        stem = out_path.name[:-7]
    else:
        stem = out_path.stem
    return out_path.with_name(f"{stem}.meta.json")


def write_zip_meta_json(
    *,
    zip_meta_json_path: Path,
    source_label: str,
    source_url: str,
    input_path: Path,
    out_path: Path,
    cbsa_meta_path: Path,
    rows_written: int,
    multi_count: int,
) -> tuple[Path, str]:
    input_sha = _sha256_file(input_path)
    out_sha = _sha256_file(out_path)
    cbsa_meta_sha = _sha256_file(cbsa_meta_path)
    dataset_incomplete = _is_incomplete_source_label(source_label)
    payload = {
        "source_label": source_label,
        "source_url": source_url,
        "input_file": input_path.name,
        "input_sha256": input_sha,
        "zip_to_cbsa_csv_gz_sha256": out_sha,
        "cbsa_meta_csv_sha256": cbsa_meta_sha,
        "rows_written": rows_written,
        "zip_multi_cbsa_count": multi_count,
        "tie_break_rule_primary": "highest RES_RATIO",
        "tie_break_rule_secondary": "lowest numeric CBSA",
        "dataset_incomplete": dataset_incomplete,
    }
    zip_meta_json_path.parent.mkdir(parents=True, exist_ok=True)
    zip_meta_json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return zip_meta_json_path, _sha256_file(zip_meta_json_path)


def write_sources_md(
    *,
    sources_path: Path,
    source_label: str,
    source_url: str,
    input_path: Path,
    out_path: Path,
    cbsa_meta_path: Path,
    zip_meta_json_path: Path,
    rows_written: int,
    multi_count: int,
    zip_meta_json_sha: str,
) -> None:
    input_sha = _sha256_file(input_path)
    out_sha = _sha256_file(out_path)
    meta_sha = _sha256_file(cbsa_meta_path)
    dataset_incomplete = _is_incomplete_source_label(source_label)
    lines = [
        "# ZIP->CBSA Data Sources",
        "",
        "## Source",
        f"- Dataset label: `{source_label}`",
        f"- Source URL: `{source_url}`",
        "- Provenance family: HUD USPS ZIP Code Crosswalk (HUD USER)",
        "- License: U.S. Federal Government work (public domain)",
        f"- Input file: `{input_path.name}`",
        f"- Input SHA256: `{input_sha}`",
        f"- Dataset incomplete: `{str(dataset_incomplete).lower()}`",
    ]
    if dataset_incomplete:
        lines.append("- Coverage note: committed artifact is a bootstrap subset, not the full nationwide extract.")
    lines.extend(
        [
            "",
            "## Output Artifacts",
            f"- `zip_to_cbsa.csv.gz` SHA256: `{out_sha}`",
            f"- `cbsa_meta.csv` SHA256: `{meta_sha}`",
            f"- `{zip_meta_json_path.name}` SHA256: `{zip_meta_json_sha}`",
            f"- ZIP rows written: `{rows_written}`",
            f"- ZIP rows with multi-CBSA candidates: `{multi_count}`",
            "",
            "## Deterministic Tie-Break Rules",
            "- Primary key: highest residential ratio (`RES_RATIO`).",
            "- Secondary key (tie): lowest numeric CBSA code.",
            "- Warning token: `WARN_ZIP_MULTI_CBSA` emitted with count.",
            "",
            "## Rebuild Command",
            "```powershell",
            "py -3 tools\\build_zip_cbsa.py --input <hud_zip_cbsa_csv> --out data\\geo\\zip_to_cbsa.csv.gz --meta data\\geo\\cbsa_meta.csv --zip-meta-json data\\geo\\zip_to_cbsa.meta.json --sources data\\geo\\SOURCES.md --source-label \"HUD USPS ZIP-CBSA <MONTH_OR_QUARTER>\"",
            "```",
        ]
    )
    sources_path.parent.mkdir(parents=True, exist_ok=True)
    sources_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def build(input_path: Path, out_path: Path, meta_path: Path) -> tuple[int, int]:
    rows = _read_rows(input_path)
    grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        zip5 = _normalize_zip5(
            row.get("ZIP")
            or row.get("zip")
            or row.get("ZIP_CODE")
            or row.get("zip_code")
            or row.get("ZIP5")
            or row.get("zip5")
        )
        if zip5:
            grouped[zip5].append(row)

    selected: list[tuple[str, str]] = []
    cbsa_labels: dict[str, str] = {}
    multi_count = 0
    for zip5 in sorted(grouped.keys()):
        record = grouped[zip5]
        if len(record) > 1:
            multi_count += 1
        picked_zip, picked_cbsa, _ratio, label = _pick(record)
        if not picked_zip or not picked_cbsa:
            continue
        selected.append((picked_zip, picked_cbsa))
        if label and picked_cbsa not in cbsa_labels:
            cbsa_labels[picked_cbsa] = label

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(out_path, "wt", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(["ZIP5", "CBSA"])
        writer.writerows(selected)

    meta_path.parent.mkdir(parents=True, exist_ok=True)
    with open(meta_path, "w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(["CBSA", "metro_label"])
        for cbsa in sorted(cbsa_labels.keys()):
            writer.writerow([cbsa, cbsa_labels[cbsa]])

    return len(selected), multi_count


def main() -> int:
    ap = argparse.ArgumentParser(description="Build deterministic ZIP5->CBSA mapping from HUD USPS crosswalk CSV.")
    ap.add_argument("--input", required=True, help="Path to HUD USPS crosswalk CSV.")
    ap.add_argument("--out", required=True, help="Output gzip CSV path (ZIP5,CBSA).")
    ap.add_argument("--meta", required=True, help="Output CBSA metadata CSV path.")
    ap.add_argument(
        "--zip-meta-json",
        default="",
        help="Output ZIP dataset metadata JSON path (default: alongside --out as zip_to_cbsa.meta.json).",
    )
    ap.add_argument("--sources", default="data/geo/SOURCES.md", help="Output SOURCES.md path.")
    ap.add_argument(
        "--source-label",
        default="",
        help="Source month/version label (for example: HUD USPS ZIP-CBSA 2025 Q4).",
    )
    ap.add_argument(
        "--source-url",
        default="https://www.huduser.gov/portal/datasets/usps_crosswalk.html",
        help="Upstream source URL for provenance in SOURCES.md.",
    )
    args = ap.parse_args()

    input_path = Path(args.input)
    if not input_path.exists():
        print(f"ERR_INPUT_MISSING path={input_path}")
        return 1

    out_path = Path(args.out)
    meta_path = Path(args.meta)
    rows_written, multi_count = build(
        input_path=input_path,
        out_path=out_path,
        meta_path=meta_path,
    )
    source_label = str(args.source_label or "").strip() or _infer_source_label(input_path)
    zip_meta_json_path = (
        Path(args.zip_meta_json)
        if str(args.zip_meta_json or "").strip()
        else _default_zip_meta_json_path(out_path)
    )
    zip_meta_json_path, zip_meta_json_sha = write_zip_meta_json(
        zip_meta_json_path=zip_meta_json_path,
        source_label=source_label,
        source_url=str(args.source_url).strip(),
        input_path=input_path,
        out_path=out_path,
        cbsa_meta_path=meta_path,
        rows_written=rows_written,
        multi_count=multi_count,
    )
    sources_path = Path(args.sources)
    write_sources_md(
        sources_path=sources_path,
        source_label=source_label,
        source_url=str(args.source_url).strip(),
        input_path=input_path,
        out_path=out_path,
        cbsa_meta_path=meta_path,
        zip_meta_json_path=zip_meta_json_path,
        rows_written=rows_written,
        multi_count=multi_count,
        zip_meta_json_sha=zip_meta_json_sha,
    )
    print(f"WARN_ZIP_MULTI_CBSA count={multi_count}")
    print(
        "BUILD_ZIP_CBSA_COMPLETE "
        f"rows={rows_written} out={args.out} meta={args.meta} zip_meta_json={zip_meta_json_path} "
        f"source_label=\"{source_label}\" sources={sources_path}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
