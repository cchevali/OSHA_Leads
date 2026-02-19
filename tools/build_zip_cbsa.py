#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import gzip
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
    args = ap.parse_args()

    input_path = Path(args.input)
    if not input_path.exists():
        print(f"ERR_INPUT_MISSING path={input_path}")
        return 1

    rows_written, multi_count = build(
        input_path=input_path,
        out_path=Path(args.out),
        meta_path=Path(args.meta),
    )
    print(f"WARN_ZIP_MULTI_CBSA count={multi_count}")
    print(f"BUILD_ZIP_CBSA_COMPLETE rows={rows_written} out={args.out} meta={args.meta}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

