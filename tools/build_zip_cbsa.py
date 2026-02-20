#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import json
import os
import re
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from pathlib import Path
from typing import Any

HUD_API_URL = "https://www.huduser.gov/hudapi/public/usps"
HUD_API_SOURCE_URL = "https://www.huduser.gov/portal/dataset/uspszip-api.html"
HUD_CROSSWALK_SOURCE_URL = "https://www.huduser.gov/portal/datasets/usps_crosswalk.html"
HUD_API_CROSSWALK_TYPE = "3"  # zip-cbsa
HUD_API_PERIOD_DISCOVERY_QUERY = "22031"
DEFAULT_OUT_PATH = "data/geo/zip_to_cbsa.csv.gz"
DEFAULT_META_PATH = "data/geo/cbsa_meta.csv"
HUD_API_STATE_CODES = (
    "AL",
    "AK",
    "AZ",
    "AR",
    "CA",
    "CO",
    "CT",
    "DE",
    "DC",
    "FL",
    "GA",
    "HI",
    "ID",
    "IL",
    "IN",
    "IA",
    "KS",
    "KY",
    "LA",
    "ME",
    "MD",
    "MA",
    "MI",
    "MN",
    "MS",
    "MO",
    "MT",
    "NE",
    "NV",
    "NH",
    "NJ",
    "NM",
    "NY",
    "NC",
    "ND",
    "OH",
    "OK",
    "OR",
    "PA",
    "RI",
    "SC",
    "SD",
    "TN",
    "TX",
    "UT",
    "VT",
    "VA",
    "WA",
    "WV",
    "WI",
    "WY",
)


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


def _normalize_year(value: Any) -> int | None:
    text = "".join(ch for ch in str(value or "").strip() if ch.isdigit())
    if len(text) != 4:
        return None
    parsed = int(text)
    if parsed < 2010 or parsed > 2100:
        return None
    return parsed


def _normalize_quarter(value: Any) -> int | None:
    text = str(value or "").strip().upper()
    if not text:
        return None
    if text.startswith("Q"):
        text = text[1:]
    if not text.isdigit():
        return None
    parsed = int(text)
    if parsed < 1 or parsed > 4:
        return None
    return parsed


def _normalize_state_code(value: Any) -> str:
    text = re.sub(r"[^A-Za-z]", "", str(value or "").strip().upper())
    if len(text) == 2:
        return text
    return ""


def _build_hud_url(*, query: str, year: int | None, quarter: int | None) -> str:
    params: dict[str, str] = {
        "type": HUD_API_CROSSWALK_TYPE,
        "query": query,
    }
    if year is not None:
        params["year"] = str(year)
    if quarter is not None:
        params["quarter"] = str(quarter)
    return f"{HUD_API_URL}?{urllib.parse.urlencode(params)}"


class HudApiRequestError(RuntimeError):
    def __init__(self, *, query: str, status: int | None, detail: str | None, url: str) -> None:
        self.query = query
        self.status = status
        self.detail = detail
        self.url = url
        if status is not None:
            message = f"ERR_HUD_API_REQUEST_FAILED state={query} status={status}"
        else:
            message = f"ERR_HUD_API_REQUEST_FAILED state={query} detail={detail}"
        super().__init__(message)


def _hud_get_json(*, query: str, token: str, year: int | None, quarter: int | None) -> tuple[dict[str, Any], str]:
    url = _build_hud_url(query=query, year=year, quarter=quarter)
    req = urllib.request.Request(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            body = resp.read()
    except urllib.error.HTTPError as exc:
        raise HudApiRequestError(query=query, status=exc.code, detail=None, url=url) from exc
    except urllib.error.URLError as exc:
        raise HudApiRequestError(query=query, status=None, detail=str(exc.reason), url=url) from exc

    try:
        payload = json.loads(body.decode("utf-8"))
    except Exception as exc:
        raise RuntimeError(f"ERR_HUD_API_RESPONSE_INVALID state={query}") from exc
    if not isinstance(payload, dict):
        raise RuntimeError(f"ERR_HUD_API_RESPONSE_INVALID state={query}")
    return payload, url


def _discover_latest_period(token: str) -> tuple[int, int]:
    url = _build_hud_url(query=HUD_API_PERIOD_DISCOVERY_QUERY, year=None, quarter=None)
    try:
        payload, _ = _hud_get_json(
            query=HUD_API_PERIOD_DISCOVERY_QUERY,
            token=token,
            year=None,
            quarter=None,
        )
    except HudApiRequestError as exc:
        print(f"DEBUG_HUD_API_PERIOD_DISCOVERY_URL url={exc.url}")
        status_token = str(exc.status) if exc.status is not None else "UNKNOWN"
        raise RuntimeError(f"ERR_HUD_API_PERIOD_DISCOVERY_FAILED status={status_token}") from exc
    except RuntimeError as exc:
        print(f"DEBUG_HUD_API_PERIOD_DISCOVERY_URL url={url}")
        raise RuntimeError("ERR_HUD_API_PERIOD_DISCOVERY_FAILED status=UNKNOWN") from exc

    _rows, discovered_year, discovered_quarter = _parse_hud_payload_rows(payload)
    if discovered_year is None or discovered_quarter is None:
        print(f"DEBUG_HUD_API_PERIOD_DISCOVERY_URL url={url}")
        raise RuntimeError("ERR_HUD_API_PERIOD_DISCOVERY_FAILED status=UNKNOWN")
    return discovered_year, discovered_quarter


def _payload_data_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows = payload.get("data")
    if not isinstance(rows, list):
        return []
    out: list[dict[str, Any]] = []
    for row in rows:
        if isinstance(row, dict):
            out.append(row)
    return out


def _parse_hud_payload_rows(payload: dict[str, Any]) -> tuple[list[dict[str, str]], int | None, int | None]:
    rows: list[dict[str, str]] = []
    observed_year: int | None = None
    observed_quarter: int | None = None

    for entry in _payload_data_rows(payload):
        entry_year = _normalize_year(entry.get("year"))
        entry_quarter = _normalize_quarter(entry.get("quarter"))
        if observed_year is None and entry_year is not None:
            observed_year = entry_year
        if observed_quarter is None and entry_quarter is not None:
            observed_quarter = entry_quarter

        entry_input = _normalize_zip5(
            entry.get("input")
            or entry.get("zip")
            or entry.get("ZIP")
            or entry.get("zip5")
            or entry.get("ZIP5")
        )
        results_raw = entry.get("results")
        if isinstance(results_raw, list):
            result_items: list[dict[str, Any]] = [r for r in results_raw if isinstance(r, dict)]
        else:
            result_items = [entry]

        for result in result_items:
            zip5 = _normalize_zip5(
                result.get("zip")
                or result.get("ZIP")
                or result.get("zip5")
                or result.get("ZIP5")
                or entry_input
            )
            cbsa = _normalize_cbsa(
                result.get("CBSA")
                or result.get("cbsa")
                or result.get("CBSA_CODE")
                or result.get("cbsa_code")
                or result.get("geoid")
            )
            if not zip5 or not cbsa:
                continue
            rows.append(
                {
                    "ZIP": zip5,
                    "CBSA": cbsa,
                    "RES_RATIO": str(result.get("res_ratio") or result.get("RES_RATIO") or ""),
                    "TOT_RATIO": str(result.get("tot_ratio") or result.get("TOT_RATIO") or ""),
                    "CBSA_TITLE": str(
                        result.get("cbsa_title")
                        or result.get("CBSA_TITLE")
                        or result.get("cbsa_name")
                        or result.get("label")
                        or result.get("name")
                        or ""
                    ).strip(),
                }
            )
    return rows, observed_year, observed_quarter


def _write_hud_input_csv(rows: list[dict[str, str]], input_path: Path) -> None:
    normalized_rows: list[tuple[str, str, float, float, str]] = []
    for row in rows:
        zip5 = _normalize_zip5(row.get("ZIP"))
        cbsa = _normalize_cbsa(row.get("CBSA"))
        if not zip5 or not cbsa:
            continue
        res_ratio = _normalize_ratio(row.get("RES_RATIO"))
        tot_ratio = _normalize_ratio(row.get("TOT_RATIO"))
        label = str(row.get("CBSA_TITLE") or "").strip()
        normalized_rows.append((zip5, cbsa, res_ratio, tot_ratio, label))
    normalized_rows.sort(key=lambda item: (item[0], item[1], -item[2], -item[3], item[4]))

    input_path.parent.mkdir(parents=True, exist_ok=True)
    with open(input_path, "w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(["ZIP", "CBSA", "RES_RATIO", "TOT_RATIO", "CBSA_TITLE"])
        for zip5, cbsa, res_ratio, tot_ratio, label in normalized_rows:
            writer.writerow([zip5, cbsa, f"{res_ratio:.9f}", f"{tot_ratio:.9f}", label])


def _fetch_hud_zip_cbsa_csv(
    *,
    token: str,
    year: int | None,
    quarter: int | None,
    cache_root: Path,
) -> tuple[Path, int, int]:
    if not token.strip():
        raise RuntimeError("ERR_HUD_API_TOKEN_MISSING env=HUD_API_TOKEN")

    requested_year = year
    requested_quarter = quarter
    explicit_period = requested_year is not None and requested_quarter is not None
    resolved_year = requested_year
    resolved_quarter = requested_quarter

    if resolved_year is None and resolved_quarter is None:
        resolved_year, resolved_quarter = _discover_latest_period(token)

    raw_payloads: list[tuple[str, str, dict[str, Any]]] = []
    all_rows: list[dict[str, str]] = []
    fallback_used = False
    states = list(HUD_API_STATE_CODES)
    idx = 0
    while idx < len(states):
        state = states[idx]
        try:
            payload, url = _hud_get_json(
                query=state,
                token=token,
                year=resolved_year,
                quarter=resolved_quarter,
            )
        except HudApiRequestError as exc:
            if explicit_period and not fallback_used and exc.status == 404:
                fallback_year, fallback_quarter = _discover_latest_period(token)
                print(
                    f"WARN_HUD_API_PERIOD_FALLBACK requested={requested_year}Q{requested_quarter} "
                    f"used={fallback_year}Q{fallback_quarter}"
                )
                resolved_year = fallback_year
                resolved_quarter = fallback_quarter
                raw_payloads = []
                all_rows = []
                fallback_used = True
                idx = 0
                continue
            raise RuntimeError(str(exc)) from exc

        state_rows, state_year, state_quarter = _parse_hud_payload_rows(payload)
        all_rows.extend(state_rows)
        raw_payloads.append((state, url, payload))

        if state_year is not None and resolved_year is not None and state_year != resolved_year:
            raise RuntimeError(
                f"ERR_HUD_API_PERIOD_MISMATCH state={state} expected_year={resolved_year} actual_year={state_year}"
            )
        if state_quarter is not None and resolved_quarter is not None and state_quarter != resolved_quarter:
            raise RuntimeError(
                f"ERR_HUD_API_PERIOD_MISMATCH state={state} expected_quarter={resolved_quarter} actual_quarter={state_quarter}"
            )
        idx += 1

    if resolved_year is None or resolved_quarter is None:
        raise RuntimeError("ERR_HUD_API_PERIOD_UNKNOWN")
    if not all_rows:
        raise RuntimeError(
            f"ERR_HUD_API_EMPTY year={resolved_year} quarter={resolved_quarter} type={HUD_API_CROSSWALK_TYPE}"
        )

    period_dir = cache_root / f"{resolved_year}_Q{resolved_quarter}"
    period_dir.mkdir(parents=True, exist_ok=True)

    for state, url, payload in raw_payloads:
        raw_path = period_dir / f"state_{state}.json"
        wrapper = {
            "state": state,
            "type": HUD_API_CROSSWALK_TYPE,
            "year": resolved_year,
            "quarter": f"Q{resolved_quarter}",
            "request_url": url,
            "response": payload,
        }
        raw_path.write_text(json.dumps(wrapper, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    input_path = period_dir / "hud_zip_cbsa_type3.csv"
    _write_hud_input_csv(all_rows, input_path)
    return input_path, resolved_year, resolved_quarter


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
    source_provenance: str,
    input_path: Path,
    out_path: Path,
    cbsa_meta_path: Path,
    zip_meta_json_path: Path,
    rows_written: int,
    multi_count: int,
    zip_meta_json_sha: str,
    rebuild_command: str,
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
        f"- Provenance family: {source_provenance}",
        "- License: U.S. Federal Government work (public domain)",
        f"- Input file: `{input_path.name}`",
        f"- Input SHA256: `{input_sha}`",
        f"- Dataset incomplete: `{str(dataset_incomplete).lower()}`",
        "- Access note: HUD crosswalk file downloads are login-gated; API token flow is supported for deterministic rebuilds.",
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
            rebuild_command,
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
    ap.add_argument("--input", default="", help="Path to HUD USPS crosswalk CSV. Required unless --hud-api is used.")
    ap.add_argument(
        "--hud-api",
        action="store_true",
        help="Fetch ZIP-CBSA crosswalk via HUD USPS API (type=3 zip-cbsa). Requires HUD_API_TOKEN.",
    )
    ap.add_argument(
        "--hud-year",
        type=int,
        default=None,
        help="HUD data year for API fetch (optional; default latest year).",
    )
    ap.add_argument(
        "--hud-quarter",
        type=int,
        default=None,
        help="HUD data quarter for API fetch (1..4, optional; default latest quarter).",
    )
    ap.add_argument(
        "--hud-cache-root",
        default=".local/hud_zip_cbsa_api",
        help="Directory to persist raw HUD API payload cache artifacts.",
    )
    ap.add_argument(
        "--out",
        default=DEFAULT_OUT_PATH,
        help=f"Output gzip CSV path (ZIP5,CBSA). Default: {DEFAULT_OUT_PATH}.",
    )
    ap.add_argument(
        "--meta",
        default=DEFAULT_META_PATH,
        help=f"Output CBSA metadata CSV path. Default: {DEFAULT_META_PATH}.",
    )
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
        default=HUD_CROSSWALK_SOURCE_URL,
        help="Upstream source URL for provenance in SOURCES.md.",
    )
    args = ap.parse_args()

    if args.hud_year is not None and _normalize_year(args.hud_year) is None:
        print(f"ERR_HUD_API_YEAR_INVALID value={args.hud_year}")
        return 1
    if args.hud_quarter is not None and _normalize_quarter(args.hud_quarter) is None:
        print(f"ERR_HUD_API_QUARTER_INVALID value={args.hud_quarter}")
        return 1

    source_url = str(args.source_url).strip() or HUD_CROSSWALK_SOURCE_URL
    source_provenance = "HUD USPS ZIP Code Crosswalk (HUD USER)"
    rebuild_command = (
        "py -3 tools\\build_zip_cbsa.py --input <hud_zip_cbsa_csv> --out data\\geo\\zip_to_cbsa.csv.gz "
        "--meta data\\geo\\cbsa_meta.csv --zip-meta-json data\\geo\\zip_to_cbsa.meta.json --sources data\\geo\\SOURCES.md "
        "--source-label \"HUD USPS ZIP-CBSA <MONTH_OR_QUARTER>\""
    )

    if args.hud_api and str(args.input or "").strip():
        print("ERR_BUILD_ZIP_CBSA_ARGS conflict=--hud-api_with_--input")
        return 1

    if args.hud_api:
        hud_token = str(os.getenv("HUD_API_TOKEN", "")).strip()
        try:
            input_path, resolved_year, resolved_quarter = _fetch_hud_zip_cbsa_csv(
                token=hud_token,
                year=args.hud_year,
                quarter=args.hud_quarter,
                cache_root=Path(args.hud_cache_root),
            )
        except RuntimeError as exc:
            print(str(exc))
            return 1
        source_url = HUD_API_SOURCE_URL
        source_provenance = (
            f"HUD USPS ZIP Code Crosswalk Files API (type=3 zip-cbsa), year={resolved_year}, quarter=Q{resolved_quarter}"
        )
        if not str(args.source_label or "").strip():
            args.source_label = f"HUD USPS ZIP-CBSA {resolved_year} Q{resolved_quarter}"
        rebuild_command = (
            "py -3 tools\\build_zip_cbsa.py --hud-api "
            f"--hud-year {resolved_year} --hud-quarter {resolved_quarter} "
            "--out data\\geo\\zip_to_cbsa.csv.gz --meta data\\geo\\cbsa_meta.csv "
            "--zip-meta-json data\\geo\\zip_to_cbsa.meta.json --sources data\\geo\\SOURCES.md "
            f"--source-label \"HUD USPS ZIP-CBSA {resolved_year} Q{resolved_quarter}\""
        )
    else:
        if not str(args.input or "").strip():
            print("ERR_INPUT_MISSING path=")
            return 1
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
        source_url=source_url,
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
        source_url=source_url,
        source_provenance=source_provenance,
        input_path=input_path,
        out_path=out_path,
        cbsa_meta_path=meta_path,
        zip_meta_json_path=zip_meta_json_path,
        rows_written=rows_written,
        multi_count=multi_count,
        zip_meta_json_sha=zip_meta_json_sha,
        rebuild_command=rebuild_command,
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
