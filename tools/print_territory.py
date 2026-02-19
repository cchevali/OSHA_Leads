#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from typing import Any

from geo.zip_cbsa import resolve_metro_label
from lead_filters import load_territory_definitions, resolve_territory_code


def _build_payload(code: str) -> dict[str, Any]:
    definitions = load_territory_definitions()
    requested = (code or "").strip().upper()
    canonical = resolve_territory_code(requested, definitions)
    if canonical not in definitions:
        raise ValueError(f"Unknown territory code: {code}")

    territory = definitions[canonical]
    aliases = territory.get("aliases") or []
    cbsas = [
        "".join(ch for ch in str(item or "").strip() if ch.isdigit()).zfill(5)
        for item in (territory.get("cbsas") or [])
        if str(item or "").strip()
    ]
    metros = []
    for cbsa in cbsas:
        metros.append(
            {
                "cbsa": cbsa,
                "metro_label": resolve_metro_label(cbsa) or "",
            }
        )
    return {
        "input_code": requested,
        "canonical_code": canonical,
        "kind": str(territory.get("kind") or "LEGACY_REGEX"),
        "label": str(territory.get("label") or territory.get("description") or canonical),
        "states": territory.get("states") or [],
        "aliases": aliases,
        "cbsas": cbsas,
        "metros": metros,
        "fallback_city_patterns": territory.get("fallback_city_patterns") or [],
        "office_patterns": territory.get("office_patterns") or [],
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Print explainable territory definition.")
    ap.add_argument("--code", required=True, help="Territory code (e.g., TX_TRI).")
    ap.add_argument("--json", action="store_true", help="Print JSON payload.")
    args = ap.parse_args()

    try:
        payload = _build_payload(str(args.code))
    except Exception as exc:
        print(f"ERR_TERRITORY_LOOKUP {exc}")
        return 1

    if args.json:
        print(json.dumps(payload, indent=2))
        return 0

    print(f"input_code={payload['input_code']}")
    print(f"canonical_code={payload['canonical_code']}")
    print(f"kind={payload['kind']}")
    print(f"label={payload['label']}")
    print(f"states={','.join(payload['states'])}")
    print(f"aliases={','.join(payload['aliases'])}")
    if payload["metros"]:
        for item in payload["metros"]:
            print(f"metro cbsa={item['cbsa']} label={item['metro_label']}")
    else:
        print("metro=NONE")
    print(f"fallback_city_patterns={len(payload['fallback_city_patterns'])}")
    print(f"office_patterns={len(payload['office_patterns'])}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

