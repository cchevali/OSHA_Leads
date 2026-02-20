# ZIP->CBSA Data Sources

## Source
- Dataset label: `HUD USPS ZIP-CBSA seed bootstrap (coverage incomplete)`
- Source URL: `https://www.huduser.gov/portal/datasets/usps_crosswalk.html`
- Provenance family: HUD USPS ZIP Code Crosswalk (HUD USER)
- License: U.S. Federal Government work (public domain)
- Input file: `hud_zip_cbsa_seed_input.csv`
- Input SHA256: `421bf164b47d202133fd9fa33d235a200ee1b5f28edfe1302640a6c09e1da046`
- Dataset incomplete: `true`
- Coverage note: committed artifact is a bootstrap subset, not the full nationwide extract.

## Output Artifacts
- `zip_to_cbsa.csv.gz` SHA256: `e423b14aaa80d3e00075324d3ac3f5f2de29577990a9cb6c33172a8a2fb8c5fe`
- `cbsa_meta.csv` SHA256: `d53cc8719822add29c8f65745cc1ea22cb4a889b47f3910602dfa8ef4c5b4b45`
- `zip_to_cbsa.meta.json` SHA256: `40a3615d18e80763a72caffab5f42dac15b947f208f468320b2604215a50813d`
- ZIP rows written: `12`
- ZIP rows with multi-CBSA candidates: `0`

## Deterministic Tie-Break Rules
- Primary key: highest residential ratio (`RES_RATIO`).
- Secondary key (tie): lowest numeric CBSA code.
- Warning token: `WARN_ZIP_MULTI_CBSA` emitted with count.

## Rebuild Command
```powershell
py -3 tools\build_zip_cbsa.py --input <hud_zip_cbsa_csv> --out data\geo\zip_to_cbsa.csv.gz --meta data\geo\cbsa_meta.csv --zip-meta-json data\geo\zip_to_cbsa.meta.json --sources data\geo\SOURCES.md --source-label "HUD USPS ZIP-CBSA <MONTH_OR_QUARTER>"
```

## County->CBSA Fallback Table
- File: `data/geo/county_to_cbsa.csv`
- Current origin: deterministic seed map curated from U.S. Census/OMB CBSA county delineations for TX trial regression coverage.
- Generation steps (manual):
1. Choose canonical county names from official CBSA county delineation sources.
2. Normalize state to two-letter USPS code.
3. Normalize county to title-case county name without the `County` suffix.
4. Write CSV rows with explicit `state,county,cbsa` values (no heuristics).
- Expected columns:
- `state`: two-letter state code (example `TX`).
- `county`: county name token (example `Williamson`).
- `cbsa`: 5-digit CBSA code (example `12420`).
- Runtime normalization rules in resolver:
- `state` uppercased and stripped to letters.
- `county` uppercased, punctuation removed, and `COUNTY` suffix removed.
- `cbsa` digits only and left-padded to 5.
