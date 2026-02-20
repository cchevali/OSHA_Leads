# ZIP->CBSA Data Sources

## Source
- Dataset label: `HUD USPS ZIP-CBSA 2025 Q3`
- Source URL: `https://www.huduser.gov/portal/dataset/uspszip-api.html`
- Provenance family: HUD USPS ZIP Code Crosswalk Files API (type=3 zip-cbsa), year=2025, quarter=Q3
- License: U.S. Federal Government work (public domain)
- Input file: `hud_zip_cbsa_type3.csv`
- Input SHA256: `2300c01318e3db8ee83f669f82ebef0ae1cb10e45341d6f19e1c97c7ffbf625c`
- Dataset incomplete: `false`
- Access note: HUD crosswalk file downloads are login-gated; API token flow is supported for deterministic rebuilds.

## Output Artifacts
- `zip_to_cbsa.csv.gz` SHA256: `de78aaa132e35fe8b7b182b0f9c5c069501fc46cc079f45956d6e4d5e5aac8e7`
- `cbsa_meta.csv` SHA256: `963df3744b947088a700555460a7ee5a7a337c25b6ed88cca9dc91a0f278d751`
- `zip_to_cbsa.meta.json` SHA256: `597bec0fc8ae3f3b408f98ae5391773a3845802e33f370b141d7fbe082761b70`
- ZIP rows written: `39298`
- ZIP rows with multi-CBSA candidates: `7030`

## Deterministic Tie-Break Rules
- Primary key: highest residential ratio (`RES_RATIO`).
- Secondary key (tie): lowest numeric CBSA code.
- Warning token: `WARN_ZIP_MULTI_CBSA` emitted with count.

## Rebuild Command
```powershell
py -3 tools\build_zip_cbsa.py --hud-api --hud-year 2025 --hud-quarter 3 --out data\geo\zip_to_cbsa.csv.gz --meta data\geo\cbsa_meta.csv --zip-meta-json data\geo\zip_to_cbsa.meta.json --sources data\geo\SOURCES.md --source-label "HUD USPS ZIP-CBSA 2025 Q3"
```
