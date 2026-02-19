# ZIP->CBSA Data Sources

## Canonical source format
- Source family: HUD USPS ZIP Code Crosswalk (HUD USER)
- Endpoint/docs:
  - https://www.huduser.gov/portal/datasets/usps_crosswalk.html
  - https://www.huduser.gov/portal/dataset/uspszip-api.html
- License: U.S. Federal Government work (public domain)

## Repository snapshot (committed artifacts)
- `zip_to_cbsa.csv.gz`
  - Format: `ZIP5,CBSA`
  - SHA256: `7383ebc66fa9f2487443f059d659153dea97ee845c50b9d892f96725226e30b0`
  - Note: deterministic bootstrap mapping for runtime/tests; regenerate from full HUD extract with `tools/build_zip_cbsa.py`.
- `cbsa_meta.csv`
  - Format: `CBSA,metro_label`
  - SHA256: `71d566e3916f61f7c5f2751c858135689af6d87d65c82909c1bc1740ae18a41a`

## Deterministic rebuild
Use:

```powershell
py -3 tools\build_zip_cbsa.py --input <hud_crosswalk_csv> --out data/geo/zip_to_cbsa.csv.gz --meta data/geo/cbsa_meta.csv
```

Selection rule when a ZIP maps to multiple CBSAs:
- Highest residential ratio wins.
- Ties break by lowest numeric CBSA code.
