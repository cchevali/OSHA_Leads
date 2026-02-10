# RUNBOOK

## Switch machines: laptop -> PC

Commands:

- git fetch origin
- git checkout main
- git pull --ff-only
- run_with_secrets.ps1 --diagnostics --check-decrypt
- py -3 -m unittest -q

## Verify Prefs Service (Internal)

- curl -sS -H "X-MFO-Internal-Key: <key>" "https://unsub.microflowops.com/api/prefs_state?subscriber_key=<sk>&territory_code=<terr>"
  - expected: 200 JSON with `lows_enabled` and `updated_at_iso`
- curl -sS -H "X-MFO-Internal-Key: <key>" "https://unsub.microflowops.com/prefs_state?subscriber_key=<sk>&territory_code=<terr>"
  - expected: 200 JSON with `lows_enabled` and `updated_at_iso`

