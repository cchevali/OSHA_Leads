param(
  [string]$Db = "data/osha.sqlite",
  [string]$Customer = "customers/wally_trial_tx_triangle_v1.json"
)

$ErrorActionPreference = "Stop"

if (-not (Test-Path -LiteralPath $Db)) {
  throw "DB not found: $Db"
}
if (-not (Test-Path -LiteralPath $Customer)) {
  throw "Customer config not found: $Customer"
}

# Single laptop-safe entrypoint: decrypt env, then render+send trial digest to Chase only.
.\run_with_secrets.ps1 -- py -3 send_digest_email.py --db $Db --customer $Customer --mode daily --smoke-cchevali --log-level ERROR
