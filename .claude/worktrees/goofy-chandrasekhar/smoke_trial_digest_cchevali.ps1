param(
  [string]$Db = "",
  [string]$Customer = "customers/wally_trial_tx_triangle_v1.json"
)

$ErrorActionPreference = "Stop"

if (-not $Db) {
  if ($env:DATA_DIR -and [System.IO.Path]::IsPathRooted($env:DATA_DIR) -and ($env:DATA_DIR -ine 'out')) {
    $Db = Join-Path $env:DATA_DIR "osha.sqlite"
  } else {
    $Db = Join-Path $PSScriptRoot "out\\osha.sqlite"
  }
}

if (-not (Test-Path -LiteralPath $Db)) {
  throw "DB not found: $Db"
}
if (-not (Test-Path -LiteralPath $Customer)) {
  throw "Customer config not found: $Customer"
}

# Single laptop-safe entrypoint: decrypt env, then render+send trial digest to Chase only.
.\run_with_secrets.ps1 -- py -3 send_digest_email.py --db $Db --customer $Customer --mode daily --smoke-cchevali --log-level ERROR
