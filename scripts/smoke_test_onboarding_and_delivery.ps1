$ErrorActionPreference = "Stop"

$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
Set-Location $RepoRoot

$ts = Get-Date -Format "yyyyMMdd_HHmmss"
$WorkDir = Join-Path $RepoRoot ("out\\smoke_tests\\" + $ts)
New-Item -ItemType Directory -Force -Path $WorkDir | Out-Null

$SrcDb = Join-Path $RepoRoot "data\\osha.sqlite"
$TmpDb = Join-Path $WorkDir "osha_smoke.sqlite"
$CustomerConfig = Join-Path $WorkDir "customer_smoke.json"

if (!(Test-Path $SrcDb)) {
  throw "Missing source DB: $SrcDb"
}

Copy-Item -Force $SrcDb $TmpDb

$Suppressed = "suppressed_smoke_test@example.com"

$env:SMOKE_DB = $TmpDb
$env:SMOKE_SUPPRESSED = $Suppressed
$InsertSuppression = @'
import os
import sqlite3

db = os.environ["SMOKE_DB"]
sup = os.environ["SMOKE_SUPPRESSED"]

con = sqlite3.connect(db)
cur = con.cursor()
cur.execute(
    "CREATE TABLE IF NOT EXISTS suppression_list (id INTEGER PRIMARY KEY AUTOINCREMENT, email_or_domain TEXT UNIQUE NOT NULL, reason TEXT, created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP)"
)
cur.execute(
    "INSERT OR IGNORE INTO suppression_list (email_or_domain, reason) VALUES (?, ?)",
    (sup, "smoke_test"),
)
con.commit()
con.close()
print("OK suppression_inserted=" + sup)
'@
$InsertSuppression | python -
if ($LASTEXITCODE -ne 0) { throw "suppression insert failed with exit code $LASTEXITCODE" }

$Block = @"
TERRITORY=TX_TRIANGLE
SEND_TIME_LOCAL=08:00
TIMEZONE=America/Chicago
THRESHOLD=MEDIUM
RECIPIENTS=$Suppressed, allowed_smoke_test@example.com
FIRM_NAME=Smoke Test Firm
NOTES=smoke_test
"@

# Onboard into temp DB (write DB + config + audit log, but do not send confirmation emails)
$Block | python onboard_subscriber.py --db $TmpDb --schema schema.sql --customer-config-out $CustomerConfig --dry-run | Out-Host

if (!(Test-Path $CustomerConfig)) {
  throw "Expected customer config not found: $CustomerConfig"
}

Write-Host "Running deliver_daily dry-run #1..."
$cmd1 = "python deliver_daily.py --db `"$TmpDb`" --customer `"$CustomerConfig`" --mode daily --skip-ingest --dry-run"
$out1Lines = cmd /c "$cmd1 2>&1"
if ($LASTEXITCODE -ne 0) { throw "deliver_daily dry-run #1 returned exit code $LASTEXITCODE" }
$out1 = ($out1Lines -join "`n")
Write-Host $out1

if ($out1 -notmatch "DRYRUN_SUPPRESSED") {
  throw "Expected DRYRUN_SUPPRESSED marker not found in first run output"
}
if ($out1 -notmatch [regex]::Escape($Suppressed)) {
  throw "Expected suppressed email not referenced in first run output"
}

Write-Host "Running deliver_daily dry-run #2 (expect duplicate render skip)..."
$cmd2 = "python deliver_daily.py --db `"$TmpDb`" --customer `"$CustomerConfig`" --mode daily --skip-ingest --dry-run"
$out2Lines = cmd /c "$cmd2 2>&1"
if ($LASTEXITCODE -ne 0) { throw "deliver_daily dry-run #2 returned exit code $LASTEXITCODE" }
$out2 = ($out2Lines -join "`n")
Write-Host $out2

if ($out2 -notmatch "\\[SKIP_DUPLICATE_DRYRUN\\]") {
  throw "Expected [SKIP_DUPLICATE_DRYRUN] not found in second run output"
}

Write-Host "SMOKE_TEST_OK workdir=$WorkDir"
