Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'

function Fail([string]$Message) {
  Write-Output ("FAIL: " + $Message)
  exit 1
}

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot '..')
$batPath = Join-Path $repoRoot 'run_wally_trial_daily.bat'
if (-not (Test-Path $batPath)) { Fail "Missing run_wally_trial_daily.bat" }

$text = Get-Content -LiteralPath $batPath -Raw
if ($text -notmatch '(?i)run_with_secrets\.ps1') {
  Fail "Task entrypoint does not reference scripts\\run_with_secrets.ps1"
}
if ($text -notmatch '(?i)scripts\\run_with_secrets\.ps1') {
  Fail "Task entrypoint must reference scripts\\run_with_secrets.ps1 (relative to repo)"
}

Write-Output "PASS: task entrypoint wired through scripts\\run_with_secrets.ps1"
exit 0
