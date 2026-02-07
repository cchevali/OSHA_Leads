Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'

function Fail([string]$Message) {
  # Single-line output only.
  Write-Output ("FAIL: " + $Message)
  exit 1
}

try {
  $repoRoot = Resolve-Path (Join-Path $PSScriptRoot '..')

  # 1) Static wiring assertion: Task entrypoint must go through run_with_secrets.ps1.
  $assertScript = Join-Path $repoRoot 'scripts\assert_task_wiring.ps1'
  $assertOut = & powershell -NoProfile -ExecutionPolicy Bypass -File $assertScript 2>$null
  if ($LASTEXITCODE -ne 0) { Fail "Task entrypoint wiring check failed" }

  # 2) Tool/key presence via the wrapper diagnostics-only mode (single-line PASS/FAIL).
  $wrapper = Join-Path $repoRoot 'scripts\run_with_secrets.ps1'
  if (-not (Test-Path $wrapper)) { Fail "Missing scripts\\run_with_secrets.ps1" }
  $diagOut = & powershell -NoProfile -ExecutionPolicy Bypass -File $wrapper --diagnostics 2>$null
  if ($LASTEXITCODE -ne 0) { Fail "run_with_secrets --diagnostics failed" }
  if (-not ($diagOut -match '^PASS:')) { Fail "run_with_secrets --diagnostics did not report PASS" }

  Write-Output "PASS: scheduler entrypoint wired; tooling/keys present"
  exit 0
} catch {
  Fail $_.Exception.Message
}

