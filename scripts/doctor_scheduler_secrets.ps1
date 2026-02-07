Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'

function Fail([string]$Message) {
  # Single-line output only.
  Write-Output ("FAIL: " + $Message)
  exit 1
}

function Get-PassFailLine([object]$Output) {
  $lines = @()
  if ($Output -is [string]) { $lines = @($Output) } else { $lines = @($Output) }
  return ($lines | Where-Object { $_ -match '^(PASS|FAIL):' } | Select-Object -First 1)
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

  $ageKeysPath = Join-Path (Join-Path $env:APPDATA 'sops\\age') 'keys.txt'
  $needDecryptCheck = (Test-Path $ageKeysPath)

  $diagOut =
    if ($needDecryptCheck) {
      & powershell -NoProfile -ExecutionPolicy Bypass -File $wrapper --diagnostics --check-decrypt 2>$null
    } else {
      & powershell -NoProfile -ExecutionPolicy Bypass -File $wrapper --diagnostics 2>$null
    }
  $pf = Get-PassFailLine -Output $diagOut
  if ($LASTEXITCODE -ne 0 -or (-not $pf) -or (-not ($pf -match '^PASS:'))) {
    if ($pf -and ($pf -match '^FAIL:')) {
      Write-Output ([string]$pf)
      exit 1
    }
    Fail "run_with_secrets diagnostics did not report PASS"
  }

  Write-Output "PASS: scheduler entrypoint wired; tooling/keys present"
  exit 0
} catch {
  Fail $_.Exception.Message
}

