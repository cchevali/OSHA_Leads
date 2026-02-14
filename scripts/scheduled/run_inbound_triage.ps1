Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
$wrapper = Join-Path $repoRoot 'run_with_secrets.ps1'
$triageScript = Join-Path $repoRoot 'inbound_inbox_triage.py'
$captureScript = Join-Path $repoRoot 'run_capture_sync.py'

try {
  Push-Location $repoRoot

  & $wrapper -- py -3 $triageScript --run-once
  $code = $LASTEXITCODE
  if ($code -ne 0) {
    exit $code
  }

  & $wrapper -- py -3 $captureScript
  exit $LASTEXITCODE
} finally {
  try { Pop-Location } catch {}
}
