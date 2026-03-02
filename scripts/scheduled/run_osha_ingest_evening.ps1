Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
$runner = Join-Path $repoRoot 'scripts\dump_signals_for_ai_review.ps1'

if (-not (Test-Path -LiteralPath $runner)) {
  Write-Output ('ERR_OSHA_INGEST_EVENING_RUNNER_MISSING path=' + $runner)
  exit 1
}

try {
  Push-Location $repoRoot
  & $runner -AllOutreach -SinceDays 14
  exit $LASTEXITCODE
}
finally {
  try { Pop-Location } catch {}
}
