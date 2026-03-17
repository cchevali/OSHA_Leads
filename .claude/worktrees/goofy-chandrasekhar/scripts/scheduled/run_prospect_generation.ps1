Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path

try {
  Push-Location $repoRoot
  & (Join-Path $repoRoot "run_with_secrets.ps1") -- py -3 (Join-Path $repoRoot "run_prospect_generation.py")
  $code = $LASTEXITCODE
  exit $code
} finally {
  try { Pop-Location } catch {}
}
