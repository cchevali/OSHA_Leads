param(
  [Nullable[int]]$Days = $null,
  [switch]$PrintConfig,
  [switch]$DryRun,
  [string]$OutputDir = ''
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$wrapper = Join-Path $repoRoot 'run_with_secrets.ps1'

if (-not (Test-Path -LiteralPath $wrapper)) {
  Write-Output ('ERR_PROSPECT_GROWTH_WRAPPER_MISSING path=' + $wrapper)
  exit 1
}

$toolArgs = @('tools\prospect_growth_decision_pack.py')

if ($PSBoundParameters.ContainsKey('Days')) {
  if ($Days -lt 1) {
    Write-Output 'ERR_PROSPECT_GROWTH_DAYS_INVALID detail=positive_integer_required'
    exit 1
  }
  $toolArgs += @('--days', ([string]$Days))
}
if ($PrintConfig) {
  $toolArgs += '--print-config'
}
if ($DryRun) {
  $toolArgs += '--dry-run'
}
if (([string]$OutputDir).Trim()) {
  $toolArgs += @('--output-dir', ([string]$OutputDir).Trim())
}

try {
  Push-Location $repoRoot
  $allOutput = & $wrapper -- py -3 @toolArgs 2>&1
  $exitCode = $LASTEXITCODE

  $outputDir = ''
  $textPath = ''
  $jsonPath = ''
  foreach ($line in @($allOutput)) {
    $text = [string]$line
    Write-Output $text
    if ($text -match '^PROSPECT_GROWTH_OUTPUT_DIR=(.+)$') {
      $outputDir = $matches[1].Trim()
    }
    if ($text -match '^PROSPECT_GROWTH_OUTPUT_TEXT_PATH=(.+)$') {
      $textPath = $matches[1].Trim()
    }
    if ($text -match '^PROSPECT_GROWTH_OUTPUT_JSON_PATH=(.+)$') {
      $jsonPath = $matches[1].Trim()
    }
  }
  if ($outputDir) {
    Write-Output ('PROSPECT_GROWTH_OUTPUT_DIR=' + $outputDir)
  }
  if ($textPath) {
    Write-Output ('PROSPECT_GROWTH_OUTPUT_TEXT_PATH=' + $textPath)
  }
  if ($jsonPath) {
    Write-Output ('PROSPECT_GROWTH_OUTPUT_JSON_PATH=' + $jsonPath)
  }
  exit $exitCode
}
finally {
  try { Pop-Location } catch {}
}
