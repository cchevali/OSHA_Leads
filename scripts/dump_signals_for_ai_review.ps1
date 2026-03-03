param(
  [string]$Since = '',
  [int]$SinceDays = 14,
  [string]$Until = '',
  [switch]$AllOutreach,
  [string]$Territory = '',
  [string[]]$States = @(),
  [switch]$PrintConfig,
  [switch]$DryRun,
  [string]$OutputDir = '',
  [string]$Output = '',
  [switch]$IncludeSuppressed
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$wrapper = Join-Path $repoRoot 'run_with_secrets.ps1'

if (-not (Test-Path -LiteralPath $wrapper)) {
  Write-Output ('ERR_AI_REVIEW_DUMP_WRAPPER_MISSING path=' + $wrapper)
  exit 1
}

$resolvedSince = ([string]$Since).Trim()
if (-not $resolvedSince) {
  $days = [int]$SinceDays
  if ($days -lt 0) {
    $days = 14
  }
  $resolvedSince = (Get-Date).Date.AddDays(-1 * $days).ToString('yyyy-MM-dd')
}

function Normalize-StateScope {
  param([object[]]$InputStates)
  $seen = @{}
  $normalized = New-Object System.Collections.Generic.List[string]
  foreach ($entry in @($InputStates)) {
    $text = [string]$entry
    foreach ($part in $text.Split(',')) {
      $token = ([string]$part).Trim().ToUpperInvariant()
      if (-not $token) { continue }
      if ($token -notmatch '^[A-Z]{2}$') {
        throw "invalid state code '$token'"
      }
      if (-not $seen.ContainsKey($token)) {
        $seen[$token] = $true
        [void]$normalized.Add($token)
      }
    }
  }
  return ,$normalized.ToArray()
}

$normalizedStates = @()
if ($PSBoundParameters.ContainsKey('States')) {
  try {
    $normalizedStates = Normalize-StateScope -InputStates $States
  } catch {
    Write-Output ('ERR_AI_REVIEW_DUMP_STATES_INVALID detail=' + $_.Exception.Message)
    exit 1
  }
  if ($normalizedStates.Count -eq 0) {
    Write-Output 'ERR_AI_REVIEW_DUMP_STATES_INVALID detail=states_required'
    exit 1
  }
}

$toolArgs = @(
  'tools\dump_signals_for_review.py',
  '--for-ai-review',
  '--since',
  $resolvedSince
)

$scopeSpecified = $false
if ($AllOutreach) {
  $toolArgs += '--all-outreach'
  $scopeSpecified = $true
}
if (([string]$Territory).Trim()) {
  $toolArgs += @('--territory', ([string]$Territory).Trim())
  $scopeSpecified = $true
}
if ($normalizedStates.Count -gt 0) {
  $statesCsv = ($normalizedStates -join ',')
  $toolArgs += @('--states', $statesCsv)
  $scopeSpecified = $true
  Write-Output ('AI_REVIEW_DUMP_SCOPE=STATES states=' + $statesCsv)
}
if (-not $scopeSpecified) {
  $toolArgs += '--all-outreach'
}

if (([string]$Until).Trim()) {
  $toolArgs += @('--until', ([string]$Until).Trim())
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
if (([string]$Output).Trim()) {
  $toolArgs += @('--output', ([string]$Output).Trim())
}
if ($IncludeSuppressed) {
  $toolArgs += '--include-suppressed'
}

try {
  Push-Location $repoRoot
  $allOutput = & $wrapper -- py -3 @toolArgs 2>&1
  $exitCode = $LASTEXITCODE

  $outputPath = ''
  foreach ($line in @($allOutput)) {
    $text = [string]$line
    Write-Output $text
    if ($text -match '^AI_REVIEW_DUMP_OUTPUT_PATH=(.+)$') {
      $outputPath = $matches[1].Trim()
    }
  }

  if ($outputPath) {
    Write-Output ('AI_REVIEW_DUMP_OUTPUT_PATH=' + $outputPath)
  }

  exit $exitCode
}
finally {
  try { Pop-Location } catch {}
}
