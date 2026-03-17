param(
  [string]$ForDate = '',
  [string[]]$States = @(),
  [Nullable[int]]$RawTarget = $null,
  [Nullable[int]]$PacketSize = $null,
  [switch]$PrintConfig,
  [switch]$DryRun,
  [string]$OutputDir = '',
  [string]$Output = ''
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$wrapper = Join-Path $repoRoot 'run_with_secrets.ps1'

if (-not (Test-Path -LiteralPath $wrapper)) {
  Write-Output ('ERR_AI_ASSIST_DUMP_WRAPPER_MISSING path=' + $wrapper)
  exit 1
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

$toolArgs = @('tools\\dump_prospect_ai_assist_review.py')

if (([string]$ForDate).Trim()) {
  $toolArgs += @('--for-date', ([string]$ForDate).Trim())
}

if ($PSBoundParameters.ContainsKey('States')) {
  try {
    $normalizedStates = Normalize-StateScope -InputStates $States
  } catch {
    Write-Output ('ERR_AI_ASSIST_DUMP_STATES_INVALID detail=' + $_.Exception.Message)
    exit 1
  }
  if ($normalizedStates.Count -eq 0) {
    Write-Output 'ERR_AI_ASSIST_DUMP_STATES_INVALID detail=states_required'
    exit 1
  }
  $statesCsv = ($normalizedStates -join ',')
  $toolArgs += @('--states', $statesCsv)
  Write-Output ('AI_ASSIST_DUMP_SCOPE=STATES states=' + $statesCsv)
}

if ($PSBoundParameters.ContainsKey('RawTarget')) {
  if ($RawTarget -lt 1) {
    Write-Output 'ERR_AI_ASSIST_DUMP_RAW_TARGET_INVALID detail=positive_integer_required'
    exit 1
  }
  $toolArgs += @('--raw-target', ([string]$RawTarget))
}
if ($PSBoundParameters.ContainsKey('PacketSize')) {
  if ($PacketSize -lt 1) {
    Write-Output 'ERR_AI_ASSIST_DUMP_PACKET_SIZE_INVALID detail=positive_integer_required'
    exit 1
  }
  $toolArgs += @('--packet-size', ([string]$PacketSize))
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

try {
  Push-Location $repoRoot
  $allOutput = & $wrapper -- py -3 @toolArgs 2>&1
  $exitCode = $LASTEXITCODE

  $outputDir = ''
  $outputPath = ''
  $packetDir = ''
  $manifestPath = ''
  foreach ($line in @($allOutput)) {
    $text = [string]$line
    Write-Output $text
    if ($text -match '^AI_ASSIST_DUMP_OUTPUT_DIR=(.+)$') {
      $outputDir = $matches[1].Trim()
    }
    if ($text -match '^AI_ASSIST_DUMP_OUTPUT_PATH=(.+)$') {
      $outputPath = $matches[1].Trim()
    }
    if ($text -match '^AI_ASSIST_PACKET_DIR=(.+)$') {
      $packetDir = $matches[1].Trim()
    }
    if ($text -match '^AI_ASSIST_PACKET_MANIFEST_PATH=(.+)$') {
      $manifestPath = $matches[1].Trim()
    }
  }
  if ($outputDir) {
    Write-Output ('AI_ASSIST_DUMP_OUTPUT_DIR=' + $outputDir)
  }
  if ($outputPath) {
    Write-Output ('AI_ASSIST_DUMP_OUTPUT_PATH=' + $outputPath)
  }
  if ($packetDir) {
    Write-Output ('AI_ASSIST_PACKET_DIR=' + $packetDir)
  }
  if ($manifestPath) {
    Write-Output ('AI_ASSIST_PACKET_MANIFEST_PATH=' + $manifestPath)
  }
  exit $exitCode
}
finally {
  try { Pop-Location } catch {}
}
