param(
  [string[]]$States = @(),
  [Nullable[int]]$TargetFirms = $null,
  [switch]$PrintConfig,
  [switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$wrapper = Join-Path $repoRoot 'run_with_secrets.ps1'

if (-not (Test-Path -LiteralPath $wrapper)) {
  Write-Output ('ERR_MANUAL_PROSPECT_RESEARCH_WRAPPER_MISSING path=' + $wrapper)
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

$toolArgs = @('tools\\prepare_manual_prospect_research.py')

if ($PSBoundParameters.ContainsKey('States')) {
  try {
    $normalizedStates = Normalize-StateScope -InputStates $States
  } catch {
    Write-Output ('ERR_MANUAL_PROSPECT_RESEARCH_STATES_INVALID detail=' + $_.Exception.Message)
    exit 1
  }
  if ($normalizedStates.Count -eq 0) {
    Write-Output 'ERR_MANUAL_PROSPECT_RESEARCH_STATES_INVALID detail=states_required'
    exit 1
  }
  $statesCsv = ($normalizedStates -join ',')
  $toolArgs += @('--states', $statesCsv)
  Write-Output ('MANUAL_PROSPECT_RESEARCH_SCOPE=STATES states=' + $statesCsv)
}

if ($PSBoundParameters.ContainsKey('TargetFirms')) {
  if ($TargetFirms -lt 1) {
    Write-Output 'ERR_MANUAL_PROSPECT_RESEARCH_TARGET_FIRMS_INVALID detail=positive_integer_required'
    exit 1
  }
  $toolArgs += @('--target-firms', ([string]$TargetFirms))
}

if ($PrintConfig) {
  $toolArgs += '--print-config'
}
if ($DryRun) {
  $toolArgs += '--dry-run'
}

try {
  Push-Location $repoRoot
  $allOutput = & $wrapper -- py -3 @toolArgs 2>&1
  $exitCode = $LASTEXITCODE

  $skipListPath = ''
  $promptOutputPath = ''
  foreach ($line in @($allOutput)) {
    $text = [string]$line
    Write-Output $text
    if ($text -match '^MANUAL_PROSPECT_RESEARCH_SKIP_LIST_PATH=(.+)$') {
      $skipListPath = $matches[1].Trim()
    }
    if ($text -match '^MANUAL_PROSPECT_RESEARCH_PROMPT_OUTPUT_PATH=(.+)$') {
      $promptOutputPath = $matches[1].Trim()
    }
  }
  if ($skipListPath) {
    Write-Output ('MANUAL_PROSPECT_RESEARCH_SKIP_LIST_PATH=' + $skipListPath)
  }
  if ($promptOutputPath) {
    Write-Output ('MANUAL_PROSPECT_RESEARCH_PROMPT_OUTPUT_PATH=' + $promptOutputPath)
  }
  exit $exitCode
}
finally {
  try { Pop-Location } catch {}
}
