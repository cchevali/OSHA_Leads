param(
  [switch]$PrintConfig,
  [switch]$DryRun,
  [switch]$Apply,
  [string]$AiReviewCsv = '',
  [int]$SinceDays = 14,
  [string]$Until = ''
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'

function Write-Token([string]$Name, [string]$Value) {
  Write-Output ($Name + '=' + $Value)
}

function Resolve-ExistingAbsolutePath([string]$RawPath) {
  $text = ([string]$RawPath).Trim()
  if (-not $text) { return '' }
  try {
    return (Resolve-Path -LiteralPath $text -ErrorAction Stop).Path
  } catch {
    return ''
  }
}

function Resolve-AbsolutePathOrLiteral([string]$RawPath) {
  $text = ([string]$RawPath).Trim()
  if (-not $text) { return '' }
  try {
    return (Resolve-Path -LiteralPath $text -ErrorAction Stop).Path
  } catch {
    try {
      return [System.IO.Path]::GetFullPath($text)
    } catch {
      return ''
    }
  }
}

function Find-NewestAiReviewCsv([string[]]$SearchDirs) {
  $candidates = @()
  foreach ($dir in @($SearchDirs)) {
    $pathText = ([string]$dir).Trim()
    if (-not $pathText) { continue }
    if (-not (Test-Path -LiteralPath $pathText)) { continue }
    $files = Get-ChildItem -Path $pathText -Filter 'ai_review_*.csv' -File -ErrorAction SilentlyContinue
    if ($files) {
      $candidates += $files
    }
  }
  if (-not $candidates -or $candidates.Count -lt 1) {
    return ''
  }
  $selected = $candidates | Sort-Object -Property LastWriteTimeUtc -Descending | Select-Object -First 1
  if (-not $selected) {
    return ''
  }
  return $selected.FullName
}

function Get-TokenValue([string[]]$Lines, [string]$TokenName) {
  foreach ($line in @($Lines)) {
    $text = [string]$line
    if ($text -match ('^' + [Regex]::Escape($TokenName) + '=(.*)$')) {
      return $matches[1].Trim()
    }
  }
  return ''
}

function Invoke-SecretsPythonStep(
  [string]$WrapperPath,
  [string]$StepName,
  [string[]]$PythonArgs
) {
  [Console]::Out.WriteLine(("PIPELINE_STEP_" + $StepName + "=START"))
  [Console]::Out.WriteLine(("PIPELINE_STEP_" + $StepName + "_COMMAND=py -3 " + ($PythonArgs -join ' ')))
  $raw = & $WrapperPath -- py -3 @PythonArgs 2>&1
  $code = $LASTEXITCODE
  $lines = @()
  foreach ($entry in @($raw)) {
    $line = [string]$entry
    $lines += $line
    [Console]::Out.WriteLine($line)
  }
  [Console]::Out.WriteLine(("PIPELINE_STEP_" + $StepName + "_EXIT=" + [string]$code))
  if ($code -eq 0) {
    [Console]::Out.WriteLine(("PIPELINE_STEP_" + $StepName + "=PASS"))
  } else {
    [Console]::Out.WriteLine(("PIPELINE_STEP_" + $StepName + "=FAIL"))
  }
  return @{
    Success = ($code -eq 0)
    ExitCode = [int]$code
    Output = @($lines)
  }
}

$modeCount = 0
if ($PrintConfig) { $modeCount += 1 }
if ($DryRun) { $modeCount += 1 }
if ($Apply) { $modeCount += 1 }
if ($modeCount -gt 1) {
  Write-Token 'ERR_PREP_PIPELINE_ARGS' 'mode_conflict'
  exit 2
}
if ($modeCount -eq 0) {
  $DryRun = $true
}

if ($SinceDays -lt 1) {
  Write-Token 'ERR_PREP_PIPELINE_ARGS' 'invalid_SinceDays'
  exit 2
}

$resolvedUntil = ''
if (([string]$Until).Trim()) {
  try {
    $resolvedUntil = [DateTime]::ParseExact(([string]$Until).Trim(), 'yyyy-MM-dd', $null).ToString('yyyy-MM-dd')
  } catch {
    Write-Token 'ERR_PREP_PIPELINE_ARGS' 'invalid_Until'
    exit 2
  }
}

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$wrapper = Join-Path $repoRoot 'run_with_secrets.ps1'
if (-not (Test-Path -LiteralPath $wrapper)) {
  Write-Token 'ERR_PREP_PIPELINE_WRAPPER_MISSING' $wrapper
  exit 2
}

$mode = 'DRY_RUN'
if ($Apply) { $mode = 'APPLY' }
if ($PrintConfig) { $mode = 'PRINT_CONFIG' }

$sinceDate = (Get-Date).Date.AddDays(-1 * $SinceDays).ToString('yyyy-MM-dd')
$forDate = if ($resolvedUntil) { $resolvedUntil } else { (Get-Date).Date.AddDays(1).ToString('yyyy-MM-dd') }

Write-Token 'PIPELINE_MODE' $mode
Write-Token 'PIPELINE_SINCE_DAYS' ([string]$SinceDays)
Write-Token 'PIPELINE_SINCE' $sinceDate
Write-Token 'PIPELINE_UNTIL' $resolvedUntil
Write-Token 'PIPELINE_FOR_DATE' $forDate

$configArgs = @(
  'tools\dump_signals_for_review.py',
  '--for-ai-review',
  '--all-outreach',
  '--since',
  $sinceDate,
  '--print-config'
)
if ($resolvedUntil) {
  $configArgs += @('--until', $resolvedUntil)
}

Push-Location $repoRoot
try {
  $configResult = Invoke-SecretsPythonStep -WrapperPath $wrapper -StepName 'CONFIG' -PythonArgs $configArgs
  if (-not [bool]$configResult['Success']) {
    Write-Token 'PIPELINE_READY_FOR_TOMORROW' '0'
    Write-Token 'PIPELINE_BLOCKERS' 'config_step_failed'
    exit 1
  }

  $effectiveDataDir = Get-TokenValue -Lines $configResult['Output'] -TokenName 'AI_REVIEW_DUMP_DATA_DIR'
  if (-not $effectiveDataDir) {
    $effectiveDataDir = (Join-Path $repoRoot 'out')
  }
  $resolvedDataDir = Resolve-AbsolutePathOrLiteral $effectiveDataDir
  if (-not $resolvedDataDir) {
    $resolvedDataDir = (Join-Path $repoRoot 'out')
  }
  Write-Token 'PIPELINE_DATA_DIR_EFFECTIVE' $resolvedDataDir

  $suppressionPath = Join-Path $resolvedDataDir 'suppression.csv'
  Write-Token 'PIPELINE_SUPPRESSION_PATH' $suppressionPath

  $importsDirData = Join-Path $resolvedDataDir 'imports'
  $importsDirCanonical = 'C:\osha_data\imports'
  Write-Token 'PIPELINE_IMPORTS_SEARCH_DIRS' ($importsDirCanonical + ';' + $importsDirData)

  $resolvedCsv = ''
  $csvSource = 'missing'
  if (([string]$AiReviewCsv).Trim()) {
    $resolvedCsv = Resolve-ExistingAbsolutePath $AiReviewCsv
    if ($resolvedCsv) {
      $csvSource = 'param'
    }
  } else {
    $resolvedCsv = Find-NewestAiReviewCsv -SearchDirs @($importsDirCanonical, $importsDirData)
    if ($resolvedCsv) {
      $csvSource = 'auto'
    }
  }
  Write-Token 'PIPELINE_IMPORT_INPUT' $resolvedCsv
  Write-Token 'PIPELINE_IMPORT_SOURCE' $csvSource

  $suppressionExists = Test-Path -LiteralPath $suppressionPath
  $suppressionCreated = $false
  if (-not $suppressionExists -and $Apply) {
    $parentDir = Split-Path -Parent $suppressionPath
    if (-not (Test-Path -LiteralPath $parentDir)) {
      New-Item -ItemType Directory -Path $parentDir -Force | Out-Null
    }
    Set-Content -LiteralPath $suppressionPath -Value 'email' -Encoding UTF8
    $suppressionExists = Test-Path -LiteralPath $suppressionPath
    $suppressionCreated = $suppressionExists
  }
  Write-Token 'PIPELINE_SUPPRESSION_EXISTS' ($(if ($suppressionExists) { '1' } else { '0' }))
  Write-Token 'PIPELINE_SUPPRESSION_CREATED' ($(if ($suppressionCreated) { '1' } else { '0' }))

  if ($PrintConfig) {
    Write-Token 'PIPELINE_IMPORT_COMMAND' ('tools\import_ai_triage.py --input <csv> --dry-run')
    Write-Token 'PIPELINE_READY_FOR_TOMORROW' '0'
    Write-Token 'PIPELINE_BLOCKERS' 'print_config_only'
    exit 0
  }

  $blockers = New-Object System.Collections.Generic.List[string]
  $selectedGenerationState = ''

  $outreachPrintConfigResult = Invoke-SecretsPythonStep -WrapperPath $wrapper -StepName 'OUTREACH_CONFIG' -PythonArgs @('run_outreach_auto.py', '--print-config', '--for-date', $forDate)
  if (-not [bool]$outreachPrintConfigResult['Success']) {
    [void]$blockers.Add('outreach_print_config_failed')
  } else {
    foreach ($line in @($outreachPrintConfigResult['Output'])) {
      $text = [string]$line
      if ($text -match 'selected_state=([A-Z]{2,3})') {
        $selectedGenerationState = $matches[1].Trim().ToUpperInvariant()
        break
      }
    }
  }
  Write-Token 'PIPELINE_GENERATION_STATE_SCOPE' $selectedGenerationState

  if (-not $resolvedCsv) {
    [void]$blockers.Add('missing_ai_review_csv')
    Write-Token 'PIPELINE_STEP_IMPORT' 'FAIL'
    Write-Token 'PIPELINE_IMPORT_STATUS' 'missing_input'
  } else {
    $importArgs = @(
      'tools\import_ai_triage.py',
      '--input',
      $resolvedCsv
    )
    if ($DryRun) {
      $importArgs += '--dry-run'
    }
    $importResult = Invoke-SecretsPythonStep -WrapperPath $wrapper -StepName 'IMPORT' -PythonArgs $importArgs
    if (-not [bool]$importResult['Success']) {
      [void]$blockers.Add('import_failed')
      Write-Token 'PIPELINE_IMPORT_STATUS' 'failed'
    } else {
      Write-Token 'PIPELINE_IMPORT_STATUS' ($(if ($DryRun) { 'dry_run_ok' } else { 'applied' }))
    }
  }

  if (-not $suppressionExists) {
    [void]$blockers.Add('suppression_missing')
  }

  $ingestArgs = @(
    'run_osha_ingest_daily.py',
    '--since-days',
    [string]$SinceDays,
    '--max-details',
    '50'
  )
  if ($DryRun) {
    $ingestArgs += '--dry-run'
  }
  $ingestResult = Invoke-SecretsPythonStep -WrapperPath $wrapper -StepName 'INGEST' -PythonArgs $ingestArgs
  if (-not [bool]$ingestResult['Success']) {
    [void]$blockers.Add('ingest_failed')
  }

  $generationArgs = @('run_prospect_generation.py', '--for-date', $forDate)
  if ($selectedGenerationState) {
    $generationArgs += @('--states', $selectedGenerationState)
  }
  if ($DryRun) {
    $generationArgs += '--dry-run'
  }
  $generationResult = Invoke-SecretsPythonStep -WrapperPath $wrapper -StepName 'GENERATION' -PythonArgs $generationArgs
  if (-not [bool]$generationResult['Success']) {
    [void]$blockers.Add('generation_failed')
  }

  $discoveryArgs = @('run_prospect_discovery.py')
  if ($DryRun) {
    $discoveryArgs += '--print-config'
  }
  $discoveryResult = Invoke-SecretsPythonStep -WrapperPath $wrapper -StepName 'DISCOVERY' -PythonArgs $discoveryArgs
  if (-not [bool]$discoveryResult['Success']) {
    [void]$blockers.Add('discovery_failed')
  }

  $doctorResult = Invoke-SecretsPythonStep -WrapperPath $wrapper -StepName 'OUTREACH_DOCTOR' -PythonArgs @('run_outreach_auto.py', '--doctor', '--for-date', $forDate)
  if (-not [bool]$doctorResult['Success']) {
    [void]$blockers.Add('outreach_doctor_failed')
  }

  $outreachDryRunResult = Invoke-SecretsPythonStep -WrapperPath $wrapper -StepName 'OUTREACH_DRY_RUN' -PythonArgs @('run_outreach_auto.py', '--dry-run', '--for-date', $forDate)
  if (-not [bool]$outreachDryRunResult['Success']) {
    [void]$blockers.Add('outreach_dry_run_failed')
  }

  $trialDryRunResult = Invoke-SecretsPythonStep -WrapperPath $wrapper -StepName 'TRIAL_DRY_RUN' -PythonArgs @('run_wally_trial.py', '--test-send-daily', '--dry-run')
  if (-not [bool]$trialDryRunResult['Success']) {
    [void]$blockers.Add('trial_dry_run_failed')
  }

  if ($blockers.Count -gt 0) {
    Write-Token 'PIPELINE_READY_FOR_TOMORROW' '0'
    Write-Token 'PIPELINE_BLOCKERS' ($blockers -join ',')
    exit 1
  }

  Write-Token 'PIPELINE_READY_FOR_TOMORROW' '1'
  Write-Token 'PIPELINE_BLOCKERS' 'none'
  exit 0
}
finally {
  try { Pop-Location } catch {}
}
