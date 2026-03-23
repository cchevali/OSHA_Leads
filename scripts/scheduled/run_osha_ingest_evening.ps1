Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
. (Join-Path $PSScriptRoot "runtime_guard.ps1")
. (Join-Path $PSScriptRoot "runtime_run_summary.ps1")

$startLocal = Get-Date
$startUtc = [datetime]::UtcNow
$taskLogDir = Resolve-DefaultTaskLogRoot -RepoRoot $repoRoot
$runSummaryRoot = Resolve-DefaultRunSummaryRoot -RepoRoot $repoRoot
$runId = New-RuntimeRunId -StartLocal $startLocal -StartUtc $startUtc
$taskLogPath = New-RuntimeTaskLogPath -TaskLogRoot $taskLogDir -WrapperName 'OSHA_Osha_Ingest_Evening' -RunId $runId

New-Item -ItemType Directory -Force -Path $taskLogDir | Out-Null
New-Item -ItemType Directory -Force -Path $runSummaryRoot | Out-Null

$ingestExitCode = 1
$signalsDumpExitCode = 1
$signalsDumpOutputPath = ''
$signalsDumpOutreachMatched = ''
$signalsDumpSubscribersMatched = ''
$manualProspectPrepExitCode = 1
$manualProspectSkipListPath = ''
$manualProspectPromptOutputPath = ''
$preflight = $null
$commandInvoked = ".\run_with_secrets.ps1 -- py -3 run_osha_ingest_daily.py --scope-mode outreach_plus_trial_live; .\scripts\dump_signals_for_ai_review.ps1 -SinceDays 14; .\scripts\prepare_manual_prospect_research.ps1"

function Write-TaskLine([string]$Line) {
  $text = [string]$Line
  if ([string]::IsNullOrWhiteSpace($text)) {
    return
  }
  Write-RuntimeTaskLogLine -TaskLogPath $taskLogPath -Line $text
}

function Invoke-And-Log([scriptblock]$Invocation) {
  $lines = & $Invocation 2>&1
  foreach ($line in @($lines)) {
    Write-TaskLine ([string]$line)
  }
}

try {
  Push-Location $repoRoot
  try {
    $preflight = Invoke-RuntimePreflight `
      -RepoRoot $repoRoot `
      -Mode 'scheduled' `
      -Intent 'write' `
      -DryRun:$false `
      -TaskLogRoot $taskLogDir `
      -RunSummaryRoot $runSummaryRoot `
      -EmitLine ${function:Write-TaskLine}
    if (-not [bool]$preflight.Ok) {
      throw "runtime preflight failed"
    }

    Invoke-And-Log { & (Join-Path $repoRoot "run_with_secrets.ps1") -- py -3 "run_osha_ingest_daily.py" --scope-mode "outreach_plus_trial_live" }
    $ingestExitCode = [int]$LASTEXITCODE
  }
  catch {
    $ingestExitCode = 1
    Write-TaskLine ('INGEST_EXCEPTION=' + ([string]$_.Exception.Message))
  }
  finally {
    try {
      Invoke-And-Log { & powershell.exe -NoProfile -ExecutionPolicy Bypass -File (Join-Path $repoRoot "scripts\dump_signals_for_ai_review.ps1") -SinceDays 14 }
      $signalsDumpExitCode = [int]$LASTEXITCODE
    }
    catch {
      $signalsDumpExitCode = 1
      Write-TaskLine ('AI_REVIEW_DUMP_EXCEPTION=' + ([string]$_.Exception.Message))
    }
  }

  try {
    Invoke-And-Log { & powershell.exe -NoProfile -ExecutionPolicy Bypass -File (Join-Path $repoRoot "scripts\prepare_manual_prospect_research.ps1") }
    $manualProspectPrepExitCode = [int]$LASTEXITCODE
  }
  catch {
    $manualProspectPrepExitCode = 1
    Write-TaskLine ('MANUAL_PROSPECT_RESEARCH_PREP_EXCEPTION=' + ([string]$_.Exception.Message))
  }

  $logTail = Get-Content -Path $taskLogPath -ErrorAction SilentlyContinue
  foreach ($line in @($logTail)) {
    $text = [string]$line
    if ($text -match '^AI_REVIEW_DUMP_OUTPUT_PATH=(.+)$') {
      $signalsDumpOutputPath = $matches[1].Trim()
    } elseif ($text -match '^AI_REVIEW_DUMP_OUTREACH_MATCHED_TOTAL=(.+)$') {
      $signalsDumpOutreachMatched = $matches[1].Trim()
    } elseif ($text -match '^AI_REVIEW_DUMP_SUBSCRIBERS_MATCHED_TOTAL=(.+)$') {
      $signalsDumpSubscribersMatched = $matches[1].Trim()
    } elseif ($text -match '^MANUAL_PROSPECT_RESEARCH_SKIP_LIST_PATH=(.+)$') {
      $manualProspectSkipListPath = $matches[1].Trim()
    } elseif ($text -match '^MANUAL_PROSPECT_RESEARCH_PROMPT_OUTPUT_PATH=(.+)$') {
      $manualProspectPromptOutputPath = $matches[1].Trim()
    }
  }
}
finally {
  try { Pop-Location } catch {}
}

Write-TaskLine ("TASK_LOG_PATH=" + $taskLogPath)
Write-TaskLine ("INGEST_EXIT_CODE=" + $ingestExitCode)
Write-TaskLine ("AI_REVIEW_DUMP_EXIT_CODE=" + $signalsDumpExitCode)
if ($signalsDumpOutputPath) {
  Write-TaskLine ("AI_REVIEW_DUMP_OUTPUT_PATH=" + $signalsDumpOutputPath)
}
if ($signalsDumpOutreachMatched) {
  Write-TaskLine ("AI_REVIEW_DUMP_OUTREACH_MATCHED_TOTAL=" + $signalsDumpOutreachMatched)
}
if ($signalsDumpSubscribersMatched) {
  Write-TaskLine ("AI_REVIEW_DUMP_SUBSCRIBERS_MATCHED_TOTAL=" + $signalsDumpSubscribersMatched)
}
Write-TaskLine ("MANUAL_PROSPECT_RESEARCH_PREP_EXIT_CODE=" + $manualProspectPrepExitCode)
if ($manualProspectSkipListPath) {
  Write-TaskLine ("MANUAL_PROSPECT_RESEARCH_SKIP_LIST_PATH=" + $manualProspectSkipListPath)
}
if ($manualProspectPromptOutputPath) {
  Write-TaskLine ("MANUAL_PROSPECT_RESEARCH_PROMPT_OUTPUT_PATH=" + $manualProspectPromptOutputPath)
}
$summaryResult = Write-RuntimeRunSummary `
  -RepoRoot $repoRoot `
  -WrapperName 'OSHA_Osha_Ingest_Evening' `
  -CommandLine $commandInvoked `
  -Mode 'scheduled' `
  -Intent 'write' `
  -DryRun:$false `
  -ExitCode $(if ($ingestExitCode -ne 0 -or $signalsDumpExitCode -ne 0 -or $manualProspectPrepExitCode -ne 0) { 1 } else { 0 }) `
  -StartLocal $startLocal `
  -StartUtc $startUtc `
  -RunId $runId `
  -TaskLogPath $taskLogPath `
  -TaskLogRoot $taskLogDir `
  -RunSummaryRoot $runSummaryRoot `
  -Fingerprint $(if ($preflight) { [hashtable]$preflight.Values } else { @{} }) `
  -ExtraArtifactPaths @(@($signalsDumpOutputPath, $manualProspectSkipListPath, $manualProspectPromptOutputPath) | Where-Object { ([string]$_).Trim() }) `
  -EmitLine ${function:Write-TaskLine}
# RUN_SUMMARY_JSON_PATH= / RUN_SUMMARY_TEXT_PATH= emitted above via Write-RuntimeRunSummary.

if ($ingestExitCode -ne 0 -or $signalsDumpExitCode -ne 0 -or $manualProspectPrepExitCode -ne 0) {
  exit 1
}
exit 0
