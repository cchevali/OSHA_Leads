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
$dumpExitCode = 1
$dumpOutputPath = ''
$dumpOutreachMatched = ''
$dumpSubscribersMatched = ''
$preflight = $null
$commandInvoked = ".\run_with_secrets.ps1 -- py -3 run_osha_ingest_daily.py --scope-mode outreach_plus_trial_live; .\scripts\dump_signals_for_ai_review.ps1 -SinceDays 14"

function Write-TaskLine([string]$Line) {
  Write-RuntimeTaskLogLine -TaskLogPath $taskLogPath -Line $Line
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
      $dumpExitCode = [int]$LASTEXITCODE
    }
    catch {
      $dumpExitCode = 1
      Write-TaskLine ('AI_REVIEW_DUMP_EXCEPTION=' + ([string]$_.Exception.Message))
    }
  }

  $logTail = Get-Content -Path $taskLogPath -ErrorAction SilentlyContinue
  foreach ($line in @($logTail)) {
    $text = [string]$line
    if ($text -match '^AI_REVIEW_DUMP_OUTPUT_PATH=(.+)$') {
      $dumpOutputPath = $matches[1].Trim()
    } elseif ($text -match '^AI_REVIEW_DUMP_OUTREACH_MATCHED_TOTAL=(.+)$') {
      $dumpOutreachMatched = $matches[1].Trim()
    } elseif ($text -match '^AI_REVIEW_DUMP_SUBSCRIBERS_MATCHED_TOTAL=(.+)$') {
      $dumpSubscribersMatched = $matches[1].Trim()
    }
  }
}
finally {
  try { Pop-Location } catch {}
}

Write-TaskLine ("TASK_LOG_PATH=" + $taskLogPath)
Write-TaskLine ("INGEST_EXIT_CODE=" + $ingestExitCode)
Write-TaskLine ("AI_REVIEW_DUMP_EXIT_CODE=" + $dumpExitCode)
if ($dumpOutputPath) {
  Write-TaskLine ("AI_REVIEW_DUMP_OUTPUT_PATH=" + $dumpOutputPath)
}
if ($dumpOutreachMatched) {
  Write-TaskLine ("AI_REVIEW_DUMP_OUTREACH_MATCHED_TOTAL=" + $dumpOutreachMatched)
}
if ($dumpSubscribersMatched) {
  Write-TaskLine ("AI_REVIEW_DUMP_SUBSCRIBERS_MATCHED_TOTAL=" + $dumpSubscribersMatched)
}
$summaryResult = Write-RuntimeRunSummary `
  -RepoRoot $repoRoot `
  -WrapperName 'OSHA_Osha_Ingest_Evening' `
  -CommandLine $commandInvoked `
  -Mode 'scheduled' `
  -Intent 'write' `
  -DryRun:$false `
  -ExitCode $(if ($ingestExitCode -ne 0 -or $dumpExitCode -ne 0) { 1 } else { 0 }) `
  -StartLocal $startLocal `
  -StartUtc $startUtc `
  -RunId $runId `
  -TaskLogPath $taskLogPath `
  -TaskLogRoot $taskLogDir `
  -RunSummaryRoot $runSummaryRoot `
  -Fingerprint $(if ($preflight) { [hashtable]$preflight.Values } else { @{} }) `
  -ExtraArtifactPaths @($dumpOutputPath) `
  -EmitLine ${function:Write-TaskLine}
# RUN_SUMMARY_JSON_PATH= / RUN_SUMMARY_TEXT_PATH= emitted above via Write-RuntimeRunSummary.

if ($ingestExitCode -ne 0 -or $dumpExitCode -ne 0) {
  exit 1
}
exit 0
