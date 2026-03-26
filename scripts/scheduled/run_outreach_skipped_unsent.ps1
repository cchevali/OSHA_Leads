Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
. (Join-Path $PSScriptRoot "runtime_guard.ps1")
. (Join-Path $PSScriptRoot "runtime_run_summary.ps1")

$startLocal = Get-Date
$startUtc = [datetime]::UtcNow
$outreachExitCode = 0
$commandInvoked = ".\run_with_secrets.ps1 -- py -3 run_outreach_skipped_unsent.py --print-config (disabled)"

$taskLogDir = Resolve-DefaultTaskLogRoot -RepoRoot $repoRoot
$runSummaryRoot = Resolve-DefaultRunSummaryRoot -RepoRoot $repoRoot
$runId = New-RuntimeRunId -StartLocal $startLocal -StartUtc $startUtc
$taskLogPath = New-RuntimeTaskLogPath -TaskLogRoot $taskLogDir -WrapperName 'OSHA_Outreach_Skipped_Unsent_Extra' -RunId $runId

New-Item -ItemType Directory -Force -Path $taskLogDir | Out-Null
New-Item -ItemType Directory -Force -Path $runSummaryRoot | Out-Null

function Write-TaskLine([string]$Line) {
  $text = [string]$Line
  if ([string]::IsNullOrWhiteSpace($text)) {
    return
  }
  Write-Output $text
  Write-RuntimeTaskLogLine -TaskLogPath $taskLogPath -Line $text
}

Write-TaskLine 'OUTREACH_SKIPPED_UNSENT_SCHEDULED_DISABLED=1 mode=diagnostic_only remediation=remove_unmanaged_task'
Write-TaskLine ('WRAPPER_COMMAND=' + $commandInvoked)
Write-TaskLine ("TASK_LOG_PATH=" + $taskLogPath)
Write-TaskLine ("OUTREACH_EXIT_CODE=" + $outreachExitCode)

$summaryResult = Write-RuntimeRunSummary `
  -RepoRoot $repoRoot `
  -WrapperName 'OSHA_Outreach_Skipped_Unsent_Extra' `
  -CommandLine $commandInvoked `
  -Mode 'scheduled' `
  -Intent 'diagnostic' `
  -DryRun:$true `
  -ExitCode $outreachExitCode `
  -StartLocal $startLocal `
  -StartUtc $startUtc `
  -RunId $runId `
  -TaskLogPath $taskLogPath `
  -TaskLogRoot $taskLogDir `
  -RunSummaryRoot $runSummaryRoot `
  -Fingerprint @{} `
  -EmitLine ${function:Write-TaskLine}
# RUN_SUMMARY_JSON_PATH= / RUN_SUMMARY_TEXT_PATH= emitted above via Write-RuntimeRunSummary.

exit 0
