Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
. (Join-Path $PSScriptRoot "runtime_guard.ps1")
. (Join-Path $PSScriptRoot "runtime_run_summary.ps1")

$startLocal = Get-Date
$startUtc = [datetime]::UtcNow
$outreachExitCode = 1
$preflight = $null
$commandInvoked = ".\run_with_secrets.ps1 -- py -3 run_outreach_skipped_unsent.py --allow-second-live-run-same-day"
$bootstrapLines = New-Object System.Collections.Generic.List[string]

function Add-BootstrapLine([string]$Line) {
  $text = [string]$Line
  if ($text) {
    [void]$bootstrapLines.Add($text)
  }
}

$preflight = Invoke-RuntimePreflight `
  -RepoRoot $repoRoot `
  -Mode 'scheduled' `
  -Intent 'send' `
  -DryRun:$false `
  -EmitLine ${function:Add-BootstrapLine}

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
  Write-RuntimeTaskLogLine -TaskLogPath $taskLogPath -Line $text
}

foreach ($line in @($bootstrapLines)) {
  Write-TaskLine ([string]$line)
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
    if (-not [bool]$preflight.Ok) {
      throw "runtime preflight failed"
    }
    Invoke-And-Log {
      & (Join-Path $repoRoot "run_with_secrets.ps1") -- py -3 "run_outreach_skipped_unsent.py" "--allow-second-live-run-same-day"
    }
    $outreachExitCode = [int]$LASTEXITCODE
  }
  catch {
    $outreachExitCode = 1
    Write-TaskLine ('OUTREACH_EXCEPTION=' + ([string]$_.Exception.Message))
  }
}
finally {
  try { Pop-Location } catch {}
}

Write-TaskLine ("TASK_LOG_PATH=" + $taskLogPath)
Write-TaskLine ("OUTREACH_EXIT_CODE=" + $outreachExitCode)
$summaryResult = Write-RuntimeRunSummary `
  -RepoRoot $repoRoot `
  -WrapperName 'OSHA_Outreach_Skipped_Unsent_Extra' `
  -CommandLine $commandInvoked `
  -Mode 'scheduled' `
  -Intent 'send' `
  -DryRun:$false `
  -ExitCode $outreachExitCode `
  -StartLocal $startLocal `
  -StartUtc $startUtc `
  -RunId $runId `
  -TaskLogPath $taskLogPath `
  -TaskLogRoot $taskLogDir `
  -RunSummaryRoot $runSummaryRoot `
  -Fingerprint $(if ($preflight) { [hashtable]$preflight.Values } else { @{} }) `
  -EmitLine ${function:Write-TaskLine}
# RUN_SUMMARY_JSON_PATH= / RUN_SUMMARY_TEXT_PATH= emitted above via Write-RuntimeRunSummary.

if ($outreachExitCode -ne 0) {
  exit 1
}
exit 0
