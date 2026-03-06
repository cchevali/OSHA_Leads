Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
. (Join-Path $PSScriptRoot "runtime_guard.ps1")
. (Join-Path $PSScriptRoot "runtime_run_summary.ps1")

$startLocal = Get-Date
$startUtc = [datetime]::UtcNow
$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$taskLogDir = Resolve-DefaultTaskLogRoot -RepoRoot $repoRoot
$runSummaryRoot = Resolve-DefaultRunSummaryRoot -RepoRoot $repoRoot
$taskLogPath = Join-Path $taskLogDir ("OSHA_Prospect_Replenish_Daily_{0}.log" -f $timestamp)
$replenishExitCode = 1
$preflight = $null
$commandInvoked = ".\run_with_secrets.ps1 -- py -3 run_prospect_replenish_daily.py"

New-Item -ItemType Directory -Force -Path $taskLogDir | Out-Null
New-Item -ItemType Directory -Force -Path $runSummaryRoot | Out-Null

function Write-TaskLine([string]$Line) {
  $text = [string]$Line
  Write-Output $text
  Add-Content -Path $taskLogPath -Value $text -Encoding UTF8
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

    Invoke-And-Log {
      & (Join-Path $repoRoot "run_with_secrets.ps1") -- py -3 "run_prospect_replenish_daily.py"
    }
    $replenishExitCode = [int]$LASTEXITCODE
  }
  catch {
    $replenishExitCode = 1
    Write-TaskLine ('PROSPECT_REPLENISH_EXCEPTION=' + ([string]$_.Exception.Message))
  }
}
finally {
  try { Pop-Location } catch {}
}

Write-TaskLine ("TASK_LOG_PATH=" + $taskLogPath)
Write-TaskLine ("PROSPECT_REPLENISH_EXIT_CODE=" + $replenishExitCode)
Write-RuntimeRunSummary `
  -RepoRoot $repoRoot `
  -WrapperName 'OSHA_Prospect_Replenish_Daily' `
  -CommandLine $commandInvoked `
  -Mode 'scheduled' `
  -Intent 'write' `
  -DryRun:$false `
  -ExitCode $replenishExitCode `
  -StartLocal $startLocal `
  -StartUtc $startUtc `
  -TaskLogPath $taskLogPath `
  -TaskLogRoot $taskLogDir `
  -RunSummaryRoot $runSummaryRoot `
  -Fingerprint $(if ($preflight) { [hashtable]$preflight.Values } else { @{} }) `
  -EmitLine ${function:Write-TaskLine} | Out-Null
# RUN_SUMMARY_JSON_PATH= / RUN_SUMMARY_TEXT_PATH= emitted above via Write-RuntimeRunSummary.

if ($replenishExitCode -ne 0) {
  exit 1
}
exit 0
