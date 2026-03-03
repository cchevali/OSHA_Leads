Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$taskLogDir = Join-Path $repoRoot "out\task_logs"
$taskLogPath = Join-Path $taskLogDir ("OSHA_Trial_FACS_Daily_{0}.log" -f $timestamp)
$trialSubscriberKey = "facs_trial"
$trialExitCode = 1

New-Item -ItemType Directory -Force -Path $taskLogDir | Out-Null

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
    Invoke-And-Log {
      & (Join-Path $repoRoot "run_with_secrets.ps1") -- py -3 "run_trial_daily.py" --subscriber-key $trialSubscriberKey --send-live
    }
    $trialExitCode = [int]$LASTEXITCODE
  }
  catch {
    $trialExitCode = 1
    Write-TaskLine ('TRIAL_EXCEPTION=' + ([string]$_.Exception.Message))
  }
}
finally {
  try { Pop-Location } catch {}
}

Write-TaskLine ("TASK_LOG_PATH=" + $taskLogPath)
Write-TaskLine ("TRIAL_SUBSCRIBER_KEY=" + $trialSubscriberKey)
Write-TaskLine ("TRIAL_EXIT_CODE=" + $trialExitCode)

if ($trialExitCode -ne 0) {
  exit 1
}
exit 0
