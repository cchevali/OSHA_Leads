Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
. (Join-Path $PSScriptRoot "runtime_guard.ps1")
. (Join-Path $PSScriptRoot "runtime_run_summary.ps1")

$startLocal = Get-Date
$startUtc = [datetime]::UtcNow
$trialSubscriberKey = "jl_safety_trial"
$trialExitCode = 1
$preflight = $null
$runtimeTickState = $null
$commandInvoked = ".\run_with_secrets.ps1 -- py -3 run_trial_daily.py --subscriber-key $trialSubscriberKey --send-live"
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

$runtimeTickState = Test-RuntimeTickDailySlotAlreadyCompleted `
  -RepoRoot $repoRoot `
  -JobName 'trial_jl_safety_daily' `
  -NowLocal $startLocal `
  -EmitLine ${function:Add-BootstrapLine}

$taskLogDir = Resolve-DefaultTaskLogRoot -RepoRoot $repoRoot
$runSummaryRoot = Resolve-DefaultRunSummaryRoot -RepoRoot $repoRoot
$runId = New-RuntimeRunId -StartLocal $startLocal -StartUtc $startUtc
$taskLogPath = New-RuntimeTaskLogPath -TaskLogRoot $taskLogDir -WrapperName 'OSHA_Trial_JL_Safety_Daily' -RunId $runId

New-Item -ItemType Directory -Force -Path $taskLogDir | Out-Null
New-Item -ItemType Directory -Force -Path $runSummaryRoot | Out-Null

function Write-TaskLine([string]$Line) {
  Write-RuntimeTaskLogLine -TaskLogPath $taskLogPath -Line $Line
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
    if ([bool]$runtimeTickState.Skip) {
      $trialExitCode = 0
      Write-TaskLine ('TRIAL_SKIPPED reason=runtime_tick_same_slot slot=' + [string]$runtimeTickState.SlotKey)
    }
    else {
      Invoke-And-Log {
        & (Join-Path $repoRoot "run_with_secrets.ps1") -- py -3 "run_trial_daily.py" --subscriber-key $trialSubscriberKey --send-live
      }
      $trialExitCode = [int]$LASTEXITCODE
    }
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
$summaryResult = Write-RuntimeRunSummary `
  -RepoRoot $repoRoot `
  -WrapperName 'OSHA_Trial_JL_Safety_Daily' `
  -CommandLine $commandInvoked `
  -Mode 'scheduled' `
  -Intent 'send' `
  -DryRun:$false `
  -ExitCode $trialExitCode `
  -StartLocal $startLocal `
  -StartUtc $startUtc `
  -RunId $runId `
  -TaskLogPath $taskLogPath `
  -TaskLogRoot $taskLogDir `
  -RunSummaryRoot $runSummaryRoot `
  -Fingerprint $(if ($preflight) { [hashtable]$preflight.Values } else { @{} }) `
  -EmitLine ${function:Write-TaskLine}

if ($trialExitCode -ne 0) {
  exit 1
}
exit 0
