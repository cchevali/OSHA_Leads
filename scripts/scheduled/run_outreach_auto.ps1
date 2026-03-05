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
$taskLogPath = Join-Path $taskLogDir ("OSHA_Outreach_Auto_{0}.log" -f $timestamp)
$outreachExitCode = 1
$preflight = $null
$commandInvoked = ".\run_with_secrets.ps1 -- py -3 run_outreach_auto.py"

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
      -Intent 'send' `
      -DryRun:$false `
      -TaskLogRoot $taskLogDir `
      -RunSummaryRoot $runSummaryRoot `
      -EmitLine ${function:Write-TaskLine}
    if (-not [bool]$preflight.Ok) {
      throw "runtime preflight failed"
    }

    Invoke-And-Log {
      & (Join-Path $repoRoot "run_with_secrets.ps1") -- py -3 "run_outreach_auto.py"
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
Write-RuntimeRunSummary `
  -RepoRoot $repoRoot `
  -WrapperName 'OSHA_Outreach_Auto' `
  -CommandLine $commandInvoked `
  -Mode 'scheduled' `
  -Intent 'send' `
  -DryRun:$false `
  -ExitCode $outreachExitCode `
  -StartLocal $startLocal `
  -StartUtc $startUtc `
  -TaskLogPath $taskLogPath `
  -TaskLogRoot $taskLogDir `
  -RunSummaryRoot $runSummaryRoot `
  -Fingerprint $(if ($preflight) { [hashtable]$preflight.Values } else { @{} }) `
  -EmitLine ${function:Write-TaskLine} | Out-Null

if ($outreachExitCode -ne 0) {
  exit 1
}
exit 0
