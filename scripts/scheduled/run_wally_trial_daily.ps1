Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'

function Get-TaskLogPath([string]$RepoRoot, [string]$BaseName) {
  $dataDir = ([string]$env:DATA_DIR).Trim()
  if ($dataDir) {
    $logRoot = Join-Path $dataDir 'task_logs'
  } else {
    $logRoot = Join-Path $RepoRoot 'out\task_logs'
  }
  New-Item -ItemType Directory -Force -Path $logRoot | Out-Null
  $datePart = (Get-Date).ToString('yyyy-MM-dd')
  return (Join-Path $logRoot ($BaseName + '_' + $datePart + '.log'))
}

function Write-TaskLogLine([string]$LogPath, [string]$Line) {
  Add-Content -LiteralPath $LogPath -Value $Line -Encoding utf8
}

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
$batchPath = Join-Path $repoRoot 'run_wally_trial_daily.bat'
$logPath = Get-TaskLogPath -RepoRoot $repoRoot -BaseName 'wally_trial_daily'
$startUtc = [datetime]::UtcNow
$stopwatch = [System.Diagnostics.Stopwatch]::StartNew()

try {
  Write-TaskLogLine -LogPath $logPath -Line ('TASK_RUN_START task=wally_trial_daily local_time=' + (Get-Date).ToString('s') + ' utc_time=' + $startUtc.ToString('o'))

  Push-Location $repoRoot
  try {
    & cmd.exe /c $batchPath *>&1 | Tee-Object -FilePath $logPath -Append
    $code = if ($null -ne $LASTEXITCODE) { [int]$LASTEXITCODE } else { 0 }
  } finally {
    try { Pop-Location } catch {}
  }
}
catch {
  $_ | Out-String | Tee-Object -FilePath $logPath -Append | Out-Null
  $code = 1
}
finally {
  $stopwatch.Stop()
  Write-TaskLogLine -LogPath $logPath -Line ('TASK_RUN_END task=wally_trial_daily local_time=' + (Get-Date).ToString('s') + ' exit_code=' + $code + ' elapsed_seconds=' + [math]::Round($stopwatch.Elapsed.TotalSeconds, 3))
}

exit $code
