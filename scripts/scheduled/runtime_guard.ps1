Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'

function Invoke-RuntimePreflight {
  param(
    [Parameter(Mandatory = $true)][string]$RepoRoot,
    [ValidateSet('scheduled', 'manual')][string]$Mode = 'manual',
    [ValidateSet('send', 'write', 'read')][string]$Intent = 'read',
    [bool]$DryRun = $false,
    [string]$TaskLogRoot = '',
    [string]$RunSummaryRoot = '',
    [bool]$RequireConfirmLiveSend = $false,
    [bool]$ConfirmLiveSend = $false,
    [scriptblock]$EmitLine = $null
  )

  $runtimeGuardPy = Join-Path $RepoRoot 'runtime_guard.py'
  if (-not (Test-Path -LiteralPath $runtimeGuardPy)) {
    throw ('ERR_RUNTIME_GUARD_MISSING path=' + $runtimeGuardPy)
  }

  $cmd = @('-3', $runtimeGuardPy, 'preflight', '--mode', $Mode, '--intent', $Intent)
  if ($DryRun) { $cmd += '--dry-run' }
  if ($TaskLogRoot) { $cmd += @('--task-log-root', $TaskLogRoot) }
  if ($RunSummaryRoot) { $cmd += @('--run-summary-root', $RunSummaryRoot) }
  if ($RequireConfirmLiveSend) { $cmd += '--require-confirm-live-send' }
  if ($ConfirmLiveSend) { $cmd += '--confirm-live-send' }

  $output = & py @cmd 2>&1
  $exitCode = [int]$LASTEXITCODE

  $values = @{}
  foreach ($line in @($output)) {
    $text = [string]$line
    if ($EmitLine) {
      & $EmitLine $text
    } else {
      Write-Output $text
    }
    if ($text -match '^([A-Z0-9_]+)=(.*)$') {
      $values[$matches[1]] = $matches[2]
    }
  }

  if ($values.ContainsKey('MFO_RUNTIME_MODE')) {
    $env:MFO_RUNTIME_MODE = [string]$values['MFO_RUNTIME_MODE']
  } else {
    $env:MFO_RUNTIME_MODE = $Mode
  }

  if ($values.ContainsKey('MFO_TRUSTED_SCHEDULED')) {
    $env:MFO_TRUSTED_SCHEDULED = [string]$values['MFO_TRUSTED_SCHEDULED']
  } else {
    $env:MFO_TRUSTED_SCHEDULED = '0'
  }

  foreach ($k in @('RUNTIME_HOSTNAME','RUNTIME_USERNAME','RUNTIME_ROLE','RUNTIME_DATA_DIR','RUNTIME_DATA_DIR_SOURCE','RUNTIME_REPO_ROOT','RUNTIME_DB_OSHA','RUNTIME_DB_CRM','RUNTIME_DB_CRM_LIGHT','RUNTIME_TIMEZONE','RUNTIME_GIT_SHA')) {
    if ($values.ContainsKey($k)) {
      Set-Item -Path ('Env:' + $k) -Value ([string]$values[$k])
    }
  }

  return @{
    Ok = ($exitCode -eq 0)
    ExitCode = $exitCode
    Values = $values
    Lines = @($output | ForEach-Object { [string]$_ })
  }
}
