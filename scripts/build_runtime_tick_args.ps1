param(
  [string]$GithubEventName = '',
  [string]$Mode = '',
  [string]$Job = '',
  [string]$NowLocal = '',
  [string]$Force = ''
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$modeValue = 'live'
$jobValue = 'all'
$nowLocalValue = ''
$forceValue = 'false'

if (([string]$GithubEventName).Trim() -eq 'workflow_dispatch') {
  if (([string]$Mode).Trim()) { $modeValue = ([string]$Mode).Trim() }
  if (([string]$Job).Trim()) { $jobValue = ([string]$Job).Trim() }
  if (([string]$NowLocal).Trim()) { $nowLocalValue = ([string]$NowLocal).Trim() }
  if (([string]$Force).Trim()) { $forceValue = ([string]$Force).Trim() }
}

$modeNorm = $modeValue.Trim().ToLowerInvariant()
if (@('live', 'doctor', 'dry_run') -notcontains $modeNorm) {
  Write-Output ('ERR_RUNTIME_TICK_WORKFLOW_INVALID_MODE mode=' + $modeNorm)
  exit 1
}

$argsOut = New-Object System.Collections.Generic.List[string]
[void]$argsOut.Add('run_runtime_tick.py')
if ($modeNorm -eq 'doctor') {
  [void]$argsOut.Add('--doctor')
}
elseif ($modeNorm -eq 'dry_run') {
  [void]$argsOut.Add('--dry-run')
}
[void]$argsOut.Add('--job')
[void]$argsOut.Add($jobValue)
if ($nowLocalValue) {
  [void]$argsOut.Add('--now-local')
  [void]$argsOut.Add($nowLocalValue)
}
if ($forceValue.Trim().ToLowerInvariant() -eq 'true') {
  [void]$argsOut.Add('--force')
}

$serialized = ($argsOut -join ' ')
if (-not ([string]$env:GITHUB_OUTPUT).Trim()) {
  Write-Output 'ERR_RUNTIME_TICK_WORKFLOW_ARGS_OUTPUT_MISSING'
  exit 1
}
"cmd_args=$serialized" | Out-File -FilePath $env:GITHUB_OUTPUT -Append -Encoding utf8
Write-Output ('RUNTIME_TICK_WORKFLOW_ARGS=' + $serialized)
