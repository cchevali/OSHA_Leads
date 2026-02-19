Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'

if ($PSVersionTable.PSVersion.Major -ge 7) {
  $PSNativeCommandUseErrorActionPreference = $false
}

function Fail([string]$Token, [string]$Message) {
  Write-Output ($Token + ' ' + $Message)
  exit 1
}

function Test-IsElevated {
  try {
    $identity = [Security.Principal.WindowsIdentity]::GetCurrent()
    $principal = New-Object Security.Principal.WindowsPrincipal($identity)
    return $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
  }
  catch {
    return $false
  }
}

function New-TaskDefinition(
  [string]$Name,
  [string]$ScheduleType,
  [string]$TaskRun,
  [string]$RunLevel = 'HIGHEST',
  [int]$MinuteInterval = 0
) {
  return @{
    Name = $Name
    ScheduleType = $ScheduleType
    MinuteInterval = $MinuteInterval
    TaskRun = $TaskRun
    RunLevel = $RunLevel
  }
}

function Resolve-MinuteStartBoundary([datetime]$NowLocal) {
  $candidate = $NowLocal.AddMinutes(5)
  if ($candidate.Second -ne 0 -or $candidate.Millisecond -ne 0) {
    $candidate = $candidate.AddMinutes(1)
  }
  return [datetime]::new($candidate.Year, $candidate.Month, $candidate.Day, $candidate.Hour, $candidate.Minute, 0)
}

function Get-TaskDefinitions([string]$RepoRoot) {
  $autosaveScript = Join-Path $RepoRoot 'scripts\autosave_wip.ps1'
  $taskRun = 'cmd.exe /c cd /d ' + $RepoRoot + ' && powershell.exe -NoProfile -ExecutionPolicy Bypass -File ' + $autosaveScript
  return @(
    (New-TaskDefinition -Name 'OSHA_WIP_Autosave_Logon' -ScheduleType 'logon' -TaskRun $taskRun -RunLevel 'HIGHEST'),
    (New-TaskDefinition -Name 'OSHA_WIP_Autosave_Hourly' -ScheduleType 'minute' -MinuteInterval 60 -TaskRun $taskRun -RunLevel 'LIMITED')
  )
}

function Add-ResolvedSchedule([array]$Tasks, [datetime]$NowLocal) {
  $resolved = @()
  for ($i = 0; $i -lt $Tasks.Count; $i++) {
    $task = $Tasks[$i]
    $entry = @{}
    foreach ($k in $task.Keys) {
      $entry[$k] = $task[$k]
    }
    if ($task.ScheduleType -eq 'minute') {
      $boundary = Resolve-MinuteStartBoundary -NowLocal $NowLocal
      $entry['StartBoundary'] = $boundary
      $entry['StartDate'] = $boundary.ToString('MM/dd/yyyy')
      $entry['StartTimeResolved'] = $boundary.ToString('HH:mm')
    }
    $resolved += $entry
  }
  return $resolved
}

function Emit-TaskConfig([array]$Tasks, [string]$Mode, [bool]$IsElevated) {
  Write-Output ('INSTALL_WIP_AUTOSAVE_TASK_MODE=' + $Mode)
  if ($IsElevated) {
    Write-Output 'INSTALL_WIP_AUTOSAVE_TASK_ELEVATED=YES'
  }
  else {
    Write-Output 'INSTALL_WIP_AUTOSAVE_TASK_ELEVATED=NO'
  }
  Write-Output 'WIP_AUTOSAVE_RUN_FROM_REPO_ROOT=powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\autosave_wip.ps1'
  Write-Output 'WIP_AUTOSAVE_INSTALL_FROM_REPO_ROOT_APPLY=powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\install_wip_autosave_task.ps1 --apply'
  Write-Output ('INSTALL_WIP_AUTOSAVE_TASK_COUNT=' + $Tasks.Count)
  for ($i = 0; $i -lt $Tasks.Count; $i++) {
    $idx = $i + 1
    $task = $Tasks[$i]
    Write-Output ('TASK_' + $idx + '_NAME=' + $task.Name)
    Write-Output ('TASK_' + $idx + '_SCHEDULE=' + $task.ScheduleType)
    Write-Output ('TASK_' + $idx + '_RL=' + $task.RunLevel)
    Write-Output ('TASK_' + $idx + '_TR=' + $task.TaskRun)
    if ($task.ScheduleType -eq 'minute') {
      Write-Output ('TASK_' + $idx + '_MINUTE_INTERVAL=' + $task.MinuteInterval)
      Write-Output ('TASK_' + $idx + '_START_DATE=' + $task.StartDate)
      Write-Output ('TASK_' + $idx + '_START_TIME=' + $task.StartTimeResolved)
      Write-Output ('TASK_' + $idx + '_START_BOUNDARY_LOCAL=' + $task.StartBoundary.ToString('yyyy-MM-ddTHH:mm:ss'))
    }
  }
}

function Build-SchtasksPreviewLine([hashtable]$Task) {
  $taskName = '\' + $Task.Name
  if ($Task.ScheduleType -eq 'logon') {
    return 'schtasks /Create /F /SC ONLOGON /TN "' + $taskName + '" /TR "' + $Task.TaskRun + '" /RL ' + $Task.RunLevel
  }
  return 'schtasks /Create /F /SC MINUTE /MO ' + $Task.MinuteInterval + ' /SD ' + $Task.StartDate + ' /ST ' + $Task.StartTimeResolved + ' /TN "' + $taskName + '" /TR "' + $Task.TaskRun + '" /RL ' + $Task.RunLevel
}

function Invoke-SchtasksCommand([string[]]$SchtasksArgs) {
  $prevErrorAction = $ErrorActionPreference
  $ErrorActionPreference = 'Continue'
  try {
    $output = & schtasks.exe @SchtasksArgs 2>&1
    $code = $LASTEXITCODE
  }
  finally {
    $ErrorActionPreference = $prevErrorAction
  }
  return @{
    Output = @($output)
    ExitCode = [int]$code
  }
}

function Invoke-TaskCreate([hashtable]$Task) {
  $taskName = '\' + $Task.Name
  $taskArgs = @('/Create', '/F', '/SC')
  if ($Task.ScheduleType -eq 'logon') {
    $taskArgs += @('ONLOGON')
  }
  else {
    $taskArgs += @('MINUTE', '/MO', ([string]$Task.MinuteInterval), '/SD', $Task.StartDate, '/ST', $Task.StartTimeResolved)
  }
  $taskArgs += @('/TN', $taskName, '/TR', $Task.TaskRun, '/RL', $Task.RunLevel)

  $create = Invoke-SchtasksCommand -SchtasksArgs $taskArgs
  if ([int]$create.ExitCode -eq 0) {
    return @{
      Applied = $true
      AccessDenied = $false
      Detail = ''
    }
  }

  $detail = ((@($create.Output) | ForEach-Object { [string]$_ }) -join ' ')
  if ($detail -match 'Access is denied') {
    return @{
      Applied = $false
      AccessDenied = $true
      Detail = $detail
    }
  }

  Fail 'ERR_INSTALL_WIP_AUTOSAVE_TASK_APPLY' ('task=' + $Task.Name + ' detail=' + $detail)
}

function Find-TaskByName([array]$Tasks, [string]$Name) {
  for ($i = 0; $i -lt $Tasks.Count; $i++) {
    if ([string]$Tasks[$i].Name -eq $Name) {
      return $Tasks[$i]
    }
  }
  return $null
}

function Copy-Hashtable([hashtable]$Source) {
  $copy = @{}
  foreach ($k in $Source.Keys) {
    $copy[$k] = $Source[$k]
  }
  return $copy
}

function Build-LogonElevatedCommand([hashtable]$Task) {
  $taskName = '\' + $Task.Name
  return 'schtasks /Create /F /SC ONLOGON /TN "' + $taskName + '" /TR "' + $Task.TaskRun + '" /RL HIGHEST'
}

$modes = @('--print-config', '--dry-run', '--apply')
if ($args.Count -ne 1) {
  Fail 'ERR_INSTALL_WIP_AUTOSAVE_TASK_ARGS' ('expected one of: ' + ($modes -join ', '))
}

$modeArg = [string]$args[0]
if ($modeArg -notin $modes) {
  Fail 'ERR_INSTALL_WIP_AUTOSAVE_TASK_ARGS' ('unknown flag: ' + $modeArg)
}

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$autosaveScriptPath = Join-Path $repoRoot 'scripts\autosave_wip.ps1'
if (-not (Test-Path -LiteralPath $autosaveScriptPath)) {
  Fail 'ERR_INSTALL_WIP_AUTOSAVE_TASK_SCRIPT_MISSING' ('missing ' + $autosaveScriptPath)
}

$rawTasks = Get-TaskDefinitions -RepoRoot $repoRoot
$resolvedTasks = Add-ResolvedSchedule -Tasks $rawTasks -NowLocal (Get-Date)
$isElevated = Test-IsElevated

if ($modeArg -eq '--print-config') {
  Emit-TaskConfig -Tasks $resolvedTasks -Mode 'print-config' -IsElevated $isElevated
  Write-Output 'PASS_INSTALL_WIP_AUTOSAVE_TASK_PRINT_CONFIG'
  exit 0
}

if ($modeArg -eq '--dry-run') {
  Emit-TaskConfig -Tasks $resolvedTasks -Mode 'dry-run' -IsElevated $isElevated
  for ($i = 0; $i -lt $resolvedTasks.Count; $i++) {
    $idx = $i + 1
    Write-Output ('DRY_RUN_COMMAND_' + $idx + '=' + (Build-SchtasksPreviewLine -Task $resolvedTasks[$i]))
  }
  Write-Output 'PASS_INSTALL_WIP_AUTOSAVE_TASK_DRY_RUN'
  exit 0
}

$hourlyTask = Find-TaskByName -Tasks $resolvedTasks -Name 'OSHA_WIP_Autosave_Hourly'
$logonTask = Find-TaskByName -Tasks $resolvedTasks -Name 'OSHA_WIP_Autosave_Logon'
if ($null -eq $hourlyTask -or $null -eq $logonTask) {
  Fail 'ERR_INSTALL_WIP_AUTOSAVE_TASK_CONFIG' 'required_task_missing'
}

Emit-TaskConfig -Tasks $resolvedTasks -Mode 'apply' -IsElevated $isElevated

# Hourly task is always attempted first, with least privilege.
$hourlyCreate = Invoke-TaskCreate -Task $hourlyTask
if ([bool]$hourlyCreate.AccessDenied) {
  Fail 'ERR_INSTALL_WIP_AUTOSAVE_TASK_APPLY' ('task=' + $hourlyTask.Name + ' access_denied=1 detail=' + (([string]$hourlyCreate.Detail).Trim()))
}
Write-Output ('TASK_APPLIED=' + $hourlyTask.Name)

# ONLOGON install is best-effort in non-elevated sessions.
$logonApplyTask = Copy-Hashtable -Source $logonTask
if (-not $isElevated) {
  $logonApplyTask['RunLevel'] = 'LIMITED'
}
$logonCreate = Invoke-TaskCreate -Task $logonApplyTask
if ([bool]$logonCreate.AccessDenied) {
  if ($isElevated) {
    Fail 'ERR_INSTALL_WIP_AUTOSAVE_TASK_APPLY' ('task=' + $logonTask.Name + ' access_denied=1 detail=' + (([string]$logonCreate.Detail).Trim()))
  }
  Write-Output 'WARN_WIP_AUTOSAVE_LOGON_NOT_INSTALLED access_denied=1'
  Write-Output ('WIP_AUTOSAVE_LOGON_INSTALL_ELEVATED_CMD=' + (Build-LogonElevatedCommand -Task $logonTask))
}
else {
  Write-Output ('TASK_APPLIED=' + $logonTask.Name)
}
Write-Output 'PASS_INSTALL_WIP_AUTOSAVE_TASK_APPLY'
exit 0
