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

function Resolve-DailyStartBoundaryFromNow([datetime]$NowLocal) {
  $candidate = Resolve-MinuteStartBoundary -NowLocal $NowLocal
  return $candidate
}

function Get-TaskDefinitions([string]$RepoRoot) {
  $autosaveScript = Join-Path $RepoRoot 'scripts\autosave_wip.ps1'
  $taskRun = 'cmd.exe /c cd /d ' + $RepoRoot + ' && powershell.exe -NoProfile -ExecutionPolicy Bypass -File ' + $autosaveScript
  return @(
    (New-TaskDefinition -Name 'OSHA_WIP_Autosave_Logon' -ScheduleType 'logon' -TaskRun $taskRun -RunLevel 'HIGHEST'),
    (New-TaskDefinition -Name 'OSHA_WIP_Autosave_Hourly' -ScheduleType 'minute' -MinuteInterval 15 -TaskRun $taskRun -RunLevel 'LIMITED')
  )
}

function Build-LogonElevatedCommand([hashtable]$Task) {
  $taskName = '\' + $Task.Name
  return 'schtasks /Create /F /SC ONLOGON /TN "' + $taskName + '" /TR "' + $Task.TaskRun + '" /RL HIGHEST'
}

function Get-ReminderTaskDefinition([string]$RepoRoot, [datetime]$NowLocal) {
  $installerScript = Join-Path $RepoRoot 'scripts\install_wip_autosave_task.ps1'
  $logPath = Join-Path $RepoRoot 'out\wip_autosave_logon_reminder.log'
  $taskRun = 'cmd.exe /c cd /d ' + $RepoRoot + ' && powershell.exe -NoProfile -ExecutionPolicy Bypass -File ' + $installerScript + ' --status >> ' + $logPath + ' 2>&1'
  $task = New-TaskDefinition -Name 'OSHA_WIP_Autosave_Logon_Reminder' -ScheduleType 'daily' -TaskRun $taskRun -RunLevel 'LIMITED'
  $resolved = @(Add-ResolvedSchedule -Tasks @($task) -NowLocal $NowLocal)
  return $resolved[0]
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
      $minuteBoundary = Resolve-MinuteStartBoundary -NowLocal $NowLocal
      $entry['StartBoundary'] = $minuteBoundary
      $entry['StartDate'] = $minuteBoundary.ToString('MM/dd/yyyy')
      $entry['StartTimeResolved'] = $minuteBoundary.ToString('HH:mm')
    }
    elseif ($task.ScheduleType -eq 'daily') {
      $dailyBoundary = Resolve-DailyStartBoundaryFromNow -NowLocal $NowLocal
      $entry['StartBoundary'] = $dailyBoundary
      $entry['StartDate'] = $dailyBoundary.ToString('MM/dd/yyyy')
      $entry['StartTimeResolved'] = $dailyBoundary.ToString('HH:mm')
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
    elseif ($task.ScheduleType -eq 'daily') {
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
  if ($Task.ScheduleType -eq 'daily') {
    return 'schtasks /Create /F /SC DAILY /SD ' + $Task.StartDate + ' /ST ' + $Task.StartTimeResolved + ' /TN "' + $taskName + '" /TR "' + $Task.TaskRun + '" /RL ' + $Task.RunLevel
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

function Parse-TaskQueryOutput([string[]]$Lines) {
  $fields = @{}
  foreach ($line in $Lines) {
    $raw = [string]$line
    foreach ($text in ($raw -split "`r?`n")) {
      if (-not $text) {
        continue
      }
      if ($text -match '^\s*([^:]+):\s*(.*)$') {
        $k = $matches[1].Trim()
        $v = $matches[2].Trim()
        if ($k -eq 'Repeat') {
          $everyMatch = [regex]::Match($v, '^(?i)Every:\s*(.*)$')
          if ($everyMatch.Success) {
            $fields['Repeat: Every'] = $everyMatch.Groups[1].Value.Trim()
          }
        }
        if (-not $fields.ContainsKey($k)) {
          $fields[$k] = $v
        }
      }
    }
  }
  return $fields
}

function Query-TaskFields([string]$TaskName) {
  $taskRef = '\' + $TaskName
  $query = Invoke-SchtasksCommand -SchtasksArgs @('/Query', '/TN', $taskRef, '/V', '/FO', 'LIST')
  if ([int]$query.ExitCode -ne 0) {
    $text = ((@($query.Output) | ForEach-Object { [string]$_ }) -join ' ')
    if ($text -match 'system cannot find the file specified') {
      return $null
    }
    Fail 'ERR_INSTALL_WIP_AUTOSAVE_TASK_STATUS' ('query_failed task=' + $TaskName + ' detail=' + $text)
  }
  return Parse-TaskQueryOutput -Lines @($query.Output)
}

function Parse-RepeatMinutes([hashtable]$Fields) {
  if ($null -eq $Fields) {
    return 999999
  }
  $repeat = ''
  if ($Fields.ContainsKey('Repeat: Every')) {
    $repeat = [string]$Fields['Repeat: Every']
  }
  elseif ($Fields.ContainsKey('Repeat')) {
    $repeatRaw = [string]$Fields['Repeat']
    if ($repeatRaw -match '(?i)^Every:\s*(.*)$') {
      $repeat = [string]$matches[1]
    }
    else {
      $repeat = $repeatRaw
    }
  }
  if (-not $repeat) {
    return 999999
  }

  $minutes = 0
  $hourMatch = [regex]::Match($repeat, '(?i)(\d+)\s*Hour\(s\)')
  if ($hourMatch.Success) {
    $minutes += ([int]$hourMatch.Groups[1].Value) * 60
  }
  $minuteMatch = [regex]::Match($repeat, '(?i)(\d+)\s*Minute\(s\)')
  if ($minuteMatch.Success) {
    $minutes += [int]$minuteMatch.Groups[1].Value
  }

  if ($minutes -le 0) {
    return 999999
  }
  return $minutes
}

function Invoke-TaskCreate([hashtable]$Task) {
  $taskName = '\' + $Task.Name
  $taskArgs = @('/Create', '/F', '/SC')

  if ($Task.ScheduleType -eq 'logon') {
    $taskArgs += @('ONLOGON')
  }
  elseif ($Task.ScheduleType -eq 'minute') {
    $taskArgs += @('MINUTE', '/MO', ([string]$Task.MinuteInterval), '/SD', $Task.StartDate, '/ST', $Task.StartTimeResolved)
  }
  elseif ($Task.ScheduleType -eq 'daily') {
    $taskArgs += @('DAILY', '/SD', $Task.StartDate, '/ST', $Task.StartTimeResolved)
  }
  else {
    Fail 'ERR_INSTALL_WIP_AUTOSAVE_TASK_CONFIG' ('unknown_schedule_type=' + $Task.ScheduleType)
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

function Delete-TaskIfExists([string]$TaskName) {
  $taskRef = '\' + $TaskName
  $delete = Invoke-SchtasksCommand -SchtasksArgs @('/Delete', '/TN', $taskRef, '/F')
  if ([int]$delete.ExitCode -eq 0) {
    return
  }
  $text = ((@($delete.Output) | ForEach-Object { [string]$_ }) -join ' ')
  if ($text -match 'system cannot find the file specified') {
    return
  }
  Fail 'ERR_INSTALL_WIP_AUTOSAVE_TASK_APPLY' ('delete_failed task=' + $TaskName + ' detail=' + $text)
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

function Emit-Status([array]$Tasks, [bool]$IsElevated) {
  $hourlyTask = Find-TaskByName -Tasks $Tasks -Name 'OSHA_WIP_Autosave_Hourly'
  $logonTask = Find-TaskByName -Tasks $Tasks -Name 'OSHA_WIP_Autosave_Logon'
  if ($null -eq $hourlyTask -or $null -eq $logonTask) {
    Fail 'ERR_INSTALL_WIP_AUTOSAVE_TASK_CONFIG' 'required_task_missing'
  }

  $hourlyFields = Query-TaskFields -TaskName $hourlyTask.Name
  $logonFields = Query-TaskFields -TaskName $logonTask.Name

  $hourlyInstalled = ($null -ne $hourlyFields)
  $logonInstalled = ($null -ne $logonFields)
  $repeatMinutes = Parse-RepeatMinutes -Fields $hourlyFields
  $effective = $false
  if ($logonInstalled) {
    $effective = $true
  }
  elseif ($hourlyInstalled -and $repeatMinutes -le 15) {
    $effective = $true
  }

  $nextAction = 'none'
  if (-not $logonInstalled) {
    $nextAction = 'run_elevated_cmd'
  }

  if ($hourlyInstalled) {
    Write-Output 'WIP_AUTOSAVE_HOURLY_INSTALLED=1'
  }
  else {
    Write-Output 'WIP_AUTOSAVE_HOURLY_INSTALLED=0'
  }

  if ($logonInstalled) {
    Write-Output 'WIP_AUTOSAVE_LOGON_INSTALLED=1'
  }
  else {
    Write-Output 'WIP_AUTOSAVE_LOGON_INSTALLED=0'
  }

  if ($effective) {
    Write-Output 'WIP_AUTOSAVE_EFFECTIVE=1'
  }
  else {
    Write-Output 'WIP_AUTOSAVE_EFFECTIVE=0'
  }

  Write-Output ('WIP_AUTOSAVE_NEXT_ACTION=' + $nextAction)
  if ($nextAction -eq 'run_elevated_cmd') {
    Write-Output ('WIP_AUTOSAVE_LOGON_INSTALL_ELEVATED_CMD=' + (Build-LogonElevatedCommand -Task $logonTask))
  }

  if ($IsElevated) {
    Write-Output 'INSTALL_WIP_AUTOSAVE_TASK_ELEVATED=YES'
  }
  else {
    Write-Output 'INSTALL_WIP_AUTOSAVE_TASK_ELEVATED=NO'
  }
  Write-Output 'PASS_INSTALL_WIP_AUTOSAVE_TASK_STATUS'
}

$modes = @('--print-config', '--dry-run', '--apply', '--status')
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

if ($modeArg -eq '--status') {
  Emit-Status -Tasks $resolvedTasks -IsElevated $isElevated
  exit 0
}

$hourlyTask = Find-TaskByName -Tasks $resolvedTasks -Name 'OSHA_WIP_Autosave_Hourly'
$logonTask = Find-TaskByName -Tasks $resolvedTasks -Name 'OSHA_WIP_Autosave_Logon'
if ($null -eq $hourlyTask -or $null -eq $logonTask) {
  Fail 'ERR_INSTALL_WIP_AUTOSAVE_TASK_CONFIG' 'required_task_missing'
}

Emit-TaskConfig -Tasks $resolvedTasks -Mode 'apply' -IsElevated $isElevated

$hourlyCreate = Invoke-TaskCreate -Task $hourlyTask
if ([bool]$hourlyCreate.AccessDenied) {
  Fail 'ERR_INSTALL_WIP_AUTOSAVE_TASK_APPLY' ('task=' + $hourlyTask.Name + ' access_denied=1 detail=' + (([string]$hourlyCreate.Detail).Trim()))
}
Write-Output ('TASK_APPLIED=' + $hourlyTask.Name)

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
  $elevatedCmd = Build-LogonElevatedCommand -Task $logonTask
  Write-Output ('WIP_AUTOSAVE_LOGON_INSTALL_ELEVATED_CMD=' + $elevatedCmd)

  $outDir = Join-Path $repoRoot 'out'
  New-Item -ItemType Directory -Force -Path $outDir | Out-Null

  $reminderTask = Get-ReminderTaskDefinition -RepoRoot $repoRoot -NowLocal (Get-Date)
  $reminderCreate = Invoke-TaskCreate -Task $reminderTask
  if ([bool]$reminderCreate.AccessDenied) {
    Fail 'ERR_INSTALL_WIP_AUTOSAVE_TASK_APPLY' ('task=' + $reminderTask.Name + ' access_denied=1 detail=' + (([string]$reminderCreate.Detail).Trim()))
  }
  Write-Output ('TASK_APPLIED=' + $reminderTask.Name)
}
else {
  Write-Output ('TASK_APPLIED=' + $logonTask.Name)
  Delete-TaskIfExists -TaskName 'OSHA_WIP_Autosave_Logon_Reminder'
}

Write-Output 'PASS_INSTALL_WIP_AUTOSAVE_TASK_APPLY'
exit 0
