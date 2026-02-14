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

function New-TaskDefinition(
  [string]$Name,
  [string]$ScheduleType,
  [string]$StartTime,
  [string]$TaskRun,
  [int]$MinuteInterval = 0
) {
  return @{
    Name = $Name
    ScheduleType = $ScheduleType
    StartTime = $StartTime
    MinuteInterval = $MinuteInterval
    TaskRun = $TaskRun
    RunLevel = 'HIGHEST'
  }
}

function Get-TaskDefinitions([string]$RepoRoot) {
  $generationRunner = Join-Path $RepoRoot 'scripts\scheduled\run_prospect_generation.ps1'
  $inboundRunner = Join-Path $RepoRoot 'scripts\scheduled\run_inbound_triage.ps1'
  $wrapper = Join-Path $RepoRoot 'run_with_secrets.ps1'
  $discovery = Join-Path $RepoRoot 'run_prospect_discovery.py'
  $outreach = Join-Path $RepoRoot 'run_outreach_auto.py'

  return @(
    (New-TaskDefinition -Name 'OSHA_Prospect_Generation' -ScheduleType 'daily' -StartTime '07:15' -TaskRun ('powershell.exe -NoProfile -ExecutionPolicy Bypass -File ' + $generationRunner)),
    (New-TaskDefinition -Name 'OSHA_Prospect_Discovery' -ScheduleType 'daily' -StartTime '07:30' -TaskRun ('powershell.exe -NoProfile -ExecutionPolicy Bypass -File ' + $wrapper + ' -- py -3 ' + $discovery)),
    (New-TaskDefinition -Name 'OSHA_Outreach_Auto' -ScheduleType 'daily' -StartTime '08:00' -TaskRun ('powershell.exe -NoProfile -ExecutionPolicy Bypass -File ' + $wrapper + ' -- py -3 ' + $outreach)),
    (New-TaskDefinition -Name 'OSHA_Inbound_Triage' -ScheduleType 'minute' -StartTime '' -MinuteInterval 15 -TaskRun ('powershell.exe -NoProfile -ExecutionPolicy Bypass -File ' + $inboundRunner))
  )
}

function Resolve-FutureStartBoundary([hashtable]$Task, [datetime]$NowLocal) {
  if ($Task.ScheduleType -eq 'minute') {
    $candidate = $NowLocal.AddMinutes(5)
    if ($candidate.Second -ne 0 -or $candidate.Millisecond -ne 0) {
      $candidate = $candidate.AddMinutes(1)
    }
    return [datetime]::new($candidate.Year, $candidate.Month, $candidate.Day, $candidate.Hour, $candidate.Minute, 0)
  }

  $parts = ($Task.StartTime -split ':')
  if ($parts.Count -ne 2) {
    Fail 'ERR_INSTALL_SCHEDULED_TASKS_CONFIG' ('invalid_start_time task=' + $Task.Name + ' value=' + $Task.StartTime)
  }

  $hour = 0
  $minute = 0
  if (-not [int]::TryParse($parts[0], [ref]$hour)) {
    Fail 'ERR_INSTALL_SCHEDULED_TASKS_CONFIG' ('invalid_start_time_hour task=' + $Task.Name + ' value=' + $Task.StartTime)
  }
  if (-not [int]::TryParse($parts[1], [ref]$minute)) {
    Fail 'ERR_INSTALL_SCHEDULED_TASKS_CONFIG' ('invalid_start_time_minute task=' + $Task.Name + ' value=' + $Task.StartTime)
  }

  $candidate = [datetime]::new($NowLocal.Year, $NowLocal.Month, $NowLocal.Day, $hour, $minute, 0)
  if ($candidate -le $NowLocal) {
    $candidate = $candidate.AddDays(1)
  }
  return $candidate
}

function Add-ResolvedSchedule([array]$Tasks, [datetime]$NowLocal) {
  $resolved = @()
  for ($i = 0; $i -lt $Tasks.Count; $i++) {
    $task = $Tasks[$i]
    $boundary = Resolve-FutureStartBoundary -Task $task -NowLocal $NowLocal
    $entry = @{}
    foreach ($k in $task.Keys) {
      $entry[$k] = $task[$k]
    }
    $entry['StartBoundary'] = $boundary
    $entry['StartDate'] = $boundary.ToString('MM/dd/yyyy')
    $entry['StartTimeResolved'] = $boundary.ToString('HH:mm')
    $resolved += $entry
  }
  return $resolved
}

function Emit-TaskConfig([array]$Tasks, [string]$Mode) {
  Write-Output ('INSTALL_SCHEDULED_TASKS_MODE=' + $Mode)
  Write-Output ('INSTALL_SCHEDULED_TASKS_TASK_COUNT=' + $Tasks.Count)
  for ($i = 0; $i -lt $Tasks.Count; $i++) {
    $idx = $i + 1
    $task = $Tasks[$i]
    Write-Output ('TASK_' + $idx + '_NAME=' + $task.Name)
    Write-Output ('TASK_' + $idx + '_SCHEDULE=' + $task.ScheduleType)
    Write-Output ('TASK_' + $idx + '_TIME=' + $task.StartTime)
    Write-Output ('TASK_' + $idx + '_START_DATE=' + $task.StartDate)
    Write-Output ('TASK_' + $idx + '_START_TIME=' + $task.StartTimeResolved)
    Write-Output ('TASK_' + $idx + '_START_BOUNDARY_LOCAL=' + $task.StartBoundary.ToString('yyyy-MM-ddTHH:mm:ss'))
    if ([int]$task.MinuteInterval -gt 0) {
      Write-Output ('TASK_' + $idx + '_MINUTE_INTERVAL=' + $task.MinuteInterval)
    }
    Write-Output ('TASK_' + $idx + '_RL=' + $task.RunLevel)
    Write-Output ('TASK_' + $idx + '_TR=' + $task.TaskRun)
    Write-Output ('TASK_' + $idx + '_TR_LENGTH=' + $task.TaskRun.Length)
  }
}

function Build-SchtasksPreviewLine([hashtable]$Task) {
  if ($Task.ScheduleType -eq 'minute') {
    return 'schtasks /Create /F /SC MINUTE /MO ' + $Task.MinuteInterval + ' /SD ' + $Task.StartDate + ' /ST ' + $Task.StartTimeResolved + ' /TN "' + $Task.Name + '" /TR "' + $Task.TaskRun + '" /RL ' + $Task.RunLevel
  }
  return 'schtasks /Create /F /SC DAILY /SD ' + $Task.StartDate + ' /ST ' + $Task.StartTimeResolved + ' /TN "' + $Task.Name + '" /TR "' + $Task.TaskRun + '" /RL ' + $Task.RunLevel
}

function Invoke-SchtasksCommand([string[]]$Args) {
  $prevErrorAction = $ErrorActionPreference
  $ErrorActionPreference = 'Continue'
  try {
    $output = & schtasks.exe @Args 2>&1
    $code = $LASTEXITCODE
  } finally {
    $ErrorActionPreference = $prevErrorAction
  }
  return @{
    Output = @($output)
    ExitCode = [int]$code
  }
}

function Invoke-CmdCommand([string]$CommandLine) {
  $prevErrorAction = $ErrorActionPreference
  $ErrorActionPreference = 'Continue'
  try {
    $output = & cmd.exe /c $CommandLine 2>&1
    $code = $LASTEXITCODE
  } finally {
    $ErrorActionPreference = $prevErrorAction
  }
  return @{
    Output = @($output)
    ExitCode = [int]$code
  }
}

function Invoke-TaskCreate([hashtable]$Task) {
  $taskArgs = @(
    '/Create',
    '/F',
    '/SC'
  )

  if ($Task.ScheduleType -eq 'minute') {
    $taskArgs += @('MINUTE', '/MO', ([string]$Task.MinuteInterval))
  } else {
    $taskArgs += @('DAILY')
  }

  $taskArgs += @(
    '/SD',
    $Task.StartDate,
    '/ST',
    $Task.StartTimeResolved,
    '/TN',
    $Task.Name,
    '/TR',
    $Task.TaskRun,
    '/RL',
    $Task.RunLevel
  )

  $createResult = Invoke-SchtasksCommand -Args $taskArgs
  $createOutput = @($createResult.Output)
  $createCode = [int]$createResult.ExitCode
  if ($createCode -eq 0) {
    return
  }

  $createText = (($createOutput | ForEach-Object { [string]$_ }) -join ' ')
  $accessDenied = $createText -match 'Access is denied'
  if (([string]$Task.RunLevel -eq 'HIGHEST') -and $accessDenied) {
    $fallbackArgs = @()
    foreach ($arg in $taskArgs) {
      $fallbackArgs += $arg
    }
    for ($i = 0; $i -lt $fallbackArgs.Count; $i++) {
      if ($fallbackArgs[$i] -eq '/RL' -and ($i + 1) -lt $fallbackArgs.Count) {
        $fallbackArgs[$i + 1] = 'LIMITED'
        break
      }
    }
    $fallbackResult = Invoke-SchtasksCommand -Args $fallbackArgs
    $fallbackOut = @($fallbackResult.Output)
    $fallbackCode = [int]$fallbackResult.ExitCode
    if ($fallbackCode -eq 0) {
      Write-Output ('WARN_INSTALL_SCHEDULED_TASKS_RUNLEVEL_FALLBACK task=' + $Task.Name + ' run_level=LIMITED')
      return
    }
    $fallbackText = (($fallbackOut | ForEach-Object { [string]$_ }) -join ' ')
    Fail 'ERR_INSTALL_SCHEDULED_TASKS_APPLY' (
      'task=' + $Task.Name + ' exit_code=' + $fallbackCode + ' detail=' + $fallbackText
    )
  }

  Fail 'ERR_INSTALL_SCHEDULED_TASKS_APPLY' (
    'task=' + $Task.Name + ' exit_code=' + $createCode + ' detail=' + $createText
  )
}

function Set-TaskOperationalSettings([hashtable]$Task) {
  if (-not (Get-Command -Name 'Set-ScheduledTask' -ErrorAction SilentlyContinue)) {
    Fail 'ERR_INSTALL_SCHEDULED_TASKS_APPLY' ('task=' + $Task.Name + ' settings_cmd_missing=Set-ScheduledTask')
  }
  if (-not (Get-Command -Name 'New-ScheduledTaskSettingsSet' -ErrorAction SilentlyContinue)) {
    Fail 'ERR_INSTALL_SCHEDULED_TASKS_APPLY' ('task=' + $Task.Name + ' settings_cmd_missing=New-ScheduledTaskSettingsSet')
  }

  try {
    $settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries
    Set-ScheduledTask -TaskName $Task.Name -Settings $settings | Out-Null
  } catch {
    $errType = $_.Exception.GetType().Name
    $errText = ([string]$_.Exception.Message)
    if ($errText -match 'Access is denied' -or $errType -eq 'CimException') {
      Write-Output ('WARN_INSTALL_SCHEDULED_TASKS_SETTINGS_UPDATE_FAILED task=' + $Task.Name + ' err=' + $errType)
      return
    }
    Fail 'ERR_INSTALL_SCHEDULED_TASKS_APPLY' ('task=' + $Task.Name + ' settings_update_failed=' + $errType)
  }
}

function Get-TaskQueryField([hashtable]$Fields, [string]$Key) {
  if ($Fields.ContainsKey($Key)) {
    return [string]$Fields[$Key]
  }
  return 'N/A'
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
        $fields[$k] = $v
      }
    }
  }
  return $fields
}

function Convert-LastResultToHex([string]$Raw) {
  $text = ([string]$Raw).Trim()
  if (-not $text -or $text -eq 'N/A') {
    return 'N/A'
  }

  $num = 0L
  if ($text.StartsWith('0x', [System.StringComparison]::OrdinalIgnoreCase)) {
    try {
      $num = [Convert]::ToInt64($text.Substring(2), 16)
      return '0x' + ([Convert]::ToString($num, 16).ToUpperInvariant())
    } catch {
      return 'UNKNOWN'
    }
  }

  if ([long]::TryParse($text, [ref]$num)) {
    return '0x' + ([Convert]::ToString($num, 16).ToUpperInvariant())
  }
  return 'UNKNOWN'
}

function Invoke-Verify([array]$Tasks) {
  $failures = @()
  $warnings = @()
  for ($i = 0; $i -lt $Tasks.Count; $i++) {
    $task = $Tasks[$i]
    $taskNameForQuery = '\' + $task.Name
    $queryCmd = 'schtasks.exe /Query /TN ' + $taskNameForQuery + ' /V /FO LIST'
    $queryResult = Invoke-CmdCommand -CommandLine $queryCmd
    $queryOut = @($queryResult.Output)
    if ([int]$queryResult.ExitCode -ne 0) {
      Fail 'ERR_INSTALL_SCHEDULED_TASKS_VERIFY' ('query_failed task=' + $task.Name + ' exit_code=' + [int]$queryResult.ExitCode)
    }

    $fields = Parse-TaskQueryOutput -Lines @($queryOut)
    $nextRun = Get-TaskQueryField -Fields $fields -Key 'Next Run Time'
    $lastRun = Get-TaskQueryField -Fields $fields -Key 'Last Run Time'
    $lastResultRaw = Get-TaskQueryField -Fields $fields -Key 'Last Result'
    $lastResultHex = Convert-LastResultToHex -Raw $lastResultRaw
    $taskToRun = Get-TaskQueryField -Fields $fields -Key 'Task To Run'
    $taskState = Get-TaskQueryField -Fields $fields -Key 'Scheduled Task State'
    $scheduleType = Get-TaskQueryField -Fields $fields -Key 'Schedule Type'
    $startTimeRaw = Get-TaskQueryField -Fields $fields -Key 'Start Time'

    Write-Output ('TASK_NAME=' + $task.Name)
    Write-Output ('TASK_TO_RUN=' + $taskToRun)
    Write-Output ('NEXT_RUN_TIME=' + $nextRun)
    Write-Output ('LAST_RUN_TIME=' + $lastRun)
    Write-Output ('LAST_RUN_RESULT=' + $lastResultRaw)
    Write-Output ('LAST_RUN_RESULT_HEX=' + $lastResultHex)

    if (-not $nextRun -or $nextRun -eq 'N/A') {
      $failures += ('task=' + $task.Name + ' next_run_time_unavailable')
    }
    if (-not $scheduleType -or $scheduleType -eq 'N/A') {
      $failures += ('task=' + $task.Name + ' schedule_type_unavailable')
    }
    if (-not $startTimeRaw -or $startTimeRaw -eq 'N/A') {
      $failures += ('task=' + $task.Name + ' start_time_unavailable')
    }
    if ($taskState -match 'Disabled') {
      $failures += ('task=' + $task.Name + ' disabled=true')
    }
    if (($taskToRun -as [string]).Trim() -ne (($task.TaskRun -as [string]).Trim())) {
      $failures += ('task=' + $task.Name + ' action_mismatch expected=' + $task.TaskRun + ' actual=' + $taskToRun)
    }
    if ($lastResultHex -eq '0x41303') {
      $warnings += ('task=' + $task.Name + ' last_run_result_hex=0x41303')
    }
  }

  for ($i = 0; $i -lt $warnings.Count; $i++) {
    Write-Output ('WARN_SCHEDTASK_NEVER_RUN ' + $warnings[$i])
  }

  if ($failures.Count -gt 0) {
    Fail 'ERR_INSTALL_SCHEDULED_TASKS_VERIFY' ($failures -join ';')
  }

  Write-Output 'PASS_SCHEDTASK_INSTALL_OK'
}

$modes = @('--print-config', '--dry-run', '--apply', '--verify')
if ($args.Count -ne 1) {
  Fail 'ERR_INSTALL_SCHEDULED_TASKS_ARGS' ('expected one of: ' + ($modes -join ', '))
}

$modeArg = [string]$args[0]
if ($modeArg -notin $modes) {
  Fail 'ERR_INSTALL_SCHEDULED_TASKS_ARGS' ('unknown flag: ' + $modeArg)
}

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$requiredPaths = @(
  (Join-Path $repoRoot 'scripts\scheduled\run_prospect_generation.ps1'),
  (Join-Path $repoRoot 'scripts\scheduled\run_inbound_triage.ps1'),
  (Join-Path $repoRoot 'run_with_secrets.ps1'),
  (Join-Path $repoRoot 'run_prospect_discovery.py'),
  (Join-Path $repoRoot 'run_outreach_auto.py')
)
foreach ($path in $requiredPaths) {
  if (-not (Test-Path -LiteralPath $path)) {
    Fail 'ERR_INSTALL_SCHEDULED_TASKS_RUNNER_MISSING' ('missing ' + $path)
  }
}

$rawTasks = Get-TaskDefinitions -RepoRoot $repoRoot
$resolvedTasks = Add-ResolvedSchedule -Tasks $rawTasks -NowLocal (Get-Date)

if ($modeArg -eq '--print-config') {
  Emit-TaskConfig -Tasks $resolvedTasks -Mode 'print-config'
  Write-Output 'PASS_INSTALL_SCHEDULED_TASKS_PRINT_CONFIG'
  exit 0
}

if ($modeArg -eq '--dry-run') {
  Emit-TaskConfig -Tasks $resolvedTasks -Mode 'dry-run'
  for ($i = 0; $i -lt $resolvedTasks.Count; $i++) {
    $idx = $i + 1
    Write-Output ('DRY_RUN_COMMAND_' + $idx + '=' + (Build-SchtasksPreviewLine -Task $resolvedTasks[$i]))
  }
  Write-Output 'PASS_INSTALL_SCHEDULED_TASKS_DRY_RUN'
  exit 0
}

if ($modeArg -eq '--verify') {
  Invoke-Verify -Tasks $resolvedTasks
  exit 0
}

Emit-TaskConfig -Tasks $resolvedTasks -Mode 'apply'
for ($i = 0; $i -lt $resolvedTasks.Count; $i++) {
  Invoke-TaskCreate -Task $resolvedTasks[$i]
  Set-TaskOperationalSettings -Task $resolvedTasks[$i]
  Write-Output ('TASK_APPLIED=' + $resolvedTasks[$i].Name)
}
Write-Output 'PASS_INSTALL_SCHEDULED_TASKS_APPLY'
exit 0
