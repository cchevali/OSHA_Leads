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

function Resolve-DefaultSchedulerUser {
  try {
    $identity = [Security.Principal.WindowsIdentity]::GetCurrent()
    if ($identity -and ([string]$identity.Name).Trim()) {
      return ([string]$identity.Name).Trim()
    }
  }
  catch {
  }

  $username = ([string]$env:USERNAME).Trim()
  if (-not $username) {
    return ''
  }
  $domain = ([string]$env:USERDOMAIN).Trim()
  if ($domain) {
    return ($domain + '\' + $username)
  }
  return $username
}

function Resolve-SchedulerCredentials([bool]$RequirePassword) {
  $schedulerUser = ([string]$env:TASK_SCHED_USER).Trim()
  if (-not $schedulerUser) {
    $schedulerUser = Resolve-DefaultSchedulerUser
  }
  if (-not $schedulerUser) {
    Fail 'ERR_INSTALL_SCHEDULED_TASKS_CONFIG' 'missing TASK_SCHED_USER'
  }

  $schedulerPassword = [string]$env:TASK_SCHED_PASSWORD
  $passwordPresent = -not [string]::IsNullOrWhiteSpace($schedulerPassword)
  if ($RequirePassword -and -not $passwordPresent) {
    Fail 'ERR_INSTALL_SCHEDULED_TASKS_CONFIG' 'missing TASK_SCHED_PASSWORD'
  }

  return @{
    User = $schedulerUser
    Password = $schedulerPassword
    PasswordPresent = $passwordPresent
  }
}

function New-TaskDefinition(
  [string]$Name,
  [string]$ScheduleType,
  [string]$StartTime,
  [string]$TaskRun,
  [int]$MinuteInterval = 0,
  [string]$Weekdays = '',
  [bool]$RecoveryOnly = $false
) {
  return @{
    Name           = $Name
    ScheduleType   = $ScheduleType
    StartTime      = $StartTime
    MinuteInterval = $MinuteInterval
    Weekdays       = $Weekdays
    TaskRun        = $TaskRun
    RunLevel       = 'HIGHEST'
    RecoveryOnly   = [bool]$RecoveryOnly
  }
}

function Get-TaskDefinitions([string]$RepoRoot) {
  $weekdaySpec = 'MON,TUE,WED,THU,FRI'
  $ingestRunner = Join-Path $RepoRoot 'scripts\scheduled\run_osha_ingest_daily.ps1'
  $replenishRunner = Join-Path $RepoRoot 'scripts\scheduled\run_prospect_replenish_daily.ps1'
  $inboundRunner = Join-Path $RepoRoot 'scripts\scheduled\run_inbound_triage.ps1'
  $facsTrialRunner = Join-Path $RepoRoot 'scripts\scheduled\run_trial_facs_daily.ps1'
  $outreachRunner = Join-Path $RepoRoot 'scripts\scheduled\run_outreach_auto.ps1'

  return @(
    (New-TaskDefinition -Name 'OSHA_Osha_Ingest_Daily' -ScheduleType 'weekly' -Weekdays $weekdaySpec -StartTime '06:45' -TaskRun ('powershell.exe -NoProfile -ExecutionPolicy Bypass -File ' + $ingestRunner) -RecoveryOnly:$true),
    (New-TaskDefinition -Name 'OSHA_Prospect_Replenish_SafetyNet' -ScheduleType 'weekly' -Weekdays $weekdaySpec -StartTime '07:15' -TaskRun ('powershell.exe -NoProfile -ExecutionPolicy Bypass -File ' + $replenishRunner) -RecoveryOnly:$true),
    (New-TaskDefinition -Name 'OSHA_Outreach_Auto_SafetyNet' -ScheduleType 'weekly' -Weekdays $weekdaySpec -StartTime '08:00' -TaskRun ('powershell.exe -NoProfile -ExecutionPolicy Bypass -File ' + $outreachRunner) -RecoveryOnly:$true),
    (New-TaskDefinition -Name 'OSHA_Trial_FACS_Daily' -ScheduleType 'weekly' -Weekdays $weekdaySpec -StartTime '09:00' -TaskRun ('powershell.exe -NoProfile -ExecutionPolicy Bypass -File ' + $facsTrialRunner) -RecoveryOnly:$true),
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
  if ($Task.ScheduleType -eq 'weekly') {
    $weekdayOrder = @('SUN','MON','TUE','WED','THU','FRI','SAT')
    $allowed = @{}
    foreach ($token in (([string]$Task.Weekdays) -split ',')) {
      $dayKey = ([string]$token).Trim().ToUpperInvariant()
      if ($dayKey) {
        $allowed[$dayKey] = $true
      }
    }
    if ($allowed.Count -gt 0) {
      while ($true) {
        $candidateKey = $weekdayOrder[[int]$candidate.DayOfWeek]
        if ($allowed.ContainsKey($candidateKey)) {
          break
        }
        $candidate = $candidate.AddDays(1)
      }
    }
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

function Emit-TaskConfig([array]$Tasks, [string]$Mode, [hashtable]$SchedulerCredentials) {
  $recoveryOnlyCount = @($Tasks | Where-Object { [bool]$_.RecoveryOnly }).Count
  Write-Output ('INSTALL_SCHEDULED_TASKS_MODE=' + $Mode)
  Write-Output 'INSTALL_SCHEDULED_TASKS_PRIMARY_SCHEDULER=runtime_tick_selfhosted'
  Write-Output ('INSTALL_SCHEDULED_TASKS_TASK_COUNT=' + $Tasks.Count)
  Write-Output ('INSTALL_SCHEDULED_TASKS_RECOVERY_ONLY_COUNT=' + $recoveryOnlyCount)
  Write-Output 'INSTALL_SCHEDULED_TASKS_WEEKDAYS_ONLY=0'
  Write-Output 'INSTALL_SCHEDULED_TASKS_WEEKDAY_SCHEDULE=MON,TUE,WED,THU,FRI'
  Write-Output ('INSTALL_SCHEDULED_TASKS_TASK_SCHED_USER=' + ([string]$SchedulerCredentials.User))
  if ([bool]$SchedulerCredentials.PasswordPresent) {
    Write-Output 'INSTALL_SCHEDULED_TASKS_TASK_SCHED_PASSWORD_PRESENT=YES'
  }
  else {
    Write-Output 'INSTALL_SCHEDULED_TASKS_TASK_SCHED_PASSWORD_PRESENT=NO'
  }
  for ($i = 0; $i -lt $Tasks.Count; $i++) {
    $idx = $i + 1
    $task = $Tasks[$i]
    Write-Output ('TASK_' + $idx + '_NAME=' + $task.Name)
    Write-Output ('TASK_' + $idx + '_SCHEDULE=' + $task.ScheduleType)
    Write-Output ('TASK_' + $idx + '_TIME=' + $task.StartTime)
    Write-Output ('TASK_' + $idx + '_RECOVERY_ONLY=' + $(if ([bool]$task.RecoveryOnly) { 'YES' } else { 'NO' }))
    Write-Output ('TASK_' + $idx + '_EXPECTED_STATE=' + (Get-TaskStateExpectation -Task $task))
    Write-Output ('TASK_' + $idx + '_START_DATE=' + $task.StartDate)
    Write-Output ('TASK_' + $idx + '_START_TIME=' + $task.StartTimeResolved)
    Write-Output ('TASK_' + $idx + '_START_BOUNDARY_LOCAL=' + $task.StartBoundary.ToString('yyyy-MM-ddTHH:mm:ss'))
    if ([int]$task.MinuteInterval -gt 0) {
      Write-Output ('TASK_' + $idx + '_MINUTE_INTERVAL=' + $task.MinuteInterval)
    }
    if (([string]$task.Weekdays).Trim()) {
      Write-Output ('TASK_' + $idx + '_WEEKDAYS=' + ([string]$task.Weekdays).Trim())
    }
    Write-Output ('TASK_' + $idx + '_RL=' + $task.RunLevel)
    Write-Output ('TASK_' + $idx + '_TR=' + $task.TaskRun)
    Write-Output ('TASK_' + $idx + '_TR_LENGTH=' + $task.TaskRun.Length)
  }
}

function Build-SchtasksPreviewLine([hashtable]$Task, [string]$SchedulerUser) {
  $taskNameForQuery = '\' + $Task.Name
  $commonSuffix = ' /RU "' + $SchedulerUser + '" /RP ***REDACTED***'
  if ($Task.ScheduleType -eq 'minute') {
    return 'schtasks /Create /F /SC MINUTE /MO ' + $Task.MinuteInterval + ' /SD ' + $Task.StartDate + ' /ST ' + $Task.StartTimeResolved + ' /TN "' + $taskNameForQuery + '" /TR "' + $Task.TaskRun + '" /RL ' + $Task.RunLevel + $commonSuffix
  }
  if ($Task.ScheduleType -eq 'weekly') {
    return 'schtasks /Create /F /SC WEEKLY /D ' + $Task.Weekdays + ' /SD ' + $Task.StartDate + ' /ST ' + $Task.StartTimeResolved + ' /TN "' + $taskNameForQuery + '" /TR "' + $Task.TaskRun + '" /RL ' + $Task.RunLevel + $commonSuffix
  }
  return 'schtasks /Create /F /SC DAILY /SD ' + $Task.StartDate + ' /ST ' + $Task.StartTimeResolved + ' /TN "' + $taskNameForQuery + '" /TR "' + $Task.TaskRun + '" /RL ' + $Task.RunLevel + $commonSuffix
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
    Output   = @($output)
    ExitCode = [int]$code
  }
}

function Get-TaskToRunFromSchtasks([string]$TaskName) {
  $taskNameForQuery = '\' + $TaskName
  $queryResult = Invoke-SchtasksCommand -SchtasksArgs @('/Query', '/TN', $taskNameForQuery, '/V', '/FO', 'LIST')
  $queryOut = @($queryResult.Output)
  if ([int]$queryResult.ExitCode -ne 0) {
    # If the task does not exist, schtasks returns error code 1 and a specific message.
    # In this case, we return $null to indicate the task is missing, which triggers creation.
    $outputString = ($queryOut -join ' ')
    Write-Verbose ("Check-Task-Failure task=" + $TaskName + " output=" + $outputString)
    if ($outputString -like '*system cannot find the file specified*') {
      return $null
    }
    if ($outputString -match '(?i)access is denied') {
      return '__ACCESS_DENIED__'
    }
    Fail 'ERR_INSTALL_SCHEDULED_TASKS_APPLY' ('task=' + $TaskName + ' query_failed exit_code=' + [int]$queryResult.ExitCode + ' output=' + $outputString)
  }
  $fields = Parse-TaskQueryOutput -Lines @($queryOut)
  return Get-TaskQueryField -Fields $fields -Key 'Task To Run'
}

function Delete-TaskIfExists([string]$TaskName) {
  $taskNameForQuery = '\' + $TaskName
  $deleteArgs = @('/Delete', '/TN', $taskNameForQuery, '/F')
  $deleteResult = Invoke-SchtasksCommand -SchtasksArgs $deleteArgs
  if ([int]$deleteResult.ExitCode -ne 0) {
    $text = ((@($deleteResult.Output) | ForEach-Object { [string]$_ }) -join ' ').Trim()
    if ($text -match 'The system cannot find the file specified') {
      return @{
        Removed = $false
        Missing = $true
        AccessDenied = $false
        Detail = $text
      }
    }
    if ($text -match '(?i)Access is denied') {
      return @{
        Removed = $false
        Missing = $false
        AccessDenied = $true
        Detail = $text
      }
    }
    return @{
      Removed = $false
      Missing = $false
      AccessDenied = $false
      Detail = ('exit_code=' + [int]$deleteResult.ExitCode + ' detail=' + $text)
    }
  }
  return @{
    Removed = $true
    Missing = $false
    AccessDenied = $false
    Detail = ''
  }
}

function Invoke-TaskCreate([hashtable]$Task, [string]$SchedulerUser, [string]$SchedulerPassword) {
  $taskNameForQuery = '\' + $Task.Name
  $taskArgs = @(
    '/Create',
    '/F',
    '/SC'
  )

  if ($Task.ScheduleType -eq 'minute') {
    $taskArgs += @('MINUTE', '/MO', ([string]$Task.MinuteInterval))
  }
  elseif ($Task.ScheduleType -eq 'weekly') {
    $taskArgs += @('WEEKLY', '/D', ([string]$Task.Weekdays))
  }
  else {
    $taskArgs += @('DAILY')
  }

  $taskArgs += @(
    '/SD',
    $Task.StartDate,
    '/ST',
    $Task.StartTimeResolved,
    '/TN',
    $taskNameForQuery,
    '/TR',
    $Task.TaskRun,
    '/RL',
    $Task.RunLevel,
    '/RU',
    $SchedulerUser,
    '/RP',
    $SchedulerPassword
  )

  $createResult = Invoke-SchtasksCommand -SchtasksArgs $taskArgs
  $createOutput = @($createResult.Output)
  $createCode = [int]$createResult.ExitCode
  if ($createCode -eq 0) {
    return @{
      Applied = $true
      AccessDenied = $false
      Detail = ''
    }
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
    $fallbackResult = Invoke-SchtasksCommand -SchtasksArgs $fallbackArgs
    $fallbackOut = @($fallbackResult.Output)
    $fallbackCode = [int]$fallbackResult.ExitCode
    if ($fallbackCode -eq 0) {
      return @{
        Applied = $true
        AccessDenied = $false
        Detail = 'runlevel_fallback_limited'
      }
    }
    $fallbackText = (($fallbackOut | ForEach-Object { [string]$_ }) -join ' ')
    if ($fallbackText -match 'Access is denied') {
      return @{
        Applied = $false
        AccessDenied = $true
        Detail = $fallbackText
      }
    }
    Fail 'ERR_INSTALL_SCHEDULED_TASKS_APPLY' (
      'task=' + $Task.Name + ' exit_code=' + $fallbackCode + ' detail=' + $fallbackText
    )
  }

  if ($accessDenied) {
    return @{
      Applied = $false
      AccessDenied = $true
      Detail = $createText
    }
  }

  Fail 'ERR_INSTALL_SCHEDULED_TASKS_APPLY' (
    'task=' + $Task.Name + ' exit_code=' + $createCode + ' detail=' + $createText
  )
}

function Set-TaskEnabledState([hashtable]$Task) {
  $taskNameForQuery = '\' + $Task.Name
  $expectedState = Get-TaskStateExpectation -Task $Task
  $changeMode = if ($expectedState -eq 'Disabled') { '/Disable' } else { '/Enable' }
  $result = Invoke-SchtasksCommand -SchtasksArgs @('/Change', '/TN', $taskNameForQuery, $changeMode)
  if ([int]$result.ExitCode -eq 0) {
    Write-Output ('TASK_EXPECTED_STATE_APPLIED=' + $Task.Name + ' state=' + $expectedState)
    return
  }

  $detail = ((@($result.Output) | ForEach-Object { [string]$_ }) -join ' ').Trim()
  if ($detail -match 'Access is denied') {
    Write-Output ('WARN_INSTALL_SCHEDULED_TASKS_STATE_ACCESS_DENIED task=' + $Task.Name + ' state=' + $expectedState)
    return
  }

  Fail 'ERR_INSTALL_SCHEDULED_TASKS_APPLY' (
    'task=' + $Task.Name + ' state_change_failed=' + $changeMode + ' detail=' + $detail
  )
}

function Get-TaskStateExpectation([hashtable]$Task) {
  return 'Enabled'
}

function Convert-StartTimeTo24Hour([string]$Raw) {
  $text = ([string]$Raw).Trim()
  if (-not $text -or $text -eq 'N/A') {
    return ''
  }

  $m = [regex]::Match($text, '^(?<h>\d{1,2}):(?<m>\d{2})(?::\d{2})?\s*(?<ampm>[AaPp][Mm])?$')
  if (-not $m.Success) {
    return ''
  }

  $hour = [int]$m.Groups['h'].Value
  $minute = [int]$m.Groups['m'].Value
  $ampm = ($m.Groups['ampm'].Value -as [string]).ToUpperInvariant()

  if ($ampm -eq 'AM') {
    if ($hour -eq 12) { $hour = 0 }
  } elseif ($ampm -eq 'PM') {
    if ($hour -lt 12) { $hour += 12 }
  }

  return ('{0:D2}:{1:D2}' -f $hour, $minute)
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
  }
  catch {
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

function Normalize-TaskQueryName([string]$TaskName) {
  $name = ([string]$TaskName).Trim()
  if (-not $name) {
    return ''
  }
  if ($name.StartsWith('\')) {
    return $name
  }
  return '\' + $name
}

function Normalize-TaskDisplayName([string]$TaskName) {
  $name = ([string]$TaskName).Trim()
  if ($name.StartsWith('\')) {
    return $name.Substring(1)
  }
  return $name
}

function Resolve-TaskRunTargetPath([string]$TaskRun) {
  $raw = ([string]$TaskRun).Trim()
  if (-not $raw -or $raw -eq 'N/A') {
    return ''
  }

  $m = [regex]::Match($raw, '(?i)-File\s+(?:"(?<path>[^"]+)"|(?<path>\S+))')
  if ($m.Success) {
    $candidate = ([string]$m.Groups['path'].Value).Trim()
    if ($candidate) {
      return $candidate
    }
  }

  $mPs1 = [regex]::Match(
    $raw,
    '(?<path>[A-Za-z]:\\[^"''\s]+\.ps1)',
    [System.Text.RegularExpressions.RegexOptions]::IgnoreCase
  )
  if ($mPs1.Success) {
    return ([string]$mPs1.Groups['path'].Value).Trim()
  }
  $mPy = [regex]::Match(
    $raw,
    '(?<path>[A-Za-z]:\\[^"''\s]+\.py)',
    [System.Text.RegularExpressions.RegexOptions]::IgnoreCase
  )
  if ($mPy.Success) {
    return ([string]$mPy.Groups['path'].Value).Trim()
  }
  return ''
}

function Get-RegisteredOshaTaskNames() {
  $queryResult = Invoke-SchtasksCommand -SchtasksArgs @('/Query', '/FO', 'LIST')
  if ([int]$queryResult.ExitCode -ne 0) {
    Fail 'ERR_INSTALL_SCHEDULED_TASKS_VERIFY' ('query_all_failed exit_code=' + [int]$queryResult.ExitCode)
  }

  $names = New-Object System.Collections.Generic.List[string]
  foreach ($line in @($queryResult.Output)) {
    $text = [string]$line
    if (-not ($text -match '^\s*TaskName:\s*(.+?)\s*$')) {
      continue
    }
    $taskName = ([string]$matches[1]).Trim()
    if (-not $taskName) {
      continue
    }
    if ($taskName -notmatch '(?i)\\?OSHA(?:_| |$)') {
      continue
    }
    if ($taskName -match '(?i)\\?OSHA_WIP_Autosave_') {
      continue
    }
    [void]$names.Add($taskName)
  }
  return ,$names.ToArray()
}

function Get-KnownLegacyTaskNames() {
  return @(
    'OSHA_Prospect_Generation',
    'OSHA_Prospect_Discovery',
    'OSHA_Prospect_Replenish_Daily',
    'OSHA_Outreach_Auto',
    'OSHA Wally Trial Daily',
    'OSHA_Daily_Pipeline'
  )
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
    }
    catch {
      return 'UNKNOWN'
    }
  }

  if ([long]::TryParse($text, [ref]$num)) {
    return '0x' + ([Convert]::ToString($num, 16).ToUpperInvariant())
  }
  return 'UNKNOWN'
}

function Invoke-StaticHealthChecks {
  param(
    [string]$RepoRoot,
    [ref]$FailuresRef
  )

  $runnerServices = @(Get-Service -Name 'actions.runner*' -ErrorAction SilentlyContinue)
  if ($runnerServices.Count -lt 1) {
    Write-Output 'ERR_INSTALL_SCHEDULED_TASKS_RUNNER_SERVICE=0 status=missing'
    $FailuresRef.Value += 'runner_service_missing'
  } else {
    $runningServices = @($runnerServices | Where-Object { $_.Status -eq 'Running' })
    if ($runningServices.Count -lt 1) {
      Write-Output ('ERR_INSTALL_SCHEDULED_TASKS_RUNNER_SERVICE=0 status=' + (($runnerServices | Select-Object -First 1).Status))
      $FailuresRef.Value += 'runner_service_not_running'
    } else {
      $runner = $runningServices | Select-Object -First 1
      Write-Output ('PASS_INSTALL_SCHEDULED_TASKS_RUNNER_SERVICE name=' + $runner.Name + ' status=' + $runner.Status)
    }
  }

  $runtimeGuardPath = Join-Path $RepoRoot 'scripts\scheduled\runtime_guard.ps1'
  if (-not (Test-Path -LiteralPath $runtimeGuardPath)) {
    Write-Output ('ERR_INSTALL_SCHEDULED_TASKS_PYTHON_RESOLUTION=0 path_missing=' + $runtimeGuardPath)
    $FailuresRef.Value += 'scheduler_python_helper_missing'
  } else {
    . $runtimeGuardPath
    try {
      $python = Resolve-PythonCommand
      $pythonExe = [string]$python.Exe
      $pythonArgs = @($python.ArgsPrefix | ForEach-Object { [string]$_ }) -join ' '
      Write-Output ('PASS_INSTALL_SCHEDULED_TASKS_PYTHON_RESOLUTION exe=' + $pythonExe + ' args_prefix=' + $pythonArgs)
    }
    catch {
      $detail = ([string]$_.Exception.Message).Trim()
      Write-Output ('ERR_INSTALL_SCHEDULED_TASKS_PYTHON_RESOLUTION=0 detail=' + $detail)
      $FailuresRef.Value += 'scheduler_python_missing'
    }
  }
}

function Invoke-Verify([array]$Tasks, [string]$RepoRoot) {
  $failures = @()
  Invoke-StaticHealthChecks -RepoRoot $RepoRoot -FailuresRef ([ref]$failures)
  $warnings = @()
  $registeredOshaTasks = Get-RegisteredOshaTaskNames
  $registeredLookup = @{}
  foreach ($rawTaskName in @($registeredOshaTasks)) {
    $normalized = Normalize-TaskQueryName -TaskName ([string]$rawTaskName)
    if ($normalized) {
      $registeredLookup[$normalized.ToLowerInvariant()] = $true
    }
  }
  foreach ($legacyTaskName in @(Get-KnownLegacyTaskNames)) {
    $legacyTaskState = Get-TaskToRunFromSchtasks -TaskName $legacyTaskName
    if (([string]$legacyTaskState).Trim() -eq '__ACCESS_DENIED__') {
      Write-Output ('ERR_SCHEDTASK_LEGACY_PRESENT=1 task=' + $legacyTaskName)
      $failures += ('task=' + $legacyTaskName + ' legacy_task_present=access_denied')
      continue
    }
    if (($legacyTaskState -as [string]).Trim()) {
      Write-Output ('ERR_SCHEDTASK_LEGACY_PRESENT=1 task=' + $legacyTaskName)
      $failures += ('task=' + $legacyTaskName + ' legacy_task_present=true')
      continue
    }
    $normalizedLegacy = Normalize-TaskQueryName -TaskName $legacyTaskName
    if ($normalizedLegacy -and $registeredLookup.ContainsKey($normalizedLegacy.ToLowerInvariant())) {
      Write-Output ('ERR_SCHEDTASK_LEGACY_PRESENT=1 task=' + $legacyTaskName)
      $failures += ('task=' + $legacyTaskName + ' legacy_task_present=enumerated')
    }
  }
  for ($i = 0; $i -lt $Tasks.Count; $i++) {
    $task = $Tasks[$i]
    $taskNameForQuery = '\' + $task.Name
    $queryResult = Invoke-SchtasksCommand -SchtasksArgs @('/Query', '/TN', $taskNameForQuery, '/V', '/FO', 'LIST')
    $queryOut = @($queryResult.Output)
    if ([int]$queryResult.ExitCode -ne 0) {
      $detail = ((@($queryOut) | ForEach-Object { [string]$_ }) -join ' ').Trim()
      if ($detail -match '(?i)cannot find the file specified') {
        Write-Output ('ERR_SCHEDTASK_MISSING=1 task=' + $task.Name)
        $failures += ('task=' + $task.Name + ' missing=true')
        continue
      }
      if ($detail -match '(?i)access is denied') {
        Write-Output ('ERR_SCHEDTASK_QUERY_ACCESS_DENIED=1 task=' + $task.Name)
        $failures += ('task=' + $task.Name + ' query_access_denied=true')
        continue
      }
      $failures += ('task=' + $task.Name + ' query_failed_exit_code=' + [int]$queryResult.ExitCode)
      continue
    }

    $fields = Parse-TaskQueryOutput -Lines @($queryOut)
    $nextRun = Get-TaskQueryField -Fields $fields -Key 'Next Run Time'
    $lastRun = Get-TaskQueryField -Fields $fields -Key 'Last Run Time'
    $lastResultRaw = Get-TaskQueryField -Fields $fields -Key 'Last Result'
    $lastResultHex = Convert-LastResultToHex -Raw $lastResultRaw
    $taskToRun = Get-TaskQueryField -Fields $fields -Key 'Task To Run'
    $taskState = Get-TaskQueryField -Fields $fields -Key 'Scheduled Task State'
    $logonMode = Get-TaskQueryField -Fields $fields -Key 'Logon Mode'
    $scheduleType = Get-TaskQueryField -Fields $fields -Key 'Schedule Type'
    $startTimeRaw = Get-TaskQueryField -Fields $fields -Key 'Start Time'

    Write-Output ('TASK_NAME=' + $task.Name)
    Write-Output ('TASK_TO_RUN=' + $taskToRun)
    Write-Output ('NEXT_RUN_TIME=' + $nextRun)
    Write-Output ('LAST_RUN_TIME=' + $lastRun)
    Write-Output ('LAST_RUN_RESULT=' + $lastResultRaw)
    Write-Output ('LAST_RUN_RESULT_HEX=' + $lastResultHex)
    Write-Output ('LOGON_MODE=' + $logonMode)

    $expectedState = Get-TaskStateExpectation -Task $task
    Write-Output ('TASK_EXPECTED_STATE=' + $expectedState)
    Write-Output ('TASK_RECOVERY_ONLY=' + $(if ([bool]$task.RecoveryOnly) { 'YES' } else { 'NO' }))

    if ((-not [bool]$task.RecoveryOnly) -and (-not $nextRun -or $nextRun -eq 'N/A')) {
      $failures += ('task=' + $task.Name + ' next_run_time_unavailable')
    }
    if (-not $scheduleType -or $scheduleType -eq 'N/A') {
      $failures += ('task=' + $task.Name + ' schedule_type_unavailable')
    }
    if (-not $startTimeRaw -or $startTimeRaw -eq 'N/A') {
      $failures += ('task=' + $task.Name + ' start_time_unavailable')
    }
    if ($task.ScheduleType -eq 'daily' -or $task.ScheduleType -eq 'weekly') {
      $actualStart = Convert-StartTimeTo24Hour -Raw $startTimeRaw
      $expectedStart = ([string]$task.StartTimeResolved).Trim()
      if (-not $actualStart) {
        $failures += ('task=' + $task.Name + ' start_time_unparseable value=' + $startTimeRaw)
      } elseif ($actualStart -ne $expectedStart) {
        $failures += ('task=' + $task.Name + ' start_time_mismatch expected=' + $expectedStart + ' actual=' + $actualStart)
      }
    }
    $isDisabled = ($taskState -match 'Disabled')
    if ($isDisabled) {
      if ([bool]$task.RecoveryOnly) {
        Write-Output ('ERR_SCHEDTASK_SAFETY_NET_DISABLED=1 task=' + $task.Name)
      }
      $failures += ('task=' + $task.Name + ' disabled=true')
    }
    if (-not $logonMode -or $logonMode -eq 'N/A') {
      $failures += ('task=' + $task.Name + ' logon_mode_unavailable')
    }
    elseif ($logonMode -match '(?i)interactive only') {
      $failures += ('task=' + $task.Name + ' logon_mode_interactive_only')
    }
    if (($taskToRun -as [string]).Trim() -ne (($task.TaskRun -as [string]).Trim())) {
      $failures += ('task=' + $task.Name + ' action_mismatch expected=' + $task.TaskRun + ' actual=' + $taskToRun)
    }
    if ($lastResultHex -eq '0x41303') {
      $warnings += ('task=' + $task.Name + ' last_run_result_hex=0x41303')
    }
  }

  for ($j = 0; $j -lt $registeredOshaTasks.Count; $j++) {
    $rawTaskName = [string]$registeredOshaTasks[$j]
    $taskNameForQuery = Normalize-TaskQueryName -TaskName $rawTaskName
    if (-not $taskNameForQuery) {
      continue
    }
    $queryResult = Invoke-SchtasksCommand -SchtasksArgs @('/Query', '/TN', $taskNameForQuery, '/V', '/FO', 'LIST')
    if ([int]$queryResult.ExitCode -ne 0) {
      continue
    }
    $fields = Parse-TaskQueryOutput -Lines @($queryResult.Output)
    $taskToRun = Get-TaskQueryField -Fields $fields -Key 'Task To Run'
    $targetPath = Resolve-TaskRunTargetPath -TaskRun $taskToRun
    if (-not $targetPath) {
      continue
    }
    if (-not (Test-Path -LiteralPath $targetPath)) {
      $displayName = Normalize-TaskDisplayName -TaskName $rawTaskName
      Write-Output ('ERR_SCHED_TASK_TARGET_MISSING=1 task=' + $displayName + ' target=' + $targetPath)
      $failures += ('task=' + $displayName + ' target_missing=' + $targetPath)
    }
  }

  $managedLookup = @{}
  foreach ($task in @($Tasks)) {
    $managedLookup[('\'+$task.Name).ToLowerInvariant()] = $true
  }
  foreach ($rawTaskName in @($registeredOshaTasks)) {
    $normalized = Normalize-TaskQueryName -TaskName ([string]$rawTaskName)
    if (-not $normalized) {
      continue
    }
    if ($managedLookup.ContainsKey($normalized.ToLowerInvariant())) {
      continue
    }
    $displayName = Normalize-TaskDisplayName -TaskName $normalized
    Write-Output ('ERR_SCHEDTASK_UNMANAGED_OSHA_TASK=1 task=' + $displayName)
    $failures += ('task=' + $displayName + ' unmanaged_osha_task=true')
  }

  for ($i = 0; $i -lt $warnings.Count; $i++) {
    Write-Output ('WARN_SCHEDTASK_NEVER_RUN ' + $warnings[$i])
  }

  if ($failures.Count -gt 0) {
    Fail 'ERR_INSTALL_SCHEDULED_TASKS_VERIFY' ($failures -join ';')
  }

  Write-Output 'PASS_SCHEDTASK_INSTALL_OK'
}

$modes = @('--print-config', '--dry-run', '--apply', '--verify', '--status')
if ($args.Count -ne 1) {
  Fail 'ERR_INSTALL_SCHEDULED_TASKS_ARGS' ('expected one of: ' + ($modes -join ', '))
}

$modeArg = [string]$args[0]
if ($modeArg -notin $modes) {
  Fail 'ERR_INSTALL_SCHEDULED_TASKS_ARGS' ('unknown flag: ' + $modeArg)
}

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$requiredPaths = @(
  (Join-Path $repoRoot 'scripts\scheduled\run_osha_ingest_daily.ps1'),
  (Join-Path $repoRoot 'scripts\scheduled\run_osha_ingest_evening.ps1'),
  (Join-Path $repoRoot 'scripts\scheduled\run_prospect_replenish_daily.ps1'),
  (Join-Path $repoRoot 'scripts\scheduled\run_trial_facs_daily.ps1'),
  (Join-Path $repoRoot 'scripts\scheduled\run_inbound_triage.ps1'),
  (Join-Path $repoRoot 'scripts\scheduled\run_outreach_auto.ps1'),
  (Join-Path $repoRoot 'run_with_secrets.ps1'),
  (Join-Path $repoRoot 'run_prospect_replenish_daily.py')
)
foreach ($path in $requiredPaths) {
  if (-not (Test-Path -LiteralPath $path)) {
    Fail 'ERR_INSTALL_SCHEDULED_TASKS_RUNNER_MISSING' ('missing ' + $path)
  }
}

$rawTasks = Get-TaskDefinitions -RepoRoot $repoRoot
$resolvedTasks = Add-ResolvedSchedule -Tasks $rawTasks -NowLocal (Get-Date)
$schedulerCredentials = Resolve-SchedulerCredentials -RequirePassword:($modeArg -eq '--apply')

if ($modeArg -eq '--print-config') {
  Emit-TaskConfig -Tasks $resolvedTasks -Mode 'print-config' -SchedulerCredentials $schedulerCredentials
  Write-Output 'PASS_INSTALL_SCHEDULED_TASKS_PRINT_CONFIG'
  exit 0
}

if ($modeArg -eq '--dry-run') {
  Emit-TaskConfig -Tasks $resolvedTasks -Mode 'dry-run' -SchedulerCredentials $schedulerCredentials
  for ($i = 0; $i -lt $resolvedTasks.Count; $i++) {
    $idx = $i + 1
    Write-Output ('DRY_RUN_COMMAND_' + $idx + '=' + (Build-SchtasksPreviewLine -Task $resolvedTasks[$i] -SchedulerUser ([string]$schedulerCredentials.User)))
    Write-Output ('DRY_RUN_STATE_COMMAND_' + $idx + '=schtasks /Change /TN "\' + $resolvedTasks[$i].Name + '" /' + $(if ((Get-TaskStateExpectation -Task $resolvedTasks[$i]) -eq 'Disabled') { 'Disable' } else { 'Enable' }))
  }
  Write-Output 'PASS_INSTALL_SCHEDULED_TASKS_DRY_RUN'
  exit 0
}

if ($modeArg -eq '--verify' -or $modeArg -eq '--status') {
  Invoke-Verify -Tasks $resolvedTasks -RepoRoot $repoRoot
  exit 0
}

Emit-TaskConfig -Tasks $resolvedTasks -Mode 'apply' -SchedulerCredentials $schedulerCredentials
$legacyCleanupFailures = New-Object System.Collections.Generic.List[string]
foreach ($legacyTaskName in @(Get-KnownLegacyTaskNames)) {
  $deleteState = Delete-TaskIfExists -TaskName $legacyTaskName
  if ([bool]$deleteState.Removed) {
    Write-Output ('TASK_REMOVED_LEGACY=' + $legacyTaskName)
    continue
  }
  if ([bool]$deleteState.Missing) {
    Write-Output ('TASK_LEGACY_ABSENT=' + $legacyTaskName)
    continue
  }
  if ([bool]$deleteState.AccessDenied) {
    Write-Output ('ERR_SCHEDTASK_LEGACY_ACCESS_DENIED=1 task=' + $legacyTaskName)
    $legacyCleanupFailures.Add('task=' + $legacyTaskName + ' delete_failed=access_denied') | Out-Null
    continue
  }
  Write-Output ('ERR_SCHEDTASK_LEGACY_DELETE_FAILED=1 task=' + $legacyTaskName)
  $legacyCleanupFailures.Add('task=' + $legacyTaskName + ' delete_failed=' + ([string]$deleteState.Detail).Trim()) | Out-Null
}
if ($legacyCleanupFailures.Count -gt 0) {
  Fail 'ERR_INSTALL_SCHEDULED_TASKS_APPLY' ('legacy_cleanup_failed ' + ($legacyCleanupFailures -join ';'))
}
$applyAccessDeniedCount = 0
for ($i = 0; $i -lt $resolvedTasks.Count; $i++) {
  $task = $resolvedTasks[$i]
  $actual = Get-TaskToRunFromSchtasks -TaskName $task.Name
  if (([string]$actual).Trim() -eq '__ACCESS_DENIED__') {
    Write-Output ('WARN_SCHEDTASK_QUERY_ACCESS_DENIED task=' + $task.Name + ' action_check=skipped_repair')
  }
  elseif (($actual -as [string]).Trim() -ne (($task.TaskRun -as [string]).Trim())) {
    Write-Output ('WARN_SCHEDTASK_ACTION_MISMATCH task=' + $task.Name + ' will_recreate=YES mode=create_force')
  }

  $createState = Invoke-TaskCreate -Task $task -SchedulerUser ([string]$schedulerCredentials.User) -SchedulerPassword ([string]$schedulerCredentials.Password)
  if (([string]$createState.Detail).Trim() -eq 'runlevel_fallback_limited') {
    Write-Output ('WARN_INSTALL_SCHEDULED_TASKS_RUNLEVEL_FALLBACK task=' + $task.Name + ' run_level=LIMITED')
  }
  if ([bool]$createState.AccessDenied) {
    $applyAccessDeniedCount += 1
    Write-Output ('WARN_INSTALL_SCHEDULED_TASKS_APPLY_ACCESS_DENIED task=' + $task.Name + ' detail=' + (([string]$createState.Detail).Trim()))
    Write-Output 'WARN_INSTALL_SCHEDULED_TASKS_REMEDIATION Re-run in elevated PowerShell to repair task permissions.'
    continue
  }

  Set-TaskOperationalSettings -Task $task
  Set-TaskEnabledState -Task $task

  $post = Get-TaskToRunFromSchtasks -TaskName $task.Name
  if (($post -as [string]).Trim() -ne (($task.TaskRun -as [string]).Trim())) {
    Fail 'ERR_INSTALL_SCHEDULED_TASKS_APPLY_ACTION_STUCK' ('task=' + $task.Name + ' actual=' + $post + ' expected=' + $task.TaskRun)
  }
  Write-Output ('TASK_APPLIED=' + $task.Name)
}
if ($applyAccessDeniedCount -gt 0) {
  Write-Output ('WARN_INSTALL_SCHEDULED_TASKS_APPLY_ACCESS_DENIED_COUNT=' + $applyAccessDeniedCount)
}
Invoke-Verify -Tasks $resolvedTasks -RepoRoot $repoRoot
Write-Output 'PASS_INSTALL_SCHEDULED_TASKS_APPLY'
exit 0
