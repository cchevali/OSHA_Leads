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
    Fail 'ERR_ENFORCE_OSHA_TASK_LOGON_MODE_CONFIG' 'missing TASK_SCHED_USER'
  }

  $schedulerPassword = [string]$env:TASK_SCHED_PASSWORD
  $passwordPresent = -not [string]::IsNullOrWhiteSpace($schedulerPassword)
  if ($RequirePassword -and -not $passwordPresent) {
    Fail 'ERR_ENFORCE_OSHA_TASK_LOGON_MODE_CONFIG' 'missing TASK_SCHED_PASSWORD'
  }

  return @{
    User = $schedulerUser
    Password = $schedulerPassword
    PasswordPresent = $passwordPresent
  }
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

function Invoke-CmdCommand([string]$CommandLine) {
  $prevErrorAction = $ErrorActionPreference
  $ErrorActionPreference = 'Continue'
  try {
    $output = & cmd.exe /c $CommandLine 2>&1
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
        if (-not $fields.ContainsKey($k)) {
          $fields[$k] = $v
        }
      }
    }
  }
  return $fields
}

function Get-TaskQueryField([hashtable]$Fields, [string]$Key) {
  if ($Fields.ContainsKey($Key)) {
    return ([string]$Fields[$Key]).Trim()
  }
  return ''
}

function Get-OshaTaskNames {
  $query = Invoke-CmdCommand -CommandLine 'schtasks.exe /Query /FO CSV /NH'
  if ([int]$query.ExitCode -ne 0) {
    $detail = ((@($query.Output) | ForEach-Object { [string]$_ }) -join ' ')
    Fail 'ERR_ENFORCE_OSHA_TASK_LOGON_MODE_DISCOVER' ('query_failed detail=' + $detail)
  }

  $names = New-Object System.Collections.Generic.List[string]
  $joined = ((@($query.Output) | ForEach-Object { [string]$_ }) -join "`n").Trim()
  if (-not $joined) {
    return @()
  }

  $rows = @()
  try {
    $rows = @($joined | ConvertFrom-Csv -Header 'TaskName', 'NextRunTime', 'Status')
  }
  catch {
    Fail 'ERR_ENFORCE_OSHA_TASK_LOGON_MODE_DISCOVER' ('parse_failed detail=' + $_.Exception.Message)
  }

  foreach ($row in $rows) {
    $taskRef = ([string]$row.TaskName).Trim()
    if (-not $taskRef) {
      continue
    }
    while ($taskRef.StartsWith('\')) {
      $taskRef = $taskRef.Substring(1)
    }
    if (-not $taskRef.StartsWith('OSHA', [System.StringComparison]::OrdinalIgnoreCase)) {
      continue
    }
    if ($names -notcontains $taskRef) {
      [void]$names.Add($taskRef)
    }
  }

  $sorted = @($names | Sort-Object)
  return @($sorted)
}

function Get-TaskDetails([string]$TaskName) {
  $taskRef = '\' + $TaskName
  $query = Invoke-CmdCommand -CommandLine ('schtasks.exe /Query /TN "' + $taskRef + '" /V /FO LIST')
  if ([int]$query.ExitCode -ne 0) {
    $detail = ((@($query.Output) | ForEach-Object { [string]$_ }) -join ' ')
    Fail 'ERR_ENFORCE_OSHA_TASK_LOGON_MODE_VERIFY' ('query_failed task=' + $TaskName + ' detail=' + $detail)
  }

  $fields = Parse-TaskQueryOutput -Lines @($query.Output)
  $logonMode = Get-TaskQueryField -Fields $fields -Key 'Logon Mode'
  $taskToRun = Get-TaskQueryField -Fields $fields -Key 'Task To Run'
  return @{
    Name = $TaskName
    LogonMode = $logonMode
    TaskToRun = $taskToRun
    Exempt = ($TaskName -eq 'OSHA_WIP_Autosave_Logon')
  }
}

function Test-IsInteractiveOnly([string]$LogonMode) {
  return ([string]$LogonMode).Trim() -match '(?i)interactive only'
}

function Emit-Config([string]$Mode, [array]$TaskDetails, [hashtable]$SchedulerCredentials) {
  Write-Output ('ENFORCE_OSHA_TASK_LOGON_MODE_MODE=' + $Mode)
  Write-Output 'ENFORCE_OSHA_TASK_LOGON_MODE_EXEMPT_TASK=OSHA_WIP_Autosave_Logon reason=ONLOGON_trigger_requires_interactive'
  Write-Output ('ENFORCE_OSHA_TASK_LOGON_MODE_USER=' + ([string]$SchedulerCredentials.User))
  if ([bool]$SchedulerCredentials.PasswordPresent) {
    Write-Output 'ENFORCE_OSHA_TASK_LOGON_MODE_PASSWORD_PRESENT=YES'
  }
  else {
    Write-Output 'ENFORCE_OSHA_TASK_LOGON_MODE_PASSWORD_PRESENT=NO'
  }
  Write-Output ('ENFORCE_OSHA_TASK_LOGON_MODE_TASK_COUNT=' + $TaskDetails.Count)
  for ($i = 0; $i -lt $TaskDetails.Count; $i++) {
    $idx = $i + 1
    $task = $TaskDetails[$i]
    $status = 'NON_INTERACTIVE'
    if (-not ([string]$task.LogonMode).Trim()) {
      $status = 'UNKNOWN'
    }
    elseif (Test-IsInteractiveOnly -LogonMode ([string]$task.LogonMode)) {
      $status = 'INTERACTIVE_ONLY'
    }
    Write-Output ('TASK_' + $idx + '_NAME=' + $task.Name)
    Write-Output ('TASK_' + $idx + '_LOGON_MODE=' + $task.LogonMode)
    Write-Output ('TASK_' + $idx + '_STATUS=' + $status)
    if ([bool]$task.Exempt) {
      Write-Output ('TASK_' + $idx + '_EXEMPT=YES')
    }
    else {
      Write-Output ('TASK_' + $idx + '_EXEMPT=NO')
    }
  }
}

function Invoke-Verify([array]$TaskDetails) {
  $failures = @()
  for ($i = 0; $i -lt $TaskDetails.Count; $i++) {
    $task = $TaskDetails[$i]
    if ([bool]$task.Exempt) {
      continue
    }
    if (-not ([string]$task.LogonMode).Trim()) {
      $failures += ('task=' + $task.Name + ' logon_mode_unavailable')
      continue
    }
    if (Test-IsInteractiveOnly -LogonMode ([string]$task.LogonMode)) {
      $failures += ('task=' + $task.Name + ' logon_mode_interactive_only')
    }
  }

  if ($failures.Count -gt 0) {
    Fail 'ERR_ENFORCE_OSHA_TASK_LOGON_MODE_VERIFY' ($failures -join ';')
  }
  Write-Output 'PASS_ENFORCE_OSHA_TASK_LOGON_MODE_VERIFY'
}

$modes = @('--print-config', '--dry-run', '--apply', '--verify')
if ($args.Count -ne 1) {
  Fail 'ERR_ENFORCE_OSHA_TASK_LOGON_MODE_ARGS' ('expected one of: ' + ($modes -join ', '))
}
$modeArg = [string]$args[0]
if ($modeArg -notin $modes) {
  Fail 'ERR_ENFORCE_OSHA_TASK_LOGON_MODE_ARGS' ('unknown flag: ' + $modeArg)
}

$schedulerCredentials = Resolve-SchedulerCredentials -RequirePassword:($modeArg -eq '--apply')
$taskNames = @(Get-OshaTaskNames)
$taskDetails = @()
for ($i = 0; $i -lt $taskNames.Count; $i++) {
  $taskDetails += (Get-TaskDetails -TaskName ([string]$taskNames[$i]))
}

if ($modeArg -eq '--print-config') {
  Emit-Config -Mode 'print-config' -TaskDetails $taskDetails -SchedulerCredentials $schedulerCredentials
  Write-Output 'PASS_ENFORCE_OSHA_TASK_LOGON_MODE_PRINT_CONFIG'
  exit 0
}

if ($modeArg -eq '--dry-run') {
  Emit-Config -Mode 'dry-run' -TaskDetails $taskDetails -SchedulerCredentials $schedulerCredentials
  $dryIdx = 0
  for ($i = 0; $i -lt $taskDetails.Count; $i++) {
    $task = $taskDetails[$i]
    if ([bool]$task.Exempt) {
      continue
    }
    $dryIdx += 1
    $taskRef = '\' + $task.Name
    $line = 'schtasks /Change /TN "' + $taskRef + '" /RU "' + ([string]$schedulerCredentials.User) + '" /RP ***REDACTED***'
    Write-Output ('DRY_RUN_COMMAND_' + $dryIdx + '=' + $line)
  }
  Write-Output 'PASS_ENFORCE_OSHA_TASK_LOGON_MODE_DRY_RUN'
  exit 0
}

if ($modeArg -eq '--verify') {
  Emit-Config -Mode 'verify' -TaskDetails $taskDetails -SchedulerCredentials $schedulerCredentials
  Invoke-Verify -TaskDetails $taskDetails
  exit 0
}

Emit-Config -Mode 'apply' -TaskDetails $taskDetails -SchedulerCredentials $schedulerCredentials
for ($i = 0; $i -lt $taskDetails.Count; $i++) {
  $task = $taskDetails[$i]
  if ([bool]$task.Exempt) {
    continue
  }
  $taskRef = '\' + $task.Name
  $change = Invoke-SchtasksCommand -SchtasksArgs @('/Change', '/TN', $taskRef, '/RU', ([string]$schedulerCredentials.User), '/RP', ([string]$schedulerCredentials.Password))
  if ([int]$change.ExitCode -ne 0) {
    $detail = ((@($change.Output) | ForEach-Object { [string]$_ }) -join ' ')
    Fail 'ERR_ENFORCE_OSHA_TASK_LOGON_MODE_APPLY' ('task=' + $task.Name + ' detail=' + $detail)
  }
  Write-Output ('TASK_UPDATED=' + $task.Name)
}

$postDetails = @()
for ($i = 0; $i -lt $taskNames.Count; $i++) {
  $postDetails += (Get-TaskDetails -TaskName ([string]$taskNames[$i]))
}
Invoke-Verify -TaskDetails $postDetails
Write-Output 'PASS_ENFORCE_OSHA_TASK_LOGON_MODE_APPLY'
exit 0
