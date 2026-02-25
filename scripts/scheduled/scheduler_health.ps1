Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'

function Normalize-Field([object]$Value) {
  if ($null -eq $Value) {
    return 'N/A'
  }
  $text = [string]$Value
  if (-not $text) {
    return 'N/A'
  }
  $text = $text -replace '\r?\n', ' '
  $text = $text.Replace('|', '%7C')
  return $text.Trim()
}

function Get-ResultHex([object]$Value) {
  if ($null -eq $Value) {
    return 'N/A'
  }
  try {
    $num = [uint32]$Value
    return ('0x{0}' -f $num.ToString('X8'))
  } catch {
    return 'N/A'
  }
}

function Get-TaskTriggerSummary([object[]]$Triggers) {
  if (-not $Triggers -or $Triggers.Count -eq 0) {
    return 'N/A'
  }

  $parts = @()
  foreach ($trigger in $Triggers) {
    if ($null -eq $trigger) {
      continue
    }
    try {
      $kind = Normalize-Field $trigger.CimClass.CimClassName
      $freq = Normalize-Field $trigger.Frequency
      $atVal = Normalize-Field $trigger.At
      $startBoundary = Normalize-Field $trigger.StartBoundary
      $daysOfWeek = Normalize-Field $trigger.DaysOfWeek
      $daysInterval = Normalize-Field $trigger.DaysInterval
      $weeksInterval = Normalize-Field $trigger.WeeksInterval
      $repetition = 'N/A'
      if ($trigger.Repetition -and $trigger.Repetition.Interval) {
        $repetition = Normalize-Field $trigger.Repetition.Interval
      }
      $parts += ('kind=' + $kind + ',freq=' + $freq + ',at=' + $atVal + ',start=' + $startBoundary + ',days=' + $daysOfWeek + ',days_interval=' + $daysInterval + ',weeks_interval=' + $weeksInterval + ',repeat=' + $repetition)
    } catch {
      $parts += ('kind=' + (Normalize-Field ($trigger | Out-String)))
    }
  }

  if ($parts.Count -eq 0) {
    return 'N/A'
  }
  return ($parts -join ';')
}

function Get-ActionFieldJoined([object[]]$Actions, [string]$PropertyName) {
  if (-not $Actions -or $Actions.Count -eq 0) {
    return 'N/A'
  }
  $values = @()
  foreach ($action in $Actions) {
    if ($null -eq $action) {
      continue
    }
    try {
      $raw = $action.$PropertyName
      $values += (Normalize-Field $raw)
    } catch {
      $values += 'N/A'
    }
  }
  if ($values.Count -eq 0) {
    return 'N/A'
  }
  return (($values | Where-Object { $_ -and $_ -ne 'N/A' }) -join ';')
}

function Get-ExpectedTaskPresence([string]$TaskName) {
  try {
    $t = Get-ScheduledTask -TaskName $TaskName -ErrorAction Stop
    if ($null -ne $t) { return 1 }
    return 0
  } catch {
    return 0
  }
}

function Get-TaskNameFromEventXml([xml]$XmlDoc) {
  foreach ($node in @($XmlDoc.Event.EventData.Data)) {
    if ($null -eq $node) { continue }
    $nameAttr = ''
    try { $nameAttr = [string]$node.Name } catch {}
    if ($nameAttr -eq 'TaskName') {
      return [string]$node.'#text'
    }
  }
  return ''
}

function Get-TaskResultFromEventXml([xml]$XmlDoc) {
  foreach ($node in @($XmlDoc.Event.EventData.Data)) {
    if ($null -eq $node) { continue }
    $nameAttr = ''
    try { $nameAttr = [string]$node.Name } catch {}
    if ($nameAttr -in @('ResultCode', 'ErrorValue', 'ResultValue')) {
      return [string]$node.'#text'
    }
  }
  return 'N/A'
}

Write-Output ('HEALTH_SNAPSHOT_LOCAL_TIME=' + (Get-Date).ToString('o'))
try {
  $tz = [System.TimeZoneInfo]::Local
  Write-Output ('HEALTH_SNAPSHOT_TIMEZONE=' + (Normalize-Field ($tz.Id + ' (' + $tz.DisplayName + ')')))
} catch {
  Write-Output 'WARN_SCHEDULER_HEALTH_TIMEZONE lookup_failed=1'
  Write-Output 'HEALTH_SNAPSHOT_TIMEZONE=N/A'
}

$tasks = @()
try {
  $tasks = @(Get-ScheduledTask -ErrorAction Stop | Where-Object {
      (($_.TaskName -like 'OSHA_*') -or ($_.TaskName -like 'OSHA *'))
    } | Sort-Object TaskName)
} catch {
  Write-Output ('WARN_SCHEDULER_HEALTH_TASK_ENUM_FAILED err=' + (Normalize-Field $_.Exception.GetType().Name))
  $tasks = @()
}

$taskNameSet = @{}
$expectedTasks = @(
  @{ Name = 'OSHA_Outreach_Auto'; Token = 'EXPECTED_TASK|TASK_NAME=OSHA_Outreach_Auto' },
  @{ Name = 'OSHA Wally Trial Daily'; Token = 'EXPECTED_TASK|TASK_NAME=OSHA Wally Trial Daily' }
)
foreach ($expected in $expectedTasks) {
  $expectedName = [string]$expected.Name
  Write-Output ([string]$expected.Token + '|PRESENT=' + (Get-ExpectedTaskPresence -TaskName $expectedName))
  $taskNameSet[$expectedName] = $true
}

foreach ($task in $tasks) {
  $taskNameSet[[string]$task.TaskName] = $true
  $info = $null
  try {
    $info = Get-ScheduledTaskInfo -TaskName $task.TaskName -TaskPath $task.TaskPath -ErrorAction Stop
  } catch {
    Write-Output ('WARN_SCHEDULER_HEALTH_TASKINFO_FAILED task=' + (Normalize-Field $task.TaskName) + ' err=' + (Normalize-Field $_.Exception.GetType().Name))
  }

  $lastRunTime = 'N/A'
  $nextRunTime = 'N/A'
  $lastRunResultDec = 'N/A'
  $lastRunResultHex = 'N/A'
  $missedRuns = 'N/A'
  if ($info) {
    $lastRunTime = Normalize-Field $info.LastRunTime
    $nextRunTime = Normalize-Field $info.NextRunTime
    $lastRunResultDec = Normalize-Field $info.LastTaskResult
    $lastRunResultHex = Get-ResultHex $info.LastTaskResult
    $missedRuns = Normalize-Field $info.NumberOfMissedRuns
  }

  $fields = @(
    ('TASK_NAME=' + (Normalize-Field $task.TaskName)),
    ('STATE=' + (Normalize-Field $task.State)),
    ('LAST_RUN_TIME=' + $lastRunTime),
    ('NEXT_RUN_TIME=' + $nextRunTime),
    ('LAST_TASK_RESULT_DEC=' + $lastRunResultDec),
    ('LAST_TASK_RESULT_HEX=' + $lastRunResultHex),
    ('MISSED_RUNS=' + $missedRuns),
    ('TRIGGER_SUMMARY=' + (Normalize-Field (Get-TaskTriggerSummary -Triggers @($task.Triggers)))),
    ('ACTION_EXECUTE=' + (Normalize-Field (Get-ActionFieldJoined -Actions @($task.Actions) -PropertyName 'Execute'))),
    ('ACTION_ARGUMENTS=' + (Normalize-Field (Get-ActionFieldJoined -Actions @($task.Actions) -PropertyName 'Arguments'))),
    ('ACTION_WORKDIR=' + (Normalize-Field (Get-ActionFieldJoined -Actions @($task.Actions) -PropertyName 'WorkingDirectory')))
  )
  $line = 'TASK_HEALTH|' + ($fields -join '|')
  Write-Output $line
}

$operationalEnabled = 0
try {
  $logConfigRaw = & wevtutil.exe gl Microsoft-Windows-TaskScheduler/Operational 2>$null
  foreach ($line in @($logConfigRaw)) {
    if (([string]$line).Trim().ToLowerInvariant() -eq 'enabled: true') {
      $operationalEnabled = 1
      break
    }
  }
} catch {
  Write-Output ('WARN_SCHEDULER_HEALTH_EVENTLOG_CONFIG_FAILED err=' + (Normalize-Field $_.Exception.GetType().Name))
}

Write-Output ('TASKSCHED_OPERATIONAL_LOG_ENABLED=' + $operationalEnabled)
if ($operationalEnabled -ne 1) {
  Write-Output 'TASK_EVENT_SUMMARY_SKIPPED|REASON=OPERATIONAL_LOG_DISABLED'
  exit 0
}

try {
  $nameLookup = @{}
  foreach ($k in $taskNameSet.Keys) {
    $nameLookup[('\'+$k).ToLowerInvariant()] = $true
    $nameLookup[$k.ToLowerInvariant()] = $true
  }

  $events = @(Get-WinEvent -LogName 'Microsoft-Windows-TaskScheduler/Operational' -MaxEvents 300 -ErrorAction Stop)
  $matched = @()
  foreach ($evt in $events) {
    try {
      $xml = [xml]$evt.ToXml()
      $evtTaskName = Get-TaskNameFromEventXml -XmlDoc $xml
      if (-not $evtTaskName) { continue }
      if (-not $nameLookup.ContainsKey($evtTaskName.ToLowerInvariant())) { continue }
      $matched += [pscustomobject]@{
        TimeCreated = $evt.TimeCreated
        Id          = $evt.Id
        TaskName    = $evtTaskName
        Result      = (Get-TaskResultFromEventXml -XmlDoc $xml)
      }
    } catch {
      continue
    }
  }

  foreach ($row in @($matched | Sort-Object TimeCreated -Descending | Select-Object -First 50)) {
    Write-Output ('TASK_EVENT|TIME=' + (Normalize-Field $row.TimeCreated) + '|ID=' + (Normalize-Field $row.Id) + '|TASK_NAME=' + (Normalize-Field $row.TaskName) + '|RESULT=' + (Normalize-Field $row.Result))
  }
} catch {
  Write-Output ('WARN_SCHEDULER_HEALTH_EVENT_QUERY_FAILED err=' + (Normalize-Field $_.Exception.GetType().Name))
}

exit 0
