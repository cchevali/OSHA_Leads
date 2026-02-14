Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'

function Fail([string]$Token, [string]$Message) {
  Write-Output ($Token + " " + $Message)
  exit 1
}

function New-TaskDefinition([string]$Name, [string]$StartTime, [string]$TaskRun) {
  return @{
    Name = $Name
    StartTime = $StartTime
    TaskRun = $TaskRun
    RunLevel = 'HIGHEST'
  }
}

function Get-TaskDefinitions([string]$RepoRoot) {
  $generationRunner = Join-Path $RepoRoot "scripts\scheduled\run_prospect_generation.ps1"
  return @(
    (New-TaskDefinition -Name "OSHA_Prospect_Generation" -StartTime "07:15" -TaskRun ("powershell.exe -NoProfile -ExecutionPolicy Bypass -File " + $generationRunner)),
    (New-TaskDefinition -Name "OSHA_Prospect_Discovery" -StartTime "07:30" -TaskRun ("powershell.exe -NoProfile -ExecutionPolicy Bypass -File " + (Join-Path $RepoRoot "run_with_secrets.ps1") + " -- py -3 " + (Join-Path $RepoRoot "run_prospect_discovery.py"))),
    (New-TaskDefinition -Name "OSHA_Outreach_Auto" -StartTime "08:00" -TaskRun ("powershell.exe -NoProfile -ExecutionPolicy Bypass -File " + (Join-Path $RepoRoot "run_with_secrets.ps1") + " -- py -3 " + (Join-Path $RepoRoot "run_outreach_auto.py")))
  )
}

function Emit-TaskConfig([array]$Tasks, [string]$Mode) {
  Write-Output ("INSTALL_SCHEDULED_TASKS_MODE=" + $Mode)
  Write-Output ("INSTALL_SCHEDULED_TASKS_TASK_COUNT=" + $Tasks.Count)
  for ($i = 0; $i -lt $Tasks.Count; $i++) {
    $idx = $i + 1
    $task = $Tasks[$i]
    Write-Output ("TASK_" + $idx + "_NAME=" + $task.Name)
    Write-Output ("TASK_" + $idx + "_TIME=" + $task.StartTime)
    Write-Output ("TASK_" + $idx + "_RL=" + $task.RunLevel)
    Write-Output ("TASK_" + $idx + "_TR=" + $task.TaskRun)
    Write-Output ("TASK_" + $idx + "_TR_LENGTH=" + $task.TaskRun.Length)
  }
}

function Invoke-TaskCreate([hashtable]$Task) {
  $taskArgs = @(
    "/Create",
    "/F",
    "/SC",
    "DAILY",
    "/ST",
    $Task.StartTime,
    "/TN",
    $Task.Name,
    "/TR",
    $Task.TaskRun,
    "/RL",
    $Task.RunLevel
  )

  & schtasks.exe @taskArgs
  if ($LASTEXITCODE -ne 0) {
    Fail "ERR_INSTALL_SCHEDULED_TASKS_APPLY" ("task=" + $Task.Name + " exit_code=" + $LASTEXITCODE)
  }
}

$modes = @("--print-config", "--dry-run", "--apply")
if ($args.Count -ne 1) {
  Fail "ERR_INSTALL_SCHEDULED_TASKS_ARGS" ("expected one of: " + ($modes -join ", "))
}

$modeArg = [string]$args[0]
if ($modeArg -notin $modes) {
  Fail "ERR_INSTALL_SCHEDULED_TASKS_ARGS" ("unknown flag: " + $modeArg)
}

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$generationRunner = Join-Path $repoRoot "scripts\scheduled\run_prospect_generation.ps1"
if (-not (Test-Path -LiteralPath $generationRunner)) {
  Fail "ERR_INSTALL_SCHEDULED_TASKS_RUNNER_MISSING" ("missing " + $generationRunner)
}

$tasks = Get-TaskDefinitions -RepoRoot $repoRoot

if ($modeArg -eq "--print-config") {
  Emit-TaskConfig -Tasks $tasks -Mode "print-config"
  Write-Output "PASS_INSTALL_SCHEDULED_TASKS_PRINT_CONFIG"
  exit 0
}

if ($modeArg -eq "--dry-run") {
  Emit-TaskConfig -Tasks $tasks -Mode "dry-run"
  for ($i = 0; $i -lt $tasks.Count; $i++) {
    $idx = $i + 1
    $task = $tasks[$i]
    $line = 'schtasks /Create /F /SC DAILY /ST ' + $task.StartTime + ' /TN "' + $task.Name + '" /TR "' + $task.TaskRun + '" /RL ' + $task.RunLevel
    Write-Output ("DRY_RUN_COMMAND_" + $idx + "=" + $line)
  }
  Write-Output "PASS_INSTALL_SCHEDULED_TASKS_DRY_RUN"
  exit 0
}

Emit-TaskConfig -Tasks $tasks -Mode "apply"
for ($i = 0; $i -lt $tasks.Count; $i++) {
  Invoke-TaskCreate -Task $tasks[$i]
  Write-Output ("TASK_APPLIED=" + $tasks[$i].Name)
}
Write-Output "PASS_INSTALL_SCHEDULED_TASKS_APPLY"
exit 0
