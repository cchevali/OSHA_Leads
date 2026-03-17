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

function Resolve-Target([string]$Value) {
  $trimmed = ([string]$Value).Trim().ToLowerInvariant()
  if ($trimmed -notin @('laptop', 'pc', 'both')) {
    Fail 'ERR_TRAVEL_PREFLIGHT_ARGS' ('invalid --target value: ' + $Value)
  }
  return $trimmed
}

function New-Step(
  [string]$Id,
  [string]$Target,
  [string]$CommandText,
  [string]$Executable,
  [string[]]$ArgumentList
) {
  return @{
    Id = $Id
    Target = $Target
    CommandText = $CommandText
    Executable = $Executable
    ArgumentList = @($ArgumentList)
  }
}

function Get-StepDefinitions([string]$RepoRoot, [bool]$SkipTests) {
  $wrapperPath = Join-Path $RepoRoot 'run_with_secrets.ps1'
  $steps = @(
    (New-Step -Id 'context_pack_check' -Target 'laptop' -CommandText 'py -3 tools\project_context_pack.py --check' -Executable 'py' -ArgumentList @('-3', 'tools\project_context_pack.py', '--check')),
    (New-Step -Id 'wrapper_diagnostics' -Target 'laptop' -CommandText '.\run_with_secrets.ps1 --diagnostics --check-decrypt' -Executable 'powershell' -ArgumentList @('-NoProfile', '-ExecutionPolicy', 'Bypass', '-File', $wrapperPath, '--diagnostics', '--check-decrypt')),
    (New-Step -Id 'runtime_tick_print_config' -Target 'both' -CommandText '.\run_with_secrets.ps1 -- py -3 run_runtime_tick.py --print-config' -Executable 'powershell' -ArgumentList @('-NoProfile', '-ExecutionPolicy', 'Bypass', '-File', $wrapperPath, '--', 'py', '-3', 'run_runtime_tick.py', '--print-config')),
    (New-Step -Id 'runtime_tick_doctor' -Target 'both' -CommandText '.\run_with_secrets.ps1 -- py -3 run_runtime_tick.py --doctor' -Executable 'powershell' -ArgumentList @('-NoProfile', '-ExecutionPolicy', 'Bypass', '-File', $wrapperPath, '--', 'py', '-3', 'run_runtime_tick.py', '--doctor')),
    (New-Step -Id 'runtime_tick_dry_run' -Target 'both' -CommandText '.\run_with_secrets.ps1 -- py -3 run_runtime_tick.py --dry-run' -Executable 'powershell' -ArgumentList @('-NoProfile', '-ExecutionPolicy', 'Bypass', '-File', $wrapperPath, '--', 'py', '-3', 'run_runtime_tick.py', '--dry-run')),
    (New-Step -Id 'workflow_run_list' -Target 'both' -CommandText 'gh run list --workflow "Runtime Tick (Self-Hosted)" --limit 5' -Executable 'gh' -ArgumentList @('run', 'list', '--workflow', 'Runtime Tick (Self-Hosted)', '--limit', '5'))
  )

  if (-not $SkipTests) {
    $steps += (New-Step -Id 'unit_tests' -Target 'laptop' -CommandText 'py -3 -m unittest -q' -Executable 'py' -ArgumentList @('-3', '-m', 'unittest', '-q'))
  }

  return $steps
}

function Get-ManualChecks([string]$Target) {
  $checks = @(
    'Use Windows-native RDP to the canonical PC as the primary travel path for live operations.',
    'Do not expose raw RDP to the public internet; use an existing secure access layer.',
    'Treat Google Remote Desktop as fallback only if you already rely on it and RDP is unavailable.'
  )

  if ($Target -eq 'laptop' -or $Target -eq 'both') {
    $checks += 'Keep the laptop clone current and use it only for development, tests, doctor, dry-run, and artifact review.'
  }

  if ($Target -eq 'pc' -or $Target -eq 'both') {
    $checks += 'From the laptop, start an RDP session to the canonical PC, disconnect, reconnect, and confirm usable resolution/performance.'
    $checks += 'Confirm the canonical PC stays awake, network-connected, and reachable after disconnect/reconnect.'
    $checks += 'From the remote PC session, inspect runtime status artifacts and confirm you can rerun the GitHub workflow if real recovery is needed.'
  }

  return $checks
}

function Emit-Config([string]$Mode, [string]$Target, [bool]$SkipTests, [string]$RepoRoot, [array]$Steps, [array]$ManualChecks) {
  $hostname = [System.Net.Dns]::GetHostName()
  Write-Output ('TRAVEL_PREFLIGHT_MODE=' + $Mode)
  Write-Output ('TRAVEL_PREFLIGHT_TARGET=' + $Target)
  Write-Output ('TRAVEL_PREFLIGHT_SKIP_TESTS=' + $(if ($SkipTests) { 'YES' } else { 'NO' }))
  Write-Output ('TRAVEL_PREFLIGHT_REPO_ROOT=' + $RepoRoot)
  Write-Output ('TRAVEL_PREFLIGHT_CURRENT_HOSTNAME=' + $hostname)
  Write-Output 'TRAVEL_PREFLIGHT_REMOTE_PRIMARY=WINDOWS_RDP'
  Write-Output 'TRAVEL_PREFLIGHT_REMOTE_FALLBACK=GOOGLE_REMOTE_DESKTOP'
  Write-Output 'TRAVEL_PREFLIGHT_LIVE_RECOVERY_PATH=CANONICAL_PC_ONLY'
  Write-Output 'TRAVEL_PREFLIGHT_LAPTOP_SCOPE=print-config,doctor,dry-run,artifact_review,development'
  Write-Output 'TRAVEL_PREFLIGHT_PC_SCOPE=live_rerun,live_send,break_glass_recovery'
  Write-Output ('TRAVEL_PREFLIGHT_STEP_COUNT=' + $Steps.Count)
  for ($i = 0; $i -lt $Steps.Count; $i++) {
    $idx = $i + 1
    $step = $Steps[$i]
    Write-Output ('TRAVEL_PREFLIGHT_STEP_' + $idx + '_TARGET=' + $step.Target)
    Write-Output ('TRAVEL_PREFLIGHT_STEP_' + $idx + '_ID=' + $step.Id)
    Write-Output ('TRAVEL_PREFLIGHT_STEP_' + $idx + '_COMMAND=' + $step.CommandText)
  }
  Write-Output ('TRAVEL_PREFLIGHT_MANUAL_CHECK_COUNT=' + $ManualChecks.Count)
  for ($i = 0; $i -lt $ManualChecks.Count; $i++) {
    $idx = $i + 1
    Write-Output ('TRAVEL_PREFLIGHT_MANUAL_CHECK_' + $idx + '=' + $ManualChecks[$i])
  }
}

function Invoke-Step([hashtable]$Step) {
  Write-Output ('TRAVEL_PREFLIGHT_STEP_BEGIN target=' + $Step.Target + ' id=' + $Step.Id + ' command=' + $Step.CommandText)
  $previous = $ErrorActionPreference
  $code = 0
  $output = @()
  try {
    $ErrorActionPreference = 'Continue'
    $output = @(& $Step.Executable @($Step.ArgumentList) 2>&1)
    if ($null -ne $LASTEXITCODE) {
      $code = [int]$LASTEXITCODE
    }
  }
  catch {
    $code = 1
    $output = @($_.Exception.Message)
  }
  finally {
    $ErrorActionPreference = $previous
  }

  foreach ($line in @($output)) {
    if ($null -eq $line) {
      continue
    }
    Write-Output ([string]$line)
  }

  if ($code -ne 0) {
    Fail 'ERR_TRAVEL_PREFLIGHT_CHECK_FAILED' ('target=' + $Step.Target + ' id=' + $Step.Id + ' exit_code=' + $code)
  }

  Write-Output ('TRAVEL_PREFLIGHT_STEP_PASS target=' + $Step.Target + ' id=' + $Step.Id)
}

$modeArg = '--print-config'
$target = 'both'
$skipTests = $false

for ($i = 0; $i -lt $args.Count; $i++) {
  $arg = [string]$args[$i]
  switch ($arg) {
    '--print-config' {
      $modeArg = '--print-config'
      continue
    }
    '--dry-run' {
      $modeArg = '--dry-run'
      continue
    }
    '--target' {
      if ($i + 1 -ge $args.Count) {
        Fail 'ERR_TRAVEL_PREFLIGHT_ARGS' 'missing value for --target'
      }
      $i += 1
      $target = Resolve-Target -Value ([string]$args[$i])
      continue
    }
    '--skip-tests' {
      $skipTests = $true
      continue
    }
    default {
      Fail 'ERR_TRAVEL_PREFLIGHT_ARGS' ('unknown flag: ' + $arg)
    }
  }
}

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$requiredPaths = @(
  (Join-Path $repoRoot 'run_with_secrets.ps1'),
  (Join-Path $repoRoot 'run_runtime_tick.py'),
  (Join-Path $repoRoot 'tools\project_context_pack.py')
)
foreach ($path in $requiredPaths) {
  if (-not (Test-Path -LiteralPath $path)) {
    Fail 'ERR_TRAVEL_PREFLIGHT_RUNNER_MISSING' ('missing ' + $path)
  }
}

$allSteps = @(Get-StepDefinitions -RepoRoot $repoRoot -SkipTests:$skipTests)
$selectedSteps = @()
foreach ($step in $allSteps) {
  if ($target -eq 'both' -or $step.Target -eq 'both' -or $step.Target -eq $target) {
    $selectedSteps += $step
  }
}
$manualChecks = @(Get-ManualChecks -Target $target)

if ($modeArg -eq '--print-config') {
  Emit-Config -Mode 'print-config' -Target $target -SkipTests:$skipTests -RepoRoot $repoRoot -Steps $selectedSteps -ManualChecks $manualChecks
  Write-Output 'PASS_TRAVEL_PREFLIGHT_PRINT_CONFIG status=OK'
  exit 0
}

Emit-Config -Mode 'dry-run' -Target $target -SkipTests:$skipTests -RepoRoot $repoRoot -Steps $selectedSteps -ManualChecks $manualChecks
foreach ($step in $selectedSteps) {
  Invoke-Step -Step $step
}
Write-Output 'PASS_TRAVEL_PREFLIGHT_DRY_RUN status=OK'
exit 0
