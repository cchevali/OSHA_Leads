Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
. (Join-Path $PSScriptRoot 'runtime_guard.ps1')
. (Join-Path $PSScriptRoot 'runtime_run_summary.ps1')

$wrapper = Join-Path $repoRoot 'run_with_secrets.ps1'
$triageScript = Join-Path $repoRoot 'inbound_inbox_triage.py'
$captureScript = Join-Path $repoRoot 'run_capture_sync.py'
$setOutreachEnv = Join-Path $repoRoot 'scripts\set_outreach_env.ps1'
$gmailCredentials = Join-Path $repoRoot 'secrets\gmail_credentials.json'
$startLocal = Get-Date
$startUtc = [datetime]::UtcNow
$inboundExitCode = 1
$preflight = $null
$runtimeTickState = $null
$commandInvoked = '.\run_with_secrets.ps1 -- py -3 inbound_inbox_triage.py --run-once ; .\run_with_secrets.ps1 -- py -3 run_capture_sync.py'
$bootstrapLines = New-Object System.Collections.Generic.List[string]

function Add-BootstrapLine([string]$Line) {
  $text = [string]$Line
  if ($text) {
    [void]$bootstrapLines.Add($text)
  }
}

$preflight = Invoke-RuntimePreflight `
  -RepoRoot $repoRoot `
  -Mode 'scheduled' `
  -Intent 'write' `
  -DryRun:$false `
  -EmitLine ${function:Add-BootstrapLine}

$runtimeTickState = Test-RuntimeTickIntervalSlotAlreadyHandled `
  -RepoRoot $repoRoot `
  -JobName 'inbound_triage' `
  -NowLocal $startLocal `
  -IntervalMinutes 15 `
  -EmitLine ${function:Add-BootstrapLine}

$taskLogDir = Resolve-DefaultTaskLogRoot -RepoRoot $repoRoot
$runSummaryRoot = Resolve-DefaultRunSummaryRoot -RepoRoot $repoRoot
$runId = New-RuntimeRunId -StartLocal $startLocal -StartUtc $startUtc
$taskLogPath = New-RuntimeTaskLogPath -TaskLogRoot $taskLogDir -WrapperName 'OSHA_Inbound_Triage' -RunId $runId

New-Item -ItemType Directory -Force -Path $taskLogDir | Out-Null
New-Item -ItemType Directory -Force -Path $runSummaryRoot | Out-Null

function Write-TaskLine([string]$Line) {
  $text = [string]$Line
  if ([string]::IsNullOrWhiteSpace($text)) {
    return
  }
  Write-RuntimeTaskLogLine -TaskLogPath $taskLogPath -Line $text
}

function Get-InboundConfigSnapshot {
  $lines = & powershell -NoProfile -ExecutionPolicy Bypass -File $setOutreachEnv -PrintConfig 2>&1
  $exitCode = [int]$LASTEXITCODE
  $values = @{}
  foreach ($line in @($lines)) {
    $text = [string]$line
    if (-not [string]::IsNullOrWhiteSpace($text)) {
      Write-TaskLine $text | Out-Null
    }
    $equalsIndex = $text.IndexOf('=')
    if ($equalsIndex -le 0) {
      continue
    }
    $key = $text.Substring(0, $equalsIndex).Trim().ToLowerInvariant()
    if (-not $key) {
      continue
    }
    $value = $text.Substring($equalsIndex + 1).Trim()
    $values[$key] = $value
  }
  return @{
    Ok = ($exitCode -eq 0)
    ExitCode = $exitCode
    Values = $values
  }
}

foreach ($line in @($bootstrapLines)) {
  Write-TaskLine ([string]$line)
}

function Invoke-And-Log([scriptblock]$Invocation) {
  $lines = & $Invocation 2>&1
  foreach ($line in @($lines)) {
    Write-TaskLine ([string]$line)
  }
}

try {
  Push-Location $repoRoot
  try {
    $inboundConfig = Get-InboundConfigSnapshot
    if (-not [bool]$preflight.Ok) {
      throw 'runtime preflight failed'
    }
    if (-not [bool]$inboundConfig.Ok) {
      throw ('set_outreach_env_print_config_failed code=' + [string]$inboundConfig.ExitCode)
    }
    if ([bool]$runtimeTickState.Skip) {
      $inboundExitCode = 0
      Write-TaskLine ('INBOUND_TRIAGE_SKIPPED reason=runtime_tick_same_slot slot=' + [string]$runtimeTickState.SlotKey)
    }
    else {
      $configValues = [hashtable]$inboundConfig.Values
      $backend = ''
      if ($configValues.ContainsKey('inbound_backend')) {
        $backend = ([string]$configValues['inbound_backend']).Trim().ToLowerInvariant()
      }
      if (-not $backend) {
        $backend = 'gmail'
      }
      if ($backend -eq 'imap') {
        $imapUser = if ($configValues.ContainsKey('imap_user')) { ([string]$configValues['imap_user']).Trim() } else { '' }
        $imapPassPresent = if ($configValues.ContainsKey('imap_pass_present')) { ([string]$configValues['imap_pass_present']).Trim().ToUpperInvariant() } else { 'NO' }
        if ((-not $imapUser) -or ($imapPassPresent -ne 'YES')) {
          $inboundExitCode = 0
          Write-TaskLine 'INBOUND_TRIAGE_SKIPPED reason=imap_credentials_missing'
        } else {
          Invoke-And-Log {
            & $wrapper -- py -3 $triageScript --run-once
          }
          $inboundExitCode = [int]$LASTEXITCODE
          if ($inboundExitCode -eq 0) {
            Invoke-And-Log {
              & $wrapper -- py -3 $captureScript
            }
            $inboundExitCode = [int]$LASTEXITCODE
          }
        }
      }
      elseif ($backend -eq 'gmail') {
        if (-not (Test-Path -LiteralPath $gmailCredentials)) {
          $inboundExitCode = 0
          Write-TaskLine ('INBOUND_TRIAGE_SKIPPED reason=gmail_credentials_missing path=' + $gmailCredentials)
        } else {
          Invoke-And-Log {
            & $wrapper -- py -3 $triageScript --run-once
          }
          $inboundExitCode = [int]$LASTEXITCODE
          if ($inboundExitCode -eq 0) {
            Invoke-And-Log {
              & $wrapper -- py -3 $captureScript
            }
            $inboundExitCode = [int]$LASTEXITCODE
          }
        }
      }
      else {
        $inboundExitCode = 0
        Write-TaskLine ('INBOUND_TRIAGE_SKIPPED reason=invalid_inbound_backend backend=' + $backend)
      }
    }
  }
  catch {
    $inboundExitCode = 1
    Write-TaskLine ('INBOUND_TRIAGE_EXCEPTION=' + ([string]$_.Exception.Message))
  }
}
finally {
  try { Pop-Location } catch {}
}

Write-TaskLine ('TASK_LOG_PATH=' + $taskLogPath)
Write-TaskLine ('INBOUND_TRIAGE_EXIT_CODE=' + $inboundExitCode)
Write-RuntimeRunSummary `
  -RepoRoot $repoRoot `
  -WrapperName 'OSHA_Inbound_Triage' `
  -CommandLine $commandInvoked `
  -Mode 'scheduled' `
  -Intent 'write' `
  -DryRun:$false `
  -ExitCode $inboundExitCode `
  -StartLocal $startLocal `
  -StartUtc $startUtc `
  -RunId $runId `
  -TaskLogPath $taskLogPath `
  -TaskLogRoot $taskLogDir `
  -RunSummaryRoot $runSummaryRoot `
  -Fingerprint $(if ($preflight) { [hashtable]$preflight.Values } else { @{} }) `
  -EmitLine ${function:Write-TaskLine} | Out-Null
# RUN_SUMMARY_JSON_PATH= / RUN_SUMMARY_TEXT_PATH= emitted above via Write-RuntimeRunSummary.

if ($inboundExitCode -ne 0) {
  exit 1
}
exit 0
