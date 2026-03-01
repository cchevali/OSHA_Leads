Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
$wrapper = Join-Path $repoRoot "run_with_secrets.ps1"
$ingestScript = Join-Path $repoRoot "run_osha_ingest_daily.py"
$reminderScript = Join-Path $repoRoot "scripts\scheduled\send_evening_manual_steps_reminder.py"

try {
  Push-Location $repoRoot

  & $wrapper -- py -3 $ingestScript
  $ingestCode = $LASTEXITCODE

  & $wrapper -- py -3 $reminderScript --ingest-exit-code $ingestCode
  $reminderCode = $LASTEXITCODE
  if ($reminderCode -ne 0) {
    Write-Output ("WARN_OSHA_INGEST_EVENING_REMINDER_FAILED exit_code=" + $reminderCode)
  }

  exit $ingestCode
} finally {
  try { Pop-Location } catch {}
}
