param(
  [string]$Batch = '',
  [switch]$PrintConfig,
  [switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$wrapper = Join-Path $repoRoot 'run_with_secrets.ps1'

if (-not (Test-Path -LiteralPath $wrapper)) {
  Write-Output ('ERR_AI_ASSIST_CLIPBOARD_WRAPPER_MISSING path=' + $wrapper)
  exit 1
}

$toolArgs = @('tools\\import_prospect_ai_assist_review.py')
if (([string]$Batch).Trim()) {
  $toolArgs += @('--batch', ([string]$Batch).Trim())
}
if ($PrintConfig) {
  $toolArgs += '--print-config'
  $toolArgs += '--stdin'
}
if ($DryRun) {
  $toolArgs += '--dry-run'
}

if ($PrintConfig) {
  try {
    Push-Location $repoRoot
    $allOutput = & $wrapper -- py -3 @toolArgs 2>&1
    $exitCode = $LASTEXITCODE
    foreach ($line in @($allOutput)) {
      Write-Output ([string]$line)
    }
    exit $exitCode
  }
  finally {
    try { Pop-Location } catch {}
  }
}

$clipboardText = [string](Get-Clipboard -Raw)
if (-not $clipboardText.Trim()) {
  Write-Output 'ERR_AI_ASSIST_CLIPBOARD_EMPTY detail=clipboard_blank'
  exit 1
}

$tempPath = [System.IO.Path]::Combine([System.IO.Path]::GetTempPath(), ('prospect_ai_assist_clipboard_' + [Guid]::NewGuid().ToString('N') + '.csv'))

try {
  [System.IO.File]::WriteAllText($tempPath, $clipboardText, [System.Text.UTF8Encoding]::new($false))
  $toolArgs += @('--input', $tempPath)
  if (-not (([string]$Batch).Trim())) {
    $toolArgs += @('--batch', ([DateTime]::Now.ToString('yyyy-MM-dd') + '_AIASSIST_MANUAL_' + [DateTime]::Now.ToString('HHmmss')))
  }

  Push-Location $repoRoot
  $allOutput = & $wrapper -- py -3 @toolArgs 2>&1
  $exitCode = $LASTEXITCODE
  foreach ($line in @($allOutput)) {
    Write-Output ([string]$line)
  }
  exit $exitCode
}
finally {
  try { Pop-Location } catch {}
  if (Test-Path -LiteralPath $tempPath) {
    Remove-Item -LiteralPath $tempPath -Force -ErrorAction SilentlyContinue
  }
}
