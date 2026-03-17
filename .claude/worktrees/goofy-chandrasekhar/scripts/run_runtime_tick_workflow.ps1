param(
  [string]$WorkspacePath = '',
  [string]$CommandArgs = ''
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

if (-not ([string]$WorkspacePath).Trim()) {
  Write-Output 'ERR_RUNTIME_TICK_WORKFLOW_WORKSPACE_MISSING'
  exit 1
}

$repoRoot = (Resolve-Path -LiteralPath $WorkspacePath).Path
$runner = Join-Path $repoRoot 'run_with_secrets.ps1'
if (-not (Test-Path -LiteralPath $runner)) {
  Write-Output ('ERR_RUNTIME_TICK_WORKFLOW_RUNNER_MISSING path=' + $runner)
  exit 1
}

$serialized = ([string]$CommandArgs).Trim()
if (-not $serialized) {
  Write-Output 'ERR_RUNTIME_TICK_WORKFLOW_ARGS_MISSING'
  exit 1
}

$argParts = $serialized.Split(' ', [System.StringSplitOptions]::RemoveEmptyEntries)
Set-Location $repoRoot
powershell -NoProfile -ExecutionPolicy Bypass -File $runner -- py -3 @argParts
exit $LASTEXITCODE
