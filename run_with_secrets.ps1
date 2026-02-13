Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'

function Invoke-ContextPackSoftCheck {
  param([string]$RepoRoot)

  $contextPackScript = Join-Path $RepoRoot 'tools\project_context_pack.py'
  if (-not (Test-Path -LiteralPath $contextPackScript)) {
    Write-Output "WARN_CONTEXT_PACK_SCRIPT_MISSING tools/project_context_pack.py"
    return
  }

  if (-not (Get-Command py -ErrorAction SilentlyContinue)) {
    Write-Output "WARN_CONTEXT_PACK_CHECK_FAILED runner_not_found"
    return
  }

  $lines = @()
  $exitCode = 0
  try {
    $output = & py -3 $contextPackScript --check --soft 2>&1
    $exitCode = $LASTEXITCODE
    foreach ($line in @($output)) {
      $text = [string]$line
      if ($text) {
        $lines += $text.Trim()
      }
    }
  } catch {
    Write-Output ("WARN_CONTEXT_PACK_CHECK_FAILED error=" + $_.Exception.GetType().Name)
    return
  }

  $hasWarn = $false
  foreach ($line in $lines) {
    if ($line.StartsWith('WARN_CONTEXT_PACK_') -or $line.StartsWith('ERR_CONTEXT_PACK_')) {
      $hasWarn = $true
      break
    }
  }

  if ($hasWarn) {
    foreach ($line in $lines) {
      if ($line.StartsWith('PASS_CONTEXT_PACK_CHECK')) {
        continue
      }
      Write-Output $line
    }
    return
  }

  if ($exitCode -ne 0) {
    Write-Output ("WARN_CONTEXT_PACK_CHECK_FAILED returncode=" + $exitCode)
    foreach ($line in $lines) {
      if ($line.StartsWith('PASS_CONTEXT_PACK_CHECK')) {
        continue
      }
      Write-Output $line
    }
  }
}

# Convenience wrapper so callers can run from repo root:
#   .\run_with_secrets.ps1 ...
$wrapperPath = $PSCommandPath
if (-not $wrapperPath) {
  $wrapperPath = $MyInvocation.MyCommand.Path
}

$targetPath = Join-Path $PSScriptRoot 'scripts\run_with_secrets.ps1'

try { $wrapperResolved = (Resolve-Path -LiteralPath $wrapperPath).Path } catch { $wrapperResolved = $wrapperPath }
try { $targetResolved = (Resolve-Path -LiteralPath $targetPath).Path } catch { $targetResolved = $targetPath }

if ($wrapperResolved -and $targetResolved -and ($wrapperResolved -ieq $targetResolved)) {
  throw ("run_with_secrets wrapper recursion guard: wrapper == target (" + $wrapperResolved + ")")
}
if (-not (Test-Path -LiteralPath $targetPath)) {
  throw ("run_with_secrets wrapper target missing: " + $targetPath)
}

Invoke-ContextPackSoftCheck -RepoRoot $PSScriptRoot

if ($args -contains '--diagnostics') {
  Write-Output ("DIAG: wrapper_path=" + $wrapperResolved)
  Write-Output ("DIAG: target_path=" + $targetResolved)
}

& $targetPath @args
exit $LASTEXITCODE
