Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'

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

if ($args -contains '--diagnostics') {
  Write-Output ("DIAG: wrapper_path=" + $wrapperResolved)
  Write-Output ("DIAG: target_path=" + $targetResolved)
}

& $targetPath @args
exit $LASTEXITCODE
