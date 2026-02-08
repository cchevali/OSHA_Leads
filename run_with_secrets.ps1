Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'

# Convenience wrapper so callers can run from repo root:
#   .\run_with_secrets.ps1 ...
& (Join-Path $PSScriptRoot 'scripts\run_with_secrets.ps1') @args
exit $LASTEXITCODE

