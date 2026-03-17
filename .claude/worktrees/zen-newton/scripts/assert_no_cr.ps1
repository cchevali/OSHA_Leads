Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'

function Fail([string]$Message) {
  # Single-line output only.
  Write-Output ("FAIL: " + $Message)
  exit 1
}

try {
  $repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path

  $paths = @(
    (Join-Path $repoRoot '.env.sops'),
    (Join-Path $repoRoot '.sops.yaml')
  )

  $missing = @()
  foreach ($p in $paths) {
    if (-not (Test-Path -LiteralPath $p)) { $missing += (Split-Path -Leaf $p) }
  }
  if ($missing.Count -gt 0) {
    Fail ("Missing required files: " + ($missing -join ', '))
  }

  $bad = @()
  foreach ($p in $paths) {
    $bytes = [System.IO.File]::ReadAllBytes($p)
    if ([System.Array]::IndexOf($bytes, [byte]13) -ge 0) {
      $bad += (Split-Path -Leaf $p)
    }
  }

  if ($bad.Count -gt 0) {
    Fail ("CR byte(s) detected in: " + ($bad -join ', '))
  }

  Write-Output "PASS: no CR bytes in checked files"
  exit 0
} catch {
  Fail $_.Exception.Message
}
