Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'
$VerbosePreference = 'SilentlyContinue'

. (Join-Path $PSScriptRoot 'secrets_tooling.ps1')

function Fail([string]$Message) {
  # Single-line error only (no secrets).
  Write-Output ("FAIL: " + $Message)
  exit 1
}

$repoRoot = Resolve-RepoRoot
$envSopsPath = Join-Path $repoRoot '.env.sops'
$sopsYamlPath = Join-Path $repoRoot '.sops.yaml'
$envExamplePath = Join-Path $repoRoot '.env.example'
$ageKeysPath = Get-AgeKeyFilePath

$sopsExe = Resolve-SopsExe
if (-not $sopsExe) { Fail "sops not found (install: winget install --id Mozilla.SOPS -e)" }

$ageExe = Resolve-AgeExe
if (-not $ageExe) { Fail "age not found (install: winget install --id FiloSottile.age -e)" }

if (-not (Test-Path $ageKeysPath)) { Fail "Missing age key file at %APPDATA%\\sops\\age\\keys.txt" }
if (-not (Test-Path $sopsYamlPath)) { Fail "Missing repo .sops.yaml" }
if (-not (Test-Path $envSopsPath)) { Fail "Missing repo .env.sops" }

Write-Output ("DIAG: sops_exe=" + $sopsExe)
Write-Output ("DIAG: age_exe=" + $ageExe)
Write-Output ("DIAG: age_keys_exists=True")

# Decrypt-test without ever printing plaintext to the console.
try {
  $plain = Decrypt-DotenvSopsFile -SopsExe $sopsExe -EnvSopsPath $envSopsPath
} catch {
  Fail "sops decrypt-test failed (check keys/permissions and that sops/age are installed)"
}

if ($plain -match 'AGE-SECRET-KEY-' -or $plain -match 'public key:\s*age1') {
  Fail "Decrypted env appears to contain an age private/public key (refusing)"
}

$plainKeys = @(Get-DotenvKeys -DotenvText $plain)
if ($plainKeys.Count -lt 1) { Fail "Decrypted env did not contain any KEY=VALUE entries" }

if (Test-Path $envExamplePath) {
  $exampleText = Get-Content -LiteralPath $envExamplePath -Raw
  $exampleKeys = @(Get-DotenvKeys -DotenvText $exampleText)
  $missing = @()
  foreach ($k in $exampleKeys) {
    if ($plainKeys -notcontains $k) { $missing += $k }
  }
  if ($missing.Count -gt 0) {
    Fail ("Missing keys from decrypted env: " + ($missing -join ', '))
  }
}

Write-Output ("PASS: decrypt OK; keys.txt present; env keys=" + $plainKeys.Count)
exit 0
