Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'
$VerbosePreference = 'SilentlyContinue'

function Fail([string]$Message) {
  Write-Output ("FAIL: " + $Message)
  exit 1
}

function Resolve-RepoRoot {
  # scripts/verify_secrets.ps1 -> repo root
  $root = Resolve-Path (Join-Path $PSScriptRoot '..')
  return $root.Path
}

function Find-ToolExe([string]$ToolName, [string[]]$WingetPackagePrefixes, [string[]]$RelativeCandidates) {
  $cmd = Get-Command $ToolName -ErrorAction SilentlyContinue
  if ($cmd -and $cmd.Source -and (Test-Path $cmd.Source)) {
    return $cmd.Source
  }

  $pkgRoot = Join-Path $env:LOCALAPPDATA 'Microsoft\WinGet\Packages'
  foreach ($prefix in $WingetPackagePrefixes) {
    $matches = @(Get-ChildItem -LiteralPath $pkgRoot -Directory -ErrorAction SilentlyContinue | Where-Object { $_.Name -like "$prefix*" })
    foreach ($m in $matches) {
      foreach ($rel in $RelativeCandidates) {
        $p = Join-Path $m.FullName $rel
        if (Test-Path $p) {
          return $p
        }
      }
    }
  }

  return $null
}

function Get-DotenvKeys([string]$DotenvText) {
  $keys = New-Object System.Collections.Generic.HashSet[string]
  $lines = $DotenvText -split "`r?`n"
  foreach ($line in $lines) {
    $t = $line.Trim()
    if ($t.Length -eq 0) { continue }
    if ($t.StartsWith('#')) { continue }
    if ($t -match '^\s*(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*=') {
      [void]$keys.Add($Matches[1])
    }
  }
  return @($keys)
}

$repoRoot = Resolve-RepoRoot
$envSopsPath = Join-Path $repoRoot '.env.sops'
$sopsYamlPath = Join-Path $repoRoot '.sops.yaml'
$envExamplePath = Join-Path $repoRoot '.env.example'
$ageKeysPath = Join-Path (Join-Path $env:APPDATA 'sops\age') 'keys.txt'

$sopsExe = Find-ToolExe -ToolName 'sops' -WingetPackagePrefixes @('Mozilla.SOPS_') -RelativeCandidates @('sops.exe')
if (-not $sopsExe) { Fail "sops not found (install: winget install --id Mozilla.SOPS -e)" }

$ageExe = Find-ToolExe -ToolName 'age' -WingetPackagePrefixes @('FiloSottile.age_') -RelativeCandidates @('age\age.exe')
if (-not $ageExe) { Fail "age not found (install: winget install --id FiloSottile.age -e)" }

if (-not (Test-Path $ageKeysPath)) { Fail "Missing age key file at %APPDATA%\\sops\\age\\keys.txt" }
if (-not (Test-Path $sopsYamlPath)) { Fail "Missing repo .sops.yaml" }
if (-not (Test-Path $envSopsPath)) { Fail "Missing repo .env.sops" }

# Decrypt-test without ever printing plaintext to the console.
# PowerShell captures native command stdout as an array of lines; join back to preserve newlines for parsing.
$plain = (& $sopsExe --decrypt --input-type dotenv --output-type dotenv $envSopsPath 2>$null) -join "`n"
if ($LASTEXITCODE -ne 0) { Fail "sops decrypt-test failed (check keys/permissions and that sops/age are installed)" }

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
