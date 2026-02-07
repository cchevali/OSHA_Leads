Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'

. (Join-Path $PSScriptRoot 'secrets_tooling.ps1')

function Fail([string]$Message) {
  # Single-line error only (no secrets).
  Write-Output ("FAIL: " + $Message)
  exit 1
}

try {
  $Diagnostics = $false
  $CheckDecrypt = $false
  $Command = @($args)

  # Avoid PowerShell parameter-binding collisions with child command flags (e.g. python -c, deliver_daily.py --db).
  # Opt into diagnostics behavior via literal sentinel args. These must precede any child command.
  while ($Command.Count -ge 1) {
    if ($Command[0] -eq '--diagnostics') {
      $Diagnostics = $true
      if ($Command.Count -ge 2) { $Command = $Command[1..($Command.Count - 1)] } else { $Command = @() }
      continue
    }
    if ($Command[0] -eq '--check-decrypt') {
      $CheckDecrypt = $true
      if ($Command.Count -ge 2) { $Command = $Command[1..($Command.Count - 1)] } else { $Command = @() }
      continue
    }
    break
  }

  if ($CheckDecrypt -and (-not $Diagnostics)) {
    Fail "--check-decrypt requires --diagnostics"
  }

  $repoRoot = Resolve-RepoRoot
  $envSopsPath = Join-Path $repoRoot '.env.sops'
  $ageKeysPath = Get-AgeKeyFilePath

  # Make behavior independent of the caller's current working directory (Task Scheduler often starts in System32).
  Push-Location $repoRoot

  # Diagnostics-only mode: check wiring prerequisites without decrypting or running anything.
  # Output MUST be a single PASS/FAIL line (no secrets).
  if ($Diagnostics -and ($Command.Count -lt 1)) {
    $sopsExe = Resolve-SopsExe
    if (-not $sopsExe) { Fail "sops not found (install: winget install --id Mozilla.SOPS -e)" }
    $ageExe = Resolve-AgeExe
    if (-not $ageExe) { Fail "age not found (install: winget install --id FiloSottile.age -e)" }

    $keysExists = Test-Path $ageKeysPath
    if (-not $keysExists) { Fail "Missing age key file at %APPDATA%\\sops\\age\\keys.txt" }
    $envSopsExists = Test-Path $envSopsPath
    if (-not $envSopsExists) { Fail "Missing repo .env.sops" }

    if ($CheckDecrypt) {
      # Sanity check: ensure this machine can decrypt .env.sops (discard plaintext; no temp files).
      $cmdLine = '"' + $sopsExe + '" --decrypt --input-type dotenv --output-type dotenv "' + $envSopsPath + '" 1>nul'
      $prevEap = $ErrorActionPreference
      $ErrorActionPreference = 'Continue'
      $err = & cmd /c $cmdLine 2>&1
      $code = $LASTEXITCODE
      $ErrorActionPreference = $prevEap
      if ($code -ne 0) {
        $errText = ''
        if ($err -is [string]) {
          $errText = $err
        } else {
          $errText = (($err | ForEach-Object { $_.ToString() }) -join ' ')
        }

        $msg = $errText.Trim()
        $msg = ($msg -replace '[\r\n]+', ' ').Trim()
        if ($msg.Length -gt 220) { $msg = $msg.Substring(0, 220) + '...' }
        if (-not $msg) { $msg = 'unknown error' }
        Fail ("sops decrypt sanity check failed: " + $msg)
      }
    }

    $decryptBit = if ($CheckDecrypt) { '; decrypt_ok=True' } else { '' }
    Write-Output ("PASS: sops_exe=" + $sopsExe + "; age_exe=" + $ageExe + "; keys_exists=True; env_sops_exists=True" + $decryptBit)
    exit 0
  }

  if (-not $Command -or $Command.Count -lt 1) {
    Fail "No command provided. Usage: scripts\\run_with_secrets.ps1 [--diagnostics] <cmd> [args...]"
  }

  if (-not (Test-Path $envSopsPath)) { Fail "Missing repo .env.sops at $envSopsPath" }
  if (-not (Test-Path $ageKeysPath)) { Fail "Missing age key file at %APPDATA%\\sops\\age\\keys.txt" }

  $sopsExe = Resolve-SopsExe
  if (-not $sopsExe) { Fail "sops not found (install: winget install --id Mozilla.SOPS -e)" }

  $ageExe = Resolve-AgeExe
  if (-not $ageExe) { Fail "age not found (install: winget install --id FiloSottile.age -e)" }

  if ($Diagnostics) {
    Write-Output ("DIAG: sops_exe=" + $sopsExe)
    Write-Output ("DIAG: age_exe=" + $ageExe)
    Write-Output ("DIAG: age_keys_exists=True")
  }

  $plain = Decrypt-DotenvSopsFile -SopsExe $sopsExe -EnvSopsPath $envSopsPath
  if ($plain -match 'AGE-SECRET-KEY-' -or $plain -match 'public key:\s*age1') {
    Fail "Decrypted env appears to contain an age key (refusing)"
  }

  # Load into process env for the child command. Never print values.
  Set-EnvFromDotenvText -DotenvText $plain

  $exe = $Command[0]
  $args = @()
  if ($Command.Count -gt 1) {
    $args = $Command[1..($Command.Count - 1)]
  }

  & $exe @args
  exit $LASTEXITCODE
} catch {
  Fail $_.Exception.Message
} finally {
  try { Pop-Location } catch {}
}
