Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'
if ($PSVersionTable.PSVersion.Major -ge 7) {
  $PSNativeCommandUseErrorActionPreference = $false
}

. (Join-Path $PSScriptRoot 'secrets_tooling.ps1')

function Fail([string]$Message) {
  # Single-line error only (no secrets).
  Write-Output ("FAIL: " + $Message)
  exit 1
}

function Invoke-NativeAllowStderr {
  param(
    [string]$FilePath,
    [string[]]$ArgumentList = @()
  )

  $stdoutPath = [System.IO.Path]::GetTempFileName()
  $stderrPath = [System.IO.Path]::GetTempFileName()
  try {
    $proc = Start-Process -FilePath $FilePath -ArgumentList $ArgumentList -NoNewWindow -Wait -PassThru -RedirectStandardOutput $stdoutPath -RedirectStandardError $stderrPath
    $stdoutLines = @()
    $stderrLines = @()
    if (Test-Path -LiteralPath $stdoutPath) {
      $stdoutLines = @(Get-Content -LiteralPath $stdoutPath -ErrorAction SilentlyContinue)
    }
    if (Test-Path -LiteralPath $stderrPath) {
      $stderrLines = @(Get-Content -LiteralPath $stderrPath -ErrorAction SilentlyContinue)
    }
    return @{
      Output = @($stdoutLines + $stderrLines)
      ExitCode = [int]$proc.ExitCode
    }
  } finally {
    if (Test-Path -LiteralPath $stdoutPath) {
      Remove-Item -LiteralPath $stdoutPath -Force -ErrorAction SilentlyContinue
    }
    if (Test-Path -LiteralPath $stderrPath) {
      Remove-Item -LiteralPath $stderrPath -Force -ErrorAction SilentlyContinue
    }
  }
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
    if (-not $keysExists) { Fail ("Missing age key file at " + $ageKeysPath) }
    $envSopsExists = Test-Path $envSopsPath
    if (-not $envSopsExists) { Fail "Missing repo .env.sops" }

    if ($CheckDecrypt) {
      # Sanity check: ensure this machine can decrypt .env.sops (discard plaintext; no temp files).
      $cmdLine = '"' + $sopsExe + '" --decrypt --input-type dotenv --output-type dotenv "' + $envSopsPath + '" 1>nul'
      $decryptResult = Invoke-NativeAllowStderr -FilePath 'cmd' -ArgumentList @('/c', $cmdLine)
      $err = @($decryptResult.Output)
      $code = [int]$decryptResult.ExitCode
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
  if (-not (Test-Path $ageKeysPath)) { Fail ("Missing age key file at " + $ageKeysPath) }

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

  $inheritedDataDir = ''
  if (Test-Path -LiteralPath 'Env:DATA_DIR') {
    $inheritedDataDir = [string]$env:DATA_DIR
  }
  $dotenvMap = ConvertFrom-DotenvTextToMap -DotenvText $plain
  $dotenvDataDir = ''
  if ($dotenvMap.ContainsKey('DATA_DIR')) {
    $dotenvDataDir = [string]$dotenvMap['DATA_DIR']
  }
  $dataDirPolicy = Resolve-MfoDataDirPolicy `
    -RepoRoot $repoRoot `
    -InheritedDataDir $inheritedDataDir `
    -DotenvDataDir $dotenvDataDir

  if ([string]$dataDirPolicy.ConflictWarnToken) {
    Write-Output ([string]$dataDirPolicy.ConflictWarnToken)
  }
  if ([string]$dataDirPolicy.NotAbsoluteWarnToken) {
    Write-Output ([string]$dataDirPolicy.NotAbsoluteWarnToken)
  }

  # Load decrypted keys for the child command; DATA_DIR is policy-managed below.
  Set-EnvFromDotenvText -DotenvText $plain -SkipKeys @('DATA_DIR')

  $effectiveDataDir = [string]$dataDirPolicy.EffectivePath
  $effectiveDataDirSource = [string]$dataDirPolicy.Source
  $env:MFO_DATA_DIR_EFFECTIVE = $effectiveDataDir
  $env:MFO_DATA_DIR_SOURCE = $effectiveDataDirSource
  if ([bool]$dataDirPolicy.UseDefaultFallback) {
    Remove-Item -Path 'Env:DATA_DIR' -ErrorAction SilentlyContinue
  } else {
    Set-Item -Path 'Env:DATA_DIR' -Value $effectiveDataDir
  }
  if ($Diagnostics) {
    Write-Output ("DIAG: mfo_data_dir_effective=" + $effectiveDataDir)
    Write-Output ("DIAG: mfo_data_dir_source=" + $effectiveDataDirSource)
  }

  $exe = $Command[0]
  if (-not (Get-Command -Name $exe -ErrorAction SilentlyContinue)) {
    Fail ("Command not found: " + $exe)
  }
  $args = @()
  if ($Command.Count -gt 1) {
    $args = $Command[1..($Command.Count - 1)]
  }

  $runResult = Invoke-NativeAllowStderr -FilePath $exe -ArgumentList $args
  foreach ($line in @($runResult.Output)) {
    Write-Output $line
  }
  $exitCode = [int]$runResult.ExitCode
  exit $exitCode
} catch {
  Fail $_.Exception.Message
} finally {
  try { Pop-Location } catch {}
}
