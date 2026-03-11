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

function Set-PythonWarningsFilter {
  $filter = "ignore:urllib3"
  $existing = [string]$env:PYTHONWARNINGS
  if ($existing -and ($existing -split ',' | Where-Object { $_.Trim() -eq $filter })) {
    return
  }
  if ($existing -and $existing.Trim().Length -gt 0) {
    $env:PYTHONWARNINGS = ($existing.TrimEnd(',') + ',' + $filter)
  } else {
    $env:PYTHONWARNINGS = $filter
  }
}

function Invoke-NativeAllowStderr {
  param(
    [string]$FilePath,
    [string[]]$ArgumentList = @()
  )

  $previousErrorActionPreference = $ErrorActionPreference
  try {
    $ErrorActionPreference = 'Continue'
    $output = @(& $FilePath @ArgumentList 2>&1)
  } catch {
    $nativePath = [string]$FilePath
    $nativeArgs = ''
    if ($ArgumentList -and $ArgumentList.Count -gt 0) {
      $nativeArgs = ($ArgumentList | ForEach-Object { [string]$_ }) -join ' '
    }
    throw ("native_start_failed file=" + $nativePath + " args=" + $nativeArgs + " err=" + $_.Exception.Message)
  } finally {
    $ErrorActionPreference = $previousErrorActionPreference
  }
  $exitCode = 0
  if ($null -ne $LASTEXITCODE) {
    $exitCode = [int]$LASTEXITCODE
  }
  $outputLines = @()
  foreach ($line in @($output)) {
    if ($null -eq $line) {
      continue
    }
    $outputLines += [string]$line
  }
  return @{
    Output = @($outputLines)
    ExitCode = $exitCode
  }
}

function Resolve-PythonExePath {
  $forced = ([string]$env:PYTHON_EXE).Trim()
  if ($forced -and (Test-Path -LiteralPath $forced)) {
    try { return (Resolve-Path -LiteralPath $forced).Path } catch { return $forced }
  }

  $candidates = New-Object System.Collections.Generic.List[string]
  $localAppData = ([string]$env:LOCALAPPDATA).Trim()
  $programData = ([string]$env:ProgramData).Trim()
  if ($programData) {
    [void]$candidates.Add((Join-Path $programData 'OSHA_Leads\python\python.exe'))
    [void]$candidates.Add((Join-Path $programData 'OSHA_Leads\Python313\python.exe'))
  }
  if ($localAppData) {
    [void]$candidates.Add((Join-Path $localAppData 'Programs\Python\Python313\python.exe'))
    [void]$candidates.Add((Join-Path $localAppData 'Programs\Python\Python312\python.exe'))
    [void]$candidates.Add((Join-Path $localAppData 'Programs\Python\Python311\python.exe'))
  }
  [void]$candidates.Add((Join-Path $env:ProgramFiles 'Python313\python.exe'))
  [void]$candidates.Add((Join-Path $env:ProgramFiles 'Python312\python.exe'))
  [void]$candidates.Add((Join-Path $env:ProgramFiles 'Python311\python.exe'))
  [void]$candidates.Add((Join-Path $env:ProgramFiles 'Python310\python.exe'))
  [void]$candidates.Add((Join-Path $env:ProgramFiles 'Python\python.exe'))
  [void]$candidates.Add((Join-Path ${env:ProgramFiles(x86)} 'Python313\python.exe'))
  [void]$candidates.Add((Join-Path ${env:ProgramFiles(x86)} 'Python312\python.exe'))
  [void]$candidates.Add((Join-Path ${env:ProgramFiles(x86)} 'Python311\python.exe'))

  foreach ($candidate in @($candidates)) {
    if (-not $candidate) { continue }
    if (Test-Path -LiteralPath $candidate) {
      try { return (Resolve-Path -LiteralPath $candidate).Path } catch { return $candidate }
    }
  }

  $pythonCmd = Get-Command -Name 'python' -ErrorAction SilentlyContinue
  if ($pythonCmd -and $pythonCmd.Source -and (Test-Path -LiteralPath $pythonCmd.Source)) {
    try { return (Resolve-Path -LiteralPath $pythonCmd.Source).Path } catch { return $pythonCmd.Source }
  }

  $userRoots = @('C:\Users\lever', 'C:\Users\Public')
  foreach ($root in $userRoots) {
    $candidate = Join-Path $root 'AppData\Local\Programs\Python\Python313\python.exe'
    if (Test-Path -LiteralPath $candidate) {
      try { return (Resolve-Path -LiteralPath $candidate).Path } catch { return $candidate }
    }
  }

  return $null
}

function Normalize-CommandForExecution {
  param(
    [string]$Exe,
    [string[]]$CommandArgs
  )

  $resolvedExe = [string]$Exe
  $resolvedArgs = @($CommandArgs)
  $requested = ([string]$Exe).Trim().ToLowerInvariant()

  if ($requested -eq 'py') {
    $pythonExe = Resolve-PythonExePath
    if ($pythonExe) {
      $resolvedExe = $pythonExe
      if ($resolvedArgs.Count -ge 1) {
        $versionArg = ([string]$resolvedArgs[0]).Trim().ToLowerInvariant()
        if ($versionArg -match '^-3(\.\d+)?$') {
          if ($resolvedArgs.Count -ge 2) {
            $resolvedArgs = $resolvedArgs[1..($resolvedArgs.Count - 1)]
          } else {
            $resolvedArgs = @()
          }
        }
      }
    }
  } elseif ($requested -eq 'python') {
    if (-not (Get-Command -Name 'python' -ErrorAction SilentlyContinue)) {
      $pythonExe = Resolve-PythonExePath
      if ($pythonExe) {
        $resolvedExe = $pythonExe
      }
    }
  }

  return @{
    Exe = $resolvedExe
    Args = @($resolvedArgs)
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
  $envSopsPath = Resolve-EnvSopsPath -RepoRoot $repoRoot
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
    if (-not $envSopsExists) { Fail ("Missing .env.sops at " + $envSopsPath) }

    if ($CheckDecrypt) {
      # Sanity check: ensure this machine can decrypt .env.sops (discard plaintext; no temp files).
      $decryptResult = Invoke-NativeAllowStderr -FilePath $sopsExe -ArgumentList @(
        '--decrypt',
        '--input-type',
        'dotenv',
        '--output-type',
        'dotenv',
        $envSopsPath
      )
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
    Write-Output ("PASS: sops_exe=" + $sopsExe + "; age_exe=" + $ageExe + "; keys_exists=True; env_sops_exists=True; env_sops_path=" + $envSopsPath + $decryptBit)
    exit 0
  }

  if (-not $Command -or $Command.Count -lt 1) {
    Fail "No command provided. Usage: scripts\\run_with_secrets.ps1 [--diagnostics] <cmd> [args...]"
  }

  if (-not (Test-Path $envSopsPath)) { Fail "Missing .env.sops at $envSopsPath" }
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
  Set-PythonWarningsFilter

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
  $childArgs = @()
  if ($Command.Count -gt 1) {
    $childArgs = $Command[1..($Command.Count - 1)]
  }

  $normalized = Normalize-CommandForExecution -Exe $exe -CommandArgs $childArgs
  $exe = [string]$normalized.Exe
  $childArgs = @()
  foreach ($arg in @($normalized['Args'])) {
    if ($null -eq $arg) {
      continue
    }
    $childArgs += [string]$arg
  }

  if (-not (Get-Command -Name $exe -ErrorAction SilentlyContinue) -and -not (Test-Path -LiteralPath $exe)) {
    Fail ("Command not found: " + $exe)
  }

  $runResult = Invoke-NativeAllowStderr -FilePath $exe -ArgumentList $childArgs
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
