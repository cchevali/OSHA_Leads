Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'

function Resolve-RepoRoot {
  # scripts/secrets_tooling.ps1 lives under scripts/ -> repo root is one level up.
  $root = Resolve-Path (Join-Path $PSScriptRoot '..')
  return $root.Path
}

function Resolve-ExternalExe {
  param(
    [Parameter(Mandatory = $true)] [string] $ToolName,
    [string[]] $WinGetLinkExeNames = @(),   # e.g. @('sops.exe')
    [string[]] $WinGetPackagePrefixes = @(), # e.g. @('Mozilla.SOPS_')
    [string[]] $WinGetPackageRelativeCandidates = @(), # e.g. @('sops.exe')
    [string[]] $ExtraAbsoluteCandidates = @()
  )

  # 1) First: whatever is already resolvable via Get-Command.
  $cmd = Get-Command $ToolName -ErrorAction SilentlyContinue
  if ($cmd -and $cmd.Source -and (Test-Path $cmd.Source)) {
    return (Resolve-Path $cmd.Source).Path
  }

  # 2) WinGet Links shims (commonly missing from PATH in scheduler contexts).
  $linksDir = Join-Path $env:LOCALAPPDATA 'Microsoft\WinGet\Links'
  if (Test-Path $linksDir) {
    foreach ($exe in $WinGetLinkExeNames) {
      $p = Join-Path $linksDir $exe
      if (Test-Path $p) {
        return (Resolve-Path $p).Path
      }
    }
  }

  # 3) Common absolute installs.
  foreach ($p in $ExtraAbsoluteCandidates) {
    if ($p -and (Test-Path $p)) {
      return (Resolve-Path $p).Path
    }
  }

  # 4) WinGet Packages directory (matches are versioned in the folder name).
  $pkgRoot = Join-Path $env:LOCALAPPDATA 'Microsoft\WinGet\Packages'
  if (Test-Path $pkgRoot) {
    foreach ($prefix in $WinGetPackagePrefixes) {
      $dirs =
        Get-ChildItem -LiteralPath $pkgRoot -Directory -ErrorAction SilentlyContinue |
        Where-Object { $_.Name -like ($prefix + '*') } |
        Sort-Object -Property LastWriteTime -Descending

      foreach ($d in $dirs) {
        foreach ($rel in $WinGetPackageRelativeCandidates) {
          $p = Join-Path $d.FullName $rel
          if (Test-Path $p) {
            return (Resolve-Path $p).Path
          }
        }
      }
    }
  }

  return $null
}

function Resolve-SopsExe {
  return Resolve-ExternalExe `
    -ToolName 'sops' `
    -WinGetLinkExeNames @('sops.exe') `
    -WinGetPackagePrefixes @('Mozilla.SOPS_') `
    -WinGetPackageRelativeCandidates @('sops.exe') `
    -ExtraAbsoluteCandidates @(
      (Join-Path $env:ProgramFiles 'sops\sops.exe'),
      (Join-Path $env:ProgramFiles 'SOPS\sops.exe')
    )
}

function Resolve-AgeExe {
  return Resolve-ExternalExe `
    -ToolName 'age' `
    -WinGetLinkExeNames @('age.exe') `
    -WinGetPackagePrefixes @('FiloSottile.age_') `
    -WinGetPackageRelativeCandidates @('age\age.exe','age.exe') `
    -ExtraAbsoluteCandidates @(
      (Join-Path $env:ProgramFiles 'age\age.exe'),
      (Join-Path $env:ProgramFiles 'Age\age.exe')
    )
}

function Resolve-AgeKeygenExe {
  return Resolve-ExternalExe `
    -ToolName 'age-keygen' `
    -WinGetLinkExeNames @('age-keygen.exe') `
    -WinGetPackagePrefixes @('FiloSottile.age_') `
    -WinGetPackageRelativeCandidates @('age\age-keygen.exe','age-keygen.exe') `
    -ExtraAbsoluteCandidates @(
      (Join-Path $env:ProgramFiles 'age\age-keygen.exe'),
      (Join-Path $env:ProgramFiles 'Age\age-keygen.exe')
    )
}

function Get-AgeKeyFilePath {
  # Prefer explicit configuration first.
  $p = $env:SOPS_AGE_KEY_FILE
  if ($p -and $p.Trim().Length -gt 0) {
    return $p
  }

  # Repo-standard location (when keys are managed alongside the repo).
  $repoRoot = Resolve-RepoRoot
  $repoKeys = Join-Path $repoRoot 'keys\age\keys.txt'
  if (Test-Path -LiteralPath $repoKeys) {
    # Make sops behavior deterministic for the remainder of this session/process.
    $env:SOPS_AGE_KEY_FILE = $repoKeys
    return $repoKeys
  }

  # Default SOPS age key location on Windows.
  return (Join-Path (Join-Path $env:APPDATA 'sops\age') 'keys.txt')
}

function Get-DotenvKeys {
  param([Parameter(Mandatory = $true)] [string] $DotenvText)
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

function ConvertFrom-DotenvTextToMap {
  param([Parameter(Mandatory = $true)] [string] $DotenvText)

  $map = @{}
  $lines = $DotenvText -split "`r?`n"
  foreach ($line in $lines) {
    $t = $line.Trim()
    if ($t.Length -eq 0) { continue }
    if ($t.StartsWith('#')) { continue }

    $work = $line
    if ($work -match '^\s*export\s+') {
      $work = $work -replace '^\s*export\s+', ''
    }
    if ($work -notmatch '^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=(.*)$') {
      continue
    }
    $key = $Matches[1]
    $rawValue = $Matches[2]
    $value = ConvertFrom-DotenvValue -Raw $rawValue
    $map[$key] = $value
  }
  return $map
}

function ConvertFrom-DotenvValue {
  param([Parameter(Mandatory = $true)] [string] $Raw)

  $v = $Raw.Trim()
  if ($v.Length -ge 2 -and $v.StartsWith('"') -and $v.EndsWith('"')) {
    $inner = $v.Substring(1, $v.Length - 2)
    # Minimal unescape for common .env patterns.
    $inner = $inner -replace '\\\\n', "`n"
    $inner = $inner -replace '\\\\r', "`r"
    $inner = $inner -replace '\\\\t', "`t"
    $inner = $inner -replace '\\"', '"'
    $inner = $inner -replace '\\\\', '\'
    return $inner
  }
  if ($v.Length -ge 2 -and $v.StartsWith("'") -and $v.EndsWith("'")) {
    return $v.Substring(1, $v.Length - 2)
  }
  return $v
}

function Set-EnvFromDotenvText {
  param(
    [Parameter(Mandatory = $true)] [string] $DotenvText,
    [string[]] $SkipKeys = @()
  )

  $skip = New-Object System.Collections.Generic.HashSet[string] ([System.StringComparer]::OrdinalIgnoreCase)
  foreach ($item in @($SkipKeys)) {
    $token = [string]$item
    if ($token) {
      [void]$skip.Add($token)
    }
  }

  $lines = $DotenvText -split "`r?`n"
  foreach ($line in $lines) {
    $t = $line.Trim()
    if ($t.Length -eq 0) { continue }
    if ($t.StartsWith('#')) { continue }

    $work = $line
    if ($work -match '^\s*export\s+') {
      $work = $work -replace '^\s*export\s+', ''
    }
    if ($work -notmatch '^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=(.*)$') {
      continue
    }
    $key = $Matches[1]
    if ($skip.Contains($key)) {
      continue
    }
    $rawValue = $Matches[2]
    $value = ConvertFrom-DotenvValue -Raw $rawValue
    Set-Item -Path ("Env:" + $key) -Value $value
  }
}

function Resolve-MfoDataDirPolicy {
  param(
    [Parameter(Mandatory = $true)] [string] $RepoRoot,
    [AllowNull()] [string] $InheritedDataDir,
    [AllowNull()] [string] $DotenvDataDir
  )

  $repoPath = (Resolve-Path -LiteralPath $RepoRoot).Path
  $defaultPath = (Join-Path $repoPath 'out')
  $inherited = ([string]$InheritedDataDir)
  $dotenv = ([string]$DotenvDataDir)
  $inheritedTrimmed = $inherited.Trim()
  $dotenvTrimmed = $dotenv.Trim()

  $selectedSource = 'default'
  $selectedValue = ''
  if ($inheritedTrimmed) {
    $selectedSource = 'inherited'
    $selectedValue = $inheritedTrimmed
  } elseif ($dotenvTrimmed) {
    $selectedSource = 'dotenv'
    $selectedValue = $dotenvTrimmed
  }

  $conflictWarn = ''
  if ($inheritedTrimmed -and $dotenvTrimmed -and (-not [string]::Equals($inheritedTrimmed, $dotenvTrimmed, [System.StringComparison]::OrdinalIgnoreCase))) {
    $conflictWarn = (
      'WARN_ENV_CONFLICT=1 key=DATA_DIR inherited=' + $inheritedTrimmed +
      ' dotenv=' + $dotenvTrimmed +
      ' using=' + $selectedSource
    )
  }

  $useDefaultFallback = $false
  $notAbsoluteWarn = ''
  if ($selectedSource -eq 'default') {
    $useDefaultFallback = $true
  } else {
    $candidate = $selectedValue
    $invalid = $false
    if (-not $candidate) {
      $invalid = $true
    } elseif ($candidate -ieq 'out') {
      $invalid = $true
    } elseif (-not [System.IO.Path]::IsPathRooted($candidate)) {
      $invalid = $true
    }

    if ($invalid) {
      $useDefaultFallback = $true
      $selectedSource = 'default'
      $notAbsoluteWarn = ('WARN_DATA_DIR_NOT_ABSOLUTE=1 value=' + $candidate + ' behavior=UNSET_FOR_CHILD')
    }
  }

  $effectivePath = $defaultPath
  if (-not $useDefaultFallback) {
    $effectivePath = [System.IO.Path]::GetFullPath($selectedValue)
  }

  return @{
    EffectivePath = $effectivePath
    Source = $selectedSource
    UseDefaultFallback = [bool]$useDefaultFallback
    ConflictWarnToken = $conflictWarn
    NotAbsoluteWarnToken = $notAbsoluteWarn
  }
}

function Decrypt-DotenvSopsFile {
  param(
    [Parameter(Mandatory = $true)] [string] $SopsExe,
    [Parameter(Mandatory = $true)] [string] $EnvSopsPath
  )

  # Important: never print plaintext to the console; return it to caller only.
  # Also capture stderr separately so failures can be reported without leaking decrypted values.
  $psi = New-Object System.Diagnostics.ProcessStartInfo
  $psi.FileName = $SopsExe
  $psi.UseShellExecute = $false
  $psi.CreateNoWindow = $true
  $psi.RedirectStandardOutput = $true
  $psi.RedirectStandardError = $true

  $quotedPath = '"' + ($EnvSopsPath -replace '"', '""') + '"'
  $psi.Arguments = "--decrypt --input-type dotenv --output-type dotenv $quotedPath"

  $p = New-Object System.Diagnostics.Process
  $p.StartInfo = $psi
  [void]$p.Start()
  $stdout = $p.StandardOutput.ReadToEnd()
  $stderr = $p.StandardError.ReadToEnd()
  $p.WaitForExit()

  if ($p.ExitCode -ne 0) {
    $msg = ''
    if ($stderr) { $msg = $stderr.Trim() }
    if ($msg.Length -gt 300) { $msg = $msg.Substring(0, 300) + '...' }
    $msg = ($msg -replace '[\r\n]+',' ').Trim()
    if (-not $msg) { $msg = 'unknown error' }
    throw ("sops decrypt failed: " + $msg)
  }

  return $stdout.TrimEnd("`r", "`n")
}

