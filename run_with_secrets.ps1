Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'
if ($PSVersionTable.PSVersion.Major -ge 7) {
  $PSNativeCommandUseErrorActionPreference = $false
}

function Invoke-ContextPackSoftCheck {
  param([string]$RepoRoot)

  if ((([string]$env:MFO_CONTEXT_PACK_SOFT_CHECK_DONE).Trim() -eq '1')) {
    return
  }
  $env:MFO_CONTEXT_PACK_SOFT_CHECK_DONE = '1'

  $contextPackScript = Join-Path $RepoRoot 'tools\project_context_pack.py'
  if (-not (Test-Path -LiteralPath $contextPackScript)) {
    Write-Output "WARN_CONTEXT_PACK_SCRIPT_MISSING tools/project_context_pack.py"
    return
  }

  $python = Resolve-ContextPackPythonCommand
  if (-not $python) {
    Write-Output "WARN_CONTEXT_PACK_CHECK_FAILED runner_not_found"
    return
  }

  $lines = @()
  $exitCode = 0
  try {
    $pythonArgs = @($python.ArgsPrefix) + @($contextPackScript, '--check', '--soft')
    $output = & $python.Exe @pythonArgs 2>&1
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

function Resolve-ContextPackPythonExePath {
  $forced = ([string]$env:PYTHON_EXE).Trim()
  if ($forced -and (Test-Path -LiteralPath $forced)) {
    try {
      return (Resolve-Path -LiteralPath $forced).Path
    }
    catch {
      return $forced
    }
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
  if ([string]$env:ProgramFiles) {
    [void]$candidates.Add((Join-Path $env:ProgramFiles 'Python313\python.exe'))
    [void]$candidates.Add((Join-Path $env:ProgramFiles 'Python312\python.exe'))
    [void]$candidates.Add((Join-Path $env:ProgramFiles 'Python311\python.exe'))
    [void]$candidates.Add((Join-Path $env:ProgramFiles 'Python310\python.exe'))
    [void]$candidates.Add((Join-Path $env:ProgramFiles 'Python\python.exe'))
  }
  if ([string]${env:ProgramFiles(x86)}) {
    [void]$candidates.Add((Join-Path ${env:ProgramFiles(x86)} 'Python313\python.exe'))
    [void]$candidates.Add((Join-Path ${env:ProgramFiles(x86)} 'Python312\python.exe'))
    [void]$candidates.Add((Join-Path ${env:ProgramFiles(x86)} 'Python311\python.exe'))
  }

  foreach ($candidate in @($candidates)) {
    if (-not $candidate) {
      continue
    }
    if (Test-Path -LiteralPath $candidate) {
      try {
        return (Resolve-Path -LiteralPath $candidate).Path
      }
      catch {
        return $candidate
      }
    }
  }

  $pythonCmd = Get-Command -Name python -ErrorAction SilentlyContinue
  if ($pythonCmd -and $pythonCmd.Source -and (Test-Path -LiteralPath $pythonCmd.Source)) {
    try {
      return (Resolve-Path -LiteralPath $pythonCmd.Source).Path
    }
    catch {
      return $pythonCmd.Source
    }
  }

  foreach ($root in @('C:\Users\lever', 'C:\Users\Public')) {
    $candidate = Join-Path $root 'AppData\Local\Programs\Python\Python313\python.exe'
    if (Test-Path -LiteralPath $candidate) {
      try {
        return (Resolve-Path -LiteralPath $candidate).Path
      }
      catch {
        return $candidate
      }
    }
  }

  return $null
}

function Resolve-ContextPackPythonCommand {
  $resolvedExe = Resolve-ContextPackPythonExePath
  if ($resolvedExe) {
    return @{
      Exe = $resolvedExe
      ArgsPrefix = @()
    }
  }

  if (Get-Command -Name py -ErrorAction SilentlyContinue) {
    return @{
      Exe = 'py'
      ArgsPrefix = @('-3')
    }
  }

  return $null
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

$forwardArgs = @($args)
if ($forwardArgs.Count -ge 1 -and $forwardArgs[0] -eq '--') {
  if ($forwardArgs.Count -ge 2) {
    $forwardArgs = $forwardArgs[1..($forwardArgs.Count - 1)]
  } else {
    $forwardArgs = @()
  }
}

if ($forwardArgs -contains '--diagnostics') {
  Write-Output ("DIAG: wrapper_path=" + $wrapperResolved)
  Write-Output ("DIAG: target_path=" + $targetResolved)
}

& $targetPath @forwardArgs
$exitCode = $LASTEXITCODE
exit $exitCode
