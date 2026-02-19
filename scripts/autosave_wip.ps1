Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'

if ($PSVersionTable.PSVersion.Major -ge 7) {
  $PSNativeCommandUseErrorActionPreference = $false
}

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path

function Fail([string]$Token, [string]$Message) {
  Write-Output ($Token + ' ' + $Message)
  exit 1
}

function Invoke-Git([string[]]$GitArgs) {
  $prevErrorAction = $ErrorActionPreference
  $ErrorActionPreference = 'Continue'
  try {
    $output = & git -C $repoRoot @GitArgs 2>&1
    $code = $LASTEXITCODE
  }
  finally {
    $ErrorActionPreference = $prevErrorAction
  }
  return @{
    Output = @($output)
    ExitCode = [int]$code
  }
}

function Last-Line([object[]]$Lines) {
  $text = (@($Lines) | ForEach-Object { [string]$_ } | Where-Object { $_.Trim().Length -gt 0 } | Select-Object -Last 1)
  if ($null -eq $text) {
    return ''
  }
  return ([string]$text).Trim()
}

$top = Invoke-Git -GitArgs @('rev-parse', '--show-toplevel')
if ([int]$top.ExitCode -ne 0) {
  Fail 'ERR_WIP_AUTOSAVE_REPO' 'git_repo_not_found'
}
$resolvedRoot = Last-Line -Lines $top.Output
if (-not $resolvedRoot) {
  Fail 'ERR_WIP_AUTOSAVE_REPO' 'repo_root_unresolved'
}
$repoRoot = $resolvedRoot

$status = Invoke-Git -GitArgs @('status', '--porcelain')
if ([int]$status.ExitCode -ne 0) {
  Fail 'ERR_WIP_AUTOSAVE_STATUS' 'status_failed'
}
$dirty = @($status.Output | ForEach-Object { [string]$_ } | Where-Object { $_.Trim().Length -gt 0 })
if ($dirty.Count -eq 0) {
  Write-Output 'PASS_WIP_AUTOSAVE_CLEAN'
  exit 0
}

$head = Invoke-Git -GitArgs @('rev-parse', '--abbrev-ref', 'HEAD')
if ([int]$head.ExitCode -ne 0) {
  Fail 'ERR_WIP_AUTOSAVE_BRANCH' 'current_branch_unresolved'
}
$origBranch = Last-Line -Lines $head.Output
if (-not $origBranch) {
  Fail 'ERR_WIP_AUTOSAVE_BRANCH' 'current_branch_empty'
}

$origRef = $origBranch
$origLabel = $origBranch
if ($origBranch -eq 'HEAD') {
  $sha = Invoke-Git -GitArgs @('rev-parse', '--short', 'HEAD')
  if ([int]$sha.ExitCode -ne 0) {
    Fail 'ERR_WIP_AUTOSAVE_BRANCH' 'detached_head_sha_unresolved'
  }
  $origRef = Last-Line -Lines $sha.Output
  if (-not $origRef) {
    Fail 'ERR_WIP_AUTOSAVE_BRANCH' 'detached_head_sha_empty'
  }
  $origLabel = 'detached-' + $origRef
}

$computerName = [string]$env:COMPUTERNAME
if (-not $computerName) {
  $computerName = 'UNKNOWN_HOST'
}
$safeHost = ($computerName -replace '[^A-Za-z0-9._-]', '_')
$stamp = Get-Date -Format 'yyyyMMdd-HHmmss'
$wipBranch = 'wip/' + $safeHost + '/' + $stamp

$createBranch = Invoke-Git -GitArgs @('switch', '-c', $wipBranch)
if ([int]$createBranch.ExitCode -ne 0) {
  $createBranch = Invoke-Git -GitArgs @('checkout', '-b', $wipBranch)
  if ([int]$createBranch.ExitCode -ne 0) {
    Fail 'ERR_WIP_AUTOSAVE_BRANCH_CREATE' ('branch=' + $wipBranch)
  }
}

$add = Invoke-Git -GitArgs @('add', '-A')
if ([int]$add.ExitCode -ne 0) {
  Fail 'ERR_WIP_AUTOSAVE_STAGE' ('branch=' + $wipBranch)
}

$msg = 'wip: autosave ' + $stamp + ' from ' + $origLabel
$commit = Invoke-Git -GitArgs @('commit', '-m', $msg)
if ([int]$commit.ExitCode -ne 0) {
  $commitOut = ((@($commit.Output) | ForEach-Object { [string]$_ }) -join ' ')
  if ($commitOut -match 'nothing to commit') {
    Write-Output 'PASS_WIP_AUTOSAVE_CLEAN'
    if ($origBranch -eq 'HEAD') {
      $returnToOriginal = Invoke-Git -GitArgs @('switch', '--detach', $origRef)
    }
    else {
      $returnToOriginal = Invoke-Git -GitArgs @('switch', $origRef)
    }
    if ([int]$returnToOriginal.ExitCode -ne 0) {
      Fail 'ERR_WIP_AUTOSAVE_RETURN' ('branch=' + $origRef)
    }
    $deleteBranch = Invoke-Git -GitArgs @('branch', '-D', $wipBranch)
    if ([int]$deleteBranch.ExitCode -ne 0) {
      Fail 'ERR_WIP_AUTOSAVE_BRANCH_DELETE' ('branch=' + $wipBranch)
    }
    exit 0
  }
  Fail 'ERR_WIP_AUTOSAVE_COMMIT' ('branch=' + $wipBranch)
}

$push = Invoke-Git -GitArgs @('push', '-u', 'origin', $wipBranch)
if ([int]$push.ExitCode -ne 0) {
  Write-Output ('ERR_WIP_AUTOSAVE_PUSH_FAILED branch=' + $wipBranch)
  exit 1
}

$commitRef = Invoke-Git -GitArgs @('rev-parse', 'HEAD')
if ([int]$commitRef.ExitCode -ne 0) {
  Fail 'ERR_WIP_AUTOSAVE_COMMIT_HASH' ('branch=' + $wipBranch)
}
$commitHash = Last-Line -Lines $commitRef.Output
if (-not $commitHash) {
  Fail 'ERR_WIP_AUTOSAVE_COMMIT_HASH' ('branch=' + $wipBranch)
}

if ($origBranch -eq 'HEAD') {
  $returnToOriginal = Invoke-Git -GitArgs @('switch', '--detach', $origRef)
}
else {
  $returnToOriginal = Invoke-Git -GitArgs @('switch', $origRef)
}
if ([int]$returnToOriginal.ExitCode -ne 0) {
  Fail 'ERR_WIP_AUTOSAVE_RETURN' ('branch=' + $origRef)
}

$reset = Invoke-Git -GitArgs @('reset', '--hard', 'HEAD')
if ([int]$reset.ExitCode -ne 0) {
  Fail 'ERR_WIP_AUTOSAVE_RESET' ('branch=' + $origRef)
}

$finalStatus = Invoke-Git -GitArgs @('status', '--porcelain')
if ([int]$finalStatus.ExitCode -ne 0) {
  Fail 'ERR_WIP_AUTOSAVE_STATUS' 'final_status_failed'
}
$finalDirty = @($finalStatus.Output | ForEach-Object { [string]$_ } | Where-Object { $_.Trim().Length -gt 0 })
if ($finalDirty.Count -gt 0) {
  Fail 'ERR_WIP_AUTOSAVE_NOT_CLEAN' ('remaining=' + $finalDirty.Count)
}

Write-Output ('PASS_WIP_AUTOSAVE_PUSHED branch=' + $wipBranch + ' commit=' + $commitHash)
exit 0
