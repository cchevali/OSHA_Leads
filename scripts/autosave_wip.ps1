Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'

if ($PSVersionTable.PSVersion.Major -ge 7) {
  $PSNativeCommandUseErrorActionPreference = $false
}

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$worktreePath = ''

function Fail([string]$Token, [string]$Message) {
  Write-Output ($Token + ' ' + $Message)
  exit 1
}

function Invoke-GitRepo([string[]]$GitArgs) {
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

function Invoke-GitWorktree([string[]]$GitArgs) {
  $prevErrorAction = $ErrorActionPreference
  $ErrorActionPreference = 'Continue'
  try {
    $output = & git -C $worktreePath @GitArgs 2>&1
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

function Ensure-Directory([string]$Path) {
  if (-not (Test-Path -LiteralPath $Path)) {
    New-Item -ItemType Directory -Force -Path $Path | Out-Null
  }
}

function Ensure-AutosaveBranch([string]$BranchName) {
  $localExists = Invoke-GitRepo -GitArgs @('show-ref', '--verify', '--quiet', ('refs/heads/' + $BranchName))
  if ([int]$localExists.ExitCode -eq 0) {
    return
  }

  $fetch = Invoke-GitRepo -GitArgs @('fetch', 'origin', ('refs/heads/' + $BranchName + ':refs/heads/' + $BranchName))
  if ([int]$fetch.ExitCode -eq 0) {
    return
  }

  $create = Invoke-GitRepo -GitArgs @('branch', $BranchName, 'HEAD')
  if ([int]$create.ExitCode -ne 0) {
    Fail 'ERR_WIP_AUTOSAVE_BRANCH_CREATE' ('branch=' + $BranchName)
  }
}

function Ensure-AutosaveWorktree([string]$BranchName) {
  if (Test-Path -LiteralPath $worktreePath) {
    $probe = Invoke-GitWorktree -GitArgs @('rev-parse', '--is-inside-work-tree')
    if ([int]$probe.ExitCode -ne 0) {
      Fail 'ERR_WIP_AUTOSAVE_WORKTREE' ('invalid_worktree=' + $worktreePath)
    }
    $checkout = Invoke-GitWorktree -GitArgs @('checkout', $BranchName)
    if ([int]$checkout.ExitCode -ne 0) {
      Fail 'ERR_WIP_AUTOSAVE_WORKTREE' ('checkout_failed branch=' + $BranchName)
    }
    return
  }

  $add = Invoke-GitRepo -GitArgs @('worktree', 'add', '--force', $worktreePath, $BranchName)
  if ([int]$add.ExitCode -ne 0) {
    Fail 'ERR_WIP_AUTOSAVE_WORKTREE' ('create_failed path=' + $worktreePath)
  }
}

function Sync-RepoToWorktree([string]$SourceRoot, [string]$DestinationRoot, [string]$LockDir, [string]$WorktreeDir) {
  $gitDir = Join-Path $SourceRoot '.git'
  $args = @(
    $SourceRoot,
    $DestinationRoot,
    '/MIR',
    '/R:1',
    '/W:1',
    '/NFL',
    '/NDL',
    '/NJH',
    '/NJS',
    '/NP',
    '/XD',
    $gitDir,
    $LockDir,
    $WorktreeDir,
    '/XF',
    '.git'
  )

  $prevErrorAction = $ErrorActionPreference
  $ErrorActionPreference = 'Continue'
  try {
    & robocopy.exe @args 2>&1 | Out-Null
    $code = $LASTEXITCODE
  }
  finally {
    $ErrorActionPreference = $prevErrorAction
  }

  if ([int]$code -ge 8) {
    Fail 'ERR_WIP_AUTOSAVE_SYNC' ('robocopy_exit=' + [int]$code)
  }
}

$top = Invoke-GitRepo -GitArgs @('rev-parse', '--show-toplevel')
if ([int]$top.ExitCode -ne 0) {
  Fail 'ERR_WIP_AUTOSAVE_REPO' 'git_repo_not_found'
}
$resolvedRoot = Last-Line -Lines $top.Output
if (-not $resolvedRoot) {
  Fail 'ERR_WIP_AUTOSAVE_REPO' 'repo_root_unresolved'
}
$repoRoot = $resolvedRoot

$computerName = [string]$env:COMPUTERNAME
if (-not $computerName) {
  $computerName = 'UNKNOWN_HOST'
}
$safeHost = ($computerName -replace '[^A-Za-z0-9._-]', '_')
$stamp = Get-Date -Format 'yyyyMMdd-HHmmss'
$autosaveBranch = 'wip/autosave/' + $safeHost

$localDir = Join-Path $repoRoot '.local'
$lockDir = Join-Path $localDir 'locks'
$lockPath = Join-Path $lockDir 'wip_autosave.lock'
$worktreePath = Join-Path $localDir 'wip_autosave_worktree'

Ensure-Directory -Path $localDir
Ensure-Directory -Path $lockDir

$lockHandle = $null
try {
  $lockHandle = [System.IO.File]::Open($lockPath, [System.IO.FileMode]::OpenOrCreate, [System.IO.FileAccess]::ReadWrite, [System.IO.FileShare]::None)
}
catch {
  Fail 'ERR_WIP_AUTOSAVE_LOCKED' ('lock=' + $lockPath)
}

try {
  $status = Invoke-GitRepo -GitArgs @('status', '--porcelain')
  if ([int]$status.ExitCode -ne 0) {
    Fail 'ERR_WIP_AUTOSAVE_STATUS' 'status_failed'
  }

  $dirty = @($status.Output | ForEach-Object { [string]$_ } | Where-Object { $_.Trim().Length -gt 0 })
  if ($dirty.Count -eq 0) {
    Write-Output 'PASS_WIP_AUTOSAVE_CLEAN'
    exit 0
  }

  $head = Invoke-GitRepo -GitArgs @('rev-parse', '--abbrev-ref', 'HEAD')
  if ([int]$head.ExitCode -ne 0) {
    Fail 'ERR_WIP_AUTOSAVE_BRANCH' 'current_branch_unresolved'
  }
  $origBranch = Last-Line -Lines $head.Output
  if (-not $origBranch) {
    Fail 'ERR_WIP_AUTOSAVE_BRANCH' 'current_branch_empty'
  }

  $origLabel = $origBranch
  if ($origBranch -eq 'HEAD') {
    $sha = Invoke-GitRepo -GitArgs @('rev-parse', '--short', 'HEAD')
    if ([int]$sha.ExitCode -ne 0) {
      Fail 'ERR_WIP_AUTOSAVE_BRANCH' 'detached_head_sha_unresolved'
    }
    $origRef = Last-Line -Lines $sha.Output
    if (-not $origRef) {
      Fail 'ERR_WIP_AUTOSAVE_BRANCH' 'detached_head_sha_empty'
    }
    $origLabel = 'detached-' + $origRef
  }

  Ensure-AutosaveBranch -BranchName $autosaveBranch
  Ensure-AutosaveWorktree -BranchName $autosaveBranch

  $resetWorktree = Invoke-GitWorktree -GitArgs @('reset', '--hard', 'HEAD')
  if ([int]$resetWorktree.ExitCode -ne 0) {
    Fail 'ERR_WIP_AUTOSAVE_WORKTREE' ('reset_failed branch=' + $autosaveBranch)
  }

  $cleanWorktree = Invoke-GitWorktree -GitArgs @('clean', '-fd')
  if ([int]$cleanWorktree.ExitCode -ne 0) {
    Fail 'ERR_WIP_AUTOSAVE_WORKTREE' ('clean_failed branch=' + $autosaveBranch)
  }

  Sync-RepoToWorktree -SourceRoot $repoRoot -DestinationRoot $worktreePath -LockDir $lockDir -WorktreeDir $worktreePath

  $add = Invoke-GitWorktree -GitArgs @('add', '-A')
  if ([int]$add.ExitCode -ne 0) {
    Fail 'ERR_WIP_AUTOSAVE_STAGE' ('branch=' + $autosaveBranch)
  }

  $worktreeStatus = Invoke-GitWorktree -GitArgs @('status', '--porcelain')
  if ([int]$worktreeStatus.ExitCode -ne 0) {
    Fail 'ERR_WIP_AUTOSAVE_STATUS' 'worktree_status_failed'
  }
  $worktreeDirty = @($worktreeStatus.Output | ForEach-Object { [string]$_ } | Where-Object { $_.Trim().Length -gt 0 })
  if ($worktreeDirty.Count -eq 0) {
    Write-Output 'PASS_WIP_AUTOSAVE_CLEAN'
    exit 0
  }

  $msg = 'wip: autosave ' + $stamp + ' from ' + $origLabel
  $commit = Invoke-GitWorktree -GitArgs @('commit', '-m', $msg)
  if ([int]$commit.ExitCode -ne 0) {
    $commitOut = ((@($commit.Output) | ForEach-Object { [string]$_ }) -join ' ')
    if ($commitOut -match 'nothing to commit') {
      Write-Output 'PASS_WIP_AUTOSAVE_CLEAN'
      exit 0
    }
    Fail 'ERR_WIP_AUTOSAVE_COMMIT' ('branch=' + $autosaveBranch)
  }

  $push = Invoke-GitWorktree -GitArgs @('push', '-u', 'origin', $autosaveBranch)
  if ([int]$push.ExitCode -ne 0) {
    Write-Output ('ERR_WIP_AUTOSAVE_PUSH_FAILED branch=' + $autosaveBranch)
    exit 1
  }

  $commitRef = Invoke-GitWorktree -GitArgs @('rev-parse', 'HEAD')
  if ([int]$commitRef.ExitCode -ne 0) {
    Fail 'ERR_WIP_AUTOSAVE_COMMIT_HASH' ('branch=' + $autosaveBranch)
  }
  $commitHash = Last-Line -Lines $commitRef.Output
  if (-not $commitHash) {
    Fail 'ERR_WIP_AUTOSAVE_COMMIT_HASH' ('branch=' + $autosaveBranch)
  }

  Write-Output ('PASS_WIP_AUTOSAVE_PUSHED branch=' + $autosaveBranch + ' commit=' + $commitHash)
  exit 0
}
finally {
  if ($null -ne $lockHandle) {
    $lockHandle.Dispose()
  }
}
