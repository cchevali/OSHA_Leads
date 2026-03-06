param(
  [switch]$PrintConfig,
  [switch]$DryRun,
  [switch]$Compress
)
# CLI compatibility notes:
#   --print-config maps to -PrintConfig
#   --dry-run maps to -DryRun

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
. (Join-Path $PSScriptRoot "runtime_guard.ps1")

function Write-BackupLine([string]$Line) {
  Write-Output ([string]$Line)
}

function Resolve-EffectiveDataDir([string]$RepoRoot) {
  $raw = ([string]$env:DATA_DIR).Trim()
  if ($raw -and [System.IO.Path]::IsPathRooted($raw) -and ($raw -ine 'out')) {
    return [System.IO.Path]::GetFullPath($raw)
  }
  return (Join-Path $RepoRoot 'out')
}

function Resolve-BackupRoot([string]$RepoRoot) {
  $override = ([string]$env:BACKUP_ROOT).Trim()
  if ($override) {
    return $override
  }
  return (Join-Path $RepoRoot 'out\backups')
}

function Sync-BackupArtifacts([string[]]$Paths) {
  $syncRootRaw = ([string]$env:ARTIFACT_SYNC_DIR).Trim()
  if (-not $syncRootRaw) {
    Write-BackupLine 'BACKUP_ARTIFACT_SYNC=LOCAL_ONLY reason=ARTIFACT_SYNC_DIR_UNSET'
    return @()
  }
  if (-not [System.IO.Path]::IsPathRooted($syncRootRaw)) {
    Write-BackupLine 'BACKUP_ARTIFACT_SYNC=LOCAL_ONLY reason=ARTIFACT_SYNC_DIR_NOT_ABSOLUTE'
    return @()
  }
  $copied = New-Object System.Collections.Generic.List[string]
  try {
    New-Item -ItemType Directory -Force -Path $syncRootRaw | Out-Null
    $backupDir = Join-Path $syncRootRaw 'backups'
    New-Item -ItemType Directory -Force -Path $backupDir | Out-Null
    foreach ($p in @($Paths)) {
      $raw = ([string]$p).Trim()
      if (-not $raw) { continue }
      if (-not (Test-Path -LiteralPath $raw)) { continue }
      $dst = Join-Path $backupDir ([System.IO.Path]::GetFileName($raw))
      Copy-Item -LiteralPath $raw -Destination $dst -Force
      [void]$copied.Add($dst)
    }
    Write-BackupLine ('BACKUP_ARTIFACT_SYNC=COPIED root=' + $backupDir + ' files=' + $copied.Count)
  } catch {
    Write-BackupLine ('BACKUP_ARTIFACT_SYNC=LOCAL_ONLY reason=SYNC_FAILED detail=' + $_.Exception.GetType().Name)
  }
  return @($copied)
}

$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$dataDir = Resolve-EffectiveDataDir -RepoRoot $repoRoot
$backupRoot = Resolve-BackupRoot -RepoRoot $repoRoot
$backupRunDir = Join-Path $backupRoot ("snapshot_" + $timestamp)
$manifestPath = Join-Path $backupRoot ("backup_manifest_" + $timestamp + ".json")
$manifestTxtPath = Join-Path $backupRoot ("backup_manifest_" + $timestamp + ".txt")

$targets = @(
  @{ Label = 'osha'; Path = (Join-Path $repoRoot 'data\osha.sqlite') },
  @{ Label = 'crm'; Path = (Join-Path $dataDir 'crm.sqlite') },
  @{ Label = 'crm_light'; Path = (Join-Path $dataDir 'crm_light.sqlite') }
)

Write-BackupLine ('BACKUP_REPO_ROOT=' + $repoRoot)
Write-BackupLine ('BACKUP_DATA_DIR=' + $dataDir)
Write-BackupLine ('BACKUP_ROOT=' + $backupRoot)
Write-BackupLine ('BACKUP_RUN_DIR=' + $backupRunDir)
Write-BackupLine ('BACKUP_DRY_RUN=' + $(if ($DryRun) { '1' } else { '0' }))

foreach ($t in $targets) {
  $exists = if (Test-Path -LiteralPath $t.Path) { 'YES' } else { 'NO' }
  Write-BackupLine ('BACKUP_TARGET label=' + $t.Label + ' path=' + $t.Path + ' exists=' + $exists)
}

if ($PrintConfig) {
  Write-BackupLine 'PASS_BACKUP_RUNTIME_STATE_PRINT_CONFIG'
  exit 0
}

$preflight = Invoke-RuntimePreflight `
  -RepoRoot $repoRoot `
  -Mode 'scheduled' `
  -Intent 'write' `
  -DryRun:$DryRun `
  -EmitLine ${function:Write-BackupLine}
if (-not [bool]$preflight.Ok) {
  exit 1
}

$manifest = [ordered]@{
  schema = 'runtime_backup_manifest_v1'
  timestamp_local = (Get-Date).ToString('o')
  timestamp_utc = [datetime]::UtcNow.ToString('o')
  dry_run = [bool]$DryRun
  repo_root = $repoRoot
  data_dir = $dataDir
  backup_root = $backupRoot
  backup_run_dir = $backupRunDir
  targets = @()
  compressed_zip = ''
  synced_artifacts = @()
}

if (-not $DryRun) {
  New-Item -ItemType Directory -Force -Path $backupRoot | Out-Null
  New-Item -ItemType Directory -Force -Path $backupRunDir | Out-Null
}

$exitCode = 0
foreach ($target in $targets) {
  $entry = [ordered]@{
    label = [string]$target.Label
    source_path = [string]$target.Path
    exists = [bool](Test-Path -LiteralPath $target.Path)
    status = ''
    snapshot_path = ''
    size_bytes = 0
    sha256 = ''
  }

  if (-not $entry.exists) {
    $entry.status = 'SOURCE_MISSING'
    Write-BackupLine ('BACKUP_SOURCE_MISSING label=' + $entry.label + ' path=' + $entry.source_path)
    $manifest.targets += $entry
    continue
  }

  if ($DryRun) {
    $entry.status = 'DRY_RUN'
    $entry.snapshot_path = (Join-Path $backupRunDir ($entry.label + '_' + $timestamp + '.sqlite'))
    Write-BackupLine ('BACKUP_DRY_RUN_TARGET label=' + $entry.label + ' would_snapshot=' + $entry.snapshot_path)
    $manifest.targets += $entry
    continue
  }

  $cmd = @(
    '-3',
    (Join-Path $repoRoot 'tools\sqlite_snapshot_backup.py'),
    '--source',
    $entry.source_path,
    '--output-dir',
    $backupRunDir,
    '--label',
    $entry.label
  )
  $lines = & py @cmd 2>&1
  $code = [int]$LASTEXITCODE
  foreach ($line in @($lines)) {
    Write-BackupLine ([string]$line)
    $text = [string]$line
    if ($text -match '^SQLITE_SNAPSHOT_RESULT=(.+)$') {
      try {
        $payload = $matches[1] | ConvertFrom-Json -ErrorAction Stop
        $entry.snapshot_path = [string]($payload.snapshot_path)
        $entry.size_bytes = [int64]($payload.size_bytes)
        $entry.sha256 = [string]($payload.sha256)
      } catch {
      }
    }
  }

  if ($code -ne 0) {
    $entry.status = 'FAILED'
    $exitCode = 1
  } else {
    $entry.status = 'OK'
    if ($entry.snapshot_path -and (Test-Path -LiteralPath $entry.snapshot_path)) {
      $entry.size_bytes = [int64](Get-Item -LiteralPath $entry.snapshot_path).Length
    }
  }
  $manifest.targets += $entry
}

if ((-not $DryRun) -and $Compress) {
  try {
    $zipPath = Join-Path $backupRoot ("snapshot_" + $timestamp + ".zip")
    if (Test-Path -LiteralPath $zipPath) {
      Remove-Item -LiteralPath $zipPath -Force
    }
    Compress-Archive -Path (Join-Path $backupRunDir '*') -DestinationPath $zipPath
    $manifest.compressed_zip = $zipPath
    Write-BackupLine ('BACKUP_COMPRESSED_ZIP=' + $zipPath)
  } catch {
    $exitCode = 1
    Write-BackupLine ('ERR_BACKUP_COMPRESS_FAILED detail=' + $_.Exception.GetType().Name)
  }
}

New-Item -ItemType Directory -Force -Path $backupRoot | Out-Null
($manifest | ConvertTo-Json -Depth 10) + "`n" | Set-Content -Path $manifestPath -Encoding UTF8
$txt = @(
  ('BACKUP_MANIFEST schema=' + $manifest.schema + ' dry_run=' + $(if ($DryRun) { 'YES' } else { 'NO' }) + ' target_count=' + $manifest.targets.Count),
  ('BACKUP_MANIFEST_JSON=' + $manifestPath),
  ('BACKUP_MANIFEST_EXIT_CODE=' + $exitCode)
)
$txt -join "`r`n" | Set-Content -Path $manifestTxtPath -Encoding UTF8

$synced = Sync-BackupArtifacts -Paths @(
  $manifestPath,
  $manifestTxtPath,
  $manifest.compressed_zip
)
$manifest.synced_artifacts = @($synced)
($manifest | ConvertTo-Json -Depth 10) + "`n" | Set-Content -Path $manifestPath -Encoding UTF8

Write-BackupLine ('BACKUP_MANIFEST_PATH=' + $manifestPath)
Write-BackupLine ('BACKUP_MANIFEST_TEXT_PATH=' + $manifestTxtPath)
if ($exitCode -eq 0) {
  Write-BackupLine 'PASS_BACKUP_RUNTIME_STATE'
  exit 0
}
Write-BackupLine 'ERR_BACKUP_RUNTIME_STATE_FAILED'
exit 1
