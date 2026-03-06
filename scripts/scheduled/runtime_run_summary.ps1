Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'

function Resolve-DefaultTaskLogRoot {
  param([string]$RepoRoot)
  $override = ([string]$env:TASK_LOG_ROOT).Trim()
  if ($override) {
    return $override
  }
  $effectiveDataDir = ([string]$env:MFO_DATA_DIR_EFFECTIVE).Trim()
  if ($effectiveDataDir -and [System.IO.Path]::IsPathRooted($effectiveDataDir)) {
    return (Join-Path (Join-Path $effectiveDataDir 'out') 'task_logs')
  }
  return (Join-Path $RepoRoot 'out\task_logs')
}

function Resolve-DefaultRunSummaryRoot {
  param([string]$RepoRoot)
  $override = ([string]$env:RUN_SUMMARY_ROOT).Trim()
  if ($override) {
    return $override
  }
  $effectiveDataDir = ([string]$env:MFO_DATA_DIR_EFFECTIVE).Trim()
  if ($effectiveDataDir -and [System.IO.Path]::IsPathRooted($effectiveDataDir)) {
    return (Join-Path (Join-Path $effectiveDataDir 'out') 'run_summaries')
  }
  return (Join-Path $RepoRoot 'out\run_summaries')
}

function _Write-TextUtf8NoBom {
  param(
    [Parameter(Mandatory = $true)][string]$Path,
    [Parameter(Mandatory = $true)][string]$Content
  )
  $enc = New-Object System.Text.UTF8Encoding($false)
  [System.IO.File]::WriteAllText($Path, $Content, $enc)
}

function _Collect-TokenLists {
  param([string[]]$Lines)
  $pass = New-Object System.Collections.Generic.List[string]
  $err = New-Object System.Collections.Generic.List[string]
  foreach ($raw in @($Lines)) {
    $line = ([string]$raw).Trim()
    if (-not $line) { continue }
    if ($line -match '^(PASS_[A-Z0-9_]+)') {
      $tok = [string]$matches[1]
      if ($pass -notcontains $tok) { [void]$pass.Add($tok) }
    }
    if ($line -match '^(ERR_[A-Z0-9_]+)') {
      $tok = [string]$matches[1]
      if ($err -notcontains $tok) { [void]$err.Add($tok) }
    }
  }
  return @{
    pass = @($pass)
    err = @($err)
  }
}

function _Collect-CountsAndArtifacts {
  param([string[]]$Lines)
  $counts = @{}
  $artifacts = New-Object System.Collections.Generic.List[string]
  foreach ($raw in @($Lines)) {
    $line = ([string]$raw).Trim()
    if (-not $line) { continue }
    if ($line -match '^([A-Z0-9_]+)=(.*)$') {
      $key = [string]$matches[1]
      $val = [string]$matches[2]
      if ($key -match '(COUNT|MATCHED|TOTAL|SENT|CONTACTED|ROWS_|EXIT_CODE|RESULTS_FOUND|DETAILS_FETCHED)') {
        $n = 0
        if ([int]::TryParse(($val -replace '[^0-9-]',''), [ref]$n)) {
          $counts[$key] = $n
        }
      }
      if ($key -match '(_PATH|_CSV|_JSON|_LOG)$') {
        $p = $val.Trim()
        if ($p) {
          if ($artifacts -notcontains $p) { [void]$artifacts.Add($p) }
        }
      }
    }
  }
  return @{
    counts = $counts
    artifacts = @($artifacts)
  }
}

function _Sync-Artifacts {
  param(
    [string]$TaskLogPath,
    [string]$SummaryJsonPath,
    [string]$SummaryTextPath,
    [scriptblock]$EmitLine
  )
  $syncRootRaw = ([string]$env:ARTIFACT_SYNC_DIR).Trim()
  if (-not $syncRootRaw) {
    if ($EmitLine) { & $EmitLine 'RUNTIME_ARTIFACT_SYNC=LOCAL_ONLY reason=ARTIFACT_SYNC_DIR_UNSET' }
    return @()
  }
  if (-not [System.IO.Path]::IsPathRooted($syncRootRaw)) {
    if ($EmitLine) { & $EmitLine 'RUNTIME_ARTIFACT_SYNC=LOCAL_ONLY reason=ARTIFACT_SYNC_DIR_NOT_ABSOLUTE' }
    return @()
  }

  $copied = New-Object System.Collections.Generic.List[string]
  try {
    $syncRoot = (Resolve-Path -LiteralPath $syncRootRaw -ErrorAction SilentlyContinue)
    if (-not $syncRoot) {
      New-Item -ItemType Directory -Force -Path $syncRootRaw | Out-Null
      $syncRoot = Resolve-Path -LiteralPath $syncRootRaw
    }
    $syncBase = $syncRoot.Path
    $syncTask = Join-Path $syncBase 'task_logs'
    $syncSummaries = Join-Path $syncBase 'run_summaries'
    New-Item -ItemType Directory -Force -Path $syncTask | Out-Null
    New-Item -ItemType Directory -Force -Path $syncSummaries | Out-Null

    if ($TaskLogPath -and (Test-Path -LiteralPath $TaskLogPath)) {
      $dst = Join-Path $syncTask ([System.IO.Path]::GetFileName($TaskLogPath))
      Copy-Item -LiteralPath $TaskLogPath -Destination $dst -Force
      [void]$copied.Add($dst)
    }
    if ($SummaryJsonPath -and (Test-Path -LiteralPath $SummaryJsonPath)) {
      $dst = Join-Path $syncSummaries ([System.IO.Path]::GetFileName($SummaryJsonPath))
      Copy-Item -LiteralPath $SummaryJsonPath -Destination $dst -Force
      [void]$copied.Add($dst)
    }
    if ($SummaryTextPath -and (Test-Path -LiteralPath $SummaryTextPath)) {
      $dst = Join-Path $syncSummaries ([System.IO.Path]::GetFileName($SummaryTextPath))
      Copy-Item -LiteralPath $SummaryTextPath -Destination $dst -Force
      [void]$copied.Add($dst)
    }
    if ($EmitLine) { & $EmitLine ('RUNTIME_ARTIFACT_SYNC=COPIED root=' + $syncBase + ' files=' + $copied.Count) }
  } catch {
    if ($EmitLine) { & $EmitLine ('RUNTIME_ARTIFACT_SYNC=LOCAL_ONLY reason=SYNC_FAILED detail=' + $_.Exception.GetType().Name) }
  }
  return @($copied)
}

function Write-RuntimeRunSummary {
  param(
    [Parameter(Mandatory = $true)][string]$RepoRoot,
    [Parameter(Mandatory = $true)][string]$WrapperName,
    [Parameter(Mandatory = $true)][string]$CommandLine,
    [string]$Mode = 'scheduled',
    [string]$Intent = 'read',
    [bool]$DryRun = $false,
    [int]$ExitCode = 0,
    [datetime]$StartLocal,
    [datetime]$StartUtc,
    [string]$TaskLogPath = '',
    [string]$TaskLogRoot = '',
    [string]$RunSummaryRoot = '',
    [hashtable]$Fingerprint = @{},
    [string[]]$ExtraArtifactPaths = @(),
    [scriptblock]$EmitLine = $null
  )

  $startLocalDt = if ($StartLocal) { $StartLocal } else { Get-Date }
  $startUtcDt = if ($StartUtc) { $StartUtc } else { [datetime]::UtcNow }
  $endLocalDt = Get-Date
  $endUtcDt = [datetime]::UtcNow

  $resolvedTaskLogRoot = if ($TaskLogRoot) { $TaskLogRoot } else { Resolve-DefaultTaskLogRoot -RepoRoot $RepoRoot }
  $resolvedSummaryRoot = if ($RunSummaryRoot) { $RunSummaryRoot } else { Resolve-DefaultRunSummaryRoot -RepoRoot $RepoRoot }

  New-Item -ItemType Directory -Force -Path $resolvedTaskLogRoot | Out-Null
  New-Item -ItemType Directory -Force -Path $resolvedSummaryRoot | Out-Null

  $logLines = @()
  if ($TaskLogPath -and (Test-Path -LiteralPath $TaskLogPath)) {
    $logLines = @(Get-Content -LiteralPath $TaskLogPath -ErrorAction SilentlyContinue)
  }

  $tokens = _Collect-TokenLists -Lines $logLines
  $parsed = _Collect-CountsAndArtifacts -Lines $logLines

  $stamp = $startUtcDt.ToString('yyyyMMdd_HHmmss')
  $baseName = ($WrapperName + '_' + $stamp)
  $summaryJsonPath = Join-Path $resolvedSummaryRoot ($baseName + '.summary.json')
  $summaryTextPath = Join-Path $resolvedSummaryRoot ($baseName + '.summary.txt')

  $fingerprintBlock = [ordered]@{}
  foreach ($k in @('RUNTIME_HOSTNAME','RUNTIME_USERNAME','RUNTIME_ROLE','RUNTIME_CANONICAL_HOSTNAME','RUNTIME_CANONICAL_HOST_MATCH','RUNTIME_TRUSTED_SCHEDULED','RUNTIME_DATA_DIR','RUNTIME_DATA_DIR_SOURCE','RUNTIME_REPO_ROOT','RUNTIME_DB_OSHA','RUNTIME_DB_CRM','RUNTIME_DB_CRM_LIGHT','RUNTIME_TIMEZONE','RUNTIME_GIT_SHA','MFO_RUNTIME_MODE','MFO_TRUSTED_SCHEDULED')) {
    if ($Fingerprint.ContainsKey($k)) {
      $fingerprintBlock[$k] = [string]$Fingerprint[$k]
    } elseif (Test-Path -LiteralPath ('Env:' + $k)) {
      $fingerprintBlock[$k] = [string](Get-Item -Path ('Env:' + $k)).Value
    }
  }

  $artifactPaths = New-Object System.Collections.Generic.List[string]
  if ($TaskLogPath) { [void]$artifactPaths.Add([string]$TaskLogPath) }
  foreach ($artifact in @($parsed.artifacts)) {
    $value = ([string]$artifact).Trim()
    if ($value) { [void]$artifactPaths.Add($value) }
  }
  foreach ($artifact in @($ExtraArtifactPaths)) {
    $value = ([string]$artifact).Trim()
    if ($value) { [void]$artifactPaths.Add($value) }
  }

  $summary = [ordered]@{
    schema = 'runtime_run_summary_v1'
    wrapper = $WrapperName
    command = $CommandLine
    mode = $Mode
    intent = $Intent
    dry_run = [bool]$DryRun
    start_local = $startLocalDt.ToString('o')
    start_utc = $startUtcDt.ToString('o')
    end_local = $endLocalDt.ToString('o')
    end_utc = $endUtcDt.ToString('o')
    duration_seconds = [math]::Round(($endUtcDt - $startUtcDt).TotalSeconds, 3)
    exit_code = [int]$ExitCode
    fingerprint = $fingerprintBlock
    tokens = [ordered]@{
      pass = @($tokens.pass | Sort-Object)
      err = @($tokens.err | Sort-Object)
    }
    counts = $parsed.counts
    artifacts = [ordered]@{
      task_log = $TaskLogPath
      summary_json = $summaryJsonPath
      summary_text = $summaryTextPath
      generated = @($artifactPaths | Sort-Object -Unique)
      synced = @()
    }
  }

  _Write-TextUtf8NoBom -Path $summaryJsonPath -Content (($summary | ConvertTo-Json -Depth 10) + "`n")

  $textLines = @(
    ('RUNTIME_RUN_SUMMARY wrapper=' + $WrapperName + ' exit_code=' + [int]$ExitCode + ' mode=' + $Mode + ' intent=' + $Intent + ' dry_run=' + ($(if($DryRun){'YES'}else{'NO'}))),
    ('RUNTIME_RUN_SUMMARY_START_LOCAL=' + $summary.start_local),
    ('RUNTIME_RUN_SUMMARY_END_LOCAL=' + $summary.end_local),
    ('RUNTIME_RUN_SUMMARY_DURATION_SECONDS=' + $summary.duration_seconds),
    ('RUNTIME_RUN_SUMMARY_TASK_LOG=' + $TaskLogPath),
    ('RUNTIME_RUN_SUMMARY_JSON=' + $summaryJsonPath),
    ('RUNTIME_RUN_SUMMARY_TEXT=' + $summaryTextPath),
    ('RUNTIME_RUN_SUMMARY_PASS_TOKENS=' + (@($tokens.pass | Sort-Object) -join ',')),
    ('RUNTIME_RUN_SUMMARY_ERR_TOKENS=' + (@($tokens.err | Sort-Object) -join ','))
  )
  _Write-TextUtf8NoBom -Path $summaryTextPath -Content ($textLines -join "`r`n")

  $synced = _Sync-Artifacts -TaskLogPath $TaskLogPath -SummaryJsonPath $summaryJsonPath -SummaryTextPath $summaryTextPath -EmitLine $EmitLine
  $summary.artifacts.synced = @($synced)
  _Write-TextUtf8NoBom -Path $summaryJsonPath -Content (($summary | ConvertTo-Json -Depth 10) + "`n")

  if ($EmitLine) {
    & $EmitLine ('RUN_SUMMARY_JSON_PATH=' + $summaryJsonPath)
    & $EmitLine ('RUN_SUMMARY_TEXT_PATH=' + $summaryTextPath)
  } else {
    Write-Output ('RUN_SUMMARY_JSON_PATH=' + $summaryJsonPath)
    Write-Output ('RUN_SUMMARY_TEXT_PATH=' + $summaryTextPath)
  }

  return @{
    SummaryJsonPath = $summaryJsonPath
    SummaryTextPath = $summaryTextPath
    Summary = $summary
  }
}
