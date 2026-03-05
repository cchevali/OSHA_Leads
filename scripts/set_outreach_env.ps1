param(
  [Nullable[int]] $OutreachDailyLimit = $null,
  [string] $OutreachStates = '',
  [string] $OshaSmokeTo = '',
  [Nullable[int]] $OutreachSuppressionMaxAgeHours = $null,
  [Nullable[int]] $OutreachFallbackOnEmptyState = $null,
  [Nullable[int]] $OutreachSkipRoleInboxes = $null,
  [Nullable[int]] $ProspectAutoGrowEnabled = $null,
  [string] $ProspectAutoGrowStates = '',
  [string] $ProspectAutoGrowSources = '',
  [Nullable[int]] $ProspectAutoGrowBacklogTarget = $null,
  [Nullable[int]] $ProspectAutoGrowMaxFetchPagesPerRun = $null,
  [Nullable[int]] $ProspectAutoGrowHttpSleepMs = $null,
  [string] $ProspectAutoGrowStateLicTxLicenseTypes = '',
  [Nullable[int]] $ProspectEnrichDomainEnabled = $null,
  [Nullable[int]] $ProspectEnrichHunterEnabled = $null,
  [string] $HunterApiKey = '',
  [string] $ApolloApiKey = '',
  [Nullable[int]] $ApolloEnrichEnabled = $null,
  [Nullable[int]] $ApolloEnrichMaxPerRun = $null,
  [string] $ApolloPersonTitles = '',
  [string] $ApolloPersonLocationsMode = '',
  [Nullable[int]] $TrialSendsLimitDefault = $null,
  [string] $TrialExpiredBehaviorDefault = '',
  [string] $TrialConversionUrl = '',
  [Nullable[int]] $AiTriageEnabled = $null,
  [string] $AiTriageOpenAiModel = '',
  [Nullable[int]] $SignalFreshnessMaxDays = $null,
  [string] $HudApiToken = '',
  [string] $StripePriceIdCore = '',
  [string] $StripePriceIdMulti = '',
  [string] $StripePriceIdPilot = '',
  [string] $WebStripeWebhookSecret = '',
  [string] $DataDir = '',
  [string] $ProspectDiscoveryInput = '',
  [string] $BounceImapHost = '',
  [Nullable[int]] $BounceImapPort = $null,
  [string] $BounceImapUser = '',
  [string] $BounceImapPass = '',
  [string] $BounceImapFolder = '',
  [Nullable[int]] $BounceImapSinceHours = $null,
  [Nullable[int]] $BounceImapMaxMessages = $null,
  [string] $TaskSchedUser = '',
  [string] $TaskSchedPassword = '',
  [string] $RuntimeRole = '',
  [string] $CanonicalHostname = '',
  [string] $ArtifactSyncDir = '',
  [string] $TaskLogRoot = '',
  [string] $RunSummaryRoot = '',
  [switch] $PrintConfig
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'

. (Join-Path $PSScriptRoot 'secrets_tooling.ps1')

$ERR_ENV_SOPS_STAGED = 'ERR_ENV_SOPS_STAGED'
$ERR_SET_OUTREACH_ENV_TOOLING = 'ERR_SET_OUTREACH_ENV_TOOLING'
$ERR_SET_OUTREACH_ENV_DECRYPT = 'ERR_SET_OUTREACH_ENV_DECRYPT'
$ERR_SET_OUTREACH_ENV_ARGS = 'ERR_SET_OUTREACH_ENV_ARGS'
$ERR_SET_OUTREACH_ENV_ENCRYPT = 'ERR_SET_OUTREACH_ENV_ENCRYPT'
$ERR_SET_OUTREACH_ENV_WRITE = 'ERR_SET_OUTREACH_ENV_WRITE'
$ERR_SET_OUTREACH_ENV_VERIFY = 'ERR_SET_OUTREACH_ENV_VERIFY'
$ERR_SET_OUTREACH_ENV_PRINT_CONFIG = 'ERR_SET_OUTREACH_ENV_PRINT_CONFIG'
$ERR_SET_OUTREACH_ENV_PRINT_CONFIG_MISSING_KEYS = 'ERR_SET_OUTREACH_ENV_PRINT_CONFIG_MISSING_KEYS'

$PASS_SET_OUTREACH_ENV_APPLY = 'PASS_SET_OUTREACH_ENV_APPLY'
$PASS_SET_OUTREACH_ENV_VERIFY = 'PASS_SET_OUTREACH_ENV_VERIFY'
$PASS_SET_OUTREACH_ENV_PRINT_CONFIG = 'PASS_SET_OUTREACH_ENV_PRINT_CONFIG'
$PASS_SET_OUTREACH_ENV_DATA_DIR = 'PASS_SET_OUTREACH_ENV_DATA_DIR'
$PASS_SET_OUTREACH_ENV_COMPLETE = 'PASS_SET_OUTREACH_ENV_COMPLETE'

function Fail-Token([string]$Token, [string]$Detail = '') {
  if ($Detail) {
    Write-Output ($Token + ' ' + $Detail)
  } else {
    Write-Output $Token
  }
  exit 1
}

function Pass-Token([string]$Token, [string]$Detail = '') {
  if ($Detail) {
    Write-Output ($Token + ' ' + $Detail)
  } else {
    Write-Output $Token
  }
}

function Compact-Detail([string]$Text) {
  $value = (($Text -replace '[\r\n]+', ' ') -replace '\s+', ' ').Trim()
  if (-not $value) { return 'unknown' }
  if ($value.Length -gt 220) { return ($value.Substring(0, 220) + '...') }
  return $value
}

function Is-ValidEmailShape([string]$Email) {
  $text = ($Email -as [string])
  if ($null -eq $text) { $text = '' }
  $text = $text.Trim().ToLowerInvariant()
  if (-not $text) { return $false }
  if (-not $text.Contains('@')) { return $false }
  $parts = $text.Split('@')
  if ($parts.Count -ne 2) { return $false }
  if (-not $parts[0] -or -not $parts[1]) { return $false }
  if (-not $parts[1].Contains('.')) { return $false }
  if ($parts[1].StartsWith('.') -or $parts[1].EndsWith('.')) { return $false }
  return $true
}

function Normalize-OutreachStates([string]$Raw) {
  $tokens = @()
  foreach ($part in ($Raw -split ',')) {
    $state = ($part -as [string]).Trim().ToUpperInvariant()
    if (-not $state) { continue }
    if ($state -notmatch '^[A-Z]{2,3}$') {
      return $null
    }
    if ($tokens -notcontains $state) {
      $tokens += $state
    }
  }
  if ($tokens.Count -lt 1) { return $null }
  return ($tokens -join ',')
}

function Normalize-CommaList([string]$Raw) {
  $tokens = @()
  foreach ($part in ($Raw -split ',')) {
    $item = ($part -as [string]).Trim()
    if (-not $item) { continue }
    if ($tokens -notcontains $item) {
      $tokens += $item
    }
  }
  return ($tokens -join ',')
}

function Test-ValidAbsoluteDataDir([string]$Value) {
  $text = (($Value -as [string]))
  if ($null -eq $text) { $text = '' }
  $text = $text.Trim()
  if (-not $text) { return $false }
  if ($text -ieq 'out') { return $false }
  return [System.IO.Path]::IsPathRooted($text)
}

function Test-ValidAbsoluteOptionalPath([string]$Value) {
  $text = (($Value -as [string]))
  if ($null -eq $text) { $text = '' }
  $text = $text.Trim()
  if (-not $text) { return $false }
  if ($text -ieq 'out') { return $false }
  return [System.IO.Path]::IsPathRooted($text)
}

function Parse-DotenvMap([string]$DotenvText) {
  $map = [ordered]@{}
  foreach ($line in ($DotenvText -split "`r?`n")) {
    $trimmed = ($line -as [string]).Trim()
    if (-not $trimmed) { continue }
    if ($trimmed.StartsWith('#')) { continue }
    if ($line -notmatch '^\s*(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*=(.*)$') { continue }
    $key = $Matches[1]
    $value = $Matches[2]
    if ($value.Length -ge 2 -and $value.StartsWith('"') -and $value.EndsWith('"')) {
      $value = $value.Substring(1, $value.Length - 2)
    } elseif ($value.Length -ge 2 -and $value.StartsWith("'") -and $value.EndsWith("'")) {
      $value = $value.Substring(1, $value.Length - 2)
    }
    $map[$key] = $value
  }
  return $map
}

function Render-DotenvMap($Map) {
  $lines = @()
  foreach ($key in $Map.Keys) {
    $value = [string]$Map[$key]
    $safe = $value -replace "`r", '' -replace "`n", ''
    $lines += ($key + '=' + $safe)
  }
  return (($lines -join "`n") + "`n")
}

function Map-HasValue($Map, [string]$Key) {
  if (-not $Map.Contains($Key)) { return $false }
  return [string]::IsNullOrWhiteSpace([string]$Map[$Key]) -eq $false
}

function Set-MapValue($Map, [string]$Key, [string]$Value, $TouchedList) {
  $existing = ''
  if ($Map.Contains($Key)) { $existing = [string]$Map[$Key] }
  if ($existing -ceq $Value) { return }
  $Map[$Key] = $Value
  if ($TouchedList -notcontains $Key) {
    [void]$TouchedList.Add($Key)
  }
}

function Ensure-ToolsAndFiles([string]$EnvSopsPath) {
  $sopsExe = Resolve-SopsExe
  if (-not $sopsExe) {
    Fail-Token $ERR_SET_OUTREACH_ENV_TOOLING 'missing_sops'
  }
  $ageExe = Resolve-AgeExe
  if (-not $ageExe) {
    Fail-Token $ERR_SET_OUTREACH_ENV_TOOLING 'missing_age'
  }
  $ageKeysPath = Get-AgeKeyFilePath
  if (-not (Test-Path -LiteralPath $ageKeysPath)) {
    Fail-Token $ERR_SET_OUTREACH_ENV_TOOLING ('missing_age_keys path=' + $ageKeysPath)
  }
  if (-not (Test-Path -LiteralPath $EnvSopsPath)) {
    Fail-Token $ERR_SET_OUTREACH_ENV_TOOLING ('missing_env_sops path=' + $EnvSopsPath)
  }
  return @{
    SopsExe = $sopsExe
    AgeExe = $ageExe
    AgeKeysPath = $ageKeysPath
  }
}

function Run-PrintConfigCheck(
  [string]$RunWithSecretsPath,
  [string]$RepoRoot,
  [string]$ExpectedStripePriceIdCore = '',
  [string]$ExpectedStripePriceIdMulti = '',
  [string]$ExpectedStripePriceIdPilot = '',
  [bool]$ExpectWebhookSecret = $false
) {
  $out = & $RunWithSecretsPath py -3 run_outreach_auto.py --print-config 2>&1
  $code = $LASTEXITCODE
  $joined = (($out | ForEach-Object { $_.ToString() }) -join "`n")
  if ($code -ne 0) {
    Fail-Token $ERR_SET_OUTREACH_ENV_PRINT_CONFIG ('code=' + $code + ' detail=' + (Compact-Detail $joined))
  }
  if ($joined -notmatch 'PASS_AUTO_PRINT_CONFIG') {
    Fail-Token $ERR_SET_OUTREACH_ENV_PRINT_CONFIG 'missing_pass_auto_print_config'
  }
  if ($joined -notmatch 'outreach_states=') {
    Fail-Token $ERR_SET_OUTREACH_ENV_PRINT_CONFIG 'missing_outreach_states'
  }

  $oldPythonPath = $null
  if (Test-Path Env:PYTHONPATH) {
    $oldPythonPath = [string]$env:PYTHONPATH
  }
  try {
    if ([string]::IsNullOrWhiteSpace($oldPythonPath)) {
      $env:PYTHONPATH = $RepoRoot
    } else {
      $env:PYTHONPATH = ($RepoRoot + [IO.Path]::PathSeparator + $oldPythonPath)
    }
    $stripeOut = & $RunWithSecretsPath py -3 scripts\subscription_registry_ops.py stripe-ingest --print-config 2>&1
    $stripeCode = $LASTEXITCODE
  } finally {
    if ($null -eq $oldPythonPath) {
      Remove-Item Env:PYTHONPATH -ErrorAction SilentlyContinue
    } else {
      $env:PYTHONPATH = $oldPythonPath
    }
  }

  $stripeJoined = (($stripeOut | ForEach-Object { $_.ToString() }) -join "`n")
  if ($stripeCode -ne 0) {
    Fail-Token $ERR_SET_OUTREACH_ENV_PRINT_CONFIG ('stripe_print_config_failed code=' + $stripeCode + ' detail=' + (Compact-Detail $stripeJoined))
  }

  $stripeJson = ''
  foreach ($line in ($stripeOut | ForEach-Object { $_.ToString().Trim() })) {
    if ($line.StartsWith('{') -and $line.EndsWith('}')) {
      $stripeJson = $line
    }
  }
  if (-not $stripeJson) {
    Fail-Token $ERR_SET_OUTREACH_ENV_PRINT_CONFIG 'missing_stripe_json_payload'
  }

  $stripePayload = $null
  try {
    $stripePayload = ($stripeJson | ConvertFrom-Json -ErrorAction Stop)
  } catch {
    Fail-Token $ERR_SET_OUTREACH_ENV_PRINT_CONFIG ('invalid_stripe_json detail=' + (Compact-Detail $_.Exception.Message))
  }

  if (-not $stripePayload -or [string]$stripePayload.command -ne 'stripe-ingest') {
    Fail-Token $ERR_SET_OUTREACH_ENV_PRINT_CONFIG 'missing_stripe_command'
  }

  $missingKeys = New-Object System.Collections.Generic.List[string]
  if (-not [bool]$stripePayload.stripe_price_id_core_present) {
    [void]$missingKeys.Add('stripe_price_id_core_present')
  }
  if (-not [bool]$stripePayload.stripe_price_id_multi_present) {
    [void]$missingKeys.Add('stripe_price_id_multi_present')
  }
  if (-not [bool]$stripePayload.web_stripe_webhook_secret_present) {
    [void]$missingKeys.Add('web_stripe_webhook_secret_present')
  }
  if ($missingKeys.Count -gt 0) {
    Fail-Token $ERR_SET_OUTREACH_ENV_PRINT_CONFIG_MISSING_KEYS ('missing=' + ($missingKeys -join ','))
  }

  if ($ExpectedStripePriceIdCore -and $stripeJoined -notmatch [Regex]::Escape($ExpectedStripePriceIdCore)) {
    Fail-Token $ERR_SET_OUTREACH_ENV_PRINT_CONFIG 'missing_stripe_price_id_core'
  }
  if ($ExpectedStripePriceIdMulti -and $stripeJoined -notmatch [Regex]::Escape($ExpectedStripePriceIdMulti)) {
    Fail-Token $ERR_SET_OUTREACH_ENV_PRINT_CONFIG 'missing_stripe_price_id_multi'
  }
  if ($ExpectedStripePriceIdPilot -and $stripeJoined -notmatch [Regex]::Escape($ExpectedStripePriceIdPilot)) {
    Fail-Token $ERR_SET_OUTREACH_ENV_PRINT_CONFIG 'missing_stripe_price_id_pilot'
  }
  if ($ExpectWebhookSecret -and $stripeJoined -notmatch '"web_stripe_webhook_secret_present"\s*:\s*true') {
    Fail-Token $ERR_SET_OUTREACH_ENV_PRINT_CONFIG 'missing_web_stripe_webhook_secret'
  }
  Pass-Token $PASS_SET_OUTREACH_ENV_PRINT_CONFIG 'print_config_ok=YES'
}

try {
  $repoRoot = Resolve-RepoRoot
  $envSopsPath = Join-Path $repoRoot '.env.sops'
  $runWithSecretsPath = Join-Path $repoRoot 'run_with_secrets.ps1'

  $mutatingArgs = @(
    'OutreachDailyLimit',
    'OutreachStates',
    'OshaSmokeTo',
    'OutreachSuppressionMaxAgeHours',
    'OutreachFallbackOnEmptyState',
    'OutreachSkipRoleInboxes',
    'ProspectAutoGrowEnabled',
    'ProspectAutoGrowStates',
    'ProspectAutoGrowSources',
    'ProspectAutoGrowBacklogTarget',
    'ProspectAutoGrowMaxFetchPagesPerRun',
    'ProspectAutoGrowHttpSleepMs',
    'ProspectAutoGrowStateLicTxLicenseTypes',
    'ProspectEnrichDomainEnabled',
    'ProspectEnrichHunterEnabled',
    'HunterApiKey',
    'ApolloApiKey',
    'ApolloEnrichEnabled',
    'ApolloEnrichMaxPerRun',
    'ApolloPersonTitles',
    'ApolloPersonLocationsMode',
    'TrialSendsLimitDefault',
    'TrialExpiredBehaviorDefault',
    'TrialConversionUrl',
    'AiTriageEnabled',
    'AiTriageOpenAiModel',
    'SignalFreshnessMaxDays',
    'HudApiToken',
    'StripePriceIdCore',
    'StripePriceIdMulti',
    'StripePriceIdPilot',
    'WebStripeWebhookSecret',
    'DataDir',
    'ProspectDiscoveryInput',
    'BounceImapHost',
    'BounceImapPort',
    'BounceImapUser',
    'BounceImapPass',
    'BounceImapFolder',
    'BounceImapSinceHours',
    'BounceImapMaxMessages',
    'TaskSchedUser',
    'TaskSchedPassword',
    'RuntimeRole',
    'CanonicalHostname',
    'ArtifactSyncDir',
    'TaskLogRoot',
    'RunSummaryRoot'
  )
  $hasMutatingArgs = $false
  foreach ($name in $mutatingArgs) {
    if ($PSBoundParameters.ContainsKey($name)) {
      $hasMutatingArgs = $true
      break
    }
  }
  if ($PrintConfig -and $hasMutatingArgs) {
    Fail-Token $ERR_SET_OUTREACH_ENV_ARGS 'print_config_conflicts_with_mutating_args'
  }

  if ($PSBoundParameters.ContainsKey('OutreachDailyLimit') -and $OutreachDailyLimit -lt 1) {
    Fail-Token $ERR_SET_OUTREACH_ENV_ARGS 'invalid_OutreachDailyLimit'
  }
  if ($PSBoundParameters.ContainsKey('OutreachSuppressionMaxAgeHours') -and $OutreachSuppressionMaxAgeHours -lt 1) {
    Fail-Token $ERR_SET_OUTREACH_ENV_ARGS 'invalid_OutreachSuppressionMaxAgeHours'
  }
  if ($PSBoundParameters.ContainsKey('OutreachFallbackOnEmptyState')) {
    if (($OutreachFallbackOnEmptyState -ne 0) -and ($OutreachFallbackOnEmptyState -ne 1)) {
      Fail-Token $ERR_SET_OUTREACH_ENV_ARGS 'invalid_OutreachFallbackOnEmptyState'
    }
  }
  if ($PSBoundParameters.ContainsKey('OutreachSkipRoleInboxes')) {
    if (($OutreachSkipRoleInboxes -ne 0) -and ($OutreachSkipRoleInboxes -ne 1)) {
      Fail-Token $ERR_SET_OUTREACH_ENV_ARGS 'invalid_OutreachSkipRoleInboxes'
    }
  }
  if ($PSBoundParameters.ContainsKey('ProspectAutoGrowEnabled') -and $ProspectAutoGrowEnabled -notin @(0, 1)) {
    Fail-Token $ERR_SET_OUTREACH_ENV_ARGS 'invalid_ProspectAutoGrowEnabled'
  }
  if ($PSBoundParameters.ContainsKey('ProspectAutoGrowBacklogTarget') -and $ProspectAutoGrowBacklogTarget -lt 1) {
    Fail-Token $ERR_SET_OUTREACH_ENV_ARGS 'invalid_ProspectAutoGrowBacklogTarget'
  }
  if ($PSBoundParameters.ContainsKey('ProspectAutoGrowMaxFetchPagesPerRun') -and $ProspectAutoGrowMaxFetchPagesPerRun -lt 1) {
    Fail-Token $ERR_SET_OUTREACH_ENV_ARGS 'invalid_ProspectAutoGrowMaxFetchPagesPerRun'
  }
  if ($PSBoundParameters.ContainsKey('ProspectAutoGrowHttpSleepMs') -and $ProspectAutoGrowHttpSleepMs -lt 0) {
    Fail-Token $ERR_SET_OUTREACH_ENV_ARGS 'invalid_ProspectAutoGrowHttpSleepMs'
  }
  if ($PSBoundParameters.ContainsKey('ProspectEnrichDomainEnabled') -and $ProspectEnrichDomainEnabled -notin @(0, 1)) {
    Fail-Token $ERR_SET_OUTREACH_ENV_ARGS 'invalid_ProspectEnrichDomainEnabled'
  }
  if ($PSBoundParameters.ContainsKey('ProspectEnrichHunterEnabled') -and $ProspectEnrichHunterEnabled -notin @(0, 1)) {
    Fail-Token $ERR_SET_OUTREACH_ENV_ARGS 'invalid_ProspectEnrichHunterEnabled'
  }
  if ($PSBoundParameters.ContainsKey('TrialSendsLimitDefault') -and $TrialSendsLimitDefault -lt 1) {
    Fail-Token $ERR_SET_OUTREACH_ENV_ARGS 'invalid_TrialSendsLimitDefault'
  }
  if ($PSBoundParameters.ContainsKey('AiTriageEnabled') -and $AiTriageEnabled -notin @(0, 1)) {
    Fail-Token $ERR_SET_OUTREACH_ENV_ARGS 'invalid_AiTriageEnabled'
  }
  if ($PSBoundParameters.ContainsKey('SignalFreshnessMaxDays') -and $SignalFreshnessMaxDays -lt 1) {
    Fail-Token $ERR_SET_OUTREACH_ENV_ARGS 'invalid_SignalFreshnessMaxDays'
  }
  if ($PSBoundParameters.ContainsKey('BounceImapPort') -and $BounceImapPort -lt 1) {
    Fail-Token $ERR_SET_OUTREACH_ENV_ARGS 'invalid_BounceImapPort'
  }
  if ($PSBoundParameters.ContainsKey('BounceImapSinceHours') -and $BounceImapSinceHours -lt 1) {
    Fail-Token $ERR_SET_OUTREACH_ENV_ARGS 'invalid_BounceImapSinceHours'
  }
  if ($PSBoundParameters.ContainsKey('BounceImapMaxMessages') -and $BounceImapMaxMessages -lt 1) {
    Fail-Token $ERR_SET_OUTREACH_ENV_ARGS 'invalid_BounceImapMaxMessages'
  }
  if ($PSBoundParameters.ContainsKey('TaskSchedUser')) {
    if (-not (($TaskSchedUser -as [string]).Trim())) {
      Fail-Token $ERR_SET_OUTREACH_ENV_ARGS 'invalid_TaskSchedUser'
    }
  }
  if ($PSBoundParameters.ContainsKey('TaskSchedPassword')) {
    if (-not (($TaskSchedPassword -as [string]).Trim())) {
      Fail-Token $ERR_SET_OUTREACH_ENV_ARGS 'invalid_TaskSchedPassword'
    }
  }
  if ($PSBoundParameters.ContainsKey('RuntimeRole')) {
    $runtimeRoleValue = (($RuntimeRole -as [string]).Trim().ToLowerInvariant())
    if (($runtimeRoleValue -ne 'canonical_scheduler') -and ($runtimeRoleValue -ne 'dev_client')) {
      Fail-Token $ERR_SET_OUTREACH_ENV_ARGS 'invalid_RuntimeRole'
    }
  }
  if ($PSBoundParameters.ContainsKey('CanonicalHostname')) {
    if (-not (($CanonicalHostname -as [string]).Trim())) {
      Fail-Token $ERR_SET_OUTREACH_ENV_ARGS 'invalid_CanonicalHostname'
    }
  }
  if ($PSBoundParameters.ContainsKey('ArtifactSyncDir')) {
    if (-not (Test-ValidAbsoluteOptionalPath ($ArtifactSyncDir -as [string]))) {
      Fail-Token $ERR_SET_OUTREACH_ENV_ARGS 'invalid_ArtifactSyncDir_absolute_required'
    }
  }
  if ($PSBoundParameters.ContainsKey('TaskLogRoot')) {
    if (-not (Test-ValidAbsoluteOptionalPath ($TaskLogRoot -as [string]))) {
      Fail-Token $ERR_SET_OUTREACH_ENV_ARGS 'invalid_TaskLogRoot_absolute_required'
    }
  }
  if ($PSBoundParameters.ContainsKey('RunSummaryRoot')) {
    if (-not (Test-ValidAbsoluteOptionalPath ($RunSummaryRoot -as [string]))) {
      Fail-Token $ERR_SET_OUTREACH_ENV_ARGS 'invalid_RunSummaryRoot_absolute_required'
    }
  }

  if ($PSBoundParameters.ContainsKey('OutreachStates')) {
    $normStates = Normalize-OutreachStates $OutreachStates
    if (-not $normStates) {
      Fail-Token $ERR_SET_OUTREACH_ENV_ARGS 'invalid_OutreachStates'
    }
  }
  if ($PSBoundParameters.ContainsKey('OshaSmokeTo')) {
    if (-not (Is-ValidEmailShape $OshaSmokeTo)) {
      Fail-Token $ERR_SET_OUTREACH_ENV_ARGS 'invalid_OshaSmokeTo'
    }
  }
  if ($PSBoundParameters.ContainsKey('TrialExpiredBehaviorDefault')) {
    $beh = ($TrialExpiredBehaviorDefault -as [string]).Trim()
    if (-not $beh) {
      Fail-Token $ERR_SET_OUTREACH_ENV_ARGS 'invalid_TrialExpiredBehaviorDefault'
    }
  }
  if ($PSBoundParameters.ContainsKey('AiTriageOpenAiModel')) {
    $aiModel = ($AiTriageOpenAiModel -as [string]).Trim()
    if (-not $aiModel) {
      Fail-Token $ERR_SET_OUTREACH_ENV_ARGS 'invalid_AiTriageOpenAiModel'
    }
  }
  if ($PSBoundParameters.ContainsKey('HudApiToken')) {
    $hudToken = ($HudApiToken -as [string]).Trim()
    if (-not $hudToken) {
      Fail-Token $ERR_SET_OUTREACH_ENV_ARGS 'invalid_HudApiToken'
    }
  }
  if ($PSBoundParameters.ContainsKey('StripePriceIdCore')) {
    $corePrice = ($StripePriceIdCore -as [string]).Trim()
    if (-not $corePrice) {
      Fail-Token $ERR_SET_OUTREACH_ENV_ARGS 'invalid_StripePriceIdCore'
    }
  }
  if ($PSBoundParameters.ContainsKey('StripePriceIdMulti')) {
    $multiPrice = ($StripePriceIdMulti -as [string]).Trim()
    if (-not $multiPrice) {
      Fail-Token $ERR_SET_OUTREACH_ENV_ARGS 'invalid_StripePriceIdMulti'
    }
  }
  if ($PSBoundParameters.ContainsKey('StripePriceIdPilot')) {
    $pilotPrice = ($StripePriceIdPilot -as [string]).Trim()
    if (-not $pilotPrice) {
      Fail-Token $ERR_SET_OUTREACH_ENV_ARGS 'invalid_StripePriceIdPilot'
    }
  }
  if ($PSBoundParameters.ContainsKey('WebStripeWebhookSecret')) {
    $webhookSecret = ($WebStripeWebhookSecret -as [string]).Trim()
    if (-not $webhookSecret) {
      Fail-Token $ERR_SET_OUTREACH_ENV_ARGS 'invalid_WebStripeWebhookSecret'
    }
  }
  if ($PSBoundParameters.ContainsKey('ProspectAutoGrowSources')) {
    $rawSources = ($ProspectAutoGrowSources -as [string])
    $srcTokens = @()
    foreach ($part in ($rawSources -split ',')) {
      $src = ($part -as [string]).Trim().ToUpperInvariant()
      if (-not $src) { continue }
      if (
        ($src -ne 'AIHA') -and
        ($src -ne 'OHS_BG') -and
        ($src -ne 'APOLLO') -and
        ($src -ne 'BCSP') -and
        ($src -ne 'OSHA_NEWS') -and
        ($src -ne 'STATE_LIC') -and
        ($src -ne 'AGC') -and
        ($src -ne 'BLUEBOOK') -and
        ($src -ne 'THOMASNET') -and
        ($src -ne 'BBB')
      ) {
        Fail-Token $ERR_SET_OUTREACH_ENV_ARGS ('invalid_ProspectAutoGrowSources value=' + $src)
      }
      if ($srcTokens -notcontains $src) {
        $srcTokens += $src
      }
    }
  }
  if ($PSBoundParameters.ContainsKey('ProspectAutoGrowStateLicTxLicenseTypes')) {
    if (-not (Normalize-CommaList ($ProspectAutoGrowStateLicTxLicenseTypes -as [string]))) {
      Fail-Token $ERR_SET_OUTREACH_ENV_ARGS 'invalid_ProspectAutoGrowStateLicTxLicenseTypes'
    }
  }
  if ($PSBoundParameters.ContainsKey('HunterApiKey')) {
    if (-not (($HunterApiKey -as [string]).Trim())) {
      Fail-Token $ERR_SET_OUTREACH_ENV_ARGS 'invalid_HunterApiKey'
    }
  }
  if ($PSBoundParameters.ContainsKey('ApolloApiKey')) {
    if (-not (($ApolloApiKey -as [string]).Trim())) {
      Fail-Token $ERR_SET_OUTREACH_ENV_ARGS 'invalid_ApolloApiKey'
    }
  }
  if ($PSBoundParameters.ContainsKey('ApolloEnrichEnabled')) {
    if ($ApolloEnrichEnabled -notin @(0, 1)) {
      Fail-Token $ERR_SET_OUTREACH_ENV_ARGS 'invalid_ApolloEnrichEnabled'
    }
  }
  if ($PSBoundParameters.ContainsKey('ApolloEnrichMaxPerRun') -and $ApolloEnrichMaxPerRun -lt 1) {
    Fail-Token $ERR_SET_OUTREACH_ENV_ARGS 'invalid_ApolloEnrichMaxPerRun'
  }
  if ($PSBoundParameters.ContainsKey('ApolloPersonTitles')) {
    if (-not (Normalize-CommaList ($ApolloPersonTitles -as [string]))) {
      Fail-Token $ERR_SET_OUTREACH_ENV_ARGS 'invalid_ApolloPersonTitles'
    }
  }
  if ($PSBoundParameters.ContainsKey('ApolloPersonLocationsMode')) {
    $locMode = (($ApolloPersonLocationsMode -as [string]).Trim().ToLowerInvariant())
    if ($locMode -ne 'state') {
      Fail-Token $ERR_SET_OUTREACH_ENV_ARGS 'invalid_ApolloPersonLocationsMode'
    }
  }
  if ($PSBoundParameters.ContainsKey('BounceImapHost')) {
    if (-not (($BounceImapHost -as [string]).Trim())) {
      Fail-Token $ERR_SET_OUTREACH_ENV_ARGS 'invalid_BounceImapHost'
    }
  }
  if ($PSBoundParameters.ContainsKey('BounceImapUser')) {
    if (-not (($BounceImapUser -as [string]).Trim())) {
      Fail-Token $ERR_SET_OUTREACH_ENV_ARGS 'invalid_BounceImapUser'
    }
  }
  if ($PSBoundParameters.ContainsKey('BounceImapPass')) {
    if (-not (($BounceImapPass -as [string]).Trim())) {
      Fail-Token $ERR_SET_OUTREACH_ENV_ARGS 'invalid_BounceImapPass'
    }
  }
  if ($PSBoundParameters.ContainsKey('BounceImapFolder')) {
    if (-not (($BounceImapFolder -as [string]).Trim())) {
      Fail-Token $ERR_SET_OUTREACH_ENV_ARGS 'invalid_BounceImapFolder'
    }
  }

  $tooling = Ensure-ToolsAndFiles $envSopsPath
  $sopsExe = [string]$tooling.SopsExe

  Push-Location $repoRoot
  try {
    $stagedEnv = (& git -C $repoRoot diff --cached --name-only -- .env.sops 2>$null) -join "`n"
    if ($stagedEnv -match '(?im)^\.env\.sops$') {
      Fail-Token $ERR_ENV_SOPS_STAGED 'path=.env.sops'
    }

    if ($PrintConfig) {
      Run-PrintConfigCheck -RunWithSecretsPath $runWithSecretsPath -RepoRoot $repoRoot
      try {
        $printPlain = Decrypt-DotenvSopsFile -SopsExe $sopsExe -EnvSopsPath $envSopsPath
      } catch {
        Fail-Token $ERR_SET_OUTREACH_ENV_PRINT_CONFIG ('decrypt_failed detail=' + (Compact-Detail $_.Exception.Message))
      }
      $printMap = Parse-DotenvMap $printPlain
      $outreachSkipRoleInboxesValue = if (Map-HasValue $printMap 'OUTREACH_SKIP_ROLE_INBOXES') { ([string]$printMap['OUTREACH_SKIP_ROLE_INBOXES']).Trim() } else { '1' }
      Write-Output ('outreach_skip_role_inboxes=' + $outreachSkipRoleInboxesValue)
      $aiTriageEnabledValue = '0'
      if (Map-HasValue $printMap 'AI_TRIAGE_ENABLED') {
        $rawAiEnabled = ([string]$printMap['AI_TRIAGE_ENABLED']).Trim().ToLowerInvariant()
        if ($rawAiEnabled -in @('1','true','yes','on')) {
          $aiTriageEnabledValue = '1'
        } else {
          $aiTriageEnabledValue = '0'
        }
      }
      $aiTriageModelValue = 'gpt-4.1-mini'
      if (Map-HasValue $printMap 'AI_TRIAGE_OPENAI_MODEL') {
        $aiTriageModelValue = ([string]$printMap['AI_TRIAGE_OPENAI_MODEL']).Trim()
      }
      $signalFreshnessMaxDaysValue = '30'
      if (Map-HasValue $printMap 'SIGNAL_FRESHNESS_MAX_DAYS') {
        $signalFreshnessMaxDaysValue = ([string]$printMap['SIGNAL_FRESHNESS_MAX_DAYS']).Trim()
      }
      $openAiKeyPresent = if (Map-HasValue $printMap 'OPENAI_API_KEY') { 'YES' } else { 'NO' }
      $apolloApiKeyPresent = if (Map-HasValue $printMap 'APOLLO_API_KEY') { 'YES' } else { 'NO' }
      $hunterApiKeyPresent = if (Map-HasValue $printMap 'HUNTER_API_KEY') { 'YES' } else { 'NO' }
      $apolloEnrichEnabledValue = if (Map-HasValue $printMap 'APOLLO_ENRICH_ENABLED') { ([string]$printMap['APOLLO_ENRICH_ENABLED']).Trim() } else { '0' }
      $apolloEnrichMaxValue = if (Map-HasValue $printMap 'APOLLO_ENRICH_MAX_PER_RUN') { ([string]$printMap['APOLLO_ENRICH_MAX_PER_RUN']).Trim() } else { '50' }
      $apolloLocationsModeValue = if (Map-HasValue $printMap 'APOLLO_PERSON_LOCATIONS_MODE') { ([string]$printMap['APOLLO_PERSON_LOCATIONS_MODE']).Trim() } else { 'state' }
      $prospectEnrichDomainEnabledValue = if (Map-HasValue $printMap 'PROSPECT_ENRICH_DOMAIN_ENABLED') { ([string]$printMap['PROSPECT_ENRICH_DOMAIN_ENABLED']).Trim() } else { '0' }
      $prospectEnrichHunterEnabledValue = if (Map-HasValue $printMap 'PROSPECT_ENRICH_HUNTER_ENABLED') { ([string]$printMap['PROSPECT_ENRICH_HUNTER_ENABLED']).Trim() } else { '0' }
      Write-Output ('ai_triage_enabled=' + $aiTriageEnabledValue)
      Write-Output ('ai_triage_openai_model=' + $aiTriageModelValue)
      Write-Output ('signal_freshness_max_days=' + $signalFreshnessMaxDaysValue)
      Write-Output ('openai_api_key_present=' + $openAiKeyPresent)
      Write-Output ('apollo_api_key_present=' + $apolloApiKeyPresent)
      Write-Output ('hunter_api_key_present=' + $hunterApiKeyPresent)
      Write-Output ('apollo_enrich_enabled=' + $apolloEnrichEnabledValue)
      Write-Output ('apollo_enrich_max_per_run=' + $apolloEnrichMaxValue)
      Write-Output ('apollo_person_locations_mode=' + $apolloLocationsModeValue)
      Write-Output ('prospect_enrich_domain_enabled=' + $prospectEnrichDomainEnabledValue)
      Write-Output ('prospect_enrich_hunter_enabled=' + $prospectEnrichHunterEnabledValue)
      $taskSchedUserValue = if (Map-HasValue $printMap 'TASK_SCHED_USER') { ([string]$printMap['TASK_SCHED_USER']).Trim() } else { '' }
      $taskSchedPasswordPresent = if (Map-HasValue $printMap 'TASK_SCHED_PASSWORD') { 'YES' } else { 'NO' }
      Write-Output ('task_sched_user=' + $taskSchedUserValue)
      Write-Output ('task_sched_password_present=' + $taskSchedPasswordPresent)
      $runtimeRoleValue = if (Map-HasValue $printMap 'RUNTIME_ROLE') { ([string]$printMap['RUNTIME_ROLE']).Trim().ToLowerInvariant() } else { 'dev_client' }
      $canonicalHostnameValue = if (Map-HasValue $printMap 'CANONICAL_HOSTNAME') { ([string]$printMap['CANONICAL_HOSTNAME']).Trim() } else { '' }
      $artifactSyncDirValue = if (Map-HasValue $printMap 'ARTIFACT_SYNC_DIR') { ([string]$printMap['ARTIFACT_SYNC_DIR']).Trim() } else { '' }
      $taskLogRootValue = if (Map-HasValue $printMap 'TASK_LOG_ROOT') { ([string]$printMap['TASK_LOG_ROOT']).Trim() } else { '' }
      $runSummaryRootValue = if (Map-HasValue $printMap 'RUN_SUMMARY_ROOT') { ([string]$printMap['RUN_SUMMARY_ROOT']).Trim() } else { '' }
      $canonicalHostnameOut = if ($canonicalHostnameValue) { $canonicalHostnameValue } else { '(unset)' }
      $artifactSyncDirOut = if ($artifactSyncDirValue) { $artifactSyncDirValue } else { '(unset)' }
      $taskLogRootOut = if ($taskLogRootValue) { $taskLogRootValue } else { '(default)' }
      $runSummaryRootOut = if ($runSummaryRootValue) { $runSummaryRootValue } else { '(default)' }
      Write-Output ('runtime_role=' + $runtimeRoleValue)
      Write-Output ('canonical_hostname=' + $canonicalHostnameOut)
      Write-Output ('artifact_sync_dir=' + $artifactSyncDirOut)
      Write-Output ('task_log_root=' + $taskLogRootOut)
      Write-Output ('run_summary_root=' + $runSummaryRootOut)
      $printDataDir = if (Map-HasValue $printMap 'DATA_DIR') { ([string]$printMap['DATA_DIR']).Trim() } else { 'out' }
      Pass-Token $PASS_SET_OUTREACH_ENV_DATA_DIR ('value=' + $printDataDir + ' source=unchanged')
      Pass-Token $PASS_SET_OUTREACH_ENV_COMPLETE 'mode=print_config'
      exit 0
    }

    try {
      $plain = Decrypt-DotenvSopsFile -SopsExe $sopsExe -EnvSopsPath $envSopsPath
    } catch {
      Fail-Token $ERR_SET_OUTREACH_ENV_DECRYPT (Compact-Detail $_.Exception.Message)
    }

    $map = Parse-DotenvMap $plain
    $touched = New-Object System.Collections.Generic.List[string]

    if ($PSBoundParameters.ContainsKey('OutreachDailyLimit')) {
      Set-MapValue -Map $map -Key 'OUTREACH_DAILY_LIMIT' -Value ([string]$OutreachDailyLimit) -TouchedList $touched
    } elseif (-not (Map-HasValue $map 'OUTREACH_DAILY_LIMIT')) {
      Set-MapValue -Map $map -Key 'OUTREACH_DAILY_LIMIT' -Value '10' -TouchedList $touched
    }

    if ($PSBoundParameters.ContainsKey('OutreachStates')) {
      Set-MapValue -Map $map -Key 'OUTREACH_STATES' -Value (Normalize-OutreachStates $OutreachStates) -TouchedList $touched
    } elseif (-not (Map-HasValue $map 'OUTREACH_STATES')) {
      Set-MapValue -Map $map -Key 'OUTREACH_STATES' -Value 'TX' -TouchedList $touched
    }

    if ($PSBoundParameters.ContainsKey('OshaSmokeTo')) {
      Set-MapValue -Map $map -Key 'OSHA_SMOKE_TO' -Value (($OshaSmokeTo -as [string]).Trim().ToLowerInvariant()) -TouchedList $touched
    } elseif (-not (Map-HasValue $map 'OSHA_SMOKE_TO')) {
      Fail-Token $ERR_SET_OUTREACH_ENV_ARGS 'missing_required_param=OshaSmokeTo'
    }

    if ($PSBoundParameters.ContainsKey('OutreachSuppressionMaxAgeHours')) {
      Set-MapValue -Map $map -Key 'OUTREACH_SUPPRESSION_MAX_AGE_HOURS' -Value ([string]$OutreachSuppressionMaxAgeHours) -TouchedList $touched
    } elseif (-not (Map-HasValue $map 'OUTREACH_SUPPRESSION_MAX_AGE_HOURS')) {
      Set-MapValue -Map $map -Key 'OUTREACH_SUPPRESSION_MAX_AGE_HOURS' -Value '240' -TouchedList $touched
    }

    if ($PSBoundParameters.ContainsKey('OutreachFallbackOnEmptyState')) {
      Set-MapValue -Map $map -Key 'OUTREACH_FALLBACK_ON_EMPTY_STATE' -Value ([string]$OutreachFallbackOnEmptyState) -TouchedList $touched
    } elseif (-not (Map-HasValue $map 'OUTREACH_FALLBACK_ON_EMPTY_STATE')) {
      Set-MapValue -Map $map -Key 'OUTREACH_FALLBACK_ON_EMPTY_STATE' -Value '0' -TouchedList $touched
    }

    if ($PSBoundParameters.ContainsKey('OutreachSkipRoleInboxes')) {
      Set-MapValue -Map $map -Key 'OUTREACH_SKIP_ROLE_INBOXES' -Value ([string]$OutreachSkipRoleInboxes) -TouchedList $touched
    } elseif (-not (Map-HasValue $map 'OUTREACH_SKIP_ROLE_INBOXES')) {
      Set-MapValue -Map $map -Key 'OUTREACH_SKIP_ROLE_INBOXES' -Value '1' -TouchedList $touched
    }

    if ($PSBoundParameters.ContainsKey('BounceImapHost')) {
      Set-MapValue -Map $map -Key 'BOUNCE_IMAP_HOST' -Value (($BounceImapHost -as [string]).Trim()) -TouchedList $touched
    } elseif (-not (Map-HasValue $map 'BOUNCE_IMAP_HOST')) {
      Set-MapValue -Map $map -Key 'BOUNCE_IMAP_HOST' -Value 'imappro.zoho.com' -TouchedList $touched
    }

    if ($PSBoundParameters.ContainsKey('BounceImapPort')) {
      Set-MapValue -Map $map -Key 'BOUNCE_IMAP_PORT' -Value ([string]$BounceImapPort) -TouchedList $touched
    } elseif (-not (Map-HasValue $map 'BOUNCE_IMAP_PORT')) {
      Set-MapValue -Map $map -Key 'BOUNCE_IMAP_PORT' -Value '993' -TouchedList $touched
    }

    if ($PSBoundParameters.ContainsKey('BounceImapUser')) {
      Set-MapValue -Map $map -Key 'BOUNCE_IMAP_USER' -Value (($BounceImapUser -as [string]).Trim().ToLowerInvariant()) -TouchedList $touched
    } elseif (-not (Map-HasValue $map 'BOUNCE_IMAP_USER')) {
      Set-MapValue -Map $map -Key 'BOUNCE_IMAP_USER' -Value 'cchevali@zohomail.com' -TouchedList $touched
    }

    if ($PSBoundParameters.ContainsKey('BounceImapPass')) {
      Set-MapValue -Map $map -Key 'BOUNCE_IMAP_PASS' -Value (($BounceImapPass -as [string]).Trim()) -TouchedList $touched
    }

    if ($PSBoundParameters.ContainsKey('BounceImapFolder')) {
      Set-MapValue -Map $map -Key 'BOUNCE_IMAP_FOLDER' -Value (($BounceImapFolder -as [string]).Trim()) -TouchedList $touched
    } elseif (-not (Map-HasValue $map 'BOUNCE_IMAP_FOLDER')) {
      Set-MapValue -Map $map -Key 'BOUNCE_IMAP_FOLDER' -Value 'INBOX' -TouchedList $touched
    }

    if ($PSBoundParameters.ContainsKey('BounceImapSinceHours')) {
      Set-MapValue -Map $map -Key 'BOUNCE_IMAP_SINCE_HOURS' -Value ([string]$BounceImapSinceHours) -TouchedList $touched
    }

    if ($PSBoundParameters.ContainsKey('BounceImapMaxMessages')) {
      Set-MapValue -Map $map -Key 'BOUNCE_IMAP_MAX_MESSAGES' -Value ([string]$BounceImapMaxMessages) -TouchedList $touched
    }

    if ($PSBoundParameters.ContainsKey('ProspectAutoGrowEnabled')) {
      Set-MapValue -Map $map -Key 'PROSPECT_AUTOGROW_ENABLED' -Value ([string]$ProspectAutoGrowEnabled) -TouchedList $touched
    } elseif (-not (Map-HasValue $map 'PROSPECT_AUTOGROW_ENABLED')) {
      Set-MapValue -Map $map -Key 'PROSPECT_AUTOGROW_ENABLED' -Value '0' -TouchedList $touched
    }

    if ($PSBoundParameters.ContainsKey('ProspectAutoGrowStates')) {
      Set-MapValue -Map $map -Key 'PROSPECT_AUTOGROW_STATES' -Value (Normalize-OutreachStates $ProspectAutoGrowStates) -TouchedList $touched
    }

    if ($PSBoundParameters.ContainsKey('ProspectAutoGrowSources')) {
      $srcTokens = @()
      foreach ($part in (($ProspectAutoGrowSources -as [string]) -split ',')) {
        $src = ($part -as [string]).Trim().ToUpperInvariant()
        if (-not $src) { continue }
        if ($srcTokens -notcontains $src) {
          $srcTokens += $src
        }
      }
      Set-MapValue -Map $map -Key 'PROSPECT_AUTOGROW_SOURCES' -Value ($srcTokens -join ',') -TouchedList $touched
    } elseif (-not (Map-HasValue $map 'PROSPECT_AUTOGROW_SOURCES')) {
      Set-MapValue -Map $map -Key 'PROSPECT_AUTOGROW_SOURCES' -Value '' -TouchedList $touched
    }

    if ($PSBoundParameters.ContainsKey('ProspectAutoGrowBacklogTarget')) {
      Set-MapValue -Map $map -Key 'PROSPECT_AUTOGROW_BACKLOG_TARGET' -Value ([string]$ProspectAutoGrowBacklogTarget) -TouchedList $touched
    } elseif (-not (Map-HasValue $map 'PROSPECT_AUTOGROW_BACKLOG_TARGET')) {
      Set-MapValue -Map $map -Key 'PROSPECT_AUTOGROW_BACKLOG_TARGET' -Value '60' -TouchedList $touched
    }

    if ($PSBoundParameters.ContainsKey('ProspectAutoGrowMaxFetchPagesPerRun')) {
      Set-MapValue -Map $map -Key 'PROSPECT_AUTOGROW_MAX_FETCH_PAGES_PER_RUN' -Value ([string]$ProspectAutoGrowMaxFetchPagesPerRun) -TouchedList $touched
    } elseif (-not (Map-HasValue $map 'PROSPECT_AUTOGROW_MAX_FETCH_PAGES_PER_RUN')) {
      Set-MapValue -Map $map -Key 'PROSPECT_AUTOGROW_MAX_FETCH_PAGES_PER_RUN' -Value '6' -TouchedList $touched
    }

    if ($PSBoundParameters.ContainsKey('ProspectAutoGrowHttpSleepMs')) {
      Set-MapValue -Map $map -Key 'PROSPECT_AUTOGROW_HTTP_SLEEP_MS' -Value ([string]$ProspectAutoGrowHttpSleepMs) -TouchedList $touched
    } elseif (-not (Map-HasValue $map 'PROSPECT_AUTOGROW_HTTP_SLEEP_MS')) {
      Set-MapValue -Map $map -Key 'PROSPECT_AUTOGROW_HTTP_SLEEP_MS' -Value '800' -TouchedList $touched
    }

    if ($PSBoundParameters.ContainsKey('ProspectAutoGrowStateLicTxLicenseTypes')) {
      Set-MapValue -Map $map -Key 'PROSPECT_AUTOGROW_STATE_LIC_TX_LICENSE_TYPES' -Value (Normalize-CommaList ($ProspectAutoGrowStateLicTxLicenseTypes -as [string])) -TouchedList $touched
    }

    if ($PSBoundParameters.ContainsKey('ProspectEnrichDomainEnabled')) {
      Set-MapValue -Map $map -Key 'PROSPECT_ENRICH_DOMAIN_ENABLED' -Value ([string]$ProspectEnrichDomainEnabled) -TouchedList $touched
    } elseif (-not (Map-HasValue $map 'PROSPECT_ENRICH_DOMAIN_ENABLED')) {
      Set-MapValue -Map $map -Key 'PROSPECT_ENRICH_DOMAIN_ENABLED' -Value '0' -TouchedList $touched
    }

    if ($PSBoundParameters.ContainsKey('ProspectEnrichHunterEnabled')) {
      Set-MapValue -Map $map -Key 'PROSPECT_ENRICH_HUNTER_ENABLED' -Value ([string]$ProspectEnrichHunterEnabled) -TouchedList $touched
    } elseif (-not (Map-HasValue $map 'PROSPECT_ENRICH_HUNTER_ENABLED')) {
      Set-MapValue -Map $map -Key 'PROSPECT_ENRICH_HUNTER_ENABLED' -Value '0' -TouchedList $touched
    }

    if ($PSBoundParameters.ContainsKey('HunterApiKey')) {
      Set-MapValue -Map $map -Key 'HUNTER_API_KEY' -Value (($HunterApiKey -as [string]).Trim()) -TouchedList $touched
    }

    if ($PSBoundParameters.ContainsKey('ApolloApiKey')) {
      Set-MapValue -Map $map -Key 'APOLLO_API_KEY' -Value (($ApolloApiKey -as [string]).Trim()) -TouchedList $touched
    }

    if ($PSBoundParameters.ContainsKey('ApolloEnrichEnabled')) {
      Set-MapValue -Map $map -Key 'APOLLO_ENRICH_ENABLED' -Value ([string]$ApolloEnrichEnabled) -TouchedList $touched
    } elseif (-not (Map-HasValue $map 'APOLLO_ENRICH_ENABLED')) {
      Set-MapValue -Map $map -Key 'APOLLO_ENRICH_ENABLED' -Value '0' -TouchedList $touched
    }

    if ($PSBoundParameters.ContainsKey('ApolloEnrichMaxPerRun')) {
      Set-MapValue -Map $map -Key 'APOLLO_ENRICH_MAX_PER_RUN' -Value ([string]$ApolloEnrichMaxPerRun) -TouchedList $touched
    } elseif (-not (Map-HasValue $map 'APOLLO_ENRICH_MAX_PER_RUN')) {
      Set-MapValue -Map $map -Key 'APOLLO_ENRICH_MAX_PER_RUN' -Value '50' -TouchedList $touched
    }

    if ($PSBoundParameters.ContainsKey('ApolloPersonTitles')) {
      Set-MapValue -Map $map -Key 'APOLLO_PERSON_TITLES' -Value (Normalize-CommaList ($ApolloPersonTitles -as [string])) -TouchedList $touched
    }

    if ($PSBoundParameters.ContainsKey('ApolloPersonLocationsMode')) {
      Set-MapValue -Map $map -Key 'APOLLO_PERSON_LOCATIONS_MODE' -Value (($ApolloPersonLocationsMode -as [string]).Trim().ToLowerInvariant()) -TouchedList $touched
    } elseif (-not (Map-HasValue $map 'APOLLO_PERSON_LOCATIONS_MODE')) {
      Set-MapValue -Map $map -Key 'APOLLO_PERSON_LOCATIONS_MODE' -Value 'state' -TouchedList $touched
    }

    if ($PSBoundParameters.ContainsKey('TrialSendsLimitDefault')) {
      Set-MapValue -Map $map -Key 'TRIAL_SENDS_LIMIT_DEFAULT' -Value ([string]$TrialSendsLimitDefault) -TouchedList $touched
    } elseif (-not (Map-HasValue $map 'TRIAL_SENDS_LIMIT_DEFAULT')) {
      Set-MapValue -Map $map -Key 'TRIAL_SENDS_LIMIT_DEFAULT' -Value '14' -TouchedList $touched
    }

    if ($PSBoundParameters.ContainsKey('TrialExpiredBehaviorDefault')) {
      Set-MapValue -Map $map -Key 'TRIAL_EXPIRED_BEHAVIOR_DEFAULT' -Value (($TrialExpiredBehaviorDefault -as [string]).Trim()) -TouchedList $touched
    } elseif (-not (Map-HasValue $map 'TRIAL_EXPIRED_BEHAVIOR_DEFAULT')) {
      Set-MapValue -Map $map -Key 'TRIAL_EXPIRED_BEHAVIOR_DEFAULT' -Value 'notify_once' -TouchedList $touched
    }

    if ($PSBoundParameters.ContainsKey('TrialConversionUrl')) {
      $conv = ($TrialConversionUrl -as [string]).Trim()
      if ($conv) {
        Set-MapValue -Map $map -Key 'TRIAL_CONVERSION_URL' -Value $conv -TouchedList $touched
      }
    }

    if ($PSBoundParameters.ContainsKey('AiTriageEnabled')) {
      $aiEnabledText = [string]$AiTriageEnabled
      Set-MapValue -Map $map -Key 'AI_TRIAGE_ENABLED' -Value $aiEnabledText -TouchedList $touched
      if ($AiTriageEnabled -eq 1) {
        $shellOpenAiKey = ($env:OPENAI_API_KEY -as [string])
        if ($null -eq $shellOpenAiKey) { $shellOpenAiKey = '' }
        $shellOpenAiKey = $shellOpenAiKey.Trim()
        if (-not $shellOpenAiKey) {
          Fail-Token $ERR_SET_OUTREACH_ENV_ARGS 'missing_shell_OPENAI_API_KEY'
        }
        Set-MapValue -Map $map -Key 'OPENAI_API_KEY' -Value $shellOpenAiKey -TouchedList $touched
      }
    } elseif (-not (Map-HasValue $map 'AI_TRIAGE_ENABLED')) {
      Set-MapValue -Map $map -Key 'AI_TRIAGE_ENABLED' -Value '0' -TouchedList $touched
    }

    if ($PSBoundParameters.ContainsKey('AiTriageOpenAiModel')) {
      Set-MapValue -Map $map -Key 'AI_TRIAGE_OPENAI_MODEL' -Value (($AiTriageOpenAiModel -as [string]).Trim()) -TouchedList $touched
    } elseif (-not (Map-HasValue $map 'AI_TRIAGE_OPENAI_MODEL')) {
      Set-MapValue -Map $map -Key 'AI_TRIAGE_OPENAI_MODEL' -Value 'gpt-4.1-mini' -TouchedList $touched
    }

    if ($PSBoundParameters.ContainsKey('SignalFreshnessMaxDays')) {
      Set-MapValue -Map $map -Key 'SIGNAL_FRESHNESS_MAX_DAYS' -Value ([string]$SignalFreshnessMaxDays) -TouchedList $touched
    } elseif (-not (Map-HasValue $map 'SIGNAL_FRESHNESS_MAX_DAYS')) {
      Set-MapValue -Map $map -Key 'SIGNAL_FRESHNESS_MAX_DAYS' -Value '30' -TouchedList $touched
    }

    if ($PSBoundParameters.ContainsKey('HudApiToken')) {
      Set-MapValue -Map $map -Key 'HUD_API_TOKEN' -Value (($HudApiToken -as [string]).Trim()) -TouchedList $touched
    }

    if ($PSBoundParameters.ContainsKey('StripePriceIdCore')) {
      Set-MapValue -Map $map -Key 'STRIPE_PRICE_ID_CORE' -Value (($StripePriceIdCore -as [string]).Trim()) -TouchedList $touched
    }

    if ($PSBoundParameters.ContainsKey('StripePriceIdMulti')) {
      Set-MapValue -Map $map -Key 'STRIPE_PRICE_ID_MULTI' -Value (($StripePriceIdMulti -as [string]).Trim()) -TouchedList $touched
    }

    if ($PSBoundParameters.ContainsKey('StripePriceIdPilot')) {
      Set-MapValue -Map $map -Key 'STRIPE_PRICE_ID_PILOT' -Value (($StripePriceIdPilot -as [string]).Trim()) -TouchedList $touched
    }

    if ($PSBoundParameters.ContainsKey('WebStripeWebhookSecret')) {
      $secret = ($WebStripeWebhookSecret -as [string]).Trim()
      Set-MapValue -Map $map -Key 'WEB_STRIPE_WEBHOOK_SECRET' -Value $secret -TouchedList $touched
      Set-MapValue -Map $map -Key 'STRIPE_WEBHOOK_SECRET' -Value $secret -TouchedList $touched
    }

    $dataDirSource = 'unchanged'
    $dataDirValue = ''
    $existingDataDir = if (Map-HasValue $map 'DATA_DIR') { ([string]$map['DATA_DIR']).Trim() } else { '' }
    $inheritedDataDir = (($env:DATA_DIR -as [string]))
    if ($null -eq $inheritedDataDir) { $inheritedDataDir = '' }
    $inheritedDataDir = $inheritedDataDir.Trim()

    if ($PSBoundParameters.ContainsKey('DataDir')) {
      $dataDirValue = ($DataDir -as [string]).Trim()
      if (-not (Test-ValidAbsoluteDataDir $dataDirValue)) {
        Fail-Token $ERR_SET_OUTREACH_ENV_ARGS 'invalid_DataDir_absolute_required'
      }
      $dataDirSource = 'param'
    } elseif ((-not (Test-ValidAbsoluteDataDir $existingDataDir)) -and (Test-ValidAbsoluteDataDir $inheritedDataDir)) {
      $dataDirValue = $inheritedDataDir
      $dataDirSource = 'inherited'
    } elseif ($existingDataDir) {
      $dataDirValue = $existingDataDir
      $dataDirSource = 'unchanged'
    } else {
      $dataDirValue = 'out'
      $dataDirSource = 'unchanged'
    }
    Set-MapValue -Map $map -Key 'DATA_DIR' -Value $dataDirValue -TouchedList $touched
    Pass-Token $PASS_SET_OUTREACH_ENV_DATA_DIR ('value=' + $dataDirValue + ' source=' + $dataDirSource)

    if ($PSBoundParameters.ContainsKey('ProspectDiscoveryInput')) {
      $discoveryInput = ($ProspectDiscoveryInput -as [string]).Trim()
      if (-not $discoveryInput) {
        Fail-Token $ERR_SET_OUTREACH_ENV_ARGS 'invalid_ProspectDiscoveryInput'
      }
      Set-MapValue -Map $map -Key 'PROSPECT_DISCOVERY_INPUT' -Value $discoveryInput -TouchedList $touched
    }

    if ($PSBoundParameters.ContainsKey('TaskSchedUser')) {
      Set-MapValue -Map $map -Key 'TASK_SCHED_USER' -Value (($TaskSchedUser -as [string]).Trim()) -TouchedList $touched
    }

    if ($PSBoundParameters.ContainsKey('TaskSchedPassword')) {
      Set-MapValue -Map $map -Key 'TASK_SCHED_PASSWORD' -Value (($TaskSchedPassword -as [string]).Trim()) -TouchedList $touched
    }

    if ($PSBoundParameters.ContainsKey('RuntimeRole')) {
      Set-MapValue -Map $map -Key 'RUNTIME_ROLE' -Value (($RuntimeRole -as [string]).Trim().ToLowerInvariant()) -TouchedList $touched
    } elseif (-not (Map-HasValue $map 'RUNTIME_ROLE')) {
      Set-MapValue -Map $map -Key 'RUNTIME_ROLE' -Value 'dev_client' -TouchedList $touched
    }

    if ($PSBoundParameters.ContainsKey('CanonicalHostname')) {
      Set-MapValue -Map $map -Key 'CANONICAL_HOSTNAME' -Value (($CanonicalHostname -as [string]).Trim().ToLowerInvariant()) -TouchedList $touched
    }

    if ($PSBoundParameters.ContainsKey('ArtifactSyncDir')) {
      Set-MapValue -Map $map -Key 'ARTIFACT_SYNC_DIR' -Value (($ArtifactSyncDir -as [string]).Trim()) -TouchedList $touched
    }

    if ($PSBoundParameters.ContainsKey('TaskLogRoot')) {
      Set-MapValue -Map $map -Key 'TASK_LOG_ROOT' -Value (($TaskLogRoot -as [string]).Trim()) -TouchedList $touched
    }

    if ($PSBoundParameters.ContainsKey('RunSummaryRoot')) {
      Set-MapValue -Map $map -Key 'RUN_SUMMARY_ROOT' -Value (($RunSummaryRoot -as [string]).Trim()) -TouchedList $touched
    }

    $rendered = Render-DotenvMap $map

    $tmpPlain = Join-Path $repoRoot ('.set_outreach_env_plain_' + [Guid]::NewGuid().ToString('N') + '.env')
    $tmpEncrypted = Join-Path $repoRoot ('.set_outreach_env_enc_' + [Guid]::NewGuid().ToString('N') + '.env')
    try {
      [System.IO.File]::WriteAllText($tmpPlain, $rendered, [System.Text.UTF8Encoding]::new($false))

      $encrypted = & $sopsExe --encrypt --input-type dotenv --output-type dotenv $tmpPlain 2>&1
      if ($LASTEXITCODE -ne 0) {
        $detail = (($encrypted | ForEach-Object { $_.ToString() }) -join ' ')
        Fail-Token $ERR_SET_OUTREACH_ENV_ENCRYPT (Compact-Detail $detail)
      }

      $encryptedText = ($encrypted -join "`n")
      [System.IO.File]::WriteAllText($tmpEncrypted, $encryptedText, [System.Text.UTF8Encoding]::new($false))

      Move-Item -LiteralPath $tmpEncrypted -Destination $envSopsPath -Force
    } catch {
      Fail-Token $ERR_SET_OUTREACH_ENV_WRITE (Compact-Detail $_.Exception.Message)
    } finally {
      if (Test-Path -LiteralPath $tmpPlain) { Remove-Item -LiteralPath $tmpPlain -Force -ErrorAction SilentlyContinue }
      if (Test-Path -LiteralPath $tmpEncrypted) { Remove-Item -LiteralPath $tmpEncrypted -Force -ErrorAction SilentlyContinue }
    }

    Pass-Token $PASS_SET_OUTREACH_ENV_APPLY ('updated_keys=' + $touched.Count)

    try {
      $verifyPlain = Decrypt-DotenvSopsFile -SopsExe $sopsExe -EnvSopsPath $envSopsPath
    } catch {
      Fail-Token $ERR_SET_OUTREACH_ENV_VERIFY ('re_decrypt_failed detail=' + (Compact-Detail $_.Exception.Message))
    }
    $verifyMap = Parse-DotenvMap $verifyPlain
    foreach ($k in $touched) {
      if (-not $verifyMap.Contains($k)) {
        Fail-Token $ERR_SET_OUTREACH_ENV_VERIFY ('missing_key=' + $k)
      }
      if ([string]$verifyMap[$k] -cne [string]$map[$k]) {
        Fail-Token $ERR_SET_OUTREACH_ENV_VERIFY ('value_mismatch key=' + $k)
      }
    }
    Pass-Token $PASS_SET_OUTREACH_ENV_VERIFY ('verified_keys=' + $touched.Count)

    $expectedCore = ''
    if ($map.Contains('STRIPE_PRICE_ID_CORE')) { $expectedCore = [string]$map['STRIPE_PRICE_ID_CORE'] }
    $expectedMulti = ''
    if ($map.Contains('STRIPE_PRICE_ID_MULTI')) { $expectedMulti = [string]$map['STRIPE_PRICE_ID_MULTI'] }
    $expectedPilot = ''
    if ($map.Contains('STRIPE_PRICE_ID_PILOT')) { $expectedPilot = [string]$map['STRIPE_PRICE_ID_PILOT'] }
    $expectWebhookSecret = $false
    if ($map.Contains('WEB_STRIPE_WEBHOOK_SECRET')) {
      $expectWebhookSecret = -not [string]::IsNullOrWhiteSpace([string]$map['WEB_STRIPE_WEBHOOK_SECRET'])
    }

    Run-PrintConfigCheck `
      -RunWithSecretsPath $runWithSecretsPath `
      -RepoRoot $repoRoot `
      -ExpectedStripePriceIdCore $expectedCore `
      -ExpectedStripePriceIdMulti $expectedMulti `
      -ExpectedStripePriceIdPilot $expectedPilot `
      -ExpectWebhookSecret:$expectWebhookSecret
    Pass-Token $PASS_SET_OUTREACH_ENV_COMPLETE ('updated_keys=' + $touched.Count)
    exit 0
  } finally {
    Pop-Location
  }
} catch {
  Fail-Token $ERR_SET_OUTREACH_ENV_TOOLING (Compact-Detail $_.Exception.Message)
}
