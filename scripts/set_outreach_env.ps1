param(
  [Nullable[int]] $OutreachDailyLimit = $null,
  [string] $OutreachStates = '',
  [string] $OshaSmokeTo = '',
  [Nullable[int]] $OutreachSuppressionMaxAgeHours = $null,
  [Nullable[int]] $TrialSendsLimitDefault = $null,
  [string] $TrialExpiredBehaviorDefault = '',
  [string] $TrialConversionUrl = '',
  [string] $DataDir = '',
  [string] $ProspectDiscoveryInput = '',
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

$PASS_SET_OUTREACH_ENV_APPLY = 'PASS_SET_OUTREACH_ENV_APPLY'
$PASS_SET_OUTREACH_ENV_VERIFY = 'PASS_SET_OUTREACH_ENV_VERIFY'
$PASS_SET_OUTREACH_ENV_PRINT_CONFIG = 'PASS_SET_OUTREACH_ENV_PRINT_CONFIG'
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

function Run-PrintConfigCheck([string]$RunWithSecretsPath, [string]$RepoRoot) {
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
    'TrialSendsLimitDefault',
    'TrialExpiredBehaviorDefault',
    'TrialConversionUrl',
    'DataDir',
    'ProspectDiscoveryInput'
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
  if ($PSBoundParameters.ContainsKey('TrialSendsLimitDefault') -and $TrialSendsLimitDefault -lt 1) {
    Fail-Token $ERR_SET_OUTREACH_ENV_ARGS 'invalid_TrialSendsLimitDefault'
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

    if ($PSBoundParameters.ContainsKey('TrialSendsLimitDefault')) {
      Set-MapValue -Map $map -Key 'TRIAL_SENDS_LIMIT_DEFAULT' -Value ([string]$TrialSendsLimitDefault) -TouchedList $touched
    } elseif (-not (Map-HasValue $map 'TRIAL_SENDS_LIMIT_DEFAULT')) {
      Set-MapValue -Map $map -Key 'TRIAL_SENDS_LIMIT_DEFAULT' -Value '10' -TouchedList $touched
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

    if ($PSBoundParameters.ContainsKey('DataDir')) {
      $dir = ($DataDir -as [string]).Trim()
      if (-not $dir) {
        Fail-Token $ERR_SET_OUTREACH_ENV_ARGS 'invalid_DataDir'
      }
      Set-MapValue -Map $map -Key 'DATA_DIR' -Value $dir -TouchedList $touched
    } elseif (-not (Map-HasValue $map 'DATA_DIR')) {
      Set-MapValue -Map $map -Key 'DATA_DIR' -Value 'out' -TouchedList $touched
    }

    if ($PSBoundParameters.ContainsKey('ProspectDiscoveryInput')) {
      $discoveryInput = ($ProspectDiscoveryInput -as [string]).Trim()
      if (-not $discoveryInput) {
        Fail-Token $ERR_SET_OUTREACH_ENV_ARGS 'invalid_ProspectDiscoveryInput'
      }
      Set-MapValue -Map $map -Key 'PROSPECT_DISCOVERY_INPUT' -Value $discoveryInput -TouchedList $touched
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

    Run-PrintConfigCheck -RunWithSecretsPath $runWithSecretsPath -RepoRoot $repoRoot
    Pass-Token $PASS_SET_OUTREACH_ENV_COMPLETE ('updated_keys=' + $touched.Count)
    exit 0
  } finally {
    Pop-Location
  }
} catch {
  Fail-Token $ERR_SET_OUTREACH_ENV_TOOLING (Compact-Detail $_.Exception.Message)
}
