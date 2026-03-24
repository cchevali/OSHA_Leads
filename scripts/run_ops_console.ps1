param(
  [int]$Port = 8420
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

$hostIp = '127.0.0.1'
$url = ('http://{0}:{1}/' -f $hostIp, $Port)
Write-Output ('MICROFLOWOPS_OPS_CONSOLE_URL=' + $url)

py -3 -m ops_console.app --host $hostIp --port $Port
exit $LASTEXITCODE
