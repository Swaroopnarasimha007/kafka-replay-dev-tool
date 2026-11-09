$ErrorActionPreference='Stop'
$labRoot=Split-Path $PSScriptRoot -Parent
$state=Join-Path $labRoot 'data\processes.json'
if(!(Test-Path $state)){
 Write-Host 'No managed Mini Kafka processes. If startup is still in progress, use Ctrl+C in its terminal.'
 return
}
$parsed=ConvertFrom-Json -InputObject (Get-Content $state -Raw)
$entries=@()
foreach($item in $parsed){$entries+=$item}
[array]::Reverse($entries)
foreach($entry in $entries){
 $p=Get-Process -Id $entry.id -ErrorAction SilentlyContinue
 if($p -and $p.StartTime.ToUniversalTime() -eq ([datetime]$entry.started).ToUniversalTime()){Stop-Process -InputObject $p -ErrorAction SilentlyContinue;Wait-Process -InputObject $p -Timeout 20 -ErrorAction SilentlyContinue;Write-Host "Stopped $($entry.name)"}
}
if(Test-Path $state){Remove-Item -LiteralPath $state -ErrorAction SilentlyContinue}
Write-Host 'Mini Kafka stopped. Broker data and event logs were retained.'
