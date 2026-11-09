$ErrorActionPreference='Stop'
$labRoot=Split-Path $PSScriptRoot -Parent
$state=Join-Path $labRoot 'data\processes.json'
if(!(Test-Path $state)){
 $stale=Get-CimInstance Win32_Process | Where-Object {
  $_.Name -eq 'java.exe' -and $_.CommandLine -match 'kafka-lab\.jar.*broker.*broker-local\.properties'
 }
 foreach($process in $stale){Stop-Process -Id $process.ProcessId -ErrorAction SilentlyContinue;Write-Host "Stopped leftover Kafka broker process $($process.ProcessId)"}
 if(!$stale){Write-Host 'No managed lab processes';return}
 Write-Host 'Data retained. You can start the lab again.'
 return
}
$parsed=ConvertFrom-Json -InputObject (Get-Content $state -Raw)
$entries=@()
foreach($item in $parsed){$entries+=$item}
[array]::Reverse($entries)
foreach($entry in $entries){
 $p=Get-Process -Id $entry.id -ErrorAction SilentlyContinue
 if($p -and $p.StartTime.ToUniversalTime() -eq ([datetime]$entry.started).ToUniversalTime()){Stop-Process -Id $entry.id;Wait-Process -Id $entry.id -Timeout 20 -ErrorAction SilentlyContinue;Write-Host "Stopped $($entry.name)"}
}
Remove-Item -LiteralPath $state
Write-Host 'Data retained. Start again to reuse the cluster and jobs.'
