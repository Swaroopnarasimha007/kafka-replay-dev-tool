$ErrorActionPreference='Stop'
$labRoot=Split-Path $PSScriptRoot -Parent
$state=Join-Path $labRoot 'data\processes.json'
if(!(Test-Path $state)){Write-Host 'No managed lab processes';return}
$entries=@(Get-Content $state -Raw|ConvertFrom-Json)
[array]::Reverse($entries)
foreach($entry in $entries){
 $p=Get-Process -Id $entry.id -ErrorAction SilentlyContinue
 if($p -and $p.StartTime.ToUniversalTime() -eq ([datetime]$entry.started).ToUniversalTime()){Stop-Process -Id $entry.id;Wait-Process -Id $entry.id -Timeout 20 -ErrorAction SilentlyContinue;Write-Host "Stopped $($entry.name)"}
}
Remove-Item -LiteralPath $state
Write-Host 'Data retained. Start again to reuse the cluster and jobs.'
