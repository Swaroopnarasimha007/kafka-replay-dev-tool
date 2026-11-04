param([int]$ConsolePort=8088)
$ErrorActionPreference='Stop'
$root=Split-Path $PSScriptRoot -Parent
$consoleDirectory=$root
if(!(Test-Path "$root/target/replay-console-lab.jar")){throw "Run scripts/setup-console.ps1 first"}
New-Item -ItemType Directory -Force "$consoleDirectory/data" | Out-Null
$saved=Join-Path $consoleDirectory 'data\console-login.json'
if(!(Test-Path $saved)){@{username='lab';password=([guid]::NewGuid().ToString('N')+[guid]::NewGuid().ToString('N'))}|ConvertTo-Json|Set-Content $saved}
$login=Get-Content $saved -Raw|ConvertFrom-Json
$names=@('LAB_PASSWORD','LAB_USER','LAB_PORT','SPRING_PROFILES_ACTIVE')
$previous=@{}
foreach($name in $names){$previous[$name]=[Environment]::GetEnvironmentVariable($name,'Process')}
Push-Location $consoleDirectory
try {
 $env:LAB_PASSWORD=$login.password
 $env:LAB_USER=$login.username
 $env:LAB_PORT="$ConsolePort"
 $env:SPRING_PROFILES_ACTIVE='lab'
 Write-Host "Console: http://localhost:$ConsolePort | Login file: $saved | Ctrl+C to stop"
 & java -jar "$root/target/replay-console-lab.jar"
 if($LASTEXITCODE){throw 'Console exited'}
} finally {
 Pop-Location
 foreach($name in $names){[Environment]::SetEnvironmentVariable($name,$previous[$name],'Process')}
}
