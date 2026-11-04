$ErrorActionPreference='Stop'
$labRoot=Split-Path $PSScriptRoot -Parent
if(Test-Path "$labRoot\data\processes.json"){throw "Run scripts\stop.ps1 before rebuilding the running lab"}
if(!$env:JAVA_HOME){
 $jdk=Get-ChildItem -LiteralPath "$env:ProgramFiles\Java" -Directory -ErrorAction SilentlyContinue | Where-Object {Test-Path (Join-Path $_.FullName 'bin\javac.exe')} | Sort-Object Name -Descending | Select-Object -First 1
 if(!$jdk){throw 'Install JDK 17 or newer and set JAVA_HOME.'}
 $env:JAVA_HOME=$jdk.FullName
}
Push-Location $labRoot
try {
 & .\mvnw.cmd -B package
 if($LASTEXITCODE){throw 'Kafka lab build failed'}
} finally {Pop-Location}
Write-Host 'Kafka broker and demo application built. Run scripts/start.ps1.'
