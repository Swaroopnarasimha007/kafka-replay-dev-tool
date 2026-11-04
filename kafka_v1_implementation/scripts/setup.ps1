$ErrorActionPreference='Stop'
$labRoot=Split-Path $PSScriptRoot -Parent
$projectRoot=Split-Path $labRoot -Parent
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
 & "$projectRoot\mvnw.cmd" -B -f "$projectRoot\pom.xml" '-Dreplay.artifact-name=replay-console-lab' -DskipTests package
 if($LASTEXITCODE){throw 'Console build failed'}
} finally {Pop-Location}
Write-Host 'Installed Kafka server, clients, Java app and console JARs. No Docker required for native mode.'
