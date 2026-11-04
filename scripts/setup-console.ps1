$ErrorActionPreference='Stop'
$root=Split-Path $PSScriptRoot -Parent
if(!$env:JAVA_HOME){
 $jdk=Get-ChildItem -LiteralPath "$env:ProgramFiles/Java" -Directory | Where-Object {Test-Path (Join-Path $_.FullName 'bin/javac.exe')} | Sort-Object Name -Descending | Select-Object -First 1
 if(!$jdk){throw 'Set JAVA_HOME to JDK 17 or newer'}
 $env:JAVA_HOME=$jdk.FullName
}
Push-Location $root
try {
 & .\mvnw.cmd -B '-Dreplay.artifact-name=replay-console-lab' -DskipTests package
 if($LASTEXITCODE){throw 'Console build failed'}
} finally {Pop-Location}
