param([int]$BrokerPort=19092,[int]$ControllerPort=19093,[int]$ConsolePort=8088,[string]$JavaHome=$env:JAVA_HOME)
$ErrorActionPreference='Stop'
$labRoot=Split-Path $PSScriptRoot -Parent
$projectRoot=Split-Path $labRoot -Parent
if(!$JavaHome){$JavaHome=(Get-ChildItem -LiteralPath "$env:ProgramFiles\Java" -Directory | Where-Object {Test-Path (Join-Path $_.FullName 'bin\java.exe')} | Sort-Object Name -Descending | Select-Object -First 1).FullName}
$java=Join-Path $JavaHome 'bin\java.exe'
if(!(Test-Path -LiteralPath $java)){throw 'Set JAVA_HOME to JDK 17 or newer'}
if(!(Test-Path "$labRoot\target\kafka-lab.jar") -or !(Test-Path "$projectRoot\target\replay-console-lab.jar")){throw 'Run scripts\setup.ps1 first'}
foreach($port in @($BrokerPort,$ControllerPort,$ConsolePort)){
 if($port -lt 1024 -or $port -gt 65535){throw 'Ports must be 1024..65535'}
 if(Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue){throw "Port $port is occupied; stop your previous lab or choose other ports"}
}
if((@($BrokerPort,$ControllerPort,$ConsolePort)|Select-Object -Unique).Count -ne 3){throw 'Choose three different ports'}
New-Item -ItemType Directory "$labRoot\data" -Force | Out-Null
if(Test-Path "$labRoot\data\processes.json"){throw 'Existing process record; run stop.ps1 before starting again'}
$saved=Join-Path $labRoot 'data\lab-settings.json'
if(!(Test-Path $saved)){@{username='lab';password=([guid]::NewGuid().ToString('N')+[guid]::NewGuid().ToString('N'))}|ConvertTo-Json|Set-Content $saved}
$login=Get-Content $saved -Raw|ConvertFrom-Json
$broker=Get-Content "$labRoot\config\broker.properties" -Raw
$broker=$broker.Replace('19092',"$BrokerPort").Replace('19093',"$ControllerPort").Replace('./data/kafka',((Join-Path $labRoot 'data\kafka').Replace('\','/')))
Set-Content "$labRoot\data\broker-local.properties" $broker
# KAFKA_BOOTSTRAP overrides only connectivity; live edits stay in config/demo.properties.
$clusterFile=Join-Path $labRoot 'data\cluster.id'
$meta=Join-Path $labRoot 'data\kafka\meta.properties'
if(Test-Path $meta){$existing=(Get-Content $meta | Where-Object {$_ -like 'cluster.id=*'}).Substring(11);Set-Content $clusterFile $existing}
if(!(Test-Path $clusterFile)){[Convert]::ToBase64String([guid]::NewGuid().ToByteArray()).TrimEnd('=').Replace('+','-').Replace('/','_')|Set-Content $clusterFile}
$started=@()
$names=@('KAFKA_BOOTSTRAP','LAB_PASSWORD','LAB_USER','LAB_PORT','SPRING_PROFILES_ACTIVE')
$previous=@{};foreach($name in $names){$previous[$name]=[Environment]::GetEnvironmentVariable($name,'Process')}
Push-Location $labRoot
try {
 & $java -jar target/kafka-lab.jar format (Get-Content $clusterFile -Raw).Trim() data/broker-local.properties
 if($LASTEXITCODE){throw 'Broker format failed'}
 $env:KAFKA_BOOTSTRAP="localhost:$BrokerPort"
 $env:LAB_PASSWORD=$login.password;$env:LAB_USER=$login.username;$env:LAB_PORT="$ConsolePort";$env:SPRING_PROFILES_ACTIVE='lab'
 function Launch($name,$arguments){
  $p=Start-Process -FilePath $java -ArgumentList $arguments -WorkingDirectory $labRoot -WindowStyle Hidden -PassThru -RedirectStandardOutput "$labRoot\data\$name.log" -RedirectStandardError "$labRoot\data\$name-error.log"
  return @{id=$p.Id;started=$p.StartTime.ToUniversalTime().ToString('o');name=$name}
 }
 $started+=Launch 'broker' @('-Xms256m','-Xmx512m','-jar','target/kafka-lab.jar','broker','data/broker-local.properties')
 $ready=$false
 for($i=0;$i -lt 60;$i++){try{$tcp=[Net.Sockets.TcpClient]::new();$tcp.Connect('127.0.0.1',$BrokerPort);$tcp.Dispose();$ready=$true;break}catch{Start-Sleep -Milliseconds 500}}
 if(!$ready){throw 'Broker startup timed out; check data/broker-error.log'}
 & $java -jar target/kafka-lab.jar inspect config/demo.properties
 if($LASTEXITCODE){throw 'Broker initialization failed'}
 $started+=Launch 'demo' @('-Xmx256m','-jar','target/kafka-lab.jar','run','config/demo.properties')
 $consoleJar=(Join-Path $projectRoot 'target\replay-console-lab.jar')
 $started+=Launch 'console' @('-Xmx384m','-jar',('"'+$consoleJar+'"'))
 $started|ConvertTo-Json|Set-Content "$labRoot\data\processes.json"
 $ready=$false
 $pair="$($login.username):$($login.password)";$auth=[Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($pair))
 for($i=0;$i -lt 90;$i++){try{$null=Invoke-RestMethod "http://127.0.0.1:$ConsolePort/api/session" -Headers @{Authorization="Basic $auth"};$ready=$true;break}catch{Start-Sleep -Milliseconds 500}}
 if(!$ready){throw 'Console startup timed out; check data/console.log'}
 Write-Host "Console: http://localhost:$ConsolePort"
 Write-Host "Login is stored in $saved (username: $($login.username))"
 Write-Host "Kafka bootstrap: localhost:$BrokerPort | protocol: PLAINTEXT | credentials: none"
 Write-Host 'Choose Kafka local demo in the console. Connection details: data/connection.json'
} catch {
 foreach($entry in $started){$p=Get-Process -Id $entry.id -ErrorAction SilentlyContinue;if($p -and $p.StartTime.ToUniversalTime() -eq ([datetime]$entry.started).ToUniversalTime()){Stop-Process -Id $entry.id}}
 if(Test-Path "$labRoot\data\processes.json"){Remove-Item -LiteralPath "$labRoot\data\processes.json"}
 throw
} finally {
 Pop-Location
 foreach($name in $names){[Environment]::SetEnvironmentVariable($name,$previous[$name],'Process')}
}
