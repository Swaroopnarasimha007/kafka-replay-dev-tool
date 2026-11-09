param([int]$BrokerPort=19092,[int]$ControllerPort=19093,[string]$JavaHome=$env:JAVA_HOME)
$ErrorActionPreference='Stop'
$labRoot=Split-Path $PSScriptRoot -Parent
if(!$JavaHome){$JavaHome=(Get-ChildItem -LiteralPath "$env:ProgramFiles\Java" -Directory | Where-Object {Test-Path (Join-Path $_.FullName 'bin\java.exe')} | Sort-Object Name -Descending | Select-Object -First 1).FullName}
$java=Join-Path $JavaHome 'bin\java.exe'
if(!(Test-Path -LiteralPath $java)){throw 'Set JAVA_HOME to JDK 17 or newer'}
if(!(Test-Path "$labRoot\target\kafka-lab.jar")){throw 'Run scripts\setup.ps1 first'}
foreach($port in @($BrokerPort,$ControllerPort)){
 if($port -lt 1024 -or $port -gt 65535){throw 'Ports must be 1024..65535'}
 if(Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue){throw "Port $port is occupied; stop your previous lab or choose other ports"}
}
if((@($BrokerPort,$ControllerPort)|Select-Object -Unique).Count -ne 2){throw 'Choose two different ports'}
New-Item -ItemType Directory "$labRoot\data" -Force | Out-Null
$state = Join-Path $labRoot 'data\processes.json'
if(Test-Path $state){throw 'Existing process record; run stop.ps1 before starting again'}
$broker=Get-Content "$labRoot\config\broker.properties" -Raw
$broker=$broker.Replace('19092',"$BrokerPort").Replace('19093',"$ControllerPort").Replace('./data/kafka',((Join-Path $labRoot 'data\kafka').Replace('\','/')))
Set-Content "$labRoot\data\broker-local.properties" $broker
# KAFKA_BOOTSTRAP overrides only connectivity; live edits stay in config/demo.properties.
$clusterFile=Join-Path $labRoot 'data\cluster.id'
$meta=Join-Path $labRoot 'data\kafka\meta.properties'
if(Test-Path $meta){$existing=(Get-Content $meta | Where-Object {$_ -like 'cluster.id=*'}).Substring(11);Set-Content $clusterFile $existing}
if(!(Test-Path $clusterFile)){[Convert]::ToBase64String([guid]::NewGuid().ToByteArray()).TrimEnd('=').Replace('+','-').Replace('/','_')|Set-Content $clusterFile}
$previous=$env:KAFKA_BOOTSTRAP
$brokerProcess = $null
$demoProcess = $null
Push-Location $labRoot
try {
 & $java -jar target/kafka-lab.jar format (Get-Content $clusterFile -Raw).Trim() data/broker-local.properties
 if($LASTEXITCODE){throw 'Broker format failed'}
 $env:KAFKA_BOOTSTRAP="localhost:$BrokerPort"
 $brokerProcess=Start-Process $java -ArgumentList @('-Xmx512m','-jar','target/kafka-lab.jar','broker','data/broker-local.properties') -WindowStyle Hidden -PassThru -RedirectStandardOutput data/broker.log -RedirectStandardError data/broker-error.log
 for($i=0;$i -lt 60;$i++){
  if($brokerProcess.HasExited){throw 'Broker exited. See data/broker-error.log'}
  try{$socket=[Net.Sockets.TcpClient]::new('localhost',$BrokerPort);$socket.Dispose();break}catch{Start-Sleep -Milliseconds 500}
 }
 $demoProcess = Start-Process $java -ArgumentList @('-Xmx256m','-jar','target/kafka-lab.jar','run','config/demo.properties') -NoNewWindow -PassThru
 $managed = @(
  @{name = 'Mini Kafka broker'; id = $brokerProcess.Id; started = $brokerProcess.StartTime.ToUniversalTime().ToString('o')}
  @{name = 'Mini Kafka application'; id = $demoProcess.Id; started = $demoProcess.StartTime.ToUniversalTime().ToString('o')}
 )
 ConvertTo-Json -InputObject $managed | Set-Content -LiteralPath $state
 Write-Host "Kafka bootstrap: localhost:$BrokerPort (PLAINTEXT). Press Ctrl+C to stop Mini Kafka."
 Wait-Process -InputObject $demoProcess
} finally {
 foreach($process in @($demoProcess,$brokerProcess)) {
  if($process -and !$process.HasExited) {
   Stop-Process -InputObject $process -ErrorAction SilentlyContinue
   $process.WaitForExit()
  }
 }
 if(Test-Path $state){Remove-Item -LiteralPath $state -ErrorAction SilentlyContinue}
 $env:KAFKA_BOOTSTRAP=$previous
 Pop-Location
}
