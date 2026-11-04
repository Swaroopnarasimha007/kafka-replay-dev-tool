$ErrorActionPreference='Stop'
$repo=Split-Path $PSScriptRoot -Parent
$details=Join-Path $repo 'data/connection.json'
if(!(Test-Path $details)){throw 'Start Kafka with scripts/start.ps1 first. It writes data/connection.json after connecting to the broker.'}
$info=Get-Content $details -Raw | ConvertFrom-Json
[pscustomobject]@{
 Name='Kafka demo'
 Bootstrap=$info.bootstrap
 Protocol=$info.protocol
 Authentication='None on the default loopback PLAINTEXT listener'
 KafkaUsername='Leave blank'
 KafkaPassword='Leave blank'
 ClusterId=$info.clusterId
 AdvertisedBrokers=($info.brokers -join ',')
 Source=$info.source
 Destination=$info.target
} | Format-List
