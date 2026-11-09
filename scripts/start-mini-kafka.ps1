param([int]$BrokerPort = 19092, [int]$ControllerPort = 19093, [switch]$Rebuild)

$ErrorActionPreference = 'Stop'
$miniKafka = Join-Path (Split-Path $PSScriptRoot -Parent) 'kafka_v1_implementation'
if ($Rebuild -or !(Test-Path (Join-Path $miniKafka 'target/kafka-lab.jar'))) {
    & (Join-Path $miniKafka 'scripts/setup.ps1')
}

& (Join-Path $miniKafka 'scripts/start.ps1') -BrokerPort $BrokerPort -ControllerPort $ControllerPort
