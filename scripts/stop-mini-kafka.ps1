$ErrorActionPreference = 'Stop'
$miniKafka = Join-Path (Split-Path $PSScriptRoot -Parent) 'kafka_v1_implementation'
& (Join-Path $miniKafka 'scripts/stop.ps1')
