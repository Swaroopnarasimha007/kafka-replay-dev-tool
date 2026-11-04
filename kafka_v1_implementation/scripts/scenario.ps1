param([ValidateSet('replay-success','consumer-failure','transport-failure','no-errors')][string]$Name='replay-success')
$ErrorActionPreference='Stop'
$labRoot=Split-Path $PSScriptRoot -Parent
$path=Join-Path $labRoot 'config\demo.properties'
$settings=Get-Content $path
$codes='VALIDATION,BUSINESS_REJECT,TRANSIENT,INVALID_JSON'
$updates=@{'producer.run.id'=[guid]::NewGuid().ToString('N');'errors.forbidden'='';'topic.main.max.message.bytes'='1048588';'topic.dlt.max.message.bytes'='2097152';'topic.main.retention.ms'='-1';'topic.dlt.retention.ms'='-1'}
foreach($i in 1,2){
 $updates["producer.$i.enabled"]='true';$updates["consumer.$i.enabled"]='true'
 $updates["producer.$i.seed.dlt"]='false';$updates["producer.$i.payload.bytes"]='128'
 $updates["producer.$i.count"]='30';$updates["producer.$i.error.every"]='2'
 $updates["producer.$i.error"]='VALIDATION';$updates["consumer.$i.initial.errors"]=$codes
 $updates["consumer.$i.replay.errors"]=''
 if($Name -eq 'consumer-failure'){$updates["consumer.$i.replay.errors"]=$codes}
 if($Name -eq 'transport-failure'){$updates["producer.$i.seed.dlt"]='true';$updates["producer.$i.payload.bytes"]='8192';$updates['topic.main.max.message.bytes']='512'}
 if($Name -eq 'no-errors'){$updates["producer.$i.error"]='NONE';$updates["consumer.$i.initial.errors"]='';$updates['errors.forbidden']=$codes}
}
foreach($key in $updates.Keys){$found=$false;$settings=@($settings|ForEach-Object{if($_ -match ('^'+[regex]::Escape($key)+'=')){$found=$true;"$key=$($updates[$key])"}else{$_}});if(!$found){$settings+="$key=$($updates[$key])"}}
# Replace the file atomically; mount the containing config directory in Docker.
$temp=Join-Path (Split-Path $path) ('scenario-'+[guid]::NewGuid().ToString('N')+'.tmp')
Set-Content $temp $settings -Encoding ascii
Move-Item -LiteralPath $temp -Destination $path -Force
Write-Host "Applied $Name; the running app reloads within a second and emits a new finite batch. Existing Kafka records remain."
