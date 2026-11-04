param([string]$Jar="$PSScriptRoot\..\target\replay-console.jar",[int]$Port=18081)
$ErrorActionPreference='Stop'
$java=(Get-Command java).Source
$release=Join-Path ([IO.Path]::GetTempPath()) ('replay-planner-'+[guid]::NewGuid())
New-Item -ItemType Directory -Path $release | Out-Null
Copy-Item -LiteralPath $Jar -Destination (Join-Path $release 'replay-console.jar')
$base="http://localhost:$Port/api"
function Start-Console {
 $script:process=Start-Process -FilePath $java -ArgumentList @('-jar','replay-console.jar',"--server.port=$Port") -WorkingDirectory $release -WindowStyle Hidden -PassThru -RedirectStandardOutput (Join-Path $release 'stdout.log') -RedirectStandardError (Join-Path $release 'stderr.log')
 for($i=0;$i -lt 60;$i++) {
  if($process.HasExited){throw "Application exited; inspect $release"}
  try {$script:session=Invoke-RestMethod "$base/session" -SessionVariable web; $script:web=$web; return} catch {Start-Sleep -Milliseconds 500}
 }
 throw 'Application startup timeout'
}
function Post-Json($route,$value) {
 Invoke-RestMethod "$base$route" -Method Post -ContentType 'application/json' -Body ($value|ConvertTo-Json -Depth 20 -Compress) -WebSession $web -Headers @{'X-XSRF-TOKEN'=$session.csrfToken}
}
try {
 Start-Console
 $mapping=Post-Json '/replay/mappings' @{profileId='demo';source='orders.DLT';target='orders';extractor='SPRING_DLT_BYTES_V1';partitionPolicy='ORIGINAL_PARTITION_V1';headerPolicy='EXPLICIT_REMOVE_V1';removeHeaders=@('kafka_dlt-original-topic','kafka_dlt-original-partition');allowedSubjects=@();bytePreservingAttested=$true}
 $preview=Post-Json '/replay/previews' @{mappingId=$mapping.id;filter=@{};rate=100;scanLimit=1000}
 if($preview.count -ne 60 -or $preview.partial){throw 'Unexpected preview selection'}
 $approval=@{previewId=$preview.id;digest=$preview.digest;idempotencyKey=[guid]::NewGuid().ToString();reason='Release smoke approved synthetic selection';canary=$true;acceptPartial=$false}
 $job=Post-Json '/replay/approvals' $approval
 $duplicate=Post-Json '/replay/approvals' $approval
 if($duplicate.id -ne $job.id){throw 'Approval was not idempotent'}
 Stop-Process -Id $process.Id; $process.WaitForExit()
 Start-Console
 $reopened=Invoke-RestMethod "$base/replay/jobs/$($job.id)" -WebSession $web
 if($reopened.digest -ne $job.digest -or $reopened.count -ne 60 -or $reopened.state -ne 'APPROVED' -or $reopened.workerEnabled){throw 'Approved job changed across process restart'}
 if($reopened.mapping.identity.targetId -ne $job.mapping.identity.targetId){throw 'Target identity changed'}
 $evidence=Join-Path $PSScriptRoot '..\target\step2-evidence'
 New-Item -ItemType Directory -Path $evidence -Force | Out-Null
 $reopened|ConvertTo-Json -Depth 20|Set-Content (Join-Path $evidence 'restarted-job.json')
 "PASS: packaged JAR mapping → fixed 60-record preview → approval → identical retry → process restart → same count/digest/target/policy; worker disabled; release=$release"
} finally {
 if($process -and !$process.HasExited){Stop-Process -Id $process.Id; $process.WaitForExit()}
}
