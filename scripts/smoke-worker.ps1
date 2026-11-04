param([string]$Jar="$PSScriptRoot\..\target\replay-console.jar",[int]$Port=18083)
$ErrorActionPreference='Stop'
$java=(Get-Command java).Source
$release=Join-Path ([IO.Path]::GetTempPath()) ('replay-worker-'+[guid]::NewGuid())
New-Item -ItemType Directory -Path $release | Out-Null
Copy-Item -LiteralPath $Jar -Destination (Join-Path $release 'replay-console.jar')
$base="http://localhost:$Port/api"
function Start-Console {
 $script:process=Start-Process -FilePath $java -ArgumentList @('-jar','replay-console.jar',"--server.port=$Port") -WorkingDirectory $release -WindowStyle Hidden -PassThru -RedirectStandardOutput (Join-Path $release 'stdout.log') -RedirectStandardError (Join-Path $release 'stderr.log')
 for($i=0;$i -lt 60;$i++) {
  if($process.HasExited){throw "Application exited; inspect $release"}
  try {$script:session=Invoke-RestMethod "$base/session" -SessionVariable web; $script:web=$web;return} catch {Start-Sleep -Milliseconds 250}
 }
 throw 'Startup timed out'
}
function Post-Json($route,$value) {
 Invoke-RestMethod "$base$route" -Method Post -ContentType 'application/json' -Body ($value|ConvertTo-Json -Depth 20 -Compress) -WebSession $web -Headers @{'X-XSRF-TOKEN'=$session.csrfToken}
}
function Wait-State($id,$state) {
 for($i=0;$i -lt 100;$i++) {
  $result=Invoke-RestMethod "$base/replay/jobs/$id/dashboard" -WebSession $web
  if($result.job.state -eq $state){return $result}
  if($result.job.state -in @('BLOCKED','FAILED')){throw "Worker stopped: $($result.statusDetail)"}
  Start-Sleep -Milliseconds 200
 }
 throw "Worker did not reach $state"
}
try {
 Start-Console
 $mapping=Post-Json '/replay/mappings' @{profileId='demo';source='orders.DLT';target='orders';extractor='SPRING_DLT_BYTES_V1';partitionPolicy='ORIGINAL_PARTITION_V1';headerPolicy='EXPLICIT_REMOVE_V1';removeHeaders=@('kafka_dlt-original-topic','kafka_dlt-original-partition');allowedSubjects=@();bytePreservingAttested=$true}
 $preview=Post-Json '/replay/previews' @{mappingId=$mapping.id;filter=@{};rate=100;scanLimit=1000}
 $job=Post-Json '/replay/approvals' @{previewId=$preview.id;digest=$preview.digest;idempotencyKey=[guid]::NewGuid().ToString();reason='Packaged release canary and restart validation';canary=$true;acceptPartial=$false}
 Post-Json "/replay/jobs/$($job.id)/start" @{} | Out-Null
 $canary=Wait-State $job.id 'AWAITING_RELEASE'
 if($canary.published -ne 1){throw 'Canary did not publish exactly one synthetic output'}
 Stop-Process -Id $process.Id; $process.WaitForExit()
 Start-Console
 $restored=Wait-State $job.id 'AWAITING_RELEASE'
 if($restored.published -ne 1 -or $restored.job.digest -ne $job.digest){throw 'Canary changed across process restart'}
 Post-Json "/replay/jobs/$($job.id)/release" @{} | Out-Null
 Post-Json "/replay/jobs/$($job.id)/release" @{} | Out-Null
 $done=Wait-State $job.id 'COMPLETED'
 if($done.published -ne 60 -or $done.skipped -ne 0){throw 'Final committed counts mismatch'}
 $positions=@($done.receipts.receipt.outcomes.position | Sort-Object -Unique)
 if($positions.Count -ne 60){throw 'Receipt manifest positions mismatch'}
 $destinationCoordinates=@($done.receipts.receipt.outcomes | ForEach-Object {"$($_.destinationPartition):$($_.destinationOffset)"} | Sort-Object -Unique)
 if($destinationCoordinates.Count -ne 60){throw 'Duplicate destination coordinate'}
 $evidence=Join-Path $PSScriptRoot '..\target\step3-evidence'
 New-Item -ItemType Directory -Path $evidence -Force | Out-Null
 $done|ConvertTo-Json -Depth 40|Set-Content (Join-Path $evidence 'demo-release.json')
 "PASS: isolated JAR → approve → one canary → kill/restart → same canary → repeated release → 60 unique synthetic outputs with committed receipts; release=$release"
} finally {if($process -and !$process.HasExited){Stop-Process -Id $process.Id;$process.WaitForExit()}}
