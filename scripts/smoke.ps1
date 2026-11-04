param([string]$Jar = "$PSScriptRoot\..\target\replay-console.jar", [int]$Port=18080)
$ErrorActionPreference='Stop'
$java=(Get-Command java).Source
$release=Join-Path ([IO.Path]::GetTempPath()) ('replay-release-'+[guid]::NewGuid())
New-Item -ItemType Directory -Path $release | Out-Null
Copy-Item -LiteralPath $Jar -Destination (Join-Path $release 'replay-console.jar')
$previousPath=$env:PATH
try {
 $env:PATH="$env:SystemRoot\System32"
 $process=Start-Process -FilePath $java -ArgumentList @('-jar','replay-console.jar',"--server.port=$Port") -WorkingDirectory $release -WindowStyle Hidden -PassThru -RedirectStandardOutput (Join-Path $release 'stdout.log') -RedirectStandardError (Join-Path $release 'stderr.log')
 $ready=$false
 for($i=0;$i -lt 60;$i++){
  if($process.HasExited){throw "Release exited; see $release"}
  try {$session=Invoke-RestMethod "http://localhost:$Port/api/session" -SessionVariable web;$ready=$true;break} catch {Start-Sleep -Milliseconds 500}
 }
 if(!$ready){throw 'Startup timed out'}
 if($session.mode -ne 'synthetic demo'){throw 'Wrong mode'}
 foreach($route in @('/','/console/records')) {
  $html=(Invoke-WebRequest "http://localhost:$Port$route").Content
  if($html -notmatch '/assets/[^" ]+\.js'){throw "Missing packaged UI: $route"}
 }
 $asset=$Matches[0]
 if((Invoke-WebRequest "http://localhost:$Port$asset").Content.Length -lt 1000){throw 'Missing built JavaScript'}
 $body='{"limit":5,"scanLimit":5}'
 $page=Invoke-RestMethod "http://localhost:$Port/api/profiles/demo/topics/orders.DLT/browse" -Method Post -ContentType 'application/json' -Body $body -WebSession $web -Headers @{'X-XSRF-TOKEN'=$session.csrfToken}
 if($page.records.Count -ne 5 -or !$page.truncated){throw 'Browse contract failed'}
 "PASS: isolated JAR; UI, nested route, packaged JS, session and masked browse; Node absent from PATH; release=$release"
} finally {
 $env:PATH=$previousPath
 if($process -and !$process.HasExited){Stop-Process -Id $process.Id; $process.WaitForExit()}
}
