$ErrorActionPreference='Stop'
$root=Split-Path $PSScriptRoot
$destination=Join-Path $root 'release'
New-Item -ItemType Directory -Path $destination -Force | Out-Null
Copy-Item -LiteralPath (Join-Path $root 'target\replay-console.jar') -Destination (Join-Path $destination 'replay-console.jar')
Copy-Item -LiteralPath (Join-Path $root 'config\application-live.example.yml') -Destination $destination
Copy-Item -LiteralPath (Join-Path $root 'docs\RELEASE_RUNBOOK.md') -Destination $destination
foreach($document in @('HANDOVER_STEP_3.md','STEP_3_REVISION.txt')) {
 $source=Join-Path $root "docs\$document"
 if(Test-Path -LiteralPath $source){Copy-Item -LiteralPath $source -Destination $destination}
}
$hash=(Get-FileHash (Join-Path $destination 'replay-console.jar') -Algorithm SHA256).Hash
Set-Content -LiteralPath (Join-Path $destination 'replay-console.jar.sha256') -Value "$hash  replay-console.jar"
"Release written to $destination; SHA256=$hash"
