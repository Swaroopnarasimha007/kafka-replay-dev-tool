$ErrorActionPreference='Stop'
Push-Location (Join-Path $PSScriptRoot '..')
try {
 .\mvnw.cmd -B test '-Dtest=WorkerBrokerTest'
 if($LASTEXITCODE -ne 0){throw 'Real-broker worker tests failed'}
 foreach($point in @('CANARY_ACKNOWLEDGED','BEFORE_COMMIT','AFTER_COMMIT_BEFORE_PROJECTION')) {
  python scripts\verify_execution.py docs\fixtures\source-500.json "target\step3-evidence\execution-$point.json" "target\step3-evidence\summary-$point.json"
  if($LASTEXITCODE -ne 0){throw "Independent verifier failed at $point"}
 }
} finally {Pop-Location}
