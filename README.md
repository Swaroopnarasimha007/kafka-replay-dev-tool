# Kafka Replay Console — step 3

Build (JDK 17–25; initial build needs network access):

```powershell
$env:JAVA_HOME='C:\Program Files\Java\jdk-25.0.2'
.\mvnw.cmd clean package
java -jar target/replay-console.jar
```

Unix: `./mvnw clean package` then `java -jar target/replay-console.jar`.
Open http://localhost:8080 (deep link: `/console/records`). Only Java is needed to run
the packaged JAR. Maven downloads pinned Node/npm and packages React into the JAR.
The default is a loopback synthetic demo, not evidence of broker connectivity.

## Live deployment

Set `SPRING_PROFILES_ACTIVE=live`, `REPLAY_DB_URL` (JDBC PostgreSQL),
`REPLAY_DB_USER`, `REPLAY_DB_PASSWORD`, `REPLAY_ENCRYPTION_KEY` (base64 32-byte key),
`REPLAY_ALLOWED_HOSTS` (exact comma-separated bootstrap AND advertised DNS hostnames),
`REPLAY_OIDC_ISSUER`, `REPLAY_OIDC_CLIENT_ID`, and `REPLAY_OIDC_CLIENT_SECRET`.
Register redirect URI `http://localhost:8080/login/oauth2/code/console` for local testing;
use the deployment's HTTPS URL in production. The trusted OIDC provider must issue an
ID-token `roles` array with `Viewer`, `Operator`, or `Admin` entries.

Use a deployment egress firewall restricted to approved broker IPs/networks. The
application validates hostnames before bootstrap and reported advertised endpoints;
Kafka's internal metadata connections and DNS rebinding additionally require network
enforcement. Host allowlisting alone is not a network sandbox.

Remote profiles use SASL_SSL / SCRAM-SHA-512 with hostname verification and either
system trust or a private CA PEM. Local PLAINTEXT requires
`REPLAY_ALLOW_PLAINTEXT=true`. Broker credentials stay server-side, AES-256-GCM
encrypted in PostgreSQL. Keep the key stable across restarts; back it up separately.
Kafka logging is disabled to prevent client diagnostic messages leaking credentials.
Separate replay credentials are stored but never used by this read-only step.
Browsing/planning never publishes or commits offsets. Step 3 adds an explicitly
started transactional worker using separate replay credentials.

Viewer can browse masked records. Operator can also reveal bytes. Admin additionally
tests and saves profiles. Browser POSTs require the session CSRF token. Profile save
accepts only a short-lived single-use successful test token; edits require a new test.
Only profile identity and bootstrap endpoints are returned from profile APIs.

Minimum browse ACLs: cluster DESCRIBE, topic DESCRIBE and READ on discovered DLTs.
No group ACL is needed because reads use direct assignment without group membership.
Restrict discovery with `replay.topic-pattern` and/or explicit `replay.topics`.

See [API contract](docs/API.md) and [step handover](docs/HANDOVER_STEP_1.md).

## Plan and approve a replay

As Admin, open **Manage mappings**, choose a destination, list the exact retry/DLT
headers to remove, and attest the source contains original Spring DLT key/value bytes.
As an authorized Operator, select the mapping, set browse filters, rate and scan
limit, and click **Freeze preview using current filters**. Review count, destination,
policy changes, boundaries and digest. Explicitly select the displayed positions
for a partial scan, supply a reason and choose whether to run a one-record canary,
then approve. Reopen the job from **Replay jobs** after restart.

Approval appends no Kafka records. Click **Start one-record canary**, inspect its
committed receipt, then release remaining records. Live execution additionally
requires `REPLAY_WORKER_ENABLED=true` and a provisioned receipt topic. Demo persists
mappings/manifests/jobs/outputs in `./data/replay-demo*`; keep this
directory with the release for restart persistence. Demo uses the same planner and
approval code with synthetic source bytes. Live mode uses PostgreSQL.

Step 2 commands:
```powershell
.\scripts\smoke-planner.ps1
python scripts\verify_manifest.py docs\fixtures\source-500.json docs\fixtures\approved-500-verifier.json
```

See [planning API and state contract](docs/STEP_2_API.md) and
[handover to step 3](docs/HANDOVER_STEP_2.md) for exact storage/digest rules,
tests, fixtures, known pending environment checks and next implementation scope.

## Execute and verify

The [release runbook](docs/RELEASE_RUNBOOK.md) supersedes the historical step-2
execution-disabled contract. It covers Start/canary/release/pause/resume, receipt
retention, minimum ACLs, live/remote configuration and the exact recovery scope.

```powershell
.\scripts\smoke-worker.ps1
.\scripts\test-crash-matrix.ps1
.\scripts\package-release.ps1
```

The crash matrix kills a separate worker JVM at three transaction checkpoints and
independently verifies 500 committed outputs. The [final handover](docs/HANDOVER_STEP_3.md)
records fixture evidence separately from PostgreSQL, secured-broker and remote checks.
