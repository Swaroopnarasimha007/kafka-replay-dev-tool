# Kafka Replay Console — run and operate

## One-command release

With Java 17–25 installed, copy `replay-console.jar` into a release directory and run:

```sh
java -jar replay-console.jar
```

Open `http://localhost:8080/console/records`. The compiled React UI, REST API, planner
and worker are inside this JAR. Runtime needs no Node, Maven or second frontend
process. Default mode is loopback-only, explicitly labeled synthetic demo. Its
`data/replay-demo*` files persist mappings, jobs, synthetic outputs and receipt state.

Build from source with `./mvnw -B package` or `mvnw.cmd -B package`. On this Windows
machine, set `JAVA_HOME=C:\Program Files\Java\jdk-25.0.2`. Then run
`scripts/package-release.ps1` to create the local distributable under `release/`.
Run a release copy rather than the `target` JAR while rebuilding on Windows.

## Demo script

1. Launch and select the synthetic cluster and `orders.DLT`.
2. Browse masked records; reveal a binary value, tombstone or repeated headers.
3. Open **Manage mappings**, choose `orders`, configure exact header removals and
   attest byte-preserving Spring DLT records. Create the mapping.
4. Set filters/rate/scan cap, freeze a preview and review its count, destination,
   boundaries, policy and digest. Select all listed positions for a partial scan.
5. Give an approval reason and choose a one-record canary. Approve the exact set.
6. Click **Start one-record canary**. Wait for AWAITING_RELEASE and one output receipt.
7. Stop and restart the JAR in the same directory. Reopen the job; the canary remains
   one record. Click **Release remaining approved records**.
8. Pause/resume at batch boundaries if desired. Observe COMPLETED or explicit skipped
   outcomes under COMPLETED_WITH_EXCEPTIONS. Expand the receipt audit to trace source
   coordinates to acknowledged destination offsets.

Demo output is simulated and never sent to Kafka. The same worker orchestration,
leases, approval controls and projection code run with a synthetic authority adapter.
An automated version is `scripts/smoke-worker.ps1`; it also kills/restarts the JAR,
repeats release and verifies all 60 synthetic coordinates are unique.

For a disposable public synthetic demo, explicitly override the loopback binding
behind a TLS reverse proxy and use an ephemeral database:

```sh
java -jar replay-console.jar --server.address=0.0.0.0 --spring.datasource.url=jdbc:h2:mem:public-demo
```

Public demo has shared synthetic state and no login. Use synthetic approval reasons;
never use this profile for live clusters. Real cluster registration is disabled in
demo mode. The normal no-configuration launch remains loopback-only.

## Live onboarding

Kafka and PostgreSQL run independently. Set:

- `SPRING_PROFILES_ACTIVE=live`
- `REPLAY_DB_URL` (PostgreSQL JDBC URL), `REPLAY_DB_USER`, `REPLAY_DB_PASSWORD`
- `REPLAY_ENCRYPTION_KEY` (stable base64-encoded 32-byte AES key)
- `REPLAY_OIDC_ISSUER`, `REPLAY_OIDC_CLIENT_ID`, `REPLAY_OIDC_CLIENT_SECRET`
- `REPLAY_ALLOWED_HOSTS` (exact comma-separated bootstrap AND advertised DNS names)
- `REPLAY_WORKER_ENABLED=true` after provisioning worker ACLs and the receipt topic
- Optional `REPLAY_RECEIPTS_TOPIC` (default `_replay_console_receipts`)

The trusted identity provider supplies `roles` with Viewer/Operator/Admin entries.
Register `/login/oauth2/code/console` on the deployment origin as its redirect URI.
Use the deployment's HTTPS origin and a correctly configured trusted reverse proxy.
`config/application-live.example.yml` is a secret-free configuration example. Live
mode fails startup on missing DB/OIDC/key/allowlist configuration; it never falls
back to synthetic mode. Worker execution is an additional explicit live setting.

As Admin, add a cluster using Kafka `host:port` endpoints, not dashboard URLs. Test
then save. Remote profiles use SCRAM-SHA-512 over verified TLS; submit the private CA
PEM if needed. Enter a separate replay username/password. The browse principal is
used for browsing/planning and optional group metrics; the replay principal is used
for worker metadata, source revalidation, transactional writes and receipt recovery.
Saved credentials are encrypted server-side and not returned by the API.

For local development only, `REPLAY_ALLOW_PLAINTEXT=true` enables a PLAINTEXT profile
without separate SASL identities. Local and remote brokers use the same client and
transactional worker paths. Production egress controls must restrict outbound broker
IPs/networks; hostname allowlisting alone does not sandbox Kafka metadata connections.

## Receipt topic and ACLs

Provision the receipt topic before starting jobs: exactly one partition, delete-only
cleanup, `retention.bytes=-1`, and `retention.ms=-1` or at least the configured
recoverability window (default 604800000 ms / seven days). Use appropriate replication
and min ISR for the deployment. A one-broker development fixture also needs broker
`transaction.state.log.replication.factor=1` and `transaction.state.log.min.isr=1`.
Production clusters should retain their normal replication/durability settings.

Example provisioning using an already-authorized Kafka administrator:

```sh
kafka-topics.sh --bootstrap-server "$BROKERS" --command-config admin.properties --create --topic _replay_console_receipts --partitions 1 --replication-factor 3 --config cleanup.policy=delete --config retention.ms=604800000 --config retention.bytes=-1
```

Minimum intended permissions, scoped to approved resources:

| Principal | Resource | Operations |
| --- | --- | --- |
| Browse | Cluster | Describe metadata |
| Browse | Approved DLTs | Read, Describe |
| Browse (optional metrics) | Named group / destination topics | Describe group offsets / Describe topics |
| Replay | Cluster | Describe metadata |
| Replay | Approved DLTs | Read, Describe |
| Replay | Approved destinations | Write, Describe |
| Replay | Receipt topic | Read, Write, Describe, DescribeConfigs |
| Replay | Transactional IDs prefixed `replay-` | Write, Describe |

No group membership or offset commits are used by replay reads. The worker does not
need topic Create/Delete/Alter permissions. Disable unwanted topic auto-creation at
deployment level. Do not use the broad `--producer` ACL shortcut if it grants Create.
Kafka authorization requirements vary by broker version; verify the transactional
and configuration operations against [Kafka's operation/resource table](https://kafka.apache.org/40/security/authorization-and-acls/)
and test the actual remote principal before claiming remote validation. Receipt data
must be writable only by trusted console workers and retained for the recovery window.

## Execution and recovery contract

Approval writes no Kafka records. Start is explicit; it records the authenticated
actor and a bounded execution authorization (default 15 minutes). A worker checks
the current mapping authorization/version each batch. Resume and first release
reauthorize from the current signed-in session. Deployment `replay.revoked-subjects`
can block an actor after restart. This is not continuous identity-provider token
introspection; use short authorization windows and mapping/ACL revocation as needed.

Each acquisition claims an exclusive database lease using database time and creates
the producer with stable transactional ID `replay-<job UUID>`. `initTransactions()`
fences a stale producer and resolves its previous transaction before reading progress.
The worker captures a receipt high-water mark and waits for read_committed LSO to
reach it. This avoids treating a committed receipt hidden behind another open
transaction as missing. If stable progress cannot be established within its bounded
scan, execution blocks; it never guesses progress from PostgreSQL counters.

The receipt origin/topic UUID is durably saved before the first transaction. Recovery
reads the complete retained receipt chain from that origin to the captured boundary,
validates positions/digest/source/destination policy and rebuilds the projection.
Topic recreation, expired/truncated history, broken chains or a projection ahead of
receipts block recovery. Keep manifests, receipt anchors and encryption keys durable;
restoring a database backup from before execution is outside this crash guarantee.

Each batch revalidates source identity and exact source bytes/headers/fingerprint.
An expired source or definite compacted offset gap becomes an explicit skipped
outcome; no output is fabricated from the saved manifest. A changed fingerprint,
unavailable offset, identity change or revoked mapping/permissions blocks execution.
A corrupt immutable manifest fails the job. Do not delete/recreate topics during
active replay; worker identity checks fail closed when changes are observed.

Output records and the receipt containing their acknowledged destination coordinates
commit in the same Kafka transaction. Source consumers use read_committed, direct
assignment and no offset commits. Output preserves nullable key/value bytes and
ordered remaining headers; the output event timestamp is the new publish timestamp.
Rate is a cap with bursts of at most min(batch size, configured records/s), applied
between committed batches. The scheduler and broker latency can make throughput lower.

Canary is the first manifest position and uses a one-record batch. Its receipt causes
AWAITING_RELEASE even after restart or acknowledgment loss. Repeated release is a
no-op, including after a later pause. Pause is applied at batch boundaries. Skipped
canary outcomes are visible and require explicit release before remaining positions.

BLOCKED requires inspection and an explicit reauthorize/reconcile/resume command.
It reinitializes the producer and reads authoritative receipts before sending again.
Do not bypass missing-history blocks by clearing anchors/counters; restore history or
perform a new, independently reviewed recovery. FAILED/COMPLETED states are terminal.

The guarantee is one approved job's committed Kafka output observed by read_committed
consumers, including explicit skipped outcomes. Separate approvals may intentionally
publish the same source again. Uncommitted/aborted records may be visible to
read_uncommitted consumers. Downstream business effects need receiving-application
evidence and are not established by the console's receipt counts.

## Operations and evidence

Job commands: POST `/api/replay/jobs/{id}/start`, `/release`, `/pause`, `/resume`.
GET `/api/replay/jobs/{id}/dashboard` returns current state, receipt-derived counts,
source-to-destination audit, actor events, age, time to first replay, publish rate,
and optional group lag with sources and sample timestamps. Role/owner/current mapping
authorization and CSRF are enforced on commands. Saved approval documents remain
immutable; the `replay_jobs.state` column is authoritative for current state.

Receipt `committedAt` is the producer's receipt-creation timestamp inside an
eventually committed transaction, not a broker commit instant. Time-to-first-replay
and publish-rate values use this timestamp proxy; counts use committed outcomes.
The state contract allows CANARY_RUNNING → PAUSED and PAUSED/BLOCKED → CANARY_RUNNING
or RUNNING on explicit resume. The worker reconciles receipts before publishing and
restores AWAITING_RELEASE when an unreleased canary is already committed. A changed
mapping version prevents the first Release command and requires a new approval.

Configuration defaults: batch 50 (valid 1..500), lease 60000 ms, authorization 900000 ms,
receipt scan cap 1000000 offsets, stable-boundary wait 10000 ms. Keep leases long
enough for metadata, recovery and source validation. A shared one-partition receipt
log favors simple ordered recovery; a long unrelated open transaction can block
recovery until it resolves. This is deliberate failure to proceed without proof.

Reproduce the real-broker crash matrix:

```powershell
$env:JAVA_HOME='C:\Program Files\Java\jdk-25.0.2'
.\scripts\test-crash-matrix.ps1
```

It starts a real embedded Kafka broker, seeds the immutable 500-record fixture, starts
a worker in a separate JVM and calls Runtime.halt at canary acknowledgment, before
transaction commit, or after commit before PostgreSQL-projection update. It expedites
lease expiry in fixture SQL, recovers and verifies 500 exact outputs with a separate
read_committed consumer. Python independently checks bytes, headers, source/destination
coordinates, fingerprints, manifest digest and receipt hash chain. Child crash hooks
exist only in test classes, never in the release JAR.

Default test database is an isolated H2 TCP server so it survives worker JVM death.
To repeat against a disposable PostgreSQL database, set `REPLAY_TEST_DB_URL`,
`REPLAY_TEST_DB_USER`, `REPLAY_TEST_DB_PASSWORD` before the command. Credentials are
inherited as environment variables, not placed in the child argument file. Source
Kafka still uses the embedded plaintext fixture. Actual PostgreSQL, secured network,
external OIDC and authorized remote evidence must be recorded separately.

See `HANDOVER_STEP_3.md` for exact completed checks, pending environments, source
revision, artifact checksum and sample audit evidence.
