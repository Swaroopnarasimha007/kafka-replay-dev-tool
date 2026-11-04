# Kafka Dead-Letter Replay Console
## Three-step implementation plan

**Delivery contract.** Distribute one executable Spring Boot JAR containing the compiled React/TypeScript UI, REST API, and replay worker. From a release directory with Java installed, the launch command is:

```sh
java -jar replay-console.jar
```

Open `http://localhost:8080`. No separate frontend server, Node installation, or second terminal is required to run the release. Build the UI during Maven packaging using pinned tool versions and a lockfile. Provide Maven Wrapper commands for Unix and Windows. Kafka is an independently running service. Live mode also requires configured PostgreSQL, secret encryption key, and identity provider. With no live configuration, start a loopback-only, clearly labeled synthetic demo. An explicitly selected live profile with invalid configuration must fail clearly.

**How to execute this plan.** Give each numbered step to one implementation agent in order. Agents may be separate sessions or the same agent. Each starts from the previous accepted commit, implements the scope, runs its tests, and records evidence in its handover. Include the commit SHA, commands, results, fixtures, known gaps, and exact next task. Mark skipped tests as pending. All commands and checks below are targets for the implementation.

## Step 1 — Ship the single-command console and connect to Kafka

**Goal.** From an unopened release, an engineer can launch the dashboard, add a local or remote cluster, and inspect a real DLT record.

**Implementation**

1. Create a Maven Spring Boot application with a React/TypeScript frontend. Package built assets into the JAR. Serve frontend routes and same-origin API routes separately. Add a seeded demo behind the same API contract as live mode.
2. In live mode, add OIDC login and enforce Viewer, Operator, and Admin privileges on the backend. Only admins manage cluster profiles. Build the first-run flow: Add cluster → enter bootstrap endpoints and credentials → Test connection → Save and open. Make Test connection the first Kafka call for a new profile. Save the broker-reported cluster ID and profile only after successful testing.
3. Accept Kafka bootstrap addresses such as `localhost:9092` or `broker-a.example.org:9093,broker-b.example.org:9093`. Explain the field accepts Kafka host/port endpoints. Validate and reject HTTP dashboard URLs. Use the same Kafka client path for both environments. Support local plaintext by explicit development configuration and remote SASL/SCRAM over verified TLS with optional private CA. Validate advertised broker reachability. Restrict live outbound broker destinations to deployment-approved hosts/networks.
4. Keep Kafka credentials server-side after submission. Encrypt saved credentials with a deployment-provided key and redact secrets from logs, API responses, and errors. Support a separate replay principal on each profile. A connection test reads metadata with a timeout and reports authentication, TLS, and connectivity failures separately.
5. Implement topic discovery by naming pattern or explicit mapping. Use direct partition assignment, bounded reads, read_committed isolation, and disabled offset commits. Filter by time, key, partition, exception, and header. Return partition cursors and scan boundaries. Render ordered repeated headers, binary values, nulls, and masked payload previews. Audit browse and reveal actions.

**Tests**

- Packaging smoke test: copy only the release JAR into an empty directory, run the launch command, load the UI and a nested route, and call the API. Run the smoke test without Node or Kafka in demo mode.
- Local integration: seed a real broker with multi-partition DLT records, duplicate headers, binary data, and tombstones. Verify filters and pagination, including a truncated scan. Snapshot an existing application group's offsets before and after browsing and assert equality.
- Remote configuration: exercise DNS hostnames, non-loopback advertised listeners, CA validation and SCRAM on a secured broker fixture. Test wrong password, untrusted CA, unreachable advertised address, and HTTP URL input. Separately record a smoke run from the JAR against an authorized remote test cluster when available.
- Access checks: a viewer cannot register brokers or retrieve saved secrets. Restart the application, select the saved profile, and verify the backend reconnects without asking the viewer for Kafka credentials.

**Completion validation.** One command serves both UI and API. A developer can browse the seeded local DLT and the secured network fixture through the same UI. Record remote smoke evidence separately from fixture evidence.

**Handover to Step 2.** Supply the packaged launch command, API contract, profile/role schema, connection fixtures, supported Spring Kafka DLT headers, and sample record coordinates. Identify which records preserve the original bytes and qualify for replay.

## Step 2 — Freeze replay selections and approve a job

**Goal.** An operator can turn a filter into an exact, reviewable set of records. A retry or a second click creates only one job for the same approval request.

**Implementation**

1. Add admin-managed, same-cluster source-to-destination mappings. Version the record extraction, partition, and header policies. Support byte-preserving Spring Kafka DLT records first. Preserve null keys/values and ordered business headers. Remove explicitly configured retry headers. Wrapped formats without a supported extractor remain browse-only.
2. Bound each preview by per-partition end offsets. Freeze each selected record's cluster ID, topic ID, partition, offset, content fingerprint, and deterministic manifest position. Store the target topic identity, policy version, rate, and manifest digest in PostgreSQL. Expose scan limits and require an explicit selection before approving a partial scan.
3. Display the count, target, header changes, and one-record canary choice. Explain that replay appends a new event visible to subscribed groups. The operator supplies a reason and approves the exact manifest. Use a database transaction and a unique idempotency key for approval. Recheck role and mapping authorization on the server.
4. Publish the job state contract for the worker: approved → canary running → awaiting release → running → completed or completed with exceptions. Include paused, blocked, and failed states, valid transitions, and an idempotent release command. Keep worker execution disabled until Step 3 is accepted.

**Tests**

- Seed 500 matching records, preview, then append 40 additional matches. Approval must contain precisely the original 500. Test zero matches, sparse offsets, partial scans, and a mapping changed after preview.
- Submit concurrent identical approvals. Assert one job, one manifest, and one approval event. Reusing the same key with a different manifest must return a conflict.
- Verify viewer denial, unauthorized destinations, unsupported envelopes, null payloads, repeated headers, and binary keys. Verify preview and approval write zero records to destination topics.

**Completion validation.** Compare the stored manifest to an independently generated expected set. Restart the API and reopen the approved job with the same count, digest, target, and policy. Demonstrate that stale mappings require review.

**Handover to Step 3.** Supply migrations, API examples, manifest order and digest rules, the transition table, the immutable 500-record fixture, and a verifier input containing expected keys, values, headers, and source coordinates.

## Step 3 — Execute replay, prove crash recovery, and release

**Goal.** Publish only approved records, recover across a worker crash, and show operators exactly what Kafka committed.

**Implementation**

1. Claim each job with an exclusive lease. Use a stable Kafka transactional ID per job to fence stale producers. After acquiring ownership, initialize the producer to resolve prior transactions, then load committed progress through a captured stable boundary before resuming. Validate cluster and topic identities, current permissions, record fingerprint, and destination policy. Handle expired offsets, gaps, and recreated topics explicitly.
2. Publish the canary as the first batch and await manual release. Apply the configured rate and batch size. Publish output records and the batch receipt in one Kafka transaction on the same cluster. Include manifest positions, source coordinates, acknowledged destination coordinates, and any skipped outcomes in the receipt. Retain receipts for the entire recoverability window. Block recovery if authoritative progress cannot be established.
3. Derive dashboard counts and audit links from committed receipts. Rebuild the PostgreSQL projection idempotently if Kafka commits before the database update. Handle pause at batch boundaries and an ambiguous commit outcome by reconciling against committed receipts. Surface record age, time to first replay, publish rate, and group offset lag where group permissions allow it. Display the source and timestamp of each metric.
4. Finish the synthetic demo with the same planner and job screens. Publish the JAR, sample configuration, minimum ACLs, local/remote onboarding, public demo instructions, and a reproducible real-broker crash test. Document which checks require external infrastructure.

**Tests**

- Real-broker crash matrix: stop the process before transaction commit, after commit before PostgreSQL update, and during canary acknowledgment. Restart and release as appropriate. Use a separate read_committed consumer to compare all 500 outputs to the approved manifest. Assert zero missing, duplicate, and unapproved records. Verify the canary appears once.
- Start two workers against the same job and expire one lease. Verify fencing prevents duplicate committed output. Exercise repeated release, pause/resume, broker outage, expired source, revoked access, and missing progress history. Verify recovery blocks or records explicit exceptions as designed.
- Browser acceptance: an admin adds a cluster, a viewer inspects masked records, and an operator approves, runs a canary, releases, and audits a job. Repeat on a local broker and a secured network fixture. On an authorized remote test cluster, verify actual transactional permissions and replay to a disposable destination before claiming live remote validation.
- Release smoke: start the built JAR with one command in demo and configured live environments. Verify the UI loads, deep links survive refresh, and operator actions are attributed to the signed-in user.

**Completion validation.** Attach the independent verifier output, worker restart evidence, manifest digest, and audit trace from one source record to its destination offset. The guarantee covers one approved job's committed Kafka output as observed by read_committed consumers. Downstream business effects require evidence from the receiving application.

**Final handover.** Deliver the release JAR, source commit, exact build/run commands, test results by environment, and a short demo script: launch → connect → filter → approve → canary → crash → resume → verify. Identify any live remote check still pending, including the access needed to run it.
