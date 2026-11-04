# Step 3 final implementation handover

Date: 2026-11-04, Asia/Calcutta. Base revision: `1a6ea04`.
Implementation revision and exact JAR checksum: `STEP_3_REVISION.txt` (recorded in the
follow-up documentation commit). Runtime distribution: `release/replay-console.jar`.

## Status and launch

Step 3 worker, controls, metrics, synthetic demo, release packaging and reproducible
local crash tests are implemented. Local acceptance passes. **Full live release
acceptance remains pending PostgreSQL, secured network/ACL and authorized remote
checks below.** No remote deployment or publication was performed.

From a release directory with Java installed:
```sh
java -jar replay-console.jar
```
Open `http://localhost:8080/console/records`. No frontend server or Node is needed.
Default mode is a loopback-only synthetic demo. Its data directory persists across
restart. Live mode needs Kafka, PostgreSQL, OIDC, an encryption key, an outbound
allowlist, a preprovisioned receipt topic, replay credentials and explicit worker
enablement. Missing live startup configuration does not fall back to demo.

See `RELEASE_RUNBOOK.md` for complete launch, onboarding, receipt retention, minimum
ACLs, source/metric semantics, operational recovery and public synthetic demo steps.
Sample secret-free live configuration: `config/application-live.example.yml`.

## Delivered implementation

- Exclusive job leases use database time. Every acquisition initializes a producer
  with stable transactional ID `replay-<job UUID>` to fence old owners before reading
  progress. Lease/mapping rows protect the Kafka commit and projection update.
- Source/target/receipt topic identities, mapping version/access, execution
  authorization and exact source fingerprints/bytes/ordered headers are checked.
  Expired records and definite offset gaps produce explicit skip receipts;
  unavailable/truncated source, changed identities/policy and revoked access block.
  Corrupt immutable manifests fail without output.
- Outputs and their acknowledged destination coordinates are recorded in a receipt
  committed in the same Kafka transaction. Batches have contiguous manifest
  positions, sequence indexes and a SHA-256 receipt hash chain.
- Recovery captures a receipt high-water mark after producer initialization and
  requires read_committed LSO to reach it. It then scans from a durable receipt
  origin to that stable boundary and rebuilds the database projection idempotently.
  An unrelated open transaction cannot cause hidden committed progress to be
  mistaken for an absent batch. Missing/expired/truncated history blocks recovery.
- Canary position zero commits alone, then waits for manual release. Release and
  Start are idempotent; repeated release cannot resume a later paused job. Pause
  applies at a batch boundary. Explicit resume reauthorizes and reconciles first.
- Rate and batch caps, current SQL state projections, receipt-based published/
  skipped counts, source-to-destination audits, actor events and metric sources/
  timestamps are exposed in the API/UI. Group lag is sampled only when configured
  and permitted. Receipt timestamps are producer-time proxies, not broker commit
  timestamps; the runbook documents metric interpretation.
- Demo uses the same worker/store/control path with a synthetic transactional
  authority adapter. Its UI explicitly distinguishes simulation from Kafka evidence.
- Flyway V3 adds execution/lease state and disposable receipt projections. V1/V2
  were preserved. Immutable approval documents remain unchanged; current job state
  is read from `replay_jobs.state`.

## Exact validation commands and results

```powershell
$env:JAVA_HOME='C:\Program Files\Java\jdk-25.0.2'
.\mvnw.cmd -B package
.\scripts\smoke.ps1
.\scripts\smoke-planner.ps1
.\scripts\smoke-worker.ps1
.\scripts\package-release.ps1
```

Unix build: `./mvnw -B package`. Actual test runtime was Windows / Java 25.0.2;
application bytecode targets Java 17. Maven Wrapper pins Maven 3.9.9; frontend build
pins Node/npm and uses the committed dependency lockfile.

| Check | Result |
| --- | --- |
| Full Maven package | PASS; 33 tests, zero failures/errors/skips, compiled UI included in executable JAR |
| Existing step-1/2 suite | 18 tests pass: browse safety, unchanged group offsets, binary/null/repeated headers, exact manifests, authorization, concurrent idempotency and migrations |
| WorkerBrokerTest | 8 real-broker tests pass: three separate-JVM crash checkpoints, lease/fencing, missing receipt history, expired source, hidden committed receipt behind unrelated open transaction, ambiguous commit acknowledgment |
| WorkerDemoTest | 7 tests pass: canary/release/pause/resume/metrics, lost projection recovery, execution/mapping authorization revocation, one-record job, no-canary rate cap, corrupted manifest failure, stale mapping Release denial |
| Isolated demo JAR smoke | PASS; empty release directory, Node absent from PATH, root/deep route/compiled assets/API |
| Planner restart smoke | PASS; approved job count/digest/target/policy persist; approval alone does not start execution |
| Worker restart smoke | PASS; start one canary, terminate/restart actual JAR, same canary, repeated release, 60 unique synthetic outputs/receipts |
| Independent Python execution verification | PASS for all three crash exports: 500 outputs, zero missing, zero duplicate, zero unapproved; canary appears once |
| Browser demo acceptance | PASS; create mapping → freeze/approve → start → observe one receipt/AWAITING_RELEASE → release → COMPLETED 60/60; metric/audit UI inspected |

Surefire reports: `target/surefire-reports/`. Release build log:
`step3-release-build.log` (ignored). Scripts stop all application test processes.
The real broker fixture and test database server are stopped after tests. No console
or worker is intentionally left running.

## Crash proof and audit artifacts

Reproduce with `scripts/test-crash-matrix.ps1`. The fixture uses an actual embedded
Kafka 3.9.1 KRaft broker with transactional replication/min ISR set to one for local
testing. It retains the intentionally old fixture source timestamps indefinitely;
an independent test explicitly expires a source offset. The default test database
is H2 TCP in PostgreSQL compatibility mode and survives child JVM termination.

For each checkpoint, the test starts a **separate worker JVM** and calls
`Runtime.halt(71)` at the named point; it does not merely throw an in-process error:

1. CANARY_ACKNOWLEDGED: producer has acknowledged its send, transaction not committed.
2. BEFORE_COMMIT: output and receipt have been sent, transaction not committed.
3. AFTER_COMMIT_BEFORE_PROJECTION: Kafka committed; database projection transaction
   has not committed and rolls back when the child connection dies.

Fixture SQL expedites the abandoned lease's expiry rather than waiting a minute.
Recovery then fences the old producer, reconciles receipts, waits for explicit
release and finishes the frozen 500-record manifest. The test also deletes the
disposable receipt projection to prove reconstruction from Kafka. A separate
read_committed consumer captures all committed destination records between independent
before/after bounds, checks the complete count and compares bytes and repeated/null
headers to the manifest. The Python verifier separately recomputes source fingerprints,
manifest digest, receipt hashes/chain, coordinates and canary count.

Committed evidence directory: `docs/evidence/step3/`:

- `execution-CANARY_ACKNOWLEDGED.json`
- `execution-BEFORE_COMMIT.json`
- `execution-AFTER_COMMIT_BEFORE_PROJECTION.json`
- Corresponding `summary-*.json` with exact job ID, manifest digest, verifier results,
  and a trace from source `orders.DLT` P0@0 to its acknowledged destination coordinate.
- `demo-release.json`: actual packaged-JAR canary/restart/release audit.
- `browser-demo.json`, `canary.jpg`, `completed.jpg`: browser simulation evidence.

Example independent verification of the strongest crash window:
```powershell
python scripts\verify_execution.py docs\fixtures\source-500.json docs\evidence\step3\execution-AFTER_COMMIT_BEFORE_PROJECTION.json
```

These exports contain synthetic fixture bytes from a **real local Kafka broker**;
browser and JAR-demo artifacts use a **synthetic authority**. They are separate
evidence categories. The ephemeral fixture broker's UUIDs/offsets are not valid
coordinates for a newly started broker; recreate the mapping and approval when
rerunning. The 500-record source JSON remains the immutable step-2 fixture.

## API and state handoff

Step-2 preview/approval/manifest contracts remain supported. New Operator/Admin
commands, additionally subject to owner/current mapping authorization and CSRF:

| Route under `/api/replay/jobs/{id}` | Semantics |
| --- | --- |
| POST /start | Explicitly begin an approved job; a repeat cannot resume a paused/blocked job |
| POST /release | First successful command releases an awaiting canary; repeats never resume execution |
| POST /pause | Request pause at a safe batch boundary |
| POST /resume | Reauthorize paused/blocked execution; reconcile before new output |
| GET /dashboard | Current state, committed counts, receipts/coordinates/audit and sourced metrics |

GET `/api/replay/state-contract` exposes the current transition table. Step-3 adds
CANARY_RUNNING → PAUSED and explicit PAUSED/BLOCKED resumes into CANARY_RUNNING or
RUNNING; reconciliation restores AWAITING_RELEASE if a canary was already committed.
COMPLETED, COMPLETED_WITH_EXCEPTIONS and FAILED are terminal. A new mapping version
invalidates the old job's first Release/Resume and requires a new reviewed approval.

## Remaining live acceptance and limits

- **PENDING actual PostgreSQL**: run migration/approval/lease/concurrency/crash recovery
  against a disposable PostgreSQL server. Set `REPLAY_TEST_DB_URL`,
  `REPLAY_TEST_DB_USER`, `REPLAY_TEST_DB_PASSWORD` before running the crash script.
  No PostgreSQL service or credentials were supplied; H2 evidence is not PG evidence.
- **PENDING secured network fixture**: real DNS/non-loopback advertised listeners,
  verified private CA, separate SCRAM principals, wrong password/untrusted CA/
  unreachable advertised broker checks. Plaintext local transaction success does
  not establish secured remote connectivity or permissions.
- **PENDING actual Kafka ACL revocation and broker outage/restart exercises** on that
  authorized fixture. Local tests cover mapping/role/authorization expiry,
  transaction acknowledgment loss and fencing; they do not prove remote authorizer
  behavior or a full broker service restart.
- **PENDING external OIDC end-to-end browser role login** with Admin/Operator/Viewer.
  Backend role/CSRF checks use authenticated test principals; browser acceptance is
  synthetic demo. Worker authorization snapshots are bounded, not continuous IdP
  introspection; current mapping and Kafka ACLs remain enforced at execution.
- **PENDING authorized remote JAR release smoke** and transactional replay to a
  disposable destination. Needs approved endpoints/CA, PostgreSQL/OIDC configuration,
  worker ACLs, durable receipts and a disposable source/destination. No authorized
  remote cluster details were provided, and no remote claim is made.
- Deployment egress restrictions are still required. A one-partition shared receipt
  log may deliberately block recovery behind a long unrelated open transaction.
  Retain anchors/manifests/receipts for the entire supported recovery window.
- This release guarantees only the approved job's committed Kafka output observed
  by read_committed consumers, with explicit skip outcomes. It does not deduplicate
  separate approvals or prove downstream application business effects. Kafka/DB
  durability, trusted receipt writers and preserved manifests/anchors are assumptions.

## Next work after this handover

The three planned implementation steps are now represented in the project. The next
task is **live acceptance**, not an unplanned fourth feature phase: provision a
disposable PostgreSQL + secured Kafka + OIDC environment, run the pending matrix,
record actual ACL/transactional and browser role evidence, repeat the JAR smoke, then
decide live release acceptance. Preserve fixture versus remote result attribution.
Do not claim full live acceptance or downstream exactly-once business effects from
the local fixture results.
