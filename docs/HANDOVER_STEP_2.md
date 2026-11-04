# Step 2 handover → Step 3

Date: 2026-11-04, Asia/Calcutta. Base commit: `03a50cf`.
Implementation commit is recorded in `STEP_2_REVISION.txt` by the follow-up
documentation commit. Step 3 has not been implemented; execution cannot be enabled.

## Delivered behavior

- Admin-managed, immutable same-cluster source/destination mapping versions with
  broker cluster/topic identities, versioned extraction/partition/header policies,
  byte-preservation attestation and optional operator-subject restrictions.
- Read-committed direct-assignment source snapshots with once-captured partition
  end offsets, retained beginnings, scan budget/time limit, sparse-offset support,
  fingerprints and deterministic zero-based manifest positions.
- Exact review/approval UI with destination, count, header removals, rate, digest,
  explicit selection of all displayed records in a partial scan, reason and canary
  choice. A new preview is needed to narrow the selected subset.
- Transactional approval referencing one immutable stored manifest, globally unique
  idempotency keys, one approval audit event, server-side access/version rechecks,
  and stale mapping/topic identity rejection.
- Versioned Flyway migrations adopting existing step-1 profiles without deleting
  credentials. PostgreSQL remains live storage; demo now persists to local H2 files.
- Saved job reopening, audited verifier exports, state-transition contract, and a
  release endpoint that fails with WORKER_DISABLED without mutating anything.

## Build, launch and evidence

```powershell
$env:JAVA_HOME='C:\Program Files\Java\jdk-25.0.2'
.\mvnw.cmd -B package
java -jar target/replay-console.jar
```

Unix: `./mvnw -B package`. Open `http://localhost:8080/console/records`.
An isolated release still needs only `java -jar replay-console.jar`. Keep its `data`
directory to retain demo jobs after restart. Do not run a JAR directly from `target`
while rebuilding it on Windows; use an isolated release copy to avoid file locking.

Final packaged JAR SHA-256:
`C3DCA4F921FECC4FBB5003653200C20753E5394964ACE1D87AE6D1C5E76CCCB8`.

| Check | Result |
| --- | --- |
| `.\mvnw.cmd -B package` | PASS, 18 tests, 0 failures/errors/skips; TypeScript/Vite and executable JAR packaging |
| ConsoleTest | 5 passing step-1 browse/API tests |
| LiveAccessTest | 2 passing test methods, including Viewer preview/approval denial and Operator mapping-management denial |
| PlannerTest | 9 passing tests: eight concurrent identical approvals, one job/manifest/event, key conflict, stale mapping, access revocation, exact partial selection, zero matches, null/binary/repeated headers, unsupported extractor/header denial, identity change, sparse offsets, legacy schema adoption, release/state guards |
| PlannerBrokerTest | PASS on actual embedded KRaft: 500-record fixed preview, append 40, approve precisely original 500, independently inspect empty destination offsets, reload stored job, new preview sees 540 |
| RealBrokerTest | PASS; step-1 group offsets unchanged after browsing |
| `.\scripts\smoke.ps1` | PASS; isolated demo JAR, UI/deep route/assets/API, no Node on PATH |
| `.\scripts\smoke-planner.ps1` | PASS on final JAR; create mapping → freeze 60 → approve → identical retry → terminate/restart process → reopen same count/digest/target/policy |
| Independent Python verifier | PASS; all 500 source coordinates, keys, values, ordered output headers, source fingerprints, destination partitions and manifest digest |
| Browser acceptance | PASS in synthetic demo: create mapping, freeze 60, supply reason/canary choice, approve, observe APPROVED job and worker-disabled message; screenshot in `docs/evidence/step2-approved.jpg` |

Commands for independent verification:
```powershell
python scripts\verify_manifest.py docs\fixtures\source-500.json docs\fixtures\approved-500-verifier.json
```

Saved verifier digest:
`eaafef81f3627cc0316d1042b0d14524e1ae0f3a9ff63c77030cfd95488d3c42`.
Saved fixture approval ID: `844155eb-a82a-4c74-a20a-dc07f785082f`.
The fixture broker was destroyed after testing. Its exported identities are evidence,
not valid identities for a newly started broker. Regenerate the approval against the
new cluster when doing execution tests.

Surefire reports: `target/surefire-reports/`; latest full build: `step2-final-build.log`.
Latest isolated restart logs:
`C:\Users\bswar\AppData\Local\Temp\replay-planner-0230d1c6-ad25-499b-84a5-7d26a1849a14`.
All application/broker test processes were stopped. Browser evidence used a separate
in-memory demo database; it did not change project demo data or real Kafka.

## Inputs for the next implementation

- `STEP_2_API.md`: HTTP examples, schema ownership, exact manifest order, fingerprint
  and digest encoding, mapping authorization rules and transition table.
- `src/main/resources/db/migration/V1__adopt_profiles_and_audit.sql` and
  `V2__immutable_replay_plans.sql`: append new migrations, do not alter accepted ones.
- `docs/fixtures/source-500.json`: immutable source fixture, SHA-256
  `9081EEB2B7EE4C3C68024A0D5470ABBEFB3519D591F52FB75A823B08C7DD915D`.
  Three partitions: 167/167/166 records; binary nullable keys/values, tombstones,
  duplicate headers, a null-valued business header and explicit retry headers.
- `scripts/generate_fixture.py`: deterministic fixture generator; no need to rerun
  it for normal tests. Committed JSON is the actual test input.
- `docs/fixtures/approved-500-verifier.json`: approved job plus exact expected
  output key/value/header bytes, source cluster/topic identities and coordinates,
  destination partition, source fingerprints and deterministic manifest positions.
- `scripts/verify_manifest.py`: independent Python canonical encoder and byte-level
  comparator. It checks a planner manifest, not committed destination output yet.
- `PlannerBrokerTest`: real-broker seeding path; extend it with a separate
  read_committed destination consumer and independent committed-output verification.
- `JobStates`: pure transition guards; repeated release must not restart a later
  paused/blocked job. Step 3 must persist a RELEASED event under the job row lock.

Mappings preserve nullable key/value bytes and remaining ordered business headers.
Only explicitly configured retry/DLT names are removed. Byte-preserving Spring DLT
records qualify after admin attestation and required original-topic/partition header
checks. Wrapped/unsupported records remain browse-only. No arbitrary object
deserialization is performed. This version supports ORIGINAL_PARTITION_V1 only.

## Known limits / pending checks

- PENDING actual PostgreSQL approval-concurrency, migration upgrade and process-
  restart checks. SQL and transactions are implemented for PostgreSQL, but tests use
  H2 PostgreSQL compatibility mode. Actual process restart was demonstrated with
  file-backed H2 demo storage, not a PostgreSQL server.
- Step-1 pending secured DNS/non-loopback/SCRAM/private-CA fixture, external OIDC
  end-to-end role login, and authorized remote JAR smoke remain pending. See the
  step-1 handover for required access. No remote evidence is claimed.
- Topic-identity-change and sparse-offset planner tests use controlled source
  adapters. Normal source reads and 500+40 behavior use real Kafka. A first attempt
  to delete/recreate a topic in the embedded Windows fixture hit Kafka's mapped-file
  cleanup lock; that destructive fixture was replaced with the adapter identity test.
- Every approved job remains APPROVED. There are no leases, transactions, receipts,
  producer, pause/resume execution, canary output, or destination-output guarantee.
  The HTTP release route intentionally returns 409; only its future state contract
  and idempotency guards are defined.
- `replay_jobs.state` and job document state both start APPROVED. Before worker
  transitions, make the state column authoritative in API projections (or update
  both atomically). Do not let immutable approval documents become stale UI state.
- The manifest verifier export contains payload data. It is Operator/Admin-only,
  owner/mapping-authorized and audited. Protect DB storage/backups accordingly.
- Kafka destination/network allowlisting still requires deployment egress controls,
  as explained in step 1. No worker principal permissions were tested.

## Exact next task: Step 3 only

When the user requests step 3, implement the worker against these approved immutable
manifests. Add job leases and stable transactional producer IDs with fencing; validate
cluster/topic identities, mapping authorization, source fingerprints, retained source
offsets and destination policy before publishing. Use the separate replay principal.
Publish output plus authoritative per-batch receipts in the same Kafka transaction.

Implement canary-first execution and manual idempotent release, rate/batch limits,
pause at batch boundaries, receipt-derived counts/audit, PostgreSQL projection
reconciliation and crash recovery through captured stable receipt boundaries. Persist
blocked recovery phase and fail closed when authoritative progress is unavailable.
Do not infer progress solely from the PostgreSQL job state or destination key counts.

Run the plan's real-broker crash matrix before claiming execution guarantees: before
commit, after Kafka commit before database update, and during canary acknowledgment;
also dual-worker fencing, lease expiry, ambiguous commit, pause/resume, outages,
expired source, revoked ACLs and missing receipt history. Independently compare all
500 committed outputs to the approved manifest with a separate read_committed
consumer, including exact bytes/repeated headers and one canary. Extend verifier
input with actual destination coordinates. Attach zero-missing/duplicate/unapproved
results, manifest digest, receipt/audit trace, final release JAR and final handover.

Retain pending PostgreSQL/secured remote checks as explicitly pending until the
required environment is available; fixture evidence and authorized remote evidence
must remain separate.
