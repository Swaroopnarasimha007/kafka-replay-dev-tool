# Replay planning API (step 2)

All routes are same-origin under `/api/replay`. Live mode requires OIDC. POSTs require
the session CSRF token as `X-XSRF-TOKEN`. Demo uses the same services, with actor `demo`
and local file-backed H2. Live storage is configured PostgreSQL. The worker is always
disabled in this release; there is no producer in application code.

| Method / route | Authorization | Purpose |
| --- | --- | --- |
| GET /mappings | Operator, Admin | Current authorized mapping versions |
| POST /mappings | Admin | Create a verified same-cluster mapping |
| POST /mappings/{id}/versions | Admin | New immutable version; requires expectedVersion |
| POST /previews | Authorized Operator, Admin | Freeze one bounded selection |
| GET /previews/{id} | Owner or Admin, current mapping access | Reopen immutable review |
| POST /approvals | Owner or Admin, current mapping access | Transactional, idempotent approval |
| GET /jobs | Operator, Admin | Own jobs, or all jobs for Admin, filtered by current access |
| GET /jobs/{id} | Owner or Admin, current mapping access | Reopen saved job |
| GET /jobs/{id}/verifier | Same as job | Audited export including full output bytes |
| POST /jobs/{id}/release | Same as job | Always 409 WORKER_DISABLED in step 2 |
| GET /state-contract | Operator, Admin | State transitions and execution-disabled flag |

Empty `allowedSubjects` permits all Operators; a nonempty list restricts operators to
exact OIDC subject/name values. Admins can manage and inspect every mapping. Viewer
cannot preview, approve, manage mappings, or export manifests. Every service mutation
also enforces authorization, independent of controller annotations.

## Mapping example

```json
{
  "profileId":"demo", "source":"orders.DLT", "target":"orders",
  "extractor":"SPRING_DLT_BYTES_V1",
  "partitionPolicy":"ORIGINAL_PARTITION_V1", "headerPolicy":"EXPLICIT_REMOVE_V1",
  "removeHeaders":["retry-attempts","kafka_dlt-original-topic","kafka_dlt-original-partition"],
  "allowedSubjects":["alice"], "bytePreservingAttested":true
}
```

Revision uses the same fields plus `expectedVersion`. Each version captures broker
cluster ID, source and destination topic UUIDs, and destination partition count using
the saved profile's browse principal. Source must satisfy the deployment discovery
policy; source and target must differ. Arbitrary cross-cluster destinations and
client-supplied topic identities are not accepted. Changed identities require a new
mapping version. Header removal lists are exact, distinct, sorted names; every
occurrence of each named header is removed, all others preserve original order/nulls.

The admin attests the source was published with original key/value bytes. This v1
extractor never unwraps JSON, Java serialization, or arbitrary envelopes. Each record
must contain exactly one non-null `kafka_dlt-original-topic` matching the authorized
destination and one four-byte big-endian `kafka_dlt-original-partition` within its
partition range. Missing/ambiguous headers and serialized Spring deserialization
exception headers reject the entire preview as browse-only. Payloads are otherwise
opaque; headers alone cannot prove an admin's byte-preservation attestation.

## Preview example

```json
{"mappingId":"mapping-uuid","filter":{"partition":null,"exception":"Failure"},"rate":100,"scanLimit":10000}
```

Filters use step-1 `Browser.Query`: partition, inclusive epoch-ms from/to, exact base64
key, exception-header substring, exact header name. Browse cursors are rejected for
planning. Preview always starts at retained beginnings. `scanLimit` is 1..10000 examined
records, `rate` is 1..100000 records/s. The filter's browse `limit`/`scanLimit` are unused;
the outer planner scanLimit governs selection. No auto-pagination refreshes boundaries.

The source consumer uses direct assignment, no group, disabled commits, read_committed,
disabled topic auto-creation. It captures all selected partition beginning/end offsets
once before reading; end offsets are exclusive stable boundaries. It scans partitions
in ascending order, up to the record budget or a 15-second polling deadline, and
ignores records at/after the captured boundary. Metadata timeouts are separate.
Sparse offsets are accepted; no contiguous offset range is inferred.

Response: `id`, complete immutable `mapping`, `rate`, `count`, `digest`, `scanned`,
`partial`, per-partition `beginnings`, `boundaries`, `cursors`, and `records` containing
position/source partition/offset/fingerprint/destination partition. Preview HTTP
responses do not expose payload bytes. Zero matches can be reviewed but not approved.

## Approval example

```json
{
  "previewId":"preview-uuid", "digest":"reviewed-sha256", "idempotencyKey":"approval-request-uuid",
  "reason":"Investigated upstream failure; safe to retry", "canary":true,
  "acceptPartial":false, "selectedPositions":null
}
```

Partial previews require `acceptPartial:true` **and** the complete, ordered list of
displayed positions, e.g. `[0,1,2]`. This release supports explicitly selecting all
records of a reviewed partial manifest. To select a smaller set, narrow filters and
create a new preview. Approval never silently changes a reviewed manifest or adds
later matches. Any supplied positions must equal the reviewed list exactly.

The server locks the mapping head, rechecks current access and version, validates
cluster/topic identity, and writes a job referencing the single stored preview
manifest plus one APPROVED event in one transaction. The globally unique idempotency
key is bound to SHA-256 of length-prefixed JSON of the parsed approval and actor name.
An identical retry returns the same job; changed preview/reason/canary/selection with
the same key returns 409. Another actor cannot claim that key. Distinct mapping races
are resolved by the database unique constraint and a post-rollback re-read. A new key
is a distinct approval request, even if the manifest digest matches another job.

Jobs contain count, digest, mapping/policy snapshot, rate, reason, canary, owner,
APPROVED state and `workerEnabled:false`. An optional canary is the first manifest
position. Rate and canary are stored instructions, not executed in step 2.

## Storage and canonical encoding

Flyway V1 adopts existing step-1 tables with baseline version 0; V2 adds mapping heads,
immutable mapping versions, previews, jobs with a unique idempotency key, and unique
job events. Migration is invoked programmatically by Profiles before planner startup;
Boot's automatic Flyway runner is disabled. Existing credentials remain encrypted.
Preview manifests contain plaintext base64 record bytes; protect DB backups and the
audited verifier export as payload data. Demo files are under `./data/replay-demo*`.

Positions are zero-based, ordered by source partition then offset. Each entry contains
cluster ID, source topic UUID/name, partition/offset, source timestamp, SHA-256 source
fingerprint, destination partition, original nullable key/value bytes and output
ordered headers after policy removal. Source headers are bound by fingerprint;
output headers are additionally bound by manifest digest.

All canonical integers use signed big-endian binary: int32 length/count/partition/
position/rate/version, int64 offset/timestamp. Strings are UTF-8 prefixed with int32
byte length. Byte arrays use int32 length and decoded bytes; null is length -1, empty
is length 0. Header lists use int32 count, then name string and nullable bytes, in
original order. SHA-256 is lowercase hex. No JSON serialization determines these
content digests.

Source fingerprint v1 concatenates: string `source-content-v1`, timestamp int64,
key bytes, value bytes, original header list.

Manifest digest v1 concatenates: string `replay-manifest-v1`, mapping ID string,
mapping version int32, cluster ID, source topic UUID, target topic UUID, source name,
target name, extractor version, partition policy, header policy (all strings),
removed-header count + strings, rate int32, entry count int32; then for each entry:
position int32, partition int32, offset int64, source fingerprint string, destination
partition int32, key bytes, value bytes, output header list.

See `scripts/verify_manifest.py` for an independent Python implementation and
`docs/fixtures/approved-500-verifier.json` for a real-broker-generated export.

## Worker state contract

| From | Allowed destinations |
| --- | --- |
| APPROVED | CANARY_RUNNING (canary chosen), RUNNING (no canary), BLOCKED, FAILED |
| CANARY_RUNNING | AWAITING_RELEASE, BLOCKED, FAILED |
| AWAITING_RELEASE | RUNNING (explicit release), BLOCKED, FAILED |
| RUNNING | PAUSED, BLOCKED, FAILED, COMPLETED, COMPLETED_WITH_EXCEPTIONS |
| PAUSED | RUNNING (resume), BLOCKED, FAILED |
| BLOCKED | PAUSED or AWAITING_RELEASE after reconciliation/revalidation; FAILED |
| COMPLETED / COMPLETED_WITH_EXCEPTIONS / FAILED | None |

Only the first APPROVED state is reachable through step-2 endpoints. `JobStates`
provides transition guards and a pure idempotent release function. In step 3 the
worker must persist the original blocked/resume phase, take a job row lock, record
one RELEASED event in the same transaction as state change, and use the prior release
event to make retries a no-op even after a later pause/block. It must not rerun or
resume execution as a side effect of repeated release. The HTTP release command
currently returns 409 without changing any state; no setting can enable execution.
