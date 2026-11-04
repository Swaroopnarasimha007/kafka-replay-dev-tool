# Step 1 API

Same-origin JSON. Live requests require an OIDC session; modifying HTTP methods also
require `X-XSRF-TOKEN` from `GET /api/session` (`csrfToken`).

| Route | Role | Result |
| --- | --- | --- |
| GET /api/session | authenticated | mode, user, roles, csrfToken |
| GET /api/profiles | Viewer/Operator/Admin | id, name, bootstrap, clusterId |
| POST /api/profiles/test | Admin | testToken, clusterId, expiresInSeconds |
| POST /api/profiles | Admin | saved profile; body `{ "testToken": "…" }` |
| GET /api/profiles/{id}/topics | Viewer/Operator/Admin | topic names matching deployment policy |
| POST /api/profiles/{id}/topics/{topic}/browse | Viewer/Operator/Admin | masked page |
| POST /api/profiles/{id}/topics/{topic}/reveal | Operator/Admin | original byte page, base64 |

Test body: `name`, `bootstrap`, `protocol` (`PLAINTEXT` or `SASL_SSL`),
`username`, `password`, `caPem` (optional PEM), `replayUsername`, `replayPassword`.
Protocol is explicit; HTTP URLs and invalid ports are rejected before client creation.
Test token is process-local, single-use, expires after 300 seconds; restart requires retesting.

Browse body example:
```json
{"partition":0,"from":null,"to":null,"key":null,"exception":"Failure","header":"trace","cursors":{"0":0},"limit":25,"scanLimit":1000}
```
`from`/`to` are inclusive epoch milliseconds, key is exact base64, exception is
a UTF-8 substring in any header whose name contains `exception`, header is an exact
header name. Limits: 1–200 returned matches, 1–10000 examined records, eight-second
poll budget (metadata calls have their own bounded timeout). Responses contain
`records`, per-partition `cursors`, retained `beginnings`, captured `boundaries`,
`scanned`, `truncated`. Continue using returned cursors. Each request captures fresh
boundaries; these are browse pages, not immutable replay manifests. No cross-partition
global ordering is promised. Refresh filters starts a new scan.

Record fields: topic, partition, offset, timestamp, keyBase64, valueBase64,
headers (ordered list of name/base64 pairs preserving repetitions and nulls), masked.
Masked non-null bytes are `[masked]`; null remains null. Reveal audits the requested
query; browse audits actor and topic. Full binary values are base64. UI escapes text.

`cluster_profiles`: UUID id, name, bootstrap, cluster_id, AES-GCM secret envelope.
`browse_audit`: serial id, timestamp, actor, action, coordinates. Tables are initialized
at startup in this step; step 2 should adopt versioned migrations before adding jobs.

All Spring Kafka headers are retained verbatim, including `kafka_dlt-original-topic`,
`kafka_dlt-original-partition`, `kafka_dlt-original-offset`, `kafka_dlt-original-timestamp`,
`kafka_dlt-exception-fqcn`, `kafka_dlt-exception-message`, `kafka_dlt-exception-stacktrace`.
This step does not interpret original-coordinate header binary encodings or declare
wrapped records eligible for replay. Step 2 must implement a versioned extractor.
