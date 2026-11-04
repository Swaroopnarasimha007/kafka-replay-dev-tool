# Kafka Replay Console — step 1

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
No producer, offset commits, group subscription, or replay worker exists in step 1.

Viewer can browse masked records. Operator can also reveal bytes. Admin additionally
tests and saves profiles. Browser POSTs require the session CSRF token. Profile save
accepts only a short-lived single-use successful test token; edits require a new test.
Only profile identity and bootstrap endpoints are returned from profile APIs.

Minimum browse ACLs: cluster DESCRIBE, topic DESCRIBE and READ on discovered DLTs.
No group ACL is needed because reads use direct assignment without group membership.
Restrict discovery with `replay.topic-pattern` and/or explicit `replay.topics`.

See [API contract](docs/API.md) and [step handover](docs/HANDOVER_STEP_1.md).
