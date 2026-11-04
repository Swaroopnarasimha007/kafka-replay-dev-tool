# Kafka v1 implementation

An independent Maven Java application, with an actual Apache Kafka 3.9.1 KRaft broker. It contains two producers and two consumers. Both consumers belong to `orders-demo`; Kafka distributes the three `orders` partitions between them. This is a single-broker test cluster, not a highly available production cluster.

| Topic | Partitions | Purpose |
| --- | --- | --- |
| `orders` | 3 | Application input and replay destination |
| `orders.DLT` | 3 | Failed original bytes and Spring-compatible DLT headers |
| `_replay_console_receipts` | 1 | Console transaction receipts, not an application topic |

Kafka also creates its consumer-offset and transaction-state internal topics. Source keys, values and partition numbers are preserved in DLT forwarding. The consumers atomically commit DLT forwarding and consumed offsets in Kafka transactions. Their business outcome is written to `data/events.jsonl` after commit; this diagnostic file is not a transactional business database.

## Windows: install and start

Java 17+ is required. A working JDK is already installed on this machine. The included Maven Wrapper installs Maven 3.9.9 and downloads the Kafka server/client libraries and all Java requirements into the Maven cache. Docker, ZooKeeper, and a system Maven installation are not required for this path.

From `C:\Work\kafka_project\kafka_v1_implementation`:

```powershell
.\scripts\setup.ps1
.\scripts\start.ps1
Get-Content .\data\lab-settings.json
Get-Content .\data\connection.json
```

Stop the lab before rerunning setup, because Windows locks running JAR files. Setup builds this Java repository and the parent console. Start formats new broker storage once, launches Kafka, initializes topics, starts two producer loops and two consumer loops, and starts the console. The console is at **http://localhost:8088**. Your browser asks for the username/password from `data/lab-settings.json`. The generated login is retained across restarts. Keep that file private.

The script refuses occupied ports and never stops other applications. Alternative ports:

```powershell
.\scripts\start.ps1 -BrokerPort 29092 -ControllerPort 29093 -ConsolePort 8089
```

Stop only this script's managed processes, keeping broker records and console jobs:

```powershell
.\scripts\stop.ps1
```

Each application restart generates a fresh producer batch with distinct IDs. Changing `producer.run.id` also triggers a new batch while running. Producers are finite, not continuous load generators. The consumer group retains its progress. Run only one demo application against this cluster: its two consumer transactional IDs are stable and a second instance fences the first.

## Connect and use the console

The lab console has a built-in **Kafka local demo** profile and a **Simulation** profile in the Cluster dropdown. Kafka is selected first. Simulation is still available explicitly; the old synthetic/simulation banner is removed. This choice routes browsing, planning, publishing, recovery and metrics through the selected adapter; selecting Kafka uses actual Kafka clients.

1. Select **Kafka local demo**, topic `orders.DLT`, then **Browse records**.
2. Open **Replay → Manage mappings**. Set destination `orders`. Retain the default DLT-header removal list and attest byte preservation. Create the mapping.
3. Freeze a preview, check its count, supply an approval reason, and leave the canary enabled.
4. Approve. In **Jobs**, start the one-record canary. Check `data/events.jsonl` for a `PROCESSED` entry with `replay:true`.
5. Release the remaining records. Inspect committed receipts in Jobs and consumer outcomes in the event log.

Default producers generate 60 records total: 15 validation failures from producer 1 plus 10 business rejections from producer 2 go to DLT. The other 35 are processed normally. Replay error lists are empty, so these 25 DLT records can be replayed successfully after the original consumer transaction commits. Each partition is handled by one of the two group members; scheduling and assignment determine which consumer sees a record.

Connection details are generated from broker metadata at **`data/connection.json`**, including bootstrap endpoint, actual cluster ID, advertised brokers, topic names and group. For native defaults:

| Field | Value |
| --- | --- |
| Bootstrap server | `localhost:19092` |
| Protocol | `PLAINTEXT` |
| Kafka username/password | None; native listener binds loopback |
| Source / target | `orders.DLT` / `orders` |
| Receipt topic | `_replay_console_receipts` |
| Console login | Generated separately in `data/lab-settings.json` |

To connect an existing live console, its deployment must explicitly allow plaintext and allowlist `localhost` (or the real broker hostname). In Connection, enter the metadata bootstrap endpoint and protocol, test, then save. `localhost` works only when that console JVM and this broker share the host network. The built-in profile avoids this setup in lab mode. The `lab` Spring profile is authenticated with one Admin account; the existing OIDC `live` profile remains available for production.

## Configure success and failure

Edit **`config/demo.properties`**. The running app validates and applies changes approximately once a second. A `CONFIG_APPLIED` event confirms acceptance. `CONFIG_REJECTED` means the previous valid application configuration remains active; topic Admin operations are not an atomic multi-topic update, so inspect broker settings if an Admin operation itself fails. Topic names, bootstrap and group ID require a restart. Docker overrides bootstrap with `KAFKA_BOOTSTRAP`.

| Configuration | Effect |
| --- | --- |
| `producer.N.enabled`, `count`, `interval.ms` | Enable producer 1/2 and control the next finite batch |
| `producer.N.customer`, `amount`, `payload.bytes` | Attributes and padding included in generated JSON |
| `producer.N.error`, `error.every` | Select error code; tag every Nth record (`0` disables tagging) |
| `producer.N.seed.dlt` | Send directly to DLT for transport-failure experiments |
| `producer.run.id` | Change to trigger a fresh batch on both enabled producers |
| `consumer.N.enabled`, `delay.ms` | Pause/resume that group member's assigned partitions; add processing delay |
| `consumer.N.minimum.amount`, `rejected.customers` | Derive VALIDATION for amounts below the minimum; BUSINESS_REJECT for comma-separated customer values |
| `consumer.N.initial.errors` | Error codes that cause first-delivery records to go to DLT |
| `consumer.N.replay.errors` | Error codes that cause replayed records to go back to DLT |
| `errors.forbidden` | Reject configurations injecting or enabling these errors |
| `consumer.max.dlt.hops` | Hold a poison record without committing its offset after this many DLT cycles |
| `topic.main.max.message.bytes`, `topic.dlt.max.message.bytes` | Apply actual broker topic size limits |
| `topic.main.retention.ms`, `topic.dlt.retention.ms` | Apply actual retention (`-1` keeps records indefinitely) |

Error codes: `NONE`, `VALIDATION`, `BUSINESS_REJECT`, `TRANSIENT`, `INVALID_JSON`. The first three error codes are controlled application fault injections through the JSON `error` attribute. `INVALID_JSON` generates malformed bytes and causes an actual parse failure. Consumer lists choose which errors should fail processing; an empty list bypasses those injected failures. `errors.forbidden` is an additional guard against accidentally enabling a prohibited code. Attribute rules apply when the explicit error attribute is NONE; the phase error list still decides whether the derived error should fail processing. Use identical rules and initial/replay lists for both consumers for deterministic behavior across group rebalances.

For example, configure `producer.1.error=TRANSIENT`, `producer.1.error.every=1`, both `consumer.N.initial.errors=TRANSIENT`, and both `consumer.N.replay.errors=`. Then change `producer.run.id`. Original deliveries fail and replays succeed. To make replays fail business processing too, add `TRANSIENT` to both replay lists. Preserve the `demo-dlt-hops` header in the console mapping; it distinguishes original delivery from replay and prevents unbounded loops.

Convenience presets (they change this same config and trigger a new batch):

```powershell
.\scripts\scenario.ps1 -Name replay-success
.\scripts\scenario.ps1 -Name consumer-failure
.\scripts\scenario.ps1 -Name transport-failure
.\scripts\scenario.ps1 -Name no-errors
```

| Preset | Expected result |
| --- | --- |
| `replay-success` | Original tagged failures enter DLT; replayed records process successfully |
| `consumer-failure` | Kafka replay commits, but the consumer sends tagged records to DLT again |
| `transport-failure` | Both producers seed large DLT records; the destination's 512-byte limit rejects replay publishing; the console job blocks with no committed output for the rejected transaction |
| `no-errors` | All 60 records process normally; no new DLT records are expected |

Existing records remain between presets. Use time filters in the console to isolate a new scenario. Restore `replay-success` after a transport-failure experiment to raise the destination limit, then explicitly resume the blocked job. Reloaded producer attributes are captured at the start of each batch; disabling a producer interrupts further sends, and the next run ID starts another batch.

**Kafka replay completion and business success are different.** A console receipt proves destination publication. `PROCESSED`/`DEAD_LETTERED` events prove what this demo consumer observed afterward. A consumer failure after a committed replay does not retroactively fail that console transaction. Retention-based source expiration may yield skipped outcomes in the console; retention cleanup is asynchronous, not an immediate failure switch.

## One cloud server: Kafka + demo + console + HTTPS

The Compose setup runs Kafka, the Java demo, PostgreSQL, the console, and Caddy on the same server. Build contexts expect this folder to remain inside the parent console repository. Docker Engine with Compose v2 must be installed on that server. No cloud account or deployment is created by this change.

```bash
cd kafka_v1_implementation
cp .env.example .env
# Replace BOTH passwords in .env before starting.
docker compose up -d --build
docker compose logs -f demo console
docker compose exec demo cat /app/data/connection.json
```

For local Docker, open http://localhost:8088. The browser uses the `LAB_USER`/`LAB_PASSWORD` from `.env`. To let a host-side Kafka client connect too, use `docker compose -f compose.yml -f compose.local.yml up -d --build` and connect to `localhost:19092`. Stop the native broker first if it uses that port.

For cloud, point a DNS name at the server and set:

```dotenv
SITE_ADDRESS=replay.example.com
HTTP_BIND=0.0.0.0
HTTP_PORT=80
HTTPS_BIND=0.0.0.0
HTTPS_PORT=443
```

Allow inbound TCP 80/443 for Caddy, then run the same Compose command. Open `https://replay.example.com`. Caddy obtains a certificate for the domain. If another reverse proxy already owns those ports, put the console behind that proxy instead of binding a second gateway there.

The browser calls relative `/api` routes on the same HTTPS origin. The console and demo connect to **`kafka:9092`**, which the broker advertises on the private Compose network. The browser never connects to Kafka. Kafka, PostgreSQL and the console have no host-published ports in the base Compose file; only the gateway is published. This avoids container clients receiving unusable `localhost` metadata. Do not use the local override on cloud. External Kafka clients would require a separately secured TLS/SASL listener and advertised public/private hostname.

Configuration is mounted as a directory so file replacements remain visible. Kafka, PostgreSQL, demo logs and Caddy certificates use named volumes. `docker compose down` keeps volumes; `docker compose down -v` erases the lab data and is not required for ordinary restarts. Native and Compose storage are separate clusters. This lab uses private-network plaintext Kafka and a single Admin login for testing; keep the existing secured Kafka/OIDC live deployment for production.

Reference behavior: [Apache Kafka 3.9 Docker](https://kafka.apache.org/39/getting-started/docker/), [KRaft quick start](https://kafka.apache.org/39/getting-started/quickstart/), [Compose health-based startup](https://docs.docker.com/compose/how-tos/startup-order/), [Caddy automatic HTTPS](https://caddyserver.com/docs/automatic-https).
