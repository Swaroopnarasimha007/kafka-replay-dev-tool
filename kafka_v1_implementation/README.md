# Mini Kafka

A standalone Kafka test application for Kafka Dev Tool. Start the tool separately using the [root README](../README.md).

~~~powershell
cd kafka_v1_implementation
./scripts/setup.ps1
./scripts/start.ps1
~~~

This terminal runs Kafka, two producers and two consumers. Press Ctrl+C to stop the application and broker. Stop it before rebuilding.

Fetch the broker's connection attributes in another terminal.

~~~powershell
cd kafka_v1_implementation
./scripts/connection-info.ps1
Get-Content ./data/connection.json
~~~

In the separately running console's Connection tab, enter a name, localhost:19092 and PLAINTEXT. Leave Kafka credentials blank, choose Test connection, then Save and open. Console startup commands are in [the console README](../README.md).

## 2. Configuration options

Change broker ports.

~~~powershell
./scripts/start.ps1 -BrokerPort 29092 -ControllerPort 29093
~~~

Edit configuration or select a preset while the application runs.

~~~powershell
notepad ./config/demo.properties
./scripts/scenario.ps1 -Name replay-success
# Other choices: consumer-failure, transport-failure, no-errors
~~~

| Property | Effect |
| --- | --- |
| producer.N.enabled, count, interval.ms | Enable, size and pace a producer batch |
| producer.N.customer, amount, payload.bytes | Generated order attributes |
| producer.N.error, error.every | Select an injected error code and its frequency |
| producer.run.id | Change to trigger a fresh finite batch |
| consumer.N.enabled, delay.ms | Pause or slow a consumer |
| consumer.N.minimum.amount | Derive validation failure below this amount |
| consumer.N.rejected.customers | Comma-separated customers to reject |
| consumer.N.initial.errors | Exception codes enabled on first delivery |
| consumer.N.replay.errors | Exception codes enabled on replay |
| errors.forbidden | Reject configurations enabling these codes |
| consumer.max.dlt.hops | Hold records after this DLT cycle limit |
| topic.main.max.message.bytes, topic.dlt.max.message.bytes | Actual broker size limits |
| topic.main.retention.ms, topic.dlt.retention.ms | Actual retention, -1 keeps records |
| producer.N.seed.dlt | Explicit direct-DLT transport-failure fixture |

N is 1 or 2. Codes are NONE, VALIDATION, BUSINESS_REJECT, TRANSIENT and INVALID_JSON. Use matching consumer rules for deterministic behavior across group rebalances. Change the producer run ID to generate another batch. Topic names, bootstrap and group changes require a restart.

For another server, edit config/broker.properties so listeners bind a reachable interface and advertised.listeners advertises a reachable DNS name. Configure clients with that address. The native launcher overrides the demo client's bootstrap to the local broker port.

## Independent Docker startup

Install Docker Engine and Compose v2. From this directory run:

```sh
docker compose -f compose.yml -f compose.local.yml up --build
```

This starts only Kafka and the test producers/consumers. Connect Kafka Dev Tool running on the host to `localhost:19092` using PLAINTEXT. For a remote host, adjust the external advertised listener and port binding to reachable addresses before starting. Stop with Ctrl+C or `docker compose -f compose.yml -f compose.local.yml down`. Named volumes retain records.

## 3. Connection attributes and explanation

This is an independent Maven Java application. Its setup script builds only its Kafka/demo JAR. Its runtime and data belong to this directory.

data/connection.json is generated from actual Kafka metadata. scripts/connection-info.ps1 prints the UI fields, cluster ID and advertised broker endpoints. The default loopback listener uses PLAINTEXT with no Kafka username/password. For a secured deployment obtain SASL credentials and CA certificates from its Kafka administrator. Passwords cannot be retrieved from broker metadata.

Two producers send orders. Two consumers share orders-demo. The application topics orders and orders.DLT each have three partitions. Kafka also creates internal transaction/group topics and the console's receipt topic.

The default batch sends 60 records. Consumer validation rejects 15 and business rules reject 10. Exceptions are thrown inside the consumer and caught by its DLT handling. Original bytes, partition and actual exception class are retained in the DLT record. Consumer offsets and DLT publication commit in one Kafka transaction.

CONSUMER_EXCEPTION, DEAD_LETTERED and PROCESSED events appear in the foreground terminal and data/events.jsonl. Replay success enables initial faults and permits replay processing. Consumer failure also rejects replayed records. Transport failure directly seeds oversized DLT fixtures and lowers the destination size limit. No-errors disables configured faults.

Existing records remain between scenarios. Use time filters for each run. Keep demo-dlt-hops in the console mapping's header policy. Receipts confirm Kafka publication. Consumer logs confirm business outcomes.

Broker and consumer offsets persist in this directory's data folder. Each application restart produces a new finite batch. Run one demo instance per cluster because consumer transactional IDs are stable.

The native Kafka runtime has been exercised locally. Validate Docker execution in your environment.
