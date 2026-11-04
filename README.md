~~~powershell
cd C:/Work/kafka_project
./scripts/setup-console.ps1
./scripts/start-console.ps1
~~~

Open http://localhost:8088. Retrieve the generated console login in another terminal.

~~~powershell
cd C:/Work/kafka_project
Get-Content ./data/console-login.json
~~~

The terminal stays attached. Press Ctrl+C to stop the console. Stop it before rebuilding.

In Connection enter your Kafka bootstrap address and protocol, choose Test connection, then Save and open. Connections and jobs persist in this console directory's data folder.

For the included demo, get the broker attributes from its independent directory.

~~~powershell
cd C:/Work/kafka_project/kafka_v1_implementation
./scripts/connection-info.ps1
~~~

Enter localhost:19092 and PLAINTEXT. Leave Kafka username/password blank. Kafka startup commands are in [its README](kafka_v1_implementation/README.md).

## 2. Configuration options

Choose another console port.

~~~powershell
./scripts/start-console.ps1 -ConsolePort 8089
~~~

Allow external broker hostnames before starting.

~~~powershell
$env:REPLAY_ALLOWED_HOSTS='broker-a.example.com,broker-b.example.com'
./scripts/start-console.ps1
~~~

Allowlist all hostnames advertised by Kafka. The lab profile supports PLAINTEXT. For SASL_SSL, use the live profile with PostgreSQL, OIDC, encryption-key and broker settings supplied through config/application-live.example.yml.

~~~powershell
$env:SPRING_PROFILES_ACTIVE='live'
java -jar ./target/replay-console-lab.jar --spring.config.additional-location=file:./config/application-live.example.yml
~~~

Run existing tests after stopping the console.

~~~powershell
./mvnw.cmd -B '-Dreplay.artifact-name=replay-console-lab' verify
~~~

## 3. Connection attributes and explanation

| UI field | Where to get it |
| --- | --- |
| Name | Choose a recognizable label |
| Bootstrap | Kafka operator, or the demo's scripts/connection-info.ps1 |
| Protocol | Broker listener configuration |
| Kafka username/password | Kafka administrator for SASL_SSL, blank for the native demo |
| CA PEM | Kafka administrator's CA chain when needed |
| Replay username/password | Separate worker credentials supplied by the Kafka administrator for SASL_SSL |
| Cluster ID | Returned by Test connection from broker metadata |

The console login is generated in this directory's data/console-login.json. Kafka authentication settings belong to the Kafka deployment. Kafka passwords cannot be fetched from broker metadata.

The console starts independently when Kafka is offline. Register a reachable broker through Connection. Each application owns its own runtime and data directory.

For separate servers, Kafka bootstrap and advertised listener addresses must be reachable from the console server. Inside the supplied Compose network use kafka:9092. The browser uses the console's relative /api endpoints.

Browse DLT records, create an authorized mapping, freeze a preview and approve it. In Jobs, start a canary, check the consumer outcome and release the remaining records. Receipts confirm publication. Consumer logs show downstream processing.

The native lab uses persistent H2 storage and a generated Admin login. The live profile supports PostgreSQL, OIDC and SASL_SSL. A JDK 17 or newer is required. The Maven Wrapper installs Maven and frontend build dependencies.

Additional release details are in docs/RELEASE_RUNBOOK.md. Use the launch commands at the top of this README.
