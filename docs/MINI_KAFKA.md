# Check Mini Kafka details

Start Kafka Dev Tool with the [root README instructions](../README.md#run-it-yourself). Start Mini Kafka separately in a second PowerShell terminal at the repository root.

```powershell
./scripts/start-mini-kafka.ps1
```

## Connection metadata

Once Mini Kafka connects to its broker, it writes the bootstrap address, cluster ID, advertised broker endpoints, topic names, partition count and consumer group to its data directory. Run these from a separate terminal at the repository root.

```powershell
./kafka_v1_implementation/scripts/connection-info.ps1
Get-Content ./kafka_v1_implementation/data/connection.json
```

The default bootstrap address is `localhost:19092`. The protocol is `PLAINTEXT`, with blank Kafka credentials. A saved `connection.json` can persist between runs. Use **Test connection** in the tool to verify that the broker is reachable now.

To use a different port, start with `./scripts/start-mini-kafka.ps1 -BrokerPort 29092 -ControllerPort 29093` and update the prefilled bootstrap address in the tool before testing.

After pulling changes to the test application, stop it and start with `./scripts/start-mini-kafka.ps1 -Rebuild` to rebuild the local JAR.

## Inspect dead-letter records

1. In **Connection**, select **Mini Kafka implementation**. Check the prefilled details and choose **Test connection**, then **Save and open**.
2. Choose `orders.DLT` and select **Browse records**. If earlier records are retained, set the `from` filter to the start of your current run to focus on newly generated orders.
3. Use **Reveal record** on a masked record if your role permits it. Inspect the decoded order payload, original Kafka coordinates and trace headers when present.

## Verify consumer outcomes

The Mini Kafka terminal streams producer and consumer events. To follow the same events from another terminal, run:

```powershell
Get-Content ./kafka_v1_implementation/data/events.jsonl -Tail 20 -Wait
```

Look for `CONSUMER_EXCEPTION`, `DEAD_LETTERED` and `PROCESSED`. The event includes a timestamp in epoch milliseconds. Consumer outcomes also include the partition and offset. After a replay, look for `PROCESSED` with `"replay":true` at the destination partition and offset shown in the publication receipt.

The default batch sends 60 orders, including 25 configured failures. To generate another batch with the replay-success preset, run:

```powershell
./kafka_v1_implementation/scripts/scenario.ps1 -Name replay-success
```

Keep `demo-dlt-hops` in your replay mapping's header policy. Existing events and Kafka records persist between runs. For additional presets and configuration options, see the [Mini Kafka application README](../kafka_v1_implementation/README.md).

## Stop Mini Kafka

Press Ctrl+C in the Mini Kafka terminal or run `./scripts/stop-mini-kafka.ps1` from the repository root in another terminal. The script stops the managed test application and broker. Broker data and logs remain available in `kafka_v1_implementation/data`.
