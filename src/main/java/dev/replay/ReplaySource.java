package dev.replay;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.springframework.stereotype.Service;

/** Read-only source adapter. No producer or consumer group exists in the planner. */
@Service
class ReplaySource {
    record Identity(String clusterId, String sourceId, String targetId, int targetPartitions) {}
    record Scan(List<Browser.Row> records, Map<Integer,Long> beginnings,
                Map<Integer,Long> boundaries, Map<Integer,Long> cursors, int scanned, boolean partial) {}
    private final Profiles profiles;
    private final Browser browser;

    ReplaySource(Profiles profiles, Browser browser) {
        this.profiles = profiles;
        this.browser = browser;
    }

    Identity identity(String profileId, String source, String target) {
        if (!browser.approved(source)) throw new IllegalArgumentException("Source topic is outside discovery policy");
        if (source.equals(target)) throw new IllegalArgumentException("Source and destination must differ");
        var profile = profiles.list().stream().filter(p -> p.id().equals(profileId)).findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unknown profile"));
        if (!profiles.live) {
            if (!source.equals("orders.DLT") || !target.equals("orders"))
                throw new IllegalArgumentException("Demo supports orders.DLT → orders only");
            return new Identity("demo-cluster", "demo-orders-dlt-v1", "demo-orders-v1", 3);
        }
        try (Admin admin = Admin.create(profiles.props(profiles.credentials(profileId)))) {
            String clusterId = admin.describeCluster().clusterId().get(7, TimeUnit.SECONDS);
            if (!profile.clusterId().equals(clusterId)) throw new Planner.Conflict("Cluster identity changed");
            var topics = admin.describeTopics(List.of(source, target)).allTopicNames().get(7, TimeUnit.SECONDS);
            var from = topics.get(source); var to = topics.get(target);
            if (from.topicId().equals(org.apache.kafka.common.Uuid.ZERO_UUID)
                    || to.topicId().equals(org.apache.kafka.common.Uuid.ZERO_UUID))
                throw new IllegalArgumentException("Broker must support topic UUIDs");
            return new Identity(clusterId, from.topicId().toString(), to.topicId().toString(), to.partitions().size());
        } catch (Planner.Conflict | IllegalArgumentException e) { throw e; }
        catch (Exception e) { throw Profiles.failure(e); }
    }

    Scan scan(String profileId, String topic, Browser.Query filter, int maxScanned) {
        if (filter.cursors() != null && !filter.cursors().isEmpty())
            throw new IllegalArgumentException("Preview starts at retained beginnings; browse cursors are not selections");
        if (!profiles.live) {
            var page = browser.demo(topic, filter, true, maxScanned, maxScanned);
            return new Scan(page.records(), page.beginnings(), page.boundaries(), page.cursors(), page.scanned(), page.truncated());
        }
        Properties config = profiles.props(profiles.credentials(profileId));
        config.put("key.deserializer", ByteArrayDeserializer.class);
        config.put("value.deserializer", ByteArrayDeserializer.class);
        config.put("enable.auto.commit", "false"); config.put("isolation.level", "read_committed");
        config.put("allow.auto.create.topics", "false"); config.put("max.poll.records", "200");
        try (var consumer = new KafkaConsumer<byte[],byte[]>(config)) {
            var partitions = consumer.partitionsFor(topic).stream()
                    .filter(p -> filter.partition() == null || filter.partition() == p.partition())
                    .map(p -> new TopicPartition(topic, p.partition()))
                    .sorted(Comparator.comparingInt(TopicPartition::partition)).toList();
            if (partitions.isEmpty()) throw new IllegalArgumentException("Partition unavailable");
            consumer.assign(partitions);
            var starts = consumer.beginningOffsets(partitions);
            // Captured once, before polling, across all partitions. Never refreshed during this preview.
            var ends = consumer.endOffsets(partitions);
            var beginnings = new TreeMap<Integer,Long>(); var boundaries = new TreeMap<Integer,Long>();
            var cursors = new TreeMap<Integer,Long>(); var rows = new ArrayList<Browser.Row>();
            for (var tp : partitions) {
                beginnings.put(tp.partition(), starts.get(tp)); boundaries.put(tp.partition(), ends.get(tp));
                cursors.put(tp.partition(), starts.get(tp)); consumer.seek(tp, starts.get(tp));
            }
            // Scan partitions in ascending order so a capped scan has deterministic selection order.
            consumer.pause(partitions);
            int scanned = 0;
            long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
            outer: for (var tp : partitions) {
                consumer.resume(List.of(tp));
                while (cursors.get(tp.partition()) < ends.get(tp)) {
                    if (scanned >= maxScanned || System.nanoTime() >= deadline) break outer;
                    var batch = consumer.poll(Duration.ofMillis(250));
                    for (var record : batch.records(tp)) {
                        if (record.offset() >= ends.get(tp)) break;
                        scanned++; cursors.put(tp.partition(), record.offset() + 1);
                        if (browser.matches(record, filter)) rows.add(Browser.row(record, true));
                        if (scanned >= maxScanned) break outer;
                    }
                    cursors.put(tp.partition(), Math.min(consumer.position(tp), ends.get(tp)));
                }
                consumer.pause(List.of(tp));
            }
            boolean partial = partitions.stream().anyMatch(tp -> cursors.get(tp.partition()) < ends.get(tp));
            return new Scan(List.copyOf(rows), beginnings, boundaries, cursors, scanned, partial);
        } catch (IllegalArgumentException e) { throw e; }
        catch (Exception e) { throw Profiles.failure(e); }
    }
}
