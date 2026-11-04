package dev.replay;

import java.util.List;

/** The worker depends on a committed receipt log, never a PostgreSQL progress guess. */
interface ReplayTransport {
    record Anchor(String topic, String topicId, long origin) {}
    record Validation(Manifest.Entry entry, String status) {}
    record Outcome(int position, String clusterId, String sourceTopicId, String sourceTopic,
                   int sourcePartition, long sourceOffset, String fingerprint, String status,
                   String destinationTopic, Integer destinationPartition, Long destinationOffset,
                   Long destinationTimestamp) {}
    record Receipt(int version, String jobId, String manifestDigest, String targetTopicId,
                   int batchIndex, int fromPosition, int nextPosition, String previousHash,
                   long committedAt, List<Outcome> outcomes) {}
    record Committed(Receipt receipt, long receiptOffset, String hash) {}
    record History(long stableBoundary, List<Committed> receipts) {}
    record Batch(Planner.StoredJob plan, int index, int fromPosition, String previousHash,
                 List<Validation> validations) {}
    enum Point { CANARY_ACKNOWLEDGED, BEFORE_COMMIT, AFTER_COMMIT_BEFORE_PROJECTION }
    @FunctionalInterface interface Hook {
        void at(Point point);
        Hook NONE = point -> {};
    }
    final class Blocked extends RuntimeException {
        Blocked(String reason) { super(reason); }
    }
    interface Session extends AutoCloseable {
        /** Must fence/resolve previous transactions before history() captures its stable boundary. */
        void initialize();
        Anchor newAnchor();
        History history(Anchor anchor);
        List<Validation> validate(List<Manifest.Entry> entries);
        Committed publish(Batch batch, Hook hook);
        @Override void close();
    }
    Session open(Planner.StoredJob job);
}
