package dev.replay;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;

/** Canonical binary encoding v1; null, empty and repeated header values stay distinct. */
final class Manifest {
    static final String EXTRACTOR = "SPRING_DLT_BYTES_V1";
    static final String PARTITION_POLICY = "ORIGINAL_PARTITION_V1";
    static final String HEADER_POLICY = "EXPLICIT_REMOVE_V1";
    record Entry(int position, String clusterId, String topicId, String topic, int partition,
                 long offset, long sourceTimestamp, String fingerprint, int destinationPartition,
                 String keyBase64, String valueBase64, List<Browser.Header> headers) {}

    private Manifest() {}

    static Entry extract(Browser.Row row, Planner.Mapping mapping, int position) {
        if (!EXTRACTOR.equals(mapping.extractor())) throw new IllegalArgumentException("Unsupported extractor; browse-only");
        // Admin attests byte-preserving Spring DLT publication. We do not guess or unwrap envelopes.
        var originalTopic = one(row.headers(), "kafka_dlt-original-topic");
        var originalPartition = one(row.headers(), "kafka_dlt-original-partition");
        if (originalTopic == null || !new String(originalTopic, StandardCharsets.UTF_8).equals(mapping.target())
                || originalPartition == null || originalPartition.length != 4)
            throw new IllegalArgumentException("Unsupported DLT headers or unauthorized original destination; browse-only");
        int partition = ByteBuffer.wrap(originalPartition).getInt();
        if (partition < 0 || partition >= mapping.identity().targetPartitions())
            throw new IllegalArgumentException("Original destination partition unavailable");
        if (row.headers().stream().anyMatch(h -> h.name().equals("springDeserializerExceptionKey")
                || h.name().equals("springDeserializerExceptionValue")))
            throw new IllegalArgumentException("Serialized deserialization exceptions require a supported extractor; browse-only");
        var headers = row.headers().stream().filter(h -> !mapping.removeHeaders().contains(h.name())).toList();
        return new Entry(position, mapping.identity().clusterId(), mapping.identity().sourceId(),
                row.topic(), row.partition(), row.offset(), row.timestamp(), fingerprint(row), partition,
                row.keyBase64(), row.valueBase64(), headers);
    }

    static byte[] one(List<Browser.Header> headers, String name) {
        var found = headers.stream().filter(h -> h.name().equals(name)).toList();
        if (found.size() != 1 || found.get(0).base64() == null) return null;
        return Base64.getDecoder().decode(found.get(0).base64());
    }

    static String fingerprint(Browser.Row row) {
        return hash(out -> { text(out, "source-content-v1"); out.writeLong(row.timestamp());
            bytes(out, row.keyBase64()); bytes(out, row.valueBase64()); headers(out, row.headers()); });
    }

    static String digest(Planner.Mapping mapping, int rate, List<Entry> entries) {
        return hash(out -> {
            text(out, "replay-manifest-v1"); text(out, mapping.id()); out.writeInt(mapping.version());
            text(out, mapping.identity().clusterId()); text(out, mapping.identity().sourceId());
            text(out, mapping.identity().targetId()); text(out, mapping.source()); text(out, mapping.target());
            text(out, mapping.extractor()); text(out, mapping.partitionPolicy()); text(out, mapping.headerPolicy());
            out.writeInt(mapping.removeHeaders().size()); for (var name : mapping.removeHeaders()) text(out, name);
            out.writeInt(rate); out.writeInt(entries.size());
            for (var entry : entries) {
                out.writeInt(entry.position()); out.writeInt(entry.partition()); out.writeLong(entry.offset());
                text(out, entry.fingerprint()); out.writeInt(entry.destinationPartition());
                bytes(out, entry.keyBase64()); bytes(out, entry.valueBase64()); headers(out, entry.headers());
            }
        });
    }

    interface Encoder { void write(DataOutputStream out) throws Exception; }
    static String hash(Encoder encoder) {
        try { var buffer = new ByteArrayOutputStream(); var out = new DataOutputStream(buffer);
            encoder.write(out); out.flush(); return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(buffer.toByteArray()));
        } catch (Exception e) { throw new IllegalStateException("Cannot encode manifest", e); }
    }
    static void text(DataOutputStream out, String value) throws IOException {
        var bytes = value.getBytes(StandardCharsets.UTF_8); out.writeInt(bytes.length); out.write(bytes);
    }
    static void bytes(DataOutputStream out, String base64) throws IOException {
        if (base64 == null) { out.writeInt(-1); return; }
        var value = Base64.getDecoder().decode(base64); out.writeInt(value.length); out.write(value);
    }
    static void headers(DataOutputStream out, List<Browser.Header> headers) throws IOException {
        out.writeInt(headers.size()); for (var header : headers) { text(out, header.name()); bytes(out, header.base64()); }
    }
}
