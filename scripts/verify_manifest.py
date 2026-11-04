"""Independent byte-level verifier. Usage: python scripts/verify_manifest.py fixture.json export.json"""
import base64
import hashlib
import json
import struct
import sys

def integer(value):
    return struct.pack(">i", value)

def text(value):
    data = value.encode("utf-8")
    return integer(len(data)) + data

def blob(value):
    if value is None:
        return integer(-1)
    data = base64.b64decode(value, validate=True)
    return integer(len(data)) + data

def headers(values):
    return integer(len(values)) + b"".join(text(h["name"]) + blob(h["base64"]) for h in values)

def fingerprint(row):
    payload = text("source-content-v1") + struct.pack(">q", row["timestamp"])
    payload += blob(row["keyBase64"]) + blob(row["valueBase64"]) + headers(row["headers"])
    return hashlib.sha256(payload).hexdigest()

def verify(expected, exported):
    preview, job = exported["preview"], exported["job"]
    mapping = preview["mapping"]
    entries = preview["entries"]
    ordered = sorted(expected, key=lambda row: (row["partition"], row["offset"]))
    assert len(entries) == len(ordered) == job["count"], "count mismatch"
    for position, (row, entry) in enumerate(zip(ordered, entries)):
        expected_headers = [h for h in row["headers"] if h["name"] not in mapping["removeHeaders"]]
        assert entry["position"] == position, "position mismatch"
        for field in ("topic", "partition", "offset", "keyBase64", "valueBase64"):
            assert entry[field] == row[field], (position, field)
        assert entry["sourceTimestamp"] == row["timestamp"]
        assert entry["headers"] == expected_headers, (position, "ordered output headers")
        assert entry["fingerprint"] == fingerprint(row), (position, "source fingerprint")
        original = [h for h in row["headers"] if h["name"] == "kafka_dlt-original-partition"]
        assert entry["destinationPartition"] == struct.unpack(">i", base64.b64decode(original[0]["base64"]))[0]
        assert entry["clusterId"] == mapping["identity"]["clusterId"]
        assert entry["topicId"] == mapping["identity"]["sourceId"]
    identity = mapping["identity"]
    encoded = text("replay-manifest-v1") + text(mapping["id"]) + integer(mapping["version"])
    for value in (identity["clusterId"], identity["sourceId"], identity["targetId"], mapping["source"],
                  mapping["target"], mapping["extractor"], mapping["partitionPolicy"], mapping["headerPolicy"]):
        encoded += text(value)
    encoded += integer(len(mapping["removeHeaders"]))
    encoded += b"".join(text(name) for name in mapping["removeHeaders"])
    encoded += integer(preview["rate"]) + integer(len(entries))
    for entry in entries:
        encoded += integer(entry["position"]) + integer(entry["partition"]) + struct.pack(">q", entry["offset"])
        encoded += text(entry["fingerprint"]) + integer(entry["destinationPartition"])
        encoded += blob(entry["keyBase64"]) + blob(entry["valueBase64"]) + headers(entry["headers"])
    digest = hashlib.sha256(encoded).hexdigest()
    assert digest == preview["digest"] == job["digest"], "manifest digest mismatch"
    print(f"PASS: {len(entries)} exact records; keys, values, ordered headers, coordinates, fingerprints and digest {digest}")

if __name__ == "__main__":
    with open(sys.argv[1], encoding="utf-8") as handle:
        expected = json.load(handle)
    with open(sys.argv[2], encoding="utf-8") as handle:
        exported = json.load(handle)
    verify(expected, exported)
