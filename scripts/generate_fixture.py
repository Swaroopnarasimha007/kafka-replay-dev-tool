"""Generate the immutable, byte-level input for the step-2 real-broker test."""
import base64
import json
import pathlib
import struct

def b64(value):
    return None if value is None else base64.b64encode(value).decode("ascii")

def record(i):
    partition = i % 3
    return {
        "topic": "orders.DLT", "partition": partition, "offset": i // 3,
        "timestamp": 1735689600000 + i * 1000,
        "keyBase64": b64(None if i % 19 == 0 else bytes([0, 255, i % 256])),
        "valueBase64": b64(None if i % 17 == 0 else bytes([i % 256, 0, 255])),
        "headers": [
            {"name": "kafka_dlt-original-topic", "base64": b64(b"orders")},
            {"name": "kafka_dlt-original-partition", "base64": b64(struct.pack(">i", partition))},
            {"name": "trace", "base64": b64(b"first")},
            {"name": "trace", "base64": b64(bytes([0, 255]))},
            {"name": "retry-attempts", "base64": b64(struct.pack(">i", 3))},
            {"name": "business-null", "base64": None},
        ],
    }

if __name__ == "__main__":
    destination = pathlib.Path(__file__).resolve().parents[1] / "docs/fixtures/source-500.json"
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes((json.dumps([record(i) for i in range(500)], indent=2) + "\n").encode("utf-8"))
    print(destination)
