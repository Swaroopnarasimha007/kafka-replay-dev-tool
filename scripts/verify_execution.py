"""Verify read_committed Kafka output against an independently encoded 500-record approval."""
import hashlib
import json
import sys
from verify_manifest import verify

def verify_execution(expected, exported):
    manifest = exported["manifest"]
    verify(expected, manifest)
    entries = manifest["preview"]["entries"]
    outputs = exported["outputs"]
    receipts = exported["dashboard"]["receipts"]
    assert len(outputs) == len(entries) == 500
    by_position = {row["position"]: row for row in outputs}
    assert len(by_position) == 500, "duplicate/unapproved manifest positions"
    acknowledged = set()
    positions = []
    previous = "GENESIS"
    for index, committed in enumerate(receipts):
        receipt = committed["receipt"]
        assert receipt["batchIndex"] == index
        assert receipt["previousHash"] == previous
        encoded = json.dumps(receipt, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        assert hashlib.sha256(encoded).hexdigest() == committed["hash"], "receipt hash mismatch"
        previous = committed["hash"]
        for outcome in receipt["outcomes"]:
            assert outcome["status"] == "PUBLISHED", "unexpected exception outcome"
            position = outcome["position"]
            entry = entries[position]
            observed = by_position[position]
            coordinate = (outcome["destinationTopic"], outcome["destinationPartition"], outcome["destinationOffset"])
            assert coordinate not in acknowledged, "duplicate acknowledged destination"
            acknowledged.add(coordinate)
            assert coordinate == (observed["topic"], observed["partition"], observed["offset"])
            assert observed["partition"] == entry["destinationPartition"]
            for field in ("keyBase64", "valueBase64", "headers"):
                assert observed[field] == entry[field], (position, field)
            for source, target in (("sourceTopic", "topic"), ("sourcePartition", "partition"), ("sourceOffset", "offset"), ("fingerprint", "fingerprint")):
                assert outcome[source] == entry[target]
            positions.append(position)
    assert positions == list(range(500)), "missing, repeated or unapproved positions"
    assert len(receipts[0]["receipt"]["outcomes"]) == 1, "canary was not one record"
    assert positions.count(0) == 1, "canary repeated"
    assert exported["dashboard"]["job"]["state"] == "COMPLETED"
    result = {"verified": 500, "missing": 0, "duplicate": 0, "unapproved": 0, "canary": 1,
              "digest": manifest["job"]["digest"], "jobId": manifest["job"]["id"],
              "batches": len(receipts), "source": "independent read_committed consumer + byte/digest verifier",
              "firstSource": {"topic": entries[0]["topic"], "partition": entries[0]["partition"], "offset": entries[0]["offset"]},
              "firstDestination": {"topic": outputs[0]["topic"], "partition": outputs[0]["partition"], "offset": outputs[0]["offset"]}}
    print(json.dumps(result, indent=2))
    return result

if __name__ == "__main__":
    with open(sys.argv[1], encoding="utf-8") as handle:
        expected = json.load(handle)
    with open(sys.argv[2], encoding="utf-8") as handle:
        exported = json.load(handle)
    result = verify_execution(expected, exported)
    if len(sys.argv) > 3:
        with open(sys.argv[3], "w", encoding="utf-8", newline="\n") as handle:
            json.dump(result, handle, indent=2)
            handle.write("\n")
