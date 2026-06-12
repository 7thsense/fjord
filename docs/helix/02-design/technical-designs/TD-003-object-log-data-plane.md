---
ddx:
  id: td-object-log-data-plane
  depends_on:
    - td-kafka-protocol-gateway
    - td-metadata-routing-coordination
    - api-kafka-compatibility-surface
    - prd
---

# Technical Design: TD-003 object-log Data Plane

## Scope

Map Kafka produce/fetch semantics onto object-log without making Fjord's local
nodes authoritative for durable log data.

## Produce Mapping

1. Decode Produce request and preserve Kafka record batch bytes.
2. Resolve topic, partition, owner epoch, and append timestamp policy.
3. Convert Kafka records to object-log records while carrying key, value,
   headers, timestamp, producer id, producer epoch, and sequence metadata.
4. Append through object-log using the configured ack boundary.
5. Return Kafka ProduceResponse offsets only after object-log says the batch is
   committed for the requested durable mode.

Acks map onto object-log's `AckMode` (CONTRACT-001): `acks=0` maps to
`AckMode::None`, which may return before durability but must not imply
committed offsets (object-log returns no offsets for uncommitted appends).
`acks=all` maps to `AckMode::All`, object-log's durable commit boundary.
`acks=1` maps to `AckMode::Leader`, which the object backend may map to `All`
or reject; Fjord must pick one behavior per deployment profile rather than
weaken safety silently, because Fjord has no leader-local durable boundary.

## Fetch Mapping

1. Resolve topic partition and requested fetch offset.
2. Use object-log manifests/indexes to locate segments.
3. Read from local cache when present; otherwise read object storage.
4. Validate checksums and offset continuity before returning data.
5. Encode FetchResponse with high watermark, log start offset, and leader epoch
   from metadata state.

## Batching and Cost

Fjord must batch enough to avoid one object per record. Configurable thresholds:

- minimum segment bytes,
- maximum segment bytes,
- maximum commit delay,
- maximum records per segment,
- partition coalescing rules where ordering allows it.

The gateway's bounded connection queue is part of the batching story: requests
can accumulate while a writer waits on object-log commit latency.

## Tests

- Produce/fetch round trip through `ObjectLogBackend` over `MemoryObjectStore`.
- Produce/fetch round trip through `ObjectLogBackend` over `LocalObjectStore`.
- Durable ack failure when object-log append fails.
- Corruption fixture fails Fetch before returning records.
- Config rejection for tiny-object production profile.

