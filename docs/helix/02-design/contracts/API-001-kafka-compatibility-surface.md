---
ddx:
  id: api-kafka-compatibility-surface
  depends_on:
    - prd
    - adr-fjord-as-kafka-compatible-object-log-system
---

# API-001: Kafka Compatibility Surface Notes

## Purpose

This contract note defines the Kafka-facing surfaces fjord must design before
wire-protocol implementation. It is intentionally not a protocol implementation
spec. It identifies the compatibility obligations that must map onto
`object-log` and fjord's metadata/control-plane services.

## Compatibility Principles

1. Kafka compatibility is measured by standard Kafka clients and tools, not by
   internal API similarity.
2. Every supported Kafka API version must have an explicit request/response,
   error, and failure-mode contract.
3. object-log offsets are the source of partition record order, but fjord must
   supply Kafka metadata, coordinator, and visibility semantics around them.
4. Local disk may improve fetch/cache performance but cannot be required for
   acknowledged durability.

## P0 Protocol Surfaces to Specify

| Surface | Kafka API Examples | fjord Design Obligation |
|---------|--------------------|--------------------------|
| Version discovery | ApiVersions | Declare supported APIs/versions and reject unsupported versions correctly |
| Metadata | Metadata | Return topics, partitions, leaders, replicas/ISR placeholders or equivalent, endpoints, errors |
| Produce | Produce | Map record batches to object-log appends and return offsets/errors after ack boundary |
| Fetch | Fetch | Serve record batches by partition/offset with high-watermark/log-start/last-stable offsets |
| Offsets | ListOffsets, OffsetForLeaderEpoch | Resolve offsets from object-log indexes and leader epoch metadata |
| Coordinator discovery | FindCoordinator | Route group and transaction coordinators according to metadata design |
| Group membership | JoinGroup, SyncGroup, Heartbeat, LeaveGroup | Maintain durable group state, generation/member epoch, assignments, and rebalance timing |
| Offset commits | OffsetCommit, OffsetFetch | Store and retrieve committed consumer offsets durably |
| Topic/admin minimum | CreateTopics, DeleteTopics, DescribeConfigs, AlterConfigs | Provide enough admin compatibility for standard tools once L3 begins |

## Compatibility Semantics

### Produce

- Produce requests may contain records for multiple topics and partitions.
- Explicit partition selection must be honored.
- Key-based and round-robin partitioning follow standard producer expectations:
  explicit partition wins, key selects a partition when partition is omitted,
  and clients may round-robin when neither key nor partition is set.
- `acks=all` is the strongest initial compatibility target: the response must
  wait until object-log makes records durable in object storage.
- `acks=1` is an open design question because classic Kafka maps it to leader
  local append, while fjord has no durable local leader log.
- `acks=0` may accept without durability claims, but metrics and docs must make
  loss windows clear.
- Producer idempotence requires producer id, producer epoch, sequence number,
  duplicate detection, and recovery snapshots.

### Fetch

- Fetch requests must return records in partition offset order.
- Fetch must not expose gaps or corrupt segments.
- High watermark, log start offset, last stable offset, and leader epoch
  behavior must be defined for object-log partitions.
- Local cache may serve bytes, but authoritative recovery must come from
  object-log plus metadata.
- If object files can be committed or uploaded out of partition-offset order,
  an ordering authority must return the ordered file/batch list for Fetch.
- Read isolation levels are unsupported until transaction state is designed.

### Metadata and Leadership

Kafka clients expect partition leaders and refresh metadata on errors. fjord
must choose one model:

1. **Emulated leader**: metadata assigns a node as leader for each partition;
   non-leaders return Kafka stale-leader errors.
2. **Any-node service with synthetic leaders**: metadata may point all clients
   through load-balanced endpoints while fjord internally routes requests.
3. **Leaderless protocol-compatible extension**: only viable if standard
   clients continue to behave correctly.

The choice blocks Produce, Fetch, ListOffsets, and OffsetForLeaderEpoch.

WarpStream shows a likely path: manipulate Metadata responses so clients route
to healthy, preferably zone-local agents even though the underlying data plane
has no real leader partitions. fjord must decide whether to copy that pattern,
use synthetic per-AZ leaders, or choose a simpler first version with explicit
tradeoffs.

### Consumer Groups and Offsets

- Group coordinator state must survive node loss.
- OffsetCommit/OffsetFetch must be durable and efficient.
- Rebalance state must include group id, members, subscriptions, assignments,
  generation/member epoch, protocol type, and heartbeat/session timers.
- The metadata/control plane may store offsets directly or store them as an
  internal object-log topic, but the choice must be explicit.
- If the product goal is S3-only durable state, offset and group metadata must
  either be stored in object-log/internal topics or in an object-storage-backed
  metadata mechanism; Postgres/etcd-style durable metadata would need an
  explicit exception.

## object-log Dependency Contract

fjord needs object-log to provide:

- opaque Kafka record batch storage or a lossless record representation,
- topic/partition offsets,
- durable segment commit and replay,
- segment/object checksums,
- manifest/index reads by partition and offset,
- producer metadata hooks,
- retention-aware segment lifecycle hooks,
- conformance tests that can also run against a Kafka-backed implementation.

fjord must not require object-log to know Kafka wire-protocol versions, group
membership, ACLs, transactions, or admin APIs.

## Known Unsupported Until Designed

- Transactions and exactly-once semantics.
- `read_committed` isolation.
- Compacted topics.
- Full admin API coverage.
- ACLs and quotas.
- MirrorMaker/Connect certification.
- Share groups and newer queue-like Kafka semantics.

These are target compatibility areas, not implied by the initial contract.
