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
5. **Version floor (decided 2026-06-12)**: fjord supports each API from its
   first flexible version (KIP-482) upward — modern clients only; legacy
   non-flexible versions are explicitly unsupported and rejected with
   `UNSUPPORTED_VERSION`. One bootstrap exception: ApiVersions **requests**
   must still be parsed at legacy versions (v0+), because version negotiation
   itself starts there and the error path must remain v0-decodable. Exact
   per-API floors are pinned in TD-001's matrix and validated by the fixture
   clients before each level is claimed.
6. **SASL/TLS intent**: L1 ships the connection-level *plumbing* (TLS
   termination hooks, SASL handshake scaffolding in the gateway and shared
   crate) with enforcement off; authentication/authorization *enforcement*
   and ACLs are L3 surface (PRD P2). FEAT-001's "SASL/TLS hooks" and the
   PRD's L3 security scope refer to these two different things.

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
- `acks=1` (decided 2026-06-12): fjord **upgrades it to the durable commit
  boundary by default** — same behavior as `acks=all`, acknowledged only
  after object-log durable commit. fjord has no leader-local durable log, so
  the classic leader-append meaning does not exist; upgrading keeps standard
  clients and tools working at the cost of higher-than-expected ack latency,
  which the published latency/cost profiles (FR-27) must disclose. A
  deployment profile may opt into rejecting `acks=1` instead; silent
  weaker-than-durable behavior is never an option.
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
- Read isolation levels (`read_committed`/`read_uncommitted`) are **Accept**,
  designed in TD-008 (LSO-bounded `read_committed`, aborted-data filtering).

### Metadata and Leadership

Kafka clients expect partition leaders and refresh metadata on errors.
**Decided (ADR-007, supersedes ADR-003's framing)**: the partition leader is the
**single dataless owner of the partition's sequencer shard** — it is the real
sequencing authority (not "presentation only"), but owns no durable data and
moves with a metadata-only handoff. Metadata names that owner; non-owners return
`NOT_LEADER_OR_FOLLOWER` and clients reroute. Leader epoch reflects **ownership
changes** (TD-007), persisted before the new owner is announced; because object
logs are immutable, Kafka-sense log truncation never occurs (a registered parity
difference). Owner-failover produce-unavailability is a disclosed, bounded budget
(ADR-007 N-B2). The earlier "emulated single leader / leaderless extension"
framing (ADR-003) is superseded.

### Consumer Groups and Offsets

- Group coordinator state must survive node loss.
- OffsetCommit/OffsetFetch must be durable and efficient.
- Rebalance state must include group id, members, subscriptions, assignments,
  generation/member epoch, protocol type, and heartbeat/session timers.
- **Decided (ADR-008, supersedes ADR-004 here)**: committed offsets and group
  metadata live in the **central coordinator** (COORD-001 `group_state` /
  `committed_offsets`, default self-hosted Postgres). Group coordination is a
  coordinator transaction (monotonic generation enforced by the store, TD-007).
  The object-log-internal-topic placement is an optional, non-default coordinator
  backend; a *hosted* metadata service is never required.

## object-log Dependency Contract

The normative surface fjord consumes is defined on the object-log side in
`object-log` CONTRACT-001 (core log API) and CONTRACT-002 (object store API).
This section records fjord's requirements against that contract and their
current status.

Provided today by object-log v1 (`CONTRACT-001`):

- lossless record representation: `AppendRecord`/`AppendedRecord` with key,
  value, ordered headers, timestamps, and attributes; payload bytes are opaque,
- topic/partition offsets: per-`TopicPartition` contiguous offsets with
  `ReadBatch.high_watermark`,
- durable segment commit and replay: `AckMode::All` returns offsets only after
  the durable boundary (manifest CAS on the object backend),
- acks mapping surface: `AckMode::None`/`Leader`/`All` align with Kafka
  `acks=0/1/all`; the object backend may map `Leader` to `All` or reject it,
- producer metadata hooks: `ProducerState` (producer id, epoch, base sequence)
  for duplicate suppression,
- caller-owned fencing: `EpochGuard` checked before durable commit,
- backends for testing: `MemoryObjectStore` and `LocalObjectStore` behind the
  `ObjectStore` trait.

Required by fjord but still pending in object-log (tracked in object-log's
implementation plan):

- S3-compatible adapter (object-log M3),
- retention-aware segment lifecycle hooks and snapshots (object-log M2),
- shared conformance fixtures that can also run against a Kafka-backed
  implementation (object-log M1/M4).

fjord must not require object-log to know Kafka wire-protocol versions, group
membership, ACLs, transactions, or admin APIs.

## Kafka Feature Capability Matrix (AR-2026-06-14b N-B5)

"Fully Kafka-capable" needs a falsifiable boundary, not a vague aspiration. Every
Kafka feature is classified **Accept** (in the parity surface; client-observable
behavior must match Kafka), **Defer** (planned, with a milestone; not yet on the
parity surface; client gets a correct "not supported yet" error, never silent
wrong behavior), or **Excluded** (structurally not provided by this architecture;
declared with a client-visible error and an entry in TP-003's expected-divergence
register). Anything Accept that diverges on the produce/fetch/commit surface is a
parity *failure* (B-10), not a registrable divergence.

| Feature | Class | Basis |
|---------|-------|-------|
| Produce/Fetch, idempotent producer, consumer groups, offsets, metadata, ApiVersions (flexible) | **Accept** | Core P0 (TD-005/006/007) |
| **Transactions / EOS / `read_committed`** | **Accept** | **Full design now (TD-008), per 2026-06-15 decision** — was "deferred," now in the parity surface; gated by TD-008's test set |
| ListOffsets, OffsetForLeaderEpoch | **Accept** | TD-007 (epoch reflects ownership, "no truncation" registered divergence) |
| CreateTopics/DeleteTopics/Describe/Alter configs (admin minimum) | **Accept** (L3) | Enough for standard tools (P0 table) |
| Compacted topics (log compaction) | **Defer** | Interacts with L0/L1 compaction (TD-005); milestone post-M5; client error until then |
| ACLs / quotas / SASL-SCRAM-OAUTH enforcement | **Defer** | L3 security scope (PRD P2); plumbing-only at L1 |
| MirrorMaker / Kafka Connect certification | **Defer** | Ecosystem cert milestone; inherits heimq FEAT-005 |
| **Follower fetch (KIP-392)** | **Excluded** | Structural: there are no replicas — only a dataless movable owner (ADR-007). No follower to fetch from. Registered divergence. |
| **Share groups / queues (KIP-932)** | **Excluded** (revisit) | Not in scope for v1; revisit as a post-parity feature. Registered. |
| **Tiered-storage protocol surfacing (KIP-405)** | **Excluded** | fjord *is* object-storage-native; it does not present the KIP-405 broker-tiered-storage protocol. Behavior registered as a deliberate difference. |
| Rack-aware / follower-read locality | **Excluded** (v1) | Tied to follower fetch; revisit with multi-AZ read routing (TD-006). |

Defer/Excluded items must surface a **client-visible error** (e.g.
`UNSUPPORTED_VERSION`/`UNSUPPORTED_FOR_MESSAGE_FORMAT`/feature-specific error),
never silent wrong behavior. Excluded items have entries in TP-003's
expected-divergence register with a rationale. This matrix is the frozen
"supported surface" the B-10 parity gate measures against; changing a class
requires an API-001 edit with review, not a test-config change.
