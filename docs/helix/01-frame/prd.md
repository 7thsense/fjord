---
ddx:
  id: prd
  depends_on:
    - product-vision
    - research-prior-art
    - concerns
---

# Product Requirements Document

## Summary

fjord is a Kafka-compatible streaming system backed for durable log data by
object storage through the embeddable `object-log` core. It should eventually
support normal Kafka producer and consumer workflows while replacing stateful
broker-local log storage with object-storage-backed segments, metadata, indexes,
and caches designed for Kafka semantics.

The product must be honest about scope. The target is full Kafka producer and
consumer functionality, but implementation must proceed through documented
compatibility levels. Produce/fetch, metadata, offsets, and consumer groups are
P0 design surfaces. Transactions, exactly-once semantics, compaction, ACLs, and
the full admin surface may be phased, but they must not be hidden.

## Problem and Goals

### Problem

Kafka is operationally expensive when durable active log data is tied to
stateful broker disks and cross-zone replication. Object storage can reduce
storage and replication cost, but naive Kafka-on-S3 designs either write too
many small objects or introduce unacceptable latency. fjord exists to explore a
Kafka-compatible system that makes the tradeoff explicit: simpler operations and
lower storage cost in exchange for object-storage-shaped batching, indexing,
and fetch/cache design.

### Goals

1. Provide a Kafka-compatible service surface for standard producers and
   consumers.
2. Use object storage as the exclusive durable data-plane log substrate.
3. Reuse `object-log` as the embedded core for Kafka-compatible records,
   partitions, offsets, object segments, manifests, and replay.
4. Define the metadata/control-plane responsibilities needed for Kafka
   compatibility before implementation.
5. Validate semantics and performance with standard Kafka clients and tools.
6. Establish whether fjord has enough differentiation from WarpStream, AutoMQ,
   Bufstream, and Kafka Diskless Topics to justify implementation.

### Non-Goals

- Implement code or a Kafka wire protocol in this documentation pass.
- Build a Kafka Connect S3 sink or classic Kafka tiered-storage plugin.
- Claim production Kafka compatibility before protocol, group, offset, failure,
  and performance tests pass.
- Store acknowledged log data durably on local broker disk.
- Put pqueue or Niflheim domain semantics into fjord.
- Pretend fjord is differentiated if its design collapses into a less mature
  WarpStream clone.

## Users and Scope

### Personas

| Persona | Goal | Pain |
|---------|------|------|
| Kafka operator | Run Kafka-compatible workloads with less broker storage management | Rebalances, disk sizing, replica traffic, and broker replacement are costly |
| Application developer | Keep using standard Kafka clients | Switching APIs is expensive and breaks tooling |
| Platform engineer | Offer a lower-cost profile for latency-tolerant streams | Standard Kafka clusters are overkill for some ingestion and replay workloads |
| object-log maintainer | Pressure-test core log semantics through a real Kafka-facing system | Library-only tests may miss protocol and coordinator requirements |

## Compatibility Levels

| Level | Meaning | Initial Status |
|-------|---------|----------------|
| L0: documented target | Kafka APIs, semantics, and open questions are specified | In scope now |
| L1: produce/fetch prototype | Standard clients can produce and fetch simple records for assigned partitions | Future |
| L2: consumer workflow | Consumer groups, offset commits/fetches, metadata, and rebalances work for common clients | Future |
| L3: operational Kafka subset | Topic admin, retention, compaction basics, auth/TLS/SASL, metrics, and failure recovery are supported | Future |
| L4: full compatibility target | Transactions, idempotent producers, read-committed fetch, quotas, ACLs, broad API/version coverage | Future |

## Critical Differentiation and Build/No-Build Rationale

fjord is deliberately entering a space with credible existing systems. The
closest is WarpStream: stateless agents speak the Kafka protocol, durable data
is stored in object storage, and a metadata/control-plane service makes Kafka's
stateful protocol expectations work over stateless data-plane nodes.

fjord's provisional build rationale is narrower:

| Differentiator | fjord Direction | Build Gate |
|----------------|-----------------|------------|
| Open/self-hostable | The whole system should run from source in a user's account | No required hosted vendor metadata service |
| S3/object-storage durability | Durable log data and, if feasible, durable metadata state use S3-compatible object storage | Any non-S3 durable metadata dependency must be explicitly justified |
| object-log embeddability | Core log mechanics are reusable by pqueue and Niflheim without fjord | fjord must not fork or duplicate object-log segment/manifest/replay logic |
| Simpler initial envelope | Accept higher latency and smaller API surface before full compatibility | Compatibility levels and unsupported APIs remain explicit |
| pqueue/Niflheim alignment | fjord validates the same log contract those systems may use directly | Requirements stay product-neutral and Kafka-shaped |

If fjord cannot satisfy these gates, the strategic recommendation should change
from "build fjord" to "use or contribute to WarpStream/AutoMQ/Bufstream/Kafka
Diskless Topics and keep object-log as an embeddable non-broker library."

## Requirements

### Must Have (P0)

- Kafka protocol and client surface design (FR-1 — FR-5)
- Produce path with explicit durability semantics (FR-6 — FR-10)
- Fetch path over object-log segments (FR-11 — FR-14, FR-31)
- Consumer groups and durable offsets (FR-15 — FR-18)
- Metadata, leadership, and coordination boundary (FR-19 — FR-22, FR-32)
- Object storage and object-log reuse rules (FR-23 — FR-26)

### Should Have (P1)

- Operations, performance, and observability surfaces (FR-27 — FR-30)

### Nice to Have (P2)

- Transactions, exactly-once, compaction, ACLs/quotas, and broad admin API
  coverage — explicitly phased behind compatibility levels L3/L4 and never
  implied before they are designed and tested.

## Functional Requirements

### Subsystem: Kafka Protocol and Client Surface

- **FR-1** — fjord MUST target the Apache Kafka binary TCP protocol, including request/response framing and API version negotiation.
- **FR-2** — fjord MUST support Metadata behavior sufficient for clients to discover brokers, topics, partitions, and the node they should address.
- **FR-3** — fjord MUST define a compatibility matrix of Kafka APIs and versions before implementing each level, including at minimum ApiVersions, Metadata, Produce, Fetch, ListOffsets, OffsetCommit, OffsetFetch, FindCoordinator, JoinGroup, SyncGroup, Heartbeat, LeaveGroup, CreateTopics, and DescribeConfigs.
- **FR-4** — fjord MUST return Kafka-compatible error codes for unsupported API versions, stale metadata, unavailable partitions, authorization failures, and transient storage errors.
- **FR-5** — fjord MUST support standard Kafka client batching for produce and fetch requests across topics and partitions.

### Subsystem: Produce Path

- **FR-6** — Producers MUST be able to append records with topic, partition, key, value, headers, timestamp, producer metadata, and batch attributes.
- **FR-7** — Acknowledged produce responses MUST correspond to records durably committed through object-log to object storage at the configured ack boundary.
- **FR-8** — `acks=0`, `acks=1`, and `acks=all` behavior MUST be explicitly mapped to fjord durability semantics; any weaker-than-Kafka behavior must be rejected or documented as unsupported.
- **FR-9** — fjord MUST support per-partition monotonic offsets and preserve Kafka ordering semantics for records accepted into a partition.
- **FR-10** — Idempotent producer state, producer epochs, and sequence numbers MUST be designed before claiming compatibility with idempotent producers.

### Subsystem: Fetch Path

- **FR-11** — Consumers MUST be able to fetch records by topic partition and offset using Kafka Fetch semantics for the supported API versions.
- **FR-12** — fjord MUST define how object-log segment indexes, object reads, local caches, and prefetch serve low-latency fetch without making local disk authoritative.
- **FR-13** — Fetch MUST preserve committed offset visibility and MUST detect gaps or corrupt object-log segments before returning records.
- **FR-14** — fjord MUST define high-watermark, log-start-offset, last-stable-offset, and leader-epoch behavior for object-storage-backed partitions.
- **FR-31** — fjord MUST define whether object-log manifests or a separate metadata store are the source of ordering when object files are written out of offset order.

### Subsystem: Consumer Groups and Offsets

- **FR-15** — fjord MUST support durable consumer offset commit and fetch behavior for supported clients.
- **FR-16** — fjord MUST define the authority for group coordination, membership, heartbeat, assignment, generation/member epoch, and rebalance state.
- **FR-17** — Consumer offset state MUST survive node loss and MUST NOT depend on local broker disk.
- **FR-18** — Group coordinator routing and FindCoordinator behavior MUST be specified before L2 compatibility.

### Subsystem: Metadata, Leadership, and Coordination

- **FR-19** — fjord MUST decide whether client-visible partition leadership is emulated over leaderless internals, assigned to nodes, or mapped to a metadata service.
- **FR-20** — Leader epoch, partition epoch, producer snapshots, and object-log manifest state MUST be coherent after node failure and reassignment.
- **FR-21** — fjord MUST not introduce hidden durable local state; node-local files may only be cache.
- **FR-22** — fjord MUST define a metadata/control-plane backend boundary for topics, partitions, epochs, groups, offsets, producer state, ACLs, and service membership.
- **FR-32** — fjord MUST decide whether durable metadata is stored in object storage, in object-log internal topics, or in a separate self-hosted metadata store; a hosted metadata service MUST NOT be required for the core product.

### Subsystem: Object Storage and object-log

- **FR-23** — Durable record data MUST be stored through object-log segments in S3-compatible object storage.
- **FR-24** — Production profiles MUST batch records across partitions where compatible with ordering and visibility semantics, avoiding one-object-per-record and tiny-object patterns.
- **FR-25** — Segment manifests, indexes, checksums, and retention metadata MUST make replay deterministic after node loss.
- **FR-26** — object-log MUST remain embeddable and product-neutral; fjord-specific protocol/coordinator state MUST stay in fjord.

### Subsystem: Operations, Performance, and Observability

- **FR-27** — fjord MUST publish latency/cost profiles that explain expected produce ack latency, fetch latency, object operation counts, and storage cost.
- **FR-28** — fjord nodes MUST be replaceable without copying partition data between nodes.
- **FR-29** — fjord MUST expose metrics for produce/fetch latency, object PUT/GET/LIST counts, segment size, cache hit rate, group rebalance activity, and metadata errors.
- **FR-30** — fjord MUST support failure tests for node loss, object-store transient failure, metadata-store conflict, corrupted segment, stale epoch, and cache loss.

## Risks

| Risk | Impact | Mitigation |
|------|--------|------------|
| fjord collapses into a less mature WarpStream clone | Project produces no differentiated value | Build/no-build gate (FEAT-007) reviewed at every milestone; stop condition recorded in ADR-001 |
| S3-only durable metadata proves impractical for groups/offsets/epochs | Core differentiation weakens; coordination latency unacceptable | TD-002 keeps object-log internal topics preferred with optional self-hosted Postgres mode as an explicit, justified exception |
| Object-storage commit latency breaks real producer workloads | Product fit fails for latency-sensitive streams | Publish latency/cost profiles (FR-27); target latency-tolerant workloads first; batching thresholds configurable |
| Consumer group coordination is a larger distributed-systems surface than planned | L2 slips or ships incorrect rebalance semantics | Group coordinator design is a named follow-up ADR before M5; standard-client rebalance tests gate L2 |
| object-log hardening (S3 adapter, retention, conformance) slips | fjord M3+ blocked | Implementation plan orders protocol/metadata work first; object-log milestones tracked in its own repo |

## Open Design Questions

| Question | Why It Matters | Blocks |
|----------|----------------|--------|
| Leader model | Kafka clients expect leaders, while object-storage systems may prefer any-node serving | Metadata, Produce, Fetch |
| Metadata store | Kafka compatibility needs coordination state beyond object bytes; fjord also wants no required hosted control plane | Groups, offsets, epochs, admin APIs |
| S3-only metadata durability | If durable state must be S3-only, group and metadata serialization may need object-log/internal-topic design rather than Postgres/etcd | Build/no-build |
| Metadata API emulation | Standard clients route by Metadata leader assignments even if fjord has no real leaders | Produce, Fetch, load balancing |
| Synthetic per-AZ leaders | Zone-local routing may need Metadata manipulation similar to WarpStream | Cost, availability |
| Fetch index/cache shape | Object reads can dominate latency and cost | Consumer performance |
| Consumer group authority | Group coordinator state is correctness-critical | L2 compatibility |
| Offset storage | Offset commits must be durable and efficiently fetchable | Consumer restart correctness |
| object-log as metadata authority | The manifest may be enough for ordering but not enough for group coordination | Architecture ADR |
| Small produce latency/cost | Single-message or tiny-batch producers may be expensive or slow on object storage | Product fit |
| Idempotent producers | Duplicate suppression requires producer state and sequence tracking | Safe retries |
| Transactions/read_committed | Exactly-once compatibility requires transaction markers and offset transactions | L4 compatibility |
| Compaction and retention | Kafka topics often rely on delete/compact policies | Operational compatibility |
| Protocol version floor | Supporting too many versions early increases surface area; core APIs include ApiVersions, Metadata, Produce, Fetch, OffsetFetch/Commit, JoinGroup/SyncGroup/Heartbeat, FindCoordinator, ListOffsets, CreateTopics, DescribeConfigs | Test scope |
| Security model | Kafka users expect TLS/SASL/ACLs | Production readiness |

## Acceptance Test Sketches

| Requirement | Scenario | Expected Result |
|-------------|----------|-----------------|
| FR-1/FR-3 | Java Kafka client sends ApiVersions | fjord returns supported versions for its declared compatibility level |
| FR-2 | Client sends Metadata after topic creation | response routes topic partitions according to fjord's chosen leader model |
| FR-6/FR-7 | Producer sends a batch with `acks=all` | response returns offsets only after object-log durable commit |
| FR-11 | Consumer fetches from offset 0 | records are returned in partition offset order |
| FR-15 | Consumer commits offset then node dies | replacement node returns the committed offset |
| FR-19 | Request arrives at non-owner/non-leader node | behavior matches documented leader model and Kafka error/routing semantics |
| FR-24 | Production profile uses 1 record per object | configuration is rejected |

## Research Inputs

- WarpStream: stateless Kafka-protocol agents over object storage with separate metadata/control plane.
- WarpStream protocol research: Kafka Metadata responses can be manipulated for zone-aware routing and load balancing because clients expect broker/leaders even when any agent can serve requests.
- WarpStream read-path research: metadata store ordering can be authoritative because object files may be written out of order.
- AutoMQ: S3Stream design with stream-set objects, metadata streams, leader epoch snapshots, and producer snapshots.
- AutoMQ comparison: WAL-backed durability can lower write latency, but fjord's target is closer to WarpStream's no-intermediary-disk direction unless a WAL is itself object-storage-backed.
- Bufstream: Kafka-compatible object-storage-backed service with a separate metadata store.
- Apache Kafka protocol and delivery semantics: protocol version negotiation, metadata, leader-routed produce/fetch, batching, committed offsets, idempotent producers, and transactions.
- Kafka producer API/configs: `ProducerRecord` partition/key/timestamp semantics plus `acks`, idempotence, retries, and in-flight ordering must be mapped explicitly.
- Kafka Diskless Topics / KIP-1150: ecosystem validation that active object-storage-backed Kafka topics are a serious direction.

## Success Criteria

- HELIX docs distinguish fjord from object-log and from Kafka-to-S3 sinks.
- HELIX docs include a build/no-build differentiation test against WarpStream and similar systems.
- Compatibility levels and open design questions are explicit.
- Produce, fetch, metadata, consumer group, offset, and coordinator requirements are documented before code.
- The test plan uses standard Kafka clients and performance tools as gates.
