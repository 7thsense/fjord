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

fjord is a Kafka-compatible streaming system that runs as **stateless brokers +
object storage (for record data) + a pluggable, self-hosted central coordinator**
(default Postgres). Record data is written through the embeddable `object-log`
core into S3-compatible object storage; sequencing, metadata, consumer-group, and
transaction state live in the coordinator. There is no hosted control plane, no
replicated broker disk, and no bespoke consensus system to operate — two
operational pieces: a broker pool and a metadata store the team already runs
(ADR-008, COORD-001).

The product is honest about scope, governed by compatibility levels. Produce,
fetch, metadata, offsets, and consumer groups are P0. Transactions / exactly-once
are now **designed** (TD-008: one coordinator transaction) and on the parity
surface; compaction, ACLs, and the full admin surface remain phased and explicit
via the API-001 capability matrix — never hidden.

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

- Require any hosted or SaaS dependency (including a hosted metadata/control
  plane) to run the core product.
- Build a Kafka Connect S3 sink or classic Kafka tiered-storage plugin.
- Claim production Kafka compatibility before protocol, group, offset, failure,
  and performance tests pass against real Kafka/Redpanda (TP-003).
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

| Level | Meaning | Closure Evidence |
|-------|---------|------------------|
| L0: documented target | Kafka APIs, semantics, and open questions are specified | API-001 + TD matrix reviewed |
| L1: produce/fetch surface | Standard clients can produce and fetch simple records for assigned partitions | Kafka differential + client smoke tests |
| L2: consumer workflow | Consumer groups, offset commits/fetches, metadata, and rebalances work for common clients | Consumer-group differential + restart/fault tests |
| L3: operational Kafka subset | Topic admin, retention, compaction basics, auth/TLS/SASL, metrics, and failure recovery are supported | Kafka CLI/admin/security/fault evidence |
| L4: full compatibility target | Transactions, idempotent producers, read-committed fetch, quotas, ACLs, broad API/version coverage | EOS, Jepsen/history, and broad API/version evidence |

## Critical Differentiation and Build/No-Build Rationale

fjord enters a space with credible existing systems. The closest are WarpStream
(stateless agents over object storage with a **hosted** metadata control plane)
and Redpanda Cloud Topics (object storage with **per-partition Raft**). fjord is
"self-hosted WarpStream without a consensus system": stateless brokers + object
storage + a self-hosted coordinator the operator already runs.

| Differentiator | fjord Direction | Build Gate |
|----------------|-----------------|------------|
| Open/self-hostable, no hosted control plane | The whole system runs from source in the user's account; the coordinator is self-hosted (default Postgres) | No required hosted/SaaS metadata service (vs WarpStream) |
| No bespoke consensus system | Coordination reuses a store the operator already runs, not an embedded Raft | No custom consensus cluster to operate (vs Redpanda) |
| Object-storage data durability | Durable record data uses S3-compatible object storage; coordination state lives in the pluggable coordinator | Record data is object-storage-exclusive; no replicated broker disk |
| object-log embeddability | Core log mechanics are reusable by pqueue and Niflheim without fjord | fjord must not fork or duplicate object-log segment/manifest/replay logic |
| Cost + operational simplicity | Win on $/GB and ops (no inter-AZ replication, stateless brokers); latency competitive but not a WarpStream-parity claim | Cost/ops parity-or-better vs WarpStream-class; latency better than classic Kafka |

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

- **FR-19** — fjord MUST present a client-visible partition leader in Metadata for routing/cache-locality while keeping offset sequencing in the central coordinator; brokers are stateless and any broker may serve any partition (ADR-008).
- **FR-20** — Leader epoch, partition epoch, producer state, and the object→offset index MUST be coherent after node failure and reassignment (coordinator-owned; TD-007).
- **FR-21** — fjord MUST not introduce hidden durable local state on brokers; node-local files may only be cache.
- **FR-22** — fjord MUST keep topics, partitions, epochs, groups, offsets, producer state, and broker membership in the pluggable central coordinator behind the `CoordinatorStore` contract (COORD-001).
- **FR-32** — Durable coordination state MUST live in a self-hosted coordinator backend (default Postgres; etcd/Dragonfly behind COORD-001; object-log internal topics optional); a hosted/SaaS metadata service MUST NOT be required for the core product.

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
| Coordinator is a throughput/availability bottleneck or SPOF-class dependency | Produce stalls; cluster availability floor becomes the coordinator's HA | Default Postgres is ms-class with well-understood HA; `CoordinatorStore` declares consistency/durability capability gates (COORD-001); SPIKE-001 characterizes per-backend latency/throughput before a backend is supported |
| Object-storage commit latency breaks real producer workloads | Product fit fails for latency-sensitive streams | Publish latency/cost profiles (FR-27); target latency-tolerant workloads first; batching thresholds configurable |
| Consumer group coordination is a larger distributed-systems surface than planned | L2 slips or ships incorrect rebalance semantics | Group coordinator design is a named follow-up ADR before M5; standard-client rebalance tests gate L2 |
| object-log hardening (S3 adapter, retention, conformance) slips | fjord M3+ blocked | Implementation plan orders protocol/metadata work first; object-log milestones tracked in its own repo |

## Resolved Design Questions

| Question | Resolution | Recorded In |
|----------|------------|-------------|
| Sequencing / metadata authority | A pluggable **central coordinator** (default self-hosted Postgres; etcd/Dragonfly behind COORD-001; object-log internal topics optional) is the per-partition serialization point and home of metadata, sequencing, group, and txn state | ADR-008, COORD-001 |
| Broker model / leadership | **Stateless brokers**; any broker serves any partition by calling the coordinator. The Metadata "leader" is a routing/cache-locality hint, not a write-correctness boundary; `NOT_LEADER_OR_FOLLOWER` is a routing convention | ADR-008, TD-005 |
| Durable metadata placement / hosted-service gate | In the coordinator (self-hosted). "Single durable substrate" is given up; "**no hosted/SaaS control plane**" still binding | ADR-008 (supersedes ADR-004) |
| object-log role | Object-storage IO for record data (L0/L1 segments); offset assignment is owned by fjord above object-log's `ObjectStore` | ADR-005, TD-005 |
| Consumer group authority | Coordinator = single owner of the groups shard for `hash(group)`; monotonic generation enforced by the store | TD-007 |
| Offset storage | `committed_offsets` rows in the coordinator, durable in the commit transaction; EOS offsets flip pending→committed atomically with the txn | TD-007, TD-008 |
| Transactions / exactly-once | **Designed**: `end_txn` is one coordinator transaction (decision + offset-flip + LSO advance sync; marker materialization async); on the parity surface | TD-008, COORD-001 |
| Idempotent producers | Epoch fencing + last-5 `(seq→offset)` map under the partition-state row lock in `commit_object` | TD-007, COORD-001 |
| acks=1 semantics | Upgraded to durable commit by default, disclosed in latency/cost profiles; reject available per profile | API-001, TD-003 |
| Protocol version floor | First flexible version (KIP-482) per API; legacy versions rejected except ApiVersions request parsing | API-001, TD-001 |

## Open Design Questions

(Fetch index/cache shape → resolved in TD-006; idempotent producers → TD-007;
transactions/read_committed → TD-008. Remaining genuinely-open items:)

| Question | Why It Matters | Blocks |
|----------|----------------|--------|
| Coordinator latency/throughput per backend | The coordinator commit is on the produce critical path; Postgres expected to pass, etcd/Dragonfly need characterization | Produce-floor confidence (SPIKE-001, re-pointed to COORD-001 backends) |
| Small produce latency/cost | Single-message or tiny-batch producers may be expensive or slow on object storage even with a fast coordinator | Product fit |
| Coordinator HA / sizing guidance | The coordinator is now the availability floor and a required component | Operational readiness (per-backend HA: PG replication, etcd quorum, Dragonfly persistence) |
| Compaction and retention | Kafka topics rely on delete/compact policies; L0→L1 compaction must keep up with ingest | Operational compatibility (TD-005 compaction; D9 invariant) |
| Security model | Kafka users expect TLS/SASL/ACLs; L1 ships plumbing only (API-001 principle 6) | Production readiness (API-001 capability matrix: Defer) |

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
